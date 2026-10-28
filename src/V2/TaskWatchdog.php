<?php

declare(strict_types=1);

namespace Workflow\V2;

use Illuminate\Support\Facades\Cache;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Schema;
use Throwable;
use Workflow\V2\Contracts\HistoryProjectionRole;
use Workflow\V2\Enums\ActivityAttemptStatus;
use Workflow\V2\Enums\ActivityStatus;
use Workflow\V2\Enums\RunStatus;
use Workflow\V2\Enums\TaskStatus;
use Workflow\V2\Enums\TaskType;
use Workflow\V2\Models\ActivityAttempt;
use Workflow\V2\Models\ActivityExecution;
use Workflow\V2\Models\WorkerCompatibilityHeartbeat;
use Workflow\V2\Models\WorkflowRun;
use Workflow\V2\Models\WorkflowRunSummary;
use Workflow\V2\Models\WorkflowTask;
use Workflow\V2\Support\ActivityRowLockOrder;
use Workflow\V2\Support\ActivityTimeoutEnforcer;
use Workflow\V2\Support\RequestedTaskScope;
use Workflow\V2\Support\TaskCompatibility;
use Workflow\V2\Support\TaskDispatcher;
use Workflow\V2\Support\TaskRepair;
use Workflow\V2\Support\TaskRepairCandidates;
use Workflow\V2\Support\TaskRepairPolicy;
use Workflow\V2\Support\WorkerCompatibilityFleet;
use Workflow\V2\Support\WorkflowExecutor;

/**
 * Periodic repair loop that scans for stuck workflow tasks, expired activity
 * leases, and missing projections. Invoked by the embedded queue runner and
 * by the standalone server's background sweep process.
 *
 * @api Stable class surface consumed by the standalone workflow-server.
 *      The public static method signatures on this class are covered by
 *      the workflow package's semver guarantee. See docs/api-stability.md.
 */
final class TaskWatchdog
{
    public const LOOP_THROTTLE_KEY = 'workflow:v2:task-watchdog:looping';

    public static function wake(?string $connection = null, ?string $queue = null): void
    {
        try {
            self::runPass($connection, $queue, respectThrottle: true);
        } catch (Throwable $throwable) {
            // Queue workers call wake() on every poll; a transient table-not-found
            // error during test migrate:fresh windows or a deadlock under heavy
            // contention must not kill the worker. The next poll re-enters the
            // pass once the schema/locks settle.
            report($throwable);
        }
    }

    /**
     * @return array{
     *     connection: string|null,
     *     queue: string|null,
     *     run_ids: list<string>,
     *     instance_id: string|null,
     *     respect_throttle: bool,
     *     throttled: bool,
     *     selected_existing_task_candidates: int,
     *     selected_missing_task_candidates: int,
     *     selected_total_candidates: int,
     *     repaired_existing_tasks: int,
     *     repaired_missing_tasks: int,
     *     dispatched_tasks: int,
     *     existing_task_failures: list<array{candidate_id: string, message: string}>,
     *     missing_run_failures: list<array{run_id: string, message: string}>,
     *     deadline_expired_candidates: int,
     *     deadline_expired_tasks_created: int,
     *     deadline_expired_failures: list<array{run_id: string, message: string}>,
     *     activity_timeout_candidates: int,
     *     activity_timeouts_enforced: int,
     *     activity_timeout_failures: list<array{execution_id: string, message: string}>
     * }
     */
    public static function runPass(
        ?string $connection = null,
        ?string $queue = null,
        bool $respectThrottle = false,
        array $runIds = [],
        ?string $instanceId = null,
    ): array {
        if (! self::tablesReady()) {
            return self::emptyReport($connection, $queue, $respectThrottle, $runIds, $instanceId);
        }

        if ($respectThrottle) {
            if (! Cache::add(self::LOOP_THROTTLE_KEY, true, TaskRepairPolicy::loopThrottleSeconds())) {
                return self::emptyReport(
                    $connection,
                    $queue,
                    $respectThrottle,
                    $runIds,
                    $instanceId,
                    throttled: true,
                );
            }
        } else {
            Cache::put(self::LOOP_THROTTLE_KEY, true, TaskRepairPolicy::loopThrottleSeconds());
        }

        WorkerCompatibilityFleet::heartbeat($connection, $queue);

        $existingTaskCandidateIds = TaskRepairCandidates::taskIds(
            runIds: $runIds,
            instanceId: $instanceId,
            connection: $connection,
            queue: $queue,
        );
        $missingRunIds = TaskRepairCandidates::runIds(
            runIds: $runIds,
            instanceId: $instanceId,
            connection: $connection,
            queue: $queue,
        );
        $report = self::emptyReport($connection, $queue, $respectThrottle, $runIds, $instanceId);
        $report['selected_existing_task_candidates'] = count($existingTaskCandidateIds);
        $report['selected_missing_task_candidates'] = count($missingRunIds);
        $report['selected_total_candidates'] = count($existingTaskCandidateIds) + count($missingRunIds);

        foreach ($existingTaskCandidateIds as $candidateId) {
            $result = self::recoverExistingTask($candidateId);

            if ($result['task'] instanceof WorkflowTask) {
                $report['repaired_existing_tasks']++;
                $report['dispatched_tasks']++;
            }

            if ($result['error'] !== null) {
                $report['existing_task_failures'][] = [
                    'candidate_id' => $candidateId,
                    'message' => $result['error'],
                ];
            }
        }

        foreach ($missingRunIds as $runId) {
            $result = self::recoverMissingTask($runId);

            if ($result['task'] instanceof WorkflowTask) {
                $report['repaired_missing_tasks']++;
                $report['dispatched_tasks']++;
            }

            if ($result['error'] !== null) {
                $report['missing_run_failures'][] = [
                    'run_id' => $runId,
                    'message' => $result['error'],
                ];
            }
        }

        $deadlineExpiredRunIds = self::deadlineExpiredRunIds($runIds, $instanceId, $connection, $queue);
        $report['deadline_expired_candidates'] = count($deadlineExpiredRunIds);

        foreach ($deadlineExpiredRunIds as $deadlineRunId) {
            $result = self::createDeadlineExpiredTask($deadlineRunId);

            if ($result['task'] instanceof WorkflowTask) {
                $report['deadline_expired_tasks_created']++;
                $report['dispatched_tasks']++;
            }

            if ($result['error'] !== null) {
                $report['deadline_expired_failures'][] = [
                    'run_id' => $deadlineRunId,
                    'message' => $result['error'],
                ];
            }
        }

        $activityTimeoutIds = ActivityTimeoutEnforcer::expiredExecutionIds(
            TaskRepairPolicy::scanLimit(),
            $connection,
            $queue,
        );
        $report['activity_timeout_candidates'] = count($activityTimeoutIds);

        foreach ($activityTimeoutIds as $activityExecutionId) {
            try {
                $result = ActivityTimeoutEnforcer::enforce($activityExecutionId);

                if ($result['enforced']) {
                    $report['activity_timeouts_enforced']++;

                    if ($result['next_task'] instanceof WorkflowTask) {
                        $report['dispatched_tasks']++;
                        TaskDispatcher::dispatch($result['next_task']);
                    }
                }

                if ($result['reason'] !== null) {
                    $report['activity_timeout_failures'][] = [
                        'execution_id' => $activityExecutionId,
                        'message' => $result['reason'],
                    ];
                }
            } catch (Throwable $throwable) {
                report($throwable);

                $report['activity_timeout_failures'][] = [
                    'execution_id' => $activityExecutionId,
                    'message' => $throwable->getMessage(),
                ];
            }
        }

        return $report;
    }

    /**
     * @return array{task: WorkflowTask|null, error: string|null}
     */
    private static function recoverExistingTask(string $candidateId): array
    {
        try {
            $connection = (new WorkflowTask())->getConnection();
            $task = $connection->transaction(static function () use ($candidateId): ?WorkflowTask {
                /** @var WorkflowTask|null $task */
                $snapshot = WorkflowTask::query()->find($candidateId);

                if ($snapshot?->task_type === TaskType::Activity) {
                    [$task, $run] = self::lockActivityRecoveryRows($snapshot);
                } else {
                    $run = $snapshot === null ? null : WorkflowRun::query()
                        ->lockForUpdate()
                        ->find($snapshot->workflow_run_id);
                    $task = $run === null ? null : WorkflowTask::query()
                        ->where('workflow_run_id', $run->id)
                        ->lockForUpdate()
                        ->find($candidateId);

                    // A changed routing snapshot must be reconsidered on the next pass.
                    if ($task?->task_type === TaskType::Activity) {
                        return null;
                    }
                }

                if ($task === null || $run === null) {
                    return null;
                }

                if (! TaskRepairPolicy::readyTaskNeedsRedispatch($task) && ! TaskRepairPolicy::leaseExpired($task)) {
                    return null;
                }

                TaskCompatibility::sync($task, $run);

                $task = TaskRepair::recoverExistingTask($task, $run);

                self::historyProjectionRole()->projectRun(
                    $run->fresh(['instance', 'tasks', 'activityExecutions', 'timers', 'failures', 'historyEvents'])
                );

                return $task;
            });

            if ($task instanceof WorkflowTask) {
                TaskDispatcher::dispatch($task);
            }
        } catch (Throwable $throwable) {
            report($throwable);

            WorkflowTask::query()
                ->whereKey($candidateId)
                ->update([
                    'last_error' => $throwable->getMessage(),
                ]);

            return [
                'task' => null,
                'error' => $throwable->getMessage(),
            ];
        }

        return [
            'task' => $task,
            'error' => null,
        ];
    }

    /**
     * Fence normalization to a prelocked attempt or the supported unclaimed
     * legacy identity. Partial identities are left for explicit repair.
     *
     * @return array{WorkflowTask|null, WorkflowRun|null}
     */
    private static function lockActivityRecoveryRows(WorkflowTask $snapshot): array
    {
        $executionId = $snapshot->payload['activity_execution_id'] ?? null;

        if (! is_string($executionId) || $executionId === '') {
            return [null, null];
        }

        $rows = ActivityRowLockOrder::lockForExecution($executionId, includeClosedAttempts: true);
        $execution = $rows['execution'];

        if (! $execution instanceof ActivityExecution) {
            return [null, null];
        }

        $run = WorkflowRun::query()->lockForUpdate()->find($execution->workflow_run_id);
        $task = WorkflowTask::query()->lockForUpdate()->find($snapshot->id);

        if (
            ! $run instanceof WorkflowRun
            || ! $task instanceof WorkflowTask
            || $task->task_type !== TaskType::Activity
            || $task->workflow_run_id !== $run->id
            || ($task->payload['activity_execution_id'] ?? null) !== $execution->id
            || $rows['snapshot_attempt_id'] !== $execution->current_attempt_id
        ) {
            return [null, null];
        }

        $attempt = $rows['attempt'];
        $legacyLease = $task->status === TaskStatus::Leased
            && TaskRepairPolicy::leaseExpired($task)
            && $execution->status === ActivityStatus::Running
            && $execution->current_attempt_id === null
            && in_array($execution->attempt_count, [0, 1], true)
            && in_array($task->attempt_count, [0, 1], true)
            && $task->attempt_count <= $execution->attempt_count;

        if (! $legacyLease && $task->attempt_count !== $execution->attempt_count) {
            return [null, null];
        }

        if (! $legacyLease && ($execution->attempt_count > 0 || $execution->current_attempt_id !== null)) {
            if (
                ! $attempt instanceof ActivityAttempt
                || $attempt->activity_execution_id !== $execution->id
                || $attempt->workflow_run_id !== $run->id
                || $attempt->attempt_number !== $execution->attempt_count
                || (($task->status === TaskStatus::Leased || $execution->status === ActivityStatus::Running)
                    && $attempt->workflow_task_id !== $task->id)
            ) {
                return [null, null];
            }
        } else {
            $initialTask = $task->status === TaskStatus::Ready
                && $execution->status === ActivityStatus::Pending
                && $execution->started_at === null
                && $execution->closed_at === null;

            if (! $legacyLease && ! $initialTask) {
                return [null, null];
            }

            if ($attempt instanceof ActivityAttempt && (
                $attempt->activity_execution_id !== $execution->id
                || $attempt->workflow_run_id !== $run->id
                || $attempt->workflow_task_id !== $task->id
                || $attempt->attempt_number !== 1
                || ! in_array($attempt->status, [ActivityAttemptStatus::Running, ActivityAttemptStatus::Expired], true)
            )) {
                return [null, null];
            }
        }

        if (! $run->status->isTerminal() && $task->status === TaskStatus::Ready) {
            if ($execution->status === ActivityStatus::Running) {
                if ($attempt?->status !== ActivityAttemptStatus::Expired) {
                    return [null, null];
                }
            } elseif ($execution->status !== ActivityStatus::Pending || ($attempt !== null && ! in_array(
                $attempt->status,
                [ActivityAttemptStatus::Failed, ActivityAttemptStatus::Expired],
                true
            ))) {
                return [null, null];
            }
        }

        return [$task, $run];
    }

    /**
     * @return array{task: WorkflowTask|null, error: string|null}
     */
    private static function recoverMissingTask(string $runId): array
    {
        try {
            $task = DB::transaction(static function () use ($runId): ?WorkflowTask {
                /** @var WorkflowRun $run */
                $run = WorkflowRun::query()
                    ->with(['instance', 'tasks', 'activityExecutions', 'timers', 'failures', 'historyEvents'])
                    ->lockForUpdate()
                    ->findOrFail($runId);

                app(WorkflowExecutor::class)->recoverClosedChildResolutions($run);
                $summary = self::historyProjectionRole()->projectRun($run);

                if ($summary->liveness_state !== 'repair_needed' || $summary->next_task_id !== null) {
                    return null;
                }

                $task = TaskRepair::repairRun($run, $summary);

                self::historyProjectionRole()->projectRun(
                    $run->fresh(['instance', 'tasks', 'activityExecutions', 'timers', 'failures', 'historyEvents'])
                );

                return $task;
            }, attempts: 3);

            if ($task instanceof WorkflowTask) {
                TaskDispatcher::dispatch($task);
            }
        } catch (Throwable $throwable) {
            report($throwable);

            return [
                'task' => null,
                'error' => $throwable->getMessage(),
            ];
        }

        return [
            'task' => $task,
            'error' => null,
        ];
    }

    /**
     * @return array{
     *     connection: string|null,
     *     queue: string|null,
     *     run_ids: list<string>,
     *     instance_id: string|null,
     *     respect_throttle: bool,
     *     throttled: bool,
     *     selected_existing_task_candidates: int,
     *     selected_missing_task_candidates: int,
     *     selected_total_candidates: int,
     *     repaired_existing_tasks: int,
     *     repaired_missing_tasks: int,
     *     dispatched_tasks: int,
     *     existing_task_failures: list<array{candidate_id: string, message: string}>,
     *     missing_run_failures: list<array{run_id: string, message: string}>,
     *     deadline_expired_candidates: int,
     *     deadline_expired_tasks_created: int,
     *     deadline_expired_failures: list<array{run_id: string, message: string}>,
     *     activity_timeout_candidates: int,
     *     activity_timeouts_enforced: int,
     *     activity_timeout_failures: list<array{execution_id: string, message: string}>
     * }
     */
    private static function emptyReport(
        ?string $connection,
        ?string $queue,
        bool $respectThrottle,
        array $runIds,
        ?string $instanceId,
        bool $throttled = false,
    ): array {
        return [
            'connection' => $connection,
            'queue' => $queue,
            'run_ids' => array_values($runIds),
            'instance_id' => $instanceId,
            'respect_throttle' => $respectThrottle,
            'throttled' => $throttled,
            'selected_existing_task_candidates' => 0,
            'selected_missing_task_candidates' => 0,
            'selected_total_candidates' => 0,
            'repaired_existing_tasks' => 0,
            'repaired_missing_tasks' => 0,
            'dispatched_tasks' => 0,
            'existing_task_failures' => [],
            'missing_run_failures' => [],
            'deadline_expired_candidates' => 0,
            'deadline_expired_tasks_created' => 0,
            'deadline_expired_failures' => [],
            'activity_timeout_candidates' => 0,
            'activity_timeouts_enforced' => 0,
            'activity_timeout_failures' => [],
        ];
    }

    /**
     * Find non-terminal runs with expired execution or run deadlines
     * that have no open workflow task to detect the timeout.
     *
     * @return list<string>
     */
    private static function deadlineExpiredRunIds(
        array $runIds = [],
        ?string $instanceId = null,
        ?string $connection = null,
        ?string $queue = null,
    ): array {
        $now = now();

        $query = WorkflowRun::query()
            ->whereIn('status', [RunStatus::Pending->value, RunStatus::Running->value, RunStatus::Waiting->value])
            ->where(static function ($deadline) use ($now): void {
                $deadline->where(static function ($execution) use ($now): void {
                    $execution->whereNotNull('execution_deadline_at')
                        ->where('execution_deadline_at', '<=', $now);
                })->orWhere(static function ($run) use ($now): void {
                    $run->whereNotNull('run_deadline_at')
                        ->where('run_deadline_at', '<=', $now);
                });
            })
            ->whereDoesntHave('tasks', static function ($task): void {
                $task->where('task_type', TaskType::Workflow->value)
                    ->whereIn('status', [TaskStatus::Ready->value, TaskStatus::Leased->value]);
            });

        if ($runIds !== []) {
            $query->whereKey($runIds);
        }

        if ($instanceId !== null) {
            $query->where('workflow_instance_id', $instanceId);
        }

        RequestedTaskScope::apply($query, $connection, $queue);

        return $query->limit(TaskRepairPolicy::scanLimit())->pluck('id')->all();
    }

    private static function tablesReady(): bool
    {
        try {
            foreach ([
                new WorkerCompatibilityHeartbeat(),
                new WorkflowTask(),
                new WorkflowRun(),
                new WorkflowRunSummary(),
                new ActivityExecution(),
            ] as $model) {
                if (! Schema::hasTable($model->getTable())) {
                    return false;
                }
            }
        } catch (Throwable) {
            return false;
        }

        return true;
    }

    /**
     * @return array{task: WorkflowTask|null, error: string|null}
     */
    private static function createDeadlineExpiredTask(string $runId): array
    {
        try {
            $task = DB::transaction(static function () use ($runId): ?WorkflowTask {
                /** @var WorkflowRun|null $run */
                $run = WorkflowRun::query()
                    ->lockForUpdate()
                    ->find($runId);

                if ($run === null || $run->status->isTerminal()) {
                    return null;
                }

                $now = now();
                $deadlineExpired = ($run->execution_deadline_at !== null && $now->gte($run->execution_deadline_at))
                    || ($run->run_deadline_at !== null && $now->gte($run->run_deadline_at));

                if (! $deadlineExpired) {
                    return null;
                }

                $existingWorkflowTask = WorkflowTask::query()
                    ->where('workflow_run_id', $run->id)
                    ->where('task_type', TaskType::Workflow->value)
                    ->whereIn('status', [TaskStatus::Ready->value, TaskStatus::Leased->value])
                    ->first();

                if ($existingWorkflowTask !== null) {
                    return null;
                }

                /** @var WorkflowTask $task */
                $task = WorkflowTask::query()->create([
                    'workflow_run_id' => $run->id,
                    'namespace' => $run->namespace,
                    'task_type' => TaskType::Workflow->value,
                    'status' => TaskStatus::Ready->value,
                    'available_at' => $now,
                    'payload' => [
                        'reason' => 'deadline_expired',
                    ],
                    'connection' => $run->connection,
                    'queue' => $run->queue,
                    'compatibility' => $run->compatibility,
                    'repair_count' => 1,
                ]);

                self::historyProjectionRole()->projectRun(
                    $run->fresh(['instance', 'tasks', 'activityExecutions', 'timers', 'failures', 'historyEvents'])
                );

                return $task;
            });

            if ($task instanceof WorkflowTask) {
                TaskDispatcher::dispatch($task);
            }
        } catch (Throwable $throwable) {
            report($throwable);

            return [
                'task' => null,
                'error' => $throwable->getMessage(),
            ];
        }

        return [
            'task' => $task,
            'error' => null,
        ];
    }

    private static function historyProjectionRole(): HistoryProjectionRole
    {
        /** @var HistoryProjectionRole $role */
        $role = app(HistoryProjectionRole::class);

        return $role;
    }
}
