<?php

declare(strict_types=1);

namespace Workflow\V2\Support;

use Illuminate\Database\Eloquent\Builder;
use Illuminate\Support\Collection;
use Workflow\V2\Enums\TaskStatus;
use Workflow\V2\Enums\TaskType;
use Workflow\V2\Models\WorkflowRun;
use Workflow\V2\Models\WorkflowTask;

/** Runtime selection never requires hydrating a run's completed task history. */
final class RuntimeTaskQuery
{
    /**
     * @return Builder<WorkflowTask>
     */
    public static function forRun(WorkflowRun $run): Builder
    {
        // Retain configured relation scopes/storage; each selection owns its ordering.
        return $run->tasks()
            ->getQuery()
            ->reorder();
    }

    /**
     * @return Collection<int, WorkflowTask>
     */
    public static function open(WorkflowRun $run): Collection
    {
        if ($run->relationLoaded('tasks')) {
            return $run->tasks->filter(static fn (WorkflowTask $task): bool => in_array(
                $task->status,
                [TaskStatus::Ready, TaskStatus::Leased],
                true,
            ));
        }

        return self::forRun($run)->whereIn('status', [TaskStatus::Ready->value, TaskStatus::Leased->value])
            ->orderBy('available_at')
            ->get();
    }

    /**
     * @return iterable<WorkflowTask>
     */
    public static function replayBlocked(WorkflowRun $run): iterable
    {
        if ($run->relationLoaded('tasks')) {
            return $run->tasks->filter(static fn (WorkflowTask $task): bool => $task->task_type === TaskType::Workflow
                && $task->status === TaskStatus::Failed && ($task->payload['replay_blocked'] ?? false) === true);
        }

        return self::forRun($run)->where('task_type', TaskType::Workflow->value)
            ->where('status', TaskStatus::Failed->value)->whereJsonContains('payload->replay_blocked', true)
            ->orderBy('available_at')
            ->lazy(128)
            ->filter(static fn (WorkflowTask $task): bool => ($task->payload['replay_blocked'] ?? false) === true);
    }

    /**
     * @return Builder<WorkflowTask>
     */
    public static function repairCandidates(WorkflowRun $run): Builder
    {
        return self::forRun($run)->where(static function (Builder $query): void {
            $query->whereIn('status', [TaskStatus::Ready->value, TaskStatus::Leased->value])
                ->orWhere(static function (Builder $query): void {
                    $query->where('task_type', TaskType::Workflow->value)->where('status', TaskStatus::Failed->value)
                        ->whereJsonContains('payload->replay_blocked', true);
                });
        });
    }

    /**
     * The caller owns the run lock before taking candidate task locks.
     */
    public static function lockCandidates(WorkflowRun $run): void
    {
        $query = self::repairCandidates($run)->select('id')->orderBy('available_at');
        if ($query->getModel()->getConnection()->getDriverName() === 'mysql') {
            // Mutable status-index scans can wait on tasks belonging to other runs.
            $query->forceIndex('workflow_tasks_workflow_run_id_index');
        }
        foreach ($query->lockForUpdate()->lazy(128) as $task) {
            // Keep row locks for the transaction without retaining the collection.
        }
    }

    public static function historicalProblem(WorkflowRun $run): bool
    {
        $query = self::forRun($run)->where('task_type', TaskType::Workflow->value);
        if ((clone $query)->where('repair_count', '>', 0)->exists()) {
            return true;
        }
        foreach ($query->whereJsonContains('payload->replay_blocked', true)->orderBy('id')->lazyById(128) as $task) {
            if (($task->payload['replay_blocked'] ?? false) === true) {
                return true;
            }
        }

        return false;
    }

    /**
     * @return iterable<WorkflowTask>
     */
    public static function withPayloadKey(WorkflowRun $run, string $key, ?TaskType $type = null): iterable
    {
        if ($run->relationLoaded('tasks')) {
            return $run->tasks;
        }

        return self::forRun($run)->when(
            $type !== null,
            static fn (Builder $query): Builder => $query->where('task_type', $type->value)
        )
            ->whereNotNull('payload->' . $key)->orderBy('available_at')->lazy(128);
    }
}
