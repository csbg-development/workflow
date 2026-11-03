<?php

declare(strict_types=1);

namespace Tests\Feature\V2;

use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Event;
use Illuminate\Support\Facades\Queue;
use ReflectionMethod;
use Tests\Fixtures\V2\TestSignalWorkflow;
use Tests\TestCase;
use Workflow\V2\Enums\HistoryEventType;
use Workflow\V2\Enums\TaskType;
use Workflow\V2\Jobs\RunWorkflowTask;
use Workflow\V2\Models\WorkflowHistoryEvent;
use Workflow\V2\Models\WorkflowInstance;
use Workflow\V2\Models\WorkflowRun;
use Workflow\V2\Models\WorkflowTask;
use Workflow\V2\Support\HistoryTimeline;
use Workflow\V2\Support\RunSummaryProjector;
use Workflow\V2\Support\RunWaitView;
use Workflow\V2\Support\TaskRepair;
use Workflow\V2\Support\UpdateWaits;
use Workflow\V2\Support\WorkflowExecutor;

final class V2HistoricalTaskLoadingTest extends TestCase
{
    public function testClaimAndWaitingProjectionDoNotHydrateCompletedTaskHistory(): void
    {
        $run = $this->waitingRun();
        $historical = $this->task($run, 'completed', [], [
            'repair_count' => 1,
        ]);
        WorkflowHistoryEvent::query()->where('workflow_run_id', $run->id)->update([
            'workflow_task_id' => $historical->id,
        ]);
        $lowQueries = $this->projectedTaskReads($run);
        for ($offset = 0; $offset < 3000; $offset += 250) {
            $rows = [];
            for ($i = $offset; $i < $offset + 250; $i++) {
                $rows[] = [
                    'id' => sprintf('%026d', $i + 1),
                    'workflow_run_id' => $run->id,
                    'task_type' => 'workflow',
                    'status' => 'completed',
                    'payload' => '{}',
                    'repair_count' => 0,
                    'attempt_count' => 1,
                    'created_at' => now(),
                    'updated_at' => now(),
                ];
            }
            DB::table('workflow_tasks')->insert($rows);
        }
        $highQueries = $this->projectedTaskReads($run);
        $this->assertLessThanOrEqual($lowQueries + 2, $highQueries);

        $hydrated = 0;
        Event::listen('eloquent.retrieved: ' . WorkflowTask::class, static function () use (&$hydrated): void {
            ++$hydrated;
        });
        $task = $this->task($run, 'ready');
        $claim = new ReflectionMethod(RunWorkflowTask::class, 'claimTask');
        $claim->setAccessible(true);
        $claimed = $claim->invoke(new RunWorkflowTask($task->id));
        $fresh = $run->fresh();
        $executor = new WorkflowExecutor();
        $this->assertFalse($executor->timeoutIfDeadlineExpired($fresh, $claimed));
        $waiting = new ReflectionMethod(WorkflowExecutor::class, 'markRunWaiting');
        $waiting->setAccessible(true);
        $waiting->invoke($executor, $fresh, $claimed);
        $this->assertLessThan(20, $hydrated);
        $this->assertFalse($fresh->relationLoaded('tasks'));
        $summary = RunSummaryProjector::project($run->fresh());
        $this->assertSame('waiting_for_signal', $summary->liveness_state);
        $this->assertTrue($summary->task_problem);
        $this->assertNull($summary->next_task_id);
        $this->assertSame(3002, $run->tasks()->count());
        $this->assertCount(1, HistoryTimeline::fromHistory($run->fresh()));
        $this->assertSame('completed', $task->fresh()->status->value);
    }

    public function testProjectionSelectionMatchesPreloadedTasksIncludingHistoricalRepairAndReplayEvidence(): void
    {
        $run = $this->waitingRun();
        $this->task($run, 'completed', [], [
            'repair_count' => 1,
        ]);
        $this->task($run, 'ready', [], [
            'available_at' => now()
                ->subHour(),
        ]);
        $this->task($run, 'leased', [], [
            'available_at' => null,
            'lease_expires_at' => now()
                ->addMinute(),
        ]);
        $this->task($run, 'failed', [
            'replay_blocked' => true,
        ], [
            'updated_at' => now()
                ->subMinute(),
        ]);
        $this->task($run, 'failed', [
            'replay_blocked' => [true],
        ], [
            'updated_at' => now(),
        ]);
        $columns = [
            'liveness_state',
            'next_task_id',
            'next_task_status',
            'wait_kind',
            'task_problem',
            'repair_blocked_reason',
        ];
        $expected = RunSummaryProjector::project($run->fresh(['tasks']))->only($columns);
        $this->assertSame($expected, RunSummaryProjector::project($run->fresh())->only($columns));
        $this->assertEquals(RunWaitView::forRun($run->fresh(['tasks'])), RunWaitView::forRun($run->fresh()));
    }

    public function testNonBooleanReplayMarkersDoNotBecomeTaskProblems(): void
    {
        $run = $this->waitingRun();
        $this->task($run, 'failed', [
            'replay_blocked' => [true],
        ]);
        $this->task($run, 'failed', [
            'replay_blocked' => 'true',
        ]);
        $this->task($run, 'completed', [
            'replay_blocked' => 1,
        ]);
        $summary = RunSummaryProjector::project($run->fresh());
        $this->assertFalse($summary->task_problem);
        $this->assertSame('waiting_for_signal', $summary->liveness_state);
    }

    public function testWaitTaskMatchingAndRepairOrderingPreservePreloadedSelection(): void
    {
        $run = $this->waitingRun();
        $this->task($run, 'ready', [], [
            'available_at' => now()
                ->subHour(),
        ]);
        $this->task($run, 'completed', [
            'workflow_update_id' => 'chosen-update',
        ]);
        $this->task($run, 'leased', [
            'workflow_wait_kind' => 'signal',
            'workflow_update_id' => 'chosen-update',
        ]);
        $this->task($run, 'cancelled', [
            'workflow_wait_kind' => null,
        ]);
        $this->task($run, 'failed', [
            'replay_blocked' => true,
        ], [
            'available_at' => now()
                ->subHours(2),
        ]);
        $method = new ReflectionMethod(UpdateWaits::class, 'preferredWorkflowTask');
        $method->setAccessible(true);
        foreach (['chosen-update', 'unmatched-update'] as $updateId) {
            $expected = $method->invoke(null, $run->fresh(['tasks']), $updateId, null, 'update:' . $updateId);
            $actual = $method->invoke(null, $run->fresh(), $updateId, null, 'update:' . $updateId);
            $this->assertSame($expected?->id, $actual?->id);
        }
        $repair = new ReflectionMethod(TaskRepair::class, 'recoverableTask');
        $repair->setAccessible(true);
        $this->assertSame(
            $repair->invoke(null, $run->fresh(['tasks']))?->id,
            $repair->invoke(null, $run->fresh())?->id
        );
    }

    public function testCandidateLocksDoNotWaitForAnUnrelatedRunsReadyTask(): void
    {
        if (DB::connection()->getDriverName() !== 'mysql') {
            $this->markTestSkipped('This locking proof requires MySQL/InnoDB.');
        }
        $run = $this->waitingRun();
        $rows = [];
        for ($i = 0; $i < 1000; $i++) {
            $rows[] = [
                'id' => sprintf('%026d', $i + 1),
                'workflow_run_id' => $run->id,
                'task_type' => 'workflow',
                'status' => 'completed',
                'payload' => '{}',
            ];
        }
        DB::table('workflow_tasks')->insert($rows);
        $this->task($run, 'ready');
        $otherRun = $this->waitingRun();
        $otherTask = $this->task($otherRun, 'ready', [], [
            'available_at' => now()->subHour(),
        ]);
        $other = (new \Illuminate\Database\Connectors\MySqlConnector())->connect(DB::connection()->getConfig());
        $timeout = (int) DB::selectOne('SELECT @@innodb_lock_wait_timeout AS seconds')->seconds;
        $other->beginTransaction();
        try {
            $statement = $other->prepare('SELECT id FROM workflow_tasks WHERE id=? FOR UPDATE');
            $statement->execute([$otherTask->id]);
            DB::statement('SET SESSION innodb_lock_wait_timeout=1');
            DB::transaction(static function () use ($run): void {
                $locked = WorkflowRun::query()->lockForUpdate()->findOrFail($run->id);
                \Workflow\V2\Support\RuntimeTaskQuery::lockCandidates($locked);
            });
            $this->assertSame('ready', $otherTask->fresh()->status->value);
            $this->assertSame(1001, $run->tasks()->count());
        } finally {
            DB::statement('SET SESSION innodb_lock_wait_timeout=' . $timeout);
            $other->rollBack();
        }
    }

    public function testRuntimeQueriesKeepTheParentRunsStorageConnection(): void
    {
        config()->set('workflows.storage.connection', null);
        config()
            ->set('database.connections.task_fixture', [
                'driver' => 'sqlite',
                'database' => ':memory:',
                'prefix' => '',
            ]);
        $connection = DB::connection('task_fixture');
        $connection->getSchemaBuilder()
            ->create('workflow_tasks', static function (\Illuminate\Database\Schema\Blueprint $table): void {
                $table->string('id')
                    ->primary();
                $table->string('workflow_run_id');
                $table->string('task_type');
                $table->string('status');
                $table->timestamp('available_at')
                    ->nullable();
                $table->json('payload')
                    ->nullable();
                $table->timestamps();
            });
        $run = (new WorkflowRun())->setConnection('task_fixture');
        $run->forceFill([
            'id' => 'storage-owner-run',
        ]);
        $task = (new WorkflowTask())->setConnection('task_fixture');
        $task->forceFill([
            'workflow_run_id' => $run->id,
            'task_type' => 'workflow',
            'status' => 'ready',
            'payload' => [],
        ])->save();
        $selected = \Workflow\V2\Support\RuntimeTaskQuery::open($run);
        $this->assertCount(1, $selected);
        $this->assertSame($task->id, $selected->first()->id);
        $this->assertSame('task_fixture', $selected->first()->getConnectionName());
    }

    public function testHistoricalReplayEvidenceSurvivesChunkingAndAvailabilityOrdering(): void
    {
        $run = $this->waitingRun();
        $this->task($run, 'completed', [
            'replay_blocked' => true,
        ], [
            'available_at' => now()->addDay(),
        ]);
        for ($i = 0; $i < 140; $i++) {
            $this->task($run, 'completed', [
                'replay_blocked' => [true],
            ], [
                'available_at' => now()->subSeconds($i),
            ]);
        }
        $summary = RunSummaryProjector::project($run->fresh());
        $this->assertTrue($summary->task_problem);
        $this->assertSame('waiting_for_signal', $summary->liveness_state);
    }

    private function projectedTaskReads(WorkflowRun $run): int
    {
        DB::enableQueryLog();
        DB::flushQueryLog();
        RunSummaryProjector::project($run->fresh());
        $count = count(
            array_filter(DB::getQueryLog(), static fn (array $query): bool => str_contains(
                $query['query'],
                'workflow_tasks'
            ))
        );
        DB::disableQueryLog();
        DB::flushQueryLog();
        return $count;
    }

    private function waitingRun(): WorkflowRun
    {
        Queue::fake();
        config()
            ->set('queue.default', 'redis');
        config()
            ->set('queue.connections.redis.driver', 'redis');
        config()
            ->set('workflows.v2.compatibility.current', 'build-a');
        config()
            ->set('workflows.v2.compatibility.supported', ['build-a']);
        $instance = WorkflowInstance::query()->create([
            'workflow_class' => TestSignalWorkflow::class,
            'workflow_type' => 'historical-task-fixture',
            'run_count' => 1,
        ]);
        $run = WorkflowRun::query()->create([
            'workflow_instance_id' => $instance->id,
            'run_number' => 1,
            'workflow_class' => TestSignalWorkflow::class,
            'workflow_type' => 'historical-task-fixture',
            'status' => 'waiting',
            'connection' => 'redis',
            'queue' => 'default',
            'compatibility' => 'build-a',
            'started_at' => now(),
        ]);
        $instance->update([
            'current_run_id' => $run->id,
        ]);
        WorkflowHistoryEvent::record($run, HistoryEventType::SignalWaitOpened, [
            'signal_wait_id' => 'current-wait',
            'signal_name' => 'name-provided',
            'sequence' => 1,
        ]);
        return $run;
    }

    /**
     * @param array<string, mixed> $payload @param array<string, mixed> $extra
     */
    private function task(WorkflowRun $run, string $status, array $payload = [], array $extra = []): WorkflowTask
    {
        return WorkflowTask::query()->create(array_merge([
            'workflow_run_id' => $run->id,
            'task_type' => TaskType::Workflow->value,
            'status' => $status,
            'payload' => $payload,
            'connection' => 'redis',
            'queue' => 'default',
            'compatibility' => 'build-a',
            'available_at' => now(),
        ], $extra));
    }
}
