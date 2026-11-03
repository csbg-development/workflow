<?php

declare(strict_types=1);

namespace Tests\Feature\V2;

use Illuminate\Support\Facades\Queue;
use Tests\Fixtures\V2\TestSignalWorkflow;
use Tests\TestCase;
use Workflow\V2\Enums\HistoryEventType;
use Workflow\V2\Models\WorkflowHistoryEvent;
use Workflow\V2\Models\WorkflowInstance;
use Workflow\V2\Models\WorkflowRun;
use Workflow\V2\Models\WorkflowSignal;
use Workflow\V2\Models\WorkflowTask;
use Workflow\V2\Support\RunSummaryProjector;
use Workflow\V2\TaskWatchdog;

final class V2SignalRepairEligibilityTest extends TestCase
{
    public function testRepeatedWatchdogPassesDoNotRepairAnUnmatchedBufferedSignal(): void
    {
        [$run, $signal] = $this->waitingRun('revision', 'old-buffered-wait');
        for ($pass = 0; $pass < 3; $pass++) {
            $summary = RunSummaryProjector::project($run->fresh());
            $this->assertSame('waiting_for_signal', $summary->liveness_state);
            if ($pass === 0) {
                // A pre-deployment projection must be rechecked under the run lock.
                $summary->update([
                    'liveness_state' => 'repair_needed',
                    'next_task_id' => null,
                ]);
            }
            $report = TaskWatchdog::runPass('redis', 'default', runIds: [$run->id]);
            $this->assertSame(0, $report['dispatched_tasks']);
            $this->assertSame(0, WorkflowTask::query()->where('workflow_run_id', $run->id)->count());
        }
        $this->assertSame('received', $signal->fresh()->status->value);
        $this->assertNull($signal->fresh()->closed_at);
        $this->assertSame('waiting', $run->fresh()->status->value);
        Queue::assertNothingPushed();
    }

    public function testMatchingReceivedSignalStillGetsExactlyOneRepairTask(): void
    {
        [$run, $signal] = $this->waitingRun('continue', 'current-wait');
        WorkflowHistoryEvent::record($run, HistoryEventType::SignalReceived, [
            'signal_id' => $signal->id,
            'signal_name' => 'continue',
            'signal_wait_id' => 'current-wait',
        ]);
        $summary = RunSummaryProjector::project($run->fresh());
        $this->assertSame('repair_needed', $summary->liveness_state);
        $report = TaskWatchdog::runPass('redis', 'default', runIds: [$run->id]);
        $this->assertSame(1, $report['repaired_missing_tasks']);
        TaskWatchdog::runPass('redis', 'default', runIds: [$run->id]);
        $this->assertSame(1, WorkflowTask::query()->where('workflow_run_id', $run->id)->count());
        $task = WorkflowTask::query()->where('workflow_run_id', $run->id)->sole();
        $this->assertSame($signal->id, $task->payload['workflow_signal_id']);
    }

    public function testSignalCanStillWakeAnOpenConditionWait(): void
    {
        [$run] = $this->waitingRun('revision', 'old-buffered-wait');
        WorkflowHistoryEvent::record($run, HistoryEventType::ConditionWaitOpened, [
            'condition_wait_id' => 'condition-wait',
            'condition_key' => 'ready',
            'sequence' => 2,
        ]);
        $summary = RunSummaryProjector::project($run->fresh());
        $this->assertSame('repair_needed', $summary->liveness_state);
        $report = TaskWatchdog::runPass('redis', 'default', runIds: [$run->id]);
        $this->assertSame(1, $report['repaired_missing_tasks']);
    }

    /**
     * @return array{WorkflowRun, WorkflowSignal}
     */
    private function waitingRun(string $signalName, string $signalWaitId): array
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
            'id' => 'signal-repair-fixture',
            'workflow_class' => TestSignalWorkflow::class,
            'workflow_type' => 'signal-repair-fixture',
            'run_count' => 1,
        ]);
        $run = WorkflowRun::query()->create([
            'workflow_instance_id' => $instance->id,
            'run_number' => 1,
            'workflow_class' => TestSignalWorkflow::class,
            'workflow_type' => 'signal-repair-fixture',
            'status' => 'waiting',
            'connection' => 'redis',
            'queue' => 'default',
            'compatibility' => 'build-a',
        ]);
        $instance->update([
            'current_run_id' => $run->id,
        ]);
        WorkflowHistoryEvent::record($run, HistoryEventType::SignalWaitOpened, [
            'signal_wait_id' => 'current-wait',
            'signal_name' => 'continue',
            'sequence' => 1,
        ]);
        $signal = WorkflowSignal::query()->create([
            'workflow_run_id' => $run->id,
            'workflow_instance_id' => $instance->id,
            'workflow_command_id' => 'buffered-command',
            'signal_name' => $signalName,
            'signal_wait_id' => $signalWaitId,
            'status' => 'received',
            'received_at' => now(),
        ]);
        return [$run, $signal];
    }
}
