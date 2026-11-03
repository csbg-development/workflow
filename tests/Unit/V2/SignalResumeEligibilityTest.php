<?php

declare(strict_types=1);

namespace Tests\Unit\V2;

use Illuminate\Database\Eloquent\Collection;
use Tests\NonDatabaseTestCase;
use Workflow\V2\Models\WorkflowHistoryEvent;
use Workflow\V2\Models\WorkflowRun;
use Workflow\V2\Support\SignalResumeEligibility;

final class SignalResumeEligibilityTest extends NonDatabaseTestCase
{
    public function testAdmissionRequiresBothTheCurrentWaitIdAndSignalName(): void
    {
        $policy = $this->policy([[
            'SignalWaitOpened', [
                'signal_wait_id' => 'wait-1',
                'signal_name' => 'continue',
            ]]]);
        $this->assertTrue($policy->accepts('continue', 'wait-1'));
        $this->assertFalse($policy->accepts('revision', 'wait-1'));
        $this->assertFalse($policy->accepts('continue', 'buffered-wait'));
        $this->assertFalse($policy->accepts('continue', null));
        $this->assertFalse($policy->accepts(null, 'missing-wait'));
    }

    public function testReceivedSignalCanBeAppliedButAppliedSignalCannotWakeAgain(): void
    {
        $events = [
            [
                'SignalWaitOpened', [
                    'signal_wait_id' => 'wait-1',
                    'signal_name' => 'continue',
                ]],
            [
                'SignalReceived', [
                    'signal_wait_id' => 'wait-1',
                    'signal_name' => 'continue',
                ]],
        ];
        $this->assertTrue($this->policy($events)->accepts('continue', 'wait-1'));
        $events[] = [
            'SignalApplied', [
                'signal_wait_id' => 'wait-1',
                'signal_name' => 'continue',
            ]];
        $this->assertFalse($this->policy($events)->accepts('continue', 'wait-1'));
    }

    public function testAnUnrelatedBufferedSignalCanBecomeEligibleWhenItsWaitOpens(): void
    {
        $this->assertFalse($this->policy([])->accepts('revision', 'future-wait'));
        $policy = $this->policy([[
            'SignalWaitOpened', [
                'signal_wait_id' => 'future-wait',
                'signal_name' => 'revision',
            ]]]);
        $this->assertTrue($policy->accepts('revision', 'future-wait'));
    }

    public function testConditionReevaluationRemainsEligibleUntilItsTimeoutFires(): void
    {
        $events = [[
            'ConditionWaitOpened', [
                'condition_wait_id' => 'condition-1',
                'sequence' => 1,
            ]]];
        $this->assertTrue($this->policy($events)->accepts('revision', 'buffered-wait'));
        $events[] = [
            'TimerFired', [
                'timer_kind' => 'condition_timeout',
                'condition_wait_id' => 'condition-1',
                'sequence' => 1,
            ]];
        $this->assertFalse($this->policy($events)->accepts('revision', 'buffered-wait'));
    }

    /**
     * @param list<array{string, array<string, mixed>}> $events
     */
    private function policy(array $events): SignalResumeEligibility
    {
        $history = [];
        foreach ($events as $index => [$type, $payload]) {
            $history[] = new WorkflowHistoryEvent([
                'event_type' => $type,
                'payload' => $payload,
                'sequence' => $index + 1,
            ]);
        }
        $run = new WorkflowRun();
        $run->setRelation('historyEvents', new Collection($history));
        return SignalResumeEligibility::forRun($run);
    }
}
