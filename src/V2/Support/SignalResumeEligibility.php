<?php

declare(strict_types=1);

namespace Workflow\V2\Support;

use Workflow\V2\Models\WorkflowRun;

/** Shared admission policy for execution and missing signal-task repair. */
final class SignalResumeEligibility
{
    /**
     * @param array<string, string> $openSignalWaits
     */
    private function __construct(
        private readonly bool $hasAdvanceableConditionWait,
        private readonly array $openSignalWaits,
    ) {
    }

    public static function forRun(WorkflowRun $run): self
    {
        $hasCondition = false;
        foreach (ConditionWaits::forRun($run) as $wait) {
            if (($wait['status'] ?? null) === 'open' && ($wait['source_status'] ?? null) !== 'timeout_fired') {
                $hasCondition = true;
                break;
            }
        }

        $signalWaits = [];
        foreach (SignalWaits::forRun($run) as $wait) {
            $id = $wait['signal_wait_id'] ?? null;
            $name = $wait['signal_name'] ?? null;
            // SignalReceived resolves the wait before its pending application task runs.
            $canApplyReceived = ($wait['status'] ?? null) === 'resolved'
                && ($wait['source_status'] ?? null) === 'received';
            if ((($wait['status'] ?? null) === 'open' || $canApplyReceived) && is_string(
                $id
            ) && $id !== '' && is_string(
                $name
            ) && $name !== '') {
                $signalWaits[$id] = $name;
            }
        }

        return new self($hasCondition, $signalWaits);
    }

    public function accepts(?string $signalName, ?string $signalWaitId): bool
    {
        return $this->hasAdvanceableConditionWait
            || ($signalName !== null && $signalName !== '' && $signalWaitId !== null && $signalWaitId !== '' && ($this->openSignalWaits[$signalWaitId] ?? null) === $signalName);
    }
}
