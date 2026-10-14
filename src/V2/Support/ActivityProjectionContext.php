<?php

declare(strict_types=1);

namespace Workflow\V2\Support;

use Workflow\V2\Models\ActivityExecution;
use Workflow\V2\Models\WorkflowHistoryEvent;
use Workflow\V2\Models\WorkflowRun;
use Workflow\V2\Models\WorkflowTask;

/**
 * Carries one self-describing activity transition through the configured
 * history role. The default projector can update the affected rows without
 * loading the run's complete history, while custom roles still own dispatch.
 */
final class ActivityProjectionContext
{
    /**
     * @var array<string, list<array{
     *     execution: ActivityExecution,
     *     source_task: WorkflowTask,
     *     next_task: WorkflowTask,
     *     event: WorkflowHistoryEvent
     * }>>
     */
    private static array $projectionsByRunId = [];

    /**
     * @template TResult
     * @param callable(): TResult $project
     * @return TResult
     */
    public static function run(
        WorkflowRun $run,
        ActivityExecution $execution,
        WorkflowTask $sourceTask,
        WorkflowTask $nextTask,
        WorkflowHistoryEvent $event,
        callable $project,
    ): mixed {
        $runId = (string) $run->getKey();
        self::$projectionsByRunId[$runId] ??= [];
        self::$projectionsByRunId[$runId][] = [
            'execution' => $execution,
            'source_task' => $sourceTask,
            'next_task' => $nextTask,
            'event' => $event,
        ];

        try {
            return $project();
        } finally {
            array_pop(self::$projectionsByRunId[$runId]);

            if (self::$projectionsByRunId[$runId] === []) {
                unset(self::$projectionsByRunId[$runId]);
            }
        }
    }

    /**
     * @return array{
     *     execution: ActivityExecution,
     *     source_task: WorkflowTask,
     *     next_task: WorkflowTask,
     *     event: WorkflowHistoryEvent
     * }|null
     */
    public static function projectionFor(WorkflowRun $run): ?array
    {
        $projections = self::$projectionsByRunId[(string) $run->getKey()] ?? [];
        $projection = end($projections);

        return is_array($projection) ? $projection : null;
    }

    /**
     * Resolve immutable parallel metadata already persisted on a current row.
     * A null return identifies legacy or complex rows that must use the
     * authoritative full-history fallback.
     *
     * @return list<array{
     *     parallel_group_id: string,
     *     parallel_group_kind: string,
     *     parallel_group_base_sequence: int,
     *     parallel_group_size: int,
     *     parallel_group_index: int
     * }>|null
     */
    public static function parallelMetadataPath(WorkflowRun $run, ActivityExecution $execution): ?array
    {
        if ($execution->getRawOriginal('parallel_group_path') !== null) {
            return ParallelChildGroup::metadataPathFromPayload([
                'parallel_group_path' => $execution->parallel_group_path,
            ]);
        }

        return null;
    }
}
