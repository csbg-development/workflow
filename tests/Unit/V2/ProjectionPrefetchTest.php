<?php

declare(strict_types=1);

namespace Tests\Unit\V2;

use Illuminate\Database\Eloquent\Model;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Schema;
use PHPUnit\Framework\Attributes\DataProvider;
use Tests\TestCase;
use Workflow\V2\Models\WorkflowInstance;
use Workflow\V2\Models\WorkflowRun;
use Workflow\V2\Models\WorkflowRunTimerEntry;
use Workflow\V2\Models\WorkflowRunWait;
use Workflow\V2\Models\WorkflowTimelineEntry;
use Workflow\V2\Support\RunTimelineProjector;
use Workflow\V2\Support\RunTimerProjector;
use Workflow\V2\Support\RunWaitProjector;

final class ProjectionPrefetchTest extends TestCase
{
    /**
     * @return iterable<string, array{class-string, string}>
     */
    public static function projectors(): iterable
    {
        yield 'timeline' => [RunTimelineProjector::class, 'history_event_id'];
        yield 'wait' => [RunWaitProjector::class, 'wait_id'];
        yield 'timer' => [RunTimerProjector::class, 'timer_id'];
    }

    /**
     * @return iterable<string, array{class-string, string, class-string<Model>}>
     */
    public static function configuredProjectors(): iterable
    {
        yield 'timeline' => [RunTimelineProjector::class, 'run_timeline_entry_model', PrefetchedTimelineEntry::class];
        yield 'wait' => [RunWaitProjector::class, 'run_wait_model', PrefetchedRunWait::class];
        yield 'timer' => [RunTimerProjector::class, 'run_timer_entry_model', PrefetchedTimerEntry::class];
    }

    #[DataProvider('configuredProjectors')]
    public function testPrefetchUsesConfiguredModelTableAndConnection(
        string $projector,
        string $configKey,
        string $model
    ): void {
        $run = $this->seedRun('configured');
        $entries = $this->entries();
        $original = $projector::project($run, $entries)[0];
        config()
            ->set('database.connections.projection-secondary', [
                'driver' => 'sqlite',
                'database' => ':memory:',
            ]);
        Schema::connection('projection-secondary')->create('custom_projection_rows', static function (Blueprint $table) use (
            $original
        ): void {
            $table->string('id')
                ->primary();

            foreach (array_keys($original->getAttributes()) as $column) {
                if ($column !== 'id') {
                    $table->text($column)
                        ->nullable();
                }
            }
        });
        config()
            ->set('workflows.v2.' . $configKey, $model);

        try {
            $created = $projector::project($run->fresh(), $entries)[0];
            $entries[0]['status'] = 'updated';
            $updated = $projector::project($run->fresh(), $entries)[0];

            $this->assertInstanceOf($model, $updated);
            $this->assertSame($created->getKey(), $updated->getKey());
            $this->assertSame('updated', $updated->fresh()->payload['status']);
            $this->assertSame('resolved', $original->fresh()->payload['status']);
            $this->assertSame(
                count($entries),
                DB::connection('projection-secondary')->table('custom_projection_rows')->count()
            );
            $projector::project($run->fresh(), []);
            $this->assertSame(0, DB::connection('projection-secondary')->table('custom_projection_rows')->count());
            $this->assertNotNull($original->fresh());
        } finally {
            DB::purge('projection-secondary');
        }
    }

    #[DataProvider('projectors')]
    public function testReprojectionUsesOneScopedReadAndPreservesRows(string $projector, string $identity): void
    {
        $run = $this->seedRun('prefetch');
        $entries = $this->entries();
        /** @var list<Model> $rows */
        $rows = $projector::project($run, $entries);
        $expected = array_map(self::attributes(...), $rows);
        $connection = $rows[0]->getConnection();
        $table = $rows[0]->getTable();
        $connection->flushQueryLog();
        $connection->enableQueryLog();

        try {
            $reprojected = $projector::project($run->fresh(), $entries);
            $queries = $connection->getQueryLog();
        } finally {
            $connection->disableQueryLog();
        }

        $reads = array_filter($queries, static fn (array $query): bool =>
            str_starts_with(strtolower($query['query']), 'select') && str_contains($query['query'], $table));
        // One prefetched row set plus the existing stale-cleanup primary-key snapshot.
        $this->assertCount(2, $reads);
        $this->assertSame($expected, array_map(self::attributes(...), $reprojected));

        $rows[0]->forceFill([
            'payload' => [
                'corrupt' => true,
            ],
        ])->save();
        $rows[1]->delete();
        $orphan = $rows[2]->replicate();
        $orphan->forceFill([
            'id' => hash('sha256', 'orphan'),
            $identity => 'orphan',
        ])->save();
        $otherRun = $this->seedRun('unrelated');
        $otherRows = $projector::project($otherRun, $entries);

        $repaired = $projector::project($run->fresh(), $entries);
        $this->assertCount(count($entries), $repaired);
        $this->assertSame($expected[0]['payload'], $repaired[0]->getRawOriginal('payload'));
        $this->assertSame($expected[1]['id'], $repaired[1]->getKey());
        $this->assertNull($orphan->fresh());
        $this->assertNotNull($otherRows[0]->fresh());

        $this->assertSame([], $projector::project($run->fresh(), []));
        $this->assertSame(0, $rows[0]->newQuery()->where('workflow_run_id', $run->id)->count());
        $this->assertSame(count($entries), $otherRows[0]->newQuery()->where('workflow_run_id', $otherRun->id)->count());
    }

    #[DataProvider('projectors')]
    public function testReprojectionDoesNotWriteSemanticallyEqualPrefetchedPayloads(
        string $projector,
        string $identity,
    ): void {
        $run = $this->seedRun('prefetch-json-noop');
        $entries = $this->entries();
        /** @var list<Model> $rows */
        $rows = $projector::project($run, $entries);
        $connection = $rows[0]->getConnection();
        $table = $rows[0]->getTable();

        foreach ($rows as $row) {
            $connection->table($table)
                ->where('id', $row->getKey())
                ->update([
                    'payload' => json_encode(self::reverseAssociativeKeys($row->payload), JSON_THROW_ON_ERROR),
                    'updated_at' => now()
                        ->subMinute(),
                ]);
        }

        $freshRun = $run->fresh();
        $connection->flushQueryLog();
        $connection->enableQueryLog();

        try {
            $reprojected = $projector::project($freshRun, $entries);
            $queries = $connection->getQueryLog();
        } finally {
            $connection->disableQueryLog();
        }

        $writes = array_filter($queries, static fn (array $query): bool => preg_match(
            '/^(insert|update|delete)\b/i',
            ltrim($query['query']),
        ) === 1 && str_contains($query['query'], $table));
        $this->assertCount(0, $writes, sprintf(
            '%s emitted projection writes: %s',
            $projector,
            json_encode(array_column($writes, 'query'), JSON_THROW_ON_ERROR),
        ));
        $this->assertCount(count($entries), $reprojected);
        $this->assertSame(
            array_column($entries, 'id'),
            array_map(static fn (Model $row): mixed => $row->getAttribute($identity), $reprojected),
        );
        $this->assertSame(
            array_column($entries, 'status', 'id'),
            array_column(array_map(static fn (Model $row): array => $row->payload, $reprojected), 'status', 'id'),
        );
    }

    public function testWaitProjectionDoesNotRewriteExistingRowsWhenDisplayOrderChanges(): void
    {
        $run = $this->seedRun('stable-wait-position');
        $resolvedWaits = [
            [
                'id' => 'B-resolved',
                'kind' => 'signal',
                'status' => 'resolved',
                'opened_at' => '2026-09-01T12:00:00.000900Z',
            ],
            [
                'id' => 'a-resolved',
                'kind' => 'signal',
                'status' => 'resolved',
                'opened_at' => '2026-09-01T12:00:00.000100Z',
            ],
        ];
        $existingRows = RunWaitProjector::project($run, $resolvedWaits);
        $existingPositions = array_column(array_map(
            static fn (Model $row): array => $row->getAttributes(),
            $existingRows,
        ), 'position', 'wait_id');
        $connection = $existingRows[0]->getConnection();
        $table = $existingRows[0]->getTable();
        $connection->flushQueryLog();
        $connection->enableQueryLog();

        try {
            $projected = RunWaitProjector::project($run->fresh(), [
                [
                    'id' => 'new-open',
                    'kind' => 'signal',
                    'status' => 'open',
                    'opened_at' => '2026-09-01T12:00:02.000000Z',
                ],
                ...$resolvedWaits,
            ]);
            $queries = $connection->getQueryLog();
        } finally {
            $connection->disableQueryLog();
        }

        $writes = array_filter($queries, static fn (array $query): bool => preg_match(
            '/^(insert|update|delete)\b/i',
            ltrim($query['query']),
        ) === 1 && str_contains($query['query'], $table));

        $this->assertCount(1, $writes, sprintf(
            'Changing wait display order emitted projection writes: %s',
            json_encode(array_column($writes, 'query'), JSON_THROW_ON_ERROR),
        ));
        $this->assertSame(
            $existingPositions,
            array_column(array_map(
                static fn (Model $row): array => $row->fresh()
                    ->getAttributes(),
                array_slice($projected, 1),
            ), 'position', 'wait_id'),
        );
        $this->assertSame(
            ['new-open', 'B-resolved', 'a-resolved'],
            $run->fresh()
                ->waits()
                ->pluck('wait_id')
                ->all(),
        );
    }

    /**
     * @return list<array<string, mixed>>
     */
    private function entries(): array
    {
        return array_map(static fn (int $sequence): array => [
            'id' => 'entry-' . $sequence,
            'sequence' => $sequence,
            'type' => 'TimerFired',
            'kind' => 'timer',
            'status' => 'resolved',
            'summary' => 'Timer completed.',
            'recorded_at' => '2026-09-01T12:00:00.123456Z',
            'opened_at' => '2026-09-01T12:00:00.123456Z',
            'fire_at' => '2026-09-01T12:00:01.123456Z',
        ], range(1, 20));
    }

    /**
     * @return array<string, mixed>
     */
    private static function attributes(Model $row): array
    {
        $attributes = $row->getAttributes();
        ksort($attributes);

        return $attributes;
    }

    private static function reverseAssociativeKeys(mixed $value): mixed
    {
        if (! is_array($value)) {
            return $value;
        }

        if (array_is_list($value)) {
            return array_map(self::reverseAssociativeKeys(...), $value);
        }

        $reversed = [];
        foreach (array_reverse($value, true) as $key => $item) {
            $reversed[$key] = self::reverseAssociativeKeys($item);
        }

        return $reversed;
    }

    private function seedRun(string $id): WorkflowRun
    {
        $instance = WorkflowInstance::query()->create([
            'id' => $id,
            'workflow_class' => 'ProjectionWorkflow',
            'workflow_type' => 'projection.workflow',
            'namespace' => 'default',
        ]);

        return WorkflowRun::query()->create([
            'workflow_instance_id' => $instance->id,
            'run_number' => 1,
            'workflow_class' => $instance->workflow_class,
            'workflow_type' => $instance->workflow_type,
            'status' => 'waiting',
        ]);
    }
}

final class PrefetchedTimelineEntry extends WorkflowTimelineEntry
{
    protected $table = 'custom_projection_rows';

    protected $connection = 'projection-secondary';
}

final class PrefetchedRunWait extends WorkflowRunWait
{
    protected $table = 'custom_projection_rows';

    protected $connection = 'projection-secondary';
}

final class PrefetchedTimerEntry extends WorkflowRunTimerEntry
{
    protected $table = 'custom_projection_rows';

    protected $connection = 'projection-secondary';
}
