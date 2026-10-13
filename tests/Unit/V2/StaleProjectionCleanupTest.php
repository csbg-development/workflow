<?php

declare(strict_types=1);

namespace Tests\Unit\V2;

use Illuminate\Support\Facades\DB;
use Illuminate\Support\Str;
use Tests\TestCase;
use Workflow\V2\Models\WorkflowInstance;
use Workflow\V2\Models\WorkflowRun;
use Workflow\V2\Models\WorkflowTimelineEntry;
use Workflow\V2\Support\StaleProjectionCleanup;

final class StaleProjectionCleanupTest extends TestCase
{
    public function testRemovesOnlyPrunedRowsAndLeavesSeenRowsIntact(): void
    {
        $run = $this->seedRun();

        $keepA = $this->seedTimelineEntry($run, 'keep-a');
        $keepB = $this->seedTimelineEntry($run, 'keep-b');
        $stale = $this->seedTimelineEntry($run, 'stale');

        StaleProjectionCleanup::forRun(WorkflowTimelineEntry::class, $run->id, [$keepA->id, $keepB->id]);

        $this->assertDatabaseHas('workflow_run_timeline_entries', [
            'id' => $keepA->id,
        ]);
        $this->assertDatabaseHas('workflow_run_timeline_entries', [
            'id' => $keepB->id,
        ]);
        $this->assertDatabaseMissing('workflow_run_timeline_entries', [
            'id' => $stale->id,
        ]);
    }

    public function testRemovesAllRowsWhenSeenIsEmpty(): void
    {
        $run = $this->seedRun();

        $this->seedTimelineEntry($run, 'doomed-a');
        $this->seedTimelineEntry($run, 'doomed-b');

        StaleProjectionCleanup::forRun(WorkflowTimelineEntry::class, $run->id, []);

        $this->assertSame(0, WorkflowTimelineEntry::query()->where('workflow_run_id', $run->id)->count());
    }

    public function testNoopWhenNoRowsExistForRun(): void
    {
        $run = $this->seedRun();

        StaleProjectionCleanup::forRun(WorkflowTimelineEntry::class, $run->id, ['unused-id']);

        $this->assertSame(0, WorkflowTimelineEntry::query()->where('workflow_run_id', $run->id)->count());
    }

    public function testDoesNotAffectRowsForOtherRuns(): void
    {
        $runA = $this->seedRun();
        $runB = $this->seedRun();

        $runAEntry = $this->seedTimelineEntry($runA, 'run-a-entry');
        $runBEntry = $this->seedTimelineEntry($runB, 'run-b-entry');

        StaleProjectionCleanup::forRun(WorkflowTimelineEntry::class, $runA->id, []);

        $this->assertDatabaseMissing('workflow_run_timeline_entries', [
            'id' => $runAEntry->id,
        ]);
        $this->assertDatabaseHas('workflow_run_timeline_entries', [
            'id' => $runBEntry->id,
        ]);
    }

    public function testProvidedSnapshotAvoidsASecondRunScopedReadAndDeletesSortedStaleIds(): void
    {
        $run = $this->seedRun();
        $keep = $this->seedTimelineEntry($run, 'keep');
        $staleA = $this->seedTimelineEntry($run, 'stale-a');
        $staleB = $this->seedTimelineEntry($run, 'stale-b');
        $snapshot = [$staleB->id, $keep->id, $staleA->id];
        $expectedDeleteIds = [$staleA->id, $staleB->id];
        sort($expectedDeleteIds);
        $connection = DB::connection($keep->getConnectionName());
        $connection->flushQueryLog();
        $connection->enableQueryLog();

        try {
            StaleProjectionCleanup::forRun(WorkflowTimelineEntry::class, $run->id, [$keep->id], $snapshot);
            $queries = $connection->getQueryLog();
        } finally {
            $connection->disableQueryLog();
        }

        $reads = array_values(array_filter($queries, static fn (array $query): bool =>
            str_starts_with(strtolower($query['query']), 'select')
            && str_contains($query['query'], 'workflow_run_timeline_entries')));
        $deletes = array_values(array_filter($queries, static fn (array $query): bool =>
            str_starts_with(strtolower($query['query']), 'delete')
            && str_contains($query['query'], 'workflow_run_timeline_entries')));

        $this->assertSame([], $reads);
        $this->assertCount(2, $deletes);
        $this->assertSame(
            array_map(static fn (string $id): array => [$run->id, $id], $expectedDeleteIds),
            array_map(static fn (array $query): array => $query['bindings'], $deletes),
        );
        foreach ($deletes as $delete) {
            $this->assertStringContainsString('workflow_run_id', $delete['query']);
        }
        $this->assertNotNull($keep->fresh());
        $this->assertNull($staleA->fresh());
        $this->assertNull($staleB->fresh());
    }

    public function testProvidedSnapshotToleratesAConcurrentStaleRowDeletion(): void
    {
        $run = $this->seedRun();
        $keep = $this->seedTimelineEntry($run, 'keep-after-race');
        $stale = $this->seedTimelineEntry($run, 'stale-deleted-by-peer');
        $snapshot = [$keep->id, $stale->id];

        $stale->delete();

        StaleProjectionCleanup::forRun(WorkflowTimelineEntry::class, $run->id, [$keep->id], $snapshot);

        $this->assertNotNull($keep->fresh());
        $this->assertNull($stale->fresh());
    }

    public function testProvidedSnapshotCannotDeleteAProjectionOwnedByAnotherRun(): void
    {
        $run = $this->seedRun();
        $otherRun = $this->seedRun();
        $stale = $this->seedTimelineEntry($run, 'stale-for-run');
        $foreign = $this->seedTimelineEntry($otherRun, 'foreign-snapshot-row');

        StaleProjectionCleanup::forRun(WorkflowTimelineEntry::class, $run->id, [], [$stale->id, $foreign->id]);

        $this->assertNull($stale->fresh());
        $this->assertNotNull($foreign->fresh());
    }

    public function testProvidedSnapshotPreservesConfiguredModelGlobalScopes(): void
    {
        $run = $this->seedRun();
        $stale = $this->seedTimelineEntry($run, 'visible-stale');
        $scopeProtected = $this->seedTimelineEntry($run, 'scope-protected');

        StaleProjectionCleanup::forRun(
            ScopeProtectedTimelineEntry::class,
            $run->id,
            [],
            [$stale->id, $scopeProtected->id],
        );

        $this->assertNull($stale->fresh());
        $this->assertNotNull($scopeProtected->fresh());
    }

    private function seedRun(): WorkflowRun
    {
        $instance = WorkflowInstance::query()->create([
            'id' => (string) Str::ulid(),
            'workflow_class' => 'App\\Fake\\Workflow',
            'workflow_type' => 'App\\Fake\\Workflow',
            'business_key' => null,
            'namespace' => 'default',
        ]);

        return WorkflowRun::query()->create([
            'id' => (string) Str::ulid(),
            'workflow_instance_id' => $instance->id,
            'run_number' => 1,
            'workflow_class' => $instance->workflow_class,
            'workflow_type' => $instance->workflow_type,
            'status' => 'pending',
            'connection' => null,
            'queue' => null,
        ]);
    }

    private function seedTimelineEntry(WorkflowRun $run, string $historyEventId): WorkflowTimelineEntry
    {
        return WorkflowTimelineEntry::query()->create([
            'id' => hash('sha256', $run->id . '|' . $historyEventId),
            'workflow_run_id' => $run->id,
            'workflow_instance_id' => $run->workflow_instance_id,
            'history_event_id' => $historyEventId,
            'sequence' => 0,
            'type' => 'TestEvent',
            'kind' => 'workflow',
            'entry_kind' => 'point',
            'source_kind' => null,
            'source_id' => null,
            'summary' => null,
            'recorded_at' => now(),
            'payload' => [],
        ]);
    }
}

final class ScopeProtectedTimelineEntry extends WorkflowTimelineEntry
{
    protected static function booted(): void
    {
        static::addGlobalScope('protect-history-event', static function (
            \Illuminate\Database\Eloquent\Builder $query,
        ): void {
            $query->where('history_event_id', '!=', 'scope-protected');
        });
    }
}
