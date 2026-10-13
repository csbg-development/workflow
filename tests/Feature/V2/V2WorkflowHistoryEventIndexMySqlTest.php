<?php

declare(strict_types=1);

namespace Tests\Feature\V2;

use Illuminate\Support\Facades\DB;
use Illuminate\Support\Str;
use Tests\SchemaTestCase;
use Workflow\V2\Enums\HistoryEventType;
use Workflow\V2\Enums\RunStatus;
use Workflow\V2\Models\WorkflowInstance;
use Workflow\V2\Models\WorkflowRun;
use Workflow\V2\Models\WorkflowTimelineEntry;
use Workflow\V2\Support\CurrentRunResolver;
use Workflow\V2\Support\StaleProjectionCleanup;

final class V2WorkflowHistoryEventIndexMySqlTest extends SchemaTestCase
{
    private const INDEX = 'wfhe_run_type_idx';

    private const RUN_COUNT = 244;

    private const NOISE_EVENTS_PER_RUN = 40;

    protected function setUp(): void
    {
        parent::setUp();

        if (DB::connection()->getDriverName() !== 'mysql') {
            $this->markTestSkipped('This query-plan regression requires MySQL 8.4.');
        }
    }

    public function testRunAndEventTypeLookupDoesNotGrowWithUnrelatedHistory(): void
    {
        $runIds = [];
        $rows = [];
        $ordinal = 1;
        $timestamp = now();

        for ($run = 1; $run <= self::RUN_COUNT; $run++) {
            $runId = sprintf('R%025d', $run);
            $runIds[] = $runId;
            $rows[] = $this->historyRow(
                ordinal: $ordinal++,
                runId: $runId,
                sequence: 1,
                eventType: 'WorkflowStarted',
                timestamp: $timestamp,
            );
        }

        DB::table('workflow_history_events')->insert($rows);
        $this->analyzeHistoryTable();

        $baseline = $this->measureLookup($runIds);

        $rows = [];
        foreach ($runIds as $runId) {
            for ($noise = 1; $noise <= self::NOISE_EVENTS_PER_RUN; $noise++) {
                $rows[] = $this->historyRow(
                    ordinal: $ordinal++,
                    runId: $runId,
                    sequence: $noise + 1,
                    eventType: 'HistoryNoise' . ($noise % 4),
                    timestamp: $timestamp,
                );

                if (count($rows) === 500) {
                    DB::table('workflow_history_events')->insert($rows);
                    $rows = [];
                }
            }
        }

        if ($rows !== []) {
            DB::table('workflow_history_events')->insert($rows);
        }

        $this->analyzeHistoryTable();

        $plan = $this->lookupPlan($runIds);
        $withNoise = $this->measureLookup($runIds);

        $this->assertSame(self::INDEX, $plan['key'] ?? null, json_encode($plan, JSON_THROW_ON_ERROR));
        $this->assertLessThanOrEqual(
            $baseline['history_reads'] + self::RUN_COUNT,
            $withNoise['history_reads'],
            sprintf(
                'History reads grew from %d to %d after adding %d unrelated events.',
                $baseline['history_reads'],
                $withNoise['history_reads'],
                self::RUN_COUNT * self::NOISE_EVENTS_PER_RUN,
            ),
        );
        $this->assertSame(self::RUN_COUNT, $withNoise['result_count']);
    }

    public function testResolverSubqueryUsesCompositeIndexAndReadsStayBoundedByMatchingStarts(): void
    {
        $instance = WorkflowInstance::query()->create([
            'id' => 'mysql-subquery-plan-instance',
            'workflow_class' => 'Tests\\Workflows\\MySqlSubqueryPlanWorkflow',
            'workflow_type' => 'mysql.subquery.plan.workflow',
            'run_count' => self::RUN_COUNT,
        ]);
        $runIds = [];
        $rows = [];
        $ordinal = 1;
        $timestamp = now();

        for ($number = 1; $number <= self::RUN_COUNT; $number++) {
            $run = $this->seedRun($instance, $number);
            $runIds[] = $run->id;
            $rows[] = $this->historyRow(
                ordinal: $ordinal++,
                runId: $run->id,
                sequence: 1,
                eventType: HistoryEventType::WorkflowStarted->value,
                timestamp: $timestamp,
                payload: $number === 1 ? [] : [
                    'continued_from_run_id' => $runIds[$number - 2],
                ],
            );
        }

        DB::table('workflow_history_events')->insert($rows);
        $this->analyzeResolverTables();
        [$sql, $bindings] = $this->resolverHistoryQuery($instance);
        $this->assertCount(2, $bindings);
        $this->assertSame([$instance->id, HistoryEventType::WorkflowStarted->value], $bindings);
        $baseline = $this->measureSql($sql, $bindings);

        $rows = [];
        foreach ($runIds as $runId) {
            for ($noise = 1; $noise <= self::NOISE_EVENTS_PER_RUN; $noise++) {
                $rows[] = $this->historyRow(
                    ordinal: $ordinal++,
                    runId: $runId,
                    sequence: $noise + 1,
                    eventType: 'HistoryNoise' . ($noise % 4),
                    timestamp: $timestamp,
                );

                if (count($rows) === 500) {
                    DB::table('workflow_history_events')->insert($rows);
                    $rows = [];
                }
            }
        }

        if ($rows !== []) {
            DB::table('workflow_history_events')->insert($rows);
        }

        $this->analyzeResolverTables();
        [$withNoiseSql, $withNoiseBindings] = $this->resolverHistoryQuery($instance);
        $this->assertSame($sql, $withNoiseSql);
        $this->assertSame($bindings, $withNoiseBindings);
        $plan = array_map(static fn (object $row): array => (array) $row, DB::select(
            'EXPLAIN ' . $withNoiseSql,
            $withNoiseBindings,
        ));
        $withNoise = $this->measureSql($withNoiseSql, $withNoiseBindings);
        $historyPlan = collect($plan)
            ->firstWhere('table', 'workflow_history_events');
        $runPlan = collect($plan)
            ->firstWhere('table', 'workflow_runs');

        $this->assertSame(self::INDEX, $historyPlan['key'] ?? null, json_encode($plan, JSON_THROW_ON_ERROR));
        $this->assertNotSame('ALL', $runPlan['type'] ?? null, json_encode($plan, JSON_THROW_ON_ERROR));
        $this->assertSame(self::RUN_COUNT, $baseline['result_count']);
        $this->assertSame(self::RUN_COUNT, $withNoise['result_count']);
        $this->assertLessThanOrEqual(
            $baseline['history_reads'] + self::RUN_COUNT,
            $withNoise['history_reads'],
            sprintf(
                'Subquery reads grew from %d to %d after adding %d unrelated events.',
                $baseline['history_reads'],
                $withNoise['history_reads'],
                self::RUN_COUNT * self::NOISE_EVENTS_PER_RUN,
            ),
        );
    }

    public function testSnapshotCleanupKeepsRunBoundaryAndUsesPrimaryPointDeletes(): void
    {
        $run = $this->seedRunForCleanup();
        $otherRun = $this->seedRunForCleanup();
        $stale = $this->seedTimelineEntry($run, 'stale-for-run');
        $foreign = $this->seedTimelineEntry($otherRun, 'foreign-snapshot-row');
        $snapshot = [$foreign->id, $stale->id];
        $expectedIds = $snapshot;
        sort($expectedIds);
        $connection = DB::connection();
        $connection->flushQueryLog();
        $connection->enableQueryLog();

        try {
            StaleProjectionCleanup::forRun(WorkflowTimelineEntry::class, $run->id, [], $snapshot);
            $queries = $connection->getQueryLog();
        } finally {
            $connection->disableQueryLog();
        }

        $deletes = collect($queries)
            ->filter(static fn (array $query): bool =>
                str_starts_with(strtolower($query['query']), 'delete')
                && str_contains($query['query'], 'workflow_run_timeline_entries'))
            ->values();
        $this->assertCount(2, $deletes);
        $this->assertSame(
            array_map(static fn (string $id): array => [$run->id, $id], $expectedIds),
            $deletes->pluck('bindings')
                ->all(),
        );
        $this->assertNull($stale->fresh());
        $this->assertNotNull($foreign->fresh());

        foreach ($deletes as $delete) {
            $plan = array_map(static fn (object $row): array => (array) $row, DB::select(
                'EXPLAIN ' . $delete['query'],
                $delete['bindings'],
            ));
            $deletePlan = collect($plan)
                ->first();

            $this->assertSame('PRIMARY', $deletePlan['key'] ?? null, json_encode($plan, JSON_THROW_ON_ERROR));
        }
    }

    /**
     * @param array<string, mixed> $payload
     * @return array{id: string, workflow_run_id: string, sequence: int, event_type: string, payload: string, recorded_at: mixed, created_at: mixed, updated_at: mixed}
     */
    private function historyRow(
        int $ordinal,
        string $runId,
        int $sequence,
        string $eventType,
        mixed $timestamp,
        array $payload = [],
    ): array {
        return [
            'id' => sprintf('E%025d', $ordinal),
            'workflow_run_id' => $runId,
            'sequence' => $sequence,
            'event_type' => $eventType,
            'payload' => json_encode($payload, JSON_THROW_ON_ERROR),
            'recorded_at' => $timestamp,
            'created_at' => $timestamp,
            'updated_at' => $timestamp,
        ];
    }

    private function analyzeHistoryTable(): void
    {
        DB::statement('ANALYZE TABLE workflow_history_events');
    }

    private function analyzeResolverTables(): void
    {
        DB::select('ANALYZE TABLE workflow_runs, workflow_history_events');
    }

    private function seedRun(WorkflowInstance $instance, int $number): WorkflowRun
    {
        return WorkflowRun::query()->create([
            'workflow_instance_id' => $instance->id,
            'run_number' => $number,
            'workflow_class' => $instance->workflow_class,
            'workflow_type' => $instance->workflow_type,
            'status' => RunStatus::Waiting->value,
            'started_at' => now(),
        ]);
    }

    private function seedRunForCleanup(): WorkflowRun
    {
        $instance = WorkflowInstance::query()->create([
            'id' => (string) Str::ulid(),
            'workflow_class' => 'Tests\\Workflows\\CleanupPlanWorkflow',
            'workflow_type' => 'mysql.cleanup.plan.workflow',
        ]);

        return $this->seedRun($instance, 1);
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
            'recorded_at' => now(),
            'payload' => [],
        ]);
    }

    /**
     * @return array{string, list<mixed>}
     */
    private function resolverHistoryQuery(WorkflowInstance $instance): array
    {
        $connection = DB::connection();
        $connection->flushQueryLog();
        $connection->enableQueryLog();

        try {
            $resolution = CurrentRunResolver::resolutionForInstance($instance->fresh());
            $queries = $connection->getQueryLog();
        } finally {
            $connection->disableQueryLog();
        }

        $this->assertNotNull($resolution['run']);
        $historyQuery = collect($queries)
            ->first(static fn (array $query): bool =>
                        str_starts_with(strtolower($query['query']), 'select')
                        && str_contains($query['query'], 'workflow_history_events'));
        $this->assertIsArray($historyQuery);

        return [$historyQuery['query'], $historyQuery['bindings']];
    }

    /**
     * @param list<string> $runIds
     * @return array<string, mixed>
     */
    private function lookupPlan(array $runIds): array
    {
        $placeholders = implode(', ', array_fill(0, count($runIds), '?'));
        $plan = DB::selectOne(
            "EXPLAIN SELECT id FROM workflow_history_events WHERE workflow_run_id IN ({$placeholders}) AND event_type = ?",
            [...$runIds, 'WorkflowStarted'],
        );

        return (array) $plan;
    }

    /**
     * @param list<string> $runIds
     * @return array{history_reads: int, result_count: int}
     */
    private function measureLookup(array $runIds): array
    {
        $before = $this->handlerReads();

        $events = DB::table('workflow_history_events')
            ->whereIn('workflow_run_id', $runIds)
            ->where('event_type', 'WorkflowStarted')
            ->get(['id']);

        $after = $this->handlerReads();

        return [
            'history_reads' => ($after['Handler_read_next'] - $before['Handler_read_next'])
                + ($after['Handler_read_rnd_next'] - $before['Handler_read_rnd_next']),
            'result_count' => $events->count(),
        ];
    }

    /**
     * @param list<mixed> $bindings
     * @return array{history_reads: int, result_count: int}
     */
    private function measureSql(string $sql, array $bindings): array
    {
        $before = $this->handlerReads();
        $events = DB::select($sql, $bindings);
        $after = $this->handlerReads();

        return [
            'history_reads' => ($after['Handler_read_next'] - $before['Handler_read_next'])
                + ($after['Handler_read_rnd_next'] - $before['Handler_read_rnd_next']),
            'result_count' => count($events),
        ];
    }

    /**
     * @return array{Handler_read_next: int, Handler_read_rnd_next: int}
     */
    private function handlerReads(): array
    {
        $status = DB::select(
            "SHOW SESSION STATUS WHERE Variable_name IN ('Handler_read_next', 'Handler_read_rnd_next')",
        );
        $reads = [];

        foreach ($status as $row) {
            $values = (array) $row;
            $reads[(string) $values['Variable_name']] = (int) $values['Value'];
        }

        return [
            'Handler_read_next' => $reads['Handler_read_next'] ?? 0,
            'Handler_read_rnd_next' => $reads['Handler_read_rnd_next'] ?? 0,
        ];
    }
}
