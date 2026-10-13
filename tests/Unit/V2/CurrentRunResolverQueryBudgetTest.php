<?php

declare(strict_types=1);

namespace Tests\Unit\V2;

use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Schema;
use PHPUnit\Framework\Attributes\DataProvider;
use Tests\SchemaTestCase;
use Workflow\V2\Enums\HistoryEventType;
use Workflow\V2\Enums\RunStatus;
use Workflow\V2\Models\WorkflowHistoryEvent;
use Workflow\V2\Models\WorkflowInstance;
use Workflow\V2\Models\WorkflowRun;
use Workflow\V2\Support\CurrentRunResolver;

final class CurrentRunResolverQueryBudgetTest extends SchemaTestCase
{
    /**
     * @return iterable<string, array{int}>
     */
    public static function lineageSizes(): iterable
    {
        yield 'short lineage' => [3];
        yield 'long lineage' => [244];
    }

    #[DataProvider('lineageSizes')]
    public function testLineageQueryHasFixedBindingsAndPreservesCanonicalTail(int $runCount): void
    {
        [$instance, $runs] = $this->seedLineage('bounded-lineage', $runCount);
        $this->seedRun($instance, $runCount + 1);
        $this->seedLineage('unrelated-lineage', 3);
        $connection = DB::connection();
        $connection->flushQueryLog();
        $connection->enableQueryLog();

        try {
            $resolution = CurrentRunResolver::resolutionForInstance($instance->fresh());
            $queries = $connection->getQueryLog();
        } finally {
            $connection->disableQueryLog();
        }

        $this->assertSame($runs[$runCount - 1]->id, $resolution['run']?->id);
        $this->assertSame(CurrentRunResolver::SOURCE_CONTINUE_AS_NEW_LINEAGE, $resolution['source']);
        $historyQueries = array_values(array_filter($queries, static fn (array $query): bool =>
            str_starts_with(strtolower($query['query']), 'select')
            && str_contains($query['query'], 'workflow_history_events')));
        $this->assertCount(1, $historyQueries);
        $this->assertLessThanOrEqual(2, count($historyQueries[0]['bindings']));
    }

    public function testLoadedRunSubsetDoesNotAdoptAChildOutsideItsSnapshot(): void
    {
        [$instance, $runs] = $this->seedLineage('loaded-lineage', 3);
        $instance->setRelation('runs', $instance->runs()->whereKey([$runs[0]->id, $runs[1]->id])->get());

        $resolution = CurrentRunResolver::resolutionForInstance($instance);

        $this->assertSame($runs[1]->id, $resolution['run']?->id);
        $this->assertSame(CurrentRunResolver::SOURCE_CONTINUE_AS_NEW_LINEAGE, $resolution['source']);
    }

    public function testLockedResolutionIgnoresStaleLoadedRunSubset(): void
    {
        [$instance, $runs] = $this->seedLineage('locked-lineage', 3);
        $instance->setRelation('runs', $instance->runs()->whereKey([$runs[0]->id, $runs[1]->id])->get());

        $resolution = DB::transaction(static fn (): array =>
            CurrentRunResolver::resolutionForInstance($instance, lockForUpdate: true));

        $this->assertSame($runs[2]->id, $resolution['run']?->id);
        $this->assertSame(CurrentRunResolver::SOURCE_CONTINUE_AS_NEW_LINEAGE, $resolution['source']);
    }

    public function testLineageSubqueryUsesConfiguredRunAndHistoryTables(): void
    {
        [$instance, $runs] = $this->seedLineage('configured-lineage', 3);
        DB::statement('CREATE TABLE lineage_custom_runs AS SELECT * FROM workflow_runs');
        DB::statement('CREATE TABLE lineage_custom_history AS SELECT * FROM workflow_history_events');
        config()
            ->set('workflows.v2.run_model', LineageConfiguredRun::class);
        config()
            ->set('workflows.v2.history_event_model', LineageConfiguredHistory::class);

        $resolution = CurrentRunResolver::resolutionForInstance($instance->fresh());

        $this->assertInstanceOf(LineageConfiguredRun::class, $resolution['run']);
        $this->assertSame($runs[2]->id, $resolution['run']?->id);
        $this->assertSame(CurrentRunResolver::SOURCE_CONTINUE_AS_NEW_LINEAGE, $resolution['source']);
    }

    public function testLineageLookupPreservesIndependentlyConfiguredHistoryConnection(): void
    {
        [$instance, $runs] = $this->seedLineage('separate-history-lineage', 3);
        config()
            ->set('database.connections.lineage-secondary', [
                'driver' => 'sqlite',
                'database' => ':memory:',
            ]);
        Schema::connection('lineage-secondary')->create(
            'workflow_history_events',
            static function (Blueprint $table): void {
                $table->string('id')
                    ->primary();
                $table->string('workflow_run_id');
                $table->string('event_type');
                $table->text('payload')
                    ->nullable();
            }
        );
        DB::connection('lineage-secondary')->table('workflow_history_events')->insert(
            DB::table('workflow_history_events')->get(['id', 'workflow_run_id', 'event_type', 'payload'])
                ->map(static fn (object $row): array => (array) $row)
                ->all(),
        );
        config()
            ->set('workflows.v2.history_event_model', LineageSeparateHistory::class);

        try {
            $resolution = CurrentRunResolver::resolutionForInstance($instance->fresh());

            $this->assertSame($runs[2]->id, $resolution['run']?->id);
            $this->assertSame(CurrentRunResolver::SOURCE_CONTINUE_AS_NEW_LINEAGE, $resolution['source']);
        } finally {
            DB::purge('lineage-secondary');
        }
    }

    /**
     * @return array{WorkflowInstance, list<WorkflowRun>}
     */
    private function seedLineage(string $name, int $count): array
    {
        $instance = WorkflowInstance::query()->create([
            'id' => $name,
            'workflow_class' => 'Tests\\Workflows\\QueryBudgetWorkflow',
            'workflow_type' => 'query.budget.workflow',
            'run_count' => $count,
        ]);
        $runs = [];

        for ($number = 1; $number <= $count; $number++) {
            $run = $this->seedRun($instance, $number);
            WorkflowHistoryEvent::query()->create([
                'workflow_run_id' => $run->id,
                'sequence' => 2,
                'event_type' => HistoryEventType::WorkflowStarted->value,
                'payload' => $number === 1 ? [] : [
                    'continued_from_run_id' => $runs[$number - 2]->id,
                ],
                'recorded_at' => now(),
            ]);
            $runs[] = $run;
        }

        return [$instance, $runs];
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
}

final class LineageConfiguredRun extends WorkflowRun
{
    protected $table = 'lineage_custom_runs';
}

final class LineageConfiguredHistory extends WorkflowHistoryEvent
{
    protected $table = 'lineage_custom_history';
}

final class LineageSeparateHistory extends WorkflowHistoryEvent
{
    protected $connection = 'lineage-secondary';
}
