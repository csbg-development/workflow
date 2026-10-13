<?php

declare(strict_types=1);

namespace Tests\Unit\Migrations;

use Illuminate\Database\Connection;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Schema;
use Tests\SchemaTestCase;

final class WorkflowHistoryEventIndexMigrationTest extends SchemaTestCase
{
    private const INDEX = 'wfhe_run_type_idx';

    private const MIGRATION = __DIR__
        . '/../../../src/migrations/2026_10_13_000100_add_run_event_type_index_to_workflow_history_events.php';

    public function testHistoryLookupIndexUsesRunAndEventTypeWithoutReplacingExistingIndexes(): void
    {
        $indexes = $this->historyIndexes();

        $this->assertSame(['workflow_run_id', 'event_type'], $indexes[self::INDEX] ?? null);
        $this->assertSame(['workflow_run_id'], $indexes['workflow_history_events_workflow_run_id_index'] ?? null);
        $this->assertSame(
            ['workflow_run_id', 'sequence'],
            $indexes['workflow_history_events_workflow_run_id_sequence_unique'] ?? null,
        );
    }

    public function testMigrationCanResumeAndRollbackAfterPartialDdl(): void
    {
        $migration = require self::MIGRATION;

        $migration->up();
        $migration->up();

        $this->assertSame(['workflow_run_id', 'event_type'], $this->historyIndexes()[self::INDEX] ?? null);

        $migration->down();
        $migration->down();

        $indexes = $this->historyIndexes();
        $this->assertArrayNotHasKey(self::INDEX, $indexes);
        $this->assertSame(['workflow_run_id'], $indexes['workflow_history_events_workflow_run_id_index'] ?? null);
        $this->assertSame(
            ['workflow_run_id', 'sequence'],
            $indexes['workflow_history_events_workflow_run_id_sequence_unique'] ?? null,
        );

        $migration->up();
    }

    public function testMigrationIsSafeWhenTheHistoryTableIsAbsent(): void
    {
        $migration = require self::MIGRATION;

        Schema::drop('workflow_history_events');

        $migration->up();
        $migration->down();

        $this->assertFalse(Schema::hasTable('workflow_history_events'));
    }

    public function testMySqlIndexBuildIsOnlineAndRestoresSessionSettings(): void
    {
        $connection = DB::connection();

        if ($connection->getDriverName() !== 'mysql') {
            $this->markTestSkipped('MySQL online DDL settings require MySQL.');
        }

        $migration = require self::MIGRATION;
        $migration->down();
        $before = $connection->select(
            "SHOW SESSION VARIABLES WHERE Variable_name IN ('lock_wait_timeout', 'innodb_ddl_threads', "
            . "'innodb_ddl_buffer_size', 'innodb_parallel_read_threads')",
        );
        $connection->flushQueryLog();
        $connection->enableQueryLog();

        try {
            $migration->up();
            $queries = $connection->getQueryLog();
        } finally {
            $connection->disableQueryLog();
        }

        $after = $connection->select(
            "SHOW SESSION VARIABLES WHERE Variable_name IN ('lock_wait_timeout', 'innodb_ddl_threads', "
            . "'innodb_ddl_buffer_size', 'innodb_parallel_read_threads')",
        );
        $this->assertEquals($before, $after);
        $ddl = array_values(array_filter($queries, static fn (array $query): bool =>
            str_starts_with($query['query'], 'ALTER TABLE')));
        $this->assertCount(1, $ddl);
        $this->assertStringContainsString('ALGORITHM=INPLACE, LOCK=NONE', $ddl[0]['query']);
        $this->assertContains([10], array_column(array_filter($queries, static fn (array $query): bool =>
            $query['query'] === 'SET SESSION lock_wait_timeout = ?'), 'bindings'));
    }

    public function testMySqlMigrationRejectsAnExistingIndexWithDifferentColumns(): void
    {
        if (DB::connection()->getDriverName() !== 'mysql') {
            $this->markTestSkipped('MySQL index definition check requires MySQL.');
        }

        $migration = require self::MIGRATION;
        $migration->down();
        Schema::table('workflow_history_events', static fn ($table) => $table->index(['event_type'], self::INDEX));

        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessage('unexpected definition');
        $migration->up();
    }

    public function testOnlineDdlDoesNotRunWithMissingSessionLimits(): void
    {
        $migration = require self::MIGRATION;
        $connection = $this->createMock(Connection::class);
        $connection->method('select')
            ->willReturn([
                (object) [
                    'Variable_name' => 'lock_wait_timeout',
                    'Value' => '31536000',
                ],
            ]);
        $connection->expects($this->never())
            ->method('statement');

        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessage('session limits');
        (new \ReflectionMethod($migration, 'alterOnlineIndex'))->invoke($migration, $connection);
    }

    /**
     * @return array<string, list<string>>
     */
    private function historyIndexes(): array
    {
        $indexes = [];

        foreach (Schema::getIndexes('workflow_history_events') as $definition) {
            $name = $definition['name'] ?? null;
            $columns = $definition['columns'] ?? null;

            if (is_string($name) && is_array($columns)) {
                $indexes[$name] = array_values($columns);
            }
        }

        return $indexes;
    }
}
