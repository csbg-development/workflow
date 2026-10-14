<?php

declare(strict_types=1);

namespace Tests\Unit\Migrations;

use Illuminate\Database\Connection;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Schema;
use Tests\SchemaTestCase;

final class WorkflowTimelineUpdatedAtIndexMigrationTest extends SchemaTestCase
{
    private const INDEX = 'wfrte_updated_at_idx';

    private const MIGRATION = __DIR__
        . '/../../../src/migrations/2026_10_14_000100_add_updated_at_index_to_workflow_run_timeline_entries.php';

    private const REPRESENTATIVE_ROW_COUNT = 10_000;

    public function testTimelineUpdatedAtIndexSupportsProjectionFreshnessMetrics(): void
    {
        $this->assertSame(['updated_at'], $this->timelineIndexes()[self::INDEX] ?? null);
    }

    public function testMigrationCanResumeAndRollback(): void
    {
        $migration = require self::MIGRATION;

        $migration->up();
        $migration->up();

        $this->assertSame(['updated_at'], $this->timelineIndexes()[self::INDEX] ?? null);

        $row = [
            'id' => 'timeline-index-rollback-entry',
            'workflow_run_id' => 'timeline-rollback-run-0001',
            'workflow_instance_id' => 'timeline-index-rollback-instance',
            'history_event_id' => 'timeline-rollback-event-01',
            'sequence' => 1,
            'type' => 'TimelineIndexRollback',
            'kind' => 'workflow',
            'entry_kind' => 'point',
            'summary' => 'Preserve this timeline entry during index rollback.',
        ];
        DB::table('workflow_run_timeline_entries')->insert($row);

        $migration->down();
        $migration->down();

        $this->assertTrue(Schema::hasTable('workflow_run_timeline_entries'));
        $this->assertArrayNotHasKey(self::INDEX, $this->timelineIndexes());
        $this->assertSame(
            $row,
            (array) DB::table('workflow_run_timeline_entries')
                ->where('id', $row['id'])
                ->first(array_keys($row)),
        );

        $migration->up();
    }

    public function testMigrationIsSafeWhenTheTimelineTableIsAbsent(): void
    {
        $migration = require self::MIGRATION;

        Schema::drop('workflow_run_timeline_entries');

        $migration->up();
        $migration->down();

        $this->assertFalse(Schema::hasTable('workflow_run_timeline_entries'));
    }

    public function testMySqlIndexBuildIsOnlineAndRestoresSessionSettings(): void
    {
        $connection = DB::connection();

        if ($connection->getDriverName() !== 'mysql') {
            $this->markTestSkipped('MySQL online DDL settings require MySQL.');
        }

        $migration = require self::MIGRATION;
        $migration->down();
        $before = $connection->selectFromWriteConnection(
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

        $after = $connection->selectFromWriteConnection(
            "SHOW SESSION VARIABLES WHERE Variable_name IN ('lock_wait_timeout', 'innodb_ddl_threads', "
            . "'innodb_ddl_buffer_size', 'innodb_parallel_read_threads')",
        );
        $this->assertEquals($before, $after);
        $ddl = array_values(array_filter(
            $queries,
            static fn (array $query): bool => str_starts_with($query['query'], 'ALTER TABLE'),
        ));
        $this->assertCount(1, $ddl);
        $this->assertStringContainsString('ADD INDEX `wfrte_updated_at_idx` (`updated_at`)', $ddl[0]['query']);
        $this->assertStringContainsString('ALGORITHM=INPLACE, LOCK=NONE', $ddl[0]['query']);
        $this->assertContains([10], array_column(array_filter(
            $queries,
            static fn (array $query): bool => $query['query'] === 'SET SESSION lock_wait_timeout = ?',
        ), 'bindings'));
    }

    public function testMySqlMigrationRejectsAnExistingIndexWithDifferentColumns(): void
    {
        if (DB::connection()->getDriverName() !== 'mysql') {
            $this->markTestSkipped('MySQL index definition check requires MySQL.');
        }

        $migration = require self::MIGRATION;
        $migration->down();
        Schema::table('workflow_run_timeline_entries', static function ($table): void {
            $table->index(['created_at'], self::INDEX);
        });

        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessage('unexpected definition');
        $migration->up();
    }

    public function testOnlineDdlRunsWhenOptionalTuningVariablesAreUnavailable(): void
    {
        $migration = require self::MIGRATION;
        $grammar = DB::connection()->getQueryGrammar();
        $connection = $this->createMock(Connection::class);
        $statements = [];
        $connection->expects($this->once())
            ->method('selectFromWriteConnection')
            ->willReturn([
                (object) [
                    'Variable_name' => 'lock_wait_timeout',
                    'Value' => '31536000',
                ],
            ]);
        $connection->method('getQueryGrammar')
            ->willReturn($grammar);
        $connection->expects($this->exactly(3))
            ->method('statement')
            ->willReturnCallback(static function (string $query, array $bindings = []) use (&$statements): bool {
                $statements[] = [$query, $bindings];

                return true;
            });
        (new \ReflectionMethod($migration, 'alterOnlineIndex'))->invoke($migration, $connection);

        $this->assertSame('SET SESSION lock_wait_timeout = ?', $statements[0][0]);
        $this->assertSame([10], $statements[0][1]);
        $this->assertStringContainsString('ALGORITHM=INPLACE, LOCK=NONE', $statements[1][0]);
        $this->assertSame('SET SESSION lock_wait_timeout = ?', $statements[2][0]);
        $this->assertSame([31536000], $statements[2][1]);
    }

    public function testOnlineDdlDoesNotRunWithoutLockWaitTimeout(): void
    {
        $migration = require self::MIGRATION;
        $connection = $this->createMock(Connection::class);
        $connection->expects($this->once())
            ->method('selectFromWriteConnection')
            ->willReturn([
                (object) [
                    'Variable_name' => 'innodb_ddl_threads',
                    'Value' => '4',
                ],
            ]);
        $connection->expects($this->never())
            ->method('statement');

        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessage('lock_wait_timeout');
        (new \ReflectionMethod($migration, 'alterOnlineIndex'))->invoke($migration, $connection);
    }

    public function testMySqlMigrationUsesWriterWhenAReaderIsConfiguredForApplyAndRollback(): void
    {
        $connection = DB::connection();

        if ($connection->getDriverName() !== 'mysql') {
            $this->markTestSkipped('This split-connection regression requires MySQL.');
        }

        $migration = require self::MIGRATION;
        $reader = $connection->getReadPdo();
        $connection->setReadPdo(new \PDO('sqlite::memory:'));

        try {
            $migration->down();
            $migration->up();
        } finally {
            $connection->setReadPdo($reader);
        }

        $this->assertSame(['updated_at'], $this->timelineIndexes()[self::INDEX] ?? null);
    }

    public function testMySqlMinAndMaxUseTheIndexWithoutChangingNullOrDuplicateTimestamps(): void
    {
        $connection = DB::connection();

        if ($connection->getDriverName() !== 'mysql') {
            $this->markTestSkipped('This aggregate query-plan regression requires MySQL 8.4.');
        }

        $migration = require self::MIGRATION;
        $migration->down();
        $this->seedRepresentativeTimelineHistory();
        $connection->statement('ANALYZE TABLE workflow_run_timeline_entries');

        $beforeMin = (array) $connection->selectOne(
            'EXPLAIN SELECT MIN(updated_at) FROM workflow_run_timeline_entries',
        );
        $beforeMax = (array) $connection->selectOne(
            'EXPLAIN SELECT MAX(updated_at) FROM workflow_run_timeline_entries',
        );
        $this->assertSame('ALL', $beforeMin['type'] ?? null, json_encode($beforeMin, JSON_THROW_ON_ERROR));
        $this->assertSame('ALL', $beforeMax['type'] ?? null, json_encode($beforeMax, JSON_THROW_ON_ERROR));

        $migration->up();
        $connection->statement('ANALYZE TABLE workflow_run_timeline_entries');

        $afterMin = (array) $connection->selectOne(
            'EXPLAIN SELECT MIN(updated_at) FROM workflow_run_timeline_entries',
        );
        $afterMax = (array) $connection->selectOne(
            'EXPLAIN SELECT MAX(updated_at) FROM workflow_run_timeline_entries',
        );
        $this->assertSame(
            'Select tables optimized away',
            $afterMin['Extra'] ?? null,
            json_encode($afterMin, JSON_THROW_ON_ERROR),
        );
        $this->assertSame(
            'Select tables optimized away',
            $afterMax['Extra'] ?? null,
            json_encode($afterMax, JSON_THROW_ON_ERROR),
        );

        $oldest = '2026-01-01 00:00:00.000000';
        $newest = '2026-12-31 23:59:59.000000';
        $aggregate = (array) $connection->selectOne(
            'SELECT MIN(updated_at) AS oldest, MAX(updated_at) AS newest '
            . 'FROM workflow_run_timeline_entries',
        );
        $this->assertSame($oldest, $aggregate['oldest'] ?? null);
        $this->assertSame($newest, $aggregate['newest'] ?? null);
        $this->assertSame(
            1,
            $connection->table('workflow_run_timeline_entries')
                ->whereNull('updated_at')
                ->count(),
        );
        $this->assertSame(
            2,
            $connection->table('workflow_run_timeline_entries')
                ->where('updated_at', $oldest)
                ->count(),
        );
        $this->assertSame(
            2,
            $connection->table('workflow_run_timeline_entries')
                ->where('updated_at', $newest)
                ->count(),
        );
    }

    private function seedRepresentativeTimelineHistory(): void
    {
        $rows = [];

        for ($ordinal = 1; $ordinal <= self::REPRESENTATIVE_ROW_COUNT; $ordinal++) {
            $updatedAt = match ($ordinal) {
                1, 2 => '2026-01-01 00:00:00.000000',
                3 => null,
                self::REPRESENTATIVE_ROW_COUNT - 1, self::REPRESENTATIVE_ROW_COUNT =>
                    '2026-12-31 23:59:59.000000',
                default => '2026-06-01 12:00:00.000000',
            };
            $rows[] = [
                'id' => sprintf('T%063d', $ordinal),
                'workflow_run_id' => sprintf('R%025d', 1),
                'workflow_instance_id' => 'timeline-index-mysql-smoke',
                'history_event_id' => sprintf('E%025d', $ordinal),
                'sequence' => $ordinal,
                'type' => 'TimelineIndexSmoke',
                'kind' => 'workflow',
                'entry_kind' => 'point',
                'updated_at' => $updatedAt,
            ];

            if (count($rows) === 500) {
                DB::table('workflow_run_timeline_entries')->insert($rows);
                $rows = [];
            }
        }

        if ($rows !== []) {
            DB::table('workflow_run_timeline_entries')->insert($rows);
        }
    }

    /**
     * @return array<string, list<string>>
     */
    private function timelineIndexes(): array
    {
        $indexes = [];

        foreach (Schema::getIndexes('workflow_run_timeline_entries') as $definition) {
            $name = $definition['name'] ?? null;
            $columns = $definition['columns'] ?? null;

            if (is_string($name) && is_array($columns)) {
                $indexes[$name] = array_values($columns);
            }
        }

        return $indexes;
    }
}
