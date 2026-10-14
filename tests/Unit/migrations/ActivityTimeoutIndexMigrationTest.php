<?php

declare(strict_types=1);

namespace Tests\Unit\Migrations;

use Illuminate\Database\Connection;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Schema;
use Tests\SchemaTestCase;

final class ActivityTimeoutIndexMigrationTest extends SchemaTestCase
{
    private const INDEXES = [
        'ae_status_schedule_deadline_idx' => ['status', 'schedule_deadline_at'],
        'ae_status_close_deadline_idx' => ['status', 'close_deadline_at'],
        'ae_status_schedule_close_idx' => ['status', 'schedule_to_close_deadline_at'],
        'ae_status_heartbeat_deadline_idx' => ['status', 'heartbeat_deadline_at'],
    ];

    private const MIGRATION = __DIR__
        . '/../../../src/migrations/2026_10_14_000100_add_activity_timeout_indexes.php';

    public function testTimeoutIndexesMatchTheCandidateQueryBranches(): void
    {
        $indexes = $this->activityIndexes();

        foreach (self::INDEXES as $name => $columns) {
            $this->assertSame($columns, $indexes[$name] ?? null);
        }
    }

    public function testMigrationCanResumeAndRollback(): void
    {
        $migration = require self::MIGRATION;

        $migration->up();
        $migration->up();

        foreach (self::INDEXES as $name => $columns) {
            $this->assertSame($columns, $this->activityIndexes()[$name] ?? null);
        }

        $migration->down();
        $migration->down();

        $indexes = $this->activityIndexes();

        foreach (array_keys(self::INDEXES) as $name) {
            $this->assertArrayNotHasKey($name, $indexes);
        }

        $migration->up();
    }

    public function testMigrationIsSafeWhenTheActivityTableIsAbsent(): void
    {
        $migration = require self::MIGRATION;

        Schema::drop('activity_executions');

        $migration->up();
        $migration->down();

        $this->assertFalse(Schema::hasTable('activity_executions'));
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
        $ddl = array_values(array_filter(
            $queries,
            static fn (array $query): bool => str_starts_with($query['query'], 'ALTER TABLE'),
        ));
        $this->assertCount(1, $ddl);
        $this->assertStringContainsString('ALGORITHM=INPLACE, LOCK=NONE', $ddl[0]['query']);

        foreach (array_keys(self::INDEXES) as $name) {
            $this->assertStringContainsString($name, $ddl[0]['query']);
        }
    }

    public function testMySqlMigrationRejectsAnExistingIndexWithDifferentColumns(): void
    {
        if (DB::connection()->getDriverName() !== 'mysql') {
            $this->markTestSkipped('MySQL index definition check requires MySQL.');
        }

        $migration = require self::MIGRATION;
        $migration->down();
        Schema::table('activity_executions', static function ($table): void {
            $table->index(['schedule_deadline_at'], 'ae_status_schedule_deadline_idx');
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
        (new \ReflectionMethod($migration, 'alterOnlineIndexes'))->invoke($migration, $connection, self::INDEXES);

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
        (new \ReflectionMethod($migration, 'alterOnlineIndexes'))->invoke($migration, $connection, self::INDEXES);
    }

    /**
     * @return array<string, list<string>>
     */
    private function activityIndexes(): array
    {
        $indexes = [];

        foreach (Schema::getIndexes('activity_executions') as $definition) {
            $name = $definition['name'] ?? null;
            $columns = $definition['columns'] ?? null;

            if (is_string($name) && is_array($columns)) {
                $indexes[$name] = array_values($columns);
            }
        }

        return $indexes;
    }
}
