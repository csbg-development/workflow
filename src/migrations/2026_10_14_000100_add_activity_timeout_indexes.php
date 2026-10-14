<?php

declare(strict_types=1);

use Illuminate\Database\Connection;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\DB;
use Workflow\Support\WorkflowMigration;

return new class() extends WorkflowMigration {
    private const INDEXES = [
        'ae_status_schedule_deadline_idx' => ['status', 'schedule_deadline_at'],
        'ae_status_close_deadline_idx' => ['status', 'close_deadline_at'],
        'ae_status_schedule_close_idx' => ['status', 'schedule_to_close_deadline_at'],
        'ae_status_heartbeat_deadline_idx' => ['status', 'heartbeat_deadline_at'],
    ];

    private const TABLE = 'activity_executions';

    public function up(): void
    {
        $connection = DB::connection($this->getConnection());
        $schema = $connection->getSchemaBuilder();

        if (! $this->tableExists($connection)) {
            return;
        }

        $missing = $this->missingIndexes($connection);

        if ($missing === []) {
            return;
        }

        if ($connection->getDriverName() === 'mysql') {
            $this->alterOnlineIndexes($connection, $missing);

            return;
        }

        $schema->table(self::TABLE, static function (Blueprint $table) use ($missing): void {
            foreach ($missing as $name => $columns) {
                $table->index($columns, $name);
            }
        });
    }

    public function down(): void
    {
        $connection = DB::connection($this->getConnection());
        $schema = $connection->getSchemaBuilder();

        if (! $this->tableExists($connection)) {
            return;
        }

        $existing = array_filter(
            self::INDEXES,
            fn (array $columns, string $name): bool => $this->indexExists($connection, $name, $columns),
            ARRAY_FILTER_USE_BOTH,
        );

        if ($existing === []) {
            return;
        }

        if ($connection->getDriverName() === 'mysql') {
            $this->alterOnlineIndexes($connection, $existing, drop: true);

            return;
        }

        $schema->table(self::TABLE, static function (Blueprint $table) use ($existing): void {
            foreach (array_keys($existing) as $name) {
                $table->dropIndex($name);
            }
        });
    }

    /**
     * @return array<string, list<string>>
     */
    private function missingIndexes(Connection $connection): array
    {
        return array_filter(
            self::INDEXES,
            fn (array $columns, string $name): bool => ! $this->indexExists($connection, $name, $columns),
            ARRAY_FILTER_USE_BOTH,
        );
    }

    /**
     * @param list<string> $expectedColumns
     */
    private function indexExists(Connection $connection, string $name, array $expectedColumns): bool
    {
        $table = $connection->getTablePrefix() . self::TABLE;

        if (in_array($connection->getDriverName(), ['mysql', 'mariadb'], true)) {
            $columns = $connection->selectFromWriteConnection(
                'SELECT column_name FROM information_schema.statistics '
                . 'WHERE table_schema = database() AND table_name = ? AND index_name = ? ORDER BY seq_in_index',
                [$table, $name],
            );

            if ($columns === []) {
                return false;
            }

            $actualColumns = array_map(
                static fn (object $column): string => $column->COLUMN_NAME ?? $column->column_name,
                $columns,
            );

            if ($actualColumns !== $expectedColumns) {
                throw new RuntimeException("Existing activity timeout index [{$name}] has an unexpected definition.");
            }

            return true;
        }

        return match ($connection->getDriverName()) {
            'pgsql' => $connection->selectOne(
                'SELECT 1 FROM pg_indexes '
                . 'WHERE schemaname = ANY (current_schemas(false)) AND tablename = ? AND indexname = ? LIMIT 1',
                [$table, $name],
                false,
            ) !== null,
            'sqlite' => $connection->selectOne(
                "SELECT 1 FROM sqlite_master WHERE type = 'index' AND tbl_name = ? AND name = ? LIMIT 1",
                [$table, $name],
                false,
            ) !== null,
            'sqlsrv' => $connection->selectOne(
                "SELECT TOP 1 1 FROM sys.indexes WHERE object_id = OBJECT_ID(?, 'U') AND name = ?",
                [$table, $name],
                false,
            ) !== null,
            default => throw new RuntimeException(sprintf(
                'Cannot inspect activity timeout indexes for database driver [%s].',
                $connection->getDriverName(),
            )),
        };
    }

    private function tableExists(Connection $connection): bool
    {
        $table = $connection->getTablePrefix() . self::TABLE;
        $sql = match ($connection->getDriverName()) {
            'mysql', 'mariadb' => 'SELECT 1 FROM information_schema.tables '
                . "WHERE table_schema = database() AND table_name = ? AND table_type IN ('BASE TABLE', 'SYSTEM VERSIONED') LIMIT 1",
            'pgsql' => 'SELECT 1 FROM information_schema.tables '
                . "WHERE table_schema = ANY (current_schemas(false)) AND table_name = ? AND table_type = 'BASE TABLE' LIMIT 1",
            'sqlite' => "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ? LIMIT 1",
            'sqlsrv' => "SELECT TOP 1 1 FROM sys.tables WHERE object_id = OBJECT_ID(?, 'U')",
            default => throw new RuntimeException(sprintf(
                'Cannot inspect activity execution tables for database driver [%s].',
                $connection->getDriverName(),
            )),
        };

        return $connection->selectOne($sql, [$table], false) !== null;
    }

    /**
     * @param array<string, list<string>> $indexes
     */
    private function alterOnlineIndexes(Connection $connection, array $indexes, bool $drop = false): void
    {
        $limits = [
            'lock_wait_timeout' => 10,
            'innodb_ddl_threads' => 1,
            'innodb_ddl_buffer_size' => 1048576,
            'innodb_parallel_read_threads' => 1,
        ];
        $settings = $connection->selectFromWriteConnection(
            "SHOW SESSION VARIABLES WHERE Variable_name IN ('lock_wait_timeout', 'innodb_ddl_threads', "
            . "'innodb_ddl_buffer_size', 'innodb_parallel_read_threads')",
        );
        $available = array_map(static fn (object $setting): string => (string) $setting->Variable_name, $settings);

        if (! in_array('lock_wait_timeout', $available, true)) {
            throw new RuntimeException('Activity timeout indexes require the MySQL lock_wait_timeout session limit.');
        }

        $original = [];
        $failure = null;

        try {
            foreach ($settings as $setting) {
                $name = (string) $setting->Variable_name;

                if (! array_key_exists($name, $limits)) {
                    continue;
                }

                $original[$name] = (int) $setting->Value;
                $connection->statement("SET SESSION {$name} = ?", [$limits[$name]]);
            }

            $grammar = $connection->getQueryGrammar();
            $table = $grammar->wrapTable(self::TABLE);
            $operations = [];

            foreach ($indexes as $name => $columns) {
                $index = $grammar->wrap($name);
                $operations[] = $drop
                    ? "DROP INDEX {$index}"
                    : sprintf('ADD INDEX %s (%s)', $index, implode(', ', $grammar->wrapArray($columns)));
            }

            $connection->statement(sprintf(
                'ALTER TABLE %s %s, ALGORITHM=INPLACE, LOCK=NONE',
                $table,
                implode(', ', $operations),
            ));
        } catch (Throwable $error) {
            $failure = $error;
        } finally {
            foreach ($original as $name => $value) {
                try {
                    $connection->statement("SET SESSION {$name} = ?", [$value]);
                } catch (Throwable $error) {
                    $failure ??= $error;
                }
            }
        }

        if ($failure !== null) {
            throw $failure;
        }
    }
};
