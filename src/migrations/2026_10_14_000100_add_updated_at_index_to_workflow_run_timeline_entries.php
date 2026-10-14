<?php

declare(strict_types=1);

use Illuminate\Database\Connection;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\DB;
use Workflow\Support\WorkflowMigration;

return new class() extends WorkflowMigration {
    private const INDEX = 'wfrte_updated_at_idx';

    private const TABLE = 'workflow_run_timeline_entries';

    public function up(): void
    {
        $connection = DB::connection($this->getConnection());
        $schema = $connection->getSchemaBuilder();

        if (! $this->tableExists($connection) || $this->indexExists($connection)) {
            return;
        }

        if ($connection->getDriverName() === 'mysql') {
            $this->alterOnlineIndex($connection);

            return;
        }

        $schema->table(self::TABLE, static function (Blueprint $table): void {
            $table->index(['updated_at'], self::INDEX);
        });
    }

    public function down(): void
    {
        $connection = DB::connection($this->getConnection());
        $schema = $connection->getSchemaBuilder();

        if (! $this->tableExists($connection) || ! $this->indexExists($connection)) {
            return;
        }

        if ($connection->getDriverName() === 'mysql') {
            $this->alterOnlineIndex($connection, drop: true);

            return;
        }

        $schema->table(self::TABLE, static function (Blueprint $table): void {
            $table->dropIndex(self::INDEX);
        });
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
            default => throw new \RuntimeException(sprintf(
                'Cannot inspect workflow timeline tables for database driver [%s].',
                $connection->getDriverName(),
            )),
        };

        return $connection->selectOne($sql, [$table], false) !== null;
    }

    private function indexExists(Connection $connection): bool
    {
        $table = $connection->getTablePrefix() . self::TABLE;

        if (in_array($connection->getDriverName(), ['mysql', 'mariadb'], true)) {
            $columns = $connection->selectFromWriteConnection(
                'SELECT column_name FROM information_schema.statistics '
                . 'WHERE table_schema = database() AND table_name = ? AND index_name = ? ORDER BY seq_in_index',
                [$table, self::INDEX],
            );

            if ($columns === []) {
                return false;
            }

            if (array_map(static fn (object $column): string => $column->COLUMN_NAME ?? $column->column_name, $columns)
                !== ['updated_at']) {
                throw new \RuntimeException('Existing workflow timeline freshness index has an unexpected definition.');
            }

            return true;
        }

        return match ($connection->getDriverName()) {
            'pgsql' => $connection->selectOne(
                'SELECT 1 FROM pg_indexes '
                . 'WHERE schemaname = ANY (current_schemas(false)) AND tablename = ? AND indexname = ? LIMIT 1',
                [$table, self::INDEX],
                false,
            ) !== null,
            'sqlite' => $connection->selectOne(
                "SELECT 1 FROM sqlite_master WHERE type = 'index' AND tbl_name = ? AND name = ? LIMIT 1",
                [$table, self::INDEX],
                false,
            ) !== null,
            'sqlsrv' => $connection->selectOne(
                "SELECT TOP 1 1 FROM sys.indexes WHERE object_id = OBJECT_ID(?, 'U') AND name = ?",
                [$table, self::INDEX],
                false,
            ) !== null,
            default => throw new \RuntimeException(sprintf(
                'Cannot inspect workflow timeline indexes for database driver [%s].',
                $connection->getDriverName(),
            )),
        };
    }

    private function alterOnlineIndex(Connection $connection, bool $drop = false): void
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
            throw new \RuntimeException(
                'Workflow timeline freshness index requires the MySQL lock_wait_timeout session limit.'
            );
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
            $index = $grammar->wrap(self::INDEX);
            $column = $grammar->wrap('updated_at');
            $operation = $drop ? "DROP INDEX {$index}" : "ADD INDEX {$index} ({$column})";
            $connection->statement("ALTER TABLE {$table} {$operation}, ALGORITHM=INPLACE, LOCK=NONE");
        } catch (\Throwable $error) {
            $failure = $error;
        } finally {
            foreach ($original as $name => $value) {
                try {
                    $connection->statement("SET SESSION {$name} = ?", [$value]);
                } catch (\Throwable $error) {
                    $failure ??= $error;
                }
            }
        }

        if ($failure !== null) {
            throw $failure;
        }
    }
};
