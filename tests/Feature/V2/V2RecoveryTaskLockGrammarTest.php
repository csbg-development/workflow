<?php

declare(strict_types=1);

namespace Tests\Feature\V2;

use Illuminate\Database\Query\Grammars\PostgresGrammar;
use Illuminate\Support\Facades\DB;
use Orchestra\Testbench\TestCase;
use Workflow\Providers\WorkflowServiceProvider;
use Workflow\V2\Enums\RunStatus;
use Workflow\V2\Models\WorkflowRun;
use Workflow\V2\Support\WorkflowExecutor;

final class V2RecoveryTaskLockGrammarTest extends TestCase
{
    public function testSqliteGuardWorksWithoutAMysqlIndex(): void
    {
        DB::statement(
            'CREATE TABLE workflow_tasks (id text, workflow_run_id text, task_type text, status text, available_at text)'
        );
        DB::table('workflow_tasks')->insert([
            'id' => 'task',
            'workflow_run_id' => 'run',
            'task_type' => 'workflow',
            'status' => 'ready',
        ]);
        DB::enableQueryLog();
        app(WorkflowExecutor::class)->recoverClosedChildResolutions(new WorkflowRun([
            'id' => 'run',
            'status' => RunStatus::Waiting,
        ]));
        $queries = DB::getQueryLog();
        $this->assertCount(1, $queries);
        $this->assertStringNotContainsString('indexed by', $queries[0]['query']);
        $this->assertStringNotContainsString('order by', $queries[0]['query']);
    }

    public function testNonMysqlGuardCompilesWithPostgresGrammar(): void
    {
        $connection = DB::connection();
        $original = $connection->getQueryGrammar();
        $connection->setQueryGrammar(new PostgresGrammar($connection));
        try {
            $queries = $connection->pretend(static function (): void {
                app(WorkflowExecutor::class)->recoverClosedChildResolutions(new WorkflowRun([
                    'id' => 'run',
                    'status' => RunStatus::Waiting,
                ]));
            });
        } finally {
            $connection->setQueryGrammar($original);
        }
        $this->assertNotEmpty($queries);
        $this->assertStringContainsString('for update', $queries[0]['query']);
        $this->assertStringNotContainsString('index', $queries[0]['query']);
    }

    protected function getPackageProviders($app): array
    {
        return [WorkflowServiceProvider::class];
    }

    protected function defineEnvironment($app): void
    {
        $app['config']->set('database.default', 'sqlite');
        $app['config']->set('database.connections.sqlite.database', ':memory:');
        $app['config']->set('cache.default', 'array');
    }
}
