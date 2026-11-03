<?php

declare(strict_types=1);

namespace Tests\Unit\V2;

use Illuminate\Support\Facades\DB;
use PDOException;
use ReflectionClass;
use RuntimeException;
use Tests\NonDatabaseTestCase;
use Throwable;
use Workflow\V2\Jobs\RunWorkflowTask;
use Workflow\V2\Models\WorkflowTask;
use Workflow\V2\Support\WorkflowExecutor;

final class RunWorkflowTaskFailureTest extends NonDatabaseTestCase
{
    public function testFailurePersistenceCannotReplaceOriginalException(): void
    {
        $original = new RuntimeException('execution failed');
        $this->expectExecutionFailure($original);
        DB::shouldReceive('transaction')->once()->ordered()
            ->andThrow(new PDOException('There is already an active transaction'));

        $this->assertOriginalIsThrown($original);
    }

    public function testOriginalIsRethrownAfterFailurePersistenceSucceeds(): void
    {
        $original = new RuntimeException('execution failed');
        $this->expectExecutionFailure($original);
        DB::shouldReceive('transaction')->once()->ordered()->andReturn(null);

        $this->assertOriginalIsThrown($original);
    }

    public function testFailurePersistenceErrorCannotReplaceAnOriginalError(): void
    {
        $original = new \Error('execution error');
        $this->expectExecutionFailure($original);
        DB::shouldReceive('transaction')->once()->ordered()
            ->andThrow(new RuntimeException('projection failed'));

        $this->assertOriginalIsThrown($original);
    }

    private function expectExecutionFailure(Throwable $original): void
    {
        DB::shouldReceive('transaction')->once()->ordered()
            ->andReturn(new WorkflowTask([
                'id' => 'task-1',
                'workflow_run_id' => 'run-1',
            ]));
        DB::shouldReceive('transaction')->once()->ordered()->andThrow($original);
    }

    private function assertOriginalIsThrown(Throwable $original): void
    {
        $executor = (new ReflectionClass(WorkflowExecutor::class))->newInstanceWithoutConstructor();
        try {
            (new RunWorkflowTask('task-1'))->handle($executor);
            $this->fail('Expected the original execution exception.');
        } catch (Throwable $caught) {
            $this->assertSame($original, $caught);
        }
    }
}
