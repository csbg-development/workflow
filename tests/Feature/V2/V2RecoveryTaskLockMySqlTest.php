<?php

declare(strict_types=1);

namespace Tests\Feature\V2;

use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Queue;
use Orchestra\Testbench\TestCase;
use PDOException;
use ReflectionMethod;
use Tests\Fixtures\V2\TestGreetingWorkflow;
use Workflow\Providers\WorkflowServiceProvider;
use Workflow\V2\Enums\RunStatus;
use Workflow\V2\Enums\TaskStatus;
use Workflow\V2\Models\WorkflowRun;
use Workflow\V2\Support\WorkflowExecutor;
use Workflow\V2\TaskWatchdog;
use Workflow\V2\WorkflowStub;

final class V2RecoveryTaskLockMySqlTest extends TestCase
{
    protected function setUp(): void
    {
        parent::setUp();
        if (DB::connection()->getDriverName() !== 'mysql' || ! function_exists('pcntl_fork')) {
            $this->markTestSkipped('Requires isolated MySQL/InnoDB and pcntl.');
        }
        Queue::fake();
        DB::statement('SET SESSION innodb_lock_wait_timeout = 2');
    }

    public function testUnrelatedPrimaryUpdateDoesNotBlockRecoveryGuard(): void
    {
        $target = $this->newRun();
        $target->tasks()
            ->delete();
        $this->seedHistory($target);
        $other = $this->newRun();
        $task = $other->tasks()
            ->firstOrFail();
        $child = $this->lockTaskInChild($task->id, false);
        try {
            DB::transaction(static function () use ($target): void {
                $run = WorkflowRun::query()->lockForUpdate()->findOrFail($target->id);
                app(WorkflowExecutor::class)->recoverClosedChildResolutions($run);
            });
            $this->assertSame(RunStatus::Pending, $target->fresh()->status);
        } finally {
            $this->finishChild($child);
        }
    }

    public function testSameRunStatusUpdateAndRecoveryUseCompatibleLocks(): void
    {
        $run = $this->newRun();
        $task = $run->tasks()
            ->firstOrFail();
        $this->seedHistory($run);
        $child = $this->lockTaskInChild($task->id, true);
        try {
            DB::transaction(static function () use ($run): void {
                $locked = WorkflowRun::query()->lockForUpdate()->findOrFail($run->id);
                app(WorkflowExecutor::class)->recoverClosedChildResolutions($locked);
            });
        } finally {
            $this->finishChild($child);
        }
        $this->assertSame(TaskStatus::Completed, $task->fresh()->status);
    }

    public function testRepairRetriesRollbackAndDispatchesOnce(): void
    {
        $run = $this->newRun();
        $run->tasks()
            ->delete();
        Queue::fake();
        $inserts = 0;
        DB::listen(static function ($query) use (&$inserts): void {
            if (str_starts_with($query->sql, 'insert into `workflow_tasks`')) {
                ++$inserts;
                if ($inserts === 1) {
                    throw new PDOException('Deadlock found when trying to get lock; try restarting transaction', 40001);
                }
            }
        });
        $result = $this->repair($run->id);
        $this->assertNull($result['error']);
        $this->assertSame(2, $inserts);
        $this->assertSame(1, $run->tasks()->where('status', TaskStatus::Ready->value)->count());
        Queue::assertPushed(\Workflow\V2\Jobs\RunWorkflowTask::class, 1);
        $this->repair($run->id);
        $this->assertSame(1, $run->tasks()->count());
        Queue::assertPushed(\Workflow\V2\Jobs\RunWorkflowTask::class, 1);
    }

    public function testOuterRollbackDoesNotDispatchRepairedTask(): void
    {
        $run = $this->newRun();
        $run->tasks()
            ->delete();
        Queue::fake();
        DB::beginTransaction();
        $result = $this->repair($run->id);
        $this->assertNull($result['error']);
        Queue::assertNothingPushed();
        DB::rollBack();
        Queue::assertNothingPushed();
        $this->assertSame(0, $run->tasks()->count());
    }

    public function testWaitingWorkflowRemainsWaitingAcrossRepeatedRecovery(): void
    {
        $stub = WorkflowStub::make(\Tests\Fixtures\V2\TestAwaitWorkflow::class);
        $stub->start();
        $run = WorkflowRun::query()->findOrFail($stub->runId());
        $task = $run->tasks()
            ->where('status', TaskStatus::Ready->value)->firstOrFail();
        app()
            ->call([(new \Workflow\V2\Jobs\RunWorkflowTask($task->id)), 'handle']);
        $this->assertSame(RunStatus::Waiting, $run->fresh()->status);
        $run->forceFill([
            'last_progress_at' => now()->subYear(),
        ])->save();
        Queue::fake();
        $this->repair($run->id);
        $this->repair($run->id);
        $this->assertSame(RunStatus::Waiting, $run->fresh()->status);
        $this->assertSame(0, $run->tasks()->whereIn('status', ['ready', 'leased'])->count());
        Queue::assertNothingPushed();
        $continuation = $stub->attemptUpdate('approve', true);
        $this->assertTrue($continuation->accepted());
        $this->assertTrue($stub->refresh()->completed());
    }

    public function testConcurrentRepairsCreateAndDispatchOneTask(): void
    {
        $run = $this->newRun();
        $run->tasks()
            ->delete();
        Queue::fake();
        DB::disconnect();
        $sockets = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, STREAM_IPPROTO_IP);
        $pid = pcntl_fork();
        if ($pid === 0) {
            fclose($sockets[0]);
            fgets($sockets[1]);
            DB::reconnect();
            $result = $this->repair($run->id);
            fwrite($sockets[1], json_encode([
                'error' => $result['error'],
                'dispatches' => Queue::pushed(\Workflow\V2\Jobs\RunWorkflowTask::class)->count(),
            ], JSON_THROW_ON_ERROR) . "\n");
            fclose($sockets[1]);
            exit(0);
        }
        fclose($sockets[1]);
        stream_set_timeout($sockets[0], 10);
        DB::reconnect();
        DB::beginTransaction();
        WorkflowRun::query()->lockForUpdate()->findOrFail($run->id);
        fwrite($sockets[0], "go\n");
        $result = $this->repair($run->id);
        Queue::assertNothingPushed();
        DB::commit();
        $child = json_decode(fgets($sockets[0]), true, flags: JSON_THROW_ON_ERROR);
        fclose($sockets[0]);
        pcntl_waitpid($pid, $status);
        $this->assertNull($result['error']);
        $this->assertNull($child['error']);
        $this->assertSame(1, $run->tasks()->count());
        $this->assertSame(1, $child['dispatches'] + Queue::pushed(\Workflow\V2\Jobs\RunWorkflowTask::class)->count());
    }

    public function testDeadlockRetriesAreBoundedAndDoNotDispatchOnExhaustion(): void
    {
        $run = $this->newRun();
        $run->tasks()
            ->delete();
        Queue::fake();
        $inserts = 0;
        DB::listen(static function ($query) use (&$inserts): void {
            if (str_starts_with($query->sql, 'insert into `workflow_tasks`')) {
                ++$inserts;
                throw new PDOException('Deadlock found when trying to get lock; try restarting transaction', 40001);
            }
        });
        $result = $this->repair($run->id);
        $this->assertNotNull($result['error']);
        $this->assertSame(3, $inserts);
        $this->assertSame(0, $run->tasks()->count());
        Queue::assertNothingPushed();
    }

    protected function getPackageProviders($app): array
    {
        return [WorkflowServiceProvider::class];
    }

    protected function defineEnvironment($app): void
    {
        $app['config']->set('queue.default', 'database');
        $app['config']->set('cache.default', 'array');
        $app['config']->set('workflows.v2.fleet.validation_mode', 'off');
    }

    protected function defineDatabaseMigrations(): void
    {
        $this->artisan('migrate:fresh')
            ->run();
    }

    private function seedHistory(WorkflowRun $run): void
    {
        for ($batch = 0; $batch < 14; ++$batch) {
            $rows = [];
            for ($i = 0; $i < 950; ++$i) {
                $rows[] = [
                    'id' => (string) \Illuminate\Support\Str::ulid(),
                    'workflow_run_id' => $run->id,
                    'task_type' => 'workflow',
                    'status' => 'completed',
                    'available_at' => '2026-09-23 00:00:00',
                ];
            }
            DB::table('workflow_tasks')->insert($rows);
        }
        DB::select('ANALYZE TABLE workflow_tasks');
    }

    private function newRun(): WorkflowRun
    {
        $stub = WorkflowStub::make(TestGreetingWorkflow::class);
        $stub->start('Taylor');
        return WorkflowRun::query()->findOrFail($stub->runId());
    }

    private function repair(string $runId): array
    {
        return (new ReflectionMethod(TaskWatchdog::class, 'recoverMissingTask'))->invoke(null, $runId);
    }

    /**
     * @return array{pid: int, socket: resource, release: bool}
     */
    private function lockTaskInChild(string $taskId, bool $complete): array
    {
        DB::disconnect();
        $sockets = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, STREAM_IPPROTO_IP);
        $pid = pcntl_fork();
        if ($pid === 0) {
            fclose($sockets[0]);
            try {
                DB::reconnect();
                DB::beginTransaction();
                DB::table('workflow_tasks')->where('id', $taskId)->lockForUpdate()->first();
                fwrite($sockets[1], "locked\n");
                if ($complete) {
                    $parentConnection = (int) trim(fgets($sockets[1]));
                    $deadline = microtime(true) + 5;
                    do {
                        $waiting = DB::selectOne(
                            'SELECT COUNT(*) AS n FROM performance_schema.data_lock_waits w '
                            . 'JOIN performance_schema.threads t ON t.THREAD_ID=w.REQUESTING_THREAD_ID '
                            . 'WHERE t.PROCESSLIST_ID=?',
                            [$parentConnection],
                        );
                        if ((int) $waiting->n > 0) {
                            break;
                        }
                        usleep(10000);
                    } while (microtime(true) < $deadline);
                    if ((int) $waiting->n === 0) {
                        throw new \RuntimeException('Recovery never reached the expected InnoDB lock wait.');
                    }
                    DB::table('workflow_tasks')->where('id', $taskId)->update([
                        'status' => 'completed',
                    ]);
                } else {
                    fgets($sockets[1]);
                }
                DB::commit();
                fwrite($sockets[1], "ok\n");
            } catch (\Throwable $error) {
                fwrite($sockets[1], 'error:' . $error->getMessage() . "\n");
            }
            fclose($sockets[1]);
            exit(0);
        }
        fclose($sockets[1]);
        stream_set_timeout($sockets[0], 10);
        $this->assertSame("locked\n", fgets($sockets[0]));
        DB::reconnect();
        DB::statement('SET SESSION innodb_lock_wait_timeout = 8');
        if ($complete) {
            fwrite($sockets[0], DB::selectOne('SELECT CONNECTION_ID() AS id')->id . "\n");
        }
        return [
            'pid' => $pid,
            'socket' => $sockets[0],
            'release' => ! $complete,
        ];
    }

    private function finishChild(array $child): void
    {
        if ($child['release']) {
            fwrite($child['socket'], "release\n");
        }
        $result = fgets($child['socket']);
        fclose($child['socket']);
        pcntl_waitpid($child['pid'], $status);
        $this->assertSame("ok\n", $result);
    }
}
