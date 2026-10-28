# Recovery task locking

`WorkflowExecutor::recoverClosedChildResolutions()` is called while its caller
holds the parent workflow-run lock. Keep that lock and the current locking read:
repair must not race task creation, and a consistent snapshot is not a substitute
for current task state.

On MySQL, force the existing `workflow_tasks_workflow_run_id_index` and clear the
relationship's `available_at` ordering for this existence check. This scopes the scan to the selected run and uses an index whose key does not change during a
task status update. A status update locks the task by PRIMARY before changing
its status-index entries. Reading through the mutable status index in the
opposite order can deadlock even when a covering run/type/status index makes
the scan highly selective. The parent run remains locked before task recovery.

InnoDB can still take range/gap locks; this is not a claim of lock-free execution.

No schema migration is required. The hint is applied only to MySQL. SQLite and PostgreSQL grammar checks
verify the guard compiles without it. Custom MySQL schemas must retain the canonical run-ID index name. Do not
silently fall back to the shared status index when it is missing.

This is a locking correction, not a history-scan optimization. The check still
scans retained tasks in one run in the no-active-task case. A synthetic MySQL 8.4
fixture with 13,325 target tasks and 100,000 unrelated tasks scanned 13,325 target
rows in about 22 ms in one local sample. That timing is advisory, not a runtime
budget or production prediction. A run/type/status candidate was faster but
reproduced a same-run 1213 deadlock and is intentionally excluded. History growth
and existing eager loading remain separate performance concerns.

Workflow and timer execution, and non-activity existing-task repair, also lock
run before task. Execution uses the task snapshot returned by its separate claim transaction;
existing-task repair first reads a locator snapshot. They lock the run, then
re-read and lock the task with a matching run-ID predicate. Execution also
checks leased status, lease owner and claimed attempt number after waiting. A
repaired lease or newer attempt makes the stale worker stop. This preserves
current task state and fails closed on changed lineage. The claim-only phases
lock a task without subsequently locking the run; activity recovery retains its
established attempt/execution/run/task order.

`TaskWatchdog::recoverMissingTask()` permits three transaction attempts. Repair
runs again from persisted state after rollback. Task dispatch remains outside
the retry closure; nested callers still defer dispatch through `DB::afterCommit`.
No new provider call or retry policy is introduced. Waiting workflows retain
explicit-continuation semantics, regardless of age or application activity text.

## Focused proof

Run `V2RecoveryTaskLockMySqlTest` with a disposable migrated MySQL 8.4 database,
`pcntl`, `posix`, Unix socket support, and access to Performance Schema lock-wait
metadata. Supply standard DB environment variables, including `DB_PORT=3306`;
the repository's default test environment otherwise specifies PostgreSQL's port.
The tests reset their isolated database. Never point them at application data.

The tests exercise unrelated and same-run concurrent PRIMARY updates with a
large completed-task history, actual workflow/timer/existing-repair workers
contending with recovery, two concurrent repairs, rollback/retry idempotency,
three-attempt exhaustion, old and newer replacement leases, outer rollback with no dispatch, successful dispatch
only after outer commit, and preservation of old waiting runs. Same-run contention
uses an observed InnoDB lock wait as its barrier. The retry fault is injected
after a real task INSERT so rollback and discarded dispatch callbacks are tested.

Before packaging for an embedded consumer, run focused MySQL regressions with
that consumer's Laravel/PHP/PHPUnit pins, package static/style checks and relevant
existing repair/child-resolution tests. Re-run the consumer's bounded release
gate after updating its Composer constraint and lockfile to a published fork
commit. Review backups, rollback and exact deployed revision separately. Local
source tests never establish production recovery.
