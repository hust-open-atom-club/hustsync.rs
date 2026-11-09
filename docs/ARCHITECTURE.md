# Architecture and Implementation Constraints

Status labels are defined in the [documentation index](README.md). Implementation descriptions below are derived from the current source; constraints are review requirements for subsequent changes. Protocol fields and compatibility scope belong in the [compatibility contracts](COMPATIBILITY.md).

## Crate Dependency Boundaries

This table is the authoritative boundary for dependencies within the workspace. Third-party libraries are outside this allowlist, but additions still need a purpose and an assessment of platform impact.

| Crate | Responsibility | Allowed normal dependencies within the workspace |
| --- | --- | --- |
| `hustsync-config-parser` | TOML parsing, includes, inheritance expansion, semantic validation | None |
| `hustsync-internal` | Wire types, status conversion, logging, and HTTP/path utilities | None |
| `hustsync-manager` | HTTP control plane, registration, persistence, and status file | config-parser, internal |
| `hustsync-worker` | Local scheduling, actors, Providers, Hooks, and reporting | config-parser, internal |
| `hustsync` | Daemon CLI, configuration overrides, signals, and component startup | config-parser, internal, manager, worker |
| `hustsynctl` | Operator CLI, HTTP client, and output | internal |

Short names in the table have the `hustsync-` prefix. Manager and Worker communicate through the protocol in production and must not depend on each other's implementations. Foundation crates must not depend on services or binaries. The control CLI must not read the Manager database directly.

**Dependency constraint:** dev-dependencies may follow the same row's normal dependency boundary. The sole additional exception is Manager → Worker for in-process integration tests; this exception must not introduce production coupling. The six current crates have no build-dependencies. Internal workspace build edges are prohibited, but justified third-party build tools are not. These rules also apply to optional dependencies, renamed dependencies, and all target conditions; being inactive on the host platform is not an exception. Every member must use `[lints] workspace = true`. When adding a member or changing a boundary, update this table, the checker, and its tests, and explain the ownership change.

Automation checks only manifest edges and lint inheritance that can be determined mechanically. Module semantics, indirect coupling, and process behavior still require review; see the [contribution guide](../CONTRIBUTING.md).

## Ownership and State

| Owner | Owned facts | What they do not establish |
| --- | --- | --- |
| [Manager](../crates/hustsync-manager/src/server.rs) and [adapter](../crates/hustsync-manager/src/database/mod.rs) | Worker registration and persisted `MirrorStatus`; redb by default, Redis when configured | Not a live process table; does not dispatch scheduled mirror jobs |
| [Worker](../crates/hustsync-worker/src/lib.rs) | Jobs/configuration snapshots, actor JoinSet, global semaphore, local ScheduleQueue, exit token, and message relay | Does not own the Manager database; broadcasts to multiple Managers are neither consensus nor automatic failover |
| [JobActor / MirrorJob](../crates/hustsync-worker/src/job.rs) | Actor owns command reception, Provider, Hooks, and execution task; handle shares the sender, atomic state, disabled notification, and interval | `STATE_*` is not wire `SyncStatus`; `READY` includes both waiting for a permit and executing |
| [Provider](../crates/hustsync-worker/src/provider/mod.rs) | A single execution, argv/env, child process, exit classification, logs, and size extraction | Does not schedule the next run or retry the whole job independently |
| [Hook](../crates/hustsync-worker/src/hooks/mod.rs) | Execution-phase directory preparation, logging, and success/failure commands | Does not own the schedule queue or Manager state |
| [hustsync](../crates/hustsync/src/main.rs) / [hustsynctl](../crates/hustsynctl/src/main.rs) | The former owns configuration entry points, signals, and the reload controller; the latter owns operator arguments and output | HTTP success does not automatically mean a child has exited or reload has been applied |

Review four kinds of state separately: persisted state belongs to the Manager adapter; runtime state is the actor's `NONE/READY/PAUSED/DISABLED/HALTING`; scheduling state belongs to the Worker's time queue (UTC for reporting, Tokio Instant for due-time checks); protocol state consists of `SyncStatus` and timestamp fields. These are not one shared state representation.

[Manager handlers](../crates/hustsync-manager/src/handlers.rs) merge reports: PreSyncing updates/preserves the start time, Success updates the successful-update time, Success/Failed updates the end time, and an empty or unknown size may retain the previous value. Schedule reports only update `next_scheduled` on existing rows; missing rows produce errors in the log. Omitting a mirror is not a deletion command. `WebMirrorStatus` is a projection for display and the status file, not a replacement for the raw `MirrorStatus` used by Worker recovery.

## Commands, Execution, and Reporting

```text
hustsynctl → Manager POST /cmd → look up worker registration → Worker POST /
    → remove the job's queue entry → MirrorJob channel → JobActor
    → concurrency permit → hooks → Provider child process → JobMessage
    → Worker relay → Manager persistence / status file
                   → Worker schedules the next run and reports a schedule snapshot
```

Before forwarding Stop/Disable, Manager makes a best-effort write of Paused/Disabled. That state may remain even if forwarding fails; this is not an atomic cross-service transaction. Worker HTTP Stop returns after enqueueing the command. Disable waits for the actor notification when dispatched from a non-Disabled state. If the state is already Disabled, it returns 200 immediately, and the actor writes Disabled before terminating and waiting for the process. A repeated Disable's successful response therefore does not prove that the previous process has been reaped. Reload only acknowledges enqueueing to the controller; duplicate requests may be coalesced. Do not conflate these completion boundaries.

**Execution constraint:** when changing commands, review CLI encoding, Manager forwarding/prewrites, Worker queue operations, actors, and result reporting together. Use isolated tests to distinguish “accepted,” “process exited,” and “persisted”; one 2xx assertion cannot stand in for all three. Ping currently also removes a queue entry; see the open items below.

## Concurrency, Retries, and Cleanup

A normal execution acquires an owned permit at the start of `run_sync_loop`. The permit covers hooks, all retries, and JobMessage sends, and is released when the future returns; it does not wait for Manager HTTP completion. ForceStart bypasses the semaphore. The actor owns the retry loop, with its count taken from Provider configuration and no backoff delay in the loop. The interval is the next periodic scheduling interval, not retry backoff. The actor supplies a zero-based `RunContext.attempt` to the Provider; HookCtx uses one-based attempts.

During execution, Stop/Disable/Halt first change runtime state, cancel the run's token, and then await the running JoinHandle. Disable subsequently notifies HTTP waiters but does not exit the actor; Halt does. Restart terminates and awaits the old execution before starting a new one. Worker shutdown sends Halt and joins actors, with no overall deadline. Background HTTP, relay, and ticker tasks are not all part of the actor JoinSet, so joining actors does not mean every background task has drained.

Unix Providers create process groups. On cancellation or timeout, the shared wait helper sends TERM, waits before sending KILL, and then calls `child.wait()`. Do not generalize this helper path into a guarantee for all exit paths: the two-stage outer timeout and shell Hooks have separate paths. JobActor gives each run its own RunContext token and cancels it on Stop, Disable, Restart, and Halt, including while the run waits for a permit.

Pre-hooks run in chain order; post-hooks run in reverse order. A pre-hook failure aborts the pipeline, while post-hook errors are logged without changing the already determined Provider result. The built-in chain is WorkingDir, LogLimit, and optional ExecPost. ExecPost has a 30-second waiting budget per command, which does not establish that a timed-out process has been reaped.

**Lifecycle constraint:** when changing permits, cancellation, processes, or Hooks, state who owns the Child/JoinHandle, who awaits exit, and when the permit is released. Cover cancellation while queued, stopping during retries, child/descendant process exit, and Hook failure. Keep only necessary state operations under locks; extract snapshots and release locks before network or process waits. If a wait must occur while holding a lock, explain lock ordering and wait boundaries. Detaching, ignoring errors, or cancelling a future must not be presented as completed cleanup. Existing gaps below are not fixed merely by introducing this constraint.

## Startup, Reload, and Failure

Startup currently registers with finite retries per address, reads job status from the first Manager, builds the local queue, reports a snapshot, and starts control HTTP/relay/ticker tasks. Recovered Disabled/Paused jobs stay out of the queue; other recovered jobs are queued at `last_update + interval`. Unknown jobs are queued immediately. Worker does not fetch an executable schedule queue from Manager.

Registration failure is logged and startup continues. A failed status read becomes an empty status list, potentially scheduling every local job immediately. When Manager configuration and an HTTP client exist, a failed report does not prevent rescheduling the job when a message has its `schedule` flag set. Network requests precede rescheduling, however, and may delay it. Without Manager configuration or a client, the relay skips messages entirely, including rescheduling. There is no durable outbox or verified guarantee of eventual report delivery.

For reload, the daemon reloads and validates the file; failure preserves the running configuration. On success, it passes only mirrors to Worker. Unchanged mirrors keep their actors, new mirrors are queued immediately, removed mirrors receive Disable and leave the map, and changed mirrors are rebuilt. New Providers still use the original Worker global configuration. Disabled mirrors stay unscheduled; rebuilt paused mirrors are initially marked Paused but queued immediately. Do not promise hot reload of all configuration or atomic replacement.

**Recovery constraint:** when changing these paths, enumerate branches for an unreachable Manager, unknown records, paused/disabled recovery, and unchanged/changed/added/removed mirrors during reload. Configuration loading, validation, and Provider default merging have separate responsibilities; CLI arguments should not become implicit reads of process environment in the parser. Preserve existing typed errors and context where possible rather than using display strings as a cross-layer error protocol.

## Open Behavior Questions and Impact

These risks were identified by reading the source. They were not reproduced at runtime or repaired during this governance work. Before changing these paths, establish behavioral evidence and decide the intended behavior; do not rewrite a risk as a guarantee.

| Evidence location | Open issue and potential impact |
| --- | --- |
| `lib.rs::disable_and_remove_job` / reload | Only sends Disable, without waiting for the old actor to finish or clearing old queue entries. Removal/rebuild may leave stale scheduling and a window for overlapping execution. |
| `server.rs::handle_cmd` | Ping and rejected job commands remove scheduling before command dispatch branches are evaluated; a liveness check can change the execution plan. |
| `provider/two_stage_rsync_provider.rs::run`, `hooks/exec.rs::spawn_shell` | Outer timeout drops the waiting future. Explicit kill/wait coverage and descendant cleanup need focused validation. |
| `lib.rs::start_message_relay` | No reliable redelivery; product expectations for Manager failure, queue backpressure, and rescheduling without a client need to be established. |

Relative source paths in this table are under `crates/hustsync-worker/src/`. Storage transaction and key-format details remain in adapter source. A new backend must demonstrate the same HTTP contract, reads after restart, and failure semantics; implementing the trait alone does not establish interchangeability. Platform and deployment evidence belongs in the compatibility contracts.
