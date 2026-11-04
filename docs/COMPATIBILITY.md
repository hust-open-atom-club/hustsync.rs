# Compatibility Contracts

## Goal and Baseline

The compatibility goal is tunasync's Manager/Worker operating model. **A traceable upstream tag/commit baseline has not been established, and no complete compatibility certification exists for a specific upstream version.** This repository's `0.1.0` version, historical commit titles, Go file/line references in comments, and tests named “parity” do not substitute for an upstream revision.

This document starts from a static review of the repository revision listed in the [documentation index](README.md). To establish an upstream baseline, record the upstream repository, full commit/tag, inspected files, fixture capture/generation method, and decisions about differences, then compare both implementations with identical inputs. Do not choose a version and claim existing tests have already validated it.

## External Contracts and Current Scope

| Interface | Current implementation and review boundary | Evidence entry point |
| --- | --- | --- |
| HTTP | Manager `/ping`, `/jobs`, `/jobs/disabled`, `/workers`, `/cmd`, `/metrics`, and worker subroutes; Worker `POST /` and `GET /metrics`. Methods, status codes, error envelopes, empty values, timeouts, and command completion boundaries are all contracts. | [Manager routes](../crates/hustsync-manager/src/server.rs), [handlers](../crates/hustsync-manager/src/handlers.rs), [Worker HTTP](../crates/hustsync-worker/src/server.rs) |
| JSON | MirrorStatus and related struct fields use snake_case; Rust `next_scheduled` maps to `next_schedule`. SyncStatus values use kebab-case, such as `pre-syncing`. Command args/options accept null as their default empty collections. | [msg](../crates/hustsync-internal/src/msg.rs), [status](../crates/hustsync-internal/src/status.rs), [roundtrip](../crates/hustsync-internal/tests/roundtrip.rs) |
| Status display | The Worker recovery endpoint returns raw MirrorStatus/RFC3339 timestamps. `/jobs` and the status file use WebMirrorStatus, with text timestamps and `*_ts` values in seconds. | [status_web](../crates/hustsync-internal/src/status_web.rs), [status file tests](../crates/hustsync-manager/tests/contract_status_file.rs) |
| TOML | Daemon Worker startup uses strict `load_worker_config` plus `validate_worker_config`. Include globs resolve against process cwd; fragments are appended before nested mirrors are flattened. Generic parsing neither performs that expansion nor applies the leniency option. | [parser](../crates/hustsync-config-parser/src/lib.rs), [configuration tests](../crates/hustsync-config-parser/tests/) |
| CLI | Daemon `manager/m`, `worker/w`; ctl list/workers/flush/rm-worker/set-size/start/stop/disable/restart/reload/ping. Argument defaults, exit codes, output, and configuration precedence are also contracts; HTTP tests alone are insufficient. | [daemon](../crates/hustsync/src/main.rs), [ctl](../crates/hustsynctl/src/main.rs) |
| Environment variables | Standard Provider variables are `HUSTSYNC_{MIRROR_NAME,WORKING_DIR,UPSTREAM_URL,LOG_DIR,LOG_FILE}` and legacy `TUNASYNC_*` aliases with matching values. Layers override in order: defaults → mirror env → Hook env. HUSTSYNC wins conflicts within a layer. Hooks also expose `JOB_EXIT_STATUS`, with `success`/`failure` values. | [Provider env](../crates/hustsync-worker/src/provider/mod.rs), [Exec Hook](../crates/hustsync-worker/src/hooks/exec.rs) |
| Providers | `rsync`, `two-stage-rsync`, and `command`. Argv ordering, deletion flags, credentials/env, log parsing, additional successful exit codes, timeouts, and termination are observable behavior. Command uses shlex splitting and direct execution, not an implicit shell; Exec Hooks use `sh -c`. | [Provider implementations](../crates/hustsync-worker/src/provider/), [Provider tests](../crates/hustsync-worker/tests/) |

The ctl configuration order is built-in defaults, `/etc/hustsync/ctl.conf`, `$HOME/.config/hustsync/ctl.conf`, explicit `--config`, then command-line overrides. Implicit file failures are ignored; explicit file failures return an error. Daemon Manager arguments override the configuration file. Worker reload scope is defined in [Architecture](ARCHITECTURE.md); “parsed” does not mean “applied to the running process.”

## Intentional Differences, Missing Features, and Unverified Claims

- ctl `list --format` uses Tera with simple replacement of Go-style field prefixes. This does not establish Go template syntax compatibility; template syntax and output require separate comparison.
- This project uses a Rust workspace and redb as its default storage. A Redis adapter exists; `db_file` is a connection URL for Redis. There is no evidence of Go database-file migration or storage binary compatibility.
- `HUSTSYNC_*` is the project's canonical naming, with `TUNASYNC_*` aliases retained. This is an explicit local naming choice. `api_base_list` is a local configuration entry point with reads from the first address and broadcast writes, not a validated HA protocol.
- ZFS hooks and Btrfs snapshot hooks are unimplemented. Strict loading does not accept arbitrary unknown fields; the library's leniency option does not imply a corresponding daemon CLI flag.
- Daemon pidfile arguments exist, but writing pidfiles is not implemented. Help text alone does not establish process-management capabilities. Configuration templates are not systemd/container deployment solutions, and the test Worker example accesses a real rsync source; it is not an offline test command.
- The token field is stored and redacted in list output, but current HTTP paths do not implement token authentication. A token in configuration is not proof of authentication. TLS implementation/tests do not establish that a production deployment has been validated for security.
- There is no validated platform support matrix. Unix signals, process groups, `sh`, symbolic links, and external rsync are material dependencies. Windows appearing in the Clippy workflow proves neither compilation nor runtime support. Calls to Unix-only functions in the shared process helper also need focused inspection.
- Evidence gaps in stopping, reload, Manager failure, and child cleanup are tracked in [Architecture's open items](ARCHITECTURE.md). This documentation work neither repairs nor endorses those behaviors.

## Fixture Provenance and Evidence Limits

These fixtures are **regression expectations committed in this repository**. There is currently no upstream export tool, capture manifest tied to a Go revision, or reproducible upstream generation record. Historical comments claiming “verified against Go” are leads to recheck, not proof.

| Data | Traceable origin in this repository | Evidence limit |
| --- | --- | --- |
| [Internal JSON](../crates/hustsync-internal/tests/fixtures/) | Introduced in `5114de7`; MirrorStatus snake_case adjustment in `b7f59d6` | Round-trips pin current Rust JSON structure; they do not independently prove service interoperability. |
| [Manager HTTP](../crates/hustsync-manager/tests/fixtures/http/) | Harness `a391291`, GET `6365969`, worker `bd772c0`, cmd `a95e5d8` | Router/HTTP regression; dynamic values such as timestamps are masked according to harness rules. Does not prove CLI arguments/output. |
| [Worker argv and fake processes](../crates/hustsync-worker/tests/fixtures/) | Argv/log `226d46c`, two-stage `9d93a10` | Arguments and fake-process stage/lifecycle tests, not acceptance against an actual rsync service or production mirror. |

The [in-process integration test](../crates/hustsync-manager/tests/e2e_inproc.rs) starts Manager/Worker components and sends commands through an HTTP client; it does not launch the real `hustsynctl`. An e2e test name does not expand its coverage. The commits above are local repository history, **not a tunasync baseline**. Test existence does not mean it passed on the current revision; report execution under the [contribution guide](../CONTRIBUTING.md).

## Coordinated Changes to Contracts

When adding, removing, or changing behavior in any external interface above, include the following in the same change:

1. Describe old/new inputs and outputs and affected callers. Distinguish compatibility repairs, intentional differences, and undecided behavior.
2. Update the actual behavior owner and consumers, checking shared types, defaults/validation, error codes, persistence/recovery, and platform branches.
3. Update this document, affected architecture sections, README/configuration examples, and source API comments.
4. Add or update tests that distinguish old and new behavior. Explain the origin and reason for each fixture change; do not change assertions to conceal contradictions.
5. Report the revision, commands, and environment actually validated. Compatibility claims additionally require an upstream baseline and comparison results; state what remains incomplete.
