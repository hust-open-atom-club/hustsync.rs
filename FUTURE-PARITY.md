# FUTURE-PARITY.md

Items deferred past v0.1. Each entry names the Go feature, the blocking
reason, and a rough cost signal. When an item becomes work-in-progress,
move it into the relevant sub-doc section and open a tracking issue.

## Format

```
### <item name>
Go behaviour: ...
Blocked by: ...
Cost signal: S / M / L
```

---

## Config / parse layer

### `api_base_list` HA failover
Go behaviour: `[manager] api_base_list = ["http://m1:14242", "http://m2:14242"]`
distributes registration and status reporting across multiple managers.
Blocked by: multi-manager routing design (round-robin vs. sequential vs. failover);
currently field is **rejected at parse** — operators must remove it before migrating.
Cost signal: M

### `dangerous_global_rsync_success_exit_codes` / `rsync_success_exit_codes`
Go behaviour: global and per-mirror allowlists of rsync exit codes that
are treated as success. Currently field is rejected at parse.
Blocked by: exit-code plumbing through ProviderError variants.
Cost signal: S

### `status_file` (legacy JSON export)
Go behaviour: manager writes a snapshot JSON file at the path given by
`files.status_file` on every status change. Used by simple dashboards
that `cat` the file.
Blocked by: low operator demand; redb snapshot or a dedicated
`GET /status` endpoint is a better solution.
Cost signal: S

## Worker features

### Persistent heartbeat / re-registration
Go behaviour: none (Go also only registers once). However operators have
requested periodic re-registration to handle manager restarts without
restarting workers.
Blocked by: interval design (how often? back-off?).
Cost signal: S

### SIGHUP reload
Go behaviour: worker re-reads config and applies diff via `ReloadMirrorConfig`.
Rust: SIGHUP is ignored; `reload` command returns 200 but is a no-op.
Blocked by: config hot-reload design and per-job state reconciliation.
Cost signal: M

### `include_mirrors` directive
Go behaviour: `[include] include_mirrors = "/etc/tunasync/mirrors/*.conf"` globs
additional mirror config fragments.
Rust: field is accepted but skipped with a warning at v0.1.
Blocked by: glob + merge logic.
Cost signal: S

## Hooks / providers (Track B)

### Cgroup v1/v2 hook
Go behaviour: wraps syncing process in a cgroup for CPU/memory accounting.
Blocked by: Linux-only; requires unsafe cgroup fd operations; design work.
Cost signal: L

### Docker hook
Go behaviour: runs providers inside Docker containers.
Blocked by: Docker API integration.
Cost signal: L

### ZFS snapshot hook
Go behaviour: creates ZFS snapshot before/after sync.
Blocked by: Linux-only; ZFS userspace bindings.
Cost signal: M

### Btrfs snapshot hook
Go behaviour: creates Btrfs snapshot before/after sync.
Blocked by: Linux-only.
Cost signal: M

### Memory limit (`memory_limit` per mirror)
Go behaviour: sets cgroup memory limit for the syncing process.
Blocked by: depends on Cgroup hook above.
Cost signal: S (once Cgroup is done)

## Storage

### BoltDB / BadgerDB / LevelDB / Redis backends
Go behaviour: multiple DB backends selectable via `db_type`.
Rust: only redb. No in-place migration tool.
Blocked by: low migration demand; redb covers all v0.1 use cases.
Cost signal: L (per backend)

## Network / Auth

### mTLS client authentication (manager→worker)
Go behaviour: partial; Rust: not implemented.
Blocked by: TLS client-cert design.
Cost signal: M

### Bearer-token verification (worker→manager)
Go behaviour: token field exists but not enforced.
Rust: same — stored at register but never checked.
Blocked by: auth design (token rotation, revocation).
Cost signal: M

## Observability

### Web status page
Go behaviour: built-in HTTP status page served by the manager.
Blocked by: frontend work.
Cost signal: M

### Log compression
Go behaviour: not implemented in Go either (noted for completeness).
Blocked by: not a parity item.
Cost signal: S
