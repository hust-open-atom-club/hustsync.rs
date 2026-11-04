# Configuration

The manager and the worker each read a TOML file. Keys match tunasync, so most tunasync configs work as they are; the differences are listed at the end.

## manager

| Key | Default | Description |
|:--:|:--:|:--:|
| `debug` | none, required | Debug logs and request tracing |
| `server.addr` | none, required | Listen address |
| `server.port` | none, required | Listen port; tunasync uses 14242 |
| `server.ssl_cert` `server.ssl_key` | none, required | HTTPS when both are set |
| `files.db_type` | none, required | `redb` or `redis` |
| `files.db_file` | none, required | The redb file, or a redis URL such as `redis://host:6379/0` |
| `files.ca_cert` | none, required | CA trusted for HTTPS workers; empty uses the system roots |
| `files.status_file` | empty | Rewritten with the content of `/jobs` on every status change |

## worker

### global

| Key | Default | Description |
|:--:|:--:|:--:|
| `name` | empty | Worker name; the manager tells workers apart by it |
| `log_dir` | `/tmp/hustsync/log/{{.Name}}` | Log directory; `{{.Name}}` becomes the mirror name |
| `mirror_dir` | `/tmp/hustsync` | Root for mirror data; a mirror lives in `mirror_dir/mirror_subdir/<mirror>` |
| `concurrent` | 10 | Mirrors syncing at once |
| `interval` | 120 | Minutes between syncs |
| `retry` | 2 | Attempts per sync |
| `timeout` | 0 | Seconds per attempt; 0 means no limit |
| `exec_on_success` `exec_on_failure` | none | Commands run on the host after a sync succeeds or fails |
| `rsync_options` | none | Extra arguments for every rsync mirror |
| `dangerous_global_success_exit_codes` | none | Exit codes treated as success for every mirror |
| `dangerous_global_rsync_success_exit_codes` | none | Exit codes treated as success for every rsync mirror |

### manager

| Key | Default | Description |
|:--:|:--:|:--:|
| `api_base` | `http://localhost:14242` | Manager address |
| `api_base_list` | none | Several managers; status goes to each, and history is read from the first at start-up |
| `token` | empty | Sent when registering |
| `ca_cert` | empty | CA trusted for an HTTPS manager |

### server

| Key | Default | Description |
|:--:|:--:|:--:|
| `hostname` | `localhost` | Host name the manager uses to reach the worker |
| `listen_addr` | `127.0.0.1` | Control port address |
| `listen_port` | 6000 | Control port |
| `ssl_cert` `ssl_key` | empty | HTTPS when both are set |

### include

| Key | Description |
|:--:|:--:|
| `include_mirrors` | A glob; the `[[mirrors]]` of every matching file are appended |

## Mirrors

Each `[[mirrors]]` entry is one mirror. Unset keys take the `[global]` value; mirrors can nest `[[mirrors.mirrors]]`, and children inherit what the parent sets.

### Common

| Key | Default | Description |
|:--:|:--:|:--:|
| `name` | required | Mirror name |
| `provider` | `rsync` | `rsync`, `two-stage-rsync` or `command` |
| `upstream` | empty | Upstream address; rsync providers need a trailing `/` |
| `interval` `retry` `timeout` | from global | As in global |
| `mirror_dir` | none | The mirror directory itself; `mirror_subdir` is ignored |
| `mirror_subdir` | none | The level between `mirror_dir` and the mirror name |
| `log_dir` | from global | As in global |
| `role` | `master` | `slave` reports the mirror as a secondary |
| `env` | none | Extra environment for the sync process |
| `exec_on_success` `exec_on_failure` | from global | Replace the global setting |
| `exec_on_success_extra` `exec_on_failure_extra` | none | Appended after the global setting |
| `success_exit_codes` | none | Extra exit codes treated as success |

The sync process sees `HUSTSYNC_MIRROR_NAME`, `HUSTSYNC_WORKING_DIR`, `HUSTSYNC_UPSTREAM_URL`, `HUSTSYNC_LOG_DIR` and `HUSTSYNC_LOG_FILE`, plus `TUNASYNC_*` with the same values.

### rsync and two-stage-rsync

| Key | Default | Description |
|:--:|:--:|:--:|
| `command` | `rsync` | rsync executable |
| `username` `password` | none | Written to `USER` and `RSYNC_PASSWORD` |
| `exclude_file` | none | File for `--exclude-from` |
| `rsync_options` | none | Extra arguments; for two-stage, the second stage only |
| `rsync_override` | none | Replaces the default arguments; rsync only |
| `rsync_override_only` | false | Uses `rsync_override` alone; rsync only |
| `rsync_timeout` | 120 | Seconds for `--timeout` |
| `rsync_no_timeout` | false | Omits `--timeout` |
| `use_ipv6` `use_ipv4` | false | Adds `-6` or `-4` |
| `rsync_success_exit_codes` | none | Extra rsync exit codes treated as success |
| `stage1_profile` | `debian` | Excludes for the first stage of two-stage: `debian` or `debian-oldstyle` |

### command

| Key | Description |
|:--:|:--:|
| `command` | Split by shell rules and run directly, without a shell |
| `fail_on_match` | A match of this regex in the log fails the run |
| `size_pattern` | Regex for the mirror size in the log; the first group of the last match |

## Differences from tunasync

| Item | tunasync | hustsync |
|:--:|:--:|:--:|
| Database | bolt, badger, leveldb, redis | redb, redis |
| Unknown keys | Ignored | Rejected, so a misspelt key cannot silently do nothing |
| Environment | `TUNASYNC_*` | `HUSTSYNC_*`, plus `TUNASYNC_*` with the same values |
