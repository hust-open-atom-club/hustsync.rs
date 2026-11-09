# Resource isolation

Each mirror syncs in its own cgroup: memory is capped, leftover processes are killed when the sync ends, and exceeding the cap is reported as an OOM. cgroup v1 and v2 are both supported and detected automatically.

## Configuration

```toml
[cgroup]
enable = true
base_path = "/sys/fs/cgroup"
group = ""

[[mirrors]]
name = "debian"
memory_limit = "512M"
```

| Aspect | Detail |
|:--:|:--:|
| Group name | The mirror name, under the parent group; a group of the same name left by a crash is removed first |
| Joining | The child joins before `exec`, so every process it starts stays in the group |
| Memory | `memory.max` on v2, `memory.limit_in_bytes` on v1; swap is disabled, so exceeding the cap triggers the OOM killer |
| Cleanup | Kills leftover processes, up to three rounds, then removes the group |
| OOM | When the process fails and the group's OOM count is above 0, the run reports `killed by the OOM killer, memory_limit exceeded` |

`memory_limit` takes `256M`, `1.5G` or `512MiB`, in powers of 1024.

## Parent group

With `group` empty, the worker uses its own group and first moves itself into a `__worker` child, as a v2 group with children cannot hold processes. With `group` set, that group must exist and hold no processes.

## v2: delegate through systemd

```ini
# /etc/systemd/system/hustsync-worker.service.d/cgroup.conf
[Service]
Delegate=yes
```

Leave `group` empty. When the worker runs as an ordinary user, systemd hands it the service's group.

## v1

Create the group and give it to the worker user:

```bash
cgcreate -t hustsync -a hustsync -g memory:hustsync
```

Set `group = "hustsync"`.

## Tests

The test binary runs as root; build as an ordinary user:

```bash
CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_RUNNER='sudo -E'   cargo test -p hustsync-worker --test cgroup_root -- --ignored --test-threads=1
```

v1 is tested in qemu; the script downloads Ubuntu 24.04's kernel:

```bash
scripts/cgroup-v1-qemu.sh
```
