# fpsync and lsyncd

Both tools take only a local directory as their source; `upstream` sets the direction:

| `upstream` | Direction | Use |
|:--:|:--:|:--:|
| A local absolute path, such as an NFS mount | From `upstream` into the mirror directory | The upstream is mounted locally |
| A remote address, such as `rsync://edge/debian/` or `edge::debian` | Pushes the mirror directory to `upstream` | A primary distributes to secondaries |

## fpsync

fpart splits the tree into parts and several rsync jobs run in parallel, which suits large trees with many files. It syncs on `interval` like any other mirror.

```toml
[[mirrors]]
name = "debian"
provider = "fpsync"
upstream = "/mnt/nfs/debian"
fpsync_options = ["-n", "8", "-f", "20000"]
```

This runs `fpsync -n 8 -f 20000 /mnt/nfs/debian/ <mirror dir>/`. `fpsync_options` are passed as written; common ones are `-n` for parallel jobs, `-f` for files per part and `-s` for bytes per part.

## lsyncd

Watches the source with inotify and syncs on every change; it runs continuously.

```toml
[[mirrors]]
name = "debian-edge"
provider = "lsyncd"
upstream = "edge::debian"
lsyncd_options = ["-delay", "5"]
```

This runs `lsyncd -nodaemon -delay 5 -rsync <mirror dir>/ edge::debian/`.

| Aspect | Detail |
|:--:|:--:|
| Start | Runs as soon as the worker starts, beginning with a full sync |
| Status | `syncing` for as long as it runs |
| Concurrency | Takes no slot from `concurrent` |
| Timeout | None |
| Unexpected exit | Restarts at once, up to `retry` times; if it still fails, the failure is reported and it starts again after `interval` minutes |
| Stop | `hustsynctl stop`, `disable` and `restart` take effect at once |

With many watched directories, raise `fs.inotify.max_user_watches`.

## Installing

```bash
apt install fpart lsyncd
```

`command` sets the path to the executable.
