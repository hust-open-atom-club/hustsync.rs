# fpsync

fpsync takes only a local directory as its source; `upstream` sets the direction:

| `upstream` | Direction | Use |
|:--:|:--:|:--:|
| A local absolute path, such as an NFS mount | From `upstream` into the mirror directory | The upstream is mounted locally |
| A remote address, such as `rsync://edge/debian/` or `edge::debian` | Pushes the mirror directory to `upstream` | A primary distributes to secondaries |

fpart splits the tree into parts and several rsync jobs run in parallel, which suits large trees with many files. It syncs on `interval` like any other mirror.

```toml
[[mirrors]]
name = "debian"
provider = "fpsync"
upstream = "/mnt/nfs/debian"
fpsync_options = ["-n", "8", "-f", "20000"]
```

This runs `fpsync -n 8 -f 20000 /mnt/nfs/debian/ <mirror dir>/`. `fpsync_options` are passed as written; common ones are `-n` for parallel jobs, `-f` for files per part and `-s` for bytes per part.

## Installing

```bash
apt install fpart
```

`command` sets the path to the executable.
