# ftpsync

Debian's official mirror script: it syncs in two stages, in the order Debian requires, and maintains `project/trace`. hustsync wraps it as a provider.

```toml
[[mirrors]]
name = "debian"
provider = "ftpsync"
upstream = "rsync://ftp.debian.org/debian/"
cron = "0 */6 * * *"
ftpsync_conf = [
    "MIRRORNAME='mirrors.example.edu.cn'",
    "ARCH_EXCLUDE='source'",
]
```

| Key | Description |
|:--:|:--:|
| `upstream` | `rsync://host/path/` or `host::path` |
| `username` `password` | Written to `RSYNC_USER` and the `RSYNC_PASSWORD` variable; the password never reaches disk |
| `ftpsync_conf` | Lines appended to the generated config, overriding earlier values; see `man ftpsync.conf` |
| `command` | Path to ftpsync, `ftpsync` by default |

## How it runs

Loading the mirror writes `<log_dir>/.ftpsync/.config/ftpsync/ftpsync-<mirror>.conf` with `TO`, `RSYNC_HOST`, `RSYNC_PATH`, `LOGDIR`, `LOCKDIR` and `BASEDIR`, then runs, with `<log_dir>/.ftpsync` as `HOME`:

```bash
ftpsync sync:archive:<mirror>
```

ftpsync's own log, `ftpsync-<mirror>.log`, and the rsync logs go in the mirror's `log_dir`.

## Installing

```bash
apt install ftpsync
```
