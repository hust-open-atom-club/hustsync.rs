# ftpsync

Debian 官方的镜像脚本，按 Debian 要求的顺序分两阶段同步，并维护 `project/trace`。hustsync 把它包装成一个 provider。

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

| 项 | 说明 |
|:--:|:--:|
| `upstream` | `rsync://主机/路径/` 或 `主机::路径` |
| `username` `password` | 写入 `RSYNC_USER` 与环境变量 `RSYNC_PASSWORD`，口令不落盘 |
| `ftpsync_conf` | 追加到生成配置末尾的行，可覆盖前面的值；变量见 `man ftpsync.conf` |
| `command` | ftpsync 的路径，默认 `ftpsync` |

## 运行方式

加载镜像时生成 `<log_dir>/.ftpsync/.config/ftpsync/ftpsync-<镜像名>.conf`，写入 `TO`、`RSYNC_HOST`、`RSYNC_PATH`、`LOGDIR`、`LOCKDIR`、`BASEDIR`，再以 `<log_dir>/.ftpsync` 作 `HOME` 执行：

```bash
ftpsync sync:archive:<镜像名>
```

ftpsync 自己的日志 `ftpsync-<镜像名>.log` 与 rsync 日志写在镜像的 `log_dir` 下。

## 安装

```bash
apt install ftpsync
```
