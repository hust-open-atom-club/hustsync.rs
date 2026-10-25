# fpsync 与 lsyncd

两个工具都只接受本地目录作为源，方向由 `upstream` 决定：

| `upstream` | 方向 | 场景 |
|:--:|:--:|:--:|
| 本地绝对路径，如 NFS 挂载点 | 从 `upstream` 同步到镜像目录 | 上游已挂载到本地 |
| 远端地址，如 `rsync://edge/debian/` 或 `edge::debian` | 把镜像目录推送到 `upstream` | 主站向从站分发 |

## fpsync

先用 fpart 把目录切成多份，再并行跑多个 rsync，适合文件极多的大目录。与其他镜像一样按 `interval` 定时同步。

```toml
[[mirrors]]
name = "debian"
provider = "fpsync"
upstream = "/mnt/nfs/debian"
fpsync_options = ["-n", "8", "-f", "20000"]
```

实际执行 `fpsync -n 8 -f 20000 /mnt/nfs/debian/ <镜像目录>/`。`fpsync_options` 原样传入，常用 `-n` 并行数、`-f` 每份文件数、`-s` 每份字节数。

## lsyncd

用 inotify 监视源目录，有变化就同步，常驻运行。

```toml
[[mirrors]]
name = "debian-edge"
provider = "lsyncd"
upstream = "edge::debian"
lsyncd_options = ["-delay", "5"]
```

实际执行 `lsyncd -nodaemon -delay 5 -rsync <镜像目录>/ edge::debian/`。

| 项 | 说明 |
|:--:|:--:|
| 启动 | worker 启动后立即运行，先做一次全量同步 |
| 状态 | 运行期间一直是 `syncing` |
| 并发 | 不占 `concurrent` 的名额 |
| 超时 | 不设超时 |
| 异常退出 | 按 `retry` 立即重启；仍失败则上报，`interval` 分钟后再启动 |
| 停止 | `hustsynctl stop`、`disable`、`restart` 即时生效 |

监视的目录很多时，调大 `fs.inotify.max_user_watches`。

## 安装

```bash
apt install fpart lsyncd
```

`command` 可指定可执行文件路径。
