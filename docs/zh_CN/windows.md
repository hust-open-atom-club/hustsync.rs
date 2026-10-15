# Windows

manager、worker 与 `hustsynctl` 都能在 Windows 上编译运行，同步任务用原生可执行文件。

| 项 | Windows 上的行为 |
|:--:|:--:|
| `command` 镜像 | 直接执行 `.exe`；脚本写成 `cmd /C ...` 或 `powershell -Command ...` |
| `exec_on_success` `exec_on_failure` | 经 `cmd /C` 执行，变量写作 `%HUSTSYNC_MIRROR_NAME%` |
| 停止与超时 | 结束整棵进程树 |
| `memory_limit` | `[cgroup] enable = true` 时生效，每个任务一个 Job Object；超限的分配失败，该次同步记为内存不足 |
| worker 被强行结束 | 开启 `[cgroup]` 时，任务进程随之结束 |
| 日志 `latest` | 优先建符号链接，没有权限时改为硬链接 |
| 热重载 | `hustsynctl reload -w <worker>` |

Windows 上 `[cgroup]` 只读 `enable`。`fpsync`、`lsyncd`、`ftpsync` 依赖 Linux，不可用。

## rsync

用 Cygwin 或 MSYS2 构建的 rsync，例如 MSYS2 的 `pacman -S rsync`，路径写在镜像的 `command` 里：

```toml
[[mirrors]]
name = "debian"
provider = "rsync"
command = 'C:\msys64\usr\bin\rsync.exe'
upstream = "rsync://mirrors.example.org/debian/"
```

`mirror_dir` 与本地 `upstream` 照常写 Windows 路径，worker 交给 rsync 时转成 `/proc/cygdrive/c/...`。

## 以服务运行

`--service` 让 manager 或 worker 由服务控制管理器启停，`--log-file` 把日志追加到文件。服务的工作目录是 `C:\Windows\System32`，配置与日志都写绝对路径。

```powershell
New-Service hustsync-worker -StartupType Automatic -BinaryPathName 'C:\hustsync\hustsync.exe worker -c C:\hustsync\worker.conf --service --log-file C:\hustsync\worker.log'
sc.exe failure hustsync-worker reset= 86400 actions= restart/5000
Start-Service hustsync-worker
```

停止服务时 worker 先终止正在跑的同步再退出。worker 自身出错退出时，服务控制管理器按 `sc.exe failure` 的设置重启它。
