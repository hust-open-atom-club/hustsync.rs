# 快速上手

在一台机器上起 manager 与 worker，用 `hustsynctl` 查看同步状态。

## 构建

```bash
cargo build --release
```

产物在 `target/release/`：`hustsync`（manager 与 worker）与 `hustsynctl`（命令行客户端）。

## 启动 manager

`manager.conf`：

```toml
debug = false

[server]
addr = "127.0.0.1"
port = 14242
ssl_cert = ""
ssl_key = ""

[files]
db_type = "redb"
db_file = "/tmp/hustsync/manager.db"
ca_cert = ""
```

```bash
hustsync manager --config manager.conf
```

## 启动 worker

`worker.conf`：

```toml
[global]
name = "test_worker"
log_dir = "/tmp/hustsync/log/{{.Name}}"
mirror_dir = "/tmp/hustsync/data"
concurrent = 10
interval = 1

[manager]
api_base = "http://localhost:14242"

[server]
hostname = "localhost"
listen_addr = "127.0.0.1"
listen_port = 6000

[[mirrors]]
name = "elvish"
provider = "rsync"
upstream = "rsync://rsync.elv.sh/elvish/"
use_ipv6 = false
```

```bash
hustsync worker --config worker.conf
```

`interval` 以分钟计，这里设为 1 便于观察。

## 查看状态

```bash
hustsynctl list -p 14242 --all
hustsynctl workers -p 14242
```

`list --all` 的输出就是前端读取的 `/jobs`。

日志在 `log_dir` 下，每次同步一个带时间的文件，`latest` 指向最新一份。

## 下一步

- 字段说明：[configs.md](configs.md)
- 生产部署：[deploy.md](deploy.md)
- 指标：[metrics.md](metrics.md)
- 告警：[alert.md](alert.md)
- 监控栈与看板：[monitoring.md](monitoring.md)
- 容器内同步：[docker.md](docker.md)
- 资源隔离：[cgroup.md](cgroup.md)
- fpsync 与 lsyncd：[tools.md](tools.md)
- ftpsync：[ftpsync.md](ftpsync.md)
- Windows：[windows.md](windows.md)
