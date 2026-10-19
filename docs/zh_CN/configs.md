# 配置说明

manager 与 worker 各读一个 TOML 文件。字段名与 tunasync 一致，tunasync 的配置大多可以直接使用，差异见文末。

## manager

| 字段 | 默认值 | 说明 |
|:--:|:--:|:--:|
| `debug` | 无，必填 | 打开调试日志与请求追踪 |
| `server.addr` | 无，必填 | 监听地址 |
| `server.port` | 无，必填 | 监听端口，tunasync 惯用 14242 |
| `server.ssl_cert` `server.ssl_key` | 无，必填 | 都不为空时启用 HTTPS |
| `files.db_type` | 无，必填 | `redb` 或 `redis` |
| `files.db_file` | 无，必填 | redb 文件路径，或 redis 地址 `redis://host:6379/0` |
| `files.ca_cert` | 无，必填 | 访问 HTTPS worker 时信任的 CA，空表示系统默认 |
| `files.status_file` | 空 | 每次状态变化时把 `/jobs` 的内容写入该文件 |

## worker

### global

| 字段 | 默认值 | 说明 |
|:--:|:--:|:--:|
| `name` | 空 | worker 名，manager 以它区分 worker |
| `log_dir` | `/tmp/hustsync/log/{{.Name}}` | 日志目录，`{{.Name}}` 替换为镜像名 |
| `log_retention` | 10 | 每个镜像保留的日志文件数，含本次 |
| `log_gzip` | `false` | 轮转时把保留的旧日志压成 `.gz` |
| `mirror_dir` | `/tmp/hustsync` | 镜像数据根目录，镜像目录为 `mirror_dir/mirror_subdir/镜像名` |
| `concurrent` | 10 | 同时同步的镜像数 |
| `interval` | 120 | 同步间隔，分钟 |
| `retry` | 2 | 一次同步失败后的尝试次数 |
| `timeout` | 0 | 单次同步的超时，秒；0 表示不限 |
| `exec_on_success` `exec_on_failure` | 无 | 同步成功、失败后在宿主机上执行的命令 |
| `rsync_options` | 无 | 追加给所有 rsync 镜像的参数 |
| `dangerous_global_success_exit_codes` | 无 | 所有镜像都视为成功的退出码 |
| `dangerous_global_rsync_success_exit_codes` | 无 | 所有 rsync 镜像都视为成功的退出码 |

### manager

| 字段 | 默认值 | 说明 |
|:--:|:--:|:--:|
| `api_base` | `http://localhost:14242` | manager 地址 |
| `api_base_list` | 无 | 多个 manager；状态同时上报给每一个，启动时从第一个读取历史状态 |
| `token` | 空 | 注册时携带 |
| `ca_cert` | 空 | 访问 HTTPS manager 时信任的 CA |
| `heartbeat` | 无 | 每隔多少秒重新注册一次，见 [alert.md](alert.md) |

### server

| 字段 | 默认值 | 说明 |
|:--:|:--:|:--:|
| `hostname` | `localhost` | manager 回连 worker 用的主机名 |
| `listen_addr` | `127.0.0.1` | 控制端口监听地址 |
| `listen_port` | 6000 | 控制端口，也提供 `/metrics` |
| `ssl_cert` `ssl_key` | 空 | 都不为空时启用 HTTPS |

### docker

| 字段 | 默认值 | 说明 |
|:--:|:--:|:--:|
| `enable` | false | 打开后，设置了 `docker_image` 的镜像在容器内同步 |
| `volumes` | 无 | 所有容器都挂载的卷，`宿主路径:容器路径[:ro]` |
| `options` | 无 | 追加给所有 `docker run` 的参数 |

详见 [docker.md](docker.md)。

### cgroup

| 字段 | 默认值 | 说明 |
|:--:|:--:|:--:|
| `enable` | false | 每个镜像在自己的 cgroup 里同步 |
| `base_path` | `/sys/fs/cgroup` | cgroupfs 挂载点 |
| `group` | 空 | 父组；空表示 worker 自己所在的组 |

详见 [cgroup.md](cgroup.md)。

### include

| 字段 | 说明 |
|:--:|:--:|
| `include_mirrors` | glob，匹配到的每个文件里的 `[[mirrors]]` 追加进来 |

## 日志

manager 与 worker 都可加 `[log]` 表，在命令行选定的终端或 `--log-file` 之外再开输出端，由 [serlog](https://github.com/LeoHeco/ServerLog) 写出。

| 字段 | 说明 |
|:--:|:--:|
| `filter` | 过滤指令，如 `info,hustsync_worker=debug`；设了 `RUST_LOG` 时不生效 |
| `sinks` | 输出端列表，键见 serlog 规格，如 `file`、`loki`、`mail` |

```toml
[log]
filter = "info"
sinks = [
  { kind = "loki", url = "http://alloy:3100/loki/api/v1/push", labels = { site = "hust" } },
  { kind = "file", path = "/var/log/hustsync/worker.json", format = "json", max_size = "64MiB", keep = 7 },
]
```

## 告警

manager 的 `[alert]` 设连续失败多少轮发告警，worker 的 `[manager] heartbeat` 设心跳间隔，详见 [alert.md](alert.md)。

## 指标

manager 与 worker 都可加 `[metrics]`：`cache` 设各组的缓存秒数，`[[metrics.push]]` 每项一个推送目标，详见 [metrics.md](metrics.md)。

## 镜像

`[[mirrors]]` 每项一个镜像。未设置的字段取 `[global]` 的值；镜像可以嵌套 `[[mirrors.mirrors]]`，子镜像继承父镜像未设置的字段。

### 通用

| 字段 | 默认值 | 说明 |
|:--:|:--:|:--:|
| `name` | 必填 | 镜像名 |
| `provider` | `rsync` | `rsync`、`two-stage-rsync`、`command`、`fpsync`、`lsyncd`、`ftpsync` |
| `upstream` | 空 | 上游地址；rsync 类必须以 `/` 结尾 |
| `interval` `retry` `timeout` | 取 global | 同 global |
| `cron` | 无 | 按 cron 表达式排期，如 `0 */6 * * *`，取代 `interval`；本地时区 |
| `log_retention` `log_gzip` | 取 global | 同 global |
| `mirror_dir` | 无 | 直接指定镜像目录，忽略 `mirror_subdir` |
| `mirror_subdir` | 无 | 镜像目录的中间一级 |
| `log_dir` | 取 global | 同 global |
| `role` | `master` | `slave` 时上报为从站 |
| `env` | 无 | 追加给同步进程的环境变量 |
| `exec_on_success` `exec_on_failure` | 取 global | 替换 global 的设置 |
| `exec_on_success_extra` `exec_on_failure_extra` | 无 | 追加在 global 的设置之后 |
| `success_exit_codes` | 无 | 额外视为成功的退出码 |
| `memory_limit` | 无 | 内存上限，如 `256M`；只在 docker 或 cgroup 下生效 |

同步进程可读到 `HUSTSYNC_MIRROR_NAME`、`HUSTSYNC_WORKING_DIR`、`HUSTSYNC_UPSTREAM_URL`、`HUSTSYNC_LOG_DIR`、`HUSTSYNC_LOG_FILE`，以及同值的 `TUNASYNC_*`。

### rsync 与 two-stage-rsync

| 字段 | 默认值 | 说明 |
|:--:|:--:|:--:|
| `command` | `rsync` | rsync 可执行文件 |
| `username` `password` | 无 | 写入 `USER` 与 `RSYNC_PASSWORD` |
| `exclude_file` | 无 | `--exclude-from` 的文件 |
| `rsync_options` | 无 | 追加的参数；two-stage 只用于第二阶段 |
| `rsync_override` | 无 | 替换默认参数，仅 rsync |
| `rsync_override_only` | false | 只用 `rsync_override`，仅 rsync |
| `rsync_timeout` | 120 | `--timeout` 秒数 |
| `rsync_no_timeout` | false | 不加 `--timeout` |
| `use_ipv6` `use_ipv4` | false | 加 `-6` 或 `-4` |
| `rsync_success_exit_codes` | 无 | 额外视为成功的 rsync 退出码 |
| `stage1_profile` | `debian` | two-stage 第一阶段的排除规则：`debian` 或 `debian-oldstyle` |

### command

| 字段 | 说明 |
|:--:|:--:|
| `command` | 按 shell 规则切分后直接执行，不经过 shell |
| `fail_on_match` | 日志命中该正则即视为失败 |
| `size_pattern` | 从日志里取镜像大小的正则，取最后一次匹配的第一个分组 |

### docker、fpsync、lsyncd、ftpsync

| 字段 | 说明 |
|:--:|:--:|
| `docker_image` | 容器镜像，见 [docker.md](docker.md) |
| `docker_volumes` `docker_options` | 追加在 `[docker]` 的设置之后 |
| `fpsync_options` `lsyncd_options` | 原样传给 fpsync、lsyncd，见 [tools.md](tools.md) |
| `ftpsync_conf` | 追加到生成的 ftpsync 配置末尾的行，见 [ftpsync.md](ftpsync.md) |

## 与 tunasync 的差异

| 项 | tunasync | hustsync |
|:--:|:--:|:--:|
| 数据库 | bolt、badger、leveldb、redis | redb、redis |
| 未知字段 | 忽略 | 报错，防止拼错的字段悄悄失效 |
| 环境变量 | `TUNASYNC_*` | `HUSTSYNC_*`，同时提供同值的 `TUNASYNC_*` |
| docker 容器名 | `tunasync-job-镜像名` | `hustsync-job-镜像名` |
| 超出 `memory_limit` | 普通失败 | 上报为 OOM |
| 新 provider | 无 | `fpsync`、`lsyncd`、`ftpsync` |
| 排期 | 只有 `interval` | 另可用 `cron` |
| 日志份数 | 固定 10 | `log_retention` 可调 |
| 旧日志压缩 | 无 | `log_gzip` |
