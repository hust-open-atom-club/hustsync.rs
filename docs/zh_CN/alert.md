# 告警

告警有两条路：程序在故障开始与结束时写一条 target 为 `hustsync::alert` 的日志，由 serlog 的 mail 输出端发信；也可由 p8s 按指标判断。两条路互不依赖，可只用一条。

## 事件

| 事件 | 级别 | 谁发 | 何时 | 字段 |
|:--:|:--:|:--:|:--:|:--:|
| `mirror failing` | error | manager | 镜像连续失败的轮数达到 `[alert] failures`，一段故障只发一次 | `mirror` `worker` `failures` `reason` |
| `mirror recovered` | info | manager | 发过 `mirror failing` 的镜像再次成功 | `mirror` `worker` `failures` |
| `manager unreachable` | error | worker | 对某个 manager 的请求由成功转为失败 | `manager` `reason` |
| `manager reachable` | info | worker | 由失败转为成功 | `manager` `failures` |

- 一轮从 `pre-syncing` 开始，轮内的重试只算一次失败。
- `hustsync::alert` 始终开到 info，不受 `--verbose` 影响；`[log]` 的 `filter` 没写到它时自动补上，设了 `RUST_LOG` 时以 `RUST_LOG` 为准。

## 配置

manager：

```toml
[alert]
failures = 3
```

| 键 | 默认 | 说明 |
|:--:|:--:|:--:|
| `failures` | 3 | 连续失败多少轮发 `mirror failing`；0 不发 |

worker 发请求才知道连不连得上。镜像都在长时间同步时可能很久不联系 manager，设心跳即可及时发现：

```toml
[manager]
api_base = "http://manager:14242"
heartbeat = 60
```

`heartbeat` 是重新注册的间隔秒数，默认不发，与 tunasync 一致；manager 的 `last_online` 也随之更新。

## 发信

在 `[log]` 里加一个只收告警的 mail 输出端，密码从环境变量取：

```toml
[log]
sinks = [
  { kind = "mail", name = "alert", filter = "hustsync::alert=info", host = "smtp.example.org", port = 465, security = "tls", user = "bot@example.org", pass = "${SMTP_PASS}", to = ["ops@example.org"], wait = "5m" },
]
```

| 键 | 作用 |
|:--:|:--:|
| `filter` | 只收 `hustsync::alert` |
| `wait` | 第一条到达后等 5 分钟再发，期间的告警合成一封，因此每 5 分钟最多一封；上游整体故障时几十个镜像的告警也只占一封 |

其余键见 serlog 规格的 mail 一节。

## 指标

| 指标 | 含义 |
|:--:|:--:|
| `hustsync_mirror_failures{mirror,worker}` | manager 启动以来各镜像连续失败的轮数 |
| `hustsync_worker_manager_up{manager}` | worker 最近一次请求该 manager 是否成功 |
| `hustsync_worker_manager_failures{manager}` | 连续失败的请求数 |
