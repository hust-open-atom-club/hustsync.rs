# Alerts

Alerts come two ways: hustsync logs an event with the target `hustsync::alert` when a fault starts and when it ends, for serlog's mail sink to send; or p8s evaluates the metrics, with rules in [monitoring.md](monitoring.md). The two are independent; either works alone.

## Events

| Event | Level | From | When | Fields |
|:--:|:--:|:--:|:--:|:--:|
| `mirror failing` | error | manager | A mirror's failed rounds in a row reach `[alert] failures`; once per outage | `mirror` `worker` `failures` `reason` |
| `mirror recovered` | info | manager | A mirror that raised `mirror failing` succeeds again | `mirror` `worker` `failures` |
| `manager unreachable` | error | worker | Requests to a manager go from succeeding to failing | `manager` `reason` |
| `manager reachable` | info | worker | They go from failing to succeeding | `manager` `failures` |

- A round starts at `pre-syncing`; retries within it count as one failure.
- `hustsync::alert` stays on at info whatever `--verbose` says; a `[log]` `filter` that does not name it gets it added, and `RUST_LOG`, when set, takes precedence.

## Configuration

Manager:

```toml
[alert]
failures = 3
```

| Key | Default | Description |
|:--:|:--:|:--:|
| `failures` | 3 | Failed rounds in a row that raise `mirror failing`; 0 turns it off |

A worker learns whether a manager is reachable only when it sends a request. With every mirror in a long sync it may not contact the manager for a long time; a heartbeat catches that early:

```toml
[manager]
api_base = "http://manager:14242"
heartbeat = 60
```

`heartbeat` is the interval in seconds between re-registrations; off by default, as in tunasync. It also keeps the manager's `last_online` current.

## Mail

Add a mail sink to `[log]` that takes only alerts, with the password from an environment variable:

```toml
[log]
sinks = [
  { kind = "mail", name = "alert", filter = "hustsync::alert=info", host = "smtp.example.org", port = 465, security = "tls", user = "bot@example.org", pass = "${SMTP_PASS}", to = ["ops@example.org"], wait = "5m" },
]
```

| Key | Purpose |
|:--:|:--:|
| `filter` | Takes only `hustsync::alert` |
| `wait` | Waits 5 minutes after the first event and sends everything since in one mail, so at most one mail per 5 minutes; when a whole upstream fails, dozens of mirrors still make one mail |

For the other keys, see the mail section of the serlog spec.

## Metrics

| Metric | Meaning |
|:--:|:--:|
| `hustsync_mirror_failures{mirror,worker}` | Failed rounds in a row per mirror since the manager started |
| `hustsync_worker_manager_up{manager}` | Whether the worker's last request to that manager succeeded |
| `hustsync_worker_manager_failures{manager}` | Failed requests in a row |
