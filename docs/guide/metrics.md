# Metrics

The manager and the worker expose p8s metrics for Prometheus or Telegraf to scrape, and can push them to Telegraf, InfluxDB or Alloy.

## Build features

| Feature | Provides | Default |
|:--:|:--:|:--:|
| `p8s` | `GET /metrics`, `/metrics/all`, `/metrics/<group>` | on |
| `influx` | Pushes InfluxDB line protocol | on |
| `otlp` | Pushes OTLP/HTTP protobuf | off |

```bash
cargo build --release
cargo build --release --features otlp
cargo build --release --no-default-features
```

With all three off no metrics code is built, and a config using a missing one fails at start-up.

## Endpoints

| Path | Content |
|:--:|:--:|
| `/metrics`, `/metrics/all` | Every group |
| `/metrics/<group>` | One group; an unknown name returns 404 with the list of groups |

The manager serves them on its own port, the worker on its control port, `listen_port`. Groups do not overlap, so fast-changing ones can be scraped more often:

```yaml
scrape_configs:
  - job_name: hustsync-mirrors
    scrape_interval: 15s
    metrics_path: /metrics/mirrors
    static_configs: [{ targets: ["manager:14242"] }]
  - job_name: hustsync-rest
    scrape_interval: 1m
    metrics_path: /metrics/workers
    static_configs: [{ targets: ["manager:14242"] }]
```

## manager

| Group | Metric | Meaning |
|:--:|:--:|:--:|
| `mirrors` | `hustsync_mirrors{status}` | Mirrors per status; they sum to the total |
| | `hustsync_mirror_state{mirror,worker}` | 0 none, 1 failed, 2 success, 3 syncing, 4 pre-syncing, 5 paused, 6 disabled |
| | `hustsync_mirror_info{mirror,worker,upstream,role}` | Always 1; carries the upstream and role |
| | `hustsync_mirror_size_bytes{mirror,worker}` | Mirror size |
| | `hustsync_mirror_sync_duration_seconds{mirror,worker}` | Duration of the last sync |
| | `hustsync_mirror_last_success_timestamp_seconds{mirror,worker}` | Time of the last success |
| | `hustsync_mirror_next_sync_timestamp_seconds{mirror,worker}` | Time of the next sync |
| | `hustsync_mirror_failures{mirror,worker}` | Failed rounds in a row since the manager started |
| `workers` | `hustsync_workers` | Registered workers |
| | `hustsync_workers_online` `hustsync_worker_up{worker}` | Probed at scrape time; a reply within 2 s counts as online |
| | `hustsync_worker_last_online_timestamp_seconds{worker}` | When the worker last contacted the manager |
| `syncs` | `hustsync_syncs_total{mirror,worker,result}` | Successful and failed syncs |
| | `hustsync_sync_duration_seconds{worker,result}` | Histogram of sync durations, buckets from 1 s to 1 day |
| `process` | `hustsync_build_info{version}` and `process_*` | Version; on Linux also CPU, memory, file descriptors and threads |

Size comes from `size` in the status: `K`, `M`, `G` and `T` are powers of 1000, as with rsync `-h`; `KiB` and the like are powers of 1024; `unknown` gives no line.

## worker

| Group | Metric | Meaning |
|:--:|:--:|:--:|
| `jobs` | `hustsync_worker_jobs` | Mirrors |
| | `hustsync_worker_jobs_running` | Mirrors syncing now |
| | `hustsync_worker_jobs_queued` | Mirrors waiting for their next sync |
| | `hustsync_worker_concurrency` | Concurrency limit |
| `manager` | `hustsync_worker_manager_up{manager}` | Whether the last request to that manager succeeded |
| | `hustsync_worker_manager_failures{manager}` | Failed requests in a row |
| | `hustsync_worker_manager_last_success_timestamp_seconds{manager}` | Time of the last success |
| `process` | As on the manager | |

## Configuration

The manager and the worker take the same keys:

```toml
[metrics]
cache = { mirrors = 10, workers = 30 }

[[metrics.push]]
url = "http://telegraf:8186/telegraf"
interval = 30
groups = ["mirrors", "workers", "syncs"]

[[metrics.push]]
url = "http://alloy:4318/v1/metrics"
format = "otlp"
```

| Key | Default | Description |
|:--:|:--:|:--:|
| `cache` | empty | Group name to seconds; groups not listed are computed per request |
| `push.url` | required | Receiver address |
| `push.format` | `influx` | `influx` or `otlp` |
| `push.interval` | 30 | Seconds |
| `push.groups` | all | Groups to push |
| `push.headers` | empty | Extra request headers, such as InfluxDB's `Authorization = "Token …"` |

A failing push logs one warning when it starts failing and one info line when it recovers.

## Receivers

| Receiver | Config |
|:--:|:--:|
| Prometheus | Scrape with the `scrape_configs` above |
| Telegraf pull | `inputs.prometheus`, `metric_version = 2` |
| Telegraf push | `inputs.http_listener_v2`, `data_format = "influx"`; the output matches the pull line for line |
| InfluxDB | `url` set to `/api/v2/write?org=…&bucket=…`, with the token in `headers` |
| Alloy | The `http` endpoint of `otelcol.receiver.otlp`; once written to Prometheus through `otelcol.exporter.prometheus`, metric names match the scraped ones, `job` is `hustsync-manager` or `hustsync-worker`, and `instance` is the manager address or the worker name |

The line protocol follows Telegraf 1.40 `inputs.prometheus` with `metric_version = 2`: the measurement is `prometheus`, tags are sorted by key, the field name is the metric name, and a histogram becomes one line for `_count` and `_sum` plus one per bucket.
