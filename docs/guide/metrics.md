# Metrics

The manager and the worker expose p8s metrics for Prometheus or Telegraf to scrape.

## Build features

| Feature | Provides | Default |
|:--:|:--:|:--:|
| `p8s` | `GET /metrics`, `/metrics/all`, `/metrics/<group>` | on |

```bash
cargo build --release
cargo build --release --no-default-features
```

Without it no metrics code is built, and a config with `[metrics]` fails at start-up.

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
| | `hustsync_mirror_sync_duration_seconds{mirror,worker}` | Duration of the last sync |
| | `hustsync_mirror_last_success_timestamp_seconds{mirror,worker}` | Time of the last success |
| `workers` | `hustsync_workers` | Registered workers |
| | `hustsync_workers_online` `hustsync_worker_up{worker}` | Probed at scrape time; a reply within 2 s counts as online |
| `syncs` | `hustsync_syncs_total{mirror,worker,result}` | Successful and failed syncs |
| `process` | `hustsync_build_info{version}` and `process_*` | Version; on Linux also CPU, memory, file descriptors and threads |

## worker

| Group | Metric | Meaning |
|:--:|:--:|:--:|
| `jobs` | `hustsync_worker_jobs` | Mirrors |
| | `hustsync_worker_jobs_running` | Mirrors syncing now |
| | `hustsync_worker_jobs_queued` | Mirrors waiting for their next sync |
| | `hustsync_worker_concurrency` | Concurrency limit |
| `process` | As on the manager | |

## Configuration

The manager and the worker take the same keys:

```toml
[metrics]
cache = { mirrors = 10, workers = 30 }
```

| Key | Default | Description |
|:--:|:--:|:--:|
| `cache` | empty | Group name to seconds; groups not listed are computed per request |
