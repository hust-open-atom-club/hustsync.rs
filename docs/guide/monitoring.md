# Monitoring stack

`deploy/` has the configs for monitoring, and compose's `monitoring` profile brings them all up with one command:

```bash
cd deploy/compose
docker compose --profile monitoring up -d
```

## Components

| Component | Config | Role |
|:--:|:--:|:--:|
| Prometheus 3.15 | `deploy/prometheus/prometheus.yml` | Scrapes each group on its own interval: mirror state every 15 s, sync counts every 30 s, workers and processes every minute |
| Alert rules | `deploy/prometheus/rules.yml` | See below |
| Alloy 1.20 | `deploy/alloy/config.alloy` | Takes program logs pushed by serlog on port 3100; tails the sync logs under each mirror's `log_dir`, taking the mirror name from the file name; takes OTLP metrics on port 4318 and forwards them to Prometheus |
| Loki 3.7 | The image's own config | Stores logs |
| Telegraf 1.40 | `deploy/telegraf/telegraf.conf` | Scrapes the groups, takes pushes on port 8186 and writes to InfluxDB |
| InfluxDB 3 Core | compose | Reachable only inside the compose network; one query reads at most 432 Parquet files, about five days of data, so longer ranges need `--query-file-limit` |

The manager's and the worker's `[log]` already push to Alloy with `on_fail = "ignore"`: without the `monitoring` profile, logs still go to the console and failed pushes are dropped quietly.

## Alert rules

| Rule | Condition |
|:--:|:--:|
| `HustsyncManagerDown` | Scraping the manager fails for 5 minutes |
| `HustsyncWorkerDown` | A worker does not answer for 10 minutes |
| `HustsyncMirrorFailing` | 3 failed rounds in a row |
| `HustsyncMirrorStale` | No success for two days, paused and disabled mirrors excepted, for 30 minutes |
| `HustsyncMirrorOverdue` | Not started an hour after its scheduled time, for 15 minutes |
| `HustsyncMirrorStuck` | Syncing for more than a day |

The rules have unit tests:

```bash
promtool test rules deploy/prometheus/rules.test.yml
```

To use Alertmanager, add an `alerting` section to `prometheus.yml`. For hustsync's own alert mails, see [alert.md](alert.md).

## Without compose

| Component | What to do |
|:--:|:--:|
| Prometheus | Change the `targets` in `prometheus.yml` |
| Alloy | Replace `/var/log/hustsync` in `config.alloy` with the worker's `log_dir` |
| Telegraf | Change `urls` and the output |
