#!/usr/bin/env bash
# Pushes a manager's metrics as OTLP to Alloy, which writes them to Prometheus; they must come back under their p8s names.
# Needs: docker, curl, python3 and a hustsync built with `--features otlp` (default target/debug/hustsync).
set -euo pipefail

bin=$(realpath "${1:-target/debug/hustsync}")
port=14298
api=http://127.0.0.1:$port
p8s=http://127.0.0.1:19090
tmp=$(mktemp -d)
trap 'kill $(jobs -p) 2>/dev/null || true; docker rm -f otlp-p8s otlp-alloy >/dev/null 2>&1 || true; rm -rf "$tmp"' EXIT
. "$(dirname "$0")/seed-manager.sh"

cat >"$tmp/manager.toml" <<EOF
debug = false
[server]
addr = "127.0.0.1"
port = $port
ssl_cert = ""
ssl_key = ""
[files]
db_type = "redb"
db_file = "$tmp/manager.db"
ca_cert = ""
[[metrics.push]]
url = "http://127.0.0.1:14318/v1/metrics"
format = "otlp"
interval = 2
EOF

cat >"$tmp/config.alloy" <<EOF
otelcol.receiver.otlp "in" {
  http {
    endpoint = "127.0.0.1:14318"
  }
  output {
    metrics = [otelcol.exporter.prometheus.out.input]
  }
}

otelcol.exporter.prometheus "out" {
  forward_to = [prometheus.remote_write.out.receiver]
}

prometheus.remote_write "out" {
  endpoint {
    url = "$p8s/api/v1/write"
  }
}
EOF

docker pull -q prom/prometheus:v3.15.0 >/dev/null
docker pull -q grafana/alloy:v1.20.1 >/dev/null
docker run -d --name otlp-p8s --network host prom/prometheus:v3.15.0 --config.file=/etc/prometheus/prometheus.yml \
  --web.listen-address=127.0.0.1:19090 --web.enable-remote-write-receiver >/dev/null
docker run -d --name otlp-alloy --network host -v "$tmp/config.alloy:/etc/alloy/config.alloy:ro" \
  grafana/alloy:v1.20.1 run /etc/alloy/config.alloy --server.http.listen-addr=127.0.0.1:12345 >/dev/null
for _ in $(seq 60); do
  curl -fs "$p8s/-/ready" >/dev/null && curl -fs http://127.0.0.1:12345/-/ready >/dev/null && break
  sleep 0.5
done

"$bin" manager --config "$tmp/manager.toml" >"$tmp/manager.log" 2>&1 &
for _ in $(seq 50); do curl -fs "$api/ping" >/dev/null && break; sleep 0.2; done
seed

series() {
  curl -fsS "$p8s/api/v1/query" --data-urlencode "query=$1" |
    python3 -c 'import json, sys; print(len(json.load(sys.stdin)["data"]["result"]))'
}
for _ in $(seq 30); do
  [[ $(series 'hustsync_mirror_state{job="hustsync-manager"}') == 3 ]] && break
  sleep 1
done
for want in 'hustsync_mirror_state{job="hustsync-manager"} 3' 'hustsync_syncs_total{job="hustsync-manager"} 2' \
  'hustsync_sync_duration_seconds_count 2' 'hustsync_mirror_size_bytes 1'; do
  query=${want% *}
  got=$(series "$query")
  [[ $got == "${want##* }" ]] || { echo "$query: $got series, want ${want##* }"; exit 1; }
done
echo "OTLP metrics reached Prometheus under their p8s names"
