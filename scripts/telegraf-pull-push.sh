#!/usr/bin/env bash
# Scrapes a manager with Telegraf while the same manager pushes to it; the two outputs must match line for line.
# Needs: docker, curl and a built hustsync (default target/debug/hustsync).
set -euo pipefail

bin=$(realpath "${1:-target/debug/hustsync}")
port=14299
api=http://127.0.0.1:$port
tmp=$(mktemp -d)
chmod a+rwx "$tmp"
trap 'kill $(jobs -p) 2>/dev/null || true; rm -rf "$tmp"' EXIT
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
[metrics]
cache = { workers = 3600 }
[[metrics.push]]
url = "http://127.0.0.1:18186/telegraf"
interval = 2
groups = ["mirrors", "workers", "syncs"]
EOF

cat >"$tmp/telegraf.conf" <<EOF
[agent]
  interval = "2s"
  flush_interval = "1s"
  omit_hostname = true
[[inputs.prometheus]]
  urls = ["$api/metrics/mirrors", "$api/metrics/workers", "$api/metrics/syncs"]
  metric_version = 2
  url_tag = ""
  [inputs.prometheus.tags]
    src = "pull"
[[inputs.http_listener_v2]]
  service_address = "127.0.0.1:18186"
  paths = ["/telegraf"]
  data_format = "influx"
  [inputs.http_listener_v2.tags]
    src = "push"
[[outputs.file]]
  files = ["/out/pull.txt"]
  data_format = "influx"
  [outputs.file.tagpass]
    src = ["pull"]
[[outputs.file]]
  files = ["/out/push.txt"]
  data_format = "influx"
  [outputs.file.tagpass]
    src = ["push"]
EOF

docker pull -q telegraf:1.40 >/dev/null
"$bin" manager --config "$tmp/manager.toml" >"$tmp/manager.log" 2>&1 &
for _ in $(seq 50); do curl -fs "$api/ping" >/dev/null && break; sleep 0.2; done
seed
seeded=$(date +%s%N)
timeout 14 docker run --rm --network host -v "$tmp:/out" -v "$tmp/telegraf.conf:/etc/telegraf/telegraf.conf:ro" \
  telegraf:1.40 >"$tmp/telegraf.log" 2>&1 || true

# Samples taken after seeding, without the source tag and the time; Telegraf writes fields in map order, so sort them.
norm() {
  python3 - "$1" "$seeded" <<'PY'
import sys

path, since = sys.argv[1], int(sys.argv[2])
seen = set()
for line in open(path):
    head, fields, ts = line.rsplit(" ", 2)
    if int(ts) >= since:
        head = head.replace(",src=pull", "").replace(",src=push", "")
        seen.add(head + " " + ",".join(sorted(fields.split(","))))
print("\n".join(sorted(seen)))
PY
}
[[ -s $tmp/pull.txt && -s $tmp/push.txt ]] || { cat "$tmp/telegraf.log" "$tmp/manager.log"; echo "Telegraf wrote nothing"; exit 1; }
norm "$tmp/pull.txt" >"$tmp/pull"
norm "$tmp/push.txt" >"$tmp/push"
diff "$tmp/pull" "$tmp/push"
echo "pull and push agree on $(wc -l <"$tmp/pull") lines"
