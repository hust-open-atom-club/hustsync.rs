#!/usr/bin/env bash
# Feeds the shipped Alloy config a few access log lines; its counters must name each visitor's country and province.
# Needs: docker, curl.
set -euo pipefail
cd "$(dirname "$0")/.."

metrics=http://127.0.0.1:12346/metrics
tmp=$(mktemp -d)
trap 'docker rm -f access-alloy >/dev/null 2>&1 || true; rm -rf "$tmp"' EXIT
mkdir -p "$tmp/log" "$tmp/geoip"
curl -fsSL "https://download.db-ip.com/free/dbip-city-lite-$(date +%Y-%m).mmdb.gz" | gunzip >"$tmp/geoip/dbip-city-lite.mmdb"
touch "$tmp/log/access.log"
chmod -R a+rwX "$tmp"

docker pull -q grafana/alloy:v1.20.1 >/dev/null
docker run -d --name access-alloy --network host \
  -v "$PWD/deploy/alloy/config.alloy:/etc/alloy/config.alloy:ro" \
  -v "$tmp/log:/var/log/nginx:ro" -v "$tmp/geoip:/etc/alloy/geoip:ro" \
  grafana/alloy:v1.20.1 run /etc/alloy/config.alloy --storage.path=/tmp/alloy --server.http.listen-addr=127.0.0.1:12346 >/dev/null
for _ in $(seq 60); do curl -fs "${metrics%/metrics}/-/ready" >/dev/null && break; sleep 0.5; done

line() { printf '%s - - [19/Oct/2026:20:00:00 +0800] "GET /debian/dists/ HTTP/1.1" 200 %s "-" "apt"\n' "$1" "$2"; }
{ line 202.114.0.242 1000; line 202.114.0.242 500; line 8.8.8.8 42; } >>"$tmp/log/access.log"

# One series per place; prints its value, or nothing until it appears.
value() { curl -fs "$metrics" | grep "^hustsync_access_$1{" | grep -F "$2" | awk '{print $NF}'; }
for _ in $(seq 60); do
  [[ $(value requests_total 'place="美国"') == 1 ]] && break
  sleep 1
done

check() {
  got=$(value "$1" "$2")
  echo "$1 $2: ${got:-none}"
  [[ $got == "$3" ]]
}
check requests_total 'country="CN",filename="/var/log/nginx/access.log",lat="30.59",lon="114.31",place="湖北",region="Hubei"' 2
check sent_bytes_total 'place="湖北",region="Hubei"' 1500
check requests_total 'country="US",filename="/var/log/nginx/access.log",place="美国",source' 1
