# Sourced by the metrics scripts: registers a worker and reports five mirror states to the manager at $api.

seed() {
  curl -fsS -XPOST "$api/workers" -H 'content-type: application/json' \
    -d '{"id":"w1","url":"http://127.0.0.1:9/","token":"","last_online":"2026-01-01T00:00:00Z","last_register":"2026-01-01T00:00:00Z"}' >/dev/null
  local s
  for s in "debian pre-syncing" "debian success 1.5G" "ubuntu pre-syncing" "ubuntu failed unknown" "arch syncing"; do
    set -- $s
    curl -fsS -XPOST "$api/workers/w1/jobs/$1" -H 'content-type: application/json' -d "{
      \"name\": \"$1\", \"worker\": \"w1\", \"upstream\": \"rsync://up/$1/\", \"size\": \"${3:-}\", \"error_msg\": \"\",
      \"last_update\": \"0001-01-01T00:00:00Z\", \"last_started\": \"0001-01-01T00:00:00Z\",
      \"last_ended\": \"0001-01-01T00:00:00Z\", \"next_schedule\": \"2030-01-01T00:00:00Z\",
      \"status\": \"$2\", \"is_master\": true}" >/dev/null
  done
}
