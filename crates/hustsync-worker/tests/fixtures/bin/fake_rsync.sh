#!/usr/bin/env bash
# Test double for rsync used by two_stage_rsync_contract tests.
#
# Detects stage by argv:
#   - presence of --include=*.diff/  → stage 1
#   - presence of --delete           → stage 2
#   - neither                        → treated as stage 1 (debian-oldstyle)
#
# Exit code is picked from env:
#   - stage 1: $FAKE_STAGE1_EXIT (default 0)
#   - stage 2: $FAKE_STAGE2_EXIT (default 0)
#
# If $FAKE_SLEEP is set, sleep that many seconds before exiting (for
# terminate-mid-stage tests). The sleep is interruptible by SIGTERM/SIGKILL.

is_stage_two=0
for arg in "$@"; do
    case "$arg" in
        --delete) is_stage_two=1 ;;
    esac
done

if [[ -n "$FAKE_ENV_DUMP" ]]; then
    {
        echo "TUNASYNC_MIRROR_NAME=${TUNASYNC_MIRROR_NAME:-}"
        echo "TUNASYNC_WORKING_DIR=${TUNASYNC_WORKING_DIR:-}"
        echo "TUNASYNC_UPSTREAM_URL=${TUNASYNC_UPSTREAM_URL:-}"
        echo "TUNASYNC_LOG_DIR=${TUNASYNC_LOG_DIR:-}"
        echo "TUNASYNC_LOG_FILE=${TUNASYNC_LOG_FILE:-}"
        echo "HUSTSYNC_MIRROR_NAME=${HUSTSYNC_MIRROR_NAME:-}"
        echo "HUSTSYNC_WORKING_DIR=${HUSTSYNC_WORKING_DIR:-}"
        echo "HUSTSYNC_UPSTREAM_URL=${HUSTSYNC_UPSTREAM_URL:-}"
        echo "HUSTSYNC_LOG_DIR=${HUSTSYNC_LOG_DIR:-}"
        echo "HUSTSYNC_LOG_FILE=${HUSTSYNC_LOG_FILE:-}"
    } >> "$FAKE_ENV_DUMP"
fi

if [[ -n "$FAKE_SLEEP" ]]; then
    sleep "$FAKE_SLEEP" &
    sleep_pid=$!
    trap 'kill $sleep_pid 2>/dev/null; exit 143' TERM INT
    wait $sleep_pid
fi

if (( is_stage_two )); then
    exit "${FAKE_STAGE2_EXIT:-0}"
else
    exit "${FAKE_STAGE1_EXIT:-0}"
fi
