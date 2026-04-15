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
