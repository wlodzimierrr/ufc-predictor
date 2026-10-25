#!/usr/bin/env bash
# Stable automation command; never start independent refresh steps in parallel.
set -Eeuo pipefail

cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.."
revision="${AUTOMATION_IMAGE_REVISION:-${IMAGE_REVISION:-unknown}}"
if [[ "$revision" == unknown ]] && command -v git >/dev/null 2>&1; then
    revision="$(git rev-parse HEAD 2>/dev/null || printf unknown)"
fi
printf 'automation start=%s revision=%s command_version=v1 command=make_update_dashboard_data\n' \
    "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$revision"
finish() {
    status=$?
    trap - EXIT
    printf 'automation end=%s status=%s exit_code=%s\n' \
        "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$([[ "$status" == 0 ]] && printf succeeded || printf failed)" "$status"
    exit "$status"
}
trap finish EXIT
export AUTOMATION_GIT_REVISION="$revision"
python3 scripts/automation/run_refresh.py "$@"
