#!/bin/bash
# Mac, ONE step: sync the latest repo to the Unraid share. The running agent notices its own code changed,
# drains (finishes leased jobs), and re-execs itself. Only Dockerfile/compose/.env changes need `bash deploy.sh`
# on Unraid, and this script says so when that is the case.
#   ./update.sh            sync + report
#   ./update.sh --restart  also restart the container through the Unraid API (optional; key read from ~/.claude.json)
set -euo pipefail
cd "$(dirname "$0")"
bash scripts/provision.sh | tee "${TMPDIR:-/tmp}/cpu-runner-provision.$$" >&2
out="$(cat "${TMPDIR:-/tmp}/cpu-runner-provision.$$")"; rm -f "${TMPDIR:-/tmp}/cpu-runner-provision.$$"
if echo "$out" | grep -q REDEPLOY_NEEDED; then
  echo
  echo "update: REBUILD NEEDED. On Unraid run:  cd /mnt/user/data/dispatch-cpu-runner && bash deploy.sh"
else
  echo
  echo "update: done. Agent code reloads by itself within ~10s (jobs in flight finish first). Watch: docker logs -f dispatch-cpu-runner (event reload_detected / reload_exec)."
fi
if [ "${1:-}" = "--restart" ]; then
  python3 scripts/unraid_restart.py dispatch-cpu-runner
fi
