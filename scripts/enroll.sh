#!/bin/bash
# Run on the machine that hosts the queue API (the Mac). Prints the two values to paste into the Unraid template:
# the queue API URL and a single-use, 1-hour enrollment code. Needs the queue-side cpu_lane.py (default ~/bin/cpu_lane.py).
#   scripts/enroll.sh            mint a code
#   scripts/enroll.sh --rotate   rotate the shared token (revokes every runner), then mint a code
set -euo pipefail
LANE="${CPU_LANE_PY:-$HOME/bin/cpu_lane.py}"
[ -f "$LANE" ] || { echo "enroll: $LANE not found (set CPU_LANE_PY)" >&2; exit 1; }
[ "${1:-}" = "--rotate" ] && python3 "$LANE" rotate-token
python3 "$LANE" enroll-code
