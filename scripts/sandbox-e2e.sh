#!/bin/bash
# Run the full test suite on a disposable remote Linux box over SSH (SANDBOX_SSH='ssh -i KEY user@host'), then remove everything it created.
set -euo pipefail
cd "$(dirname "$0")/.."
[ -n "${SANDBOX_SSH:-}" ] || { echo "set SANDBOX_SSH, e.g. 'ssh -i ~/.ssh/key user@host'" >&2; exit 2; }
read -r -a SSH <<< "$SANDBOX_SSH -o BatchMode=yes"
REMOTE=/tmp/dispatch-cpu-runner-e2e
trap '"${SSH[@]}" "rm -rf $REMOTE /tmp/cpurunner-test-*" || true' EXIT
"${SSH[@]}" "rm -rf $REMOTE && mkdir -p $REMOTE"
tar --exclude .git --exclude __pycache__ -cf - . | "${SSH[@]}" "tar -xf - -C $REMOTE"
"${SSH[@]}" "cd $REMOTE && python3 -W ignore -m unittest discover -s tests -v 2>&1 | tail -40; \
  echo '--- agent selftest (expected to FAIL closed here: docker default seccomp blocks unshare) ---'; \
  CPU_RUNNER_ISOLATION=required python3 agent/agent.py selftest || true; \
  echo '--- leftover processes ---'; pgrep -af 'agent/agent.py|sleep 300' || echo none"
