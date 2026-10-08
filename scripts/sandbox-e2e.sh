#!/bin/bash
# Run the full test suite on claude-sandbox over SSH, then remove everything it created.
set -euo pipefail
cd "$(dirname "$0")/.."
SSH=(ssh -i "$HOME/.ssh/id_ed25519_sandbox" -o BatchMode=yes claude@claude-sandbox)
REMOTE=/tmp/dispatch-cpu-runner-e2e
trap '"${SSH[@]}" "rm -rf $REMOTE /tmp/cpurunner-test-*" || true' EXIT
"${SSH[@]}" "rm -rf $REMOTE && mkdir -p $REMOTE"
tar --exclude .git --exclude __pycache__ -cf - . | "${SSH[@]}" "tar -xf - -C $REMOTE"
"${SSH[@]}" "cd $REMOTE && python3 -W ignore -m unittest discover -s tests -v 2>&1 | tail -40; \
  echo '--- selftest (expected to FAIL closed here: docker default seccomp blocks unshare) ---'; \
  CPU_RUNNER_ISOLATION=required python3 agent/agent.py selftest || true; \
  echo '--- leftover processes ---'; pgrep -af 'agent/agent.py|sleep 300' || echo none"
