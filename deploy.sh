#!/bin/bash
# Run ON UNRAID in the deploy dir (/mnt/user/data/dispatch-cpu-runner):   bash deploy.sh
# Idempotent: first deploy, re-deploy and "rebuild needed" updates are all this one command.
#   bash deploy.sh                  build + start + wait healthy + selftest + PASS/FAIL summary
#   bash deploy.sh --relax-seccomp  also write docker-compose.override.yml (seccomp=unconfined) first
# Prerequisite (once, and after Mac IP changes): run scripts/provision.sh (or update.sh) on the Mac.
set -uo pipefail
cd "$(dirname "$0")"
NAME=dispatch-cpu-runner
RELAX=0
for a in "$@"; do case "$a" in --relax-seccomp) RELAX=1;; -h|--help) sed -n 2,6p "$0"; exit 0;; *) echo "unknown option $a"; exit 2;; esac; done
fail() { echo; echo "DEPLOY: FAIL - $*"; exit 1; }

command -v docker >/dev/null 2>&1 || fail "docker not found"
if docker compose version >/dev/null 2>&1; then DC=(docker compose)
elif command -v docker-compose >/dev/null 2>&1; then DC=(docker-compose)
else fail "neither 'docker compose' nor 'docker-compose' is available"; fi
echo "deploy: using: ${DC[*]}"

[ -s secrets/token ] && [ -f .env ] || fail "secrets/token or .env missing - run scripts/provision.sh on the Mac first"
grep -qE '^CPU_RUNNER_API=http' .env || fail ".env has no CPU_RUNNER_API - run scripts/provision.sh on the Mac first"
grep -q '<MAC-LAN-IP>' .env && fail ".env still has the placeholder IP - run scripts/provision.sh on the Mac first"
chmod 600 secrets/token .env 2>/dev/null || true
mkdir -p cache

if [ "$RELAX" = 1 ]; then
  cat > docker-compose.override.yml <<'YML'
# written by deploy.sh --relax-seccomp (delete this file to restore the default seccomp profile)
services:
  cpu-runner:
    security_opt: ["seccomp=unconfined"]
YML
  echo "deploy: wrote docker-compose.override.yml (seccomp=unconfined)"
fi

echo "deploy: building and starting ..."
"${DC[@]}" up -d --build || fail "compose up failed"

echo "deploy: waiting for the container healthcheck (up to 150 s) ..."
status=""
for i in $(seq 1 75); do
  status="$(docker inspect --format '{{if .State.Running}}{{.State.Health.Status}}{{else}}stopped{{end}}' "$NAME" 2>/dev/null || echo missing)"
  [ "$status" = healthy ] && break
  sleep 2
done
logs="$(docker logs --tail 60 "$NAME" 2>&1)"

echo "deploy: running selftest ..."
st_out="$(docker exec "$NAME" python3 /opt/runner/agent.py selftest 2>&1)"; st_rc=$?
echo "$st_out" | tail -3

ns_fail=0
echo "$st_out$logs" | grep -qE "empty network namespace|isolation_probe_failed|isolation required but unavailable|selftest.*FAIL" && ns_fail=1
api_state="$(echo "$logs" | grep -oE '"event": "(api_ok|token_rejected|api_unreachable|api_unhealthy)"' | tail -1)"

echo
echo "================ DEPLOY SUMMARY ================"
ok=1
[ "$status" = healthy ] && echo "container health : PASS" || { echo "container health : FAIL ($status)"; ok=0; }
[ $st_rc -eq 0 ] && echo "isolation selftest: PASS" || { echo "isolation selftest: FAIL"; ok=0; }
case "$api_state" in
  *api_ok*) echo "queue API + token : PASS";;
  *token_rejected*) echo "queue API + token : FAIL (token rejected: re-run scripts/provision.sh on the Mac)"; ok=0;;
  *) echo "queue API + token : FAIL/unknown (${api_state:-no result in logs}); check CPU_RUNNER_API and that the Mac queue API is running"; ok=0;;
esac
if [ $ok -eq 1 ]; then
  { cat Dockerfile docker-compose.yml .env | sha256sum | cut -c1-16; } > .deployed-stamp
  echo "RESULT: PASS - runner is up; \`docker logs -f $NAME\` to watch. Updates: run update.sh on the Mac."
  exit 0
fi
if [ $ns_fail -eq 1 ]; then
  echo
  echo "HINT: the container could not create a network namespace (docker's default seccomp profile blocks unshare)."
  echo "      Run:  bash deploy.sh --relax-seccomp      (applies seccomp=unconfined via docker-compose.override.yml)"
fi
echo "RESULT: FAIL - last container log lines:"; echo "$logs" | tail -12
exit 1
