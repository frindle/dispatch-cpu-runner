#!/bin/bash
# Mac-side, idempotent: make the Unraid deploy dir fully ready. Run it any time (it is also step 1 of update.sh).
#   - rsync the build context to the data share (managed paths only)
#   - write secrets/token from the Mac's shared token file (created here if absent); NEVER printed
#   - create/refresh .env: only CPU_RUNNER_API is (re)written, every other key is left alone
#   - create cache/
# Env overrides (tests): CPU_RUNNER_SHARE, CPU_RUNNER_TOKEN_SRC, CPU_RUNNER_API_URL, CPU_LANE_PY
set -euo pipefail
cd "$(dirname "$0")/.."
SRC="$PWD"
DEST="${CPU_RUNNER_SHARE:-/Volumes/data/dispatch-cpu-runner}"
TOKEN_SRC="${CPU_RUNNER_TOKEN_SRC:-$HOME/.config/dispatch-cpu-runner/token}"
CPU_LANE_PY="${CPU_LANE_PY:-$HOME/bin/cpu_lane.py}"
die() { echo "provision: ERROR: $*" >&2; exit 1; }
say() { echo "provision: $*"; }

hash_cat() { if command -v shasum >/dev/null; then cat "$@" | shasum -a 256; else cat "$@" | sha256sum; fi | cut -c1-16; }
perm() { stat -f '%Lp' "$1" 2>/dev/null || stat -c '%a' "$1"; }
size() { stat -f '%z' "$1" 2>/dev/null || stat -c '%s' "$1"; }

# ---- 0. the share must be a real mount (the mountpoint gets reaped / recreated as a local dir otherwise)
case "$DEST" in
  /Volumes/*)
    vol="/Volumes/$(echo "${DEST#/Volumes/}" | cut -d/ -f1)"
    mount | grep -q " on $vol " || die "$vol is not mounted (Finder: Go > Connect to Server, smb://<unraid>/data). Refusing to write into a local directory."
    ;;
esac
parent="$(dirname "$DEST")"
[ -d "$parent" ] || die "$parent does not exist"
[ -w "$parent" ] || die "$parent is not writable"
mkdir -p "$DEST"

# ---- 1. shared token on the Mac (create once)
if [ ! -s "$TOKEN_SRC" ]; then
  mkdir -p "$(dirname "$TOKEN_SRC")"; chmod 700 "$(dirname "$TOKEN_SRC")" 2>/dev/null || true
  ( umask 077; openssl rand -hex 32 > "$TOKEN_SRC" )
  say "created Mac token file $TOKEN_SRC (random 32 bytes hex)"
fi
chmod 600 "$TOKEN_SRC"
[ -s "$TOKEN_SRC" ] || die "token file $TOKEN_SRC is empty"

# ---- 2. api url (Mac LAN address)
if [ -n "${CPU_RUNNER_API_URL:-}" ]; then API="$CPU_RUNNER_API_URL"
else
  [ -f "$CPU_LANE_PY" ] || die "$CPU_LANE_PY not found (needed for the Mac LAN URL); set CPU_RUNNER_API_URL to override"
  API="$(python3 "$CPU_LANE_PY" api-url)"
fi
case "$API" in http://*:[0-9]*) ;; *) die "bad api url '$API'";; esac
case "$API" in *'<'*|*' '*) die "api url '$API' is not usable (no LAN IP detected?)";; esac

# ---- 3. sync the build context
RS=(rsync -rlt --no-perms --no-owner --no-group --exclude __pycache__ --exclude '*.pyc' --exclude .DS_Store)
for d in agent client reference scripts; do
  [ -d "$SRC/$d" ] && "${RS[@]}" --delete "$SRC/$d/" "$DEST/$d/"      # --delete ONLY inside these managed dirs
done
for f in Dockerfile docker-compose.yml env.example deploy.sh update.sh README.md .gitignore; do
  [ -f "$SRC/$f" ] && "${RS[@]}" "$SRC/$f" "$DEST/$f"
done
chmod +x "$DEST/deploy.sh" "$DEST/update.sh" "$DEST"/scripts/*.sh 2>/dev/null || true

# ---- 4. secrets/token (byte-identical to the Mac file, mode 600; content never printed)
mkdir -p "$DEST/secrets"; chmod 700 "$DEST/secrets" 2>/dev/null || true
if ! cmp -s "$TOKEN_SRC" "$DEST/secrets/token" 2>/dev/null; then
  ( umask 077; cp "$TOKEN_SRC" "$DEST/secrets/.token.tmp" )
  chmod 600 "$DEST/secrets/.token.tmp"
  mv -f "$DEST/secrets/.token.tmp" "$DEST/secrets/token"
  say "wrote secrets/token"
else
  say "secrets/token already current"
fi
chmod 600 "$DEST/secrets/token" 2>/dev/null || true

# ---- 5. .env : create from env.example once; afterwards only CPU_RUNNER_API is refreshed
if [ ! -f "$DEST/.env" ]; then
  cp "$SRC/env.example" "$DEST/.env"; say "created .env from env.example"
fi
cur="$(grep -E '^CPU_RUNNER_API=' "$DEST/.env" | tail -1 | cut -d= -f2- || true)"
if [ "$cur" != "$API" ]; then
  tmp="$DEST/.env.tmp"
  if grep -qE '^CPU_RUNNER_API=' "$DEST/.env"; then
    API_NEW="$API" awk '/^CPU_RUNNER_API=/{print "CPU_RUNNER_API=" ENVIRON["API_NEW"]; next} {print}' "$DEST/.env" > "$tmp"
  else
    { cat "$DEST/.env"; echo "CPU_RUNNER_API=$API"; } > "$tmp"
  fi
  mv -f "$tmp" "$DEST/.env"
  say "CPU_RUNNER_API set to $API (was: ${cur:-unset})"
else
  say "CPU_RUNNER_API already $API"
fi
chmod 600 "$DEST/.env" 2>/dev/null || true

# ---- 6. cache dir (persistent npm + node_modules caches)
mkdir -p "$DEST/cache"

# ---- 7. verify (sizes and permissions only; never contents)
[ -s "$DEST/secrets/token" ] || die "secrets/token missing or empty after write"
cmp -s "$TOKEN_SRC" "$DEST/secrets/token" || die "secrets/token does not match the Mac token"
grep -qE '^CPU_RUNNER_API=http' "$DEST/.env" || die ".env has no CPU_RUNNER_API"
[ -d "$DEST/cache" ] || die "cache/ missing"
[ -f "$DEST/agent/agent.py" ] && [ -f "$DEST/docker-compose.yml" ] && [ -f "$DEST/deploy.sh" ] || die "build context incomplete"
say "OK  token: $(size "$DEST/secrets/token") bytes mode $(perm "$DEST/secrets/token") (matches Mac)   .env: mode $(perm "$DEST/.env")   cache/: present"
[ "$(perm "$DEST/secrets/token")" = 600 ] || say "note: share reports token mode $(perm "$DEST/secrets/token") (SMB may not honour chmod); deploy.sh re-applies 600 on Unraid"

# ---- 8. does the container need a rebuild/recreate? (Dockerfile, compose or .env changed since last deploy.sh)
stamp="$(hash_cat "$DEST/Dockerfile" "$DEST/docker-compose.yml" "$DEST/.env")"
if [ "$(cat "$DEST/.deployed-stamp" 2>/dev/null || true)" = "$stamp" ]; then
  say "NO_REDEPLOY_NEEDED: Dockerfile/compose/.env unchanged since last deploy; a running agent reloads new agent code by itself"
else
  say "REDEPLOY_NEEDED: Dockerfile/compose/.env changed (or never deployed). On Unraid run:  cd /mnt/user/data/dispatch-cpu-runner && bash deploy.sh"
fi
