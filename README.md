# dispatch-cpu-runner

A small, security-conscious **CPU job runner** you run as a container. It polls a job-queue API on your LAN, checks the
job's git bundle out into a scratch dir, installs dependencies (cached), runs the job's command **inside an empty network
namespace as an unprivileged user** (no network, no capabilities), and posts the exit code and output tails back. Python
standard library only; Node 26 for JavaScript jobs and a pinned python 3.14 (+ `requirements-runner.txt` packages, sqlite3 CLI)
for python jobs. The agent itself runs on Debian's python.

### Capabilities (what the queue may ship here)
On its first claim and every 60 s the agent sends `caps` in the claim body: `{v, agent, arch, node, node_full, python,
python_full, py_modules[], sqlite3, subpkg_install, setup_phase, llm_relay, artifacts, log_stream, pythons[], uv}` (probed
with the same `node`/`python3` a job gets; `pythons` lists the extra interpreters for project venvs, `llm_relay` is false
when the relay selftest failed at startup, see "Remote lane jobs"). The queue keeps the latest report
per runner and serves it on `GET /api/cpu/runners`; it treats caps older than about 3 minutes as absent, so an older image
that stops reporting cannot inherit them. The queue compares a stage's needs (node >= 23 for `mock.module`, the python
minor, third-party imports vs `py_modules`, sqlite3) with these instead of hard-coding the image. Jobs have no network and
`PIP_NO_INDEX=1`: add a python package by editing `requirements-runner.txt` and updating the container, or build a
per-job venv in `spec.setup` (below).

It is a pull-based worker for a queue **you provide**: it needs a queue API that implements the contract below (a reference
implementation is in `reference/queue_server.py`) and a shared token that the queue issues to the runner through a one-time
enrollment code. It is not useful on its own.

Image: `ghcr.io/frindle/dispatch-cpu-runner:latest` (linux/amd64, linux/arm64; also `:<short-sha>` and `:vX.Y.Z` tags). The
agent code is baked into the image, so updating the container updates the agent.

## Install from the Unraid Docker UI
Prerequisite: the queue API is reachable from Unraid at `http://<queue-host-lan-ip>:7684`.

1. **On the queue host** mint an enrollment code and note the API URL it prints (`cpu_lane.py` is the queue-side module of the
   maintainer's own queue; the enroll and config endpoints are specified in "Queue API contract" if you write your own):

       python3 ~/bin/cpu_lane.py enroll-code        # or: scripts/enroll.sh
       CPU_RUNNER_API=http://<queue-host-lan-ip>:7684
       CPU_RUNNER_ENROLL_CODE=...                     # single use, valid 1 hour

2. **In Unraid**: Docker tab > Add Container. Easiest: Settings > Docker (advanced view) > *Template repositories*, add
   `https://github.com/frindle/dispatch-cpu-runner`, then Add Container and pick **dispatch-cpu-runner**. (Or paste
   `unraid/dispatch-cpu-runner.xml` into `/boot/config/plugins/dockerMan/templates-user/`, or fill the form by hand with
   the repository `ghcr.io/frindle/dispatch-cpu-runner:latest`.)
3. Paste **Queue API URL** and **Enrollment code**. Check Network Type `br0`, Fixed IP `10.0.12.52` (template default) and
   that Extra Parameters contains `--mac-address=02:70:0A:00:0C:34`. Apply.
4. On first start the container exchanges the code for the shared token over the LAN and stores it at
   `/mnt/user/appdata/dispatch-cpu-runner/state/token` (0600). You can now clear the code field. Check
   `docker logs dispatch-cpu-runner` for `enrolled`, `selftest ... PASS`, `api_ok`, and `python3 ~/bin/cpu_lane.py summary`
   on the queue host for the runner.

Paths on Unraid: `/mnt/user/appdata/dispatch-cpu-runner/{state,cache}`. Nothing else is needed on Unraid; there is no file
push from the queue host. Equivalent compose file: `docker-compose.yml` + `.env` (see `env.example`) in the same directory.

### Network identity (fixed IP and MAC)
Containers here normally use the default bridge; this one is a deliberate exception so the firewall/router sees a stable
device: IP **10.0.12.52** on Unraid's `br0` macvlan, MAC **02:70:0A:00:0C:34** (locally administered; the last four octets
encode the IP). If your router quarantines unknown MACs or hands out reservations, allow/reserve that MAC. Macvlan caveat: a
macvlan container cannot talk to the Unraid host itself, only to other LAN hosts (the queue API is on another host, so this
is fine).

### Update
Unraid Docker tab > *Check for updates* > *Apply update* (pulls the new public image, recreates from the template; token
and cache persist). From the queue host: `./update.sh --restart` does the same through the Unraid GraphQL API
(`--dry-run` shows what it would do). Development override: `docker-compose.dev.yml` builds locally and bind-mounts
`./agent` for hot reload (the agent re-execs itself when its file changes).

### Re-enroll / rotate the token
On the queue host: `scripts/enroll.sh --rotate` (rotates the shared token: every runner now gets 401, then mints a code).
Put the new code in the container's *Enrollment code* field and apply. On start the stored (now rejected) token is replaced
automatically. With no fresh code the container logs `token_rejected` with the exact command to run and reports unhealthy.
A pasted `CPU_RUNNER_TOKEN` is a fallback only; it is never rotated for you.

### Settings pulled from the queue
`concurrency`, `lease_s`, `max_job_timeout_s`, `cache_max_entries` and `node_options` are pulled from `GET /api/cpu/config`
at start (queue host: `python3 ~/bin/cpu_lane.py config concurrency=3`, applied on next container start). Any env var
set on the container wins, and isolation is never remote. Container limits (`--cpus=16 --memory=40g`, in Extra Parameters)
bound the total; defaults are 5 concurrent jobs sharing 16 CPUs / 40 GiB (about 3 CPU / 8 GiB each; `NODE_OPTIONS` heap 6 GiB worst case x 5 = 30 GiB fits). The runner reports `slots` on every claim; the queue derives busy/saturated per runner. The node_modules cache is flock-guarded per lockfile key, entries touched within 10 min are never evicted, and every job has its own checkout, so two stages of the same worktree never collide.

## Security
- **The token** is the one secret. It is never in the image, the template, git or logs. It reaches the container over the
  LAN once via a single-use, one-hour code (stored hashed on the queue side, constant-time compared, failures rate limited
  and slowed, requests that look proxied or CDN-fronted are refused) and is stored 0600 in the container's state volume.
  Protect `/mnt/user/appdata/dispatch-cpu-runner/state` like a password file. Jobs never see it (clean job environment).
- The enrollment endpoint sends the token in plaintext HTTP on your LAN; do not enroll over an untrusted network. Use
  `--rotate` if a code or token might have leaked.
- Job code runs with no network (empty netns, loopback only) as uid 10001 with an empty capability set and
  `no-new-privs`. `CPU_RUNNER_ISOLATION=required` (default) makes the agent refuse to start if it cannot prove the
  isolation (startup selftest). No docker socket is mounted.
- The container keeps only `SYS_ADMIN NET_ADMIN SETUID SETGID SETPCAP CHOWN DAC_OVERRIDE FOWNER KILL` (everything else
  dropped), to create the namespace and drop privileges. `SYS_ADMIN` is broad: the safety rests on the wrapper dropping
  everything before job code runs. If namespace creation fails, add `--security-opt seccomp=unconfined`.
- The queue API must be LAN-only and enforce the token itself.
- Model keys never reach the runner: `spec.llm` calls are relayed to the queue host, which adds the key (see "Remote lane
  jobs"). `spec.setup` runs third-party build code WITH network (as the job user, like the dependency install).

## Sizing
Defaults: 5 concurrent jobs (raised from 2 on 2026-10-10; the host is 44 cores / 88 threads / 256 GiB), container limit 16 CPUs / 40 GiB, `NODE_OPTIONS=--max-old-space-size` per job, jobs `nice`d (5).
Scale by raising concurrency, `--cpus` and `--memory` together.

## Layout
- `agent/agent.py` the runner (claim loop x N threads, heartbeat, deps cache, isolation, enrollment, results)
- `client/cpu_job.py` `submit_cpu_job(...)` for the queue side (+ local fallback)
- `reference/queue_server.py` in-memory reference implementation of the API
- `tests/` unit and end-to-end tests (`python3 -W ignore -m unittest discover -s tests`)
- `scripts/enroll.sh` (queue host: mint code / rotate token), `update.sh` + `scripts/unraid_restart.py` (optional Unraid GraphQL update)
- `Dockerfile`, `docker-compose.yml`, `docker-compose.dev.yml`, `env.example`, `unraid/dispatch-cpu-runner.xml` (+ `icon.png`)

## Design
Per job: claim (lease) -> download bundle/patch/tools -> `git clone` bundle into a fresh tmp dir
(+ `git apply --binary` of the uncommitted diff) -> find `package-lock.json` (cwd upward) ->
deps -> run command in an empty network namespace as an unprivileged user -> kill the process
group on timeout -> POST exit code, stdout/stderr tails (64 KB), timings.

- **Shipping**: `git bundle create HEAD` (full history, default) or `mode="archive"` (tree only, much
  smaller; a throwaway git repo with one commit is created on the runner). Dirty + untracked
  (non-ignored) files travel as `git diff --binary` built from a temporary index: the worktree's real
  index is never touched. Extra helper scripts (e.g. `verify-relevance.py` from a tools dir) go in
  `tools={name: path}` and appear in `$JOB_TOOLS`.
- **Dep cache** (`/cache`, bind-mounted from appdata): key = sha256(package-lock.json +
  package.json + prisma/schema.prisma + node major + arch). Miss: `npm ci --prefer-offline` (+ `prisma generate`
  if prisma is a dependency, network allowed in this phase only), then `cp -a node_modules` into the cache.
  Hit: `cp -a` from the cache. Shared npm cache at `/cache/npm`. Per-key flock so concurrent runners don't
  double-install. LRU prune to `CPU_RUNNER_CACHE_MAX` (8). Reported as `timings.deps.cache = hit|miss|none`.
- **Env**: jobs get a clean environment (never the agent's, so the token cannot leak) plus spec env keys
  that match `CPU_RUNNER_ENV_ALLOW` (`VERIFY_*,DISPATCH_*,TEST_*,NODE_ENV,CI,TZ,LANG,LC_*,PRISMA_*,DATABASE_URL`).
- **Timeout**: job in its own session; SIGTERM -> 5 s -> SIGKILL on the group; inside the netns wrapper
  `--pid --fork --kill-child` also tears down every descendant.
- **Lease**: claim returns a `lease_token`; heartbeat every 15 s extends it. 409 on heartbeat = lease lost:
  the job is killed and no result is posted. If a runner dies, the lease lapses and the queue re-queues
  (max 3 attempts, then `failed_infra`). SIGTERM: stop claiming, wait `SHUTDOWN_GRACE_S`, kill, `release`.
- **Logs**: JSON lines on stdout (`docker logs`). Healthcheck = loop heartbeat file.

### Isolation (no network for job code)
Method: the supervising agent (root, but `cap_drop: ALL` + only `SYS_ADMIN NET_ADMIN SETUID SETGID SETPCAP
CHOWN DAC_OVERRIDE FOWNER KILL`) runs each job as

    unshare --net --pid --fork --kill-child -- sh -c 'ip link set lo up; exec "$@"' sh \
      setpriv --reuid=10001 --regid=10001 --clear-groups --bounding-set=-all --inh-caps=-all --no-new-privs <cmd>

i.e. an empty network namespace (loopback only, brought up so localhost test servers work, no DNS/route),
then privileges dropped to uid 10001 with an empty capability bounding set. No docker socket, no sibling
containers. Chosen over `docker run --network none` per job (needs the docker socket = root on the host).
`CPU_RUNNER_ISOLATION=required` (default) makes the agent probe at startup and **refuse to start (exit 3)**
if the wrapper does not produce a namespace containing only `lo`. `selftest` asserts: only `lo`, 1.1.1.1 and a LAN IP
unreachable, DNS fails, loopback bind works, uid != 0, CapEff = 0.
**Verification status**: A CI/dev sandbox container whose seccomp blocks `unshare` could only test the
fail-closed behaviour and command shape; the real namespace is proven by the `selftest` the container runs at every start.
Troubleshooting: if it cannot create a namespace, add `--security-opt seccomp=unconfined` to Extra Parameters (compose: `security_opt`).
The dependency-install phase has network (it must) and runs as the unprivileged user; so does `spec.setup`.

## Remote lane jobs
Optional spec fields for longer jobs that need a prepared environment, model calls, files back and live output. Each
has a cap the queue checks before it sends one (`setup_phase`, `llm_relay`, `artifacts`, `log_stream`). The client
sets them with `submit_cpu_job(..., setup=, llm=, artifacts=, on_log=, on_progress=, cancel_if=)`.

- **`setup`** `{cmd, timeout_s}` (or just the cmd string): runs after deps and before the run, in the job's cwd, as the
  job user WITH network. Under the full wrapper it gets its own pid namespace, so nothing it starts survives into the
  isolated run. Env: the job env without `PIP_NO_INDEX`, plus `UV_CACHE_DIR=/cache/uv` and `UV_PYTHON_DOWNLOADS=never`
  (use the baked interpreters, e.g. `uv venv --python 3.12 .venv && uv pip install -r requirements.txt`). Output goes to
  `out/setup.log`. The timeout is clamped to `CPU_RUNNER_SETUP_TIMEOUT_S` (1800). A failure is an `infra_error`
  `setup failed (rc=N | timed out after Ns): <last 2000 bytes of setup.log>` and the run never starts. Timing: `setup_s`.
- **`llm`** (`true`, or a dict of limits the queue applies): the job gets `CPU_RUNNER_LLM_URL`, an OpenAI-style base URL
  (`http://127.0.0.1:41651/v1` inside the netns). Only `POST /v1/chat/completions` and `GET /v1/models` are forwarded;
  anything else gets a 404 from the relay. Path: job -> `agent.py relay` (in the netns, as the job user) -> unix socket
  `out/llm.sock` (0600) -> agent -> `GET|POST /api/cpu/jobs/<id>/llm/v1/<models|chat/completions>` with the runner token
  and `X-Lease-Token` -> the queue host checks the job, its lease and its limits, adds the model key and forwards. The key
  never reaches the runner and the runner token never reaches the job. Request bodies are capped at 16 MiB; each call
  may take up to `CPU_RUNNER_LLM_TIMEOUT_S` (1200). Timing: `llm: {calls, errors}`. At startup `relay_selftest` proves
  the relay inside a real netns; if it fails, the caps say `llm_relay: false` and only llm jobs are affected.
- **`artifacts`** (`true`): the job gets `CPU_RUNNER_ARTIFACTS`, an empty dir. After the run, the agent packs it with
  `tar -czf` run as the job user (links are stored, never followed), capped at `CPU_RUNNER_ARTIFACTS_MAX` (256 MiB
  compressed), and `PUT`s it with `X-Lease-Token`. The result carries `artifacts: {bytes, sha256, http[, warning]}` or
  `{error}`; an artifacts problem never changes the exit code. The client downloads the tarball and checks size and
  sha256 (`result.artifacts_path`). Extract it with a safe extractor: it is job output.
- **`log_stream`** (`true`): every `CPU_RUNNER_LOG_INTERVAL_S` (3 s) the agent posts new output, `setup.log` first and
  then stdout (stderr is only in the result tail), to `POST jobs/<id>/log`. The queue appends at `off`, so a resent
  chunk is harmless; it answers 416 `{size}` when it holds fewer bytes than `off` (it lost some, e.g. a restart), and the
  agent carries on from `size`. Capped at `CPU_RUNNER_LOG_MAX` (32 MiB) with a `[cpu-runner] log truncated` note. The
  agent stops streaming after 20 failures in a row or on 400/403/404/409/410/413; the job itself carries on.
- **Progress** (every job): `CPU_RUNNER_PROGRESS` names a file; its first line (printable, at most 200 chars) goes out
  with every heartbeat and log post as `progress`, with `phase` (`materialize|deps|setup|running|artifacts|done`).
- **Cancel**: `DELETE jobs/<id>` works in every phase, setup included (the next heartbeat returns `cancel:true`; the job
  is killed and nothing is posted). The client cancels on `cancel_if()` and when its caller is interrupted.

Job dir (`/work/job-<id>-*`, root 0711): `checkout/ home/ tmp/ tools/ art/` belong to the job user; `out/` (root 0711)
holds `stdout`, `stderr` and `setup.log` (created by the agent, written through inherited fds), `progress` (job user) and
`llm.sock`; `dl/` (root 0700) holds the downloaded payload, patch and tools.

Known limits:
- All jobs share uid 10001, so a concurrent job could reach another job's files and relay socket (for example through
  `/proc`) and spend its model calls. Per-slot uids would close this.
- `/tmp`, `/cache/npm` and `/cache/uv` are shared and writable by every job.
- Setup runs third-party build code with network.
- A model call in flight when its job ends still finishes upstream; the queue's per-job limits bound this.
- Without the full wrapper (`unshare --net` only), setup gets no pid namespace: a process that leaves the setup's
  process group (`setsid`) can outlive it.

## Queue API contract (reference: `reference/queue_server.py`)
All under `/api/cpu/`, header `Authorization: Bearer <token>` (compare constant-time; `GET health` and `POST enroll` are
unauthenticated). These routes must be LAN-only and check the token themselves (refuse proxied/CDN-fronted requests).

Producer (client):
- `POST jobs` `{spec,label,stage,bundle_id}` -> 201 `{id}`; status `uploading`.
  spec: `{cmd, cwd, timeout_s, env{}, payload_kind: bundle|archive, lockfile_hash, stage, network: "none"}`, plus
  optional `setup`, `llm`, `artifacts`, `log_stream`, `extra_pkg_dirs`, `owner` (see "Remote lane jobs")
- `PUT jobs/<id>/payload` (bundle or tar), optional `PUT jobs/<id>/patch` (git diff), `PUT jobs/<id>/tools` (tgz)
- `POST jobs/<id>/ready` -> status `pending` (400 if no payload)
- `GET jobs/<id>` -> `{status: uploading|pending|running|done|failed_infra|cancelled, attempt, result, phase, progress,
  log_size, has_artifacts, llm_calls}`
- `GET jobs/<id>/log?off=N` -> the streamed log from byte N (raw), header `X-Log-Size`
- `GET jobs/<id>/artifacts` -> the job's artifacts tarball, or 404
- `DELETE jobs/<id>` -> cancel (pending -> cancelled; running -> heartbeat returns `cancel:true`)

Enrollment and config (agent, see "Enrollment"):
- `POST enroll` `{code, runner_id}` -> 200 `{token, runner_id, config}`; 403 for an unknown/expired/used code; 429 after repeated failures. Codes are single use.
- `GET config` -> `{config: {concurrency, lease_s, max_job_timeout_s, cache_max_entries, node_options}}`; explicit env vars on the runner win; `isolation` is never remote.

Runner (agent):
- `POST claim` `{runner_id, lease_s, caps?}` (`caps`: capability report, see "Capabilities"; optional, unknown fields are ignored) -> 200 `{job:{id, spec(+has_patch,has_tools), attempt, lease_token}}` or 204.
  Atomic; picks oldest `pending`; sets lease_expires = now + lease_s.
- `GET jobs/<id>/payload|patch|tools` -> bytes
- `POST jobs/<id>/heartbeat` `{runner_id, lease_token, lease_s, phase?, progress?}` -> 200 `{cancel: bool}`; 409 if lease_token no longer holds
- `POST jobs/<id>/log` `{runner_id, lease_token, off, data_b64, phase, progress}` -> 200 `{size}`; 409 (lease); 400 (bad
  chunk); 416 `{error: "gap", size}` when `off` is past what the queue holds. Append `data[size-off:]`.
- `PUT jobs/<id>/artifacts` (body: the tarball, header `X-Lease-Token`) -> 200; 409 if the lease no longer holds
- `GET jobs/<id>/llm/v1/models`, `POST jobs/<id>/llm/v1/chat/completions` (header `X-Lease-Token`): 404 unknown job, 409
  lease, 403 the spec has no `llm`, 405 other method, 503 no upstream configured, else the upstream's status and body
  (502 if it is unreachable). The queue adds the model key and enforces its limits here.
- `POST jobs/<id>/result` `{runner_id, lease_token, exit_code, timed_out, stdout_tail, stderr_tail, timings, infra_error?}` -> 200 / 409
- `POST jobs/<id>/release` `{runner_id, lease_token}` -> job back to `pending`
- Server-side reaper (on every request or timer): `running` with lease_expires < now -> `pending` (attempt kept; at 3 -> `failed_infra`).

## Client / integration points
    from cpu_job import submit_cpu_job
    r = submit_cpu_job(worktree, "bash verify.sh", timeout_s=300, stage="final-verify",
                       cwd_rel=None, env={"VERIFY_X": "1"}, tools={"verify-relevance.py": path})
    r.ran_on  # "runner" | "local"; r.exit_code, r.timed_out, r.stdout_tail, r.stderr_tail, r.timings, r.fell_back
Falls back to a local subprocess (same timeout/process-group semantics) if `CPU_RUNNER_API` is unset, the API is
unreachable/unauthorised, no runner claims within `claim_timeout_s` (180), the job exceeds `max_wait_s`, or the
runner reports `infra_error`. `fallback=False` raises instead. Config: `CPU_RUNNER_API`, `CPU_RUNNER_TOKEN_FILE`
(default `~/.config/dispatch-cpu-runner/token`, a file you create).
(ship helper scripts via `tools`). Caveat: the command must be self-contained inside the checkout (+ `$JOB_TOOLS`); the runner has no network for the command itself.

Remote lane jobs add `setup=`, `llm=`, `artifacts=` (+ `artifacts_dest=`), `log_stream=` / `on_log=`, `on_progress=`,
`cancel_if=` and `unreachable_grace_s=` (60). `setup`, `llm` and `artifacts` need `fallback=False`: a local run could
not honour them. CLI: `python3 cpu_job.py WORKTREE CMD [--setup CMD] [--llm] [--artifacts DEST] [--follow]
[--no-fallback]`; SIGTERM cancels the job.

## Tests
`python3 -W ignore -m unittest discover -s tests -v` (needs git and python3 on Linux; no Docker). Covers bundle, dirty and
untracked file shipping with the real index unchanged, archive mode and cwd, env allowlist and token non-leak, dep cache
miss/hit, timeouts, lease expiry requeue, lease-lost handling, local fallback, isolation fail-closed, enrollment (single
use, 0600 storage, rotation, remote config), self-reload, caps, and the remote lane features (`tests/test_remote.py`):
setup env, failure, timeout clamp and cancel; the LLM relay on both paths (in-agent bridge, and the `relay` subcommand
with its ready handshake, with `env` standing in for `unshare`), key and token non-leak, only the two routes forwarded;
artifacts round trip, cap and corrupt download; log streaming, cap and 416 resync; heartbeat progress; job dir modes; and
the queue-side contract of the new routes. The namespace itself is proven by the container's startup selftests.
CI runs these and publishes the image on every push to `main` and on `v*` tags.

## License
MIT, see `LICENSE`.
