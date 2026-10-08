# dispatch-cpu-runner

Purpose-built Unraid container that runs the dispatch pipeline's CPU-only stages
(baseline/final verify, preflight both-ways verify, relevance mutation, harness
self-check, slicer scaffolding) so they stop idling the GPU lanes. Phase 6 of
`~/.claude/plans/so-i-m-getting-really-tender-tiger.md`. Not claude-sandbox (testing only).

Pull-based: the container polls the queue API over the LAN; the Mac never needs to
reach into Unraid (Unraid has no SSH). Python stdlib only; Node 22 in the image for jobs.

## Status
Built and tested (agent, client, reference queue). **Queue side is implemented** (2026-10-08) in
`~/bin/cpu_lane.py` + `~/bin/ollama-queue-api.py` (see "Queue side"), with `~/bin/cpu_dispatch.py`
as the stage-facing wrapper; stages are NOT wired in yet. Real network-namespace
isolation can only be proven inside the container (`selftest`), see "Isolation".

## Sizing (Unraid GraphQL, 2026-10-08)
- CPU: Intel Xeon E5-2699 v4, 44 cores / 88 threads, ~10% utilised.
- RAM: 256 GiB installed (4x64 GiB), ~190-204 GiB available (cache included); 37 containers, 34 running.
  Penn is freeing more memory.
- Defaults: **2 runners** (`CPU_RUNNER_CONCURRENCY`), container limit **8 CPUs / 16 GiB** (`CPU_RUNNER_CPUS`,
  `CPU_RUNNER_MEM`) = ~4 CPU / 8 GiB per job; `NODE_OPTIONS=--max-old-space-size` set per job.
  Median relevance run is 4 s, TS mutants ~72 s, verify <=300 s; 2 slots cover a bundle. Scale by
  raising the three numbers together (e.g. 4 / 16 / 32g). Jobs are `nice`d (5).

## Layout
- `agent/agent.py` the runner (claim loop x N threads, heartbeat, deps cache, isolation, results)
- `client/cpu_job.py` `submit_cpu_job(...)` for the queue side (+ local fallback)
- `reference/queue_server.py` in-memory reference implementation of the API (port this into the queue)
- `tests/test_e2e.py` 14 tests; `scripts/sandbox-e2e.sh` runs them on claude-sandbox and cleans up
- `Dockerfile`, `docker-compose.yml`, `env.example`

## Design
Per job: claim (lease) -> download bundle/patch/tools -> `git clone` bundle into a fresh tmp dir
(+ `git apply --binary` of the uncommitted diff) -> find `package-lock.json` (cwd upward) ->
deps -> run command in an empty network namespace as an unprivileged user -> kill the process
group on timeout -> POST exit code, stdout/stderr tails (64 KB), timings.

- **Shipping**: `git bundle create HEAD` (full history, default) or `mode="archive"` (tree only, much
  smaller; a throwaway git repo with one commit is created on the runner). Dirty + untracked
  (non-ignored) files travel as `git diff --binary` built from a temporary index: the worktree's real
  index is never touched. Extra helper scripts (e.g. `verify-relevance.py` from `~/bin`) go in
  `tools={name: path}` and appear in `$JOB_TOOLS`.
- **Dep cache** (`/cache`, bind-mounted from the data share): key = sha256(package-lock.json +
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
**Verification status**: claude-sandbox is itself a container whose seccomp blocks `unshare`, so only the
fail-closed behaviour and command shape were tested there. Run `selftest` on Unraid (step 4 below).
Troubleshooting: if selftest cannot create a namespace, uncomment `seccomp=unconfined` in the compose.
The dependency-install phase has network (it must) and runs as the unprivileged user.

## Steps for Penn on Unraid
The files are copied to `/mnt/user/data/dispatch-cpu-runner/` (if not already: copy this directory there, minus `.git`).
0. On the Mac (already done once; idempotent): `python3 ~/bin/cpu_lane.py token` creates
   `~/.config/dispatch-cpu-runner/token` (0600, random 32 bytes hex) if absent and prints its PATH only;
   `python3 ~/bin/cpu_lane.py api-url` prints the exact `CPU_RUNNER_API` value (the Mac's LAN IP, discovered,
   not the WARP tunnel address). The queue API (dashboard, :7684) must have been restarted once so it serves
   `/api/cpu/*`; check from any LAN host: `curl http://<mac-lan-ip>:7684/api/cpu/health` -> `{"ok": true}`.
1. Token: copy the Mac's token file to the data share WITHOUT printing it, e.g. on the Mac
   `scp ~/.config/dispatch-cpu-runner/token <unraid>:/mnt/user/data/dispatch-cpu-runner/secrets/token`
   (or paste via the Unraid file browser), then on Unraid `chmod 600 secrets/token`. It must be byte-identical
   to the Mac file (a trailing newline is fine).
2. `cp env.example .env`; set `CPU_RUNNER_API=` to the `api-url` output from step 0; adjust concurrency/limits.
3. `docker compose up -d --build`
4. `docker compose run --rm cpu-runner selftest` -> must print PASS. Then `docker logs -f dispatch-cpu-runner`
   should show `start ... isolation: [unshare,...]`.
Unraid macvlan/bridge: the container needs to reach the Mac's port 7684 (default bridge works). macOS may ask
once to allow incoming connections for python (allow). Verify on the Mac: `python3 ~/bin/cpu_lane.py summary`
shows the runner under `runners` (seen_s_ago < 120) and the dashboard "CPU lane" section reads "1 runner(s) online".
Until a runner has polled in the last 120 s, `run_cpu_stage` runs every stage locally (no 3-minute claim wait).

## Queue side (implemented in `~/bin`, mirrored in machine-config `bin/` + `docs/cpu-lane.md`)
- `cpu_lane.py`: sqlite WAL store `~/.ollama-dispatch/cpu-jobs/jobs.sqlite` + blobs on disk (rows pruned after 7 d,
  blobs of finished jobs after 1 d), lease reaper thread (10 s tick, also on every claim), token handling,
  `outstanding_by_bundle()`, `lan_ip()`.
- `ollama-queue-api.py`: `/api/cpu/*` routes (own bearer token, constant-time compare; requests with Cloudflare /
  proxy headers or a public Host are refused with 403 even with a valid token), read-only `/api/cpu-lane` feed and
  the dashboard "CPU lane" section.
- `ollama-queue.py`: a bundle waiting only on a CPU stage (remote job or local marker) is `waiting`, so the GPU
  commitment is released to other bundles and the bundle resumes first when the result lands.
- `cpu_dispatch.py` `run_cpu_stage(worktree, cmd, timeout_s, stage, bundle_id, lockfile_hash=None, tools=None)`.
- Tests: `tests/test_queue_integration.py` (real agent + real API + wrapper; isolation `none` for the test only),
  `~/bin/test-cpu-lane-api.py`, `~/bin/test-cpu-lane-queue.py` (canary seams `cpuapi`, `cpulane`).

## Queue API contract (reference: `reference/queue_server.py`)
All under `/api/cpu/`, header `Authorization: Bearer <token>` (token from file; compare constant-time;
`GET health` is unauthenticated). Add a CORS-less, Access-bypassed LAN path: ollama-queue-api.py currently
trusts every request because Cloudflare Access fronts it; these routes must check the token themselves.

Producer (client):
- `POST jobs` `{spec,label,stage,bundle_id}` -> 201 `{id}`; status `uploading`.
  spec: `{cmd, cwd, timeout_s, env{}, payload_kind: bundle|archive, lockfile_hash, stage, network: "none"}`
- `PUT jobs/<id>/payload` (bundle or tar), optional `PUT jobs/<id>/patch` (git diff), `PUT jobs/<id>/tools` (tgz)
- `POST jobs/<id>/ready` -> status `pending` (400 if no payload)
- `GET jobs/<id>` -> `{status: uploading|pending|running|done|failed_infra|cancelled, attempt, result}`
- `DELETE jobs/<id>` -> cancel (pending -> cancelled; running -> heartbeat returns `cancel:true`)

Runner (agent):
- `POST claim` `{runner_id, lease_s}` -> 200 `{job:{id, spec(+has_patch,has_tools), attempt, lease_token}}` or 204.
  Atomic; picks oldest `pending`; sets lease_expires = now + lease_s.
- `GET jobs/<id>/payload|patch|tools` -> bytes
- `POST jobs/<id>/heartbeat` `{runner_id, lease_token, lease_s}` -> 200 `{cancel: bool}`; 409 if lease_token no longer holds
- `POST jobs/<id>/result` `{runner_id, lease_token, exit_code, timed_out, stdout_tail, stderr_tail, timings, infra_error?}` -> 200 / 409
- `POST jobs/<id>/release` `{runner_id, lease_token}` -> job back to `pending`
- Server-side reaper (on every request or timer): `running` with lease_expires < now -> `pending` (attempt kept; at 3 -> `failed_infra`).

Queue integration (done): CPU jobs are not queue rows at all, so they never occupy a lane; what used to hold the
GPU was the bundle commitment, which now yields to a bundle that only waits on a CPU stage (see "Queue side").

## Client / integration points
    from cpu_job import submit_cpu_job
    r = submit_cpu_job(worktree, "bash verify.sh", timeout_s=300, stage="final-verify",
                       cwd_rel=None, env={"VERIFY_X": "1"}, tools={"verify-relevance.py": path})
    r.ran_on  # "runner" | "local"; r.exit_code, r.timed_out, r.stdout_tail, r.stderr_tail, r.timings, r.fell_back
Falls back to a local subprocess (same timeout/process-group semantics) if `CPU_RUNNER_API` is unset, the API is
unreachable/unauthorised, no runner claims within `claim_timeout_s` (180), the job exceeds `max_wait_s`, or the
runner reports `infra_error`. `fallback=False` raises instead. Config: `CPU_RUNNER_API`, `CPU_RUNNER_TOKEN_FILE`
(default `~/.config/dispatch-cpu-runner/token`).
Stages to move: baseline verify, final verify, preflight both-ways verify, relevance mutation
(`verify-relevance.py`, ship via `tools`), harness-check (`auto-harness-check.py`), slicer scaffolding
(npm ci / prisma generate are covered by the dep cache). **Not** in-job `run_bash` (latency; stays local).
Caveats: the verify command must be self-contained inside the checkout (+ `$JOB_TOOLS`); it cannot reach
`~/.ollama-dispatch/node_modules-cache` or Mac paths; the runner has no network for the command itself.

## Tests
`python3 -W ignore -m unittest discover -s tests -v` (needs git, python3; no Docker). Covers: bundle + dirty +
untracked + ignored-excluded shipping and unchanged real index, archive mode/cwd, env allowlist and token
non-leak, dep cache miss/hit/lockfile change, timeout kills process group, exit codes/tails, lease expiry
requeue after hard runner kill (attempt 2), lease-lost kills job without posting, local fallback (unreachable,
unconfigured, unclaimed -> cancel, bad token), isolation fail-closed. `scripts/sandbox-e2e.sh` runs them on
claude-sandbox over SSH (no node there: tests use a fake `npm`) and deletes everything afterwards.
