#!/usr/bin/env python3
"""Queue-side client for dispatch-cpu-runner (stdlib only).

    from cpu_job import submit_cpu_job
    res = submit_cpu_job(worktree, "bash verify.sh", timeout_s=300, stage="final-verify")
    res.exit_code, res.stdout_tail, res.ran_on  # "runner" | "local"

Ships `git bundle` of the worktree HEAD (+ an uncommitted/untracked diff built
from a temporary index, so the real index is never touched), waits for the
result, and falls back to a LOCAL subprocess run if the runner is unreachable,
nobody claims the job in time, or the runner reports an infrastructure failure.

Remote lane jobs (README "Remote lane jobs") may also ask for: setup= (a prepare command run WITH network before the
isolated run, e.g. a project venv), llm= (OpenAI-style model calls relayed through the queue host, which holds the key),
artifacts= (files the job leaves in $CPU_RUNNER_ARTIFACTS come back as a checked .tgz), on_log= / on_progress= (live
output and progress line) and cancel_if= (stop it from the caller's side). setup/llm/artifacts never fall back locally.

Config (env, overridable by kwargs): CPU_RUNNER_API, CPU_RUNNER_TOKEN_FILE
(default ~/.config/dispatch-cpu-runner/token) or CPU_RUNNER_TOKEN.
"""
import hashlib, io, json, os, signal, subprocess, sys, tarfile, tempfile, time
import urllib.error, urllib.request
from dataclasses import dataclass, field, asdict
from typing import Optional

DEFAULT_TOKEN_FILE = os.path.expanduser("~/.config/dispatch-cpu-runner/token")


@dataclass
class CpuJobResult:
    exit_code: Optional[int]
    timed_out: bool = False
    stdout_tail: str = ""
    stderr_tail: str = ""
    ran_on: str = "local"            # "runner" | "local"
    job_id: Optional[str] = None
    timings: dict = field(default_factory=dict)
    infra_error: Optional[str] = None  # why the runner path was abandoned / failed
    fell_back: bool = False
    artifacts: Optional[dict] = None   # the runner's report {bytes, sha256, http[, warning]} or {error}, when asked
    artifacts_path: Optional[str] = None  # the fetched .tgz (checked against the report), when there is one

    def to_dict(self):
        return asdict(self)


class _Unreachable(Exception):
    pass


def _token(token=None):
    if token:
        return token
    if os.environ.get("CPU_RUNNER_TOKEN"):
        return os.environ["CPU_RUNNER_TOKEN"]
    f = os.environ.get("CPU_RUNNER_TOKEN_FILE", DEFAULT_TOKEN_FILE)
    try:
        return open(f).read().strip()
    except OSError:
        return ""


def _http(api, tok, method, path, body=None, raw=None, timeout=30):
    data = raw if raw is not None else (json.dumps(body).encode() if body is not None else None)
    req = urllib.request.Request(api.rstrip("/") + path, data=data, method=method)
    req.add_header("Authorization", "Bearer " + tok)
    req.add_header("Content-Type", "application/octet-stream" if raw is not None else "application/json")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            b = r.read()
            return r.status, (json.loads(b) if b else None)
    except urllib.error.HTTPError as e:
        try:
            return e.code, json.loads(e.read())
        except Exception:
            return e.code, None
    except (urllib.error.URLError, OSError, TimeoutError) as e:
        raise _Unreachable(str(e))


def _get_raw(api, tok, path, timeout=60, sink=None):
    """GET raw bytes: (status, bytes, headers); with sink, each chunk goes to sink(b) instead and bytes is None."""
    req = urllib.request.Request(api.rstrip("/") + path)
    req.add_header("Authorization", "Bearer " + tok)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            if sink is None:
                return r.status, r.read(), r.headers
            while True:
                b = r.read(1 << 20)
                if not b:
                    return r.status, None, r.headers
                sink(b)
    except urllib.error.HTTPError as e:
        return e.code, None, e.headers
    except (urllib.error.URLError, OSError, TimeoutError) as e:
        raise _Unreachable(str(e))


def _safe_call(fn, *a):
    try:
        return fn(*a)
    except Exception:           # noqa: BLE001 -- a broken caller callback must never fail or orphan the job
        return None


def _cancel(api, tok, job_id):
    try:
        _http(api, tok, "DELETE", "/api/cpu/jobs/%s" % job_id, timeout=10)
    except Exception:           # noqa: BLE001 -- best effort; an unclaimed job also expires queue-side
        pass


def _pull_log(api, tok, job_id, size, off, on_log):
    """Hand the job's streamed log from off to on_log; returns the new offset."""
    if not isinstance(size, int):
        return off
    if size < off:              # the queue's copy restarted (it lost bytes): all it holds now is new
        off = 0
    if size <= off:
        return off
    try:
        code, data, _ = _get_raw(api, tok, "/api/cpu/jobs/%s/log?off=%d" % (job_id, off), timeout=60)
    except _Unreachable:
        return off
    if code != 200 or not data:
        return off
    _safe_call(on_log, data)
    return off + len(data)


def _fetch_artifacts(api, tok, job_id, report, dest):
    """Download the job's artifacts .tgz to dest (default: a new temp file the caller owns) and check it against the
    runner's report. Returns (path, error)."""
    if dest is None:
        fd, dest = tempfile.mkstemp(prefix="cpu-job-%s-" % job_id[:12], suffix=".tgz")
        os.close(fd)
    h, n = hashlib.sha256(), [0]
    code = None
    try:
        with open(dest, "wb") as f:
            def sink(b):
                h.update(b)
                n[0] += len(b)
                f.write(b)
            code, _, _ = _get_raw(api, tok, "/api/cpu/jobs/%s/artifacts" % job_id, timeout=300, sink=sink)
    except (_Unreachable, OSError) as e:
        code = "error (%s)" % e
    if code == 200 and n[0] == report.get("bytes") and h.hexdigest() == report.get("sha256"):
        return dest, None
    try:
        os.unlink(dest)
    except OSError:
        pass
    return None, "artifacts download failed: http %s, %d bytes, sha256 %s (runner reported %s bytes, %s)" % (
        code, n[0], h.hexdigest()[:16], report.get("bytes"), str(report.get("sha256"))[:16])


def _setup_timeout(setup):
    if not setup:
        return 0
    try:
        return int((setup.get("timeout_s") if isinstance(setup, dict) else None) or 1800)
    except (TypeError, ValueError):
        return 1800


def _git(wt, *args, env=None, check=True):
    r = subprocess.run(["git"] + list(args), cwd=wt, capture_output=True, env=env)
    if check and r.returncode:
        raise RuntimeError("git %s failed: %s" % (args[0], r.stderr[-300:].decode("utf-8", "replace")))
    return r.stdout


def make_payload(worktree, mode="bundle"):
    """Return (payload_bytes, patch_bytes_or_empty, payload_kind). Never mutates the worktree or its index."""
    _git(worktree, "rev-parse", "--verify", "HEAD")
    if mode == "bundle":
        with tempfile.TemporaryDirectory() as td:
            bp = os.path.join(td, "b.bundle")
            _git(worktree, "bundle", "create", bp, "HEAD")
            payload = open(bp, "rb").read()
        kind = "bundle"
    elif mode == "archive":
        payload = _git(worktree, "archive", "--format=tar", "HEAD")
        kind = "archive"
    else:
        raise ValueError("mode must be bundle|archive")
    # uncommitted + untracked(non-ignored) changes, via a throwaway index
    gitdir = _git(worktree, "rev-parse", "--absolute-git-dir").decode().strip()
    with tempfile.TemporaryDirectory() as td:
        env = dict(os.environ, GIT_INDEX_FILE=os.path.join(td, "idx"))
        _git(worktree, "read-tree", "HEAD", env=env)
        _git(worktree, "add", "-A", "--", ".", env=env)
        patch = _git(worktree, "diff", "--cached", "--binary", "HEAD", env=env)
    return payload, patch, kind


def make_tools_tgz(tools):
    """tools: {relative_name: local_path_or_bytes}; extracted on the runner to $JOB_TOOLS."""
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as tf:
        for name, src in tools.items():
            data = src if isinstance(src, bytes) else open(src, "rb").read()
            ti = tarfile.TarInfo(name)
            ti.size, ti.mode = len(data), 0o755
            tf.addfile(ti, io.BytesIO(data))
    return buf.getvalue()


def run_local(worktree, cmd, timeout_s, cwd_rel=None, env=None, tail_bytes=65536):
    """Local fallback: same semantics as the runner (timeout kills the process group)."""
    cwd = os.path.join(worktree, cwd_rel) if cwd_rel else worktree
    argv = ["bash", "-c", cmd] if isinstance(cmd, str) else list(cmd)
    e = dict(os.environ)
    e.update({k: str(v) for k, v in (env or {}).items()})
    t0 = time.time()
    with tempfile.TemporaryFile() as so, tempfile.TemporaryFile() as se:
        p = subprocess.Popen(argv, cwd=cwd, env=e, stdout=so, stderr=se, stdin=subprocess.DEVNULL,
                             start_new_session=True)
        timed_out = False
        try:
            p.wait(timeout=timeout_s)
        except subprocess.TimeoutExpired:
            timed_out = True
            for sig in (signal.SIGTERM, signal.SIGKILL):
                try:
                    os.killpg(p.pid, sig)
                except ProcessLookupError:
                    break
                try:
                    p.wait(timeout=5)
                    break
                except subprocess.TimeoutExpired:
                    pass
            p.wait()

        def tail(f):
            f.seek(0, 2)
            n = f.tell()
            f.seek(max(0, n - tail_bytes))
            return f.read().decode("utf-8", "replace")
        return CpuJobResult(exit_code=p.returncode, timed_out=timed_out, stdout_tail=tail(so),
                            stderr_tail=tail(se), ran_on="local", timings={"run_s": round(time.time() - t0, 2)})


def submit_cpu_job(worktree_path, cmd, timeout_s=600, *, cwd_rel=None, env=None, stage=None, label=None,
                   bundle_id=None, tools=None, mode="bundle", api=None, token=None,
                   claim_timeout_s=180, poll_s=2.0, fallback=True, max_wait_s=None, lockfile_hash=None,
                   should_abort=None, owner=None, extra_pkg_dirs=None, setup=None, llm=None, artifacts=False,
                   artifacts_dest=None, log_stream=False, on_log=None, on_progress=None, cancel_if=None,
                   unreachable_grace_s=60):
    """Run `cmd` for a worktree on the CPU runner; return CpuJobResult.

    claim_timeout_s: if no runner claims the job within this long, cancel and run locally.
    max_wait_s: hard cap waiting for a claimed job (default timeout_s + 600).
    fallback=False raises RuntimeError instead of running locally.
    should_abort: optional callable polled (~every 10s) while the job is still pending; a truthy return (a reason
    string) cancels the job and takes the fallback -- e.g. the runner's heartbeat went stale while we queued.
    lockfile_hash: override the dep-cache key hint (default: sha256 of package-lock.json at cwd_rel).

    Remote lane features (each needs a runner that reports the matching cap; see README "Remote lane jobs"):
    setup: a command (str / argv) or {"cmd", "timeout_s"} run on the runner as the job user WITH network after deps
      and before the isolated run (e.g. build a project venv). A failed setup is a runner failure carrying its log tail.
    llm: True, or a dict of queue-side limits (e.g. {"models": [...], "max_calls": 200}); the job gets
      CPU_RUNNER_LLM_URL, an OpenAI-style base URL relayed through the queue host (the key never reaches the runner).
    artifacts: the job leaves files in $CPU_RUNNER_ARTIFACTS; they come back as a .tgz at result.artifacts_path
      (artifacts_dest, else a temp file the caller owns), checked against the runner's sha256. Problems land in
      result.artifacts["error"]; they never change the job's exit code.
    log_stream / on_log(bytes) / on_progress(phase, progress): live setup + run output (stdout) and the job's
      progress line ($CPU_RUNNER_PROGRESS); on_log implies log_stream.
    cancel_if: callable polled every poll_s in ANY state; a truthy return (a reason) cancels the job and returns
      infra_error "cancelled: <reason>" without falling back.
    unreachable_grace_s: how long the queue API may stay unreachable while we wait before giving up on the job.
    setup / llm / artifacts need fallback=False (a local run could not honour them).
    """
    if fallback and (setup or llm or artifacts):
        raise ValueError("setup/llm/artifacts jobs cannot fall back to a local run: pass fallback=False")
    api = api or os.environ.get("CPU_RUNNER_API")
    tok = _token(token)
    worktree_path = os.path.abspath(worktree_path)

    def fb(why):
        if not fallback:
            raise RuntimeError("cpu runner unavailable: " + why)
        r = run_local(worktree_path, cmd, timeout_s, cwd_rel, env)
        r.fell_back, r.infra_error = True, why
        return r

    if not api:
        return fb("CPU_RUNNER_API not configured")
    job_id = None
    live = False            # the job exists queue-side and may still run: cancel it if we leave early
    try:
        code, _ = _http(api, tok, "GET", "/api/cpu/health", timeout=5)
        if code != 200:
            return fb("health http %s" % code)
        payload, patch, kind = make_payload(worktree_path, mode)
        lock = os.path.join(worktree_path, cwd_rel or ".", "package-lock.json")
        lock_hash = lockfile_hash or (
            hashlib.sha256(open(lock, "rb").read()).hexdigest()[:24] if os.path.exists(lock) else None)
        spec = {"cmd": cmd, "cwd": cwd_rel or ".", "timeout_s": int(timeout_s), "env": env or {},
                "payload_kind": kind, "lockfile_hash": lock_hash, "stage": stage, "network": "none"}
        if extra_pkg_dirs:   # sub-packages with their own package-lock.json: the runner's dep phase installs them too
            spec["extra_pkg_dirs"] = [str(d) for d in extra_pkg_dirs][:4]
        if owner:        # additive display provenance (who submitted this stage); absent == old behaviour
            spec["owner"] = str(owner)[:160]
        if setup:
            spec["setup"] = dict(setup) if isinstance(setup, dict) else {"cmd": setup}
        if llm:
            spec["llm"] = dict(llm) if isinstance(llm, dict) else True
        if artifacts:
            spec["artifacts"] = True
        if log_stream or on_log is not None:
            spec["log_stream"] = True
        code, body = _http(api, tok, "POST", "/api/cpu/jobs",
                           {"spec": spec, "label": label, "stage": stage, "bundle_id": bundle_id})
        if code != 201:
            return fb("create http %s" % code)
        job_id = body["id"]
        live = True
        blobs = [("payload", payload)]
        if patch:
            blobs.append(("patch", patch))
        if tools:
            blobs.append(("tools", make_tools_tgz(tools)))
        for name, data in blobs:
            code, _ = _http(api, tok, "PUT", "/api/cpu/jobs/%s/%s" % (job_id, name), raw=data, timeout=300)
            if code != 200:
                raise _Unreachable("upload %s http %s" % (name, code))
        # has_patch / has_tools are derived server-side from the uploaded blobs
        code, _ = _http(api, tok, "POST", "/api/cpu/jobs/%s/ready" % job_id, {})
        if code != 200:
            raise _Unreachable("ready http %s" % code)

        t0 = time.time()
        max_wait = max_wait_s or (timeout_s + _setup_timeout(setup) + 600)
        claimed_at = None
        last_abort_chk = t0
        log_off, seen, down_since = 0, (None, None), None
        while True:
            why_cancel = _safe_call(cancel_if) if cancel_if is not None else None
            if why_cancel:
                _cancel(api, tok, job_id)
                live = False
                return CpuJobResult(exit_code=None, ran_on="runner", job_id=job_id,
                                    infra_error="cancelled: %s" % why_cancel)
            try:
                code, st = _http(api, tok, "GET", "/api/cpu/jobs/%s" % job_id)
            except _Unreachable:
                down_since = down_since or time.time()
                if time.time() - down_since > unreachable_grace_s:
                    raise
                time.sleep(poll_s)
                continue
            down_since = None
            if code == 404:
                live = False
                return fb("job %s vanished from the queue" % job_id)
            st = st if isinstance(st, dict) else {}
            s = st.get("status")
            if on_progress is not None and (st.get("phase") or st.get("progress")):
                cur = (st.get("phase"), st.get("progress"))
                if cur != seen:
                    seen = cur
                    _safe_call(on_progress, *cur)
            if on_log is not None:          # also drains the rest once the job is done (posted before the result)
                log_off = _pull_log(api, tok, job_id, st.get("log_size"), log_off, on_log)
            if s in ("done", "failed_infra", "cancelled"):
                live = False
                break
            now = time.time()
            if s == "pending" and should_abort is not None and now - last_abort_chk >= 10:
                last_abort_chk = now
                why_abort = _safe_call(should_abort)   # a broken probe must never fail the job
                if why_abort:
                    _cancel(api, tok, job_id)
                    live = False
                    return fb(str(why_abort))
            if s == "pending" and now - t0 > claim_timeout_s:
                _cancel(api, tok, job_id)
                live = False
                return fb("no runner claimed within %ss" % claim_timeout_s)
            if s == "running":
                claimed_at = claimed_at or now
            if claimed_at and now - claimed_at > max_wait:
                _cancel(api, tok, job_id)
                live = False
                return fb("job exceeded max_wait %ss" % max_wait)
            time.sleep(poll_s)
        res = st.get("result") or {}
        if s != "done" or res.get("infra_error"):
            return fb("runner failure: %s" % (res.get("infra_error") or s))
        out = CpuJobResult(exit_code=res.get("exit_code"), timed_out=bool(res.get("timed_out")),
                           stdout_tail=res.get("stdout_tail", ""), stderr_tail=res.get("stderr_tail", ""),
                           ran_on="runner", job_id=job_id, timings=res.get("timings") or {})
        if artifacts:
            rep = res.get("artifacts")
            out.artifacts = dict(rep) if isinstance(rep, dict) else {"error": "the runner sent no artifacts report"}
            if not out.artifacts.get("error"):
                out.artifacts_path, err = _fetch_artifacts(api, tok, job_id, out.artifacts, artifacts_dest)
                if err:
                    out.artifacts["error"] = err
        return out
    except _Unreachable as e:
        if job_id and live:
            live = False
            _cancel(api, tok, job_id)
        return fb(str(e))
    finally:
        if job_id and live:     # interrupted (KeyboardInterrupt, SystemExit from a signal handler...): never orphan it
            _cancel(api, tok, job_id)


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser(description="Run a command for a worktree on the CPU runner (local fallback)")
    ap.add_argument("worktree")
    ap.add_argument("cmd")
    ap.add_argument("--timeout", type=int, default=600)
    ap.add_argument("--cwd-rel")
    ap.add_argument("--stage")
    ap.add_argument("--setup", help="prepare command run on the runner WITH network before the isolated run")
    ap.add_argument("--llm", action="store_true", help="relay model calls through the queue host (CPU_RUNNER_LLM_URL)")
    ap.add_argument("--artifacts", metavar="DEST", help="fetch $CPU_RUNNER_ARTIFACTS back as a .tgz at DEST")
    ap.add_argument("--follow", action="store_true", help="stream the job's output and progress to stderr")
    ap.add_argument("--no-fallback", action="store_true", help="never run locally")
    a = ap.parse_args()
    stop = []
    signal.signal(signal.SIGTERM, lambda *_: stop.append("SIGTERM"))

    def show(b):
        sys.stderr.buffer.write(b)
        sys.stderr.flush()
    try:
        r = submit_cpu_job(a.worktree, a.cmd, a.timeout, cwd_rel=a.cwd_rel, stage=a.stage, setup=a.setup,
                           llm=a.llm, artifacts=bool(a.artifacts), artifacts_dest=a.artifacts,
                           fallback=not (a.no_fallback or a.setup or a.llm or a.artifacts),
                           on_log=show if a.follow else None,
                           on_progress=(lambda ph, pr: print("[cpu_job] %s %s" % (ph, pr or ""), file=sys.stderr))
                           if a.follow else None,
                           cancel_if=lambda: stop and stop[0])
    except RuntimeError as e:
        print("[cpu_job] %s" % e, file=sys.stderr)
        sys.exit(3)
    if not a.follow:
        sys.stdout.write(r.stdout_tail)
    sys.stderr.write(r.stderr_tail)
    print("[cpu_job] ran_on=%s exit=%s timed_out=%s fell_back=%s %s%s" % (
        r.ran_on, r.exit_code, r.timed_out, r.fell_back, r.infra_error or "",
        (" artifacts=%s" % (r.artifacts_path or r.artifacts)) if a.artifacts else ""), file=sys.stderr)
    sys.exit(r.exit_code if r.exit_code is not None else 1)
