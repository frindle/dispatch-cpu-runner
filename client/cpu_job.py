#!/usr/bin/env python3
"""Queue-side client for dispatch-cpu-runner (stdlib only).

    from cpu_job import submit_cpu_job
    res = submit_cpu_job(worktree, "bash verify.sh", timeout_s=300, stage="final-verify")
    res.exit_code, res.stdout_tail, res.ran_on  # "runner" | "local"

Ships `git bundle` of the worktree HEAD (+ an uncommitted/untracked diff built
from a temporary index, so the real index is never touched), waits for the
result, and falls back to a LOCAL subprocess run if the runner is unreachable,
nobody claims the job in time, or the runner reports an infrastructure failure.

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
                   claim_timeout_s=180, poll_s=2.0, fallback=True, max_wait_s=None):
    """Run `cmd` for a worktree on the CPU runner; return CpuJobResult.

    claim_timeout_s: if no runner claims the job within this long, cancel and run locally.
    max_wait_s: hard cap waiting for a claimed job (default timeout_s + 600).
    fallback=False raises RuntimeError instead of running locally.
    """
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
    try:
        code, _ = _http(api, tok, "GET", "/api/cpu/health", timeout=5)
        if code != 200:
            return fb("health http %s" % code)
        payload, patch, kind = make_payload(worktree_path, mode)
        lock = os.path.join(worktree_path, cwd_rel or ".", "package-lock.json")
        lock_hash = hashlib.sha256(open(lock, "rb").read()).hexdigest()[:24] if os.path.exists(lock) else None
        spec = {"cmd": cmd, "cwd": cwd_rel or ".", "timeout_s": int(timeout_s), "env": env or {},
                "payload_kind": kind, "lockfile_hash": lock_hash, "stage": stage, "network": "none"}
        code, body = _http(api, tok, "POST", "/api/cpu/jobs",
                           {"spec": spec, "label": label, "stage": stage, "bundle_id": bundle_id})
        if code != 201:
            return fb("create http %s" % code)
        job_id = body["id"]
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
        max_wait = max_wait_s or (timeout_s + 600)
        claimed_at = None
        while True:
            code, st = _http(api, tok, "GET", "/api/cpu/jobs/%s" % job_id)
            s = (st or {}).get("status")
            if s in ("done", "failed_infra", "cancelled"):
                break
            now = time.time()
            if s == "pending" and now - t0 > claim_timeout_s:
                _http(api, tok, "DELETE", "/api/cpu/jobs/%s" % job_id)
                return fb("no runner claimed within %ss" % claim_timeout_s)
            if s == "running":
                claimed_at = claimed_at or now
            if claimed_at and now - claimed_at > max_wait:
                _http(api, tok, "DELETE", "/api/cpu/jobs/%s" % job_id)
                return fb("job exceeded max_wait %ss" % max_wait)
            time.sleep(poll_s)
        res = (st or {}).get("result") or {}
        if s != "done" or res.get("infra_error"):
            return fb("runner failure: %s" % (res.get("infra_error") or s))
        return CpuJobResult(exit_code=res.get("exit_code"), timed_out=bool(res.get("timed_out")),
                            stdout_tail=res.get("stdout_tail", ""), stderr_tail=res.get("stderr_tail", ""),
                            ran_on="runner", job_id=job_id, timings=res.get("timings") or {})
    except _Unreachable as e:
        if job_id:
            try:
                _http(api, tok, "DELETE", "/api/cpu/jobs/%s" % job_id, timeout=5)
            except Exception:
                pass
        return fb(str(e))


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser(description="Run a command for a worktree on the CPU runner (local fallback)")
    ap.add_argument("worktree")
    ap.add_argument("cmd")
    ap.add_argument("--timeout", type=int, default=600)
    ap.add_argument("--cwd-rel")
    ap.add_argument("--stage")
    a = ap.parse_args()
    r = submit_cpu_job(a.worktree, a.cmd, a.timeout, cwd_rel=a.cwd_rel, stage=a.stage)
    sys.stdout.write(r.stdout_tail)
    sys.stderr.write(r.stderr_tail)
    print("[cpu_job] ran_on=%s exit=%s timed_out=%s fell_back=%s %s" % (
        r.ran_on, r.exit_code, r.timed_out, r.fell_back, r.infra_error or ""), file=sys.stderr)
    sys.exit(r.exit_code if r.exit_code is not None else 1)
