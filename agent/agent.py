#!/usr/bin/env python3
"""dispatch-cpu-runner agent: pull-based CPU job runner (stdlib only).

Loop (x CONCURRENCY threads): claim -> download payload -> materialize a fresh
checkout -> restore/populate the node_modules cache (keyed by lockfile hash) ->
run the command with a timeout inside an empty network namespace -> post the
result. A heartbeat thread keeps the lease alive; if the lease is lost (409) or
the producer cancels, the job's process group is killed and no result is posted.

Optional per-job features (each off unless the spec asks; see README "Remote lane jobs"):
  spec.setup       {cmd, timeout_s}: a preparation command run as the job user WITH network after deps (out/setup.log)
  spec.llm         {...}: relay the job's OpenAI-style calls (POST /v1/chat/completions, GET /v1/models) from its empty
                   netns to the queue host, which adds the model key; the job sees CPU_RUNNER_LLM_URL
  spec.artifacts   true: tar.gz of $CPU_RUNNER_ARTIFACTS (made as the job user, no link following) PUT before the result
  spec.log_stream  true: setup.log then stdout, plus the job's progress line ($CPU_RUNNER_PROGRESS), posted every few seconds

Limits (README "Disk use"): at most CPU_RUNNER_LANE_MAX lane jobs (spec.setup / spec.llm) run at once; below
CPU_RUNNER_MIN_FREE_GB free on /work or /cache no lane job is claimed (below 2 GB no job at all); the uv / npm /
node_modules caches are kept under their caps. Each claim tells the queue what this runner takes (accept) and why it
takes less (hold), so a waiting job shows the reason; the disk thread reports /work and cache use with the claims.
CPU_RUNNER_ACCEPT=lane makes a lane-only runner (say a second one beside the main runner): it claims lane jobs only and
reports hold "plain", so a plain job waiting on it says why.

Subcommands: run (default) | selftest | healthcheck | relay HOST:PORT SOCKET (internal: the in-netns end of the LLM relay)
Config is env (see README / env.example); a few tunables can also be pulled from the queue API (env wins).
The shared token comes from CPU_RUNNER_TOKEN_FILE / CPU_RUNNER_TOKEN, else from the state dir (CPU_RUNNER_STATE_DIR/token),
which the agent fills itself by enrolling once with CPU_RUNNER_ENROLL_CODE (single-use code minted on the queue host).
"""
import base64, fnmatch, re, hashlib, io, json, os, pwd, shlex, shutil, signal, socket, socketserver
import subprocess, sys, tarfile, tempfile, threading, time, urllib.error, urllib.request
import fcntl
from http.server import BaseHTTPRequestHandler

VERSION = "1.5.0"


def env(name, default=None, cast=str):
    v = os.environ.get(name)
    if v is None or v == "":
        return default
    return cast(v)


class Config:
    def __init__(self):
        self.api = (env("CPU_RUNNER_API", "http://127.0.0.1:7684")).rstrip("/")
        self.state_dir = env("CPU_RUNNER_STATE_DIR", "/state")
        self.enroll_code = (env("CPU_RUNNER_ENROLL_CODE", "") or "").strip()
        self.token = self._token()
        self.runner_id = env("CPU_RUNNER_ID", socket.gethostname())
        self.concurrency = max(1, env("CPU_RUNNER_CONCURRENCY", 5, int))
        self.cache_dir = env("CPU_RUNNER_CACHE", "/cache")
        self.work_dir = env("CPU_RUNNER_WORK", "/work")
        self.poll_s = env("CPU_RUNNER_POLL_S", 3.0, float)
        self.lease_s = env("CPU_RUNNER_LEASE_S", 60, int)
        self.heartbeat_s = env("CPU_RUNNER_HEARTBEAT_S", 15.0, float)
        self.install_timeout_s = env("CPU_RUNNER_INSTALL_TIMEOUT_S", 900, int)
        self.max_job_timeout_s = env("CPU_RUNNER_MAX_TIMEOUT_S", 3600, int)
        self.tail_bytes = env("CPU_RUNNER_TAIL_BYTES", 65536, int)
        self.cache_max_entries = env("CPU_RUNNER_CACHE_MAX", 8, int)
        self.shutdown_grace_s = env("CPU_RUNNER_SHUTDOWN_GRACE_S", 30, int)
        # required: refuse to run unless the empty-netns wrapper works (default)
        # auto: use it when available, else run unisolated (logged loudly)
        # none: never isolate (tests / sandbox only)
        self.isolation = env("CPU_RUNNER_ISOLATION", "required")
        self.install_cmd = env("CPU_RUNNER_INSTALL_CMD", "npm ci --prefer-offline --no-audit --no-fund")
        self.prisma_cmd = env("CPU_RUNNER_PRISMA_CMD", "npx --no-install prisma generate")
        self.env_allow = [p.strip() for p in env(
            "CPU_RUNNER_ENV_ALLOW",
            "VERIFY_*,DISPATCH_*,TEST_*,NODE_ENV,CI,TZ,LANG,LC_*,PRISMA_*,DATABASE_URL").split(",") if p.strip()]
        self.node_options = env("CPU_RUNNER_NODE_OPTIONS", "--max-old-space-size=3072")
        self.nice = env("CPU_RUNNER_NICE", 5, int)
        self.job_user = env("CPU_RUNNER_JOB_USER", "runner")
        self.health_file = env("CPU_RUNNER_HEALTH_FILE", "/tmp/cpu-runner.health")
        self.status_file = env("CPU_RUNNER_STATUS_FILE", "/tmp/cpu-runner.status")
        # code reload: the agent watches its own file; on change it drains (finishes leased jobs,
        # up to reload_grace_s, then releases them) and re-execs itself. 0 disables.
        self.reload_check_s = env("CPU_RUNNER_RELOAD_CHECK_S", 5.0, float)
        self.reload_grace_s = env("CPU_RUNNER_RELOAD_GRACE_S", 1800, int)
        # per-job features (spec.setup / spec.llm / spec.artifacts / spec.log_stream)
        self.setup_timeout_s = env("CPU_RUNNER_SETUP_TIMEOUT_S", 1800, int)
        self.artifacts_max = env("CPU_RUNNER_ARTIFACTS_MAX", 256 << 20, int)
        self.log_interval_s = env("CPU_RUNNER_LOG_INTERVAL_S", 3.0, float)
        self.log_max = env("CPU_RUNNER_LOG_MAX", 32 << 20, int)
        self.llm_timeout_s = env("CPU_RUNNER_LLM_TIMEOUT_S", 1200, int)
        # disk guards (see LOW_DISK_ANY_GB and README "Disk use"); remote-tunable like the settings in REMOTE
        self.lane_max = max(0, env("CPU_RUNNER_LANE_MAX", 2, int))
        self.min_free_gb = max(0.0, env("CPU_RUNNER_MIN_FREE_GB", 6.0, float))
        self.uv_cache_max_gb = env("CPU_RUNNER_UV_CACHE_MAX_GB", 10.0, float)
        self.npm_cache_max_gb = env("CPU_RUNNER_NPM_CACHE_MAX_GB", 4.0, float)
        self.nm_cache_max_gb = env("CPU_RUNNER_NM_CACHE_MAX_GB", 6.0, float)
        self.disk_check_s = max(5.0, env("CPU_RUNNER_DISK_CHECK_S", 120.0, float))
        # what this runner takes: "all" (default) or "lane" (lane jobs only; checked at startup)
        self.accept = (env("CPU_RUNNER_ACCEPT", "all") or "all").strip().lower()

    # remote-tunable settings: API key -> (attr, env var that overrides it, cast, lo, hi)
    REMOTE = {"concurrency": ("concurrency", "CPU_RUNNER_CONCURRENCY", int, 1, 32),
              "lease_s": ("lease_s", "CPU_RUNNER_LEASE_S", int, 15, 600),
              "max_job_timeout_s": ("max_job_timeout_s", "CPU_RUNNER_MAX_TIMEOUT_S", int, 30, 21600),
              "cache_max_entries": ("cache_max_entries", "CPU_RUNNER_CACHE_MAX", int, 1, 64),
              "poll_s": ("poll_s", "CPU_RUNNER_POLL_S", float, 0.5, 60),
              "lane_max": ("lane_max", "CPU_RUNNER_LANE_MAX", int, 0, 32),
              "min_free_gb": ("min_free_gb", "CPU_RUNNER_MIN_FREE_GB", float, 0, 10000),
              "uv_cache_max_gb": ("uv_cache_max_gb", "CPU_RUNNER_UV_CACHE_MAX_GB", float, 0.5, 10000),
              "npm_cache_max_gb": ("npm_cache_max_gb", "CPU_RUNNER_NPM_CACHE_MAX_GB", float, 0.5, 10000),
              "nm_cache_max_gb": ("nm_cache_max_gb", "CPU_RUNNER_NM_CACHE_MAX_GB", float, 0.5, 10000)}

    def apply_remote(self, conf):
        """Apply API-provided settings. Explicit env vars always win; values are clamped; isolation is never remote."""
        applied = {}
        if not isinstance(conf, dict):
            return applied
        for key, (attr, envname, cast, lo, hi) in self.REMOTE.items():
            if key not in conf or env(envname) is not None:
                continue
            try:
                v = min(hi, max(lo, cast(conf[key])))
            except (TypeError, ValueError):
                continue
            setattr(self, attr, v)
            applied[key] = v
        no = conf.get("node_options")
        if env("CPU_RUNNER_NODE_OPTIONS") is None and isinstance(no, str) and re.fullmatch(r"(--[a-z-]+=[\w.]+ ?)+", no.strip()):
            self.node_options = no.strip()
            applied["node_options"] = self.node_options
        return applied

    @property
    def stored_token_path(self):
        return os.path.join(self.state_dir, "token")

    def _token(self):
        """Token from CPU_RUNNER_TOKEN_FILE, CPU_RUNNER_TOKEN, or the stored enrolled token (state dir).
        Never invented here. A missing source leaves token "" and sets token_error (-> enroll or exit)."""
        self.token_error = None
        self.token_source = None
        f = env("CPU_RUNNER_TOKEN_FILE")
        if f and os.path.exists(f):
            try:
                with open(f) as fh:
                    tok = fh.read().strip()
            except OSError as e:
                self.token_error = "cannot read token file %s (%s)" % (f, e.strerror or e)
                return ""
            if not tok:
                self.token_error = "token file %s is empty" % f
            self.token_source = "file"
            return tok
        tok = env("CPU_RUNNER_TOKEN", "")
        if tok:
            self.token_source = "env"
            return tok
        try:
            with open(self.stored_token_path) as fh:
                tok = fh.read().strip()
            if tok:
                self.token_source = "stored"
                return tok
        except OSError:
            pass
        self.token_error = ("no token: CPU_RUNNER_TOKEN_FILE %s not found, CPU_RUNNER_TOKEN unset, nothing stored in %s"
                            % (f, self.state_dir)) if f else ("no token: CPU_RUNNER_TOKEN unset, nothing stored in %s" % self.state_dir)
        return ""


PRUNE_PROTECT_S = 600     # node_modules cache entries touched this recently are never evicted (concurrent jobs)

# Disk guards (README "Disk use"). A lane job (spec.setup / spec.llm: a project venv plus a model loop) can take a few
# GB of /work, so a runner runs at most lane_max of them at once and claims one only with min_free_gb free on both
# /work and /cache. The caches under /cache are capped: uv (Python packages) and npm are deleted whole once over their
# cap (while no job runs), node_modules copies (nm) are evicted oldest first.
LOW_DISK_ANY_GB = 2.0     # below this much free space (or min_free_gb when lower) the runner claims nothing at all
FREE_RECHECK_S = 5.0      # free space is re-read at most this often (every claim asks)
REFUSE_BACKOFF_S = 30     # after handing back a job that an older queue gave out against the claim's `accept`
LANE_ONLY_HOLD = "lane-only runner (CPU_RUNNER_ACCEPT=lane)"   # its standing hold "plain"
CACHE_REMEASURE_S = 6 * 3600   # the caches are walked after a job that touched them, else only this often
CLEAN_RETRY_S = 3600      # a cache still over its cap after being deleted is left alone this long (logged)
CACHES = ("uv", "npm", "nm")


def is_lane_job(spec):
    """A remote lane job: it builds a project venv (spec.setup) and/or runs a model loop (spec.llm). These are the big
    ones on disk and the long ones; the queue applies the same test to the claim's `accept`."""
    return isinstance(spec, dict) and bool(spec.get("setup") or spec.get("llm"))


def free_gb(path):
    """Free space in GB on path's filesystem (what an unprivileged writer can use); None when it cannot be read."""
    try:
        st = os.statvfs(path)
    except OSError:
        return None
    return st.f_bavail * st.f_frsize / 1e9


def du_bytes(path):
    """Bytes allocated under path: symlinks are not followed, unreadable entries are skipped, a missing path is 0."""
    total, stack = 0, [path]
    while stack:
        try:
            it = os.scandir(stack.pop())
        except OSError:
            continue
        with it:
            for e in it:
                try:
                    total += e.stat(follow_symlinks=False).st_blocks * 512
                    if e.is_dir(follow_symlinks=False):
                        stack.append(e.path)
                except OSError:
                    pass
    return total


def log(event, **kw):
    kw.update(ts=round(time.time(), 3), event=event)
    sys.stdout.write(json.dumps(kw, default=str) + "\n")
    sys.stdout.flush()


# ----------------------------------------------------------------- API client
class LeaseLost(Exception):
    pass


def enroll(cfg, timeout=15):
    """Exchange the single-use enrollment code for the long-lived token (over the LAN), store it 0600 in the
    state dir. Returns (ok, message, remote_config). The token and the code are never logged."""
    if not cfg.enroll_code:
        return False, "no CPU_RUNNER_ENROLL_CODE configured", None
    req = urllib.request.Request(cfg.api + "/api/cpu/enroll", method="POST",
                                 data=json.dumps({"code": cfg.enroll_code, "runner_id": cfg.runner_id}).encode(),
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            body = json.loads(r.read())
    except urllib.error.HTTPError as e:
        try:
            msg = json.loads(e.read()).get("error", "")
        except Exception:
            msg = ""
        return False, "enrollment refused (HTTP %d %s)" % (e.code, msg), None
    except Exception as e:
        return False, "queue API unreachable at %s (%s)" % (cfg.api, e), None
    tok = (body.get("token") or "").strip()
    if not tok:
        return False, "enrollment response had no token", None
    try:
        os.makedirs(cfg.state_dir, exist_ok=True)
        tmp = cfg.stored_token_path + ".tmp"
        fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(fd, "w") as fh:
            fh.write(tok + "\n")
        os.replace(tmp, cfg.stored_token_path)
        os.chmod(cfg.stored_token_path, 0o600)
    except OSError as e:
        return False, "cannot store token in %s (%s): is the state volume mounted writable?" % (cfg.state_dir, e.strerror or e), None
    cfg.token, cfg.token_source, cfg.token_error = tok, "stored", None
    return True, "enrolled", body.get("config")


class Api:
    def __init__(self, cfg):
        self.cfg = cfg
        self._re_lock = threading.Lock()
        self._re_last = 0.0

    def _reenroll(self, old_token):
        """After a 401: if a fresh code is configured, enroll again (rate-limited); True when the token changed."""
        with self._re_lock:
            if self.cfg.token != old_token:
                return True               # another thread already refreshed it
            if self.cfg.token_source in ("file", "env") or not self.cfg.enroll_code:
                return False
            if time.time() - self._re_last < 60:
                return False
            self._re_last = time.time()
            ok, msg, conf = enroll(self.cfg)
            if ok:
                log("reenrolled", applied=self.cfg.apply_remote(conf or {}))
            else:
                log("reenroll_failed", error=msg,
                    hint="token rejected. On the queue host run 'cpu_lane.py enroll-code' and put the new code in the container's CPU_RUNNER_ENROLL_CODE")
            return ok

    def call(self, method, path, body=None, raw=None, timeout=30, want_bytes=False, headers=None):
        tok = self.cfg.token
        code, out = self._call(method, path, body, raw, timeout, want_bytes, headers)
        if code in (401, 403) and raw is None and self._reenroll(tok):
            return self._call(method, path, body, raw, timeout, want_bytes, headers)
        return code, out

    def _call(self, method, path, body=None, raw=None, timeout=30, want_bytes=False, headers=None):
        data = raw if raw is not None else (json.dumps(body).encode() if body is not None else None)
        req = urllib.request.Request(self.cfg.api + path, data=data, method=method)
        req.add_header("Authorization", "Bearer " + self.cfg.token)
        req.add_header("Content-Type", "application/octet-stream" if raw is not None else "application/json")
        for k, v in (headers or {}).items():
            req.add_header(k, v)
        try:
            with urllib.request.urlopen(req, timeout=timeout) as r:
                payload = r.read()
                if r.status == 204 or not payload:
                    return r.status, None
                return r.status, payload if want_bytes else json.loads(payload)
        except urllib.error.HTTPError as e:
            txt = e.read()
            try:
                return e.code, json.loads(txt)
            except Exception:
                return e.code, {"error": txt[:200].decode("utf-8", "replace")}

    def download(self, path, dest):
        req = urllib.request.Request(self.cfg.api + path)
        req.add_header("Authorization", "Bearer " + self.cfg.token)
        with urllib.request.urlopen(req, timeout=300) as r, open(dest, "wb") as out:
            shutil.copyfileobj(r, out, 1 << 20)


# ------------------------------------------------------------------ isolation
def am_root():
    return os.geteuid() == 0


def job_ids(cfg):
    try:
        pw = pwd.getpwnam(cfg.job_user)
        return pw.pw_uid, pw.pw_gid
    except KeyError:
        return None, None


def priv_drop_prefix(cfg):
    """setpriv prefix that drops root to the job user with no capabilities."""
    if not am_root():
        return []
    uid, gid = job_ids(cfg)
    if uid is None:
        raise RuntimeError("job user %r missing" % cfg.job_user)
    return ["setpriv", "--reuid=%d" % uid, "--regid=%d" % gid, "--clear-groups",
            "--bounding-set=-all", "--inh-caps=-all", "--no-new-privs"]


# Kernel-created fallback tunnel devices (appear, DOWN and address-less, in every new netns when the tunnel
# modules are loaded, e.g. Unraid's tunl0). They have no route out; real reachability is tested by selftest.
FALLBACK_TUNNEL_IFACES = {"tunl0", "sit0", "ip6tnl0", "ip6gre0", "gre0", "gretap0", "erspan0", "ip_vti0", "ip6_vti0"}


def only_loopback(ifaces):
    return "lo" in ifaces and not (set(ifaces) - {"lo"} - FALLBACK_TUNNEL_IFACES)


NET_FLAGS_FULL = ["unshare", "--net", "--pid", "--fork", "--kill-child"]
NET_FLAGS_MIN = ["unshare", "--net"]
PID_NS = ["unshare", "--pid", "--fork", "--kill-child", "--"]
LO_UP = 'ip link set lo up 2>/dev/null; exec "$@"'
RELAY_WAIT_STEPS = 100          # the job starts once the relay listens (polled every 0.05 s, at most ~5 s)


def isolation_prefix(cfg, flags, pre=None, ready=None):
    """Command prefix: empty netns (only loopback, brought up) then drop privileges.
    pre: a shell command line started in the background inside the netns before the job (the LLM relay); with
    ready, the job waits (bounded) until that file exists, which pre creates once it is listening."""
    script = LO_UP
    if pre:
        wait = ""
        if ready:
            wait = ('i=0; while [ ! -e %s ] && [ "$i" -lt %d ]; do sleep 0.05; i=$((i+1)); done; '
                    % (shlex.quote(ready), RELAY_WAIT_STEPS))
        script = 'ip link set lo up 2>/dev/null; %s & %sexec "$@"' % (pre, wait)
    return flags + ["--", "sh", "-c", script, "sh"] + priv_drop_prefix(cfg)


# ------------------------------------------------------------------ LLM relay
# A job whose spec asks for `llm` gets CPU_RUNNER_LLM_URL. Inside its empty netns a small forwarder (this file's
# `relay` subcommand, run as the job user) listens on 127.0.0.1:LLM_PORT and bridges each connection to a unix
# socket that the agent serves in the job's out/ dir. The agent forwards only LLM_PATHS to the queue host
# (/api/cpu/jobs/<id>/llm/v1/...) with its own token and the job's lease; the queue host checks the job and adds the
# model key. No other traffic leaves the netns, and no key or token is ever visible inside the job.
LLM_PORT = 41651
LLM_PATHS = {("POST", "/v1/chat/completions"): "chat/completions", ("GET", "/v1/models"): "models"}
LLM_BODY_MAX = 16 << 20


def _copy(src, dst):
    try:
        while True:
            b = src.recv(1 << 16)
            if not b:
                break
            dst.sendall(b)
    except OSError:
        pass
    try:
        dst.shutdown(socket.SHUT_WR)
    except OSError:
        pass


def _bridge(client, upstream):
    """Pump bytes both ways. The upstream end answers one request per connection, so once it is done both sockets
    are torn down: a client that never closes cannot pin the thread."""
    t = threading.Thread(target=_copy, args=(client, upstream), daemon=True)
    t.start()
    _copy(upstream, client)
    for s in (client, upstream):
        try:
            s.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
    t.join(5)
    for s in (client, upstream):
        try:
            s.close()
        except OSError:
            pass


def _relay_loop(srv, sock_path, alive):
    """Accept TCP connections on srv and bridge each to the unix socket sock_path while alive() holds."""
    srv.settimeout(1.0)
    while alive():
        try:
            c, _ = srv.accept()
        except socket.timeout:
            continue
        except OSError:
            break
        c.settimeout(None)
        u = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            u.connect(sock_path)
        except OSError:
            u.close()
            c.close()
            continue
        threading.Thread(target=_bridge, args=(c, u), daemon=True).start()
    try:
        srv.close()
    except OSError:
        pass


def relay_main(args):
    """`agent.py relay HOST:PORT SOCKET [READY]`: the in-netns end of the LLM relay, started in the background by the
    isolation wrapper as the job user. Creates READY once listening; exits when its parent (the job, which the wrapper
    shell exec'd into) is gone. In a pid namespace the kernel also kills it together with the job."""
    if len(args) not in (2, 3):
        sys.stderr.write("usage: agent.py relay HOST:PORT SOCKET [READY_FILE]\n")
        return 2
    host, _, port = args[0].rpartition(":")
    ppid = os.getppid()
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    try:
        srv.bind((host or "127.0.0.1", int(port)))
        srv.listen(64)
        if len(args) == 3:
            open(args[2], "w").close()
    except (OSError, ValueError) as e:
        sys.stderr.write("cpu-runner relay: cannot listen on %s: %s\n" % (args[0], e))
        return 1
    _relay_loop(srv, args[1], lambda: os.getppid() == ppid)
    return 0


def relay_cmdline(cfg, sock, ready):
    """Shell command line (for isolation_prefix's pre) that starts this file's relay as the job user."""
    argv = priv_drop_prefix(cfg) + [sys.executable, "-I", os.path.abspath(__file__), "relay",
                                    "127.0.0.1:%d" % LLM_PORT, sock, ready]
    return " ".join(shlex.quote(a) for a in argv)


class _LLMServer(socketserver.ThreadingMixIn, socketserver.UnixStreamServer):
    daemon_threads = True
    block_on_close = False         # stop_llm must not wait for an in-flight model call (it finishes upstream anyway)

    def handle_error(self, request, client_address):
        log("llm_relay_error", job=getattr(getattr(self, "job", None), "id", None),
            error=repr(sys.exc_info()[1])[:200])


class _LLMHandler(BaseHTTPRequestHandler):
    """Serves the job's side of the relay (one request per connection) and forwards only LLM_PATHS upstream."""
    protocol_version = "HTTP/1.1"
    server_version = "cpu-runner-llm"
    timeout = 300                  # a stalled job-side connection cannot pin a handler thread forever

    def log_message(self, *a):
        pass

    def address_string(self):      # unix-socket peers have no address
        return "job"

    def _reply(self, code, msg):
        data = json.dumps({"error": {"message": msg}}).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Connection", "close")
        self.end_headers()
        self.wfile.write(data)

    def _forward(self):
        self.close_connection = True
        job = self.server.job
        sub = LLM_PATHS.get((self.command, self.path.split("?", 1)[0]))
        if sub is None:
            return self._reply(404, "the cpu-runner relay only forwards POST /v1/chat/completions and GET /v1/models")
        body = None
        if self.command == "POST":
            try:
                n = int(self.headers.get("Content-Length"))
            except (TypeError, ValueError):
                return self._reply(411, "Content-Length required")
            if not 0 <= n <= LLM_BODY_MAX:
                return self._reply(413, "request body over %d bytes" % LLM_BODY_MAX)
            body = self.rfile.read(n)
            if len(body) != n:
                return
        cfg = job.cfg
        req = urllib.request.Request("%s/api/cpu/jobs/%s/llm/v1/%s" % (cfg.api, job.id, sub), data=body,
                                     method=self.command)
        req.add_header("Authorization", "Bearer " + cfg.token)
        req.add_header("X-Lease-Token", job.lease_token)
        if body is not None:
            req.add_header("Content-Type", "application/json")
        try:
            resp = urllib.request.urlopen(req, timeout=cfg.llm_timeout_s)
        except urllib.error.HTTPError as e:
            resp = e
        except Exception as e:
            job.llm_count(error=True)
            return self._reply(502, "queue host unreachable (%s)" % type(e).__name__)
        try:
            code = resp.getcode()
            job.llm_count(error=code >= 400)
            self.send_response(code)
            self.send_header("Content-Type", resp.headers.get("Content-Type") or "application/json")
            for h in ("Content-Length", "Content-Encoding"):
                if resp.headers.get(h):
                    self.send_header(h, resp.headers[h])
            self.send_header("Connection", "close")
            self.end_headers()
            read = getattr(resp, "read1", None) or resp.read
            while True:
                chunk = read(1 << 16)
                if not chunk:
                    break
                self.wfile.write(chunk)
        finally:
            resp.close()

    def do_GET(self):
        self._forward()

    def do_POST(self):
        self._forward()


def probe_isolation(cfg):
    """Return the working unshare flag list, or None. Proves netns works by running
    a command inside it that must see only 'lo'."""
    if cfg.isolation == "none":
        return None
    # /proc/net/dev follows the CALLING process's netns; /sys/class/net stays bound to the netns sysfs was
    # mounted in (the container's), so it lists eth0 even inside a fresh empty netns.
    check = ["sh", "-c", "awk 'NR>2{sub(/:.*/,\"\",$1); print $1}' /proc/net/dev | tr '\\n' ' '"]
    for flags in (NET_FLAGS_FULL, NET_FLAGS_MIN):
        try:
            r = subprocess.run(isolation_prefix(cfg, flags) + check, capture_output=True,
                               text=True, timeout=20)
        except Exception as e:  # missing binary
            log("isolation_probe_error", flags=flags, error=str(e))
            continue
        ifs = r.stdout.split()
        if r.returncode == 0 and only_loopback(ifs):
            return flags
        log("isolation_probe_failed", flags=flags, rc=r.returncode, ifaces=ifs, err=r.stderr[-200:])
    return None


# ----------------------------------------------------------------- job runner
# Job dir layout. The dir itself, out/ and dl/ stay root-owned (the job can neither rename nor plant links in them);
# only these subdirs are handed to the job user.
JOB_OWNED = ("home", "tmp", "tools", "checkout", "art")
OUT_FILES = ("stdout", "stderr", "setup.log")    # created by the agent; the job writes them through inherited fds
LOG_SOURCES = ("setup.log", "stdout")            # streamed in this order (spec.log_stream); never goes back
LOG_CHUNK = 1 << 20
PHASES = ("materialize", "deps", "setup", "running", "artifacts", "done")


def _read_at(path, n, off=0):
    """Up to n bytes of path from off; never follows a final symlink. b"" when unreadable."""
    try:
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC)
    except OSError:
        return b""
    try:
        return os.pread(fd, n, off)
    except OSError:
        return b""
    finally:
        os.close(fd)


def _killpg(pid, sig=signal.SIGKILL):
    try:
        os.killpg(pid, sig)
    except (ProcessLookupError, PermissionError):
        pass


class Job:
    def __init__(self, cfg, api, claim, iso_flags):
        self.cfg, self.api, self.iso_flags = cfg, api, iso_flags
        self.id = claim["id"]
        self.spec = claim["spec"]
        self.lease_token = claim["lease_token"]
        self.attempt = claim.get("attempt", 1)
        self.dir = tempfile.mkdtemp(prefix="job-%s-" % self.id[:12], dir=cfg.work_dir)
        self.out = os.path.join(self.dir, "out")
        self.proc = None
        self.abort = None  # "lease_lost" | "cancelled" | "shutdown"
        self.timings = {}
        self.phase = "materialize"
        self._hb_stop = threading.Event()
        self._wfd = {}                     # OUT_FILES name -> write fd handed to the job's processes
        self._progress = ""
        # spec.llm
        self._llm_srv = None
        self._llm_stop = threading.Event()
        self._llm_lock = threading.Lock()
        self._llm_stats = {"calls": 0, "errors": 0}
        # spec.log_stream
        self._tel_stop = threading.Event()
        self._log_lock = threading.Lock()
        self._log_src, self._log_pos, self._log_off = 0, 0, 0
        self._log_capped = self._log_dead = False
        self._log_fails = 0
        self._log_sent = None
        self.touched = set()               # caches (CACHES) this job may have grown; re-measured once it ends

    # ---- helpers
    def _t(self, name, t0):
        self.timings[name] = round(time.time() - t0, 2)

    def llm_count(self, error=False):
        with self._llm_lock:
            self._llm_stats["calls"] += 1
            self._llm_stats["errors"] += bool(error)

    def read_progress(self):
        """First line of the job's $CPU_RUNNER_PROGRESS (printable, <= 200 chars); the last non-empty one is kept so a
        writer's truncate-then-write never blanks it."""
        line = _read_at(os.path.join(self.out, "progress"), 512).split(b"\n", 1)[0].decode("utf-8", "replace")
        line = "".join(ch for ch in line if ch.isprintable()).strip()[:200]
        if line:
            self._progress = line
        return self._progress

    def run_user(self, argv, cwd, env_, timeout, out_path=None):
        """Run argv as the job user (privilege dropped), WITH network (install phase)."""
        argv = priv_drop_prefix(self.cfg) + argv
        out = open(out_path, "ab") if out_path else subprocess.DEVNULL
        try:
            return subprocess.run(argv, cwd=cwd, env=env_, stdout=out, stderr=subprocess.STDOUT,
                                  timeout=timeout)
        finally:
            if out_path:
                out.close()

    def base_env(self):
        e = {"PATH": os.environ.get("PATH", "/usr/local/bin:/usr/bin:/bin"),
             "HOME": os.path.join(self.dir, "home"), "TMPDIR": os.path.join(self.dir, "tmp"),
             "CI": "1", "LANG": "C.UTF-8", "JOB_TOOLS": os.path.join(self.dir, "tools"),
             "NPM_CONFIG_CACHE": os.path.join(self.cfg.cache_dir, "npm"),
             # python jobs: a verify that bootstraps a venv with `pip install -r requirements.txt` must fail
             # FAST offline (no 5 x backoff retries against a dead network) and fall back to the baked packages
             "PIP_NO_INDEX": "1", "PLAYWRIGHT_SKIP_BROWSER_DOWNLOAD": "1", "PIP_DISABLE_PIP_VERSION_CHECK": "1",
             "NODE_OPTIONS": self.cfg.node_options, "CPU_RUNNER_JOB_ID": self.id}
        return e

    def job_env(self):
        e = self.base_env()
        for k, v in (self.spec.get("env") or {}).items():
            if any(fnmatch.fnmatchcase(k, p) for p in self.cfg.env_allow):
                e[k] = str(v)
            else:
                log("env_dropped", job=self.id, key=k)
        e["CPU_RUNNER_PROGRESS"] = os.path.join(self.out, "progress")
        if self.spec.get("artifacts"):
            e["CPU_RUNNER_ARTIFACTS"] = os.path.join(self.dir, "art")
        return e

    def _cwd(self):
        cwd = os.path.normpath(os.path.join(self.root, self.spec.get("cwd") or "."))
        if not (cwd == self.root or cwd.startswith(self.root + os.sep)):
            raise RuntimeError("cwd escapes checkout")
        return cwd

    # ---- phases
    def layout(self):
        """Job dir skeleton + the output files the job writes through inherited fds (created here, by the agent,
        with O_EXCL|O_NOFOLLOW). out/progress is the one file the job may rewrite in place."""
        d = self.dir
        os.chmod(d, 0o711)
        for sub in ("home", "tmp", "out", "tools", "dl", "art"):
            os.makedirs(os.path.join(d, sub))
        os.chmod(self.out, 0o711)
        os.chmod(os.path.join(d, "dl"), 0o700)
        excl = os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC
        for n in OUT_FILES:
            self._wfd[n] = os.open(os.path.join(self.out, n), excl | os.O_WRONLY | os.O_APPEND, 0o644)
        prog = os.path.join(self.out, "progress")
        os.close(os.open(prog, excl | os.O_WRONLY, 0o644))
        if am_root():
            uid, gid = job_ids(self.cfg)
            os.chown(prog, uid, gid)

    def close_fds(self):
        for n in list(self._wfd):
            try:
                os.close(self._wfd.pop(n))
            except OSError:
                pass

    def materialize(self):
        t0 = time.time()
        d = self.dir
        self.root = os.path.join(d, "checkout")
        kind = self.spec.get("payload_kind", "bundle")
        pay = os.path.join(d, "dl", "payload")
        self.api.download("/api/cpu/jobs/%s/payload" % self.id, pay)
        if kind == "bundle":
            self.git("clone", "-q", pay, self.root, cwd=d)
        elif kind == "archive":
            os.makedirs(self.root)
            with tarfile.open(pay) as tf:
                _safe_extract(tf, self.root)
            self.git("init", "-q", cwd=self.root)
            self.git("add", "-A", cwd=self.root)
            self.git("-c", "user.name=runner", "-c", "user.email=runner@localhost",
                     "commit", "-q", "-m", "snapshot", "--allow-empty", cwd=self.root)
        else:
            raise RuntimeError("unknown payload_kind %r" % kind)
        if self.spec.get("has_patch"):
            patch = os.path.join(d, "dl", "patch")
            self.api.download("/api/cpu/jobs/%s/patch" % self.id, patch)
            if os.path.getsize(patch):
                self.git("apply", "--binary", "--whitespace=nowarn", patch, cwd=self.root)
        if self.spec.get("has_tools"):
            tools = os.path.join(d, "dl", "tools.tgz")
            self.api.download("/api/cpu/jobs/%s/tools" % self.id, tools)
            with tarfile.open(tools) as tf:
                _safe_extract(tf, os.path.join(d, "tools"))
        if am_root():
            uid, gid = job_ids(self.cfg)
            for sub in JOB_OWNED:
                for dp, dns, fns in os.walk(os.path.join(d, sub)):
                    os.chown(dp, uid, gid)
                    for n in dns + fns:
                        try:
                            os.lchown(os.path.join(dp, n), uid, gid)
                        except OSError:
                            pass
        self._t("materialize_s", t0)

    def git(self, *args, cwd):
        r = subprocess.run(["git", "-c", "safe.directory=*"] + list(args), cwd=cwd, capture_output=True,
                           text=True, timeout=300)
        if r.returncode:
            raise RuntimeError("git %s failed: %s" % (args[0], (r.stderr or r.stdout)[-400:]))

    def find_pkg_root(self):
        p = self._cwd()
        while True:
            if os.path.exists(os.path.join(p, "package-lock.json")):
                return p
            if p == self.root:
                return None
            p = os.path.dirname(p)

    def extra_pkg_dirs(self):
        """Sub-package dirs (spec.extra_pkg_dirs, relative to the checkout) that carry their OWN package-lock.json
        and need their own node_modules (e.g. sidecar/). Anything outside the checkout / without a lock is ignored."""
        out = []
        for rel in (self.spec.get("extra_pkg_dirs") or [])[:4]:
            if not isinstance(rel, str) or not rel or os.path.isabs(rel):
                continue
            d = os.path.normpath(os.path.join(self.root, rel))
            if d != self.root and d.startswith(self.root + os.sep) and os.path.isfile(os.path.join(d, "package-lock.json")):
                out.append(d)
        return out

    def deps(self):
        t0 = time.time()
        pkg = self.find_pkg_root()
        info = {"cache": "none"}
        if pkg is not None:
            self._install_pkg(pkg, info)
        subs = []
        for d in self.extra_pkg_dirs():
            si = {"cache": "none"}
            self._install_pkg(d, si)
            subs.append(dict(si, dir=os.path.relpath(d, self.root)))
        if subs:
            info["sub"] = subs
        info["seconds"] = round(time.time() - t0, 2)
        self.timings["deps"] = info
        log("deps", job=self.id, **info)

    def _install_pkg(self, pkg, info):
        t0 = time.time()
        h = hashlib.sha256()
        h.update(open(os.path.join(pkg, "package-lock.json"), "rb").read())
        for extra in ("prisma/schema.prisma", "package.json"):
            fp = os.path.join(pkg, extra)
            if extra == "package.json" or os.path.exists(fp):
                # package.json: only deps matter for install, but scripts/postinstall can too
                h.update(extra.encode())
                try:
                    h.update(open(fp, "rb").read())
                except OSError:
                    pass
        h.update(("node:" + _node_major() + ":" + os.uname().machine).encode())
        key = h.hexdigest()[:24]
        hint = self.spec.get("lockfile_hash")
        info["key"] = key
        nm_cache = os.path.join(self.cfg.cache_dir, "nm")
        entry = os.path.join(nm_cache, key)
        os.makedirs(os.path.join(self.cfg.cache_dir, "locks"), exist_ok=True)
        lockf = open(os.path.join(self.cfg.cache_dir, "locks", key + ".lock"), "w")
        fcntl.flock(lockf, fcntl.LOCK_EX)
        try:
            dest = os.path.join(pkg, "node_modules")
            env_ = self.base_env()
            if os.path.isdir(entry):
                info["cache"] = "hit"
                os.utime(entry, None)
                self.run_user(["cp", "-a", entry, dest], pkg, env_, 600)
            else:
                info["cache"] = "miss"
                self.touched.update(("npm", "nm"))
                out = os.path.join(self.dir, "out", "install.log")
                r = self.run_user(["bash", "-c", self.cfg.install_cmd], pkg, env_, self.cfg.install_timeout_s, out)
                if r.returncode:
                    raise RuntimeError("dependency install failed rc=%d: %s" % (r.returncode, _tail(out, 2000)))
                has_prisma = os.path.exists(os.path.join(pkg, "prisma", "schema.prisma")) and \
                    os.path.isdir(os.path.join(dest, "prisma"))
                if has_prisma:
                    r = self.run_user(["bash", "-c", self.cfg.prisma_cmd], pkg, env_, 600, out)
                    if r.returncode:
                        raise RuntimeError("prisma generate failed rc=%d: %s" % (r.returncode, _tail(out, 2000)))
                    info["prisma"] = True
                os.makedirs(nm_cache, exist_ok=True)
                # The agent is root; the cp below runs as the job user. A root-owned nm_cache made every
                # cache fill fail with ENOENT on the rename (cp could not create tmp). Own it first.
                if am_root():
                    uid, gid = job_ids(self.cfg)
                    if uid is not None:
                        os.chown(nm_cache, uid, gid)
                tmp = entry + ".tmp-%d-%s" % (os.getpid(), self.id[:8])
                r = self.run_user(["cp", "-a", dest, tmp], pkg, env_, 600)
                if r.returncode:
                    shutil.rmtree(tmp, ignore_errors=True)
                    raise RuntimeError("cache fill (cp node_modules) failed rc=%d" % r.returncode)
                os.rename(tmp, entry)
                self._prune(nm_cache)
        finally:
            fcntl.flock(lockf, fcntl.LOCK_UN)
            lockf.close()
        info["seconds"] = round(time.time() - t0, 2)

    def _prune(self, nm_cache):
        """Evict node_modules copies, oldest first: beyond cache_max_entries, then until the rest fits in
        nm_cache_max_gb. Concurrent jobs: never an entry another job is (or just was) copying from. A hit touches the
        entry (os.utime), so "recent mtime" == "possibly in use"."""
        try:
            ents = [os.path.join(nm_cache, n) for n in os.listdir(nm_cache) if ".tmp-" not in n]
            ents.sort(key=lambda p: os.stat(p).st_mtime)
            now = time.time()
            old = [p for p in ents if now - os.stat(p).st_mtime > PRUNE_PROTECT_S]
            gone = set(old[:max(0, len(old) - self.cfg.cache_max_entries)])
            for p in old:
                if p in gone:
                    shutil.rmtree(p, ignore_errors=True)
                    log("cache_evict", path=p, reason="count")
            sizes = {p: du_bytes(p) for p in ents if p not in gone}
            total, cap = sum(sizes.values()), self.cfg.nm_cache_max_gb * 1e9
            for p in old:
                if total <= cap:
                    break
                if p not in gone:
                    shutil.rmtree(p, ignore_errors=True)
                    total -= sizes[p]
                    log("cache_evict", path=p, reason="size", gb=round(sizes[p] / 1e9, 2))
        except OSError:
            pass

    def _run_proc(self, full, cwd, env_, out_fd, err_fd, timeout):
        """Run full in its own session, polling for abort / timeout; afterwards sweep whatever it left running in
        its process group. Returns (returncode, timed_out)."""
        t0 = time.time()
        timed_out = False
        self.proc = subprocess.Popen(full, cwd=cwd, env=env_, stdout=out_fd, stderr=err_fd,
                                     stdin=subprocess.DEVNULL, start_new_session=True)
        while True:
            try:
                self.proc.wait(timeout=0.5)
                break
            except subprocess.TimeoutExpired:
                pass
            if self.abort:
                self.kill()
                break
            if time.time() - t0 > timeout:
                timed_out = True
                self.kill()
                break
        _killpg(self.proc.pid)
        return self.proc.returncode, timed_out

    def setup_phase(self):
        """spec.setup {cmd, timeout_s}: prepare the checkout (e.g. build a venv) as the job user WITH network, before
        the isolated run. Runs in its own pid namespace when the host allows it, so nothing it starts (with network)
        survives into the run phase. Output -> out/setup.log; a failure is an infra error carrying its tail."""
        st = self.spec.get("setup")
        if not st:
            return
        self.touched.update(("uv", "npm"))
        if isinstance(st, str):
            st = {"cmd": st}
        cmd = st.get("cmd") if isinstance(st, dict) else None
        if not cmd:
            raise RuntimeError("spec.setup needs a cmd")
        argv = ["bash", "-c", cmd] if isinstance(cmd, str) else [str(a) for a in cmd]
        try:
            timeout = int(st.get("timeout_s") or self.cfg.setup_timeout_s)
        except (TypeError, ValueError):
            timeout = self.cfg.setup_timeout_s
        timeout = max(1, min(timeout, self.cfg.setup_timeout_s))
        env_ = self.job_env()
        env_.pop("PIP_NO_INDEX", None)
        # copy, never hardlink, out of the uv cache: a job's venv then shares no inode with the cache (an in-place edit
        # cannot reach it, deleting it frees the space, and du does not count it twice); across filesystems uv copies anyway
        env_.update(UV_CACHE_DIR=os.path.join(self.cfg.cache_dir, "uv"), UV_PYTHON_DOWNLOADS="never", UV_LINK_MODE="copy")
        full = (PID_NS if self.iso_flags == NET_FLAGS_FULL else []) + priv_drop_prefix(self.cfg) + argv
        if self.cfg.nice:
            full = ["nice", "-n", str(self.cfg.nice)] + full
        self.phase = "setup"
        t0 = time.time()
        rc, timed_out = self._run_proc(full, self._cwd(), env_, self._wfd["setup.log"], self._wfd["setup.log"], timeout)
        self._t("setup_s", t0)
        if self.abort:
            raise LeaseLost()
        if timed_out or rc:
            why = ("timed out after %ds" % timeout) if timed_out else ("rc=%s" % rc)
            raise RuntimeError("setup failed (%s): %s" % (why, _tail(os.path.join(self.out, "setup.log"), 2000)))

    # ---- spec.llm: the job-side end of the relay
    def start_llm(self):
        """Serve the relay's unix socket (out/llm.sock, owned by the job user, 0600). Returns its path."""
        path = os.path.join(self.out, "llm.sock")
        if len(path.encode()) >= 104:
            raise RuntimeError("llm socket path too long (%d bytes); use a shorter CPU_RUNNER_WORK" % len(path))
        srv = _LLMServer(path, _LLMHandler)
        srv.job = self
        if am_root():
            uid, gid = job_ids(self.cfg)
            os.chown(path, uid, gid)
        os.chmod(path, 0o600)
        threading.Thread(target=srv.serve_forever, kwargs={"poll_interval": 0.5}, daemon=True).start()
        self._llm_srv = srv
        return path

    def start_tcp_bridge(self, sock):
        """No netns (isolation off, or a job with network): bridge an agent-side loopback port instead."""
        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.bind(("127.0.0.1", 0))
        srv.listen(64)
        port = srv.getsockname()[1]
        threading.Thread(target=_relay_loop, args=(srv, sock, lambda: not self._llm_stop.is_set()),
                         daemon=True).start()
        return port

    def stop_llm(self):
        self._llm_stop.set()
        srv, self._llm_srv = self._llm_srv, None
        if srv is not None:
            srv.shutdown()
            srv.server_close()
        if self._llm_stats["calls"]:
            self.timings["llm"] = dict(self._llm_stats)

    def execute(self):
        spec = self.spec
        cmd = spec["cmd"]
        argv = ["bash", "-c", cmd] if isinstance(cmd, str) else list(cmd)
        timeout = min(int(spec.get("timeout_s") or 600), self.cfg.max_job_timeout_s)
        cwd = self._cwd()
        env_ = self.job_env()
        net = spec.get("network", "none")
        isolated = net == "none" and bool(self.iso_flags)
        if net == "none" and not self.iso_flags and self.cfg.isolation == "required":
            raise RuntimeError("isolation_unavailable")
        pre = ready = None
        if spec.get("llm"):
            sock = self.start_llm()
            if isolated:
                # unguessable, so nothing the setup phase left in tmp/ can fake readiness
                ready = os.path.join(self.dir, "tmp", ".cpu-runner-relay-ready-" + os.urandom(6).hex())
                pre, port = relay_cmdline(self.cfg, sock, ready), LLM_PORT
            else:
                port = self.start_tcp_bridge(sock)
            env_["CPU_RUNNER_LLM_URL"] = "http://127.0.0.1:%d/v1" % port
        if isolated:
            full = isolation_prefix(self.cfg, self.iso_flags, pre, ready) + argv
        else:
            full = priv_drop_prefix(self.cfg) + argv
        if self.cfg.nice:
            full = ["nice", "-n", str(self.cfg.nice)] + full
        self.phase = "running"
        t0 = time.time()
        rc, timed_out = self._run_proc(full, cwd, env_, self._wfd["stdout"], self._wfd["stderr"], timeout)
        self._t("run_s", t0)
        so, se = (os.path.join(self.out, n) for n in ("stdout", "stderr"))
        return {"exit_code": rc if rc is not None else -1, "timed_out": timed_out,
                "stdout_tail": _tail(so, self.cfg.tail_bytes), "stderr_tail": _tail(se, self.cfg.tail_bytes)}

    # ---- spec.artifacts
    def collect_artifacts(self):
        """tar.gz of art/, made BY THE JOB USER without following links (so it can only pack what the job could read
        anyway), capped at CPU_RUNNER_ARTIFACTS_MAX, then PUT to the queue. Returns the result's `artifacts` field."""
        self.phase = "artifacts"
        t0 = time.time()
        dst, errp = os.path.join(self.out, "artifacts.tgz"), os.path.join(self.out, "artifacts.err")
        cap = self.cfg.artifacts_max
        h, n, over = hashlib.sha256(), 0, False
        argv = priv_drop_prefix(self.cfg) + ["tar", "-C", os.path.join(self.dir, "art"), "-czf", "-", "."]
        try:
            with open(dst, "wb") as out, open(errp, "wb") as err:
                p = subprocess.Popen(argv, cwd="/", env={"PATH": "/usr/bin:/bin", "LANG": "C.UTF-8"},
                                     stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=err,
                                     start_new_session=True)
                timer = threading.Timer(300, _killpg, (p.pid,))
                timer.start()
                try:
                    while True:
                        b = p.stdout.read(1 << 20)
                        if not b:
                            break
                        if self.abort:
                            _killpg(p.pid)
                            break
                        n += len(b)
                        if n > cap:
                            over = True
                            _killpg(p.pid)
                            break
                        h.update(b)
                        out.write(b)
                    p.stdout.close()
                    rc = p.wait()
                finally:
                    timer.cancel()
        except (OSError, RuntimeError) as e:
            return {"error": "artifacts: %s" % e}
        if self.abort:
            return {"error": "artifacts: job %s" % self.abort}
        if over:
            return {"error": "artifacts over CPU_RUNNER_ARTIFACTS_MAX (%d bytes)" % cap}
        if rc not in (0, 1):          # GNU tar: 1 = a file changed while being read (kept, with a warning)
            return {"error": "artifacts tar rc=%s: %s" % (rc, _tail(errp, 500))}
        try:
            with open(dst, "rb") as f:
                code, _ = self.api.call("PUT", "/api/cpu/jobs/%s/artifacts" % self.id, raw=f, timeout=300,
                                        headers={"X-Lease-Token": self.lease_token, "Content-Length": str(n)})
        except Exception as e:
            return {"error": "artifacts upload failed: %s" % e, "bytes": n}
        self._t("artifacts_s", t0)
        res = {"bytes": n, "sha256": h.hexdigest(), "http": code}
        if code != 200:
            res["error"] = "artifacts upload http %s" % code
        if rc == 1:
            res["warning"] = _tail(errp, 300)
        log("artifacts", job=self.id, **res)
        return res

    # ---- spec.log_stream
    def _telemetry(self):
        while not self._tel_stop.wait(self.cfg.log_interval_s):
            self.flush_log()

    def _log_chunk(self, final):
        """The next unsent bytes: setup.log first, then stdout. setup.log is complete once the run phase has begun;
        on the final flush every source is."""
        while self._log_src < len(LOG_SOURCES):
            name = LOG_SOURCES[self._log_src]
            data = _read_at(os.path.join(self.out, name), LOG_CHUNK, self._log_pos)
            if data:
                return data
            if not (final or (name == "setup.log" and PHASES.index(self.phase) >= PHASES.index("running"))):
                return b""
            self._log_src, self._log_pos = self._log_src + 1, 0
        return b""

    def _log_fail(self, fatal=False, **why):
        self._log_fails += 1
        if fatal or self._log_fails >= 20:
            self._log_dead = True
            log("log_stream_stopped", job=self.id, **why)

    def flush_log(self, final=False):
        """Post new output (setup.log, then stdout) with the phase and progress line. The queue appends at `off`
        (an overlap is idempotent), so a failed post is simply sent again next time. The final flush drains
        everything and always posts once. Never raises."""
        if not self.spec.get("log_stream"):
            return
        with self._log_lock:
            posted = False
            for _ in range(64):                 # bounded catch-up per call (64 x LOG_CHUNK)
                if self._log_dead:
                    return
                data = b"" if self._log_capped else self._log_chunk(final)
                consumed, capped_now = len(data), False
                if data and self._log_off + len(data) > self.cfg.log_max:
                    data = data[:max(0, self.cfg.log_max - self._log_off)] + (
                        b"\n[cpu-runner] log truncated at %d bytes\n" % self.cfg.log_max)
                    capped_now = True
                state = (self.phase, self.read_progress())
                if not data and state == self._log_sent and (posted or not final):
                    return
                body = {"runner_id": self.cfg.runner_id, "lease_token": self.lease_token, "off": self._log_off,
                        "data_b64": base64.b64encode(data).decode(), "phase": state[0], "progress": state[1]}
                try:
                    code, resp = self.api.call("POST", "/api/cpu/jobs/%s/log" % self.id, body, timeout=30)
                except Exception as e:
                    return self._log_fail(error=str(e)[:200])
                if code == 416 and isinstance(resp, dict) and isinstance(resp.get("size"), int) \
                        and 0 <= resp["size"] < self._log_off:
                    self._log_off = resp["size"]     # the queue lost bytes (restarted): carry on from its end
                    continue
                if code != 200:
                    return self._log_fail(fatal=code in (400, 403, 404, 409, 410, 413), http=code)
                posted = True
                self._log_fails = 0
                self._log_sent = state
                self._log_off += len(data)
                self._log_pos += consumed
                if capped_now:
                    self._log_capped = True
                    return
                if consumed < LOG_CHUNK and not (final and consumed):
                    return

    def kill(self):
        p = self.proc
        if not p or p.poll() is not None:
            return
        for sig, wait in ((signal.SIGTERM, 5), (signal.SIGKILL, 10)):
            try:
                os.killpg(p.pid, sig)
            except ProcessLookupError:
                break
            try:
                p.wait(timeout=wait)
                break
            except subprocess.TimeoutExpired:
                continue
        # sweep stragglers in the group (children that ignored TERM)
        try:
            os.killpg(p.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass

    # ---- heartbeat
    def _heartbeat(self):
        """Keeps the lease alive. Also carries the job's phase and progress line, so the queue can show what a job is
        doing even without spec.log_stream (older queues ignore the extra keys)."""
        misses = 0
        while not self._hb_stop.wait(self.cfg.heartbeat_s):
            try:
                code, body = self.api.call("POST", "/api/cpu/jobs/%s/heartbeat" % self.id,
                                           {"runner_id": self.cfg.runner_id, "lease_token": self.lease_token,
                                            "lease_s": self.cfg.lease_s, "phase": self.phase,
                                            "progress": self.read_progress()}, timeout=10)
            except Exception as e:
                misses += 1
                log("heartbeat_error", job=self.id, error=str(e), misses=misses)
                continue
            if self._hb_stop.is_set():      # the result is (being) posted: a 409 now means nothing
                return
            misses = 0
            if code == 409:
                self.abort = "lease_lost"
                log("lease_lost", job=self.id)
                self.kill()
                return
            if code == 200 and body and body.get("cancel"):
                self.abort = "cancelled"
                log("job_cancelled", job=self.id)
                self.kill()
                return

    def run(self):
        t_all = time.time()
        hb = threading.Thread(target=self._heartbeat, daemon=True)
        hb.start()
        tel = None
        result = None
        try:
            try:
                self.layout()
                if self.spec.get("log_stream"):
                    tel = threading.Thread(target=self._telemetry, daemon=True)
                    tel.start()
                self.materialize()
                if self.abort:
                    raise LeaseLost()
                self.phase = "deps"
                self.deps()
                if self.abort:
                    raise LeaseLost()
                self.setup_phase()
                if self.abort:
                    raise LeaseLost()
                result = self.execute()
            finally:
                self.stop_llm()
        except LeaseLost:
            pass
        except Exception as e:
            log("job_infra_error", job=self.id, error=str(e))
            result = {"exit_code": None, "timed_out": False, "infra_error": str(e)[:1000],
                      "stdout_tail": "", "stderr_tail": ""}
        if tel is not None:
            self._tel_stop.set()
            tel.join(40)
        if self.abort != "lease_lost":
            self.flush_log(final=True)
        # the heartbeat keeps the lease alive through the artifacts upload
        if result is not None and not self.abort and self.spec.get("artifacts") and not result.get("infra_error"):
            result["artifacts"] = self.collect_artifacts()
        self.phase = "done"
        self._hb_stop.set()
        self._t("total_s", t_all)
        if self.abort in ("lease_lost", "cancelled", "shutdown") or result is None:
            log("job_abandoned", job=self.id, reason=self.abort)
        else:
            result.update(runner_id=self.cfg.runner_id, lease_token=self.lease_token,
                          timings=self.timings, attempt=self.attempt, aborted=self.abort)
            for i in range(5):
                try:
                    code, body = self.api.call("POST", "/api/cpu/jobs/%s/result" % self.id, result, timeout=30)
                    log("result_posted", job=self.id, http=code, exit_code=result["exit_code"],
                        timed_out=result["timed_out"], timings=self.timings)
                    break
                except Exception as e:
                    log("result_post_error", job=self.id, error=str(e), try_=i)
                    time.sleep(2 ** i)
        self.close_fds()
        shutil.rmtree(self.dir, ignore_errors=True)


def _tail(path, n):
    try:
        with open(path, "rb") as f:
            f.seek(0, 2)
            sz = f.tell()
            f.seek(max(0, sz - n))
            return f.read().decode("utf-8", "replace")
    except OSError:
        return ""


def _node_major():
    try:
        return subprocess.run(["node", "-v"], capture_output=True, text=True, timeout=10).stdout.strip().split(".")[0]
    except Exception:
        return "nonode"


# Reported to the queue (claim body) so ITS eligibility check compares a stage's needs to what this image really
# has, instead of a hardcoded guess. Runs the same `node` / `python3` a job gets (job PATH), not the agent's own
# interpreter (the agent runs on Debian's python; jobs run on the baked job python).
CAPS_VERSION = 1
_PY_PROBE = ("import json,sys,importlib.metadata as m\n"
             "print(json.dumps({'minor':'%d.%d'%sys.version_info[:2],'full':sys.version.split()[0],"
             "'mods':sorted(m.packages_distributions())}))\n")


def detect_caps(run=subprocess.run, which=shutil.which):
    """{v, agent, node, node_full, python, python_full, py_modules, sqlite3, arch}; keys that cannot be probed are
    omitted (the queue treats an absent capability as 'not available'). Never raises."""
    caps = {"v": CAPS_VERSION, "agent": VERSION, "arch": os.uname().machine}
    try:
        nv = run(["node", "-v"], capture_output=True, text=True, timeout=10).stdout.strip()
        if nv.startswith("v") and nv[1:].split(".")[0].isdigit():
            caps["node_full"], caps["node"] = nv, int(nv[1:].split(".")[0])
    except Exception:
        pass
    try:
        r = run(["python3", "-c", _PY_PROBE], capture_output=True, text=True, timeout=30)
        d = json.loads(r.stdout.strip().splitlines()[-1])
        caps["python"], caps["python_full"] = str(d["minor"]), str(d["full"])
        caps["py_modules"] = sorted({str(x) for x in d["mods"]})
    except Exception:
        pass
    caps["sqlite3"] = which("sqlite3") is not None
    caps["subpkg_install"] = True       # deps phase honours spec.extra_pkg_dirs (npm ci in sub-packages with their own lock)
    # per-job features this agent implements (README "Remote lane jobs")
    for k in ("setup_phase", "llm_relay", "artifacts", "log_stream"):
        caps[k] = True
    caps["pythons"] = [v for v in ("3.12", "3.13", "3.14") if which("python" + v)]
    try:
        uv = run(["uv", "--version"], capture_output=True, text=True, timeout=10).stdout.split()
        if len(uv) >= 2 and uv[0] == "uv" and re.fullmatch(r"\d+(\.\d+)*", uv[1]):
            caps["uv"] = uv[1]
    except Exception:
        pass
    return caps


def _safe_extract(tf, dest):
    base = os.path.realpath(dest)
    for m in tf.getmembers():
        tgt = os.path.realpath(os.path.join(dest, m.name))
        if not (tgt == base or tgt.startswith(base + os.sep)):
            raise RuntimeError("unsafe tar member %r" % m.name)
        if m.islnk() or m.issym():
            lt = os.path.realpath(os.path.join(os.path.dirname(tgt), m.linkname))
            if m.islnk():
                lt = os.path.realpath(os.path.join(dest, m.linkname))
            if not (lt == base or lt.startswith(base + os.sep)):
                raise RuntimeError("unsafe tar link %r" % m.name)
    # members were validated above; python 3.12+ warns (3.14: errors on absolute/outside links) without a filter
    tf.extractall(dest, **({"filter": "fully_trusted"} if hasattr(tarfile, "fully_trusted_filter") else {}))


# ------------------------------------------------------------------ main loop
class Agent:
    def __init__(self, cfg):
        self.cfg = cfg
        self.api = Api(cfg)
        self.stop = threading.Event()
        self.draining = threading.Event()   # code changed: finish current jobs, claim no more, re-exec
        self.active = {}  # thread name -> Job
        self.iso = None
        self.status = {"selftest": "pending", "api": "unknown"}
        self._src = os.path.abspath(__file__)
        self._src_hash = self._hash_src()
        self._bad_hash = None
        self._cand = None
        self.caps = {}
        self._caps_sent = 0.0
        # disk guards (README "Disk use"): the claim gate and the usage report
        self.gate = threading.Condition()
        self.jobs_n = 0              # claims in flight that may bring a job + jobs running (a cache cleanup waits for 0)
        self.lane_n = 0              # lane jobs running
        self.lane_resv = 0           # claims in flight that may bring back a lane job
        self.cleanup = None          # why claims are paused while a cache is deleted
        self.disk = {}               # the last measurement; a claim carries it when new, else every CAPS_RESEND_S
        self._disk_sent, self._disk_sent_t = None, 0.0
        self._free, self._free_t = None, 0.0
        self._sizes = {}             # cache name -> bytes, from the last measurement
        self._dirty = set(CACHES)    # caches to re-measure: all at first, then the ones a finished job touched
        self._all_t = 0.0            # the last full re-measure (CACHE_REMEASURE_S)
        self._clean_after = 0.0      # CLEAN_RETRY_S
        self._hold_kinds = {}        # what the last "hold" log line said, by kind (logged on change)

    # ---- status / health
    def set_status(self, **kw):
        self.status.update(kw)
        try:
            with open(self.cfg.status_file, "w") as f:
                json.dump(self.status, f)
        except OSError:
            pass

    def _hash_src(self):
        try:
            with open(self._src, "rb") as f:
                return hashlib.sha256(f.read()).hexdigest()
        except OSError:
            return None

    def check_reload(self):
        """True once the agent's own file has changed to something that compiles (seen twice in a
        row, so a half-synced file is never acted on)."""
        if not self.cfg.reload_check_s:
            return False
        h = self._hash_src()
        if h is None or h == self._src_hash:
            self._cand = None
            return False
        if h != self._cand:
            self._cand = h          # debounce: require the same new hash on the next check
            return False
        if h == self._bad_hash:
            return False
        try:
            with open(self._src, "rb") as f:
                compile(f.read(), self._src, "exec")
        except (SyntaxError, ValueError) as e:
            self._bad_hash = h
            log("reload_skipped", reason="new code does not compile", error=str(e)[:200])
            return False
        log("reload_detected", old=self._src_hash[:12], new=h[:12])
        return True

    def preflight_api(self):
        """Startup self-check: API reachable, token accepted (cheap read-only authenticated call)."""
        cfg = self.cfg
        try:
            code, _ = self.api.call("GET", "/api/cpu/health", timeout=10)
        except Exception as e:
            log("api_unreachable", api=cfg.api, error=str(e),
                hint="check CPU_RUNNER_API (the queue host's LAN URL) and that the queue API is running")
            self.set_status(api="unreachable")
            return
        if code != 200:
            log("api_unhealthy", api=cfg.api, http=code)
            self.set_status(api="unhealthy")
            return
        try:
            code, _ = self.api.call("GET", "/api/cpu/runners", timeout=10)
        except Exception as e:
            log("api_unreachable", api=cfg.api, error=str(e))
            self.set_status(api="unreachable")
            return
        # log BEFORE publishing the status: anything that sees status api=ok / token_rejected
        # (the health file, tests, an operator) can then rely on the event line already being out.
        if code in (401, 403):
            log("token_rejected", api=cfg.api, http=code,
                hint="token rejected (rotated?). Mint a new code on the queue host (cpu_lane.py enroll-code), set CPU_RUNNER_ENROLL_CODE, restart")
            self.set_status(api="token_rejected")
        elif code == 200:
            log("api_ok", api=cfg.api, token="accepted")
            self.set_status(api="ok")
        else:
            log("api_unhealthy", api=cfg.api, http=code)
            self.set_status(api="unhealthy")

    def touch_health(self):
        try:
            with open(self.cfg.health_file, "w") as f:
                f.write(str(time.time()))
        except OSError:
            pass

    def setup(self):
        for d in (self.cfg.cache_dir, self.cfg.work_dir):
            os.makedirs(d, exist_ok=True)
        if am_root():
            uid, gid = job_ids(self.cfg)
            os.chown(self.cfg.cache_dir, uid, gid)
            # job dirs are made by the agent: the job user may traverse the work dir, never list or create in it
            os.chown(self.cfg.work_dir, 0, 0)
            os.chmod(self.cfg.work_dir, 0o711)
            # stale job dirs from a crashed previous run
            for n in os.listdir(self.cfg.work_dir):
                shutil.rmtree(os.path.join(self.cfg.work_dir, n), ignore_errors=True)
        self.iso = probe_isolation(self.cfg)
        if self.cfg.isolation == "required" and not self.iso:
            log("fatal", error="network isolation required but unavailable (need CAP_SYS_ADMIN+NET_ADMIN, "
                               "see README troubleshooting); set CPU_RUNNER_ISOLATION=none only for tests")
            return False
        if self.cfg.isolation == "auto" and not self.iso:
            log("warning", error="NETWORK ISOLATION UNAVAILABLE - jobs run with network")
        if self.cfg.accept not in ("all", "lane"):
            log("fatal", error="CPU_RUNNER_ACCEPT must be all or lane, not %r" % self.cfg.accept[:40])
            return False
        log("start", version=VERSION, runner=self.cfg.runner_id, concurrency=self.cfg.concurrency,
            isolation=self.iso or self.cfg.isolation, api=self.cfg.api, code=(self._src_hash or "")[:12])
        if self.cfg.isolation != "none":
            ok, text = selftest_run(self.cfg)
            self.set_status(selftest="PASS" if ok else "FAIL")
            log("selftest", result="PASS" if ok else "FAIL", detail=text)
            if not ok and self.cfg.isolation == "required":
                log("fatal", error="startup selftest FAILED; refusing to run jobs (fail closed). "
                                   "If it cannot create a network namespace, add --security-opt seccomp=unconfined (see README)")
                return False
        else:
            self.set_status(selftest="skipped")
        relay_ok = True             # without a netns the relay is an in-process loopback bridge
        if self.iso:
            relay_ok, text = relay_selftest(self.cfg, self.iso)
            log("relay_selftest", result="PASS" if relay_ok else "FAIL", detail=text)
        self.caps = detect_caps()
        self.caps["llm_relay"] = relay_ok      # a failed relay only disables spec.llm jobs here, nothing else
        log("caps", **self.caps)
        self.preflight_api()
        return True

    CAPS_RESEND_S = 60      # the queue treats caps older than ~3 min as stale, so a downgraded (old) image ages out

    def claim_body(self, accept="all", holds=None):
        """The claim request. accept (what this claim may bring back), hold (why the runner takes less work), lane_max
        and disk (the usage report) are for the queue's dashboard and checks; a queue that predates them ignores them."""
        body = {"runner_id": self.cfg.runner_id, "lease_s": self.cfg.lease_s, "slots": self.cfg.concurrency,
                "accept": accept, "hold": holds or {}, "lane_max": self.cfg.lane_max}
        now = time.time()
        if self.caps and now - self._caps_sent >= self.CAPS_RESEND_S:
            body["caps"] = self.caps
        d = self.disk
        if d and (d != self._disk_sent or now - self._disk_sent_t >= self.CAPS_RESEND_S):
            body["disk"] = d
        return body

    def claim_sent(self, body, code):
        """The queue took the claim (200 / 204): the caps and disk report it carried need not be re-sent for a while."""
        if code in (200, 204):
            now = time.time()
            if "caps" in body:
                self._caps_sent = now
            if "disk" in body:
                self._disk_sent, self._disk_sent_t = body["disk"], now

    # ---- disk guards (README "Disk use")
    def free_space(self):
        """(GB, dir): the lower free space of the work and cache dirs, re-read at most every FREE_RECHECK_S; None when
        neither can be read (nothing is held back then)."""
        now = time.time()
        if now - self._free_t >= FREE_RECHECK_S:
            got = [(g, d) for d in (self.cfg.work_dir, self.cfg.cache_dir) for g in [free_gb(d)] if g is not None]
            self._free, self._free_t = (min(got) if got else None), now
        return self._free

    def holds(self):
        """Why this runner takes less work right now, as the queue shows it: {"all": why} = nothing, {"lane": why} = no
        lane jobs, {"plain": why} = no plain jobs (a lane-only runner). Call with the gate held."""
        cfg, h = self.cfg, {}
        if self.cleanup:
            h["all"] = self.cleanup
        fs = self.free_space()
        if fs is not None:
            gb, where = fs
            floor = min(LOW_DISK_ANY_GB, cfg.min_free_gb)
            if gb < floor:
                h.setdefault("all", "low disk: %.1f GB free on %s, every job needs %.1f GB" % (gb, where, floor))
            elif gb < cfg.min_free_gb:
                h["lane"] = "low disk: %.1f GB free on %s, lane jobs need %.1f GB" % (gb, where, cfg.min_free_gb)
        if "lane" not in h:
            if cfg.lane_max <= 0:
                h["lane"] = "lane jobs are off on this runner (lane_max 0)"
            elif self.lane_n >= cfg.lane_max:
                h["lane"] = "lane-job limit: %d of %d running" % (self.lane_n, cfg.lane_max)
        if cfg.accept == "lane":
            h["plain"] = LANE_ONLY_HOLD
        return h

    def plan_claim(self):
        """What the next claim may bring back, reserved under the gate -> (accept, holds, reserved). accept is "all",
        "plain" (no lane jobs), "lane" (lane jobs only: a lane-only runner) or "none". Every call is paired with
        claim_done()."""
        with self.gate:
            h = self.holds()
            kinds = {k: v.split(":")[0] for k, v in h.items()}
            if kinds != self._hold_kinds:
                self._hold_kinds = kinds
                if h:
                    log("hold", **h)
                else:
                    log("hold_cleared")
            if "all" in h:
                return "none", h, False
            lane_full = "lane" in h or self.lane_n + self.lane_resv >= self.cfg.lane_max
            if lane_full and self.cfg.accept == "lane":
                return "none", h, False
            self.jobs_n += 1
            if lane_full:
                return "plain", h, False
            self.lane_resv += 1
            return ("lane" if self.cfg.accept == "lane" else "all"), h, True

    def claim_done(self, accept, reserved, claim):
        """The claim call is over -> (run, lane). run is False without a job, and for a job this claim did not accept
        (an older queue ignores `accept`): the caller gives that one straight back."""
        with self.gate:
            if reserved:
                self.lane_resv -= 1
            lane = claim is not None and is_lane_job(claim.get("spec"))
            run = claim is not None and (accept == "all" or (accept == "plain" and not lane) or (accept == "lane" and lane))
            if run:
                self.lane_n += lane
            elif accept != "none":
                self.jobs_n -= 1
            self.gate.notify_all()
            return run, lane

    def job_done(self, lane, touched=()):
        with self.gate:
            self.jobs_n -= 1
            self.lane_n -= bool(lane)
            self._dirty.update(touched)
            self._free_t = 0.0            # its scratch dir is gone: re-read the free space before the next claim
            self.gate.notify_all()

    def give_back(self, claim, event, **kw):
        """Hand a claimed job straight back to the queue (pending again)."""
        log(event, job=claim.get("id"), **kw)
        try:
            self.api.call("POST", "/api/cpu/jobs/%s/release" % claim.get("id"),
                          {"runner_id": self.cfg.runner_id, "lease_token": claim.get("lease_token")}, timeout=10)
        except Exception as e:
            log("release_error", job=claim.get("id"), error=str(e)[:200])

    def disk_loop(self):
        """Measures the work dir and the caches for the queue dashboard (the next claim carries the report) and keeps
        the uv / npm caches under their caps. First pass a few seconds after start, then every disk_check_s."""
        wait = 5.0
        while not self.stop.wait(wait):
            wait = self.cfg.disk_check_s
            try:
                self.measure()
                self.clean_caches()
            except Exception as e:          # never let the thread die
                log("disk_error", error=str(e)[:300])

    def measure(self):
        """Re-measure the work dir, and the caches a finished job touched (all of them at first and every
        CACHE_REMEASURE_S, so an idle cache dir is not walked for nothing). Publishes and returns self.disk."""
        cfg, now = self.cfg, time.time()
        with self.gate:
            dirty, self._dirty = self._dirty, set()
        if now - self._all_t >= CACHE_REMEASURE_S:
            dirty, self._all_t = set(CACHES), now
        for name in dirty:
            self._sizes[name] = du_bytes(os.path.join(cfg.cache_dir, name))
        rep = {"work_gb": du_bytes(cfg.work_dir) / 1e9, "work_free_gb": free_gb(cfg.work_dir),
               "cache_free_gb": free_gb(cfg.cache_dir), "min_free_gb": cfg.min_free_gb}
        for name, cap in (("uv", cfg.uv_cache_max_gb), ("npm", cfg.npm_cache_max_gb), ("nm", cfg.nm_cache_max_gb)):
            rep[name + "_gb"] = self._sizes.get(name, 0) / 1e9
            rep[name + "_max_gb"] = cap
        self.disk = {k: round(v, 2) for k, v in rep.items() if v is not None}
        return self.disk

    def clean_caches(self):
        """Delete the uv / npm cache once it is over its cap. Both are plain download caches (a job fetches again what
        it needs), but a running job may be reading or filling one, so claims pause and the deletion waits until no
        job runs. node_modules copies are capped in Job._prune instead (oldest first, never one in use)."""
        cfg = self.cfg
        over = [n for n, cap in (("uv", cfg.uv_cache_max_gb), ("npm", cfg.npm_cache_max_gb))
                if self._sizes.get(n, 0) > cap * 1e9]
        if not over or time.time() < self._clean_after:
            return
        gb = {n: round(self._sizes[n] / 1e9, 2) for n in over}
        with self.gate:
            self.cleanup = "clearing the %s cache (over its cap)" % " and ".join(over)
            log("cache_cleanup_wait", caches=over, gb=gb, jobs=self.jobs_n)
            while self.jobs_n > 0 and not self.stop.is_set():
                self.gate.wait(5)
        try:
            if self.stop.is_set():
                return
            for n in over:
                p = os.path.join(cfg.cache_dir, n)
                if os.path.islink(p):
                    os.unlink(p)
                else:
                    shutil.rmtree(p, ignore_errors=True)
                log("cache_cleared", cache=n, gb=gb[n])
            with self.gate:
                self._dirty.update(over)
            self.measure()
            left = [n for n in over if self._sizes.get(n, 0) > getattr(cfg, n + "_cache_max_gb") * 1e9]
            if left:
                self._clean_after = time.time() + CLEAN_RETRY_S
                log("cache_cleanup_incomplete", caches=left, retry_s=CLEAN_RETRY_S)
        finally:
            with self.gate:
                self.cleanup = None
                self._free_t = 0.0
                self.gate.notify_all()

    def worker(self):
        cfg = self.cfg
        me = threading.current_thread().name
        while not self.stop.is_set() and not self.draining.is_set():
            self.touch_health()
            accept, holds, reserved = self.plan_claim()
            cb, code, body, err = self.claim_body(accept, holds), None, None, None
            try:
                code, body = self.api.call("POST", "/api/cpu/claim", cb, timeout=15)
            except Exception as e:
                err = e
            claim = body.get("job") if code == 200 and isinstance(body, dict) else None
            run, lane = self.claim_done(accept, reserved, claim if isinstance(claim, dict) else None)
            if err is not None:
                log("claim_error", error=str(err))
                self.stop.wait(min(30, cfg.poll_s * 3))
                continue
            self.claim_sent(cb, code)
            if code in (401, 403):
                if self.status.get("api") != "token_rejected":
                    log("token_rejected", http=code, hint="token rejected (rotated?). Mint a new code on the queue host (cpu_lane.py enroll-code), set CPU_RUNNER_ENROLL_CODE, restart")
                    self.set_status(api="token_rejected")
                self.stop.wait(min(30, cfg.poll_s * 5))
                continue
            if self.status.get("api") != "ok" and code in (200, 204):
                log("api_ok", api=cfg.api, token="accepted")
                self.set_status(api="ok")
            if not isinstance(claim, dict):
                self.stop.wait(cfg.poll_s)
                continue
            if not run:
                self.give_back(claim, "claim_refused", accept=accept, lane=lane,
                               hint="this queue API ignores the claim's accept (older version): update it")
                self.stop.wait(REFUSE_BACKOFF_S)
                continue
            log("claimed", slot=me, active=len(self.active) + 1, job=claim["id"], attempt=claim.get("attempt"),
                stage=claim["spec"].get("stage"), lane=lane)
            job = None
            try:
                job = Job(cfg, self.api, claim, self.iso)
                self.active[me] = job
                job.run()
            except Exception as e:  # never let the worker thread die
                log("worker_error", job=claim["id"], error=str(e))
            finally:
                self.active.pop(me, None)
                self.job_done(lane, job.touched if job is not None else ())
            if job is None:         # it could not even start (no space for its dir?): hand it back, slow down
                self.give_back(claim, "job_start_failed")
                self.stop.wait(REFUSE_BACKOFF_S)

    def run(self):
        if not self.setup():
            return 3
        signal.signal(signal.SIGTERM, lambda *_: self.stop.set())
        signal.signal(signal.SIGINT, lambda *_: self.stop.set())
        threading.Thread(target=self.disk_loop, name="disk", daemon=True).start()
        threads = [threading.Thread(target=self.worker, name="w%d" % i) for i in range(self.cfg.concurrency)]
        for t in threads:
            t.start()
        reloading = False
        tick = min(5.0, self.cfg.reload_check_s or 5.0)
        while not self.stop.is_set():
            self.touch_health()
            if self.check_reload():
                reloading = True
                break
            self.stop.wait(tick)
        if reloading:
            # graceful: stop claiming, let leased jobs finish (heartbeats keep leases), then re-exec
            self.draining.set()
            log("reload_drain", active=len(self.active), grace_s=self.cfg.reload_grace_s)
            deadline = time.time() + self.cfg.reload_grace_s
            while any(t.is_alive() for t in threads) and not self.stop.is_set() and time.time() < deadline:
                self.touch_health()
                self.stop.wait(1)
            if not any(t.is_alive() for t in threads) and not self.stop.is_set():
                log("reload_exec", argv=sys.argv)
                sys.stdout.flush()
                os.execv(sys.executable, [sys.executable] + sys.argv)
            log("reload_drain_timeout_or_stop", active=len(self.active))
            self.stop.set()
        log("shutdown_begin", active=len(self.active), grace_s=self.cfg.shutdown_grace_s)
        deadline = time.time() + self.cfg.shutdown_grace_s
        for t in threads:
            t.join(max(0, deadline - time.time()))
        for name, job in list(self.active.items()):
            job.abort = "shutdown"
            job.kill()
            try:  # hand the job back immediately instead of waiting for lease expiry
                self.api.call("POST", "/api/cpu/jobs/%s/release" % job.id,
                              {"runner_id": self.cfg.runner_id, "lease_token": job.lease_token}, timeout=5)
            except Exception:
                pass
        for t in threads:
            t.join(15)
        log("shutdown_done")
        return 0


def selftest(cfg):
    ok, text = selftest_run(cfg)
    print(text)
    if ok:
        saved, cfg.isolation = cfg.isolation, "required"
        try:
            flags = probe_isolation(cfg)
        finally:
            cfg.isolation = saved
        os.makedirs(cfg.work_dir, exist_ok=True)
        rok, rtext = relay_selftest(cfg, flags)
        print("llm relay: %s %s" % ("PASS" if rok else "FAIL", rtext))
        ok = rok
    print("PASS" if ok else "FAIL")
    return 0 if ok else 1


def _ifaces_ok(stdout):
    import re
    m = re.search(r"'ifaces': \[([^\]]*)\]", stdout)
    return bool(m) and only_loopback([x.strip().strip("'\"") for x in m.group(1).split(",") if x.strip()])


def selftest_run(cfg):
    """Prove isolation inside the real container: a job must see only lo, no route out,
    not be root, and carry no capabilities. Returns (ok, detail)."""
    saved, cfg.isolation = cfg.isolation, "required"     # probe even when configured "none"/"auto"; restore after
    try:
        flags = probe_isolation(cfg)
    finally:
        cfg.isolation = saved
    if not flags:
        return False, "cannot create an empty network namespace"
    script = ("import socket,os,sys\n"
              "r={}\n"
              "r['uid']=os.getuid()\n"
              "r['ifaces']=sorted(l.split(':')[0].strip() for l in open('/proc/net/dev').read().splitlines()[2:])\n"
              "for host in ('1.1.1.1','10.0.0.1'):\n"
              "  try: socket.create_connection((host,53),3); r[host]='REACHABLE'\n"
              "  except OSError as e: r[host]='blocked'\n"
              "try: socket.getaddrinfo('example.com',80); r['dns']='RESOLVED'\n"
              "except OSError: r['dns']='blocked'\n"
              "s=socket.socket(); s.bind(('127.0.0.1',0)); r['loopback']='ok'\n"
              "print(r, open('/proc/self/status').read().split('CapEff:')[1].split()[0])\n")
    out = subprocess.run(isolation_prefix(cfg, flags) + ["python3", "-c", script], capture_output=True, text=True)
    text = (out.stdout.strip() + " " + out.stderr.strip()[-300:]).strip()
    ok = ("REACHABLE" not in out.stdout and "RESOLVED" not in out.stdout and _ifaces_ok(out.stdout)
          and "'loopback': 'ok'" in out.stdout and out.stdout.strip().endswith("0000000000000000")
          and (not am_root() or "'uid': 0" not in out.stdout))
    return ok, text


def relay_selftest(cfg, flags):
    """Prove the LLM relay end to end inside the empty netns (as a job would use it): a request to
    127.0.0.1:LLM_PORT from the job side must arrive on a unix socket the agent serves. Returns (ok, detail)."""
    d = tempfile.mkdtemp(prefix="relay-selftest-", dir=cfg.work_dir)
    srv = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        os.chmod(d, 0o711)
        tmp, sock = os.path.join(d, "tmp"), os.path.join(d, "llm.sock")
        os.makedirs(tmp)
        srv.bind(sock)
        srv.listen(4)
        if am_root():
            uid, gid = job_ids(cfg)
            os.chown(tmp, uid, gid)
            os.chown(sock, uid, gid)
        os.chmod(sock, 0o600)
        got = {}

        def serve():
            srv.settimeout(40)
            try:
                c, _ = srv.accept()
                c.settimeout(10)
                req = b""
                while b"\r\n\r\n" not in req and len(req) < 65536:
                    b = c.recv(4096)
                    if not b:
                        break
                    req += b
                got["line"] = req.split(b"\r\n", 1)[0].decode("latin-1")
                c.sendall(b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 11\r\n"
                          b"Connection: close\r\n\r\n{\"ok\":true}")
                c.close()
            except OSError as e:
                got["error"] = str(e)

        t = threading.Thread(target=serve, daemon=True)
        t.start()
        probe = ("import urllib.request as u; o=u.build_opener(u.ProxyHandler({})); "
                 "print(o.open('http://127.0.0.1:%d/v1/models', timeout=15).read().decode())" % LLM_PORT)
        ready = os.path.join(tmp, "ready")
        r = subprocess.run(isolation_prefix(cfg, flags, relay_cmdline(cfg, sock, ready), ready) + ["python3", "-c", probe],
                           capture_output=True, text=True, timeout=60)
        t.join(5)
        ok = r.returncode == 0 and '{"ok":true}' in r.stdout and got.get("line", "").startswith("GET /v1/models ")
        return ok, ("%s %s %s" % (r.stdout.strip()[-200:], r.stderr.strip()[-300:], got)).strip()
    except Exception as e:
        return False, "relay selftest error: %s" % e
    finally:
        srv.close()
        shutil.rmtree(d, ignore_errors=True)


def healthcheck(cfg):
    """Healthy = loop heartbeat fresh AND startup selftest not failed AND token not rejected."""
    try:
        age = time.time() - float(open(cfg.health_file).read())
    except Exception:
        return 1
    if age >= 90:
        return 1
    try:
        st = json.load(open(cfg.status_file))
    except Exception:
        st = {}
    if st.get("selftest") == "FAIL" or st.get("api") == "token_rejected":
        print("unhealthy: %s" % st)
        return 1
    return 0


def main(argv):
    if len(argv) > 1 and argv[1] == "relay":     # runs as the job user inside its netns: no config, no token
        return relay_main(argv[2:])
    cfg = Config()
    cmd = argv[1] if len(argv) > 1 else "run"
    if cmd == "run":
        if not cfg.token:
            if cfg.enroll_code:
                ok, msg, conf = enroll(cfg)
                log("enrolled" if ok else "enroll_failed", ok=ok, message=msg, api=cfg.api)
                if not ok:
                    sys.stderr.write("cpu-runner: %s. Mint a fresh code on the queue host (cpu_lane.py enroll-code) and set CPU_RUNNER_ENROLL_CODE.\n" % msg)
                    time.sleep(30)          # slow the restart loop
                    return 4
            else:
                sys.stderr.write("cpu-runner: %s. Set CPU_RUNNER_ENROLL_CODE (cpu_lane.py enroll-code on the queue host) "
                                 "or CPU_RUNNER_TOKEN. The token is never generated on this side.\n" % cfg.token_error)
                log("fatal", error=str(cfg.token_error), fix="set CPU_RUNNER_ENROLL_CODE (run 'cpu_lane.py enroll-code' on the queue host)")
                return 4
        try:
            code, body = Api(cfg).call("GET", "/api/cpu/config", timeout=10)
            if code == 200 and isinstance(body, dict):
                log("config_pulled", applied=cfg.apply_remote(body.get("config")))
        except Exception as e:
            log("config_pull_failed", error=str(e))
        return Agent(cfg).run()
    if cmd == "selftest":
        return selftest(cfg)
    if cmd == "healthcheck":
        return healthcheck(cfg)
    print("usage: agent.py [run|selftest|healthcheck]   (relay HOST:PORT SOCKET [READY] is internal)")
    return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv))
