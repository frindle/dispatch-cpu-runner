#!/usr/bin/env python3
"""dispatch-cpu-runner agent: pull-based CPU job runner (stdlib only).

Loop (x CONCURRENCY threads): claim -> download payload -> materialize a fresh
checkout -> restore/populate the node_modules cache (keyed by lockfile hash) ->
run the command with a timeout inside an empty network namespace -> post the
result. A heartbeat thread keeps the lease alive; if the lease is lost (409) or
the producer cancels, the job's process group is killed and no result is posted.

Subcommands: run (default) | selftest | healthcheck
Config is env (see README / env.example); a few tunables can also be pulled from the queue API (env wins).
The shared token comes from CPU_RUNNER_TOKEN_FILE / CPU_RUNNER_TOKEN, else from the state dir (CPU_RUNNER_STATE_DIR/token),
which the agent fills itself by enrolling once with CPU_RUNNER_ENROLL_CODE (single-use code minted on the queue host).
"""
import base64, fnmatch, re, hashlib, io, json, os, pwd, shlex, shutil, signal, socket
import subprocess, sys, tarfile, tempfile, threading, time, urllib.error, urllib.request
import fcntl

VERSION = "1.3.0"


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

    # remote-tunable settings: API key -> (attr, env var that overrides it, cast, lo, hi)
    REMOTE = {"concurrency": ("concurrency", "CPU_RUNNER_CONCURRENCY", int, 1, 32),
              "lease_s": ("lease_s", "CPU_RUNNER_LEASE_S", int, 15, 600),
              "max_job_timeout_s": ("max_job_timeout_s", "CPU_RUNNER_MAX_TIMEOUT_S", int, 30, 21600),
              "cache_max_entries": ("cache_max_entries", "CPU_RUNNER_CACHE_MAX", int, 1, 64),
              "poll_s": ("poll_s", "CPU_RUNNER_POLL_S", float, 0.5, 60)}

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

    def call(self, method, path, body=None, raw=None, timeout=30, want_bytes=False):
        tok = self.cfg.token
        code, out = self._call(method, path, body, raw, timeout, want_bytes)
        if code in (401, 403) and raw is None and self._reenroll(tok):
            return self._call(method, path, body, raw, timeout, want_bytes)
        return code, out

    def _call(self, method, path, body=None, raw=None, timeout=30, want_bytes=False):
        data = raw if raw is not None else (json.dumps(body).encode() if body is not None else None)
        req = urllib.request.Request(self.cfg.api + path, data=data, method=method)
        req.add_header("Authorization", "Bearer " + self.cfg.token)
        req.add_header("Content-Type", "application/octet-stream" if raw is not None else "application/json")
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
LO_UP = 'ip link set lo up 2>/dev/null; exec "$@"'


def isolation_prefix(cfg, flags):
    """Command prefix: empty netns (only loopback, brought up) then drop privileges."""
    return flags + ["--", "sh", "-c", LO_UP, "sh"] + priv_drop_prefix(cfg)


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
class Job:
    def __init__(self, cfg, api, claim, iso_flags):
        self.cfg, self.api, self.iso_flags = cfg, api, iso_flags
        self.id = claim["id"]
        self.spec = claim["spec"]
        self.lease_token = claim["lease_token"]
        self.attempt = claim.get("attempt", 1)
        self.dir = tempfile.mkdtemp(prefix="job-%s-" % self.id[:12], dir=cfg.work_dir)
        self.proc = None
        self.abort = None  # "lease_lost" | "cancelled" | "shutdown"
        self.timings = {}
        self._hb_stop = threading.Event()

    # ---- helpers
    def _t(self, name, t0):
        self.timings[name] = round(time.time() - t0, 2)

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
             "PIP_NO_INDEX": "1", "PIP_DISABLE_PIP_VERSION_CHECK": "1",
             "NODE_OPTIONS": self.cfg.node_options, "CPU_RUNNER_JOB_ID": self.id}
        return e

    def job_env(self):
        e = self.base_env()
        for k, v in (self.spec.get("env") or {}).items():
            if any(fnmatch.fnmatchcase(k, p) for p in self.cfg.env_allow):
                e[k] = str(v)
            else:
                log("env_dropped", job=self.id, key=k)
        return e

    # ---- phases
    def materialize(self):
        t0 = time.time()
        d = self.dir
        for sub in ("home", "tmp", "out", "tools", "dl"):
            os.makedirs(os.path.join(d, sub))
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
            for dp, dns, fns in os.walk(d):
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
        cwd = os.path.normpath(os.path.join(self.root, self.spec.get("cwd") or "."))
        if not (cwd == self.root or cwd.startswith(self.root + os.sep)):
            raise RuntimeError("cwd escapes checkout")
        p = cwd
        while True:
            if os.path.exists(os.path.join(p, "package-lock.json")):
                return p
            if p == self.root:
                return None
            p = os.path.dirname(p)

    def deps(self):
        t0 = time.time()
        pkg = self.find_pkg_root()
        info = {"cache": "none"}
        if pkg is None:
            self.timings["deps"] = info
            return
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
        self.timings["deps"] = info
        log("deps", job=self.id, **info)

    def _prune(self, nm_cache):
        try:
            ents = [os.path.join(nm_cache, n) for n in os.listdir(nm_cache) if ".tmp-" not in n]
            ents.sort(key=lambda p: os.stat(p).st_mtime)
            # Concurrent jobs: never evict an entry another job is (or just was) copying from. A hit touches the
            # entry (os.utime), so "recent mtime" == "possibly in use".
            now = time.time()
            ents = [p for p in ents if now - os.stat(p).st_mtime > PRUNE_PROTECT_S]
            for p in ents[:max(0, len(ents) - self.cfg.cache_max_entries)]:
                shutil.rmtree(p, ignore_errors=True)
                log("cache_evict", path=p)
        except OSError:
            pass

    def execute(self):
        spec = self.spec
        cmd = spec["cmd"]
        argv = ["bash", "-c", cmd] if isinstance(cmd, str) else list(cmd)
        timeout = min(int(spec.get("timeout_s") or 600), self.cfg.max_job_timeout_s)
        cwd = os.path.normpath(os.path.join(self.root, spec.get("cwd") or "."))
        if not (cwd == self.root or cwd.startswith(self.root + os.sep)):
            raise RuntimeError("cwd escapes checkout")
        net = spec.get("network", "none")
        if net == "none" and self.iso_flags:
            full = isolation_prefix(self.cfg, self.iso_flags) + argv
        elif net == "none" and self.cfg.isolation == "required":
            raise RuntimeError("isolation_unavailable")
        else:
            full = priv_drop_prefix(self.cfg) + argv
        if self.cfg.nice:
            full = ["nice", "-n", str(self.cfg.nice)] + full
        so, se = (os.path.join(self.dir, "out", n) for n in ("stdout", "stderr"))
        t0 = time.time()
        timed_out = False
        with open(so, "wb") as fo, open(se, "wb") as fe:
            self.proc = subprocess.Popen(full, cwd=cwd, env=self.job_env(), stdout=fo, stderr=fe,
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
        self._t("run_s", t0)
        rc = self.proc.returncode
        return {"exit_code": rc if rc is not None else -1, "timed_out": timed_out,
                "stdout_tail": _tail(so, self.cfg.tail_bytes), "stderr_tail": _tail(se, self.cfg.tail_bytes)}

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
        misses = 0
        while not self._hb_stop.wait(self.cfg.heartbeat_s):
            try:
                code, body = self.api.call("POST", "/api/cpu/jobs/%s/heartbeat" % self.id,
                                           {"runner_id": self.cfg.runner_id, "lease_token": self.lease_token,
                                            "lease_s": self.cfg.lease_s}, timeout=10)
            except Exception as e:
                misses += 1
                log("heartbeat_error", job=self.id, error=str(e), misses=misses)
                continue
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
        result = None
        try:
            self.materialize()
            if self.abort:
                raise LeaseLost()
            self.deps()
            if self.abort:
                raise LeaseLost()
            result = self.execute()
        except LeaseLost:
            pass
        except Exception as e:
            log("job_infra_error", job=self.id, error=str(e))
            result = {"exit_code": None, "timed_out": False, "infra_error": str(e)[:1000],
                      "stdout_tail": "", "stderr_tail": ""}
        finally:
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
            self.set_status(api="unreachable")
            log("api_unreachable", api=cfg.api, error=str(e),
                hint="check CPU_RUNNER_API (the queue host's LAN URL) and that the queue API is running")
            return
        if code != 200:
            self.set_status(api="unhealthy")
            log("api_unhealthy", api=cfg.api, http=code)
            return
        try:
            code, _ = self.api.call("GET", "/api/cpu/runners", timeout=10)
        except Exception as e:
            self.set_status(api="unreachable")
            log("api_unreachable", api=cfg.api, error=str(e))
            return
        if code in (401, 403):
            self.set_status(api="token_rejected")
            log("token_rejected", api=cfg.api, http=code,
                hint="token rejected (rotated?). Mint a new code on the queue host (cpu_lane.py enroll-code), set CPU_RUNNER_ENROLL_CODE, restart")
        elif code == 200:
            self.set_status(api="ok")
            log("api_ok", api=cfg.api, token="accepted")
        else:
            self.set_status(api="unhealthy")
            log("api_unhealthy", api=cfg.api, http=code)

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
            for d in (self.cfg.cache_dir, self.cfg.work_dir):
                os.chown(d, uid, gid)
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
        self.caps = detect_caps()
        log("caps", **self.caps)
        self.preflight_api()
        return True

    CAPS_RESEND_S = 60      # the queue treats caps older than ~3 min as stale, so a downgraded (old) image ages out

    def claim_body(self):
        body = {"runner_id": self.cfg.runner_id, "lease_s": self.cfg.lease_s, "slots": self.cfg.concurrency}
        if self.caps and time.time() - self._caps_sent >= self.CAPS_RESEND_S:
            body["caps"] = self.caps
        return body

    def worker(self):
        cfg = self.cfg
        while not self.stop.is_set() and not self.draining.is_set():
            self.touch_health()
            try:
                cb = self.claim_body()
                code, body = self.api.call("POST", "/api/cpu/claim", cb, timeout=15)
                if "caps" in cb and code in (200, 204):
                    self._caps_sent = time.time()
            except Exception as e:
                log("claim_error", error=str(e))
                self.stop.wait(min(30, cfg.poll_s * 3))
                continue
            if code in (401, 403):
                if self.status.get("api") != "token_rejected":
                    self.set_status(api="token_rejected")
                    log("token_rejected", http=code, hint="token rejected (rotated?). Mint a new code on the queue host (cpu_lane.py enroll-code), set CPU_RUNNER_ENROLL_CODE, restart")
                self.stop.wait(min(30, cfg.poll_s * 5))
                continue
            if self.status.get("api") != "ok" and code in (200, 204):
                self.set_status(api="ok")
                log("api_ok", api=cfg.api, token="accepted")
            if code != 200 or not body or not body.get("job"):
                self.stop.wait(cfg.poll_s)
                continue
            claim = body["job"]
            log("claimed", slot=threading.current_thread().name, active=len(self.active) + 1, job=claim["id"], attempt=claim.get("attempt"), stage=claim["spec"].get("stage"))
            job = Job(cfg, self.api, claim, self.iso)
            self.active[threading.current_thread().name] = job
            try:
                job.run()
            except Exception as e:  # never let the worker thread die
                log("worker_error", error=str(e))
            finally:
                self.active.pop(threading.current_thread().name, None)

    def run(self):
        if not self.setup():
            return 3
        signal.signal(signal.SIGTERM, lambda *_: self.stop.set())
        signal.signal(signal.SIGINT, lambda *_: self.stop.set())
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
    print("PASS" if ok else "FAIL")
    return 0 if ok else 1


def _ifaces_ok(stdout):
    import re
    m = re.search(r"'ifaces': \[([^\]]*)\]", stdout)
    return bool(m) and only_loopback([x.strip().strip("'\"") for x in m.group(1).split(",") if x.strip()])


def selftest_run(cfg):
    """Prove isolation inside the real container: a job must see only lo, no route out,
    not be root, and carry no capabilities. Returns (ok, detail)."""
    cfg.isolation = "required"
    flags = probe_isolation(cfg)
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
    print("usage: agent.py [run|selftest|healthcheck]")
    return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv))
