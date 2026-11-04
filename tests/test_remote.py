"""Remote lane job features end to end: agent + client + reference queue on one machine (no Docker).
spec.setup, spec.llm (both relay paths), spec.artifacts, spec.log_stream, heartbeat phase/progress, job dir modes,
caller-side cancel, the client's queue wait (claim clock, reasons, the model hold, a job handed back and re-run, an older
queue's job view), the queue-side contract of the new routes and the model hold, and the feature caps.
The network namespace itself needs CAP_SYS_ADMIN and is proven by `agent.py selftest` inside the container; here the
in-netns relay path runs with `env` standing in for `unshare` (same wrapper, relay and ready-file handshake).
Run: python3 -W ignore -m unittest discover -s tests -v
"""
import hashlib, json, os, re, socket, subprocess, sys, tarfile, tempfile, threading, time, unittest
import urllib.error, urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from unittest import mock

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from test_e2e import Base, ROOT, TOKEN, sh  # noqa: E402
from test_caps import FakeRun  # noqa: E402
import agent as agent_mod  # noqa: E402
import cpu_job  # noqa: E402
import queue_server  # noqa: E402

AGENT = os.path.join(ROOT, "agent", "agent.py")
KEY = "sk-upstream-secret-123"
NOTE = b"\n[cpu-runner] log truncated at 1000 bytes\n"

# What a job does with the relay: the model list, a chat call, an upstream error, a path the relay must refuse, and
# whether the key or the runner token is anywhere in its environment.
LLM_CLIENT = r'''
import json, os, urllib.error, urllib.request
base = os.environ["CPU_RUNNER_LLM_URL"]
o = urllib.request.build_opener(urllib.request.ProxyHandler({}))
def call(method, path, body=None):
    req = urllib.request.Request(base + path, method=method, headers={"Content-Type": "application/json"},
                                 data=None if body is None else json.dumps(body).encode())
    try:
        with o.open(req, timeout=30) as r:
            return r.status, json.loads(r.read())
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read() or b"null")
code, body = call("GET", "/models")
print("models", code, body["data"][0]["id"])
code, body = call("POST", "/chat/completions", {"model": "fake-model", "messages": [{"role": "user", "content": "hi"}]})
print("chat", code, body["choices"][0]["message"]["content"])
print("limited", call("POST", "/chat/completions", {"model": "boom", "messages": []})[0])
print("other", call("GET", "/embeddings")[0])
print("env-has-key", any("sk-upstream" in v for v in os.environ.values()))
print("env-has-token", any("test-token" in v for v in os.environ.values()))
'''


class FakeUpstream:
    """An OpenAI-style model server that records what reached it: (method, path, Authorization, body)."""

    def __init__(self):
        seen = self.seen = []

        class H(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, *a):
                pass

            def _send(self, code, obj):
                data = json.dumps(obj).encode()
                self.send_response(code)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

            def do_GET(self):
                seen.append(("GET", self.path, self.headers.get("Authorization"), b""))
                if self.path == "/v1/models":
                    return self._send(200, {"object": "list", "data": [{"id": "fake-model"}]})
                self._send(404, {"error": "no such route"})

            def do_POST(self):
                body = self.rfile.read(int(self.headers.get("Content-Length") or 0))
                seen.append(("POST", self.path, self.headers.get("Authorization"), body))
                req = json.loads(body)
                if req.get("model") == "boom":
                    return self._send(429, {"error": {"message": "slow down"}})
                self._send(200, {"choices": [{"message": {"role": "assistant",
                                                          "content": "echo:" + req["messages"][-1]["content"]}}]})

        self.srv = ThreadingHTTPServer(("127.0.0.1", 0), H)
        threading.Thread(target=self.srv.serve_forever, daemon=True).start()
        self.url = "http://127.0.0.1:%d/v1" % self.srv.server_address[1]

    def close(self):
        self.srv.shutdown()
        self.srv.server_close()


def port_free(port):
    """Nothing listens on 127.0.0.1:port (TIME_WAIT leftovers don't count: the relay binds with SO_REUSEADDR too)."""
    s = socket.socket()
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    try:
        s.bind(("127.0.0.1", port))
        return True
    except OSError:
        return False
    finally:
        s.close()


def gone(pid):
    """True once pid has exited (a zombie nobody reaped counts as exited)."""
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return True
    try:
        with open("/proc/%d/stat" % pid) as f:
            return f.read().rsplit(")", 1)[1].split()[0] == "Z"
    except OSError:
        return True


def wait_for(pred, timeout=30, step=0.05):
    deadline = time.time() + timeout
    while time.time() < deadline:
        v = pred()
        if v:
            return v
        time.sleep(step)
    return pred()


class RemoteBase(Base):
    def setUp(self):
        super().setUp()
        # no package-lock: these jobs have no node deps phase (TestDepsCache covers it)
        sh(self.repo, "git", "rm", "-q", "package-lock.json", "package.json")
        open(os.path.join(self.repo, "llm_client.py"), "w").write(LLM_CLIENT)
        sh(self.repo, "git", "add", "-A")
        sh(self.repo, "git", "commit", "-qm", "remote fixtures")

    def submit(self, cmd, **kw):
        kw.setdefault("fallback", False)
        kw.setdefault("poll_s", 0.2)
        kw.setdefault("timeout_s", 60)
        return super().submit(cmd, **kw)

    def job(self):
        with self.store.lock:
            return dict(list(self.store.jobs.values())[-1])

    def upstream(self):
        up = FakeUpstream()
        self.addCleanup(up.close)
        self.store.llm_upstream, self.store.llm_key = up.url, KEY
        return up

    def assert_llm_run(self, r, up):
        self.assertEqual((r.ran_on, r.exit_code), ("runner", 0), (r.infra_error, r.stderr_tail))
        out = r.stdout_tail
        for line in ("models 200 fake-model", "chat 200 echo:hi", "limited 429", "other 404",
                     "env-has-key False", "env-has-token False"):
            self.assertIn(line, out)
        # what reached the model server: only the two forwarded routes, always with the queue host's key
        self.assertEqual([(m, p) for m, p, _, _ in up.seen],
                         [("GET", "/v1/models"), ("POST", "/v1/chat/completions"), ("POST", "/v1/chat/completions")])
        self.assertEqual({a for _, _, a, _ in up.seen}, {"Bearer " + KEY})
        self.assertEqual(json.loads(up.seen[1][3])["messages"][0]["content"], "hi")
        self.assertEqual(r.timings.get("llm"), {"calls": 3, "errors": 1})
        self.assertEqual((self.job()["llm_calls"], self.job().get("llm_errors")), (3, 1))   # the 429 is an error
        self.assertEqual({k: r.llm[k] for k in ("calls", "errors", "held")}, {"calls": 3, "errors": 1, "held": 0})

    def pending_and_polled(self):
        """The newest job is pending while a runner is polling the queue."""
        with self.store.lock:
            js = list(self.store.jobs.values())
            return bool(js and js[-1]["status"] == "pending" and self.store.runner_seen)


class TestSetupPhase(RemoteBase):
    def test_setup_runs_first_with_network_env_and_logs(self):
        self.start_agent()
        r = self.submit('cat made-by-setup; echo "RUN PIP_NO_INDEX=${PIP_NO_INDEX:-unset}"',
                        setup='echo "PIP=${PIP_NO_INDEX:-unset} UVC=$UV_CACHE_DIR UVD=$UV_PYTHON_DOWNLOADS" > made-by-setup')
        self.assertEqual((r.ran_on, r.exit_code), ("runner", 0), r.infra_error)
        self.assertIn("PIP=unset UVC=%s UVD=never" % os.path.join(self.td, "cache", "uv"), r.stdout_tail)
        self.assertIn("RUN PIP_NO_INDEX=1", r.stdout_tail)       # the isolated run keeps failing fast offline
        self.assertIn("setup_s", r.timings)

    def test_setup_failure_is_a_runner_failure_with_its_tail(self):
        self.start_agent()
        marker = os.path.join(self.td, "ran")
        with self.assertRaises(RuntimeError) as cm:
            self.submit("touch %s" % marker, setup="echo resolving; echo 'no matching distribution' >&2; exit 3")
        self.assertIn("setup failed (rc=3)", str(cm.exception))
        self.assertIn("resolving\nno matching distribution", str(cm.exception))
        self.assertFalse(os.path.exists(marker))
        self.assertEqual(self.job()["status"], "failed_infra")

    def test_setup_timeout_is_clamped_and_kills_it(self):
        self.start_agent(CPU_RUNNER_SETUP_TIMEOUT_S="2")
        marker = os.path.join(self.td, "setup-survived")
        t0 = time.time()
        with self.assertRaises(RuntimeError) as cm:
            self.submit("true", setup={"cmd": "sleep 20; touch %s" % marker, "timeout_s": 600})
        self.assertIn("setup failed (timed out after 2s)", str(cm.exception))
        self.assertLess(time.time() - t0, 20)
        time.sleep(0.5)
        self.assertFalse(os.path.exists(marker))

    def test_setup_without_cmd_is_refused(self):
        self.start_agent()
        with self.assertRaises(RuntimeError) as cm:
            self.submit("true", setup={"timeout_s": 5})
        self.assertIn("spec.setup needs a cmd", str(cm.exception))

    def test_setup_features_never_fall_back_locally(self):
        for kw in ({"setup": "true"}, {"llm": True}, {"artifacts": True}):
            with self.assertRaises(ValueError):
                cpu_job.submit_cpu_job(self.repo, "true", 10, api="http://127.0.0.1:1", token=TOKEN, **kw)


class TestCancel(RemoteBase):
    def test_cancel_if_stops_a_job_in_setup(self):
        self.start_agent(CPU_RUNNER_HEARTBEAT_S="0.3")
        pidf, marker = os.path.join(self.td, "setup.pid"), os.path.join(self.td, "ran")
        t0 = time.time()
        r = self.submit("touch %s" % marker, setup="echo $$ > %s; exec sleep 30" % pidf,
                        cancel_if=lambda: os.path.exists(pidf) and os.path.getsize(pidf) and "operator stop")
        self.assertEqual((r.exit_code, r.infra_error, r.ran_on), (None, "cancelled: operator stop", "runner"))
        self.assertLess(time.time() - t0, 20)
        pid = int(open(pidf).read())
        self.assertTrue(wait_for(lambda: gone(pid), 15), "setup process survived the cancel")
        time.sleep(1)
        self.assertFalse(os.path.exists(marker))
        j = self.job()
        self.assertTrue(j["cancel"])
        self.assertIsNone(j["result"])

    def test_interrupted_caller_cancels_the_job(self):
        self.start_agent(CPU_RUNNER_HEARTBEAT_S="0.3")
        pidf = os.path.join(self.td, "run.pid")

        def interrupt():
            if os.path.exists(pidf) and os.path.getsize(pidf):
                raise KeyboardInterrupt
        with self.assertRaises(KeyboardInterrupt):
            self.submit("echo $$ > %s; exec sleep 30" % pidf, cancel_if=interrupt)
        self.assertTrue(self.job()["cancel"])
        pid = int(open(pidf).read())
        self.assertTrue(wait_for(lambda: gone(pid), 15), "the job kept running after its caller went away")


class TestLlmRelay(RemoteBase):
    def test_bridge_path_forwards_only_llm_routes_with_the_key_added_queue_side(self):
        up = self.upstream()
        self.start_agent()
        r = self.submit("python3 llm_client.py", llm=True)
        self.assert_llm_run(r, up)

    def test_no_llm_env_without_spec_llm(self):
        self.start_agent()
        r = self.submit('echo "URL=${CPU_RUNNER_LLM_URL:-unset} ART=${CPU_RUNNER_ARTIFACTS:-unset} PROG=$CPU_RUNNER_PROGRESS"')
        self.assertIn("URL=unset ART=unset PROG=", r.stdout_tail)
        self.assertTrue(r.stdout_tail.strip().endswith("/out/progress"))

    def test_netns_path_relay_subcommand_and_ready_handshake(self):
        """The isolated path: the wrapper starts `agent.py relay` in the background as the job user, the job waits for
        the ready file, and the relay dies with the job. `env` stands in for unshare (no netns here)."""
        if not port_free(agent_mod.LLM_PORT):
            self.skipTest("port %d busy on this host" % agent_mod.LLM_PORT)
        up = self.upstream()
        cfg = agent_mod.Config()
        cfg.api, cfg.token, cfg.runner_id = self.api, TOKEN, "inproc"
        cfg.cache_dir, cfg.work_dir = os.path.join(self.td, "cache"), os.path.join(self.td, "work")
        cfg.isolation, cfg.poll_s, cfg.heartbeat_s, cfg.concurrency = "required", 0.2, 0.5, 1
        cfg.health_file, cfg.status_file = os.path.join(self.td, "health"), os.path.join(self.td, "status")
        for d in (cfg.cache_dir, cfg.work_dir):
            os.makedirs(d, exist_ok=True)
        events = []
        with mock.patch.object(agent_mod, "log", lambda ev, **kw: events.append((ev, kw))):
            ag = agent_mod.Agent(cfg)
            ag.iso = ["env"]
            t = threading.Thread(target=ag.worker, daemon=True)
            t.start()
            try:
                r = self.submit('python3 llm_client.py; echo "URL=$CPU_RUNNER_LLM_URL"; ls -a "$TMPDIR"', llm=True)
            finally:
                ag.stop.set()
                t.join(15)
        self.assert_llm_run(r, up)
        self.assertIn("URL=http://127.0.0.1:%d/v1" % agent_mod.LLM_PORT, r.stdout_tail)
        self.assertRegex(r.stdout_tail, r"\.cpu-runner-relay-ready-[0-9a-f]{12}")
        self.assertTrue(wait_for(lambda: port_free(agent_mod.LLM_PORT), 5), "relay outlived its job")
        self.assertNotIn("llm_relay_error", [e for e, _ in events])

    def test_relay_selftest_passes_without_a_netns(self):
        if not port_free(agent_mod.LLM_PORT):
            self.skipTest("port %d busy on this host" % agent_mod.LLM_PORT)
        cfg = agent_mod.Config()
        cfg.work_dir = os.path.join(self.td, "work")
        os.makedirs(cfg.work_dir, exist_ok=True)
        ok, detail = agent_mod.relay_selftest(cfg, ["env"])
        self.assertTrue(ok, detail)
        self.assertEqual(os.listdir(cfg.work_dir), [])            # its scratch dir is cleaned up

    def test_relay_subcommand_bridges_and_exits_with_its_parent(self):
        d = tempfile.mkdtemp(dir=self.td)
        sock_path, ready, pidf = os.path.join(d, "s.sock"), os.path.join(d, "ready"), os.path.join(d, "pid")
        u = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        u.bind(sock_path)
        u.listen(4)
        self.addCleanup(u.close)
        got = []

        def serve():
            c, _ = u.accept()
            req = b""
            while b"\r\n\r\n" not in req:
                b = c.recv(4096)
                if not b:
                    break
                req += b
            got.append(req.split(b"\r\n", 1)[0])
            c.sendall(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok")
            c.close()
        threading.Thread(target=serve, daemon=True).start()
        s = socket.socket()
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]
        s.close()
        probe = ("import urllib.request as u; print(u.build_opener(u.ProxyHandler({}))"
                 ".open('http://127.0.0.1:%d/v1/models', timeout=10).read().decode())" % port)
        script = ('"$0" -I "$1" relay 127.0.0.1:%d "$2" "$3" & echo $! > "$4"; '
                  'while [ ! -e "$3" ]; do sleep 0.05; done; "$0" -c "$5"' % port)
        with open(os.path.join(d, "out"), "wb") as out:
            r = subprocess.run(["sh", "-c", script, sys.executable, AGENT, sock_path, ready, pidf, probe],
                               stdout=out, stderr=subprocess.STDOUT, timeout=30)
        self.assertEqual(r.returncode, 0, open(os.path.join(d, "out")).read())
        self.assertEqual(open(os.path.join(d, "out")).read().strip(), "ok")
        self.assertEqual(got, [b"GET /v1/models HTTP/1.1"])
        pid = int(open(pidf).read())
        self.assertTrue(wait_for(lambda: gone(pid), 5), "relay outlived its parent")

    def test_relay_subcommand_usage(self):
        r = subprocess.run([sys.executable, AGENT, "relay"], capture_output=True, text=True, timeout=30)
        self.assertEqual(r.returncode, 2)
        self.assertIn("usage", r.stderr)


class TestArtifacts(RemoteBase):
    def test_round_trip_is_checked_and_links_are_not_followed(self):
        self.start_agent()
        dest = os.path.join(self.td, "got.tgz")
        r = self.submit('mkdir -p "$CPU_RUNNER_ARTIFACTS/sub" && echo hello > "$CPU_RUNNER_ARTIFACTS/sub/a.txt" && '
                        'ln -s /etc/hostname "$CPU_RUNNER_ARTIFACTS/link"', artifacts=True, artifacts_dest=dest)
        self.assertEqual((r.ran_on, r.exit_code), ("runner", 0), r.infra_error)
        self.assertNotIn("error", r.artifacts)
        self.assertEqual(r.artifacts_path, dest)
        data = open(dest, "rb").read()
        self.assertEqual((len(data), hashlib.sha256(data).hexdigest()), (r.artifacts["bytes"], r.artifacts["sha256"]))
        with tarfile.open(dest) as tf:
            m = {x.name[2:] if x.name.startswith("./") else x.name: x for x in tf.getmembers()}
            self.assertEqual(tf.extractfile(m["sub/a.txt"]).read(), b"hello\n")
            self.assertTrue(m["link"].issym())
            self.assertEqual(m["link"].linkname, "/etc/hostname")
        self.assertIn("artifacts_s", r.timings)

    def test_default_dest_is_a_temp_file(self):
        self.start_agent()
        r = self.submit('echo x > "$CPU_RUNNER_ARTIFACTS/x"', artifacts=True)
        self.addCleanup(lambda: r.artifacts_path and os.path.exists(r.artifacts_path) and os.unlink(r.artifacts_path))
        self.assertTrue(r.artifacts_path and os.path.isfile(r.artifacts_path), r.artifacts)

    def test_over_the_cap_is_reported_not_uploaded(self):
        self.start_agent(CPU_RUNNER_ARTIFACTS_MAX="2000")
        r = self.submit('head -c 200000 /dev/urandom > "$CPU_RUNNER_ARTIFACTS/big"', artifacts=True)
        self.assertEqual(r.exit_code, 0)
        self.assertIn("over CPU_RUNNER_ARTIFACTS_MAX", r.artifacts["error"])
        self.assertIsNone(r.artifacts_path)
        self.assertFalse(self.job().get("artifacts"))

    def test_corrupt_download_is_rejected(self):
        self.start_agent()
        real = cpu_job._get_raw

        def flaky(api, tok, path, timeout=60, sink=None):
            if path.endswith("/artifacts") and sink is not None:
                sink(b"not the tarball")
                return 200, None, {}
            return real(api, tok, path, timeout, sink)
        with mock.patch.object(cpu_job, "_get_raw", flaky):
            r = self.submit('echo x > "$CPU_RUNNER_ARTIFACTS/x"', artifacts=True,
                            artifacts_dest=os.path.join(self.td, "bad.tgz"))
        self.assertIn("artifacts download failed", r.artifacts["error"])
        self.assertIsNone(r.artifacts_path)
        self.assertFalse(os.path.exists(os.path.join(self.td, "bad.tgz")))


class TestLogStream(RemoteBase):
    def test_streams_setup_then_stdout_with_progress(self):
        self.start_agent(CPU_RUNNER_LOG_INTERVAL_S="0.2")
        chunks, progress = [], []
        r = self.submit('echo run-1; echo "step 1/2" > "$CPU_RUNNER_PROGRESS"; sleep 1.5; echo run-2; echo hidden >&2; '
                        'echo "step 2/2" > "$CPU_RUNNER_PROGRESS"; sleep 1.5; echo run-3',
                        setup="echo setup-out; echo setup-err >&2", on_log=chunks.append,
                        on_progress=lambda ph, pr: progress.append((ph, pr)))
        self.assertEqual(r.exit_code, 0, r.infra_error)
        self.assertEqual(b"".join(chunks), b"setup-out\nsetup-err\nrun-1\nrun-2\nrun-3\n")
        self.assertGreater(len(chunks), 1)                  # streamed while running, not only drained at the end
        self.assertIn(("running", "step 1/2"), progress)
        self.assertIn(("running", "step 2/2"), progress)
        self.assertEqual(self.job()["progress"], "step 2/2")

    def test_log_is_capped_but_the_result_tail_is_not(self):
        self.start_agent(CPU_RUNNER_LOG_INTERVAL_S="0.2", CPU_RUNNER_LOG_MAX="1000")
        chunks = []
        r = self.submit("head -c 5000 /dev/zero | tr '\\0' x; echo", on_log=chunks.append)
        log = b"".join(chunks)
        self.assertEqual(log, b"x" * 1000 + NOTE)
        self.assertEqual(r.stdout_tail.count("x"), 5000)

    def test_resyncs_when_the_queue_loses_bytes(self):
        self.start_agent(CPU_RUNNER_LOG_INTERVAL_S="0.2")
        res = {}
        t = threading.Thread(target=lambda: res.update(
            r=self.submit("echo first; sleep 2; echo after; sleep 0.5", log_stream=True)))
        t.start()

        def lose():
            with self.store.lock:
                js = list(self.store.jobs.values())
                if js and bytes(js[0].get("log") or b"") == b"first\n":
                    js[0]["log"] = bytearray()          # the queue restarted and lost what it had
                    return True
        self.assertTrue(wait_for(lose, 30), "log never arrived")
        t.join(60)
        self.assertEqual(res["r"].exit_code, 0)
        self.assertEqual(bytes(self.job()["log"]), b"after\n")

    def test_heartbeat_carries_phase_and_progress_without_log_stream(self):
        self.start_agent(CPU_RUNNER_HEARTBEAT_S="0.3")
        res = {}
        t = threading.Thread(target=lambda: res.update(
            r=self.submit('echo "half way" > "$CPU_RUNNER_PROGRESS"; sleep 3')))
        t.start()

        def seen():
            with self.store.lock:
                for j in self.store.jobs.values():
                    if j.get("progress") == "half way":
                        return j.get("phase"), j["progress"]
        got = wait_for(seen, 30)
        t.join(60)
        self.assertEqual(got, ("running", "half way"))
        self.assertEqual(res["r"].exit_code, 0)
        self.assertNotIn("log", self.job())                 # no log_stream: nothing was posted


class TestJobDir(RemoteBase):
    def test_layout_modes(self):
        self.start_agent()
        r = self.submit('cd .. && stat -c "%a %n" . out dl && ls out')
        for line in ("711 .", "711 out", "700 dl", "progress", "setup.log", "stdout", "stderr"):
            self.assertIn(line, r.stdout_tail)


class TestQueueWait(RemoteBase):
    """What the client does while its job is not running: the claim clock, the queue's reason, the model hold, and a
    job its runner hands back."""

    def test_claim_timeout_names_the_queue_reason(self):
        progress = []
        with self.assertRaises(RuntimeError) as cm:          # no runner at all
            self.submit("true", claim_timeout_s=1, on_progress=lambda ph, pr: progress.append((ph, pr)))
        self.assertIn("no runner claimed within 1s (no runner online)", str(cm.exception))
        self.assertIn(("waiting", "no runner online"), progress)
        self.assertEqual(self.job()["status"], "cancelled")

    def test_model_hold_wait_is_not_claim_time_and_is_reported(self):
        up = self.upstream()
        self.store.model_hold = True
        self.start_agent()
        progress, res = [], {}
        t = threading.Thread(target=lambda: res.update(r=self.submit(
            "python3 llm_client.py", llm=True, claim_timeout_s=2, on_progress=lambda ph, pr: progress.append((ph, pr)))))
        t.start()
        self.assertTrue(wait_for(self.pending_and_polled, 30), "the job never reached the queue")
        time.sleep(3.5)                                    # the runner polls all along; claim_timeout_s is long gone
        self.assertEqual(self.job()["status"], "pending")
        self.assertIn(("waiting", queue_server.HOLD_WAITING), progress)
        self.store.model_hold = False
        t.join(60)
        r = res["r"]
        self.assert_llm_run(r, up)
        self.assertGreaterEqual(r.llm["hold_wait_s"], 3)
        self.assertEqual(self.job()["attempt"], 1)

    def test_a_job_handed_back_gets_a_fresh_claim_window_and_its_rerun_a_fresh_log(self):
        """Its runner is stopped mid-run (later than claim_timeout_s after submit): the agent kills the job and hands it
        back, it waits in the queue, and another runner runs it again from the start."""
        once = os.path.join(self.td, "once")
        a = self.start_agent(CPU_RUNNER_LOG_INTERVAL_S="0.2")
        chunks, res = [], {}
        t0 = time.time()
        cmd = "if [ -e %s ]; then echo second-run; else touch %s; echo first-run; sleep 30; fi" % (once, once)
        t = threading.Thread(target=lambda: res.update(r=self.submit(cmd, claim_timeout_s=5, on_log=chunks.append)))
        t.start()
        self.assertTrue(wait_for(lambda: b"first-run" in b"".join(chunks), 30), "the first run never streamed")
        time.sleep(max(0.0, 6 - (time.time() - t0)))
        a.terminate()                                      # graceful stop: kill the job, release it
        self.assertEqual(a.wait(30), 0)
        self.assertTrue(wait_for(lambda: self.job()["status"] == "pending", 10), self.job()["status"])
        time.sleep(1)                                      # the client sees it queued again
        self.assertTrue(t.is_alive(), res)
        self.start_agent(CPU_RUNNER_LOG_INTERVAL_S="0.2")
        t.join(60)
        r = res["r"]
        self.assertEqual((r.ran_on, r.exit_code), ("runner", 0), r.infra_error)
        self.assertEqual(r.stdout_tail.strip(), "second-run")
        self.assertEqual(b"".join(chunks), b"first-run\n\n[cpu_job] the job restarted (attempt 2): its output starts "
                                           b"again\nsecond-run\n")
        self.assertEqual(self.job()["attempt"], 2)

    def test_an_older_queue_view_still_works(self):
        """A queue from before the remote lane state answers GET jobs/<id> with id, status, attempt, result, runner_id."""
        self.start_agent()
        real = cpu_job._http

        def old(api, tok, method, path, body=None, raw=None, timeout=30):
            code, st = real(api, tok, method, path, body, raw, timeout)
            if method == "GET" and re.match(r"^/api/cpu/jobs/[0-9a-f]+$", path) and isinstance(st, dict):
                st = {k: st[k] for k in ("id", "status", "attempt", "result", "runner_id")}
            return code, st
        progress, chunks = [], []
        with mock.patch.object(cpu_job, "_http", old):
            r = self.submit("echo hi", llm=True, on_log=chunks.append, on_progress=lambda *a: progress.append(a))
        self.assertEqual((r.ran_on, r.exit_code, r.stdout_tail), ("runner", 0, "hi\n"), r.infra_error)
        self.assertEqual(r.llm, {"calls": 0, "errors": 0, "held": 0, "hold_wait_s": 0.0})
        self.assertEqual((progress, chunks), ([], []))


class TestQueueContract(Base):
    """The queue-side half of the new routes (reference/queue_server.py), as the real queue must implement it."""

    def put_job(self, spec, status="running", lease="L1"):
        jid = os.urandom(16).hex()
        with self.store.lock:
            self.store.jobs[jid] = {"id": jid, "spec": spec, "status": status, "attempt": 1, "blobs": {},
                                    "result": None, "lease_token": lease, "runner_id": "r",
                                    "lease_expires": time.time() + 600, "cancel": False, "events": [],
                                    "label": None, "created": time.time()}
            self.store.order.append(jid)
        return jid

    def req(self, method, path, body=None, raw=None, headers=None, token=TOKEN):
        data = raw if raw is not None else (json.dumps(body).encode() if body is not None else None)
        r = urllib.request.Request(self.api + path, data=data, method=method)
        r.add_header("Authorization", "Bearer " + token)
        for k, v in (headers or {}).items():
            r.add_header(k, v)
        try:
            with urllib.request.urlopen(r, timeout=10) as resp:
                return resp.status, resp.read(), resp.headers
        except urllib.error.HTTPError as e:
            return e.code, e.read(), e.headers

    def test_llm_route_checks_job_lease_and_spec(self):
        up = FakeUpstream()
        self.addCleanup(up.close)
        llm = self.put_job({"cmd": "x", "llm": True})
        plain = self.put_job({"cmd": "x"})
        path = "/api/cpu/jobs/%s/llm/v1/models"
        self.assertEqual(self.req("GET", path % llm, headers={"X-Lease-Token": "L1"}, token="bad")[0], 401)
        self.assertEqual(self.req("GET", path % llm, headers={"X-Lease-Token": "nope"})[0], 409)
        self.assertEqual(self.req("GET", path % plain, headers={"X-Lease-Token": "L1"})[0], 403)
        self.assertEqual(self.req("GET", path % ("0" * 32), headers={"X-Lease-Token": "L1"})[0], 404)
        self.assertEqual(self.req("GET", path % llm, headers={"X-Lease-Token": "L1"})[0], 503)      # no upstream yet
        self.store.llm_upstream, self.store.llm_key = up.url, KEY
        self.assertEqual(self.req("GET", "/api/cpu/jobs/%s/llm/v1/chat/completions" % llm,
                                  headers={"X-Lease-Token": "L1"})[0], 405)
        code, body, _ = self.req("GET", path % llm, headers={"X-Lease-Token": "L1"})
        self.assertEqual((code, json.loads(body)["data"][0]["id"]), (200, "fake-model"))
        self.assertEqual(up.seen, [("GET", "/v1/models", "Bearer " + KEY, b"")])
        self.assertEqual(self.req("GET", "/api/cpu/jobs/%s/llm/v1/embeddings" % llm,
                                  headers={"X-Lease-Token": "L1"})[0], 404)

    def test_log_append_is_offset_checked_and_idempotent(self):
        jid = self.put_job({"cmd": "x", "log_stream": True})
        post = lambda off, data, lease="L1": self.req("POST", "/api/cpu/jobs/%s/log" % jid, {
            "lease_token": lease, "off": off, "data_b64": __import__("base64").b64encode(data).decode(),
            "phase": "running", "progress": "p"})
        self.assertEqual(post(0, b"abc")[0], 200)
        self.assertEqual(post(0, b"abcdef")[0], 200)                    # a resend that overlaps appends only the rest
        code, body, _ = post(9, b"zz")
        self.assertEqual((code, json.loads(body)), (416, {"error": "gap", "size": 6}))
        self.assertEqual(post(6, b"g", lease="other")[0], 409)
        code, body, hdrs = self.req("GET", "/api/cpu/jobs/%s/log?off=2" % jid)
        self.assertEqual((code, body, hdrs["X-Log-Size"]), (200, b"cdef", "6"))
        st = json.loads(self.req("GET", "/api/cpu/jobs/%s" % jid)[1])
        self.assertEqual((st["log_size"], st["phase"], st["progress"]), (6, "running", "p"))

    def test_artifacts_need_the_live_lease(self):
        jid = self.put_job({"cmd": "x", "artifacts": True})
        path = "/api/cpu/jobs/%s/artifacts" % jid
        self.assertEqual(self.req("GET", path)[0], 404)
        self.assertEqual(self.req("PUT", path, raw=b"tgz", headers={"X-Lease-Token": "nope"})[0], 409)
        self.assertEqual(self.req("PUT", path, raw=b"tgz", headers={"X-Lease-Token": "L1"})[0], 200)
        self.assertEqual(self.req("GET", path)[:2], (200, b"tgz"))
        self.assertTrue(json.loads(self.req("GET", "/api/cpu/jobs/%s" % jid)[1])["has_artifacts"])

    def view(self, jid):
        return json.loads(self.req("GET", "/api/cpu/jobs/%s" % jid)[1])

    def claim(self, runner="r9"):
        code, body, _ = self.req("POST", "/api/cpu/claim", {"runner_id": runner, "lease_s": 60})
        return code, (json.loads(body)["job"] if code == 200 else None)

    def test_model_hold_keeps_llm_jobs_queued_and_turns_relay_calls_away(self):
        up = FakeUpstream()
        self.addCleanup(up.close)
        self.store.llm_upstream, self.store.llm_key = up.url, KEY
        self.store.model_hold = True
        queued = self.put_job({"cmd": "x", "llm": True}, status="pending", lease=None)
        plain = self.put_job({"cmd": "x"}, status="pending", lease=None)
        self.assertEqual(self.view(plain)["waiting"], "no runner online")
        code, got = self.claim()
        self.assertEqual((code, got["id"]), (200, plain))           # the older llm job is passed over
        self.assertEqual(self.claim()[0], 204)
        v = self.view(queued)
        self.assertEqual((v["status"], v["model_hold"], v["waiting"]), ("pending", True, queue_server.HOLD_WAITING))
        v = self.view(plain)
        self.assertEqual((v["model_hold"], v["waiting"]), (False, None))
        running = self.put_job({"cmd": "x", "llm": True})
        code, body, hdrs = self.req("GET", "/api/cpu/jobs/%s/llm/v1/models" % running, headers={"X-Lease-Token": "L1"})
        self.assertEqual((code, json.loads(body)["error"]["type"], hdrs["Retry-After"]), (503, "sandbox_hold", "30"))
        self.assertEqual(up.seen, [])                                # nothing went on to the model server
        v = self.view(running)
        self.assertEqual((v["model_hold"], v["waiting"], v["llm_calls"], v["llm_held"], v["llm_errors"]),
                         (True, None, 1, 1, 0))
        self.store.model_hold = False
        self.assertEqual(self.req("GET", "/api/cpu/jobs/%s/llm/v1/models" % running,
                                  headers={"X-Lease-Token": "L1"})[0], 200)
        self.assertEqual(self.req("POST", "/api/cpu/jobs/%s/llm/v1/chat/completions" % running,
                                  {"model": "boom", "messages": []}, headers={"X-Lease-Token": "L1"})[0], 429)
        v = self.view(running)
        self.assertEqual((v["model_hold"], v["llm_calls"], v["llm_held"], v["llm_errors"]), (False, 3, 1, 1))
        code, got = self.claim()
        self.assertEqual((code, got["id"]), (200, queued))           # claimable once the hold is gone

    def test_a_new_attempt_starts_clean(self):
        jid = self.put_job({"cmd": "x", "llm": True, "log_stream": True, "artifacts": True})
        with self.store.lock:
            self.store.jobs[jid].update(log=bytearray(b"old output"), phase="running", progress="9/10", artifacts=b"t",
                                        llm_calls=4, llm_errors=1, llm_held=2)
        keys = ("log_size", "phase", "progress", "has_artifacts", "llm_calls", "llm_errors", "llm_held")
        self.assertEqual([self.view(jid)[k] for k in keys], [10, "running", "9/10", True, 4, 1, 2])
        self.assertEqual(self.req("POST", "/api/cpu/jobs/%s/release" % jid, {"lease_token": "L1"})[0], 200)
        code, got = self.claim("r2")
        self.assertEqual((code, got["id"], got["attempt"]), (200, jid, 2))
        self.assertEqual([self.view(jid)[k] for k in keys], [0, None, None, False, 0, 0, 0])
        self.assertEqual(self.req("GET", "/api/cpu/jobs/%s/log?off=0" % jid)[1], b"")


class TestRemoteCaps(unittest.TestCase):
    def test_feature_caps_interpreters_and_uv(self):
        c = agent_mod.detect_caps(run=FakeRun({"uv": "uv 0.12.24 (0a1b2c3d 2026-10-01)\n"}),
                                  which=lambda n: "/usr/local/bin/" + n if n in ("python3.12", "python3.13") else None)
        for k in ("setup_phase", "llm_relay", "artifacts", "log_stream"):
            self.assertIs(c[k], True)
        self.assertEqual((c["pythons"], c["uv"]), (["3.12", "3.13"], "0.12.24"))

    def test_garbage_uv_is_omitted(self):
        c = agent_mod.detect_caps(run=FakeRun({"uv": "error: nope"}), which=lambda n: None)
        self.assertNotIn("uv", c)
        self.assertEqual(c["pythons"], [])

    def test_wrapper_waits_for_the_relay(self):
        cfg = agent_mod.Config()
        cmd = agent_mod.isolation_prefix(cfg, agent_mod.NET_FLAGS_FULL, "RELAY", "/w/j/tmp/.ready")
        self.assertEqual(cmd[:6], ["unshare", "--net", "--pid", "--fork", "--kill-child", "--"])
        script = cmd[8]
        self.assertTrue(script.startswith("ip link set lo up 2>/dev/null; RELAY & "))
        self.assertIn("[ ! -e /w/j/tmp/.ready ]", script)
        self.assertTrue(script.endswith('exec "$@"'))


if __name__ == "__main__":
    unittest.main()
