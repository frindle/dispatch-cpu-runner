"""Agent + client + reference queue, end to end on one machine (no Docker needed).
Run: python3 -m unittest discover -s tests -v
Network isolation itself needs CAP_SYS_ADMIN and is proven by `agent.py selftest`
inside the container; here we test fail-closed behaviour and command construction.
"""
import json, os, signal, subprocess, sys, tempfile, textwrap, time, unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path[:0] = [os.path.join(ROOT, "client"), os.path.join(ROOT, "reference"), os.path.join(ROOT, "agent")]
import cpu_job, queue_server  # noqa: E402
import agent as agent_mod  # noqa: E402

TOKEN = "test-token-xyz"


def sh(cwd, *a):
    subprocess.run(a, cwd=cwd, check=True, capture_output=True,
                   env=dict(os.environ, GIT_AUTHOR_NAME="t", GIT_AUTHOR_EMAIL="t@t",
                            GIT_COMMITTER_NAME="t", GIT_COMMITTER_EMAIL="t@t"))


def make_repo(td, lock=True):
    r = os.path.join(td, "repo")
    os.makedirs(r)
    sh(r, "git", "init", "-q")
    open(os.path.join(r, "a.txt"), "w").write("committed\n")
    if lock:
        open(os.path.join(r, "package.json"), "w").write('{"name":"x","version":"1.0.0"}')
        open(os.path.join(r, "package-lock.json"), "w").write('{"lockfileVersion":3}')
    open(os.path.join(r, ".gitignore"), "w").write("ignored.txt\nnode_modules\n")
    sh(r, "git", "add", "-A")
    sh(r, "git", "commit", "-qm", "init")
    return r


class Base(unittest.TestCase):
    def setUp(self):
        self.td = tempfile.mkdtemp(prefix="cpurunner-test-")
        self.srv, self.store = queue_server.serve(TOKEN)
        self.api = "http://127.0.0.1:%d" % self.srv.server_address[1]
        self.procs = []
        # fake npm: counts invocations, creates node_modules/dep/index.js
        self.bin = os.path.join(self.td, "bin")
        os.makedirs(self.bin)
        self.count = os.path.join(self.td, "npm-count")
        p = os.path.join(self.bin, "npm")
        open(p, "w").write(textwrap.dedent("""\
            #!/bin/sh
            echo x >> "%s"
            mkdir -p node_modules/dep && echo 'module.exports=1' > node_modules/dep/index.js
            """ % self.count))
        os.chmod(p, 0o755)
        self.repo = make_repo(self.td)

    def tearDown(self):
        for p in self.procs:
            if p.poll() is None:
                p.kill()
                p.wait()
        self.srv.shutdown()
        subprocess.run(["rm", "-rf", self.td])

    def start_agent(self, **over):
        e = dict(os.environ, CPU_RUNNER_API=self.api, CPU_RUNNER_TOKEN=TOKEN,
                 CPU_RUNNER_CACHE=os.path.join(self.td, "cache"), CPU_RUNNER_WORK=os.path.join(self.td, "work"),
                 CPU_RUNNER_ISOLATION="none", CPU_RUNNER_POLL_S="0.2", CPU_RUNNER_HEARTBEAT_S="0.5",
                 CPU_RUNNER_LEASE_S="60", CPU_RUNNER_CONCURRENCY="1", CPU_RUNNER_SHUTDOWN_GRACE_S="2",
                 CPU_RUNNER_HEALTH_FILE=os.path.join(self.td, "health"), CPU_RUNNER_ID="r%d" % len(self.procs),
                 PATH=self.bin + os.pathsep + os.environ["PATH"], SECRET_THING="leak-me")
        e.update({k: str(v) for k, v in over.items()})
        p = subprocess.Popen([sys.executable, os.path.join(ROOT, "agent", "agent.py")], env=e,
                             stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        self.procs.append(p)
        return p

    def submit(self, cmd, **kw):
        kw.setdefault("api", self.api)
        kw.setdefault("token", TOKEN)
        return cpu_job.submit_cpu_job(self.repo, cmd, **kw)


class TestShipping(Base):
    def test_bundle_patch_untracked_and_env(self):
        self.start_agent()
        open(os.path.join(self.repo, "a.txt"), "w").write("dirty\n")          # uncommitted edit
        open(os.path.join(self.repo, "new.txt"), "w").write("untracked\n")    # untracked
        open(os.path.join(self.repo, "ignored.txt"), "w").write("ignored\n")  # must NOT ship
        idx_before = open(os.path.join(self.repo, ".git", "index"), "rb").read()
        r = self.submit("cat a.txt new.txt; test ! -e ignored.txt && echo noignored; git log --oneline | wc -l; "
                        "echo SECRET=${SECRET_THING:-unset} TOK=${CPU_RUNNER_TOKEN:-unset} KEEP=$VERIFY_X DROP=${EVIL:-unset}",
                        timeout_s=60, env={"VERIFY_X": "1", "EVIL": "1"}, tools={"t.sh": b"echo hi"})
        self.assertEqual(r.ran_on, "runner", r.infra_error)
        self.assertEqual(r.exit_code, 0, r.stderr_tail)
        out = r.stdout_tail
        self.assertIn("dirty", out); self.assertIn("untracked", out); self.assertIn("noignored", out)
        self.assertIn("SECRET=unset TOK=unset KEEP=1 DROP=unset", out)
        self.assertEqual(open(os.path.join(self.repo, ".git", "index"), "rb").read(), idx_before)

    def test_archive_mode_and_cwd(self):
        self.start_agent()
        os.makedirs(os.path.join(self.repo, "sub"))
        open(os.path.join(self.repo, "sub", "f"), "w").write("x")
        sh(self.repo, "git", "add", "-A"); sh(self.repo, "git", "commit", "-qm", "sub")
        r = self.submit("ls; pwd", cwd_rel="sub", mode="archive", timeout_s=60)
        self.assertEqual(r.ran_on, "runner"); self.assertIn("f", r.stdout_tail); self.assertTrue(r.stdout_tail.strip().endswith("sub"))


class TestDepsCache(Base):
    def test_second_run_hits_cache(self):
        self.start_agent()
        r1 = self.submit("test -f node_modules/dep/index.js && echo ok", timeout_s=60)
        r2 = self.submit("test -f node_modules/dep/index.js && echo ok", timeout_s=60)
        self.assertEqual((r1.exit_code, r2.exit_code), (0, 0))
        self.assertEqual(r1.timings["deps"]["cache"], "miss")
        self.assertEqual(r2.timings["deps"]["cache"], "hit")
        self.assertEqual(len(open(self.count).read().split()), 1)  # npm ran exactly once
        # lockfile change -> new key -> miss
        open(os.path.join(self.repo, "package-lock.json"), "w").write('{"lockfileVersion":3,"x":1}')
        sh(self.repo, "git", "commit", "-qam", "lock")
        r3 = self.submit("true", timeout_s=60)
        self.assertEqual(r3.timings["deps"]["cache"], "miss")
        self.assertEqual(len(open(self.count).read().split()), 2)


class TestTimeoutAndExit(Base):
    def test_timeout_kills_process_group(self):
        self.start_agent()
        marker = os.path.join(self.td, "alive")
        t0 = time.time()
        r = self.submit("(sleep 300 & echo $! > %s; wait) & sleep 300" % marker, timeout_s=3)
        self.assertTrue(r.timed_out); self.assertLess(time.time() - t0, 40)
        time.sleep(0.5)
        pid = int(open(marker).read())
        with self.assertRaises(ProcessLookupError):
            os.kill(pid, 0)

    def test_exit_code_and_tails(self):
        self.start_agent()
        r = self.submit("echo out; echo err >&2; exit 7", timeout_s=30)
        self.assertEqual((r.exit_code, r.stdout_tail.strip(), r.stderr_tail.strip()), (7, "out", "err"))


class TestLease(Base):
    def test_dead_runner_job_is_requeued(self):
        p = self.start_agent(CPU_RUNNER_LEASE_S="2", CPU_RUNNER_HEARTBEAT_S="0.5")
        import threading
        res = {}
        t = threading.Thread(target=lambda: res.update(r=self.submit("sleep 4; echo finished", timeout_s=60)))
        t.start()
        for _ in range(100):  # wait until claimed
            if any(j["status"] == "running" for j in self.store.jobs.values()):
                break
            time.sleep(0.1)
        time.sleep(1.0)
        p.kill(); p.wait()                       # runner dies hard: no release, no result
        time.sleep(3)                             # > lease_s
        self.start_agent(CPU_RUNNER_LEASE_S="30")  # fresh runner picks up the requeued job
        t.join(60)
        r = res["r"]
        self.assertEqual(r.ran_on, "runner", r.infra_error)
        self.assertIn("finished", r.stdout_tail)
        job = list(self.store.jobs.values())[0]
        self.assertEqual(job["attempt"], 2)
        self.assertIn("lease_expired", [e[0] for e in job["events"]])

    def test_lease_lost_kills_job_and_posts_nothing(self):
        self.start_agent(CPU_RUNNER_HEARTBEAT_S="0.3")
        import threading
        res = {}
        marker = os.path.join(self.td, "ran-to-end")
        def go():
            try:
                self.submit("sleep 6; touch %s" % marker, timeout_s=60, max_wait_s=3, fallback=False)
            except RuntimeError as e:
                res["err"] = str(e)   # no local fallback here, so the marker can only come from the runner
        t = threading.Thread(target=go)
        t.start()
        for _ in range(100):
            if any(j["status"] == "running" for j in self.store.jobs.values()):
                break
            time.sleep(0.1)
        with self.store.lock:   # steal the lease
            for j in self.store.jobs.values():
                j["lease_token"] = "someone-else"
        t.join(60)
        time.sleep(1)
        self.assertFalse(os.path.exists(marker))
        self.assertIn("max_wait", res.get("err", ""))


class TestFallbackAndSafety(Base):
    def test_unreachable_falls_back_local(self):
        r = cpu_job.submit_cpu_job(self.repo, "echo local-run", 30, api="http://127.0.0.1:1", token=TOKEN)
        self.assertEqual((r.ran_on, r.fell_back, r.exit_code), ("local", True, 0)); self.assertIn("local-run", r.stdout_tail)

    def test_no_api_configured_and_no_fallback(self):
        os.environ.pop("CPU_RUNNER_API", None)
        self.assertEqual(cpu_job.submit_cpu_job(self.repo, "true", 10).ran_on, "local")
        with self.assertRaises(RuntimeError):
            cpu_job.submit_cpu_job(self.repo, "true", 10, api="http://127.0.0.1:1", fallback=False)

    def test_no_claim_falls_back_and_cancels(self):
        r = self.submit("echo nobody-home", timeout_s=30, claim_timeout_s=1, poll_s=0.2)  # no agent running
        self.assertEqual((r.ran_on, r.exit_code), ("local", 0))
        self.assertEqual(list(self.store.jobs.values())[0]["status"], "cancelled")

    def test_local_timeout(self):
        r = cpu_job.run_local(self.repo, "sleep 60", 2)
        self.assertTrue(r.timed_out)

    def test_bad_token_rejected(self):
        r = cpu_job.submit_cpu_job(self.repo, "echo x", 10, api=self.api, token="wrong")
        self.assertTrue(r.fell_back)

    def test_isolation_required_fails_closed(self):
        # on a host without a working empty-netns wrapper the agent must refuse to start
        p = self.start_agent(CPU_RUNNER_ISOLATION="required", PATH="/nonexistent:/usr/bin:/bin")
        try:
            rc = p.wait(30)
        except subprocess.TimeoutExpired:
            self.skipTest("isolation works on this host (container-capable); covered by `agent.py selftest`")
        if rc == 0:
            self.fail("agent started")
        self.assertEqual(rc, 3)

    def test_isolation_command_shape(self):
        cfg = agent_mod.Config(); cfg.isolation = "required"
        cmd = agent_mod.isolation_prefix(cfg, agent_mod.NET_FLAGS_FULL)
        self.assertEqual(cmd[:6], ["unshare", "--net", "--pid", "--fork", "--kill-child", "--"])
        self.assertIn("ip link set lo up", " ".join(cmd))


if __name__ == "__main__":
    unittest.main()
