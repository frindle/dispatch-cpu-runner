"""The REAL agent + client against the REAL queue-side API (~/bin/ollama-queue-api.py with
~/bin/cpu_lane.py), plus the ~/bin/cpu_dispatch.py wrapper. Skipped when ~/bin lacks them.
Isolation is 'none' ONLY here (a Mac has no netns); the production default stays 'required'.
Run: python3 -W ignore -m unittest discover -s tests -v
"""
import importlib.util, json, os, subprocess, sys, tempfile, threading, time, unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BIN = os.path.expanduser(os.environ.get("CPU_LANE_BIN", "~/bin"))
HAVE = all(os.path.exists(os.path.join(BIN, f)) for f in ("ollama-queue-api.py", "cpu_lane.py", "cpu_dispatch.py"))
sys.path[:0] = [os.path.join(ROOT, "client"), os.path.join(ROOT, "tests"), BIN]
TOKEN = "integration-token"


@unittest.skipUnless(HAVE, "queue-side modules not in ~/bin")
class QueueIntegration(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.td = tempfile.mkdtemp(prefix="cpulane-int-")
        cls.tok = os.path.join(cls.td, "token")
        open(cls.tok, "w").write(TOKEN + "\n")
        os.environ.update(CPU_LANE_DIR=os.path.join(cls.td, "jobs"), CPU_RUNNER_TOKEN_FILE=cls.tok,
                          DISPATCH_VERIFY_SANDBOX="1", OLLAMA_QUEUE_NO_NOTIFY="1")
        import cpu_lane
        cls.cpu_lane = cpu_lane
        spec = importlib.util.spec_from_file_location("oqapi_int", os.path.join(BIN, "ollama-queue-api.py"))
        cls.api = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(cls.api)
        cls.srv = cls.api.ThreadingServer(("127.0.0.1", 0), cls.api.Handler)
        threading.Thread(target=cls.srv.serve_forever, daemon=True).start()
        cls.url = "http://127.0.0.1:%d" % cls.srv.server_address[1]
        os.environ["CPU_RUNNER_API"] = cls.url
        import cpu_dispatch
        cls.cd = cpu_dispatch
        from test_e2e import make_repo
        cls.repo = make_repo(cls.td)
        cls.procs = []

    @classmethod
    def tearDownClass(cls):
        for p in cls.procs:
            if p.poll() is None:
                p.kill(); p.wait()
        cls.srv.shutdown()
        subprocess.run(["rm", "-rf", cls.td])

    def setUp(self):
        for p in self.procs:                      # one test's runner must not serve the next
            if p.poll() is None:
                p.kill(); p.wait()

    def agent(self, **over):
        fake = os.path.join(self.td, "bin"); os.makedirs(fake, exist_ok=True)
        npm = os.path.join(fake, "npm")
        if not os.path.exists(npm):
            open(npm, "w").write("#!/bin/sh\nmkdir -p node_modules/dep\n"); os.chmod(npm, 0o755)
        e = dict(os.environ, CPU_RUNNER_API=self.url, CPU_RUNNER_TOKEN_FILE=self.tok, CPU_RUNNER_ISOLATION="none",
                 CPU_RUNNER_CACHE=os.path.join(self.td, "cache"), CPU_RUNNER_WORK=os.path.join(self.td, "work"),
                 CPU_RUNNER_POLL_S="0.2", CPU_RUNNER_HEARTBEAT_S="0.5", CPU_RUNNER_LEASE_S="60",
                 CPU_RUNNER_CONCURRENCY="1", CPU_RUNNER_SHUTDOWN_GRACE_S="2", CPU_RUNNER_ID="int-r%d" % len(self.procs),
                 CPU_RUNNER_HEALTH_FILE=os.path.join(self.td, "health"), PATH=fake + os.pathsep + os.environ["PATH"])
        e.update({k: str(v) for k, v in over.items()})
        p = subprocess.Popen([sys.executable, os.path.join(ROOT, "agent", "agent.py")], env=e,
                             stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        self.procs.append(p)
        return p

    def wait_runner(self):
        for _ in range(100):
            if self.cd._runner_online(self.url, TOKEN):
                return
            time.sleep(0.1)
        self.fail("runner never polled")

    def test_1_no_runner_falls_back_local_and_registers_marker(self):
        seen = {}

        def probe():
            for _ in range(60):
                o = self.cpu_lane.get_store().outstanding_by_bundle()
                if o.get("bLocal"):
                    seen["o"] = o
                    return
                time.sleep(0.1)
        t = threading.Thread(target=probe); t.start()
        r = self.cd.run_cpu_stage(self.repo, "sleep 1; echo local-ok", 30, "verify", "bLocal")
        t.join()
        self.assertEqual((r.ran_on, r.fell_back, r.exit_code), ("local", True, 0))
        self.assertIn("no CPU runner online", r.infra_error)
        self.assertIn("local-ok", r.stdout_tail)
        self.assertIn("bLocal", seen.get("o", {}), "local stage was invisible to the queue while running")
        self.assertEqual(self.cpu_lane.get_store().outstanding_by_bundle(), {}, "marker not cleared")

    def test_2_real_agent_runs_stage_and_bundle_is_outstanding_meanwhile(self):
        self.agent()
        self.wait_runner()
        seen = {}

        def probe():
            for _ in range(100):
                o = self.cpu_lane.get_store().outstanding_by_bundle()
                if o.get("bRemote"):
                    seen["o"] = o
                    return
                time.sleep(0.1)
        t = threading.Thread(target=probe); t.start()
        open(os.path.join(self.repo, "a.txt"), "w").write("dirty\n")
        r = self.cd.run_cpu_stage(self.repo, "sleep 1; cat a.txt; exit 5", 60, "final-verify", "bRemote",
                                  tools={"t.sh": b"echo hi"})
        t.join()
        subprocess.run(["git", "checkout", "a.txt"], cwd=self.repo)
        self.assertEqual((r.ran_on, r.exit_code), ("runner", 5), r.infra_error)
        self.assertIn("dirty", r.stdout_tail)
        self.assertIn("bRemote", seen.get("o", {}))
        self.assertEqual(self.cpu_lane.get_store().outstanding_by_bundle(), {})
        s = self.cpu_lane.get_store().summary()
        self.assertTrue(s["recent"] and s["recent"][0]["runner"].startswith("int-r"))
        self.assertTrue(any(x["claims"] >= 1 for x in s["runners"]))

    def test_3_runner_killed_mid_job_requeues_attempt_2(self):
        p = self.agent(CPU_RUNNER_LEASE_S="2")
        self.wait_runner()
        res = {}
        t = threading.Thread(target=lambda: res.update(r=self.cd.run_cpu_stage(
            self.repo, "sleep 3; echo survived", 60, "verify", "bKill")))
        t.start()
        s = self.cpu_lane.get_store()
        for _ in range(100):
            if s.summary()["running"]:
                break
            time.sleep(0.1)
        time.sleep(0.8)
        p.kill(); p.wait()
        time.sleep(3)
        self.agent(CPU_RUNNER_LEASE_S="30")
        t.join(90)
        r = res["r"]
        self.assertEqual(r.ran_on, "runner", r.infra_error)
        self.assertIn("survived", r.stdout_tail)
        rows = [x for x in s.summary()["recent"] if x["bundle"] == "bKill"]
        self.assertEqual(rows[0]["status"], "done")
        self.assertEqual(s.get(rows[0]["id"])["attempt"], 2)

    def test_4_producer_cancel_stops_running_job(self):
        self.agent()
        self.wait_runner()
        marker = os.path.join(self.td, "should-not-exist")
        import cpu_job
        res = {}

        def go():
            try:
                cpu_job.submit_cpu_job(self.repo, "sleep 8; touch %s" % marker, 60, bundle_id="bCancel",
                                       api=self.url, max_wait_s=2, fallback=False)
            except RuntimeError as e:
                res["e"] = str(e)
        t = threading.Thread(target=go); t.start(); t.join(60)
        time.sleep(2.5)       # heartbeat (0.5 s) sees cancel:true and kills the job
        self.assertFalse(os.path.exists(marker))
        self.assertIn("max_wait", res.get("e", ""))


if __name__ == "__main__":
    unittest.main()
