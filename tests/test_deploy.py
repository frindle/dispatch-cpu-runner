"""provision.sh / deploy flow / agent token + reload behaviour. No Docker, no Unraid."""
import json, os, shutil, subprocess, sys, tempfile, threading, time, unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from test_e2e import Base, ROOT, TOKEN  # noqa: E402



class TestAgentToken(Base):
    def test_missing_token_file_exits_with_pointer(self):
        p = self.start_agent(CPU_RUNNER_TOKEN="", CPU_RUNNER_TOKEN_FILE=os.path.join(self.td, "nope"))
        out = p.communicate(timeout=20)[0]
        self.assertEqual(p.returncode, 4)
        self.assertIn("CPU_RUNNER_ENROLL_CODE", out)

    def test_empty_token_file_exits(self):
        f = os.path.join(self.td, "tok"); open(f, "w").write("\n")
        p = self.start_agent(CPU_RUNNER_TOKEN="", CPU_RUNNER_TOKEN_FILE=f)
        out = p.communicate(timeout=20)[0]
        self.assertEqual(p.returncode, 4); self.assertIn("CPU_RUNNER_ENROLL_CODE", out)

    def test_token_from_file_accepted_and_preflight_logged(self):
        f = os.path.join(self.td, "tok"); open(f, "w").write(TOKEN + "\n")
        p = self.start_agent(CPU_RUNNER_TOKEN="", CPU_RUNNER_TOKEN_FILE=f)
        st = os.path.join(self.td, "status")
        for _ in range(100):
            time.sleep(0.1)
            try:
                if json.load(open(st)).get("api") == "ok":
                    break
            except Exception:
                pass
        s = json.load(open(st))
        self.assertEqual(s["api"], "ok")
        p.terminate(); out = p.communicate(timeout=20)[0]
        self.assertIn('"event": "api_ok"', out)
        self.assertNotIn(TOKEN, out)

    def test_wrong_token_logged_and_unhealthy(self):
        p = self.start_agent(CPU_RUNNER_TOKEN="wrong")
        st = os.path.join(self.td, "status")
        for _ in range(100):
            time.sleep(0.1)
            try:
                if json.load(open(st)).get("api") == "token_rejected":
                    break
            except Exception:
                pass
        self.assertEqual(json.load(open(st))["api"], "token_rejected")
        e = dict(os.environ, CPU_RUNNER_HEALTH_FILE=os.path.join(self.td, "health"), CPU_RUNNER_STATUS_FILE=st)
        hc = subprocess.run([sys.executable, os.path.join(ROOT, "agent", "agent.py"), "healthcheck"], env=e)
        self.assertEqual(hc.returncode, 1)
        p.terminate(); out = p.communicate(timeout=20)[0]
        self.assertIn("token_rejected", out); self.assertIn("enroll-code", out)

    def start_agent(self, **over):
        over.setdefault("CPU_RUNNER_STATUS_FILE", os.path.join(self.td, "status"))
        return super().start_agent(**over)


class TestReload(Base):
    """Agent code lives in a mounted dir; changing it makes the agent drain and re-exec WITHOUT dropping a leased job."""

    def setUp(self):
        super().setUp()
        self.code = os.path.join(self.td, "agentdir"); os.makedirs(self.code)
        self.agent_py = os.path.join(self.code, "agent.py")
        shutil.copy(os.path.join(ROOT, "agent", "agent.py"), self.agent_py)

    def start_from_copy(self):
        e = dict(os.environ, CPU_RUNNER_API=self.api, CPU_RUNNER_TOKEN=TOKEN,
                 CPU_RUNNER_CACHE=os.path.join(self.td, "cache"), CPU_RUNNER_WORK=os.path.join(self.td, "work"),
                 CPU_RUNNER_ISOLATION="none", CPU_RUNNER_POLL_S="0.2", CPU_RUNNER_HEARTBEAT_S="0.5",
                 CPU_RUNNER_CONCURRENCY="1", CPU_RUNNER_SHUTDOWN_GRACE_S="2", CPU_RUNNER_RELOAD_CHECK_S="0.3", CPU_RUNNER_ID="rl", PATH=self.bin + os.pathsep + os.environ["PATH"],
                 CPU_RUNNER_HEALTH_FILE=os.path.join(self.td, "health"),
                 CPU_RUNNER_STATUS_FILE=os.path.join(self.td, "status"))
        p = subprocess.Popen([sys.executable, self.agent_py], env=e, stdout=subprocess.PIPE,
                             stderr=subprocess.STDOUT, text=True)
        self.procs.append(p)
        self.lines = []
        threading.Thread(target=lambda: [self.lines.append(l) for l in p.stdout], daemon=True).start()
        return p

    def events(self, name):
        return [l for l in list(self.lines) if '"event": "%s"' % name in l]

    def wait_for(self, fn, secs=30):
        for _ in range(int(secs * 10)):
            if fn():
                return True
            time.sleep(0.1)
        return False

    def test_code_change_drains_then_reexecs_without_dropping_job(self):
        p = self.start_from_copy()
        self.assertTrue(self.wait_for(lambda: self.events("start")))
        res = {}
        t = threading.Thread(target=lambda: res.update(r=self.submit("sleep 4; echo survived", timeout_s=60)))
        t.start()
        self.assertTrue(self.wait_for(lambda: any(j["status"] == "running" for j in self.store.jobs.values())))
        pid = p.pid
        open(self.agent_py, "a").write("\n# new release\n")          # rsync lands a new agent.py
        self.assertTrue(self.wait_for(lambda: self.events("reload_drain")))
        self.assertEqual(self.events("reload_exec"), [])             # job still running: no exec yet
        t.join(60)
        r = res["r"]
        self.assertEqual((r.ran_on, r.exit_code), ("runner", 0), r.infra_error)
        self.assertIn("survived", r.stdout_tail)
        self.assertTrue(self.wait_for(lambda: self.events("reload_exec")))
        self.assertTrue(self.wait_for(lambda: len(self.events("start")) >= 2))   # re-exec'd: second start, same pid
        self.assertEqual(p.pid, pid); self.assertIsNone(p.poll())
        job = list(self.store.jobs.values())[0]
        self.assertEqual(job["attempt"], 1)                           # never re-queued / released
        self.assertNotIn("lease_expired", [e[0] for e in job["events"]])
        r2 = self.submit("echo after-reload", timeout_s=60)           # the new process serves jobs
        self.assertIn("after-reload", r2.stdout_tail)

    def test_broken_new_code_is_not_loaded(self):
        p = self.start_from_copy()
        self.assertTrue(self.wait_for(lambda: self.events("start")))
        open(self.agent_py, "a").write("\ndef broken(:\n")
        self.assertTrue(self.wait_for(lambda: self.events("reload_skipped")))
        time.sleep(1.5)
        self.assertEqual(self.events("reload_drain"), [])
        self.assertIsNone(p.poll())
        r = self.submit("echo still-fine", timeout_s=60)
        self.assertIn("still-fine", r.stdout_tail)

    def test_sigterm_during_drain_releases_job(self):
        p = self.start_from_copy()
        self.assertTrue(self.wait_for(lambda: self.events("start")))
        t = threading.Thread(target=lambda: self.submit("sleep 60", timeout_s=120, max_wait_s=100, claim_timeout_s=100), daemon=True)
        t.start()
        self.assertTrue(self.wait_for(lambda: any(j["status"] == "running" for j in self.store.jobs.values())))
        open(self.agent_py, "a").write("\n# v2\n")
        self.assertTrue(self.wait_for(lambda: self.events("reload_drain")))
        p.terminate()
        self.assertTrue(self.wait_for(lambda: p.poll() is not None, 60))
        job = list(self.store.jobs.values())[0]
        self.assertIn(job["status"], ("pending", "cancelled"), "".join(self.lines[-8:]))       # released, not lost


class TestEnroll(Base):
    """First start trades a single-use code for the token (stored 0600 in the state dir); later starts reuse it."""

    def agent(self, code="", **over):
        over.setdefault("CPU_RUNNER_STATE_DIR", os.path.join(self.td, "state"))
        over.setdefault("CPU_RUNNER_STATUS_FILE", os.path.join(self.td, "status"))
        return self.start_agent(CPU_RUNNER_TOKEN="", CPU_RUNNER_ENROLL_CODE=code, **over)

    def wait_status(self, want, secs=15):
        st = os.path.join(self.td, "status")
        for _ in range(int(secs * 10)):
            time.sleep(0.1)
            try:
                if json.load(open(st)).get("api") == want:
                    return True
            except Exception:
                pass
        return False

    def test_enroll_stores_token_and_later_starts_need_no_code(self):
        self.store.codes.add("code-one-AAAA")
        p = self.agent("code-one-AAAA")
        self.assertTrue(self.wait_status("ok"))
        tf = os.path.join(self.td, "state", "token")
        self.assertEqual(open(tf).read().strip(), TOKEN)
        self.assertEqual(oct(os.stat(tf).st_mode & 0o777), "0o600")
        self.assertEqual(self.store.enrolled, ["r0"])
        p.terminate(); out = p.communicate(timeout=20)[0]
        self.assertNotIn(TOKEN, out); self.assertNotIn("code-one-AAAA", out)
        self.assertEqual(self.store.codes, set())                      # consumed
        os.remove(os.path.join(self.td, "status"))
        p2 = self.agent("")                                            # no code any more: stored token is used
        self.assertTrue(self.wait_status("ok"))
        self.assertEqual(len(self.store.enrolled), 1)
        p2.terminate(); p2.communicate(timeout=20)

    def test_bad_code_exits_with_clear_error_and_stores_nothing(self):
        p = self.agent("not-a-real-code", CPU_RUNNER_API=self.api)
        out = p.communicate(timeout=60)[0]
        self.assertEqual(p.returncode, 4)
        self.assertIn("enroll_failed", out); self.assertIn("enroll-code", out)
        self.assertFalse(os.path.exists(os.path.join(self.td, "state", "token")))

    def test_rotated_token_is_replaced_when_a_fresh_code_is_configured(self):
        self.store.codes.add("code-one-AAAA")
        p = self.agent("code-one-AAAA")
        self.assertTrue(self.wait_status("ok"))
        p.terminate(); p.communicate(timeout=20)
        self.store.token = "rotated-token-2"; self.store.codes.add("code-two-BBBB")
        os.remove(os.path.join(self.td, "status"))
        p2 = self.agent("code-two-BBBB")
        self.assertTrue(self.wait_status("ok"))
        self.assertEqual(open(os.path.join(self.td, "state", "token")).read().strip(), "rotated-token-2")
        p2.terminate(); out = p2.communicate(timeout=20)[0]
        self.assertIn("reenrolled", out)

    def test_rotated_token_without_fresh_code_reports_clearly_and_is_unhealthy(self):
        self.store.codes.add("code-one-AAAA")
        p = self.agent("code-one-AAAA")
        self.assertTrue(self.wait_status("ok"))
        p.terminate(); p.communicate(timeout=20)
        self.store.token = "rotated-token-2"
        os.remove(os.path.join(self.td, "status"))
        p2 = self.agent("")
        self.assertTrue(self.wait_status("token_rejected"))
        p2.terminate(); out = p2.communicate(timeout=20)[0]
        self.assertIn("token_rejected", out); self.assertIn("enroll-code", out)

    def test_pasted_token_env_still_works(self):
        p = self.start_agent(CPU_RUNNER_STATE_DIR=os.path.join(self.td, "state"),
                             CPU_RUNNER_STATUS_FILE=os.path.join(self.td, "status"))
        self.assertTrue(self.wait_status("ok"))
        self.assertFalse(os.path.exists(os.path.join(self.td, "state", "token")))
        p.terminate(); p.communicate(timeout=20)

    def test_remote_config_applies_but_explicit_env_wins(self):
        self.store.config = {"concurrency": 3, "lease_s": 45, "isolation": "none", "node_options": "--max-old-space-size=1234"}
        self.store.codes.add("c-AAAAAAAA")
        p = self.agent("c-AAAAAAAA", CPU_RUNNER_CONCURRENCY="", CPU_RUNNER_LEASE_S="90")
        self.assertTrue(self.wait_status("ok"))
        p.terminate(); out = p.communicate(timeout=20)[0]
        pulled = [json.loads(l) for l in out.splitlines() if '"config_pulled"' in l][0]["applied"]
        self.assertEqual(pulled.get("concurrency"), 3)
        self.assertNotIn("lease_s", pulled)                            # explicit env wins
        self.assertNotIn("isolation", pulled)                          # never remote
        start = [json.loads(l) for l in out.splitlines() if '"event": "start"' in l][0]
        self.assertEqual(start["concurrency"], 3)


class TestScripts(unittest.TestCase):
    def test_shell_scripts_parse(self):
        for f in ("update.sh", "scripts/enroll.sh", "scripts/sandbox-e2e.sh"):
            r = subprocess.run(["bash", "-n", os.path.join(ROOT, f)], capture_output=True, text=True)
            self.assertEqual(r.returncode, 0, f + r.stderr)


if __name__ == "__main__":
    unittest.main()
