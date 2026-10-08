"""provision.sh / deploy flow / agent token + reload behaviour. No Docker, no Unraid."""
import json, os, shutil, subprocess, sys, tempfile, threading, time, unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from test_e2e import Base, ROOT, TOKEN  # noqa: E402

PROV = os.path.join(ROOT, "scripts", "provision.sh")


def prov(share, token_src, api="http://10.9.8.7:7684", **extra):
    e = dict(os.environ, CPU_RUNNER_SHARE=share, CPU_RUNNER_TOKEN_SRC=token_src, CPU_RUNNER_API_URL=api)
    e.update(extra)
    return subprocess.run(["bash", PROV], env=e, capture_output=True, text=True)


@unittest.skipUnless(shutil.which("rsync"), "rsync not installed (provision.sh needs it; the Mac has it)")
class TestProvision(unittest.TestCase):
    def setUp(self):
        self.td = tempfile.mkdtemp(prefix="cpurunner-test-")
        self.share = os.path.join(self.td, "data", "dispatch-cpu-runner")
        self.tok = os.path.join(self.td, "cfg", "token")
        os.makedirs(os.path.dirname(self.tok)); os.makedirs(os.path.dirname(self.share))
        open(self.tok, "w").write("s3cr3t-token-value-0123456789abcdef\n")
        os.chmod(self.tok, 0o600)

    def tearDown(self):
        shutil.rmtree(self.td, ignore_errors=True)

    def rd(self, *p):
        return open(os.path.join(self.share, *p)).read()

    def test_fresh_provision_and_token_never_printed(self):
        r = prov(self.share, self.tok)
        self.assertEqual(r.returncode, 0, r.stderr + r.stdout)
        self.assertNotIn("s3cr3t", r.stdout + r.stderr)
        self.assertEqual(self.rd("secrets", "token"), open(self.tok).read())
        self.assertEqual(oct(os.stat(os.path.join(self.share, "secrets", "token")).st_mode & 0o777), "0o600")
        self.assertIn("CPU_RUNNER_API=http://10.9.8.7:7684", self.rd(".env"))
        self.assertNotIn("<MAC-LAN-IP>", self.rd(".env"))
        self.assertTrue(os.path.isdir(os.path.join(self.share, "cache")))
        for f in ("agent/agent.py", "Dockerfile", "docker-compose.yml", "deploy.sh", "update.sh"):
            self.assertTrue(os.path.exists(os.path.join(self.share, f)), f)
        for bad in ("tests", ".git", "__pycache__"):
            self.assertFalse(os.path.exists(os.path.join(self.share, bad)), bad)
        self.assertIn("REDEPLOY_NEEDED", r.stdout)

    def test_idempotent_and_env_refresh_keeps_user_settings(self):
        self.assertEqual(prov(self.share, self.tok).returncode, 0)
        env_p = os.path.join(self.share, ".env")
        s = open(env_p).read().replace("CPU_RUNNER_CONCURRENCY=2", "CPU_RUNNER_CONCURRENCY=5") + "MY_KEY=keepme\n"
        open(env_p, "w").write(s)
        tok_mtime = os.stat(os.path.join(self.share, "secrets", "token")).st_mtime_ns
        r = prov(self.share, self.tok)                        # same IP: nothing changes
        self.assertEqual(r.returncode, 0, r.stderr)
        self.assertEqual(open(env_p).read(), s)
        self.assertIn("already current", r.stdout)
        self.assertEqual(os.stat(os.path.join(self.share, "secrets", "token")).st_mtime_ns, tok_mtime)
        r = prov(self.share, self.tok, api="http://10.1.2.3:7684")   # Mac IP changed: only that key moves
        self.assertEqual(r.returncode, 0, r.stderr)
        new = open(env_p).read()
        self.assertEqual(new, s.replace("10.9.8.7", "10.1.2.3"))
        self.assertIn("CPU_RUNNER_CONCURRENCY=5", new); self.assertIn("MY_KEY=keepme", new)

    def test_token_rotation_and_creation(self):
        self.assertEqual(prov(self.share, self.tok).returncode, 0)
        open(self.tok, "w").write("rotated-token-value\n")
        self.assertEqual(prov(self.share, self.tok).returncode, 0)
        self.assertEqual(self.rd("secrets", "token"), "rotated-token-value\n")
        missing = os.path.join(self.td, "new", "token")        # absent Mac token is created, 64 hex
        r = prov(self.share, missing)
        self.assertEqual(r.returncode, 0, r.stderr)
        t = open(missing).read().strip()
        self.assertEqual(len(t), 64); int(t, 16)
        self.assertNotIn(t, r.stdout + r.stderr)
        self.assertEqual(self.rd("secrets", "token").strip(), t)

    def test_delete_only_in_managed_dirs_and_user_files_survive(self):
        self.assertEqual(prov(self.share, self.tok).returncode, 0)
        open(os.path.join(self.share, "agent", "stale.py"), "w").write("x")       # managed dir: removed
        open(os.path.join(self.share, "docker-compose.override.yml"), "w").write("keep")  # user file: kept
        open(os.path.join(self.share, "cache", "blob"), "w").write("keep")
        self.assertEqual(prov(self.share, self.tok).returncode, 0)
        self.assertFalse(os.path.exists(os.path.join(self.share, "agent", "stale.py")))
        self.assertTrue(os.path.exists(os.path.join(self.share, "docker-compose.override.yml")))
        self.assertTrue(os.path.exists(os.path.join(self.share, "cache", "blob")))

    def test_redeploy_flag_follows_stamp(self):
        self.assertEqual(prov(self.share, self.tok).returncode, 0)
        h = subprocess.run("cat Dockerfile docker-compose.yml .env | shasum -a 256 2>/dev/null || "
                           "cat Dockerfile docker-compose.yml .env | sha256sum", shell=True, cwd=self.share,
                           capture_output=True, text=True).stdout[:16]
        open(os.path.join(self.share, ".deployed-stamp"), "w").write(h + "\n")
        r = prov(self.share, self.tok)
        self.assertIn("NO_REDEPLOY_NEEDED", r.stdout)
        open(os.path.join(self.share, "Dockerfile"), "a").write("# changed\n")   # next sync restores it -> differs from stamp? no:
        r = prov(self.share, self.tok)                                            # sync overwrites; stamp matches again
        self.assertIn("NO_REDEPLOY_NEEDED", r.stdout)
        r = prov(self.share, self.tok, api="http://10.5.5.5:7684")                # .env changed -> redeploy
        self.assertIn("REDEPLOY_NEEDED", r.stdout)

    def test_unmounted_volume_refused(self):
        r = prov("/Volumes/definitely-not-mounted-xyz/dispatch-cpu-runner", self.tok)
        self.assertNotEqual(r.returncode, 0)
        self.assertIn("not mounted", r.stderr)
        self.assertFalse(os.path.exists("/Volumes/definitely-not-mounted-xyz"))

    def test_bad_api_url_rejected(self):
        self.assertNotEqual(prov(self.share, self.tok, api="http://<mac-lan-ip>:7684").returncode, 0)


class TestAgentToken(Base):
    def test_missing_token_file_exits_with_pointer(self):
        p = self.start_agent(CPU_RUNNER_TOKEN="", CPU_RUNNER_TOKEN_FILE=os.path.join(self.td, "nope"))
        out = p.communicate(timeout=20)[0]
        self.assertEqual(p.returncode, 4)
        self.assertIn("scripts/provision.sh", out)

    def test_empty_token_file_exits(self):
        f = os.path.join(self.td, "tok"); open(f, "w").write("\n")
        p = self.start_agent(CPU_RUNNER_TOKEN="", CPU_RUNNER_TOKEN_FILE=f)
        out = p.communicate(timeout=20)[0]
        self.assertEqual(p.returncode, 4); self.assertIn("scripts/provision.sh", out)

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
        self.assertIn("token_rejected", out); self.assertIn("provision.sh", out)

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


class TestScripts(unittest.TestCase):
    def test_shell_scripts_parse(self):
        for f in ("deploy.sh", "update.sh", "scripts/provision.sh"):
            r = subprocess.run(["bash", "-n", os.path.join(ROOT, f)], capture_output=True, text=True)
            self.assertEqual(r.returncode, 0, f + r.stderr)

    def test_deploy_requires_provisioned_files(self):
        td = tempfile.mkdtemp(prefix="cpurunner-test-")
        try:
            shutil.copy(os.path.join(ROOT, "deploy.sh"), td)
            fake = os.path.join(td, "fakebin"); os.makedirs(fake)
            open(os.path.join(fake, "docker"), "w").write("#!/bin/sh\n[ \"$1\" = compose ] && [ \"$2\" = version ] && exit 0\nexit 1\n")
            os.chmod(os.path.join(fake, "docker"), 0o755)
            r = subprocess.run(["bash", os.path.join(td, "deploy.sh")], capture_output=True, text=True,
                               env=dict(os.environ, PATH=fake + os.pathsep + os.environ["PATH"]))
            self.assertEqual(r.returncode, 1)
            self.assertIn("scripts/provision.sh on the Mac first", r.stdout)
        finally:
            shutil.rmtree(td, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
