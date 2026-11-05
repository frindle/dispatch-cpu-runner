"""Runner capability report (node / python / modules / sqlite3) + the image recipe that backs it. No Docker."""
import json, os, re, subprocess, sys, time, unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from test_e2e import Base, ROOT  # noqa: E402
import agent as agent_mod  # noqa: E402


class FakeRun:
    """subprocess.run stand-in: maps argv[0] -> stdout."""
    def __init__(self, outs):
        self.outs = outs

    def __call__(self, argv, **kw):
        out = self.outs.get(argv[0])
        if out is None:
            raise FileNotFoundError(argv[0])
        return subprocess.CompletedProcess(argv, 0, stdout=out, stderr="")


PROBE = json.dumps({"minor": "3.14", "full": "3.14.8", "mods": ["flask", "requests", "paramiko", "flask"]})


class TestDetectCaps(unittest.TestCase):
    def test_reports_what_the_probes_see(self):
        c = agent_mod.detect_caps(run=FakeRun({"node": "v26.7.0\n", "python3": PROBE + "\n"}), which=lambda n: "/usr/bin/" + n)
        self.assertEqual((c["node"], c["node_full"]), (26, "v26.7.0"))
        self.assertEqual((c["python"], c["python_full"]), ("3.14", "3.14.8"))
        self.assertEqual(c["py_modules"], ["flask", "paramiko", "requests"])
        self.assertTrue(c["sqlite3"])
        self.assertEqual(c["agent"], agent_mod.VERSION)

    def test_missing_tools_are_omitted_not_guessed(self):
        c = agent_mod.detect_caps(run=FakeRun({}), which=lambda n: None)
        self.assertNotIn("node", c); self.assertNotIn("python", c); self.assertNotIn("py_modules", c)
        self.assertFalse(c["sqlite3"])

    def test_garbage_probe_output_is_ignored(self):
        c = agent_mod.detect_caps(run=FakeRun({"node": "not-a-version", "python3": "oops"}), which=lambda n: None)
        self.assertNotIn("node", c); self.assertNotIn("python", c)

    def test_real_probe_on_this_machine(self):
        c = agent_mod.detect_caps()
        self.assertRegex(c.get("python", ""), r"^\d+\.\d+$")
        self.assertIn("py_modules", c)


class TestImageRecipe(unittest.TestCase):
    """The image's requirements must cover what dispatch python stages import (the queue refuses uncovered stages)."""
    def test_requirements_cover_dispatch_project_imports(self):
        with open(os.path.join(ROOT, "requirements-runner.txt")) as fh:
            pins = {l.split("==")[0].strip().lower().replace("_", "-") for l in fh if l.strip() and not l.startswith("#")}
        for pkg in ("fastapi", "pydantic", "starlette", "httpx", "pytest", "uvicorn", "pyyaml", "flask", "requests", "paramiko"):
            self.assertIn(pkg, pins)

    def test_py_modules_come_from_installed_distributions_not_a_list(self):
        src = open(os.path.join(ROOT, "agent", "agent.py")).read()
        self.assertIn("packages_distributions", src)

    def test_reported_import_names_include_new_modules(self):
        mods = ["fastapi", "pydantic", "starlette", "httpx", "pytest", "uvicorn", "yaml"]
        probe = json.dumps({"minor": "3.14", "full": "3.14.8", "mods": mods})
        c = agent_mod.detect_caps(run=FakeRun({"python3": probe}), which=lambda n: "/usr/bin/" + n)
        for m in mods:
            self.assertIn(m, c["py_modules"])


class TestClaimBody(unittest.TestCase):
    def test_caps_sent_first_then_only_every_resend_interval(self):
        a = agent_mod.Agent.__new__(agent_mod.Agent)
        a.cfg = type("C", (), {"runner_id": "r", "lease_s": 60, "concurrency": 5, "lane_max": 2})()
        a.caps, a._caps_sent = {"node": 26}, 0.0
        a.disk, a._disk_sent, a._disk_sent_t = {}, None, 0.0
        self.assertEqual(a.claim_body()["caps"], {"node": 26})
        self.assertEqual(a.claim_body()["slots"], 5)   # saturation signal for the queue
        a._caps_sent = time.time()
        self.assertNotIn("caps", a.claim_body())
        a._caps_sent = time.time() - agent_mod.Agent.CAPS_RESEND_S - 1
        self.assertIn("caps", a.claim_body())
        a.caps = {}
        self.assertNotIn("caps", a.claim_body())

    def agent(self, **cfg):
        a = agent_mod.Agent.__new__(agent_mod.Agent)
        a.cfg = type("C", (), dict({"runner_id": "r", "lease_s": 60, "concurrency": 5, "lane_max": 2}, **cfg))()
        a.caps, a._caps_sent = {}, 0.0
        a.disk, a._disk_sent, a._disk_sent_t = {}, None, 0.0
        return a

    def test_the_limit_fields_ride_on_every_claim(self):
        a = self.agent()
        b = a.claim_body()
        self.assertEqual((b["accept"], b["hold"], b["lane_max"]), ("all", {}, 2))
        self.assertEqual(a.claim_body("plain")["accept"], "plain")
        self.assertEqual(a.claim_body("none")["accept"], "none")
        self.assertEqual(a.claim_body("lane")["accept"], "lane")      # a lane-only runner (CPU_RUNNER_ACCEPT=lane)
        hold = {"lane": "lane-job limit: 2 of 2 running"}
        self.assertEqual(a.claim_body("plain", hold)["hold"], hold)
        lane_only = dict(hold, plain=agent_mod.LANE_ONLY_HOLD)         # its standing hold: no plain jobs
        self.assertEqual(a.claim_body("none", lane_only)["hold"], lane_only)
        self.assertEqual(json.loads(json.dumps(a.claim_body("lane", lane_only))), a.claim_body("lane", lane_only))
        self.assertEqual(a.claim_body("all", None)["hold"], {})          # no hold is {} on the wire, never null
        self.assertEqual(json.loads(json.dumps(a.claim_body("plain", hold)))["hold"], hold)
        self.assertEqual(self.agent(lane_max=0).claim_body()["lane_max"], 0)       # 0 is a setting (lane jobs off)
        self.assertEqual((b["runner_id"], b["lease_s"], b["slots"]), ("r", 60, 5))

    def test_the_disk_report_goes_out_when_new_changed_or_stale(self):
        a = self.agent()
        self.assertNotIn("disk", a.claim_body())                      # nothing measured yet
        a.disk = {"work_gb": 1.0, "uv_gb": 0.5}
        b = a.claim_body()
        self.assertEqual(b["disk"], {"work_gb": 1.0, "uv_gb": 0.5})
        a.claim_sent(b, 200)
        self.assertNotIn("disk", a.claim_body())                      # same report, sent a moment ago
        a.disk = {"work_gb": 1.0, "uv_gb": 0.6}
        b = a.claim_body()
        self.assertEqual(b["disk"], {"work_gb": 1.0, "uv_gb": 0.6})   # a new measurement goes out at once
        a.claim_sent(b, 204)
        self.assertNotIn("disk", a.claim_body())
        a._disk_sent_t = time.time() - agent_mod.Agent.CAPS_RESEND_S + 5
        self.assertNotIn("disk", a.claim_body())
        a._disk_sent_t = time.time() - agent_mod.Agent.CAPS_RESEND_S - 1
        self.assertEqual(a.claim_body()["disk"], {"work_gb": 1.0, "uv_gb": 0.6})   # the queue drops old reports: refresh

    def test_only_a_claim_the_queue_took_counts_as_sent(self):
        a = self.agent()
        a.caps, a.disk = {"node": 26}, {"work_gb": 1.0}
        b = a.claim_body()
        self.assertIn("caps", b)
        self.assertIn("disk", b)
        for code in (401, 403, 409, 500, 503):
            a.claim_sent(b, code)
        self.assertEqual((a._caps_sent, a._disk_sent, a._disk_sent_t), (0.0, None, 0.0))
        a.claim_sent({"runner_id": "r"}, 204)                         # a claim that carried neither changes nothing
        self.assertEqual((a._caps_sent, a._disk_sent, a._disk_sent_t), (0.0, None, 0.0))
        a.claim_sent(b, 200)
        self.assertGreater(a._caps_sent, 0.0)
        self.assertEqual(a._disk_sent, {"work_gb": 1.0})
        self.assertGreater(a._disk_sent_t, 0.0)
        later = a.claim_body()
        self.assertNotIn("caps", later)
        self.assertNotIn("disk", later)


class TestCapsEndToEnd(Base):
    def test_agent_reports_caps_on_claim_and_jobs_get_offline_pip(self):
        self.start_agent()
        for _ in range(100):
            if self.store.runner_caps:
                break
            time.sleep(0.1)
        caps = list(self.store.runner_caps.values())[0]
        self.assertEqual(caps["v"], agent_mod.CAPS_VERSION)
        self.assertRegex(caps["python"], r"^\d+\.\d+$")
        r = self.submit("echo PIPNOINDEX=$PIP_NO_INDEX", timeout_s=60)
        self.assertIn("PIPNOINDEX=1", r.stdout_tail)


class TestImageRecipe(unittest.TestCase):
    """The python image cannot be built here (no Docker); pin its recipe so a drive-by edit cannot silently break it."""

    def read(self, name):
        with open(os.path.join(ROOT, name)) as f:
            return f.read()

    def test_dockerfile_pins_node26_and_a_checksummed_python(self):
        d = self.read("Dockerfile")
        self.assertTrue(d.startswith("FROM node:26-"))
        for arch in ("X86_64", "AARCH64"):
            self.assertRegex(d, r"ARG PY_SHA256_%s=[0-9a-f]{64}\n" % arch)
        self.assertRegex(d, r"ARG PY_VERSION=3\.14\.\d+")
        self.assertIn("sha256sum -c", d)
        self.assertIn("sqlite3", d)
        self.assertIn("ENV PATH=/opt/venv/bin:", d)
        self.assertIn('ENTRYPOINT ["/usr/bin/python3"', d)        # the agent stays on Debian's python
        self.assertIn("requirements-runner.txt", d)

    def test_requirements_are_pinned_and_cover_the_aw_imports(self):
        lines = [l.strip() for l in self.read("requirements-runner.txt").splitlines() if l.strip() and not l.startswith("#")]
        self.assertTrue(lines and all(re.match(r"^[A-Za-z0-9_.-]+==[0-9][\w.]*$", l) for l in lines), lines)
        names = {l.split("==")[0].lower() for l in lines}
        self.assertTrue({"flask", "requests", "paramiko"} <= names, names)

    def test_fastapi_ships_with_its_form_runtime_dep(self):
        # fastapi raises at import of any Form/File route without python-multipart; a static import scan
        # (the queue's eligibility check) cannot see that, so the image must carry it whenever fastapi is baked
        lines = [l.strip() for l in self.read("requirements-runner.txt").splitlines() if l.strip() and not l.startswith("#")]
        names = {l.split("==")[0].lower() for l in lines}
        if "fastapi" in names:
            self.assertIn("python-multipart", names)

    def test_reportlab_and_pillow_are_baked_and_smoke_tested(self):
        # cpu-lane log: stages importing reportlab fell back to the Mac (not in the runner's py_modules)
        lines = [l.strip() for l in self.read("requirements-runner.txt").splitlines() if l.strip() and not l.startswith("#")]
        names = {l.split("==")[0].lower() for l in lines}
        self.assertTrue({"reportlab", "pillow"} <= names, names)
        self.assertRegex(self.read("Dockerfile"), r"import [^;\n]*reportlab[^;\n]*PIL")

    def test_dockerignore_keeps_the_requirements_file(self):
        self.assertNotIn("requirements-runner.txt", self.read(".dockerignore"))


if __name__ == "__main__":
    unittest.main()
