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


class TestClaimBody(unittest.TestCase):
    def test_caps_sent_first_then_only_every_resend_interval(self):
        a = agent_mod.Agent.__new__(agent_mod.Agent)
        a.cfg = type("C", (), {"runner_id": "r", "lease_s": 60})()
        a.caps, a._caps_sent = {"node": 26}, 0.0
        self.assertEqual(a.claim_body()["caps"], {"node": 26})
        a._caps_sent = time.time()
        self.assertNotIn("caps", a.claim_body())
        a._caps_sent = time.time() - agent_mod.Agent.CAPS_RESEND_S - 1
        self.assertIn("caps", a.claim_body())
        a.caps = {}
        self.assertNotIn("caps", a.claim_body())


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

    def test_dockerignore_keeps_the_requirements_file(self):
        self.assertNotIn("requirements-runner.txt", self.read(".dockerignore"))


if __name__ == "__main__":
    unittest.main()
