"""Runner limits (README "Disk use"): the lane-job cap and its reservations, the free-space guard, the uv / npm /
node_modules cache caps, the claim's accept / hold / lane_max / disk fields and the job a runner hands back.
Units drive the Agent's gate, holds, disk thread and cache cleanup directly (free space is faked, caches are real files
in a temp dir); the end-to-end classes run the agent against the reference queue (and, where ~/bin has it, the real one).
Run: python3 -W ignore -m unittest discover -s tests -v
"""
import importlib.util, json, os, shlex, shutil, subprocess, sys, tempfile, threading, time, types, unittest
from unittest import mock

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from test_e2e import Base, ROOT, TOKEN, make_repo  # noqa: E402   (also puts agent/, client/ and reference/ on sys.path)
from test_remote import RemoteBase, wait_for  # noqa: E402
import agent as agent_mod  # noqa: E402
import cpu_job  # noqa: E402
import queue_server  # noqa: E402

LANE = {"cmd": "x", "setup": "uv sync"}
LLM = {"cmd": "x", "llm": True}
PLAIN = {"cmd": "x"}
CLEARING_NPM = "clearing the npm cache (over its cap)"


def job(spec, jid="J1"):
    """A claim as the queue hands it out."""
    return {"id": jid, "spec": spec, "lease_token": "L-" + jid, "attempt": 1}


def runner_env(td, **extra):
    """What a Config() sees: nothing from the real environment (a CPU_RUNNER_* variable there must not leak in)."""
    e = {"CPU_RUNNER_API": "http://127.0.0.1:1", "CPU_RUNNER_TOKEN": "unit-token", "CPU_RUNNER_ID": "unit",
         "CPU_RUNNER_STATE_DIR": os.path.join(td, "state"), "CPU_RUNNER_WORK": os.path.join(td, "work"),
         "CPU_RUNNER_CACHE": os.path.join(td, "cache"), "CPU_RUNNER_HEALTH_FILE": os.path.join(td, "health"),
         "CPU_RUNNER_STATUS_FILE": os.path.join(td, "status"), "CPU_RUNNER_POLL_S": "0.25"}
    e.update({"CPU_RUNNER_" + k: str(v) for k, v in extra.items()})
    return mock.patch.dict(os.environ, e, clear=True)


def make_config(td, **extra):
    with runner_env(td, **extra):
        return agent_mod.Config()


def aged(path, age_s):
    """Make path look last used age_s seconds ago."""
    t = time.time() - age_s
    os.utime(path, (t, t))


def blob(path, nbytes):
    """A file of incompressible bytes (so every filesystem allocates them)."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "wb") as f:
        f.write(os.urandom(nbytes))
    return path


class FakeApi:
    """agent.api stand-in: records (method, path, body as sent); each call takes the next answer, then 204. An answer is
    (code, body), an Exception to raise, or a callable(method, path, body) -> (code, body)."""

    def __init__(self, *answers):
        self.answers, self.calls, self.lock = list(answers), [], threading.Lock()

    def call(self, method, path, body=None, raw=None, timeout=30, **kw):
        with self.lock:
            self.calls.append((method, path, json.loads(json.dumps(body))))
            ans = self.answers.pop(0) if self.answers else (204, None)
        if isinstance(ans, Exception):
            raise ans
        return ans(method, path, body) if callable(ans) else ans


class Ticks:
    """agent.stop stand-in for a loop: wait() returns at once and is recorded; the stop reads as set after n waits."""

    def __init__(self, n):
        self.n, self.waits = n, []

    def wait(self, t=None):
        self.waits.append(t)
        return self.is_set()

    def is_set(self):
        return len(self.waits) > self.n

    def set(self):
        self.n = -1


class UnitBase(unittest.TestCase):
    """An Agent over a temp dir whose free space is `self.free` and whose log lines land in `self.logged`."""
    ENV = {}

    def setUp(self):
        self.td = tempfile.mkdtemp(prefix="cpurunner-limits-")
        self.addCleanup(shutil.rmtree, self.td, ignore_errors=True)
        self.free, self.reads, self.logged = {}, [], []
        for name, fake in (("free_gb", self._free_gb), ("log", self._log)):
            p = mock.patch.object(agent_mod, name, fake)
            p.start()
            self.addCleanup(p.stop)
        self.build(**self.ENV)

    def build(self, **env):
        self.cfg = make_config(self.td, **env)
        for d in (self.cfg.work_dir, self.cfg.cache_dir):
            os.makedirs(d, exist_ok=True)
        self.a = agent_mod.Agent(self.cfg)
        self.addCleanup(self._release)
        return self.a

    def _release(self):             # no test leaves a cleanup thread waiting on the gate
        self.a.stop.set()
        with self.a.gate:
            self.a.gate.notify_all()

    def _free_gb(self, path):
        self.reads.append(path)
        return self.free.get(path, 500.0)

    def _log(self, event, **kw):
        self.logged.append((event, kw))

    def events(self, name):
        return [kw for ev, kw in self.logged if ev == name]

    def set_free(self, work=500.0, cache=500.0):
        self.free[self.cfg.work_dir], self.free[self.cfg.cache_dir] = work, cache
        self.a._free_t = 0.0

    def holds(self):
        with self.a.gate:
            return self.a.holds()

    def cycle(self, claim=None):
        """One claim's trip through the gate: plan it, let the queue answer with `claim`, settle it."""
        accept, h, reserved = self.a.plan_claim()
        self.a.claim_done(accept, reserved, claim)
        return accept, h, reserved


class TestLimitConfig(unittest.TestCase):
    def setUp(self):
        self.td = tempfile.mkdtemp(prefix="cpurunner-limits-")
        self.addCleanup(shutil.rmtree, self.td, ignore_errors=True)

    def values(self, c):
        return (c.lane_max, c.min_free_gb, c.uv_cache_max_gb, c.npm_cache_max_gb, c.nm_cache_max_gb, c.disk_check_s)

    def test_defaults(self):
        c = make_config(self.td)
        self.assertEqual(self.values(c), (2, 6.0, 10.0, 4.0, 6.0, 120.0))
        self.assertEqual(c.cache_max_entries, 8)

    def test_env_overrides(self):
        c = make_config(self.td, LANE_MAX=3, MIN_FREE_GB=12.5, UV_CACHE_MAX_GB=20, NPM_CACHE_MAX_GB=1.5,
                        NM_CACHE_MAX_GB=2.5, DISK_CHECK_S=30)
        self.assertEqual(self.values(c), (3, 12.5, 20.0, 1.5, 2.5, 30.0))

    def test_env_values_are_floored(self):
        c = make_config(self.td, LANE_MAX=-4, MIN_FREE_GB=-1, DISK_CHECK_S=1)
        self.assertEqual((c.lane_max, c.min_free_gb, c.disk_check_s), (0, 0.0, 5.0))

    def test_zero_is_a_setting_but_empty_is_unset(self):
        c = make_config(self.td, LANE_MAX=0, MIN_FREE_GB=0)
        self.assertEqual((c.lane_max, c.min_free_gb), (0, 0.0))
        c = make_config(self.td, LANE_MAX="", MIN_FREE_GB="", DISK_CHECK_S="")
        self.assertEqual((c.lane_max, c.min_free_gb, c.disk_check_s), (2, 6.0, 120.0))

    def test_the_queue_can_set_every_limit_within_bounds(self):
        with runner_env(self.td):
            c = agent_mod.Config()
            got = c.apply_remote({"lane_max": 99, "min_free_gb": -5, "uv_cache_max_gb": 0.1, "npm_cache_max_gb": "7",
                                  "nm_cache_max_gb": 20000})
        self.assertEqual(got, {"lane_max": 32, "min_free_gb": 0, "uv_cache_max_gb": 0.5, "npm_cache_max_gb": 7.0,
                               "nm_cache_max_gb": 10000})
        self.assertEqual((c.lane_max, c.min_free_gb, c.uv_cache_max_gb, c.npm_cache_max_gb, c.nm_cache_max_gb),
                         (32, 0, 0.5, 7.0, 10000))
        with runner_env(self.td):
            c = agent_mod.Config()
            self.assertEqual(c.apply_remote({"lane_max": -3, "min_free_gb": 10001}), {"lane_max": 0, "min_free_gb": 10000})

    def test_the_queue_can_switch_lane_jobs_off(self):
        with runner_env(self.td):
            c = agent_mod.Config()
            self.assertEqual(c.apply_remote({"lane_max": 0}), {"lane_max": 0})
        self.assertEqual(c.lane_max, 0)

    def test_env_wins_over_the_queue(self):
        with runner_env(self.td, LANE_MAX=1, UV_CACHE_MAX_GB=3):
            c = agent_mod.Config()
            got = c.apply_remote({"lane_max": 5, "uv_cache_max_gb": 20, "min_free_gb": 9})
        self.assertEqual(got, {"min_free_gb": 9.0})
        self.assertEqual((c.lane_max, c.uv_cache_max_gb, c.min_free_gb), (1, 3.0, 9.0))

    def test_garbage_from_the_queue_is_skipped(self):
        with runner_env(self.td):
            c = agent_mod.Config()
            before = self.values(c)
            got = c.apply_remote({"lane_max": "many", "min_free_gb": None, "uv_cache_max_gb": [1], "nm_cache_max_gb": {}})
            self.assertEqual((got, c.apply_remote("oops"), c.apply_remote(None)), ({}, {}, {}))
        self.assertEqual(self.values(c), before)

    def test_accept_is_all_unless_the_environment_says_lane(self):
        self.assertEqual(make_config(self.td).accept, "all")
        self.assertEqual(make_config(self.td, ACCEPT="").accept, "all")          # empty is unset, as for the other settings
        self.assertEqual(make_config(self.td, ACCEPT="lane").accept, "lane")
        self.assertEqual(make_config(self.td, ACCEPT=" LANE ").accept, "lane")   # case and spaces do not matter
        self.assertEqual(make_config(self.td, ACCEPT="All").accept, "all")

    def test_a_bad_accept_is_kept_as_it_is_for_setup_to_refuse(self):
        for v in ("plain", "none", "lanes", "bogus"):
            self.assertEqual(make_config(self.td, ACCEPT=v).accept, v)

    def test_the_queue_cannot_change_accept(self):
        self.assertNotIn("accept", agent_mod.Config.REMOTE)
        with runner_env(self.td):
            c = agent_mod.Config()
            self.assertEqual(c.apply_remote({"accept": "lane", "lane_max": 1}), {"lane_max": 1})
        self.assertEqual(c.accept, "all")                       # a queue cannot turn a runner into a lane-only one
        with runner_env(self.td, ACCEPT="lane"):
            c = agent_mod.Config()
            self.assertEqual(c.apply_remote({"accept": "all"}), {})
        self.assertEqual(c.accept, "lane")                      # ... or back


class TestIsLaneJob(unittest.TestCase):
    def test_a_lane_job_has_a_setup_or_an_llm(self):
        for spec in (LANE, {"cmd": "x", "setup": {"cmd": "uv sync"}}, LLM, {"cmd": "x", "llm": {"max_calls": 5}},
                     {"cmd": "x", "setup": "s", "llm": True}):
            self.assertTrue(agent_mod.is_lane_job(spec), spec)
            self.assertTrue(queue_server.is_lane(spec), spec)       # the queue applies the same test to `accept`

    def test_everything_else_is_a_plain_job(self):
        for spec in (PLAIN, {"cmd": "x", "setup": ""}, {"cmd": "x", "setup": None, "llm": False}, {"setup": {}}, {}):
            self.assertFalse(agent_mod.is_lane_job(spec), spec)
            self.assertFalse(queue_server.is_lane(spec), spec)
        for spec in (None, "setup", ["setup"], 7):
            self.assertFalse(agent_mod.is_lane_job(spec), spec)


class TestGate(UnitBase):
    """plan_claim / claim_done / job_done: how many lane jobs may be running or on their way."""

    def test_claims_reserve_a_lane_slot_until_the_queue_answers(self):
        a = self.a                                  # lane_max 2
        p1, p2, p3 = a.plan_claim(), a.plan_claim(), a.plan_claim()
        self.assertEqual((p1[0], p1[2]), ("all", True))
        self.assertEqual((p2[0], p2[2]), ("all", True))
        self.assertEqual((p3[0], p3[2]), ("plain", False))       # two lane jobs may already be on their way
        self.assertEqual((a.jobs_n, a.lane_resv, a.lane_n), (3, 2, 0))
        a.claim_done("all", True, None)
        a.claim_done("all", True, None)
        a.claim_done("plain", False, None)
        self.assertEqual((a.jobs_n, a.lane_resv, a.lane_n), (0, 0, 0))
        self.assertEqual(self.cycle()[0], "all")                # the slots are free again

    def test_concurrent_claims_never_reserve_more_than_lane_max(self):
        a = self.build(LANE_MAX=3)
        barrier, plans = threading.Barrier(8), []

        def go():
            barrier.wait(10)
            plans.append(a.plan_claim())
        ts = [threading.Thread(target=go) for _ in range(8)]
        for t in ts:
            t.start()
        for t in ts:
            t.join(10)
        self.assertEqual(sorted(p[0] for p in plans), ["all"] * 3 + ["plain"] * 5)
        self.assertEqual(sorted(p[2] for p in plans), [False] * 5 + [True] * 3)
        self.assertEqual((a.jobs_n, a.lane_resv, a.lane_n), (8, 3, 0))

    def test_running_lane_jobs_use_up_the_limit_and_the_hold_says_so(self):
        a = self.a
        for i in range(2):
            accept, h, reserved = a.plan_claim()
            self.assertEqual((accept, h, reserved), ("all", {}, True))
            self.assertEqual(a.claim_done(accept, reserved, job(LANE, "J%d" % i)), (True, True))
        self.assertEqual((a.lane_n, a.lane_resv, a.jobs_n), (2, 0, 2))
        self.assertEqual(a.plan_claim(), ("plain", {"lane": "lane-job limit: 2 of 2 running"}, False))
        self.assertEqual(a.jobs_n, 3)
        a.job_done(True)
        self.assertEqual((a.lane_n, a.jobs_n), (1, 2))
        self.assertEqual(a.plan_claim()[0], "all")              # one lane slot is free again

    def test_plain_jobs_do_not_count_against_the_lane_limit(self):
        a = self.a
        for i in range(5):
            accept, h, reserved = a.plan_claim()
            self.assertEqual((accept, a.claim_done(accept, reserved, job(PLAIN, "P%d" % i))), ("all", (True, False)))
            self.assertEqual(a.lane_n, 0)
        self.assertEqual((a.jobs_n, a.lane_resv), (5, 0))

    def test_a_plain_claim_that_brings_a_lane_job_is_not_run(self):
        a = self.build(LANE_MAX=0)                  # an older queue ignores accept and gives out the lane job anyway
        accept, h, reserved = a.plan_claim()
        self.assertEqual((accept, h, reserved), ("plain", {"lane": "lane jobs are off on this runner (lane_max 0)"}, False))
        self.assertEqual(a.jobs_n, 1)
        self.assertEqual(a.claim_done(accept, reserved, job(LANE)), (False, True))
        self.assertEqual((a.jobs_n, a.lane_n, a.lane_resv), (0, 0, 0))

    def test_a_plain_claim_that_brings_a_plain_job_runs_it(self):
        a = self.build(LANE_MAX=0)
        accept, h, reserved = a.plan_claim()
        self.assertEqual(a.claim_done(accept, reserved, job(PLAIN)), (True, False))
        self.assertEqual((a.jobs_n, a.lane_n), (1, 0))          # the running job holds its count until job_done
        a.job_done(False)
        self.assertEqual(a.jobs_n, 0)

    def test_a_claim_that_may_bring_nothing_gives_its_reservation_back(self):
        a = self.a
        accept, h, reserved = a.plan_claim()
        self.assertEqual((accept, reserved, a.lane_resv, a.jobs_n), ("all", True, 1, 1))
        self.assertEqual(a.claim_done(accept, reserved, None), (False, False))
        self.assertEqual((a.jobs_n, a.lane_resv, a.lane_n), (0, 0, 0))

    def test_a_lane_job_under_an_all_claim_takes_the_reservation_over(self):
        a = self.a
        accept, h, reserved = a.plan_claim()
        self.assertEqual(a.claim_done(accept, reserved, job(LLM)), (True, True))
        self.assertEqual((a.jobs_n, a.lane_n, a.lane_resv), (1, 1, 0))

    def test_a_none_claim_never_runs_what_it_gets(self):
        a = self.a
        a.cleanup = CLEARING_NPM
        for claim, lane in ((job(PLAIN), False), (job(LANE), True), (None, False)):
            accept, h, reserved = a.plan_claim()
            self.assertEqual((accept, h, reserved), ("none", {"all": CLEARING_NPM}, False))
            self.assertEqual(a.claim_done(accept, reserved, claim), (False, lane))
            self.assertEqual((a.jobs_n, a.lane_n, a.lane_resv), (0, 0, 0))

    def test_job_done_frees_the_counts_and_asks_for_a_re_measure(self):
        a = self.a
        a.jobs_n, a.lane_n, a._dirty, a._free_t = 2, 1, set(), 123.0
        a.job_done(True, ("uv", "npm"))
        self.assertEqual((a.jobs_n, a.lane_n, a._dirty, a._free_t), (1, 0, {"uv", "npm"}, 0.0))
        a.job_done(False)
        self.assertEqual((a.jobs_n, a.lane_n, a._dirty), (0, 0, {"uv", "npm"}))

    def test_a_none_or_plain_claim_costs_no_lane_slot(self):
        a = self.a
        self.set_free(work=4.0)                     # lane jobs held back by free space
        self.assertEqual(a.plan_claim()[0], "plain")
        self.assertEqual((a.jobs_n, a.lane_resv), (1, 0))
        self.set_free(work=1.0)                     # everything held back: no claim in flight at all
        self.assertEqual(a.plan_claim(), ("none", {"all": "low disk: 1.0 GB free on %s, every job needs 2.0 GB" % self.cfg.work_dir}, False))
        self.assertEqual((a.jobs_n, a.lane_resv), (1, 0))


class TestHolds(UnitBase):
    """The reasons a runner takes less work, word for word (they are what the queue shows a waiting job)."""

    def test_nothing_is_held_with_room_and_a_free_lane_slot(self):
        self.assertEqual(self.holds(), {})

    def test_lane_job_limit(self):
        self.a.lane_n = 2
        self.assertEqual(self.holds(), {"lane": "lane-job limit: 2 of 2 running"})
        self.a.lane_n = 1
        self.assertEqual(self.holds(), {})

    def test_lane_jobs_off(self):
        a = self.build(LANE_MAX=0)
        self.assertEqual(self.holds(), {"lane": "lane jobs are off on this runner (lane_max 0)"})

    def test_low_disk_holds_lane_jobs_naming_the_fuller_dir(self):
        self.set_free(work=7.3, cache=4.26)
        self.assertEqual(self.holds(), {"lane": "low disk: 4.3 GB free on %s, lane jobs need 6.0 GB" % self.cfg.cache_dir})
        self.set_free(work=3.04, cache=50.0)
        self.assertEqual(self.holds(), {"lane": "low disk: 3.0 GB free on %s, lane jobs need 6.0 GB" % self.cfg.work_dir})

    def test_low_disk_beats_the_lane_job_limit_in_the_text(self):
        self.a.lane_n = 2
        self.set_free(work=4.0)
        self.assertEqual(self.holds(), {"lane": "low disk: 4.0 GB free on %s, lane jobs need 6.0 GB" % self.cfg.work_dir})

    def test_below_two_gb_nothing_is_claimed(self):
        self.set_free(work=1.2, cache=30.0)
        self.assertEqual(self.holds(), {"all": "low disk: 1.2 GB free on %s, every job needs 2.0 GB" % self.cfg.work_dir})

    def test_the_limits_are_strict(self):
        for work, want in ((6.0, set()), (5.99, {"lane"}), (2.0, {"lane"}), (1.99, {"all"})):
            self.set_free(work=work)
            h = self.holds()
            self.assertEqual(set(h), want, (work, h))

    def test_the_any_job_floor_never_exceeds_min_free_gb(self):
        self.build(MIN_FREE_GB=1.0)
        self.set_free(work=0.8)
        self.assertEqual(self.holds(), {"all": "low disk: 0.8 GB free on %s, every job needs 1.0 GB" % self.cfg.work_dir})
        self.set_free(work=1.5)
        self.assertEqual(self.holds(), {})

    def test_min_free_gb_zero_never_holds(self):
        self.build(MIN_FREE_GB=0)
        self.set_free(work=0.0, cache=0.0)
        self.assertEqual(self.holds(), {})

    def test_unreadable_free_space_holds_nothing(self):
        self.set_free(work=None, cache=None)
        self.assertEqual(self.holds(), {})
        self.set_free(work=None, cache=1.5)         # one dir unreadable: the other one counts
        self.assertEqual(self.holds(), {"all": "low disk: 1.5 GB free on %s, every job needs 2.0 GB" % self.cfg.cache_dir})

    def test_a_cache_cleanup_holds_everything(self):
        self.a.cleanup = CLEARING_NPM
        self.assertEqual(self.holds(), {"all": CLEARING_NPM})

    def test_cleanup_text_and_low_disk_combine(self):
        self.a.cleanup = CLEARING_NPM
        self.set_free(work=4.0)
        self.assertEqual(self.holds(), {"all": CLEARING_NPM, "lane": "low disk: 4.0 GB free on %s, lane jobs need 6.0 GB" % self.cfg.work_dir})
        self.set_free(work=1.0)                     # the cleanup is the reason it already pauses: its text stays
        self.assertEqual(self.holds()["all"], CLEARING_NPM)

    def test_free_space_is_read_at_most_every_few_seconds(self):
        self.set_free(work=500.0)
        self.assertEqual(self.holds(), {})
        self.assertEqual(self.reads, [self.cfg.work_dir, self.cfg.cache_dir])
        self.free[self.cfg.work_dir] = 1.0          # the disk fills up, but the last reading is fresh
        self.assertEqual(self.holds(), {})
        self.assertEqual(len(self.reads), 2)
        self.a._free_t -= agent_mod.FREE_RECHECK_S + 1
        self.assertEqual(set(self.holds()), {"all"})
        self.assertEqual(len(self.reads), 4)

    def test_a_finished_job_forces_a_fresh_reading(self):
        self.set_free(work=500.0)
        self.assertEqual(self.holds(), {})
        self.free[self.cfg.work_dir] = 4.0          # a big job's scratch dir was still there a moment ago
        self.a.jobs_n = 1
        self.a.job_done(False)
        self.assertEqual(set(self.holds()), {"lane"})


class TestHoldLog(UnitBase):
    """plan_claim logs a hold when the kind of hold changes, not on every claim."""

    def test_one_line_per_change_not_per_claim(self):
        self.cycle()
        self.assertEqual(self.logged, [])
        self.set_free(work=4.0)
        self.cycle()
        self.assertEqual([k.get("lane", "")[:9] for k in self.events("hold")], ["low disk:"])
        self.set_free(work=3.5)                     # the numbers moved, the kind did not
        self.cycle()
        self.assertEqual(len(self.events("hold")), 1)
        self.set_free(work=1.5)                     # now nothing is claimed
        self.assertEqual(self.cycle()[0], "none")
        self.assertEqual([sorted(k) for k in self.events("hold")], [["lane"], ["all"]])
        self.set_free()
        self.cycle()
        self.cycle()
        self.assertEqual(len(self.events("hold_cleared")), 1)
        self.assertEqual(len(self.events("hold")), 2)

    def test_a_lane_limit_hold_is_logged_once(self):
        self.a.lane_n = 2
        for _ in range(3):
            self.cycle()
        self.assertEqual(self.events("hold"), [{"lane": "lane-job limit: 2 of 2 running"}])


class TestLaneOnly(UnitBase):
    """CPU_RUNNER_ACCEPT=lane: a second runner beside the main one that takes lane jobs only. Its claims ask for lane jobs
    and nothing else, and its holds always say "plain", so a plain job waiting in the queue shows why this runner leaves it."""
    ENV = {"ACCEPT": "lane"}
    WHY = "lane-only runner (CPU_RUNNER_ACCEPT=lane)"
    LIMIT = "lane-job limit: 2 of 2 running"
    STATES = {"idle": set(), "lane limit": {"lane"}, "lane jobs off": {"lane"}, "cache cleanup": {"all"},
              "low disk": {"lane"}, "very low disk": {"all"}}      # the kinds of hold a main runner has in each

    def put(self, state, accept=None):
        """A fresh agent (CPU_RUNNER_ACCEPT unset when accept is None) in one of the states the holds differ in."""
        env = {"LANE_MAX": 0 if state == "lane jobs off" else 2}
        if accept is not None:
            env["ACCEPT"] = accept
        self.free.clear()                    # the free space of an earlier state does not carry over
        a = self.build(**env)
        if state == "lane limit":
            a.lane_n = 2
        elif state == "cache cleanup":
            a.cleanup = CLEARING_NPM
        elif state == "low disk":
            self.set_free(work=4.0)
        elif state == "very low disk":
            self.set_free(work=1.0)
        return a

    def counts(self):
        return (self.a.jobs_n, self.a.lane_resv, self.a.lane_n)

    def test_the_standing_hold_is_the_documented_text(self):
        self.assertEqual(agent_mod.LANE_ONLY_HOLD, self.WHY)    # the README quotes it and a waiting job shows it
        self.assertEqual(self.holds(), {"plain": self.WHY})

    def test_the_plain_hold_comes_with_every_other_hold(self):
        for state, kinds in self.STATES.items():
            self.put(state)
            main = self.holds()
            self.assertEqual(set(main), kinds, state)           # what a main runner holds there
            self.put(state, "lane")
            self.assertEqual(self.holds(), dict(main, plain=self.WHY), state)

    def test_a_runner_that_takes_everything_never_holds_plain_jobs(self):
        for state in self.STATES:
            for accept in (None, "", "all", "ALL"):
                self.put(state, accept)
                self.assertNotIn("plain", self.holds(), (state, accept))
                self.assertNotIn("plain", self.a.plan_claim()[1], (state, accept))

    def test_a_claim_asks_for_lane_jobs_and_reserves_a_lane_slot_until_the_queue_answers(self):
        a = self.a                                              # lane_max 2
        want = ("lane", {"plain": self.WHY}, True)
        self.assertEqual(a.plan_claim(), want)
        self.assertEqual(self.counts(), (1, 1, 0))
        self.assertEqual(a.plan_claim(), want)
        self.assertEqual(self.counts(), (2, 2, 0))
        self.assertEqual(a.plan_claim(), ("none", {"plain": self.WHY}, False))     # both slots are on their way
        self.assertEqual(self.counts(), (2, 2, 0))              # a claim that asks for nothing counts for nothing
        a.claim_done("lane", True, None)
        a.claim_done("lane", True, None)
        self.assertEqual(self.counts(), (0, 0, 0))
        self.assertEqual(self.cycle()[0], "lane")               # the slots are free again

    def test_running_jobs_and_reserved_slots_count_against_lane_max_together(self):
        a = self.a
        accept, h, reserved = a.plan_claim()
        self.assertEqual(a.claim_done(accept, reserved, job(LANE, "J1")), (True, True))
        self.assertEqual(self.counts(), (1, 0, 1))              # one lane job running
        accept, h, reserved = a.plan_claim()                    # a second claim on its way: 1 + 1 of 2
        self.assertEqual((accept, reserved, self.counts()), ("lane", True, (2, 1, 1)))
        self.assertEqual(a.plan_claim(), ("none", {"plain": self.WHY}, False))
        self.assertEqual(self.counts(), (2, 1, 1))
        a.claim_done(accept, reserved, None)
        a.job_done(True)
        self.assertEqual(self.counts(), (0, 0, 0))

    def test_at_the_lane_limit_it_asks_for_nothing(self):
        a = self.a
        a.lane_n = 2
        self.assertEqual(a.plan_claim(), ("none", {"lane": self.LIMIT, "plain": self.WHY}, False))
        self.assertEqual(self.counts(), (0, 0, 2))              # (a main runner would still claim plain jobs here)
        a.lane_n = 1
        self.assertEqual(a.plan_claim()[0], "lane")

    def test_a_hold_on_everything_or_on_lane_jobs_leaves_nothing_to_ask_for(self):
        for state, why in (("cache cleanup", {"all": CLEARING_NPM}),
                           ("very low disk", {"all": "low disk: 1.0 GB free on {work}, every job needs 2.0 GB"}),
                           ("low disk", {"lane": "low disk: 4.0 GB free on {work}, lane jobs need 6.0 GB"}),
                           ("lane jobs off", {"lane": "lane jobs are off on this runner (lane_max 0)"})):
            a = self.put(state, "lane")
            want = {k: v.format(work=self.cfg.work_dir) for k, v in why.items()}
            self.assertEqual(a.plan_claim(), ("none", dict(want, plain=self.WHY), False), state)
            self.assertEqual(self.counts(), (0, 0, 0), state)

    def test_concurrent_claims_never_reserve_more_than_lane_max(self):
        a = self.build(ACCEPT="lane", LANE_MAX=3)
        barrier, plans = threading.Barrier(8), []

        def go():
            barrier.wait(10)
            plans.append(a.plan_claim())
        ts = [threading.Thread(target=go) for _ in range(8)]
        for t in ts:
            t.start()
        for t in ts:
            t.join(10)
        self.assertEqual(sorted(p[0] for p in plans), ["lane"] * 3 + ["none"] * 5)
        self.assertEqual(sorted(p[2] for p in plans), [False] * 5 + [True] * 3)
        self.assertEqual(self.counts(), (3, 3, 0))              # the five that found no room counted for nothing

    def test_only_a_lane_job_runs_and_any_other_gives_its_count_back(self):
        a = self.a
        for spec in (LANE, LLM, {"cmd": "x", "setup": {"cmd": "uv sync"}}, {"cmd": "x", "llm": {"max_calls": 5}},
                     {"cmd": "x", "setup": "s", "llm": True}):
            accept, h, reserved = a.plan_claim()
            self.assertEqual(a.claim_done(accept, reserved, job(spec)), (True, True), spec)
            self.assertEqual(self.counts(), (1, 0, 1), spec)    # it runs: it holds its count and its lane slot
            a.job_done(True)
            self.assertEqual(self.counts(), (0, 0, 0), spec)
        for spec in (PLAIN, {"cmd": "x", "setup": ""}, {"cmd": "x", "llm": False}, {"cmd": "x"}):
            accept, h, reserved = a.plan_claim()
            self.assertEqual(a.claim_done(accept, reserved, job(spec)), (False, False), spec)
            self.assertEqual(self.counts(), (0, 0, 0), spec)    # refused: the worker hands it back
        accept, h, reserved = a.plan_claim()
        self.assertEqual(a.claim_done(accept, reserved, None), (False, False))
        self.assertEqual(self.counts(), (0, 0, 0))

    def test_a_claim_that_asked_for_nothing_never_runs_what_it_gets(self):
        a = self.a
        a.lane_n = a.jobs_n = 2                                 # two lane jobs running
        for claim, lane in ((job(PLAIN), False), (job(LANE), True), (None, False)):
            accept, h, reserved = a.plan_claim()
            self.assertEqual(accept, "none")
            self.assertEqual(a.claim_done(accept, reserved, claim), (False, lane))
            self.assertEqual(self.counts(), (2, 0, 2))

    def test_the_standing_hold_is_logged_once_and_never_cleared(self):
        for _ in range(3):
            self.cycle()
        self.assertEqual(self.events("hold"), [{"plain": self.WHY}])
        self.a.lane_n = 2
        for _ in range(3):
            self.cycle()
        self.a.lane_n = 0
        for _ in range(3):
            self.cycle()
        self.assertEqual(self.events("hold"), [{"plain": self.WHY}, {"lane": self.LIMIT, "plain": self.WHY}, {"plain": self.WHY}])
        self.assertEqual(self.events("hold_cleared"), [])       # "plain" stays, so the hold never clears


class TestGiveBack(UnitBase):
    def test_posts_the_release_with_the_lease_token_and_logs_why(self):
        logged_by_then = []

        def answer(method, path, body):
            logged_by_then.append(len(self.events("claim_refused")))
            return 200, {"ok": True}
        self.a.api = FakeApi(answer)
        self.a.give_back({"id": "J1", "lease_token": "LT"}, "claim_refused", accept="plain")
        self.assertEqual(self.a.api.calls, [("POST", "/api/cpu/jobs/J1/release", {"runner_id": "unit", "lease_token": "LT"})])
        self.assertEqual(self.events("claim_refused"), [{"job": "J1", "accept": "plain"}])
        self.assertEqual(logged_by_then, [1])                   # the reason is on record before the call can fail

    def test_a_failed_release_is_logged_not_raised(self):
        self.a.api = FakeApi(OSError("x" * 500))
        self.a.give_back({"id": "J2", "lease_token": "LT"}, "job_start_failed")
        self.assertEqual(self.events("job_start_failed"), [{"job": "J2"}])
        (err,) = self.events("release_error")
        self.assertEqual((err["job"], len(err["error"])), ("J2", 200))


class TestDiskHelpers(unittest.TestCase):
    """du_bytes (allocated bytes under a path) and free_gb, unpatched."""

    def setUp(self):
        self.td = tempfile.mkdtemp(prefix="cpurunner-limits-")
        self.addCleanup(shutil.rmtree, self.td, ignore_errors=True)
        self.MB = 1 << 20

    def du(self, p):
        return agent_mod.du_bytes(p)

    def test_missing_and_empty_are_zero(self):
        self.assertEqual(self.du(os.path.join(self.td, "nope")), 0)
        os.makedirs(os.path.join(self.td, "empty"))
        self.assertEqual(self.du(os.path.join(self.td, "empty")), 0)

    def test_counts_allocated_bytes_through_nested_dirs(self):
        blob(os.path.join(self.td, "t", "a", "b", "c", "f1"), self.MB)
        blob(os.path.join(self.td, "t", "f2"), self.MB // 2)
        got = self.du(os.path.join(self.td, "t"))
        self.assertGreaterEqual(got, self.MB + self.MB // 2)
        self.assertLess(got, self.MB + self.MB // 2 + (256 << 10))

    def test_symlinks_are_not_followed(self):
        big = os.path.join(self.td, "elsewhere")
        blob(os.path.join(big, "f"), 2 * self.MB)
        t = os.path.join(self.td, "t")
        os.makedirs(t)
        os.symlink(big, os.path.join(t, "dirlink"))
        os.symlink(os.path.join(big, "f"), os.path.join(t, "filelink"))
        self.assertLess(self.du(t), 64 << 10)

    def test_a_sparse_file_counts_what_is_allocated_not_its_length(self):
        p = os.path.join(self.td, "sparse")
        with open(p, "wb") as f:
            f.truncate(64 * self.MB)
        if os.stat(p).st_blocks * 512 >= 64 * self.MB:
            self.skipTest("this filesystem allocates sparse files in full")
        self.assertLess(self.du(self.td), self.MB)

    def test_an_unreadable_dir_is_skipped_not_fatal(self):
        t = os.path.join(self.td, "t")
        blob(os.path.join(t, "ok", "f"), self.MB)
        blob(os.path.join(t, "locked", "f"), self.MB)
        real = os.scandir

        def scandir(p):
            if os.path.basename(p) == "locked":
                raise PermissionError(13, "denied", p)
            return real(p)
        with mock.patch.object(os, "scandir", scandir):
            got = self.du(t)
        self.assertGreaterEqual(got, self.MB)
        self.assertLess(got, 2 * self.MB)

    def test_free_gb_is_what_an_unprivileged_writer_can_use(self):
        st = types.SimpleNamespace(f_bavail=2_000_000, f_bfree=9_000_000, f_frsize=4096)
        with mock.patch.object(os, "statvfs", lambda p: st):
            self.assertAlmostEqual(agent_mod.free_gb("/x"), 8.192)

    def test_free_gb_of_a_missing_path_is_none(self):
        self.assertIsNone(agent_mod.free_gb(os.path.join(self.td, "nope")))
        got = agent_mod.free_gb(self.td)
        self.assertIsInstance(got, float)
        self.assertGreaterEqual(got, 0.0)


class TestPrune(UnitBase):
    """Job._prune: node_modules copies, oldest first, never one used in the last PRUNE_PROTECT_S."""

    def setUp(self):
        super().setUp()
        self.nm = os.path.join(self.cfg.cache_dir, "nm")
        os.makedirs(self.nm)
        self.sizes = {}

    def entry(self, name, age, nbytes=1000):
        p = os.path.join(self.nm, name)
        blob(os.path.join(p, "f"), nbytes)
        aged(p, age)
        return p

    def prune(self, max_entries=8, cap_gb=100.0, sizes=True):
        ns = types.SimpleNamespace(cfg=types.SimpleNamespace(cache_max_entries=max_entries, nm_cache_max_gb=cap_gb))
        if sizes:
            fake = lambda p: int(self.sizes[os.path.basename(p)])      # noqa: E731
            with mock.patch.object(agent_mod, "du_bytes", fake):
                agent_mod.Job._prune(ns, self.nm)
        else:
            agent_mod.Job._prune(ns, self.nm)

    def left(self):
        return sorted(os.listdir(self.nm))

    def evicted(self):
        return [(os.path.basename(k["path"]), k["reason"]) for k in self.events("cache_evict")]

    def test_the_oldest_beyond_the_entry_limit_go(self):
        for name, age in (("A", 5000), ("B", 4000), ("C", 3000), ("D", 2000)):
            self.entry(name, age)
        self.sizes = {k: 1 for k in "ABCD"}
        self.prune(max_entries=2)
        self.assertEqual(self.left(), ["C", "D"])
        self.assertEqual(self.evicted(), [("A", "count"), ("B", "count")])

    def test_an_entry_used_in_the_last_ten_minutes_is_never_evicted(self):
        for name, age in (("A", 5000), ("B", 610), ("C", 590)):
            self.entry(name, age)
        self.sizes = {k: 1 for k in "ABC"}
        self.prune(max_entries=1)                   # C (590 s) may be in use and does not count; B (610 s) does
        self.assertEqual(self.left(), ["B", "C"])
        self.assertEqual(self.evicted(), [("A", "count")])

    def test_size_cap_evicts_oldest_first_until_it_fits(self):
        for name, age in (("A", 5000), ("B", 4000), ("C", 3000), ("D", 2000)):
            self.entry(name, age)
        self.sizes = {"A": 3e9, "B": 2e9, "C": 2e9, "D": 1e9}
        self.prune(cap_gb=2.5)
        self.assertEqual(self.left(), ["D"])
        self.assertEqual(self.evicted(), [("A", "size"), ("B", "size"), ("C", "size")])
        self.assertEqual([k["gb"] for k in self.events("cache_evict")], [3.0, 2.0, 2.0])

    def test_nothing_goes_while_the_total_fits(self):
        for name, age in (("A", 5000), ("B", 4000)):
            self.entry(name, age)
        self.sizes = {"A": 3e9, "B": 3e9}
        self.prune(cap_gb=6.0)                      # exactly at the cap is fine
        self.assertEqual(self.left(), ["A", "B"])
        self.assertEqual(self.events("cache_evict"), [])

    def test_recent_entries_count_toward_the_size_but_are_not_evicted(self):
        self.entry("A", 5000)
        self.entry("P", 100)
        self.sizes = {"A": 1e9, "P": 6e9}
        self.prune(cap_gb=6.0)                      # 7 GB in all: the old one must go to get under 6
        self.assertEqual(self.left(), ["P"])
        self.entry("A", 5000)
        self.sizes = {"A": 1e9, "P": 9e9}
        self.prune(cap_gb=6.0)                      # still over after A: P stays, nothing more can be done
        self.assertEqual(self.left(), ["P"])

    def test_count_and_size_limits_work_together(self):
        for name, age in (("A", 5000), ("B", 4000), ("C", 3000)):
            self.entry(name, age)
        self.sizes = {"A": 1e9, "B": 1e9, "C": 3e9}
        self.prune(max_entries=2, cap_gb=3.5)       # A by count; then B+C = 4 GB > 3.5: B by size
        self.assertEqual(self.left(), ["C"])
        self.assertEqual(self.evicted(), [("A", "count"), ("B", "size")])

    def test_copies_being_filled_are_not_entries(self):
        self.entry("abc123.tmp-77-deadbeef", 9000)
        self.entry("A", 5000)
        self.sizes = {"abc123.tmp-77-deadbeef": 9e9, "A": 1}
        self.prune(max_entries=1, cap_gb=1.0)
        self.assertEqual(self.left(), ["A", "abc123.tmp-77-deadbeef"])

    def test_a_missing_cache_dir_is_not_an_error(self):
        ns = types.SimpleNamespace(cfg=types.SimpleNamespace(cache_max_entries=1, nm_cache_max_gb=1.0))
        agent_mod.Job._prune(ns, os.path.join(self.td, "nowhere"))
        self.assertEqual(self.events("cache_evict"), [])

    def test_real_files_real_sizes(self):
        for name, age in (("A", 4000), ("B", 3000), ("C", 2000), ("D", 1000)):
            self.entry(name, age, nbytes=1 << 20)
        self.prune(cap_gb=0.0025, sizes=False)      # 2.5 MB: two 1 MiB copies fit, three do not
        self.assertEqual(self.left(), ["C", "D"])


class TestMeasure(UnitBase):
    """measure(): the usage report a claim carries."""
    KEYS = {"work_gb", "work_free_gb", "cache_free_gb", "min_free_gb", "uv_gb", "uv_max_gb", "npm_gb", "npm_max_gb",
            "nm_gb", "nm_max_gb"}

    def setUp(self):
        super().setUp()
        c = self.cfg
        self.du_by = {c.work_dir: 1.5e9, os.path.join(c.cache_dir, "uv"): 2.5e9,
                      os.path.join(c.cache_dir, "npm"): 4.25e9, os.path.join(c.cache_dir, "nm"): 0.999e9}
        self.walked = []
        p = mock.patch.object(agent_mod, "du_bytes", self._du)
        p.start()
        self.addCleanup(p.stop)

    def _du(self, path):
        self.walked.append(path)
        return self.du_by.get(path, 0)

    def cache(self, name):
        return os.path.join(self.cfg.cache_dir, name)

    def test_the_report_has_usage_free_space_and_caps(self):
        self.set_free(work=123.456, cache=88.884)
        got = self.a.measure()
        self.assertIs(got, self.a.disk)
        self.assertEqual(set(got), self.KEYS)
        self.assertEqual(got, {"work_gb": 1.5, "work_free_gb": 123.46, "cache_free_gb": 88.88, "min_free_gb": 6.0,
                               "uv_gb": 2.5, "uv_max_gb": 10.0, "npm_gb": 4.25, "npm_max_gb": 4.0,
                               "nm_gb": 1.0, "nm_max_gb": 6.0})

    def test_what_cannot_be_read_is_left_out(self):
        self.set_free(work=None, cache=50.0)
        got = self.a.measure()
        self.assertEqual(set(got), self.KEYS - {"work_free_gb"})

    def test_only_the_caches_a_job_touched_are_walked_again(self):
        self.a.measure()
        self.assertEqual(sorted(self.walked), sorted([self.cfg.work_dir] + [self.cache(n) for n in ("uv", "npm", "nm")]))
        del self.walked[:]
        self.a.measure()
        self.assertEqual(self.walked, [self.cfg.work_dir])      # an idle cache dir is not walked for nothing
        del self.walked[:]
        self.du_by[self.cache("npm")] = 1e9
        self.a.jobs_n = 1
        self.a.job_done(False, ("npm",))
        got = self.a.measure()
        self.assertEqual(sorted(self.walked), sorted([self.cfg.work_dir, self.cache("npm")]))
        self.assertEqual((got["npm_gb"], got["uv_gb"]), (1.0, 2.5))

    def test_every_cache_is_walked_again_now_and_then(self):
        self.a.measure()
        del self.walked[:]
        self.a._all_t -= agent_mod.CACHE_REMEASURE_S + 1
        self.du_by[self.cache("uv")] = 3e9
        got = self.a.measure()
        self.assertEqual(sorted(self.walked), sorted([self.cfg.work_dir] + [self.cache(n) for n in ("uv", "npm", "nm")]))
        self.assertEqual(got["uv_gb"], 3.0)


class TestCleanCaches(UnitBase):
    """clean_caches(): a uv / npm cache over its cap is deleted whole, once no job is running."""
    ENV = {"UV_CACHE_MAX_GB": 0.001, "NPM_CACHE_MAX_GB": 0.001, "NM_CACHE_MAX_GB": 0.001}      # 1 MB caps

    def cache(self, name, nbytes):
        return blob(os.path.join(self.cfg.cache_dir, name, "blob"), nbytes)

    def start_clean(self):
        t = threading.Thread(target=self.a.clean_caches, daemon=True)
        t.start()
        self.assertTrue(wait_for(lambda: self.a.cleanup, 3), "the cleanup never started")
        return t

    def test_a_cache_under_its_cap_is_left_alone(self):
        keep = self.cache("uv", 100 << 10)
        self.a.measure()
        self.a.clean_caches()
        self.assertTrue(os.path.exists(keep))
        self.assertEqual(self.events("cache_cleanup_wait"), [])
        self.assertIsNone(self.a.cleanup)

    def test_a_cache_over_its_cap_is_deleted_whole(self):
        self.cache("npm", 2 << 20)
        keep = self.cache("uv", 100 << 10)
        self.a.measure()
        self.assertGreater(self.a._sizes["npm"], 1e6)
        self.a._free_t = 12345.0
        self.a.clean_caches()
        self.assertFalse(os.path.exists(os.path.join(self.cfg.cache_dir, "npm")))
        self.assertTrue(os.path.exists(keep))
        (wait,) = self.events("cache_cleanup_wait")
        self.assertEqual((wait["caches"], wait["jobs"]), (["npm"], 0))
        self.assertEqual([e["cache"] for e in self.events("cache_cleared")], ["npm"])
        self.assertIsNone(self.a.cleanup)
        self.assertEqual(self.a._free_t, 0.0)
        self.assertEqual(self.a._sizes["npm"], 0)
        self.assertEqual(self.a.disk["npm_gb"], 0.0)            # the next claim reports the freed space
        self.assertEqual(self.events("cache_cleanup_incomplete"), [])

    def test_both_over_are_named_in_the_pause_text(self):
        self.cache("uv", 2 << 20)
        self.cache("npm", 2 << 20)
        self.a.measure()
        seen, real = [], shutil.rmtree

        def spy(p, *args, **kw):
            seen.append(self.a.cleanup)
            return real(p, *args, **kw)
        with mock.patch.object(shutil, "rmtree", spy):
            self.a.clean_caches()
        self.assertEqual(seen, ["clearing the uv and npm cache (over its cap)"] * 2)
        self.assertEqual(sorted(os.listdir(self.cfg.cache_dir)), [])

    def test_node_modules_copies_are_not_the_cleanups_business(self):
        keep = self.cache("nm", 3 << 20)            # far over its (test) cap
        self.a.measure()
        self.a.clean_caches()
        self.assertTrue(os.path.exists(keep))
        self.assertEqual(self.events("cache_cleanup_wait"), [])

    def test_waits_for_the_running_jobs_and_pauses_claims_meanwhile(self):
        for release in ("job_done", "claim_done"):
            self.cache("npm", 2 << 20)
            self.a._dirty.add("npm")
            self.a.measure()
            self.a.jobs_n = 1
            del self.logged[:]
            t = self.start_clean()
            self.assertEqual(self.holds()["all"], CLEARING_NPM)
            self.assertEqual(self.a.plan_claim(), ("none", {"all": CLEARING_NPM}, False))
            self.assertEqual(self.a.jobs_n, 1)                  # no claim goes out while it waits
            time.sleep(0.3)
            self.assertTrue(t.is_alive(), release)
            self.assertTrue(os.path.exists(os.path.join(self.cfg.cache_dir, "npm", "blob")), release)
            if release == "job_done":
                self.a.job_done(False)
            else:
                self.a.claim_done("plain", False, None)
            t.join(3)                                           # woken at once, not after the gate's 5 s poll
            self.assertFalse(t.is_alive(), release)
            self.assertFalse(os.path.exists(os.path.join(self.cfg.cache_dir, "npm")), release)
            self.assertEqual(self.events("cache_cleanup_wait")[0]["jobs"], 1)
            self.assertIsNone(self.a.cleanup)
            self.assertEqual(self.holds(), {})                  # claims resume

    def test_a_stop_during_the_wait_deletes_nothing(self):
        keep = self.cache("npm", 2 << 20)
        self.a.measure()
        self.a.jobs_n = 1
        t = self.start_clean()
        self.a.stop.set()
        with self.a.gate:
            self.a.gate.notify_all()
        t.join(3)
        self.assertFalse(t.is_alive())
        self.assertTrue(os.path.exists(keep))
        self.assertIsNone(self.a.cleanup)
        self.assertEqual(self.events("cache_cleared"), [])

    def test_a_cache_that_stays_over_its_cap_is_retried_only_after_an_hour(self):
        self.cache("npm", 2 << 20)
        self.a.measure()
        with mock.patch.object(shutil, "rmtree", lambda *args, **kw: None):       # the delete does not work
            self.a.clean_caches()
            self.assertEqual(self.events("cache_cleanup_incomplete"), [{"caches": ["npm"], "retry_s": 3600}])
            self.assertGreater(self.a._clean_after - time.time(), 3000)
            self.a.clean_caches()                                                 # too soon: nothing happens
            self.assertEqual(len(self.events("cache_cleanup_wait")), 1)
            self.a._clean_after = time.time() - 1
            self.a.clean_caches()
            self.assertEqual(len(self.events("cache_cleanup_wait")), 2)

    def test_a_symlinked_cache_dir_loses_the_link_not_the_target(self):
        target = os.path.join(self.td, "elsewhere")
        keep = blob(os.path.join(target, "blob"), 2 << 20)
        os.symlink(target, os.path.join(self.cfg.cache_dir, "npm"))
        self.a.measure()
        self.assertGreater(self.a._sizes["npm"], 1e6)
        self.a.clean_caches()
        self.assertFalse(os.path.lexists(os.path.join(self.cfg.cache_dir, "npm")))
        self.assertTrue(os.path.exists(keep))


class TestDiskLoop(UnitBase):
    ENV = {"DISK_CHECK_S": 30}

    def run_loop(self, measure, clean, passes=2):
        self.a.stop = Ticks(passes)
        with mock.patch.object(self.a, "measure", measure), mock.patch.object(self.a, "clean_caches", clean):
            self.a.disk_loop()

    def test_first_pass_after_five_seconds_then_every_disk_check(self):
        order = []
        self.run_loop(lambda: order.append("measure"), lambda: order.append("clean"))
        self.assertEqual(self.a.stop.waits, [5.0, 30.0, 30.0])
        self.assertEqual(order, ["measure", "clean", "measure", "clean"])

    def test_a_failing_pass_is_logged_and_the_loop_goes_on(self):
        order = []

        def measure():
            order.append("measure")
            if len(order) == 1:
                raise RuntimeError("boom")
        self.run_loop(measure, lambda: order.append("clean"))
        self.assertEqual(order, ["measure", "measure", "clean"])
        self.assertEqual(self.events("disk_error"), [{"error": "boom"}])


class WorkerHarness:
    """Mixin for worker() tests (list it before UnitBase): one claim after another against a scripted queue, the Job
    a stand-in."""

    def setUp(self):
        super().setUp()
        self.built, self.seen = [], []

    def fake_job(self, touched=(), fail=None):
        test = self

        class FakeJob:
            def __init__(self, cfg, api, claim, iso):
                if fail:
                    raise fail
                test.built.append(claim["id"])
                self.id, self.touched = claim["id"], set(touched)

            def run(self):
                a = test.a
                test.seen.append((a.lane_n, a.jobs_n, a.lane_resv))
        return mock.patch.object(agent_mod, "Job", FakeJob)

    def run_worker(self, api, ticks, **jobkw):
        self.a.api, self.a.stop = api, Ticks(ticks)
        with self.fake_job(**jobkw):
            self.a.worker()
        return self.a.api.calls

    def counters(self):
        return (self.a.jobs_n, self.a.lane_n, self.a.lane_resv)

    def bodies(self, api):
        return [c[2] for c in api.calls if c[1] == "/api/cpu/claim"]


class TestWorker(WorkerHarness, UnitBase):
    """worker(): one claim after another against a scripted queue; the Job is a stand-in."""
    ENV = {"LANE_MAX": 0}

    def test_a_lane_job_from_a_queue_that_ignores_accept_is_handed_back(self):
        calls = self.run_worker(FakeApi((200, {"job": job(LANE, "J1")})), 1)
        claim = calls[0]
        self.assertEqual(claim[:2], ("POST", "/api/cpu/claim"))
        self.assertEqual({k: claim[2][k] for k in ("runner_id", "lease_s", "slots", "accept", "hold", "lane_max")},
                         {"runner_id": "unit", "lease_s": self.cfg.lease_s, "slots": self.cfg.concurrency,
                          "accept": "plain", "hold": {"lane": "lane jobs are off on this runner (lane_max 0)"}, "lane_max": 0})
        self.assertEqual(calls[1], ("POST", "/api/cpu/jobs/J1/release", {"runner_id": "unit", "lease_token": "L-J1"}))
        (refused,) = self.events("claim_refused")
        self.assertEqual((refused["job"], refused["accept"], refused["lane"]), ("J1", "plain", True))
        self.assertIn("update it", refused["hint"])
        self.assertEqual(self.built, [])                        # it never started
        self.assertEqual(self.counters(), (0, 0, 0))
        self.assertGreaterEqual(agent_mod.REFUSE_BACKOFF_S, 10)
        self.assertEqual(self.a.stop.waits[0], agent_mod.REFUSE_BACKOFF_S)      # and the runner does not spin on it

    def test_a_job_from_a_none_claim_is_handed_back_too(self):
        self.build(LANE_MAX=2)
        self.a.cleanup = CLEARING_NPM
        calls = self.run_worker(FakeApi((200, {"job": job(PLAIN, "J2")})), 0)
        self.assertEqual(calls[0][2]["accept"], "none")
        self.assertEqual(calls[0][2]["hold"], {"all": CLEARING_NPM})
        self.assertEqual(calls[1][1], "/api/cpu/jobs/J2/release")
        (refused,) = self.events("claim_refused")
        self.assertEqual((refused["accept"], refused["lane"]), ("none", False))
        self.assertEqual(self.built, [])
        self.assertEqual(self.counters(), (0, 0, 0))

    def test_a_job_that_cannot_start_is_handed_back_and_the_runner_slows_down(self):
        self.build(LANE_MAX=2)
        self.run_worker(FakeApi((200, {"job": job(LANE, "J3")})), 0, fail=OSError(28, "No space left on device"))
        self.assertIn("No space left", self.events("worker_error")[0]["error"])
        self.assertEqual(self.events("job_start_failed"), [{"job": "J3"}])
        self.assertEqual(self.a.api.calls[1], ("POST", "/api/cpu/jobs/J3/release", {"runner_id": "unit", "lease_token": "L-J3"}))
        self.assertEqual(self.a.stop.waits[0], agent_mod.REFUSE_BACKOFF_S)
        self.assertEqual(self.counters(), (0, 0, 0))            # the lane slot it took is free again

    def test_a_running_lane_job_counts_and_its_caches_are_marked(self):
        self.build(LANE_MAX=2)
        self.a._dirty = set()
        self.run_worker(FakeApi((200, {"job": job(LANE, "J4")})), 0, touched=("uv", "npm"))
        self.assertEqual(self.seen, [(1, 1, 0)])                # lane_n, jobs_n, lane_resv while it runs
        self.assertEqual(self.counters(), (0, 0, 0))
        self.assertEqual(self.a._dirty, {"uv", "npm"})          # re-measured by the next disk pass
        (claimed,) = self.events("claimed")
        self.assertEqual((claimed["job"], claimed["lane"], claimed["attempt"]), ("J4", True, 1))
        self.assertEqual(self.a.active, {})

    def test_a_running_plain_job_takes_no_lane_slot(self):
        self.run_worker(FakeApi((200, {"job": job(PLAIN, "J5")})), 0)
        self.assertEqual(self.seen, [(0, 1, 0)])
        self.assertEqual(self.counters(), (0, 0, 0))
        self.assertIs(self.events("claimed")[0]["lane"], False)

    def test_a_claim_that_fails_leaves_the_counts_alone(self):
        self.build(LANE_MAX=2)
        self.run_worker(FakeApi(ConnectionError("down")), 0)
        self.assertEqual(self.events("claim_error"), [{"error": "down"}])
        self.assertEqual(self.counters(), (0, 0, 0))
        self.assertEqual(self.a.stop.waits, [min(30, self.cfg.poll_s * 3)])

    def test_the_disk_report_is_re_sent_only_when_it_changed_or_the_claim_failed(self):
        self.build(LANE_MAX=2)
        a = self.a
        a.disk = {"work_gb": 1.0}

        def change(method, path, body):
            a.disk = {"work_gb": 2.0}
            return 204, None
        api = FakeApi((500, {"error": "boom"}), (204, None), change, (204, None), (204, None))
        self.run_worker(api, 4)
        self.assertEqual(["disk" in b for b in self.bodies(api)], [True, True, False, True, False])
        self.assertEqual(self.bodies(api)[3]["disk"], {"work_gb": 2.0})

    def test_caps_follow_the_same_rule(self):
        self.build(LANE_MAX=2)
        self.a.caps = {"node": 26}
        api = FakeApi((503, {"error": "busy"}), (200, {}), (204, None))
        self.run_worker(api, 2)
        self.assertEqual(["caps" in b for b in self.bodies(api)], [True, True, False])


class TestLaneOnlyWorker(WorkerHarness, UnitBase):
    """worker() of a lane-only runner: it runs lane jobs, and a plain job (a queue from before `accept` hands out any job)
    goes straight back."""
    ENV = {"ACCEPT": "lane"}
    WHY = agent_mod.LANE_ONLY_HOLD

    def test_a_lane_job_runs(self):
        calls = self.run_worker(FakeApi((200, {"job": job(LANE, "J1")})), 0)
        body = calls[0][2]
        self.assertEqual((body["accept"], body["hold"], body["lane_max"]), ("lane", {"plain": self.WHY}, 2))
        self.assertEqual(self.built, ["J1"])
        self.assertEqual(self.seen, [(1, 1, 0)])                # lane_n, jobs_n, lane_resv while it runs
        self.assertEqual(self.counters(), (0, 0, 0))
        self.assertIs(self.events("claimed")[0]["lane"], True)
        self.assertEqual(self.events("claim_refused"), [])
        self.assertEqual([c[1] for c in calls], ["/api/cpu/claim", "/api/cpu/claim"])   # the next claim found nothing; no release
        self.assertEqual(self.a.stop.waits, [self.cfg.poll_s])

    def test_a_plain_job_is_handed_back_and_the_runner_backs_off(self):
        calls = self.run_worker(FakeApi((200, {"job": job(PLAIN, "P1")})), 0)
        self.assertEqual(calls[0][2]["accept"], "lane")
        self.assertEqual(calls[1], ("POST", "/api/cpu/jobs/P1/release", {"runner_id": "unit", "lease_token": "L-P1"}))
        (refused,) = self.events("claim_refused")
        self.assertEqual((refused["job"], refused["accept"], refused["lane"]), ("P1", "lane", False))
        self.assertIn("update it", refused["hint"])
        self.assertEqual(self.built, [])                        # it never started
        self.assertEqual(self.counters(), (0, 0, 0))
        self.assertEqual(self.a.stop.waits, [agent_mod.REFUSE_BACKOFF_S])

    def test_a_refused_plain_job_does_not_keep_the_next_trip_from_running_a_lane_job(self):
        api = FakeApi((200, {"job": job(PLAIN, "P1")}), (200, {"ok": True}), (200, {"job": job(LANE, "L2")}))
        self.run_worker(api, 1)
        self.assertEqual([c[1] for c in api.calls], ["/api/cpu/claim", "/api/cpu/jobs/P1/release", "/api/cpu/claim", "/api/cpu/claim"])
        self.assertEqual(self.built, ["L2"])
        self.assertEqual(self.a.stop.waits, [agent_mod.REFUSE_BACKOFF_S, self.cfg.poll_s])
        self.assertEqual(self.counters(), (0, 0, 0))

    def test_at_the_lane_limit_the_claim_asks_for_nothing_and_a_job_it_gets_goes_back(self):
        self.a.lane_n = 2
        calls = self.run_worker(FakeApi((200, {"job": job(LANE, "J2")})), 0)
        self.assertEqual((calls[0][2]["accept"], calls[0][2]["hold"]),
                         ("none", {"lane": "lane-job limit: 2 of 2 running", "plain": self.WHY}))
        self.assertEqual(calls[1][1], "/api/cpu/jobs/J2/release")
        (refused,) = self.events("claim_refused")
        self.assertEqual((refused["accept"], refused["lane"]), ("none", True))
        self.assertEqual(self.built, [])
        self.assertEqual(self.counters(), (0, 2, 0))            # the two lane jobs that are running are left alone

    def test_a_cache_cleanup_holds_everything_and_the_plain_hold_rides_along(self):
        self.a.cleanup = CLEARING_NPM
        calls = self.run_worker(FakeApi(), 0)
        self.assertEqual((calls[0][2]["accept"], calls[0][2]["hold"]), ("none", {"all": CLEARING_NPM, "plain": self.WHY}))
        self.assertEqual(self.counters(), (0, 0, 0))

    def test_a_claim_that_finds_no_job_frees_its_slot_for_the_next_one(self):
        api = FakeApi()
        self.run_worker(api, 2)
        self.assertEqual([b["accept"] for b in self.bodies(api)], ["lane"] * 3)
        self.assertEqual(self.a.stop.waits, [self.cfg.poll_s] * 3)
        self.assertEqual(self.counters(), (0, 0, 0))


class TestSetupAccept(UnitBase):
    """CPU_RUNNER_ACCEPT is all or lane. setup() refuses anything else before it calls the queue: a typo such as "plain"
    must not start a runner that takes something other than what was meant."""

    def setUp(self):
        super().setUp()
        for name, fake in (("am_root", lambda: False), ("probe_isolation", lambda *args, **kw: None),
                           ("detect_caps", lambda: {})):
            p = mock.patch.object(agent_mod, name, fake)
            p.start()
            self.addCleanup(p.stop)

    def start(self, accept=None):
        """(agent, what setup() returned) with CPU_RUNNER_ACCEPT=accept (unset for None)."""
        self.logged.clear()
        a = self.build(ISOLATION="none", **({} if accept is None else {"ACCEPT": accept}))
        a.api = FakeApi((200, {"ok": True}), (200, {"runners": {}}))
        return a, a.setup()

    def test_all_and_lane_start_in_any_case(self):
        for v in (None, "", "all", "ALL", " All ", "lane", "LANE", " Lane "):
            a, ok = self.start(v)
            self.assertIs(ok, True, v)
            self.assertEqual(self.events("fatal"), [], v)
            self.assertEqual(len(self.events("start")), 1, v)
            self.assertEqual(a.api.calls[0][:2], ("GET", "/api/cpu/health"), v)
            self.assertEqual(a.cfg.accept, "lane" if v and v.strip().lower() == "lane" else "all", v)

    def test_anything_else_is_fatal_and_the_queue_is_never_called(self):
        for v in ("plain", "none", "bogus", "lanes", "lane-only", "lane_only", "both"):
            a, ok = self.start(v)
            self.assertIs(ok, False, v)
            (fatal,) = self.events("fatal")
            self.assertIn("CPU_RUNNER_ACCEPT must be all or lane", fatal["error"], v)
            self.assertIn(repr(v), fatal["error"], v)
            self.assertEqual(self.events("start"), [], v)       # it never announced itself as running
            self.assertEqual(a.api.calls, [], v)

    def test_a_long_value_is_cut_in_the_message(self):
        a, ok = self.start("x" * 100)
        self.assertIs(ok, False)
        (fatal,) = self.events("fatal")
        self.assertIn(repr("x" * 40), fatal["error"])
        self.assertNotIn("x" * 41, fatal["error"])

    def test_run_gives_up_with_exit_status_3_before_it_starts_anything(self):
        a = self.build(ISOLATION="none", ACCEPT="plain")
        a.api = FakeApi()
        a.stop.set()        # with the check gone run() would carry on into its loop; stopped, it ends there, not hangs
        with mock.patch.object(agent_mod.signal, "signal"):     # ...and leaves this process's signal handlers alone
            self.assertEqual(a.run(), 3)
        self.assertEqual(a.api.calls, [])
        self.assertEqual(self.events("start"), [])


class TestSetupPhaseEnv(UnitBase):
    """spec.setup runs with the uv cache in /cache and copies (never hardlinks) out of it."""

    def make(self, spec):
        self.cfg.job_user = "root"                  # priv_drop_prefix looks the job user up when we are root
        job_ = agent_mod.Job(self.cfg, None, {"id": "j" * 16, "spec": spec, "lease_token": "L"}, [])
        job_.root, job_._wfd, ran = job_.dir, {"setup.log": 1}, []

        def fake_run(full, cwd, env_, out_fd, err_fd, timeout):
            ran.append((full, env_))
            return 0, False
        job_._run_proc = fake_run
        return job_, ran

    def test_setup_marks_uv_and_npm_and_sets_the_uv_env(self):
        job_, ran = self.make({"cmd": "true", "setup": "echo hi"})
        job_.setup_phase()
        self.assertEqual(job_.touched, {"uv", "npm"})
        ((full, env_),) = ran
        self.assertEqual(full[-3:], ["bash", "-c", "echo hi"])
        self.assertEqual((env_["UV_LINK_MODE"], env_["UV_PYTHON_DOWNLOADS"]), ("copy", "never"))
        self.assertEqual(env_["UV_CACHE_DIR"], os.path.join(self.cfg.cache_dir, "uv"))
        self.assertNotIn("PIP_NO_INDEX", env_)

    def test_a_job_without_setup_touches_no_cache(self):
        job_, ran = self.make({"cmd": "true"})
        job_.setup_phase()
        self.assertEqual((job_.touched, ran), (set(), []))


class TestInstallTouch(UnitBase):
    """A node_modules cache fill marks npm and nm for re-measuring and prunes; a cache hit marks nothing."""

    def test_miss_then_hit(self):
        cfg, pkg = self.cfg, os.path.join(self.td, "pkg")
        cfg.install_cmd = "mkdir -p node_modules/dep && echo x > node_modules/dep/index.js"
        cfg.cache_max_entries = 1
        os.makedirs(pkg)
        open(os.path.join(pkg, "package-lock.json"), "w").write('{"lockfileVersion":3}')
        open(os.path.join(pkg, "package.json"), "w").write('{"name":"x","version":"1.0.0"}')
        nm = os.path.join(cfg.cache_dir, "nm")
        for name, age in (("old1", 6000), ("old2", 5000)):
            os.makedirs(os.path.join(nm, name))
            aged(os.path.join(nm, name), age)

        def new_job(n):
            j = agent_mod.Job(cfg, None, {"id": "i%015d" % n, "spec": PLAIN, "lease_token": "L"}, [])
            os.makedirs(os.path.join(j.dir, "out"))
            return j
        with mock.patch.object(agent_mod, "priv_drop_prefix", lambda c: []), mock.patch.object(agent_mod, "am_root", lambda: False):
            first, info1 = new_job(1), {}
            first._install_pkg(pkg, info1)
            shutil.rmtree(os.path.join(pkg, "node_modules"))
            second, info2 = new_job(2), {}
            second._install_pkg(pkg, info2)
        self.assertEqual((info1["cache"], first.touched), ("miss", {"npm", "nm"}))
        self.assertEqual((info2["cache"], second.touched), ("hit", set()))
        self.assertEqual(sorted(os.listdir(nm)), sorted(["old2", info1["key"]]))     # the fill pruned the oldest copy



class TestDeployFiles(unittest.TestCase):
    """The docs and deploy files say what the code does: the template must not pin the limits (an env var set on the
    container beats what the queue pulls), and every new setting is written down."""
    KEYS = {"lane_max": "CPU_RUNNER_LANE_MAX", "min_free_gb": "CPU_RUNNER_MIN_FREE_GB",
            "uv_cache_max_gb": "CPU_RUNNER_UV_CACHE_MAX_GB", "npm_cache_max_gb": "CPU_RUNNER_NPM_CACHE_MAX_GB",
            "nm_cache_max_gb": "CPU_RUNNER_NM_CACHE_MAX_GB"}

    def read(self, *path):
        with open(os.path.join(ROOT, *path)) as f:
            return f.read()

    def test_the_settings_are_pulled_from_the_queue_under_these_names(self):
        remote = agent_mod.Config.REMOTE
        for key, name in self.KEYS.items():
            self.assertEqual(remote[key][1], name)

    def test_the_unraid_template_offers_the_limits_unset(self):
        import xml.etree.ElementTree as ET
        root = ET.fromstring(self.read("unraid", "dispatch-cpu-runner.xml"))
        conf = {c.get("Target"): c for c in root.findall("Config")}
        for name in list(self.KEYS.values()) + ["CPU_RUNNER_DISK_CHECK_S"]:
            c = conf.get(name)
            self.assertIsNotNone(c, name)
            self.assertEqual((c.get("Type"), c.get("Display"), c.get("Default")), ("Variable", "advanced", ""), name)
        self.assertEqual(conf["/cache"].get("Default"), "/mnt/user/appdata/dispatch-cpu-runner/cache")
        self.assertNotIn("/work", conf)                    # the scratch dir stays Docker storage, not a share
        self.assertIn("Disk use", root.findtext("Overview"))

    def test_every_setting_is_in_the_readme_env_example_and_compose_file(self):
        readme, example, compose = self.read("README.md"), self.read("env.example"), self.read("docker-compose.yml")
        self.assertIn("\n## Disk use\n", readme)          # the agent's module doc points at this section
        for key, name in list(self.KEYS.items()) + [("disk_check_s", "CPU_RUNNER_DISK_CHECK_S")]:
            self.assertIn(name, readme, name)
            self.assertIn(name, example, name)
        for key in self.KEYS:
            self.assertIn("`%s`" % key, readme, key)       # listed among the settings pulled from the queue
        self.assertIn("cpu-runner-work:/work", compose)
        live = [l.split("=")[0] for l in example.splitlines() if l.strip() and not l.startswith("#")]
        for name in list(self.KEYS.values()) + ["CPU_RUNNER_DISK_CHECK_S"]:
            self.assertNotIn(name, live, "%s is set in env.example: it would beat the queue's value" % name)

    def test_accept_is_the_environments_alone(self):
        self.assertNotIn("accept", agent_mod.Config.REMOTE)        # the queue cannot turn a runner into a lane-only one

    def test_the_lane_only_setting_is_written_down_where_the_others_are(self):
        import xml.etree.ElementTree as ET
        readme, example, compose = self.read("README.md"), self.read("env.example"), self.read("docker-compose.yml")
        for name, text in (("README.md", readme), ("env.example", example), ("docker-compose.yml", compose)):
            self.assertIn("CPU_RUNNER_ACCEPT", text, name)
        self.assertIn(agent_mod.LANE_ONLY_HOLD, readme)            # the text a waiting job shows
        for name, text in (("env.example", example), ("docker-compose.yml", compose)):
            live = [l.split("=")[0].strip() for l in text.splitlines() if l.strip() and not l.lstrip().startswith("#")]
            self.assertFalse([l for l in live if "CPU_RUNNER_ACCEPT" in l], "%s would make every runner lane-only" % name)
        root = ET.fromstring(self.read("unraid", "dispatch-cpu-runner.xml"))
        c = {c.get("Target"): c for c in root.findall("Config")}.get("CPU_RUNNER_ACCEPT")
        self.assertIsNotNone(c)
        self.assertEqual((c.get("Type"), c.get("Display"), c.get("Default")), ("Variable", "advanced", ""))


class TestReferenceQueueLaneOnly(Base):
    """What the reference queue does with a lane-only runner's claims (cpu_lane.py does the same; ~/bin/test-cpu-lane-api.py
    covers that one): accept "lane" hands out lane jobs only, and the hold "plain" is kept and read for plain jobs."""
    WHY = agent_mod.LANE_ONLY_HOLD
    LIMIT = "lane-job limit: 2 of 2 running"
    OFF = "lane jobs are off on this runner (lane_max 0)"

    def http(self, method, path, body=None, raw=None):
        return cpu_job._http(self.api, TOKEN, method, path, body, raw)

    def add(self, spec):
        """A pending job -> its id."""
        code, body = self.http("POST", "/api/cpu/jobs", {"spec": dict({"cmd": "x"}, **spec)})
        self.assertEqual(code, 201, body)
        jid = body["id"]
        self.assertEqual(self.http("PUT", "/api/cpu/jobs/%s/payload" % jid, raw=b"x")[0], 200)
        self.assertEqual(self.http("POST", "/api/cpu/jobs/%s/ready" % jid, {})[0], 200)
        return jid

    def claim(self, rid, accept, hold=None):
        """POST /api/cpu/claim as runner rid -> the job it hands out, or None."""
        code, body = self.http("POST", "/api/cpu/claim", {"runner_id": rid, "lease_s": 60, "slots": 1, "accept": accept,
                                                          "hold": hold or {}, "lane_max": 2})
        self.assertIn(code, (200, 204), body)
        return body["job"] if code == 200 else None

    def state(self, jid):
        with self.store.lock:
            return self.store.jobs[jid]["status"], self.store.jobs[jid]["attempt"]

    def waiting(self, jid):
        code, body = self.http("GET", "/api/cpu/jobs/%s" % jid)
        self.assertEqual(code, 200, body)
        return body["waiting"]

    def test_accept_lane_skips_plain_jobs_and_takes_the_lane_jobs_queued_behind_them(self):
        p1, p2 = self.add({}), self.add({"setup": False, "llm": None})      # falsy setup / llm: still plain
        l1, l2 = self.add({"setup": "true"}), self.add({"llm": True})
        self.assertEqual(self.claim("r-lane", "lane")["id"], l1)
        self.assertEqual(self.claim("r-lane", "lane")["id"], l2)
        self.assertIsNone(self.claim("r-lane", "lane"))         # only plain jobs are left, and they are not for this runner
        self.assertEqual([self.state(j) for j in (p1, p2)], [("pending", 0)] * 2)       # untouched: no attempt spent
        self.assertEqual(self.claim("r-main", "plain")["id"], p1)                       # a plain claim takes them in order
        self.assertEqual(self.claim("r-main", "plain")["id"], p2)
        self.assertEqual([self.state(j) for j in (l1, l2)], [("running", 1)] * 2)

    def test_an_accept_the_queue_does_not_know_means_all(self):
        for accept in ("LANE", "Lane", " lane", "lane ", "lanes", "lane-only", "lane_only", "", None, 7):
            jid = self.add({})
            got = self.claim("r-odd", accept)
            self.assertEqual(got and got["id"], jid, repr(accept))         # a plain job: only "all" hands it out

    def test_the_plain_hold_is_kept_with_the_other_kinds(self):
        self.claim("r1", "lane", {"plain": self.WHY})
        self.assertEqual(self.store.runner_hold["r1"], {"plain": self.WHY})
        both = {"all": CLEARING_NPM, "lane": self.LIMIT, "plain": self.WHY}
        self.claim("r1", "none", both)
        self.assertEqual(self.store.runner_hold["r1"], both)
        self.claim("r1", "lane", {"plain": "x" * 500})
        self.assertEqual(self.store.runner_hold["r1"], {"plain": "x" * 300})            # cut like the others
        self.claim("r1", "lane", {"plain": "", "lane": None, "Plain": "no", "lane-only": "no", "other": "no"})
        self.assertEqual(self.store.runner_hold["r1"], {})      # empty texts and kinds it does not know are dropped
        for odd in (["plain"], "plain", 3):
            self.claim("r1", "lane", {"plain": self.WHY})
            self.claim("r1", "lane", odd)
            self.assertEqual(self.store.runner_hold["r1"], {}, odd)             # a hold that is no object: nothing kept
        self.claim("r1", "lane", {"plain": self.WHY})
        self.claim("r1", "lane")                                # a claim without a hold clears it
        self.assertEqual(self.store.runner_hold["r1"], {})

    def test_a_plain_job_waits_on_the_plain_hold_and_a_lane_job_on_the_lane_hold(self):
        plain, lane = self.add({}), self.add({"setup": "true"})
        self.claim("r-only", "none", {"plain": self.WHY})       # a lane-only runner with room for lane jobs
        self.assertEqual((self.waiting(plain), self.waiting(lane)), (self.WHY, None))
        self.claim("r-only", "none", {"lane": self.LIMIT, "plain": self.WHY})         # ... and at its limit
        self.assertEqual((self.waiting(plain), self.waiting(lane)), (self.WHY, self.LIMIT))
        self.claim("r-only", "none", {"lane": self.LIMIT})      # a main runner at its lane limit still takes plain jobs
        self.assertEqual((self.waiting(plain), self.waiting(lane)), (None, self.LIMIT))
        self.claim("r-only", "none", {"all": CLEARING_NPM, "plain": self.WHY})        # "all" covers both kinds
        self.assertEqual((self.waiting(plain), self.waiting(lane)), (CLEARING_NPM, CLEARING_NPM))

    def test_with_two_runners_the_reason_names_each_in_the_order_they_first_claimed(self):
        plain, lane = self.add({}), self.add({"llm": True})
        self.claim("r-a", "none", {"lane": self.OFF})                                 # the main runner: no lane jobs
        self.claim("r-b", "none", {"lane": self.LIMIT, "plain": self.WHY})            # the lane-only runner at its limit
        self.assertIsNone(self.waiting(plain))                  # r-a takes plain jobs
        self.assertEqual(self.waiting(lane), "r-a: %s; r-b: %s" % (self.OFF, self.LIMIT))
        self.claim("r-a", "none", {"all": CLEARING_NPM})
        self.assertEqual(self.waiting(plain), "r-a: %s; r-b: %s" % (CLEARING_NPM, self.WHY))
        self.assertEqual(self.waiting(lane), "r-a: %s; r-b: %s" % (CLEARING_NPM, self.LIMIT))

    def test_a_queue_that_ignores_accept_hands_a_lane_only_runner_a_plain_job(self):
        self.store.ignore_accept = True                         # a queue from before `accept`
        plain = self.add({})
        got = self.claim("r-lane", "lane")
        self.assertEqual(got and got["id"], plain)              # the agent gives it back (TestLaneOnlyEndToEnd)
        self.assertEqual(self.state(plain), ("running", 1))


# ---------------------------------------------------------------------------------------------------------------------
# End to end: the real agent process against the reference queue.

# A job that records when it ran: `probe.py TRACE TAG SECS [STOPFILE]` appends "TAG start t" and, after SECS seconds (or
# once STOPFILE exists), "TAG end t". One O_APPEND write per line, so concurrent probes never tear each other's lines.
PROBE = r"""
import os, sys, time
trace, tag, secs = sys.argv[1], sys.argv[2], float(sys.argv[3])
stop = sys.argv[4] if len(sys.argv) > 4 else ""


def mark(what):
    fd = os.open(trace, os.O_WRONLY | os.O_APPEND | os.O_CREAT, 0o644)
    os.write(fd, ("%s %s %.3f\n" % (tag, what, time.time())).encode())
    os.close(fd)


mark("start")
end = time.time() + secs
while time.time() < end and not (stop and os.path.exists(stop)):
    time.sleep(0.05)
mark("end")
"""


class LimitsBase(RemoteBase):
    """RemoteBase plus: the agent's log lines as events, probe jobs that record when they ran, and jobs submitted from
    threads. (Base.start_agent sets CPU_RUNNER_MIN_FREE_GB 0, so the host's free space never decides a test.)"""

    def setUp(self):
        super().setUp()
        self.probe, self.trace = os.path.join(self.td, "probe.py"), os.path.join(self.td, "trace")
        with open(self.probe, "w") as f:
            f.write(PROBE)
        open(self.trace, "w").close()
        self.logs, self.over = [], threading.Event()
        self.addCleanup(self.over.set)          # runs after tearDown: ends submit threads that are still polling

    def start_agent(self, **over):
        p = super().start_agent(**over)
        threading.Thread(target=self._pump, args=(p,), daemon=True).start()
        return p

    def _pump(self, p):
        for line in p.stdout:
            try:
                ev = json.loads(line)
            except ValueError:
                continue
            if isinstance(ev, dict):
                self.logs.append(ev)

    def events(self, name):
        return [e for e in list(self.logs) if e.get("event") == name]

    def ids(self):
        with self.store.lock:
            return list(self.store.order)

    def status(self, jid):
        with self.store.lock:
            return self.store.jobs[jid]["status"]

    def view(self, jid):
        """GET /api/cpu/jobs/<id>, what a client polling the job sees."""
        code, body = cpu_job._http(self.api, TOKEN, "GET", "/api/cpu/jobs/%s" % jid)
        self.assertEqual(code, 200, body)
        return body

    def probe_cmd(self, tag, secs, stop=None):
        argv = [sys.executable, self.probe, self.trace, tag, str(secs)] + ([stop] if stop else [])
        return " ".join(shlex.quote(a) for a in argv)

    def marks(self):
        """[(tag, "start" | "end", time)] in the order the probes wrote them."""
        with open(self.trace) as f:
            rows = [ln.split() for ln in f if ln.strip()]
        return [(t, w, float(ts)) for t, w, ts in (r for r in rows if len(r) == 3)]

    def started(self, tag):
        return any(t == tag and w == "start" for t, w, _ in self.marks())

    def span(self, tag):
        """(start, end) of a probe job; end is None while it runs, start None before it began."""
        d = {w: ts for t, w, ts in self.marks() if t == tag}
        return d.get("start"), d.get("end")

    def submit_async(self, cmd, **kw):
        """submit() on a thread -> (thread, results, job id), once the queue holds the job (it is not 'uploading' any
        more). results gets "r" (the CpuJobResult) or "e" (what submit raised)."""
        known, res = set(self.ids()), {}
        kw.setdefault("cancel_if", lambda: "the test is over" if self.over.is_set() else None)

        def go():
            try:
                res["r"] = self.submit(cmd, **kw)
            except Exception as e:                      # noqa: BLE001 - the test reads it
                res["e"] = e
        t = threading.Thread(target=go, daemon=True)
        t.start()
        jid = wait_for(lambda: next((j for j in self.ids() if j not in known), None), 30)
        self.assertTrue(jid, "the job never reached the queue")
        self.assertTrue(wait_for(lambda: self.status(jid) != "uploading", 30), "the job never became ready")
        return t, res, jid

    def finished(self, t, res, what):
        """Join a submit thread and return its result (a failed submit fails the test with the error)."""
        t.join(90)
        self.assertFalse(t.is_alive(), "%s never finished" % what)
        self.assertNotIn("e", res, "%s: %r" % (what, res.get("e")))
        return res["r"]


class TestLaneLimitEndToEnd(LimitsBase):
    LIMIT = "lane-job limit: 1 of 1 running"

    def test_lane_max_1_runs_one_lane_job_at_a_time_with_plain_jobs_alongside(self):
        subs = []                   # queued before the runner exists, so its first claims all race for the same queue
        for tag, kw, secs in (("L1", {"setup": "true"}, 1.5), ("L2", {"setup": "true"}, 1.5),
                              ("L3", {"setup": "true"}, 1.5), ("P1", {}, 1.0), ("P2", {}, 1.0)):
            subs.append((tag,) + self.submit_async(self.probe_cmd(tag, secs), **kw))
        seen, done = set(), threading.Event()
        self.addCleanup(done.set)

        def sample():                 # every hold the queue was told about
            while not done.is_set():
                seen.add(json.dumps(self.store.runner_hold.get("r0") or {}, sort_keys=True))
                time.sleep(0.02)
        threading.Thread(target=sample, daemon=True).start()
        self.start_agent(CPU_RUNNER_CONCURRENCY=3, CPU_RUNNER_LANE_MAX=1)
        for tag, t, res, _ in subs:
            r = self.finished(t, res, tag)
            self.assertEqual((r.ran_on, r.exit_code), ("runner", 0), (tag, r.infra_error))
        done.set()
        spans = {tag: self.span(tag) for tag, *_ in subs}
        lane = sorted(spans[t] for t in ("L1", "L2", "L3"))
        for (_, end1), (start2, _) in zip(lane, lane[1:]):
            self.assertLessEqual(end1, start2, "two lane jobs ran at once: %s" % spans)
        beside = [p for p in ("P1", "P2") if any(spans[p][0] < e and s < spans[p][1] for s, e in lane)]
        self.assertTrue(beside, "a plain job never ran beside a lane job: %s" % spans)
        self.assertEqual(sorted(e["lane"] for e in self.events("claimed")), [False, False, True, True, True])
        self.assertEqual(self.store.runner_lane_max, {"r0": 1})
        self.assertIn(json.dumps({"lane": self.LIMIT}), seen)               # what the queue was told while L1 ran
        self.assertTrue(all(set(json.loads(h)) <= {"lane"} for h in seen), seen)       # never a hold on everything

    def test_a_lane_job_waits_with_the_limit_as_its_reason_while_a_plain_job_still_runs(self):
        release = os.path.join(self.td, "release")
        self.start_agent(CPU_RUNNER_CONCURRENCY=2, CPU_RUNNER_LANE_MAX=1)
        ta, ra, _ = self.submit_async(self.probe_cmd("A", 120, release), setup="true")
        self.assertTrue(wait_for(lambda: self.started("A"), 40), "the first lane job never started")
        marker, progress = os.path.join(self.td, "b-ran"), []
        tb, rb, jb = self.submit_async("touch %s" % shlex.quote(marker), setup="true", claim_timeout_s=3,
                                       on_progress=lambda ph, pr: progress.append((ph, pr)))
        self.assertTrue(wait_for(lambda: self.view(jb).get("waiting") == self.LIMIT, 10), self.view(jb))
        plain = self.submit(self.probe_cmd("P", 0.1))              # the other slot still takes plain work
        self.assertEqual((plain.ran_on, plain.exit_code), ("runner", 0), plain.infra_error)
        tb.join(30)
        self.assertIn("e", rb, rb)
        self.assertIn("no runner claimed within 3s (%s)" % self.LIMIT, str(rb["e"]))
        self.assertIn(("waiting", self.LIMIT), progress)
        self.assertFalse(os.path.exists(marker))
        self.assertEqual((self.status(jb), self.store.jobs[jb]["attempt"]), ("cancelled", 0))
        open(release, "w").close()
        r = self.finished(ta, ra, "A")
        self.assertEqual((r.ran_on, r.exit_code), ("runner", 0), r.infra_error)

    def test_lane_max_0_turns_lane_jobs_off_and_says_so(self):
        why = "lane jobs are off on this runner (lane_max 0)"
        self.start_agent(CPU_RUNNER_LANE_MAX=0)
        plain = self.submit("echo plain-ok")
        self.assertEqual((plain.ran_on, plain.exit_code, plain.stdout_tail.strip()), ("runner", 0, "plain-ok"),
                         plain.infra_error)
        marker = os.path.join(self.td, "lane-ran")
        with self.assertRaises(RuntimeError) as cm:
            self.submit("touch %s" % shlex.quote(marker), setup="true", claim_timeout_s=2)
        self.assertIn("no runner claimed within 2s (%s)" % why, str(cm.exception))
        self.assertFalse(os.path.exists(marker))
        self.assertEqual(self.store.runner_lane_max, {"r0": 0})
        self.assertEqual(self.store.runner_hold["r0"], {"lane": why})

    def test_a_queue_that_ignores_accept_gets_the_lane_job_handed_back(self):
        self.store.ignore_accept = True                     # a queue from before `accept`: hands out any pending job
        self.start_agent(CPU_RUNNER_LANE_MAX=0)             # this runner takes no lane job at all
        marker = os.path.join(self.td, "lane-ran")
        t, res, jid = self.submit_async("touch %s" % shlex.quote(marker), setup="true", claim_timeout_s=5)
        refused = wait_for(lambda: self.events("claim_refused"), 30)
        self.assertTrue(refused, "the runner never handed the job back")
        self.assertEqual((refused[0]["job"], refused[0]["accept"], refused[0]["lane"]), (jid, "plain", True))
        self.assertIn("update it", refused[0]["hint"])
        self.assertTrue(wait_for(lambda: self.status(jid) == "pending", 10), self.status(jid))
        self.assertEqual(self.store.jobs[jid]["attempt"], 1)
        time.sleep(2)                       # it backs off (REFUSE_BACKOFF_S) instead of taking the same job again
        self.assertEqual(len(self.events("claim_refused")), 1)
        self.assertEqual(self.store.jobs[jid]["attempt"], 1)
        t.join(30)
        self.assertIn("no runner claimed within 5s", str(res.get("e")))
        self.assertFalse(os.path.exists(marker))
        self.assertEqual(self.status(jid), "cancelled")

    def test_a_low_disk_runner_leaves_lane_jobs_pending_with_the_reason_but_runs_plain_jobs(self):
        free = agent_mod.free_gb(self.td)
        if free is None or free < 3:
            self.skipTest("needs 3 GB free here (below 2 GB the runner takes no job at all)")
        self.start_agent(CPU_RUNNER_MIN_FREE_GB="1000000")
        plain = self.submit("echo plain-ok")
        self.assertEqual((plain.ran_on, plain.exit_code), ("runner", 0), plain.infra_error)
        marker = os.path.join(self.td, "lane-ran")
        with self.assertRaises(RuntimeError) as cm:
            self.submit("touch %s" % shlex.quote(marker), setup="true", claim_timeout_s=3)
        self.assertRegex(str(cm.exception), r"no runner claimed within 3s \(low disk: [\d.]+ GB free on .+, "
                                            r"lane jobs need 1000000\.0 GB\)")
        self.assertFalse(os.path.exists(marker))
        hold = self.store.runner_hold["r0"]
        self.assertEqual(list(hold), ["lane"])                 # lane jobs only: this runner still takes plain ones
        self.assertTrue(hold["lane"].startswith("low disk: "), hold)
        held = self.events("hold")
        self.assertTrue(held and held[0]["lane"].startswith("low disk: "), held)


class TestLaneOnlyEndToEnd(LimitsBase):
    """The agent as a lane-only runner (CPU_RUNNER_ACCEPT=lane) against the reference queue."""
    WHY = agent_mod.LANE_ONLY_HOLD
    LIMIT = "lane-job limit: 1 of 1 running"
    OFF = "lane jobs are off on this runner (lane_max 0)"

    def own(self, name, **over):
        """The environment a second agent needs of its own: two agents must not share a work dir, cache or status file."""
        e = {"CPU_RUNNER_WORK": os.path.join(self.td, name + "-work"), "CPU_RUNNER_CACHE": os.path.join(self.td, name + "-cache"),
             "CPU_RUNNER_HEALTH_FILE": os.path.join(self.td, name + "-health"),
             "CPU_RUNNER_STATUS_FILE": os.path.join(self.td, name + "-status")}
        e.update(over)
        return e

    def cancelled(self, t, res, what):
        """End a job that is still pending (the test is over) -> its result."""
        self.over.set()
        r = self.finished(t, res, what)
        self.assertTrue((r.infra_error or "").startswith("cancelled"), r)
        return r

    def test_a_lane_only_runner_takes_a_lane_job_past_an_older_plain_one_and_says_why_it_leaves_that_one(self):
        marker, progress = os.path.join(self.td, "plain-ran"), []
        tp, rp, jp = self.submit_async("touch %s" % shlex.quote(marker), on_progress=lambda ph, pr: progress.append((ph, pr)))
        self.start_agent(CPU_RUNNER_ACCEPT="lane")
        lane = self.submit("echo lane-ok", setup="true")
        self.assertEqual((lane.ran_on, lane.exit_code, lane.stdout_tail.strip()), ("runner", 0, "lane-ok"), lane.infra_error)
        self.assertTrue(wait_for(lambda: self.view(jp).get("waiting") == self.WHY, 10), self.view(jp))
        self.assertTrue(wait_for(lambda: ("waiting", self.WHY) in progress, 10), progress)     # the client is told too
        self.assertEqual((self.status(jp), self.store.jobs[jp]["attempt"]), ("pending", 0))     # never claimed: no attempt
        self.assertFalse(os.path.exists(marker))
        self.assertEqual(self.store.runner_hold["r0"], {"plain": self.WHY})
        self.assertEqual(self.store.runner_lane_max, {"r0": 2})
        self.assertEqual([e["lane"] for e in self.events("claimed")], [True])
        self.assertEqual(self.events("claim_refused"), [])
        held = [{k: v for k, v in e.items() if k not in ("ts", "event")} for e in self.events("hold")]
        self.assertEqual(held, [{"plain": self.WHY}])           # logged once, at the first claim
        self.assertEqual(self.events("hold_cleared"), [])       # it never clears: the runner never takes plain jobs
        self.cancelled(tp, rp, "the plain job")
        self.assertEqual(self.status(jp), "cancelled")

    def test_at_its_limit_a_lane_only_runner_reports_the_limit_with_the_plain_hold(self):
        release = os.path.join(self.td, "release")
        self.start_agent(CPU_RUNNER_ACCEPT="lane", CPU_RUNNER_CONCURRENCY=2, CPU_RUNNER_LANE_MAX=1)
        ta, ra, _ = self.submit_async(self.probe_cmd("A", 120, release), setup="true")
        self.assertTrue(wait_for(lambda: self.started("A"), 40), "the first lane job never started")
        tb, rb, jb = self.submit_async(self.probe_cmd("B", 0.1), setup="true")
        tp, rp, jp = self.submit_async(self.probe_cmd("P", 0.1))
        self.assertTrue(wait_for(lambda: self.view(jb).get("waiting") == self.LIMIT, 10), self.view(jb))
        self.assertEqual(self.view(jp)["waiting"], self.WHY)    # the free slot is no use to a plain job
        self.assertEqual(self.store.runner_hold["r0"], {"lane": self.LIMIT, "plain": self.WHY})
        self.assertFalse(self.started("B"))
        open(release, "w").close()
        for tag, t, res in (("A", ta, ra), ("B", tb, rb)):
            r = self.finished(t, res, tag)
            self.assertEqual((r.ran_on, r.exit_code), ("runner", 0), (tag, r.infra_error))
        self.assertTrue(self.started("B"))
        self.assertEqual((self.status(jp), self.store.jobs[jp]["attempt"], self.started("P")), ("pending", 0, False))
        self.assertEqual(self.events("claim_refused"), [])
        self.assertEqual(self.events("hold_cleared"), [])
        self.cancelled(tp, rp, "the plain job")

    def test_a_queue_that_ignores_accept_gets_the_plain_job_handed_back(self):
        self.store.ignore_accept = True                         # a queue from before `accept`: hands out any pending job
        self.start_agent(CPU_RUNNER_ACCEPT="lane")
        marker = os.path.join(self.td, "plain-ran")
        t, res, jid = self.submit_async("touch %s" % shlex.quote(marker), claim_timeout_s=5)
        refused = wait_for(lambda: self.events("claim_refused"), 30)
        self.assertTrue(refused, "the runner never handed the job back")
        self.assertEqual((refused[0]["job"], refused[0]["accept"], refused[0]["lane"]), (jid, "lane", False))
        self.assertIn("update it", refused[0]["hint"])
        self.assertTrue(wait_for(lambda: self.status(jid) == "pending", 10), self.status(jid))
        self.assertEqual(self.store.jobs[jid]["attempt"], 1)    # the refusal cost the job an attempt
        time.sleep(2)                       # it backs off (REFUSE_BACKOFF_S) instead of taking the same job again
        self.assertEqual(len(self.events("claim_refused")), 1)
        self.assertEqual(self.store.jobs[jid]["attempt"], 1)
        t.join(30)
        self.assertIn("no runner claimed within 5s", str(res.get("e")))
        self.assertFalse(os.path.exists(marker))
        self.assertEqual(self.status(jid), "cancelled")

    def test_a_bad_accept_stops_the_agent_before_it_claims_anything(self):
        p = self.start_agent(CPU_RUNNER_ACCEPT="plain")
        self.assertEqual(p.wait(timeout=30), 3)
        fatal = wait_for(lambda: self.events("fatal"), 10)
        self.assertTrue(fatal, self.logs)
        self.assertIn("CPU_RUNNER_ACCEPT must be all or lane, not 'plain'", fatal[0]["error"])
        self.assertEqual(self.events("start"), [])
        self.assertEqual(self.events("claimed"), [])
        self.assertEqual(self.store.runner_seen, {})            # it pulled its settings, then quit: no claim, no preflight

    def test_a_main_runner_and_a_lane_only_runner_split_the_work(self):
        release = os.path.join(self.td, "release")
        self.start_agent(CPU_RUNNER_LANE_MAX=0)                 # r0, the main runner: plain jobs only
        self.assertTrue(wait_for(lambda: "r0" in self.store.runner_seen, 30), "r0 never claimed")
        self.start_agent(**self.own("r1", CPU_RUNNER_ACCEPT="lane", CPU_RUNNER_CONCURRENCY=2, CPU_RUNNER_LANE_MAX=1))
        ta, ra, ja = self.submit_async(self.probe_cmd("A", 120, release), setup="true")
        self.assertTrue(wait_for(lambda: self.started("A"), 40), "the first lane job never started")
        self.assertEqual(self.store.jobs[ja]["runner_id"], "r1")
        tb, rb, jb = self.submit_async(self.probe_cmd("B", 0.1), setup="true")
        both = "r0: %s; r1: %s" % (self.OFF, self.LIMIT)
        self.assertTrue(wait_for(lambda: self.view(jb).get("waiting") == both, 10), self.view(jb))
        plain = self.submit("echo plain-ok")
        self.assertEqual((plain.ran_on, plain.exit_code), ("runner", 0), plain.infra_error)
        self.assertEqual(self.store.jobs[plain.job_id]["runner_id"], "r0")     # the lane-only runner left it to the main one
        open(release, "w").close()
        for tag, t, res in (("A", ta, ra), ("B", tb, rb)):
            r = self.finished(t, res, tag)
            self.assertEqual((r.ran_on, r.exit_code), ("runner", 0), (tag, r.infra_error))
        self.assertEqual(self.store.jobs[jb]["runner_id"], "r1")
        self.assertEqual(self.store.runner_hold["r0"], {"lane": self.OFF})


class TestCacheCapsEndToEnd(LimitsBase):
    """The disk thread: first pass 5 s after the start, then every CPU_RUNNER_DISK_CHECK_S (5 s at least). Caches are
    ~15 MB files against caps of 0.01 GB, so the disk report (rounded to 0.01 GB) still tells them apart."""
    MB15 = 15 * 2 ** 20

    def test_a_cache_over_its_cap_is_cleared_once_no_job_runs_and_claims_pause_meanwhile(self):
        why = CLEARING_NPM
        npm, release = os.path.join(self.td, "cache", "npm"), os.path.join(self.td, "release")
        blob(os.path.join(npm, "blob"), self.MB15)
        ta, ra, _ = self.submit_async(self.probe_cmd("A", 120, release))     # claimed at once: it spans the first pass
        self.start_agent(CPU_RUNNER_CONCURRENCY=2, CPU_RUNNER_NPM_CACHE_MAX_GB="0.01", CPU_RUNNER_DISK_CHECK_S=5)
        self.assertTrue(wait_for(lambda: self.store.runner_hold.get("r0") == {"all": why}, 40), self.store.runner_hold)
        disk = self.store.runner_disk["r0"]
        self.assertEqual((disk["npm_gb"], disk["npm_max_gb"]), (0.02, 0.01))
        tb, rb, jb = self.submit_async(self.probe_cmd("B", 0.1))
        self.assertTrue(wait_for(lambda: self.view(jb).get("waiting") == why, 10), self.view(jb))
        time.sleep(1.5)                      # the free slot polls all along, but claims nothing while the cleanup waits
        self.assertEqual(self.status(jb), "pending")
        self.assertTrue(os.path.isdir(npm), "the cache was deleted under a running job")
        open(release, "w").close()
        for tag, t, res in (("A", ta, ra), ("B", tb, rb)):
            r = self.finished(t, res, tag)
            self.assertEqual((r.ran_on, r.exit_code), ("runner", 0), (tag, r.infra_error))
        order = [e["event"] for e in list(self.logs) if e["event"] in ("claimed", "cache_cleanup_wait", "cache_cleared")]
        self.assertEqual(order, ["claimed", "cache_cleanup_wait", "cache_cleared", "claimed"])
        (waited,), (cleared,) = self.events("cache_cleanup_wait"), self.events("cache_cleared")
        self.assertEqual((waited["caches"], waited["gb"]), (["npm"], {"npm": 0.02}))
        self.assertGreaterEqual(waited["jobs"], 1)
        self.assertEqual((cleared["cache"], cleared["gb"]), ("npm", 0.02))
        self.assertGreaterEqual(self.span("B")[0], cleared["ts"], "B was claimed before the cache was cleared")
        self.assertFalse(os.path.exists(npm))
        ok = wait_for(lambda: self.store.runner_hold.get("r0") == {} and self.store.runner_disk["r0"]["npm_gb"] == 0.0, 20)
        self.assertTrue(ok, (self.store.runner_hold, self.store.runner_disk))
        self.assertEqual(set(self.store.runner_disk["r0"]), {"work_gb", "work_free_gb", "cache_free_gb", "min_free_gb",
                                                             "uv_gb", "uv_max_gb", "npm_gb", "npm_max_gb", "nm_gb",
                                                             "nm_max_gb"})

    def test_a_cache_a_job_filled_is_noticed_at_the_next_pass_and_cleared_when_over_its_cap(self):
        go, uv = os.path.join(self.td, "go"), os.path.join(self.td, "cache", "uv")
        # the job's setup holds until the first pass has measured an empty uv cache, then fills it with 15 MB
        # (the wait ends after 2 minutes at the latest: a test that died must not leave this loop spinning for good)
        setup = ('mkdir -p "$UV_CACHE_DIR"; echo "$UV_LINK_MODE" > link-mode; i=0; '
                 'while [ ! -e %s ] && [ $i -lt 2400 ]; do sleep 0.05; i=$((i+1)); done; '
                 'head -c %d /dev/urandom > "$UV_CACHE_DIR/blob"' % (shlex.quote(go), self.MB15))
        # two slots: the disk report rides on claims, and a runner whose only slot is busy makes none
        self.start_agent(CPU_RUNNER_CONCURRENCY=2, CPU_RUNNER_UV_CACHE_MAX_GB="0.01", CPU_RUNNER_DISK_CHECK_S=5)
        t, res, _ = self.submit_async("cat link-mode", setup=setup)
        self.assertTrue(wait_for(lambda: "r0" in self.store.runner_disk, 40), "the first pass never reported")
        self.assertEqual(self.store.runner_disk["r0"]["uv_gb"], 0.0)
        open(go, "w").close()
        r = self.finished(t, res, "the lane job")
        self.assertEqual((r.ran_on, r.exit_code), ("runner", 0), r.infra_error)
        self.assertEqual(r.stdout_tail.strip(), "copy")                     # uv copies files into the venv: no hard links
        cleared = wait_for(lambda: self.events("cache_cleared"), 30)        # only a job that touched uv gets it re-measured
        self.assertTrue(cleared, "the uv cache the job filled was never noticed")
        self.assertEqual((cleared[0]["cache"], cleared[0]["gb"]), ("uv", 0.02))
        self.assertFalse(os.path.exists(uv))
        self.assertTrue(wait_for(lambda: self.store.runner_disk["r0"]["uv_gb"] == 0.0, 20), self.store.runner_disk)


# ---------------------------------------------------------------------------------------------------------------------
# The same limits against the REAL queue API (~/bin/ollama-queue-api.py + cpu_lane.py), as test_queue_integration does.
BIN = os.path.expanduser(os.environ.get("CPU_LANE_BIN", "~/bin"))
HAVE_QUEUE = all(os.path.exists(os.path.join(BIN, f)) for f in ("ollama-queue-api.py", "cpu_lane.py"))
RQ_TOKEN, RID = "limits-token", "lim-real"


def _restore_env(saved):
    for k, v in saved.items():
        if v is None:
            os.environ.pop(k, None)
        else:
            os.environ[k] = v


def _forget_modules(had, prev):
    """Undo the imports of the queue-side modules: cpu_lane keeps its job directory from import time, so a later test
    module (test_queue_integration) must import its own."""
    for name in set(sys.modules) - had:
        f = getattr(sys.modules[name], "__file__", None)
        if f and os.path.dirname(os.path.abspath(f)) == os.path.abspath(BIN):
            del sys.modules[name]
    sys.modules.pop("cpu_lane", None)
    if prev is not None:
        sys.modules["cpu_lane"] = prev


@unittest.skipUnless(HAVE_QUEUE, "queue-side modules not in ~/bin")
class TestRealQueueLimits(unittest.TestCase):
    """One runner id for the whole class: the queue keeps its row (hold, lane_max, disk) and an earlier test's runner
    would otherwise count as online for the next three minutes."""

    @classmethod
    def setUpClass(cls):
        cls.procs = []
        cls.td = tempfile.mkdtemp(prefix="cpulane-limits-")
        saved = {k: os.environ.get(k) for k in ("CPU_LANE_DIR", "CPU_RUNNER_TOKEN_FILE", "DISPATCH_VERIFY_SANDBOX",
                                                "OLLAMA_QUEUE_NO_NOTIFY", "CPU_RUNNER_API")}
        had, prev = set(sys.modules), sys.modules.pop("cpu_lane", None)
        for fn, *args in ((shutil.rmtree, cls.td, True), (_restore_env, saved), (_forget_modules, had, prev),
                          (cls.kill_agents,)):
            cls.addClassCleanup(fn, *args)
        sys.path.insert(0, BIN)
        cls.addClassCleanup(sys.path.remove, BIN)
        cls.tok = os.path.join(cls.td, "token")
        with open(cls.tok, "w") as f:
            f.write(RQ_TOKEN + "\n")
        os.environ.update(CPU_LANE_DIR=os.path.join(cls.td, "jobs"), CPU_RUNNER_TOKEN_FILE=cls.tok,
                          DISPATCH_VERIFY_SANDBOX="1", OLLAMA_QUEUE_NO_NOTIFY="1")
        import cpu_lane
        cls.cpu_lane = cpu_lane
        spec = importlib.util.spec_from_file_location("oqapi_limits", os.path.join(BIN, "ollama-queue-api.py"))
        api = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(api)
        cls.srv = api.ThreadingServer(("127.0.0.1", 0), api.Handler)
        cls.addClassCleanup(cls.srv.shutdown)
        threading.Thread(target=cls.srv.serve_forever, daemon=True).start()
        cls.url = "http://127.0.0.1:%d" % cls.srv.server_address[1]
        cls.repo = make_repo(cls.td, lock=False)

    @classmethod
    def kill_agents(cls):
        for p in cls.procs:
            if p.poll() is None:
                p.kill()
                p.wait()

    def setUp(self):
        self.kill_agents()                  # one test's runner must not serve the next

    def agent(self, **over):
        e = dict(os.environ, CPU_RUNNER_API=self.url, CPU_RUNNER_TOKEN_FILE=self.tok, CPU_RUNNER_ISOLATION="none",
                 CPU_RUNNER_CACHE=os.path.join(self.td, "cache"), CPU_RUNNER_WORK=os.path.join(self.td, "work"),
                 CPU_RUNNER_POLL_S="0.2", CPU_RUNNER_HEARTBEAT_S="0.5", CPU_RUNNER_LEASE_S="60",
                 CPU_RUNNER_CONCURRENCY="2", CPU_RUNNER_SHUTDOWN_GRACE_S="2", CPU_RUNNER_ID=RID,
                 CPU_RUNNER_MIN_FREE_GB="0", CPU_RUNNER_HEALTH_FILE=os.path.join(self.td, "health"))
        e.update({k: str(v) for k, v in over.items()})
        self.procs.append(subprocess.Popen([sys.executable, os.path.join(ROOT, "agent", "agent.py")], env=e,
                                           stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL))

    def submit(self, cmd, **kw):
        kw.setdefault("api", self.url)
        kw.setdefault("token", RQ_TOKEN)
        kw.update(fallback=False, poll_s=0.2)
        return cpu_job.submit_cpu_job(self.repo, cmd, 60, **kw)

    def runner(self):
        return next((r for r in self.cpu_lane.get_store().summary()["runners"] if r["runner"] == RID), {})

    def test_1_the_feed_carries_the_runners_lane_limit_and_disk_report(self):
        self.agent(CPU_RUNNER_LANE_MAX=2, CPU_RUNNER_DISK_CHECK_S=5)
        res = {}

        def go():
            try:
                res["r"] = self.submit("sleep 2; echo lane-ok", setup="true")
            except Exception as e:          # noqa: BLE001 - the test reads it
                res["e"] = e
        t = threading.Thread(target=go)
        t.start()
        store = self.cpu_lane.get_store()
        running = wait_for(lambda: [j for j in store.summary()["running"] if j["runner"] == RID], 30)
        self.assertTrue(running, "the lane job never ran")
        self.assertIs(running[0]["lane"], True)                 # the feed marks lane jobs
        t.join(60)
        self.assertNotIn("e", res, res)
        self.assertEqual((res["r"].ran_on, res["r"].exit_code), ("runner", 0), res["r"].infra_error)
        ent = wait_for(lambda: self.runner() if "disk" in self.runner() else None, 40)
        self.assertTrue(ent, "the runner's disk report never reached the feed")
        self.assertEqual((ent["lane_max"], ent["lane_busy"]), (2, 0))
        self.assertNotIn("hold", ent)
        self.assertEqual(ent["disk"].keys(), {"work_gb", "work_free_gb", "cache_free_gb", "min_free_gb", "uv_gb",
                                              "uv_max_gb", "npm_gb", "npm_max_gb", "nm_gb", "nm_max_gb"})
        self.assertEqual((ent["disk"]["uv_max_gb"], ent["disk"]["npm_max_gb"], ent["disk"]["nm_max_gb"]), (10, 4, 6))
        self.assertEqual(ent["disk"]["min_free_gb"], 0)
        self.assertLess(ent["disk_s_ago"], 90)

    def test_2_a_runner_with_lane_jobs_off_runs_plain_jobs_and_the_queue_says_why_a_lane_job_waits(self):
        why = "lane jobs are off on this runner (lane_max 0)"
        self.agent(CPU_RUNNER_LANE_MAX=0)
        plain = self.submit("echo plain-ok")
        self.assertEqual((plain.ran_on, plain.exit_code), ("runner", 0), plain.infra_error)
        self.assertIn("plain-ok", plain.stdout_tail)
        progress = []
        with self.assertRaises(RuntimeError) as cm:
            self.submit("true", setup="true", claim_timeout_s=3, on_progress=lambda ph, pr: progress.append((ph, pr)))
        self.assertIn("no runner claimed within 3s (%s)" % why, str(cm.exception))
        self.assertIn(("waiting", why), progress)
        ent = self.runner()
        self.assertEqual((ent["lane_max"], ent["lane_busy"], ent["hold"]), (0, 0, {"lane": why}))

    def test_3_a_lane_only_runner_takes_a_lane_job_past_an_older_plain_one_and_the_queue_says_why_that_one_waits(self):
        why = "lane-only runner (CPU_RUNNER_ACCEPT=lane)"
        self.agent(CPU_RUNNER_ACCEPT="lane")
        stop, progress, res = threading.Event(), [], {}
        self.addCleanup(stop.set)

        def go():
            try:
                res["r"] = self.submit("echo plain-never", claim_timeout_s=120,
                                       on_progress=lambda ph, pr: progress.append((ph, pr)),
                                       cancel_if=lambda: "the test is over" if stop.is_set() else None)
            except Exception as e:          # noqa: BLE001 - the test reads it
                res["e"] = e
        t = threading.Thread(target=go, daemon=True)
        t.start()
        self.assertTrue(wait_for(lambda: ("waiting", why) in progress, 30), progress)     # queued, and the runner says why
        lane = self.submit("echo lane-ok", setup="true")                                  # newer than the plain job
        self.assertEqual((lane.ran_on, lane.exit_code), ("runner", 0), lane.infra_error)
        self.assertIn("lane-ok", lane.stdout_tail)
        ent = self.runner()
        self.assertEqual((ent["lane_max"], ent["lane_busy"], ent["hold"]), (2, 0, {"plain": why}))
        self.assertTrue(t.is_alive())                           # the older plain job is still waiting for a runner
        stop.set()
        t.join(30)
        self.assertFalse(t.is_alive())
        self.assertNotIn("e", res, res)
        self.assertTrue(res["r"].infra_error.startswith("cancelled"), res["r"].infra_error)


if __name__ == "__main__":
    unittest.main()
