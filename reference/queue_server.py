#!/usr/bin/env python3
"""Reference implementation of the queue-side `cpu` job API (stdlib, in-memory).

This is the contract the real queue (ollama-queue-api.py) must implement; the
tests run the agent and client against it. State is in-memory; the real queue
would persist rows in ollama-queue-state.json under its flock.
Auth: `Authorization: Bearer <token>` on every request except GET /api/cpu/health.
"""
import json, re, secrets, threading, time, uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

MAX_ATTEMPTS = 3


class Store:
    def __init__(self, token, clock=time.time):
        self.token, self.clock = token, clock
        self.jobs, self.order = {}, []
        self.lock = threading.Lock()
        self.runner_seen = {}
        self.runner_caps = {}       # runner_id -> last capability report (claim body `caps`)
        self.codes = set()          # live single-use enrollment codes
        self.config = {"concurrency": 2, "lease_s": 60}
        self.enrolled = []

    def reap(self):
        """Lease expiry: running jobs whose lease lapsed go back to pending (or failed_infra)."""
        now = self.clock()
        for j in self.jobs.values():
            if j["status"] == "running" and j["lease_expires"] < now:
                j["events"].append(("lease_expired", j["runner_id"], now))
                j["lease_token"] = j["runner_id"] = None
                if j["attempt"] >= MAX_ATTEMPTS:
                    j["status"] = "failed_infra"
                    j["result"] = {"exit_code": None, "infra_error": "lease expired %d times" % j["attempt"]}
                else:
                    j["status"] = "pending"


class H(BaseHTTPRequestHandler):
    store = None
    protocol_version = "HTTP/1.1"

    def log_message(self, *a):
        pass

    def _send(self, code, body=None, raw=None):
        data = raw if raw is not None else (json.dumps(body).encode() if body is not None else b"")
        self.send_response(code)
        self.send_header("Content-Type", "application/octet-stream" if raw is not None else "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _body(self):
        n = int(self.headers.get("Content-Length") or 0)
        return self.rfile.read(n) if n else b""

    def _json(self):
        b = self._body()
        return json.loads(b) if b else {}

    def _route(self, method):
        S = self.store
        path = self.path.split("?")[0]
        if path == "/api/cpu/health":
            return self._send(200, {"ok": True})
        if method == "POST" and path == "/api/cpu/enroll":
            b = self._json()
            with S.lock:
                ok = b.get("code") in S.codes
                S.codes.discard(b.get("code"))
                if ok:
                    S.enrolled.append(b.get("runner_id"))
            if not ok:
                return self._send(403, {"error": "invalid, expired or already used enrollment code"})
            return self._send(200, {"token": S.token, "runner_id": b.get("runner_id"), "config": S.config})
        if self.headers.get("Authorization") != "Bearer " + S.token:
            return self._send(401, {"error": "unauthorized"})
        with S.lock:
            S.reap()
            if method == "POST" and path == "/api/cpu/jobs":
                b = self._json()
                jid = uuid.uuid4().hex
                S.jobs[jid] = {"id": jid, "spec": b["spec"], "status": "uploading", "attempt": 0,
                               "blobs": {}, "result": None, "lease_token": None, "runner_id": None,
                               "lease_expires": 0, "cancel": False, "events": [],
                               "label": b.get("label"), "created": S.clock()}
                S.order.append(jid)
                return self._send(201, {"id": jid})
            if method == "POST" and path == "/api/cpu/claim":
                b = self._json()
                S.runner_seen[b.get("runner_id")] = S.clock()
                if isinstance(b.get("caps"), dict):
                    S.runner_caps[b.get("runner_id")] = b["caps"]
                for jid in S.order:
                    j = S.jobs[jid]
                    if j["status"] == "pending" and not j["cancel"]:
                        j["status"] = "running"
                        j["attempt"] += 1
                        j["runner_id"] = b["runner_id"]
                        j["lease_token"] = secrets.token_hex(8)
                        j["lease_expires"] = S.clock() + int(b.get("lease_s") or 60)
                        j["events"].append(("claimed", b["runner_id"], S.clock()))
                        spec = dict(j["spec"], has_patch="patch" in j["blobs"], has_tools="tools" in j["blobs"])
                        return self._send(200, {"job": {"id": jid, "spec": spec, "attempt": j["attempt"],
                                                        "lease_token": j["lease_token"]}})
                return self._send(204)
            if method == "GET" and path == "/api/cpu/config":
                return self._send(200, {"config": S.config})
            if method == "GET" and path == "/api/cpu/runners":
                return self._send(200, {"runners": S.runner_seen})
            m = re.match(r"^/api/cpu/jobs/([0-9a-f]+)(?:/(\w+))?$", path)
            if not m or m.group(1) not in S.jobs:
                return self._send(404, {"error": "not found"})
            j, sub = S.jobs[m.group(1)], m.group(2)
            if sub is None and method == "GET":
                return self._send(200, {"id": j["id"], "status": j["status"], "attempt": j["attempt"],
                                        "result": j["result"], "runner_id": j["runner_id"]})
            if sub is None and method == "DELETE":
                j["cancel"] = True
                if j["status"] in ("pending", "uploading"):
                    j["status"] = "cancelled"
                return self._send(200, {"ok": True})
            if sub in ("payload", "patch", "tools"):
                if method == "PUT":
                    j["blobs"][sub] = self._body()
                    return self._send(200, {"ok": True, "bytes": len(j["blobs"][sub])})
                if method == "GET" and sub in j["blobs"]:
                    return self._send(200, raw=j["blobs"][sub])
                return self._send(404, {"error": "no blob"})
            if sub == "ready" and method == "POST":
                if "payload" not in j["blobs"]:
                    return self._send(400, {"error": "payload missing"})
                if j["status"] == "uploading":
                    j["status"] = "pending"
                return self._send(200, {"ok": True})
            if sub in ("heartbeat", "result", "release") and method == "POST":
                b = self._json()
                if j["status"] != "running" or j["lease_token"] != b.get("lease_token"):
                    return self._send(409, {"error": "lease lost"})
                if sub == "heartbeat":
                    j["lease_expires"] = S.clock() + int(b.get("lease_s") or 60)
                    return self._send(200, {"ok": True, "cancel": j["cancel"]})
                if sub == "release":
                    j["status"], j["lease_token"], j["runner_id"] = "pending", None, None
                    return self._send(200, {"ok": True})
                j["result"] = {k: v for k, v in b.items() if k != "lease_token"}
                j["status"] = "failed_infra" if b.get("infra_error") else "done"
                if b.get("infra_error") and j["attempt"] < MAX_ATTEMPTS and b.get("infra_error") == "isolation_unavailable":
                    j["status"], j["result"] = "pending", None
                return self._send(200, {"ok": True})
        return self._send(404, {"error": "no route"})

    def do_GET(self): self._route("GET")
    def do_POST(self): self._route("POST")
    def do_PUT(self): self._route("PUT")
    def do_DELETE(self): self._route("DELETE")


def serve(token, host="127.0.0.1", port=0):
    store = Store(token)
    handler = type("Handler", (H,), {"store": store})
    srv = ThreadingHTTPServer((host, port), handler)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    return srv, store


if __name__ == "__main__":
    import sys
    srv, _ = serve(sys.argv[1], "0.0.0.0", int(sys.argv[2]) if len(sys.argv) > 2 else 7685)
    print("listening", srv.server_address, flush=True)
    threading.Event().wait()
