#!/usr/bin/env python3
"""Optional: restart a container through the Unraid GraphQL API (no SSH needed).
Usage: unraid_restart.py <container-name> [--dry-run]
Credentials come from the unraid MCP entry in ~/.claude.json (UNRAID_API_URL / UNRAID_API_KEY) or the same
env vars; the key is only ever sent as the x-api-key header and is never printed.
A restart reloads agent code too, but it does NOT apply Dockerfile/compose/.env changes (use deploy.sh)."""
import json, os, sys, urllib.request


def creds():
    url, key = os.environ.get("UNRAID_API_URL"), os.environ.get("UNRAID_API_KEY")
    if not (url and key):
        try:
            e = json.load(open(os.path.expanduser("~/.claude.json")))["mcpServers"]["unraid"]["env"]
            url, key = url or e["UNRAID_API_URL"], key or e["UNRAID_API_KEY"]
        except (OSError, KeyError, ValueError):
            sys.exit("unraid_restart: no credentials (set UNRAID_API_URL/UNRAID_API_KEY or configure the unraid MCP)")
    return url, key


def gql(url, key, query):
    req = urllib.request.Request(url, json.dumps({"query": query}).encode(),
                                 {"x-api-key": key, "Content-Type": "application/json"})
    r = json.load(urllib.request.urlopen(req, timeout=60))
    if r.get("errors"):
        sys.exit("unraid_restart: API error: %s" % json.dumps(r["errors"])[:300])
    return r["data"]


def main(argv):
    if len(argv) < 2:
        sys.exit(__doc__)
    name, dry = argv[1], "--dry-run" in argv
    url, key = creds()
    cs = gql(url, key, "{docker{containers{id names state}}}")["docker"]["containers"]
    hit = [c for c in cs if "/" + name in c["names"] or name in c["names"]]
    if not hit:
        sys.exit("unraid_restart: container %r not found (not deployed yet? run deploy.sh on Unraid)" % name)
    c = hit[0]
    print("unraid_restart: %s is %s" % (name, c["state"]))
    if dry:
        print("unraid_restart: dry run, not restarting")
        return 0
    gql(url, key, 'mutation{docker{restart(id:"%s"){id state}}}' % c["id"])
    print("unraid_restart: restart requested")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
