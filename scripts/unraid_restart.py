#!/usr/bin/env python3
"""Update / restart a container through the Unraid GraphQL API (no SSH needed).
Usage: unraid_restart.py <container-name> [--update] [--dry-run]
  default   restart the container (reloads nothing new: the agent is baked into the image)
  --update  docker.updateContainer: pull the container's image and recreate it from its template (Unraid "apply update")
Credentials come from UNRAID_API_URL / UNRAID_API_KEY in the environment, or from the `unraid` MCP entry in
~/.claude.json; the key is only ever sent as the x-api-key header and is never printed."""
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
    r = json.load(urllib.request.urlopen(req, timeout=300))
    if r.get("errors"):
        sys.exit("unraid_restart: API error: %s" % json.dumps(r["errors"])[:300])
    return r["data"]


def main(argv):
    args = [a for a in argv[1:] if not a.startswith("--")]
    if not args:
        sys.exit(__doc__)
    name, dry, update = args[0], "--dry-run" in argv, "--update" in argv
    url, key = creds()
    cs = gql(url, key, "{docker{containers{id names state isUpdateAvailable}}}")["docker"]["containers"]
    hit = [c for c in cs if "/" + name in c["names"] or name in c["names"]]
    if not hit:
        sys.exit("unraid_restart: container %r not found (not installed yet? see README 'Install from the Unraid Docker UI')" % name)
    c = hit[0]
    print("unraid_restart: %s is %s, update available: %s" % (name, c["state"], c.get("isUpdateAvailable")))
    op = "updateContainer" if update else "restart"
    if dry:
        print("unraid_restart: dry run, would call docker.%s on %s" % (op, name))
        return 0
    gql(url, key, 'mutation{docker{%s(id:"%s"){id state}}}' % (op, c["id"]))
    print("unraid_restart: %s requested" % op)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
