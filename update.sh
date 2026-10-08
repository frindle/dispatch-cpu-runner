#!/bin/bash
# Mac: roll out the newest published image. Unraid pulls it itself (Docker tab > check for updates > apply update);
# this does the same through the Unraid GraphQL API.
#   ./update.sh                 just print how
#   ./update.sh --restart       also: Unraid pulls the latest image and recreates the container (updateContainer)
#   ./update.sh --restart --dry-run   show what the API would do, change nothing
# The Unraid Docker tab ("check for updates" / "apply update") does the same by hand.
set -euo pipefail
cd "$(dirname "$0")"
case " $* " in
  *" --restart "*)
    extra=(); case " $* " in *" --dry-run "*) extra=(--dry-run);; esac
    python3 scripts/unraid_restart.py dispatch-cpu-runner --update ${extra[@]+"${extra[@]}"};;
  *) echo "update: nothing to push (the container pulls its own image). Run ./update.sh --restart, or use the Unraid Docker tab > apply update.";;
esac
