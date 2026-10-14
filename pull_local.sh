#!/usr/bin/env bash
# On your desktop: pull normalized data (and optionally raw) from the remote into a local folder.
#   ops/pull_local.sh <remote> <local_dir> [--with-raw]
set -euo pipefail
REMOTE=${1:?remote, e.g. gcs_md:bucket/md-capture}; LOCAL=${2:?local dir}
rclone copy "$REMOTE/normalized" "$LOCAL/normalized" --fast-list -P
rclone copyto "$REMOTE/health/delivered.json" "$LOCAL/delivered.json"
[[ "${3:-}" == "--with-raw" ]] && rclone copy "$REMOTE/raw" "$LOCAL/raw" --fast-list -P
python3 - "$LOCAL" <<'PYEOF'
import json, sys, time, datetime as dt
d = json.load(open(f"{sys.argv[1]}/delivered.json"))
ts = d.get("ts")
print("newest delivered event:", "none" if ts is None else dt.datetime.fromtimestamp(ts, dt.timezone.utc), f"({(time.time()-ts)/60:.0f} min ago)" if ts else "")
PYEOF
