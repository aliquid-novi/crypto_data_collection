#!/usr/bin/env bash
# Ship closed raw captures + normalized parquet to the remote, then record what was delivered.
# copy (not sync): never deletes anything remotely. Raw is shipped too - it's the source of truth.
set -euo pipefail
cd "$(dirname "$0")/.."
PY=${PY:-.venv/bin/python}
read -r REMOTE MIN_AGE RAW NORM HEALTH < <($PY - <<'PYEOF'
from mdcap.config import load_settings
s = load_settings()
d = s.delivery
print(d.get("remote", ""), d.get("min_age", "15m"), s.raw_dir, s.norm_dir, s.health_dir)
PYEOF
)
if [[ -z "$REMOTE" || "$REMOTE" == *YOUR-BUCKET* ]]; then echo "delivery.remote not configured in config/settings.yaml" >&2; exit 2; fi

rclone copy "$RAW/prod" "$REMOTE/raw/prod" --min-age "$MIN_AGE" --include "*.pcap*" --include "*.meta.json" --fast-list
rclone copy "$NORM" "$REMOTE/normalized" --min-age 2m --exclude "*.tmp" --fast-list

# Freshness marker = newest EVENT time that is now remote (from the manifest), not file mtimes.
$PY - "$HEALTH" <<'PYEOF'
import json, sys, time
from pathlib import Path
from mdcap.config import load_settings
from mdcap.normalize import manifest
s = load_settings()
last = max((m["last_ts_ns"] for m in manifest(s.norm_dir) if m.get("last_ts_ns")), default=None)
Path(sys.argv[1]).mkdir(parents=True, exist_ok=True)
(Path(sys.argv[1]) / "delivered.json").write_text(json.dumps({"ts": last / 1e9 if last else None, "run_at": time.time()}))
PYEOF
rclone copyto "$HEALTH/delivered.json" "$REMOTE/health/delivered.json"
echo "delivered OK"
