#!/usr/bin/env bash
# Free VM disk: delete LOCAL raw captures older than N days ONLY if the remote has an identical copy.
# Dry run by default. Mark runs this; agents are blocked from running it with --execute (see .claude/hooks).
#   ops/prune_raw.sh 7            # show what would go
#   ops/prune_raw.sh 7 --execute  # do it
set -euo pipefail
cd "$(dirname "$0")/.."
DAYS=${1:?days to keep locally}; MODE=${2:-}
PY=${PY:-.venv/bin/python}
read -r REMOTE RAW < <($PY -c "from mdcap.config import load_settings as l; s=l(); print(s.delivery.get('remote',''), s.raw_dir)")
find "$RAW/prod" -name '*.pcap*' -mtime +"$DAYS" -print0 | while IFS= read -r -d '' f; do
  rel=${f#"$RAW/prod/"}
  if rclone check "$f" "$REMOTE/raw/prod/$(dirname "$rel")" --one-way --include "$(basename "$rel")" -q 2>/dev/null; then
    if [[ "$MODE" == "--execute" ]]; then rm -v "$f"; else echo "would delete (verified remote): $f"; fi
  else
    echo "KEEP (not verified on remote): $f"
  fi
done
