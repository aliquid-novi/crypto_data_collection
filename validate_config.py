#!/usr/bin/env python3
"""PostToolUse: after any edit to markets.yaml, the venue registry or a venue
profile, run `python -m mdcap validate` and feed errors back immediately."""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys
from pathlib import Path

WATCH = re.compile(r"(config/markets\.yaml|mdcap/venues\.py|venues/[^/]+\.md)$")


def main() -> int:
    try:
        p = json.load(sys.stdin)
    except Exception:
        return 0
    f = ((p.get("tool_input") or {}).get("file_path") or "").replace("\\", "/")
    if not WATCH.search(f):
        return 0
    root = Path(os.environ.get("CLAUDE_PROJECT_DIR", Path(__file__).resolve().parents[2]))
    r = subprocess.run([sys.executable, "-m", "mdcap", "validate"], cwd=root, capture_output=True, text=True)
    if r.returncode != 0:
        print("CONFIG INVALID after this edit:\n" + (r.stdout + r.stderr)[-3000:], file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
