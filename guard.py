#!/usr/bin/env python3
"""PreToolUse guard: things only Mark does.

Blocks (exit 2, reason fed back to Claude):
  * deleting / moving / truncating / overwriting raw captures
  * prune_raw.sh --execute, rclone sync/delete/purge (remote deletion)
  * stopping/restarting/disabling the production collector (it causes a data gap)
  * writing or editing files under the raw data dir
  * an agent setting status: active or accepted_warnings in markets.yaml (promotion is Mark's)
"""

from __future__ import annotations

import json
import re
import sys

RAW = r"(data/raw|/raw/prod|\.pcap(\.zst)?\b|\.meta\.json\b)"
BASH_RULES = [
    (rf"\b(rm|mv|shred|truncate|unlink)\b[^|;&]*{RAW}", "deletes or moves raw captures - raw is the source of truth; pruning is Mark's call (ops/prune_raw.sh)"),
    (rf">\s*\S*{RAW}", "overwrites a raw capture"),
    (r"\bfind\b[^|;&]*(data/raw|raw/prod)[^|;&]*-delete", "deletes raw captures"),
    (r"prune_raw\.sh\b.*--execute", "prunes raw - Mark runs this himself after checking the dry run"),
    (r"\brclone\s+(sync|delete|deletefile|purge|rmdir|rmdirs|move|moveto)\b", "remote deletion/move - only `rclone copy` is allowed"),
    (r"systemctl\s+(stop|restart|disable|kill|mask)\s+\S*mdcap-collector", "restarting/stopping the production collector creates a data gap - give Mark the command instead"),
    (r"\bkill(all)?\b.*(mdcap|cryptofeed)", "killing the collector - give Mark the command instead"),
]


def main() -> int:
    try:
        p = json.load(sys.stdin)
    except Exception:
        return 0
    tool = p.get("tool_name")
    ti = p.get("tool_input", {}) or {}
    if tool == "Bash":
        cmd = ti.get("command", "")
        for rx, why in BASH_RULES:
            if re.search(rx, cmd):
                print(f"BLOCKED: {why}.\nCommand: {cmd}", file=sys.stderr)
                return 2
        return 0
    path = ti.get("file_path") or ti.get("notebook_path") or ""
    if re.search(r"(^|/)data/raw/", path) or re.search(r"\.pcap(\.zst)?$", path):
        print("BLOCKED: raw captures are never written or edited by agents.", file=sys.stderr)
        return 2
    if path.endswith("config/markets.yaml") and tool in ("Write", "Edit", "MultiEdit"):
        new = ti.get("content") or ti.get("new_string") or ""
        old = ti.get("old_string") or ""
        if tool == "Write":
            try:
                old = open(path).read()
            except OSError:
                old = ""
        for key in (r"status:\s*active", r"accepted_warnings:\s*\S"):
            if len(re.findall(key, new)) > len(re.findall(key, old)):
                print("BLOCKED: promoting a market (status: active / accepted_warnings) is Mark's decision. "
                      "Show him the probe report and give him the exact lines to change in config/markets.yaml; "
                      "he makes the edit himself.", file=sys.stderr)
                return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
