#!/usr/bin/env python3
"""Static scan for code that makes captured data less raw or less loud.

PostToolUse hook on edits under mdcap/ and ops/; also runnable by hand:
    python .claude/hooks/rawness_scan.py mdcap/parsers/okx.py

Every pattern below was a real failure in the old collector, or is the usual
way the same failure gets reintroduced. Silence a line you've justified with
a trailing comment:  # rawness-ok: <reason>
"""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path

RULES = [
    (r"fillna\(\s*0", "zero-filling invents prices (the ±86 'spread' bug). Leave nulls; research decides ffill"),
    (r"\.resample\(", "resampling in the capture layer destroys arrival times. Research's job"),
    (r"remove_feed", "on_feed_error='remove_feed' lets a dead feed vanish silently. Use 'raise' + systemd restart"),
    (r"put_nowait|QueueFull", "drop-on-full queue = silent loss. Raw recorder + counters only"),
    (r"\b(mid|microprice|imbalance|spread(_bps)?|vwap)\s*=", "derived feature in the capture layer. Keep raw fields; derive in research"),
    (r"\b(time|asyncio)\.sleep\(", "sleep in capture code: is this POLLING? Polled streams must be declared in the venue profile"),
    (r"\b(requests|httpx)\.(get|post)\(|aiohttp\.ClientSession", "REST call in capture code: polling? Use the venue's WebSocket stream"),
    (r"iloc\[\s*0\s*\].*(strftime|date|day|hour)|(strftime|date|day|hour).*iloc\[\s*0\s*\]", "partitioning by the first row of a batch (rows near midnight land in the wrong day)"),
    (r"nSigFigs|mantissa|snapshot_interval|snapshots_only", "aggregated/snapshot-only book option: hides the real tick grid or update stream"),
    (r"drop_duplicates\(", "deduplicating in the capture layer hides replays the audits need to see"),
    (r"\bexcept\b[^:]*:\s*(pass|continue)\s*$", "swallowed exception: a frame or error disappears silently"),
]
COMPILED = [(re.compile(p), why) for p, why in RULES]
OK = re.compile(r"#\s*rawness-ok\b")
# Where the rules apply. Audits/report/synth legitimately sleep, dedupe or fabricate data.
SCOPE = re.compile(r"(^|/)mdcap/(collector|rawio|normalize|parsers/[^/]+)\.py$")


def scan(path: Path) -> list[str]:
    hits = []
    in_doc = False
    for i, line in enumerate(path.read_text().splitlines(), 1):
        n_q = line.count('"""') + line.count("'''")
        if in_doc or (n_q and line.strip().startswith(('"""', "'''", 'r"""'))):
            if n_q % 2 == 1:
                in_doc = not in_doc
            continue  # docstrings describe rules, they don't break them
        code = line.split("#", 1)[0]
        if not code.strip() or OK.search(line):
            continue
        for rx, why in COMPILED:
            if rx.search(code):
                hits.append(f"{path.name}:{i}: {why}\n    {line.strip()}")
    return hits


def main() -> int:
    if len(sys.argv) > 1:
        hits = [h for f in sys.argv[1:] for h in scan(Path(f))]
        print("\n".join(hits) or "rawness scan: clean")
        return 1 if hits else 0
    try:
        p = json.load(sys.stdin)
    except Exception:
        return 0
    f = (p.get("tool_input") or {}).get("file_path") or ""
    if not f or not SCOPE.search(f.replace("\\", "/")) or not Path(f).exists():
        return 0
    hits = scan(Path(f))
    if hits:
        print("RAWNESS TRIPWIRE: fix each hit, or justify it on the line with '# rawness-ok: <reason>'.\n" + "\n".join(hits), file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
