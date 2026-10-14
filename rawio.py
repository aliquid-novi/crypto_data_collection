"""Read raw cryptofeed captures (L0) as a flat, time-ordered frame stream.

A capture file is loaded whole by cryptofeed's reader, so file size bounds
memory: keep `rotate_size_mb` modest (the old compactor OOM'd by doing the
opposite). One file at a time, always.
"""

from __future__ import annotations

import os
import re
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator

from cryptofeed.capture.pcap import read_capture

ROTATED = re.compile(r"^(?P<base>.+?)(?:\.(?P<idx>\d+))?\.pcap(?:\.zst)?$")


@dataclass(frozen=True)
class Frame:
    exchange: str
    conn_id: str
    ordinal: int  # connect count for this connection id (reconnect => +1)
    resumed: bool  # True when the session continues across a file rotation
    kind: str  # 'ws' | 'http'
    ts_ns: int  # local receive time, ns since epoch (wall clock at frame read)
    payload: str
    url: str = ""  # http only


@dataclass(frozen=True)
class SessionEvent:
    exchange: str
    conn_id: str
    ordinal: int
    resumed: bool
    kind: str
    opened_ns: int
    n_in: int
    first_in_ns: int | None
    last_in_ns: int | None
    subscription: str


def _text(p) -> str:
    return p.decode() if isinstance(p, (bytes, bytearray)) else p


def read_file(path: str | Path) -> tuple[list[Frame], list[SessionEvent]]:
    cap = read_capture(str(path))
    frames: list[Frame] = []
    sessions: list[SessionEvent] = []
    for s in cap.ws:
        ins = [(ts, _text(p)) for ts, d, p in s.messages if d == "in"]
        for ts, p in ins:
            frames.append(Frame(s.exchange, s.conn_id, s.connect_ordinal, s.resumed, "ws", ts, p))
        sessions.append(SessionEvent(s.exchange, s.conn_id, s.connect_ordinal, s.resumed, "ws", s.opened_ns, len(ins),
                                     ins[0][0] if ins else None, ins[-1][0] if ins else None, str(s.subscription)))
    for s in cap.http:
        for ts, method, url, body, _req in s.requests:
            frames.append(Frame(s.exchange, s.conn_id, 0, s.resumed, "http", ts, _text(body), url))
        sessions.append(SessionEvent(s.exchange, s.conn_id, 0, s.resumed, "http", s.opened_ns, len(s.requests),
                                     s.requests[0][0] if s.requests else None, s.requests[-1][0] if s.requests else None, ""))
    frames.sort(key=lambda f: f.ts_ns)
    return frames, sessions


def iter_frames(path) -> Iterator[Frame]:
    frames, _ = read_file(path)
    yield from frames


def closed_files(raw_dir: Path, min_idle_s: float = 600.0, now: float | None = None) -> list[Path]:
    """Capture files safe to read: every rotated file except the newest of the
    newest recording, plus anything untouched for `min_idle_s` (process gone)."""
    now = now or time.time()
    files = [p for p in Path(raw_dir).rglob("*.pcap*") if not p.name.endswith(".tmp")]
    groups: dict[str, list[tuple[int, Path]]] = {}
    for p in files:
        m = ROTATED.match(p.name)
        if not m:
            continue
        groups.setdefault(str(p.parent / m.group("base")), []).append((int(m.group("idx") or 0), p))
    if not groups:
        return []
    newest_base = max(groups, key=lambda b: max(os.path.getmtime(p) for _, p in groups[b]))
    out = []
    for base, items in groups.items():
        items.sort()
        for idx, p in items:
            is_live_tail = base == newest_base and idx == items[-1][0]
            if not is_live_tail or now - os.path.getmtime(p) > min_idle_s:
                out.append(p)
    return sorted(out)
