"""The live process. Deliberately does almost nothing.

* Raw capture: cryptofeed's PcapRecorder records every frame the venues send,
  with the receive timestamp cryptofeed takes when it reads the frame.
* Callbacks only COUNT messages per stream (no parsing, no features, no
  queues that can drop). Normalization happens later, from raw.
* Health: a heartbeat line every few seconds with per-stream counts, last
  receive time, and event-loop lag (how late receive timestamps can be).
* Loud failure: on_feed_error='raise', and a watchdog that exits non-zero if
  any subscribed stream goes silent longer than its documented tolerance or
  the disk is nearly full. systemd restarts the process; the gap is visible
  in the data and the heartbeat says why. The old system used 'remove_feed',
  which let a dead feed disappear quietly.

Known limitation: the receive timestamp is userland wall-clock time after the
event loop gets to the frame, not a kernel timestamp. Loop lag in the
heartbeat bounds how wrong it can be.
"""

from __future__ import annotations

import asyncio
import datetime as dt
import importlib
import json
import logging
import os
import shutil
import sys
import time
from collections import defaultdict

from .config import Market, Settings
from .venues import venue as get_venue

LOG = logging.getLogger("mdcap.collector")
WARMUP_S = 90.0


def _cls(path: str):
    mod, name = path.rsplit(".", 1)
    return getattr(importlib.import_module(mod), name)


class Health:
    def __init__(self, settings: Settings, mode: str, expected: dict[tuple, float]):
        self.s = settings
        self.mode = mode
        self.expected = expected  # (venue, symbol, channel) -> stale_after_s
        self.counts = defaultdict(int)
        self.last = {}
        self.lag_max_ms = 0.0
        self.started = time.time()
        self.recorder = None
        self.fail_reason = None

    def counter(self, venue: str, channel: str):
        async def cb(obj, receipt_timestamp):
            k = (venue, str(getattr(obj, "symbol", "?")), channel)
            self.counts[k] += 1
            self.last[k] = receipt_timestamp
        return cb

    async def loop_lag(self, every: float = 0.1):
        while True:
            t = time.perf_counter()
            await asyncio.sleep(every)  # rawness-ok: measures event-loop lag, not polling
            lag = (time.perf_counter() - t - every) * 1000
            self.lag_max_ms = max(self.lag_max_ms, lag)

    def _write(self, rec: dict):
        self.s.health_dir.mkdir(parents=True, exist_ok=True)
        day = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%d")
        with open(self.s.health_dir / f"heartbeat-{self.mode}-{day}.jsonl", "a") as fh:
            fh.write(json.dumps(rec) + "\n")

    async def heartbeat(self, fh_stop):
        prev = defaultdict(int)
        while True:
            await asyncio.sleep(self.s.heartbeat_s)  # rawness-ok: health heartbeat, not data
            now = time.time()
            streams = {}
            for k in sorted(set(self.expected) | set(self.counts)):
                last = self.last.get(k)
                streams[":".join(k)] = {"n": self.counts[k] - prev[k], "last_recv": last,
                                        "silence_s": None if last is None else round(now - last, 3)}
                prev[k] = self.counts[k]
            free_gb = shutil.disk_usage(self.s.raw_dir).free / 2**30
            rec = {"ts": now, "mode": self.mode, "pid": os.getpid(), "streams": streams,
                   "loop_lag_max_ms": round(self.lag_max_ms, 2), "disk_free_gb": round(free_gb, 2),
                   "raw_file": getattr(getattr(self.recorder, "_pcap", None), "path", None)}
            self.lag_max_ms = 0.0
            # watchdog
            if now - self.started > WARMUP_S:
                for k, tol in self.expected.items():
                    last = self.last.get(k)
                    silent = now - (last if last is not None else self.started)
                    if silent > tol:
                        self.fail_reason = f"STALE {':'.join(k)} silent {silent:.1f}s > {tol:g}s"
                        break
            if free_gb < self.s.min_free_disk_gb:
                self.fail_reason = f"DISK free {free_gb:.1f} GB < {self.s.min_free_disk_gb} GB"
            if self.fail_reason:
                rec["fatal"] = self.fail_reason
            await asyncio.to_thread(self._write, rec)
            if self.fail_reason:
                LOG.critical("watchdog: %s - stopping so systemd restarts us", self.fail_reason)
                fh_stop()
                return


def build(markets: list[Market], settings: Settings, mode: str):
    from cryptofeed import FeedHandler
    from cryptofeed import defines as D
    from cryptofeed.capture import PcapRecorder

    raw_dir = settings.raw_dir / mode
    raw_dir.mkdir(parents=True, exist_ok=True)
    stamp = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    rec = PcapRecorder(str(raw_dir / f"capture-{stamp}.pcap.zst"), rotate_size=settings.rotate_size_mb * 2**20)
    fh = FeedHandler(on_feed_error="raise", record=rec)

    by_venue: dict[str, list[Market]] = defaultdict(list)
    for m in markets:
        by_venue[m.venue].append(m)

    expected = {}
    health = Health(settings, mode, expected)
    health.recorder = rec
    for vname, ms in by_venue.items():
        V = get_venue(vname)
        sub: dict[str, list[str]] = defaultdict(list)
        cbs = {}
        max_depth = max((m.max_depth for m in ms), default=0)
        for m in ms:
            for ch in m.channels:
                st = V.streams[ch]
                cf_ch = getattr(D, st.cryptofeed_channel)
                sub[cf_ch].append(m.symbol)
                cbs[cf_ch] = health.counter(vname, ch)
                expected[(vname, m.symbol, ch)] = float((m.stale_overrides or {}).get(ch, st.stale_after_s))
        feed = _cls(V.feed_class)(subscription=dict(sub), callbacks=cbs, max_depth=max_depth, timeout=60, **V.feed_kwargs)
        fh.add_feed(feed)
    return fh, health, rec


async def run(markets: list[Market], settings: Settings, mode: str = "prod", duration_s: float | None = None) -> int:
    if not markets:
        LOG.error("no markets to collect")
        return 2
    fh, health, _rec = build(markets, settings, mode)
    LOG.info("collecting %d markets -> %s", len(markets), settings.raw_dir / mode)
    feeds = asyncio.create_task(fh.run_async(install_signal_handlers=True), name="feeds")
    lag = asyncio.create_task(health.loop_lag(), name="loop-lag")
    hb = asyncio.create_task(health.heartbeat(fh.request_stop), name="heartbeat")
    waiters = {feeds, hb}
    if duration_s:
        waiters.add(asyncio.create_task(asyncio.sleep(duration_s), name="duration"))  # rawness-ok: probe length
    await asyncio.wait(waiters, return_when=asyncio.FIRST_COMPLETED)
    fh.request_stop()
    for t in (lag, hb, *[w for w in waiters if w is not feeds]):
        t.cancel()
    await feeds  # run_async closes the recorder; a feed exception propagates => non-zero exit
    return 3 if health.fail_reason else 0


def main(markets, settings, mode="prod", duration_s=None) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    code = asyncio.run(run(markets, settings, mode, duration_s))
    sys.exit(code)
