"""Synthetic raw captures in cryptofeed's real PCAP format, with planted faults.

Used by the tests, and by agents to reproduce an incident before fixing it:
a fix isn't done until a synthetic capture showing the fault is caught by an
audit. Message shapes match the venue wire formats in mdcap/parsers.

Faults you can plant (all off by default):
  hl_poll_s        Hyperliquid book arrives on a sleep(N)+jitter timer   (old system)
  depth_break_at   Binance depth: pu chain broken once at this message index
  agg_gap_at       Binance aggTrade: ids skip at this index
  dup_burst_at     replay the same bbo N times with zero gap (resubscribe)
  stall_every_s    every stream silent for `stall_len_s` at this period    (hourly stall)
  sizes_missing    bookTicker without B/A sizes (a lossy upstream)
  clock_skew_ms    receive clock behind venue clock by this much
"""

from __future__ import annotations

import json
from pathlib import Path

import numpy as np

from cryptofeed.capture.pcap import (META_ADDRESS, META_CONN_ID, META_EXCHANGE, META_ORDINAL, META_SUBSCRIPTION,
                                     HTTPStream, PcapFile, WSStream)

T0 = 1_791_158_400 * 10**9  # 2026-10-05 00:00:00 UTC


def _meta(exchange, conn_id, address, sub, ordinal=1):
    return {META_CONN_ID: conn_id, META_EXCHANGE: exchange, META_ADDRESS: address, META_ORDINAL: str(ordinal),
            META_SUBSCRIPTION: json.dumps(sub)}


def _event_times(rng, start_ns, dur_s, rate_hz):
    n = rng.poisson(rate_hz * dur_s)
    return np.sort(start_ns + (rng.random(n) * dur_s * 1e9).astype("int64"))


def _apply_stalls(ts, start_ns, every_s, len_s):
    if not every_s:
        return ts
    phase = ((ts - start_ns) / 1e9) % every_s
    return ts[phase >= len_s]


def write_capture(path, dur_s=1800.0, seed=0, start_ns=T0, coin="HYPE", bsym="HYPEUSDT",
                  hl_poll_s=None, depth_break_at=None, agg_gap_at=None, dup_burst_at=None, dup_burst_n=50,
                  stall_every_s=None, stall_len_s=10.0, sizes_missing=False, clock_skew_ms=0.0,
                  net_latency_ms=3.0) -> Path:
    rng = np.random.default_rng(seed)
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    pc = PcapFile(str(path))
    lat = lambda n: (rng.lognormal(np.log(net_latency_ms), 0.4, n) * 1e6).astype("int64") - int(clock_skew_ms * 1e6)  # noqa: E731
    px = 86.2

    # ---------------- Hyperliquid
    addr = "wss://api.hyperliquid.xyz/ws"
    hl = WSStream(pc, 40001, start_ns, addr, _meta("HYPERLIQUID", "HYPERLIQUID.ws.1", addr, {"bbo": [coin], "l2Book": [coin], "trades": [coin]}))
    msgs = []
    if hl_poll_s:  # the old failure: a timer, not the venue
        t, tt = start_ns, []
        while t < start_ns + dur_s * 1e9:
            t += int((hl_poll_s + rng.uniform(0.0, 0.5)) * 1e9)
            tt.append(t)
        book_t = np.array(tt, dtype="int64")
        bbo_t = book_t.copy()
    else:
        blocks = start_ns + np.cumsum(rng.exponential(0.08, int(dur_s / 0.08 * 1.2)) * 1e9).astype("int64")
        blocks = blocks[blocks < start_ns + dur_s * 1e9]
        bbo_t = blocks[rng.random(len(blocks)) < 0.35]  # bbo changes on ~35% of blocks
        book_t, last = [], -10**18
        for b in blocks:  # >= 0.5 s since last push
            if b - last >= 5 * 10**8:
                book_t.append(b)
                last = b
        book_t = np.array(book_t, dtype="int64")
    bbo_t = _apply_stalls(bbo_t, start_ns, stall_every_s, stall_len_s)
    book_t = _apply_stalls(book_t, start_ns, stall_every_s, stall_len_s)
    for t in bbo_t:
        bid = round(px + rng.normal(0, 0.01), 3)
        msgs.append((t, {"channel": "bbo", "data": {"coin": coin, "time": int(t // 10**6),
                     "bbo": [{"px": f"{bid:.3f}", "sz": f"{rng.gamma(2, 50):.2f}", "n": int(rng.integers(1, 30))},
                             {"px": f"{bid + 0.001:.3f}", "sz": f"{rng.gamma(2, 50):.2f}", "n": int(rng.integers(1, 30))}]}}))
    for t in book_t:
        mid = round(px + rng.normal(0, 0.01), 3)
        lv = lambda s: [{"px": f"{mid + s * (i + 0.5) * 0.001:.4f}", "sz": f"{rng.gamma(2, 50):.2f}", "n": int(rng.integers(1, 20))} for i in range(20)]  # noqa: E731
        msgs.append((t, {"channel": "l2Book", "data": {"coin": coin, "time": int(t // 10**6), "levels": [lv(-1), lv(1)]}}))
    for t in _apply_stalls(_event_times(rng, start_ns, dur_s, 0.5), start_ns, stall_every_s, stall_len_s):
        msgs.append((t, {"channel": "trades", "data": [{"coin": coin, "side": "B" if rng.random() < 0.5 else "A", "px": f"{px:.3f}",
                                                         "sz": f"{rng.exponential(5):.2f}", "hash": "0x" + "ab" * 32,
                                                         "time": int(t // 10**6), "tid": int(rng.integers(0, 2**50)), "users": ["0x1", "0x2"]}]}))
    msgs.sort(key=lambda x: x[0])
    if dup_burst_at is not None:
        bb = [m for m in msgs if m[1]["channel"] == "bbo"]
        t0, m0 = bb[min(dup_burst_at, len(bb) - 1)]
        msgs += [(t0, m0)] * dup_burst_n
        msgs.sort(key=lambda x: x[0])
    hl.message(start_ns + 10**6, json.dumps({"channel": "subscriptionResponse", "data": {"method": "subscribe"}}))
    recv = np.maximum.accumulate(np.array([t for t, _ in msgs], dtype="int64") + lat(len(msgs)))  # TCP keeps order
    for i in range(len(msgs)):
        hl.message(int(recv[i]), json.dumps(msgs[i][1]))
    hl.close(start_ns + int(dur_s * 1e9))

    # ---------------- Binance futures
    baddr = f"wss://fstream.binance.com/stream?streams={bsym.lower()}@bookTicker/{bsym.lower()}@depth@100ms/{bsym.lower()}@aggTrade"
    bn = WSStream(pc, 40002, start_ns, baddr, _meta("BINANCE_FUTURES", "BINANCE_FUTURES.ws.2", baddr,
                                                   {"bookTicker": [bsym], "depth": [bsym], "aggTrade": [bsym]}))
    http = HTTPStream(pc, 40003, start_ns, {META_CONN_ID: "BINANCE_FUTURES.http.0", META_EXCHANGE: "BINANCE_FUTURES"})
    bmsgs = []
    upd = 5_000_000
    tick_t = _apply_stalls(_event_times(rng, start_ns, dur_s, 8.0), start_ns, stall_every_s, stall_len_s)
    for t in tick_t:
        upd += int(rng.integers(1, 40))
        bid = round(px + rng.normal(0, 0.01), 3)
        d = {"e": "bookTicker", "u": upd, "s": bsym, "b": f"{bid:.3f}", "a": f"{bid + 0.001:.3f}", "T": int(t // 10**6), "E": int(t // 10**6) + 1}
        if not sizes_missing:
            d |= {"B": f"{rng.gamma(2, 80):.1f}", "A": f"{rng.gamma(2, 80):.1f}"}
        bmsgs.append((t, {"stream": f"{bsym.lower()}@bookTicker", "data": d}))
    grid = start_ns + np.arange(1, int(dur_s / 0.1)) * 10**8  # depth@100ms: venue-clock grid
    grid = grid[rng.random(len(grid)) < 0.9]
    grid = _apply_stalls(grid, start_ns, stall_every_s, stall_len_s)
    last_u = 9_000_000
    http.exchange(start_ns + 2 * 10**8, "GET", f"https://fapi.binance.com/fapi/v1/depth?symbol={bsym}&limit=1000",
                  json.dumps({"lastUpdateId": last_u, "E": int(start_ns // 10**6), "T": int(start_ns // 10**6),
                              "bids": [[f"{px - 0.001 * i:.3f}", "10"] for i in range(5)], "asks": [[f"{px + 0.001 * (i + 1):.3f}", "10"] for i in range(5)]}))
    for k, t in enumerate(grid):
        U = last_u + 1
        u = U + int(rng.integers(0, 30))
        pu = last_u if k != depth_break_at else last_u - 7
        bmsgs.append((t, {"stream": f"{bsym.lower()}@depth@100ms", "data": {"e": "depthUpdate", "E": int(t // 10**6), "T": int(t // 10**6) - 2,
                     "s": bsym, "U": U, "u": u, "pu": pu, "b": [[f"{px - 0.001:.3f}", f"{rng.gamma(2, 30):.1f}"]], "a": [[f"{px + 0.002:.3f}", "0"]]}}))
        last_u = u
    a_id, f_id = 700_000, 3_000_000
    for k, t in enumerate(_apply_stalls(_event_times(rng, start_ns, dur_s, 2.0), start_ns, stall_every_s, stall_len_s)):
        n = int(rng.integers(1, 4))
        if k == agg_gap_at:
            a_id += 3
            f_id += 9
        bmsgs.append((t, {"stream": f"{bsym.lower()}@aggTrade", "data": {"e": "aggTrade", "E": int(t // 10**6) + 1, "a": a_id, "s": bsym,
                     "p": f"{px:.3f}", "q": f"{rng.exponential(5):.1f}", "f": f_id, "l": f_id + n - 1, "T": int(t // 10**6), "m": bool(rng.random() < 0.5)}}))
        a_id += 1
        f_id += n
    bmsgs.sort(key=lambda x: x[0])
    bn.message(start_ns + 10**6, json.dumps({"result": None, "id": 1}))
    recv = np.maximum.accumulate(np.array([t for t, _ in bmsgs], dtype="int64") + lat(len(bmsgs)))
    for i in range(len(bmsgs)):
        bn.message(int(recv[i]), json.dumps(bmsgs[i][1]))
    bn.close(start_ns + int(dur_s * 1e9))
    http.close(start_ns + int(dur_s * 1e9))
    pc.close()
    return path
