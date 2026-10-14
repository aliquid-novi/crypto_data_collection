"""Binance USD-M futures raw frames -> normalized rows.

Wire formats (cryptofeed subscribes via /stream?streams=..., so payloads are
wrapped as {"stream": ..., "data": {...}}; unwrapped is also accepted):

bookTicker  {e, u, E, T, s, b, B, a, A}
depthUpdate {e, E, T, s, U, u, pu, b: [[px, qty]...], a: [...]}  qty is ABSOLUTE; 0 = remove level
aggTrade    {e, E, a, s, p, q, f, l, T, m}   m = buyer is maker  =>  aggressor is SELL
REST /fapi/v1/depth -> {lastUpdateId, E, T, bids, asks}  (cryptofeed's book anchor)
"""

from __future__ import annotations

import json
from urllib.parse import parse_qs, urlsplit

from . import f, ms_to_ns

VENUE = "BINANCE_FUTURES"


def norm_symbol(s: str) -> str:
    s = s.upper()
    if "_" in s:  # dated delivery contract, e.g. BTCUSDT_261225
        return s
    for q in ("USDT", "USDC", "BUSD"):
        if s.endswith(q) and len(s) > len(q):
            return f"{s[:-len(q)]}-{q}-PERP"
    return s


def _base(frame, venue_symbol):
    return {
        "venue": VENUE,
        "symbol": norm_symbol(venue_symbol) if venue_symbol else None,
        "venue_symbol": venue_symbol,
        "ts_recv_ns": frame.ts_ns,
        "conn_id": frame.conn_id,
        "conn_ordinal": frame.ordinal,
    }


def _rest(frame):
    u = urlsplit(frame.url)
    if "/fapi/v1/depth" in u.path:
        q = parse_qs(u.query)
        sym = (q.get("symbol") or [None])[0]
        d = json.loads(frame.payload)
        b = _base(frame, sym)
        rows = []
        common = {**b, "ts_exch_ns": ms_to_ns(d.get("E")), "ts_txn_ns": ms_to_ns(d.get("T")), "kind": "snapshot",
                  "first_id": None, "last_id": d.get("lastUpdateId"), "prev_id": None}
        for side, key in (("bid", "bids"), ("ask", "asks")):
            for lvl, (px, qty, *_) in enumerate(d.get(key, [])):
                rows.append(("l2", {**common, "side": side, "level": lvl, "price": f(px), "size": f(qty), "n_orders": None}))
        if not rows:  # empty book is still a snapshot
            rows.append(("l2", {**common, "side": None, "level": None, "price": None, "size": None, "n_orders": None}))
        return "rest_snapshot", rows
    return "rest_meta", [("events", {**_base(frame, None), "kind": "rest_meta", "detail": u.path})]


def parse(frame):
    if frame.kind == "http":
        return _rest(frame)
    msg = json.loads(frame.payload)
    d = msg.get("data", msg) if isinstance(msg, dict) else msg
    if not isinstance(d, dict):
        return "unknown", [("events", {**_base(frame, None), "kind": "unknown", "detail": frame.payload[:500]})]
    e = d.get("e")

    if e == "bookTicker" or (e is None and {"b", "B", "a", "A"} <= d.keys()):
        return "l1", [("l1", {**_base(frame, d.get("s")), "ts_exch_ns": ms_to_ns(d.get("E")), "ts_txn_ns": ms_to_ns(d.get("T")),
                               "update_id": d.get("u"), "bid_px": f(d.get("b")), "bid_sz": f(d.get("B")),
                               "ask_px": f(d.get("a")), "ask_sz": f(d.get("A")), "bid_n": None, "ask_n": None})]

    if e == "depthUpdate":
        b = _base(frame, d.get("s"))
        common = {**b, "ts_exch_ns": ms_to_ns(d.get("E")), "ts_txn_ns": ms_to_ns(d.get("T")), "kind": "delta",
                  "first_id": d.get("U"), "last_id": d.get("u"), "prev_id": d.get("pu")}
        rows = []
        for side, key in (("bid", "b"), ("ask", "a")):
            for px, qty, *_ in d.get(key, []):
                rows.append(("l2", {**common, "side": side, "level": None, "price": f(px), "size": f(qty), "n_orders": None}))
        if not rows:  # a sequence-carrying message with no levels still matters for pu/u continuity
            rows.append(("l2", {**common, "side": None, "level": None, "price": None, "size": None, "n_orders": None}))
        return "l2", rows

    if e == "aggTrade":
        return "trades", [("trades", {**_base(frame, d.get("s")), "ts_exch_ns": ms_to_ns(d.get("E")), "ts_trade_ns": ms_to_ns(d.get("T")),
                                       "trade_id": str(d.get("a")), "first_trade_id": d.get("f"), "last_trade_id": d.get("l"),
                                       "price": f(d.get("p")), "size": f(d.get("q")),
                                       "aggressor": "sell" if d.get("m") else "buy", "hash": None})]

    if "result" in msg and "id" in msg:
        return "ack", [("events", {**_base(frame, None), "kind": "ack", "detail": frame.payload[:500]})]
    if "code" in msg and "msg" in msg:
        return "venue_error", [("events", {**_base(frame, None), "kind": "venue_error", "detail": frame.payload[:500]})]
    return "unknown", [("events", {**_base(frame, d.get("s")), "kind": "unknown", "detail": frame.payload[:500]})]
