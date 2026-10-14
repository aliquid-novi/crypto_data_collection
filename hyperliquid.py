"""Hyperliquid raw frames -> normalized rows.

Wire formats (docs: hyperliquid-docs .../websocket/subscriptions):

bbo      {channel: 'bbo', data: {coin, time, bbo: [WsLevel|null, WsLevel|null]}}   WsLevel = {px, sz, n}
         "sent only if the bbo changes on a block"
l2Book   {channel: 'l2Book', data: {coin, time, levels: [[bids...], [asks...]]}}
         "snapshot feed, pushed on each block that is at least 0.5 since last push"; 20 levels (5 if fast)
trades   {channel: 'trades', data: [{coin, side, px, sz, hash, time, tid, users}]}
         tid = 50-bit hash of (buyer_oid, seller_oid): NOT a sequence. Unique key (time, coin, tid).

No sequence numbers on any public feed: gaps can only be inferred from time.
"""

from __future__ import annotations

import json

from . import f, ms_to_ns

VENUE = "HYPERLIQUID"


def norm_symbol(coin: str) -> str:
    if coin and ("/" in coin or coin.startswith("@") or ":" in coin):
        return coin  # spot pair or builder-deployed market: keep venue spelling
    return f"{coin}-USD-PERP"


def _base(frame, coin):
    return {
        "venue": VENUE,
        "symbol": norm_symbol(coin) if coin else None,
        "venue_symbol": coin,
        "ts_recv_ns": frame.ts_ns,
        "conn_id": frame.conn_id,
        "conn_ordinal": frame.ordinal,
    }


def _lvl(x, k):
    return None if x is None else x.get(k)


def parse(frame):
    if frame.kind == "http":
        return "rest_meta", [("events", {**_base(frame, None), "kind": "rest_meta", "detail": frame.url[:200]})]
    msg = json.loads(frame.payload)
    ch = msg.get("channel") if isinstance(msg, dict) else None
    d = msg.get("data") if isinstance(msg, dict) else None

    if ch == "bbo":
        bid, ask = (d.get("bbo") or [None, None])[:2]
        return "l1", [("l1", {**_base(frame, d.get("coin")), "ts_exch_ns": ms_to_ns(d.get("time")), "ts_txn_ns": None, "update_id": None,
                               "bid_px": f(_lvl(bid, "px")), "bid_sz": f(_lvl(bid, "sz")), "bid_n": _lvl(bid, "n"),
                               "ask_px": f(_lvl(ask, "px")), "ask_sz": f(_lvl(ask, "sz")), "ask_n": _lvl(ask, "n")})]

    if ch == "l2Book":
        b = _base(frame, d.get("coin"))
        common = {**b, "ts_exch_ns": ms_to_ns(d.get("time")), "ts_txn_ns": None, "kind": "snapshot",
                  "first_id": None, "last_id": None, "prev_id": None}
        rows = []
        levels = d.get("levels") or [[], []]
        for side, lv in zip(("bid", "ask"), levels):
            for i, x in enumerate(lv):
                rows.append(("l2", {**common, "side": side, "level": i, "price": f(x.get("px")), "size": f(x.get("sz")), "n_orders": x.get("n")}))
        if not rows:
            rows.append(("l2", {**common, "side": None, "level": None, "price": None, "size": None, "n_orders": None}))
        return "l2", rows

    if ch == "trades":
        rows = []
        for t in d or []:
            rows.append(("trades", {**_base(frame, t.get("coin")), "ts_exch_ns": ms_to_ns(t.get("time")), "ts_trade_ns": ms_to_ns(t.get("time")),
                                    "trade_id": str(t.get("tid")), "first_trade_id": None, "last_trade_id": None,
                                    "price": f(t.get("px")), "size": f(t.get("sz")),
                                    # ASSUMPTION (venue profile, unverified): 'B' = buy-side aggressor, as cryptofeed maps it
                                    "aggressor": {"B": "buy", "A": "sell"}.get(t.get("side")), "hash": t.get("hash")}))
        if not rows:
            rows.append(("events", {**_base(frame, None), "kind": "empty_trades", "detail": frame.payload[:200]}))
        return "trades", rows

    kind = {"subscriptionResponse": "ack", "pong": "pong", "error": "venue_error"}.get(ch, "unknown")
    return kind, [("events", {**_base(frame, None), "kind": kind, "detail": frame.payload[:500]})]
