"""Per-venue raw-frame parsers.

Contract for every parser module:

    parse(frame: Frame) -> (kind, rows)

kind  - what the frame was: 'l1' | 'l2' | 'trades' | 'rest_snapshot' | 'rest_meta'
        | 'ack' | 'pong' | 'venue_error' | 'unknown'
rows  - list of (table, row_dict) with the columns in SCHEMAS (missing -> null)

Rules (enforced by tests and the rawness scan):
* Keep every field the venue sends that a market maker could care about:
  sizes, order counts, update ids, sequence ids, BOTH venue timestamps.
* Never compute derived quantities (mid, spread, microprice, imbalance).
* Never drop a frame. Anything not data becomes a row in `events`, so
  reconciliation can prove frames-in == rows-accounted-for.
* An empty book side is a row with null price/size, not a skipped row.
"""

from __future__ import annotations

import importlib

# capture = recording session (raw file base name): conn ids restart at 1 after a collector restart,
# so (capture, conn_id, conn_ordinal) is the unique connection-session key.
COMMON = ["venue", "symbol", "venue_symbol", "ts_recv_ns", "capture", "conn_id", "conn_ordinal", "frame_idx"]

SCHEMAS: dict[str, list[str]] = {
    "l1": COMMON + ["ts_exch_ns", "ts_txn_ns", "update_id", "bid_px", "bid_sz", "ask_px", "ask_sz", "bid_n", "ask_n"],
    "l2": COMMON + ["ts_exch_ns", "ts_txn_ns", "kind", "first_id", "last_id", "prev_id", "side", "level", "price", "size", "n_orders"],
    "trades": COMMON + ["ts_exch_ns", "ts_trade_ns", "trade_id", "first_trade_id", "last_trade_id", "price", "size", "aggressor", "hash"],
    "events": COMMON + ["kind", "detail"],
}

DATA_KINDS = {"l1", "l2", "trades", "rest_snapshot"}


def get(name: str):
    return importlib.import_module(f"{__name__}.{name}")


def ms_to_ns(x) -> int | None:
    return None if x is None else int(x) * 1_000_000


def f(x) -> float | None:
    return None if x is None else float(x)
