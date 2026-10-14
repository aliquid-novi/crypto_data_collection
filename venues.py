"""Venue registry: what this repo knows how to capture, and what each stream
is SUPPOSED to look like.

`expected` is the contract the cadence audit checks against. It comes from the
venue profile in venues/<VENUE>.md, where every claim has a source (official
docs, cryptofeed source, or a probe measurement). If you change an expectation
here, change the profile and its source in the same commit.

Modes:
  event      pushed on every change (inter-arrival roughly exponential, CV ~>= 1)
  conflated  at most one push per `interval_ms`, on change (gaps cluster at
             multiples of the interval; min gap ~= interval on the venue clock)
  block      pushed per chain block when changed (gaps cluster at block-time
             multiples; treat like conflated with a soft interval)

A stream whose measured cadence is TIMER_LIKE (tight cluster at one round
period, not explained by its expected mode) is the failure the old system had:
something is polling.
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class Stream:
    cryptofeed_channel: str  # name in cryptofeed.defines
    venue_channel: str  # what goes over the wire
    table: str  # normalized table it parses into
    expected_mode: str  # event | conflated | block
    interval_ms: float | None = None
    has_sequence: bool = False
    stale_after_s: float = 60.0  # watchdog: silence longer than this kills the process (loud restart)
    notes: str = ""


@dataclass(frozen=True)
class Venue:
    cryptofeed_id: str
    feed_class: str  # import path under cryptofeed.exchanges
    parser: str  # module under mdcap.parsers
    profile: str  # venues/<file>.md
    streams: dict[str, Stream] = field(default_factory=dict)
    feed_kwargs: dict = field(default_factory=dict)


VENUES: dict[str, Venue] = {
    "BINANCE_FUTURES": Venue(
        cryptofeed_id="BINANCE_FUTURES",
        feed_class="cryptofeed.exchanges.BinanceFutures",
        parser="binance_futures",
        profile="venues/BINANCE_FUTURES.md",
        feed_kwargs={"depth_interval": "100ms"},  # fastest cryptofeed allows; @depth@0ms needs a custom subscription
        streams={
            "l1": Stream("TICKER", "bookTicker", "l1", "event", has_sequence=True, stale_after_s=30,
                         notes="real-time per docs/community (UNVERIFIED - confirm in probe); carries u, E, T and sizes"),
            "l2": Stream("L2_BOOK", "depth@100ms", "l2", "conflated", interval_ms=100, has_sequence=True, stale_after_s=30,
                         notes="diff depth with U/u/pu; REST snapshot anchors via lastUpdateId"),
            "trades": Stream("TRADES", "aggTrade", "trades", "event", has_sequence=True, stale_after_s=120,
                             notes="AGGREGATED trades (one taker order at one price = one row); f..l are raw trade ids"),
        },
    ),
    "HYPERLIQUID": Venue(
        cryptofeed_id="HYPERLIQUID",
        feed_class="cryptofeed.exchanges.Hyperliquid",
        parser="hyperliquid",
        profile="venues/HYPERLIQUID.md",
        streams={
            "l1": Stream("L1_BOOK", "bbo", "l1", "block", interval_ms=None, stale_after_s=60,
                         notes="docs: sent only if the bbo changes on a block"),
            "l2": Stream("L2_BOOK", "l2Book", "l2", "conflated", interval_ms=500, stale_after_s=60,
                         notes="docs: snapshot, pushed on each block >= 0.5s since last push; 20 levels; no sequence"),
            "trades": Stream("TRADES", "trades", "trades", "event", stale_after_s=300,
                             notes="tid is a hash, not a sequence; unique key is (time, coin, tid)"),
        },
    ),
}


def venue(name: str) -> Venue:
    try:
        return VENUES[name]
    except KeyError:
        raise KeyError(f"Unknown venue {name!r}. Known: {sorted(VENUES)}. Adding one is /add-venue.") from None


def by_cryptofeed_id(cf_id: str) -> tuple[str, Venue]:
    for k, v in VENUES.items():
        if v.cryptofeed_id == cf_id:
            return k, v
    raise KeyError(cf_id)
