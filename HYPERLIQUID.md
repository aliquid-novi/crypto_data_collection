---
venue: HYPERLIQUID
cryptofeed_id: HYPERLIQUID
cryptofeed_version_checked: 3.0.1
last_reviewed: 2026-10-14
---

# Hyperliquid: venue profile

Every claim has a source tag: **[docs]** official docs, **[cf]** cryptofeed source,
**[probe]** measured by `mdcap probe` (report linked), or **[UNVERIFIED]**.
The cadence audit's expectations in `mdcap/venues.py` must match this file.

Docs: https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket/subscriptions

## Streams

| our channel | wire | delivery | depth | sequence | source |
|---|---|---|---|---|---|
| l1 | `bbo` | pushed only when the bbo changes on a block | 1 | none | [docs] |
| l2 | `l2Book` | full **snapshot**, pushed on each block that is >= 0.5 s since the last push | 20 levels (5 with `fast`) | none | [docs] |
| trades | `trades` | array of trades per push | - | none (tid is a hash) | [docs] |

- `l2Book` optional params: `nSigFigs`, `mantissa` (price aggregation), `fast` (5 levels). Not used here;
  aggregation would hide the real tick grid. [docs]
- cryptofeed subscribes to `{type, coin}` with no extra params, and refuses `max_depth` > 20. [cf]
- Level objects carry `n` = number of orders at that level. We keep it. [docs][cf]

## Timestamps
- `time` field on bbo/l2Book/trades: milliseconds. Its exact meaning isn't documented; treat it as
  block time. [docs: field only] [UNVERIFIED: semantics]
- Receive time: cryptofeed `time.time()` when the frame is read. [cf]

## Sequencing and gaps
- **No sequence numbers on any public feed.** Missed messages can't be detected except by timing. [docs]
- Trade `tid` = "50-bit hash of (buyer_oid, seller_oid)". Unique key is `(time, coin, tid)`. [docs]
- On subscribe, time-series feeds may send an `isSnapshot: true` replay, which "can be ignored if
  the previous messages were already processed". Expect duplicates after every reconnect. [docs]

## Trade side
- cryptofeed maps `side == 'B'` -> BUY. Whether `side` is the aggressor or the resting side is
  **[UNVERIFIED]**. Check against bbo moves in a probe before using it for order-flow work.

## What this means for market-making research
- Top of book is block-granular, not tick-granular. L2 is a 0.5 s+ snapshot, so you can't see
  queue changes between snapshots. Sub-second HL book dynamics come from `bbo`, not `l2Book`.
- If any HL stream's gaps cluster tightly at one period well above 0.5 s, it's NOT the venue.
  That was the old system: a ~5.4 s cadence from a timer on our side.

## Probe history
| date | report | verdict | notes |
|---|---|---|---|
| - | - | - | not yet probed |
