---
venue: BINANCE_FUTURES
cryptofeed_id: BINANCE_FUTURES
cryptofeed_version_checked: 3.0.1
last_reviewed: 2026-10-14
---

# Binance USD-M futures: venue profile

Source tags: **[docs]** official Binance docs, **[cf]** cryptofeed source, **[3p]** reputable third
party, **[probe]** measured (report linked), **[UNVERIFIED]**.

Docs: https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/How-to-manage-a-local-order-book-correctly

## Streams

| our channel | wire | delivery | sequence | source |
|---|---|---|---|---|
| l1 | `<sym>@bookTicker` | every top-of-book change (real-time) | `u` update id | delivery [UNVERIFIED in official docs, confirm in probe]; fields [cf] |
| l2 | `<sym>@depth@100ms` | diff depth, conflated to 100 ms | `U`, `u`, `pu` | [docs][cf] |
| trades | `<sym>@aggTrade` | **aggregated**: fills from one taker order at one price = one row | `a` agg id; `f`..`l` raw trade ids | [cf] |

- cryptofeed's `L2_BOOK` for futures allows only `depth_interval` in {100ms, 250ms, 500ms}; the
  class default is 250ms. We set 100ms. [cf]
- `@depth@0ms` ("real-time dynamically adjusted") is used by some collectors [3p: Tardis docs], but
  cryptofeed can't subscribe to it without a custom feed. Open question: worth it for jobbing?
- cryptofeed's normalized `TICKER` object for bookTicker **drops bid/ask sizes**. We parse sizes from
  the raw frame, which is one reason raw capture is the source of truth. [cf]
- cryptofeed subscribes via the combined endpoint `/stream?streams=a/b/c`; payloads arrive as
  `{"stream": ..., "data": {...}}`. Max 200 streams per connection, chunked automatically. [cf]

## Local order book (diff depth) [docs]
1. Buffer `depth` events. 2. GET `/fapi/v1/depth?symbol=X&limit=1000` -> `lastUpdateId`.
3. Drop events with `u < lastUpdateId`. 4. First event: `U <= lastUpdateId <= u`.
5. Every later event: **`pu` == previous event's `u`**, otherwise re-snapshot.
6. Quantities are absolute; 0 = remove the level.
cryptofeed does this internally and records the REST snapshot in raw capture. Our `l2` table
keeps U/u/pu so research can rebuild the book and see every resync. [cf]

## Timestamps
- `E` event time, `T` transaction time (ms), on bookTicker, depthUpdate and aggTrade. We keep both:
  `ts_exch_ns` = E, `ts_txn_ns` / `ts_trade_ns` = T. [cf payload examples]
- Receive time: cryptofeed `time.time()` at frame read. [cf]

## Trade side
- `m = true` means the buyer is the maker, so the aggressor is SELL. [cf]

## What this means for market-making research
- bookTicker is the unconflated top of book. depth@100ms can't show sub-100ms queue dynamics.
- aggTrade undercounts fill events, because partial fills of one taker order at one price merge.
  The raw `@trade` stream would fix this; cryptofeed's TRADES doesn't use it. Open question.

## Probe history
| date | report | verdict | notes |
|---|---|---|---|
| - | - | - | not yet probed |
