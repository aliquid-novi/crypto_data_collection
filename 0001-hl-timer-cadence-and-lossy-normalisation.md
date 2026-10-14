# 0001: Hyperliquid ~5.4 s timer cadence, lossy L1-only storage (previous collector)

**Status:** root cause partly unconfirmed (the old VM's deployed code was never retrieved).
**System:** the pre-md-capture collector (`market_data.py`, 27 Sep 2026, deployed on the GCP VM).
**Found:** 13 Oct 2026, while running an FFT/periodicity analysis of HYPE quotes (HL vs Binance).

## Symptom
- The periodogram showed a sharp ~5 s comb (and harmonics), which looked like a scheduled trader.
- 80% of Hyperliquid update gaps fell between 5.04 and 5.53 s (median 5.38 s). 296,748 HL events
  over 247.7 h is one every ~3 s, on HYPE, HL's flagship perp.
- A max gap of exactly 3600.07 s, and ~9,000 zero-gap duplicate rows.

## What it actually was
1. **The ~5 s comb was our capture, not the market.** A tight gap cluster just above a round
   number is the signature of `sleep(5)` plus processing time. HL's documented `l2Book` cadence is
   >= 0.5 s per block and `bbo` is per block on change ([docs], see `venues/HYPERLIQUID.md`), and
   cryptofeed 3.0.1's Hyperliquid adapter is WebSocket-based, not polling ([cf]). So the timer was
   most likely in the old deployment's own HL integration. **Unconfirmed:** the deployed code was
   never compared with the saved source.
2. **Storage wasn't raw.** Only L1-derived features were written (mid, spread, spread_bps,
   imbalance, microprice). Depth, order counts, update ids and the raw messages were discarded, so
   none of it can be recovered. Optional PCAP recording existed but was off.
3. **Loss could be silent.** A bounded asyncio queue dropped rows when full (counter in memory
   only). `on_feed_error="remove_feed"` could drop a whole feed while the service still showed "running".
4. **Partitions came from the first row of a batch**, so rows near midnight could land in the wrong day.
5. **An hourly stall** (3600.07 s gap). Never diagnosed. Candidates: token refresh, a timer job, or
   rclone/compaction blocking the host.
6. **Duplicate bursts** (zero-gap identical rows), consistent with resubscribe snapshot replays.
7. (Research side, not the collector) An outer join with `fillna(0)` produced ±86 "spreads" and a
   fake event series. Recorded here because the collector's lack of explicit per-venue update
   flags made that mistake easy.

## Fixes in md-capture
| failure | prevention | test |
|---|---|---|
| timer cadence passes as market | cadence audit vs venue profile -> TIMER_LIKE = FAIL; probe gate before production | `test_timer_like_stream_fails` |
| derived-only storage | raw PCAP of every frame is the source of truth; parsers keep all fields | `test_l1_keeps_sizes_and_ids`, `test_every_frame_accounted_for` |
| silent queue/feed loss | no queue; `on_feed_error='raise'`; stale-stream watchdog exits -> systemd restart; rawness scan blocks `put_nowait`/`remove_feed` | `test_rawness_scan_flags_old_patterns` |
| first-row partitioning | partition by each row's own receive hour; partition audit | `test_rows_partitioned_by_own_hour` |
| hourly stall | process-stall audit (all streams silent together) + recurrence detection; loop-lag heartbeat; batch jobs at :07/:22/:41, low priority | `test_recurring_process_stall_detected` |
| duplicate bursts | duplicate audits (L1 consecutive states, repeated snapshots) | `test_duplicate_burst_detected` |
| research zero-fill | rawness scan blocks `fillna(0)` in the capture layer; research side handled in qr-harness | - |

## Research to re-run
Anything built on HL data from the old collector below ~10 s resolution: the FFT/periodicity
analysis, HL microprice/imbalance features, and the spread cost estimates.
