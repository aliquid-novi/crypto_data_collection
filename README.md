# md-capture

Market-data capture for crypto market-making research, built on cryptofeed 3.0.1 and maintained
by Claude Code agents. It replaces the previous collector, whose data looked fine and wasn't
(see `incidents/0001-*`).

## What's different, and why

| Old collector | md-capture |
|---|---|
| Stored derived L1 features (mid, microprice...). Depth, sizes, ids lost forever | **Raw first**: every WebSocket frame and REST response recorded byte-for-byte (cryptofeed PCAP). Normalized tables are derived and rebuildable |
| HL arrived on a ~5.4 s timer and nobody knew | Every stream has a sourced **venue profile** (event / conflated / snapshot) and a **cadence audit** that fails TIMER_LIKE streams |
| Markets went straight to production | **Probe gate**: 30-min probe -> audit -> report. A market can't be `active` without a PASS (or a WARN Mark explicitly accepts) |
| Lossy queue, `remove_feed` | No queue, `on_feed_error='raise'`, stale-stream **watchdog** -> systemd restart. Loss is loud |
| Partition by first row of batch | Partition by each row's own receive hour; audited |
| Hourly 3600.07 s stall, undiagnosed | **Process-stall audit** (all streams silent at once, with recurrence detection), **event-loop lag** in heartbeats, batch jobs offset from the hour and run at low priority |
| One freshness question for 3 layers | Freshness **per stage**: raw, normalized, heartbeat, delivered, local |
| Drive OAuth expiring | rclone remote configurable; a GCS bucket + service account is recommended (no token expiry) |

## Layout

```
L0 raw         data/raw/prod/capture-<UTC>.pcap.zst (+ .meta.json)   <- source of truth, rotated at 128 MB
L1 normalized  data/normalized/table={l1,l2,trades,events,sessions}/venue=/symbol=/date=/hour=/*.parquet
health         data/health/heartbeat-{prod,probe}-YYYYMMDD.jsonl, delivered.json
reports        reports/probes/*.md (promotion evidence), reports/health/*.md
```

Normalized tables keep every venue field: `l1` (bid/ask px+sz, order counts, update id, venue
event+transaction time), `l2` (Binance diff deltas with U/u/pu + REST anchors; HL 20-level
snapshots with order counts), `trades` (ids, aggressor, both venue times), `events` (acks,
errors, unknown frames: nothing dropped), `sessions` (every connect/reconnect). Two clocks on
every row: `ts_recv_ns` (ours) and `ts_exch_ns` (theirs).

## Setup (VM)

```bash
sudo mkdir -p /opt/md-capture && sudo chown $USER /opt/md-capture
git clone <this repo> /opt/md-capture && cd /opt/md-capture
sudo ops/install.sh $USER          # chrony, rclone, venv, systemd templates
rclone config                      # create the remote named in config/settings.yaml (GCS: service account JSON)
.venv/bin/python -m pytest -q      # 36 tests: each old failure reproduced and caught
.venv/bin/python -m mdcap validate
```

Then, in Claude Code from the repo dir: `/add-market HYPERLIQUID HYPE-USD-PERP ...`. Both HYPE
markets are already in `config/markets.yaml` as `probe`, so the first thing to run is:

```bash
.venv/bin/python -m mdcap probe HYPERLIQUID:HYPE-USD-PERP BINANCE_FUTURES:HYPE-USDT-PERP --minutes 30
```

Read the report. If you promote them (`status: active`, `probe_report: <path>`), start production:

```bash
sudo systemctl enable --now mdcap-collector@$USER mdcap-normalize@$USER.timer mdcap-deliver@$USER.timer mdcap-audit@$USER.timer
python -m mdcap freshness
```

Run the old collector in parallel for a few days, then stop it once `/health` is consistently PASS.

## On your desktop

```bash
ops/pull_local.sh gcs_md:YOUR-BUCKET/md-capture ~/market-data/md-capture
```

Reading it in research (or point `qr-harness/config/data.yaml` at it):

```python
from mdcap.normalize import load
l1 = load("~/market-data/md-capture/normalized", "l1", "HYPERLIQUID", "HYPE-USD-PERP", start="2026-10-15", end="2026-10-16")
```

`qr-harness/config/data.yaml` mapping:
```yaml
data_root: ~/market-data/md-capture/normalized
quotes:
  glob: "table=l1/venue={exchange}/symbol={symbol}/**/*.parquet"
  time_unit: ns
  columns: {ts_exch: ts_exch_ns, ts_recv: ts_recv_ns, bid_px: bid_px, bid_sz: bid_sz, ask_px: ask_px, ask_sz: ask_sz}
trades:
  glob: "table=trades/venue={exchange}/symbol={symbol}/**/*.parquet"
  time_unit: ns
  columns: {ts_exch: ts_trade_ns, ts_recv: ts_recv_ns, price: price, size: size, side: aggressor}
```
(Venue and symbol live in the path, not the files. Use the market's venue/symbol names from `markets.yaml`.)

## Maintaining it (Claude Code)

| You want to | Run |
|---|---|
| collect a new market | `/add-market VENUE SYMBOL <what it's for>` |
| support a new exchange | `/add-venue VENUE` |
| know if everything's working | `/health` |
| understand something weird in the data | `/incident <symptom>` |
| change anything else | `/change <what>` |

The agents verify venue facts with sources, write code with tests, run probes and audits, and
investigate incidents. **You** decide which markets and why, promote probes to production, restart
the production collector, and delete raw data. Hooks enforce that split.

## Known limitations (read before trusting a number)

- **Not yet run against live exchanges.** Everything here is tested offline, with synthetic
  captures in cryptofeed's real PCAP format (including the full probe gate). The first probe on
  the VM is the real test. Expect to update the venue profiles from it.
- **Receive timestamps are userland**: `time.time()` when cryptofeed reads the frame, after event-loop
  scheduling. Not kernel or NIC timestamps. Loop lag in the heartbeat bounds the error. Fine for
  ms-scale lead-lag, not for microsecond work.
- **PcapRecorder writes inside the event loop** (flushes every 5 s). If loop lag shows a 5 s rhythm,
  that's the cause. Then move recording off the loop (a `/change`).
- **Hyperliquid has no sequence numbers.** Missed messages are invisible except by timing. HL depth is
  a >= 0.5 s snapshot.
- **Binance**: depth is conflated at 100 ms (cryptofeed's fastest option; `@depth@0ms` would need a
  custom subscription). Trades are `aggTrade` (aggregated). bookTicker "real-time" is not yet
  confirmed from official docs; the probe will show it.
- **Disk/cost**: unknown until the first probe. The probe report plus the raw file sizes give the
  daily volume. Decide retention (`ops/prune_raw.sh` only deletes files verified on the remote).
- One collector process for all markets: a crash restarts every stream (seconds of gap, recorded in
  `sessions` and heartbeats). Split by venue later if that matters.
