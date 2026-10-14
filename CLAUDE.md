# CLAUDE.md: md-capture

Mark's market-data capture system for crypto microstructure / market-making research
(cross-venue fair value, jobbing, lead-lag). Built on cryptofeed. It runs 24/7 on a GCP VM and
gets maintained over time, mostly through the slash commands below.

## Why this exists (read once)

The previous collector produced data that looked fine and wasn't (see `incidents/0001-*`):
- Hyperliquid book updates arrived on a ~5.4 s timer. A spectral analysis "found" periodic
  traders that were really our own poller.
- Only derived top-of-book features were stored (mid, spread, microprice), so depth, sizes,
  order counts and update ids were gone for good.
- A lossy in-memory queue, `remove_feed` on error, date partitions taken from a batch's first
  row, an unexplained hourly stall, and three delivery layers with no separate freshness checks.

Every rule below is there to stop one of those happening again.

## Principles (the market-making mindset)

1. **Raw is the source of truth.** Every frame each venue sends is recorded byte-for-byte with its
   receive timestamp (cryptofeed PcapRecorder). Normalized tables are derived and can always be
   rebuilt. Agents never edit, move or delete raw data.
2. **Know each stream's clock.** For every stream, the venue profile says whether it's
   event-driven, conflated (and at what interval), snapshot, or polled, and cites a source. The
   cadence audit checks the data against that. A stream nobody can explain doesn't go to production.
3. **Prefer the unconflated stream.** Top of book: bookTicker/bbo over depth snapshots. If the
   only option is conflated or polled, the profile says so in bold and research gets told.
4. **Loss is loud.** No drop-on-full queues. A feed that fails crashes the process (systemd
   restarts it). The watchdog exits on a stale stream. Sequence gaps are counted, not smoothed over.
5. **Keep every field; derive nothing.** Sizes, order counts, update/sequence ids, both venue
   timestamps. No mid, microprice, imbalance or resampling in this repo. That's research code's job.
6. **Two clocks, never confused.** `ts_recv_ns` (ours, when we read the frame) and `ts_exch_ns` /
   `ts_txn_ns` (theirs). Partition by each row's own `ts_recv_ns`. Keep chrony synced and
   measure loop lag, because that's how wrong `ts_recv_ns` can be.
7. **The collector does the minimum.** Normalizing, delivering and auditing run as separate,
   low-priority jobs that can't block the event loop.
8. **Freshness is measured per stage.** Raw written, normalized, delivered and pulled locally are
   different facts. Never infer one from another.

## Who decides what

| Mark | Agents |
|---|---|
| Which markets, and the research question each serves (`purpose`) | Verifying venue facts, with sources |
| What resolution the research needs (sub-second L1? full depth?) | Implementation, parsers, tests |
| Promoting probe -> active; accepting a WARN (`accepted_warnings`) | Running probes and audits, explaining findings |
| Restarting the production collector; deleting or pruning raw | Writing exact commands for Mark to run |
| Storage/cost choices (remote, retention) | Incident forensics and regression tests |

Never set `status: active`, write `accepted_warnings`, or write a `purpose` in your own words.
Transcribe Mark's words. Hooks block restarts of the prod collector and destructive raw operations.

## Workflows

- `/add-market VENUE SYMBOL <what it's for>`: registry check -> markets.yaml (probe) -> probe on
  the VM -> forensics review -> Mark promotes.
- `/add-venue VENUE`: venue-researcher writes the sourced profile -> collector-engineer adds the
  registry entry, parser and real-frame fixtures -> then `/add-market`.
- `/health`: freshness per stage + 24 h audit + heartbeat review. Read-only.
- `/incident <symptom>`: forensics from research -> normalized -> raw -> heartbeat. Reproduce
  with `mdcap synth --fault`, fix, regression test, postmortem in `incidents/`.
- `/change <what>`: any other maintenance. Plan -> data-semantics impact -> tests -> probe -> Mark deploys.

## Commands

```
python -m mdcap validate | inventory | freshness
python -m mdcap probe VENUE:SYMBOL --minutes 30      # on the VM (needs exchange network access)
python -m mdcap normalize                            # closed raw files only
python -m mdcap audit --hours 24                     # exit 0 PASS / 1 WARN / 2 FAIL
python -m mdcap synth out.pcap.zst --fault hl_poll_s=5.0 stall_every_s=3600
python -m pytest -q
```

## Repo map

```
mdcap/venues.py        registry: streams, expected cadence, sequence availability, watchdog limits
mdcap/config.py        markets.yaml / settings.yaml loading + strict validation
mdcap/collector.py     live process: raw recorder + counters + heartbeat + watchdog
mdcap/rawio.py         read raw captures; which files are closed
mdcap/parsers/<v>.py   raw frame -> rows, every field kept, nothing dropped
mdcap/normalize.py     raw -> hive-partitioned parquet + _manifest.jsonl reconciliation
mdcap/audits.py        cadence, stalls, duplicates, sequence, latency, completeness, partitions, freshness
mdcap/report.py        audit orchestration + markdown report (first line "Verdict: ...")
mdcap/synth.py         synthetic captures in real PCAP format with plantable faults
venues/<V>.md          venue profiles: every claim tagged [docs]/[cf]/[probe]/[3p]/[UNVERIFIED]
config/                markets.yaml (what), settings.yaml (where)
ops/                   systemd units, deliver.sh, pull_local.sh, prune_raw.sh, install.sh
reports/probes|health  audit outputs (committed: they're the evidence for promotions)
incidents/             postmortems; each one has a regression test in tests/
```

## Conventions

- When behaviour depends on cryptofeed, read the **installed** adapter source
  (`python -c "import cryptofeed, os; print(os.path.dirname(cryptofeed.__file__))"`). Don't trust
  memory, and record the version you checked in the venue profile.
- New parser => fixtures of REAL frames from a probe in `tests/fixtures/<venue>/`, plus a test
  asserting every field survives.
- New failure mode => a `synth` fault that reproduces it, plus a test showing an audit catches it.
- Changing what a normalized column means => bump `__version__`, note it in CHANGELOG.md, and say
  whether existing normalized data must be rebuilt from raw.
- No employer data, configs, credentials or venue fee schedules in this repo. Public feeds only.
- Style: direct. Lead with what's broken or uncertain. "Unverified" is a fine answer; a guess
  presented as fact is not.
