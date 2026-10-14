"""python -m mdcap <command>

  validate                         check config/markets.yaml (exit 1 on errors)
  inventory                        what's configured, with each stream's expected cadence
  run [--mode prod]                production collector (active markets only)
  probe VENUE:SYMBOL ... [--minutes 30] [--channels l1 l2 trades]
                                   capture -> normalize -> audit -> reports/probes/*.md
  normalize [--min-idle 600]       normalize closed raw captures not yet in the manifest
  audit [--hours 24]               audit active markets; exit 0 PASS / 1 WARN / 2 FAIL
  freshness                        newest data at each stage
  synth OUT.pcap.zst [--fault ...] write a synthetic capture (reproduce incidents)
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import sys
import time
from pathlib import Path

import pandas as pd

from . import config, normalize, report
from .config import Market
from .rawio import closed_files


def _hb(settings, hours=None, mode="prod") -> pd.DataFrame:
    rows = []
    for p in sorted(Path(settings.health_dir).glob(f"heartbeat-{mode}-*.jsonl"))[-3:]:
        for line in p.read_text().splitlines():
            if line.strip():
                rows.append(json.loads(line))
    df = pd.DataFrame(rows)
    if hours and not df.empty:
        df = df[df["ts"] >= time.time() - hours * 3600]
    return df


def cmd_validate(a, s):
    import yaml

    _, errors = config.validate(yaml.safe_load(config.MARKETS.read_text()) or {})
    if errors:
        print("INVALID:\n  " + "\n  ".join(errors))
        return 1
    print("markets.yaml OK")
    return 0


def cmd_inventory(a, s):
    from .venues import venue

    for m in config.load_markets(strict=False):
        print(f"{m.status:8} {m.key:40} {','.join(m.channels):18} {m.purpose[:60]}")
        for ch in m.channels:
            st = venue(m.venue).streams[ch]
            iv = f" every {st.interval_ms:.0f}ms" if st.interval_ms else ""
            print(f"           {ch:7} {st.venue_channel:14} {st.expected_mode}{iv}  seq={'yes' if st.has_sequence else 'NO'}  {st.notes[:70]}")
    return 0


def cmd_run(a, s):
    from . import collector

    markets = config.load_markets(status={"active"})
    collector.main(markets, s, mode=a.mode)


def cmd_probe(a, s):
    from . import collector

    all_m = {m.key: m for m in config.load_markets(strict=False)}
    chosen = []
    for k in a.markets:
        if k not in all_m:
            print(f"{k} is not in markets.yaml - add it with status: probe first")
            return 1
        m = all_m[k]
        if a.channels:
            m = Market(**{**vars(m), "channels": a.channels})
        chosen.append(m)
    stamp = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    probe_settings = config.Settings(**{**vars(s), "raw_dir": s.raw_dir, "norm_dir": s.norm_dir.parent / f"probe_{stamp}"})
    before = set((s.raw_dir / "probe").glob("*.pcap*")) if (s.raw_dir / "probe").exists() else set()
    print(f"probing {len(chosen)} market(s) for {a.minutes} min ...")
    import asyncio

    code = asyncio.run(collector.run(chosen, s, mode="probe", duration_s=a.minutes * 60))
    new = sorted(set((s.raw_dir / "probe").glob("*.pcap*")) - before)
    for f in new:
        normalize.normalize_file(f, probe_settings.norm_dir)
    hb = _hb(s, hours=a.minutes / 60 + 0.1, mode="probe")
    rep = report.run(probe_settings, chosen, heartbeats=hb)
    out = s.reports_dir / "probes" / f"{stamp}_{'_'.join(m.key.replace(':', '-') for m in chosen)}.md"
    out.parent.mkdir(parents=True, exist_ok=True)
    md = report.to_markdown(rep, f"Probe {', '.join(m.key for m in chosen)} ({a.minutes} min)")
    if code:
        md = md.replace(f"Verdict: {rep['verdict']}", f"Verdict: FAIL (collector exited {code})", 1)
    out.write_text(md)
    print(md.split("<details>")[0])
    print(f"\nreport: {out.relative_to(config.ROOT) if out.is_relative_to(config.ROOT) else out}")
    return 0 if rep["verdict"] == "PASS" and not code else 2


def cmd_normalize(a, s):
    todo = normalize.pending(closed_files(s.raw_dir / a.mode, min_idle_s=a.min_idle), s.norm_dir)
    for f in todo:
        rec = normalize.normalize_file(f, s.norm_dir)
        print(f"{f.name}: {rec['frames']:,} frames, rows {rec['rows']}, errors {rec['parse_errors']}")
    if not todo:
        print("nothing to normalize")
    return 0


def cmd_audit(a, s):
    markets = config.load_markets(status={"active"}, strict=False)
    end = pd.Timestamp.now(tz="UTC")
    start = end - pd.Timedelta(hours=a.hours)
    rep = report.run(s, markets, start.tz_localize(None), end.tz_localize(None), heartbeats=_hb(s, a.hours))
    md = report.to_markdown(rep, f"Health audit, last {a.hours}h")
    out = s.reports_dir / "health" / f"{end:%Y%m%dT%H%M}Z.md"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(md)
    print(md.split("<details>")[0])
    return {"PASS": 0, "WARN": 1}.get(rep["verdict"], 2)


def cmd_freshness(a, s):
    from .audits import freshness

    for stage, v in freshness(s).items():
        ts = "never" if v["ts"] is None else dt.datetime.fromtimestamp(v["ts"], dt.timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        lag = "" if v["lag_s"] is None else f"  ({v['lag_s'] / 60:.1f} min ago)"
        print(f"{stage:26} {ts}{lag}")
    return 0


def cmd_synth(a, s):
    from . import synth

    kw = {}
    for f in a.fault or []:
        k, _, v = f.partition("=")
        kw[k] = float(v) if v.replace(".", "", 1).isdigit() and "." in v else int(v) if v.isdigit() else v
    print(synth.write_capture(a.out, dur_s=a.minutes * 60, **kw))
    return 0


def main(argv=None):
    ap = argparse.ArgumentParser(prog="mdcap")
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("validate")
    sub.add_parser("inventory")
    r = sub.add_parser("run")
    r.add_argument("--mode", default="prod")
    p = sub.add_parser("probe")
    p.add_argument("markets", nargs="+", help="VENUE:SYMBOL as in markets.yaml")
    p.add_argument("--minutes", type=float, default=30)
    p.add_argument("--channels", nargs="*")
    n = sub.add_parser("normalize")
    n.add_argument("--min-idle", type=float, default=600)
    n.add_argument("--mode", default="prod")
    au = sub.add_parser("audit")
    au.add_argument("--hours", type=float, default=24)
    sub.add_parser("freshness")
    sy = sub.add_parser("synth")
    sy.add_argument("out")
    sy.add_argument("--minutes", type=float, default=30)
    sy.add_argument("--fault", nargs="*", help="e.g. hl_poll_s=5.0 depth_break_at=500 stall_every_s=600")
    a = ap.parse_args(argv)
    s = config.load_settings()
    fn = globals()[f"cmd_{a.cmd}"]
    sys.exit(fn(a, s) or 0)


if __name__ == "__main__":
    main()
