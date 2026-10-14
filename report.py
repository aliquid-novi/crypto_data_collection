"""Run the audits for a set of markets over a time window and write a report.

Used by `mdcap audit` (routine health) and `mdcap probe` (gate before a market
goes active). The report's first line is `Verdict: PASS|WARN|FAIL`, which is
what config validation reads.
"""

from __future__ import annotations

import datetime as dt
import json
from pathlib import Path

import numpy as np
import pandas as pd

from . import audits
from .normalize import load, manifest
from .venues import venue as get_venue

ORDER = {"OK": 0, "PASS": 0, "INSUFFICIENT_DATA": 1, "WARN": 1, "FAIL": 2}


def _worst(verdicts) -> str:
    v = max((ORDER.get(x, 1) for x in verdicts), default=1)
    return {0: "PASS", 1: "WARN", 2: "FAIL"}[v]


def audit_market(norm_dir: Path, mk, start=None, end=None) -> dict:
    v = get_venue(mk.venue)
    res = {"market": mk.key, "streams": {}}
    verdicts = []
    for ch in mk.channels:
        st = v.streams[ch]
        df = load(norm_dir, st.table, mk.venue, mk.symbol, start, end)
        r: dict = {"rows": int(len(df))}
        if df.empty:
            r["verdict"] = "FAIL"
            r["findings"] = [f"no {ch} data at all for {mk.key} in the window"]
            res["streams"][ch] = r
            verdicts.append("FAIL")
            continue
        msgs = df
        if ch == "l2":
            msgs = df.drop_duplicates(["capture", "frame_idx"])  # one row per pushed message, not per level
            if st.has_sequence:  # diff-depth venues: 'snapshot' rows are REST anchors, not pushes
                msgs = msgs[msgs["kind"] == "delta"]
        r["cadence"] = audits.cadence(msgs["ts_recv_ns"], msgs.get("ts_exch_ns"), st.expected_mode, st.interval_ms)
        r["latency"] = audits.latency(msgs["ts_recv_ns"], msgs.get("ts_exch_ns"))
        if ch == "l1":
            r["duplicates"] = audits.duplicates(df, ["bid_px", "bid_sz", "ask_px", "ask_sz", "ts_exch_ns"], group="conn_id")
            if st.has_sequence:
                r["sequence"] = audits.sequence_monotonic(df, "update_id")
        if ch == "l2":
            r["sequence"] = audits.sequence_depth(df) if st.has_sequence else {"status": "UNSEQUENCED"}
            r["snapshot_duplicates"] = audits.snapshot_duplicates(df)
        if ch == "trades":
            r["sequence"] = audits.sequence_aggtrades(df) if st.has_sequence else {"status": "UNSEQUENCED"}
            key = ["ts_trade_ns", "trade_id"]
            r["duplicate_trades"] = int(df.duplicated(key).sum())

        findings = list(r["cadence"].get("findings", [])) + list(r["latency"].get("findings", []))
        sv = [r["cadence"].get("verdict", "WARN"), r["latency"].get("verdict", "WARN")]
        seq = r.get("sequence", {})
        if seq.get("breaks"):
            findings.append(f"{seq['breaks']} pu/u breaks ({seq['break_rate']:.3%} of messages): book reconstruction must resync there")
            sv.append("FAIL" if seq["break_rate"] > 0.001 else "WARN")
        if seq.get("backwards"):
            findings.append(f"update_id went backwards {seq['backwards']} times within a connection")
            sv.append("FAIL")
        if seq.get("gaps"):
            findings.append(f"aggTrade id gaps: {seq['gaps']} ({seq['missing_trade_ids']} trade ids missing)")
            sv.append("WARN")
        dup = r.get("duplicates", {}).get("frac", 0) or r.get("snapshot_duplicates", {}).get("frac", 0)
        if dup > 0.01:
            findings.append(f"{dup:.1%} consecutive duplicate book states (resubscribe replays?)")
            sv.append("WARN")
        if r.get("duplicate_trades"):
            findings.append(f"{r['duplicate_trades']} duplicate trades on (time, id)")
            sv.append("WARN")
        if seq.get("status") == "UNSEQUENCED":
            findings.append("venue provides no sequence numbers: missed messages are undetectable except by timing")
        r["findings"] = findings
        r["verdict"] = _worst(sv)
        verdicts.append(r["verdict"])
        res["streams"][ch] = r
    res["verdict"] = _worst(verdicts)
    return res


def run(settings, markets, start=None, end=None, heartbeats: pd.DataFrame | None = None) -> dict:
    norm = settings.norm_dir
    per = [audit_market(norm, m, start, end) for m in markets]
    allts = []
    for m in markets:
        for ch in m.channels:
            t = load(norm, get_venue(m.venue).streams[ch].table, m.venue, m.symbol, start, end, columns=["ts_recv_ns"])
            allts.append(t["ts_recv_ns"].to_numpy())
    ts = np.concatenate(allts) if allts else np.array([], dtype="int64")
    stalls = audits.process_stalls(ts) if len(ts) > 1 else {"n_stalls": 0, "recurring": []}
    man = manifest(norm)
    if start is not None or end is not None:
        lo = pd.Timestamp(start, tz="UTC").value if start is not None else -np.inf
        hi = pd.Timestamp(end, tz="UTC").value if end is not None else np.inf
        man = [m for m in man if m.get("last_ts_ns") and m["last_ts_ns"] >= lo and m["first_ts_ns"] < hi]
    comp = audits.completeness(man)
    parts = audits.partitions(norm)
    lag = audits.loop_lag(heartbeats if heartbeats is not None else pd.DataFrame(), settings.loop_lag_warn_ms)
    global_v = [comp["verdict"], parts["verdict"]]
    if stalls["n_stalls"]:
        global_v.append("WARN")
    if stalls.get("recurring"):
        global_v.append("FAIL")
    if lag.get("verdict") == "WARN":
        global_v.append("WARN")
    verdict = _worst([p["verdict"] for p in per] + global_v)
    return {"verdict": verdict, "window": [str(start), str(end)], "markets": per, "process_stalls": stalls,
            "completeness": comp, "partitions": parts, "loop_lag": lag}


def to_markdown(rep: dict, title: str) -> str:
    L = [f"Verdict: {rep['verdict']}", "", f"# {title}", "",
         f"Window: {rep['window'][0]} -> {rep['window'][1]}  |  generated {dt.datetime.now(dt.timezone.utc):%Y-%m-%d %H:%M UTC}", ""]
    L += ["| market | stream | rows | verdict | cadence | median gap | modal gap (share) | latency med/p99 ms | sequence |", "|---|---|---|---|---|---|---|---|---|"]
    for m in rep["markets"]:
        for ch, r in m["streams"].items():
            c, la, sq = r.get("cadence", {}), r.get("latency", {}), r.get("sequence", {})
            seq = sq.get("status", "")
            if sq.get("breaks") is not None:
                seq += f", {sq['breaks']} breaks"
            if sq.get("gaps") is not None:
                seq += f", {sq['gaps']} gaps"
            L.append(f"| {m['market']} | {ch} | {r['rows']:,} | **{r['verdict']}** | {c.get('verdict', '-')} | "
                     f"{c.get('median_gap_s', float('nan')):.3f}s | {c.get('modal_gap_s', float('nan')):.3f}s ({c.get('frac_within_10pct_of_mode', 0):.0%}) | "
                     f"{la.get('median_ms', float('nan')):.1f} / {la.get('p99_ms', float('nan')):.1f} | {seq} |")
    L += ["", "## Findings", ""]
    any_f = False
    for m in rep["markets"]:
        for ch, r in m["streams"].items():
            for f in r.get("findings", []):
                L.append(f"- **{m['market']} {ch}**: {f}")
                any_f = True
    ps = rep["process_stalls"]
    if ps.get("n_stalls"):
        L.append(f"- **collector**: {ps['n_stalls']} moments where EVERY stream went silent > 5 s (max {ps['max_gap_s']:.1f}s) - collector/host/network, not the venue")
        any_f = True
    for r in ps.get("recurring", []):
        anchor = f", starting {r['offset_s']:+.1f}s from the top of each {r['wall_clock_anchor']}" if "wall_clock_anchor" in r else ""
        L.append(f"- **collector**: stalls recur every {r['period_s']}s{anchor} - something scheduled is blocking the collector")
        any_f = True
    for f in rep["completeness"].get("findings", []):
        L.append(f"- **completeness**: {f}")
        any_f = True
    if rep["partitions"]["misplaced_files"]:
        L.append(f"- **partitions**: {len(rep['partitions']['misplaced_files'])} files hold rows from another hour")
        any_f = True
    for f in rep["loop_lag"].get("findings", []):
        L.append(f"- **collector**: {f}")
        any_f = True
    if not any_f:
        L.append("- none")
    L += ["", "<details><summary>raw JSON</summary>", "", "```json", json.dumps(rep, indent=1, default=str)[:60000], "```", "</details>", ""]
    return "\n".join(L)
