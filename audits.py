"""Deterministic data audits. Each one exists because of a real failure.

cadence          Is this stream event-driven, conflated at the documented
                 interval, or TIMER_LIKE (something polling)?  <- HL 5.4 s poller
process_stalls   Did EVERY stream go quiet at once? That's the collector, not
                 the venue. Recurring phase (e.g. top of each hour) is flagged.
                 <- 3600.07 s gap
duplicates       Identical consecutive book states / repeated snapshots.
                 <- ~9,000 zero-gap rows after resubscribes
sequence         pu/u continuity (Binance depth), update_id monotonicity,
                 aggTrade id continuity. Venues without sequences are reported
                 as UNSEQUENCED, never as "OK".
latency          receive - venue time: median, tails, negative share (clock
                 skew), drift per hour.
completeness     frames in == frames accounted for; parse errors; unknown frames.
partitions       every row sits in the hour partition of its own timestamp.
                 <- first-row date partitioning
freshness        newest data at each stage (raw, normalized, delivered, health),
                 measured separately. <- "Drive is stale" != "collector stopped"

All functions take plain arrays/DataFrames so they're testable without a VM.
"""

from __future__ import annotations

import json
import time
from pathlib import Path

import numpy as np
import pandas as pd

NS = 1_000_000_000


# ---------------------------------------------------------------- cadence

def _gaps_s(ts_ns) -> np.ndarray:
    t = np.sort(np.asarray(ts_ns, dtype="int64"))
    return np.diff(t) / NS if len(t) > 1 else np.array([])


def _modal_gap(g: np.ndarray) -> float:
    """Mode of positive gaps on a log grid (robust to heavy tails)."""
    g = g[g > 1e-4]
    if len(g) < 5:
        return float("nan")
    lg = np.log10(g)
    hist, edges = np.histogram(lg, bins=np.arange(lg.min(), lg.max() + 0.02, 0.02))
    i = int(np.argmax(hist))
    return float(10 ** ((edges[i] + edges[i + 1]) / 2))


def cadence(recv_ns, exch_ns=None, expected_mode: str = "event", interval_ms: float | None = None,
            min_n: int = 30) -> dict:
    g = _gaps_s(recv_ns)
    out: dict = {"n": int(len(g) + (1 if len(g) else 0))}
    if len(g) < min_n:
        return out | {"verdict": "INSUFFICIENT_DATA"}
    pos = g[g > 0]
    mode = _modal_gap(pos)
    near = float(np.mean(np.abs(pos - mode) <= 0.10 * mode)) if np.isfinite(mode) else 0.0
    cv = float(np.std(pos) / np.mean(pos)) if len(pos) else float("nan")
    # robust spread: exponential (event-driven) gaps give ~1.6; a sleep(N) loop gives ~0.05
    rel_iqr = float((np.quantile(pos, 0.75) - np.quantile(pos, 0.25)) / np.median(pos)) if len(pos) else float("nan")
    out |= {
        "median_gap_s": float(np.median(g)),
        "p01_gap_s": float(np.quantile(g, 0.01)),
        "p99_gap_s": float(np.quantile(g, 0.99)),
        "cv": cv,
        "rel_iqr": rel_iqr,
        "modal_gap_s": mode,
        "frac_within_10pct_of_mode": near,
        "zero_gap_frac": float(np.mean(g == 0)),
    }

    # venue-clock quantisation: are venue timestamps spaced on a fixed grid?
    if exch_ns is not None:
        ge = _gaps_s(pd.Series(exch_ns).dropna().to_numpy())
        ge = ge[ge > 0]
        if len(ge) >= min_n:
            out["venue_min_gap_ms"] = float(ge.min() * 1000)
            out["venue_p05_gap_ms"] = float(np.quantile(ge, 0.05) * 1000)
            # Conflation test relative to the stream's own rate: a Poisson stream with this mean gap
            # would put frac_expected of its gaps under 100 ms. Conflated streams put ~none there.
            frac_exp = float(1 - np.exp(-0.1 / ge.mean()))
            frac_obs = float(np.mean(ge < 0.1))
            out["venue_frac_gaps_lt_100ms"] = {"observed": frac_obs, "if_poisson": frac_exp}
            if interval_ms:
                q = interval_ms / 1000
                out["venue_frac_on_interval_grid"] = float(np.mean(np.abs(ge / q - np.round(ge / q)) * q <= 0.005))

    reasons = []
    timer = near >= 0.5 and rel_iqr < 0.3 and mode >= 0.2
    expected_period = (interval_ms or 0) / 1000
    if timer and not (expected_mode in ("conflated", "block") and expected_period and abs(mode - expected_period) <= 0.25 * expected_period):
        reasons.append(f"TIMER_LIKE: {near:.0%} of gaps within 10% of {mode:.3f}s (relative IQR {rel_iqr:.2f}; event-driven would be ~1.6). "
                       f"Something is polling on a timer.")
    fq = out.get("venue_frac_gaps_lt_100ms")
    if expected_mode == "event" and fq and fq["if_poisson"] >= 0.2 and fq["observed"] < 0.1 * fq["if_poisson"]:
        reasons.append(f"CONFLATED_UNEXPECTED: expected event-driven, but only {fq['observed']:.1%} of venue-clock gaps are < 100 ms "
                       f"(a Poisson stream at this rate would have {fq['if_poisson']:.0%})")
    if expected_mode == "conflated" and expected_period and out.get("venue_p05_gap_ms") is not None \
            and out["venue_p05_gap_ms"] < 0.8 * expected_period * 1000:
        reasons.append(f"FASTER_THAN_DOCUMENTED: venue gaps below the {interval_ms:.0f} ms interval - profile may be wrong (good news, but update it)")
    if expected_period and np.median(g) > 20 * expected_period:
        reasons.append(f"TOO_SLOW: median gap {np.median(g):.2f}s vs documented {expected_period:.2f}s (illiquid, or not the stream you think)")
    out["findings"] = reasons
    out["verdict"] = "FAIL" if any(r.startswith(("TIMER_LIKE", "CONFLATED_UNEXPECTED")) for r in reasons) else ("WARN" if reasons else "OK")
    return out


# ---------------------------------------------------------------- stalls

def process_stalls(all_recv_ns, threshold_s: float = 5.0, period_checks=(3600, 60)) -> dict:
    """Gaps in the UNION of every stream's receive times. If all streams are
    silent together, the collector (or its host/network) stalled."""
    t = np.sort(np.asarray(all_recv_ns, dtype="int64"))
    g = np.diff(t) / NS
    idx = np.flatnonzero(g > threshold_s)
    stalls = [{"start": pd.Timestamp(int(t[i]), unit="ns", tz="UTC").isoformat(), "gap_s": round(float(g[i]), 3)} for i in idx]
    out = {"n_stalls": len(stalls), "max_gap_s": float(g.max()) if len(g) else 0.0, "stalls": stalls[:50], "recurring": []}
    if len(idx) >= 3:
        starts = t[idx] / NS
        iv = np.diff(starts)
        # regular spacing between stalls = something scheduled (cron, timer, token refresh, flush)
        if np.median(iv) > 0 and np.std(iv) / np.median(iv) < 0.05:
            P = float(np.median(iv))
            rec = {"period_s": round(P, 2)}
            for W in period_checks:  # also say if it's pinned to the wall clock (e.g. top of the hour)
                if abs(P - W) / W < 0.02 or (P > W and abs(P / W - round(P / W)) < 0.02):
                    ph = float(np.median(starts % W))
                    rec["wall_clock_anchor"] = {3600: "hour", 60: "minute"}.get(W, f"{W}s")
                    rec["offset_s"] = round(ph - W if ph > W / 2 else ph, 2)  # negative = just before
                    break
            out["recurring"].append(rec)
    return out


# ---------------------------------------------------------------- duplicates

def duplicates(df: pd.DataFrame, cols: list[str], group: str | None = None) -> dict:
    """Consecutive identical rows on `cols` (book state + venue time)."""
    if df.empty:
        return {"n": 0, "dup_consecutive": 0, "frac": 0.0}
    d = df[cols]
    same = (d == d.shift()).all(axis=1) | (d.isna() & d.shift().isna()).all(axis=1)
    same.iloc[0] = False
    if group:
        same &= df[group].eq(df[group].shift())
    return {"n": len(df), "dup_consecutive": int(same.sum()), "frac": float(same.mean())}


def snapshot_duplicates(l2: pd.DataFrame) -> dict:
    """Repeated identical full snapshots (same venue time + same levels)."""
    s = l2[l2["kind"] == "snapshot"]
    if s.empty:
        return {"snapshots": 0, "repeated": 0}
    sig = s.groupby(["capture", "frame_idx"], sort=False).apply(
        lambda x: hash((x["ts_exch_ns"].iloc[0], tuple(x["price"].fillna(-1)), tuple(x["size"].fillna(-1)))), include_groups=False)
    rep = int((sig == sig.shift()).sum())
    return {"snapshots": int(len(sig)), "repeated": rep, "frac": rep / max(len(sig), 1)}


# ---------------------------------------------------------------- sequence

def _per_message(l2: pd.DataFrame) -> pd.DataFrame:
    m = l2[l2["kind"] == "delta"].drop_duplicates(["capture", "frame_idx"])
    return m.sort_values(["capture", "conn_id", "conn_ordinal", "ts_recv_ns", "frame_idx"], kind="stable")


def sequence_depth(l2: pd.DataFrame) -> dict:
    """Binance diff-depth continuity: each message's pu == previous message's u
    within one connection session. First message per session is a resync."""
    if l2.empty or "prev_id" not in l2 or l2["prev_id"].isna().all():
        return {"status": "UNSEQUENCED"}
    m = _per_message(l2)
    key = ["capture", "conn_id", "conn_ordinal"]
    prev_last = m.groupby(key)["last_id"].shift()
    breaks = m[prev_last.notna() & (m["prev_id"] != prev_last)]
    return {
        "status": "SEQUENCED",
        "messages": int(len(m)),
        "sessions": int(m.groupby(key).ngroups),
        "breaks": int(len(breaks)),
        "break_rate": float(len(breaks) / max(len(m), 1)),
        "first_breaks": breaks[["ts_recv_ns", "prev_id"]].head(5).assign(expected=prev_last[breaks.index].head(5).values).to_dict("records"),
    }


def sequence_monotonic(df: pd.DataFrame, col: str) -> dict:
    if df.empty or col not in df or df[col].isna().all():
        return {"status": "UNSEQUENCED"}
    d = df.sort_values(["capture", "conn_id", "conn_ordinal", "ts_recv_ns", "frame_idx"], kind="stable")
    prev = d.groupby(["capture", "conn_id", "conn_ordinal"])[col].shift()
    back = int((d[col] < prev).sum())
    return {"status": "SEQUENCED", "n": int(len(d)), "backwards": back}


def sequence_aggtrades(tr: pd.DataFrame) -> dict:
    if tr.empty or tr["first_trade_id"].isna().all():
        return {"status": "UNSEQUENCED"}
    d = tr.sort_values(["capture", "conn_id", "conn_ordinal", "ts_recv_ns", "frame_idx"], kind="stable")
    prev_l = d.groupby(["capture", "conn_id", "conn_ordinal"])["last_trade_id"].shift()
    gaps = d[prev_l.notna() & (d["first_trade_id"] != prev_l + 1)]
    missing = int((gaps["first_trade_id"] - prev_l[gaps.index] - 1).clip(lower=0).sum())
    return {"status": "SEQUENCED", "n": int(len(d)), "gaps": int(len(gaps)), "missing_trade_ids": missing}


# ---------------------------------------------------------------- latency

def latency(recv_ns, exch_ns) -> dict:
    r = pd.Series(recv_ns, dtype="float64")
    e = pd.Series(exch_ns, dtype="float64")
    ok = r.notna() & e.notna()
    if ok.sum() < 30:
        return {"n": int(ok.sum()), "verdict": "INSUFFICIENT_DATA"}
    lat = (r[ok] - e[ok]) / 1e6
    hours = (r[ok] - r[ok].iloc[0]) / NS / 3600
    slope = float(np.polyfit(hours, lat, 1)[0]) if hours.iloc[-1] > 0.25 else float("nan")
    out = {
        "n": int(ok.sum()),
        "median_ms": float(lat.median()), "p01_ms": float(lat.quantile(0.01)), "p99_ms": float(lat.quantile(0.99)),
        "negative_frac": float((lat < 0).mean()), "drift_ms_per_hour": slope,
    }
    findings = []
    if out["negative_frac"] > 0.01:
        findings.append(f"{out['negative_frac']:.1%} of rows received before the venue stamped them: clock skew. Check chrony.")
    if np.isfinite(slope) and abs(slope) > 1.0:
        findings.append(f"latency drifting {slope:+.2f} ms/hour: local clock drift or growing backlog")
    out["findings"] = findings
    out["verdict"] = "WARN" if findings else "OK"
    return out


# ---------------------------------------------------------------- completeness / partitions

def completeness(manifest_rows: list[dict]) -> dict:
    frames = sum(m["frames"] for m in manifest_rows)
    kinds: dict = {}
    for m in manifest_rows:
        for k, v in m["kinds"].items():
            kinds[k] = kinds.get(k, 0) + v
    errors = sum(m["parse_errors"] for m in manifest_rows)
    accounted = sum(kinds.values())
    findings = []
    if accounted != frames:
        findings.append(f"frames in {frames} != frames accounted {accounted}")
    if errors:
        findings.append(f"{errors} parse errors - see events table kind=parse_error")
    if kinds.get("unknown"):
        findings.append(f"{kinds['unknown']} unknown frames - parser doesn't recognise something the venue sends")
    if kinds.get("venue_error"):
        findings.append(f"{kinds['venue_error']} error messages from the venue")
    return {"files": len(manifest_rows), "frames": frames, "kinds": kinds, "parse_errors": errors, "findings": findings,
            "verdict": "FAIL" if (errors or accounted != frames) else ("WARN" if findings else "OK")}


def partitions(norm_dir: Path, sample: int = 200) -> dict:
    bad, checked = [], 0
    for p in sorted(Path(norm_dir).rglob("*.parquet"))[:sample]:
        parts = dict(x.split("=", 1) for x in p.parts if "=" in x)
        if "date" not in parts:
            continue
        col = "opened_ns" if parts.get("table") == "sessions" else "ts_recv_ns"
        ts = pd.to_datetime(pd.read_parquet(p, columns=[col])[col], unit="ns", utc=True)
        checked += 1
        if not ((ts.dt.strftime("%Y-%m-%d") == parts["date"]) & (ts.dt.strftime("%H") == parts["hour"])).all():
            bad.append(str(p))
    return {"checked": checked, "misplaced_files": bad, "verdict": "FAIL" if bad else "OK"}


# ---------------------------------------------------------------- freshness

def freshness(settings, now: float | None = None) -> dict:
    """Newest data per stage, each measured from its own evidence."""
    from .normalize import manifest

    now = now or time.time()
    out = {}
    raws = list(Path(settings.raw_dir).rglob("*.pcap*"))
    out["raw_file_written"] = max((p.stat().st_mtime for p in raws), default=None)
    man = manifest(settings.norm_dir)
    last = [m["last_ts_ns"] for m in man if m.get("last_ts_ns")]
    out["normalized_newest_event"] = max(last) / NS if last else None
    hb = sorted(Path(settings.health_dir).glob("heartbeat-prod-*.jsonl"))
    if hb:
        lines = hb[-1].read_text().strip().splitlines()
        out["heartbeat"] = json.loads(lines[-1])["ts"] if lines else None
    else:
        out["heartbeat"] = None
    dmark = Path(settings.health_dir) / "delivered.json"
    out["delivered"] = json.loads(dmark.read_text()).get("ts") if dmark.exists() else None
    return {k: {"ts": v, "lag_s": None if v is None else round(now - v, 1)} for k, v in out.items()}


def loop_lag(heartbeats: pd.DataFrame, warn_ms: float = 50.0) -> dict:
    if heartbeats.empty or "loop_lag_max_ms" not in heartbeats:
        return {"verdict": "INSUFFICIENT_DATA"}
    mx = heartbeats["loop_lag_max_ms"]
    out = {"p50_ms": float(mx.median()), "p99_ms": float(mx.quantile(0.99)), "max_ms": float(mx.max())}
    out["verdict"] = "WARN" if out["p99_ms"] > warn_ms else "OK"
    if out["verdict"] == "WARN":
        out["findings"] = [f"event loop lag p99 {out['p99_ms']:.0f} ms: receive timestamps are late by up to this much"]
    return out
