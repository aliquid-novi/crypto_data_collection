"""Each test reproduces a failure of the previous collector (incidents/0001) or a
core guarantee of this one, using synthetic captures in cryptofeed's real PCAP format."""

import json
import subprocess
import sys
from pathlib import Path

import pandas as pd
import pytest
import yaml

from mdcap import audits, config, normalize, parsers, report, synth
from mdcap.config import Market, Settings
from mdcap.rawio import Frame, closed_files, read_file

ROOT = Path(__file__).resolve().parents[1]
MARKETS = [
    Market("HYPERLIQUID", "HYPE-USD-PERP", ["l1", "l2", "trades"], "x" * 20, "probe"),
    Market("BINANCE_FUTURES", "HYPE-USDT-PERP", ["l1", "l2", "trades"], "x" * 20, "probe"),
]


def _run(tmp, **fault):
    raw = synth.write_capture(tmp / "raw" / "prod" / "capture-20261005T000000Z.pcap.zst", dur_s=1800, seed=2, **fault)
    rec = normalize.normalize_file(raw, tmp / "norm")
    st = Settings(raw_dir=tmp / "raw", norm_dir=tmp / "norm", health_dir=tmp / "h", reports_dir=tmp / "r")
    return rec, report.run(st, MARKETS), st


def _stream(rep, market, ch):
    return next(m for m in rep["markets"] if m["market"] == market)["streams"][ch]


@pytest.fixture(scope="module")
def clean(tmp_path_factory):
    return _run(tmp_path_factory.mktemp("clean"))


# ---------------------------------------------------------------- the baseline passes

def test_clean_capture_passes(clean):
    _, rep, _ = clean
    assert rep["verdict"] == "PASS", report.to_markdown(rep, "clean")[:3000]


# ---------------------------------------------------------------- incident 0001 failures are caught

def test_timer_like_stream_fails(tmp_path):
    _, rep, _ = _run(tmp_path, hl_poll_s=5.0)
    for ch in ("l1", "l2"):
        s = _stream(rep, "HYPERLIQUID:HYPE-USD-PERP", ch)
        assert s["verdict"] == "FAIL"
        assert any(f.startswith("TIMER_LIKE") for f in s["findings"])
    assert rep["verdict"] == "FAIL"
    # Binance unaffected: the audit localises the problem
    assert _stream(rep, "BINANCE_FUTURES:HYPE-USDT-PERP", "l1")["verdict"] == "PASS"


def test_recurring_process_stall_detected(tmp_path):
    _, rep, _ = _run(tmp_path, stall_every_s=600, stall_len_s=12)
    ps = rep["process_stalls"]
    assert ps["n_stalls"] >= 2 and ps["recurring"], ps
    assert abs(ps["recurring"][0]["period_s"] - 600) < 5
    assert rep["verdict"] == "FAIL"


def test_hourly_stall_anchored_to_wall_clock():
    import numpy as np

    t0 = synth.T0
    ts = np.concatenate([t0 + h * 3600 * 10**9 + np.arange(0, 3590, 0.5).astype("int64") * 10**9 for h in range(4)])
    ps = audits.process_stalls(ts)
    r = ps["recurring"][0]
    assert r["wall_clock_anchor"] == "hour" and abs(r["period_s"] - 3600) < 1


def test_duplicate_burst_detected(tmp_path):
    _, rep, _ = _run(tmp_path, dup_burst_at=300, dup_burst_n=200)
    s = _stream(rep, "HYPERLIQUID:HYPE-USD-PERP", "l1")
    assert s["duplicates"]["dup_consecutive"] >= 199
    assert any("duplicate" in f for f in s["findings"])


def test_depth_sequence_break_detected(tmp_path):
    _, rep, _ = _run(tmp_path, depth_break_at=400)
    s = _stream(rep, "BINANCE_FUTURES:HYPE-USDT-PERP", "l2")
    assert s["sequence"]["breaks"] == 1


def test_aggtrade_gap_detected(tmp_path):
    _, rep, _ = _run(tmp_path, agg_gap_at=50)
    s = _stream(rep, "BINANCE_FUTURES:HYPE-USDT-PERP", "trades")
    assert s["sequence"]["gaps"] == 1 and s["sequence"]["missing_trade_ids"] > 0


def test_clock_skew_flagged(tmp_path):
    _, rep, _ = _run(tmp_path, clock_skew_ms=20)
    s = _stream(rep, "BINANCE_FUTURES:HYPE-USDT-PERP", "l1")
    assert s["latency"]["negative_frac"] > 0.5 and s["verdict"] == "WARN"


def test_conflation_not_confused_with_sparse_events():
    import numpy as np

    rng = np.random.default_rng(0)
    sparse = np.cumsum(rng.exponential(2.0, 2000) * 1e9).astype("int64")  # 0.5 Hz Poisson: NOT conflated
    assert audits.cadence(sparse, sparse, "event")["verdict"] == "OK"
    grid = (np.cumsum(rng.integers(1, 4, 5000)) * 100_000_000).astype("int64")  # 100 ms grid, ~5 Hz
    assert any(f.startswith("CONFLATED_UNEXPECTED") for f in audits.cadence(grid, grid, "event")["findings"])


# ---------------------------------------------------------------- rawness guarantees

def test_every_frame_accounted_for(clean):
    rec, rep, _ = clean
    assert sum(rec["kinds"].values()) == rec["frames"]
    assert rec["parse_errors"] == 0 and "unknown" not in rec["kinds"]
    assert rep["completeness"]["verdict"] == "OK"


def test_l1_keeps_sizes_and_ids(clean):
    _, _, st = clean
    b = normalize.load(st.norm_dir, "l1", "BINANCE_FUTURES", "HYPE-USDT-PERP")
    assert b[["bid_sz", "ask_sz", "update_id", "ts_exch_ns", "ts_txn_ns"]].notna().all().all()
    h = normalize.load(st.norm_dir, "l1", "HYPERLIQUID", "HYPE-USD-PERP")
    assert h[["bid_sz", "ask_sz", "bid_n", "ask_n", "ts_exch_ns"]].notna().all().all()


def test_l2_keeps_full_depth_and_sequence(clean):
    _, _, st = clean
    h = normalize.load(st.norm_dir, "l2", "HYPERLIQUID", "HYPE-USD-PERP")
    per_snap = h.groupby(["capture", "frame_idx"]).size()
    assert per_snap.min() == 40  # 20 levels x 2 sides, nothing truncated to L1
    b = normalize.load(st.norm_dir, "l2", "BINANCE_FUTURES", "HYPE-USDT-PERP")
    assert b.loc[b["kind"] == "delta", ["first_id", "last_id", "prev_id"]].notna().all().all()
    assert (b["kind"] == "snapshot").any()  # the REST anchor is kept


def test_no_derived_columns():
    derived = {"mid", "spread", "spread_bps", "microprice", "imbalance"}
    for cols in parsers.SCHEMAS.values():
        assert not derived & set(cols)


def test_empty_book_side_is_a_row_not_a_drop():
    from mdcap.parsers import hyperliquid

    fr = Frame("HYPERLIQUID", "c", 1, False, "ws", 1, json.dumps({"channel": "bbo", "data": {"coin": "X", "time": 1, "bbo": [None, {"px": "1", "sz": "2", "n": 1}]}}))
    kind, rows = hyperliquid.parse(fr)
    assert kind == "l1" and len(rows) == 1 and rows[0][1]["bid_px"] is None and rows[0][1]["ask_px"] == 1.0


def test_unknown_frames_counted_not_dropped(tmp_path):
    from cryptofeed.capture.pcap import META_CONN_ID, META_EXCHANGE, PcapFile, WSStream

    p = tmp_path / "capture-x.pcap.zst"
    pc = PcapFile(str(p))
    s = WSStream(pc, 40001, synth.T0, "wss://x", {META_CONN_ID: "HYPERLIQUID.ws.1", META_EXCHANGE: "HYPERLIQUID"})
    s.message(synth.T0 + 1, json.dumps({"channel": "somethingNew", "data": {}}))
    s.message(synth.T0 + 2, "not json at all")
    s.close(synth.T0 + 3)
    pc.close()
    rec = normalize.normalize_file(p, tmp_path / "n")
    assert rec["kinds"] == {"unknown": 1, "parse_error": 1} and rec["rows"]["events"] == 2
    assert audits.completeness([rec])["verdict"] == "FAIL"


def test_rows_partitioned_by_own_hour(tmp_path):
    # a capture spanning midnight: rows must split across date partitions by their own timestamp
    raw = synth.write_capture(tmp_path / "raw" / "capture-m.pcap.zst", dur_s=600, start_ns=synth.T0 - 300 * 10**9, seed=3)
    normalize.normalize_file(raw, tmp_path / "n")
    dates = {p.name for p in (tmp_path / "n" / "table=l1" / "venue=HYPERLIQUID" / "symbol=HYPE-USD-PERP").iterdir()}
    assert dates == {"date=2026-10-04", "date=2026-10-05"}
    assert audits.partitions(tmp_path / "n")["verdict"] == "OK"


def test_renormalize_is_idempotent(tmp_path):
    raw = synth.write_capture(tmp_path / "raw" / "capture-i.pcap.zst", dur_s=300, seed=4)
    normalize.normalize_file(raw, tmp_path / "n")
    a = normalize.load(tmp_path / "n", "l1")
    normalize.normalize_file(raw, tmp_path / "n")  # e.g. after a parser fix
    b = normalize.load(tmp_path / "n", "l1")
    assert len(a) == len(b)


def test_closed_files_excludes_live_tail(tmp_path):
    import os
    import time

    d = tmp_path / "raw"
    d.mkdir()
    for n in ("capture-A.pcap.zst", "capture-A.1.pcap.zst", "capture-B.pcap.zst", "capture-B.1.pcap.zst"):
        (d / n).write_bytes(b"x")
    now = time.time()
    os.utime(d / "capture-A.pcap.zst", (now - 7200, now - 7200))
    os.utime(d / "capture-A.1.pcap.zst", (now - 3600, now - 3600))
    names = {p.name for p in closed_files(d, min_idle_s=600, now=now)}
    assert "capture-B.1.pcap.zst" not in names  # newest file of the newest recording is still being written
    assert {"capture-A.pcap.zst", "capture-A.1.pcap.zst", "capture-B.pcap.zst"} <= names


# ---------------------------------------------------------------- config gate

def _validate(markets, tmp_path=None):
    return config.validate({"markets": markets})[1]


def test_config_requires_purpose_and_known_channels():
    errs = _validate([{"venue": "HYPERLIQUID", "symbol": "BTC-USD-PERP", "channels": ["l1", "depth9000"], "purpose": "", "status": "probe"}])
    assert any("purpose" in e for e in errs) and any("depth9000" in e for e in errs)


def test_active_requires_passing_probe(tmp_path):
    base = {"venue": "HYPERLIQUID", "symbol": "BTC-USD-PERP", "channels": ["l1"], "purpose": "fair value research on BTC", "status": "active"}
    v = lambda m: config.validate({"markets": [m]}, root=ROOT)[1]  # noqa: E731
    assert any("probe_report" in e for e in v(base))
    rep = tmp_path / "probe.md"
    rep.write_text("Verdict: FAIL\n")
    assert any("FAIL" in e for e in v({**base, "probe_report": str(rep)}))
    rep.write_text("Verdict: WARN\n")
    assert any("accepted_warnings" in e for e in v({**base, "probe_report": str(rep)}))
    assert v({**base, "probe_report": str(rep), "accepted_warnings": "unsequenced venue accepted by Mark"}) == []
    rep.write_text("Verdict: PASS\n")
    assert v({**base, "probe_report": str(rep)}) == []


def test_probe_command_end_to_end(tmp_path, monkeypatch):
    """The gate: probe -> normalize -> audit -> report whose Verdict line config validation reads.
    The live capture is replaced by a synthetic one (no exchange network here)."""
    import mdcap.__main__ as cli
    from mdcap import collector

    st = Settings(raw_dir=tmp_path / "raw", norm_dir=tmp_path / "normalized", health_dir=tmp_path / "health", reports_dir=tmp_path / "reports")
    monkeypatch.setattr(config, "load_settings", lambda *a, **k: st)

    def fake_run(fault):
        async def run(markets, settings, mode="prod", duration_s=None):
            synth.write_capture(settings.raw_dir / mode / "capture-20261005T000000Z.pcap.zst", dur_s=1200, seed=5, **fault)
            return 0
        return run

    for fault, want in (({}, "PASS"), ({"hl_poll_s": 5.0}, "FAIL")):
        monkeypatch.setattr(collector, "run", fake_run(fault))
        for p in (tmp_path / "raw").rglob("*.pcap*"):
            p.unlink()
        with pytest.raises(SystemExit):
            cli.main(["probe", "HYPERLIQUID:HYPE-USD-PERP", "BINANCE_FUTURES:HYPE-USDT-PERP", "--minutes", "1"])
        newest = max((tmp_path / "reports" / "probes").glob("*.md"), key=lambda p: p.stat().st_mtime_ns)
        assert newest.read_text().splitlines()[0] == f"Verdict: {want}", newest.read_text()[:1500]


def test_repo_config_is_valid():
    _, errs = config.validate(yaml.safe_load((ROOT / "config/markets.yaml").read_text()))
    assert not errs, errs


# ---------------------------------------------------------------- hooks

def _hook(name, payload):
    return subprocess.run([sys.executable, str(ROOT / ".claude/hooks" / name)], input=json.dumps(payload), capture_output=True, text=True)


@pytest.mark.parametrize("cmd", [
    "rm -rf data/raw/prod",
    "find data/raw -name '*.pcap.zst' -delete",
    "mv data/raw/prod/capture-1.pcap.zst /tmp/",
    "ops/prune_raw.sh 7 --execute",
    "rclone sync data gcs_md:bucket",
    "sudo systemctl restart mdcap-collector@mark",
])
def test_guard_blocks_destructive(cmd):
    assert _hook("guard.py", {"tool_name": "Bash", "tool_input": {"command": cmd}}).returncode == 2


@pytest.mark.parametrize("cmd", ["ops/prune_raw.sh 7", "python -m mdcap audit --hours 24", "rclone copy data/normalized gcs_md:b/n", "ls data/raw/prod"])
def test_guard_allows_safe(cmd):
    assert _hook("guard.py", {"tool_name": "Bash", "tool_input": {"command": cmd}}).returncode == 0


def test_guard_blocks_agent_promotion(tmp_path):
    p = tmp_path / "config" / "markets.yaml"
    p.parent.mkdir()
    p.write_text("markets:\n  - status: probe\n")
    r = _hook("guard.py", {"tool_name": "Edit", "tool_input": {"file_path": str(p), "old_string": "status: probe", "new_string": "status: active"}})
    assert r.returncode == 2
    ok = _hook("guard.py", {"tool_name": "Edit", "tool_input": {"file_path": str(p), "old_string": "status: probe", "new_string": "status: retired"}})
    assert ok.returncode == 0


def test_rawness_scan_flags_old_patterns(tmp_path):
    f = tmp_path / "mdcap" / "parsers" / "newvenue.py"
    f.parent.mkdir(parents=True)
    f.write_text(
        "df = df.fillna(0)\n"
        "mid = (bid + ask) / 2\n"
        "q.put_nowait(row)\n"
        "fh = FeedHandler(on_feed_error='remove_feed')\n"
        "while True:\n    data = requests.get(url)\n    time.sleep(5)\n"
        "ok = x.resample('1ms')  # rawness-ok: test of the escape hatch\n"
    )
    r = _hook("rawness_scan.py", {"tool_name": "Write", "tool_input": {"file_path": str(f)}})
    assert r.returncode == 2
    for needle in ("zero-filling", "derived feature", "drop-on-full", "remove_feed", "REST call", "POLLING"):
        assert needle in r.stderr, needle
    assert "resampling" not in r.stderr


def test_own_capture_code_is_clean():
    files = [str(p) for p in (ROOT / "mdcap").rglob("*.py") if config.ROOT and __import__("re").search(r"(collector|rawio|normalize|parsers/[^/]+)\.py$", str(p))]
    r = subprocess.run([sys.executable, str(ROOT / ".claude/hooks/rawness_scan.py"), *files], capture_output=True, text=True)
    assert r.returncode == 0, r.stdout


# ---------------------------------------------------------------- live-process health (offline)

def test_watchdog_exits_on_stale_stream(tmp_path, monkeypatch):
    import asyncio
    import time

    from mdcap import collector

    monkeypatch.setattr(collector, "WARMUP_S", 0.0)
    st = Settings(raw_dir=tmp_path, norm_dir=tmp_path, health_dir=tmp_path / "h", reports_dir=tmp_path, heartbeat_s=0.05, min_free_disk_gb=0)
    h = collector.Health(st, "prod", {("HYPERLIQUID", "HYPE-USD-PERP", "l1"): 0.2, ("HYPERLIQUID", "HYPE-USD-PERP", "l2"): 30.0})
    stopped = []

    class Obj:
        symbol = "HYPE-USD-PERP"

    async def go():
        cb = h.counter("HYPERLIQUID", "l2")
        await cb(Obj(), time.time())  # l2 alive, l1 never arrives
        await asyncio.wait_for(h.heartbeat(lambda: stopped.append(1)), timeout=5)

    asyncio.run(go())
    assert stopped and h.fail_reason.startswith("STALE HYPERLIQUID:HYPE-USD-PERP:l1")
    lines = [json.loads(x) for x in next((tmp_path / "h").glob("heartbeat-prod-*.jsonl")).read_text().splitlines()]
    assert lines[-1]["fatal"].startswith("STALE") and sum(x["streams"]["HYPERLIQUID:HYPE-USD-PERP:l2"]["n"] for x in lines) == 1
