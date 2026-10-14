"""L0 raw captures -> L1 normalized parquet.

Output layout (hive partitions, readable by pyarrow.dataset / DuckDB / polars):

  <norm>/table=<t>/venue=<v>/symbol=<s>/date=YYYY-MM-DD/hour=HH/<raw-file-stem>.parquet

* Partitioned by EACH ROW's own receive time (the old writer used the first
  row of a batch, so rows near midnight landed in the wrong day).
* One output file per (raw file, partition): reprocessing a raw file replaces
  exactly its own outputs. Atomic tmp+rename, so readers never see half a file.
* `_manifest.jsonl` records frames in, kinds, rows out, parse errors per raw
  file - the reconciliation the audits check.
* Nothing derived is computed here.
"""

from __future__ import annotations

import datetime as dt
import json
import os
from collections import Counter, defaultdict
from pathlib import Path

import pandas as pd

from . import __version__, parsers
from .rawio import ROTATED, read_file
from .venues import by_cryptofeed_id

TABLES = ("l1", "l2", "trades", "events", "sessions")


def _parser_for(exchange: str):
    _, v = by_cryptofeed_id(exchange)
    return parsers.get(v.parser)


def _write_atomic(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".parquet.tmp")
    df.to_parquet(tmp, index=False)
    os.replace(tmp, path)


def _partition_and_write(table: str, df: pd.DataFrame, out: Path, stem: str, ts_col: str = "ts_recv_ns") -> int:
    if df.empty:
        return 0
    ts = pd.to_datetime(df[ts_col], unit="ns", utc=True)
    df = df.assign(_date=ts.dt.strftime("%Y-%m-%d"), _hour=ts.dt.strftime("%H"))
    df["venue"] = df["venue"].fillna("UNKNOWN")
    df["symbol"] = df["symbol"].fillna("_none")
    n = 0
    for (v, s, d, h), g in df.groupby(["venue", "symbol", "_date", "_hour"], sort=True):
        safe_s = str(s).replace("/", "_")
        p = out / f"table={table}" / f"venue={v}" / f"symbol={safe_s}" / f"date={d}" / f"hour={h}" / f"{stem}.parquet"
        _write_atomic(g.drop(columns=["_date", "_hour", "venue", "symbol"]), p)
        n += 1
    return n


def normalize_file(raw_path: Path, norm_dir: Path) -> dict:
    raw_path = Path(raw_path)
    frames, sessions = read_file(raw_path)
    stem = raw_path.name.split(".pcap")[0]
    m = ROTATED.match(raw_path.name)
    capture = m.group("base") if m else stem
    rows = defaultdict(list)
    kinds: Counter = Counter()
    by_exchange: Counter = Counter()
    errors = 0
    for i, fr in enumerate(frames):
        by_exchange[fr.exchange] += 1
        try:
            kind, out = _parser_for(fr.exchange).parse(fr)
        except Exception as e:  # never lose a frame silently
            errors += 1
            kind, out = "parse_error", [("events", {"venue": fr.exchange, "symbol": None, "venue_symbol": None,
                                                    "ts_recv_ns": fr.ts_ns, "conn_id": fr.conn_id, "conn_ordinal": fr.ordinal,
                                                    "kind": "parse_error", "detail": f"{type(e).__name__}: {e} | {fr.payload[:300]}"})]
        kinds[kind] += 1
        for table, r in out:
            r["frame_idx"] = i
            r["capture"] = capture
            rows[table].append(r)

    files_written = {}
    row_counts = {}
    for table in ("l1", "l2", "trades", "events"):
        cols = parsers.SCHEMAS[table]
        df = pd.DataFrame(rows.get(table, []), columns=cols)
        row_counts[table] = len(df)
        files_written[table] = _partition_and_write(table, df, norm_dir, stem)

    s = pd.DataFrame([{**vars(x), "venue": by_cryptofeed_id(x.exchange)[0] if x.exchange else "UNKNOWN", "symbol": "_session",
                       "raw_file": raw_path.name, "capture": capture} for x in sessions])
    if not s.empty:
        s = s.drop(columns=["exchange"])
        row_counts["sessions"] = len(s)
        files_written["sessions"] = _partition_and_write("sessions", s, norm_dir, stem, ts_col="opened_ns")

    rec = {
        "raw_file": raw_path.name,
        "raw_bytes": raw_path.stat().st_size,
        "frames": len(frames),
        "frames_by_exchange": dict(by_exchange),
        "kinds": dict(kinds),
        "parse_errors": errors,
        "rows": row_counts,
        "files_written": files_written,
        "first_ts_ns": frames[0].ts_ns if frames else None,
        "last_ts_ns": frames[-1].ts_ns if frames else None,
        "mdcap_version": __version__,
        "processed_at": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"),
    }
    norm_dir.mkdir(parents=True, exist_ok=True)
    with (norm_dir / "_manifest.jsonl").open("a") as fh:
        fh.write(json.dumps(rec) + "\n")
    return rec


def manifest(norm_dir: Path) -> list[dict]:
    p = Path(norm_dir) / "_manifest.jsonl"
    if not p.exists():
        return []
    return [json.loads(x) for x in p.read_text().splitlines() if x.strip()]


def pending(raw_files: list[Path], norm_dir: Path) -> list[Path]:
    done = {(m["raw_file"], m["raw_bytes"]) for m in manifest(norm_dir)}
    return [p for p in raw_files if (p.name, p.stat().st_size) not in done]


def load(norm_dir: Path, table: str, venue: str | None = None, symbol: str | None = None,
         start=None, end=None, columns: list[str] | None = None) -> pd.DataFrame:
    """Read a normalized table with partition pruning. start/end filter on ts_recv_ns."""
    import pyarrow.dataset as ds

    root = Path(norm_dir) / f"table={table}"
    if not root.exists():
        return pd.DataFrame(columns=parsers.SCHEMAS.get(table, []))
    d = ds.dataset(root, format="parquet", partitioning="hive")
    flt = None
    for k, v in (("venue", venue), ("symbol", symbol)):
        if v is not None:
            e = ds.field(k) == v
            flt = e if flt is None else flt & e
    if start is not None:
        flt_t = ds.field("ts_recv_ns") >= pd.Timestamp(start, tz="UTC").value
        flt = flt_t if flt is None else flt & flt_t
    if end is not None:
        flt_t = ds.field("ts_recv_ns") < pd.Timestamp(end, tz="UTC").value
        flt = flt_t if flt is None else flt & flt_t
    df = d.to_table(filter=flt, columns=columns).to_pandas()
    sort_col = "ts_recv_ns" if "ts_recv_ns" in df.columns else None
    return df.sort_values([sort_col, "frame_idx"] if sort_col and "frame_idx" in df else sort_col or [], kind="stable").reset_index(drop=True) if sort_col else df
