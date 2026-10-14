"""Load and validate config/settings.yaml and config/markets.yaml.

markets.yaml is the one place that says what gets collected. Every market
carries the research purpose it serves (Mark's words) and a status:

  probe    captured only by `mdcap probe` runs, never by the production service
  active   captured by production. Requires a probe report with verdict PASS.
  retired  kept for history; not captured

Validation is strict on purpose: a market can't silently go live with an
unverified stream, and a typo can't drop a channel.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

import yaml

from .venues import VENUES

ROOT = Path(__file__).resolve().parents[1]
SETTINGS = ROOT / "config" / "settings.yaml"
MARKETS = ROOT / "config" / "markets.yaml"
STATUSES = {"probe", "active", "retired"}


class ConfigError(ValueError):
    pass


@dataclass
class Market:
    venue: str
    symbol: str
    channels: list[str]
    purpose: str
    status: str
    requested_by: str = ""
    added: str = ""
    probe_report: str = ""
    max_depth: int = 0
    stale_overrides: dict | None = None  # {channel: seconds} for illiquid markets
    accepted_warnings: str = ""  # Mark's reason for promoting a probe whose verdict was WARN
    notes: str = ""

    @property
    def key(self) -> str:
        return f"{self.venue}:{self.symbol}"


@dataclass
class Settings:
    raw_dir: Path
    norm_dir: Path
    health_dir: Path
    reports_dir: Path
    rotate_size_mb: int = 128
    heartbeat_s: float = 10.0
    loop_lag_warn_ms: float = 50.0
    min_free_disk_gb: float = 5.0
    delivery: dict = field(default_factory=dict)


def _expand(p: str) -> Path:
    path = Path(os.path.expandvars(os.path.expanduser(p)))
    return path if path.is_absolute() else ROOT / path


def load_settings(path: Path = SETTINGS) -> Settings:
    d = yaml.safe_load(Path(path).read_text()) or {}
    paths = d.get("paths", {})
    return Settings(
        raw_dir=_expand(paths.get("raw", "data/raw")),
        norm_dir=_expand(paths.get("normalized", "data/normalized")),
        health_dir=_expand(paths.get("health", "data/health")),
        reports_dir=_expand(paths.get("reports", "reports")),
        rotate_size_mb=int(d.get("rotate_size_mb", 128)),
        heartbeat_s=float(d.get("heartbeat_s", 10.0)),
        loop_lag_warn_ms=float(d.get("loop_lag_warn_ms", 50.0)),
        min_free_disk_gb=float(d.get("min_free_disk_gb", 5.0)),
        delivery=d.get("delivery", {}) or {},
    )


def _probe_verdict(path: Path) -> str | None:
    if not path.exists():
        return None
    for line in path.read_text().splitlines():
        if line.lower().startswith("verdict:"):
            return line.split(":", 1)[1].strip().upper()
    return None


def validate(raw: dict, root: Path = ROOT) -> tuple[list[Market], list[str]]:
    """Return (markets, errors). Never raises on content problems - reports them all."""
    errors: list[str] = []
    markets: list[Market] = []
    seen = set()
    for i, m in enumerate((raw or {}).get("markets", []) or []):
        where = f"markets[{i}]"
        try:
            mk = Market(**m)
        except TypeError as e:
            errors.append(f"{where}: {e}")
            continue
        where = f"{where} {mk.key}"
        if mk.venue not in VENUES:
            errors.append(f"{where}: unknown venue (known: {sorted(VENUES)})")
            continue
        v = VENUES[mk.venue]
        if mk.key in seen:
            errors.append(f"{where}: duplicate market")
        seen.add(mk.key)
        if mk.status not in STATUSES:
            errors.append(f"{where}: status must be one of {sorted(STATUSES)}")
        if not mk.channels:
            errors.append(f"{where}: no channels")
        for c in mk.channels:
            if c not in v.streams:
                errors.append(f"{where}: channel {c!r} not supported for {mk.venue} (supported: {sorted(v.streams)})")
        if len(mk.purpose.strip()) < 15:
            errors.append(f"{where}: 'purpose' must say what research question this data serves (Mark's words)")
        if not (root / v.profile).exists():
            errors.append(f"{where}: venue profile {v.profile} missing")
        if mk.status == "active":
            if not mk.probe_report:
                errors.append(f"{where}: active markets need probe_report (run `mdcap probe` first)")
            else:
                verdict = _probe_verdict(root / mk.probe_report)
                if verdict is None:
                    errors.append(f"{where}: probe_report {mk.probe_report} not found or has no 'Verdict:' line")
                elif verdict.startswith("WARN") and len(mk.accepted_warnings.strip()) >= 15:
                    pass  # Mark has read the warnings and said why they're acceptable
                elif not verdict.startswith("PASS"):
                    errors.append(f"{where}: probe verdict is {verdict}, not PASS"
                                  + (" (a WARN can be promoted only with 'accepted_warnings: <reason>' from Mark)" if verdict.startswith("WARN") else ""))
        markets.append(mk)
    return markets, errors


def load_markets(path: Path = MARKETS, status: set[str] | None = None, strict: bool = True) -> list[Market]:
    markets, errors = validate(yaml.safe_load(Path(path).read_text()) or {})
    if errors and strict:
        raise ConfigError("markets.yaml invalid:\n  " + "\n  ".join(errors))
    if status:
        markets = [m for m in markets if m.status in status]
    return markets
