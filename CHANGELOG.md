# Changelog

Changes to what normalized data MEANS go here, with whether existing normalized data must be
rebuilt from raw (`python -m mdcap normalize` after removing the affected partitions).

## 0.1.0 (2026-10-14)
- Initial system: raw PCAP capture, venue registry (BINANCE_FUTURES, HYPERLIQUID), parsers keeping
  all fields, row-hour partitioning, audits, probe gate, ops units. Replaces the previous collector
  (incidents/0001).
