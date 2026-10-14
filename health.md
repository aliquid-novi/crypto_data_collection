---
description: Is the pipeline healthy right now? Freshness per stage, 24h audit, collector heartbeat. Read-only.
argument-hint: [hours, default 24]
---

Window: ${ARGUMENTS:-24} hours. Read-only: change nothing.

1. `python -m mdcap freshness`. Report each stage separately (raw written, normalized,
   heartbeat, delivered). If you're on the desktop rather than the VM, also check the local pull
   (`delivered.json` from `ops/pull_local.sh`). Name the FIRST stage that's behind: that's where the
   problem is, not further downstream.
2. `python -m mdcap normalize` (safe: closed files only), then `python -m mdcap audit --hours <window>`.
3. Heartbeats (`data/health/heartbeat-prod-*.jsonl`): restarts (pid changes), any `fatal` lines, loop-lag
   trend, disk-free trend (days of headroom at the current rate).
4. `chronyc tracking` if on the VM: system time offset.
5. If anything is WARN or FAIL, hand the report to the **data-forensics** subagent.

Reply: one line per stage (OK / behind by X / broken), the findings that matter, and the single
next check or command. If everything passes, say so in one line.
