---
description: Something in the data looks wrong - find out whether it's the market, the venue or our pipeline, fix it, and make sure it can't happen silently again
argument-hint: <symptom, e.g. "HL bbo gaps look like 5 s" or "notebook shows ±86 spreads">
---

Symptom: $ARGUMENTS

1. **data-forensics** subagent: work down the ladder (research -> normalized -> raw -> heartbeat
   -> venue profile -> host) and return a verdict: PIPELINE / VENUE / MARKET / UNRESOLVED.
   - If MARKET or VENUE: explain it to Mark, update the venue profile if it was undocumented, and stop.
   - If it's in research code (e.g. a join that zero-fills), say so. This repo can't fix that, but
     note it for `qr-harness` (`/explain` or `/plumb` there).
2. **Reproduce** (PIPELINE only): add a fault to `mdcap/synth.py` if none fits, and show that the
   current audits do or don't catch it.
3. **collector-engineer** subagent: fix it, plus a regression test where the audit catches the
   synthetic reproduction. If a parser bug corrupted normalized data, the fix includes rebuilding
   from raw. Raw is untouched; deleting the old normalized output is Mark's call.
4. **Postmortem** in `incidents/NNNN-<slug>.md`: symptom, timeline, root cause with evidence,
   blast radius (which dates/markets/columns), fix, regression test, and research that needs re-running.
5. Give Mark the deploy commands and the list of affected research.
