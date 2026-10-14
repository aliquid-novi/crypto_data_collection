---
description: Any other maintenance or improvement to the capture system
argument-hint: <what to change>
---

Change: $ARGUMENTS

1. **Plan in 3-6 lines**: what changes, which component, and whether it changes **data semantics**
   (what a normalized column means, what's captured, timestamps, partitioning). If it does, ask Mark
   before building.
2. Check the principles in CLAUDE.md. If the change would trade rawness or loudness for convenience
   (e.g. "drop the depth stream to save disk", "skip frames when behind"), say so and offer the
   alternatives. Don't just do it.
3. **collector-engineer** subagent implements it with tests.
4. Collector changes: run a short probe (`python -m mdcap probe <an active market> --minutes 10`)
   before deploying, and compare cadence/latency/loop-lag with the market's last probe report.
5. Mark deploys. Give him the exact commands, and what `/health` should show afterwards.
