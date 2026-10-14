---
description: Add support for a new venue - sourced profile, registry entry, parser with real-frame fixtures, tests
argument-hint: VENUE (cryptofeed id, e.g. OKX, BYBIT, DERIBIT)
---

Venue: $ARGUMENTS

1. Check cryptofeed supports it (installed version):
   `python -c "from cryptofeed.exchanges import EXCHANGE_MAP; print(sorted(EXCHANGE_MAP))"`.
   If it doesn't, stop and tell Mark. A custom adapter is a bigger decision, and it has to be WebSocket, not polling.

2. **venue-researcher** subagent: write `venues/<VENUE>.md` from `venues/_TEMPLATE.md`. Show Mark the
   "what this means for market-making research" section and the UNVERIFIED list before building.

3. **Capture real frames.** On the VM, record a few minutes with a throwaway registry entry, or use
   cryptofeed directly with `FeedHandler(record=...)`. Extract 20-50 representative raw frames per
   stream (including acks, errors, empty books if seen) into `tests/fixtures/<venue>/*.jsonl`.

4. **collector-engineer** subagent: add the `Venue(...)` entry in `mdcap/venues.py` (as proposed in the
   profile), `mdcap/parsers/<venue>.py`, and tests that:
   - parse every fixture frame without errors, with no frame unaccounted for
   - assert every payload field a market maker could use appears in the output
   - (if the venue has sequences) a planted gap is detected by the sequence audit
   Optionally extend `mdcap/synth.py` for the venue.

5. `python -m pytest -q && python -m mdcap validate`. Then continue with `/add-market`.
