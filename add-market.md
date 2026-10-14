---
description: Add a market (venue + symbol) - probe it, audit it, and let Mark decide whether it goes live
argument-hint: VENUE SYMBOL <what research it's for>
---

Request: $ARGUMENTS

1. **Purpose and resolution (Mark's call).** If he hasn't said, ask in one message:
   - What research question is this for? (goes in `purpose`, in his words)
   - What does the research need: sub-second top of book? Depth (how many levels)? Every trade?
   Don't propose a purpose.

2. **Can the venue deliver that?** `python -m mdcap inventory`, and read `venues/<VENUE>.md`.
   - Venue not in the registry: stop, run `/add-venue <VENUE>` first.
   - Tell Mark plainly what the streams can and can't support for his stated need, e.g. "HL depth
     is a 0.5 s snapshot: you can't see queue changes inside it." Ask whether that's acceptable.

3. **Symbol.** Use cryptofeed's normalized spelling (e.g. `HYPE-USD-PERP`, `BTC-USDT-PERP`). On the VM:
   `python -c "from cryptofeed.exchanges import <Class>; print([s for s in <Class>.symbols() if '<COIN>' in s])"`

4. **Add to `config/markets.yaml` with `status: probe`.** Fields: venue, symbol, channels,
   purpose (his words), requested_by: mark, added (today). Run `python -m mdcap validate`.

5. **Probe** (needs exchange network access, so on the VM, from the repo dir):
   `python -m mdcap probe <VENUE>:<SYMBOL> --minutes 30`
   If you can't reach the VM from this session, give Mark the exact command and wait for the
   report path. Probing from a laptop measures a different network path, so say so if he does that.

6. **Forensics review.** Use the **data-forensics** subagent on the probe report and probe data.
   Pay special attention to cadence vs the venue profile, process stalls, and loop lag.

7. **Present** the verdict, every finding, and what the data will support for his purpose. If the
   probe showed something the profile didn't predict, update the profile (tag [probe] + report link),
   or run venue-researcher if it contradicts [docs].

8. **Promotion is Mark's decision.** If he says yes, set `status: active` and
   `probe_report: <path>`. For a WARN verdict, also add `accepted_warnings:` with HIS reason, verbatim.
   Validate. Add a row to the venue profile's probe history.

9. **Deploy (Mark runs it):** the collector reads config at start-up, so a restart is needed, and
   it causes a few seconds' gap in every stream. Give him:
   `git pull && sudo systemctl restart mdcap-collector@$USER && sleep 120 && python -m mdcap freshness`
   Then suggest `/health` after about an hour.
