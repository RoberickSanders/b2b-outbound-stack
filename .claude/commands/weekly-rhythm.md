---
description: Operational playbook for running cold email continuously across CLIENT_A, CLIENT_B, and CLIENT_C. Mon/Wed/Fri/biweekly/monthly/quarterly cadence. Pure schedule — no scripts. The difference between hobbyist and top-1% operators is doing this every week.
argument-hint: [day] (optional — mon|wed|fri|biweekly|monthly|quarterly)
---

# Cold Email Weekly Rhythm

Having a Forge full of tools doesn't help if you don't run them on a schedule. **This is that schedule** — the operational rhythm that separates hobbyist outbound from top-1% production.

This is a pure playbook. No scripts, no automatic reminders. You put the rhythm on your own calendar (Step 1) and run the prescribed action at the prescribed time. That's the system.

> Adapted for our 3 active clients: **CLIENT_A** (Sender One, fire protection / Denver), **CLIENT_B** (Sender Two, MSP cyber), **CLIENT_C** (the operator, B2B services / national).

---

## Step 1 — Put the rhythm on your calendar (one-time)

Open Apple Calendar / Google Calendar / whatever you actually look at every day. Create these as recurring events. Copy the titles and cadences exactly.

| Event title | Cadence |
|---|---|
| CLIENT_C: Monday deliverability audit | Every Monday, 9:00 am ET |
| CLIENT_C: Wednesday positive-reply sweep | Every Wednesday, 10:00 am ET |
| CLIENT_C: Friday campaign retrospectives | Every Friday, 3:00 pm ET |
| CLIENT_C: Inbox rotation | Every other Monday, 11:00 am ET |
| CLIENT_C: Monthly spam placement test | 1st of each month, 10:00 am ET |
| CLIENT_C: Quarterly experiment review | First Monday of each quarter, 1:00 pm ET |

**Do not skip Step 1.** This skill has no built-in reminder — intentionally — because if it did and it broke, your ops would silently fail. Your calendar is the accountability system.

---

## Monday — Deliverability audit (15 min, all clients)

**Run for each active client (CLIENT_A, CLIENT_B, CLIENT_C):**

```bash
f autopilot                # daily mailbox health watchdog (alert-only by default)
f monitor                  # DNS + bounce trend across all 125+ mailboxes
```

For any campaign that flagged in the last 7 days:

```
/diagnose-campaign <campaign_id>
```

Runs the 4-point Oliverify diagnostic via Smartlead MCP (Deliverability → Targeting → Offer → Speed-to-Lead).

**Review:**
- Fleet reply rate over last 7 days — must be ≥1% (the 1% rule)
- Flagged campaigns / inboxes
- Bounce rate — pause anything spiking >2%

**Action:**
- If a campaign failed the 1% rule → run `f compound --client X --niche Y` to mine winning angles, then iterate copy
- If bounce rate spiked >2% → pause the offending campaign in Smartlead, triage with `/diagnose-campaign`
- If everything is clean → log the check + close the tab

---

## Wednesday — Positive-reply sweep (30-60 min)

**Run:**

```bash
f triage                   # classify replies + draft response in sender voice (Kimi + Opus)
f triage-notify            # push notifications for any positive replies
```

**Review:**
- Any `interested` or `meeting_booked` replies — these are leads
- Any `referral` replies — "talk to Jane instead"
- Any `hostile` replies — investigate (often signals bad targeting)

**Action:**
- Respond to every positive reply within 30 seconds of seeing it. **Do not batch.** A reply that feels minutes old converts ~3x better than one that took hours.
- For referrals: reach out to the referred person within 24h, mention the referrer by name.
- For hostile: apologize, remove from all lists, dig into why they were flagged.
- For booked meetings: log to master DB:
  ```bash
  f meetings log --email <prospect> --client <X> --niche <Y>
  ```
- Pre-call brief for tomorrow's calls:
  ```bash
  f prep <email>
  ```

**Volume cutover:** if positive replies > 50/week, hand off Wednesday-morning to a closer/AE.

---

## Friday — Campaign retrospectives (20 min per campaign)

**Identify campaigns hitting their 21-day mark this week.** (21 days = minimum reply-rate signal stabilization.)

For each one:

```bash
f analyze --campaign-id <id>     # Kimi-powered objection themes + recommendations
f client-report --client <X>     # weekly per-client performance markdown
```

**Decide for each campaign:**
- **Winner** (positive reply rate >= 2x client baseline) -> keep running, consider scaling (clone to more inboxes)
- **Middling** (near baseline) -> iterate on copy or list. Plan next variant for Monday's launch.
- **Loser** (< 50% of baseline) -> kill it. Document why.

**Log every result.** Append to the campaign's experiment file:
- `positive_reply_rate`, `reply_rate`, `bounce_rate`
- Decision (keep / iterate / kill) + reasoning
- Hypothesis for the next iteration

Skip the log -> quarterly review is impossible. Don't skip the log.

---

## Every other Monday — Inbox rotation (30 min)

**Run:**

```bash
f autopilot                      # surfaces unhealthy inboxes
f monitor                        # warmup score + bounce trend per mailbox
```

**Review:**
- Any inboxes with reputation "bad"?
- Any with `is_warmup_blocked: true`?
- Any sending <5/day despite being attached to active campaigns?

**Action via Smartlead UI (or MCP):**
- Retire the failing inboxes (tag `retired`, disable warmup)
- Promote insurance inboxes to `active`
- If insurance pool < 5 inboxes -> kick off a new domain purchase + provisioning cycle:
  ```bash
  f onboard --client <X> --domains domains.txt          # Google fleet
  f onboard-msft --client <X> --domains domains.txt     # Microsoft fleet
  ```
  Takes ~2 weeks from purchase to sendable — start early.

---

## Monthly (1st) — Spam placement test (25 min active, ~20 min compute)

Run a placement test against the highest-volume active campaign for each client (3 tests total: CLIENT_A, CLIENT_B, CLIENT_C).

Use Smartlead's built-in Smart Delivery test (Settings -> Smart Delivery -> Run Test -> 100 senders, mix Google + Microsoft). Or via MCP:

```
mcp__smartlead__smartlead_get_campaign_top_level_analytics_by_date_range
```

**Review:**
- Inbox placement % — target >=85%
- Spam filter triggers — which fired, which senders affected

**Action:**
- >=90% placement -> great, keep doing what you're doing
- 80-90% -> yellow. Look at filter details. Fix highest-frequency trigger next week.
- <80% -> red. Pause campaign. Run `/diagnose-campaign` + `f monitor` to triage. Don't send more until fixed.

---

## Quarterly (first Monday of each quarter) — Experiment review (90 min)

Read all the campaign experiment files from the last quarter. Identify patterns:

- Which campaigns had the highest positive reply rate?
- Which discovery sources produced the best leads (Blitz keyword? AI Ark lookalike? Firecrawl directory? competitor engagers?)
- Which copy angles resonated (timeline hook? performance/risk-free? compliance? social proof? Nowoslawski colleague_internal?)
- Which client+niche combos had the best ROI per `f meetings roi`?

**Output:** a 1-page Q<N> retrospective at `01-Projects/<client>/retrospectives/<YYYY>-Q<N>.md` per active client containing:

- Top 3 campaigns + what made them work
- Bottom 3 campaigns + what to avoid
- 3-5 hypotheses for next quarter's experiments
- Adjustments to `CLIENT.md` ICP / personas / verticals if data suggests it

Use the retrospective to design next quarter via:

```bash
f compound --list                                 # show all (client, niche) combos w/ wins
f compound --client <X> --niche <Y>               # write the brief
f auto-research --client <X> --niche <Y>          # autonomous research -> DRAFTED launch
```

---

## What to skip

You do **not** need to:

- Check Smartlead every day (Wednesday sweep catches everything important)
- Obsess over daily reply-rate fluctuations (wait for 7-day averages)
- Read every positive reply in real time — set up Pushover via `f triage-notify` if you want immediacy, but Wednesday is the system

Daily pokes at your cold email stack are a procrastination pattern, not a performance pattern.

---

## Critical rules (always, every day)

From `02-Areas/lead-pipeline/CLAUDE.md`:

1. **Never auto-START a Smartlead campaign.** Always create DRAFTED. the operator clicks Start in UI.
2. **Never auto-pause a running campaign.** Mailbox Autopilot alerts via Pushover — the operator decides.
3. **Always run `f audit-data` before upload.** Prevents the 2026-04-20 Property Tax Appeal disaster.
4. **Heavy copy stays on Claude Opus 4.** `paf_copy_gate` enforces this — do not revert to Sonnet/Haiku.
5. **Light tasks route through `llm_router.get_light_client()` -> Kimi K2.6.** Don't hardcode anthropic.Anthropic().

---

## What `$ARGUMENTS` does

If you pass an argument to this skill, it jumps straight to that section:

- `/weekly-rhythm mon` -> just the Monday playbook
- `/weekly-rhythm wed` -> just the Wednesday playbook
- `/weekly-rhythm fri` -> just the Friday playbook
- `/weekly-rhythm biweekly` -> inbox rotation
- `/weekly-rhythm monthly` -> spam placement test
- `/weekly-rhythm quarterly` -> quarterly review
- `/weekly-rhythm` (no arg) -> full playbook

If `$ARGUMENTS` is set, skip directly to the matching section above and ignore the rest. Otherwise, this is a reference doc — pick today's section.

---

## What to do next

This skill IS the loop. Your next action is the next item on your calendar.

If you haven't run a campaign yet -> skip this skill. Come back after your first campaign hits the 7-day mark.

## Related commands

- `/diagnose-campaign` — Monday triage when a campaign flags
- `/today` — daily snapshot before kicking off the day's work
- `f compound` — Friday retrospective input (winning angles)
- `f client-report` — Friday auto-generated client deliverable
- `f triage` / `f triage-notify` — Wednesday reply sweep
- `f autopilot` / `f monitor` — daily and biweekly mailbox health
- `f auto-research` — quarterly experiment design execution
