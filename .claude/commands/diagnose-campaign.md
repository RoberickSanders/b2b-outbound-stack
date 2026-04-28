---
description: Run the 4-point Oliverify diagnostic on a Smartlead campaign and surface the failure point with concrete fixes
argument-hint: <smartlead_campaign_id>
---

# Diagnose Campaign

You are running the 4-point cold-email diagnostic from Oliverify's playbook on Smartlead campaign `$ARGUMENTS`. Cold email breaks at one of four points, and the order matters — diagnose top-down. This command produces a structured report that names the failure point, cites the data, and suggests specific fixes.

## Diagnostic order (top-down — STOP at the first failure)

1. **Deliverability** — are emails landing in the inbox?
2. **Targeting** — are the right people receiving them?
3. **Copy / Offer** — are people interested when they reply?
4. **Speed-to-Lead** — are interested replies converting to meetings?

If layer 1 fails, layers 2–4 are noise. Don't diagnose targeting until deliverability is confirmed healthy.

## Step 1 — Pull campaign data via Smartlead MCP

Use these MCP tools (in this order):

1. `mcp__smartlead__smartlead_get_campaign` with `campaign_id=$ARGUMENTS` — basic info, status, name
2. `mcp__smartlead__smartlead_get_campaign_top_level_analytics` with `campaign_id=$ARGUMENTS` — aggregate stats
3. `mcp__smartlead__smartlead_get_campaign_lead_statistics` with `campaign_id=$ARGUMENTS` — per-lead status
4. `mcp__smartlead__smartlead_get_campaign_mailbox_statistics` with `campaign_id=$ARGUMENTS` — sender health
5. `mcp__smartlead__smartlead_fetch_lead_categories` with `campaign_id=$ARGUMENTS` — reply category breakdown
6. `mcp__smartlead__smartlead_get_campaign_sequence` with `campaign_id=$ARGUMENTS` — copy + subject lines

If the campaign id is invalid, stop and tell the user.

## Step 2 — Compute the four diagnostic metrics

From the data above, derive:

```
emails_sent           = top_level_analytics.unique_sent_count    or sum across mailboxes
unique_replies        = lead_categories where reply == true (deduped by lead_id)
positive_replies      = leads with category in {Interested, Meeting Booked, Information Request}
bounced               = lead_statistics where status == "BOUNCED"
out_of_office         = lead_categories where category == "Out of Office"

reply_rate            = unique_replies / emails_sent
positive_rate         = positive_replies / unique_replies        (when replies > 0)
bounce_rate           = bounced / emails_sent
ooo_rate              = out_of_office / emails_sent              (deliverability proxy — you can't OOO from spam)
meetings_per_positive = bookings / positive_replies             (if known; else "n/a")
```

If any value can't be computed from the API, note that explicitly — don't fabricate.

## Step 3 — Walk the diagnostic top-down

### Layer 1 — Deliverability

Healthy thresholds (from Oliverify's data on 2.27M emails):
- reply_rate ≥ 2.5% → healthy
- reply_rate 1–2% → watch
- reply_rate < 1% → **deliverability problem** (or audience problem if rate is suspiciously zero)
- bounce_rate ≤ 2% → healthy
- bounce_rate ≥ 3% → **deliverability problem** (and pause-worthy at ≥ 3%)
- ooo_rate < 0.3% with reply_rate < 1% → strong deliverability red flag (mailbox is in spam everywhere)

Also check mailbox health from `smartlead_get_campaign_mailbox_statistics`:
- Any mailbox with < 5% reply contribution while rest are higher → that mailbox is degrading
- Mailboxes recently disconnected → reconnect or replace

If deliverability fails, STOP and emit fix recommendations:
- Verify list against MillionVerifier (run `tools/mv_bulk_verify.py`)
- Check warmup is still on for every sender (Smartlead → mailbox → warmup tab)
- Pause campaign if bounce rate ≥ 3% pending list re-verification
- Swap any mailbox with reply contribution < 5% via `tools/mailbox_autopilot.py`
- Check `tools/deliverability_monitor.py` output for SPF/DKIM/DMARC drift

### Layer 2 — Targeting

Only run this layer if Layer 1 passed.

Pull lead category breakdown. Look for:
- "Wrong Person" / "Not Decision Maker" / "Out of Scope" categories → titles or company filters too loose
- High volume of replies from titles outside ICP (e.g., "Executive Assistant", "Intern") → AI title-filter step missing
- Reply rate > 2.5% but positive_rate < 30% → audience is reading but not the right audience

If targeting fails, emit:
- Run `tools/verify_title_redflags.py` against the lead list
- Run `tools/verify_niche_fit_website.py` to confirm companies match ICP
- Tighten Blitz/AI Ark filter inclusion — add exclusionary keywords for noise titles
- Pull a sample of 30 sent leads and manually inspect for fit (`smartlead_list_leads_by_campaign`)

### Layer 3 — Copy / Offer

Only run this layer if Layers 1 and 2 passed.

Pull the campaign sequence and grade it with two tools:
1. `tools/paf_copy_gate.py <sequence_json>` — tactical writing rubric (humanizer + 18-point)
2. `tools/score_offer.py <sequence_json>` — strategic offer scorecard (10-component, /50)

Failure modes:
- paf_copy_gate score < 14/18 → tactical rewrite (use `cold-email-writer` skill)
- score_offer grade C or D → offer is weak; rewrite the OFFER not just the copy
- positive_rate < 20% with reply_rate ≥ 2.5% → people are responding but not interested → offer mismatch
- "Not Interested" / "Unsubscribe" categories dominant → offer + audience mismatch

If copy/offer fails, emit:
- Top 3 improvements from `score_offer` output verbatim
- Suggested rewrite path: "Run `cold-email-writer` skill with the offer-scorecard improvements as constraints"
- If score_offer flagged risk-reversal weakness specifically, suggest performance-pricing or free-deliverable angles

### Layer 4 — Speed-to-Lead

Only run this layer if Layers 1–3 passed.

Compute:
- Time from positive reply → first response
- Booking rate (meetings / positive replies) — Oliverify benchmark: 20–40%
- Show rate — Oliverify benchmark: 75–80%

If you don't have meeting data in Smartlead, query master_leads.db:
```
sqlite3 master-leads/master_leads.db "
  SELECT m.outcome, COUNT(*) FROM meetings m
   WHERE m.campaign_name LIKE '%<campaign_name>%'
   GROUP BY m.outcome
"
```

Failure modes:
- Booking rate < 20% → reply handling is too slow OR meetings asset (calendar link, deliverable) missing
- Show rate < 70% → bookings happening too far in the future, or missing day-of confirmation
- Time-to-first-response > 30 min → no notification routing on positive replies

If speed-to-lead fails, emit:
- Set up Pushover/Slack alert on positive reply via `tools/reply_triage_notify.py`
- Pre-stage assets (case study PDF, calendar link, deliverable template) in inbox draft templates
- Aim for 5–15 min response window
- If mobile enrichment is missing, evaluate adding a Prospeo/BetterContact step on positive replies only

## Step 4 — Output the report

Write a single structured report. Format:

```
DIAGNOSIS — Campaign $ARGUMENTS
================================
Name: <campaign name>
Status: <ACTIVE/PAUSED/DRAFTED>
Sent: <N>  |  Replies: <N> (<X%>)  |  Positive: <N> (<X%>)  |  Bounced: <N> (<X%>)

VERDICT: <Layer N — Failure Type>

Layer 1 — Deliverability:    [ PASS / FAIL ]   <one-line evidence>
Layer 2 — Targeting:         [ PASS / FAIL / SKIPPED ]   <one-line evidence>
Layer 3 — Copy / Offer:      [ PASS / FAIL / SKIPPED ]   <one-line evidence>
Layer 4 — Speed-to-Lead:     [ PASS / FAIL / SKIPPED ]   <one-line evidence>

ROOT CAUSE
<2-3 sentences naming the specific failure with cited numbers from the data>

CONCRETE FIXES (in priority order)
1. <command-line or workflow step>
2. <command-line or workflow step>
3. <command-line or workflow step>

SHIP-GATE STATUS
<one of: continue running / pause and fix / rewrite required>
```

Stop after writing the report. Do not auto-pause the campaign — that requires the operator's explicit approval per CLAUDE.md operating rules.

## Hard rules

- Never call `smartlead_update_campaign_status` to pause/start a campaign from this command.
- Never auto-rewrite copy. Only emit fix recommendations.
- If data is missing or sparse (< 500 emails sent), say so explicitly — verdicts on tiny samples are noise.
- Cite numbers verbatim. Don't say "deliverability is bad" — say "reply rate is 0.6% (healthy threshold ≥ 2.5%)".
