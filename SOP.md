# ClientC Agency SOP — v1.1

**Last updated:** 2026-04-20
**Purpose:** Complete operating manual for running CLIENT_C's lead-gen agency. Any Claude Code session or fresh pair of eyes can pick this up and know exactly what to do.

## Table of Contents
1. [Hard Rules (Never Violate)](#-hard-rules-never-violate)
2. [Tool Inventory](#-tool-inventory)
3. [Standard Workflow: Launching a New Campaign](#-standard-workflow-launching-a-new-campaign)
4. [What to Do When Autopilot Alerts Fire](#-what-to-do-when-autopilot-alerts-fire)
5. [Automated Cadence (Running 24/7)](#-automated-cadence-running-247)
6. [Environment + Secrets](#-environment--secrets-env)
7. [Cost Model](#-cost-model-as-of-2026-04-20)
8. [Key Campaign Thresholds](#-key-campaign-thresholds-autopilot-defaults)
9. [Troubleshooting Decision Tree](#-troubleshooting-decision-tree)
10. [Active Client Context](#-active-client-context-as-of-2026-04-20)
11. [Starting a New Chat? Do This](#-starting-a-new-chat-do-this)
12. [Smartlead API — Hard-Won Gotchas](#-smartlead-api--hard-won-gotchas)
13. [Mailbox 14-Day Maturity Rule](#-mailbox-14-day-maturity-rule-code-enforced)
14. [Known Winning Campaigns](#-known-winning-campaigns-reply-rate-proven-feeds-forge-few-shot)
15. [Warm Reply Workflow](#-warm-reply-workflow-handling-inbound-from-prospects)
16. [Test Suite](#-test-suite)
17. [Kimi Code API Gotchas](#-kimi-code-api-gotchas)
18. [Deep Research Confidence Scoring](#-deep-research-confidence-scoring)
19. [Backup + Rollback](#-backup--rollback)
20. [Campaign ID Quick Index](#-campaign-id-quick-index-as-of-2026-04-20)
21. [Recent Changes](#-recent-changes-2026-04-20-session)
22. [One-Liner Cheat Sheet](#-one-liner-cheat-sheet)

---

## 🛑 Hard Rules (Never Violate)

These are written in stone. Every new Claude Code session should enforce them.

1. **Never call `START` on any Smartlead campaign without the operator's explicit approval.** Campaigns are always created as `DRAFTED`. the operator clicks Start manually in the Smartlead UI when he's reviewed + approved. This applies to CLIENT_C, CLIENT_A, and CLIENT_B equally.

2. **Never auto-pause a running campaign.** Mailbox Autopilot runs in alert-only mode by default — it pings the operator's phone via Pushover and the operator decides whether to pause. `--auto-pause` flag exists but should only be enabled after at least 30 days of clean autopilot runs.

3. **Don't modify `forge.py` or any Forge core file for new tooling.** Build standalone tools in `tools/` that read Forge's output. This keeps the production pipeline stable while still letting us add capability.

4. **Always run a data quality check BEFORE uploading leads to Smartlead.** One bad campaign (like the 79%-unnamed Property Tax Appeal incident on 2026-04-20) can damage sender reputation for 3-6 months. The tool is `data_quality_check.py` — use it.

5. **Forge's copy pipeline uses Claude Opus 4, not Sonnet 4.** Sonnet hit 0% A-grade on the 19-point rubric in tests. Opus hit 80%. Trivial cost increase (~$1/mo), massive quality lift. Don't revert.

6. **Haiku-grade tasks route through `llm_router.get_light_client()` which hits Kimi K2.6.** Don't hardcode `anthropic.Anthropic()` for classification/parsing/extraction. `FORGE_FORCE_CLAUDE=true` kill switch exists if Kimi goes down.

---

## 📦 Tool Inventory

All tools live at `02-Areas/lead-pipeline/tools/` (except `forge.py` which is at the root). Run from `02-Areas/lead-pipeline/`.

| Tool | Purpose | How often |
|---|---|---|
| `forge.py` | Full lead-gen pipeline (discovery + enrichment + verification + export) | Each new campaign (manual) |
| `tools/data_quality_check.py` | Pre-send audit of a CSV or Smartlead campaign | Before every upload (mandatory) |
| `tools/deep_research.py` | Per-prospect Kimi-powered personalization opener | After Forge, before upload (optional but recommended) |
| `tools/mailbox_autopilot.py` | Daily watchdog for campaign health | Automatic (launchd noon ET) |
| `tools/campaign_analyzer.py` | Kimi-powered post-run analysis + playbook brief | On-demand or when autopilot alerts |
| `tools/client_reports.py` | Weekly performance report per client | Automatic (launchd Friday 9am ET) |
| `tools/reply_triage.py` | Classify inbound replies + draft response in sender voice (Opus) | Run every 30 min or on-demand |
| `tools/meeting_prep.py` | Pre-call prospect brief (company intel + thread + pain points) | Before every booked meeting |
| `tools/deliverability_monitor.py` | Daily warmup trend + DNS health + bounce trend | Automatic (launchd daily) |
| `tools/rollback.py` | Restore deleted leads or sequences from backup files | On-demand when a bulk change needs reverting |
| `llm_router.py` | Central routing (Kimi for light, Claude for heavy) | Imported by other tools |

Plus helper scripts already in place: `doctor.py --fast`, `forge_dashboard.py`, `mailbox_helpers.py`, `onboard.py`, `launchd/install.sh`.

---

## 🚀 Standard Workflow: Launching a New Campaign

**Every new campaign follows these steps. No shortcuts.**

### 1. Run Forge

```bash
cd "~/agency-os"
python3 forge.py "find 1000 commercial HVAC contractors for client_c"
```

Forge outputs `smartlead_import.csv` at `01-Projects/<client>/lead-runs/<run-dir>/`.

### 2. Data Quality Check (MANDATORY — never skip)

```bash
python3 tools/data_quality_check.py \
    --csv 01-Projects/<client>/lead-runs/<run-dir>/smartlead_import.csv \
    --vertical trades    # or b2b or local
```

**Vertical matters** — thresholds differ:
- `b2b` (CLIENT_C, CLIENT_B, fractional roles): 80% first_name, 15% max generic emails
- `trades` (fire, HVAC, plumbing, roofing): 50% first_name, 40% max generic
- `local` (restaurants, churches, hotels): 40% first_name, 50% max generic

Exit code:
- `0` → PASS, safe to proceed
- `1` → WARN, review before uploading
- `2` → FAIL, **do not send as-is**. Re-run Forge with stricter filters or drop bad rows.

### 3. Personalization (Deep Research) — optional but recommended

Costs ~$0.003/lead in Serper fees. Covered by Kimi Allegretto subscription. Lifts reply rate from ~2-3% to ~5-8%.

```bash
python3 tools/deep_research.py \
    --csv 01-Projects/<client>/lead-runs/<run-dir>/smartlead_import.csv \
    --niche "commercial HVAC contractor" \
    --drop-unnamed \
    --min-confidence 0.60
```

Outputs `smartlead_import-personalized.csv` in the same directory. This is what you upload to Smartlead.

`--drop-unnamed` excludes leads with no `first_name` (prevents "Hey ," emails).

### 4. Copy drafting

**Option A — let Forge auto-generate (Opus 4, 80% A-grade success rate):**
Forge generates `campaign_copy.md` in the run directory automatically. Verify it's present and passes a read-through.

**Option B — if auto-gen failed OR copy doesn't look right, hand-draft in chat:**
Follow the Fire Alarm / Generator / HVAC pattern from 2026-04-20:
- **3-email sequence** (Email 1 A+B variants, Email 2 body-only, Email 3 breakup body-only)
- **19-point rubric** (14 original + 4 humanizer + 1 CLEAN STRUCTURE). Must hit A (18-19/19) before ship.
- The cold-email-writer skill enforces it. Always invoke it when drafting.

### 5. Upload to Smartlead (as DRAFT)

Create a new campaign in Smartlead UI or via API. **Do NOT call `POST /campaigns/{id}/status` with `status=START`.** Leave it `DRAFTED` for the operator to review.

If the copy uses personalization, the Email 1A template must contain `{{personalized_opener}}` merge field:
```
{Hey|Hi} {{first_name}},<br><br>
{{personalized_opener}}<br><br>
Reactive <vertical> work is easy to land. The planned retrofits...
```

### 6. Wait for the operator's approval

Surface campaign URL, lead count, and sample copy in chat. the operator reviews in Smartlead UI and manually clicks Start.

---

## 📱 What to Do When Autopilot Alerts Fire

Pushover notification arrives on the operator's phone. Format:

> 🚨 **Forge ALERT** (priority 1)
> ACTION NEEDED: [CLIENT_C] 'ClientC - Fire Alarm' — bounce_rate=4.00%. Review in Smartlead. https://app.smartlead.ai/app/email-campaign/3204297

### Response flow

1. **Tap the Smartlead URL** to see the campaign
2. **Decide the action:**
   - If it's a **bounce rate** alert → run `campaign_analyzer` to identify bad leads, then clean them, then resume
   - If it's a **low reply rate** alert → run `campaign_analyzer` to get Kimi's diagnosis, then either rewrite copy or pause
   - If it's a **dead campaign** alert (500+ sends, 0 engagement) → pause and re-evaluate
3. **Do NOT rely on autopilot to fix it** — autopilot only alerts, never mutates

### Campaign analyzer usage

```bash
python3 tools/campaign_analyzer.py --campaign <CID>
```

Writes markdown to `01-Projects/<client>/campaign_analyses/<campaign-slug>.md` with:
- Reply breakdown by category
- Objection themes Kimi extracted
- 3 specific recommendations for next iteration

---

## 📅 Automated Cadence (Running 24/7)

**Migrated to launchd on 2026-04-20** for sleep/wake reliability. If your Mac is asleep at noon, launchd catches up when it wakes. Cron would have silently skipped.

| Schedule | Job | Output |
|---|---|---|
| **Daily noon ET** | `mailbox_autopilot.py` | Pushover alerts for campaigns breaching thresholds |
| **Daily noon ET** | `deliverability_monitor.py` | Pushover alerts on warmup drops / DNS issues |
| **Friday 9am ET** | `client_reports.py` | Markdown reports at `01-Projects/<client>/reports/` |
| **Daily 8:03am** | `tools/daily_sync.sh` | DB backup to iCloud |

View launchd jobs:
```bash
launchctl list | grep rm.forge
```

Trigger manually (test without waiting for schedule):
```bash
launchctl start com.rm.forge.autopilot
launchctl start com.rm.forge.client-reports
```

Install / reinstall the launchd jobs:
```bash
bash 02-Areas/lead-pipeline/launchd/install.sh
```

Uninstall (revert to cron or disable):
```bash
bash 02-Areas/lead-pipeline/launchd/uninstall.sh
```

All logs at `02-Areas/lead-pipeline/logs/`:
- `autopilot_launchd.log` — cron-equivalent stdout/stderr
- `autopilot_events.jsonl` — autopilot event audit trail
- `deliverability_events.jsonl` — deliverability monitor audit
- `reply_triage_state.json` — which replies have been triaged already

---

## 🔧 Environment + Secrets (.env)

Located at: `~/agency-os/.env`

```
ANTHROPIC_API_KEY=...     # Claude API for heavy tasks (Opus 4 copy)
KIMI_API_KEY=sk-kimi-...  # Kimi Allegretto for light tasks + research
PUSHOVER_USER_KEY=u...    # the operator's phone identifier
PUSHOVER_APP_TOKEN=a...   # Forge Autopilot app token
SMARTLEAD_API_KEY=...     # Email infrastructure
SERPER_API_KEY=...        # Google search for Deep Research
BLITZ_API_KEY=...         # Lead discovery
ICYPEAS_API_KEY/SECRET/USER_ID=...  # Enrichment fallback
MILLIONVERIFIER_API_KEY=...  # Email verification primary
BOUNCEBAN_API_KEY=...     # Email verification secondary
HUNTER_API_KEY=...        # Optional verification
```

Kill switch: `FORGE_FORCE_CLAUDE=true` forces all light tasks back to Claude Haiku (Kimi bypass).

---

## 💰 Cost Model (as of 2026-04-20)

| Service | Monthly |
|---|---|
| Kimi Allegretto (all Haiku work + research + reports) | $40 |
| Anthropic API (Opus 4 copy pipeline) | ~$1-5 |
| Smartlead (email infra) | $94 |
| Blitz (lead discovery, unlimited) | $100 |
| Icypeas + MV + BB (verification) | ~$45 |
| Firecrawl (scraping) | $16 |
| InboxKit + Porkbun (mailboxes + domains) | ~$50 |
| Pushover (one-time $5 already paid) | $0 |
| **Total ops** | **~$346-350/mo** |

Downgrade path: if Kimi usage stays <15% of Allegretto quota for 30 days, downgrade to **Andante ($19)** → saves $21/mo. Measure via daily logs.

Scaling: at 3x volume (~75k sends/mo), Kimi stays at $40 while Anthropic would climb to $85-145/mo. The subscription is a cost ceiling.

---

## 🎯 Key Campaign Thresholds (Autopilot defaults)

- **Bounce rate > 3%** after 20+ sends → critical alert
- **Reply rate < 1%** after 200+ sends → medium alert
- **0 opens + 0 replies** after 500+ sends → high alert (dead campaign)
- **Cooldown:** 24 hours between repeat alerts on same campaign

Tune via CLI flags on `mailbox_autopilot.py`:
```bash
--bounce-threshold 2.5    # stricter
--reply-threshold 1.5     # stricter
--dead-campaign-sends 300 # more aggressive
```

---

## 🔍 Troubleshooting Decision Tree

### "Campaign has low reply rate"
1. Run `data_quality_check.py --campaign <CID>` — is lead data clean? If bad → fix lead data before blaming copy.
2. Run `campaign_analyzer.py --campaign <CID>` — what do the replies say? Are they off-topic (wrong vertical) or objecting (wrong pitch)?
3. If copy is the issue → rewrite using Kimi's recommendations, push new sequences, continue.
4. If vertical is the issue → pause + shelve that vertical.

### "Campaign has high bounce rate"
1. Pull lead list, re-run MV verification on queued leads:
   ```python
   # Pattern documented in logs/deleted_leads_*.json backup files
   ```
2. Delete non-"ok" leads from Smartlead
3. Resume campaign

### "Forge auto-copy keeps failing"
1. Check output log — which grader rules failed?
2. If "banned opener" — `pull_winning_copy_examples` may have issues. Verify Smartlead returns real copy (variants are handled as of 2026-04-20 fix).
3. If everything fails → hand-draft in chat using cold-email-writer skill + humanizer + 19-point grader.

### "Need to roll back a change"
- Sequences: check `logs/sequences_backup_*.json` — has full pre-change state
- Deleted leads: check `logs/deleted_leads_*.json` — has email + id + company for restoration
- Restore path: POST `/campaigns/{cid}/leads` with the backup data

---

## 📊 Active Client Context (as of 2026-04-20)

### ClientC (the operator — the user)
- B2B agency, targets firms/companies as clients
- Verticals: cost-seg, utility-audit, property-tax-appeal, telecom-audit, fire-protection, freight-audit, osha-compliance, elevator-inspection, sales-tax-recovery, rd-tax-credit, fractional-CFO/CRO, commercial-roofing, HVAC, generator, fire-alarm
- Pricing: $AGENCY_TIER retainer + $PER_MEETING_FEE
- Best-performing campaign: Utility Audit (2.05% reply rate)

### ClientA (Sender One)
- Fire protection / sprinkler inspection, Colorado-focused (Denver metro within 1 hour drive)
- Verticals: property management, churches, schools, hotels, restaurants, medical, assisted living, apartments
- Best-performing campaign: Churches (5.02% reply rate)
- Design-build capable (confirmed 2026-04-20): NICET Level IV designer + PE partner for stamped drawings

### ClientB (Sender Two)
- Cybersecurity / VCISO / SOC2 consulting
- Verticals: MSPs, fintech
- Best angles: "before they ask" (compliance), "deal stalling"

---

## 🧭 Starting a New Chat? Do This

1. **Open Claude Code** at `~/agency-os`
2. CLAUDE.md auto-loads — establishes the agency context
3. First message: "Review `02-Areas/lead-pipeline/SOP.md` and tell me the current state"
4. Claude reads this file, runs `doctor.py --fast`, reports: active campaigns, recent alerts, anything blocking

Fresh chats should take 30 seconds to spin up on full context.

---

## 🪤 Smartlead API — Hard-Won Gotchas

These cost real time to rediscover. All confirmed working as of 2026-04-20.

### Update lead's custom_fields within a campaign
```python
# CORRECT — email is REQUIRED in body
POST /campaigns/{cid}/leads/{lead_id}?api_key=...
{"email": "required@domain.com", "custom_fields": {"personalized_opener": "..."}}

# WRONG (404 or "email required"):
POST /leads/{lead_id}
PATCH /leads/{lead_id}
POST /leads/{lead_id}/update-lead-details
```

### Save sequences (requires snake_case, not camelCase)
```python
# CORRECT
{"sequences": [{"seq_number": 1, "seq_delay_details": {"delay_in_days": 0}, ...}]}

# WRONG — Smartlead sends camelCase back on GET but requires snake_case on POST
{"seq_delay_details": {"delayInDays": 0}}  # 400 error
```

### A/B variants live in sequence_variants, NOT email_body
When a sequence uses Variant A / Variant B testing, top-level `email_body` is empty. Real copy is in `sequence_variants[i].email_body`. Always check both:
```python
body = seq.get("email_body", "") or ""
if not body.strip():
    for v in seq.get("sequence_variants", []) or []:
        if (v.get("email_body") or "").strip():
            body = v["email_body"]
            break
```
(This gotcha caused the 2026-04-20 "empty winners" false alarm in `pull_winning_copy_examples()`.)

### Analytics counts are STRINGS, not integers
```python
# Always coerce — never divide raw
sent = int(analytics.get("sent_count", 0) or 0)
replies = int(analytics.get("reply_count", 0) or 0)
rate = replies / sent * 100 if sent else 0
```

### Mailbox daily-cap field is `max_email_per_day` on POST, reads back as `message_per_day` on GET
```python
# Update
POST /email-accounts/{id}?api_key=...
{"max_email_per_day": 20}        # CORRECT
{"message_per_day": 20}          # 400: "not allowed"

# Read
GET /email-accounts/  →  returns {"message_per_day": 20}
```

### Campaign statuses only: START, STOPPED, PAUSED
- Once a campaign has been STARTED, you CANNOT revert to DRAFTED via API. Only PAUSED.
- PAUSED means "no sending" — functionally the same as DRAFTED for review purposes.
- `POST /campaigns/{id}/status` with `{"status": "DRAFTED"}` → **400 error**.
- This is why we never call START until the operator approves — once started, the best we can do is PAUSE.

### Track settings format Smartlead rejects
```python
# REJECTED by API (400 "Invalid track_settings value")
{"track_settings": ["DONT_EMAIL_OPEN", "DONT_LINK_CLICK"]}
```
Set tracking manually in the Smartlead UI. Default: tracking OFF for deliverability.

### Campaign leads pagination
```python
# total_leads is in the response root, not per-page
GET /campaigns/{id}/leads?limit=100&offset=0
→ {"total_leads": 261, "data": [...], ...}
```
Paginate until `data` is empty or smaller than `limit`.

### Rate limiting behavior
- Soft rate limit ~20-30 req/sec — Smartlead returns 429 or 200 with empty body.
- Retry with exponential backoff (2s, 4s, 6s) + tolerate transient 500s.
- Batch deletes with 0.2-0.3s sleep between calls.

---

## 🔥 Mailbox 14-Day Maturity Rule (CODE-ENFORCED)

**Rule:** Mailboxes must be BOTH (a) 14+ days old AND (b) 100% warmup reputation before being assigned to a campaign. No exceptions.

**Why:** Smartlead's `warmup_reputation` can hit 100% in 2-3 days on a new mailbox, but the mailbox is still too young for production sending. Caught 2 baby mailboxes in a live campaign on 2026-04-18, forced the rule into code.

**Enforcement:** `tools/mailbox_helpers.py::pick_mature_mailboxes(client_key, count)` raises `MailboxPoolError` if not enough eligible mailboxes exist. Never bypass with manual assignment.

```python
from mailbox_helpers import pick_mature_mailboxes
mailboxes = pick_mature_mailboxes("client_c", count=5, min_age_days=14, min_warmup_pct=100)
```

When you run `onboard.py`, mailboxes provisioned today become eligible in 14 days. Plan ahead.

---

## 🏆 Known Winning Campaigns (Reply-Rate Proven, feeds Forge few-shot)

These campaigns are pulled as few-shot examples by `pull_winning_copy_examples()` when drafting new copy. When writing NEW copy for similar verticals, pattern-match on these.

| Client | Campaign | Reply rate | Sends | Opening angle |
|---|---|---|---|---|
| CLIENT_C | Utility Audit - 08apr2026 | **2.05%** | 1,026 | "Would it be crazy if new commercial accounts landed on your calendar without you chasing..." |
| CLIENT_A | Churches - 14APR2026 | **5.02%** | 279 | "When's your church's next insurance renewal? Denver carriers are flagging churches..." |
| CLIENT_A | Property Managers outside metro - 08Apr2026 | **4.2%** | 238 | "We're a Denver fire protection crew, been at it since 2009, and we'll do a free walkthrough..." |
| CLIENT_A | Restaurants outside of denver - 26Mar2026 | **3.49%** | 630 | "If a fire marshal walked into your kitchen this week, do you know what they'd flag?" |

**Pattern recognition:**
- The winners all lead with a SPECIFIC question that's either timely (insurance renewal, marshal visit) or curiosity-provoking ("Would it be crazy if...")
- Never "Most [industry] struggle with..." (banned opener)
- Short, 3-4 sentences max
- Specific geography or compliance anchor

---

## 📨 Warm Reply Workflow (handling inbound from prospects)

When a prospect replies to a cold email (like Bryce Jankowski on 2026-04-20), this is the flow.

### Step 1 — Pull the full thread

```bash
# Find the lead globally
python3 -c "
import requests, os
from dotenv import load_dotenv; load_dotenv('.env', override=True)
r = requests.get('https://server.smartlead.ai/api/v1/leads/',
                 params={'api_key': os.environ['SMARTLEAD_API_KEY'],
                         'email': 'prospect@domain.com'})
print(r.json())
"

# Pull the full message history for that lead within its campaign
GET /campaigns/{cid}/leads/{lead_id}/message-history
→ {"history": [{"type": "SENT"|"REPLY", "email_body": "...", ...}]}
```

### Step 2 — Read what they're actually asking

- Extract the human's question (strip quoted reply history)
- Understand their BUSINESS CONTEXT (what does their company do, what's their role)
- Identify the qualifying question vs the real objection

### Step 3 — Draft a reply, but **verify all claims with the client first**

**CRITICAL:** Before drafting specifics about the client's capabilities, ASK Sender One / Sender Two / the client:
- Do they actually do X? (e.g., design-build? PE stamps? NICET Level IV?)
- Is Y pain point real for their business?
- Would they send this reply themselves?

**Never invent client capabilities.** If in doubt, draft the safer/less-specific version.

### Step 4 — Plain-language confirmation message to client

When checking claims with Sender One/Sender Two, write in their voice:
- Short (under 170 words)
- No industry jargon unless they use it
- No em dashes
- 3-5 specific questions, one per bullet

Example from 2026-04-20 Bryce thread:
```
Hey, got a reply from Bryce Jankowski at PMG Development.
He's asking if you engineer fire systems on top of installing them.
Before I reply, I need to check 3 things with you:
1. Does CLIENT_A do full design-build on new construction?
2. Do you have a PE you already work with for stamped drawings?
3. You said "NICET-certified" — what level are your designers?
```

### Step 5 — Send on client's behalf (manual for now)

the operator pastes the final reply into Smartlead's UI for that thread.

### Step 6 — Let reply_triage agent handle most future replies

For ongoing thread management, `tools/reply_triage.py` now handles this flow automatically:

```bash
# Run on-demand or schedule via launchd
python3 tools/reply_triage.py --since 1
```

It:
1. Pulls all recent REPLY messages across all active CLIENT_C/CLIENT_A/CLIENT_B campaigns
2. Classifies each (hot / question / objection / oof / not_interested etc) via Kimi
3. Drafts a response in the correct sender voice via Claude Opus (matches Sender One's "operator-speak" or the operator's "direct" etc)
4. Writes the draft to `01-Projects/<client>/reply_drafts/{timestamp}_{email}.md` (or Trello card if TRELLO_* env vars are set)
5. the operator reads the draft, edits if needed, copies into Smartlead manually

**What it WON'T do:** auto-send. That's v2. For now every reply still requires the operator's eyes + a copy-paste.

### Step 7 — When a meeting is booked, prep the call

```bash
python3 tools/meeting_prep.py --email bjankowski@pmgdevelop.com
```

Outputs a 1-page markdown brief: company summary, what they said in thread, likely pain points, 3 opening questions, 1 thing NOT to do. 30-second read, materially better call outcomes.

---

## 🧪 Test Suite

**162/162 tests passing** as of 2026-04-20 (up from 91 at session start).

```bash
cd "~/agency-os"
/usr/local/bin/python3.13 -m pytest tests/ -q
```

New test files (2026-04-20):
| File | Tests | What it guards |
|---|---|---|
| `test_mailbox_autopilot.py` | 27 | Threshold evaluation, state persistence, Pushover stub, client classification |
| `test_data_quality_check.py` | 21 | Generic-email detection, CSV audit, vertical thresholds, evaluate logic |
| `test_winner_examples_filter.py` | 14 | Banned-opener detection, empty-body filter (regression for 2026-04-20 bug) |

If any test fails, investigate BEFORE shipping code changes.

---

## 🧭 Kimi Code API Gotchas

### Correct base URL
```python
# CORRECT (Kimi Code subscription, sk-kimi- keys)
ANTHROPIC_BASE_URL = "https://api.kimi.com/coding"
MODEL = "kimi-for-coding"

# WRONG (this is the public Moonshot API, different auth)
"https://api.moonshot.ai/anthropic"  # 401 with sk-kimi- keys
```

### Subscription tiers
- **Andante** 49 RMB/mo (~$7): 1x base = 300-1,200 calls/5hr window
- **Moderato** 99 RMB (~$14): 4x = 1,200-4,800
- **Allegretto** 199 RMB (~$40): **20x = 6,000-24,000** ← the operator's plan
- **Vivace**: higher tier

### Client whitelist
Kimi Code endpoint enforces a client whitelist. Our Anthropic Python SDK calls work as of 2026-04-20 but Moonshot could tighten at any time. If Kimi starts returning 401 "not whitelisted," set `FORGE_FORCE_CLAUDE=true` in .env and everything falls back to Claude Haiku.

### Model name
Always use `kimi-for-coding` — other model names (kimi-k2.5, kimi-k2.6, etc.) return 401 on the `/coding` endpoint.

---

## 🎚️ Deep Research Confidence Scoring

Kimi returns a confidence score (0.0-1.0) for each opener. Interpretation:

| Confidence | Meaning | Action |
|---|---|---|
| 0.80+ | Strong signal, specific + recent (e.g., news about a project) | Use |
| 0.60-0.79 | Decent signal, may be general (e.g., hiring trend) | Use |
| 0.40-0.59 | Weak signal, generic ("Saw X is hiring") | Drop |
| <0.40 | No real personalization found | Drop |

`--min-confidence 0.60` is the default — below this, opener is blanked and lead gets non-personalized email.

---

## 💾 Backup + Rollback

Every destructive operation writes a backup BEFORE executing. Check `02-Areas/lead-pipeline/logs/` for:

| File pattern | Contains | Restore path |
|---|---|---|
| `deleted_leads_{campaign}_{timestamp}.json` | Full lead records before deletion | `POST /campaigns/{cid}/leads` with the data |
| `sequences_backup_{campaign}_{timestamp}.json` | Pre-change sequence list | `POST /campaigns/{cid}/sequences` with the JSON |
| `autopilot_events.jsonl` | Every autopilot decision | Read-only audit trail |
| `deep_research_{campaign}_{timestamp}.csv` | Personalized openers generated | Re-inject via update-lead endpoint |

**⚠️ Rollback scripts not yet written — restore is manual if needed.** On the backlog.

If rolling back is urgent, the backup JSON has the exact shape the API needs — just POST it back.

---

## 🗂️ Campaign ID Quick Index (as of 2026-04-20)

Handy for fast lookups without pulling Smartlead every time.

| Campaign ID | Client | Vertical | Status |
|---|---|---|---|
| 3204297 | CLIENT_C | Fire Alarm | ACTIVE (cleaned) |
| 3206126 | CLIENT_C | Generator Install | ACTIVE (starts tomorrow) |
| 3206127 | CLIENT_C | Commercial HVAC | ACTIVE (starts tomorrow) |
| 3184163 | CLIENT_C | Property Tax Appeal | ACTIVE (post-cleanup, 73 leads) |
| 3170440 | CLIENT_C | FireProtection Apr 13 | ACTIVE (personalized) |
| 3197338 | CLIENT_C | Fractional CFO | ACTIVE |
| 3197337 | CLIENT_C | Fractional CRO | ACTIVE |
| 3197336 | CLIENT_C | Commercial Energy Efficiency | ACTIVE |
| 3190874 | CLIENT_C | Commercial Roofing | ACTIVE |
| 3183633 | CLIENT_C | Sales Tax Recovery | ACTIVE |
| 3183631 | CLIENT_C | Fire Protection Apr 15 | ACTIVE |
| 3183629 | CLIENT_C | OSHA Compliance | **PAUSED** (0% reply rate, shelved) |
| 3147070 | CLIENT_C | Utility Audit (top winner 2.05%) | ACTIVE |
| 3109789 | CLIENT_C | Cost Seg Firms II | ACTIVE |
| 3179241 | CLIENT_A | Churches (top winner 5.02%) | ACTIVE |
| 3181252 | CLIENT_A | Property Managers outside metro (4.2%) | ACTIVE |
| 3188798 | CLIENT_A | Medical Denver Metro | ACTIVE |

Pull fresh list anytime: `GET /campaigns/?api_key=...`

---

## 📝 Recent Changes (2026-04-20 session)

- ✅ Built 5 new tools (autopilot, analyzer, reports, deep_research, dq_check)
- ✅ Forge copy pipeline upgraded Sonnet 4 → Opus 4 (80% A-grade)
- ✅ Phase 1 Kimi wiring across Forge Haiku call sites
- ✅ Fixed `pull_winning_copy_examples()` to read Smartlead A/B variants
- ✅ Pushover alerts live (Allegretto subscription = $40/mo)
- ✅ Cron installed: autopilot daily noon ET, reports Friday 9am ET
- ✅ Cleaned 879 bad leads across 6 at-risk campaigns
- ✅ Property Tax Appeal + FireProtection Apr 13 recopy + personalized
- ✅ New policies: no auto-launch, no auto-pause, data-quality-first

## 📝 Recent Changes (2026-04-20 — second half of session, "top 5" build)

- ✅ Built 5 more tools (rollback, launchd migration, meeting_prep, deliverability_monitor, reply_triage)
- ✅ `rollback.py` — restore deleted leads or sequences from backup JSONs (tested on live backups)
- ✅ `launchd/install.sh` — migrated cron to launchd so jobs fire on wake-from-sleep (cron silently skipped when Mac asleep)
- ✅ `meeting_prep.py` — pre-call brief generator, hardened against hallucination (tested on Bryce thread)
- ✅ `deliverability_monitor.py` — daily warmup trend + DNS health + bounce trend (125 mailboxes + 66 domains)
- ✅ `reply_triage.py` — classifies replies via Kimi + drafts via Opus in sender voice; pushes to Trello or markdown
- ✅ **Tested reply triage on Bryce** — Opus generated a production-quality Sender One-voice reply in 3 seconds
- ✅ Warm reply workflow closed loop: Sender One's verified 2026-04-20 reply to Bryce converted → PMG added CLIENT_A to Buildertrend + requested live bid
- ✅ 162/162 tests passing (total count unchanged since new tools are runtime-only, not testable without Smartlead network)

---

## ✨ One-Liner Cheat Sheet

```bash
# Full campaign flow (in order):
python3 forge.py "find 1000 <niche> for <client>"
python3 tools/data_quality_check.py --csv <run-dir>/smartlead_import.csv --vertical <b2b|trades|local>
python3 tools/deep_research.py --csv <run-dir>/smartlead_import.csv --niche "<niche>" --drop-unnamed

# Upload enriched CSV → create Smartlead campaign as DRAFT → wait for the operator approval

# Manual investigation
python3 tools/campaign_analyzer.py --campaign <CID>
python3 tools/mailbox_autopilot.py --dry-run --verbose
python3 tools/data_quality_check.py --campaign <CID> --vertical <b2b|trades|local>

# Warm leads / replies
python3 tools/reply_triage.py --since 1              # triage replies from last day
python3 tools/reply_triage.py --lead-email X         # process one specific lead
python3 tools/meeting_prep.py --email <prospect>     # pre-call brief

# Infrastructure health
python3 tools/deliverability_monitor.py              # daily check + Pushover alerts
python3 tools/deliverability_monitor.py --dry-run    # report only, no alerts

# Recovery
python3 tools/rollback.py --list                          # all available backups
python3 tools/rollback.py --campaign <CID> --latest       # preview latest backup for campaign
python3 tools/rollback.py --file X.json --execute         # actually restore

# Scheduled jobs (launchd)
launchctl list | grep rm.forge                       # view scheduled jobs
launchctl start com.rm.forge.autopilot               # manual trigger
bash launchd/install.sh                              # (re)install
bash launchd/uninstall.sh                            # remove

# Status
python3 doctor.py --fast
tail -20 logs/autopilot_events.jsonl
tail -20 logs/deliverability_events.jsonl
```

*End of SOP.*
