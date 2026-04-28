# Forge Cookbook — "I want to ___, run this"

The single goal-oriented entry point for the whole ClientC agency stack. Pairs with `f.py` (the front-door dispatcher) so you don't have to remember individual tool paths.

> **One-time setup.** Add this to your `~/.zshrc` or `~/.bashrc`:
> ```
> alias f='/usr/local/bin/python3.13 "~/agency-os/f.py"'
> ```
> Then `f` shows the full menu and `f <command>` runs it.

---

## Quick Reference (the 12 commands you'll actually use)

| Goal | Command |
|---|---|
| Show all commands | `f` |
| Search commands | `f --search mailbox` |
| Find leads end-to-end | `f find "1000 fire alarm companies for client_c"` |
| Score offer strength | `f score sequence.json` |
| 18-point writing gate | `f gate sequence.json` |
| Score list quality | `f score-list --csv leads.csv --client X` |
| Lookalike research | `f lookalike prep --seed-niche X --target-niche Y --client Z` |
| LinkedIn engager harvest | `f engagers --client X --niche Y --discover-competitors 10` |
| Mine past winners | `f compound --client X --niche Y` |
| Autonomous campaign loop | `f auto-research --client X --niche Y` |
| Sort CSV non-SEG-first | `f seg-sort --input leads.csv` |
| Daily snapshot | `f today` |
| Health check | `f health` |
| Diagnose a campaign | `/diagnose-campaign <campaign_id>` (slash command) |
| Weekly operational rhythm | `/weekly-rhythm` (slash command) |
| Generate a proposal | `/proposal` (slash command) |

---

## I want to launch a new campaign

The full path from "I have an idea" to "campaign DRAFTED in Smartlead":

```bash
# 1. Optional: pull winning-angles brief so the prompt knows what's worked
f compound --client client_a --niche restaurants
# → writes winning-angles/client_a-restaurants-YYYYMMDD.md

# 2. Run the full Forge pipeline
f find "find me 200 restaurants in denver for client_a"
# → discovers, enriches, verifies, generates copy, gates with paf_copy_gate

# 3. Optional: layer the strategic offer scorecard on top of the writing gate
f score 02-Areas/lead-pipeline/01-Projects/client_a/restaurants/sequence.json

# 4. Optional: sort the lead CSV non-SEG-first before upload (deliverability play)
f seg-sort --input ...exported.csv

# 5. Forge auto-runs Phase 8 launch — DRAFTED in Smartlead, mailboxes attached
#    YOU MANUALLY click START in Smartlead UI per the operating rule
```

**Hard rules** (from `02-Areas/lead-pipeline/CLAUDE.md`):
- Never auto-START a campaign — DRAFTED only
- Never auto-pause a running campaign — autopilot alerts only
- Always run `f audit-data` before upload (prevents the 2026-04-20 unnamed-leads disaster)
- Heavy copy stays on Claude Sonnet 4 (paf_copy_gate enforces); never revert to Haiku

---

## I want to find lookalike companies for a niche

3-stage workflow because the subagent dispatch (Stage 2) needs to live inside an interactive Claude Code session to use the subsidized web-search subagents.

```bash
# Stage 1 — Python: pull seeds, extract signal profile, write spec
f lookalike prep \
    --seed-niche paf-medical-denver \
    --target-niche paf-assisted-living-denver \
    --client client_a \
    --geo "Denver metro" \
    --n-agents 20

# Stage 2 — Inside Claude Code:
/lookalike-research <run_id>

# Stage 3 — Python: dedupe results, ingest into master_leads.db
f lookalike ingest --run-id <run_id>

# Anytime: see status of all runs
f lookalike status
```

---

## I want to score an offer's strength before I ship

`f gate` enforces 18 tactical writing rules (humanizer + word count + format + spintax). `f score` is the new strategic layer — does the OFFER itself work on cold traffic?

```bash
# Tactical writing rubric (must hit 14+/18 to ship)
f gate sequence.json

# Strategic offer scorecard (Oliverify 10-component, target B+ to ship)
f score sequence.json

# Combine both — only ship if both pass
f gate sequence.json && f score sequence.json
```

Score interpretation:
- **A (45-50/50)** — launch
- **B (35-44/50)** — acceptable, layer in top-3 improvements
- **C (25-34/50)** — rewrite the offer, not just the copy
- **D (<25/50)** — back to the drawing board

---

## I want to diagnose a campaign that's not performing

In Claude Code, run the slash command:

```
/diagnose-campaign <smartlead_campaign_id>
```

It walks the 4-point top-down framework (Deliverability → Targeting → Offer → Speed-to-Lead), pulls real data via Smartlead MCP, and emits a verdict + concrete fixes. Stops at the first failed layer because layers below a failure are noise.

Healthy thresholds (Oliverify's 2.27M-email dataset):
- Reply rate ≥ 2.5% — healthy
- Reply rate < 1% — deliverability problem
- Bounce rate ≤ 2% — healthy
- Bounce rate ≥ 3% — pause and investigate
- Booking rate (positive→meeting) — 20-40%
- Show rate — 75-80%

---

## I want to learn from past campaigns to seed the next one

```bash
# See every (client, niche) combo with positive meetings
f compound --list

# Generate a winning-angles brief
f compound --client client_a --niche restaurants
# → 02-Areas/lead-pipeline/winning-angles/client_a-restaurants-YYYYMMDD.md
```

The brief tells you which industries / titles / states actually converted. Inject it into the prompt for the NEXT campaign of that client+niche so the offer leans toward what worked. This is the compounding-data feedback loop.

---

## I want to grade a list before I send

`f audit-data` checks **integrity** (first-name %, dupes, MV status, generic-email %). `f score-list` checks **signal quality** across 8 dimensions vs your declared ICP and emits a letter grade A-F. Run both before upload.

```bash
# Default — grade against client's declared ICP from CLIENT.md
f score-list --csv path/to/leads.csv --client client_a

# Explicit ICP titles + industries (overrides CLIENT.md)
f score-list --csv leads.csv \
    --icp-titles "Owner,GM,Operations Manager" \
    --icp-industries "fire protection,sprinkler,life safety"

# Stop the pipeline if grade < B
f score-list --csv leads.csv --client client_a --min-grade B
# Exit codes: 0 = pass, 1 = one tier under, 2 = two+ tiers under
```

Grade map: A+/A (≥90) ship · B (80-89) minor fixes · C (70-79) fix top 3 · D (60-69) serious cleanup · F (<60) rebuild. Reports save to `list-scorecards/`.

---

## I want to harvest LinkedIn engagers from competitor posts

The "competitor-engagers" lane: people who like or comment on competitor company posts are warm. Harvest them, enrich, drop into Forge.

```bash
# Auto-discover competitors via web search (needs SERPER_API_KEY)
f engagers --client client_a --niche fire-protection \
    --discover-competitors 10 --geo "Denver metro"

# Explicit competitor URL list
f engagers --client client_b --niche msps \
    --competitors competitor_urls.txt --posts 30

# Dry run — review competitor list before scraping
f engagers --client X --niche Y --discover-competitors 5 --dry-run
```

Needs `RAPIDAPI_KEY` in workspace `.env` (subscribe to "Realtime LinkedIn Bulk Data" on RapidAPI). Output is a Forge-ready CSV with `signal_type='competitor_engagement'` and a JSON payload of which competitor / which post each lead engaged with.

Recommended next chain:
```bash
f enrich --input <engagers.csv> --niche <X> --client <Y>
f verify-niche --input <enriched.csv>
f score-list --csv <verified.csv> --client <Y>
```

---

## I want to run an autonomous campaign loop

End-to-end: past winners → lookalikes → discover → score list → copy → gate → DRAFTED launch.  Per Forge rule, **stops at DRAFTED** — never auto-STARTs.

```bash
# Interactive (recommended) — pauses for approval between phases
f auto-research --client client_a --niche restaurants --target 200

# With a lookalike phase (if the seed niche has converters in master DB)
f auto-research --client client_a --niche assisted-living \
    --seed-niche medical --geo "Denver metro" --n-agents 20

# Resume a previous run (e.g. after the cold-email-writer step)
f auto-research --resume client_a-restaurants-20260427-1530

# Cron / unattended — stops at the copy phase (writer needs interactive Claude)
f auto-research --client X --niche Y --unattended --target 200
```

Run dirs at `auto-research-runs/{client}-{niche}-{date}/` contain everything: winners brief, lookalike spec, leads CSV, scorecard, sequence YAML, gate report, launch result, full stderr.

---

## I want to onboard a new client (mailboxes + project folder)

```bash
# 1. Buy 20 domains in Porkbun manually (~10 min)

# 2. Run onboarding (creates DNS records, mailboxes, attaches to Smartlead)
f onboard --client client_b --domains domains.txt
# Or for Microsoft mailboxes:
f onboard-msft --client client_b --domains domains.txt

# 3. Check current state
f onboard --client client_b --check
f onboard --audit  # health check across all clients

# 4. Generate a sales proposal after the discovery call
/proposal
# (in Claude Code — generates Gamma deck from sales call transcript)
```

Mailboxes auto-warmup for 14 days. Day 14 = ready for cold sends.

---

## I want a daily/weekly pulse

```bash
f today          # daily snapshot
f dashboard      # live dashboard
f status         # current pipeline state
f client-report  # weekly per-client (also runs Friday 9am via launchd)
f health         # 7-category health check (run start of every session)
```

Slash command alternative:
```
/today           # in Claude Code
```

---

## I want to keep deliverability healthy

```bash
f autopilot         # daily mailbox watchdog (alert-only)
f autopilot --auto-pause  # opt-in: actually pause bad mailboxes
f monitor           # DNS + bounce trend across all 125+ mailboxes
f seg-sort --input leads.csv  # non-SEG (Google/MSFT) first, SEG last
f health            # full doctor.py --fast
```

The daily autopilot + monitor jobs run via launchd at noon ET. See `launchd/install.sh` if they're not firing.

---

## I want to handle inbound replies

```bash
f triage             # classify replies via Kimi + draft response in sender voice
f triage-notify      # Pushover/Slack notification on every positive reply
f prep --email <addr>  # pre-call brief before a booked meeting
f meetings log --email <addr> --client X --niche Y       # log a booked meeting
f meetings close --email <addr> --value 2500 --monthly 2500  # log a closed deal
f meetings roi       # which niches actually make money
```

---

## I want to manage the master DB safely

```bash
f backup             # backup master_leads.db before bulk writes
f rollback           # restore deleted leads/sequences from backups
```

**Hard rules:**
- `sent_date` is write-once. Never modified after first set.
- Trust `mv_result`, not `verified` (verified column is legacy).
- Always `f backup` before any bulk write.
- Pre-enrich dedup is mandatory (built into Forge).

---

## I want to do my job-search outreach (the operator-internal)

```bash
# Use lookalike-research to find target companies
f lookalike prep \
    --seed-niche the-vector-seeds \
    --target-niche the-vector-d100 \
    --client client_c \
    --geo "United States" \
    --n-agents 15

# Generate a per-prospect landing page
python3 tools/d100_landing_page.py --client the-vector --prospect <domain>

# Dream 100 framework lives at:
01-Projects/the-vector/dream-100.md
01-Projects/the-vector/CLIENT.md
```

---

## Tool ↔ Slash Command Cross-Reference

| What you want to do | CLI (`f`) | Slash command |
|---|---|---|
| Diagnose a campaign | — | `/diagnose-campaign <id>` |
| Lookalike research | `f lookalike` | `/lookalike-research <run_id>` |
| Generate a proposal | — | `/proposal` |
| New client setup | `f onboard` | `/new-client` |
| New campaign scaffold | — | `/new-campaign` |
| Daily plan | `f today` | `/today` |

---

## Where everything lives

```
02-Areas/lead-pipeline/
├── f.py                      # Front-door dispatcher (this cookbook)
├── COOKBOOK.md               # This file
├── CLAUDE.md                 # Operating manual (loaded into every session)
├── SOP.md                    # Standard operating procedures
├── WORKFLOW.md               # Pipeline workflow doc
│
├── forge.py                  # Core pipeline orchestrator (don't modify)
├── forge_campaign_launch.py  # Phase 8 launch (Forge core)
├── llm_router.py             # Central LLM routing (Kimi for light, Sonnet for heavy)
├── master-leads/master_leads.db  # SQLite, 225K+ leads (gitignored)
│
├── tools/                    # All the focused tools — f.py routes to these
│   ├── score_offer.py        # NEW: 10-component offer scorecard
│   ├── seg_aware_sort.py     # NEW: SEG-aware lead CSV sort
│   ├── forge_compound.py     # NEW: winning-angles brief generator
│   ├── forge_lookalike_research.py  # NEW: 3-stage lookalike pipeline
│   ├── paf_copy_banks.py     # 9 niches × spintax copy banks + PLASCENCIA + NOWOSLAWSKI
│   ├── paf_copy_gate.py      # Tactical writing gate (humanizer + 18-point)
│   └── ... (~50 more tools)
│
├── lookalike-runs/           # Lookalike-research run dirs
├── winning-angles/           # forge_compound briefs
└── logs/                     # Autopilot, deliverability, etc.

.claude/commands/             # Slash commands
├── diagnose-campaign.md      # NEW: 4-point Oliverify diagnostic
├── lookalike-research.md     # NEW: subagent dispatcher for Stage 2
├── proposal.md
├── new-client.md
├── new-campaign.md
└── today.md

01-Projects/                  # Active client work
├── client_a/CLIENT.md   # CLIENT_A (Sender One, Denver fire protection)
├── client_b/CLIENT.md  # CLIENT_B (Sender Two, MSP cyber/VCISO)
├── client_c/CLIENT.md # CLIENT_C (the operator's agency)
└── the-vector/           # the operator's job-search project
    ├── CLIENT.md
    └── dream-100.md
```

---

## Operating rules (always)

1. **Never auto-START a Smartlead campaign.** Always DRAFTED. the operator clicks Start in the UI.
2. **Never auto-pause a running campaign.** Autopilot alerts only. the operator decides.
3. **Always backup before bulk DB writes.** `f backup`
4. **Always run `f audit-data` before upload.** Prevents the unnamed-leads disaster.
5. **Heavy copy stays on Claude Sonnet 4.** Don't route copy generation to Kimi.
6. **Forge core is hands-off** for one-off experiments. Use `tools/`.
7. **Light tasks route through `llm_router.get_light_client()`** → Kimi K2.6.

Full operating manual: `02-Areas/lead-pipeline/CLAUDE.md` + `SOP.md`.
