# The Forge — Lead Generation Engine

**Production lead-generation system for ClientC (the agency) and its clients (ClientA, ClientB).** Built to find companies, discover who owns them, get verified emails, confirm niche fit, and export campaign-ready leads.

**System name:** The Forge
**Master DB:** master-leads/master_leads.db (SQLite, WAL mode)
**Python:** /usr/local/bin/python3.13

## 📖 Day-to-day usage: read `COOKBOOK.md`

For "I want to do X, run Y" workflows, see [`COOKBOOK.md`](./COOKBOOK.md). It's the goal-oriented entry point and the place to start any session.

The CLI is consolidated under one dispatcher: `f.py`. Add this alias to `~/.zshrc` once:

```bash
alias f='/usr/local/bin/python3.13 "~/agency-os/f.py"'
```

Then anywhere: `f` shows the menu, `f find ...` runs the full pipeline, `f score sequence.json` runs the offer scorecard, `f compound --client X --niche Y` mines winning angles, etc. Tools in `tools/` keep working at their original paths — `f` is just the discoverable front door so cron, launchd, and slash commands aren't disrupted.

Slash commands available in Claude Code: `/diagnose-campaign`, `/lookalike-research`, `/proposal`, `/new-client`, `/new-campaign`, `/today`.

## 🚨 READ FIRST — Operating Rules (2026-04-20)

**Every Claude Code session MUST enforce these rules. No exceptions.**

1. **Never call `START` on a Smartlead campaign without the operator's explicit approval.** Always create campaigns in `DRAFTED` state. the operator clicks Start in the Smartlead UI when he's reviewed. This applies to CLIENT_C, CLIENT_A, and CLIENT_B.
2. **Never auto-pause a running campaign.** Mailbox Autopilot alerts the operator via Pushover — he decides. `--auto-pause` is opt-in only.
3. **Forge owns end-to-end automation (updated 2026-04-23).** Forge now includes Phase 8 Campaign Launch via `forge_campaign_launch.py`. It auto-creates the Smartlead campaign, uploads leads, enforces ship gate (`paf_copy_gate`), uses humanized copy banks (`paf_copy_banks`), and attaches FREE mailboxes. Modifications to Forge core are allowed when they improve automation or quality AND preserve `--no-*` opt-out flags. Don't patch Forge for one-off experiments — use `tools/` for those.
4. **Always run `data_quality_check.py` before uploading to Smartlead.** Prevents the 2026-04-20 Property Tax Appeal disaster (79% unnamed leads, `Hey ,` greetings).
5. **Forge copy pipeline uses Claude Opus 4.** Do not revert to Sonnet — it hits 0% A-grade on the 19-point rubric.
6. **Light tasks route through `llm_router.get_light_client()` → Kimi K2.6.** Don't hardcode `anthropic.Anthropic()` for classification.
7. **🔥 14-Day Mailbox Maturity Rule (CODE-ENFORCED).** Never attach a mailbox to a campaign without going through `mailbox_helpers.pick_mature_mailboxes()` (or at minimum verifying both: `age_days >= 14` AND `warmup_reputation >= 100`). Smartlead's `warmup_reputation` can hit 100% in 2-3 days on a fresh mailbox — the rep is misleading; the mailbox is still too young for production cold sends. **Never call Smartlead's `POST /campaigns/{id}/email-accounts` directly.** See SOP.md "🔥 Mailbox 14-Day Maturity Rule" for the full mandate. Caught 22 baby mailboxes (3 days warmed) attached to live CLIENT_C campaigns on 2026-04-27 — same failure that originally forced this rule into code on 2026-04-18.
8. **Per-mailbox daily cap stays in 20-30 range.** Default sweet spot: 25/day. Going above 30 burns inboxes; going below 20 wastes capacity. Field name: `message_per_day` on `POST /email-accounts/{id}`.

**Full operating manual:** see `02-Areas/lead-pipeline/SOP.md`

## Start every session with

```bash
cd "~/agency-os"
cat SOP.md | head -80                              # refresh on policies + workflow
/usr/local/bin/python3.13 doctor.py --fast         # health check

# Optional — view pending alerts or recent autopilot activity
tail -20 logs/autopilot_events.jsonl 2>/dev/null
```

Fix any warnings before touching leads or campaigns.

---

## The Pipeline — How Everything Works

### You type one sentence:
```bash
/usr/local/bin/python3.13 forge.py "find me 1000 fire alarm companies for client_c"
```

> **IMPORTANT:** Always use `forge.py`, not `lead.py` or one-off scripts.
> forge.py is THE entry point — it handles discovery, enrichment, verification, and export in one command.
> Never write throwaway scripts to call Blitz/AI Ark/Serper directly.

### What happens automatically:

1. **Haiku parser** — detects client, niche, target, geography, discovery method
2. **Cascade discovery** — tries sources in cost order until target is met (uses POST-DEDUP count):
   - [1/4] Blitz keyword search ($0, B2B — correct payload: `{"company": {"keywords": {"include": [...]}, "hq": {"country_code": ["US"]}}}`)
   - [2/4] AI Ark lookalike (0.1 credits/10 companies — URL: `api.ai-ark.com`, NOT `api.aiark.io`)
   - [3/4] Firecrawl CRAWL + niche directory registry + Playwright for interactive dirs
   - [4/4] Serper Maps geo-grid fallback (local businesses)
3. **Dedup** — checks master DB (22,500+ leads) before spending enrichment credits
4. **forge_enrich.py** — 13-step enrichment cascade on every company:
   - Steps 1-8 are FREE: MX check → domain memory → Blitz phone lookup → Blitz email reverse → Google Maps email → Blitz direct → website scraping → owner search
   - Steps 9-13 are PAID (only if free fails): smart patterns → Icypeas reverse → Icypeas name+domain → Icypeas domain → catch-all acceptance
5. **Verification** — MV primary + BB second pass (with early exit)
6. **Quality** — LLM niche-fit check + title red flags + quality scoring (0-9)
7. **Export** — master DB + _master/ CSVs + Smartlead-ready format

### Smart routing per niche type:
- B2B professional (cost seg, M&A, CPAs) → Blitz keyword + AI Ark lookalike
- Trades (fire alarm, elevator, OSHA) → Firecrawl directories + Playwright
- Local business (restaurants, churches) → Serper Maps geo-grid

### Cost controls:
- $10 cost flag blocks enrichment runs over limit (FORGE_COST_LIMIT env var)
- Credit safeguards: 500 (info), 2,000 (soft-block), 5,000 (hard-block) on Serper
- Auto-center grid for 10+ cities (89% Serper savings)
- Icypeas only fires when free methods fail (~60% never touch it)
- MV stops on first valid email

---

## Common Requests

### "Find me N leads for client/niche"
```bash
/usr/local/bin/python3.13 lead.py "find me 1000 fire protection firms for client_c" --aiark --seeds "domain1;domain2"
```
Flags: `--force b2b|local`, `--aiark`, `--firecrawl`, `--no-fallback`, `--dry-run`, `--seeds`

### "Re-enrich old leads that didn't get emails"
```bash
/usr/local/bin/python3.13 tools/forge_enrich.py --input path/to/companies.csv --niche "fire protection" --client client_c
```
Uses all 13 enrichment steps on companies the old pipeline couldn't enrich.

### "What untapped leads do I have?"
```bash
/usr/local/bin/python3.13 -c "
import sqlite3
c=sqlite3.connect('master-leads/master_leads.db'); cur=c.cursor()
cur.execute(\"SELECT client,niche,COUNT(*) FROM leads WHERE status='new' GROUP BY client,niche ORDER BY client,COUNT(*) DESC\")
for r in cur.fetchall(): print(r)
"
```

### "Sync with Smartlead"
```bash
/usr/local/bin/python3.13 smartlead_sync.py
```

### "Which niche makes money?"
```bash
/usr/local/bin/python3.13 tools/meetings.py roi
```

### "Log a meeting"
```bash
/usr/local/bin/python3.13 tools/meetings.py log --email steve@abcfire.com --client client_c --niche fire-protection
/usr/local/bin/python3.13 tools/meetings.py close --email steve@abcfire.com --value 2500 --monthly 2500
```

### "Retry failed enrichments"
```bash
/usr/local/bin/python3.13 tools/enrich_retry.py --input companies.csv --retry-all
```

### "Check enrichment routing before running"
```bash
/usr/local/bin/python3.13 tools/enrich_smart_route.py --input companies.csv --niche "cost segregation" --dry-run
```

### "Find second contacts at top companies"
```bash
/usr/local/bin/python3.13 tools/enrich_second_contact.py --niche "fire protection" --client client_c --dry-run
```

### "Scrape a state licensing database"
```bash
/usr/local/bin/python3.13 tools/scrape_state_licenses.py --list-states
/usr/local/bin/python3.13 tools/scrape_state_licenses.py --state texas --niche "fire alarm"
```

### "Clean a Smartlead campaign"
Pause → fetch leads → MV+BB verify → delete bad → resume. Use Smartlead API:
```python
KEY = os.environ['SMARTLEAD_API_KEY']
BASE = 'https://server.smartlead.ai/api/v1'
requests.post(f'{BASE}/campaigns/{cid}/status?api_key={KEY}', json={'status':'PAUSED'})
# ... verify + delete bad leads ...
requests.post(f'{BASE}/campaigns/{cid}/status?api_key={KEY}', json={'status':'START'})
```

---

## Tools Reference

### Core Pipeline
| Script | Purpose |
|---|---|
| `forge.py` | **THE entry point** — lead-gen pipeline orchestrator (copy pipeline uses Opus 4 as of 2026-04-20) |
| `lead.py` | Natural language router with cascade discovery (parse + keyword gen route via llm_router → Kimi) |
| `llm_router.py` | **Central LLM routing** — Kimi K2.6 for light tasks, Claude Opus 4 for heavy copy tasks |
| `lead_generator_v2.py` | B2B pipeline (Blitz + AI Ark) |
| `lead_pipeline_v6.py` | Local business pipeline (Serper Maps) |
| `enrichment.py` | Email enrichment waterfall |
| `verification.py` | MV + BB verification (with early exit) |
| `export.py` | Smartlead-ready CSV export |
| `master_db.py` | SQLite master database |
| `smartlead_sync.py` | Campaign sync + lead status |
| `doctor.py` | 7-category health check |
| `config.py` | Central configuration |

### Agency Ops Tools (2026-04-20 builds)
| Script | Purpose | Cadence |
|---|---|---|
| `tools/data_quality_check.py` | **MANDATORY pre-send audit.** first_name %, generic email %, dupes, MV status. Vertical-aware thresholds (b2b / trades / local). | Before every Smartlead upload |
| `tools/deep_research.py` | Per-prospect Kimi-powered personalization opener. Use `--drop-unnamed --min-confidence 0.60`. | After Forge, before upload |
| `tools/mailbox_autopilot.py` | Daily watchdog. Alert-only by default, auto-pause opt-in via `--auto-pause`. | **launchd daily noon ET** |
| `tools/deliverability_monitor.py` | Warmup trend + DNS + bounce trend watchdog across all 125+ mailboxes / 66+ domains. | **launchd daily noon ET** |
| `tools/campaign_analyzer.py` | Kimi-powered post-run analysis. Reply classification + objection themes + 3 recommendations. | On-demand or when autopilot alerts |
| `tools/client_reports.py` | Weekly per-client performance report (markdown, Kimi narrative). | **launchd Friday 9am ET** |
| `tools/reply_triage.py` | Classify inbound replies via Kimi + draft response in sender voice via Claude Opus. Writes to Trello or markdown file. | Run every 30 min or on-demand |
| `tools/meeting_prep.py` | Pre-call brief: company intel + thread + pain points + opening questions. Hardened against hallucination. | Before every booked meeting |
| `tools/rollback.py` | Restore deleted leads or sequences from backup JSONs. Dry-run by default. | On-demand to revert bulk changes |
| `launchd/install.sh` | Migrate cron scheduled jobs → launchd so they fire on wake-from-sleep (cron silently skips asleep Macs). | One-time setup |

### The Forge Tools (tools/)
| Script | Purpose |
|---|---|
| `forge_enrich.py` | **Unified 13-step enrichment engine** |
| `enrich_owner_search.py` | Google + Haiku owner name discovery |
| `enrich_retry.py` | Retry failed enrichments from cache |
| `enrich_second_contact.py` | Find 2nd decision maker (Blitz employee finder first, Serper fallback) |
| `enrich_smart_route.py` | MX-based domain routing + niche directory registry |
| `enrich_from_blitz_cache.py` | Backfill industry data |
| `mv_bulk_verify.py` | Batch MV verification (upload CSV) |
| `dedup_before_enrich.py` | Pre-enrichment dedup (domain + company name) |
| `discover_google_to_blitz.py` | Google search → Blitz enrichment |
| `verify_niche_fit.py` | LLM niche-fit (name-based) |
| `verify_niche_fit_website.py` | Website content verification |
| `verify_niche_fit_blitz.py` | Blitz LinkedIn verification |
| `verify_combine.py` | Consensus combiner (2-of-3 rule) |
| `verify_title_redflags.py` | Bad title regex filter |
| `llm_classify.py` | Haiku company classifier |
| `scrape_directory_playwright.py` | Interactive directory scraper |
| `scrape_state_licenses.py` | State licensing database scraper (10 states) |
| `signal_job_postings.py` | Hiring intent signals |
| `signal_permits.py` | Building permit signals |
| `meetings.py` | Meeting + deal tracking + ROI reports |
| `backup_db.sh` | iCloud backup (keeps last 30) |
| `daily_sync.sh` | Cron automation (8am daily) |

---

## Data Integrity Rules (ENFORCED)

1. **`sent_date` is write-once.** Never modify after first set.
2. **Trust `mv_result`, not `verified`.** Verified column is legacy.
3. **Always backup before bulk writes.** Run `./tools/backup_db.sh`
4. **Never commit master DB or API keys.** Already gitignored.
5. **`city_source` must be populated** when writing city.
6. **Pre-enrich dedup is mandatory.** Always check master DB before spending credits.
7. **Cost flag at $10.** forge_enrich blocks runs over FORGE_COST_LIMIT.

---

## forge_enrich.py — 13-Step Enrichment Cascade

```
FREE (steps 1-8):
  1.  MX pre-check              skip dead domains
  2.  Domain memory              known winning pattern (11,466 domains tracked)
  3.  Blitz phone lookup         reverse phone → owner + LinkedIn → email
  4.  Blitz email reverse        info@ → who manages it → personal email
  5.  Google Maps email          business email from Maps profile
  6.  Blitz direct               employee email + format inference
  7.  Website scraping           contact page emails (/, /contact, /about)
  8.  Owner search               Google + Haiku for name ($0.002)

PAID (steps 9-13, only if free fails):
  9.  Smart patterns             MX-type + Blitz-format routed ($0.001 MV)
  10. Icypeas reverse            different DB than Blitz ($0.015)
  11. Icypeas name+domain        accurate finder ($0.015)
  12. Icypeas domain-only        any email at domain ($0.015)
  13. Catch-all acceptance       firstname@ guaranteed delivery (FREE)
```

Auto-triggers in cascade when Blitz enrichment rate < 30%.

### Speed Optimizations (April 2026)
- **Parallel processing:** 5 concurrent workers (ThreadPoolExecutor). Cuts 8hr runs → 1.6hrs.
  - Configurable: `--workers 10` or `FORGE_WORKERS=10` env var
  - Set `--workers 1` for sequential (debugging)
- **MV verification cache:** In-memory cache prevents re-verifying the same email across threads/steps
- **MX result cache:** `dig` results cached in memory per domain — no duplicate DNS lookups
- **Website scraping:** Reduced to 3 paths (/, /contact, /about) with 4s timeout (was 5 paths, 6s)
- **MV Bulk API:** `verify_mv_batch()` available for batch pattern testing (uses `mv_bulk_verify.py`)
- **Thread-safe stats:** All counters use locks — safe for parallel workers

---

## Niche Directory Registry

Known industry directories per niche (in enrich_smart_route.py NICHE_DIRECTORIES):

| Niche | Directories |
|---|---|
| cost segregation | ASCSP, KBKG Rankings |
| fire protection | fireinspectiondirectory.com (50 states) |
| MSPs | Cloudtango, MSP Database (51 states), MSP Companies |
| elevator inspection | NAEC |
| OSHA compliance | ASSP |

Firecrawl CRAWL hits these automatically. Playwright handles interactive ones.

---

## API Endpoints in Use

### Blitz ($100/mo unlimited) — 10 endpoints
- `/search/companies` — keyword discovery
- `/enrichment/domain-to-linkedin` — domain → LinkedIn URL
- `/enrichment/company` — full company data
- `/search/waterfall-icp-keyword` — find best decision maker
- `/enrichment/email` — email from LinkedIn URL
- `/enrichment/phone-to-person` — reverse phone → owner
- `/enrichment/email-to-person` — reverse email → person
- `/search/employee-finder` — all employees at company
- `/search/people` — bulk people search
- `/account/api-key-details` — credit check

### Icypeas (~$40/mo) — 4 endpoints
- `/api/email-search` — find email by name + domain
- `/api/domain-search` — find emails at domain (via enrichment.py)
- `/api/reverse-email-lookup` — email → person identity
- `/api/bulk-single-searchs/read` — poll for results

### Smartlead ($94/mo) — key endpoints
- `GET /campaigns/` — list campaigns
- `GET /campaigns/{id}/leads` — get leads (paginate)
- `POST /campaigns/{id}/leads` — bulk add leads
- `DELETE /campaigns/{id}/leads/{lead_id}` — delete lead
- `POST /campaigns/{id}/status` — pause/resume (PAUSED/START)
- `DELETE /campaigns/{id}/email-accounts` — unlink mailboxes
- `GET /email-accounts/` — list mailboxes + warmup scores

---

## Cold Email Rules (enforce on all drafts)

- Under 80 words per email
- No em dashes
- No "we help" openers
- No `{{company_name}}` variable
- Full spintax — greeting, body, CTA, signature
- Single hard CTA per email
- Performance/risk-free positioning
- Timeline hooks > problem hooks
- Plain text only, no links/images/tracking
- Sequence: Email 1 + Day-3 follow-up (question not reminder) + Day-7 graceful exit
- Campaign naming: `{Client} - {Target} - DDMMMYYYY`

---

## Client Context

### ClientA (Sender One)
- Fire protection / sprinkler inspection service, Colorado-focused
- Verticals: property management, churches, schools, hotels, restaurants, medical, assisted-living, apartments
- Strip fire-protection COMPETITORS from any list
- Service guarantees: 24hr response, no travel fees, one vendor statewide

### ClientC (the operator — the user)
- B2B agency, targets firms/companies as clients
- Niches: cost-segregation, utility-audit, property-tax-appeal, telecom-audit, fire-protection, freight-audit, osha-compliance, elevator-inspection, sales-tax-recovery, rd-tax-credit
- Pricing: $AGENCY_TIER retainer + $PER_MEETING_FEE

### ClientB (Sender Two)
- Cybersecurity / VCISO / SOC2 consulting
- Niches: MSPs, fintech
- Best angles: "before they ask" (compliance questions from clients), "deal stalling"

---

## Plugins

- **Superpowers** — general performance
- **Firecrawl** — web scraping + directory crawling ($16/mo Hobby)
- **Context7** — live documentation for coding
- **Playwright** — browser automation for interactive sites (free)
- **Brand Voice** — content consistency

---

## Dotenv Quirk

Claude Code's shell sets `ANTHROPIC_API_KEY=""` (empty). Use `not os.environ.get(k)` instead of `k not in os.environ` when loading env files.

---

## When in doubt

1. Run `doctor.py --fast`
2. Check master DB directly
3. Read this file
4. Don't write new scripts when existing ones do the job
5. Cheapest tool first, always
