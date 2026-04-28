# Lead-Pipeline Workflow — The Full Campaign Lifecycle

How the research/copy/execution tools connect. Use this as the session playbook when starting a new vertical.

---

## The Pipeline at a Glance

```
┌─────────────────────────────────────────────────────────────────┐
│                                                                 │
│  New vertical idea                                              │
│       │                                                         │
│       ▼                                                         │
│  ┌─────────────────────┐   SKIP / TEST / GO                     │
│  │  niche_research.py  │──▶ + TAM + DM sample + awareness       │
│  │  (decision support) │    + discoverability score             │
│  └─────────────────────┘                                        │
│       │                                                         │
│       ▼ (if GO or TEST)                                         │
│  ┌─────────────────────────┐                                    │
│  │  framework_variants.py  │──▶ 4 copy hypotheses across        │
│  │  (A/B exploration)      │    awareness × lead-type combos    │
│  └─────────────────────────┘                                    │
│       │                                                         │
│       ▼ (pick 2 variants to test — or skip if vertical is       │
│         familiar and you have proven copy already)              │
│  ┌─────────────────────────┐                                    │
│  │  /cold-email-writer     │──▶ A-grade production copy         │
│  │  (production pipeline)  │    (18-point rubric + humanizer)   │
│  └─────────────────────────┘                                    │
│       │                                                         │
│       ▼ (optional strategic check)                              │
│  ┌─────────────────────────┐                                    │
│  │  framework_audit.py     │──▶ 27-point framework scorecard    │
│  │  (strategic QA)         │    (complements tactical rubric)   │
│  └─────────────────────────┘                                    │
│       │                                                         │
│       ▼                                                         │
│  ┌─────────────────────────┐                                    │
│  │  Forge → Smartlead      │──▶ 50-lead seed or 500-1000 scale  │
│  │  (execution)            │                                    │
│  └─────────────────────────┘                                    │
│       │                                                         │
│       ▼ (after 5-7 days of sending)                             │
│  ┌─────────────────────────┐                                    │
│  │  campaign_analyzer.py   │──▶ Post-mortem + framework         │
│  │  (learning loop)        │    diagnostics + prediction log    │
│  └─────────────────────────┘    actual reply rate backfilled    │
│                                 into _predictions.jsonl         │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

---

## Step-by-step (the decision tree)

### Step 1 — Is this vertical worth investigating at all?

Run `niche_research.py`:

```bash
/usr/local/bin/python3.13 tools/niche_research.py \
  --niche "commercial HVAC contractors" \
  --client client_c
```

Optional flags:
- `--deep` — Firecrawl full-page scrape of top pain-point articles (+$0.60, +30s)
- `--dry-run` — show what would happen without spending
- `--no-log` — skip writing to prediction log (testing only)

**Interpret the output:**

| Multi-signal score | Action |
|---|---|
| **<40** | SKIP — market too small / saturated / wrong DM fit / discoverability too low |
| **40-70** | TEST_50_LEADS — plausible but risky; seed test before scaling |
| **>70** | GO — strong signals, proceed to copy generation |

**Key numbers to watch:**
- **TAM count (BLS QCEW)** — if <500 establishments, niche probably too thin for sustainable sending
- **Blitz coverage %** — <10% of true TAM means Blitz will hit list-exhaustion fast; consider Serper Maps for discovery
- **Discoverability rate** — <40% means Forge will produce garbage leads (Property Tax Appeal disaster)
- **first_name rate** — <60% means "Hey ," greeting problems; MUST run data_quality_check before upload

### Step 2 — Generate copy hypotheses (ONLY for unfamiliar verticals)

If the vertical is already covered by the `/cold-email-writer` skill's `EXAMPLES.md` or you have a proven winner from past campaigns, **skip this step**. Your existing patterns are proven.

For new verticals, run `framework_variants.py`:

```bash
/usr/local/bin/python3.13 tools/framework_variants.py \
  --niche "commercial HVAC contractors" \
  --client client_c \
  --what-we-sell "cold email lead gen — find facility managers, book qualified meetings"
```

Output: 4 hypothesis variants in `03-Resources/framework-variants/`. Each is deliberately different (different awareness level × lead type combinations) so A/B testing reveals which framework position actually resonates.

**Critical:** these are RESEARCH output, not production copy. Don't ship them directly. Pick 2 winners, then go to Step 3.

### Step 3 — Generate production copy

Invoke the `/cold-email-writer` skill (in Claude Code):

> "Write a cold email sequence for ClientC → commercial HVAC contractors. Pain point: [the winning pain from framework_variants winner]. CTA: 'Open to a call this week?'"

The skill:
- Reads `WRITING_RULES.md` (18-point tactical rubric)
- May consult `COPYWRITING_FRAMEWORKS.md` (optional strategic reference)
- Generates 2-4 variants through its proven pipeline
- Passes through humanizer loop until zero AI flags
- Returns A-grade ship-ready copy

### Step 4 — Optional strategic audit

Before launching, run framework audit for one more check:

```bash
/usr/local/bin/python3.13 tools/framework_audit.py \
  --subject "your winning subject" \
  --body "your winning email body" \
  --niche "commercial HVAC contractors" \
  --client client_c
```

Returns 27-point framework scorecard across 6 layers:
- Strategy (awareness + sophistication match)
- Desire (channeled LF8)
- Opening (lead type recognizable)
- Persuasion (Cialdini weapons stacked)
- Execution (Hopkins + Halbert principles)
- CTA (single-variable ask)

**Grade thresholds:**
- A (26-27): SHIP
- B (22-25): MINOR_REVISIONS
- C or lower: REWRITE

### Step 5 — Pre-flight data quality check (MANDATORY)

Before any Smartlead upload:

```bash
/usr/local/bin/python3.13 tools/data_quality_check.py --input path/to/leads.csv
```

**Non-negotiable.** This stops the Property-Tax-Appeal-class disasters where 79% of leads had no first name.

### Step 6 — Launch the test campaign

Create a Smartlead campaign in DRAFTED state. Upload leads. Add sequence. Review. Then (and only then) click Start in the Smartlead UI.

**Operating rule (never broken):** Claude never calls `START` on a campaign without the operator's explicit approval. Always DRAFTED.

### Step 7 — After 5-7 days of sending, run the analyzer

```bash
/usr/local/bin/python3.13 tools/campaign_analyzer.py --campaign 3184163
```

Returns:
- What worked / what didn't / 3 specific recommendations
- Framework diagnostics (was the lead type right? weapons sufficient?)
- **Backfills `actual_reply_rate`** into `_predictions.jsonl` for the matching niche_research entry — this builds your calibration dataset over time

### Step 8 — Decide: scale or kill

- **Reply rate ≥ 1.5% on 50 sends** → scale to full 500-1000 lead run
- **Reply rate < 1%** → iterate copy (back to Step 2 or 3 with a new variant) OR kill niche
- **Bounce rate > 3%** → stop sending, run deliverability MCP + data_quality_check, fix before resuming

---

## When to use which tool (quick reference)

| Situation | Tool |
|---|---|
| "Is this vertical worth pursuing?" | `niche_research.py` |
| "I don't know the right awareness level / lead type for this niche" | `framework_variants.py` |
| "Generate ship-ready cold email copy" | `/cold-email-writer` skill |
| "Does this draft pass framework muster?" | `framework_audit.py` |
| "Are these leads safe to upload to Smartlead?" | `data_quality_check.py` |
| "Did my campaign work? Why/why not?" | `campaign_analyzer.py` |
| "Which mailboxes are at risk?" | `mailbox_autopilot.py` |
| "Deliverability audit across all domains?" | deliverability MCP (say "check deliverability on all CLIENT_C domains") |

---

## Operating rules (never violated)

1. **Never call START on a Smartlead campaign without explicit approval.**
2. **Never auto-pause a running campaign.** Alerts only, the operator decides.
3. **Forge core code is hands-off.** Build new tools as separate files.
4. **Always run `data_quality_check.py` before uploading to Smartlead.**
5. **Copy pipeline uses Claude Opus 4.** Don't revert to Sonnet for generation.
6. **Light tasks route through `llm_router.get_light_client()` → Kimi.**

See `SOP.md` for the full operating manual.

---

## Total workflow cost per new vertical

| Step | Cost |
|---|---|
| niche_research.py (LIGHT mode) | ~$0.008-0.010 |
| niche_research.py (DEEP mode, optional Firecrawl) | ~$0.61 |
| framework_variants.py | ~$0.005 (Kimi × 4 variant calls) |
| /cold-email-writer | Claude Opus cost — ~$0.20 per sequence |
| framework_audit.py | ~$0.001 per draft |
| 50-lead Forge run | ~$15 (Blitz + Icypeas + MV) |
| Smartlead sends | ~$0 (inbox cost already paid) |
| campaign_analyzer.py | ~$0.01 |
| **Total research → campaign** | **~$15-16 per vertical** |

One prevented Property-Tax-Appeal-class disaster pays for 5-10 cycles of research.
