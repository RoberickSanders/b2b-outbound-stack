# Agency OS — Production B2B Outbound Infrastructure Framework

Open-source framework for production-grade B2B outbound operations. Built and operated end-to-end on Python 3.13 + Claude Code skills.

> **Status:** runs in production. Public version is sanitized of client data, copy banks, and proprietary patterns. The framework + architecture are open-sourced as a portfolio artifact.

## What this is

A complete operational stack for B2B outbound — discovery → enrichment → verification → copy generation → DRAFTED Smartlead campaign — with multi-tenant data isolation, safety rules codified in code, and an LLM cost router.

## Notable components

### 1. LLM cost router (`llm_router.py`)
Routes light tasks (classification, parsing) to a cheap model and heavy tasks (copy generation, deep research) to a flagship model.

### 2. 18-point copy quality gate (`tools/copy_quality_gate.py`)
Programmatic rubric every cold email passes before ship:
- Em-dash detection
- Anti-pattern matching ("we help", "I hope this finds you well")
- Spintax structure validation
- Subject-pattern format detection (colleague_internal, vendor_scheduling, customer_inquiry)
- Single-CTA enforcement
- Minimum score required to ship

### 3. 13-step enrichment cascade (`tools/forge_enrich.py`)
Free paths first, paid paths only when free fails:

```
FREE (steps 1-8):
  1. MX pre-check               (skip dead domains)
  2. Domain memory               (known patterns cached)
  3. Reverse phone lookup        (phone → owner → email)
  4. Reverse email lookup        (info@ → who manages it → personal email)
  5. Google Maps email
  6. Direct contact API
  7. Website scraping
  8. Owner search via SERP + LLM

PAID (steps 9-13, only if free fails):
  9. Smart-pattern guess + verify
  10. Reverse-email-finder
  11. Name + domain finder
  12. Domain-only finder
  13. Catch-all acceptance
```

### 4. Code-enforced safety rules (`mailbox_helpers.py`)
The 14-day mailbox maturity rule lives at the code layer, not in docs:

```python
def is_mature(acct: dict, min_age_days: int = 14, min_warmup_pct: int = 100):
    """Hard rule: mailbox must be at least N days old AND at min reputation."""
    return _age_days(acct) >= min_age_days and _warmup_pct(acct) >= min_warmup_pct
```

Wrappers around `Smartlead.attach_email_account()` route through this so no campaign can attach an unmature mailbox in production.

### 5. Compounding-data feedback loop (`tools/forge_compound.py`)
After every campaign cycle, joins the leads + meetings tables, identifies which industries, titles, and geos converted, and writes a markdown brief that feeds the next campaign's prompt as context.

### 6. Subagent dispatch for parallel research
The `/lookalike-research` slash command dispatches N parallel Claude Code subagents — each independently researches lookalike companies matching a signal profile, then results are aggregated, deduplicated, and ingested. See `tools/forge_lookalike_research.py` + `.claude/commands/lookalike-research.md`.

### 7. Operational rhythm codified as a slash command
The `/weekly-rhythm` skill is a pure operational playbook (Mon/Wed/Fri/biweekly/monthly/quarterly cadences) with no scripts.

### 8. 4-point campaign diagnostic (`/diagnose-campaign`)
Top-down: deliverability → targeting → copy/offer → speed-to-lead. Stops at the first failed layer because layers below a failure are noise.

## Architecture — pipeline overview

```mermaid
flowchart TD
    A[Natural-language input] --> B[Phase 1: Parse<br/>Extract client/niche/target/geo]
    B --> C[Phase 2: Cascade discovery]
    C --> C1[Blitz keyword]
    C --> C2[Lookalike]
    C --> C3[Firecrawl directories]
    C --> C4[Serper Maps geo-grid]
    C1 & C2 & C3 & C4 --> D[Phase 3: Master DB dedup<br/>SQLite]
    D --> E[Phase 4: 13-step enrichment<br/>free → paid waterfall]
    E --> F[Phase 5: Verification<br/>MillionVerifier + BounceBan early-exit]
    F --> G[Phase 6: Quality<br/>LLM niche-fit + title red-flag filter]
    G --> H[Phase 7: Copy generation<br/>flagship LLM + subject patterns]
    H --> I[Phase 8: Ship gate<br/>18-point copy gate + 10-component offer scorecard]
    I -->|pass| J[Phase 9: DRAFTED Smartlead campaign<br/>mature-mailbox-only attach]
    I -->|fail| H
    J --> K[Human review<br/>operator clicks Start in Smartlead UI]

    style A fill:#0d1117,color:#fff,stroke:#58a6ff
    style J fill:#1a472a,color:#fff,stroke:#3fb950
    style K fill:#3a2c0f,color:#fff,stroke:#d29922
```

## 13-step enrichment cascade

Free paths exhaust before any paid call fires.

```mermaid
flowchart LR
    L[Lead] --> S1{1. MX check}
    S1 -->|live| S2{2. Domain memory<br/>known pattern?}
    S1 -->|dead| X[skip]
    S2 -->|hit| OK1[email]
    S2 -->|miss| S3{3. Reverse phone}
    S3 -->|hit| OK1
    S3 -->|miss| S4{4. Reverse email}
    S4 -->|hit| OK1
    S4 -->|miss| S5{5. Maps email}
    S5 -->|hit| OK1
    S5 -->|miss| S6{6. Direct contact}
    S6 -->|hit| OK1
    S6 -->|miss| S7{7. Site scrape}
    S7 -->|hit| OK1
    S7 -->|miss| S8{8. Owner search<br/>SERP + LLM}
    S8 -->|hit| OK1

    S8 -->|miss| P9{9. Smart pattern<br/>+ MV verify}
    P9 -->|hit| OK2[email]
    P9 -->|miss| P10{10. Reverse-email-finder}
    P10 -->|hit| OK2
    P10 -->|miss| P11{11. Name+domain}
    P11 -->|hit| OK2
    P11 -->|miss| P12{12. Domain-only}
    P12 -->|hit| OK2
    P12 -->|miss| P13{13. Catch-all<br/>firstname@}
    P13 --> OK2

    OK1 -.->|FREE PATH| END[verified email]
    OK2 -.->|PAID PATH| END

    style OK1 fill:#1a472a,color:#fff,stroke:#3fb950
    style OK2 fill:#3a2c0f,color:#fff,stroke:#d29922
    style END fill:#0d1117,color:#fff,stroke:#58a6ff
```

## LLM cost router

Heavy work routes to flagship only when the cost premium is justified by output quality.

```mermaid
flowchart LR
    REQ[LLM request] --> R{llm_router.py<br/>route by task type}
    R -->|classification<br/>parsing<br/>niche-fit<br/>title red-flag| K[Cheap model]
    R -->|copy generation<br/>deep research<br/>compound briefs<br/>reply drafts| O[Flagship model]

    style K fill:#1a472a,color:#fff,stroke:#3fb950
    style O fill:#3a2c0f,color:#fff,stroke:#d29922
```

## Multi-tenant data isolation

Multiple clients share infrastructure but never share leads, mailboxes, copy banks, or DBs.

```mermaid
flowchart TB
    subgraph FORGE[Shared Framework]
        F1[forge.py + f.py dispatcher]
        F2[13-step enrichment cascade]
        F3[Copy quality gate]
        F4[Mailbox helpers]
        F5[LLM router]
    end

    subgraph CLIENT_A[Client A workspace]
        A1[copy_banks_a.py — private]
        A2[campaigns_a — Smartlead]
        A3[mailboxes_a — Inboxkit]
        A4[leads tagged client=a]
    end

    subgraph CLIENT_B[Client B workspace]
        B1[copy_banks_b.py — private]
        B2[campaigns_b — Smartlead]
        B3[mailboxes_b — Inboxkit]
        B4[leads tagged client=b]
    end

    subgraph CLIENT_C[Client C workspace]
        C1[copy_banks_c.py — private]
        C2[campaigns_c — Smartlead]
        C3[mailboxes_c — Inboxkit]
        C4[leads tagged client=c]
    end

    subgraph DB[Shared SQLite]
        D1[(leads<br/>client column)]
        D2[(meetings<br/>client column)]
    end

    FORGE --> CLIENT_A
    FORGE --> CLIENT_B
    FORGE --> CLIENT_C
    CLIENT_A --> D1
    CLIENT_B --> D1
    CLIENT_C --> D1
    CLIENT_A --> D2
    CLIENT_B --> D2
    CLIENT_C --> D2

    style FORGE fill:#0d1117,color:#fff,stroke:#58a6ff
    style DB fill:#3a2c0f,color:#fff,stroke:#d29922
```

## Deployment architecture

```mermaid
flowchart TB
    subgraph LOCAL[Operator]
        PHONE[iPhone — Telegram + SSH]
        MAC[Mac — Claude Code CLI for development]
    end

    subgraph TS[Tailscale private VPN]
        DROPLET[forge-prod droplet<br/>DigitalOcean]
    end

    subgraph SERVICES[Operator-managed APIs]
        SL[Smartlead<br/>email infra]
        IK[Inboxkit<br/>mailbox provisioning]
        BL[Blitz / Hunter / Icypeas<br/>contact enrichment]
        MV[MillionVerifier<br/>email verification]
        ANT[Anthropic API<br/>flagship LLM]
        KIMI[Moonshot API<br/>cheap LLM]
        SERP[Serper / Firecrawl<br/>web search + scrape]
        PUSH[Pushover<br/>phone alerts]
    end

    subgraph DROPLET_INTERNALS[On the droplet]
        FORGE[Forge — Python 3.13]
        SYSTEMD[systemd timers<br/>autopilot daily<br/>client_reports weekly]
        HERMES[Hermes Agent<br/>Telegram bot gateway]
        DBLOCAL[(master_leads.db<br/>WAL mode)]
    end

    PHONE --> TS
    MAC --> TS
    TS --> DROPLET
    HERMES <-->|natural-language ops| PHONE
    FORGE <--> SERVICES
    SYSTEMD --> FORGE
    HERMES --> FORGE
    FORGE <--> DBLOCAL
    SYSTEMD -.->|alerts| PUSH

    style DROPLET fill:#0d1117,color:#fff,stroke:#58a6ff
    style HERMES fill:#1a472a,color:#fff,stroke:#3fb950
    style PHONE fill:#3a2c0f,color:#fff,stroke:#d29922
```

## Stack

- **Language:** Python 3.13
- **Database:** SQLite (WAL mode)
- **LLMs:** Anthropic Claude (heavy) + Moonshot Kimi (light)
- **Email infra:** Smartlead, Inboxkit, MillionVerifier
- **Discovery:** Blitz, AI Ark, Firecrawl, Serper Maps
- **Enrichment:** Hunter, Icypeas, MillionVerifier
- **Subagents:** Claude Code Task tool
- **Deploy:** DigitalOcean droplet + systemd timers + Tailscale
- **Conversational layer:** Hermes Agent for Telegram-bot ops

## See it in action

Concrete walkthroughs of the framework's key mechanisms (synthetic data, real architecture):

- [Sample lead through the 13-step cascade](examples/sample_lead_walkthrough.md) — fictional lead routed through every step, showing what each one tries and how the cascade short-circuits on hit
- [Copy quality gate worked examples](examples/copy_quality_gate_examples.md) — 3 fictional emails through the 18-point gate (PASS / FAIL / borderline), with full score breakdowns
- [Architecture decisions](docs/architecture_decisions.md) — the WHY behind 5 key design choices, with the failure modes each one prevents

## Repository structure

```
agency-os/
├── README.md                            (this file)
├── CLAUDE.md                            operating doctrine
├── HERMES.md                            Hermes Agent integration bridge
├── COOKBOOK.md                          goal-oriented "I want to ___, run this"
├── SOP.md                               operational policies
├── WORKFLOW.md                          end-to-end campaign workflow
├── forge.py                             top-level orchestrator
├── f.py                                 unified CLI dispatcher (50+ subcommands)
├── doctor.py                            7-category health check
├── llm_router.py                        cost-aware LLM routing
├── mailbox_helpers.py                   14-day maturity rule (code-enforced)
├── master_db.py                         SQLite master leads + meetings
├── enrichment.py                        email enrichment waterfall
├── verification.py                      MV + BB verification
├── tools/
│   ├── forge_compound.py                winning-angles miner from past meetings
│   ├── forge_lookalike_research.py     subagent-dispatch lookalike (3-stage)
│   ├── forge_auto_research.py          autonomous loop
│   ├── competitor_engagers.py           LinkedIn engager harvest from competitors
│   ├── list_quality_scorecard.py        8-dim list grading (A-F before send)
│   ├── copy_quality_gate.py             18-point ship gate
│   ├── score_offer.py                   10-component offer scorecard
│   ├── mailbox_autopilot.py             daily mailbox health watchdog
│   ├── data_quality_check.py            pre-send audit (vertical-aware thresholds)
│   ├── forge_enrich.py                  unified 13-step cascade
│   └── ...                              25+ more showcase tools
└── .claude/commands/
    ├── diagnose-campaign.md             4-point diagnostic
    ├── lookalike-research.md            stage-2 subagent dispatcher
    └── weekly-rhythm.md                 Mon/Wed/Fri operational cadence
```

## What's NOT in this public repo

- Production copy banks (proprietary, agency-tuned per client)
- Real lead databases or CSVs
- Real client identifiers, sender personas, or meeting/conversion data
- API keys (gitignored)
- Winning-angles briefs (these are client deliverables)

## License

MIT — use it, fork it, adapt it.

The framework is open-source. Specific operator implementations (copy banks, client data, trained patterns) are proprietary to their owners.
