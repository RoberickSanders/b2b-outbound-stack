# Architecture Decisions

This document explains the WHY behind 5 of the system's most important design choices. Each one was made in response to a specific failure mode.

---

## 1. LLM cost router (split between cheap + flagship models)

### The decision

Light tasks (classification, parsing, niche-fit checks, title red-flag filtering) route to a cheap model. Heavy tasks (copy generation, deep research, compounding-data briefs) route to the flagship model.

### What it prevents

Without the router, every LLM call would default to the flagship model. Most production calls are simple ("is this title relevant for this niche?"). Running them on the flagship is paying ~3000x for compute that's wasted because the cheap model gets the same answer.

The router is critical for unit economics. A typical campaign requires hundreds of LLM calls; if all of them go to the flagship, the cost-per-campaign moves from cents to dollars.

### What's in the router

- A task-type classifier
- A routing rule per task type
- A retry/fallback path (if cheap model fails or hits rate limit, escalate to flagship)
- Cost logging per call

### Tradeoff accepted

Marginal quality drop on the cheap-model paths. Worth it for the cost reduction.

---

## 2. 14-day mailbox maturity rule (code-enforced)

### The decision

A mailbox cannot be attached to a campaign unless it has been warming for at least 14 days AND has a reputation score of 100%. Both required.

### What it prevents

Production incidents where a mailbox showing "100% reputation" after 7 days of warmup got burned in cold campaigns. The reputation metric reflects warmup performance, not deliverability — fresh-but-warm mailboxes still trip spam filters in cold cohorts.

### Why it's in code, not docs

Docs decay. Code doesn't. By making the rule a runtime check inside `Smartlead.attach_email_account()`, no operator can attach an unmature mailbox even if they forget the rule. The rule survives team turnover, doc drift, and operator fatigue.

### Tradeoff accepted

Slower mailbox-fleet ramp. New mailboxes can't be deployed in week 1. This costs 14 days of capacity per provisioning cycle. Worth it because burned mailboxes cost weeks to recover.

---

## 3. 18-point copy quality gate

### The decision

Every cold email passes through 18 deterministic checks before it can be attached to any campaign. Score below 14 = ship blocked.

### What it prevents

Two failure modes:

1. **AI-tell emails reach customers.** Without the gate, LLM-generated emails ship with em-dashes, "I hope this finds you well" openers, and "we help X achieve Y" pitches. These signals tank reply rates and burn sending domains.

2. **Operators ship under pressure.** When campaigns are due and copy is tight, operators rationalize "good enough." The gate is a forcing function that prevents under-pressure quality drops.

### Why deterministic, not LLM-judged

LLMs are inconsistent judges. A deterministic gate gives the same answer every time, can be debugged, and can be tested in CI. LLM-judges drift over time and are hard to reason about.

### Tradeoff accepted

False positives — sometimes a good email gets blocked because it triggers a check it shouldn't. The fix is operator override (blocked emails can be reviewed and manually approved if the operator confirms the check was wrong). Override rate is monitored separately to catch over-fitting.

---

## 4. Multi-tenant data isolation (shared infrastructure, isolated data)

### The decision

The framework, cascade, copy gate, and infrastructure are shared across clients. Lead databases, mailbox pools, copy banks, and conversion data are tagged by client and never cross-referenced.

### What it prevents

Two failure modes:

1. **Cross-contamination of lead data.** If client A's leads contaminate client B's targeting, you get spam-flag risk and trust loss with both clients.

2. **Maintenance burden.** Without shared infrastructure, every client needs a parallel codebase. Patches don't propagate. Best practices learned at one client don't help others.

### How it works

A `client` column on every shared table. Every query that touches lead, meeting, or conversion data has a `WHERE client=?` clause. The cascade and copy gate are stateless — they take a client identifier and operate on isolated data.

### Tradeoff accepted

A bug in the shared layer affects all clients simultaneously. Mitigation: aggressive testing (the framework has a comprehensive unit test suite) and gradual rollout of changes (canary client first, then broader fleet).

---

## 5. Free-before-paid enrichment cascade

### The decision

The 13-step enrichment cascade runs free paths (steps 1-8) before any paid API call (steps 9-13). A lead exits the cascade as soon as a verified email is found.

### What it prevents

Three failure modes:

1. **Burning paid API budget on leads that resolve free.** Most leads in production resolve in steps 1-8. Without ordering, those leads would all fire paid calls anyway.

2. **Slow cascade for high-volume targeting.** Free paths are typically faster (cached, single-source). Running them first means the median lead resolves in under 2 seconds. Paid paths are slower (multi-API, retry logic).

3. **Cost-per-lead drift.** Without the cascade order, cost-per-lead would be roughly an order of magnitude higher. The free-first ordering keeps the unit economics viable at scale.

### Why this specific ordering

Each step's position in the cascade reflects a tradeoff between:

- **Hit rate** — how often this step alone resolves the lead
- **Latency** — how long this step takes to fail when it doesn't hit
- **Cost** — paid steps go later
- **Reliability** — flaky APIs are positioned later

Steps 1-2 are cached/local (instant). Steps 3-4 are reverse lookups against existing data. Steps 5-7 are external scrapes (slower but free). Step 8 is the SERP + LLM (slow but free). Steps 9-13 are paid.

### Tradeoff accepted

Some leads that would resolve in step 11 take longer because the cascade exhausts steps 1-10 first. This costs latency on the long-tail leads but saves money on the median lead. Acceptable because campaign throughput is dominated by the median, not the long tail.

---

## What's NOT in this document

- Specific patterns the cascade tries in step 9 (smart-pattern guess library — proprietary)
- Specific anti-pattern dictionaries used by the copy gate (proprietary)
- Real cost-per-lead numbers or hit-rate percentages
- Real conversion data
- Real client identifiers
- Specific niche-tuning rules
- Production prompts

The architecture is open-source. The tuning is private.

---

## How this list was built

Each decision in this list was forced into existence by a specific failure mode in production. The list isn't theoretical — every entry has a real incident behind it. Documentation here describes what was learned, not what was hypothesized.
