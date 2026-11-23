# Sample Lead Walkthrough — 13-Step Enrichment Cascade

This is a synthetic example showing how a single lead flows through the cascade. All client and lead data is fictional.

## Input lead

```yaml
name: Sarah Chen
title: Marketing Director
company: AcmeMidMarketCorp
domain: acmemidmarket.example
phone: ""
linkedin: https://linkedin.com/in/sarah-chen-example
```

## Cascade execution

### Step 1: MX pre-check

```
Lookup: dig MX acmemidmarket.example
Result: 1 valid MX record (Google Workspace)
Decision: PROCEED
```

### Step 2: Domain memory

```
Lookup: SELECT * FROM domain_patterns WHERE domain='acmemidmarket.example'
Result: NO HIT — domain not previously enriched
Decision: PROCEED
```

### Step 3: Reverse phone lookup

```
Lead has no phone — SKIPPED
```

### Step 4: Reverse email lookup

```
Lead has no email — SKIPPED
```

### Step 5: Google Maps email

```
Search: "AcmeMidMarketCorp" near company HQ
Result: Map listing found, no contact email visible
Decision: PROCEED
```

### Step 6: Direct contact API

```
Lookup: cached result for acmemidmarket.example
Result: NO HIT
Decision: PROCEED
```

### Step 7: Website scraping

```
Scrape: acmemidmarket.example/contact
Result: info@acmemidmarket.example, support@acmemidmarket.example
Both are generic, not personal — NOT a match for Sarah
Decision: PROCEED
```

### Step 8: Owner search via SERP + LLM

```
Search: "Sarah Chen" "AcmeMidMarketCorp" email
LLM extraction: looking for personal email in top 5 results
Result: NO PERSONAL EMAIL found in public SERP
Decision: ESCALATE to paid path
```

> **Note:** Steps 1-8 are all FREE paths. ~60% of real leads in production resolve in this range and never hit a paid API. This lead is going to step 9+.

### Step 9: Smart-pattern guess + MV verify

```
Generate candidate emails based on common patterns:
  sarah@acmemidmarket.example
  sarah.chen@acmemidmarket.example
  schen@acmemidmarket.example
  s.chen@acmemidmarket.example

Verify each via MillionVerifier:
  sarah.chen@acmemidmarket.example -> STATUS: ok (verified)

Decision: HIT — use sarah.chen@acmemidmarket.example
```

### Steps 10-13: Skipped

```
Step 9 hit — cascade exits early
```

## Final cascade output

```yaml
sarah.chen@acmemidmarket.example:
  source: "smart_pattern_guess+mv_ok"
  confidence: 99
  hits_per_path:
    free_paths_tried: [1, 2, 5, 6, 7, 8]
    free_paths_hit: []
    paid_paths_tried: [9]
    paid_paths_hit: [9]
  cost_incurred: ~$0.005 (1 MV API call)
```

## What this example demonstrates

1. **Free paths exhaust before paid paths fire** — most real leads resolve in steps 1-8 and never incur paid cost.
2. **Each step short-circuits the cascade on hit** — no unnecessary calls after a verified email is found.
3. **Paid paths are tightly verified** — every email returned from a paid path passes MillionVerifier before being marked as a hit. No "guess and pray" outputs.
4. **Source labeling supports post-campaign analysis** — the `source` field tells you which path produced each email, so you can identify which paths are paying off vs. which are wasting calls.

## What this example deliberately does NOT show

- Real domain or lead data (all examples synthetic)
- The specific patterns the smart-pattern guess in step 9 tries (the pattern library is tuned per niche and stays private)
- Specific signals that trigger early-exit decisions in step 6 (proprietary scoring layer)
- The full production-grade niche fitness check that runs after this cascade

The point is to show **how** the system handles a lead, not **which exact patterns** it uses.
