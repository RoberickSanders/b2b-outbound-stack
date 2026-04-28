---
description: Dispatch parallel Task subagents to find lookalike companies for a forge_lookalike_research run
argument-hint: <run_id>
---

# Lookalike Research Dispatcher

You are running stage 2 of `forge_lookalike_research.py`. Stage 1 (`prep`) has already pulled seed leads from `master_leads.db` and written a spec at:

```
02-Areas/lead-pipeline/lookalike-runs/$ARGUMENTS/spec.json
```

Your job is to dispatch parallel Task subagents — each one does independent web research to find lookalike companies — and aggregate their results into `results.json` in the same directory.

## Step 1 — Read the spec

Read `02-Areas/lead-pipeline/lookalike-runs/$ARGUMENTS/spec.json`.

If the file doesn't exist, stop and tell the user: "spec.json missing for run `$ARGUMENTS` — run `python3 tools/forge_lookalike_research.py prep ...` first."

The spec contains:
- `client` — client_a / client_b / client_c
- `seed_niche` / `target_niche`
- `geo` — geographic constraint (may be null)
- `n_agents` — how many subagents to dispatch
- `signal_profile` — distilled patterns from the seed leads
- `signal_profile.search_angles` — array of distinct search angles, one per subagent
- `seed_sample` — 10 representative seed companies for subagents to use as exemplars
- `output_schema` — the exact JSON shape each subagent must return per company

## Step 2 — Dispatch N subagents in parallel

Use the Agent tool with `subagent_type: general-purpose`. Send all N subagents in a **single message with multiple Agent tool uses** so they run concurrently.

For each search angle in `signal_profile.search_angles[0:n_agents]`, dispatch one subagent with this prompt structure:

```
You are agent {idx} of {n_agents} doing lookalike-company research for a B2B
cold-email lead-generation pipeline.

CLIENT: {client}
TARGET NICHE: {target_niche}
GEO CONSTRAINT: {geo or "none"}

SIGNAL PROFILE (what makes a good match):
{json.dumps(signal_profile, indent=2)}

YOUR SEARCH ANGLE (use this approach, do NOT replicate other agents):
{search_angle}

EXEMPLAR SEED COMPANIES (you are looking for companies LIKE these):
{json.dumps(seed_sample, indent=2)}

WHAT TO DO:
- Use WebSearch / WebFetch to find 10-20 companies that match the signal profile
- Cross-check each candidate against the exclusion_signals — drop any that match
- Verify each match by reading the company's actual website or a credible
  third-party listing (state directory, Google Maps, news, etc.)
- For each match, capture the EVIDENCE URL where you confirmed the match
- Apply geo constraint strictly — companies outside the geo are not valid

OUTPUT:
Return ONLY a JSON array — no prose, no markdown fences. Each element:
{
  "company_name": "...",
  "domain": "rootdomain.com (no www, no https://)",
  "city": "..." | null,
  "state": "XX" | null,
  "match_reason": "one sentence",
  "evidence_url": "https://...",
  "confidence": 0.0 to 1.0
}

Confidence scale:
  0.9-1.0 — direct evidence on company website + matches all signals
  0.7-0.8 — third-party confirmation + matches most signals
  0.5-0.6 — circumstantial match, single signal
  below 0.5 — DO NOT INCLUDE

Hard rules:
- Do not invent companies. If unsure, drop it.
- Do not include companies whose domain is already known to the user — the
  ingest stage will dedupe against master_leads.db, but cleaner input = faster ingest.
- Cap output at 20 companies per agent.
- Return ONLY the JSON array. Nothing else.
```

If `n_agents` is larger than the number of `search_angles` provided, cycle through them with different sub-angles or geos to ensure each subagent has a unique mission.

## Step 3 — Aggregate

Each subagent returns a JSON array. Collect them all:

```json
{
  "run_id": "$ARGUMENTS",
  "completed_at": "<ISO8601>",
  "agents": [
    {"idx": 0, "search_angle": "...", "company_count": N, "error": null},
    ...
  ],
  "companies": [
    {... full row from agent ..., "agent_idx": 0},
    ...
  ]
}
```

If any subagent errored or returned malformed JSON, record it under `agents[i].error` but continue — don't fail the whole run for one bad agent.

## Step 4 — Write results.json

Write the aggregated structure to:

```
02-Areas/lead-pipeline/lookalike-runs/$ARGUMENTS/results.json
```

Then tell the user:

```
✓ Lookalike research complete for run $ARGUMENTS
  Agents: <successful>/<total>
  Companies returned: <total>
  Confidence ≥ 0.7: <count>

Next:
  python3 tools/forge_lookalike_research.py ingest --run-id $ARGUMENTS
```

## Notes

- This slash command lives inside an interactive Claude Code session because the Task tool's subagents have web-research subsidized by the Max plan. Don't try to call this from a headless script — it loses the cost subsidy.
- Subagents run for 1-3 minutes each in parallel. The whole dispatch finishes in roughly the time of the slowest agent.
- Per Forge rule: never modify forge.py or any core Forge file from this command. The Python ingest stage is the only thing that touches master_leads.db.
- Per Forge rule: never trigger Smartlead campaign actions from this command — this tool only writes to master_leads.db with `source='claude_lookalike'`. Downstream Forge enrichment + the operator's manual approval gate handle the campaign side.
