#!/usr/bin/env python3
"""
score_offer.py — Cold Traffic Offer Scorecard (Oliverify 10-component rubric).

Complements paf_copy_gate. Where paf_copy_gate enforces tactical writing rules
(humanizer, word count, format), score_offer audits the OFFER strength itself
against the framework Oliverify documented from 2.27M emails of agency data.

Use cases:
  - Grade a generated sequence against the 10-component scorecard
  - Flag campaigns whose copy passes paf_copy_gate but whose offer is weak
  - Pre-launch sanity check: "would a stranger respond to this?"

The 10 components (each scored 1-5):
  1. NEW MONEY              new revenue for prospect (vs. optimization)
  2. SPECIFIC DREAM OUTCOME concrete: audience + number + timeframe
  3. PERCEIVED LIKELIHOOD   trigger event + mechanism + social proof
  4. OFFER IN THE EMAIL     tangible deliverable / risk-reversed direct response
  5. RISK REVERSAL          performance pricing / guarantee / no-strings free work
  6. LOW-FRICTION CTA       one-step ask, minimal commitment
  7. SPECIFICITY            no vague claims, named numbers
  8. OUTCOME NOT PRODUCT    result-focused, not feature-focused
  9. DONE-FOR-YOU POSITIONING  minimizes prospect effort
  10. TIMING RELEVANCE      real reason to respond now

Total 50. Letter grades:
  45-50 = A — launch
  35-44 = B — acceptable, layer in improvements
  25-34 = C — rewrite recommended
  <25   = D — rewrite required

Routing:
  Uses llm_router.get_heavy_client() per CLAUDE.md rule 5 (heavy copy quality
  tasks stay on Claude Sonnet 4 — do not route to Kimi).

Usage:
    # Standalone CLI on a sequence JSON file
    python3 tools/score_offer.py path/to/sequence.json
    python3 tools/score_offer.py path/to/sequence.json --min-grade B --json

    # Importable from other tools
    from score_offer import score_offer
    result = score_offer(sequence_payload, offer_context=optional_dict)
    if result["grade"] in ("C", "D"):
        print("Rewrite the offer before launch")

Returns:
    {
      "components": [
        {"id": "new_money", "name": "New Money", "score": 4,
         "evidence": "...", "improvement": "..."},
        ...
      ],
      "total": 41,
      "grade": "B",
      "ship_recommended": True,
      "top_3_improvements": ["...", "...", "..."],
      "model_used": "claude-sonnet-4-20250514"
    }
"""

import argparse
import json
import os
import re
import sys
from pathlib import Path

# Forge import path
SCRIPT_DIR = Path(__file__).resolve().parent
LEAD_PIPELINE_DIR = SCRIPT_DIR.parent
sys.path.insert(0, str(LEAD_PIPELINE_DIR))

from llm_router import get_heavy_client, CLAUDE_HEAVY_MODEL  # noqa: E402


# ─── Component spec ──────────────────────────────────────────────────────────
COMPONENTS = [
    ("new_money", "New Money / New Revenue",
     "Does the offer create new revenue for the prospect (new leads / new pipeline / "
     "new business), versus optimization or cost-savings? Strangers respond more to "
     "new-money offers than optimization offers. Compliance/risk-removal also counts "
     "as 'new money' if framed as protecting revenue (e.g. CMS Five-Star rating "
     "drives census which drives revenue). Score 5 only if framed as net-new value."),

    ("specific_outcome", "Specific Dream Outcome",
     "Is the outcome concrete with audience + number + timeframe? Example: 'help your "
     "new sales hire book 15 qualified meetings in their first month' beats 'we help "
     "B2B companies grow.' For CLIENT_A: 'free walkthrough that catches NFPA 25 issues "
     "before your CMS survey' is concrete; 'help with fire protection' is vague."),

    ("perceived_likelihood", "Perceived Likelihood",
     "Why should a stranger believe this will work for THEM? Strongest signals: "
     "(a) trigger event indicating you've done research (recent permit, hire, "
     "renovation, survey cycle), (b) named mechanism that differentiates, "
     "(c) relevant social proof from same vertical/size. CLIENT_A scores well via "
     "regulatory anchors (NFPA 25, AHJ, CMS) — they're industry-specific authority."),

    ("offer_in_email", "Offer In The Email",
     "Is there a tangible offer the prospect can say yes to without committing time? "
     "Three good patterns: (1) free deliverable (audit, walkthrough, sample, mockup), "
     "(2) direct-response with risk reversal (performance-based, money-back), "
     "(3) both combined. Bad: just asking for a meeting with no value attached."),

    ("risk_reversal", "Risk Reversal",
     "What makes saying yes feel safe? Performance-based pricing, money-back "
     "guarantee, free deliverable with no strings, or 'pay only on results.' "
     "Oliverify's data: same product, +86% reply rate from risk-reversal alone."),

    ("low_friction_cta", "Low-Friction CTA",
     "Is the ask one small step? 'Mind if I send it over?' or 'Open to a quick "
     "call this week?' beats 'Book a 30-min discovery call.' Hard CTA OK if the "
     "offer/risk-reversal stack is strong enough to justify time commitment."),

    ("specificity", "Specificity",
     "Are claims specific (named numbers, audiences, time frames, mechanisms) "
     "or vague? 'We'll help you grow' = D. 'Cut your NFPA 25 prep from 6 hours "
     "to 30 minutes for your next CMS survey' = A. Every vague word reduces "
     "believability."),

    ("outcome_not_product", "Outcome Not Product",
     "Does the email sell the result the prospect gets, or describe the service "
     "you provide? 'We do fire inspections' is product. 'Pass your next CMS survey "
     "with zero life-safety deficiencies' is outcome. Outcome wins on cold."),

    ("done_for_you", "Done-For-You Positioning",
     "Is prospect effort minimized? 'Free walkthrough, we handle the report and "
     "send it to you' beats 'we'll show you how to fix your sprinkler system.' "
     "Lower-friction = higher reply on cold."),

    ("timing_relevance", "Timing Relevance",
     "Why now versus six months from now? Trigger events (recent permit, new hire, "
     "upcoming inspection cycle) outperform broad campaigns ~10x in Oliverify's data. "
     "Regulatory deadlines and seasonal cycles (CMS surveys, kitchen renovations, "
     "annual inspections) count as natural timing anchors."),
]


GRADE_THRESHOLDS = [
    (45, "A", True),
    (35, "B", True),
    (25, "C", False),
    (0,  "D", False),
]


def _to_grade(total: int) -> tuple[str, bool]:
    for floor, grade, ship in GRADE_THRESHOLDS:
        if total >= floor:
            return grade, ship
    return "D", False


# ─── Sequence flattener (matches paf_copy_gate input format) ────────────────
def _flatten_sequence(seq: dict) -> str:
    """Produce a single text block representing the full sequence for scoring.

    Accepts the same payload paf_copy_gate accepts:
        {"sequences": [{"seq_number": 1, "seq_variants": [
            {"variant_label": "A", "subject": "...", "email_body": "..."},
            ...
        ]}, ...]}
    """
    chunks = []
    for s in seq.get("sequences", []):
        seq_no = s.get("seq_number", "?")
        for v in s.get("seq_variants", []) or []:
            label = v.get("variant_label", "")
            subject = v.get("subject") or ""
            body = v.get("email_body") or ""
            # Strip HTML, leave spintax visible (it shows variant intent)
            body = re.sub(r"<[^>]+>", "\n", body)
            body = re.sub(r"\n{3,}", "\n\n", body).strip()
            chunks.append(f"=== Email {seq_no}{label} ===\nSubject: {subject}\n\n{body}\n")
    return "\n".join(chunks)


# ─── Prompt construction ────────────────────────────────────────────────────
def _build_prompt(sequence_text: str, offer_context: dict | None) -> str:
    components_block = "\n".join(
        f"{i+1}. {name} ({cid}): {desc}"
        for i, (cid, name, desc) in enumerate(COMPONENTS)
    )
    ctx_block = ""
    if offer_context:
        ctx_block = (
            "\n\nADDITIONAL OFFER CONTEXT (provided by the campaign owner):\n"
            + json.dumps(offer_context, indent=2)
        )
    return f"""You are scoring a B2B cold email sequence against the 10-component Cold Traffic Offer Scorecard.

The framework (each component scored 1-5; 5 = excellent, 1 = absent or wrong):

{components_block}

Score honestly. Strangers in inboxes have zero trust. A weak offer with great copy gets ignored; a strong offer with average copy gets replies.

For each component, return:
  - id: the component id from above
  - score: 1-5 integer
  - evidence: a SHORT verbatim quote from the sequence supporting your score (under 25 words)
  - improvement: ONE concrete suggestion if score < 4, else empty string

Then return:
  - total: sum of scores (max 50)
  - top_3_improvements: the three highest-leverage rewrites, in priority order

Return ONLY valid JSON in this exact schema, no prose, no markdown fences:
{{
  "components": [
    {{"id": "new_money", "score": 4, "evidence": "...", "improvement": "..."}},
    ...10 entries total...
  ],
  "total": 41,
  "top_3_improvements": ["...", "...", "..."]
}}

SEQUENCE TO SCORE:
{sequence_text}{ctx_block}"""


# ─── Main scoring function ──────────────────────────────────────────────────
def score_offer(seq: dict, offer_context: dict | None = None) -> dict:
    """Score a sequence against the 10-component cold-traffic-offer rubric.

    Args:
        seq: sequence payload (same shape paf_copy_gate.grade_sequence accepts)
        offer_context: optional dict describing the offer (deliverable, risk
            reversal, target audience, USP). Helps the model when the sequence
            doesn't make these explicit.

    Returns:
        dict with components, total, grade, ship_recommended, top_3_improvements.
    """
    sequence_text = _flatten_sequence(seq)
    if not sequence_text.strip():
        return {
            "error": "no email content found in sequence payload",
            "total": 0, "grade": "D", "ship_recommended": False,
            "components": [], "top_3_improvements": [],
        }

    prompt = _build_prompt(sequence_text, offer_context)

    client = get_heavy_client()
    resp = client.messages.create(
        model=CLAUDE_HEAVY_MODEL,
        max_tokens=2500,
        temperature=0.0,
        messages=[{"role": "user", "content": prompt}],
    )
    raw = resp.content[0].text.strip()

    # Tolerate accidental fencing
    raw = re.sub(r"^```(?:json)?\s*", "", raw)
    raw = re.sub(r"\s*```$", "", raw)

    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as e:
        return {
            "error": f"model returned invalid JSON: {e}",
            "raw": raw[:500],
            "total": 0, "grade": "D", "ship_recommended": False,
            "components": [], "top_3_improvements": [],
        }

    # Hydrate component names from our spec (model only returns id+score+evidence+improvement)
    name_by_id = {cid: name for (cid, name, _) in COMPONENTS}
    components = []
    seen_ids = set()
    for c in parsed.get("components", []):
        cid = c.get("id")
        if cid not in name_by_id or cid in seen_ids:
            continue
        seen_ids.add(cid)
        components.append({
            "id": cid,
            "name": name_by_id[cid],
            "score": int(c.get("score", 0)),
            "evidence": (c.get("evidence") or "").strip(),
            "improvement": (c.get("improvement") or "").strip(),
        })

    # Backfill any missing components with score=0 (model omitted them = penalty)
    for cid, name, _ in COMPONENTS:
        if cid not in seen_ids:
            components.append({
                "id": cid, "name": name, "score": 0,
                "evidence": "", "improvement": "model omitted this component",
            })

    total = sum(c["score"] for c in components)
    grade, ship = _to_grade(total)

    return {
        "components": components,
        "total": total,
        "max": 50,
        "grade": grade,
        "ship_recommended": ship,
        "top_3_improvements": parsed.get("top_3_improvements", [])[:3],
        "model_used": CLAUDE_HEAVY_MODEL,
    }


# ─── Pretty printer ─────────────────────────────────────────────────────────
def format_report(result: dict) -> str:
    if "error" in result:
        return f"ERROR: {result['error']}\n{result.get('raw', '')}"
    out = []
    out.append(f"OFFER SCORECARD — {result['total']}/{result['max']}  Grade: {result['grade']}  "
               f"({'SHIP' if result['ship_recommended'] else 'REWRITE'})")
    out.append("=" * 78)
    for c in result["components"]:
        bar = "█" * c["score"] + "·" * (5 - c["score"])
        out.append(f"  [{bar}] {c['score']}/5  {c['name']}")
        if c["evidence"]:
            out.append(f"           evidence: {c['evidence'][:90]}")
        if c["improvement"]:
            out.append(f"           fix:      {c['improvement'][:90]}")
    out.append("")
    out.append("TOP IMPROVEMENTS:")
    for i, imp in enumerate(result.get("top_3_improvements", []), 1):
        out.append(f"  {i}. {imp}")
    return "\n".join(out)


# ─── CLI ────────────────────────────────────────────────────────────────────
def main():
    ap = argparse.ArgumentParser(description="Score a sequence against the cold-traffic offer scorecard.")
    ap.add_argument("file", help="Path to sequence JSON (paf_copy_gate format)")
    ap.add_argument("--offer-context", help="Path to JSON file with offer context "
                                            "(deliverable, risk_reversal, audience, usp)")
    ap.add_argument("--min-grade", default="B", choices=["A", "B", "C", "D"],
                    help="Exit nonzero if grade falls below this (default: B)")
    ap.add_argument("--json", action="store_true", help="Print raw JSON only")
    args = ap.parse_args()

    seq = json.loads(Path(args.file).read_text())
    ctx = None
    if args.offer_context:
        ctx = json.loads(Path(args.offer_context).read_text())

    result = score_offer(seq, offer_context=ctx)

    if args.json:
        print(json.dumps(result, indent=2))
    else:
        print(format_report(result))

    grade_order = ["A", "B", "C", "D"]
    if grade_order.index(result.get("grade", "D")) > grade_order.index(args.min_grade):
        sys.exit(1)
    sys.exit(0)


if __name__ == "__main__":
    main()
