#!/usr/bin/env python3
"""Framework audit tool — grade any cold email draft against the 27-point
framework checklist.

Complementary to (NOT replacing) the 18-point tactical rubric in
/cold-email-writer skill. The skill's rubric covers execution (word count,
em dashes, banned openers, spintax). This tool covers strategy (Schwartz
awareness, Masterson lead type, Cialdini weapons, Whitman Life Force 8,
Hopkins specificity, Halbert voice).

---

Critical boundary:
  Does NOT modify /cold-email-writer skill. The skill is the production
  copy generator. This tool audits output AFTER generation (or audits any
  arbitrary draft you paste in) to surface framework gaps.

  Think of it as: skill writes copy. Audit tool grades why it works or
  doesn't, in framework terms.

---

Usage:
  # Audit a draft pasted in
  python3 tools/framework_audit.py --subject "quick question" --body "Hey, ..."

  # Audit from a file (markdown, text, or campaign_copy.md)
  python3 tools/framework_audit.py --file path/to/draft.md

  # Audit a live Smartlead campaign's sequences
  python3 tools/framework_audit.py --campaign 3184163

Output: 27-point scorecard + framework classification + diagnostic notes.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
LEAD_PIPELINE_DIR = SCRIPT_DIR.parent
WORKSPACE_ROOT = LEAD_PIPELINE_DIR.parent.parent
sys.path.insert(0, str(LEAD_PIPELINE_DIR))

try:
    from dotenv import load_dotenv
    load_dotenv(WORKSPACE_ROOT / ".env", override=True)
except ImportError:
    pass


def _load_framework() -> str:
    fw = WORKSPACE_ROOT / "03-Resources" / "copywriting-frameworks" / "PROMPT.md"
    try:
        return fw.read_text()
    except Exception:
        return ""


def audit_draft(subject: str, body: str, niche: str = "", client: str = "",
                what_we_sell: str = "", no_case_study: bool = False,
                no_named_mechanism: bool = False) -> dict:
    """Run Kimi-powered framework audit. Returns structured scorecard.

    Asset-availability flags (added 2026-04-21 after CLIENT_C calibration discovery):
    When the sender honestly lacks certain marketing assets, the corresponding
    checks are marked N/A instead of FAIL. This produces a CALIBRATED grade
    alongside the RAW (ideal-ceiling) grade.

    - no_case_study: marks `specific_social_proof` N/A. Use when no verified
      peer case study or client result can be cited yet.
    - no_named_mechanism: marks `copy_matches_sophistication` N/A (Stage 3-4
      markets ideally have a named proprietary mechanism; if sender lacks one,
      they should either target less-saturated niches or accept this gap).

    CLIENT_C's actual winners (CLIENT_A Churches 6.5%, CLIENT_C Telecom 3.4%) likely lack both,
    yet perform well — the calibrated grade reflects that reality.
    """
    try:
        from llm_router import get_light_client
        llm_client, model = get_light_client()
    except Exception as e:
        return {"error": f"Kimi unavailable: {e}"}

    framework = _load_framework()

    context_block = ""
    if niche:
        context_block += f"NICHE: {niche}\n"
    if client:
        context_block += f"CLIENT (sender): {client}\n"
    if what_we_sell:
        context_block += f"WHAT WE SELL: {what_we_sell}\n"

    asset_block = ""
    na_items = []
    if no_case_study:
        asset_block += "ASSET GAP: No verified case study available. Mark `specific_social_proof` as \"na\" (not true/false).\n"
        na_items.append("specific_social_proof")
    if no_named_mechanism:
        asset_block += "ASSET GAP: No proprietary mechanism available to name. Mark `copy_matches_sophistication` as \"na\" (not true/false).\n"
        na_items.append("copy_matches_sophistication")
    if na_items:
        asset_block += f"\nIMPORTANT: The above \"na\" items should NOT be counted as failures. Report them as \"na\" in the JSON. The calibrated_passing count excludes them from the denominator.\n"

    prompt = f"""Audit this cold email draft against the 6-book copywriting framework. Be strict — this is QA, not encouragement.

{context_block}
{asset_block}
DRAFT SUBJECT: {subject}

DRAFT BODY:
{body}

{framework}

Return structured JSON (no markdown, no code fences). For each check, use true, false, or "na" (string). Only use "na" when the ASSET GAP block above explicitly permits it:

{{
  "classification": {{
    "awareness_level": "<Most Aware | Product Aware | Solution Aware | Problem Aware | Unaware — what awareness level does this copy TARGET?>",
    "lead_type": "<Offer | Promise | Problem-Solution | Big Secret | Proclamation | Story>",
    "weapons_detected": ["<Cialdini weapons explicitly present>"],
    "life_force_8": "<primary LF8 desire this channels>",
    "sophistication_fit": "<what market sophistication stage is this copy built for?>"
  }},
  "scorecard_27pt": {{
    "strategy_layer": {{
      "awareness_identified": true/false,
      "sophistication_stage_identified": true/false,
      "opener_matches_awareness": true/false,
      "copy_matches_sophistication": true/false/"na"
    }},
    "desire_layer": {{
      "channels_existing_desire": true/false,
      "life_force_8_activated": true/false,
      "permanent_or_change_force_clear": true/false,
      "dated_hook_if_change_force": true/false
    }},
    "opening_layer": {{
      "recognizable_lead_type": true/false,
      "lead_matches_matrix": true/false,
      "one_big_idea": true/false,
      "golden_thread_to_cta": true/false
    }},
    "persuasion_layer": {{
      "weapon_deployed": true/false,
      "multi_weapon_stack": true/false,
      "specific_social_proof": true/false/"na",
      "scarcity_is_real": true/false,
      "ethical_test_pass": true/false
    }},
    "execution_layer": {{
      "specific_number_or_fact": true/false,
      "no_superlatives": true/false,
      "no_corporate_filler": true/false,
      "one_human_to_one_human": true/false,
      "readable_aloud": true/false,
      "simple_words": true/false,
      "positive_frame": true/false,
      "no_pointless_humor": true/false
    }},
    "cta": {{
      "single_specific_ask": true/false,
      "commitment_consistency_used": true/false
    }},
    "raw_passing": <integer count of trues out of 27 — IDEAL ceiling>,
    "na_count": <integer count of "na" items — don't count toward failures>,
    "calibrated_passing": <raw_passing + na_count — passing including N/A>,
    "calibrated_total": <27 minus na_count — denominator after excluding N/A>,
    "total_items": 27
  }},
  "diagnostic_notes": [
    "<specific failing item — explain what's wrong and the fix; skip N/A items>",
    "<another>"
  ],
  "raw_grade": "<A = 26-27 raw_passing, B = 22-25, C = 18-21, D = <18. Based on IDEAL ceiling with all assets.>",
  "calibrated_grade": "<A = 23-24 of 24, B = 20-22 of 24, C = 16-19 of 24, D = <16 of 24. Based on assets actually available to sender.>",
  "ship_readiness": "<SHIP | MINOR_REVISIONS | REWRITE — based on calibrated grade, not raw>",
  "framework_match_score_raw": <integer 0-100 — raw score against ideal ceiling>,
  "framework_match_score_calibrated": <integer 0-100 — calibrated score excluding N/A items>,
  "top_3_improvements": [
    "<actionable specific change — cite a framework principle>",
    "<actionable specific change>",
    "<actionable specific change>"
  ]
}}

Scoring discipline:
- Missing specificity = fail several checks (Hopkins rule)
- "hope this email finds you well" = automatic fail on corporate_filler
- Generic benefits ("grow your business") = fail on channels_existing_desire
- No named weapon stack = fail on multi_weapon_stack
- If lead type is ambiguous (not one of the 6) = fail on recognizable_lead_type
- If ASSET GAP block permits "na" for a specific item, use "na" instead of false

Be honest. Copy that reads as "fine" can still miss 8-10 framework checks. But the calibrated grade should reflect what the sender can actually execute with their current assets."""

    try:
        resp = llm_client.messages.create(
            model=model, max_tokens=2500,
            messages=[{"role": "user", "content": prompt}],
        )
        text = resp.content[0].text.strip()
        text = re.sub(r"^```(?:json)?\s*", "", text)
        text = re.sub(r"\s*```$", "", text)
        result = json.loads(text)

        # Defensive calibration math in case Kimi miscounts
        scorecard = result.get("scorecard_27pt", {}) or {}
        raw_pass = 0
        na_count = 0
        for layer_name, layer in scorecard.items():
            if not isinstance(layer, dict):
                continue
            for k, v in layer.items():
                if v is True:
                    raw_pass += 1
                elif v == "na":
                    na_count += 1
        scorecard["raw_passing"] = raw_pass
        scorecard["na_count"] = na_count
        scorecard["calibrated_passing"] = raw_pass + na_count  # treat N/A as "not a failure"
        scorecard["calibrated_total"] = 27 - na_count
        result["scorecard_27pt"] = scorecard

        # Recompute grades from deterministic math
        def grade_for_raw(n):
            if n >= 26: return "A"
            if n >= 22: return "B"
            if n >= 18: return "C"
            return "D"

        def grade_for_calibrated(passing, total):
            pct = passing / total if total else 0
            if pct >= 0.95: return "A"
            if pct >= 0.82: return "B"
            if pct >= 0.68: return "C"
            return "D"

        result["raw_grade"] = grade_for_raw(raw_pass)
        result["calibrated_grade"] = grade_for_calibrated(scorecard["calibrated_passing"], scorecard["calibrated_total"])
        result["framework_match_score_raw"] = round(raw_pass / 27 * 100)
        result["framework_match_score_calibrated"] = round(
            scorecard["calibrated_passing"] / scorecard["calibrated_total"] * 100
        ) if scorecard["calibrated_total"] else 0
        return result
    except Exception as e:
        return {"error": f"audit failed: {e}"}


def render_audit(subject: str, body: str, result: dict, niche: str = "",
                 no_case_study: bool = False, no_named_mechanism: bool = False) -> str:
    if result.get("error"):
        return f"# Framework Audit FAILED\n\nError: {result['error']}"

    classif = result.get("classification", {})
    scorecard = result.get("scorecard_27pt", {})
    raw_pass = scorecard.get("raw_passing", scorecard.get("total_passing", 0))
    cal_pass = scorecard.get("calibrated_passing", raw_pass)
    cal_total = scorecard.get("calibrated_total", 27)
    na_count = scorecard.get("na_count", 0)

    raw_grade = result.get("raw_grade", result.get("grade", "?"))
    cal_grade = result.get("calibrated_grade", raw_grade)
    ship = result.get("ship_readiness", "?")
    raw_score = result.get("framework_match_score_raw", result.get("framework_match_score", 0))
    cal_score = result.get("framework_match_score_calibrated", raw_score)

    raw_icon = {"A": "✅", "B": "🟡", "C": "🟠", "D": "🔴"}.get(raw_grade[0] if raw_grade else "?", "❓")
    cal_icon = {"A": "✅", "B": "🟡", "C": "🟠", "D": "🔴"}.get(cal_grade[0] if cal_grade else "?", "❓")
    ship_icon = {"SHIP": "✅", "MINOR_REVISIONS": "🟡", "REWRITE": "🔴"}.get(ship, "❓")

    asset_gap_note = ""
    if no_case_study or no_named_mechanism:
        gaps = []
        if no_case_study: gaps.append("no verified case study")
        if no_named_mechanism: gaps.append("no named proprietary mechanism")
        asset_gap_note = f"\n**Asset gaps declared:** {', '.join(gaps)} — {na_count} checks marked N/A instead of FAIL."

    out = f"""# Framework Audit — {niche or 'cold email draft'}

**Audited:** {datetime.now(timezone.utc).isoformat()}

**Calibrated score:** {cal_icon} **{cal_grade}** ({cal_pass}/{cal_total} checks · {cal_score}/100 match)
**Raw score (ideal ceiling):** {raw_icon} **{raw_grade}** ({raw_pass}/27 checks · {raw_score}/100 match){asset_gap_note}

---

## 🚨 Important: this is NOT a ship gate

**The ship gate for production copy is the 18-point WRITING_RULES rubric in `/cold-email-writer` skill.** That rubric has been empirically validated by CLIENT_C's actual reply rates (CLIENT_A Churches 6.5%, CLIENT_C Telecom 3.4%, etc.).

**This 27-point audit is a STRATEGIC DIAGNOSTIC.** It measures framework-ideal execution — showing you what an A+ version would look like if all assets were available. It will consistently grade lower than the 18-point rubric because:

1. It's subjective on items like "lead type match" and "awareness identified" — Kimi's interpretation
2. It requires explicit weapons/mechanisms that battle-tested copy often handles implicitly
3. It grades against a published-book ideal, not against real reply-rate data

**When tactical rubric (18/18 WRITING_RULES) passes but framework audit grades D:**
→ Copy is ship-ready. Framework audit is flagging asset gaps (case study, mechanism) and aspirational improvements. Use it to plan what to BUILD NEXT (a real case study, a named proprietary scan, etc.), not to block shipping.

**When tactical rubric fails:**
→ Do NOT ship. Fix tactical issues first (banned openers, em dashes, word count) before even considering framework audit.

---

## Draft under audit

**Subject:** `{subject}`

```
{body}
```

---

## Framework classification

- **Awareness level:** {classif.get('awareness_level', '?')}
- **Lead type:** {classif.get('lead_type', '?')}
- **Sophistication stage fit:** {classif.get('sophistication_fit', '?')}
- **Cialdini weapons detected:** {', '.join(classif.get('weapons_detected', [])) or '(none clearly present)'}
- **Life Force 8 desire:** {classif.get('life_force_8', '?')}

---

## 27-point scorecard

"""
    # Render each layer
    layer_labels = {
        "strategy_layer":     "Strategy (Schwartz awareness + sophistication)",
        "desire_layer":       "Desire (Mass Desire + Life Force 8)",
        "opening_layer":      "Opening (Masterson lead types)",
        "persuasion_layer":   "Persuasion (Cialdini weapons)",
        "execution_layer":    "Execution (Hopkins + Halbert)",
        "cta":                "CTA",
    }
    for key, label in layer_labels.items():
        checks = scorecard.get(key, {})
        if isinstance(checks, dict):
            # Filter out metadata-style keys that aren't actual checks
            actual_checks = {k: v for k, v in checks.items()
                             if isinstance(v, bool) or v == "na"}
            passes = sum(1 for v in actual_checks.values() if v is True)
            na = sum(1 for v in actual_checks.values() if v == "na")
            total_sub = len(actual_checks)
            na_label = f" + {na} N/A" if na else ""
            out += f"### {label} ({passes}/{total_sub} passing{na_label})\n\n"
            for check_name, passed in actual_checks.items():
                if passed is True:
                    icon = "✓"
                elif passed == "na":
                    icon = "—"
                else:
                    icon = "✗"
                nice_name = check_name.replace("_", " ").capitalize()
                out += f"- {icon} {nice_name}\n"
            out += "\n"

    # Diagnostic notes
    notes = result.get("diagnostic_notes", [])
    if notes:
        out += "## Diagnostic notes\n\n"
        for note in notes:
            out += f"- {note}\n"
        out += "\n"

    # Top 3 improvements
    improvements = result.get("top_3_improvements", [])
    if improvements:
        out += "## Top 3 improvements\n\n"
        for i, imp in enumerate(improvements, 1):
            out += f"{i}. {imp}\n"
        out += "\n"

    out += f"""---

## Interpretation

"""
    if cal_grade.startswith("A"):
        out += "Copy is strategically sound AND tactically shippable. Strong alignment across Schwartz awareness, Masterson lead types, and Cialdini weapons.\n"
    elif cal_grade.startswith("B"):
        out += "Copy is strong. A few framework gaps exist — apply the Top 3 improvements for the next iteration. Shippable now if WRITING_RULES 18-point also passes.\n"
    elif cal_grade.startswith("C"):
        out += "Copy has execution gaps worth addressing. **Check WRITING_RULES first** — if tactical rubric passes, still shippable while you build missing assets (case study, mechanism). Framework audit identifies the next-level improvements.\n"
    else:
        out += """Framework audit flagged structural gaps. **BUT do not auto-reject.**

Check two things before rewriting:
1. **Does WRITING_RULES 18-point rubric pass?** If yes, the copy is tactically shippable. Framework audit may be flagging asset gaps (no case study, no named mechanism) that can't be fixed without building real assets. Use the Top 3 improvements as a roadmap for what to build, not what to rewrite.
2. **Are the failing checks subjective (lead-type classification, awareness matching) or objective (missing numbers, bad CTA)?** Objective fails = rewrite. Subjective fails = iterate in future versions.
"""

    out += "\n*The 18-point WRITING_RULES tactical rubric is the SHIP GATE. This 27-point framework audit is a strategic diagnostic — use it to plan improvements, not to block shipping tactically-compliant copy.*\n"
    return out


def main():
    ap = argparse.ArgumentParser(
        description="Framework audit tool — grade cold email drafts against 27-point framework checklist",
        epilog="Asset gap flags: use --no-case-study and --no-named-mechanism (or --honest-mode for both) when the sender honestly lacks these marketing assets. These checks will be marked N/A instead of FAIL, producing a calibrated grade that reflects achievable quality with current assets.",
    )
    ap.add_argument("--subject", help="email subject line")
    ap.add_argument("--body", help="email body text")
    ap.add_argument("--file", help="read subject+body from a markdown/text file")
    ap.add_argument("--niche", default="", help="target niche (for context)")
    ap.add_argument("--client", default="", help="sender client for context")
    ap.add_argument("--what-we-sell", default="", help="offering description")
    ap.add_argument("--no-case-study", action="store_true",
                    help="flag: no verified case study available (marks `specific_social_proof` N/A)")
    ap.add_argument("--no-named-mechanism", action="store_true",
                    help="flag: no proprietary mechanism to name (marks `copy_matches_sophistication` N/A)")
    ap.add_argument("--honest-mode", action="store_true",
                    help="shorthand for --no-case-study + --no-named-mechanism (use for new verticals without battle-tested assets)")
    ap.add_argument("--out", help="save audit markdown to file (default: stdout only)")
    ap.add_argument("--json", action="store_true", help="print raw JSON instead of rendered markdown")
    args = ap.parse_args()

    # Honest-mode is shorthand for both asset gap flags
    no_case_study = args.no_case_study or args.honest_mode
    no_named_mechanism = args.no_named_mechanism or args.honest_mode

    # Parse input
    if args.file:
        content = Path(args.file).read_text()
        # Try to split on a SUBJECT line if present
        subj_match = re.search(r"(?im)^\s*SUBJECT\s*[:|=]\s*(.+)$", content)
        subject = subj_match.group(1).strip() if subj_match else ""
        body = re.sub(r"(?im)^\s*SUBJECT\s*[:|=].*$\n?", "", content, count=1).strip()
    else:
        if not args.subject or not args.body:
            ap.error("either --file OR both --subject and --body required")
        subject = args.subject
        body = args.body

    mode_note = ""
    if no_case_study and no_named_mechanism:
        mode_note = " (honest-mode: both case study + mechanism marked N/A)"
    elif no_case_study:
        mode_note = " (no case study — social proof marked N/A)"
    elif no_named_mechanism:
        mode_note = " (no named mechanism — sophistication fit marked N/A)"
    print(f"Auditing draft against 27-point framework checklist{mode_note}...")

    result = audit_draft(
        subject=subject, body=body,
        niche=args.niche, client=args.client, what_we_sell=args.what_we_sell,
        no_case_study=no_case_study, no_named_mechanism=no_named_mechanism,
    )

    if args.json:
        print(json.dumps(result, indent=2))
        return

    md = render_audit(subject, body, result, niche=args.niche,
                       no_case_study=no_case_study, no_named_mechanism=no_named_mechanism)

    if args.out:
        Path(args.out).write_text(md)
        print(f"\n✓ Audit saved: {args.out}")

    # Always print summary to stdout
    scorecard = result.get("scorecard_27pt", {})
    raw_pass = scorecard.get("raw_passing", scorecard.get("total_passing", 0))
    cal_pass = scorecard.get("calibrated_passing", raw_pass)
    cal_total = scorecard.get("calibrated_total", 27)
    na_count = scorecard.get("na_count", 0)
    raw_grade = result.get("raw_grade", result.get("grade", "?"))
    cal_grade = result.get("calibrated_grade", raw_grade)
    ship = result.get("ship_readiness", "?")
    raw_score = result.get("framework_match_score_raw", result.get("framework_match_score", 0))
    cal_score = result.get("framework_match_score_calibrated", raw_score)
    print(f"\n{'='*65}")
    if na_count > 0:
        print(f"Calibrated: {cal_grade}  ({cal_pass}/{cal_total})  ·  {cal_score}/100  ·  Ship: {ship}")
        print(f"Raw ceiling: {raw_grade}  ({raw_pass}/27)  ·  {raw_score}/100  ·  {na_count} check(s) N/A due to asset gaps")
    else:
        print(f"Grade: {cal_grade}  ({cal_pass}/27 passing)  ·  Framework match: {cal_score}/100  ·  Ship: {ship}")
    print(f"{'='*65}")
    if not args.out and not args.json:
        print(md)


if __name__ == "__main__":
    main()
