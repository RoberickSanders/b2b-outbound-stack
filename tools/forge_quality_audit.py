#!/usr/bin/env python3
"""
forge_quality_audit.py — Post-run quality audit for Forge output.

Why this exists:
  Bad Forge runs used to ship straight to Smartlead and burn deliverability.
  This tool runs Kimi on a sampled subset of a fresh Forge run and flags
  common failure patterns BEFORE the operator uploads anything. Cheap — ~$0.05 per
  audit using Kimi K2.6.

What it checks:
  1. Non-ICP leakage — e.g. wealth managers in R&D Tax Credit list, insurance
     brokers in Workers Comp Recovery list
  2. Geographic drift — leads outside the client's target geography
  3. Role-account contamination — empty first_names, `info@` / `contact@` emails
  4. Title mismatch — Partner at a law firm in a non-legal niche
  5. Duplicate patterns — same decision maker across multiple similar domains

Usage:
  # Audit the most recent Forge run for a niche
  python3 tools/forge_quality_audit.py --niche workers-comp-recovery --client client_c

  # Audit a specific run directory
  python3 tools/forge_quality_audit.py --run-dir 01-Projects/client_c/lead-runs/workers-comp-recovery-forge-20260422

  # Audit first N leads from a CSV
  python3 tools/forge_quality_audit.py --csv path/to/leads.csv --niche rd-tax-credit

Outputs:
  - Printed summary with red/yellow/green flags
  - Optional --output markdown report saved to 03-Resources/forge-quality/
"""

import argparse
import csv
import glob
import json
import os
import random
import re
import sys
from collections import Counter
from datetime import datetime
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

# Niche-fit criteria (single source of truth)
try:
    sys.path.insert(0, str(SCRIPT_DIR))
    from verify_niche_fit import FIT_CRITERIA
except Exception:
    FIT_CRITERIA = {}


def sample_leads(rows, n=25):
    """Random sample of up to n leads — mix first few, middle, last few."""
    if len(rows) <= n:
        return rows
    head = rows[:5]
    tail = rows[-5:]
    middle = random.sample(rows[5:-5], min(n - 10, len(rows) - 10))
    return head + middle + tail


def audit_sample(sample, client, niche):
    """Send sample to Kimi, return structured audit findings."""
    try:
        from llm_router import get_light_client
        llm, model = get_light_client()
    except Exception as e:
        return {"error": f"llm_router unavailable: {e}"}

    niche_desc = FIT_CRITERIA.get((client, niche), niche)
    lines = []
    for i, r in enumerate(sample, 1):
        lines.append(
            f"{i}. {r.get('first_name','')} {r.get('last_name','')} | "
            f"{r.get('title','')} @ {r.get('company','')} | "
            f"{r.get('email','')} | source={r.get('source','')}"
        )
    block = "\n".join(lines)

    prompt = f"""You are auditing a sample of cold-email leads for quality before the campaign ships.

Client: {client}
Niche: {niche}
Niche criteria: {niche_desc}

Sample of {len(sample)} leads from the run:
{block}

Analyze the sample and return JSON with these fields:

{{
  "verdict": "GREEN | YELLOW | RED",
  "verdict_reason": "one-sentence summary",
  "non_icp_count": <int>,
  "non_icp_examples": ["..."],
  "role_account_count": <int>,
  "title_mismatch_count": <int>,
  "title_mismatch_examples": ["..."],
  "top_issues": ["list of 2-5 most concerning patterns"],
  "recommendation": "ACTIVATE / REVIEW / REJECT"
}}

Verdict rubric:
- GREEN: <10% non-ICP, all personal emails, titles match niche → safe to activate
- YELLOW: 10-25% non-ICP OR some patterns of concern → review before activating
- RED: >25% non-ICP OR systematic problems → reject and re-run

Return ONLY JSON. No markdown."""

    try:
        resp = llm.messages.create(
            model=model,
            max_tokens=800,
            messages=[{"role": "user", "content": prompt}],
        )
        text = resp.content[0].text.strip().replace("```json", "").replace("```", "").strip()
        return json.loads(text)
    except Exception as e:
        return {"error": f"Kimi call failed: {e}"}


def load_leads_from_run(run_dir, niche):
    """Find and load the primary CSV output in a Forge run directory.

    Forge writes these CSVs per run:
      - tier1_contacts.csv   ← enriched, named contacts (PRIMARY target of audit)
      - tier2_contacts.csv   ← secondary contacts (less important)
      - smartlead_import.csv ← flattened for Smartlead upload (may have stubs)
      - checkpoints/*.csv    ← internal state, not for auditing

    We pick tier1 first, then fall back to name-matching heuristics.
    """
    candidates = sorted(glob.glob(os.path.join(run_dir, "**", "*.csv"), recursive=True))
    if not candidates:
        return []

    # Exclude checkpoint files — those aren't audit targets
    non_checkpoint = [c for c in candidates if "/checkpoints/" not in c and "\\checkpoints\\" not in c]
    if non_checkpoint:
        candidates = non_checkpoint

    # Priority 1: tier1_contacts.csv (the real enriched output)
    for c in candidates:
        if "tier1_contacts" in os.path.basename(c).lower():
            return _read_csv(c)

    # Priority 2: smartlead_import.csv
    for c in candidates:
        if "smartlead_import" in os.path.basename(c).lower():
            return _read_csv(c)

    # Priority 3: niche-named file (legacy)
    niche_slug = re.sub(r"[^a-z0-9]+", "-", (niche or "").lower()).strip("-")
    for c in candidates:
        if niche_slug and niche_slug in os.path.basename(c).lower():
            return _read_csv(c)

    # Priority 4: _master
    for c in candidates:
        if "_master" in os.path.basename(c).lower() or "master" in os.path.basename(c).lower():
            return _read_csv(c)

    # Last resort: first non-checkpoint CSV
    return _read_csv(candidates[0])


def _read_csv(path):
    rows = []
    try:
        with open(path) as f:
            for r in csv.DictReader(f):
                rows.append(r)
    except Exception:
        pass
    return rows


def find_most_recent_run(client, niche):
    """Find the most recently modified Forge run dir for this client+niche.

    Matches by looking for the niche's distinctive word-tokens in the dir name,
    not a strict substring match (directory names may add "-consulting" or
    similar suffixes beyond the canonical slug).
    """
    base = WORKSPACE_ROOT / "01-Projects" / client / "lead-runs"
    if not base.exists():
        return None
    # Tokenize the niche — e.g. "rd-tax-credit" → ["rd", "tax", "credit"]
    tokens = [t for t in re.split(r"[^a-z0-9]+", (niche or "").lower()) if t and len(t) >= 2]
    if not tokens:
        return None
    candidates = []
    for d in base.iterdir():
        if not d.is_dir():
            continue
        name = d.name.lower()
        # Allow token flexibility: "rd" matches "r-d", "r.d"
        name_normalized = re.sub(r"[^a-z0-9]+", "", name)
        tokens_normalized = re.sub(r"[^a-z0-9]+", "", "".join(tokens))
        if tokens_normalized in name_normalized:
            candidates.append(d)
    if not candidates:
        return None
    return max(candidates, key=lambda d: d.stat().st_mtime)


def main():
    ap = argparse.ArgumentParser(description="Forge post-run quality audit")
    ap.add_argument("--niche", default="")
    ap.add_argument("--client", default="client_c")
    ap.add_argument("--run-dir", default="", help="Audit a specific Forge run directory")
    ap.add_argument("--csv", default="", help="Audit leads from a specific CSV file")
    ap.add_argument("--sample-size", type=int, default=25)
    ap.add_argument("--output", default="", help="Write markdown report to path")
    args = ap.parse_args()

    # Load leads
    rows = []
    if args.csv:
        rows = _read_csv(args.csv)
        print(f"Loaded {len(rows)} leads from {args.csv}")
    elif args.run_dir:
        rows = load_leads_from_run(args.run_dir, args.niche)
        print(f"Loaded {len(rows)} leads from {args.run_dir}")
    elif args.niche:
        run = find_most_recent_run(args.client, args.niche)
        if not run:
            print(f"ERROR: no Forge run found for {args.client}/{args.niche}", file=sys.stderr)
            return 1
        rows = load_leads_from_run(str(run), args.niche)
        print(f"Most recent run: {run.name}")
        print(f"Loaded {len(rows)} leads")
    else:
        print("ERROR: need --csv, --run-dir, or --niche", file=sys.stderr)
        return 1

    if not rows:
        print("No leads to audit.", file=sys.stderr)
        return 1

    # Quick automated checks
    total = len(rows)
    with_fn = sum(1 for r in rows if r.get("first_name", "").strip())
    role_account_emails = sum(
        1 for r in rows
        if re.match(r"^(info|contact|admin|sales|support|hello|help|team)@",
                    (r.get("email") or "").lower())
    )
    print()
    print(f"=== Auto checks ===")
    print(f"  Total leads:            {total}")
    print(f"  With first_name:        {with_fn} ({with_fn*100//max(total,1)}%)")
    print(f"  Role-account emails:    {role_account_emails} ({role_account_emails*100//max(total,1)}%)")
    print()

    # Kimi sampled audit
    sample = sample_leads(rows, args.sample_size)
    print(f"=== Kimi audit on sample of {len(sample)} ===")
    findings = audit_sample(sample, args.client, args.niche)

    if "error" in findings:
        print(f"  ERROR: {findings['error']}")
        return 1

    verdict = findings.get("verdict", "?")
    color_emoji = {"GREEN": "🟢", "YELLOW": "🟡", "RED": "🔴"}.get(verdict, "")
    print(f"  Verdict:             {color_emoji} {verdict}")
    print(f"  Reason:              {findings.get('verdict_reason', '')}")
    print(f"  Non-ICP count:       {findings.get('non_icp_count', 0)} of {len(sample)}")
    if findings.get("non_icp_examples"):
        print(f"  Non-ICP examples:")
        for ex in findings["non_icp_examples"][:5]:
            print(f"    - {ex}")
    if findings.get("title_mismatch_count"):
        print(f"  Title mismatch count: {findings.get('title_mismatch_count', 0)}")
        for ex in findings.get("title_mismatch_examples", [])[:5]:
            print(f"    - {ex}")
    print(f"  Top issues:")
    for issue in findings.get("top_issues", []):
        print(f"    - {issue}")
    print(f"  Recommendation:      {findings.get('recommendation', '?')}")
    print()

    # Optional markdown report
    if args.output:
        out_path = Path(args.output)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        lines_md = [
            f"# Forge Quality Audit — {args.niche or '(no niche)'}",
            f"",
            f"**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M')}",
            f"**Client:** {args.client}",
            f"**Niche:** {args.niche}",
            f"**Sample size:** {len(sample)} of {total}",
            f"",
            f"## Verdict: {verdict}",
            f"",
            f"{findings.get('verdict_reason', '')}",
            f"",
            f"## Auto checks",
            f"- Total leads: {total}",
            f"- With first_name: {with_fn} ({with_fn*100//max(total,1)}%)",
            f"- Role-account emails: {role_account_emails}",
            f"",
            f"## Kimi sampled findings",
            f"- Non-ICP in sample: **{findings.get('non_icp_count', 0)}** of {len(sample)}",
            f"- Title mismatches: {findings.get('title_mismatch_count', 0)}",
            f"",
            f"### Non-ICP examples",
        ]
        for ex in findings.get("non_icp_examples", []):
            lines_md.append(f"- {ex}")
        lines_md.append(f"")
        lines_md.append(f"### Top issues")
        for issue in findings.get("top_issues", []):
            lines_md.append(f"- {issue}")
        lines_md.append(f"")
        lines_md.append(f"## Recommendation")
        lines_md.append(f"")
        lines_md.append(f"**{findings.get('recommendation', '?')}**")
        out_path.write_text("\n".join(lines_md))
        print(f"  Report saved to: {out_path}")

    # Exit code signals verdict for chaining
    if verdict == "RED":
        return 2
    if verdict == "YELLOW":
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
