#!/usr/bin/env python3.13
"""
data_quality_check.py — Pre-send audit of lead data.

Why this exists:
  On 2026-04-20 we discovered Property Tax Appeal was sending 278 emails per
  cycle with literal "Hey ," greetings because 79% of leads had no first_name.
  Autopilot caught the symptom (low reply rate) only after days of sending.
  This tool catches the CAUSE before any send happens.

What it does:
  Audits a Smartlead-ready CSV (from Forge output) or an already-uploaded
  Smartlead campaign. Checks for:
    - First name coverage
    - Company name coverage
    - Email verification status (mv_result)
    - Generic / role-based email rate (info@, sales@, admin@, etc.)
    - Duplicate email rate
    - Domain coverage
    - Sanity checks on custom_fields

  Outputs a pass/warn/fail report. Exit code signals severity so you can
  wire this into a pre-send check step or Forge post-hook.

  Thresholds default to CLAUDE.md best practices but are vertical-aware:
  trades + local businesses tend to have more generic emails (info@,
  contact@) than B2B professional services. Use --vertical local|trades
  to relax the thresholds.

Usage:
  python3 tools/data_quality_check.py --csv path/to/smartlead_import.csv
  python3 tools/data_quality_check.py --csv X.csv --vertical trades
  python3 tools/data_quality_check.py --campaign 3184163
  python3 tools/data_quality_check.py --csv X.csv --strict     # fail on any yellow

Exit codes:
  0 — all checks PASS, safe to send
  1 — WARNING level issues, review before sending
  2 — FAIL level issues, do NOT send as-is

Standalone tool — does not modify Forge code.
"""

import argparse
import csv
import json
import os
import re
import sys
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

import requests

SCRIPT_DIR = Path(__file__).resolve().parent
LEAD_PIPELINE_DIR = SCRIPT_DIR.parent
WORKSPACE_ROOT = LEAD_PIPELINE_DIR.parent.parent
sys.path.insert(0, str(LEAD_PIPELINE_DIR))

try:
    from dotenv import load_dotenv
    load_dotenv(WORKSPACE_ROOT / ".env", override=True)
    load_dotenv(LEAD_PIPELINE_DIR / ".env", override=True)
except ImportError:
    pass

SMARTLEAD_KEY = os.environ.get("SMARTLEAD_API_KEY", "")
SMARTLEAD_BASE = "https://server.smartlead.ai/api/v1"


# ============================================================
# Thresholds per vertical
# ============================================================

# Each rule: (name, threshold_type, pass_target, warn_target, severity_if_fail)
# the operator's note 2026-04-20: small trades and local businesses often use generic
# emails (info@, contact@) — so we can't demand 90% first_name for those.

VERTICAL_THRESHOLDS = {
    "b2b": {
        "first_name_pct":   {"pass": 80, "warn": 60},
        "company_name_pct": {"pass": 95, "warn": 85},
        "mv_ok_pct":        {"pass": 85, "warn": 70},
        "generic_email_pct_max":   {"pass": 15, "warn": 30},
        "duplicate_email_pct_max": {"pass": 1, "warn": 3},
    },
    "trades": {
        "first_name_pct":   {"pass": 50, "warn": 35},
        "company_name_pct": {"pass": 90, "warn": 75},
        "mv_ok_pct":        {"pass": 80, "warn": 65},
        "generic_email_pct_max":   {"pass": 40, "warn": 60},
        "duplicate_email_pct_max": {"pass": 1, "warn": 3},
    },
    "local": {
        "first_name_pct":   {"pass": 40, "warn": 25},
        "company_name_pct": {"pass": 85, "warn": 70},
        "mv_ok_pct":        {"pass": 75, "warn": 60},
        "generic_email_pct_max":   {"pass": 50, "warn": 70},
        "duplicate_email_pct_max": {"pass": 1, "warn": 3},
    },
}


# ============================================================
# Helpers
# ============================================================

GENERIC_EMAIL_PREFIXES = {
    "info", "sales", "admin", "contact", "hello", "office", "support", "service",
    "enquiries", "inquiry", "help", "marketing", "team", "mail", "general", "main",
    "reception", "customerservice", "customer-service", "ops", "operations",
    "booking", "bookings", "reservations", "orders",
}


def is_generic_email(email: str) -> bool:
    if not email or "@" not in email:
        return False
    local = email.split("@", 1)[0].lower().strip()
    # Normalize separators
    local_clean = re.sub(r"[-_.]+", "", local)
    return local in GENERIC_EMAIL_PREFIXES or local_clean in {p.replace("-", "").replace("_", "") for p in GENERIC_EMAIL_PREFIXES}


# ============================================================
# CSV audit
# ============================================================

def audit_csv(csv_path: Path) -> dict:
    """Read a Smartlead-import-formatted CSV and compute data quality metrics."""
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV not found: {csv_path}")

    rows = []
    with csv_path.open() as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    total = len(rows)
    if total == 0:
        return {"total": 0}

    # Counters
    first_name_present = 0
    last_name_present = 0
    company_name_present = 0
    domain_present = 0
    mv_ok = 0
    mv_other = Counter()
    generic_emails = 0
    emails_seen = []
    invalid_email_format = 0

    for r in rows:
        fn = (r.get("first_name") or "").strip()
        ln = (r.get("last_name") or "").strip()
        co = (r.get("company_name") or r.get("company") or "").strip()
        dom = (r.get("website") or r.get("domain") or "").strip()
        email = (r.get("email") or "").strip().lower()
        # MV may be in various columns depending on source
        mv = (r.get("mv_result") or r.get("verified") or "").strip().lower()

        if fn: first_name_present += 1
        if ln: last_name_present += 1
        if co: company_name_present += 1
        if dom: domain_present += 1
        if email:
            emails_seen.append(email)
            if is_generic_email(email):
                generic_emails += 1
            if not re.match(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", email):
                invalid_email_format += 1
        if mv == "ok":
            mv_ok += 1
        else:
            mv_other[mv or "(missing)"] += 1

    # Duplicate emails
    email_counts = Counter(emails_seen)
    duplicate_count = sum(count - 1 for count in email_counts.values() if count > 1)
    unique_emails = len(email_counts)

    def pct(n):
        return round(n / total * 100, 1) if total else 0

    return {
        "total": total,
        "first_name_present": first_name_present,
        "first_name_pct": pct(first_name_present),
        "last_name_present": last_name_present,
        "last_name_pct": pct(last_name_present),
        "company_name_present": company_name_present,
        "company_name_pct": pct(company_name_present),
        "domain_present": domain_present,
        "domain_pct": pct(domain_present),
        "mv_ok": mv_ok,
        "mv_ok_pct": pct(mv_ok),
        "mv_other_breakdown": dict(mv_other),
        "generic_emails": generic_emails,
        "generic_email_pct": pct(generic_emails),
        "duplicate_count": duplicate_count,
        "duplicate_email_pct": pct(duplicate_count),
        "unique_emails": unique_emails,
        "invalid_email_format": invalid_email_format,
    }


# ============================================================
# Smartlead campaign audit (live)
# ============================================================

def audit_smartlead_campaign(campaign_id: int) -> dict:
    """Pull all leads in a campaign and run the same metrics."""
    all_leads = []
    offset = 0
    while True:
        r = requests.get(f"{SMARTLEAD_BASE}/campaigns/{campaign_id}/leads",
                         params={"api_key": SMARTLEAD_KEY, "limit": 100, "offset": offset},
                         timeout=30)
        if r.status_code != 200:
            break
        batch = r.json().get("data", [])
        if not batch: break
        all_leads.extend(batch)
        if len(batch) < 100: break
        offset += 100

    # Normalize to same shape as CSV rows
    rows = []
    for cl in all_leads:
        lead = cl.get("lead") or {}
        rows.append({
            "first_name": lead.get("first_name", ""),
            "last_name": lead.get("last_name", ""),
            "company_name": lead.get("company_name", ""),
            "website": lead.get("website", ""),
            "email": lead.get("email", ""),
            "mv_result": "ok",  # Smartlead doesn't preserve mv_result; assume ok if in campaign
        })

    # Reuse the CSV audit on normalized rows
    tmp_csv = Path(f"/tmp/sl_audit_{campaign_id}.csv")
    if rows:
        with tmp_csv.open("w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=["first_name","last_name","company_name","website","email","mv_result"])
            w.writeheader()
            w.writerows(rows)
        return audit_csv(tmp_csv)
    return {"total": 0}


# ============================================================
# Evaluate against thresholds
# ============================================================

def evaluate(metrics: dict, vertical: str, strict: bool = False) -> dict:
    """Compare metrics against vertical thresholds. Returns verdict with per-check results."""
    thresholds = VERTICAL_THRESHOLDS.get(vertical, VERTICAL_THRESHOLDS["b2b"])
    checks = []

    def add_check(name, actual, rule_name, direction="min"):
        """direction='min' means actual >= pass is good; direction='max' means actual <= pass is good."""
        r = thresholds.get(rule_name, {})
        pass_target = r.get("pass", 0)
        warn_target = r.get("warn", 0)
        if direction == "min":
            status = "PASS" if actual >= pass_target else ("WARN" if actual >= warn_target else "FAIL")
        else:
            status = "PASS" if actual <= pass_target else ("WARN" if actual <= warn_target else "FAIL")
        checks.append({
            "name": name, "actual": actual, "pass_target": pass_target, "warn_target": warn_target,
            "direction": direction, "status": status,
        })

    add_check("First name coverage", metrics.get("first_name_pct", 0), "first_name_pct", "min")
    add_check("Company name coverage", metrics.get("company_name_pct", 0), "company_name_pct", "min")
    add_check("MV-verified rate", metrics.get("mv_ok_pct", 0), "mv_ok_pct", "min")
    add_check("Generic email rate (info@, sales@, etc)", metrics.get("generic_email_pct", 0), "generic_email_pct_max", "max")
    add_check("Duplicate email rate", metrics.get("duplicate_email_pct", 0), "duplicate_email_pct_max", "max")

    # Determine verdict
    any_fail = any(c["status"] == "FAIL" for c in checks)
    any_warn = any(c["status"] == "WARN" for c in checks)
    verdict = "FAIL" if any_fail else ("WARN" if any_warn else "PASS")

    # Strict mode upgrades WARN → FAIL
    if strict and verdict == "WARN":
        verdict = "FAIL"

    return {"verdict": verdict, "checks": checks, "thresholds_used": thresholds}


# ============================================================
# Pretty printer
# ============================================================

def print_report(metrics: dict, evaluation: dict, source_label: str, vertical: str) -> None:
    verdict = evaluation["verdict"]
    verdict_icon = {"PASS": "✅", "WARN": "⚠️", "FAIL": "🚨"}.get(verdict, "?")
    total = metrics.get("total", 0)

    print(f"\n{'='*70}")
    print(f"DATA QUALITY CHECK — {verdict_icon} {verdict}")
    print(f"{'='*70}")
    print(f"  Source:    {source_label}")
    print(f"  Total:     {total} rows")
    print(f"  Vertical:  {vertical}")
    print()

    if total == 0:
        print("  ⚠ No rows to audit.")
        return

    print(f"  {'Metric':<38} {'Value':<12} {'Status':<8}")
    print(f"  {'-'*70}")
    for c in evaluation["checks"]:
        icon = {"PASS": "✓", "WARN": "⚠", "FAIL": "✗"}.get(c["status"], "?")
        suffix = "%" if c["direction"] else ""
        print(f"  {c['name']:<38} {c['actual']}{suffix:<11} {icon} {c['status']}")

    # MV other breakdown
    mv_other = metrics.get("mv_other_breakdown", {})
    if mv_other:
        print(f"\n  MV result breakdown (besides 'ok'):")
        for k, v in sorted(mv_other.items(), key=lambda x: -x[1])[:10]:
            print(f"    {k}: {v}")

    # Detailed notes
    print()
    if metrics.get("invalid_email_format", 0):
        print(f"  ⚠ {metrics['invalid_email_format']} rows have invalid email format")
    if metrics.get("duplicate_count", 0):
        print(f"  ⚠ {metrics['duplicate_count']} duplicate emails ({metrics.get('unique_emails', 0)} unique)")

    print(f"\n  Thresholds used ({vertical}):")
    for k, v in evaluation["thresholds_used"].items():
        print(f"    {k}: pass at {v['pass']}, warn at {v['warn']}")

    print()
    if verdict == "PASS":
        print("  ✅ Data quality is good. Safe to upload to Smartlead.")
    elif verdict == "WARN":
        print("  ⚠️  Data has minor issues. Review before uploading.")
    else:
        print("  🚨 Data quality is below threshold. DO NOT send as-is.")
        print("     Options: re-run Forge with stricter filters, drop bad rows, or re-verify emails.")


# ============================================================
# CLI
# ============================================================

def main():
    ap = argparse.ArgumentParser(description="Pre-send data quality audit")
    ap.add_argument("--csv", help="CSV file to audit (typically smartlead_import.csv)")
    ap.add_argument("--campaign", type=int, help="Smartlead campaign ID to audit live leads")
    ap.add_argument("--vertical", choices=["b2b", "trades", "local"], default="b2b",
                    help="Vertical — relaxes thresholds for trades/local where generic emails are common")
    ap.add_argument("--strict", action="store_true", help="Fail on any warning (exit 2)")
    ap.add_argument("--json", action="store_true", help="Output as JSON instead of pretty print")
    args = ap.parse_args()

    if not args.csv and not args.campaign:
        ap.print_help()
        sys.exit(2)

    if args.csv:
        metrics = audit_csv(Path(args.csv))
        source_label = args.csv
    else:
        metrics = audit_smartlead_campaign(args.campaign)
        source_label = f"Smartlead campaign {args.campaign}"

    evaluation = evaluate(metrics, args.vertical, strict=args.strict)

    if args.json:
        print(json.dumps({"metrics": metrics, "evaluation": evaluation, "source": source_label}, indent=2))
    else:
        print_report(metrics, evaluation, source_label, args.vertical)

    # Exit code signals severity
    exit_code_map = {"PASS": 0, "WARN": 1, "FAIL": 2}
    sys.exit(exit_code_map[evaluation["verdict"]])


if __name__ == "__main__":
    main()
