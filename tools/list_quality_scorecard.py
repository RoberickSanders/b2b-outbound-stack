#!/usr/bin/env python3.13
"""
list_quality_scorecard.py — Pre-send signal-quality scorecard for lead lists.

Mirrors Eric Nowoslawski's `list-quality-scorecard` (8 dimensions, letter grade)
but adapted to the Forge schema and integrated with our existing audit tooling.

This is the *strategic* scorecard — does this list look like good targets? Pair
with `data_quality_check.py` (the *integrity* audit — names + emails + dupes
threshold-checked vertical-aware).

The 8 dimensions, scored 0-100:
  1. Email verification coverage    (% with mv_result='ok')
  2. Duplicate email rate           (lower is better)
  3. Per-domain concentration       (avg leads per domain — ~1-2 ideal)
  4. Title relevance                (% matching declared ICP titles)
  5. Bad-title detection            (% matching known bad patterns)
  6. Catch-all / generic email      (% of info@/contact@/sales@ etc.)
  7. ICP fit                        (% matching declared industry filter)
  8. Name quality                   (% with first + last + non-fake)

Letter grade (verification + ICP weighted 2x):
  A+/A: 90-100   ship it
  B:    80-89    minor fixes, ship after
  C:    70-79    fix top 3 issues first
  D:    60-69    serious cleanup required
  F:    <60      do NOT send. rebuild.

Usage:
  python3 tools/list_quality_scorecard.py --csv path/to/leads.csv

  # With ICP file (CLIENT.md or a YAML profile) for dimensions 4 + 7
  python3 tools/list_quality_scorecard.py --csv leads.csv \\
      --client client_a \\
      --icp-niche "fire protection"

  # Custom output path
  python3 tools/list_quality_scorecard.py --csv leads.csv --out scorecard.md

  # Stop the pipeline on grade < B
  python3 tools/list_quality_scorecard.py --csv leads.csv --min-grade B

Exit codes:
  0 — grade >= min_grade (default B)
  1 — grade is one tier below min_grade
  2 — grade is two+ tiers below min_grade

Standalone tool — does not modify Forge core or master DB.
"""

import argparse
import csv
import json
import re
import sys
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
LEAD_PIPELINE_DIR = SCRIPT_DIR.parent
WORKSPACE_ROOT = LEAD_PIPELINE_DIR.parent.parent
OUT_DIR = LEAD_PIPELINE_DIR / "list-scorecards"


# ============================================================
# Reference patterns
# ============================================================

GENERIC_EMAIL_PREFIXES = {
    "info", "sales", "admin", "contact", "hello", "office", "support", "service",
    "enquiries", "inquiry", "inquiries", "help", "marketing", "team", "mail",
    "general", "main", "reception", "customerservice", "ops", "operations",
    "booking", "bookings", "reservations", "orders", "billing", "accounts",
    "noreply", "no-reply", "donotreply", "do-not-reply", "webmaster",
}

BAD_TITLE_PATTERNS = [
    r"\bintern\b", r"\bassistant\b", r"\bcoordinator\b", r"\bstudent\b",
    r"\bpart[\s\-]?time\b", r"\bretired\b", r"\bvolunteer\b",
    r"\bjunior\b", r"\bjr\.?\s", r"\btrainee\b", r"\bapprentice\b",
    r"\b(former|ex)[\s\-]\b",  # "former CEO", "ex-VP"
    r"\b(receptionist|front desk|secretary)\b",
]

FAKE_NAME_PATTERNS = {
    "admin", "info", "support", "sales", "team", "owner", "manager", "office",
    "hello", "contact", "user", "test", "n/a", "na", "none", "unknown",
}


# ============================================================
# Helpers
# ============================================================

def now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def slug_now() -> str:
    return datetime.now().strftime("%Y%m%d-%H%M")


def is_generic_email(email: str) -> bool:
    if not email or "@" not in email:
        return False
    local = email.split("@", 1)[0].lower().strip()
    local_clean = re.sub(r"[-_.]+", "", local)
    return (local in GENERIC_EMAIL_PREFIXES
            or local_clean in {p.replace("-", "").replace("_", "")
                               for p in GENERIC_EMAIL_PREFIXES})


def is_bad_title(title: str) -> bool:
    if not title:
        return False
    t = title.lower()
    return any(re.search(p, t) for p in BAD_TITLE_PATTERNS)


def is_fake_name(name: str) -> bool:
    if not name:
        return True
    n = name.strip().lower()
    if n in FAKE_NAME_PATTERNS:
        return True
    if "@" in n:  # email-as-name
        return True
    if n.isupper() and len(n) > 2:  # ALL CAPS — usually a placeholder
        return True
    if re.match(r"^[a-z]{1,2}$", n):  # single letters
        return True
    return False


def domain_of(email: str) -> str:
    return email.split("@", 1)[1].lower() if email and "@" in email else ""


# ============================================================
# Score one dimension at a time
# ============================================================

def score_verification(rows: list[dict]) -> tuple[float, str]:
    """100 if 100% mv_result='ok'. Drops linearly to 0 at <50%."""
    n = len(rows)
    ok = sum(1 for r in rows if (r.get("mv_result", "") or r.get("verified", "")
                                 ).strip().lower() == "ok")
    pct = (ok / n) * 100 if n else 0
    if pct >= 99:
        score, note = 100, f"{ok}/{n} verified ({pct:.1f}%)"
    elif pct >= 50:
        score, note = (pct - 50) * 2, f"{ok}/{n} verified ({pct:.1f}%) — verify the rest"
    else:
        score, note = 0, f"only {ok}/{n} verified ({pct:.1f}%) — must verify before sending"
    return round(score, 1), note


def score_dup_emails(rows: list[dict]) -> tuple[float, str]:
    emails = [(r.get("email") or "").lower().strip() for r in rows
              if r.get("email")]
    n = len(emails)
    if n == 0:
        return 0, "no emails to score"
    seen = Counter(emails)
    dup = sum(c for c in seen.values() if c > 1) - len({e for e, c in seen.items()
                                                        if c > 1})
    pct = (dup / n) * 100
    if pct == 0:
        return 100, "0 duplicates"
    if pct < 1:
        return 95, f"{dup} duplicates ({pct:.1f}%) — minor"
    if pct < 5:
        return max(0, 100 - pct * 10), f"{dup} duplicates ({pct:.1f}%) — dedupe before send"
    return max(0, 50 - pct), f"{dup} duplicates ({pct:.1f}%) — significant"


def score_dup_domains(rows: list[dict]) -> tuple[float, str]:
    by_domain: dict[str, int] = defaultdict(int)
    for r in rows:
        d = domain_of((r.get("email") or "").strip())
        if d:
            by_domain[d] += 1
    if not by_domain:
        return 0, "no domains"
    avg = sum(by_domain.values()) / len(by_domain)
    over = [(d, c) for d, c in by_domain.items() if c >= 5]
    if avg < 2:
        return 100, f"avg {avg:.1f} leads/domain"
    if avg < 5:
        return 60, f"avg {avg:.1f} leads/domain — over-concentration warning ({len(over)} domains with 5+)"
    return 30, f"avg {avg:.1f} leads/domain — heavy concentration ({len(over)} domains with 5+)"


def score_title_relevance(rows: list[dict], icp_titles: list[str]) -> tuple[float, str]:
    if not icp_titles:
        return None, "skipped — no ICP titles supplied"
    n = len(rows)
    if n == 0:
        return 0, "no rows"
    targets = [t.lower() for t in icp_titles]
    hits = 0
    for r in rows:
        t = (r.get("title") or r.get("job_title") or "").lower()
        if any(tt in t or t in tt for tt in targets):
            hits += 1
    pct = (hits / n) * 100
    if pct >= 80:
        return 100, f"{hits}/{n} match ICP titles ({pct:.1f}%)"
    if pct >= 40:
        return 50, f"{hits}/{n} match ({pct:.1f}%) — drift; tune Prospeo/Blitz filters"
    return 0, f"only {hits}/{n} match ({pct:.1f}%) — major drift"


def score_bad_titles(rows: list[dict]) -> tuple[float, str]:
    n = len(rows)
    if n == 0:
        return 0, "no rows"
    bad = sum(1 for r in rows if is_bad_title(r.get("title")
                                              or r.get("job_title", "")))
    pct = (bad / n) * 100
    if pct < 2:
        return 100, f"{bad} bad-title rows ({pct:.1f}%)"
    if pct < 10:
        return max(0, 100 - (pct - 2) * 8), f"{bad} bad-title rows ({pct:.1f}%) — filter"
    return 0, f"{bad} bad-title rows ({pct:.1f}%) — Prospeo/Blitz filter too loose"


def score_catch_all(rows: list[dict]) -> tuple[float, str]:
    n = len(rows)
    if n == 0:
        return 0, "no rows"
    generic = sum(1 for r in rows if is_generic_email(r.get("email", "")))
    pct = (generic / n) * 100
    if pct < 5:
        return 100, f"{generic} generic emails ({pct:.1f}%)"
    if pct < 15:
        return 50, f"{generic} generic emails ({pct:.1f}%) — drop or deprioritize"
    return 0, f"{generic} generic emails ({pct:.1f}%) — too many for B2B"


def score_icp_fit(rows: list[dict], icp_industries: list[str]) -> tuple[float, str]:
    if not icp_industries:
        return None, "skipped — no ICP industries supplied"
    n = len(rows)
    if n == 0:
        return 0, "no rows"
    targets = [i.lower() for i in icp_industries]
    hits = 0
    for r in rows:
        ind = (r.get("industry") or r.get("company_industry") or "").lower()
        if any(t in ind or ind in t for t in targets):
            hits += 1
    pct = (hits / n) * 100
    if pct >= 80:
        return 100, f"{hits}/{n} match declared industries ({pct:.1f}%)"
    if pct >= 40:
        return 50, f"{hits}/{n} match ({pct:.1f}%) — broaden filter or tighten list"
    return 0, f"only {hits}/{n} match ({pct:.1f}%) — list mostly off-ICP"


def score_name_quality(rows: list[dict]) -> tuple[float, str]:
    n = len(rows)
    if n == 0:
        return 0, "no rows"
    good = 0
    for r in rows:
        fn = (r.get("first_name") or "").strip()
        ln = (r.get("last_name") or "").strip()
        if fn and ln and not is_fake_name(fn) and not is_fake_name(ln):
            good += 1
    pct = (good / n) * 100
    if pct >= 95:
        return 100, f"{good}/{n} have clean first+last ({pct:.1f}%)"
    if pct >= 80:
        return 70, f"{good}/{n} have clean first+last ({pct:.1f}%) — clean before send"
    return max(0, pct - 20), f"only {good}/{n} clean ({pct:.1f}%) — half your sends will say 'Hey ,'"


# ============================================================
# ICP loading from CLIENT.md or YAML profile
# ============================================================

def load_icp(client: str | None, niche: str | None,
             icp_titles: list[str], icp_industries: list[str]) -> tuple[list[str], list[str]]:
    """Return (titles, industries). Args take precedence over CLIENT.md."""
    if icp_titles or icp_industries:
        return icp_titles or [], icp_industries or []
    if not client:
        return [], []
    client_md = WORKSPACE_ROOT / "01-Projects" / client / "CLIENT.md"
    if not client_md.is_file():
        return [], []
    text = client_md.read_text()
    # Extract a YAML block
    m = re.search(r"```yaml(.*?)```", text, re.S)
    if not m:
        return [], []
    yaml_str = m.group(1)
    titles = []
    industries = []
    # Cheap YAML parse for the keys we care about
    for line in yaml_str.splitlines():
        line = line.strip()
        if line.startswith("target_audience:"):
            val = line.split(":", 1)[1].strip()
            # Often "Owners of commercial buildings" — pull comma-split
            titles += [t.strip() for t in re.split(r"[,;|]", val) if t.strip()]
        if line.startswith("industries:") or line.startswith("verticals:"):
            val = line.split(":", 1)[1].strip()
            industries += [i.strip() for i in re.split(r"[,;|]", val) if i.strip()]
    return titles, industries


# ============================================================
# Aggregate + grade
# ============================================================

def grade_letter(score: float) -> tuple[str, str]:
    if score >= 95:
        return "A+", "Ship it. List looks great."
    if score >= 90:
        return "A", "Ship it."
    if score >= 80:
        return "B", "Minor fixes, then ship."
    if score >= 70:
        return "C", "Fix top 3 issues first."
    if score >= 60:
        return "D", "Serious cleanup required."
    return "F", "Don't send. Rebuild the list."


GRADE_RANK = {"A+": 6, "A": 5, "B": 4, "C": 3, "D": 2, "F": 1}


def render_markdown(scorecard: dict, csv_path: Path) -> str:
    out = []
    out.append(f"# List Quality Scorecard")
    out.append("")
    out.append(f"**File:** `{csv_path.name}`")
    out.append(f"**Rows:** {scorecard['n_rows']}")
    out.append(f"**Generated:** {scorecard['generated_at']}")
    out.append("")
    out.append(f"## Grade: **{scorecard['letter']}** ({scorecard['weighted_score']:.1f}/100)")
    out.append("")
    out.append(f"> {scorecard['action']}")
    out.append("")
    out.append("## Dimensions")
    out.append("")
    out.append("| # | Dimension | Score | Note |")
    out.append("|---|-----------|------:|------|")
    for i, dim in enumerate(scorecard["dimensions"], start=1):
        score = dim["score"]
        score_str = "skip" if score is None else f"{score}/100"
        out.append(f"| {i} | {dim['name']} | {score_str} | {dim['note']} |")
    out.append("")
    if scorecard["issues"]:
        out.append("## Top issues to fix")
        out.append("")
        for i, issue in enumerate(scorecard["issues"], start=1):
            out.append(f"{i}. {issue}")
        out.append("")
    out.append("## Pre-send checklist")
    out.append("")
    for cb in scorecard["checklist"]:
        out.append(f"- [ ] {cb}")
    out.append("")
    out.append("---")
    out.append("")
    out.append(f"_Run again after fixes:_ `f score-list --csv {csv_path}`")
    return "\n".join(out)


def collect_issues(dims: list[dict]) -> list[str]:
    issues = []
    for d in dims:
        if d["score"] is None:
            continue
        if d["score"] < 80:
            issues.append(f"{d['name']}: {d['note']}")
    return issues[:5]


def generate_checklist(dims: list[dict], n_rows: int) -> list[str]:
    cb = ["Deduplicate by email"]
    for d in dims:
        if d["score"] is None or d["score"] >= 80:
            continue
        name = d["name"].lower()
        if "verification" in name:
            cb.append("Re-verify all emails (MillionVerifier or BounceBan)")
        if "catch-all" in name or "generic" in name:
            cb.append("Drop or deprioritize info@/sales@/contact@ rows")
        if "bad-title" in name or "title relevance" in name:
            cb.append("Filter titles by seniority ≥ Manager / Owner")
        if "domain" in name and "concentration" in d["note"].lower():
            cb.append("Cap leads per domain to 3 max")
        if "icp fit" in name:
            cb.append("Filter rows whose company_industry is outside the ICP")
        if "name" in name:
            cb.append("Drop rows missing first_name or with fake names (admin, info, etc.)")
    if n_rows > 5000:
        cb.append("Split into batches of 1,500-2,000 for first send")
    return cb


# ============================================================
# Main
# ============================================================

def main():
    p = argparse.ArgumentParser(
        prog="list_quality_scorecard",
        description="Pre-send 8-dimensional quality scorecard for lead lists.")
    p.add_argument("--csv", required=True, help="Path to leads CSV")
    p.add_argument("--out", help="Output markdown path "
                                 "(default: list-scorecards/<csv>-<ts>.md)")
    p.add_argument("--client", help="Client slug — used to load ICP from CLIENT.md")
    p.add_argument("--icp-titles", help="Comma-separated ICP job titles")
    p.add_argument("--icp-industries", help="Comma-separated ICP industries")
    p.add_argument("--icp-niche", help="(Reserved for future) niche-aware ICP")
    p.add_argument("--min-grade", default="B",
                   choices=list(GRADE_RANK.keys()),
                   help="Minimum acceptable grade (default B)")
    p.add_argument("--json-out", help="Also write JSON output for programmatic use")
    args = p.parse_args()

    csv_path = Path(args.csv)
    if not csv_path.is_file():
        print(f"CSV not found: {csv_path}", file=sys.stderr)
        return 2

    rows = []
    with csv_path.open() as f:
        for r in csv.DictReader(f):
            rows.append(r)

    if not rows:
        print("CSV is empty.", file=sys.stderr)
        return 2

    icp_titles = [t.strip() for t in (args.icp_titles or "").split(",") if t.strip()]
    icp_industries = [i.strip() for i in (args.icp_industries or "").split(",")
                      if i.strip()]
    titles, industries = load_icp(args.client, args.icp_niche,
                                  icp_titles, icp_industries)

    dimensions = []
    for name, fn, weight in [
        ("Email verification", lambda: score_verification(rows), 2),
        ("Duplicate emails",   lambda: score_dup_emails(rows), 1),
        ("Domain concentration", lambda: score_dup_domains(rows), 1),
        ("Title relevance",    lambda: score_title_relevance(rows, titles), 1),
        ("Bad-title detection", lambda: score_bad_titles(rows), 1),
        ("Catch-all density",  lambda: score_catch_all(rows), 1),
        ("ICP fit",            lambda: score_icp_fit(rows, industries), 2),
        ("Name quality",       lambda: score_name_quality(rows), 1),
    ]:
        score, note = fn()
        dimensions.append({"name": name, "score": score, "note": note,
                           "weight": weight})

    # Weighted average across non-None dimensions
    weighted_sum = sum(d["score"] * d["weight"] for d in dimensions
                       if d["score"] is not None)
    total_weight = sum(d["weight"] for d in dimensions if d["score"] is not None)
    weighted_score = weighted_sum / total_weight if total_weight else 0
    letter, action = grade_letter(weighted_score)

    scorecard = {
        "csv": str(csv_path),
        "n_rows": len(rows),
        "generated_at": now_iso(),
        "weighted_score": round(weighted_score, 1),
        "letter": letter,
        "action": action,
        "dimensions": dimensions,
        "issues": collect_issues(dimensions),
        "checklist": generate_checklist(dimensions, len(rows)),
        "icp_titles_used": titles,
        "icp_industries_used": industries,
    }

    # Output
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    out_md_path = (Path(args.out) if args.out
                   else OUT_DIR / f"{csv_path.stem}-{slug_now()}.md")
    out_md_path.write_text(render_markdown(scorecard, csv_path))

    if args.json_out:
        Path(args.json_out).write_text(json.dumps(scorecard, indent=2))

    print(render_markdown(scorecard, csv_path))
    print(f"\nScorecard saved to: {out_md_path}", file=sys.stderr)

    # Exit code by grade gap
    min_rank = GRADE_RANK[args.min_grade]
    got_rank = GRADE_RANK[letter]
    if got_rank >= min_rank:
        return 0
    if got_rank == min_rank - 1:
        return 1
    return 2


if __name__ == "__main__":
    sys.exit(main())
