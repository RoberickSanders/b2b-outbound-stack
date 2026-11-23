#!/usr/bin/env python3.13
"""
competitor_engagers.py — Find people engaging with competitor LinkedIn posts.

Mirrors Eric Nowoslawski's `competitor-engagers` skill but built for the Forge
stack so output flows straight into our enrichment + verification pipeline.

What it does:
  1. Take a competitor list (--competitors urls.txt) OR auto-discover from a
     niche (--niche fire-protection --client client_a) using web search.
  2. For each competitor LinkedIn company URL:
       a. Fetch the company's recent posts (90-day lookback)
       b. Fetch top employees and their recent posts
       c. Pull commenters + reactors on every post
  3. Dedupe, drop people employed at any competitor.
  4. Output a CSV in our standard schema (first_name, last_name, company,
     title, linkedin_url, source, niche, client, signal_type='engagement',
     signal_payload).
  5. Suggest follow-on commands (Forge enrichment + ICP verify + niche fit).

Required env (in Workspace .env):
  RAPIDAPI_KEY       — for realtime-linkedin-bulk-data on RapidAPI
  RAPIDAPI_HOST      — defaults to 'linkedin-bulk-data-scraper.p.rapidapi.com'

If RAPIDAPI_KEY is missing, the script prints the signup URL and exits 0
without spending budget.

Usage:
  # Explicit competitor URLs
  python3 tools/competitor_engagers.py \\
      --client client_b --niche msps \\
      --competitors competitor_urls.txt --posts 30

  # Auto-discover competitors via web search
  python3 tools/competitor_engagers.py \\
      --client client_a --niche fire-protection \\
      --discover-competitors 10 --geo "Denver metro"

  # Dry run — list competitors only
  python3 tools/competitor_engagers.py \\
      --client client_a --niche fire-protection \\
      --discover-competitors 5 --dry-run

Output:
  02-Areas/lead-pipeline/competitor-engagers/
    {client}-{niche}-{YYYYMMDD-HHMM}/
      competitors.json    # the list we crawled
      engagers.csv        # deduped engager list (Forge-ready columns)
      checkpoint.json     # incremental save (resumable with --resume)
      meta.json           # run metadata, costs, timings
"""

import argparse
import csv
import json
import os
import re
import sys
import time
from collections import OrderedDict
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests

SCRIPT_DIR = Path(__file__).resolve().parent
LEAD_PIPELINE_DIR = SCRIPT_DIR.parent
WORKSPACE_ROOT = LEAD_PIPELINE_DIR.parent.parent

try:
    from dotenv import load_dotenv
    load_dotenv(WORKSPACE_ROOT / ".env", override=True)
    load_dotenv(LEAD_PIPELINE_DIR / ".env", override=True)
except ImportError:
    pass

RAPIDAPI_KEY = os.environ.get("RAPIDAPI_KEY", "")
RAPIDAPI_HOST = os.environ.get(
    "RAPIDAPI_HOST",
    "linkedin-bulk-data-scraper.p.rapidapi.com",
)

OUTPUT_ROOT = LEAD_PIPELINE_DIR / "competitor-engagers"

# ============================================================
# Helpers
# ============================================================

def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def slug_now() -> str:
    return datetime.now().strftime("%Y%m%d-%H%M")


def linkedin_company_slug(url: str) -> str | None:
    """Extract '/company/<slug>' part of a LinkedIn URL."""
    m = re.search(r"linkedin\.com/(company|school)/([^/?#]+)", url, re.I)
    return m.group(2) if m else None


def linkedin_person_slug(url: str) -> str | None:
    m = re.search(r"linkedin\.com/in/([^/?#]+)", url, re.I)
    return m.group(1) if m else None


def write_checkpoint(run_dir: Path, state: dict):
    (run_dir / "checkpoint.json").write_text(json.dumps(state, indent=2, default=str))


# ============================================================
# RapidAPI calls — wrapped so a missing endpoint is a soft failure
# ============================================================

def rapid_post(endpoint: str, payload: dict, timeout: int = 30) -> dict:
    """POST to the RapidAPI LinkedIn endpoint. Returns {} on failure."""
    if not RAPIDAPI_KEY:
        return {"_error": "RAPIDAPI_KEY missing"}
    url = f"https://{RAPIDAPI_HOST}{endpoint}"
    headers = {
        "x-rapidapi-host": RAPIDAPI_HOST,
        "x-rapidapi-key": RAPIDAPI_KEY,
        "content-type": "application/json",
    }
    try:
        r = requests.post(url, headers=headers, json=payload, timeout=timeout)
        if r.status_code == 200:
            return r.json()
        return {"_error": f"HTTP {r.status_code}", "_body": r.text[:300]}
    except requests.RequestException as e:
        return {"_error": f"request failed: {e}"}


def fetch_company_posts(company_slug: str, limit: int = 30) -> list[dict]:
    """Pull recent posts from a company page."""
    res = rapid_post("/company/posts", {"username": company_slug, "limit": limit})
    if "_error" in res:
        return []
    return res.get("posts") or res.get("data") or []


def fetch_company_employees(company_slug: str, limit: int = 200) -> list[dict]:
    res = rapid_post("/company/employees",
                     {"username": company_slug, "limit": limit})
    if "_error" in res:
        return []
    return res.get("employees") or res.get("data") or []


def fetch_post_engagers(post_url: str) -> dict:
    """Return {'reactors': [...], 'commenters': [...]} for a post URL."""
    out = {"reactors": [], "commenters": []}
    react = rapid_post("/post/reactions", {"post_url": post_url, "limit": 200})
    if "_error" not in react:
        out["reactors"] = react.get("reactions") or react.get("data") or []
    comm = rapid_post("/post/comments", {"post_url": post_url, "limit": 200})
    if "_error" not in comm:
        out["commenters"] = comm.get("comments") or comm.get("data") or []
    return out


# ============================================================
# Auto-discover competitors via web search
# ============================================================

def discover_competitors(client: str, niche: str, count: int,
                         geo: str | None = None) -> list[dict]:
    """Use web search to find competitor LinkedIn URLs.

    Returns [{'name', 'linkedin_url'}]. We deliberately keep this simple —
    the user can review/edit the produced competitors.json before scraping.
    """
    # Use the lead.py / forge.py heritage of "use Serper for SERP". If
    # SERPER_API_KEY exists we hit it; otherwise we return an empty list so the
    # user provides --competitors manually.
    serper_key = os.environ.get("SERPER_API_KEY", "")
    if not serper_key:
        return []
    geo_bit = f" {geo}" if geo else ""
    q = f"{niche} companies{geo_bit} site:linkedin.com/company"
    try:
        r = requests.post(
            "https://google.serper.dev/search",
            headers={"X-API-KEY": serper_key, "Content-Type": "application/json"},
            json={"q": q, "num": count * 2},
            timeout=20,
        )
        if r.status_code != 200:
            return []
        results = r.json().get("organic", [])
    except requests.RequestException:
        return []

    out = []
    seen = set()
    for hit in results:
        url = hit.get("link", "")
        slug = linkedin_company_slug(url)
        if not slug or slug in seen:
            continue
        out.append({
            "name": hit.get("title", "").split(" | ")[0],
            "linkedin_url": f"https://www.linkedin.com/company/{slug}/",
            "slug": slug,
        })
        seen.add(slug)
        if len(out) >= count:
            break
    return out


# ============================================================
# Engager harvest
# ============================================================

def harvest(competitors: list[dict], posts_per_company: int,
            run_dir: Path, resume_state: dict | None = None) -> list[dict]:
    """Iterate competitors → posts → engagers. Resumable."""
    competitor_slugs = {c["slug"] for c in competitors}
    state = resume_state or {"completed_companies": [], "engagers": {}}
    engagers = state["engagers"]  # dict[person_url, person_dict]

    for ci, comp in enumerate(competitors):
        if comp["slug"] in state["completed_companies"]:
            print(f"[{ci+1}/{len(competitors)}] skip (resume) {comp['slug']}")
            continue
        print(f"[{ci+1}/{len(competitors)}] {comp['slug']}")
        posts = fetch_company_posts(comp["slug"], posts_per_company)
        # Also pull employees + their posts for richer engagement signal
        employees = fetch_company_employees(comp["slug"], 50)
        for emp in employees[:25]:
            emp_slug = (emp.get("username")
                        or linkedin_person_slug(emp.get("linkedin_url", "")))
            if not emp_slug:
                continue
            emp_posts = rapid_post("/person/posts",
                                   {"username": emp_slug, "limit": 10})
            if "_error" not in emp_posts:
                posts += emp_posts.get("posts", [])

        for p in posts:
            post_url = p.get("post_url") or p.get("url")
            if not post_url:
                continue
            eng = fetch_post_engagers(post_url)
            for who in eng["reactors"] + eng["commenters"]:
                pname_url = who.get("linkedin_url") or who.get("profile_url")
                if not pname_url:
                    continue
                # Drop people employed at any competitor we're crawling
                p_company_slug = linkedin_company_slug(
                    who.get("company_linkedin_url") or "") or ""
                if p_company_slug in competitor_slugs:
                    continue
                if pname_url not in engagers:
                    engagers[pname_url] = {
                        "first_name": who.get("first_name", ""),
                        "last_name": who.get("last_name", ""),
                        "title": who.get("title", "") or who.get("headline", ""),
                        "company": who.get("company", ""),
                        "linkedin_url": pname_url,
                        "engagement_count": 0,
                        "engagement_targets": [],
                    }
                engagers[pname_url]["engagement_count"] += 1
                engagers[pname_url]["engagement_targets"].append({
                    "competitor": comp["slug"],
                    "post_url": post_url,
                    "post_summary": (p.get("text") or "")[:120],
                })
            # Be polite to RapidAPI
            time.sleep(0.5)

        state["completed_companies"].append(comp["slug"])
        write_checkpoint(run_dir, state)

    return list(engagers.values())


# ============================================================
# CSV writer (Forge-ready schema)
# ============================================================

def write_csv(engagers: list[dict], out_csv: Path,
              client: str, niche: str):
    cols = [
        "first_name", "last_name", "title", "company",
        "linkedin_url", "source", "client", "niche",
        "signal_type", "signal_payload",
    ]
    with out_csv.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for e in sorted(engagers,
                        key=lambda x: x.get("engagement_count", 0),
                        reverse=True):
            payload = json.dumps({
                "engagement_count": e.get("engagement_count", 0),
                "targets": e.get("engagement_targets", [])[:5],  # cap noise
            })
            w.writerow({
                "first_name": e.get("first_name", ""),
                "last_name": e.get("last_name", ""),
                "title": e.get("title", ""),
                "company": e.get("company", ""),
                "linkedin_url": e.get("linkedin_url", ""),
                "source": "competitor_engagers",
                "client": client,
                "niche": niche,
                "signal_type": "competitor_engagement",
                "signal_payload": payload,
            })


# ============================================================
# Main
# ============================================================

def main():
    p = argparse.ArgumentParser(
        prog="competitor_engagers",
        description="Harvest LinkedIn engagers from competitor company posts.")
    p.add_argument("--client", required=True,
                   help="client_a | client_b | client_c")
    p.add_argument("--niche", required=True)
    p.add_argument("--competitors",
                   help="Path to a file with one LinkedIn company URL per line")
    p.add_argument("--discover-competitors", type=int, default=0,
                   help="Auto-discover N competitors via web search (Serper)")
    p.add_argument("--geo", help="Geographic constraint for auto-discover")
    p.add_argument("--posts", type=int, default=30,
                   help="Posts per company to scrape (default 30)")
    p.add_argument("--resume", help="Resume run by ID")
    p.add_argument("--dry-run", action="store_true",
                   help="Print competitor list and exit (no scraping)")
    p.add_argument("--check-auth", action="store_true",
                   help="Test RAPIDAPI_KEY and exit")

    args = p.parse_args()

    if args.check_auth:
        if not RAPIDAPI_KEY:
            print("RAPIDAPI_KEY not set in environment.")
            print("Sign up + subscribe: "
                  "https://rapidapi.com/apibuilderz/api/realtime-linkedin-bulk-data")
            return 1
        # Tiny test call
        ok = rapid_post("/health", {})
        print("RAPIDAPI_KEY present.", "OK" if "_error" not in ok else ok)
        return 0

    if not RAPIDAPI_KEY and not args.dry_run:
        print("RAPIDAPI_KEY not set.\n"
              "Add it to Workspace .env. Sign up: "
              "https://rapidapi.com/apibuilderz/api/realtime-linkedin-bulk-data",
              file=sys.stderr)
        return 1

    # ─── Resolve competitors list ────────────────────────────────────────
    competitors: list[dict] = []
    if args.competitors:
        with open(args.competitors) as f:
            for line in f:
                url = line.strip()
                if not url or url.startswith("#"):
                    continue
                slug = linkedin_company_slug(url)
                if slug:
                    competitors.append({
                        "name": slug.replace("-", " ").title(),
                        "linkedin_url": url,
                        "slug": slug,
                    })
    if args.discover_competitors:
        discovered = discover_competitors(
            args.client, args.niche, args.discover_competitors, args.geo)
        # Dedupe with manually provided
        seen = {c["slug"] for c in competitors}
        for d in discovered:
            if d["slug"] not in seen:
                competitors.append(d)
                seen.add(d["slug"])

    if not competitors:
        print("No competitors resolved. Pass --competitors urls.txt OR "
              "--discover-competitors N (requires SERPER_API_KEY).",
              file=sys.stderr)
        return 2

    print(f"Resolved {len(competitors)} competitor(s):")
    for c in competitors:
        print(f"  - {c['slug']:<40s} {c['linkedin_url']}")

    # ─── Set up run dir ──────────────────────────────────────────────────
    if args.resume:
        run_dir = OUTPUT_ROOT / args.resume
        if not run_dir.is_dir():
            print(f"resume run not found: {run_dir}", file=sys.stderr)
            return 2
        meta = json.loads((run_dir / "meta.json").read_text())
        chk = json.loads((run_dir / "checkpoint.json").read_text())
    else:
        run_id = f"{args.client}-{args.niche.replace(' ', '_')}-{slug_now()}"
        run_dir = OUTPUT_ROOT / run_id
        run_dir.mkdir(parents=True, exist_ok=True)
        meta = {
            "run_id": run_id,
            "client": args.client,
            "niche": args.niche,
            "started_at": now_iso(),
            "competitor_count": len(competitors),
            "posts_per_company": args.posts,
        }
        (run_dir / "meta.json").write_text(json.dumps(meta, indent=2))
        (run_dir / "competitors.json").write_text(json.dumps(competitors, indent=2))
        chk = None

    if args.dry_run:
        print("\nDry run — competitors written to:", run_dir / "competitors.json")
        return 0

    # ─── Harvest ─────────────────────────────────────────────────────────
    engagers = harvest(competitors, args.posts, run_dir, chk)

    out_csv = run_dir / "engagers.csv"
    write_csv(engagers, out_csv, args.client, args.niche)

    meta["finished_at"] = now_iso()
    meta["engager_count"] = len(engagers)
    (run_dir / "meta.json").write_text(json.dumps(meta, indent=2))

    print()
    print(f"✓ {len(engagers)} unique engagers written to {out_csv}")
    print()
    print("Next steps:")
    print(f"  1. Sample 50 rows + run /icp-prompt-builder before enriching everything.")
    print(f"  2. f enrich --input {out_csv} --niche '{args.niche}' --client {args.client}")
    print(f"  3. f verify-niche --input <enriched.csv>")
    print(f"  4. f score-list --csv <verified.csv>  # quality grade before send")
    return 0


if __name__ == "__main__":
    sys.exit(main())
