#!/usr/bin/env python3
"""
enrich_retry.py — Re-process companies that failed during owner search enrichment.

Reads the owner search cache, finds companies where:
- Domain search returned None (timeout/error)
- Owner search returned None (no results or timeout)
- Email verification failed (all patterns bounced)

Re-tries only the failed step, using cached successes from earlier steps.
Zero wasted credits — only retries what actually failed.

Usage:
    # Show what failed
    python3 tools/enrich_retry.py --input .firecrawl/fire-protection-all-states.csv --stats

    # Retry failed domain lookups
    python3 tools/enrich_retry.py --input .firecrawl/fire-protection-all-states.csv --retry-domains

    # Retry failed owner searches (has domain but no owner)
    python3 tools/enrich_retry.py --input .firecrawl/fire-protection-all-states.csv --retry-owners

    # Retry everything that failed
    python3 tools/enrich_retry.py --input .firecrawl/fire-protection-all-states.csv --retry-all

    # Full retry with different niche
    python3 tools/enrich_retry.py --input companies.csv --niche "elevator inspection" --retry-all
"""

import os
import sys
import csv
import json
import argparse
import requests
import re
import time
import sqlite3
from datetime import datetime

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.dirname(SCRIPT_DIR)
DB_PATH = os.path.join(ROOT_DIR, "master-leads", "master_leads.db")
CACHE_FILE = os.path.join(SCRIPT_DIR, "_owner_search_cache.json")

def _load_env_file(path):
    if not os.path.isfile(path):
        return
    try:
        for line in open(path):
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            k = k.strip()
            v = v.strip().strip('"').strip("'")
            if k and not os.environ.get(k):
                os.environ[k] = v
    except Exception:
        pass

for _p in (
    "~/agency-os/.env",
    os.path.join(ROOT_DIR, ".env"),
):
    _load_env_file(_p)


def load_cache():
    if os.path.isfile(CACHE_FILE):
        try:
            return json.load(open(CACHE_FILE))
        except Exception:
            return {}
    return {}


def save_cache(cache):
    with open(CACHE_FILE, "w") as f:
        json.dump(dict(cache), f, indent=2)


def classify_failures(companies, cache, known_domains):
    """Classify each company into: success, failed_domain, failed_owner, failed_email, already_known."""
    stats = {"success": [], "failed_domain": [], "failed_owner": [], "failed_email": [],
             "already_known": [], "no_name": []}

    for co in companies:
        company = co.get("company", "").strip()
        city = co.get("city", "").strip()
        state = co.get("state", "").strip()
        if not company:
            stats["no_name"].append(co)
            continue

        # Check domain
        dk = f"domain|{company.lower()}|{city.lower()}|{state.lower()}"
        domain = cache.get(dk)

        if domain is None:
            # Never attempted or explicitly failed
            if dk in cache:
                stats["failed_domain"].append(co)
            else:
                stats["failed_domain"].append(co)  # never attempted
            continue

        if domain.lower() in known_domains:
            stats["already_known"].append(co)
            continue

        # Check owner
        ok = f"owner|{company.lower()}|{domain}"
        owner = cache.get(ok)

        if owner is None:
            if ok in cache:
                stats["failed_owner"].append(co)
            else:
                stats["failed_owner"].append(co)
            continue

        # Has domain + owner — check if we got an email into the DB
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        cur.execute("SELECT id FROM leads WHERE LOWER(domain)=? AND client='client_c' AND status='new'",
                    (domain.lower(),))
        if cur.fetchone():
            stats["success"].append(co)
        else:
            stats["failed_email"].append(co)
        conn.close()

    return stats


def main():
    ap = argparse.ArgumentParser(description="Retry failed enrichments from owner search")
    ap.add_argument("--input", required=True, help="original CSV of companies")
    ap.add_argument("--niche", default="fire protection")
    ap.add_argument("--client", default="client_c")
    ap.add_argument("--stats", action="store_true", help="just show failure stats, no retries")
    ap.add_argument("--retry-domains", action="store_true", help="retry failed domain lookups")
    ap.add_argument("--retry-owners", action="store_true", help="retry failed owner searches")
    ap.add_argument("--retry-all", action="store_true", help="retry all failures")
    ap.add_argument("--limit", type=int, default=0)
    args = ap.parse_args()

    companies = []
    with open(args.input) as f:
        for r in csv.DictReader(f):
            companies.append(r)

    cache = load_cache()
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT DISTINCT LOWER(domain) FROM leads WHERE domain IS NOT NULL AND domain!=''")
    known_domains = {r[0] for r in cur.fetchall()}
    conn.close()

    stats = classify_failures(companies, cache, known_domains)

    print(f"{'='*50}")
    print(f"  ENRICHMENT RETRY STATUS")
    print(f"{'='*50}")
    print(f"  total companies:   {len(companies)}")
    print(f"  success:           {len(stats['success'])}")
    print(f"  already known:     {len(stats['already_known'])}")
    print(f"  failed domain:     {len(stats['failed_domain'])} (retryable)")
    print(f"  failed owner:      {len(stats['failed_owner'])} (retryable)")
    print(f"  failed email:      {len(stats['failed_email'])} (retryable)")
    print(f"  no company name:   {len(stats['no_name'])}")
    retryable = len(stats["failed_domain"]) + len(stats["failed_owner"]) + len(stats["failed_email"])
    print(f"  TOTAL RETRYABLE:   {retryable}")
    print(f"{'='*50}")

    if args.stats:
        return

    if not (args.retry_domains or args.retry_owners or args.retry_all):
        print("\nUse --retry-domains, --retry-owners, or --retry-all to retry.")
        return

    # Import the enrichment functions
    sys.path.insert(0, SCRIPT_DIR)
    from enrich_owner_search import find_domain, find_owner, generate_email_patterns, verify_email_mv

    SERPER_KEY = os.environ.get("SERPER_API_KEY", "")
    MV_KEY = os.environ.get("MILLIONVERIFIER_API_KEY", "")

    # Route light retry-classification through llm_router (Kimi K2.6 ~8x cheaper).
    # Falls back to Anthropic Haiku if Kimi key not set.
    _pipe_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    if _pipe_dir not in sys.path:
        sys.path.insert(0, _pipe_dir)
    try:
        from llm_router import get_light_client
        haiku, _haiku_model_name = get_light_client()
    except Exception:
        import anthropic
        haiku = anthropic.Anthropic(api_key=os.environ.get("ANTHROPIC_API_KEY", ""))

    retried = 0
    new_leads = 0

    # Retry domain failures
    if args.retry_domains or args.retry_all:
        targets = stats["failed_domain"]
        if args.limit:
            targets = targets[:args.limit]
        print(f"\nRetrying {len(targets)} domain lookups...")

        for i, co in enumerate(targets):
            company = co["company"].strip()
            city = co.get("city", "").strip()
            state = co.get("state", "").strip()

            # Clear cache entry so it retries
            dk = f"domain|{company.lower()}|{city.lower()}|{state.lower()}"
            cache.pop(dk, None)

            domain = find_domain(company, city, state, SERPER_KEY, cache)
            retried += 1

            if domain and domain.lower() not in known_domains:
                # Also try owner search
                owner = find_owner(company, city, state, domain, SERPER_KEY, haiku, cache)
                if owner:
                    fn = owner.get("first_name", "")
                    ln = owner.get("last_name", "")
                    patterns = generate_email_patterns(fn, ln, domain)
                    for email in patterns:
                        if verify_email_mv(email, MV_KEY):
                            # Add to DB
                            conn2 = sqlite3.connect(DB_PATH)
                            cur2 = conn2.cursor()
                            if not cur2.execute("SELECT id FROM leads WHERE LOWER(email)=?", (email.lower(),)).fetchone():
                                title = owner.get("title", "") or ""
                                if title == "null":
                                    title = ""
                                cur2.execute("""INSERT INTO leads (email,first_name,last_name,company,phone,title,domain,
                                               city,state,source,niche,client,verified,mv_result,status,date_added,date_updated)
                                               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,1,'ok','new',datetime('now'),datetime('now'))""",
                                            (email, fn, ln, company, co.get("phone", ""), title, domain,
                                             city, state, "owner_search_retry", args.niche, args.client))
                                new_leads += 1
                            conn2.commit()
                            conn2.close()
                            break
                        time.sleep(0.3)

            if (i + 1) % 20 == 0:
                save_cache(cache)
                print(f"  [{i+1}/{len(targets)}] retried={retried} new_leads={new_leads}", flush=True)
            time.sleep(0.1)

    # Retry owner failures (has domain, no owner)
    if args.retry_owners or args.retry_all:
        targets = stats["failed_owner"]
        if args.limit:
            targets = targets[:args.limit]
        print(f"\nRetrying {len(targets)} owner searches...")

        for i, co in enumerate(targets):
            company = co["company"].strip()
            city = co.get("city", "").strip()
            state = co.get("state", "").strip()

            dk = f"domain|{company.lower()}|{city.lower()}|{state.lower()}"
            domain = cache.get(dk)
            if not domain:
                continue

            ok = f"owner|{company.lower()}|{domain}"
            cache.pop(ok, None)

            owner = find_owner(company, city, state, domain, SERPER_KEY, haiku, cache)
            retried += 1

            if owner:
                fn = owner.get("first_name", "")
                ln = owner.get("last_name", "")
                patterns = generate_email_patterns(fn, ln, domain)
                for email in patterns:
                    if verify_email_mv(email, MV_KEY):
                        conn2 = sqlite3.connect(DB_PATH)
                        cur2 = conn2.cursor()
                        if not cur2.execute("SELECT id FROM leads WHERE LOWER(email)=?", (email.lower(),)).fetchone():
                            title = owner.get("title", "") or ""
                            if title == "null":
                                title = ""
                            cur2.execute("""INSERT INTO leads (email,first_name,last_name,company,phone,title,domain,
                                           city,state,source,niche,client,verified,mv_result,status,date_added,date_updated)
                                           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,1,'ok','new',datetime('now'),datetime('now'))""",
                                        (email, fn, ln, company, co.get("phone", ""), title, domain,
                                         city, state, "owner_search_retry", args.niche, args.client))
                            new_leads += 1
                        conn2.commit()
                        conn2.close()
                        break
                    time.sleep(0.3)

            if (i + 1) % 20 == 0:
                save_cache(cache)
                print(f"  [{i+1}/{len(targets)}] retried={retried} new_leads={new_leads}", flush=True)

    save_cache(cache)

    print(f"\n{'='*50}")
    print(f"  RETRY RESULTS")
    print(f"{'='*50}")
    print(f"  companies retried:  {retried}")
    print(f"  new leads found:    {new_leads}")
    print(f"{'='*50}")


if __name__ == "__main__":
    main()
