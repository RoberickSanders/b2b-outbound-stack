#!/usr/bin/env python3
"""
enrich_smart_route.py — Smart enrichment routing based on domain characteristics.

Instead of trying every enrichment method sequentially on every company,
this pre-sorts domains into buckets and routes each to the method most
likely to work. Saves credits and increases hit rate.

Flow:
  1. Batch MX check ALL domains (FREE, instant)
  2. Route each domain to the right enrichment path:
     - No MX → skip (no email server)
     - Catch-all → use firstname@ (guaranteed delivery)
     - Google Workspace → firstname.lastname@ pattern (highest hit for GWS)
     - Microsoft 365 → firstname@ or firstlast@ pattern
     - Custom/unknown → full cascade (owner search → Icypeas)
  3. MV verify only the patterns predicted to work
  4. Icypeas ONLY on custom domains where patterns fail

Usage:
    python3 tools/enrich_smart_route.py --input companies.csv --niche "cost segregation" --client client_c
    python3 tools/enrich_smart_route.py --input companies.csv --dry-run  # show routing plan without spending

Designed to be called automatically by the cascade when Blitz enrichment
comes up short. Can also run standalone on any company list.
"""

import os
import re
import sys
import csv
import json
import time
import socket
import sqlite3
import argparse
import requests
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.dirname(SCRIPT_DIR)
DB_PATH = os.path.join(ROOT_DIR, "master-leads", "master_leads.db")

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


# ============================================================================
# KNOWN NICHE DIRECTORIES (Firecrawl + Playwright hit these directly)
# ============================================================================

NICHE_DIRECTORIES = {
    "cost segregation": [
        {"url": "https://www.ascsp.org/find-a-cost-segregation-professional", "type": "interactive", "name": "ASCSP"},
        {"url": "https://www.kbkg.com/cost-segregation/the-top-cost-segregation-providers-in-2025-ranked-compared", "type": "static", "name": "KBKG Rankings"},
    ],
    "fire protection": [
        {"url": "https://fireinspectiondirectory.com/states/{state}", "type": "static", "name": "Fire Inspection Directory", "per_state": True},
    ],
    "fire alarm": [
        {"url": "https://fireinspectiondirectory.com/states/{state}", "type": "static", "name": "Fire Inspection Directory", "per_state": True},
    ],
    "msps": [
        {"url": "https://www.cloudtango.net/topMSPs/USA/", "type": "static", "name": "Cloudtango"},
        {"url": "https://mspdatabase.com/usa/{state}", "type": "static", "name": "MSP Database", "per_state": True},
        {"url": "https://mspcompanies.us/", "type": "static", "name": "MSP Companies"},
    ],
    "managed service provider": [
        {"url": "https://www.cloudtango.net/topMSPs/USA/", "type": "static", "name": "Cloudtango"},
        {"url": "https://mspdatabase.com/usa/{state}", "type": "static", "name": "MSP Database", "per_state": True},
    ],
    "elevator inspection": [
        {"url": "https://www.naec.org/find-a-company", "type": "interactive", "name": "NAEC"},
    ],
    "osha compliance": [
        {"url": "https://www.assp.org/membership/find-a-chapter", "type": "interactive", "name": "ASSP"},
    ],
    "property tax appeal": [
        {"url": "https://www.iaao.org/wcm/Membership/Find_a_Member/wcm/Membership_Content/Find_a_Member.aspx", "type": "interactive", "name": "IAAO"},
    ],
    "freight audit": [
        {"url": "https://www.parcelindustry.com/directory", "type": "static", "name": "Parcel Industry Directory"},
    ],
    "sales tax recovery": [
        {"url": "https://www.cost.org/membership/member-directory/", "type": "interactive", "name": "COST Directory"},
    ],
}


# ============================================================================
# MX RECORD CHECKING
# ============================================================================

def batch_mx_check(domains, workers=20):
    """Batch check MX records for a list of domains. Returns dict of domain → mx_type.
    mx_type: 'google' | 'microsoft' | 'custom' | 'none'
    """
    import dns.resolver
    results = {}

    def check_one(domain):
        try:
            answers = dns.resolver.resolve(domain, 'MX')
            mx_hosts = [str(r.exchange).lower() for r in answers]
            mx_str = ' '.join(mx_hosts)
            if 'google' in mx_str or 'gmail' in mx_str or 'googlemail' in mx_str:
                return domain, 'google'
            elif 'outlook' in mx_str or 'microsoft' in mx_str or 'office365' in mx_str:
                return domain, 'microsoft'
            else:
                return domain, 'custom'
        except Exception:
            return domain, 'none'

    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = [ex.submit(check_one, d) for d in domains]
        for f in as_completed(futs):
            domain, mx_type = f.result()
            results[domain] = mx_type

    return results


def batch_mx_check_simple(domains):
    """Simpler MX check without dnspython — uses socket."""
    import subprocess
    results = {}

    for domain in domains:
        try:
            # Use dig or nslookup via subprocess
            r = subprocess.run(['dig', '+short', 'MX', domain],
                             capture_output=True, text=True, timeout=5)
            mx_text = r.stdout.lower()
            if not mx_text.strip():
                results[domain] = 'none'
            elif 'google' in mx_text or 'gmail' in mx_text:
                results[domain] = 'google'
            elif 'outlook' in mx_text or 'microsoft' in mx_text:
                results[domain] = 'microsoft'
            else:
                results[domain] = 'custom'
        except Exception:
            results[domain] = 'unknown'

    return results


# ============================================================================
# CATCH-ALL CHECK
# ============================================================================

def is_catch_all_cached(domain):
    """Check if domain is catch-all from master DB cache."""
    try:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        # Check domain_memory or leads table for catch-all flag
        cur.execute("SELECT catch_all FROM leads WHERE LOWER(domain)=? AND catch_all IS NOT NULL LIMIT 1",
                    (domain.lower(),))
        row = cur.fetchone()
        conn.close()
        if row:
            return bool(row[0])
    except Exception:
        pass
    return None


# ============================================================================
# SMART ROUTING
# ============================================================================

def route_domains(domains_with_companies, mx_results):
    """Route each domain to the optimal enrichment method.
    Returns dict of route_type → list of (domain, company_info) tuples.
    """
    routes = {
        'skip': [],        # No MX — don't waste any credits
        'catch_all': [],   # Catch-all — use firstname@, guaranteed delivery
        'google': [],      # Google Workspace — predictable patterns
        'microsoft': [],   # Microsoft 365 — predictable patterns
        'custom': [],      # Custom mail server — needs full cascade
        'unknown': [],     # Couldn't determine — treat as custom
    }

    for domain, company_info in domains_with_companies:
        mx_type = mx_results.get(domain, 'unknown')

        if mx_type == 'none':
            routes['skip'].append((domain, company_info))
        else:
            # Check catch-all
            ca = is_catch_all_cached(domain)
            if ca:
                routes['catch_all'].append((domain, company_info))
            elif mx_type == 'google':
                routes['google'].append((domain, company_info))
            elif mx_type == 'microsoft':
                routes['microsoft'].append((domain, company_info))
            elif mx_type == 'custom':
                routes['custom'].append((domain, company_info))
            else:
                routes['unknown'].append((domain, company_info))

    return routes


def get_niche_directories(niche):
    """Get known directories for a niche."""
    niche_lower = niche.lower()
    for key, dirs in NICHE_DIRECTORIES.items():
        if key in niche_lower or niche_lower in key:
            return dirs
    return []


def main():
    ap = argparse.ArgumentParser(description="Smart enrichment routing based on domain type")
    ap.add_argument("--input", required=True, help="CSV with company,domain columns")
    ap.add_argument("--niche", default="")
    ap.add_argument("--client", default="client_c")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--limit", type=int, default=0)
    args = ap.parse_args()

    # Load companies
    companies = []
    with open(args.input) as f:
        for r in csv.DictReader(f):
            domain = (r.get('domain', '') or '').strip().lower()
            if domain:
                companies.append((domain, r))
    if args.limit:
        companies = companies[:args.limit]

    print(f"{'='*60}")
    print(f"  SMART ENRICHMENT ROUTING")
    print(f"{'='*60}")
    print(f"  companies: {len(companies)}")
    print(f"  niche: {args.niche}")
    print()

    # Step 1: Batch MX check
    print(f"  [1/3] MX check on {len(companies)} domains...", flush=True)
    domains = [d for d, _ in companies]
    mx_results = batch_mx_check_simple(domains)

    # Step 2: Route
    routes = route_domains(companies, mx_results)

    print(f"\n  [2/3] ROUTING PLAN:")
    print(f"    skip (no MX):       {len(routes['skip']):>5}  ← save ALL credits")
    print(f"    catch-all:          {len(routes['catch_all']):>5}  ← firstname@ guaranteed")
    print(f"    Google Workspace:   {len(routes['google']):>5}  ← firstname.lastname@ pattern")
    print(f"    Microsoft 365:      {len(routes['microsoft']):>5}  ← firstname@ pattern")
    print(f"    custom server:      {len(routes['custom']):>5}  ← full cascade needed")
    print(f"    unknown:            {len(routes['unknown']):>5}  ← treat as custom")

    # Cost estimate
    predictable = len(routes['catch_all']) + len(routes['google']) + len(routes['microsoft'])
    needs_cascade = len(routes['custom']) + len(routes['unknown'])
    estimated_mv = predictable * 1 + needs_cascade * 4  # 1 MV for predictable, 4 for cascade
    estimated_serper = needs_cascade * 2  # owner search only on cascade
    estimated_cost = estimated_mv * 0.001 + estimated_serper * 0.001

    print(f"\n  COST ESTIMATE:")
    print(f"    MV credits:    ~{estimated_mv} (${estimated_mv * 0.001:.2f})")
    print(f"    Serper:        ~{estimated_serper} (${estimated_serper * 0.001:.2f})")
    print(f"    Total:         ~${estimated_cost:.2f}")

    savings = len(routes['skip']) * 5 * 0.001 + predictable * 3 * 0.001
    print(f"    Savings vs blind cascade: ~${savings:.2f}")

    # Show known directories
    dirs = get_niche_directories(args.niche)
    if dirs:
        print(f"\n  KNOWN DIRECTORIES for '{args.niche}':")
        for d in dirs:
            print(f"    {d['name']}: {d['url'][:60]}")

    if args.dry_run:
        print(f"\n  [DRY RUN] Would route {len(companies)} companies as shown above.")
        return

    print(f"\n  [3/3] Routing not yet integrated into enrichment pipeline.")
    print(f"  Run enrich_owner_search.py for now — smart routing coming in next update.")


if __name__ == "__main__":
    main()
