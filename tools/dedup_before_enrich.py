#!/usr/bin/env python3
"""
dedup_before_enrich.py — Check discovered companies against master DB
BEFORE spending credits on enrichment.

Use as a filter between discovery (AI Ark / Firecrawl) and enrichment (Blitz / MV).
Returns only genuinely new domains worth spending credits on.

Usage (as module):
    from tools.dedup_before_enrich import filter_new_domains
    new_only = filter_new_domains(discovered_domains)
    # now enrich only new_only → saves credits

Usage (CLI):
    python3 tools/dedup_before_enrich.py domains.txt
    python3 tools/dedup_before_enrich.py --from-csv companies.csv --domain-col website
"""

import os
import sys
import csv
import sqlite3
import argparse

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.dirname(SCRIPT_DIR)
DB_PATH = os.path.join(ROOT_DIR, "master-leads", "master_leads.db")


def get_known_domains():
    """Return set of domains that already have a verified email in master DB.
    Domains with only unverified/unenriched leads are NOT considered known,
    so forge_enrich will process them."""
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("""SELECT DISTINCT LOWER(domain) FROM leads
                   WHERE domain IS NOT NULL AND domain!=''
                   AND email IS NOT NULL AND email!=''
                   AND mv_result IS NOT NULL AND mv_result!=''""")
    known = {r[0] for r in cur.fetchall()}
    conn.close()
    return known


def get_known_emails():
    """Return set of all emails already in master DB (any status)."""
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT DISTINCT LOWER(email) FROM leads WHERE email IS NOT NULL AND email!=''")
    known = {r[0] for r in cur.fetchall()}
    conn.close()
    return known


def get_known_companies():
    """Return set of normalized company names already in master DB."""
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT DISTINCT company FROM leads WHERE company IS NOT NULL AND company!=''")
    known = set()
    for (name,) in cur.fetchall():
        # Normalize: lowercase, strip LLC/Inc/Corp/etc, strip punctuation
        import re
        normalized = re.sub(r'\b(llc|inc|corp|ltd|co|company|group|services|solutions)\b', '',
                           name.lower()).strip()
        normalized = re.sub(r'[^a-z0-9\s]', '', normalized).strip()
        normalized = re.sub(r'\s+', ' ', normalized)
        if normalized:
            known.add(normalized)
    conn.close()
    return known


def normalize_company(name):
    """Normalize a company name for fuzzy matching."""
    import re
    n = re.sub(r'\b(llc|inc|corp|ltd|co|company|group|services|solutions)\b', '',
               (name or '').lower()).strip()
    n = re.sub(r'[^a-z0-9\s]', '', n).strip()
    n = re.sub(r'\s+', ' ', n)
    return n


def filter_new_domains(domains):
    """Filter a list of domains to only those NOT in master DB.
    Also filters by company name to catch same-company-different-domain duplicates.

    Args:
        domains: list of domain strings, or list of dicts with 'domain' key
    Returns:
        list of same type, filtered to new-only
    """
    known_dom = get_known_domains()
    known_comp = get_known_companies()
    if not domains:
        return []
    if isinstance(domains[0], dict):
        result = []
        for d in domains:
            domain = d.get("domain", "").lower().strip()
            company = normalize_company(d.get("company", "") or d.get("name", ""))
            if domain in known_dom:
                continue
            if company and len(company) > 5 and company in known_comp:
                continue  # same company name, different domain — still a dupe
            result.append(d)
        return result
    return [d for d in domains if d.lower().strip() not in known_dom]


def filter_new_emails(emails):
    """Filter emails to only those NOT in master DB."""
    known = get_known_emails()
    if not emails:
        return []
    if isinstance(emails[0], dict):
        return [e for e in emails if e.get("email", "").lower().strip() not in known]
    return [e for e in emails if e.lower().strip() not in known]


def main():
    ap = argparse.ArgumentParser(description="Filter domains/emails against master DB")
    ap.add_argument("file", nargs="?", help="file with domains (one per line) or CSV")
    ap.add_argument("--from-csv", help="CSV file path")
    ap.add_argument("--domain-col", default="domain", help="column name for domain")
    ap.add_argument("--output", help="output file for new-only domains")
    args = ap.parse_args()

    known = get_known_domains()
    print(f"master DB has {len(known)} known domains")

    domains = []
    if args.from_csv:
        with open(args.from_csv) as f:
            for r in csv.DictReader(f):
                d = r.get(args.domain_col, "").strip()
                if d:
                    domains.append(d)
    elif args.file:
        with open(args.file) as f:
            domains = [line.strip() for line in f if line.strip()]

    if not domains:
        print("no domains to check")
        return

    new = [d for d in domains if d.lower() not in known]
    existing = len(domains) - len(new)

    print(f"input:    {len(domains)}")
    print(f"existing: {existing} (skip — already in DB)")
    print(f"NEW:      {len(new)} (worth enriching)")

    if args.output and new:
        with open(args.output, "w") as f:
            for d in new:
                f.write(d + "\n")
        print(f"saved to: {args.output}")
    elif new:
        for d in new[:20]:
            print(f"  {d}")
        if len(new) > 20:
            print(f"  ... and {len(new)-20} more")


if __name__ == "__main__":
    main()
