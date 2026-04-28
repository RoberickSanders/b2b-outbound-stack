#!/usr/bin/env python3
"""
signal_job_postings.py — Find companies hiring for roles that signal they need
your services. A company posting for a "Facilities Manager" or "Safety Director"
is 10x more likely to need fire protection services.

Uses Serper web search (you already pay for this).

Usage:
    # CLIENT_A: find companies hiring facilities/safety roles in Colorado
    python3 tools/signal_job_postings.py --client client_a --geo colorado

    # CLIENT_C: find companies hiring roles that signal they need cost seg/utility audit
    python3 tools/signal_job_postings.py --client client_c --geo "united states"

    # CLIENT_B: find companies hiring IT/security roles (potential MSP clients)
    python3 tools/signal_job_postings.py --client client_b --geo texas

    # Custom search
    python3 tools/signal_job_postings.py --titles "facilities manager,safety director" --geo colorado
"""

import os
import re
import sys
import csv
import json
import time
import sqlite3
import argparse
import requests
from datetime import datetime

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

SERPER_KEY = os.environ.get("SERPER_API_KEY", "")

# Job titles that signal buying intent per client
SIGNAL_TITLES = {
    "client_a": [
        "facilities manager",
        "facilities director",
        "safety director",
        "safety manager",
        "building manager",
        "property manager",
        "maintenance director",
        "fire safety officer",
        "life safety",
        "building operations",
        "risk manager",
    ],
    "client_c": [
        "tax director",
        "VP of tax",
        "director of real estate",
        "real estate controller",
        "property accountant",
        "energy manager",
        "sustainability director",
        "utilities manager",
        "facilities energy",
        "telecom manager",
    ],
    "client_b": [
        "IT director",
        "IT manager",
        "CISO",
        "security engineer",
        "compliance manager",
        "information security",
        "systems administrator",
        "network administrator",
        "managed services",
        "help desk manager",
    ],
}


def search_serper(query, num=20):
    if not SERPER_KEY:
        print("ERROR: SERPER_API_KEY not set")
        return []
    try:
        r = requests.post(
            "https://google.serper.dev/search",
            headers={"X-API-KEY": SERPER_KEY, "Content-Type": "application/json"},
            json={"q": query, "num": num},
            timeout=15,
        )
        if r.status_code != 200:
            return []
        return r.json().get("organic", [])
    except Exception:
        return []


def extract_company_from_result(result):
    """Try to extract company name + domain from a job posting search result.

    Handles many job board title formats:
      Indeed:        "Job Title - CompanyName - City, ST"
      LinkedIn:      "CompanyName hiring Job Title" / "CompanyName is hiring a Job Title"
      Glassdoor:     "Job Title Jobs at CompanyName | Glassdoor"
      ZipRecruiter:  "Job Title Job in City, ST - CompanyName | ZipRecruiter"
      Generic:       "Job Title at CompanyName"
      Pipe-sep:      "Job Title | CompanyName | City, ST"
      Cannabis:      "CompanyName - Master Grower" (reversed order)
      Google Jobs:   company name in snippet ("CompanyName posted...")
    Also extracts company slugs from Indeed/Glassdoor URLs.
    """
    title = result.get("title", "")
    snippet = result.get("snippet", "")
    link = result.get("link", "")

    company = None
    domain = None

    # --- TITLE-BASED PATTERNS (order matters: most specific first) ---

    # 1. Indeed pattern (dashes): "Job Title - Company - City, ST"
    if not company:
        m = re.search(r"[-–—]\s*(.+?)\s*[-–—]\s*\w+,\s*\w{2}", title)
        if m:
            company = m.group(1).strip()

    # 2. Pipe-separated with city: "Job Title | Company Name | City, ST"
    if not company:
        m = re.search(r"\|\s*(.+?)\s*\|\s*\w+,\s*\w{2}", title)
        if m:
            company = m.group(1).strip()

    # 3. Pipe-separated (no city): "Job Title | CompanyName" or "Job Title | CompanyName | SiteName"
    if not company:
        m = re.search(r"\|\s*([^|]+?)\s*(?:\|.*)?$", title)
        if m:
            candidate = m.group(1).strip()
            # Skip if it's just a site name like "Glassdoor", "Indeed", "ZipRecruiter"
            site_names = {"glassdoor", "indeed", "ziprecruiter", "monster", "linkedin", "salary.com", "jooble"}
            if candidate.lower() not in site_names:
                company = candidate

    # 4. ZipRecruiter: "Job Title Job in City, ST - CompanyName | ZipRecruiter"
    if not company:
        m = re.search(r"(?:Job|Jobs)\s+in\s+[\w\s]+,\s*\w{2}\s*[-–—]\s*(.+?)(?:\s*\|.*)?$", title, re.I)
        if m:
            company = m.group(1).strip()

    # 5. Glassdoor: "Job Title Jobs at CompanyName | Glassdoor"
    if not company:
        m = re.search(r"Jobs?\s+at\s+(.+?)(?:\s*\|.*)?$", title, re.I)
        if m:
            company = m.group(1).strip()

    # 6. Colon prefix: "Now Hiring: Role at Company" or "Hiring: Company - Role"
    if not company:
        m = re.search(r"(?:hiring|now\s+hiring|apply)\s*:?\s*.*?\bat\s+(.+?)(?:\s*[-|]|$)", title, re.I)
        if m:
            company = m.group(1).strip()

    # 7. LinkedIn: "Company is hiring a Job Title" / "Company hiring Job Title"
    if not company:
        m = re.search(r"^(.+?)\s+(?:is\s+)?hiring\s+(?:a\s+)?", title, re.I)
        if m:
            company = m.group(1).strip()

    # 8. Generic "at" pattern: "Job Title at CompanyName"
    if not company:
        m = re.search(r"\bat\s+(.+?)(?:\s*[-–—|]|$)", title, re.I)
        if m:
            company = m.group(1).strip()

    # 9. Cannabis-specific: "Company Name - Job Title" (reversed order, no city)
    if not company:
        cannabis_kw = ["cannabis", "marijuana", "dispensary", "grow", "cultivation", "mmj", "thc",
                       "hemp", "cbd", "weed", "budtender", "trimmer"]
        if any(kw in title.lower() or kw in snippet.lower() for kw in cannabis_kw):
            m = re.search(
                r"^([A-Z][\w&.\',\s]+?)\s*[-–—]\s*"
                r"(?:Master\s+)?(?:Grower|Cultivat|Trimm|Budtend|Dispensary|Facility|Compliance|"
                r"Operations|Director|Manager|Cannabis|Marijuana|Hemp|Extract)",
                title
            )
            if m:
                company = m.group(1).strip()

    # 10. "Company Name - Job Title" (generic reversed, only if short company part)
    if not company:
        m = re.match(r"^(.+?)\s*[-–—]\s+.+", title)
        if m:
            candidate = m.group(1).strip()
            # Only accept if the pre-dash part looks like a company name (short, starts uppercase)
            if candidate and candidate[0].isupper() and len(candidate.split()) <= 5:
                company = candidate

    # --- SNIPPET-BASED FALLBACK ---

    # 11. Google Jobs / aggregator snippets: "CompanyName posted ..." or "CompanyName · City, ST"
    if not company:
        m = re.match(r"^([A-Z][\w&.\',\s]+?)\s+(?:posted|is (?:hiring|looking)|·)\s", snippet)
        if m:
            company = m.group(1).strip()

    # --- URL-BASED EXTRACTION ---

    from urllib.parse import urlparse, unquote
    parsed = urlparse(link) if link else None
    host = (parsed.hostname or "") if parsed else ""
    path = (parsed.path or "") if parsed else ""

    # 12. Indeed company pages: indeed.com/cmp/Company-Name/...
    if not company and "indeed" in host:
        m = re.search(r"/cmp/([^/]+)", path)
        if m:
            company = unquote(m.group(1)).replace("-", " ").strip()

    # 13. Glassdoor: glassdoor.com/job-listing/...-companyName-JV...
    if not company and "glassdoor" in host:
        m = re.search(r"/job-listing/.*-([a-zA-Z][\w-]+)-JV", path)
        if m:
            company = unquote(m.group(1)).replace("-", " ").strip()

    # Domain from link (skip job boards)
    if link and parsed:
        job_boards = ["indeed", "linkedin", "glassdoor", "ziprecruiter", "monster",
                      "google", "jooble", "salary.com", "careerbuilder", "simplyhired",
                      "snagajob", "greenhouse", "lever.co", "workday"]
        if not any(jb in host for jb in job_boards):
            domain = host.replace("www.", "")

    # --- CLEANUP ---
    if company:
        # Strip trailing site labels that leak through
        company = re.sub(r"\s*\|\s*(?:Glassdoor|Indeed|ZipRecruiter|LinkedIn|Monster).*$", "", company, flags=re.I)
        # Strip trailing punctuation / whitespace
        company = company.strip(" -–—|.,;:")
        # Skip if the "company" is clearly a job title fragment
        title_words = {"job", "jobs", "hiring", "position", "career", "careers", "employment",
                       "apply", "application", "search", "find", "view", "see", "all"}
        if company.lower() in title_words or len(company) < 2:
            company = None

    return company, domain


def main():
    ap = argparse.ArgumentParser(description="Find companies hiring roles that signal buying intent")
    ap.add_argument("--client", default="client_a", choices=list(SIGNAL_TITLES.keys()))
    ap.add_argument("--titles", help="comma-separated custom job titles to search")
    ap.add_argument("--geo", default="colorado", help="geographic scope")
    ap.add_argument("--max-searches", type=int, default=20, help="max Serper searches")
    ap.add_argument("--output", help="output CSV path")
    args = ap.parse_args()

    titles = args.titles.split(",") if args.titles else SIGNAL_TITLES.get(args.client, [])
    geo = args.geo

    if not args.output:
        args.output = os.path.join(ROOT_DIR, "tools", f"_signal_jobs_{args.client}_{datetime.now().strftime('%Y%m%d')}.csv")

    print(f"client: {args.client}")
    print(f"geo: {geo}")
    print(f"titles: {len(titles)}")
    print(f"max searches: {args.max_searches}")
    print()

    # Load existing domains from master DB to flag overlaps
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT DISTINCT LOWER(domain) FROM leads WHERE domain IS NOT NULL AND domain!=''")
    known_domains = {r[0] for r in cur.fetchall()}
    conn.close()

    all_results = []
    searches_done = 0

    for title in titles:
        if searches_done >= args.max_searches:
            break

        # Search Indeed
        query = f'site:indeed.com "{title}" {geo}'
        results = search_serper(query, num=10)
        searches_done += 1
        time.sleep(0.3)

        # Search LinkedIn Jobs
        query2 = f'site:linkedin.com/jobs "{title}" {geo}'
        results2 = search_serper(query2, num=10)
        searches_done += 1
        time.sleep(0.3)

        for r in results + results2:
            company, domain = extract_company_from_result(r)
            if company:
                already_known = domain and domain.lower() in known_domains
                all_results.append({
                    "signal_title": title,
                    "company": company,
                    "domain": domain or "",
                    "source_url": r.get("link", ""),
                    "snippet": (r.get("snippet", "") or "")[:200],
                    "already_in_db": already_known,
                })

        print(f"  '{title}': {len(results) + len(results2)} results ({searches_done}/{args.max_searches} searches)", flush=True)

    # Dedup by company name (rough)
    seen = set()
    deduped = []
    for r in all_results:
        key = (r["company"].lower().strip(), r["domain"].lower())
        if key not in seen:
            seen.add(key)
            deduped.append(r)

    # Write CSV
    with open(args.output, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["signal_title", "company", "domain", "source_url", "snippet", "already_in_db"])
        w.writeheader()
        w.writerows(deduped)

    new_companies = [r for r in deduped if not r["already_in_db"]]
    known_companies = [r for r in deduped if r["already_in_db"]]

    print(f"\n=== RESULTS ===")
    print(f"  total found: {len(deduped)}")
    print(f"  NEW (not in master DB): {len(new_companies)}")
    print(f"  already known: {len(known_companies)}")
    print(f"  Serper credits used: {searches_done}")
    print(f"  output: {args.output}")

    if new_companies:
        print(f"\n=== TOP NEW SIGNAL COMPANIES ===")
        for r in new_companies[:15]:
            print(f"  [{r['signal_title']}] {r['company'][:40]}  {r['domain'] or '(no domain)'}")


if __name__ == "__main__":
    main()
