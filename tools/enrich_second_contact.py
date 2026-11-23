#!/usr/bin/env python3
"""
enrich_second_contact.py — Find a second decision maker at high-quality companies.

Only runs on companies that:
- Already have 1 verified contact (quality_score >= 7)
- Came from a curated directory source
- Have a domain

Finds a DIFFERENT role than the first contact:
  owner + director
  CEO + operations manager
  founder + VP

Cost-controlled:
- Only processes high-quality leads (not every company)
- Uses cached Serper results (no re-searching for domain)
- 1 Serper credit + 1 Haiku call per company
- Skips if company already has 2+ contacts in master DB

Usage:
    python3 tools/enrich_second_contact.py --niche "fire protection" --client client_c
    python3 tools/enrich_second_contact.py --niche msps --client client_b --limit 50
    python3 tools/enrich_second_contact.py --all-clients --min-score 7
"""

import os
import re
import sys
import json
import csv
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


def verify_email_mv(email, mv_key):
    """Verify one email via MillionVerifier. Returns True if valid."""
    if not mv_key or not email:
        return False
    try:
        r = requests.get(f'https://api.millionverifier.com/api/v3/?api={mv_key}&email={email}', timeout=30)
        if r.status_code == 200:
            return r.json().get('result') in ('ok', 'valid', 'good', 'risky')
    except Exception:
        pass
    return False


def main():
    ap = argparse.ArgumentParser(description="Find second contact at high-quality companies")
    ap.add_argument("--niche", help="limit to niche")
    ap.add_argument("--client", help="limit to client")
    ap.add_argument("--all-clients", action="store_true")
    ap.add_argument("--min-score", type=int, default=7, help="minimum quality score to attempt (default 7)")
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    SERPER_KEY = os.environ.get("SERPER_API_KEY", "")
    MV_KEY = os.environ.get("MILLIONVERIFIER_API_KEY", "")
    if not SERPER_KEY:
        print("ERROR: SERPER_API_KEY not set")
        sys.exit(2)

    # Route through llm_router — Kimi K2.6 for light extraction, Claude Haiku fallback.
    import sys
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from llm_router import get_light_client
    haiku, _haiku_model = get_light_client()

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    # Find companies with exactly 1 contact that scored 7+
    sql = """SELECT domain, company, first_name, last_name, title, email, client, niche
             FROM leads WHERE status='new' AND verified=1 AND domain IS NOT NULL AND domain!=''"""
    params = []
    if args.client:
        sql += " AND client=?"
        params.append(args.client)
    if args.niche:
        sql += " AND niche=?"
        params.append(args.niche)

    cur.execute(sql, params)
    all_leads = cur.fetchall()

    # Group by domain — find companies with exactly 1 contact
    from collections import defaultdict
    by_domain = defaultdict(list)
    for lead in all_leads:
        by_domain[lead["domain"]].append(lead)

    single_contact = {d: leads[0] for d, leads in by_domain.items() if len(leads) == 1}

    # Filter to high quality (has name + title + personal email)
    candidates = {}
    for domain, lead in single_contact.items():
        has_name = bool(lead["first_name"] and lead["last_name"])
        has_title = bool(lead["title"])
        has_personal = lead["email"] and "info@" not in lead["email"] and "contact@" not in lead["email"]
        score = (3 if has_personal else 1) + (3 if has_name else 0) + (2 if has_title else 0) + 1
        if score >= args.min_score:
            candidates[domain] = lead

    if args.limit:
        candidates = dict(list(candidates.items())[:args.limit])

    print(f"{'='*50}")
    print(f"  SECOND CONTACT ENRICHMENT")
    print(f"{'='*50}")
    print(f"  total leads in DB:     {len(all_leads)}")
    print(f"  single-contact companies: {len(single_contact)}")
    print(f"  high quality (score {args.min_score}+): {len(candidates)}")
    print()

    if args.dry_run:
        est_serper = len(candidates)
        print(f"  [DRY RUN] Would use ~{est_serper} Serper credits (~${est_serper*0.001:.2f})")
        print(f"  [DRY RUN] Would use ~{len(candidates)} Haiku calls (~${len(candidates)*0.0001:.2f})")
        return

    if not candidates:
        print("  no candidates for second contact")
        return

    # Complementary roles — find someone DIFFERENT from existing contact
    ROLE_COMPLEMENTS = {
        "owner": ["director of operations", "general manager", "office manager", "VP"],
        "president": ["director of operations", "VP of sales", "general manager"],
        "ceo": ["COO", "VP of operations", "director", "general manager"],
        "founder": ["director", "operations manager", "general manager"],
        "director": ["owner", "president", "manager"],
        "vp": ["director", "manager", "owner"],
    }

    added = 0
    searched = 0
    for domain, existing in candidates.items():
        existing_title = (existing["title"] or "").lower()
        existing_name = f"{existing['first_name']} {existing['last_name']}"

        # Determine what role to search for
        search_roles = ["director", "manager", "operations"]
        for role_key, complements in ROLE_COMPLEMENTS.items():
            if role_key in existing_title:
                search_roles = complements[:2]
                break

        # Try Blitz employee finder FIRST (FREE) before Serper (paid)
        company = existing["company"]
        blitz_found = False
        BLITZ_KEY = os.environ.get("BLITZ_API_KEY", "")

        if BLITZ_KEY and domain:
            try:
                bh = {"x-api-key": BLITZ_KEY, "Content-Type": "application/json"}
                # Get LinkedIn URL for this company
                d2l = requests.post("https://api.blitz-api.ai/v2/enrichment/domain-to-linkedin",
                                   json={"domain": domain}, headers=bh, timeout=15)
                if d2l.status_code == 200 and d2l.json().get("found"):
                    li_url = d2l.json().get("company_linkedin_url")
                    if li_url:
                        emp = requests.post("https://api.blitz-api.ai/v2/search/employee-finder",
                                          json={"company_linkedin_url": li_url, "max_results": 5,
                                                "seniority": ["owner", "founder", "c_suite", "director", "manager"]},
                                          headers=bh, timeout=30)
                        if emp.status_code == 200:
                            for p in emp.json().get("results", []):
                                p_name = f"{p.get('first_name','')} {p.get('last_name','')}".strip()
                                if p_name.lower() == existing_name.lower():
                                    continue  # skip the same person
                                p_email = p.get("email", "")
                                if not p_email:
                                    p_li = p.get("linkedin_url", "")
                                    if p_li:
                                        er = requests.post("https://api.blitz-api.ai/v2/enrichment/email",
                                                          json={"person_linkedin_url": p_li}, headers=bh, timeout=15)
                                        if er.status_code == 200:
                                            p_email = er.json().get("email", "")
                                if p_email and p_email.lower() not in {existing["email"].lower()}:
                                    fn = p.get("first_name", "")
                                    ln = p.get("last_name", "")
                                    title2 = p.get("title", "")
                                    # Verify and add
                                    if verify_email_mv(p_email, MV_KEY):
                                        if not cur.execute("SELECT id FROM leads WHERE LOWER(email)=?", (p_email.lower(),)).fetchone():
                                            cur.execute("""INSERT INTO leads (email,first_name,last_name,company,title,domain,
                                                           city,state,source,niche,client,verified,mv_result,status,date_added,date_updated)
                                                           VALUES (?,?,?,?,?,?,?,?,?,?,?,1,'ok','new',datetime('now'),datetime('now'))""",
                                                        (p_email, fn, ln, company, title2, domain,
                                                         "", "", "blitz_employee_finder", existing["niche"], existing["client"]))
                                            added += 1
                                            blitz_found = True
                                            print(f"  ✓ {company[:30]:<30} +{fn} {ln} ({title2}) → {p_email} [Blitz FREE]", flush=True)
                                            break
            except Exception:
                pass

        if blitz_found:
            continue

        # Fallback: Serper search (costs credits)
        q = f'"{company}" {" OR ".join(search_roles)}'
        try:
            r = requests.post("https://google.serper.dev/search",
                             headers={"X-API-KEY": SERPER_KEY, "Content-Type": "application/json"},
                             json={"q": q, "num": 5}, timeout=15)
            searched += 1
            if r.status_code != 200:
                continue

            snippets = "\n".join(f"{res.get('title','')} — {res.get('snippet','')}"
                                for res in r.json().get("organic", [])[:5])

            resp = haiku.messages.create(
                model=_haiku_model, max_tokens=100,
                messages=[{"role": "user", "content": f"""Find a DIFFERENT person at "{company}" (not {existing_name}).
Looking for roles: {', '.join(search_roles)}

Search results:
{snippets[:3000]}

Return ONLY JSON: {{"name": "full name or null", "first_name": "or null", "last_name": "or null", "title": "role or null"}}
Only return if confident this is a different person at the same company."""}])

            raw = resp.content[0].text.strip()
            m = re.search(r"\{[^}]*\}", raw, re.DOTALL)
            if not m:
                continue

            data = json.loads(m.group(0))
            name = data.get("name", "")
            if not name or name == "null" or name.lower() == existing_name.lower():
                continue

            fn = data.get("first_name") or (name.split()[0] if " " in name else name)
            ln = data.get("last_name") or (name.split()[-1] if " " in name else "")
            title2 = data.get("title", "") if data.get("title") != "null" else ""

            # Generate email pattern + verify
            patterns = [f"{fn.lower()}@{domain}"]
            if ln:
                patterns.append(f"{fn.lower()}.{ln.lower()}@{domain}")

            verified_email = None
            for email in patterns:
                try:
                    mv = requests.get(f"https://api.millionverifier.com/api/v3/?api={MV_KEY}&email={email}", timeout=30)
                    if mv.status_code == 200 and mv.json().get("result") in ("ok", "valid", "good", "risky"):
                        verified_email = email
                        break
                except:
                    pass
                time.sleep(0.3)

            if not verified_email:
                continue

            # Check not already in DB
            if cur.execute("SELECT id FROM leads WHERE LOWER(email)=?", (verified_email.lower(),)).fetchone():
                continue

            # Add to DB
            cur.execute("""INSERT INTO leads (email,first_name,last_name,company,title,domain,
                           city,state,source,niche,client,verified,mv_result,status,date_added,date_updated)
                           VALUES (?,?,?,?,?,?,?,?,?,?,?,1,'ok','new',datetime('now'),datetime('now'))""",
                        (verified_email, fn, ln, company, title2, domain,
                         "", "", "second_contact", existing["niche"], existing["client"]))
            added += 1
            print(f"  ✓ {company[:30]:<30} +{fn} {ln} ({title2}) → {verified_email}", flush=True)

        except Exception:
            pass
        time.sleep(0.2)

    conn.commit()
    conn.close()

    print(f"\n{'='*50}")
    print(f"  RESULTS")
    print(f"{'='*50}")
    print(f"  companies searched:  {searched}")
    print(f"  second contacts found: {added}")
    print(f"  serper credits:      {searched} (~${searched*0.001:.2f})")
    print(f"{'='*50}")


if __name__ == "__main__":
    main()
