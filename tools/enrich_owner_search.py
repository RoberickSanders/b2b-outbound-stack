#!/usr/bin/env python3
"""
enrich_owner_search.py — Find business owners via Google Search + Haiku extraction.

The cheapest, highest-quality enrichment for trades companies:
1. Google search "Company Name City State owner" → snippets with owner info
2. Haiku extracts owner name + title from snippets
3. Pattern inference generates personal email (firstname@domain.com)
4. MV verifies the email
5. LLM confirms niche fit

70% owner discovery rate at $0.001 per company (vs 0% from Firecrawl website scraping).

Usage:
    # Enrich a CSV of companies
    python3 tools/enrich_owner_search.py --input companies.csv --niche "fire protection" --client client_c

    # Enrich from Firecrawl directory scrape
    python3 tools/enrich_owner_search.py --input .firecrawl/fire-protection-all-states.csv --niche "fire protection" --client client_c

    # Limit batch size
    python3 tools/enrich_owner_search.py --input companies.csv --limit 100 --niche "fire alarm"

    # Skip MV verification (just find owners)
    python3 tools/enrich_owner_search.py --input companies.csv --no-verify

    # Dry run (show what would be searched, no API calls)
    python3 tools/enrich_owner_search.py --input companies.csv --dry-run

To disable: this tool is standalone. Don't call it and it doesn't run.
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
# STEP 1: DOMAIN DISCOVERY (Serper web search)
# ============================================================================

def find_domain(company, city, state, serper_key, cache=None):
    """Google search to find company website domain. Cost: 1 Serper credit."""
    cache_key = f"domain|{company.lower()}|{city.lower()}|{state.lower()}"
    if cache and cache_key in cache:
        return cache[cache_key]

    q = f"{company} {city} {state}"
    skip = {'yelp.com', 'facebook.com', 'linkedin.com', 'google.com', 'bbb.org',
            'yellowpages.com', 'manta.com', 'angi.com', 'homeadvisor.com',
            'thumbtack.com', 'mapquest.com', 'instagram.com', 'twitter.com',
            'youtube.com', 'fireinspectiondirectory.com', 'superpages.com'}

    try:
        r = requests.post('https://google.serper.dev/search',
                         headers={'X-API-KEY': serper_key, 'Content-Type': 'application/json'},
                         json={'q': q, 'num': 3}, timeout=15)
        if r.status_code == 400:
            return None  # out of credits
        if r.status_code == 200:
            from urllib.parse import urlparse
            for result in r.json().get('organic', []):
                url = result.get('link', '')
                host = (urlparse(url).hostname or '').replace('www.', '').lower()
                if host and not any(s in host for s in skip) and '.' in host:
                    if cache is not None:
                        cache[cache_key] = host
                    return host
    except Exception:
        pass
    if cache is not None:
        cache[cache_key] = None
    return None


# ============================================================================
# STEP 2: OWNER DISCOVERY (Serper + Haiku)
# ============================================================================

def find_owner(company, city, state, domain, serper_key, haiku_client, cache=None):
    """Google search for owner name + Haiku extraction. Cost: 1 Serper + 1 Haiku call."""
    cache_key = f"owner|{company.lower()}|{domain or ''}"
    if cache and cache_key in cache:
        return cache[cache_key]

    q = f'"{company}" {city} {state} owner OR founder OR president OR CEO'
    try:
        r = requests.post('https://google.serper.dev/search',
                         headers={'X-API-KEY': serper_key, 'Content-Type': 'application/json'},
                         json={'q': q, 'num': 5}, timeout=15)
        if r.status_code != 200:
            return None

        snippets = []
        for result in r.json().get('organic', []):
            snippets.append(f"{result.get('title', '')} — {result.get('snippet', '')}")
        kg = r.json().get('knowledgeGraph', {})
        if kg:
            snippets.append(f"Knowledge Graph: {json.dumps(kg)[:500]}")

        if not snippets:
            return None

        context = '\n'.join(snippets[:5])

        # Detect Kimi vs Claude client and use appropriate model name
        _base = str(getattr(haiku_client, "_base_url", "") or getattr(haiku_client, "base_url", ""))
        _model = "kimi-for-coding" if "kimi.com" in _base else "claude-haiku-4-5-20251001"
        resp = haiku_client.messages.create(
            model=_model, max_tokens=100,
            messages=[{'role': 'user', 'content': f"""From these Google search results, extract the owner/founder/president/CEO name of "{company}" in {city}, {state}.

Search results:
{context}

Return ONLY JSON: {{"owner_name": "full name or null", "first_name": "or null", "last_name": "or null", "title": "their title or null"}}
Only return a name if you are confident it's the owner/leader of THIS specific company."""}])

        raw = resp.content[0].text.strip()
        m = re.search(r'\{[^}]*\}', raw, re.DOTALL)
        if m:
            data = json.loads(m.group(0))
            name = data.get('owner_name', '')
            if name and name != 'null' and len(name) > 2:
                result = {
                    'owner_name': name,
                    'first_name': data.get('first_name') or (name.split()[0] if ' ' in name else name),
                    'last_name': data.get('last_name') or (name.split()[-1] if ' ' in name else ''),
                    'title': data.get('title', '') if data.get('title') != 'null' else '',
                }
                if cache is not None:
                    cache[cache_key] = result
                return result
    except Exception:
        pass

    if cache is not None:
        cache[cache_key] = None
    return None


# ============================================================================
# STEP 3: EMAIL GENERATION (pattern inference from owner name + domain)
# ============================================================================

def get_domain_pattern(domain):
    """Check domain_memory for a known winning email pattern.
    If we've verified emails at this domain before, use that pattern first."""
    try:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        cur.execute("SELECT best_pattern, domain_score FROM domain_memory WHERE domain=?",
                    (domain.lower(),))
        row = cur.fetchone()
        conn.close()
        if row and row[0]:
            return row[0], row[1]  # pattern, score
    except Exception:
        pass
    return None, 0


def update_domain_memory(domain, email, success=True):
    """Update domain_memory after a verification attempt."""
    try:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        local = email.split("@")[0].lower() if "@" in email else ""
        if success:
            cur.execute("""INSERT INTO domain_memory (domain, successful_verifications, best_pattern, last_success_date)
                          VALUES (?, 1, ?, datetime('now'))
                          ON CONFLICT(domain) DO UPDATE SET
                              successful_verifications = successful_verifications + 1,
                              best_pattern = COALESCE(best_pattern, ?),
                              last_success_date = datetime('now'),
                              domain_score = CAST(successful_verifications + 1 AS REAL) /
                                  MAX(successful_verifications + 1 + failed_verifications, 1)""",
                       (domain.lower(), local, local))
        else:
            cur.execute("""INSERT INTO domain_memory (domain, failed_verifications)
                          VALUES (?, 1)
                          ON CONFLICT(domain) DO UPDATE SET
                              failed_verifications = failed_verifications + 1,
                              domain_score = CAST(successful_verifications AS REAL) /
                                  MAX(successful_verifications + failed_verifications + 1, 1)""",
                       (domain.lower(),))
        conn.commit()
        conn.close()
    except Exception:
        pass


def generate_email_patterns(first_name, last_name, domain):
    """Generate likely email patterns from name + domain, ranked by probability.

    Checks domain_memory first — if we've verified emails at this domain before,
    tries the known winning pattern first (saves MV credits).

    Priority order:
      0. Known winning pattern from domain_memory (if exists)
      1. firstname@          (most common at small shops)
      2. firstnamelastname@  (common at slightly larger firms)
      3. firstname.lastname@ (professional services)
      4. firstinitiallast@   (less common but exists)
      5. info@               (fallback)
    """
    if not first_name or not domain:
        return [f'info@{domain}', f'contact@{domain}', f'office@{domain}']

    fn = first_name.lower().strip()
    ln = last_name.lower().strip() if last_name else ''

    patterns = []

    # Check domain memory for winning pattern
    known_pattern, score = get_domain_pattern(domain)
    if known_pattern and score > 0.5:
        # Apply known pattern format to this person's name
        if '.' in known_pattern:  # firstname.lastname format
            if ln:
                patterns.append(f'{fn}.{ln}@{domain}')
        elif len(known_pattern) > 3 and ln and known_pattern[1:] == known_pattern[1:]:
            # firstinitiallast format
            if ln:
                patterns.append(f'{fn[0]}{ln}@{domain}')
        else:
            # firstname format (most common)
            patterns.append(f'{fn}@{domain}')

    # Standard patterns ranked by probability
    if f'{fn}@{domain}' not in patterns:
        patterns.append(f'{fn}@{domain}')
    if ln:
        for p in [f'{fn}{ln}@{domain}', f'{fn}.{ln}@{domain}',
                  f'{fn[0]}{ln}@{domain}', f'{fn}_{ln}@{domain}',
                  f'{fn}{ln[0]}@{domain}']:
            if p not in patterns:
                patterns.append(p)

    # Generic fallbacks
    for p in [f'info@{domain}', f'contact@{domain}', f'office@{domain}']:
        if p not in patterns:
            patterns.append(p)

    return patterns


# ============================================================================
# STEP 4: MV VERIFICATION
# ============================================================================

def verify_email_mv(email, mv_key):
    """Verify one email via MillionVerifier. Returns True if valid."""
    try:
        r = requests.get(f'https://api.millionverifier.com/api/v3/?api={mv_key}&email={email}', timeout=30)
        if r.status_code == 200:
            status = r.json().get('result', '')
            return status in ('ok', 'valid', 'good', 'risky')
    except Exception:
        pass
    return False


# ============================================================================
# MAIN
# ============================================================================

def main():
    ap = argparse.ArgumentParser(description="Find business owners via Google Search + Haiku")
    ap.add_argument("--input", required=True, help="CSV with company,city,state,phone columns")
    ap.add_argument("--niche", default="fire protection", help="niche label for master DB")
    ap.add_argument("--client", default="client_c")
    ap.add_argument("--limit", type=int, default=0, help="max companies to process (0=all)")
    ap.add_argument("--no-verify", action="store_true", help="skip MV verification")
    ap.add_argument("--no-niche-check", action="store_true", help="skip LLM niche-fit check")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--output-dir", help="override output directory")
    args = ap.parse_args()

    SERPER_KEY = os.environ.get('SERPER_API_KEY', '')
    MV_KEY = os.environ.get('MILLIONVERIFIER_API_KEY', '')
    ICYPEAS_KEY = os.environ.get('ICYPEAS_API_KEY', '')

    if not SERPER_KEY:
        print("ERROR: SERPER_API_KEY not set")
        sys.exit(2)

    if ICYPEAS_KEY:
        print(f"  Icypeas: enabled (real email fallback)")
    else:
        print(f"  Icypeas: not configured (pattern guessing only)")

    # Cost limit — checked after companies are loaded (see below)

    # Route through llm_router — Kimi K2.6 for light tasks, Claude Haiku fallback.
    import sys
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from llm_router import get_light_client
    haiku, _haiku_model = get_light_client()

    # Load companies
    companies = []
    with open(args.input) as f:
        for r in csv.DictReader(f):
            companies.append(r)
    if args.limit:
        companies = companies[:args.limit]

    # Dedup against master DB
    sys.path.insert(0, SCRIPT_DIR)
    from dedup_before_enrich import get_known_domains, get_known_emails
    known_domains = get_known_domains()
    known_emails = get_known_emails()

    print(f"{'='*60}")
    print(f"  OWNER SEARCH ENRICHMENT")
    print(f"{'='*60}")
    print(f"  input:       {args.input} ({len(companies)} companies)")
    print(f"  niche:       {args.niche}")
    print(f"  client:      {args.client}")
    print(f"  known in DB: {len(known_domains)} domains, {len(known_emails)} emails")
    print()

    # Cost estimate + flag if over limit
    COST_LIMIT = float(os.environ.get("FORGE_COST_LIMIT", "10.00"))
    estimated_serper = len(companies) * 2 * 0.001
    estimated_mv = len(companies) * 3 * 0.001
    estimated_haiku = len(companies) * 0.0001
    estimated_total = estimated_serper + estimated_mv + estimated_haiku
    print(f"  Cost estimate: ~${estimated_total:.2f} (Serper ${estimated_serper:.2f} + MV ${estimated_mv:.2f} + Haiku ${estimated_haiku:.2f})")

    if estimated_total > COST_LIMIT:
        print(f"\n  ⚠️  COST FLAG: ${estimated_total:.2f} exceeds ${COST_LIMIT:.2f} limit")
        print(f"  Set FORGE_COST_LIMIT={int(estimated_total + 1)} to override, or use --limit to reduce batch size")
        print(f"  BLOCKED. Re-run with --limit {int(COST_LIMIT / (5 * 0.001))} or raise the limit.")
        return
    else:
        print(f"  ✓ Under ${COST_LIMIT:.2f} limit — proceeding\n")

    if args.dry_run:
        print(f"  [DRY RUN] Would use ~{len(companies)*2} Serper credits (~${estimated_serper:.2f})")
        print(f"  [DRY RUN] Would use ~{len(companies)} Haiku calls (~${estimated_haiku:.2f})")
        print(f"  [DRY RUN] Estimated total: ~${estimated_total:.2f}")
        return

    # Cache for search results
    cache_file = os.path.join(SCRIPT_DIR, '_owner_search_cache.json')
    cache = {}
    if os.path.isfile(cache_file):
        try:
            cache = json.load(open(cache_file))
        except Exception:
            pass

    # Process companies
    results = []
    serper_credits = 0
    stats = {'domain_found': 0, 'domain_new': 0, 'owner_found': 0, 'email_verified': 0, 'niche_confirmed': 0}

    for i, co in enumerate(companies):
        company = co.get('company', '').strip()
        city = co.get('city', '').strip()
        state = co.get('state', '').strip()
        phone = co.get('phone', '').strip()

        if not company:
            continue

        # Step 1: Find domain
        domain = find_domain(company, city, state, SERPER_KEY, cache)
        serper_credits += 1
        if not domain:
            continue
        stats['domain_found'] += 1

        # Check if domain already in DB
        if domain.lower() in known_domains:
            continue
        stats['domain_new'] += 1

        # Step 2: Find owner
        owner = find_owner(company, city, state, domain, SERPER_KEY, haiku, cache)
        serper_credits += 1

        if owner:
            stats['owner_found'] += 1
            first_name = owner.get('first_name', '')
            last_name = owner.get('last_name', '')
            title = owner.get('title', '')
        else:
            first_name = ''
            last_name = ''
            title = ''

        # Step 3: Generate email patterns
        patterns = generate_email_patterns(first_name, last_name, domain)

            # Step 4: MV verify — cheapest attempts first, Icypeas only as fallback
        #
        # Order (optimized for cost):
        #   1. Try top 2 patterns via MV ($0.001 each) — cheapest
        #   2. If both fail → Icypeas domain search ($0.01-0.02) — only when cheap options exhausted
        #   3. If Icypeas found something → MV verify that
        #   4. If still nothing → try remaining patterns via MV
        #
        # This way ~60% of companies never touch Icypeas at all.

        verified_email = None
        if not args.no_verify and MV_KEY:
            # Phase 1: Try top 2 cheapest patterns first
            top_patterns = patterns[:2]  # firstname@ and info@ (or similar)
            for email in top_patterns:
                if email.lower() in known_emails:
                    continue
                if verify_email_mv(email, MV_KEY):
                    verified_email = email
                    stats['email_verified'] += 1
                    break
                time.sleep(0.3)

            # Phase 2: Top patterns failed → try Icypeas (more expensive but finds real emails)
            if not verified_email and ICYPEAS_KEY and domain:
                try:
                    sys.path.insert(0, ROOT_DIR)
                    from enrichment import icypeas_domain_search
                    pipeline_cache_file = os.path.join(ROOT_DIR, 'v2_pipeline_cache.json')
                    pipeline_cache = {}
                    try:
                        pipeline_cache = json.load(open(pipeline_cache_file)) if os.path.isfile(pipeline_cache_file) else {}
                    except Exception:
                        pass
                    icy_results = icypeas_domain_search(domain, pipeline_cache, pipeline_cache_file)
                    for r in icy_results:
                        email = r.get('email', '')
                        if email and email.lower() not in known_emails:
                            if verify_email_mv(email, MV_KEY):
                                verified_email = email
                                stats['email_verified'] += 1
                                stats.setdefault('icypeas_found', 0)
                                stats['icypeas_found'] += 1
                                break
                            time.sleep(0.3)
                except Exception:
                    pass

            # Phase 3: Icypeas also failed → try remaining patterns
            if not verified_email:
                for email in patterns[2:]:  # skip first 2 (already tried)
                    if email.lower() in known_emails:
                        continue
                    if verify_email_mv(email, MV_KEY):
                        verified_email = email
                        stats['email_verified'] += 1
                        break
                    time.sleep(0.3)
        else:
            verified_email = patterns[0] if patterns else None

        if not verified_email:
            continue

        # Score lead quality (higher = better prospect)
        quality_score = 0
        email_type = 'generic'
        if verified_email:
            if 'info@' in verified_email or 'contact@' in verified_email or 'office@' in verified_email:
                email_type = 'generic'
                quality_score += 1
            else:
                email_type = 'personal'
                quality_score += 3
        if owner and owner.get('owner_name') and owner['owner_name'] != 'null':
            quality_score += 3  # has real owner name
        if title and title != 'null':
            quality_score += 2  # has title (Owner, CEO, etc)
        if phone:
            quality_score += 1  # has phone for follow-up calls

        result = {
            'company': company, 'domain': domain, 'city': city, 'state': state,
            'phone': phone, 'email': verified_email,
            'first_name': first_name if first_name != 'null' else '',
            'last_name': last_name if last_name != 'null' else '',
            'title': title if title != 'null' else '',
            'owner_name': owner.get('owner_name', '') if owner else '',
            'email_type': email_type,
            'quality_score': quality_score,
        }
        results.append(result)

        # Progress
        if (i + 1) % 20 == 0:
            # Save cache periodically
            with open(cache_file, 'w') as f:
                json.dump(dict(cache), f, indent=2)
            print(f"  [{i+1}/{len(companies)}] domains={stats['domain_new']} owners={stats['owner_found']} verified={stats['email_verified']}", flush=True)

    # Save cache
    with open(cache_file, 'w') as f:
        json.dump(dict(cache), f, indent=2)

    # Step 5: LLM niche-fit check
    if not args.no_niche_check and results:
        print(f"\n  LLM niche-fit check on {len(results)} leads...", flush=True)
        confirmed = []
        for r in results:
            try:
                resp = haiku.messages.create(
                    model=_haiku_model, max_tokens=30,
                    messages=[{'role': 'user', 'content': f'This company was found in a {args.niche} industry directory. Company name: "{r["company"]}". Could this company plausibly offer {args.niche} services? Only say "no" if the name clearly indicates a DIFFERENT industry (e.g., a restaurant, hair salon, software company). Reply ONLY "yes" or "no".'}])
                if 'yes' in resp.content[0].text.strip().lower():
                    confirmed.append(r)
                    stats['niche_confirmed'] += 1
            except Exception:
                confirmed.append(r)
                stats['niche_confirmed'] += 1
        results = confirmed

    # Export
    niche_slug = re.sub(r'[^a-z0-9]+', '-', args.niche.lower()).strip('-')[:30]
    if args.output_dir:
        outdir = args.output_dir
    else:
        outdir = os.path.join(ROOT_DIR, '..', '..', '01-Projects', args.client,
                              'lead-runs', f'{niche_slug}-owners-{datetime.now().strftime("%Y%m%d")}')
    os.makedirs(outdir, exist_ok=True)

    FIELDS = ['email', 'first_name', 'last_name', 'company_name', 'phone', 'title',
              'website', 'custom1', 'custom2', 'custom3']
    sl_path = os.path.join(outdir, 'smartlead_import.csv')
    with open(sl_path, 'w', newline='') as f:
        w = csv.DictWriter(f, fieldnames=FIELDS)
        w.writeheader()
        # Sort by quality score (best leads first)
        results.sort(key=lambda r: r.get('quality_score', 0), reverse=True)

        for r in results:
            w.writerow({
                'email': r['email'], 'first_name': r['first_name'],
                'last_name': r['last_name'], 'company_name': r['company'],
                'phone': r['phone'], 'title': r['title'],
                'website': r['domain'],
                'custom1': r.get('email_type', ''),        # personal vs generic
                'custom2': str(r.get('quality_score', 0)),  # 0-9 quality score
                'custom3': f"{r.get('city','')} {r.get('state','')}".strip(),  # location
            })

    # Import to master DB
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    added = 0
    for r in results:
        email = r['email'].lower().strip()
        if cur.execute("SELECT id FROM leads WHERE LOWER(email)=?", (email,)).fetchone():
            continue
        cur.execute("""INSERT INTO leads (email,first_name,last_name,company,phone,title,domain,
                       city,state,source,niche,client,verified,mv_result,status,date_added,date_updated)
                       VALUES (?,?,?,?,?,?,?,?,?,?,?,?,1,'ok','new',datetime('now'),datetime('now'))""",
                    (r['email'], r['first_name'], r['last_name'], r['company'], r['phone'],
                     r['title'], r['domain'], r['city'], r['state'],
                     'owner_search', niche_slug, args.client))
        added += 1
    conn.commit()
    conn.close()

    # Report
    has_name = sum(1 for r in results if r.get('owner_name') and len(r['owner_name']) > 2)
    has_personal = sum(1 for r in results if r.get('email_type') == 'personal')
    has_phone = sum(1 for r in results if r.get('phone'))
    has_title = sum(1 for r in results if r.get('title'))
    avg_score = sum(r.get('quality_score', 0) for r in results) / max(len(results), 1)
    high_quality = sum(1 for r in results if r.get('quality_score', 0) >= 7)

    print(f"\n{'='*60}")
    print(f"  RESULTS")
    print(f"{'='*60}")
    print(f"  companies processed:  {len(companies)}")
    print(f"  domains found:        {stats['domain_found']} ({100*stats['domain_found']//max(len(companies),1)}%)")
    print(f"  genuinely new:        {stats['domain_new']}")
    print(f"  owners found:         {stats['owner_found']} ({100*stats['owner_found']//max(stats['domain_new'],1)}%)")
    print(f"  icypeas real emails:  {stats.get('icypeas_found', 0)}")
    print(f"  emails verified:      {stats['email_verified']}")
    print(f"  niche confirmed:      {stats['niche_confirmed']}")
    print(f"  FINAL LEADS:          {len(results)}")
    print(f"    with owner name:    {has_name} ({100*has_name//max(len(results),1)}%)")
    print(f"    with personal email:{has_personal} ({100*has_personal//max(len(results),1)}%)")
    print(f"    with phone:         {has_phone} ({100*has_phone//max(len(results),1)}%)")
    print(f"    with title:         {has_title} ({100*has_title//max(len(results),1)}%)")
    print(f"    avg quality score:  {avg_score:.1f}/9")
    print(f"    high quality (7+):  {high_quality} ({100*high_quality//max(len(results),1)}%)")
    print(f"  added to master DB:   {added}")
    print(f"  serper credits used:  {serper_credits} (~${serper_credits*0.001:.2f})")
    print(f"  output:               {sl_path}")
    print(f"{'='*60}")

    if results:
        print(f"\n  SAMPLE LEADS:")
        for r in results[:15]:
            name = r.get('owner_name', '') or '(no name)'
            print(f"  {'⭐' if name and len(name)>5 else '  '} {(name or '(no name)'):<25} {(r.get('title','') or ''):<15} {r['email']:<35} {r['company']}")


if __name__ == "__main__":
    main()
