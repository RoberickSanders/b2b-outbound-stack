#!/usr/bin/env python3
"""
forge_enrich.py — The Forge's unified enrichment engine.

Takes a list of companies (with or without domains) and squeezes every possible
verified lead out of them using ALL available tools in optimal order.

This replaces the scattered approach where each tool ran separately.
One function, all tools, optimal routing, cost-controlled.

Flow per company:
  1. MX pre-check (FREE) → skip dead domains
  2. Domain memory → try known winning pattern first (FREE)
  3. Blitz waterfall → try if LinkedIn-heavy (FREE)
  4. Owner search → Google + Haiku for owner name ($0.002)
  5. Smart pattern → based on MX type (Google/M365/custom) ($0.001 MV)
  6. Icypeas name+domain → if we have owner name ($0.015)
  7. Icypeas domain-only → fallback ($0.015)
  8. Catch-all check → if catch-all, accept firstname@ (FREE)
  9. Google Maps email → check Maps listing ($0.001)

Stops at first verified email. No wasted credits after success.

Usage:
    # Enrich companies from a CSV
    python3 tools/forge_enrich.py --input companies.csv --niche "cost segregation" --client client_c

    # Dry run (show cost estimate + routing plan)
    python3 tools/forge_enrich.py --input companies.csv --dry-run

    # Called automatically by the cascade after discovery
"""

import os
import re
import sys
import csv
import json
import time
import sqlite3
import subprocess
import argparse
import requests
import threading
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


SERPER_KEY = os.environ.get("SERPER_API_KEY", "")
MV_KEY = os.environ.get("MILLIONVERIFIER_API_KEY", "")
ICYPEAS_KEY = os.environ.get("ICYPEAS_API_KEY", "")
ICYPEAS_SECRET = os.environ.get("ICYPEAS_API_SECRET", "")
BLITZ_KEY = os.environ.get("BLITZ_API_KEY", "")
AIARK_KEY = os.environ.get("AIARK_API_KEY", "")


# ============================================================================
# THREAD-SAFE HELPERS
# ============================================================================

_stats_lock = threading.Lock()
_emails_lock = threading.Lock()
_mx_cache = {}
_mx_cache_lock = threading.Lock()


def safe_stat_inc(stats, key, n=1):
    """Thread-safe stats increment."""
    with _stats_lock:
        stats[key] = stats.get(key, 0) + n


def safe_check_email(known_emails, email):
    """Thread-safe email set check."""
    with _emails_lock:
        return email.lower() in known_emails


def safe_add_email(known_emails, email):
    """Thread-safe email set add."""
    with _emails_lock:
        known_emails.add(email.lower())


# ============================================================================
# STEP 1: MX PRE-CHECK (with caching)
# ============================================================================

def mx_check(domain):
    """Check MX record type. Returns 'google'|'microsoft'|'custom'|'none'. Cached."""
    with _mx_cache_lock:
        if domain in _mx_cache:
            return _mx_cache[domain]
    try:
        r = subprocess.run(['dig', '+short', 'MX', domain],
                          capture_output=True, text=True, timeout=5)
        mx = r.stdout.lower()
        if not mx.strip():
            result = 'none'
        elif 'google' in mx or 'gmail' in mx:
            result = 'google'
        elif 'outlook' in mx or 'microsoft' in mx:
            result = 'microsoft'
        else:
            result = 'custom'
    except Exception:
        result = 'unknown'
    with _mx_cache_lock:
        _mx_cache[domain] = result
    return result


# ============================================================================
# STEP 2: DOMAIN MEMORY
# ============================================================================

def get_domain_memory(domain):
    """Get winning pattern and score from domain_memory table."""
    try:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        cur.execute("SELECT best_pattern, domain_score FROM domain_memory WHERE domain=?",
                    (domain.lower(),))
        row = cur.fetchone()
        conn.close()
        if row and row[0]:
            return row[0], row[1]
    except Exception:
        pass
    return None, 0


def update_domain_memory(domain, email, success=True):
    """Update domain_memory after verification."""
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
                              failed_verifications = failed_verifications + 1""",
                       (domain.lower(),))
        conn.commit()
        conn.close()
    except Exception:
        pass


# ============================================================================
# STEP 3: OWNER SEARCH (Google + Haiku)
# ============================================================================

def find_owner_google(company, city, state, domain, haiku_client):
    """Search Google for owner name + extract with Haiku."""
    if not SERPER_KEY:
        return None
    q = f'"{company}" {city} {state} owner OR founder OR president OR CEO'
    try:
        r = requests.post('https://google.serper.dev/search',
                         headers={'X-API-KEY': SERPER_KEY, 'Content-Type': 'application/json'},
                         json={'q': q, 'num': 5}, timeout=15)
        if r.status_code != 200:
            return None
        snippets = '\n'.join(f"{res.get('title','')} — {res.get('snippet','')}"
                            for res in r.json().get('organic', [])[:5])
        if not snippets:
            return None
        # Model name detected from the client. Kimi's endpoint requires kimi-for-coding
        # as the model string; Anthropic's SDK accepts the Haiku model string.
        _base = str(getattr(haiku_client, "_base_url", "") or getattr(haiku_client, "base_url", ""))
        _model = "kimi-for-coding" if "kimi.com" in _base else "claude-haiku-4-5-20251001"
        resp = haiku_client.messages.create(
            model=_model, max_tokens=100,
            messages=[{'role': 'user', 'content': f"""From these Google results, extract the owner/founder/president name of "{company}".
Return ONLY JSON: {{"name": "full name or null", "first_name": "or null", "last_name": "or null", "title": "or null"}}
Only return if confident."""}])
        raw = resp.content[0].text.strip()
        m = re.search(r'\{[^}]*\}', raw, re.DOTALL)
        if m:
            data = json.loads(m.group(0))
            if data.get('name') and data['name'] != 'null' and len(data['name']) > 2:
                return data
    except Exception:
        pass
    return None


# ============================================================================
# STEP 4: EMAIL VERIFICATION (single + bulk)
# ============================================================================

_mv_cache = {}
_mv_cache_lock = threading.Lock()


def verify_mv(email):
    """Verify one email via MV. Returns True/False. Thread-safe with cache."""
    if not MV_KEY:
        return False
    email_lower = email.lower()
    with _mv_cache_lock:
        if email_lower in _mv_cache:
            return _mv_cache[email_lower]
    try:
        r = requests.get(f'https://api.millionverifier.com/api/v3/?api={MV_KEY}&email={email}', timeout=30)
        if r.status_code == 200:
            result = r.json().get('result') in ('ok', 'valid', 'good', 'risky')
            with _mv_cache_lock:
                _mv_cache[email_lower] = result
            return result
    except Exception:
        pass
    with _mv_cache_lock:
        _mv_cache[email_lower] = False
    return False


def verify_mv_batch(emails):
    """Batch verify emails via MV Bulk API. Returns dict of email → True/False.
    Falls back to single API if bulk upload fails."""
    if not MV_KEY or not emails:
        return {}
    try:
        from mv_bulk_verify import bulk_verify, filter_valid
        results = bulk_verify(emails, max_wait=120)
        valid_set = set(filter_valid(results).keys())
        out = {}
        for email in emails:
            is_valid = email.lower() in valid_set
            out[email] = is_valid
            with _mv_cache_lock:
                _mv_cache[email.lower()] = is_valid
        return out
    except Exception as e:
        print(f"  MV bulk fallback to single API: {e}")
        return {email: verify_mv(email) for email in emails}


# ============================================================================
# STEP 5: ICYPEAS
# ============================================================================

def icypeas_find_by_name_domain(first_name, last_name, domain):
    """Icypeas email finder by name + domain. More accurate than domain-only."""
    if not ICYPEAS_KEY:
        return None
    try:
        r = requests.post('https://app.icypeas.com/api/email-search',
                         headers={'Authorization': ICYPEAS_KEY, 'X-API-SECRET': ICYPEAS_SECRET or ''},
                         json={'firstName': first_name, 'lastName': last_name, 'domainOrCompany': domain},
                         timeout=30)
        if r.status_code == 200:
            data = r.json()
            if data.get('success') and data.get('item', {}).get('_id'):
                # Poll for result
                search_id = data['item']['_id']
                for _ in range(10):
                    time.sleep(2)
                    pr = requests.post('https://app.icypeas.com/api/bulk-single-searchs/read',
                                      headers={'Authorization': ICYPEAS_KEY, 'X-API-SECRET': ICYPEAS_SECRET or ''},
                                      json={'id': search_id}, timeout=15)
                    if pr.status_code == 200:
                        pd = pr.json()
                        if pd.get('status') == 'FINISHED':
                            emails = pd.get('results', {}).get('emails', [])
                            if emails:
                                return emails[0].get('email') if isinstance(emails[0], dict) else emails[0]
                            break
    except Exception:
        pass
    return None


def icypeas_domain_search(domain):
    """Icypeas domain search — find any email at this domain."""
    if not ICYPEAS_KEY:
        return []
    try:
        sys.path.insert(0, ROOT_DIR)
        from enrichment import icypeas_domain_search as _icy
        pipeline_cache_file = os.path.join(ROOT_DIR, 'v2_pipeline_cache.json')
        cache = {}
        try:
            cache = json.load(open(pipeline_cache_file)) if os.path.isfile(pipeline_cache_file) else {}
        except Exception:
            pass
        results = _icy(domain, cache, pipeline_cache_file)
        return [r['email'] for r in results if r.get('email')]
    except Exception:
        return []


# ============================================================================
# STEP 6: CATCH-ALL CHECK
# ============================================================================

def check_catch_all(domain):
    """Check if domain is catch-all from DB or by testing a random address."""
    try:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        cur.execute("SELECT catch_all FROM leads WHERE LOWER(domain)=? AND catch_all IS NOT NULL LIMIT 1",
                    (domain.lower(),))
        row = cur.fetchone()
        conn.close()
        if row:
            return bool(row[0])
    except Exception:
        pass
    # If not cached, check via MV with a random address
    import random, string
    random_local = ''.join(random.choices(string.ascii_lowercase, k=12))
    return verify_mv(f'{random_local}@{domain}')


# ============================================================================
# MAIN ENRICHMENT ENGINE
# ============================================================================

def enrich_company(company, domain, city, state, phone, mx_type, haiku_client, known_emails, stats, allow_nameless=False, niche=""):
    """Run the full enrichment cascade on one company.
    Returns dict with email + contact info, or None if all methods fail.

    Args:
      allow_nameless: when False (default), skip role-account fallback sources
        (Google Maps email, website scrape, Icypeas domain-only without a name).
        Matches v2 pipeline's `allow_nameless=False` default — ensures {{first_name}}
        merge works in cold email copy. Set to True for privacy-heavy niches
        (cannabis, healthcare) where generic business emails are acceptable.
      niche: canonical niche slug (e.g. "workers-comp-recovery"). Drives the
        NICHE_TITLES lookup for AI Ark people-search — tailors decision-maker
        titles per niche. Falls back to generic DM title set if niche is unknown.
    """
    if not domain:
        return None

    # Step 1: MX pre-check (already done in batch)
    if mx_type == 'none':
        safe_stat_inc(stats, 'skip_no_mx')
        return None

    # Step 2: Domain memory — try known winning pattern
    known_pattern, score = get_domain_memory(domain)
    if known_pattern and score > 0.5:
        # Find owner name first for pattern application
        owner = find_owner_google(company, city, state, domain, haiku_client)
        if owner:
            fn = (owner.get('first_name') or '').lower()
            ln = (owner.get('last_name') or '').lower()
            if fn:
                # Apply known pattern format
                if '.' in known_pattern and ln:
                    email = f'{fn}.{ln}@{domain}'
                elif len(known_pattern) > 1 and known_pattern[0] != known_pattern[1:] and ln:
                    email = f'{fn[0]}{ln}@{domain}'
                else:
                    email = f'{fn}@{domain}'
                if email.lower() not in known_emails and verify_mv(email):
                    update_domain_memory(domain, email, True)
                    safe_stat_inc(stats, 'domain_memory_hit')
                    return {'email': email, 'first_name': fn.title(), 'last_name': ln.title(),
                            'title': owner.get('title', ''), 'company': company, 'domain': domain,
                            'city': city, 'state': state, 'phone': phone, 'source': 'domain_memory'}

    # Step 2b: Blitz reverse phone lookup (FREE — unlimited plan)
    if BLITZ_KEY and phone:
        try:
            bh = {'x-api-key': BLITZ_KEY, 'Content-Type': 'application/json'}
            # Normalize phone to +1XXXXXXXXXX format
            import re as _re
            digits = _re.sub(r'\D', '', phone)
            if len(digits) == 10:
                digits = '1' + digits
            if len(digits) == 11 and digits[0] == '1':
                formatted = f'+{digits}'
                rp = requests.post('https://api.blitz-api.ai/v2/enrichment/phone-to-person',
                                  json={'phone': formatted}, headers=bh, timeout=15)
                if rp.status_code == 200 and rp.json().get('found'):
                    person = rp.json().get('person', {})
                    p_name = person.get('full_name', '')
                    p_title = person.get('title', '')
                    p_linkedin = person.get('linkedin_url', '')
                    if p_name and p_linkedin:
                        # Found owner via phone — now get email via LinkedIn
                        er = requests.post('https://api.blitz-api.ai/v2/enrichment/email',
                                          json={'person_linkedin_url': p_linkedin}, headers=bh, timeout=15)
                        if er.status_code == 200:
                            p_email = er.json().get('email', '')
                            if p_email and p_email.lower() not in known_emails:
                                if verify_mv(p_email):
                                    parts = p_name.split()
                                    fn = parts[0] if parts else ''
                                    ln = parts[-1] if len(parts) > 1 else ''
                                    update_domain_memory(domain, p_email, True)
                                    safe_stat_inc(stats, 'phone_lookup_hit')
                                    return {'email': p_email, 'first_name': fn, 'last_name': ln,
                                            'title': p_title, 'company': company, 'domain': domain,
                                            'city': city, 'state': state, 'phone': phone, 'source': 'blitz_phone'}
        except Exception:
            pass

    # Step 2b2: Blitz reverse email lookup (FREE — if we already found a generic email)
    if BLITZ_KEY and domain:
        for generic in [f'info@{domain}', f'contact@{domain}']:
            try:
                bh = {'x-api-key': BLITZ_KEY, 'Content-Type': 'application/json'}
                re_r = requests.post('https://api.blitz-api.ai/v2/enrichment/email-to-person',
                                   json={'email': generic}, headers=bh, timeout=15)
                if re_r.status_code == 200 and re_r.json().get('found'):
                    person = re_r.json().get('person', {})
                    p_name = person.get('full_name', '')
                    if p_name:
                        parts = p_name.split()
                        fn = parts[0].lower() if parts else ''
                        ln = parts[-1].lower() if len(parts) > 1 else ''
                        p_title = person.get('title', '')
                        # Now we know who manages info@ — try their personal email
                        personal = f'{fn}@{domain}' if fn else None
                        if personal and personal.lower() not in known_emails and verify_mv(personal):
                            update_domain_memory(domain, personal, True)
                            safe_stat_inc(stats, 'email_reverse_hit')
                            return {'email': personal, 'first_name': fn.title(), 'last_name': ln.title(),
                                    'title': p_title, 'company': company, 'domain': domain,
                                    'city': city, 'state': state, 'phone': phone, 'source': 'blitz_email_reverse'}
                        break
            except Exception:
                pass

    # Step 2c: Check for Google Maps email (FREE — already in Serper response)
    # ROLE-ACCOUNT SOURCE: returns a business email with no person name attached.
    # Skip when allow_nameless=False — the lead would have empty first_name, which
    # breaks {{first_name}} merge in cold email copy.
    if SERPER_KEY and allow_nameless:
        try:
            r = requests.post('https://google.serper.dev/places',
                             headers={'X-API-KEY': SERPER_KEY, 'Content-Type': 'application/json'},
                             json={'q': f'{company} {city} {state}', 'num': 1}, timeout=10)
            if r.status_code == 200:
                places = r.json().get('places', [])
                if places:
                    maps_email = places[0].get('email', '')
                    if maps_email and maps_email.lower() not in known_emails:
                        if verify_mv(maps_email):
                            update_domain_memory(domain, maps_email, True)
                            safe_stat_inc(stats, 'maps_email_hit')
                            return {'email': maps_email, 'first_name': '', 'last_name': '',
                                    'title': '', 'company': company, 'domain': domain,
                                    'city': city, 'state': state, 'phone': phone, 'source': 'google_maps'}
        except Exception:
            pass

    # Step 2c: Check Blitz for ANY employee email to infer format
    blitz_format = None
    if BLITZ_KEY:
        try:
            bh = {'x-api-key': BLITZ_KEY, 'Content-Type': 'application/json'}
            r1 = requests.post('https://api.blitz-api.ai/v2/enrichment/domain-to-linkedin',
                              json={'domain': domain}, headers=bh, timeout=15)
            if r1.status_code == 200 and r1.json().get('found'):
                li = r1.json().get('company_linkedin_url')
                if li:
                    r2 = requests.post('https://api.blitz-api.ai/v2/search/waterfall-icp-keyword',
                                      json={'company': {'linkedin_url': li},
                                            'contact': {'seniority': ['owner','founder','c_suite','director','manager']},
                                            'max_results': 3},
                                      headers=bh, timeout=30)
                    if r2.status_code == 200:
                        for p in r2.json().get('results', []):
                            emp_email = p.get('email', '')
                            if emp_email and '@' in emp_email:
                                local = emp_email.split('@')[0].lower()
                                # Infer format: john.smith → first.last, jsmith → firstinitiallast, john → first
                                if '.' in local:
                                    blitz_format = 'first.last'
                                elif len(local) > 4:
                                    blitz_format = 'firstlast'
                                else:
                                    blitz_format = 'first'
                                # If this IS an owner-level person, use their email directly
                                if p.get('email') and p['email'].lower() not in known_emails:
                                    if verify_mv(p['email']):
                                        fn = p.get('first_name', '')
                                        ln = p.get('last_name', '')
                                        title_found = p.get('title', '')
                                        update_domain_memory(domain, p['email'], True)
                                        safe_stat_inc(stats, 'blitz_direct')
                                        return {'email': p['email'], 'first_name': fn, 'last_name': ln,
                                                'title': title_found, 'company': company, 'domain': domain,
                                                'city': city, 'state': state, 'phone': phone, 'source': 'blitz_direct'}
                                break
        except Exception:
            pass

    # Step 2c.5 NEW: AI Ark people-search (real names + BounceBan-verified emails).
    # Called BEFORE falling back to role-account sources (website scrape below).
    # AI Ark returns decision-makers with titles, and exports emails pre-verified
    # by BounceBan. Cost: 0.5 credits per people-search page + 1 credit per found
    # email (0 credits if not found).
    #
    # Titles are niche-specific via NICHE_TITLES lookup (falls back to generic
    # DM titles). This improves match quality for verticals where the decision
    # maker isn't always "CEO/Founder" — e.g. WC Recovery targets BD Directors,
    # MSPs often surface through CTOs, R&D Tax Credit goes through Manufacturing
    # Practice Leaders.
    if AIARK_KEY:
        try:
            import sys as _sys
            _parent = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            if _parent not in _sys.path:
                _sys.path.insert(0, _parent)
            from v2_aiark import find_people, export_email

            # Niche-specific title lists. Fall back to default set for unknown niches.
            _GENERIC_TITLES = [
                "Owner", "Founder", "CEO", "President",
                "Partner", "Managing Director", "Principal",
                "Director of Business Development", "VP Business Development",
            ]
            NICHE_TITLES = {
                "workers-comp-recovery": [
                    "Owner", "Founder", "President", "CEO", "Managing Partner",
                    "Director of Business Development", "VP Business Development",
                    "Director of Claims", "VP Claims", "Director of Operations",
                    "Chief Revenue Officer",
                ],
                "rd-tax-credit": [
                    "Owner", "Founder", "Partner", "Managing Partner", "President", "CEO",
                    "Manufacturing Practice Leader", "Practice Leader",
                    "Director of Business Development", "VP Business Development",
                    "Director of Sales",
                ],
                "sales-tax-recovery": [
                    "Owner", "Founder", "Partner", "Managing Partner", "President", "CEO",
                    "Director of Business Development", "VP Business Development",
                    "Principal", "Director of Sales",
                ],
                "cost-segregation": [
                    "Owner", "Founder", "Principal", "President", "CEO", "Managing Partner",
                    "Director of Business Development", "VP Business Development",
                ],
                "property-tax-appeal": [
                    "Owner", "Founder", "Partner", "Managing Partner", "President", "CEO",
                    "Director of Business Development", "Principal",
                ],
                "utility-audit": [
                    "Owner", "Founder", "President", "CEO", "Managing Partner",
                    "Director of Business Development", "VP Business Development",
                    "Principal Consultant",
                ],
                "telecom-audit": [
                    "Owner", "Founder", "President", "CEO", "Managing Partner",
                    "Director of Business Development", "VP Business Development",
                ],
                "freight-audit": [
                    "Owner", "Founder", "President", "CEO", "Managing Partner",
                    "Director of Business Development", "VP Business Development",
                ],
                "osha-compliance": [
                    "Owner", "Founder", "President", "CEO", "Principal",
                    "Director of Safety Consulting", "VP Business Development",
                ],
                "fire-protection": [
                    "Owner", "Founder", "President", "CEO", "General Manager",
                    "Operations Manager", "Branch Manager",
                ],
                "msps": [
                    "Owner", "Founder", "CEO", "President", "CTO",
                    "Director of Operations", "VP Operations",
                ],
                "fintech": [
                    "CEO", "CTO", "CISO", "VP Engineering", "Head of Security",
                    "Director of Compliance",
                ],
                "ma-advisory": [
                    "Managing Partner", "Partner", "Principal", "Founder",
                    "Managing Director", "Senior Advisor",
                ],
                # ClientA sub-niches — targets BUILDING OPERATORS/OWNERS
                # who need fire inspection services
                "assisted-living": [
                    "Administrator", "Executive Director", "Director of Operations",
                    "Facilities Director", "Director of Maintenance",
                    "Regional Director", "Owner", "President", "CEO",
                ],
                "warehouses": [
                    "Facilities Director", "Facilities Manager", "Operations Director",
                    "General Manager", "Plant Manager", "Warehouse Manager",
                    "Owner", "President", "CEO",
                ],
                "manufacturing": [
                    "Plant Manager", "Operations Manager", "Facilities Manager",
                    "Safety Director", "EHS Director", "Owner", "President", "CEO",
                ],
                "hotels": [
                    "General Manager", "Director of Operations",
                    "Chief Engineer", "Facilities Director",
                    "Owner", "President", "Regional Manager",
                ],
                "schools": [
                    "Business Manager", "Director of Facilities",
                    "Facilities Director", "Director of Operations",
                    "Head of School", "Principal", "Superintendent",
                ],
                "daycares": [
                    "Owner", "Director", "Executive Director",
                    "President", "Regional Director",
                ],
            }

            # Canonicalize niche slug to match NICHE_TITLES keys
            _niche_slug = re.sub(r"[^a-z0-9]+", "-", (niche or "").lower()).strip("-") if niche else ""
            titles_for_niche = NICHE_TITLES.get(_niche_slug, _GENERIC_TITLES)

            people = find_people(domain, AIARK_KEY, titles_for_niche, size=2)
            for person in (people or [])[:2]:
                em = export_email(
                    person.get("aiark_id", ""),
                    AIARK_KEY,
                    linkedin_url=person.get("linkedin", ""),
                )
                if em.get("email") and em["email"].lower() not in known_emails:
                    # Trust BounceBan "valid" status; otherwise double-check with MV.
                    bb_status = em.get("bb_status", "")
                    if bb_status == "valid" or verify_mv(em["email"]):
                        fn_p = (person.get("first_name") or "").strip()
                        ln_p = (person.get("last_name") or "").strip()
                        title_p = person.get("title", "")
                        update_domain_memory(domain, em["email"], True)
                        safe_stat_inc(stats, 'aiark_people_hit')
                        return {'email': em["email"], 'first_name': fn_p, 'last_name': ln_p,
                                'title': title_p, 'company': company, 'domain': domain,
                                'city': city, 'state': state, 'phone': phone, 'source': 'aiark_people'}
        except Exception:
            pass

    # Step 2d: Website scraping for emails (FREE — checks actual contact pages).
    # ROLE-ACCOUNT SOURCE: emails scraped from contact/about pages are usually
    # info@/contact@ with no name. Skip when allow_nameless=False — the resulting
    # lead would have empty first_name, breaking {{first_name}} merge.
    if allow_nameless:
        EMAIL_PAT = re.compile(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}')
        SKIP_EMAIL_DOMAINS = {'sentry.io', 'wordpress.com', 'wixpress.com', 'googleapis.com',
                              'w3.org', 'schema.org', 'google.com', 'example.com', 'gravatar.com'}
        for path in ['', '/contact', '/about']:
            try:
                wr = requests.get(f'https://{domain}{path}',
                                 headers={'User-Agent': 'Mozilla/5.0'}, timeout=4, allow_redirects=True)
                if wr.status_code == 200:
                    found_emails = EMAIL_PAT.findall(wr.text)
                    for fe in found_emails:
                        fe = fe.lower()
                        if (not any(s in fe for s in SKIP_EMAIL_DOMAINS)
                            and fe not in known_emails
                            and domain.split('.')[0] in fe.split('@')[1]):
                            if verify_mv(fe):
                                update_domain_memory(domain, fe, True)
                                safe_stat_inc(stats, 'website_scrape_hit')
                                return {'email': fe, 'first_name': '', 'last_name': '',
                                        'title': '', 'company': company, 'domain': domain,
                                        'city': city, 'state': state, 'phone': phone, 'source': 'website_scrape'}
            except Exception:
                pass

    # Step 3: Owner search + smart pattern based on MX type
    owner = find_owner_google(company, city, state, domain, haiku_client) if not (known_pattern and score > 0.5) else owner
    fn = ''
    ln = ''
    title = ''
    if owner:
        fn = (owner.get('first_name') or '').lower().strip()
        ln = (owner.get('last_name') or '').lower().strip()
        title = owner.get('title', '') or ''
        if title == 'null': title = ''
        safe_stat_inc(stats, 'owner_found')

    # Build patterns based on MX type + Blitz format inference
    patterns = []
    if fn:
        # If Blitz told us the email format at this domain, use it first
        if blitz_format == 'first.last' and ln:
            patterns = [f'{fn}.{ln}@{domain}', f'{fn}@{domain}']
        elif blitz_format == 'firstlast' and ln:
            patterns = [f'{fn}{ln}@{domain}', f'{fn}.{ln}@{domain}', f'{fn}@{domain}']
        elif blitz_format == 'first':
            patterns = [f'{fn}@{domain}']
            if ln:
                patterns.append(f'{fn}.{ln}@{domain}')
        # Otherwise use MX type to guess
        elif mx_type == 'google' and ln:
            patterns = [f'{fn}.{ln}@{domain}', f'{fn}@{domain}', f'{fn}{ln}@{domain}']
        elif mx_type == 'microsoft':
            patterns = [f'{fn}@{domain}', f'{fn}.{ln}@{domain}' if ln else f'{fn}@{domain}']
        else:
            patterns = [f'{fn}@{domain}']
            if ln:
                patterns.extend([f'{fn}.{ln}@{domain}', f'{fn}{ln}@{domain}', f'{fn[0]}{ln}@{domain}'])
    patterns.extend([f'info@{domain}', f'contact@{domain}'])

    # Try patterns
    for email in patterns:
        if email.lower() in known_emails:
            continue
        if verify_mv(email):
            update_domain_memory(domain, email, True)
            safe_stat_inc(stats, 'pattern_hit')
            return {'email': email, 'first_name': fn.title() if fn else '', 'last_name': ln.title() if ln else '',
                    'title': title, 'company': company, 'domain': domain,
                    'city': city, 'state': state, 'phone': phone, 'source': 'pattern'}
        time.sleep(0.3)

    # Step 4: Icypeas name+domain (if we have owner name)
    if fn and ln and ICYPEAS_KEY:
        icy_email = icypeas_find_by_name_domain(fn.title(), ln.title(), domain)
        if icy_email and icy_email.lower() not in known_emails:
            if verify_mv(icy_email):
                update_domain_memory(domain, icy_email, True)
                safe_stat_inc(stats, 'icypeas_name_hit')
                return {'email': icy_email, 'first_name': fn.title(), 'last_name': ln.title(),
                        'title': title, 'company': company, 'domain': domain,
                        'city': city, 'state': state, 'phone': phone, 'source': 'icypeas_name'}

    # Step 4b: Icypeas reverse email lookup (different DB from Blitz)
    # If we found a generic email but Blitz couldn't identify who it belongs to,
    # try Icypeas reverse lookup — they have different data sources
    if ICYPEAS_KEY and domain:
        for generic in [f'info@{domain}', f'contact@{domain}']:
            try:
                re_r = requests.post('https://app.icypeas.com/api/reverse-email-lookup',
                                    headers={'Authorization': ICYPEAS_KEY, 'Content-Type': 'application/json'},
                                    json={'email': generic}, timeout=20)
                if re_r.status_code == 200:
                    rd = re_r.json()
                    if rd.get('success') and rd.get('item', {}).get('_id'):
                        # Poll for results
                        sid = rd['item']['_id']
                        for _ in range(8):
                            time.sleep(2)
                            pr = requests.post('https://app.icypeas.com/api/bulk-single-searchs/read',
                                             headers={'Authorization': ICYPEAS_KEY, 'Content-Type': 'application/json'},
                                             json={'id': sid}, timeout=15)
                            if pr.status_code == 200:
                                pd = pr.json()
                                if pd.get('status') == 'FINISHED' or pd.get('items'):
                                    items = pd.get('items', [])
                                    if items:
                                        res = items[0].get('results', {})
                                        found_name = f"{res.get('firstname','')} {res.get('lastname','')}".strip()
                                        if found_name and len(found_name) > 2:
                                            fn = res.get('firstname', '').lower()
                                            ln = res.get('lastname', '').lower()
                                            personal = f'{fn}@{domain}' if fn else None
                                            if personal and personal.lower() not in known_emails and verify_mv(personal):
                                                update_domain_memory(domain, personal, True)
                                                safe_stat_inc(stats, 'icypeas_reverse_hit')
                                                return {'email': personal, 'first_name': fn.title(), 'last_name': ln.title(),
                                                        'title': '', 'company': company, 'domain': domain,
                                                        'city': city, 'state': state, 'phone': phone, 'source': 'icypeas_reverse'}
                                    break
            except Exception:
                pass

    # Step 5: Icypeas domain-only search.
    # ROLE-ACCOUNT PRONE: returns any email at the domain (often sales@, admin@)
    # and attaches the fn/ln from earlier steps only if those were found.
    # When allow_nameless=False, only return if fn is populated (avoids returning
    # a role-account email with empty first_name).
    if ICYPEAS_KEY and (allow_nameless or fn):
        icy_emails = icypeas_domain_search(domain)
        for icy_email in icy_emails[:2]:
            if icy_email.lower() not in known_emails:
                if verify_mv(icy_email):
                    # If strict mode and fn is missing, skip this email
                    if not allow_nameless and not fn:
                        continue
                    update_domain_memory(domain, icy_email, True)
                    safe_stat_inc(stats, 'icypeas_domain_hit')
                    return {'email': icy_email, 'first_name': fn.title() if fn else '', 'last_name': ln.title() if ln else '',
                            'title': title, 'company': company, 'domain': domain,
                            'city': city, 'state': state, 'phone': phone, 'source': 'icypeas_domain'}

    # Step 6: Catch-all acceptance
    if fn and check_catch_all(domain):
        email = f'{fn}@{domain}'
        safe_stat_inc(stats, 'catch_all_accepted')
        return {'email': email, 'first_name': fn.title(), 'last_name': ln.title() if ln else '',
                'title': title, 'company': company, 'domain': domain,
                'city': city, 'state': state, 'phone': phone, 'source': 'catch_all'}

    # All methods exhausted
    update_domain_memory(domain, f'failed@{domain}', False)
    safe_stat_inc(stats, 'exhausted')
    return None


# ============================================================================
# CLI INTERFACE
# ============================================================================

def main():
    ap = argparse.ArgumentParser(description="The Forge unified enrichment engine")
    ap.add_argument("--input", required=True)
    ap.add_argument("--niche", default="")
    ap.add_argument("--client", default="client_c")
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--output-dir", help="override output directory")
    ap.add_argument("--workers", type=int, default=5, help="parallel workers (default 5, set 1 for sequential)")
    ap.add_argument("--force", action="store_true", help="override overlap safeguard (>80%% already in DB)")
    ap.add_argument("--allow-nameless", action="store_true",
                    help="Allow role-account fallback sources (Google Maps email, website scraping, "
                         "Icypeas domain-only without a found name). DEFAULT IS OFF — most B2B "
                         "campaigns need {{first_name}} merge, so role-accounts are skipped. "
                         "Matches forge.py's --allow-nameless polarity. Set for privacy-heavy "
                         "niches (cannabis, healthcare) where generic emails are acceptable.")
    args = ap.parse_args()

    # Route light classification/extraction through llm_router.
    # Kimi K2.6 handles these tasks (owner extraction, niche-fit) at 8x cheaper
    # than Haiku. Falls back to Haiku if KIMI_API_KEY not set.
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

    # Normalize domain column: accept either `domain` or `website` as source
    # (common for CSVs exported from Apollo, Clay, Blitz, etc.)
    if companies:
        first_row = companies[0]
        has_domain = 'domain' in first_row
        has_website = 'website' in first_row
        if not has_domain and has_website:
            print(f"  ℹ️  Input uses 'website' column instead of 'domain'. Auto-mapping...")
            for co in companies:
                w = (co.get('website', '') or '').strip()
                # Extract domain from URL if needed
                w = w.replace('https://', '').replace('http://', '').replace('www.', '').split('/')[0].lower()
                co['domain'] = w

    # Dedup against master DB
    from dedup_before_enrich import get_known_domains, get_known_emails
    known_domains = get_known_domains()
    known_emails = get_known_emails()

    # Filter to companies with domains not already enriched
    to_enrich = []
    already_known = 0
    missing_domain = 0
    for co in companies:
        domain = (co.get('domain', '') or '').strip().lower()
        if not domain:
            missing_domain += 1
            continue
        if domain in known_domains:
            already_known += 1
        else:
            to_enrich.append(co)

    # Validate domains found — exit early if none
    valid_domain_count = len(companies) - missing_domain
    if not companies or valid_domain_count == 0:
        print(f"\n  ⚠ No valid domains found in input CSV. Check that the 'website' or 'domain' column has values.")
        if companies:
            print(f"     Rows in CSV: {len(companies)}, all empty/blank domains.")
            print(f"     Columns found: {list(companies[0].keys())}")
        sys.exit(1)

    # Report domain count + skipped empties
    if missing_domain > 0:
        print(f"  Found {valid_domain_count} domains in input CSV ({missing_domain} empty rows skipped)")
    else:
        print(f"  Found {valid_domain_count} domains in input CSV")

    # Warn loudly if majority of input has no domain (data quality issue, not a dedup issue)
    if missing_domain / len(companies) > 0.5:
        print(f"\n  ⚠️  WARNING: {missing_domain}/{len(companies)} rows ({missing_domain/len(companies)*100:.0f}%) have no domain/website.")
        print(f"     Check your input CSV — expected columns: 'domain' or 'website'")
        print(f"     Columns found: {list(companies[0].keys())}")

    # OVERLAP SAFEGUARD — abort if >80% of input is already in DB
    # Prevents wasting hours re-enriching already-scraped niches
    overlap_pct = (already_known / len(companies) * 100) if companies else 0
    if overlap_pct > 80 and not args.dry_run:
        print(f"\n  ⛔ OVERLAP SAFEGUARD: {overlap_pct:.0f}% of input ({already_known}/{len(companies)}) already in DB.")
        print(f"  Only {len(to_enrich)} new companies — not worth a full run.")
        print(f"  This niche has already been enriched. Use --force to override.")
        if not getattr(args, 'force', False):
            return

    print(f"{'='*60}")
    print(f"  THE FORGE — UNIFIED ENRICHMENT")
    print(f"{'='*60}")
    print(f"  input:          {len(companies)} companies")
    print(f"  already in DB:  {already_known} ({overlap_pct:.0f}% overlap)")
    print(f"  after dedup:    {len(to_enrich)} new")
    print(f"  niche:          {args.niche}")
    print(f"  client:         {args.client}")

    # Cost estimate
    COST_LIMIT = float(os.environ.get("FORGE_COST_LIMIT", "10.00"))
    est = len(to_enrich) * 0.005  # ~$0.005 per company average
    print(f"  cost estimate:  ~${est:.2f}")

    if est > COST_LIMIT:
        print(f"\n  ⚠️ COST FLAG: ${est:.2f} exceeds ${COST_LIMIT:.2f} limit")
        print(f"  Use --limit {int(COST_LIMIT / 0.005)} or set FORGE_COST_LIMIT={int(est+1)}")
        return

    if args.dry_run:
        # Batch MX check for routing preview
        domains = [(co.get('domain', '').strip().lower()) for co in to_enrich if co.get('domain')]
        print(f"\n  [DRY RUN] MX checking {len(domains)} domains...")
        from enrich_smart_route import batch_mx_check_simple
        mx_results = batch_mx_check_simple(domains[:50])  # sample 50
        from collections import Counter
        mx_dist = Counter(mx_results.values())
        print(f"  MX distribution (sample 50):")
        for mx_type, count in mx_dist.most_common():
            print(f"    {mx_type:<15} {count}")
        print(f"\n  [DRY RUN] Would enrich {len(to_enrich)} companies. Exiting.")
        return

    # Batch MX check
    print(f"\n  [1/2] MX pre-check...", flush=True)
    domains_list = [co.get('domain', '').strip().lower() for co in to_enrich if co.get('domain')]
    from enrich_smart_route import batch_mx_check_simple
    mx_results = batch_mx_check_simple(domains_list)
    from collections import Counter
    mx_dist = Counter(mx_results.values())
    print(f"  MX: {dict(mx_dist)}")

    # Enrich — parallel processing (5 companies at a time)
    WORKERS = args.workers or int(os.environ.get("FORGE_WORKERS", "5"))
    print(f"\n  [2/2] Enriching {len(to_enrich)} companies ({WORKERS} parallel workers)...\n", flush=True)
    stats = {}
    results = []
    results_lock = threading.Lock()
    completed = [0]  # mutable counter for progress

    def _enrich_one(co):
        domain = (co.get('domain', '') or '').strip().lower()
        company = co.get('company', '')
        city = co.get('city', '')
        state = co.get('state', '')
        phone = co.get('phone', '')
        mx_type = mx_results.get(domain, 'unknown')

        result = enrich_company(company, domain, city, state, phone, mx_type, haiku, known_emails, stats,
                                 allow_nameless=args.allow_nameless,
                                 niche=args.niche)
        if result:
            with results_lock:
                results.append(result)
            safe_add_email(known_emails, result['email'])

        with results_lock:
            completed[0] += 1
            c = completed[0]
        if c % 20 == 0:
            with _stats_lock:
                s = dict(stats)
            print(f"  [{c}/{len(to_enrich)}] verified={len(results)} "
                  f"(memory={s.get('domain_memory_hit',0)} pattern={s.get('pattern_hit',0)} "
                  f"icypeas={s.get('icypeas_name_hit',0)+s.get('icypeas_domain_hit',0)} "
                  f"catch_all={s.get('catch_all_accepted',0)} skip={s.get('skip_no_mx',0)})", flush=True)

    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        futures = [pool.submit(_enrich_one, co) for co in to_enrich]
        for f in as_completed(futures):
            try:
                f.result()  # raise any exceptions
            except Exception as e:
                print(f"  ⚠ worker error: {e}", flush=True)

    # Save pre-niche-fit results (so we can re-run niche-fit without re-enriching)
    if results:
        niche_slug_pre = re.sub(r'[^a-z0-9]+', '-', args.niche.lower()).strip('-')[:30]
        pre_niche_dir = os.path.join(ROOT_DIR, 'pre-niche-fit-cache')
        os.makedirs(pre_niche_dir, exist_ok=True)
        pre_niche_path = os.path.join(pre_niche_dir, f'{niche_slug_pre}_{args.client}_{datetime.now().strftime("%Y%m%d_%H%M")}.json')
        with open(pre_niche_path, 'w') as pf:
            json.dump(results, pf, indent=2)
        print(f"\n  pre-niche-fit cache: {pre_niche_path} ({len(results)} leads)", flush=True)

    # LLM niche-fit check
    if args.niche and results:
        print(f"\n  LLM niche-fit check on {len(results)} leads...", flush=True)
        confirmed = []
        for r in results:
            # Build company identifier — use company name, fall back to domain
            company_id = r.get("company", "").strip()
            domain = r.get("domain", "")
            if not company_id and domain:
                company_id = domain  # use domain as identifier when company name is empty
            if not company_id:
                confirmed.append(r)  # no info to check — keep it
                continue
            try:
                resp = haiku.messages.create(
                    model=_haiku_model, max_tokens=30,
                    messages=[{'role': 'user', 'content': f'This company was found in a {args.niche} industry directory. Company/domain: "{company_id}". Could this plausibly be a {args.niche} company? Only say "no" if it is CLEARLY a different industry. Reply ONLY "yes" or "no".'}])
                if 'yes' in resp.content[0].text.strip().lower():
                    confirmed.append(r)
            except Exception:
                confirmed.append(r)
        rejected = len(results) - len(confirmed)
        print(f"  niche-fit: {len(confirmed)} confirmed, {rejected} rejected", flush=True)
        results = confirmed

    # Export
    niche_slug = re.sub(r'[^a-z0-9]+', '-', args.niche.lower()).strip('-')[:30]
    if args.output_dir:
        outdir = args.output_dir
    else:
        outdir = os.path.join(ROOT_DIR, '..', '..', '01-Projects', args.client,
                              'lead-runs', f'{niche_slug}-forge-{datetime.now().strftime("%Y%m%d")}')
    os.makedirs(outdir, exist_ok=True)

    FIELDS = ['email', 'first_name', 'last_name', 'company_name', 'phone', 'title',
              'website', 'custom1', 'custom2', 'custom3']
    with open(f'{outdir}/smartlead_import.csv', 'w', newline='') as f:
        w = csv.DictWriter(f, fieldnames=FIELDS)
        w.writeheader()
        for r in results:
            email_type = 'personal' if r['email'] and 'info@' not in r['email'] else 'generic'
            w.writerow({
                'email': r['email'], 'first_name': r.get('first_name', ''),
                'last_name': r.get('last_name', ''), 'company_name': r['company'],
                'phone': r.get('phone', ''), 'title': r.get('title', ''),
                'website': r['domain'], 'custom1': email_type,
                'custom2': r.get('source', ''), 'custom3': f"{r.get('city','')} {r.get('state','')}".strip(),
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
                    (r['email'], r.get('first_name', ''), r.get('last_name', ''), r['company'],
                     r.get('phone', ''), r.get('title', ''), r['domain'],
                     r.get('city', ''), r.get('state', ''),
                     f'forge_{r.get("source","unknown")}', niche_slug, args.client))
        added += 1
    conn.commit()
    conn.close()

    # Report
    print(f"\n{'='*60}")
    print(f"  FORGE ENRICHMENT RESULTS")
    print(f"{'='*60}")
    print(f"  companies processed:   {len(to_enrich)}")
    print(f"  verified leads:        {len(results)}")
    print(f"  added to DB:           {added}")
    print(f"")
    print(f"  ENRICHMENT SOURCES:")
    print(f"    blitz phone lookup:  {stats.get('phone_lookup_hit', 0)}")
    print(f"    blitz email reverse: {stats.get('email_reverse_hit', 0)}")
    print(f"    google maps email:   {stats.get('maps_email_hit', 0)}")
    print(f"    blitz direct:        {stats.get('blitz_direct', 0)}")
    print(f"    domain memory:       {stats.get('domain_memory_hit', 0)}")
    print(f"    pattern (MX-routed): {stats.get('pattern_hit', 0)}")
    print(f"    icypeas reverse:     {stats.get('icypeas_reverse_hit', 0)}")
    print(f"    icypeas (name):      {stats.get('icypeas_name_hit', 0)}")
    print(f"    icypeas (domain):    {stats.get('icypeas_domain_hit', 0)}")
    print(f"    catch-all accepted:  {stats.get('catch_all_accepted', 0)}")
    print(f"    skipped (no MX):     {stats.get('skip_no_mx', 0)}")
    print(f"    exhausted:           {stats.get('exhausted', 0)}")
    print(f"    owners found:        {stats.get('owner_found', 0)}")
    print(f"    website scrape:      {stats.get('website_scrape_hit', 0)}")
    print(f"  output:                {outdir}")
    print(f"{'='*60}")

    # Log enrichment analytics for future optimization
    try:
        aconn = sqlite3.connect(DB_PATH)
        acur = aconn.cursor()
        acur.execute("""INSERT INTO enrichment_analytics
            (niche, client, companies_processed, leads_produced,
             source_mx_skip, source_domain_memory, source_google_maps, source_blitz_direct,
             source_website_scrape, source_pattern, source_icypeas_name, source_icypeas_domain,
             source_catch_all, source_exhausted, owners_found, total_cost_estimate)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (args.niche, args.client, len(to_enrich), len(results),
             stats.get('skip_no_mx', 0), stats.get('domain_memory_hit', 0),
             stats.get('maps_email_hit', 0), stats.get('blitz_direct', 0),
             stats.get('website_scrape_hit', 0), stats.get('pattern_hit', 0),
             stats.get('icypeas_name_hit', 0), stats.get('icypeas_domain_hit', 0),
             stats.get('catch_all_accepted', 0), stats.get('exhausted', 0),
             stats.get('owner_found', 0), est))
        aconn.commit()
        aconn.close()
        print(f"\n  analytics logged to enrichment_analytics table")
    except Exception:
        pass


if __name__ == "__main__":
    main()
