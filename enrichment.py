"""
Tiered contact enrichment.
Website scraping, SERP contacts, Icypeas, Hunter, Apify LinkedIn, staff page scraping,
email pattern inference, and email propagation.
"""

import re
import time
import hashlib
import requests
import threading
from datetime import datetime, timezone
from urllib.parse import urljoin
from concurrent.futures import ThreadPoolExecutor, as_completed

from config import (
    SERPER_API_KEY, SERPER_WEB_ENDPOINT,
    ICYPEAS_API_KEY, ICYPEAS_EMAIL_ENDPOINT, ICYPEAS_DOMAIN_ENDPOINT,
    HUNTER_API_KEY, HUNTER_ENDPOINT, HUNTER_DELAY,
    MILLIONVERIFIER_API_KEY,
    EMAIL_SCRAPE_TIMEOUT, RATE_LIMIT_DELAY,
    ICYPEAS_POLL_INTERVAL, ICYPEAS_POLL_MAX_WAIT,
    WATERFALL_WORKERS, ALL_PATTERNS,
    EMAIL_REGEX, CONTACT_PATHS,
    COMMON_FIRST_NAMES, BUSINESS_NAME_WORDS,
)
from cache import cache_key, save_cache
from utils import (
    get_domain, classify_email, is_junk_email,
    retry_with_backoff, is_valid_person_name,
)


# ============================================================
# TIER 1: WEBSITE EMAIL SCRAPING
# ============================================================

def scrape_website_emails(website_url):
    """Scrape emails from a website. Tries homepage first; only hits contact/about pages if homepage finds zero."""
    if not website_url:
        return []

    url = website_url.strip()
    if not url.startswith("http"):
        url = "http://" + url

    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
        "Accept": "text/html",
    }

    homepage_emails = set()
    homepage_html = ""

    try:
        resp = requests.get(url, headers=headers, timeout=EMAIL_SCRAPE_TIMEOUT, allow_redirects=True)
        resp.raise_for_status()
        homepage_html = resp.text
        homepage_emails.update(EMAIL_REGEX.findall(homepage_html))
    except Exception:
        pass

    cleaned_homepage = [e for e in homepage_emails if not is_junk_email(e)]

    if cleaned_homepage:
        seen = set()
        unique = []
        for email in cleaned_homepage:
            if email.lower() not in seen:
                seen.add(email.lower())
                unique.append(email)
        return unique

    all_emails = set()
    try:
        base_domain = get_domain(url)
        link_pattern = re.compile(r'href=["\']([^"\']+)["\']', re.IGNORECASE)
        links = link_pattern.findall(homepage_html)

        contact_urls = set()
        for link in links:
            link_lower = link.lower()
            if any(p in link_lower for p in ["contact", "about", "team", "staff", "leadership"]):
                full_url = urljoin(url, link)
                if base_domain in full_url.lower():
                    contact_urls.add(full_url)

        for path in CONTACT_PATHS:
            contact_urls.add(url.rstrip("/") + path)

        for contact_url in list(contact_urls)[:4]:
            try:
                resp2 = requests.get(contact_url, headers=headers,
                                     timeout=EMAIL_SCRAPE_TIMEOUT, allow_redirects=True)
                if resp2.status_code == 200:
                    all_emails.update(EMAIL_REGEX.findall(resp2.text))
            except Exception:
                pass
    except Exception:
        pass

    cleaned = [e for e in all_emails if not is_junk_email(e)]
    seen = set()
    unique = []
    for email in cleaned:
        if email.lower() not in seen:
            seen.add(email.lower())
            unique.append(email)

    return unique


# Helper alias for config.PipelineContext.infer_pattern
def classify_email_type(email):
    """Alias for classify_email, used by PipelineContext."""
    return classify_email(email)


# ============================================================
# TIER 1b: GOOGLE SERP CONTACT SEARCH
# ============================================================

SERP_NAME_PATTERN = re.compile(
    r'\b([A-Z][a-z]{1,15}\s+(?:[A-Z]\.?\s+)?[A-Z][a-z]{1,20})\b'
)

SERP_TITLE_PATTERNS = [
    re.compile(r'\b(owner|founder|co-founder|ceo|president|principal|managing\s+partner|'
               r'general\s+manager|director|managing\s+director)\b', re.IGNORECASE),
]

SERP_NAME_BLACKLIST = {
    "better business", "best western", "best buy", "home depot",
    "real estate", "united states", "new york", "los angeles",
    "san francisco", "san diego", "san antonio", "las vegas",
    "salt lake", "grand junction", "fort collins", "kansas city",
    "santa monica", "santa cruz", "palm beach", "west palm",
    "south florida", "north carolina", "south carolina",
    "fire protection", "property management", "customer service",
    "general contractor", "read more", "learn more", "click here",
    "privacy policy", "terms conditions", "all rights",
}


def _is_valid_serp_name(name, company_name=""):
    """Validate that a SERP-extracted name is actually a person's name."""
    name_lower = name.lower().strip()
    parts = name.split()

    if len(parts) < 2 or len(parts) > 3:
        return False
    if any(len(p) < 2 or len(p) > 15 for p in parts):
        return False
    if any(c.isdigit() for c in name):
        return False
    if name_lower in SERP_NAME_BLACKLIST:
        return False
    if any(bl in name_lower for bl in SERP_NAME_BLACKLIST):
        return False
    if any(w in name_lower for w in ["llc", "inc", "corp", "ltd", "www", "http", ".com", "&"]):
        return False

    name_words = [w.lower() for w in parts]
    for word in name_words:
        if word in BUSINESS_NAME_WORDS:
            return False

    if company_name:
        company_words = {w.lower() for w in company_name.split() if len(w) > 2}
        overlap = sum(1 for w in name_words if w in company_words)
        if overlap > 0 and overlap >= len(name_words) * 0.5:
            return False

    first_word = parts[0].lower()
    if first_word in COMMON_FIRST_NAMES:
        last_word = parts[-1]
        if last_word.isalpha() and last_word[0].isupper() and len(last_word) >= 2:
            return True

    return False


def _is_relevant_serp_result(text, company_name, domain):
    text_lower = text.lower()
    company_words = [w.lower() for w in company_name.split() if len(w) > 3]
    if company_words and any(w in text_lower for w in company_words[:3]):
        return True
    if domain and domain.split(".")[0] in text_lower:
        return True
    return False


def _parse_linkedin_serp(title_text, snippet, company_name):
    title_clean = title_text.replace(" | LinkedIn", "").replace(" - LinkedIn", "").strip()
    parts = re.split(r'\s*[-\u2013|]\s*', title_clean)

    if len(parts) >= 2:
        potential_name = parts[0].strip()
        potential_title = parts[1].strip() if len(parts) > 1 else ""

        if _is_valid_serp_name(potential_name, company_name):
            combined = f"{title_text} {snippet}".lower()
            company_words = [w.lower() for w in company_name.split() if len(w) > 3]
            if any(w in combined for w in company_words[:3]):
                return {
                    "name": potential_name,
                    "title": potential_title,
                    "source": "serp_linkedin",
                }
    return None


def _title_matches_roles(title, target_roles):
    if not target_roles or not title:
        return True
    title_lower = title.lower()
    for role in target_roles:
        role_lower = role.lower()
        role_words = role_lower.split()
        if all(w in title_lower for w in role_words):
            return True
        if role_lower in title_lower or title_lower in role_lower:
            return True
    return False


def serp_search_contacts(company_name, domain, target_roles, cache, cache_file):
    """Search Google via Serper to find owner/CEO names for a company."""
    key = cache_key("serp_contact", domain)
    if key in cache:
        return cache[key].get("results", [])

    if not SERPER_API_KEY:
        return []

    queries = [
        f'"{company_name}" owner OR founder OR CEO',
        f'site:linkedin.com "{company_name}" owner OR founder OR CEO',
    ]

    headers = {
        "X-API-KEY": SERPER_API_KEY,
        "Content-Type": "application/json",
    }

    found_contacts = []

    for query in queries:
        try:
            payload = {"q": query, "num": 10, "gl": "us", "hl": "en"}

            def _web_search(p=payload):
                r = requests.post(SERPER_WEB_ENDPOINT, headers=headers, json=p, timeout=15)
                r.raise_for_status()
                return r.json()

            data = retry_with_backoff(_web_search, retries=1)
            time.sleep(RATE_LIMIT_DELAY)

            organic = data.get("organic", [])
            knowledge_graph = data.get("knowledgeGraph", {})

            if knowledge_graph:
                kg_attrs = knowledge_graph.get("attributes", {})
                for attr_key, attr_val in kg_attrs.items():
                    if any(t in attr_key.lower() for t in ["owner", "founder", "ceo", "president"]):
                        names = SERP_NAME_PATTERN.findall(str(attr_val))
                        for name in names:
                            if _is_valid_serp_name(name, company_name):
                                found_contacts.append({
                                    "name": name.strip(),
                                    "title": attr_key.strip(),
                                    "source": "serp_knowledge_graph",
                                })

            for result in organic[:8]:
                snippet = result.get("snippet", "")
                title_text = result.get("title", "")
                link = result.get("link", "")
                combined_text = f"{title_text} {snippet}"

                if not _is_relevant_serp_result(combined_text, company_name, domain):
                    continue

                for title_pat in SERP_TITLE_PATTERNS:
                    title_matches = title_pat.finditer(combined_text)
                    for tmatch in title_matches:
                        title_found = tmatch.group(0).strip()
                        start = max(0, tmatch.start() - 80)
                        end = min(len(combined_text), tmatch.end() + 80)
                        nearby_text = combined_text[start:end]

                        names = SERP_NAME_PATTERN.findall(nearby_text)
                        for name in names:
                            if _is_valid_serp_name(name, company_name):
                                found_contacts.append({
                                    "name": name.strip(),
                                    "title": title_found.title(),
                                    "source": "serp_organic",
                                })

                if "linkedin.com" in link:
                    linkedin_contact = _parse_linkedin_serp(title_text, snippet, company_name)
                    if linkedin_contact:
                        found_contacts.append(linkedin_contact)

        except Exception:
            continue

    seen_names = set()
    unique = []
    for c in found_contacts:
        name_lower = c["name"].lower()
        if name_lower not in seen_names:
            seen_names.add(name_lower)
            if target_roles and not _title_matches_roles(c.get("title", ""), target_roles):
                continue
            unique.append({
                "name": c["name"],
                "title": c.get("title", ""),
                "email": "",
                "type": "personal",
                "source": c.get("source", "serp_web"),
                "verified": False,
            })

    cache[key] = {"timestamp": datetime.now(timezone.utc).isoformat(), "results": unique[:5]}
    save_cache(cache, cache_file)
    return unique[:5]


# ============================================================
# TIER 2: ICYPEAS
# ============================================================

def _icypeas_headers():
    return {
        "Content-Type": "application/json",
        "Authorization": ICYPEAS_API_KEY,
    }


def _icypeas_poll_result(search_id):
    headers = _icypeas_headers()
    elapsed = 0
    while elapsed < ICYPEAS_POLL_MAX_WAIT:
        time.sleep(ICYPEAS_POLL_INTERVAL)
        elapsed += ICYPEAS_POLL_INTERVAL
        try:
            r = requests.post(
                "https://app.icypeas.com/api/bulk-single-searchs/read",
                headers=headers,
                json={"id": search_id},
                timeout=15,
            )
            if r.status_code != 200:
                continue
            data = r.json()
            if not data.get("success"):
                continue
            items = data.get("items", [])
            if not items:
                continue
            item = items[0]
            status = item.get("status", "")
            if status in ("NONE", "SCHEDULED", "IN_PROGRESS"):
                continue
            return item
        except Exception:
            continue
    return None


def icypeas_find_email(first_name, last_name, domain, cache, cache_file):
    if not ICYPEAS_API_KEY:
        return ""

    key = cache_key("icypeas_email", first_name, last_name, domain)
    if key in cache:
        return cache[key].get("email", "")

    try:
        headers = _icypeas_headers()
        payload = {
            "firstname": first_name,
            "lastname": last_name,
            "domainOrCompany": domain,
        }

        def _submit():
            r = requests.post(ICYPEAS_EMAIL_ENDPOINT, headers=headers, json=payload, timeout=15)
            r.raise_for_status()
            return r.json()

        submit_data = retry_with_backoff(_submit, retries=2, base_delay=2.0)

        if not submit_data.get("success"):
            cache[key] = {"timestamp": datetime.now(timezone.utc).isoformat(), "email": ""}
            save_cache(cache, cache_file)
            return ""

        search_id = submit_data.get("item", {}).get("_id", "")
        if not search_id:
            cache[key] = {"timestamp": datetime.now(timezone.utc).isoformat(), "email": ""}
            save_cache(cache, cache_file)
            return ""

        result = _icypeas_poll_result(search_id)
        if not result:
            cache[key] = {"timestamp": datetime.now(timezone.utc).isoformat(), "email": ""}
            save_cache(cache, cache_file)
            return ""

        email = ""
        results_data = result.get("results", {})
        emails_list = results_data.get("emails", [])
        if emails_list:
            best = emails_list[0]
            email = best.get("email", "")

        cache[key] = {"timestamp": datetime.now(timezone.utc).isoformat(), "email": email}
        save_cache(cache, cache_file)
        return email

    except Exception as e:
        if "429" not in str(e):
            cache[key] = {"timestamp": datetime.now(timezone.utc).isoformat(), "email": ""}
            save_cache(cache, cache_file)
        return ""


def icypeas_domain_search(domain, cache, cache_file):
    if not ICYPEAS_API_KEY:
        return []

    key = cache_key("icypeas_domain", domain)
    if key in cache:
        return cache[key].get("results", [])

    try:
        headers = _icypeas_headers()
        payload = {"domainOrCompany": domain}

        def _submit():
            r = requests.post(ICYPEAS_DOMAIN_ENDPOINT, headers=headers, json=payload, timeout=15)
            r.raise_for_status()
            return r.json()

        submit_data = retry_with_backoff(_submit, retries=2, base_delay=2.0)

        if not submit_data.get("success"):
            cache[key] = {"timestamp": datetime.now(timezone.utc).isoformat(), "results": []}
            save_cache(cache, cache_file)
            return []

        search_id = submit_data.get("item", {}).get("_id", "")
        if not search_id:
            cache[key] = {"timestamp": datetime.now(timezone.utc).isoformat(), "results": []}
            save_cache(cache, cache_file)
            return []

        result = _icypeas_poll_result(search_id)
        if not result:
            cache[key] = {"timestamp": datetime.now(timezone.utc).isoformat(), "results": []}
            save_cache(cache, cache_file)
            return []

        contacts = []
        results_data = result.get("results", {})
        emails_list = results_data.get("emails", [])
        for item in emails_list:
            email = item.get("email", "")
            if email and "@" in email:
                contacts.append({
                    "name": "",
                    "title": "",
                    "email": email,
                    "type": classify_email(email),
                    "source": "icypeas",
                    "verified": False,
                })

        cache[key] = {"timestamp": datetime.now(timezone.utc).isoformat(), "results": contacts}
        save_cache(cache, cache_file)
        return contacts

    except Exception as e:
        if "429" not in str(e):
            cache[key] = {"timestamp": datetime.now(timezone.utc).isoformat(), "results": []}
            save_cache(cache, cache_file)
        return []


def icypeas_enrich_contacts(domain, contacts_with_names, cache, cache_file):
    enriched = 0
    for contact in contacts_with_names:
        if contact.get("email") or not contact.get("name"):
            continue
        parts = contact["name"].strip().split()
        if len(parts) < 2:
            continue
        first = parts[0]
        last = parts[-1]

        email = icypeas_find_email(first, last, domain, cache, cache_file)
        if email:
            contact["email"] = email
            contact["type"] = classify_email(email)
            contact["source"] = "icypeas"
            enriched += 1

    return enriched


# ============================================================
# TIER 3: HUNTER.IO
# ============================================================

def hunter_domain_search(domain, cache, cache_file):
    if not HUNTER_API_KEY:
        return []
    key = hashlib.md5(f"hunter:{domain}".encode()).hexdigest()
    if key in cache:
        return cache[key].get("results", [])
    try:
        r = requests.get(HUNTER_ENDPOINT, params={
            "domain": domain,
            "api_key": HUNTER_API_KEY
        }, timeout=10)
        if r.status_code == 429:
            return []
        if r.status_code != 200:
            cache[key] = {"timestamp": datetime.now(timezone.utc).isoformat(), "results": []}
            save_cache(cache, cache_file)
            return []
        data = r.json().get("data", {})
        emails = data.get("emails", [])
        contacts = []
        for e in emails:
            first = e.get("first_name", "") or ""
            last = e.get("last_name", "") or ""
            name = f"{first} {last}".strip()
            email = e.get("value", "")
            confidence = e.get("confidence", 0)
            email_type = e.get("type", "generic")
            if confidence < 50:
                continue
            contacts.append({
                "name": name,
                "title": e.get("position", "") or "",
                "email": email,
                "type": email_type if email_type in ("personal", "generic") else classify_email(email),
                "source": "hunter",
                "verified": False,
                "confidence": confidence,
            })
        cache[key] = {"timestamp": datetime.now(timezone.utc).isoformat(), "results": contacts}
        save_cache(cache, cache_file)
        time.sleep(HUNTER_DELAY)
        return contacts
    except Exception as ex:
        print(f"    Hunter error for {domain}: {ex}")
        return []




# ============================================================
# TIER 4: STAFF PAGE SCRAPING
# ============================================================

NAME_PATTERNS = [
    re.compile(r'<h[1-6][^>]*>([A-Z][a-z]+ [A-Z][a-z]+)</h[1-6]>', re.IGNORECASE),
    re.compile(r'"name"\s*:\s*"([A-Z][a-z]+ [A-Z][a-z]+)"'),
]

TITLE_KEYWORDS = [
    "ceo", "president", "founder", "owner", "director", "manager",
    "vp", "vice president", "partner", "principal", "administrator",
    "superintendent", "general manager", "chief",
]


def scrape_staff_pages(website_url, target_roles=None):
    if not website_url:
        return []

    url = website_url.strip()
    if not url.startswith("http"):
        url = "http://" + url

    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    }

    staff_paths = ["/team", "/about", "/leadership", "/staff", "/about-us", "/our-team"]
    contacts = []

    for path in staff_paths:
        try:
            page_url = url.rstrip("/") + path
            resp = requests.get(page_url, headers=headers, timeout=EMAIL_SCRAPE_TIMEOUT, allow_redirects=True)
            if resp.status_code != 200:
                continue

            text = resp.text

            for pattern in NAME_PATTERNS:
                names = pattern.findall(text)
                for name in names:
                    name = name.strip()
                    if len(name.split()) == 2 and len(name) < 40:
                        name_idx = text.lower().find(name.lower())
                        if name_idx >= 0:
                            nearby = text[max(0, name_idx - 200):name_idx + 200].lower()
                            for title_kw in TITLE_KEYWORDS:
                                if title_kw in nearby:
                                    found_title = title_kw.title()
                                    if not _title_matches_roles(found_title, target_roles):
                                        break
                                    contacts.append({
                                        "name": name,
                                        "title": found_title,
                                        "email": "",
                                        "type": "personal",
                                        "source": "staff_scrape",
                                        "verified": False,
                                    })
                                    break

        except Exception:
            continue

    seen = set()
    unique = []
    for c in contacts:
        if c["name"].lower() not in seen:
            seen.add(c["name"].lower())
            unique.append(c)

    return unique[:5]


# ============================================================
# EMAIL PATTERN INFERENCE + PROPAGATION
# ============================================================

def infer_pattern_from_email(email):
    """Infer the naming pattern from a single email address."""
    if not email or "@" not in email:
        return None
    local = email.lower().split("@")[0]
    if "." in local and len(local.split(".")) == 2:
        parts = local.split(".")
        if len(parts[0]) > 1 and len(parts[1]) > 1:
            return "first.last"
        elif len(parts[0]) == 1 and len(parts[1]) > 1:
            return "f.last"
    elif len(local) > 3 and local.isalpha():
        return "first"
    return None


def generate_email(name, domain, pattern):
    """Generate an email address from name + domain + pattern."""
    if not name or not domain or not pattern:
        return ""

    if not is_valid_person_name(name):
        return ""

    parts = name.strip().split()
    if len(parts) < 2:
        return ""

    first = parts[0].lower()
    last = parts[-1].lower()

    first = re.sub(r'[^a-z]', '', first)
    last = re.sub(r'[^a-z]', '', last)

    if not first or not last:
        return ""

    templates = {
        "first.last": f"{first}.{last}@{domain}",
        "first": f"{first}@{domain}",
        "f.last": f"{first[0]}.{last}@{domain}",
        "firstlast": f"{first}{last}@{domain}",
        "first_last": f"{first}_{last}@{domain}",
        "flast": f"{first[0]}{last}@{domain}",
    }

    return templates.get(pattern, "")


def generate_all_email_candidates(name, domain):
    candidates = []
    for pattern in ALL_PATTERNS:
        email = generate_email(name, domain, pattern)
        if email:
            candidates.append((email, pattern))
    return candidates


def waterfall_verify_email(name, domain, ctx):
    """Try multiple email patterns, verify each via MV until one hits."""
    from verification import verify_email_mv

    known_pattern = ctx.pattern_cache.get(domain)
    if known_pattern:
        email = generate_email(name, domain, known_pattern)
        if email:
            return email, known_pattern

    candidates = generate_all_email_candidates(name, domain)
    if not candidates:
        return "", None

    if not MILLIONVERIFIER_API_KEY:
        return candidates[0][0], candidates[0][1]

    for email, pattern in candidates:
        if verify_email_mv(email):
            ctx.pattern_cache[domain] = pattern
            return email, pattern

    return "", None


def propagate_emails(contacts, domain, ctx):
    """Generate emails for contacts that don't have one."""
    known_emails = [c["email"] for c in contacts if c.get("email")]
    pattern = ctx.infer_pattern(known_emails, domain)

    if pattern:
        for contact in contacts:
            if not contact.get("email") and contact.get("name"):
                generated = generate_email(contact["name"], domain, pattern)
                if generated:
                    contact["email"] = generated
                    contact["source"] = "propagated"
                    contact["verified"] = False
    else:
        needs_waterfall = [c for c in contacts if not c.get("email") and c.get("name")]
        if needs_waterfall:
            def _waterfall_one(contact):
                email, found_pattern = waterfall_verify_email(
                    contact["name"], domain, ctx
                )
                return contact, email

            with ThreadPoolExecutor(max_workers=min(WATERFALL_WORKERS, len(needs_waterfall))) as executor:
                futures = {executor.submit(_waterfall_one, c): c for c in needs_waterfall}
                for future in as_completed(futures, timeout=120):
                    try:
                        contact, email = future.result(timeout=30)
                        if email:
                            contact["email"] = email
                            contact["source"] = "waterfall_verified"
                            contact["verified"] = True
                    except Exception:
                        pass

    return contacts


# ============================================================
# ENRICHMENT ORCHESTRATOR
# ============================================================

def enrich_company(company_data, target_roles, cache, cache_file, ctx, dry_run=False):
    """Tiered enrichment for a single company."""
    domain = company_data["domain"]
    website = company_data["website"]
    company_name = company_data["company"]

    if dry_run:
        return []

    contacts = []
    has_personal = False
    has_generic = False

    # Tier 1: Website email scraping
    scraped_emails = scrape_website_emails(website)
    if scraped_emails:
        for email in scraped_emails[:5]:
            email_type = classify_email(email)
            contacts.append({
                "name": "",
                "title": "",
                "email": email,
                "type": email_type,
                "source": "website_scrape",
                "verified": False,
                "company": company_name,
                "domain": domain,
            })
            if email_type == "personal":
                has_personal = True
            else:
                has_generic = True

    ctx.update_domain_state(domain,
                            has_personal_email=has_personal,
                            has_generic_email=has_generic)

    # Tier 1b: Google SERP contact search
    if not has_personal and SERPER_API_KEY:
        serp_contacts = serp_search_contacts(company_name, domain, target_roles, cache, cache_file)
        if serp_contacts:
            for c in serp_contacts:
                c["company"] = company_name
                c["domain"] = domain
                contacts.append(c)
            scraped_personal = [ct["email"] for ct in contacts
                                if ct.get("email") and ct.get("type") == "personal"]
            if scraped_personal:
                pattern = ctx.infer_pattern(scraped_personal, domain)
                if pattern:
                    for c in serp_contacts:
                        if c.get("name") and not c.get("email"):
                            generated = generate_email(c["name"], domain, pattern)
                            if generated:
                                c["email"] = generated
                                c["type"] = "personal"
                                c["source"] = "serp_propagated"
                                has_personal = True

    # Tier 2: Icypeas
    if not has_personal and ICYPEAS_API_KEY:
        icypeas_contacts = icypeas_domain_search(domain, cache, cache_file)
        if icypeas_contacts:
            for c in icypeas_contacts:
                c["company"] = company_name
                c["domain"] = domain
                contacts.append(c)
                if c.get("email") and c.get("type") == "personal":
                    has_personal = True

        named_no_email = [c for c in contacts if c.get("name") and not c.get("email")]
        if named_no_email:
            enriched = icypeas_enrich_contacts(domain, named_no_email, cache, cache_file)
            if enriched > 0:
                has_personal = True

    # Tier 3: Hunter.io — DISABLED (subscription canceled 07APR2026)
    # Icypeas + AI Ark email export cover this tier better at lower cost.
    # To re-enable: uncomment the block below and add HUNTER_API_KEY to .env
    # if not has_personal and HUNTER_API_KEY:
    #     hunter_contacts = hunter_domain_search(domain, cache, cache_file)
    #     if hunter_contacts:
    #         for c in hunter_contacts:
    #             if not c.get("email"):
    #                 continue
    #             c["company"] = company_name
    #             c["domain"] = domain
    #             c["type"] = classify_email(c["email"])
    #             contacts.append(c)
    #             if c["type"] == "personal":
    #                 has_personal = True

    # Tier 4: Staff page scraping fallback
    if not has_personal:
        staff_contacts = scrape_staff_pages(website, target_roles=target_roles)
        for c in staff_contacts:
            c["company"] = company_name
            c["domain"] = domain
            c.setdefault("type", "personal")
            contacts.append(c)

    # Tier 4b: Pattern inference + propagation fallback
    named_without_email = [c for c in contacts if c.get("name") and not c.get("email")]
    personal_emails = [c["email"] for c in contacts
                       if c.get("email") and c.get("type") == "personal"]
    if named_without_email and personal_emails:
        pattern = ctx.infer_pattern(personal_emails, domain)
        if pattern:
            for c in named_without_email:
                generated = generate_email(c["name"], domain, pattern)
                if generated:
                    c["email"] = generated
                    c["type"] = "personal"
                    c["source"] = "propagated"

    # Attach company phone to all contacts
    phone = company_data.get("phone", "")
    if phone:
        for c in contacts:
            c.setdefault("phone", phone)

    return contacts
