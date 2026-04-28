"""
BlitzAPI integration module.
Domain-to-LinkedIn resolution, Waterfall ICP search, and email enrichment.
Completely optional — if BLITZ_API_KEY is not set, all functions return empty results.
"""

import time
import requests
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

from config import BLITZ_API_KEY, BLITZ_BASE_URL

logger = logging.getLogger(__name__)

# ============================================================
# CORE API CALLS
# ============================================================

def _blitz_headers():
    """Return auth headers for Blitz API."""
    return {
        "x-api-key": BLITZ_API_KEY,
        "Content-Type": "application/json",
    }


def _blitz_post(endpoint, payload, timeout=30):
    """Make a POST request to Blitz API with error handling."""
    if not BLITZ_API_KEY:
        return None
    try:
        url = f"{BLITZ_BASE_URL}{endpoint}"
        resp = requests.post(url, json=payload, headers=_blitz_headers(), timeout=timeout)
        if resp.status_code == 200:
            return resp.json()
        elif resp.status_code == 429:
            # Rate limited — wait and retry once
            time.sleep(1)
            resp = requests.post(url, json=payload, headers=_blitz_headers(), timeout=timeout)
            return resp.json() if resp.status_code == 200 else None
        else:
            logger.debug(f"Blitz {endpoint} returned {resp.status_code}: {resp.text[:200]}")
            return None
    except Exception as e:
        logger.debug(f"Blitz {endpoint} error: {e}")
        return None


# ============================================================
# DOMAIN TO LINKEDIN
# ============================================================

def domain_to_linkedin(domain):
    """
    Convert a company domain to its LinkedIn company URL.
    Returns the LinkedIn URL string or empty string if not found.
    """
    if not BLITZ_API_KEY or not domain:
        return ""
    data = _blitz_post("/enrichment/domain-to-linkedin", {"domain": domain})
    if data and data.get("company_linkedin_url"):
        return data["company_linkedin_url"]
    # Try alternate endpoint
    data = _blitz_post("/search/domain-to-linkedin-company", {"domain": domain})
    if data and data.get("company_linkedin_url"):
        return data["company_linkedin_url"]
    return ""


# ============================================================
# WATERFALL ICP SEARCH
# ============================================================

def waterfall_icp_search(company_linkedin_url, target_roles, max_results=3):
    """
    Find decision makers at a company using Blitz's Waterfall ICP Search.

    Args:
        company_linkedin_url: Full LinkedIn company URL
        target_roles: List of target role strings (e.g., ["Owner", "General Manager"])
        max_results: Max contacts to return (default 3)

    Returns:
        List of dicts with: first_name, last_name, title, linkedin_url, location
    """
    if not BLITZ_API_KEY or not company_linkedin_url:
        return []

    # Build cascade tiers from target roles
    # Tier 1: Owner/Founder/CEO (highest priority)
    tier1_titles = []
    tier2_titles = []
    tier3_titles = []

    owner_keywords = {"owner", "founder", "co-founder", "cofounder", "president", "ceo",
                      "proprietor", "partner", "principal"}
    exec_keywords = {"director", "vp", "vice president", "head", "chief", "gm",
                     "general manager", "regional manager", "operations manager"}

    for role in target_roles:
        role_lower = role.lower()
        if any(k in role_lower for k in owner_keywords):
            tier1_titles.append(role)
        elif any(k in role_lower for k in exec_keywords):
            tier2_titles.append(role)
        else:
            tier3_titles.append(role)

    # Ensure we always have at least tier 1
    if not tier1_titles:
        tier1_titles = ["Owner", "Founder", "CEO", "President"]
    if not tier2_titles:
        tier2_titles = ["General Manager", "Director", "VP Operations"]
    if not tier3_titles:
        tier3_titles = ["Manager", "Administrator"]

    cascade = [
        {
            "include_title": tier1_titles,
            "exclude_title": ["assistant", "intern", "junior", "associate"],
            "location": ["US"],
            "include_headline_search": False,
        },
        {
            "include_title": tier2_titles,
            "exclude_title": ["assistant", "intern", "junior"],
            "location": ["US"],
            "include_headline_search": True,
        },
        {
            "include_title": tier3_titles,
            "exclude_title": ["assistant", "intern", "junior"],
            "location": ["US"],
            "include_headline_search": True,
        },
    ]

    payload = {
        "company_linkedin_url": company_linkedin_url,
        "cascade": cascade,
        "max_results": max_results,
    }

    data = _blitz_post("/search/waterfall-icp-keyword", payload, timeout=30)
    if not data or not data.get("results"):
        return []

    contacts = []
    for result in data["results"]:
        person = result.get("person", {})
        location = person.get("location", {})

        # Get current job title from experiences
        title = ""
        experiences = person.get("experiences", [])
        for exp in experiences:
            if exp.get("job_is_current"):
                title = exp.get("job_title", "")
                break
        if not title:
            # Fall back to headline
            headline = person.get("headline", "")
            if headline:
                title = headline.split(" at ")[0].split(" @ ")[0].strip()

        contacts.append({
            "first_name": person.get("first_name", ""),
            "last_name": person.get("last_name", ""),
            "title": title,
            "linkedin_url": person.get("linkedin_url", ""),
            "location_city": location.get("city", ""),
            "location_state": location.get("state_code", ""),
            "icp_tier": result.get("icp", 0),
            "matched_on": [m.get("value", "") for m in result.get("what_matched", [])],
        })

    return contacts


# ============================================================
# EMAIL ENRICHMENT
# ============================================================

def enrich_email(linkedin_url):
    """
    Get verified work email from a LinkedIn profile URL.
    Returns email string or empty string if not found.
    """
    if not BLITZ_API_KEY or not linkedin_url:
        return ""
    data = _blitz_post("/enrichment/email", {"person_linkedin_url": linkedin_url})
    if data and data.get("found") and data.get("email"):
        return data["email"]
    return ""


# ============================================================
# BATCH ENRICHMENT (parallel)
# ============================================================

def blitz_enrich_company(company_name, domain, target_roles, max_results=3):
    """
    Full Blitz enrichment for a single company:
    1. Domain → LinkedIn URL
    2. Waterfall ICP → find decision makers
    3. Email enrichment for each contact

    Returns list of enriched contact dicts ready for the pipeline.
    """
    if not BLITZ_API_KEY:
        return []

    # Step 1: Get LinkedIn URL
    linkedin_url = domain_to_linkedin(domain)
    if not linkedin_url:
        return []

    # Step 2: Waterfall ICP search
    contacts = waterfall_icp_search(linkedin_url, target_roles, max_results=max_results)
    if not contacts:
        return []

    # Step 3: Enrich emails
    enriched = []
    for contact in contacts:
        email = ""
        if contact.get("linkedin_url"):
            email = enrich_email(contact["linkedin_url"])

        if email or contact.get("first_name"):
            enriched.append({
                "name": f"{contact['first_name']} {contact['last_name']}".strip(),
                "first_name": contact["first_name"],
                "last_name": contact["last_name"],
                "title": contact["title"],
                "email": email,
                "linkedin_url": contact.get("linkedin_url", ""),
                "company": company_name,
                "domain": domain,
                "source": "blitz",
                "icp_tier": contact.get("icp_tier", 0),
            })

    return enriched


def blitz_enrich_batch(companies, target_roles, max_workers=10, progress_callback=None):
    """
    Enrich a batch of companies via Blitz in parallel.

    Args:
        companies: List of dicts with 'name' and 'domain' keys
        target_roles: List of target role strings
        max_workers: Parallel threads (default 10, Blitz supports 50 RPS)
        progress_callback: Optional function called with (done_count, total, contacts_found)

    Returns:
        List of all enriched contacts across all companies.
    """
    if not BLITZ_API_KEY:
        return []

    all_contacts = []
    done = 0
    total = len(companies)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {}
        for company in companies:
            name = company.get("name", "")
            domain = company.get("domain", "")
            if not domain:
                continue
            future = executor.submit(
                blitz_enrich_company, name, domain, target_roles, max_results=3
            )
            futures[future] = company

        for future in as_completed(futures):
            done += 1
            try:
                contacts = future.result(timeout=60)
                all_contacts.extend(contacts)
            except Exception:
                pass

            if progress_callback and done % 25 == 0:
                progress_callback(done, total, len(all_contacts))

    return all_contacts


# ============================================================
# EMAIL VALIDATION
# ============================================================

def blitz_validate_email(email):
    """
    Validate an email via Blitz's validation endpoint.
    Returns: 'valid', 'catch_all', 'invalid', or 'unknown'
    """
    if not BLITZ_API_KEY or not email:
        return "unknown"
    data = _blitz_post("/utilities/email/validate", {"email": email})
    if data:
        # Map Blitz response to our standard categories
        status = data.get("status", "").lower()
        if status in ("valid", "deliverable"):
            return "valid"
        elif "catch" in status:
            return "catch_all"
        elif status in ("invalid", "undeliverable"):
            return "invalid"
    return "unknown"


# ============================================================
# AVAILABILITY CHECK
# ============================================================

def is_blitz_available():
    """Check if Blitz API is configured and accessible."""
    if not BLITZ_API_KEY:
        return False
    try:
        resp = requests.get(
            f"{BLITZ_BASE_URL}/account/key-info",
            headers=_blitz_headers(),
            timeout=10,
        )
        return resp.status_code == 200
    except Exception:
        return False
