"""
utils.py: small shared helpers used by enrichment.py and lead_pipeline_v6.py.

Pure functions only (no network), so they are covered by tests/test_utils.py.
"""

import re
import time
from urllib.parse import urlparse

from config import (
    JUNK_EMAIL_PATTERNS, GENERIC_PREFIXES, CITY_COORDS,
    COMMON_FIRST_NAMES, BUSINESS_NAME_WORDS,
)


def slugify(text):
    """'Fire Protection, Tampa!' -> 'fire-protection-tampa'."""
    text = (text or "").lower().strip()
    text = re.sub(r"[^a-z0-9]+", "-", text)
    return text.strip("-")


def get_domain(url):
    """Return the bare registrable host for a URL or host string ('' if none)."""
    if not url:
        return ""
    url = url.strip()
    if "://" not in url:
        url = "http://" + url
    host = (urlparse(url).hostname or "").lower()
    return host[4:] if host.startswith("www.") else host


def is_junk_email(email):
    """True for scraped strings that are not real contact addresses."""
    if not email or "@" not in email:
        return True
    e = email.lower().strip()
    return any(p in e for p in JUNK_EMAIL_PATTERNS)


def classify_email(email):
    """'generic' for role inboxes (info@, sales@ ...), else 'personal'."""
    local = (email or "").split("@", 1)[0].lower()
    local_clean = re.sub(r"[^a-z]", "", local)
    if local in GENERIC_PREFIXES or local_clean in GENERIC_PREFIXES:
        return "generic"
    return "personal"


def is_valid_person_name(name):
    """Reject business names, single tokens that are not first names, and junk."""
    if not name:
        return False
    parts = [p for p in re.split(r"\s+", name.strip()) if p]
    if not parts or len(parts) > 4:
        return False
    if any(ch.isdigit() or ch == "@" for ch in name):
        return False
    lowered = [p.lower().strip(".,") for p in parts]
    if any(p in BUSINESS_NAME_WORDS for p in lowered):
        return False
    if len(parts) == 1:
        return lowered[0] in COMMON_FIRST_NAMES
    return all(re.fullmatch(r"[A-Za-z][A-Za-z'\-\.]*", p) for p in parts)


def retry_with_backoff(fn, retries=2, base_delay=1.0):
    """Call fn(); on exception retry with exponential backoff, then re-raise."""
    for attempt in range(retries + 1):
        try:
            return fn()
        except Exception:
            if attempt == retries:
                raise
            time.sleep(base_delay * (2 ** attempt))


def geocode_city(city, cache=None, cache_key_func=None, save_cache_func=None):
    """Offline lookup against data/cities.json. Returns (lat, lng) or None."""
    if not city:
        return None
    return CITY_COORDS.get(city.lower().strip())
