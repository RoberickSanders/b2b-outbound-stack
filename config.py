"""
Configuration for the lead pipeline.
API keys, constants, data loading, and pipeline context.
"""

import os
import json
import re
import threading
from collections import Counter

# ============================================================
# PATHS
# ============================================================

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
WORKSPACE_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, "..", ".."))
PROJECTS_DIR = os.path.join(WORKSPACE_ROOT, "01-Projects")
DATA_DIR = os.path.join(SCRIPT_DIR, "data")

CACHE_FILE = os.path.join(SCRIPT_DIR, "pipeline_cache.json")
CACHE_VERSION = 3
CLASSIFICATION_CACHE_FILE = os.path.join(SCRIPT_DIR, "classification_cache.json")
GLOBAL_DEDUP_FILE = os.path.join(SCRIPT_DIR, "global_sent_emails.csv")

# Runtime output directory — set by setup_client_folder() during pipeline run
OUTPUT_DIR = SCRIPT_DIR

# ============================================================
# LOAD .env FILES (workspace root + local)
# ============================================================

from dotenv import load_dotenv
# Load workspace-level .env first, then local .env (local overrides)
# override=True ensures .env values take precedence over empty shell vars
load_dotenv(os.path.join(WORKSPACE_ROOT, ".env"), override=True)
load_dotenv(os.path.join(SCRIPT_DIR, ".env"), override=True)

# ============================================================
# API KEYS
# ============================================================

SERPER_API_KEY = os.environ.get("SERPER_API_KEY", "")
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
ICYPEAS_API_KEY = os.environ.get("ICYPEAS_API_KEY", "")
ICYPEAS_API_SECRET = os.environ.get("ICYPEAS_API_SECRET", "")
ICYPEAS_USER_ID = os.environ.get("ICYPEAS_USER_ID", "")
HUNTER_API_KEY = os.environ.get("HUNTER_API_KEY", "")
MILLIONVERIFIER_API_KEY = os.environ.get("MILLIONVERIFIER_API_KEY", "")
# APIFY removed 08APR2026 (subscription canceled, unused)
APIFY_API_KEY = ""
BLITZ_API_KEY = os.environ.get("BLITZ_API_KEY", "")
BOUNCEBAN_API_KEY = os.environ.get("BOUNCEBAN_API_KEY", "")
BOUNCEBAN_ENDPOINT = "https://api-waterfall.bounceban.com/v1/verify/single"

# ============================================================
# API ENDPOINTS
# ============================================================

SERPER_MAPS_ENDPOINT = "https://google.serper.dev/maps"
SERPER_WEB_ENDPOINT = "https://google.serper.dev/search"
ICYPEAS_EMAIL_ENDPOINT = "https://app.icypeas.com/api/email-search"
ICYPEAS_DOMAIN_ENDPOINT = "https://app.icypeas.com/api/domain-search"
MILLIONVERIFIER_ENDPOINT = "https://api.millionverifier.com/api/v3/"
HUNTER_ENDPOINT = "https://api.hunter.io/v2/domain-search"
BLITZ_BASE_URL = "https://api.blitz-api.ai/v2"

# ============================================================
# PIPELINE SETTINGS
# ============================================================

GRID_RADIUS = 0.07
PAGES = 1
MAX_WORKERS = 10
RATE_LIMIT_DELAY = 0.3
EMAIL_SCRAPE_TIMEOUT = 8
EMAIL_SCRAPE_WORKERS = 10

# Classifier settings
CLASSIFIER_HAIKU_MODEL = "claude-haiku-4-5-20251001"
CLASSIFIER_CONFIDENCE_THRESHOLD = 60
CLASSIFIER_SCRAPE_TIMEOUT = 6
CLASSIFIER_SCRAPE_WORKERS = 20
CLASSIFIER_HAIKU_BATCH_SIZE = 20
CLASSIFIER_SCRAPE_PAGES = ["/", "/about", "/about-us", "/services", "/our-services", "/what-we-do"]

# Verification settings
MV_RATE_DELAY = 0.15
VERIFY_WORKERS = 12
CATCH_ALL_WORKERS = 8
CATCH_ALL_RATE_DELAY = 0.15

# Enrichment settings
HUNTER_DELAY = 0.8
ICYPEAS_POLL_INTERVAL = 2
ICYPEAS_POLL_MAX_WAIT = 30
WATERFALL_WORKERS = 8
ENRICHMENT_WORKERS = 10

# ============================================================
# LOAD DATA FILES
# ============================================================

def _load_json(filename):
    """Load a JSON data file from the data/ directory."""
    filepath = os.path.join(DATA_DIR, filename)
    with open(filepath, "r", encoding="utf-8") as f:
        return json.load(f)


CITY_COORDS = {k: tuple(v) for k, v in _load_json("cities.json")["coords"].items()}
CITY_PRESETS = _load_json("cities.json")["presets"]

_chains_data = _load_json("chains.json")
CHAIN_KEYWORDS = _chains_data["chain_keywords"]
JUNK_DOMAINS = _chains_data["junk_domains"]

_names_data = _load_json("names.json")
COMMON_FIRST_NAMES = set(_names_data["common_first_names"])
BUSINESS_NAME_WORDS = set(_names_data["business_name_words"])

# ============================================================
# STATIC LISTS (too small for separate files)
# ============================================================

OWNER_ROLES = [
    "owner", "founder", "co-founder", "president",
    "principal", "managing partner", "partner",
    "managing director", "founder & ceo",
]

EXCLUDED_TITLES = [
    "assistant", "executive assistant", "admin", "administrator",
    "coordinator", "sdr", "bdr", "sales development",
    "business development representative",
    "recruiter", "talent", "intern", "associate",
    "nurse", "nursing", "clinical", "medical director", "physician",
    "therapist", "rehabilitation", "pharmacist", "dentist", "surgeon",
    "radiologist", "pathologist", "anesthesiologist",
    "food and beverage", "food & beverage", "f&b director",
    "human resources", "hr director", "hr manager",
    "marketing director", "marketing manager", "marketing coordinator",
    "finance director", "controller", "accounting",
    "social media", "content", "copywriter", "graphic design",
    "receptionist", "front desk", "concierge", "housekeeper",
    "housekeeping", "bellman", "valet", "bartender", "server",
    "chef", "sous chef", "pastry", "sommelier",
    "event coordinator", "event planner", "wedding",
]

CONTACT_PATHS = ["/contact", "/about", "/team", "/leadership", "/staff", "/about-us", "/contact-us"]

JUNK_EMAIL_PATTERNS = [
    "example.com", "test.com", "domain.com", "sentry.io",
    "wixpress.com", "wordpress.com", "squarespace.com",
    "googleapis.com", "google.com", "facebook.com", "twitter.com",
    "schema.org", "w3.org", "cloudflare.com", "jquery.com",
    ".png", ".jpg", ".gif", ".svg", ".webp", ".css", ".js",
]

GENERIC_PREFIXES = [
    "info", "sales", "support", "contact", "admin", "hello", "office",
    "help", "service", "billing", "mail", "enquiries", "inquiries",
    "general", "team", "hr", "careers", "jobs", "marketing", "press",
    "media", "reception", "frontdesk", "customerservice", "feedback",
    "scheduling", "patients", "privacy", "helpdesk", "dispatch",
    "orders", "returns", "bookings", "reservations", "apply", "lease",
    "tickets", "webmaster", "events", "catering", "groups",
    "denver", "nashville", "austin", "chicago", "miami", "seattle",
    "portland", "boston", "dallas", "houston", "phoenix", "atlanta",
    "tampa", "orlando", "catskills", "brooklyn", "manhattan",
    "manager", "director", "accounting", "leasing", "explore",
    "reservations", "talktous", "book", "stay", "guest", "concierge",
    "valet", "housekeeping", "maintenance", "engineering", "security",
    "lbd", "rsvp", "inquiry", "request",
]

EMAIL_REGEX = re.compile(r"[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}")

ALL_PATTERNS = ["first.last", "firstlast", "f.last", "first_last", "flast", "first"]

SYNONYM_MAP = {
    "hospital": ["medical center", "healthcare facility", "clinic"],
    "hotels": ["hotel", "resort", "lodging", "inn", "boutique hotel"],
    "warehouse": ["distribution center", "logistics facility", "fulfillment center"],
    "apartment": ["multifamily housing", "residential complex", "condominium"],
    "school": ["academy", "K-12 school", "educational institution"],
    "university": ["college", "higher education"],
    "church": ["place of worship", "religious organization"],
    "office": ["commercial building", "business park", "corporate office"],
    "assisted living": ["senior care", "nursing home", "memory care", "retirement community"],
    "restaurant": ["dining", "food service", "eatery"],
    "shopping center": ["strip mall", "retail plaza", "shopping mall"],
    "medical center": ["outpatient clinic", "surgery center", "healthcare clinic"],
    "property management": ["real estate management", "HOA management"],
    "managed service provider": ["IT support company", "managed IT services"],
    "fire protection": ["fire sprinkler", "fire suppression", "fire alarm"],
    "cleaning": ["janitorial", "custodial", "sanitation"],
}

TECH_SIGNATURES = {
    "WordPress": ["wp-content", "wp-includes", "wordpress"],
    "Shopify": ["cdn.shopify.com", "shopify.com/s/"],
    "Wix": ["wix.com", "wixsite.com", "parastorage.com"],
    "Squarespace": ["squarespace.com", "sqsp.com", "squarespace-cdn.com"],
    "Webflow": ["webflow.com", "assets.website-files.com"],
    "HubSpot CMS": ["hs-scripts.com", "hubspot.com", "hbspt.cq.load"],
    "Google Analytics": ["google-analytics.com", "gtag/js", "googletagmanager.com"],
    "Google Tag Manager": ["googletagmanager.com/gtm.js"],
    "Facebook Pixel": ["connect.facebook.net", "fbq("],
    "HubSpot": ["js.hs-scripts.com", "js.hubspot.com"],
    "Salesforce": ["pardot.com", "salesforce.com", "force.com"],
    "Marketo": ["marketo.com", "munchkin.js", "mktoForms"],
    "Intercom": ["intercom.io", "intercomcdn.com"],
    "Drift": ["drift.com", "driftt.com"],
    "Zendesk": ["zendesk.com", "zdassets.com"],
    "Freshdesk": ["freshdesk.com", "freshworks.com"],
    "Mailchimp": ["mailchimp.com", "chimpstatic.com"],
    "ActiveCampaign": ["activecampaign.com", "trackcmp.net"],
    "Klaviyo": ["klaviyo.com", "a.]klaviyo.com"],
    "Stripe": ["stripe.com", "js.stripe.com"],
    "PayPal": ["paypal.com", "paypalobjects.com"],
    "LiveChat": ["livechatinc.com", "livechat.com"],
    "Crisp": ["crisp.chat", "client.crisp.chat"],
    "Tawk.to": ["tawk.to", "embed.tawk.to"],
    "Cloudflare": ["cdnjs.cloudflare.com", "cloudflare.com"],
    "AWS": ["amazonaws.com", "aws.amazon.com"],
}

# ============================================================
# PIPELINE CONTEXT
# ============================================================

_domain_state_lock = threading.Lock()
_pattern_cache_lock = threading.Lock()


class PipelineContext:
    """Holds all mutable pipeline state. Passed through functions instead of globals."""

    def __init__(self):
        self.positive_keywords = []
        self.negative_keywords = []
        self.required_keywords = []
        self.strict_icp = False
        self.excluded_titles = []
        self.buyer_keywords = []
        self.pattern_cache = {}
        self.domain_state = {}
        # v6: classifier fields
        self.classifier_positive_keywords = []
        self.classifier_negative_keywords = []
        self.query_exclusion_terms = []
        self.valid_maps_categories = []
        self.client_summary = ""
        self.target_description = ""
        self.classification_threshold = CLASSIFIER_CONFIDENCE_THRESHOLD

    def score_company(self, name, category):
        text = f"{name} {category}".lower()
        score = 0
        if any(k in text for k in self.positive_keywords):
            score += 3
        if any(k in text for k in self.negative_keywords):
            score -= 5
        if any(k in text for k in CHAIN_KEYWORDS):
            score -= 10
        return score

    def is_qualified(self, company):
        score = self.score_company(company["company"], company.get("category", ""))
        if self.strict_icp:
            return score >= 2
        else:
            return score >= -1

    def is_target_company(self, company):
        text = f"{company['company']} {company.get('category', '')}".lower()
        if any(n in text for n in self.negative_keywords):
            return False
        return True

    def update_domain_state(self, domain, **kwargs):
        with _domain_state_lock:
            if domain not in self.domain_state:
                self.domain_state[domain] = {
                    "has_personal_email": False,
                    "has_generic_email": False,
                }
            self.domain_state[domain].update(kwargs)

    def infer_pattern(self, emails, domain):
        from enrichment import infer_pattern_from_email, classify_email_type
        with _pattern_cache_lock:
            if domain in self.pattern_cache:
                return self.pattern_cache[domain]

        domain_emails = [e.lower() for e in emails if e.lower().endswith("@" + domain)]
        domain_emails = [e for e in domain_emails if classify_email_type(e) != "generic"]

        if len(domain_emails) < 1:
            return None

        patterns_found = []
        for email in domain_emails:
            pattern = infer_pattern_from_email(email)
            if pattern:
                patterns_found.append(pattern)

        if patterns_found:
            most_common = Counter(patterns_found).most_common(1)[0][0]
            with _pattern_cache_lock:
                self.pattern_cache[domain] = most_common
            return most_common

        return None
