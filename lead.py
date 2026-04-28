#!/usr/bin/env python3
"""
Lead — Smart Lead Router
========================
Natural language entry point for lead generation.

Usage:
    python3 lead.py "find me 1000 fire protection firms for client_c"
    python3 lead.py "500 MSPs in Texas for client_b"
    python3 lead.py "hotels in Denver for client_a, 300 leads"

    # With flags
    python3 lead.py "cost seg firms" --target 500 --dry-run
    python3 lead.py "fire protection" --force b2b
    python3 lead.py "500 R&D tax firms" --no-fallback
    python3 lead.py "MSPs for client_b" --aiark

What it does:
1. Parses your natural language request via Claude Haiku
2. Detects the client (client_c / client_b / client_a)
3. Decides whether to use B2B pipeline (lead_generator_v2.py) or LOCAL
   pipeline (lead_pipeline_v6.py) based on what you're looking for
4. Runs the right pipeline with the right settings
5. Routes output to the correct client folder
6. Falls back to the other pipeline if the first comes up short
"""

import os
import sys
import re
import json
import argparse
import subprocess
from datetime import datetime

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
WORKSPACE_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, "..", ".."))
PROJECTS_DIR = os.path.join(WORKSPACE_ROOT, "01-Projects")

# Use Python 3.13 for subprocess calls (needs dotenv)
PYTHON_BIN = "/usr/local/bin/python3.13"
if not os.path.exists(PYTHON_BIN):
    PYTHON_BIN = sys.executable

sys.path.insert(0, SCRIPT_DIR)

# ── Known clients ────────────────────────────────────────────────────────────
KNOWN_CLIENTS = {
    "client_c": [
        "client_c", "client c", "rm", "revmech", "rev mech",
        "me", "myself", "my", "our agency",
    ],
    "client_b": [
        "client_b", "client b", "sc", "secure",
    ],
    "client_a": [
        "client_a", "client a", "preaction", "pf", "preaction-fire",
    ],
}

CLIENT_DEFAULTS = {
    "client_c": "b2b",   # CLIENT_C prospects for itself = industry firms
    "client_b": "b2b",     # CLIENT_B targets MSPs as partners = B2B
    "client_a": "local",    # PF targets buildings/facilities = local
}

# ── Routing signals ──────────────────────────────────────────────────────────
B2B_SIGNALS = [
    # Industry structure words
    "firm", "firms", "company", "companies", "agency", "agencies",
    "provider", "providers", "consultancy", "consultancies", "consultant",
    "consultants", "practice", "practices", "service provider",
    # Tech / SaaS
    "saas", "msp", "msps", "mssp", "mssps", "ai startup", "software",
    "platform", "technology company", "tech company",
    # Professional services
    "cpa firm", "law firm", "tax firm", "accounting firm", "law office",
    "legal", "tax advisory", "tax consulting",
    # Specific niches
    "cost segregation", "cost seg", "r&d tax", "property tax",
    "telecom audit", "telecom expense", "utility audit", "insurance audit",
    "osha compliance", "hazmat", "workers comp", "sales tax",
    "freight audit", "forensic accounting",
]

LOCAL_SIGNALS = [
    # Physical business types
    "hotel", "hotels", "resort", "resorts", "motel", "inn",
    "restaurant", "restaurants", "cafe", "cafes", "bar", "bars",
    "church", "churches", "school", "schools", "daycare", "daycares",
    "hospital", "hospitals", "clinic", "clinics", "gym", "gyms",
    "apartment", "apartments", "property", "properties", "building",
    "buildings", "warehouse", "warehouses", "facility", "facilities",
    "retail store", "shop", "shops", "storefront", "salon", "salons",
    "spa", "spas", "studio", "studios",
    # Geographic signals
    "near me", "local businesses",
]

GEO_KEYWORDS = [
    "in ", "near ", "around ", "throughout "
]

US_STATES = {
    "alabama", "alaska", "arizona", "arkansas", "california", "colorado",
    "connecticut", "delaware", "florida", "georgia", "hawaii", "idaho",
    "illinois", "indiana", "iowa", "kansas", "kentucky", "louisiana",
    "maine", "maryland", "massachusetts", "michigan", "minnesota",
    "mississippi", "missouri", "montana", "nebraska", "nevada",
    "new hampshire", "new jersey", "new mexico", "new york",
    "north carolina", "north dakota", "ohio", "oklahoma", "oregon",
    "pennsylvania", "rhode island", "south carolina", "south dakota",
    "tennessee", "texas", "utah", "vermont", "virginia", "washington",
    "west virginia", "wisconsin", "wyoming",
}


# ==============================================================================
# INTENT PARSING
# ==============================================================================

def detect_client(text):
    """Detect which client the request is for. Returns client key or None."""
    text_lower = text.lower()
    for client, aliases in KNOWN_CLIENTS.items():
        for alias in aliases:
            # Match ""for X"" or ""@X"" or the full client name anywhere
            if re.search(rf"\bfor\s+{re.escape(alias)}\b", text_lower):
                return client
            if re.search(rf"\b{re.escape(alias)}\b", text_lower) and len(alias) >= 4:
                return client
    return None


def extract_target(text):
    """Extract target lead count from text. Returns int or None."""
    # Look for "1000 leads", "500 contacts", "1k", "2k"
    m = re.search(r"\b(\d{1,5})\s*k\b", text, re.IGNORECASE)
    if m:
        return int(m.group(1)) * 1000
    m = re.search(r"\b(\d{2,6})\b\s*(leads?|contacts?|prospects?|companies|firms|results?)?", text, re.IGNORECASE)
    if m:
        return int(m.group(1))
    return None


def extract_geo(text):
    """Extract geographic scope from text. Returns state/city or None."""
    text_lower = text.lower()
    # Look for "in <state>" or "<state>"
    for state in US_STATES:
        if re.search(rf"\b{re.escape(state)}\b", text_lower):
            return state
    # Look for "in <City>"
    m = re.search(r"\bin\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)\b", text)
    if m:
        return m.group(1).lower()
    return None


def strip_metadata(text):
    """Remove client references, target numbers, and geo phrases to get just the niche."""
    clean = text
    # Remove "find me", "get me", "I need", etc.
    clean = re.sub(r"^\s*(find|get|give|show)\s+(me\s+)?", "", clean, flags=re.IGNORECASE)
    clean = re.sub(r"^\s*(i\s+(?:need|want|am looking for))\s+", "", clean, flags=re.IGNORECASE)
    # Remove target counts
    clean = re.sub(r"\b\d{1,5}\s*k?\s*(leads?|contacts?|prospects?|companies|firms|results?)?\b",
                   "", clean, flags=re.IGNORECASE)
    # Remove client references
    for client, aliases in KNOWN_CLIENTS.items():
        for alias in aliases:
            clean = re.sub(rf"\bfor\s+{re.escape(alias)}\b", "", clean, flags=re.IGNORECASE)
            if len(alias) >= 4:
                clean = re.sub(rf"\b{re.escape(alias)}\b", "", clean, flags=re.IGNORECASE)
    # Clean up whitespace and punctuation
    clean = re.sub(r"\s+", " ", clean).strip(" ,.-")
    return clean


def detect_routing(niche_text, geo=None, client=None):
    """
    Decide which pipeline to use: b2b, local, or unclear.
    Rule-based. Returns ('b2b' | 'local' | 'unclear', reason).
    """
    text = niche_text.lower()

    b2b_score = sum(1 for s in B2B_SIGNALS if s in text)
    local_score = sum(1 for s in LOCAL_SIGNALS if s in text)

    # Strong geographic signal → local
    if geo:
        local_score += 2

    # Strong signals
    if b2b_score >= 2 and local_score == 0:
        return "b2b", f"strong b2b match ({b2b_score} keywords)"
    if local_score >= 2 and b2b_score == 0:
        return "local", f"strong local match ({local_score} keywords)"

    # Moderate signals with tiebreaker
    if b2b_score > local_score:
        return "b2b", f"b2b lean ({b2b_score} vs {local_score})"
    if local_score > b2b_score:
        return "local", f"local lean ({local_score} vs {b2b_score})"

    # Client-aware tiebreaker for truly ambiguous
    if client and client in CLIENT_DEFAULTS:
        default = CLIENT_DEFAULTS[client]
        return default, f"client default for {client}"

    return "unclear", "no clear signals"


def generate_keywords_with_haiku(niche):
    """Generate keyword sets for the niche.

    Routed via llm_router.get_light_client() — uses Kimi K2.6 if KIMI_API_KEY is set,
    falls back to Claude Haiku otherwise. Both produce equivalent-quality keyword lists
    for this task (simple classification/generation, no nuance needed).
    """
    try:
        from llm_router import get_light_client
    except Exception as e:
        print(f"  [WARN] llm_router unavailable: {e}")
        return []

    try:
        client, model = get_light_client()

        prompt = f"""Generate 10-15 keyword variations for searching B2B companies in this niche:

Niche: {niche}

Return ONLY a JSON array of strings (no prose, no markdown). Each string should be a
distinct keyword phrase someone would use to find companies in this niche. Cover:
- Main industry name variations
- Related services / offerings
- Common synonyms
- Specific deliverables
- Terminology variations

Example output for "cost segregation firms":
["cost segregation", "cost seg", "cost segregation study", "accelerated depreciation", "property depreciation", "cost segregation services", "cost segregation specialists", "engineering-based depreciation", "real estate tax depreciation", "tax depreciation study"]
"""

        response = client.messages.create(
            model=model,
            max_tokens=1024,
            messages=[{"role": "user", "content": prompt}],
        )

        text = response.content[0].text.strip()
        # Strip markdown code fences if present
        text = re.sub(r"^```(?:json)?\s*", "", text)
        text = re.sub(r"\s*```$", "", text)

        keywords = json.loads(text)
        if isinstance(keywords, list):
            return [k.strip() for k in keywords if isinstance(k, str) and k.strip()]
    except Exception as e:
        print(f"  [WARN] Keyword generation failed: {e}")

    return []


def parse_intent_haiku(query, default_client=None, force_route=None):
    """
    Use Claude Haiku to parse complex natural language into structured intent.
    Handles queries like:
        "property managers in Denver who manage 10+ units"
        "find MSPs in Texas with under 50 employees"
        "restaurants in mountain towns in Colorado, owners only"
        "show me CFOs at fintech startups raising Series A"
    Returns dict or None if Haiku is unavailable.
    """
    try:
        # Load API key
        for env_path in (
            os.path.join(WORKSPACE_ROOT, ".env"),
            os.path.join(SCRIPT_DIR, ".env"),
        ):
            if os.path.isfile(env_path):
                for line in open(env_path):
                    line = line.strip()
                    if not line or line.startswith("#") or "=" not in line:
                        continue
                    k, v = line.split("=", 1)
                    k = k.strip()
                    v = v.strip().strip('"').strip("'")
                    if k and not os.environ.get(k):
                        os.environ[k] = v

        api_key = os.environ.get("ANTHROPIC_API_KEY", "")
        # Route through llm_router — uses Kimi K2.6 for this light classification
        # task (natural language → JSON intent) at ~8x cheaper than Claude Haiku.
        # Falls back to Claude Haiku if KIMI_API_KEY not set.
        from llm_router import get_light_client
        client, haiku_model = get_light_client()

        system = """You parse natural language lead-generation requests into structured JSON.

Known clients:
- client_c (aliases: rm, rev mech, "me", "my", "our agency") — B2B agency, targets firms/companies
- client_b (aliases: sc, secure) — cybersecurity, targets MSPs/fintech
- client_a (aliases: paf, preaction, pf) — fire protection services, targets local businesses with buildings

Routing rules:
- "b2b" = looking for FIRMS/COMPANIES in a professional niche (consulting, SaaS, financial services, etc.)
- "local" = looking for LOCAL BUSINESSES with physical locations (restaurants, hotels, churches, schools, etc.)
- If the client is client_a, default to "local" unless explicitly B2B
- If the client is client_c or client_b, default to "b2b"

Discovery method (pick the CHEAPEST effective source):
- "blitz" = B2B professional service firms with LinkedIn presence (cost seg, M&A, CPAs, MSPs, SaaS). Use Blitz keyword search. DEFAULT for b2b routing.
- "directory" = Trades, inspection, compliance, or niche service companies (fire alarm, elevator, OSHA, plumbing, HVAC, roofing). These are small local firms with no LinkedIn. Scrape industry directories via Firecrawl. CHEAPEST for trades.
- "serper_maps" = Local consumer-facing businesses with Google Maps listings (restaurants, hotels, churches, schools, salons, gyms). DEFAULT for local routing.
- "hybrid" = Try multiple sources. Use when unsure or when one source won't be enough.

How to pick discovery_method:
- Is it a trades/inspection/compliance niche? → "directory" (Firecrawl is 40x cheaper than Serper for these)
- Is it a B2B professional service with LinkedIn presence? → "blitz"
- Is it a local consumer-facing business? → "serper_maps"
- Could go either way? → "hybrid"

Return ONLY a JSON object with these fields:
{
  "client": "client_c|client_b|client_a",
  "niche": "<concise niche description, 2-5 words>",
  "target": <number of leads requested, default 500>,
  "geo": "<state or city or null>",
  "routing": "b2b|local",
  "routing_reason": "<brief reason>",
  "discovery_method": "blitz|directory|serper_maps|hybrid",
  "discovery_reason": "<why this method is cheapest for this niche>",
  "title_filter": "<role to target, e.g. 'Owner', 'CFO', or null>",
  "company_size": "<size filter like 'under 50 employees' or null>",
  "extra_instructions": "<any other constraints mentioned, or null>",
  "directory_url": "<if discovery_method is 'directory', suggest a likely industry directory URL or null>"
}

Rules:
- Output ONLY the JSON. No preamble, no markdown.
- If no client is mentioned, default to "client_c".
- If no target count is mentioned, default to 500.
- Parse numbers like "1k" = 1000, "2k" = 2000.
- For geo, extract the most specific location mentioned."""

        response = client.messages.create(
            model=haiku_model,
            max_tokens=300,
            system=system,
            messages=[{"role": "user", "content": query}],
        )

        raw = response.content[0].text.strip() if response.content else ""
        m = re.search(r"\{[^}]*\}", raw, re.DOTALL)
        if not m:
            return None

        parsed = json.loads(m.group(0))

        # Validate required fields
        niche = parsed.get("niche", "").strip()
        if not niche:
            return None

        client_name = parsed.get("client", default_client or "client_c")
        if client_name not in KNOWN_CLIENTS:
            client_name = default_client or "client_c"

        routing = force_route or parsed.get("routing", "b2b")
        if routing not in ("b2b", "local"):
            routing = CLIENT_DEFAULTS.get(client_name, "b2b")

        target = parsed.get("target", 500)
        if not isinstance(target, int) or target < 1:
            target = 500

        geo = parsed.get("geo")
        if geo and isinstance(geo, str):
            geo = geo.strip().lower()
            if geo in ("null", "none", ""):
                geo = None
        else:
            geo = None

        slug_base = re.sub(r"[^a-z0-9]+", "-", niche.lower()).strip("-")[:40]
        slug = f"{slug_base}-{datetime.now().strftime('%Y%m%d')}"

        # Determine discovery method
        discovery = parsed.get("discovery_method", "blitz")
        if discovery not in ("blitz", "directory", "serper_maps", "hybrid"):
            discovery = "blitz" if routing == "b2b" else "serper_maps"

        return {
            "client": client_name,
            "niche": niche,
            "target": target,
            "geo": geo,
            "routing": routing,
            "routing_reason": parsed.get("routing_reason", "haiku parsed"),
            "discovery_method": discovery,
            "discovery_reason": parsed.get("discovery_reason", ""),
            "directory_url": parsed.get("directory_url"),
            "slug": slug,
            "keywords": [],
            "title_filter": parsed.get("title_filter"),
            "company_size": parsed.get("company_size"),
            "extra_instructions": parsed.get("extra_instructions"),
            "_parsed_by": "haiku",
        }

    except Exception as e:
        print(f"  [INFO] Haiku parse unavailable ({e}), falling back to regex")
        return None


def parse_intent(query, default_client=None, force_route=None):
    """
    Parse natural language query into structured intent.
    Tries Haiku first for complex queries, falls back to regex.
    Returns dict with: client, niche, target, geo, routing, keywords, slug
    """
    if not query or not query.strip():
        raise ValueError("Empty query")

    # Try Haiku first (handles complex queries, title filters, company size, etc.)
    haiku_result = parse_intent_haiku(query, default_client, force_route)
    if haiku_result:
        return haiku_result

    # Fallback: regex-based parsing (no API needed, always works)
    # Detect client
    client = detect_client(query) or default_client or "client_c"

    # Extract target count (default 500)
    target = extract_target(query) or 500

    # Extract geo
    geo = extract_geo(query)

    # Strip metadata to get niche
    niche = strip_metadata(query)
    if not niche:
        niche = query.strip()

    # Route
    if force_route in ("b2b", "local"):
        routing = force_route
        reason = "forced by --force flag"
    else:
        routing, reason = detect_routing(niche, geo=geo, client=client)

    # Generate slug
    slug_base = re.sub(r"[^a-z0-9]+", "-", niche.lower()).strip("-")[:40]
    slug = f"{slug_base}-{datetime.now().strftime('%Y%m%d')}"

    return {
        "client": client,
        "niche": niche,
        "target": target,
        "geo": geo,
        "routing": routing,
        "routing_reason": reason,
        "slug": slug,
        "keywords": [],  # filled in later if needed
        "_parsed_by": "regex",
    }


# ==============================================================================
# PIPELINE INVOCATION
# ==============================================================================

def get_output_dir(client, slug):
    return os.path.join(PROJECTS_DIR, client, "lead-runs", slug)


def run_b2b_pipeline(intent, extra_args=None):
    """Invoke lead_generator_v2.py with parsed intent."""
    outdir = get_output_dir(intent["client"], intent["slug"])

    # Generate keywords via Haiku if not already set
    if not intent.get("keywords"):
        print(f"  Generating keywords for \"{intent['niche']}\"...")
        keywords = generate_keywords_with_haiku(intent["niche"])
        if keywords:
            intent["keywords"] = keywords
            print(f"  Generated {len(keywords)} keyword variations")

    cmd = [
        PYTHON_BIN,
        os.path.join(SCRIPT_DIR, "lead_generator_v2.py"),
        "--client", intent["niche"],
        "--target", str(intent["target"]),
        "--name", intent["slug"],
        "--max-contacts", "2",
        "--max-companies", "5000",
        "--no-double-verify",
        "--output-dir", outdir,
    ]

    if intent.get("keywords"):
        cmd.extend(["--keywords", ";".join(intent["keywords"])])

    if intent.get("geo"):
        cmd.extend(["--geo", intent["geo"]])

    if extra_args:
        cmd.extend(extra_args)

    print(f"\n  → Running B2B pipeline (lead_generator_v2.py)")
    print(f"  → Output: {outdir}")
    print()

    result = subprocess.run(cmd, cwd=SCRIPT_DIR)
    return result.returncode, outdir


def run_local_pipeline(intent, extra_args=None):
    """Invoke lead_pipeline_v6.py with parsed intent."""
    outdir = get_output_dir(intent["client"], intent["slug"])

    cmd = [
        PYTHON_BIN,
        os.path.join(SCRIPT_DIR, "lead_pipeline_v6.py"),
        "--client", intent["niche"],
        "--name", intent["slug"],
    ]

    # lead_pipeline_v6.py supports --cities
    if intent.get("geo"):
        # Convert geo to a city preset or single city
        geo = intent["geo"]
        if geo in ("us", "united states", "usa"):
            cmd.extend(["--cities", "us_top_30"])
        elif " " not in geo:
            # Single word state or city — pass directly
            cmd.extend(["--cities", geo])
        else:
            cmd.extend(["--cities", geo])

    if extra_args:
        cmd.extend(extra_args)

    print(f"\n  → Running LOCAL pipeline (lead_pipeline_v6.py)")
    print(f"  → Output: {outdir}")
    print()

    result = subprocess.run(cmd, cwd=SCRIPT_DIR)
    return result.returncode, outdir


# ==============================================================================
# OPTIONAL DISCOVERY MODULES (removable — set ENABLED flags to False to disable)
# ==============================================================================

# ── AI Ark Lookalike (B2B professional services) ──────────────────────────
# Finds companies similar to seed domains via LinkedIn data.
# To disable: set AIARK_ENABLED = False (or remove this entire section)
AIARK_ENABLED = True

def run_aiark_discovery(intent, seeds=None, budget=500):
    """Run AI Ark lookalike discovery as a pre-step before B2B pipeline.
    Returns list of company dicts or empty list. Does NOT replace the main pipeline.
    """
    if not AIARK_ENABLED:
        return []
    try:
        from v2_aiark import lookalike_discover, find_people, check_credits
        from v2_cache import load_cache, save_cache
    except ImportError:
        print("  [WARN] AI Ark module not found — skipping")
        return []

    # Load env
    for env_path in (
        os.path.join(WORKSPACE_ROOT, ".env"),
        os.path.join(SCRIPT_DIR, ".env"),
    ):
        if os.path.isfile(env_path):
            for line in open(env_path):
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                k, v = line.split("=", 1)
                k = k.strip()
                v = v.strip().strip('"').strip("'")
                if k and not os.environ.get(k):
                    os.environ[k] = v

    api_key = os.environ.get("AIARK_API_KEY", "")
    if not api_key:
        print("  [WARN] AIARK_API_KEY not set — skipping AI Ark")
        return []

    credits = check_credits(api_key)
    if credits < 10:
        print(f"  [WARN] AI Ark credits too low ({credits:.0f}) — skipping")
        return []

    if not seeds:
        print("  [INFO] No seed domains for AI Ark lookalike — skipping")
        print("         Use --seeds 'domain1.com;domain2.com' to provide seeds")
        return []

    max_pages = min(budget // 10, 50)  # ~0.1 credits per page
    print(f"  → AI Ark lookalike: {len(seeds)} seeds, max {max_pages} pages, {credits:.0f} credits available")

    cache_file = os.path.join(SCRIPT_DIR, "v2_pipeline_cache.json")
    cache = {}
    try:
        cache = json.load(open(cache_file)) if os.path.isfile(cache_file) else {}
    except Exception:
        pass

    companies = lookalike_discover(seeds, api_key, max_pages=max_pages, cache=cache, cache_file=cache_file)
    print(f"  → AI Ark found {len(companies)} companies")
    return companies


# ── Firecrawl Directory Discovery (trades/niche companies) ────────────────
# Scrapes industry directories to find trades companies Blitz/AI Ark can't.
# To disable: set FIRECRAWL_ENABLED = False (or remove this entire section)
FIRECRAWL_ENABLED = True

def run_firecrawl_discovery(intent):
    """Run Firecrawl + Playwright directory discovery.

    1. Check niche directory registry for KNOWN URLs
    2. Scrape known directories (Firecrawl for static, Playwright for interactive)
    3. Also do generic search for directories not in registry

    Returns list of company dicts.
    """
    if not FIRECRAWL_ENABLED:
        return []

    import shutil
    has_firecrawl = shutil.which("firecrawl")
    has_playwright = False
    try:
        from playwright.sync_api import sync_playwright
        has_playwright = True
    except ImportError:
        pass

    if not has_firecrawl and not has_playwright:
        print("  [INFO] Neither Firecrawl nor Playwright available — skipping directory discovery")
        return []

    niche = intent.get("niche", "")
    print(f"  → Directory discovery for '{niche}'")

    outdir = os.path.join(SCRIPT_DIR, ".firecrawl")
    os.makedirs(outdir, exist_ok=True)
    slug = re.sub(r"[^a-z0-9]+", "-", niche.lower()).strip("-")[:30]

    companies = []

    # ── Step 1: Check niche directory registry ──
    try:
        sys.path.insert(0, os.path.join(SCRIPT_DIR, "tools"))
        from enrich_smart_route import NICHE_DIRECTORIES

        known_dirs = []
        niche_lower = niche.lower()
        for key, dirs in NICHE_DIRECTORIES.items():
            if key in niche_lower or niche_lower in key:
                known_dirs = dirs
                break

        if known_dirs:
            print(f"  → Found {len(known_dirs)} known directories in registry")
            for d in known_dirs:
                dir_name = d.get("name", "unknown")
                dir_url = d.get("url", "")
                dir_type = d.get("type", "static")

                if not dir_url:
                    continue

                print(f"    → {dir_name} ({dir_type}): {dir_url[:60]}...")

                if dir_type == "static" and has_firecrawl:
                    # Try CRAWL first (follows pagination, gets ALL pages)
                    # Falls back to single scrape if crawl fails
                    crawl_dir = os.path.join(outdir, f"crawl-{slug}-{dir_name.lower().replace(' ','-')}")
                    page_file = os.path.join(outdir, f"registry-{slug}-{dir_name.lower().replace(' ','-')}.md")
                    crawl_success = False

                    try:
                        # Firecrawl crawl — follows links, gets all pages, more data per credit
                        result = subprocess.run(
                            ["firecrawl", "crawl", dir_url, "--limit", "20",
                             "-o", crawl_dir, "--format", "markdown"],
                            capture_output=True, text=True, timeout=120, cwd=SCRIPT_DIR,
                        )
                        if os.path.isdir(crawl_dir):
                            # Read all crawled pages
                            content = ""
                            for f_name in os.listdir(crawl_dir):
                                if f_name.endswith(".md"):
                                    content += open(os.path.join(crawl_dir, f_name)).read() + "\n"
                            crawl_success = len(content) > 500
                            if crawl_success:
                                print(f"      crawled: {len(os.listdir(crawl_dir))} pages")
                    except Exception:
                        pass

                    if not crawl_success:
                        # Fallback to single page scrape
                        try:
                            subprocess.run(
                                ["firecrawl", "scrape", dir_url, "-o", page_file],
                                capture_output=True, text=True, timeout=30, cwd=SCRIPT_DIR,
                            )
                            if os.path.isfile(page_file):
                                content = open(page_file).read()
                        except Exception as e:
                            print(f"      scrape failed: {e}")
                            content = ""

                    if content:
                        from urllib.parse import urlparse
                        urls_found = re.findall(r'https?://(?:www\.)?([a-zA-Z0-9.-]+\.[a-zA-Z]{2,})', content)
                        skip_domains = {"yelp.com", "google.com", "facebook.com", "linkedin.com",
                                       "twitter.com", "instagram.com", "youtube.com", "bbb.org"}
                        for domain in set(urls_found):
                            domain = domain.lower().strip(".")
                            if domain and not any(s in domain for s in skip_domains) and len(domain) > 5:
                                companies.append({"name": "", "domain": domain, "source": f"registry_{dir_name}"})
                        print(f"      domains found: {len(set(urls_found))}")

                elif dir_type == "interactive" and has_playwright:
                    # Playwright scrape
                    try:
                        from playwright.sync_api import sync_playwright as _sp
                        with _sp() as p:
                            browser = p.chromium.launch(headless=True)
                            page = browser.new_page()
                            page.goto(dir_url, wait_until="networkidle", timeout=15000)
                            text = page.inner_text("body")
                            # Extract domains from the page text
                            from urllib.parse import urlparse
                            urls_found = re.findall(r'https?://(?:www\.)?([a-zA-Z0-9.-]+\.[a-zA-Z]{2,})', page.content())
                            skip_domains = {"yelp.com", "google.com", "facebook.com", "linkedin.com"}
                            for domain in set(urls_found):
                                domain = domain.lower().strip(".")
                                if domain and not any(s in domain for s in skip_domains) and len(domain) > 5:
                                    companies.append({"name": "", "domain": domain, "source": f"playwright_{dir_name}"})
                            print(f"      playwright: {len(urls_found)} domains found")
                            browser.close()
                    except Exception as e:
                        print(f"      playwright failed: {e}")
    except ImportError:
        pass

    # ── Step 2: Use Firecrawl MAP on discovered directory sites to find ALL pages ──
    if has_firecrawl and companies:
        # Get unique directory domains from step 1
        dir_domains = set()
        for c in companies:
            src = c.get("source", "")
            if "registry" in src and c.get("domain"):
                # This is a domain FROM a directory — the directory site itself is different
                pass

    # ── Step 3: Generic Firecrawl search (catches directories not in registry) ──
    if has_firecrawl:
        outfile = os.path.join(outdir, f"search-{slug}.json")
        try:
            subprocess.run(
                ["firecrawl", "search", f"{niche} companies directory", "--limit", "5",
                 "--json", "-o", outfile],
                capture_output=True, text=True, timeout=60, cwd=SCRIPT_DIR,
            )
        except Exception:
            pass

        if os.path.isfile(outfile):
            try:
                data = json.load(open(outfile))
                web = data.get("data", {}).get("web", []) if isinstance(data.get("data"), dict) else []
                for r in web[:3]:
                    url = r.get("url", "")
                    title = r.get("title", "")
                    if not url or any(skip in url for skip in ["yelp.com", "google.com", "facebook.com"]):
                        continue
                    print(f"    → Generic: {title[:50]}...")
                    page_file = os.path.join(outdir, f"page-{slug}-generic-{len(companies)}.md")
                    try:
                        subprocess.run(
                            ["firecrawl", "scrape", url, "-o", page_file],
                            capture_output=True, text=True, timeout=30, cwd=SCRIPT_DIR,
                        )
                        if os.path.isfile(page_file):
                            content = open(page_file).read()
                            from urllib.parse import urlparse
                            urls_found = re.findall(r'https?://(?:www\.)?([a-zA-Z0-9.-]+\.[a-zA-Z]{2,})', content)
                            skip_domains = {"yelp.com", "google.com", "facebook.com", "linkedin.com",
                                           "twitter.com", "instagram.com", "youtube.com", "bbb.org"}
                            for domain in set(urls_found):
                                domain = domain.lower().strip(".")
                                if domain and not any(s in domain for s in skip_domains) and len(domain) > 5:
                                    companies.append({"name": "", "domain": domain, "source": "firecrawl_generic"})
                    except Exception:
                        continue
            except Exception:
                pass

    # Dedup
    seen = set()
    unique = []
    for c in companies:
        if c.get("domain") and c["domain"] not in seen:
            seen.add(c["domain"])
            unique.append(c)

    print(f"  → Directory discovery total: {len(unique)} unique company domains")
    return unique


def count_final_leads(output_dir):
    """Count leads in smartlead_import.csv if it exists."""
    path = os.path.join(output_dir, "smartlead_import.csv")
    if not os.path.exists(path):
        return 0
    try:
        with open(path) as f:
            return max(0, sum(1 for _ in f) - 1)  # minus header
    except Exception:
        return 0


# ==============================================================================
# MAIN
# ==============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Smart lead router — natural language entry point",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  lead.py "find me 1000 fire protection firms for client_c"
  lead.py "500 MSPs for client_b"
  lead.py "hotels in Denver for client_a, 300 leads"
  lead.py "cost seg firms" --target 500 --dry-run
  lead.py "fire protection" --force b2b
""",
    )
    parser.add_argument("query", nargs="?", default="", help="Natural language request")
    parser.add_argument("--client", help="Override detected client")
    parser.add_argument("--niche", help="Override detected niche")
    parser.add_argument("--target", type=int, help="Override target lead count")
    parser.add_argument("--geo", help="Override detected geography")
    parser.add_argument("--force", choices=["b2b", "local"],
                        help="Force pipeline choice (overrides auto-routing)")
    parser.add_argument("--no-fallback", action="store_true",
                        help="Don't try the other pipeline if primary comes up short")
    parser.add_argument("--dry-run", action="store_true",
                        help="Show parsed intent and planned routing without running")
    parser.add_argument("--aiark", action="store_true",
                        help="Enable AI Ark lookalike discovery (B2B niches)")
    parser.add_argument("--aiark-budget", type=int, default=100,
                        help="AI Ark credit budget (default 100 = ~1000 companies)")
    parser.add_argument("--seeds", type=str, default="",
                        help="Seed domains for AI Ark lookalike (semicolon-separated)")
    parser.add_argument("--firecrawl", action="store_true",
                        help="Enable Firecrawl directory discovery (trades niches)")
    parser.add_argument("--no-aiark", action="store_true",
                        help="Disable AI Ark even if --aiark is set")
    parser.add_argument("--no-firecrawl", action="store_true",
                        help="Disable Firecrawl even if --firecrawl is set")

    args = parser.parse_args()

    if not args.query and not args.niche:
        parser.error("Provide a natural language query or --niche")

    query = args.query or args.niche
    print("=" * 70)
    print(f"  Query: \"{query}\"")
    print("=" * 70)

    # Parse intent
    intent = parse_intent(query, default_client=args.client, force_route=args.force)

    # CLI overrides
    if args.client:
        intent["client"] = args.client
    if args.niche:
        intent["niche"] = args.niche
    if args.target:
        intent["target"] = args.target
    if args.geo:
        intent["geo"] = args.geo

    # Show parsed intent
    parser_tag = f" [via {intent.get('_parsed_by', '?')}]" if intent.get("_parsed_by") else ""
    discovery = intent.get("discovery_method", "blitz" if intent["routing"] == "b2b" else "serper_maps")
    intent["discovery_method"] = discovery  # ensure it's set

    DISCOVERY_LABELS = {
        "blitz": "Blitz keyword ($0, LinkedIn-heavy B2B)",
        "directory": "Firecrawl directories (cheapest, trades/niche)",
        "serper_maps": "Serper Maps (local businesses, geo-grid)",
        "hybrid": "Hybrid (multiple sources)",
    }

    print(f"  Client:       {intent['client']}")
    print(f"  Niche:        {intent['niche']}")
    print(f"  Target:       {intent['target']} leads")
    print(f"  Geo:          {intent['geo'] or '(none)'}")
    print(f"  Routing:      {intent['routing']} ({intent['routing_reason']}){parser_tag}")
    print(f"  Discovery:    {DISCOVERY_LABELS.get(discovery, discovery)}")
    if intent.get("discovery_reason"):
        print(f"                {intent['discovery_reason']}")
    if intent.get("directory_url"):
        print(f"  Directory:    {intent['directory_url']}")
    if intent.get("title_filter"):
        print(f"  Title filter: {intent['title_filter']}")
    if intent.get("company_size"):
        print(f"  Company size: {intent['company_size']}")
    if intent.get("extra_instructions"):
        print(f"  Extra:        {intent['extra_instructions']}")
    print(f"  Output slug:  {intent['slug']}")
    print()

    # Handle unclear routing
    if intent["routing"] == "unclear":
        print("  ⚠️  Routing is unclear. Please specify --force b2b or --force local")
        print()
        print("     b2b   = Find FIRMS/COMPANIES in this niche (Blitz + AI Ark)")
        print("     local = Find LOCAL BUSINESSES with physical locations (Serper Maps)")
        return 1

    # ── Smart discovery routing ──
    # Routes to cheapest effective source based on discovery_method
    extra_args = []
    discovery = intent.get("discovery_method", "blitz" if intent["routing"] == "b2b" else "serper_maps")

    # Always run pre-discovery if AI Ark or Firecrawl flags are set
    # OR if the auto-router picked directory/hybrid
    run_aiark = (args.aiark or discovery in ("hybrid",)) and not args.no_aiark and AIARK_ENABLED
    run_fc = (args.firecrawl or discovery in ("directory", "hybrid")) and not args.no_firecrawl and FIRECRAWL_ENABLED

    pre_discovery_companies = []

    # AI Ark lookalike (B2B niches — costs 0.1 credits/10 companies)
    if run_aiark:
        seeds = [s.strip() for s in args.seeds.split(";") if s.strip()] if args.seeds else []
        aiark_companies = run_aiark_discovery(intent, seeds=seeds, budget=args.aiark_budget)
        if aiark_companies:
            pre_discovery_companies.extend(aiark_companies)
            extra_args.extend(["--aiark", "--aiark-budget", str(args.aiark_budget)])
            if args.seeds:
                extra_args.extend(["--seeds", args.seeds])

    # Firecrawl directory discovery (trades — costs 1 credit/page, finds 500+ companies)
    if run_fc:
        fc_companies = run_firecrawl_discovery(intent)
        if fc_companies:
            pre_discovery_companies.extend(fc_companies)
            fc_domains_file = os.path.join(get_output_dir(intent["client"], intent["slug"]),
                                           "firecrawl_domains.txt")
            os.makedirs(os.path.dirname(fc_domains_file), exist_ok=True)
            with open(fc_domains_file, "w") as f:
                for c in fc_companies:
                    if c.get("domain"):
                        f.write(c["domain"] + "\n")
            print(f"  → Saved {len(fc_companies)} Firecrawl domains to {fc_domains_file}")

    # Dedup pre-discovery against master DB BEFORE spending enrichment credits
    if pre_discovery_companies:
        try:
            sys.path.insert(0, os.path.join(SCRIPT_DIR, "tools"))
            from dedup_before_enrich import filter_new_domains
            before = len(pre_discovery_companies)
            pre_discovery_companies = filter_new_domains(pre_discovery_companies)
            skipped = before - len(pre_discovery_companies)
            print(f"\n  Pre-discovery: {before} found → {skipped} already in DB → {len(pre_discovery_companies)} genuinely new")
        except ImportError:
            print(f"\n  Pre-discovery total: {len(pre_discovery_companies)} companies (dedup module not found)")
        print()

    # ── CASCADE DISCOVERY ──
    # Try sources in order of cost until target is met.
    # Each source only runs if the previous ones didn't produce enough leads.
    # Cheapest first: Blitz ($0) → AI Ark (0.1 cr) → Firecrawl (1 cr) → Serper Maps ($$$)

    target = intent["target"]
    total_found = 0
    all_outdirs = []

    # Build the cascade order based on discovery_method
    # The auto-router already picked the BEST primary source.
    # The cascade adds the others as fallbacks.
    cascade = []

    if discovery == "blitz" or intent["routing"] == "b2b":
        cascade.append(("blitz", "Blitz keyword search ($0)"))
        if AIARK_ENABLED and not args.no_aiark:
            cascade.append(("aiark", "AI Ark lookalike (0.1 credits/10 companies)"))
        if FIRECRAWL_ENABLED and not args.no_firecrawl:
            cascade.append(("firecrawl", "Firecrawl directories (1 credit/page)"))
        cascade.append(("serper_local", "Serper Maps fallback"))
    elif discovery == "directory":
        if FIRECRAWL_ENABLED and not args.no_firecrawl:
            cascade.append(("firecrawl", "Firecrawl directories (1 credit/page)"))
        cascade.append(("blitz", "Blitz keyword search ($0)"))
        if AIARK_ENABLED and not args.no_aiark:
            cascade.append(("aiark", "AI Ark lookalike (0.1 credits/10 companies)"))
        cascade.append(("serper_local", "Serper Maps fallback"))
    else:  # serper_maps or hybrid
        cascade.append(("serper_local", "Serper Maps"))
        cascade.append(("blitz", "Blitz keyword search ($0)"))
        if FIRECRAWL_ENABLED and not args.no_firecrawl:
            cascade.append(("firecrawl", "Firecrawl directories (1 credit/page)"))

    print(f"  Cascade order ({len(cascade)} sources):")
    for i, (src, label) in enumerate(cascade):
        print(f"    {i+1}. {label}")
    print()

    if args.dry_run:
        print("  [DRY RUN] Would cascade through the sources above. Exiting.")
        return 0

    for step, (source, label) in enumerate(cascade):
        if total_found >= target:
            print(f"\n  ✓ Target reached ({total_found}/{target}). Skipping remaining sources.")
            break

        deficit = target - total_found
        step_intent = dict(intent)
        step_intent["target"] = deficit
        if step > 0:
            step_intent["slug"] = intent["slug"] + f"-cascade-{step}"

        print(f"\n  ── CASCADE STEP {step+1}: {label} (need {deficit} more) ──")

        code = 1
        outdir = get_output_dir(step_intent["client"], step_intent["slug"])

        if source == "blitz":
            code, outdir = run_b2b_pipeline(step_intent, extra_args=extra_args)
        elif source == "aiark":
            seeds = [s.strip() for s in args.seeds.split(";") if s.strip()] if args.seeds else []
            if seeds:
                aiark_companies = run_aiark_discovery(step_intent, seeds=seeds, budget=args.aiark_budget)
                if aiark_companies:
                    print(f"  → AI Ark found {len(aiark_companies)} companies (enrichment via Blitz)")
                    # Run B2B pipeline to enrich AI Ark discoveries
                    extra = list(extra_args) + ["--aiark", "--aiark-budget", str(args.aiark_budget), "--seeds", args.seeds]
                    code, outdir = run_b2b_pipeline(step_intent, extra_args=extra)
                else:
                    print(f"  → AI Ark: no seeds provided or no results. Use --seeds 'domain1;domain2'")
                    continue
            else:
                print(f"  → AI Ark: no seeds provided. Use --seeds 'domain1;domain2'. Skipping.")
                continue
        elif source == "firecrawl":
            fc_companies = run_firecrawl_discovery(step_intent)
            if fc_companies:
                # Dedup
                try:
                    sys.path.insert(0, os.path.join(SCRIPT_DIR, "tools"))
                    from dedup_before_enrich import filter_new_domains
                    fc_companies = filter_new_domains(fc_companies)
                except ImportError:
                    pass
                if fc_companies:
                    fc_domains_file = os.path.join(outdir, "firecrawl_domains.txt")
                    os.makedirs(outdir, exist_ok=True)
                    with open(fc_domains_file, "w") as f:
                        for c in fc_companies:
                            if c.get("domain"):
                                f.write(c["domain"] + "\n")
                    print(f"  → Firecrawl: {len(fc_companies)} new companies. Enriching via Blitz...")
                    code, outdir = run_b2b_pipeline(step_intent, extra_args=extra_args)
                else:
                    print(f"  → Firecrawl: all found companies already in DB")
                    continue
            else:
                print(f"  → Firecrawl: no directories found for this niche")
                continue
        elif source == "serper_local":
            code, outdir = run_local_pipeline(step_intent, extra_args=extra_args)

        if code == 0:
            step_found = count_final_leads(outdir)
            total_found += step_found
            all_outdirs.append(outdir)
            print(f"\n  ✓ Step {step+1} found {step_found} leads (running total: {total_found}/{target})")

            # Auto-enrich with forge_enrich if Blitz enrichment rate was low
            # Check: if companies were discovered but few emails found, run forge_enrich
            companies_file = os.path.join(outdir, "companies.csv")
            if os.path.isfile(companies_file) and step_found < deficit * 0.3:
                try:
                    import csv as _csv
                    companies_count = sum(1 for _ in open(companies_file)) - 1
                    enrichment_rate = step_found / max(companies_count, 1)
                    if enrichment_rate < 0.3 and companies_count > 20:
                        print(f"\n  ⚠️  Low enrichment rate ({enrichment_rate:.0%}). Running Forge enrichment on unenriched companies...")
                        forge_cmd = [
                            PYTHON_BIN, os.path.join(SCRIPT_DIR, "tools", "forge_enrich.py"),
                            "--input", companies_file,
                            "--niche", intent["niche"],
                            "--client", intent["client"],
                        ]
                        forge_result = subprocess.run(forge_cmd, cwd=SCRIPT_DIR)
                        if forge_result.returncode == 0:
                            # Check how many more leads were found
                            forge_niche_slug = re.sub(r"[^a-z0-9]+", "-", intent["niche"].lower()).strip("-")[:30]
                            forge_outdir = os.path.join(PROJECTS_DIR, intent["client"], "lead-runs",
                                                       f"{forge_niche_slug}-forge-{datetime.now().strftime('%Y%m%d')}")
                            forge_found = count_final_leads(forge_outdir)
                            total_found += forge_found
                            if forge_found:
                                all_outdirs.append(forge_outdir)
                            print(f"  ✓ Forge enrichment found {forge_found} additional leads (total: {total_found}/{target})")
                except Exception as e:
                    print(f"  [WARN] Forge auto-enrich failed: {e}")
        else:
            print(f"\n  ⚠️  Step {step+1} exited with code {code}")

        if args.no_fallback:
            break

    # Final summary
    print(f"\n{'='*60}")
    print(f"  CASCADE COMPLETE")
    print(f"{'='*60}")
    print(f"  target:         {target}")
    print(f"  total found:    {total_found}")
    print(f"  sources used:   {len(all_outdirs)}")
    if total_found >= target:
        print(f"  status:         ✓ TARGET MET")
    else:
        print(f"  status:         ⚠️ SHORT by {target - total_found}")
    for od in all_outdirs:
        print(f"  output:         {od}")
    print(f"{'='*60}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
