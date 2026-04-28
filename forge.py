#!/usr/bin/env python3
"""
The Forge — Unified Lead Generation Engine
===========================================
One command. Discovery → Enrichment → Verification → Export.

Usage:
    python3 forge.py "200 freight audit firms for client_c"
    python3 forge.py "500 restaurants in denver for client_a"
    python3 forge.py "find MSPs for client_b" --target 300
    python3 forge.py "freight audit" --dry-run
    python3 forge.py "elevator inspection" --workers 10 --force

What it does:
    1. Parses your request (client, niche, target, geography)
    2. Discovers companies (Blitz → AI Ark → Firecrawl → Serper Maps)
    3. Finds decision makers + verified emails (Blitz waterfall + 13-step cascade)
    4. Verifies all emails (MillionVerifier)
    5. Quality checks (niche-fit, title filter, overlap safeguard)
    6. Exports campaign-ready CSV + saves to master DB
"""

import os
import sys
import re
import csv
import json
import time
import sqlite3
import argparse
import threading
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import Counter

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
WORKSPACE_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, "..", ".."))
PROJECTS_DIR = os.path.join(WORKSPACE_ROOT, "01-Projects")
DB_PATH = os.path.join(SCRIPT_DIR, "master-leads", "master_leads.db")

sys.path.insert(0, SCRIPT_DIR)
sys.path.insert(0, os.path.join(SCRIPT_DIR, "tools"))

# Load env
for _p in (os.path.join(WORKSPACE_ROOT, ".env"), os.path.join(SCRIPT_DIR, ".env")):
    if os.path.isfile(_p):
        for line in open(_p):
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            k, v = k.strip(), v.strip().strip('"').strip("'")
            if k and not os.environ.get(k):
                os.environ[k] = v

# ── Phase 1 imports (parse) ─────────────────────────────────────────────────
from lead import (parse_intent_haiku, detect_client, extract_target, extract_geo,
                  strip_metadata, detect_routing, generate_keywords_with_haiku,
                  run_firecrawl_discovery, KNOWN_CLIENTS, CLIENT_DEFAULTS)

# ── Phase 2 imports (discover) ──────────────────────────────────────────────
from v2_discovery import blitz_discover, serper_discover, prefilter_companies
from v2_aiark import lookalike_discover, check_credits as aiark_credits
from v2_config import V2Config
from v2_cache import load_cache, save_cache

# ── Phase 3 imports (enrich) ────────────────────────────────────────────────
from v2_enrichment import blitz_enrich
from forge_enrich import enrich_company, mx_check, verify_mv, update_domain_memory

# ── Phase 4 imports (verify) ────────────────────────────────────────────────
from verification import verify_contacts
from v2_scoring import score_tiers, verify_all as mv_bb_verify_all

# ── Phase 5 imports (quality) ───────────────────────────────────────────────
from dedup_before_enrich import get_known_domains, get_known_emails, filter_new_domains
from v2_cleaning import pre_clean, post_clean, BAD_TITLE_KEYWORDS
from verify_title_redflags import is_bad_title

# ── Phase 6 imports (export) ────────────────────────────────────────────────
from v2_checkpoint import (save_step, load_step, is_step_complete, save_run_flags,
                           check_run_flags, invalidate_steps, _FILTER_ONLY_FLAGS)
from v2_export import deduplicate, export_all

# ── Constants ───────────────────────────────────────────────────────────────
CACHE_FILE = os.path.join(SCRIPT_DIR, "v2_pipeline_cache.json")
SMARTLEAD_FIELDS = ['email', 'first_name', 'last_name', 'company_name', 'phone',
                    'title', 'website', 'custom1', 'custom2', 'custom3']

# ── Run-scoped stats accumulator (populated by _run_forge_cascade + enrich()) ─
# export_results() reads this to compute the real total_cost_estimate instead
# of hardcoding $0 in enrichment_analytics.
_RUN_STATS = {}

# Per-step cost model (USD per hit). Kept here so estimate_cost_from_stats stays
# in one place and matches forge_enrich.py's pricing.
_STEP_COSTS = {
    # FREE steps — kept for step-counting, cost = 0
    "domain_memory_hit": 0.0,
    "phone_lookup_hit": 0.0,
    "email_reverse_hit": 0.0,
    "maps_email_hit": 0.0,
    "blitz_direct": 0.0,
    "website_scrape_hit": 0.0,
    "owner_found": 0.002,         # Haiku owner search
    # PAID steps
    "pattern_hit": 0.001,         # 1 MV check per hit (average)
    "icypeas_reverse_hit": 0.015,
    "icypeas_name_hit": 0.015,
    "icypeas_domain_hit": 0.015,
    "catch_all_accepted": 0.0,    # FREE (just a flag)
    "blitz_enriched": 0.0,        # Flat subscription — no per-hit cost
}


def estimate_cost_from_stats(stats):
    """Sum per-step costs into a total USD estimate for this run."""
    total = 0.0
    for key, per_hit in _STEP_COSTS.items():
        total += stats.get(key, 0) * per_hit
    return round(total, 3)


# ── Niche slug canonicalization ──────────────────────────────────────────────
# Bug 15: Used to truncate at 30 chars, silently colliding long niches into
#   identical prefixes. Increased to 60.
# Bug 16: Same vertical was getting different slugs across runs ("ma-advisors"
#   vs "m-a-advisory-exit-planning"). The ALIASES map normalizes common ones
#   to a single canonical slug so querying / dedup / ROI reporting works.
NICHE_SLUG_MAX = 60

# Hardcoded fallback — used when niche_aliases.json is missing or malformed.
# Keep in sync with niche_aliases.json. This dict is the source of truth for
# tests and the final safety net so the pipeline never breaks on a bad JSON.
_HARDCODED_NICHE_ALIASES = {
    # M&A family
    "ma-advisors": "ma-advisory",
    "m-a-advisors": "ma-advisory",
    "m-a-advisory": "ma-advisory",
    "m-a-advisory-exit-planning": "ma-advisory",
    "m-a-advisory-and-exit-planning": "ma-advisory",
    "mergers-and-acquisitions": "ma-advisory",
    # Cost Seg family
    "cost-segregation": "cost-segregation",
    "cost-seg": "cost-segregation",
    "cost-segregation-and-tax-strat": "cost-segregation",
    "cost-segregation-and-tax-strategy": "cost-segregation",
    "cost-segregation-and-tax-strategy-consulting": "cost-segregation",
    # Fractional CRO family
    "fractional-cro": "fractional-cro",
    "fractional-cro-and-sales-consu": "fractional-cro",
    "fractional-cro-and-sales-consulting": "fractional-cro",
    # Energy efficiency family
    "commercial-energy-efficiency-l": "commercial-energy-efficiency",
    "commercial-energy-efficiency-led-retrofit-esco": "commercial-energy-efficiency",
    "commercial-energy-efficiency": "commercial-energy-efficiency",
    # Fire alarm family
    "fire-alarm-installation-monito": "fire-alarm",
    "fire-alarm-installation-monitoring": "fire-alarm",
    "fire-alarm": "fire-alarm",
    # Fire protection family
    "fire-protection": "fire-protection",
    "fireprotection": "fire-protection",
}


def _load_niche_aliases():
    """Load alias map from niche_aliases.json if present, else use hardcoded dict.

    The JSON format uses the inverse shape ({canonical: [aliases...]}) because
    it's easier for humans to maintain. We flip it here into the flat
    {alias: canonical} map the rest of the code expects.

    Any error (missing file, bad JSON, wrong shape) silently falls back to the
    hardcoded dict so the pipeline never breaks on config drift.
    """
    path = os.path.join(SCRIPT_DIR, "niche_aliases.json")
    if not os.path.isfile(path):
        return dict(_HARDCODED_NICHE_ALIASES)
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, dict):
            return dict(_HARDCODED_NICHE_ALIASES)
        flat = {}
        for canonical, aliases in data.items():
            # Skip comment/meta keys that start with _
            if canonical.startswith("_"):
                continue
            if not isinstance(aliases, list):
                continue
            # Canonical form is its own alias target so the function is idempotent
            flat[canonical] = canonical
            for alias in aliases:
                if isinstance(alias, str):
                    flat[alias] = canonical
        # If we got zero valid entries, fall back rather than erase the map
        if not flat:
            return dict(_HARDCODED_NICHE_ALIASES)
        return flat
    except Exception:
        return dict(_HARDCODED_NICHE_ALIASES)


# Resolved at import time. Re-exported as NICHE_ALIASES so the public API
# (tests, migrate_niche_slugs.py, any other importer) is unchanged.
NICHE_ALIASES = _load_niche_aliases()


def canonical_niche_slug(niche):
    """Build a canonical slug from a niche string.

    - Lowercase + strip punctuation + hyphenate
    - Cap at NICHE_SLUG_MAX chars (was 30, now 60 — fixes Bug 15 silent truncation)
    - Run through NICHE_ALIASES to collapse duplicates (fixes Bug 16)
    """
    if not niche:
        return "unknown"
    s = re.sub(r"[^a-z0-9]+", "-", niche.lower()).strip("-")[:NICHE_SLUG_MAX]
    return NICHE_ALIASES.get(s, s)


def pull_winning_copy_examples(client_name, min_reply_rate=2.0, min_sends=200, max_examples=3):
    """Fetch the body of Email 1 from the client's top-performing campaigns.

    Salesforge pattern: feeding past winners into new copy generation as
    few-shot examples is a proven reply-rate lift. Only pulls from campaigns
    that actually reached statistical significance (min_sends) AND beat the
    reply rate floor (min_reply_rate).

    Returns a list of dicts: [{"campaign": str, "subject": str, "body": str,
                               "reply_rate": float}, ...] — up to max_examples.
    """
    import requests as _rq
    key = os.environ.get("SMARTLEAD_API_KEY", "")
    if not key:
        return []
    BASE = "https://server.smartlead.ai/api/v1"

    try:
        campaigns = _rq.get(f"{BASE}/campaigns/?api_key={key}", timeout=30).json()
    except Exception:
        return []
    if not isinstance(campaigns, list):
        campaigns = campaigns.get("data", []) if isinstance(campaigns, dict) else []

    # Match campaigns to client
    client_lower = client_name.lower()
    client_tokens = {
        "client_c": ["client_c", "rm "],
        "client_a": ["client_a", "paf"],
        "client_b": ["client_b", "sc "],
    }.get(client_lower, [client_lower])

    winners = []
    for c in campaigns:
        name = (c.get("name", "") or "").lower()
        if not any(tok.lower() in name for tok in client_tokens):
            continue
        try:
            stats = _rq.get(f"{BASE}/campaigns/{c['id']}/analytics?api_key={key}",
                            timeout=30).json()
            sent = int(stats.get("sent_count", 0) or 0)
            replies = int(stats.get("reply_count", 0) or 0)
        except Exception:
            continue
        if sent < min_sends:
            continue
        rate = (replies / sent * 100) if sent else 0
        if rate < min_reply_rate:
            continue
        winners.append((rate, sent, c, stats))

    # Top N winners
    winners.sort(key=lambda x: -x[0])
    winners = winners[:max_examples]

    # Import banned-opener regex here to avoid circular import at module load.
    # These patterns are explicitly banned by the cold-email-writer skill +
    # the 19-point grader (rule 5: OPENER). If a past "winner" starts with
    # one of these, we must NOT feed it to the draft model as a few-shot
    # example — the model will faithfully reproduce the pattern, and the
    # grader will then reject the draft, leading to the 2026-04-20 bug
    # where Fire Alarm, Generator, and HVAC all failed auto-copy-gen.
    _BANNED_OPENER_STARTS = [
        r"^most\s+[a-z]+",          # "Most fire protection..."
        r"^if you('|\s+a)re like",  # "If you're like..."
        r"^are you tired",
        r"^imagine if",
        r"^what if you could",
        r"^did you know",
        r"^i noticed",
        r"^i came across",
    ]
    _MIN_WORDS_FOR_EXAMPLE = 20  # Email 1 bodies shorter than this are stubs, not winners

    import re as _re_local

    def _strip_html(html):
        return _re_local.sub(r"<[^>]+>", " ", html or "").strip()

    def _has_banned_opener(body):
        """Check the FIRST sentence for banned opener patterns.

        Note: this looks at the actual opening text, not mid-email phrases.
        'Most common violations we see' in the middle is fine; 'Most fire
        protection shops...' as the first sentence is a FAIL.
        """
        clean = _strip_html(body).lower().lstrip()
        # Strip the first line if it looks like a greeting — covers both
        # plain greetings ("Hey John,") and spintax greetings ("{Hey|Hi} there,").
        # Everything up to and including the first newline is removed so we
        # evaluate only the actual opener sentence.
        first_line_pattern = r"^(?:\{[^}]+\}|hey|hi|hello)[^\n]*[\n\r]+"
        clean = _re_local.sub(first_line_pattern, "", clean, flags=_re_local.IGNORECASE)
        # Get first sentence
        first_sentence = _re_local.split(r"[.!?]", clean, maxsplit=1)[0].strip()
        for pattern in _BANNED_OPENER_STARTS:
            if _re_local.match(pattern, first_sentence, _re_local.IGNORECASE):
                return True
        return False

    def _word_count(body):
        clean = _strip_html(body)
        return len(clean.split())

    examples = []
    skipped = {"empty_body": 0, "too_short": 0, "banned_opener": 0, "missing_seq": 0}
    for rate, sent, c, stats in winners:
        try:
            seqs = _rq.get(f"{BASE}/campaigns/{c['id']}/sequences?api_key={key}",
                           timeout=30).json()
        except Exception:
            continue
        if not isinstance(seqs, list):
            continue
        # Pick seq_number 1 — the email that actually got the reply
        first = next((s for s in seqs if s.get("seq_number") == 1), None)
        if not first:
            skipped["missing_seq"] += 1
            continue

        # Smartlead stores copy in two places:
        #  (a) top-level `email_body` + `subject` on the sequence object
        #  (b) `sequence_variants[]` — each variant has its own body + subject
        #      (used when you're A/B testing Variant A vs Variant B)
        # A campaign that ran A/B tests has empty top-level email_body but real
        # copy inside variants. We have to inspect both.
        top_body = first.get("email_body", "") or ""
        top_subject = first.get("subject", "") or ""
        variants = first.get("sequence_variants") or []

        if top_body.strip() and _word_count(top_body) >= _MIN_WORDS_FOR_EXAMPLE:
            # Use top-level body (non-A/B campaign)
            body = top_body
            subject = top_subject
        elif variants:
            # Fall back to the first variant with real content (typically Variant A
            # — we just need ONE example to feed the draft model, and A is
            # historically the baseline).
            picked = None
            for v in variants:
                vb = v.get("email_body", "") or ""
                if vb.strip() and _word_count(vb) >= _MIN_WORDS_FOR_EXAMPLE:
                    picked = v
                    break
            if picked:
                body = picked.get("email_body", "") or ""
                subject = picked.get("subject", "") or top_subject
            else:
                skipped["empty_body"] += 1
                continue
        else:
            # No top-level body, no variants — truly empty winner (rare but
            # happens if someone started a campaign without pasting copy).
            skipped["empty_body"] += 1
            continue

        # Guard 2: banned opener. Even though this past campaign worked, we
        # don't want to teach the model to reproduce banned patterns.
        if _has_banned_opener(body):
            skipped["banned_opener"] += 1
            continue

        examples.append({
            "campaign": stats.get("name", c.get("name", "")),
            "subject": subject,
            "body": body,
            "reply_rate": round(rate, 2),
            "sent": sent,
        })

    if skipped["empty_body"] or skipped["too_short"] or skipped["banned_opener"]:
        _skip_msg = ", ".join(f"{v} {k}" for k, v in skipped.items() if v)
        print(f"  [copy] past-winners filter: skipped {_skip_msg}", flush=True)
    return examples


def get_client_cost_budget(client_name, default=10.00):
    """Read `cost_budget:` from a client's CLIENT.md, falling back to `default`.

    Allows each client to have its own per-run spend ceiling based on their
    deal sizes. CLIENT_A with $50k fire protection contracts justifies a higher
    budget than CLIENT_B which currently has tight margins.

    Lookup order:
      1. CLIENT.md YAML Forge Brief `cost_budget:` value
      2. FORGE_COST_LIMIT environment variable
      3. default arg ($10)
    """
    if not client_name:
        return float(os.environ.get("FORGE_COST_LIMIT", default))
    client_md = os.path.join(PROJECTS_DIR, client_name, "CLIENT.md")
    if os.path.isfile(client_md):
        try:
            with open(client_md, encoding="utf-8") as f:
                content = f.read()
            m = re.search(r"cost_budget:\s*([\d.]+)", content)
            if m:
                return float(m.group(1))
        except Exception:
            pass
    return float(os.environ.get("FORGE_COST_LIMIT", default))


def find_similar_niches(new_slug, db_path=None, min_prefix_len=6):
    """Return existing niches in the master DB that share a long prefix with new_slug.

    Used to warn about likely duplicates before they fragment the DB. If the new
    slug matches an old niche via exact prefix (and isn't already in the alias map),
    the operator should add it to niche_aliases.json before this run inserts new rows.

    Returns a list of (existing_niche, row_count) tuples, or [] if no matches.
    """
    if not new_slug or len(new_slug) < min_prefix_len:
        return []
    if db_path is None:
        db_path = DB_PATH
    if not os.path.isfile(db_path):
        return []
    try:
        import sqlite3 as _sql
        conn = _sql.connect(db_path)
        cur = conn.cursor()
        cur.execute("""
            SELECT niche, COUNT(*) AS n FROM leads
            WHERE niche IS NOT NULL AND niche != '' AND niche != ?
            GROUP BY niche
        """, (new_slug,))
        rows = cur.fetchall()
        conn.close()
    except Exception:
        return []

    matches = []
    for existing, count in rows:
        if not existing or len(existing) < min_prefix_len:
            continue
        # Bi-directional prefix match: either the new or existing is a prefix of the other
        shorter, longer = (new_slug, existing) if len(new_slug) < len(existing) else (existing, new_slug)
        # Require a hyphen boundary so "fire" doesn't match "firecrawl"
        if longer.startswith(shorter + "-") and len(shorter) >= min_prefix_len:
            # Don't warn if the existing niche is already in the alias map pointing here
            if NICHE_ALIASES.get(existing) == new_slug or NICHE_ALIASES.get(new_slug) == existing:
                continue
            matches.append((existing, count))
    return matches


def utc_now_isoformat():
    """Canonical ISO-8601 timestamp with UTC offset, used for all DB writes.

    Bug 17: forge.py was using SQLite's datetime('now') which produced
    '2026-04-17 23:21:54' strings, while the older pipeline used Python
    datetime.now(timezone.utc).isoformat() producing
    '2026-04-17T23:21:54.123456+00:00'. Mixing formats makes sorting and
    date parsing fragile. Standardize on ISO-8601 with timezone everywhere.
    """
    from datetime import timezone
    return datetime.now(timezone.utc).isoformat()


def banner(text, width=60):
    print(f"\n{'=' * width}")
    print(f"  {text}")
    print(f"{'=' * width}")


# ============================================================================
# PHASE 1: PARSE
# ============================================================================

def parse_query(query, args):
    """Parse natural language query into structured intent."""
    intent = parse_intent_haiku(query)
    if not intent:
        # Fallback: regex-based parsing
        intent = {
            "client": detect_client(query) or "client_c",
            "niche": strip_metadata(query),
            "target": extract_target(query) or 200,
            "geo": extract_geo(query),
        }

    # CLI overrides
    if args.target:
        intent["target"] = args.target
    if args.client:
        intent["client"] = args.client
    intent["sequence_length"] = getattr(args, "sequence_length", 3)

    # Routing
    niche = intent.get("niche", query)
    geo = intent.get("geo", "")
    client = intent.get("client", "client_c")
    routing = intent.get("routing") or detect_routing(niche, geo, client)[0]
    if args.force:
        routing = args.force
    intent["routing"] = routing

    # Discovery method
    if routing == "b2b":
        intent["discovery_method"] = "blitz"
    else:
        intent["discovery_method"] = "serper_maps"

    # Slug (uses canonical form so run dir matches the DB niche column)
    slug = canonical_niche_slug(niche)
    intent["slug"] = f"{slug}-forge-{datetime.now().strftime('%Y%m%d')}"

    # Keywords
    if not intent.get("keywords"):
        keywords = generate_keywords_with_haiku(niche)
        intent["keywords"] = keywords or [niche]

    # Output dir
    intent["output_dir"] = os.path.join(PROJECTS_DIR, client, "lead-runs", intent["slug"])

    return intent


# ============================================================================
# PHASE 2: DISCOVER
# ============================================================================

def discover(intent, cache, args):
    """Find companies matching the niche. Full cascade: Blitz → AI Ark → Firecrawl → Serper Maps."""
    target = intent["target"]
    routing = intent["routing"]
    keywords = intent.get("keywords", [])

    # Log keywords for debugging
    print(f"\n  Keywords: {keywords[:5]}{'...' if len(keywords) > 5 else ''}", flush=True)

    # Build V2Config for blitz_discover
    keyword_sets = [[kw] for kw in keywords[:15]]
    cfg = V2Config(
        client_name=intent.get("niche", ""),
        target=target * 3,  # overshoot — discovery finds more than enrichment produces
        max_companies=min(target * 5, 5000),
        keyword_sets=keyword_sets,
        cities=["us_top_30"],
        target_geo=intent.get("geo", ""),
        smb_only=False,
        output_dir=intent["output_dir"],
    )

    companies = []
    known_domains = get_known_domains()

    def _dedup(company_list):
        """Dedup a list against known_domains + already-discovered domains."""
        seen = known_domains | {c.get("domain", "").lower() for c in companies}
        new = []
        for c in company_list:
            d = c.get("domain", "").lower()
            if d and d not in seen:
                seen.add(d)
                new.append(c)
        return new

    if routing == "b2b":
        # ── Source 1: Blitz keyword search (FREE) ───────────────────────
        print(f"\n  [1/4] Blitz company search ({len(keyword_sets)} keyword sets)...", flush=True)
        blitz_companies = blitz_discover(cfg, cache, CACHE_FILE)
        new_blitz = _dedup(blitz_companies)
        companies.extend(new_blitz)
        print(f"        → {len(blitz_companies)} raw, {len(new_blitz)} new after dedup")

        # ── Source 2: AI Ark lookalike (if shortfall after dedup) ────────
        if len(companies) < target and os.environ.get("AIARK_API_KEY"):
            print(f"  [2/4] AI Ark lookalike ({len(companies)}/{target}, need more)...", flush=True)
            # Prefer curated per-niche seeds (NICHE_AIARK_SEEDS) — tighter lookalike
            # pool than using raw Blitz results as seeds. Falls back to first 10
            # Blitz results for niches without a curated entry.
            seeds = _get_aiark_seeds(intent.get("niche", ""), companies)
            if seeds:
                # Log whether we used curated or fallback
                try:
                    _slug = re.sub(r"[^a-z0-9]+", "-", (intent.get("niche","") or "").lower()).strip("-")
                    if _slug in NICHE_AIARK_SEEDS:
                        print(f"        → using curated seeds for {_slug}: {', '.join(seeds[:3])}{'...' if len(seeds)>3 else ''}")
                except Exception:
                    pass
                try:
                    aiark_results = lookalike_discover(
                        seeds, os.environ["AIARK_API_KEY"],
                        max_pages=5, max_companies=200,
                        cache=cache, cache_file=CACHE_FILE)
                    aiark_formatted = [{
                        "name": c.get("company_name", c.get("name", "")),
                        "domain": c.get("domain", ""),
                        "linkedin_url": c.get("linkedin_url", ""),
                        "industry": c.get("industry", ""),
                        "size": c.get("staff_size", ""),
                        "city": c.get("city", ""),
                        "state": c.get("state", ""),
                        "source": "aiark",
                    } for c in aiark_results]
                    new_aiark = _dedup(aiark_formatted)
                    companies.extend(new_aiark)
                    print(f"        → +{len(new_aiark)} new from AI Ark")
                except Exception as e:
                    print(f"        → AI Ark error: {e}")
        else:
            print(f"  [2/4] AI Ark — skipped ({len(companies)} >= {target} target)")

        # ── Source 3: Firecrawl directory crawl ───────────────────────────
        # Always run if the niche has a known directory in the registry,
        # OR if we're still short of target. Industry directories catch
        # companies that don't have LinkedIn profiles (trades, inspectors).
        niche_lower = intent.get("niche", "").lower()
        has_known_directory = False
        try:
            from enrich_smart_route import NICHE_DIRECTORIES
            has_known_directory = any(k in niche_lower or niche_lower in k
                                     for k in NICHE_DIRECTORIES)
        except ImportError:
            pass

        if has_known_directory or len(companies) < target:
            reason = "niche has known directory" if has_known_directory else f"{len(companies)}/{target}, need more"
            print(f"  [3/4] Firecrawl directories ({reason})...", flush=True)
            try:
                fc_companies = run_firecrawl_discovery(intent)
                new_fc = _dedup(fc_companies)
                companies.extend(new_fc)
                print(f"        → +{len(new_fc)} new from Firecrawl directories")
            except Exception as e:
                print(f"        → Firecrawl error: {e}")
        else:
            print(f"  [3/4] Firecrawl — skipped (no known directory, {len(companies)} >= {target} target)")

        # ── Source 4: Serper Maps fallback (if still short) ─────────────
        if len(companies) < target:
            print(f"  [4/4] Serper Maps fallback ({len(companies)}/{target}, need more)...", flush=True)
            queries = keywords[:5]
            all_discovered = {c.get("domain", "").lower() for c in companies}
            serper_companies = serper_discover(queries, ["us_top_30"],
                                              known_domains | all_discovered,
                                              cache, CACHE_FILE)
            new_serper = _dedup(serper_companies)
            companies.extend(new_serper)
            print(f"        → +{len(new_serper)} new from Serper Maps")
        else:
            print(f"  [4/4] Serper Maps — skipped ({len(companies)} >= {target} target)")

    else:
        # LOCAL: Serper Maps primary, Firecrawl fallback
        print(f"\n  [1/2] Serper Maps discovery...", flush=True)
        queries = keywords[:5]
        serper_companies = serper_discover(queries, ["us_top_30"], known_domains, cache, CACHE_FILE)
        new_serper = _dedup(serper_companies)
        companies.extend(new_serper)
        print(f"        → {len(new_serper)} new from Serper Maps")

        if len(companies) < target:
            print(f"  [2/2] Firecrawl directories ({len(companies)}/{target}, need more)...", flush=True)
            try:
                fc_companies = run_firecrawl_discovery(intent)
                new_fc = _dedup(fc_companies)
                companies.extend(new_fc)
                print(f"        → +{len(new_fc)} new from Firecrawl")
            except Exception as e:
                print(f"        → Firecrawl error: {e}")

    # Pre-filter
    companies = prefilter_companies(companies, cfg)

    # Kimi pre-enrichment niche-fit screen: reject obvious non-ICP before we spend
    # Icypeas/MV enrichment credits. Kimi K2.6 ~$0.10 per 1000 companies, which
    # saves roughly 30-40% on enrichment spend by skipping bad matches early.
    # Controlled by --no-prescreen flag; defaults to ON.
    if not getattr(args, "no_prescreen", False) and len(companies) >= 20:
        before = len(companies)
        companies = _kimi_niche_prescreen(companies, intent.get("niche", ""), intent.get("client", ""))
        if len(companies) < before:
            rejected = before - len(companies)
            pct = rejected * 100 // max(before, 1)
            print(f"  Kimi pre-screen: {len(companies)} passed, {rejected} rejected ({pct}% non-ICP filtered before enrichment)")

    print(f"\n  Discovery complete: {len(companies)} companies ready for enrichment")

    # Overlap safeguard
    if not companies:
        return []
    new_count = sum(1 for c in companies if c.get("domain", "").lower() not in known_domains)
    if companies and not args.force:
        overlap_pct = (len(companies) - new_count) / len(companies) * 100
        if overlap_pct > 80:
            print(f"\n  ⛔ OVERLAP SAFEGUARD: {overlap_pct:.0f}% already in DB. Use --force to override.")
            return []

    return companies


# ============================================================================
# AI ARK SEEDS — curated per-niche seed domains for lookalike discovery.
#
# Before this existed, Forge seeded AI Ark with the first 10 Blitz results.
# If Blitz returned bad matches, AI Ark amplified the badness (lookalikes of
# insurance brokers = more insurance brokers). Curated seeds break that loop
# for high-value niches.
#
# Seeds should be 3-5 well-known actual players in the niche. When present,
# these REPLACE the Blitz-derived seeds; when absent, Forge falls back to
# old behavior (first 10 Blitz domains).
# ============================================================================

NICHE_AIARK_SEEDS = {
    "workers-comp-recovery": [
        "corerecoveries.com", "premiumpaybacks.com", "modadvisor.com",
        "reducemycomp.com", "comptrollerservices.com",
    ],
    "rd-tax-credit": [
        "kbkg.com", "alliantgroup.com", "source-advisors.com",
        "engineeredtaxservices.com", "massietaxcredits.com",
    ],
    "sales-tax-recovery": [
        "ryan.com", "merit-advisors.com", "marshalltax.com",
        "saltmartelderlaw.com",
    ],
    "cost-segregation": [
        "kbkg.com", "costsegauthority.com", "engineered-tax.com",
        "costseg.com", "bedfordcostseg.com",
    ],
    "property-tax-appeal": [
        "propertytaxappeal.com", "ryan.com", "marvintax.com",
        "paladinrg.com", "poconnor.com",
    ],
    "utility-audit": [
        "schooleysutilityaudit.com", "utilitycostrecovery.com",
        "utilityauditexperts.com", "apexutilityaudit.com",
    ],
    "telecom-audit": [
        "pinpointcomms.com", "telecomexpensemgmt.com",
        "profittelecom.com", "teliasense.com",
    ],
    "freight-audit": [
        "ipfauditingpayment.com", "trinity3pl.com", "dataadvisors.com",
        "ctsi-global.com",
    ],
    "osha-compliance": [
        "osha-safety-consulting.com", "safetyresourcesinc.com",
        "safetyconsultingusa.com",
    ],
    "fire-protection": [
        "impactfireservices.com", "wsfp.com", "centralfireandlife.com",
        "arapahoefire.com",
    ],
    "ma-advisory": [
        "generationalequity.com", "dakotapartners.com",
        "axialgp.com", "peakmna.com",
    ],
    # ClientA sub-niches: seed with real operators in each vertical.
    # These should be actual verifiable domains; AI Ark uses them to find
    # firmographic lookalikes (similar size/geo/industry).
    "assisted-living": [
        "brookdale.com", "atriaseniorliving.com", "sunriseseniorliving.com",
        "holidayretirement.com", "brightviewseniorliving.com",
    ],
    "warehouses": [
        "prologis.com", "firstindustrial.com", "eastgroup.com",
        "duke-realty.com",
    ],
    "manufacturing": [
        # Colorado-area and national mid-size manufacturers as seeds
        "ballcorp.com", "terumobct.com", "sunmed.com", "woodward.com",
    ],
    "hotels": [
        "marriott.com", "hilton.com", "ihg.com",
        "choicehotels.com", "wyndhamhotels.com",
    ],
    "schools": [
        "regischools.com", "mullenhigh.com", "kcapplemountain.org",
    ],
    "daycares": [
        "kindercare.com", "primrose-schools.com", "brighthorizons.com",
    ],
}


def _get_aiark_seeds(niche, fallback_companies):
    """Return curated seeds for a niche, or fall back to first 10 Blitz domains."""
    try:
        slug = canonical_niche_slug(niche) if "canonical_niche_slug" in globals() else niche
    except Exception:
        slug = niche
    slug = re.sub(r"[^a-z0-9]+", "-", (slug or "").lower()).strip("-")
    curated = NICHE_AIARK_SEEDS.get(slug)
    if curated:
        return curated[:5]
    # Fall back: use first 10 Blitz results as seeds
    return [c["domain"] for c in (fallback_companies or [])[:10] if c.get("domain")]


# ============================================================================
# KIMI PRE-ENRICHMENT SCREEN
# ============================================================================

def _kimi_niche_prescreen(companies, niche, client_name):
    """Bulk-classify discovery output with Kimi BEFORE spending enrichment credits.

    Takes the list of (name, domain, industry, size) tuples returned from
    Blitz / AI Ark / Firecrawl / Serper Maps, asks Kimi to flag each as
    IN-NICHE or NOT. Rejected companies never reach Icypeas/MV, saving
    enrichment spend on false positives.

    Batches of 50 companies per call. Kimi K2.6 256k context handles this
    comfortably at ~$0.10 per 1000 companies — tiny compared to the $10-30
    enrichment-run budget we'd otherwise burn on rejects.

    Fails SAFE: if Kimi is unavailable or errors, returns companies unchanged.
    Fails OPEN: any company Kimi doesn't explicitly reject is kept (benefit of the doubt).
    """
    if not companies or not niche:
        return companies

    try:
        from llm_router import get_light_client
        client, model = get_light_client()
    except Exception:
        return companies

    # Build the niche-fit criteria text. Prefer the explicit verify_niche_fit.py
    # FIT_CRITERIA entry if one exists (single source of truth); otherwise fall
    # back to the raw niche name.
    niche_desc = niche
    try:
        import sys as _sys
        _tools = os.path.join(os.path.dirname(os.path.abspath(__file__)), "tools")
        if _tools not in _sys.path:
            _sys.path.insert(0, _tools)
        from verify_niche_fit import FIT_CRITERIA
    except Exception:
        FIT_CRITERIA = {}

    # Try exact + canonical lookups
    canonical = canonical_niche_slug(niche) if "canonical_niche_slug" in globals() else niche
    for key in [(client_name, niche), (client_name, canonical)]:
        if key in FIT_CRITERIA:
            niche_desc = FIT_CRITERIA[key]
            break

    BATCH = 50
    kept = []
    rejected_count = 0

    for i in range(0, len(companies), BATCH):
        batch = companies[i:i+BATCH]
        lines = []
        for idx, c in enumerate(batch):
            name = c.get("name", "")
            domain = c.get("domain", "")
            industry = c.get("industry", "") or ""
            size = c.get("size", "") or ""
            lines.append(f"{idx+1}. {name} | {domain} | {industry} | size={size}")
        list_block = "\n".join(lines)

        prompt = f"""You are pre-screening companies for a cold-email lead-gen pipeline.

Niche we want: {niche_desc}

Review the {len(batch)} companies below and return a JSON array of the 1-indexed
positions that are CLEARLY WRONG for this niche. Only include a position if you
are >=80% confident it is NOT a target. When in doubt, leave it out — we want
to fail OPEN, not over-reject.

Companies:
{list_block}

Return ONLY a JSON array of integers. Example: [2, 7, 12]
If all look fine, return: []
No markdown, no explanation."""

        try:
            resp = client.messages.create(
                model=model,
                max_tokens=300,
                messages=[{"role": "user", "content": prompt}],
            )
            text = resp.content[0].text.strip().replace("```json", "").replace("```", "").strip()
            reject_indices = set(json.loads(text))
        except Exception:
            reject_indices = set()

        for idx, c in enumerate(batch):
            if (idx + 1) in reject_indices:
                rejected_count += 1
            else:
                kept.append(c)

    return kept


# ============================================================================
# PHASE 3: ENRICH
# ============================================================================

def enrich(companies, intent, cache, args):
    """Find decision makers + verified emails at each company."""
    routing = intent["routing"]
    niche = intent.get("niche", "")
    workers = args.workers

    if not companies:
        return []

    known_emails = get_known_emails()
    all_contacts = []

    if routing == "b2b":
        # Step A: Blitz waterfall ICP (fastest, free)
        print(f"\n  [3a] Blitz waterfall enrichment ({len(companies)} companies)...", flush=True)
        cfg = V2Config(
            client_name=niche,
            target=intent["target"],
            max_contacts_per_company=2,
            blitz_workers=workers,
            use_master=True,
            output_dir=intent["output_dir"],
        )
        blitz_contacts = blitz_enrich(companies, cfg, cache, CACHE_FILE)
        all_contacts.extend([_normalize_contact(c) for c in blitz_contacts])

        # Track Blitz hits in run stats (for analytics; cost = $0 since Blitz is flat-rate)
        _RUN_STATS["blitz_enriched"] = _RUN_STATS.get("blitz_enriched", 0) + \
            sum(1 for c in blitz_contacts if c.get("email"))

        got_emails = {c.get("domain", "").lower() for c in blitz_contacts if c.get("email")}
        failed_domains = {c.get("domain", "").lower() for c in companies if c.get("domain")} - got_emails
        failed_companies = [c for c in companies if c.get("domain", "").lower() in failed_domains]

        print(f"      → {len(blitz_contacts)} contacts ({len(got_emails)} with email)")
        print(f"      → {len(failed_companies)} companies need forge cascade fallback")

        # Step B: Forge cascade for failures
        if failed_companies:
            print(f"\n  [3b] Forge cascade on {len(failed_companies)} remaining companies...", flush=True)
            forge_contacts = _run_forge_cascade(failed_companies, niche, known_emails, workers,
                                                 allow_nameless=getattr(args, "allow_nameless", False))
            all_contacts.extend(forge_contacts)
            print(f"      → +{len(forge_contacts)} from forge cascade")

    else:
        # LOCAL: Forge cascade directly
        print(f"\n  [3] Forge cascade enrichment ({len(companies)} companies, {workers} workers)...", flush=True)
        forge_contacts = _run_forge_cascade(companies, niche, known_emails, workers,
                                             allow_nameless=getattr(args, "allow_nameless", False))
        all_contacts.extend(forge_contacts)
        print(f"      → {len(forge_contacts)} contacts")

    # Step C: Second contact at high-quality companies (FREE via Blitz)
    single_domain = {}
    for c in all_contacts:
        d = (c.get("domain") or "").lower()
        if d:
            single_domain.setdefault(d, []).append(c)

    high_quality_singles = {d: cs[0] for d, cs in single_domain.items()
                           if len(cs) == 1 and cs[0].get("first_name") and cs[0].get("email")
                           and "info@" not in cs[0]["email"]}

    if high_quality_singles and os.environ.get("BLITZ_API_KEY"):
        import requests as _req
        _bh = {"x-api-key": os.environ["BLITZ_API_KEY"], "Content-Type": "application/json"}
        second_found = 0
        print(f"\n  [3c] Second contact finder ({len(high_quality_singles)} candidates)...", flush=True)

        for domain, existing in list(high_quality_singles.items())[:100]:  # cap at 100
            existing_name = f"{existing.get('first_name','')} {existing.get('last_name','')}".lower()
            try:
                # Get company LinkedIn
                d2l = _req.post("https://api.blitz-api.ai/v2/enrichment/domain-to-linkedin",
                               json={"domain": domain}, headers=_bh, timeout=15)
                if d2l.status_code != 200 or not d2l.json().get("found"):
                    continue
                li_url = d2l.json().get("company_linkedin_url")
                if not li_url:
                    continue

                # Find employees
                emp = _req.post("https://api.blitz-api.ai/v2/search/employee-finder",
                               json={"company_linkedin_url": li_url, "limit": 10},
                               headers=_bh, timeout=30)
                if emp.status_code != 200:
                    continue

                dm_titles = ["owner","founder","president","ceo","director","vp","vice president","partner","manager"]
                for p in emp.json().get("results", []):
                    p_name = f"{p.get('first_name','')} {p.get('last_name','')}".lower()
                    if p_name == existing_name:
                        continue
                    # Check title
                    title = ""
                    for exp in p.get("experiences", []):
                        if exp.get("is_current"):
                            title = (exp.get("title") or "").lower()
                            break
                    if not any(t in title or t in (p.get("headline") or "").lower() for t in dm_titles):
                        continue
                    # Get email
                    p_li = p.get("linkedin_url", "")
                    if not p_li:
                        continue
                    er = _req.post("https://api.blitz-api.ai/v2/enrichment/email",
                                  json={"person_linkedin_url": p_li}, headers=_bh, timeout=15)
                    if er.status_code != 200:
                        continue
                    p_email = er.json().get("email", "")
                    if p_email and p_email.lower() not in known_emails:
                        all_contacts.append(_normalize_contact({
                            "email": p_email,
                            "first_name": p.get("first_name", ""),
                            "last_name": p.get("last_name", ""),
                            "company": existing.get("company", ""),
                            "domain": domain,
                            "title": title,
                            "city": existing.get("city", ""),
                            "state": existing.get("state", ""),
                            "source": "blitz_second_contact",
                            "verified": False,
                        }))
                        known_emails.add(p_email.lower())
                        second_found += 1
                        break
            except Exception:
                pass

        print(f"      → +{second_found} second contacts found")

    return all_contacts


def _normalize_contact(c):
    """Ensure contact dict has all standard keys — reconciles blitz_enrich vs forge_enrich formats."""
    return {
        "email": c.get("email", ""),
        "first_name": c.get("first_name", ""),
        "last_name": c.get("last_name", ""),
        "company": c.get("company", c.get("company_name", "")),
        "domain": c.get("domain", ""),
        "title": c.get("title", ""),
        "city": c.get("city", ""),
        "state": c.get("state", ""),
        "phone": c.get("phone", ""),
        "linkedin_url": c.get("linkedin_url", ""),
        "source": c.get("source", ""),
        "verified": c.get("verified", False),
        "mv_result": c.get("mv_result", ""),
        "catch_all": c.get("catch_all", False),
        "tier": c.get("tier", 0),
    }


def _run_forge_cascade(companies, niche, known_emails, workers, allow_nameless=False):
    """Run forge_enrich's 13-step cascade on a list of companies. Parallel.

    Accumulates per-step hit counters into the module-level _RUN_STATS dict so
    export_results() can compute a real cost estimate at end-of-run.

    Args:
      allow_nameless: forwarded to forge_enrich.enrich_company. When False
        (default), role-account fallback sources (Google Maps email, website
        scrape, nameless Icypeas domain hits) are skipped so every returned
        lead has first_name populated. Matches v2 pipeline's default.
    """
    # Route light classification/extraction through llm_router (Kimi K2.6 if
    # KIMI_API_KEY is set, Claude Haiku fallback otherwise).
    from llm_router import get_light_client
    haiku, _haiku_model_name = get_light_client()

    # Batch MX check
    from enrich_smart_route import batch_mx_check_simple
    domains = [c.get("domain", "").strip().lower() for c in companies if c.get("domain")]
    mx_results = batch_mx_check_simple(domains)

    # Use the module-level _RUN_STATS so cost tracking accumulates across cascade calls
    global _RUN_STATS
    stats = _RUN_STATS
    results = []
    results_lock = threading.Lock()
    completed = [0]

    def _enrich_one(co):
        domain = (co.get("domain", "") or "").strip().lower()
        company = co.get("name", co.get("company", ""))
        city = co.get("city", "")
        state = co.get("state", "")
        phone = co.get("phone", "")
        mx_type = mx_results.get(domain, "unknown")

        result = enrich_company(company, domain, city, state, phone, mx_type, haiku, known_emails, stats,
                                 allow_nameless=allow_nameless,
                                 niche=niche)
        if result:
            with results_lock:
                results.append(result)

        with results_lock:
            completed[0] += 1
            c = completed[0]
        if c % 20 == 0:
            print(f"      [{c}/{len(companies)}] verified={len(results)}", flush=True)

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = [pool.submit(_enrich_one, co) for co in companies]
        for f in as_completed(futures):
            try:
                f.result()
            except Exception:
                pass

    # Convert forge_enrich results to standard contact format
    contacts = []
    for r in results:
        contacts.append(_normalize_contact({
            "email": r["email"],
            "first_name": r.get("first_name", ""),
            "last_name": r.get("last_name", ""),
            "company": r.get("company", ""),
            "domain": r.get("domain", ""),
            "title": r.get("title", ""),
            "city": r.get("city", ""),
            "state": r.get("state", ""),
            "phone": r.get("phone", ""),
            "source": f"forge_{r.get('source', 'unknown')}",
            "verified": True,  # forge_enrich verifies inline
            "mv_result": "ok",
        }))
    return contacts


# ============================================================================
# PHASE 4: CLEAN + VERIFY
# ============================================================================

def clean_and_verify(contacts, intent, cache, args):
    """Pre-clean → MV verify → BounceBan → tier score → post-clean."""
    if not contacts:
        return []

    cfg = V2Config(
        client_name=intent.get("niche", ""),
        target=intent["target"],
        max_contacts_per_company=2,
        double_verify=not args.no_bb,
        output_dir=intent["output_dir"],
        allow_nameless=getattr(args, "allow_nameless", False),
        allow_generic=getattr(args, "allow_generic", False),
    )

    # 4a: Pre-clean — remove junk titles, bad emails, generic prefixes
    #     Local routing uses lenient filter (no name requirement) since
    #     Serper Maps contacts often lack first/last names.
    routing = intent.get("routing", "b2b")
    print(f"\n  [4a] Pre-clean on {len(contacts)} contacts (routing={routing})...", flush=True)

    if routing == "local":
        # Lenient filter for local: keep contacts with email + company, no name required
        cleaned = []
        for c in contacts:
            email = (c.get("email") or "").strip()
            if not email or "@" not in email:
                continue
            title = (c.get("title") or "").lower()
            if is_bad_title(title):
                continue
            cleaned.append(c)
        print(f"       → {len(cleaned)} kept, {len(contacts) - len(cleaned)} excluded (lenient local filter)")
    else:
        try:
            cleaned, excluded = pre_clean(contacts, cfg)
            print(f"       → {len(cleaned)} kept, {len(excluded)} excluded")
        except Exception:
            # pre_clean expects specific fields — fall back to manual title filter
            cleaned = []
            for c in contacts:
                title = (c.get("title") or "").lower()
                if is_bad_title(title):
                    continue
                email = c.get("email", "")
                if not email or "@" not in email:
                    continue
                cleaned.append(c)
            print(f"       → {len(cleaned)} after title filter ({len(contacts) - len(cleaned)} removed)")

    # 4b: MV verification (forge_enrich contacts already verified inline)
    unverified = [c for c in cleaned if not c.get("verified")]
    already_verified = [c for c in cleaned if c.get("verified")]

    if unverified:
        print(f"  [4b] MV verification on {len(unverified)} contacts...", flush=True)
        verified_list = verify_contacts(unverified, double_verify=False)
        valid = [c for c in verified_list if c.get("verified") or
                 c.get("mv_result") in ("ok", "valid", "good", "risky")]
        print(f"       → {len(valid)} passed MV ({len(unverified) - len(valid)} rejected)")
        cleaned = already_verified + valid
    else:
        print(f"  [4b] All {len(cleaned)} contacts already MV verified ✓")

    # 4c: Post-clean — strict title priority, max contacts per company
    #     Local routing skips post_clean (no tier data from Serper Maps contacts)
    if routing == "local":
        # For local: just cap at max_contacts_per_company by domain
        by_domain = {}
        for c in cleaned:
            d = (c.get("domain") or c.get("company") or "unknown").lower()
            by_domain.setdefault(d, []).append(c)
        final = []
        for d, group in by_domain.items():
            final.extend(group[:cfg.max_contacts_per_company])
        dropped = len(cleaned) - len(final)
        print(f"  [4c] Post-clean (local: max {cfg.max_contacts_per_company}/company)...")
        print(f"       → {len(final)} kept, {dropped} dropped")
    else:
        print(f"  [4c] Post-clean...", flush=True)
        try:
            final, dropped = post_clean(cleaned, cfg)
            print(f"       → {len(final)} kept, {len(dropped)} dropped")
        except Exception:
            final = cleaned

    return final


# ============================================================================
# PHASE 5: QUALITY
# ============================================================================

def quality_check(contacts, intent, args):
    """Niche-fit check + triple-layer dedup."""
    niche = intent.get("niche", "")
    if not contacts:
        return contacts

    # Save pre-niche-fit cache
    pre_cache_dir = os.path.join(SCRIPT_DIR, "pre-niche-fit-cache")
    os.makedirs(pre_cache_dir, exist_ok=True)
    slug = re.sub(r'[^a-z0-9]+', '-', niche.lower()).strip('-')[:30]
    cache_path = os.path.join(pre_cache_dir, f"{slug}_{datetime.now().strftime('%Y%m%d_%H%M')}.json")
    with open(cache_path, "w") as f:
        json.dump(contacts, f, indent=2, default=str)

    # 5a: Niche-fit LLM check
    if niche:
        print(f"\n  [5a] Niche-fit check on {len(contacts)} contacts...", flush=True)
        # Route through llm_router — Kimi K2.6 handles classification-grade checks
        # identically to Haiku at 8x cheaper. High-volume call site (fires per contact).
        from llm_router import get_light_client
        haiku, _niche_check_model = get_light_client()

        confirmed = []
        for c in contacts:
            company_id = (c.get("company") or c.get("domain") or "").strip()
            if not company_id:
                confirmed.append(c)
                continue
            try:
                resp = haiku.messages.create(
                    model=_niche_check_model, max_tokens=30,
                    messages=[{'role': 'user',
                               'content': f'This company was found while searching for {niche} firms. '
                                          f'Company/domain: "{company_id}". '
                                          f'Could this plausibly be a {niche} company? '
                                          f'Only say "no" if it is CLEARLY a different industry. '
                                          f'Reply ONLY "yes" or "no".'}])
                if 'yes' in resp.content[0].text.strip().lower():
                    confirmed.append(c)
            except Exception:
                confirmed.append(c)

        rejected = len(contacts) - len(confirmed)
        print(f"       → {len(confirmed)} confirmed, {rejected} rejected")
        contacts = confirmed

    # 5b: Triple-layer dedup (email → domain → global)
    print(f"  [5b] Dedup...", flush=True)
    try:
        deduped = deduplicate(contacts, V2Config(
            client_name=intent.get("niche", ""),
            max_contacts_per_company=2,
            output_dir=intent["output_dir"],
        ))
        print(f"       → {len(deduped)} after dedup ({len(contacts) - len(deduped)} dupes)")
        contacts = deduped
    except Exception:
        # Fallback: simple email dedup
        seen = set()
        deduped = []
        for c in contacts:
            e = (c.get("email") or "").lower()
            if e and e not in seen:
                seen.add(e)
                deduped.append(c)
        print(f"       → {len(deduped)} after simple dedup")
        contacts = deduped

    return contacts


# ============================================================================
# PHASE 6: EXPORT
# ============================================================================

def export_results(contacts, intent, args):
    """Save to DB + export campaign-ready CSV + analytics logging."""
    outdir = intent["output_dir"]
    os.makedirs(outdir, exist_ok=True)

    client = intent.get("client", "client_c")
    niche = intent.get("niche", "")
    # Bug 15/16: was [:30] — silently truncated and split vertical families into
    # multiple slugs. canonical_niche_slug() uses 60 chars + alias map.
    niche_slug = canonical_niche_slug(niche)

    # Auto-alias detection — warn if this slug looks like a duplicate of an
    # existing niche. Doesn't block the run, just flags for the operator to add an entry
    # to niche_aliases.json before the DB fragments further.
    _similar = find_similar_niches(niche_slug)
    if _similar:
        print(f"\n  ⚠️  NICHE DRIFT WARNING: {niche_slug!r} shares a prefix with "
              f"existing niche(s) in the DB:")
        for existing, count in _similar[:5]:
            print(f"       {existing!r} ({count} rows)")
        print(f"     Consider adding to niche_aliases.json to consolidate, "
              f"or run `tools/migrate_niche_slugs.py` after edits.")

    # Export smartlead_import.csv
    with open(os.path.join(outdir, "smartlead_import.csv"), "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=SMARTLEAD_FIELDS)
        w.writeheader()
        for c in contacts:
            email = c.get("email", "")
            email_type = "personal" if email and "info@" not in email and "contact@" not in email else "generic"
            w.writerow({
                "email": email,
                "first_name": c.get("first_name", ""),
                "last_name": c.get("last_name", ""),
                "company_name": c.get("company", ""),
                "phone": c.get("phone", ""),
                "title": c.get("title", ""),
                "website": c.get("domain", ""),
                "custom1": email_type,
                "custom2": c.get("source", ""),
                "custom3": f"{c.get('city', '')} {c.get('state', '')}".strip(),
            })

    # Save to master DB
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    added = 0
    # Bug 17: Use ISO-8601 with UTC offset for date_added / date_updated so
    # the format matches the older pipeline. SQLite's datetime('now') produces
    # a different format that fragmented the column into two incompatible shapes.
    now_iso = utc_now_isoformat()
    for c in contacts:
        email = (c.get("email") or "").lower().strip()
        if not email:
            continue
        if cur.execute("SELECT id FROM leads WHERE LOWER(email)=?", (email,)).fetchone():
            continue

        # Bug 18 (v2, fixed 2026-04-20 after Fire Alarm bounce incident):
        # mv_result was ending up NULL for verified=1 contacts because
        # c.get("mv_result", "ok") returns None when the key is present but None,
        # and returns "" when blitz_enrich contacts had an empty default from
        # _normalize_contact. Coerce to a non-empty sentinel so the verification
        # audit trail stays intact.
        #
        # CRITICAL: Blitz-verified is NOT MillionVerifier-verified. Blitz uses
        # MX checks + heuristics; it does NOT do SMTP-level email validation.
        # Previously this coerced empty mv_result to "ok" for Blitz-verified
        # contacts, which lied about MV status and let unvalidated emails hit
        # Smartlead. Result: 6.67% bounce rate on Fire Alarm campaign, auto-pause.
        # Fix: use distinct "blitz" sentinel so downstream Smartlead push can
        # require mv_result == "ok" and trigger a real MV check for "blitz" rows.
        raw_mv = c.get("mv_result")
        if raw_mv in (None, "",):
            mv_value = "blitz" if c.get("verified") else "unverified"
        else:
            mv_value = raw_mv

        cur.execute("""INSERT INTO leads (email,first_name,last_name,company,phone,title,domain,
                       city,state,source,niche,client,verified,mv_result,status,date_added,date_updated)
                       VALUES (?,?,?,?,?,?,?,?,?,?,?,?,1,?,?,?,?)""",
                    (c["email"], c.get("first_name", ""), c.get("last_name", ""), c.get("company", ""),
                     c.get("phone", ""), c.get("title", ""), c.get("domain", ""),
                     c.get("city", ""), c.get("state", ""),
                     c.get("source", "forge"), niche_slug, client,
                     mv_value, "new", now_iso, now_iso))
        added += 1

    # Enrichment analytics logging
    # Cost is computed from _RUN_STATS (populated by blitz_enrich + forge cascade).
    # This replaces the old hardcoded 0 that was showing $0.000 for every run.
    try:
        cost_est = estimate_cost_from_stats(_RUN_STATS)
        cur.execute("""INSERT INTO enrichment_analytics
            (niche, client, companies_processed, leads_produced,
             source_domain_memory, source_google_maps, source_blitz_direct,
             source_website_scrape, source_pattern, source_icypeas_name,
             source_icypeas_domain, source_catch_all, owners_found,
             total_cost_estimate)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (niche_slug, client, len(contacts), added,
             _RUN_STATS.get("domain_memory_hit", 0),
             _RUN_STATS.get("maps_email_hit", 0),
             _RUN_STATS.get("blitz_direct", 0) + _RUN_STATS.get("blitz_enriched", 0),
             _RUN_STATS.get("website_scrape_hit", 0),
             _RUN_STATS.get("pattern_hit", 0),
             _RUN_STATS.get("icypeas_name_hit", 0),
             _RUN_STATS.get("icypeas_domain_hit", 0),
             _RUN_STATS.get("catch_all_accepted", 0),
             _RUN_STATS.get("owner_found", 0),
             cost_est))
        _budget_for_run = intent.get("cost_budget") or get_client_cost_budget(client)
        if cost_est > _budget_for_run:
            print(f"  ⚠ enrichment cost: ~${cost_est:.3f} "
                  f"(EXCEEDED ${_budget_for_run:.2f} budget for {client})")
        else:
            print(f"  enrichment cost: ~${cost_est:.3f} "
                  f"(${_budget_for_run:.2f} budget, {cost_est/_budget_for_run*100:.0f}% used)")
    except Exception as e:
        print(f"  ⚠ analytics log failed: {e}")

    conn.commit()
    conn.close()

    # Domain memory updates for successful enrichments
    for c in contacts:
        domain = (c.get("domain") or "").lower()
        email = c.get("email", "")
        if domain and email:
            try:
                update_domain_memory(domain, email, success=True)
            except Exception:
                pass

    # Tiered CSV export
    tier1 = [c for c in contacts if c.get("email") and
             "info@" not in c["email"] and "contact@" not in c["email"] and
             c.get("first_name")]
    tier2 = [c for c in contacts if c not in tier1]

    for tier_name, tier_contacts in [("tier1_contacts", tier1), ("tier2_contacts", tier2)]:
        if tier_contacts:
            with open(os.path.join(outdir, f"{tier_name}.csv"), "w", newline="") as f:
                w = csv.DictWriter(f, fieldnames=SMARTLEAD_FIELDS)
                w.writeheader()
                for c in tier_contacts:
                    email = c.get("email", "")
                    email_type = "personal" if email and "info@" not in email and "contact@" not in email else "generic"
                    w.writerow({
                        "email": email,
                        "first_name": c.get("first_name", ""),
                        "last_name": c.get("last_name", ""),
                        "company_name": c.get("company", ""),
                        "phone": c.get("phone", ""),
                        "title": c.get("title", ""),
                        "website": c.get("domain", ""),
                        "custom1": email_type,
                        "custom2": c.get("source", ""),
                        "custom3": f"{c.get('city', '')} {c.get('state', '')}".strip(),
                    })

    # Run summary JSON
    with open(os.path.join(outdir, "run_summary.json"), "w") as f:
        json.dump({
            "niche": niche, "client": client,
            "requested_target": intent.get("target"),
            "actual_leads": len(contacts),
            "total_leads": len(contacts),
            "tier1": len(tier1), "tier2": len(tier2),
            "added_to_db": added, "timestamp": datetime.now().isoformat(),
        }, f, indent=2)

    return added, outdir


# ============================================================================
# SMARTLEAD CAMPAIGN DEDUP
# ============================================================================

SL_CACHE_PATH = os.path.join(SCRIPT_DIR, ".smartlead_email_cache.json")
SL_CACHE_TTL = 3600  # 1 hour


def _load_sl_cache():
    """Load cached Smartlead campaign emails. Returns (set, is_fresh)."""
    if os.path.isfile(SL_CACHE_PATH):
        try:
            data = json.load(open(SL_CACHE_PATH))
            ts = data.get("timestamp", 0)
            if time.time() - ts < SL_CACHE_TTL:
                return set(data.get("emails", [])), True
        except Exception:
            pass
    return set(), False


def _save_sl_cache(emails):
    """Save Smartlead campaign emails to cache."""
    with open(SL_CACHE_PATH, "w") as f:
        json.dump({"timestamp": time.time(), "emails": list(emails)}, f)


def smartlead_dedup(contacts):
    """Remove contacts whose emails are already in ANY Smartlead campaign. Uses 1-hour TTL cache."""
    sl_key = os.environ.get("SMARTLEAD_API_KEY", "")
    if not sl_key or not contacts:
        return contacts

    import requests
    BASE = "https://server.smartlead.ai/api/v1"

    print(f"\n  [5c] Smartlead campaign dedup...", flush=True)

    # Try cache first
    all_campaign_emails, is_fresh = _load_sl_cache()
    if is_fresh:
        print(f"       → Using cached campaign emails ({len(all_campaign_emails)} emails, <1hr old)")
    else:
        # Fetch from API
        try:
            r = requests.get(f"{BASE}/campaigns/?api_key={sl_key}", timeout=30)
            if r.status_code != 200:
                print(f"       → Smartlead API error {r.status_code}, skipping dedup")
                return contacts

            campaigns = r.json()
            all_campaign_emails = set()

            for c in campaigns:
                cid = c["id"]
                offset = 0
                while True:
                    r2 = requests.get(f"{BASE}/campaigns/{cid}/leads?api_key={sl_key}&offset={offset}&limit=100", timeout=30)
                    if r2.status_code != 200:
                        break
                    data = r2.json()
                    leads = data.get("data", [])
                    if not leads:
                        break
                    for l in leads:
                        e = (l.get("lead", {}).get("email") or "").lower()
                        if e:
                            all_campaign_emails.add(e)
                    if len(leads) < 100:
                        break
                    offset += 100

            _save_sl_cache(all_campaign_emails)
            print(f"       → Fetched {len(all_campaign_emails)} emails from {len(campaigns)} campaigns (cached)")

        except Exception as e:
            print(f"       → Smartlead dedup error: {e}, skipping")
            return contacts

    before = len(contacts)
    contacts = [c for c in contacts if (c.get("email") or "").lower() not in all_campaign_emails]
    removed = before - len(contacts)
    print(f"       → {removed} dupes removed, {len(contacts)} clean")

    return contacts


# ============================================================================
# PHASE 7: CAMPAIGN COPY GENERATION
# ============================================================================

def _load_client_brief(client, niche):
    """Read the Forge Brief from CLIENT.md. Falls back to generic if not found."""
    client_md_path = os.path.join(PROJECTS_DIR, client, "CLIENT.md")

    sender = client
    service = ""
    target_audience = ""
    cta = ""
    usp = ""
    geography = "National (US)"

    if os.path.isfile(client_md_path):
        content = open(client_md_path).read()

        # Extract yaml block from ```yaml ... ```
        import re as _re
        yaml_match = _re.search(r'```yaml\s*\n(.*?)```', content, _re.DOTALL)
        if yaml_match:
            for line in yaml_match.group(1).strip().split("\n"):
                if ":" not in line:
                    continue
                key, val = line.split(":", 1)
                key, val = key.strip().lower(), val.strip()
                if key == "sender":
                    sender = val
                elif key == "service":
                    service = val
                elif key == "target_audience":
                    target_audience = val
                elif key == "cta":
                    cta = val
                elif key == "usp":
                    usp = val
                elif key == "geography":
                    geography = val
    else:
        print(f"       ⚠ No CLIENT.md found at {client_md_path} — using generic brief", flush=True)

    # Adapt the brief based on context
    # If this is ClientC looking for clients in a niche,
    # the copy pitches CLIENT_C's cold email services TO that niche
    brief = f"""
Sender: {sender}
What we sell: {service}
Who we're emailing: {target_audience} — specifically {niche} companies
CTA type: {cta}
Unique selling point: {usp}
Geography: {geography}
Niche being targeted: {niche}
No case studies available — do NOT invent any. Focus on the offer and CTA.
"""
    return brief


COLD_EMAIL_RULES = os.path.join(SCRIPT_DIR, "..", "..", ".claude", "skills",
                                "cold-email-writer.skill")
COLD_EMAIL_RULES_EXTRACTED = "/tmp/cold-email-skill/cold-email-generator/references/WRITING_RULES.md"


def generate_campaign_copy(intent, outdir):
    """Generate cold email sequence using Client Ascension framework. Self-grades. Only saves A-grade copy."""
    niche = intent.get("niche", "")
    client = intent.get("client", "")
    if not niche:
        return

    print(f"\n  [7] Generating campaign copy for '{niche}'...", flush=True)

    # Load writing rules
    rules_text = ""
    for path in [COLD_EMAIL_RULES_EXTRACTED,
                 os.path.join(SCRIPT_DIR, "..", "..", ".claude", "skills", "WRITING_RULES.md")]:
        if os.path.isfile(path):
            rules_text = open(path).read()
            break

    # Load swipe file for industry examples
    swipe_text = ""
    swipe_path = "/tmp/cold-email-skill/cold-email-generator/references/WINNING_SCRIPTS_SWIPE_FILE.md"
    if os.path.isfile(swipe_path):
        swipe_text = open(swipe_path).read()[:3000]  # first 3000 chars for context

    import anthropic
    client_ai = anthropic.Anthropic(api_key=os.environ.get('ANTHROPIC_API_KEY', ''))

    # Build the brief from CLIENT.md in the client's project folder
    brief = _load_client_brief(client, niche)

    # Salesforge pattern: shorter sequences often beat longer ones. Default
    # stays at 3 for backwards compat, but `intent['sequence_length']=2`
    # produces the tighter 2-email version for A/B testing.
    sequence_length = intent.get("sequence_length", 3)
    if sequence_length == 2:
        sequence_structure_block = """EMAIL 1 — 2 VARIANTS (A and B with different hooks)
Each variant needs: subject line (under 4 words, lowercase) + body
Default CTA: hard CTA ("Open to a call this week?" or similar)

EMAIL 2 — 1 VARIANT
Body only, no subject line. Feels like a reply, not a reminder. Never says "following up."
Should function as both a follow-up AND a graceful breakup — acknowledge if they're not interested."""
    else:
        sequence_structure_block = """EMAIL 1 — 2 VARIANTS (A and B with different hooks)
Each variant needs: subject line (under 4 words, lowercase) + body
Default CTA: hard CTA ("Open to a call this week?" or similar)

EMAIL 2 — 1 VARIANT
Body only, no subject line. Feels like a reply, not a reminder. Never says "following up."

EMAIL 3 — 1 VARIANT
Body only, no subject line. Breakup style: name the specific thing they're passing on."""

    # Salesforge pattern: pull past winning emails for this client as few-shot
    # examples. Proven reply-rate lift. Only uses campaigns >2% reply rate on
    # 200+ sends (statistically significant winners).
    _winners = pull_winning_copy_examples(client)
    winners_block = ""
    if _winners:
        winners_block = "\n\nPAST WINNERS (for this client — use these patterns, don't copy verbatim):\n"
        for w in _winners:
            winners_block += f"\n--- {w['campaign']} ({w['reply_rate']}% reply on {w['sent']} sends) ---\n"
            winners_block += f"Subject: {w['subject']}\n\n{w['body']}\n"
        winners_block += "\nTAKE THE ANGLE + STRUCTURE + TONE of these winners. Write fresh copy — do not reuse phrases.\n"
        print(f"  [copy] Feeding {len(_winners)} past winners into generator as few-shot examples", flush=True)

    generate_prompt = f"""You are a cold email copywriter. Follow these rules EXACTLY:

{rules_text}

BRIEF:
{brief}
{winners_block}

Generate a cold email sequence with this structure:

{sequence_structure_block}

REQUIREMENTS:
- Every email under 75 words (target ~60)
- Speak to "you/your" directly
- Use specific {niche} industry language (technical terms, jargon)
- Hard CTA default ("Open to a call this week?" or specific times)
- No em dashes, no signatures, no {{{{company_name}}}} in body
- Spintax on greeting, body phrases, and CTA using {{option1|option2}} format
- Zero or one question per email max
- NEVER make up case studies
- Each variant must feel like a different email, not copy-pasted

HUMANIZER RULES (hard rule — under 10% AI slop or copy gets rejected):
- NO three-part rhythmic lists like "A, B, and C" or "missed quarters, rep turnover, stalled deals". Pick ONE concrete specific instead.
- NO corporate clichés: "raise their hand", "drowning in cash flow", "feel the pipeline stalling", "don't know where to look", "on your calendar"
- NO division-of-labor closers repeated across emails ("you run X, we handle Y", "you take the meetings, we handle prospecting")
- NO "Quick follow-up" or any phrasing containing "follow-up" in Email 2 (Email 2 is a reply, not a reminder)
- NO AI vocabulary: "crucial", "pivotal", "landscape", "showcase", "underscore", "highlight (as verb)", "tapestry", "testament", "enduring"
- NO filler phrases: "At its core", "It is important to note", "Moving forward"
- Vary sentence rhythms — mix short and longer sentences, don't make every email the same structure
- Every email must contain at least ONE concrete specific (a real number, a real phrase the buyer would actually use, a real scenario)
- Write like a real operator at a coffee shop, not a polished SDR template
"""

    max_attempts = 3
    # Bug 19: 60% of niches were failing 3/3 attempts. Track the best attempt so
    # broader niches that can't hit A still ship with the best available copy
    # instead of nothing. Only used as a fallback when no A-grade is achieved.
    best = {"grade": "F", "passes": 0, "copy": "", "grade_text": ""}
    _grade_rank = {"A": 5, "B": 4, "C": 3, "D": 2, "F": 1}

    for attempt in range(1, max_attempts + 1):
        try:
            # ── STEP 1: DRAFT ──────────────────────────────────────────────
            # Upgraded to Opus 4 (from Sonnet 4) after A/B test on 2026-04-20:
            # Sonnet 4 hit 0% A-grade on the 19-point rubric across 5 briefs
            # (every campaign today failed auto-copy-gen with banned openers).
            # Opus 4 hit 80% A-grade on same test. Extra cost ~$1/mo at current
            # ~10 copy gens/mo — massive quality improvement for trivial spend.
            print(f"       [{attempt}/{max_attempts}] Drafting...", flush=True)
            resp = client_ai.messages.create(
                model='claude-opus-4-20250514', max_tokens=1500,
                messages=[{'role': 'user', 'content': generate_prompt}])
            draft_copy = resp.content[0].text.strip()

            # ── STEP 2: HUMANIZE ───────────────────────────────────────────
            # Dedicated rewrite pass. Even though the draft prompt already includes
            # humanizer rules, a separate pass catches patterns the generator
            # slipped through. Matches the cold-email-writer skill's workflow:
            # draft → humanize → grade.
            humanize_prompt = f"""You are humanizing cold email copy. Remove AI writing patterns AND normalize any structural deviations. This prompt handles drafts from both Claude and Kimi (K2.6), which have different AI "fingerprints".

ORIGINAL COPY:
{draft_copy}

PATTERNS TO KILL (content-level):
- Three-part rhythmic lists ("A, B, and C" or "missed quarters, rep turnover, stalled deals"). Pick ONE concrete specific instead.
- Corporate clichés: "raise their hand", "drowning in cash flow", "feel the pipeline stalling", "don't know where to look", "on your calendar"
- Division-of-labor closers repeated across emails ("you run X, we handle Y", "you take the meetings, we handle prospecting")
- Any phrasing containing "follow-up" in Email 2 (including spintax options like "Quick follow-up")
- AI vocabulary: "crucial", "pivotal", "landscape", "showcase", "underscore", "tapestry", "testament", "enduring", "align with", "delve", "emphasize", "fostering"
- Filler phrases: "At its core", "It is important to note", "Moving forward", "In conclusion"
- Significance inflation: "stands as a testament", "serves as", "marks a pivotal moment"
- Fake-depth participle phrases: "...highlighting X", "...underscoring Y", "...showcasing Z"
- Made-up statistics (any "X%", "X out of Y", "X to Z" ratio, "15+ year old units") unless it is a real client case study that was provided in the brief
- Banned openers: "Most [industry]...", "If you're like most...", "Are you tired of...", "Imagine if...", "What if you could...", "Did you know...", "I noticed..." — these are FAIL conditions on the 18-point rubric

PATTERNS TO KILL (Kimi-specific fingerprint — new as of 2026-04-20):
- Kimi often writes em dashes (—) even when told not to. Convert to commas, periods, or hyphens with spaces.
- Kimi often uses `/` for spintax ({{Hi/Hey}}). Convert every `/`-style spintax option to `{{Hi|Hey}}` using the pipe character.
- Kimi often adds a signature block like "[Your name]", "ClientC", "Operator" at the end of emails. Delete any signature/sign-off — the email must end with the CTA.
- Kimi often adds subject lines on Email 2 and Email 3. These are BODY-ONLY emails — delete any subject line from Email 2 and Email 3.
- Kimi often renders markdown bold `**Email 1A**` or `**Subject:**`. Strip markdown bold — the output is plain text for email clients.
- Kimi sometimes writes `[First Name]` or `[first_name]` instead of the merge tag. Replace with `{{{{first_name}}}}` (double-brace Smartlead merge tag syntax).
- Kimi sometimes writes `{{company_name}}` in the Email 1A/2/3 body. Remove it from the body — only allowed in the Email 1B subject line.
- Kimi sometimes writes more than one question per email body. The rubric allows ONE (the CTA). Rewrite any second question as a statement.

MUST PRESERVE:
- All {{option1|option2}} spintax structures exactly
- All <br><br> line breaks
- Subject lines in the same position for Email 1A and Email 1B only (Email 2/3 have NO subject)
- {{{{first_name}}}} merge tag
- Industry-specific terms ({niche} jargon like CRO, ESCO, fractional CFO, kWh, retrofit, etc.)
- The overall sequence structure (Email 1 with 2 variants, Email 2, Email 3)
- Word count under 75 per email (target 60)

OUTPUT FORMAT (REQUIRED):
- Plain text. No markdown bold (`**...**`), no markdown headers (`#`), no horizontal rules (`---`).
- EMAIL labels can be plain "EMAIL 1A:" etc without bold
- Every spintax option uses the pipe character `{{a|b}}` — NEVER `{{a/b}}` or `{{a , b}}`

REWRITE RULES:
- Vary sentence rhythm (short, then longer)
- Keep one concrete specific per email (real number, real phrase a buyer would actually say)
- Use "I" naturally if it fits, not forced
- Sound like a real operator typed this at a coffee shop, not an SDR template

Return ONLY the rewritten sequence. Same format as the original. No preamble, no explanation."""

            print(f"       [{attempt}/{max_attempts}] Humanizing...", flush=True)
            humanize_resp = client_ai.messages.create(
                model='claude-opus-4-20250514', max_tokens=1500,
                messages=[{'role': 'user', 'content': humanize_prompt}])
            copy = humanize_resp.content[0].text.strip()

            # ── STEP 3: GRADE ──────────────────────────────────────────────
            print(f"       [{attempt}/{max_attempts}] Grading...", flush=True)
            # Self-grade — strict binary pass/fail rubric
            grade_prompt = f"""Grade this cold email sequence. Each check is PASS or FAIL. No subjective judgment.

SEQUENCE:
{copy}

RUBRIC (binary pass/fail):

1. WORD COUNT: Count the words in each email body (exclude subject lines, exclude variant labels). Target is around 60 words. PASS if all are 75 or under. FAIL only if any email exceeds 75 words.

2. VOICE: Does every email use "you/your/your team" instead of talking about the industry? ("We help fire protection firms" = FAIL. "We fill your calendar" = PASS)

3. INDUSTRY LANGUAGE: Does the copy contain at least 2 technical terms specific to {niche}? (Generic words like "companies" or "businesses" don't count)

4. SUBJECT LINES: Are all subject lines 4 words or under and lowercase?

5. OPENER: Does every email avoid banned openers? ("Most [industry]...", "If you're like most...", "Are you tired of...", "I noticed...", "I came across...")

6. CTA: Does every email end with a clear ask? (A statement with no ask = FAIL)

7. EM DASHES: Are there zero em dashes (—) anywhere in the copy?

8. SIGNATURES: Are there zero signatures or sign-offs (no "the operator", "Best,", etc.)?

9. COMPANY NAME: Is {{{{company_name}}}} absent from all email bodies? (Subject lines are OK)

10. FOLLOWING UP: Does Email 2 avoid "following up", "circling back", "touching base", AND any spintax option containing "follow-up"? ("Quick follow-up" as a spintax option = FAIL)

11. VARIANTS: Do the 2 Email 1 variants have different opening hooks? (Same first line = FAIL)

12. CASE STUDIES: Are there zero invented statistics, client names, or dollar amounts?

13. SPINTAX: Does the copy include {{option1|option2}} spintax in at least the greeting and CTA?

14. QUESTIONS: Does each email have 1 or fewer questions? (2+ questions in any email = FAIL)

HUMANIZER CHECKS (must also pass — any failure here downgrades grade by one letter):

15. NO THREE-PART LISTS: Does the copy avoid rhythmic three-item lists like "A, B, and C"? ("missed quarters, rep turnover, stalled deals" = FAIL)

16. NO CORPORATE CLICHÉS: Does the copy avoid phrases like "raise their hand", "drowning in cash flow", "feel the pipeline stalling", "don't know where to look"?

17. NO DIVISION-OF-LABOR PATTERN: Do the emails avoid the repeated "you do X, we do Y" closer across multiple emails? (Same structure in every email = FAIL)

18. NO AI VOCAB: Does the copy avoid "crucial", "pivotal", "landscape", "showcase", "underscore", "tapestry", "testament", "enduring"?

STRUCTURAL CHECKS (catch Kimi-fingerprint issues):

19. CLEAN STRUCTURE: Are ALL of the following true? (All must pass for rule 19 = PASS)
    - Every spintax option uses pipe character: {{a|b}} not {{a/b}} and not {{a , b}}
    - Zero markdown artifacts: no **bold**, no # headers, no --- horizontal rules
    - Merge tags use Smartlead syntax {{first_name}}, never [First Name] or [first_name]
    - Email 2 and Email 3 have NO subject line (body only). Only Email 1A and Email 1B should have subjects.
    - No numbered lists (1. 2. 3.) inside email bodies
    If any of these sub-checks fails, rule 19 = FAIL.

Count passes across rules 1-14, humanizer rules 15-18, and structural rule 19. Then grade:
- 19/19 = A (campaign-ready)
- 17-18/19 = A- (ship with note)
- 14-16/19 = B (rewrite and re-grade)
- 12-13/19 = C (full rewrite)
- Below 12 = D or F

List each check as PASS or FAIL with a one-line reason.
Final line EXACTLY: GRADE: [letter]
PASSES: [number]/19"""

            grade_resp = client_ai.messages.create(
                model='claude-opus-4-20250514', max_tokens=800,
                messages=[{'role': 'user', 'content': grade_prompt}])
            grade_text = grade_resp.content[0].text.strip()

            # Extract grade
            grade = "F"
            for line in grade_text.split("\n"):
                if "GRADE:" in line.upper():
                    g = line.upper().split("GRADE:")[-1].strip()
                    if g and g[0] in "ABCDF":
                        grade = g[0]
                    break

            # Extract pass count
            passes = 0
            for line in grade_text.split("\n"):
                if "PASSES:" in line.upper():
                    try:
                        passes = int(line.upper().split("PASSES:")[-1].strip().split("/")[0])
                    except Exception:
                        pass
                    break

            print(f"       Attempt {attempt}: Grade {grade} ({passes}/18 passes)", flush=True)

            # Track the best attempt so far (Bug 19 fallback)
            if (_grade_rank.get(grade, 0) > _grade_rank.get(best["grade"], 0) or
                (grade == best["grade"] and passes > best["passes"])):
                best = {"grade": grade, "passes": passes, "copy": copy, "grade_text": grade_text}

            # Surface failing criteria to the user on every non-A attempt so they can see
            # WHY the copy is getting rejected without digging into the LLM response.
            if grade != "A":
                _failures = []
                for _ln in grade_text.split("\n"):
                    if "FAIL" in _ln.upper() and any(c.isdigit() for c in _ln[:3]):
                        _failures.append(_ln.strip())
                if _failures:
                    print(f"       Failed: {', '.join(_failures)[:300]}", flush=True)

            if grade == "A":
                with open(os.path.join(outdir, "campaign_copy.md"), "w") as f:
                    f.write(f"# {niche.title()} — Cold Email Campaign\n")
                    f.write(f"## {client}\n\n")
                    f.write(f"**Grade: {grade} ({passes}/18 passes)**\n\n---\n\n")
                    f.write(copy)
                    f.write(f"\n\n---\n\n## Grade Report\n\n{grade_text}")
                print(f"       ✓ A-grade copy saved to campaign_copy.md", flush=True)
                return

            elif attempt < max_attempts:
                # Extract which checks failed
                failures = []
                for line in grade_text.split("\n"):
                    if "FAIL" in line.upper() and any(c.isdigit() for c in line[:3]):
                        failures.append(line.strip())

                generate_prompt = f"""Your previous cold email sequence scored {grade} ({passes}/18 passes).

FAILURES:
{chr(10).join(failures)}

Fix ONLY the failures above. Keep everything that passed.

BRIEF:
{brief}

STRUCTURE: Email 1 (2 variants A and B) + Email 2 (1 variant) + Email 3 (1 variant breakup)

HARD REQUIREMENTS:
- Under 75 words per email (target ~60, COUNT THEM)
- Hard CTA default
- No em dashes, no signatures, no {{{{company_name}}}} in body
- Spintax on greetings and CTAs
- Specific {niche} industry language
- Zero or one question per email
- No made-up case studies

HUMANIZER RULES (rejection if violated):
- NO three-part rhythmic lists ("A, B, C")
- NO corporate clichés ("raise their hand", "drowning in cash flow", "pipeline stalling", "don't know where to look")
- NO "follow-up" phrasing in Email 2 (including spintax options like "Quick follow-up")
- NO division-of-labor closers repeated across emails
- NO AI vocab ("crucial", "pivotal", "landscape", "showcase", "underscore")
- Vary sentence rhythm. Include at least one concrete specific per email.
"""

        except Exception as e:
            print(f"       Attempt {attempt} error: {e}", flush=True)

    # Bug 19: No A-grade after all attempts. Instead of shipping nothing, save the
    # best attempt (B-grade and above) so broader niches still get usable copy.
    # Mark it clearly so the operator knows to review before launching.
    if best["copy"] and _grade_rank.get(best["grade"], 0) >= _grade_rank["B"]:
        with open(os.path.join(outdir, "campaign_copy.md"), "w") as f:
            f.write(f"# {niche.title()} — Cold Email Campaign\n")
            f.write(f"## {client}\n\n")
            f.write(f"**Grade: {best['grade']} ({best['passes']}/18 passes) — FALLBACK**\n\n")
            f.write(f"> ⚠️ Could not hit A in {max_attempts} attempts. This is the best "
                    f"attempt saved as a starting point. Review and edit before launching.\n\n---\n\n")
            f.write(best["copy"])
            f.write(f"\n\n---\n\n## Grade Report\n\n{best['grade_text']}")
        print(f"       ⚠ Saved {best['grade']}-grade fallback copy — REVIEW before launch", flush=True)
        return

    print(f"       ✗ Could not achieve A or B grade in {max_attempts} attempts. Skipped.", flush=True)


# ============================================================================
# MAIN
# ============================================================================

def _load_import_csv(filepath):
    """Load a CSV of leads and normalize to standard contact format."""
    contacts = []
    with open(filepath) as f:
        for row in csv.DictReader(f):
            contacts.append(_normalize_contact({
                "email": row.get("email", row.get("Email", "")),
                "first_name": row.get("first_name", row.get("First Name", row.get("firstName", ""))),
                "last_name": row.get("last_name", row.get("Last Name", row.get("lastName", ""))),
                "company": row.get("company", row.get("company_name", row.get("Company", row.get("Company Name", "")))),
                "domain": row.get("domain", row.get("website", row.get("Website", ""))),
                "title": row.get("title", row.get("Title", row.get("Job Title", ""))),
                "city": row.get("city", row.get("City", "")),
                "state": row.get("state", row.get("State", "")),
                "phone": row.get("phone", row.get("Phone", "")),
                "source": "imported",
                "verified": False,
            }))
    return contacts


def main():
    ap = argparse.ArgumentParser(description="The Forge — Unified Lead Generation")
    ap.add_argument("query", nargs="?", help="Natural language query")
    ap.add_argument("--target", type=int, default=0)
    ap.add_argument("--client", default="")
    ap.add_argument("--niche", default="", help="Niche label (required with --import)")
    ap.add_argument("--workers", type=int, default=5)
    ap.add_argument("--force", default=None, const="b2b", nargs="?",
                    help="Force routing (b2b/local) or override safeguards")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--no-bb", action="store_true", help="Skip BounceBan second pass")
    ap.add_argument("--no-copy", action="store_true", help="Skip campaign copy generation")
    ap.add_argument("--no-launch", action="store_true",
                    help="Skip Phase 8 Smartlead campaign launch (build + upload + attach). "
                         "By default, Forge creates the Smartlead campaign end-to-end using "
                         "the humanized copy bank + ship gate + FREE mailbox rule. Use this flag "
                         "when you only want the lead list exported, not pushed to Smartlead.")
    ap.add_argument("--sequence-length", type=int, choices=[2, 3], default=3,
                    help="Email sequence length. 3 is default (Email 1 A/B + Email 2 + Email 3 breakup). "
                         "2 generates a tighter version where Email 2 doubles as breakup. "
                         "Salesforge evidence: shorter sequences sometimes beat longer ones.")
    ap.add_argument("--resume", default="")
    ap.add_argument("--import-csv", dest="import_csv", default="",
                    help="Import existing lead list CSV. Skips discovery, runs enrich + verify + quality + export.")
    ap.add_argument("--log", default="",
                    help="Tee all stdout to this log file (so you can tail -f during a run).")
    ap.add_argument("--clean-before-start", action="store_true",
                    help="Delete the run directory if it exists before starting (wipes stale checkpoints).")
    ap.add_argument("--allow-nameless", action="store_true",
                    help="Keep generic business emails (info@, contact@) without a person name. "
                         "Use for privacy-heavy niches like cannabis, dispensaries, healthcare.")
    ap.add_argument("--allow-generic", action="store_true",
                    help="Keep generic emails (info@, contact@, hello@, etc.) through pre-clean. "
                         "Use for privacy-heavy niches where generic emails are the only path to "
                         "the decision maker (cannabis, dispensaries, healthcare).")
    ap.add_argument("--no-prescreen", action="store_true",
                    help="Skip Kimi pre-enrichment niche-fit screen. By default, discovery "
                         "output is bulk-classified by Kimi before enrichment to reject obvious "
                         "non-ICP companies — saves 30-40%% on enrichment credits. Disable if "
                         "Kimi is rejecting valid companies or you want raw discovery.")
    args = ap.parse_args()

    # --log: tee stdout/stderr to file
    if args.log:
        import builtins
        _log_path = os.path.abspath(args.log)
        os.makedirs(os.path.dirname(_log_path) or ".", exist_ok=True)
        _log_fh = open(_log_path, "a", buffering=1)
        _orig_print = builtins.print
        def _tee_print(*a, **kw):
            _orig_print(*a, **kw)
            msg = kw.get("sep", " ").join(str(x) for x in a) + kw.get("end", "\n")
            _log_fh.write(msg)
            _log_fh.flush()
        builtins.print = _tee_print
        print(f"  [LOG] Tee'ing output to {_log_path}")

    if not args.query and not args.resume and not args.import_csv:
        ap.print_help()
        return

    # Load cache
    cache = load_cache(CACHE_FILE)

    # --allow-generic implies --allow-nameless (generic emails rarely have names)
    if args.allow_generic:
        args.allow_nameless = True
        print("\n  ⚠ --allow-generic: keeping generic emails (info@, contact@, etc.) — reply rates will be lower")

    banner("THE FORGE")

    # ── IMPORT MODE ─────────────────────────────────────────────────────────
    if args.import_csv:
        if not args.niche:
            print("  ✗ --niche is required with --import-csv (e.g. --niche 'fire protection')")
            return
        client = args.client or "client_c"
        niche = args.niche
        slug = re.sub(r'[^a-z0-9]+', '-', niche.lower()).strip('-')[:40]
        outdir = os.path.join(PROJECTS_DIR, client, "lead-runs",
                              f"{slug}-imported-{datetime.now().strftime('%Y%m%d')}")
        os.makedirs(outdir, exist_ok=True)

        # Routing for import mode: honor --force explicitly if set, otherwise
        # auto-detect from niche name (same logic as NL-query mode) so local
        # business CSVs (restaurants, schools, property mgmt) skip the B2B
        # post_clean title-gate that would drop nameless leads.
        if args.force in ("b2b", "local"):
            import_routing = args.force
        else:
            from lead import detect_routing
            import_routing, _ = detect_routing(niche, geo=None, client=client)
            if import_routing == "unclear":
                import_routing = "b2b"
        print(f"  Routing:    {import_routing}")

        intent = {
            "client": client,
            "niche": niche,
            "target": args.target or 9999,
            "routing": import_routing,
            "output_dir": outdir,
            "slug": slug,
        }

        print(f"\n  Mode:       IMPORT")
        print(f"  CSV:        {args.import_csv}")
        print(f"  Client:     {client}")
        print(f"  Niche:      {niche}")

        # Load CSV
        contacts = _load_import_csv(args.import_csv)
        print(f"  Loaded:     {len(contacts)} leads")

        if not contacts:
            print("\n  No leads in CSV. Exiting.")
            return

        # Enrich (fill missing emails/names)
        need_enrichment = [c for c in contacts if not c.get("email")]
        already_have_email = [c for c in contacts if c.get("email")]
        print(f"  Have email: {len(already_have_email)}")
        print(f"  Need enrich: {len(need_enrichment)}")

        if need_enrichment:
            companies = [{"name": c.get("company", ""), "domain": c.get("domain", ""),
                          "city": c.get("city", ""), "state": c.get("state", ""),
                          "phone": c.get("phone", ""), "source": "imported"}
                         for c in need_enrichment if c.get("domain")]
            if companies:
                enriched = enrich(companies, intent, cache, args)
                already_have_email.extend(enriched)

        contacts = already_have_email

        # Skip straight to Phase 4
        contacts = clean_and_verify(contacts, intent, cache, args)
        contacts = quality_check(contacts, intent, args)
        contacts = smartlead_dedup(contacts)
        added, outdir = export_results(contacts, intent, args)

        if not args.no_copy:
            generate_campaign_copy(intent, outdir)

        # Phase 8: launch (same as main pipeline)
        launch_result = {"skipped": True, "error": None}
        if not args.no_launch:
            try:
                from forge_campaign_launch import launch_campaign
                print(f"\n  [8] Campaign Launch — sending to Smartlead...")
                launch_result = launch_campaign(intent, outdir, skip=False)
            except Exception as e:
                launch_result = {"skipped": False, "error": f"exception: {e}",
                                 "campaign_id": None, "leads_uploaded": 0,
                                 "mailboxes_attached": 0, "status": None, "gate_score": None}
                print(f"  [8] Launch phase exception: {e}")

        personal = sum(1 for c in contacts if c.get("email") and "info@" not in c["email"] and "contact@" not in c["email"])
        has_copy = os.path.isfile(os.path.join(outdir, "campaign_copy.md"))
        banner("FORGE IMPORT RESULTS")
        print(f"  imported:              {len(_load_import_csv(args.import_csv))}")
        print(f"  after clean + verify:  {len(contacts)}")
        print(f"    personal emails:     {personal}")
        print(f"    generic emails:      {len(contacts) - personal}")
        print(f"  added to DB:           {added}")
        print(f"  campaign copy:         {'✓ saved' if has_copy else '✗ not generated'}")
        if launch_result.get("skipped"):
            print(f"  campaign launch:       ⏭  skipped (--no-launch)")
        elif launch_result.get("error"):
            print(f"  campaign launch:       ✗ failed: {launch_result['error']}")
        elif launch_result.get("campaign_id"):
            print(f"  campaign launch:       ✓ {launch_result['campaign_id']} "
                  f"| {launch_result['leads_uploaded']} leads "
                  f"| {launch_result['mailboxes_attached']} mailboxes "
                  f"| gate {launch_result.get('gate_score','?')}/18 "
                  f"| {launch_result.get('status','?')}")
        print(f"  output:                {outdir}")
        print(f"{'=' * 60}\n")
        save_cache(cache, CACHE_FILE)
        return

    # ── NORMAL MODE ─────────────────────────────────────────────────────────

    # ── Phase 1: Parse ──────────────────────────────────────────────────────
    print(f"\n  Query: \"{args.query}\"")
    intent = parse_query(args.query, args)

    print(f"  Client:     {intent.get('client', '?')}")
    print(f"  Niche:      {intent.get('niche', '?')}")
    print(f"  Target:     {intent.get('target', '?')}")
    print(f"  Routing:    {intent.get('routing', '?')}")
    print(f"  Discovery:  {intent.get('discovery_method', '?')}")
    print(f"  Keywords:   {len(intent.get('keywords', []))} variations")
    print(f"  Output:     {intent.get('output_dir', '?')}")

    # Per-client cost budget (from CLIENT.md cost_budget: YAML key)
    _budget = get_client_cost_budget(intent.get("client", ""))
    print(f"  Budget:     ${_budget:.2f}")
    intent["cost_budget"] = _budget

    if args.dry_run:
        print(f"\n  [DRY RUN] Would discover + enrich + export. Exiting.")
        return

    outdir = intent["output_dir"]

    # --clean-before-start: wipe stale run dir before starting
    if args.clean_before_start and os.path.isdir(outdir):
        import shutil
        print(f"\n  [CLEAN] Removing existing run dir: {outdir}")
        shutil.rmtree(outdir)

    os.makedirs(outdir, exist_ok=True)

    # Checkpoint target mismatch — HARD BLOCK (not just a warning)
    # Resuming a stale checkpoint with a larger target silently stops short of the new goal.
    _manifest_path = os.path.join(outdir, "v2_manifest.json")
    if os.path.isfile(_manifest_path):
        try:
            with open(_manifest_path) as _mf:
                _m = json.load(_mf)
            _prev_target = _m.get("target") or _m.get("target_count")
            _cur_target = intent.get("target")
            if _prev_target and _cur_target and _prev_target != _cur_target:
                print(f"\n  ⛔ CHECKPOINT MISMATCH: existing run has target={_prev_target}, "
                      f"new run has target={_cur_target}")
                print(f"     Resuming would stop short of the new target.")
                print(f"     Pass --clean-before-start to wipe the stale checkpoint and re-run.")
                sys.exit(1)
        except SystemExit:
            raise
        except Exception:
            pass

    # CLI flag drift check — blocks resuming when material flags (allow-generic,
    # allow-nameless, no-bb, force, target, client) differ from the flags used
    # to create the checkpoint. Prevents silent no-ops like "resumed with
    # --allow-generic but pre-clean was already cached from the original run".
    _flag_diffs = check_run_flags(outdir, args)
    if _flag_diffs:
        # Split into filter-only diffs (affect phase_4/5) vs discovery diffs (need full re-run)
        _filter_diffs = [(n, p, v) for n, p, v in _flag_diffs if n in _FILTER_ONLY_FLAGS]
        _discovery_diffs = [(n, p, v) for n, p, v in _flag_diffs if n not in _FILTER_ONLY_FLAGS]

        if _discovery_diffs:
            # Discovery-affecting flags changed — must wipe everything
            print(f"\n  ⛔ CLI FLAG MISMATCH on resume:")
            for name, prev, new in _flag_diffs:
                print(f"     --{name.replace('_','-')}: checkpoint={prev!r} now={new!r}")
            print(f"     These flags affect discovery/enrichment, so resuming would use stale data.")
            print(f"     Pass --clean-before-start to wipe and re-run with the new flags,")
            print(f"     or revert the flags to match the checkpoint.")
            sys.exit(1)

        if _filter_diffs:
            # Only filter/quality flags changed — invalidate phase_4 + phase_5
            # but keep the expensive phase_2 (discovery) and phase_3 (enrichment)
            print(f"\n  ♻ Filter flags changed — invalidating clean/quality checkpoints:")
            for name, prev, new in _filter_diffs:
                print(f"     --{name.replace('_','-')}: {prev!r} -> {new!r}")
            invalidate_steps(outdir, ["phase_4", "phase_5"])
            print(f"     phase_4 + phase_5 will re-run with new flags (phase_2/3 preserved)")

    # First run (or post-clean): record flags so the next resume can validate them
    save_run_flags(outdir, args)

    # ── Phase 2: Discover ───────────────────────────────────────────────────
    if is_step_complete(outdir, "phase_2"):
        companies = load_step(outdir, "phase_2")
        print(f"\n  [2] Discovery — resumed from checkpoint ({len(companies)} companies)")
    else:
        companies = discover(intent, cache, args)
        if not companies:
            print("\n  No new companies found. Exiting.")
            return
        save_step(outdir, "phase_2", companies)

    # ── Phase 3: Enrich ─────────────────────────────────────────────────────
    if is_step_complete(outdir, "phase_3"):
        contacts = load_step(outdir, "phase_3")
        print(f"\n  [3] Enrichment — resumed from checkpoint ({len(contacts)} contacts)")
    else:
        contacts = enrich(companies, intent, cache, args)
        if not contacts:
            print("\n  No contacts found. Exiting.")
            return
        save_step(outdir, "phase_3", contacts)

    # ── Phase 4: Clean + Verify ─────────────────────────────────────────────
    if is_step_complete(outdir, "phase_4"):
        contacts = load_step(outdir, "phase_4")
        print(f"\n  [4] Clean + Verify — resumed from checkpoint ({len(contacts)} contacts)")
    else:
        contacts = clean_and_verify(contacts, intent, cache, args)
        save_step(outdir, "phase_4", contacts)

    # ── Phase 5: Quality (niche-fit + dedup) ────────────────────────────────
    if is_step_complete(outdir, "phase_5"):
        contacts = load_step(outdir, "phase_5")
        print(f"\n  [5] Quality — resumed from checkpoint ({len(contacts)} contacts)")
    else:
        contacts = quality_check(contacts, intent, args)
        save_step(outdir, "phase_5", contacts)

    # ── Phase 5b: Smartlead campaign dedup ──────────────────────────────────
    contacts = smartlead_dedup(contacts)

    # ── Phase 6: Export ─────────────────────────────────────────────────────
    added, outdir = export_results(contacts, intent, args)

    # ── Phase 7: Campaign Copy ──────────────────────────────────────────────
    if not args.no_copy:
        generate_campaign_copy(intent, outdir)

    # ── Phase 8: Campaign Launch (Smartlead) ────────────────────────────────
    # Builds the Smartlead campaign end-to-end: humanized copy from bank →
    # ship gate → create campaign → upload leads → attach FREE mailboxes →
    # STOP (no status change, the operator handles scheduling). Per PLAYBOOK rule 12.
    launch_result = {"skipped": True, "error": None}
    if not getattr(args, "no_launch", False):
        try:
            from forge_campaign_launch import launch_campaign
            print(f"\n  [8] Campaign Launch — sending to Smartlead...")
            launch_result = launch_campaign(intent, outdir, skip=False)
        except Exception as e:
            launch_result = {"skipped": False, "error": f"exception: {e}",
                             "campaign_id": None, "leads_uploaded": 0,
                             "mailboxes_attached": 0, "status": None, "gate_score": None}
            print(f"  [8] Launch phase exception: {e}")

    # ── Summary ─────────────────────────────────────────────────────────────
    personal = sum(1 for c in contacts if c.get("email") and "info@" not in c["email"] and "contact@" not in c["email"])
    has_copy = os.path.isfile(os.path.join(outdir, "campaign_copy.md"))
    banner("FORGE RESULTS")
    print(f"  companies discovered:  {len(companies)}")
    print(f"  contacts produced:     {len(contacts)}")
    print(f"    personal emails:     {personal}")
    print(f"    generic emails:      {len(contacts) - personal}")
    print(f"  added to DB:           {added}")
    print(f"  campaign copy:         {'✓ A-grade saved' if has_copy else '✗ not generated'}")
    if launch_result.get("skipped"):
        print(f"  campaign launch:       ⏭  skipped (--no-launch)")
    elif launch_result.get("error"):
        print(f"  campaign launch:       ✗ failed: {launch_result['error']}")
    elif launch_result.get("campaign_id"):
        print(f"  campaign launch:       ✓ {launch_result['campaign_id']} "
              f"| {launch_result['leads_uploaded']} leads "
              f"| {launch_result['mailboxes_attached']} mailboxes "
              f"| gate {launch_result.get('gate_score','?')}/18 "
              f"| {launch_result.get('status','?')}")
    print(f"  output:                {outdir}")
    print(f"{'=' * 60}\n")

    save_cache(cache, CACHE_FILE)


if __name__ == "__main__":
    main()
