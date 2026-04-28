#!/usr/bin/env python3.13
"""
niche_research.py — Pre-Forge vertical validation.

Why this exists:
  the operator burns $10-15 of Forge budget per new-vertical run. Some verticals
  produce 2-4% reply rates, others produce 0.5% (Property Tax Appeal
  incident on 2026-04-20 is the canonical example). Right now the only way
  to know is to run the campaign and find out.

  This agent burns 30 seconds of Kimi + a few Serper calls BEFORE
  committing Forge budget. Kimi synthesizes:
    - TAM estimate (how many companies fit in the US)
    - Typical decision-maker title + their pain points
    - 2-3 angles that have historically worked in similar verticals
    - Competing agencies / vendors flooding this space
    - Confidence score (0-10): should the operator run Forge or skip?

  At confidence 3 or below, the agent explicitly recommends NOT running.
  At 4-6, proceed with caution (test 50-lead batch first). At 7+, go.

What it does NOT do:
  - Not a substitute for actually running a campaign to see real replies
  - Not great for niches with very limited online presence (trades in
    small local markets) — those will score low even if viable

Usage:
  python3 tools/niche_research.py --niche "commercial HVAC contractors"
  python3 tools/niche_research.py --niche "fractional CMO" --client client_c
  python3 tools/niche_research.py --niche "bookkeeping services" --out brief.md

Writes a markdown brief to 03-Resources/niche-research/<niche-slug>.md
unless --out is specified.

Standalone tool — does not modify Forge code.
"""

import argparse
import json
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

import requests

SCRIPT_DIR = Path(__file__).resolve().parent
LEAD_PIPELINE_DIR = SCRIPT_DIR.parent
WORKSPACE_ROOT = LEAD_PIPELINE_DIR.parent.parent
RESEARCH_DIR = WORKSPACE_ROOT / "03-Resources" / "niche-research"
sys.path.insert(0, str(LEAD_PIPELINE_DIR))

try:
    from dotenv import load_dotenv
    load_dotenv(WORKSPACE_ROOT / ".env", override=True)
    load_dotenv(LEAD_PIPELINE_DIR / ".env", override=True)
except ImportError:
    pass

SERPER_KEY = os.environ.get("SERPER_API_KEY", "")
SERPER_ENDPOINT = "https://google.serper.dev/search"
BLITZ_API_KEY = os.environ.get("BLITZ_API_KEY", "")
# Canonical Blitz base URL — the env var may not be set in all contexts
BLITZ_BASE_URL = os.environ.get("BLITZ_BASE_URL") or "https://api.blitz-api.ai/v2"


# ============================================================
# Blitz data grounding — replaces Kimi TAM guessing with real counts
# ============================================================

def _blitz_post(endpoint: str, payload: dict, timeout: int = 25):
    """Minimal Blitz POST helper (matches pattern in run_msp_blitz.py)."""
    if not BLITZ_API_KEY:
        return None
    try:
        import time as _time
        r = requests.post(
            f"{BLITZ_BASE_URL}{endpoint}",
            json=payload,
            headers={"x-api-key": BLITZ_API_KEY, "Content-Type": "application/json"},
            timeout=timeout,
        )
        if r.status_code == 200:
            return r.json()
        if r.status_code == 429:
            _time.sleep(1)
            r = requests.post(
                f"{BLITZ_BASE_URL}{endpoint}",
                json=payload,
                headers={"x-api-key": BLITZ_API_KEY, "Content-Type": "application/json"},
                timeout=timeout,
            )
            return r.json() if r.status_code == 200 else None
        return None
    except Exception:
        return None


def blitz_tam_check(niche: str) -> dict:
    """Query Blitz for real company count matching the niche.

    Replaces Kimi's training-data guess about market size with a hard number
    from Blitz's B2B database. Returns:
      - tam_count: int (may be >1000 even if only 25 returned — check for total)
      - sample_companies: list of up to 10 company names for empirical decision-maker check
      - status: "ok" | "no_key" | "no_results" | "error"
    """
    if not BLITZ_API_KEY:
        return {"status": "no_key", "tam_count": None, "sample_companies": []}

    # Try progressively simpler keywords — Blitz does phrase matching, so
    # shorter forms usually match far more companies.
    # "commercial HVAC contractors" → ["commercial HVAC contractors",
    #                                   "HVAC contractors",
    #                                   "commercial HVAC",
    #                                   "HVAC"]
    words = niche.split()
    candidates = [niche]
    if len(words) >= 3:
        candidates.append(" ".join(words[1:]))       # drop first word
        candidates.append(" ".join(words[:-1]))      # drop last word
    if len(words) >= 2:
        # Also try each individual noun-like word (4+ chars, not an article)
        for w in words:
            if len(w) >= 4 and w.lower() not in ("commercial", "services", "consulting", "professional"):
                if w not in candidates:
                    candidates.append(w)
    # Dedupe while preserving order
    seen = set()
    candidates = [c for c in candidates if not (c.lower() in seen or seen.add(c.lower()))]

    best = None
    for kw in candidates:
        data = _blitz_post("/search/companies", {
            "company": {
                "keywords": {"include": [kw]},
                "hq": {"country_code": ["US"]},
            },
            "max_results": 25,
        })
        if not data:
            continue
        results = data.get("results", []) or []
        # Blitz uses "total_results" (fallback chain for future API changes)
        total = (
            data.get("total_results")
            or data.get("total_count")
            or data.get("total")
            or len(results)
        )
        sample = []
        for c in results[:10]:
            name = c.get("name") or c.get("company_name") or ""
            li = c.get("linkedin_url", "")
            if name or li:
                sample.append({"name": name, "linkedin_url": li})
        candidate_result = {
            "status": "ok" if sample else "no_results",
            "tam_count": int(total) if total else len(results),
            "sample_companies": sample,
            "keyword_used": kw,
        }
        # Prefer the MOST SPECIFIC keyword that still returns meaningful data
        # (not the broadest) — a broader term finds 10x more companies but they
        # may not match the niche. Take the first candidate with ≥5 results,
        # fall back to broadest if nothing meets that bar.
        if candidate_result["tam_count"] >= 5:
            if not best or (best.get("tam_count", 0) < 5 and candidate_result["tam_count"] >= 5):
                best = candidate_result
                break  # found a narrow match — use it
        elif not best:
            best = candidate_result

    return best or {"status": "error", "tam_count": None, "sample_companies": []}


def enrichment_quality_sample(sample_companies: list, max_lookups: int = 10) -> dict:
    """Phase 3: Mini-Forge equivalent — sample actual enrichment outcomes.

    For each sampled company from the Blitz TAM check, probe:
      1. Can we find a decision-maker? (title + first_name populated)
      2. Does the LinkedIn person have a usable email pattern?
      3. Is first_name present? (Property Tax Appeal preventer)

    Combines with title distribution (previous empirical_decision_maker_sample).
    One Blitz call per company — does the work of 2 functions.

    Returns:
      titles: [{title, count}] — distribution of decision-maker titles
      top_title: str — most common
      companies_sampled: int — how many we queried

      # Discoverability metrics (the Phase 3 win)
      discoverability_rate: float — % with findable decision-maker
      first_name_rate: float — % with populated first_name (KILLER metric: if
                                 <60% on sample, expect "Hey ," disasters downstream)
      total_persons_returned: int — across all sampled companies
      has_findable_dm: int — companies where a DM was returned
    """
    if not BLITZ_API_KEY or not sample_companies:
        return {
            "titles": [], "companies_sampled": 0, "top_title": None,
            "discoverability_rate": None, "first_name_rate": None,
            "total_persons_returned": 0, "has_findable_dm": 0,
        }

    from collections import Counter
    title_counter = Counter()
    companies_sampled = 0
    companies_with_dm = 0
    total_persons = 0
    persons_with_first_name = 0

    for c in sample_companies[:max_lookups]:
        li = c.get("linkedin_url") or ""
        if not li:
            continue
        # Cascade matches v2_enrichment.py — Owner/Founder tier first, then VP/Director fallbacks
        data = _blitz_post("/search/waterfall-icp-keyword", {
            "company_linkedin_url": li,
            "cascade": [
                {"include_title": ["Owner", "Founder", "President", "CEO", "Managing Partner"],
                 "exclude_title": ["assistant", "intern", "junior"],
                 "location": ["US"], "include_headline_search": False},
                {"include_title": ["Vice President", "VP", "Managing Director", "Partner"],
                 "exclude_title": ["assistant", "intern"],
                 "location": ["US"], "include_headline_search": True},
                {"include_title": ["Director", "General Manager", "Operations Manager"],
                 "exclude_title": ["assistant", "intern"],
                 "location": ["US"], "include_headline_search": True},
            ],
            "max_results": 2,
        })
        if not data:
            continue
        companies_sampled += 1
        persons = data.get("results") or []
        if persons:
            companies_with_dm += 1
        for result in persons[:2]:
            person = result.get("person", {})
            total_persons += 1
            # First-name presence = Property Tax Appeal preventer
            first = (person.get("first_name") or "").strip()
            if first:
                persons_with_first_name += 1

            # Title bucket
            title = ""
            for exp in person.get("experiences", []) or []:
                if exp.get("job_is_current"):
                    title = exp.get("job_title", "")
                    break
            if title:
                t = title.lower()
                if "owner" in t or "founder" in t:
                    bucket = "Owner/Founder"
                elif "president" in t or "ceo" in t:
                    bucket = "President/CEO"
                elif "vp" in t or "vice president" in t:
                    bucket = "VP"
                elif "director" in t:
                    bucket = "Director"
                elif "manager" in t:
                    bucket = "Manager"
                else:
                    bucket = title[:40]
                title_counter[bucket] += 1

    top_title = title_counter.most_common(1)[0][0] if title_counter else None
    titles = [{"title": t, "count": c} for t, c in title_counter.most_common(8)]
    discoverability_rate = (companies_with_dm / companies_sampled * 100) if companies_sampled else None
    first_name_rate = (persons_with_first_name / total_persons * 100) if total_persons else None

    return {
        "titles": titles,
        "companies_sampled": companies_sampled,
        "top_title": top_title,
        "discoverability_rate": discoverability_rate,
        "first_name_rate": first_name_rate,
        "total_persons_returned": total_persons,
        "has_findable_dm": companies_with_dm,
    }


# Keep the old name as a thin alias for backward compatibility
def empirical_decision_maker_sample(sample_companies: list, max_lookups: int = 5) -> dict:
    """Deprecated name — use enrichment_quality_sample. Kept for backward compat."""
    return enrichment_quality_sample(sample_companies, max_lookups=max_lookups)


# ============================================================
# Historical fit — compare niche to past CLIENT_C campaigns
# ============================================================

def historical_fit_score(niche: str) -> dict:
    """Read campaign_analyses/ markdown files, find closest match to this niche.

    Extracts reply rates from historical post-mortems. If we've run a similar
    vertical before, we have EMPIRICAL evidence of whether it converts.

    Returns:
      - closest_match: niche name or None
      - match_reply_rate: float or None
      - similarity_reason: string explaining the match
      - historical_campaigns: count of total past campaigns analyzed
    """
    projects_dir = WORKSPACE_ROOT / "01-Projects"
    if not projects_dir.exists():
        return {"closest_match": None, "match_reply_rate": None, "historical_campaigns": 0}

    analyses = []
    for client_dir in projects_dir.iterdir():
        if not client_dir.is_dir():
            continue
        ca_dir = client_dir / "campaign_analyses"
        if not ca_dir.exists():
            continue
        for md_file in ca_dir.glob("*.md"):
            try:
                text = md_file.read_text()
                # Extract reply rate from the analysis if present
                # Pattern: "Replies: X (Y%)" or "Reply rate: Y%"
                rate_match = re.search(r"Repl(?:ies|y rate)[:\s]+[\d,]+\s*\(?([\d.]+)%", text)
                reply_rate = float(rate_match.group(1)) if rate_match else None
                analyses.append({
                    "file": md_file.name,
                    "name": md_file.stem,
                    "text_snippet": text[:500].lower(),
                    "reply_rate": reply_rate,
                })
            except Exception:
                continue

    if not analyses:
        return {"closest_match": None, "match_reply_rate": None, "historical_campaigns": 0}

    # Simple keyword overlap scoring — count how many niche words appear in each analysis
    niche_words = set(w for w in re.findall(r"[a-z]+", niche.lower()) if len(w) >= 4)
    scored = []
    for a in analyses:
        hit_count = sum(1 for w in niche_words if w in a["text_snippet"])
        if hit_count > 0:
            scored.append({**a, "overlap": hit_count})

    scored.sort(key=lambda x: -x["overlap"])
    if not scored:
        return {
            "closest_match": None,
            "match_reply_rate": None,
            "historical_campaigns": len(analyses),
            "similarity_reason": f"no keyword overlap with {len(analyses)} past campaigns",
        }

    top = scored[0]
    return {
        "closest_match": top["name"],
        "match_reply_rate": top["reply_rate"],
        "historical_campaigns": len(analyses),
        "similarity_reason": f"matched {top['overlap']} of {len(niche_words)} niche keywords in {top['name']}",
    }


# ============================================================
# Quick Serper search for market context
# ============================================================

def serper_search(query: str, num: int = 10) -> list:
    if not SERPER_KEY:
        return []
    try:
        r = requests.post(
            SERPER_ENDPOINT,
            headers={"X-API-KEY": SERPER_KEY, "Content-Type": "application/json"},
            json={"q": query, "num": num},
            timeout=20,
        )
        if r.status_code != 200:
            return []
        return r.json().get("organic", [])
    except Exception:
        return []


def gather_market_signals(niche: str, deep: bool = False) -> dict:
    """Run targeted searches to build market context for Kimi to synthesize.

    Default (light mode): Serper snippets for market/pain/competition +
    Reddit complaint threads + sales-role job postings + regulatory deadlines.
    Cost: ~$0.006 Serper. Runtime ~5s.

    Deep mode (--deep): also runs Firecrawl full-page scrape on top 3
    pain-points articles. Gives Kimi ~15K chars of real article content
    instead of 150-char snippets. Cost: +$0.60 Firecrawl. Runtime ~30s.

    Returns dict with keys: market, pain_points, competition, reddit,
    job_postings, regulatory, article_content (deep only).
    """
    signals = {}

    size_q = f'"{niche}" market size OR landscape OR industry report'
    size_results = serper_search(size_q, num=5)
    signals["market"] = [{"title": r.get("title",""), "snippet": r.get("snippet","")}
                          for r in size_results[:5]]

    pain_q = f'"{niche}" challenges OR problems OR pain points OR struggles'
    pain_results = serper_search(pain_q, num=5)
    signals["pain_points"] = [{"title": r.get("title",""), "snippet": r.get("snippet",""), "link": r.get("link","")}
                               for r in pain_results[:5]]

    agency_q = f'lead generation agency for "{niche}" OR marketing for "{niche}"'
    agency_results = serper_search(agency_q, num=5)
    signals["competition"] = [{"title": r.get("title",""), "snippet": r.get("snippet","")}
                               for r in agency_results[:5]]

    # Phase 2: Reddit signals — raw unfiltered complaint language
    reddit_q = f'site:reddit.com {niche} problem OR complaint OR frustrated OR struggle'
    reddit_results = serper_search(reddit_q, num=8)
    signals["reddit"] = [{"title": r.get("title",""), "snippet": r.get("snippet","")}
                         for r in reddit_results[:8]]

    # Phase 2: Job posting signals — sales/BD roles = growth budget for outbound
    jobs_q = f'"VP of sales" OR "sales director" OR "business development" {niche} hiring 2026'
    jobs_results = serper_search(jobs_q, num=5)
    signals["job_postings"] = [{"title": r.get("title",""), "snippet": r.get("snippet","")}
                               for r in jobs_results[:5]]

    # Phase 2: Regulatory/dated deadlines — specific Change-force hooks
    reg_q = f'{niche} 2026 OR 2027 deadline OR regulation OR compliance mandate OR rule'
    reg_results = serper_search(reg_q, num=5)
    signals["regulatory"] = [{"title": r.get("title",""), "snippet": r.get("snippet","")}
                             for r in reg_results[:5]]

    # Phase 2 deep: Firecrawl full-page scrape of top pain-points articles
    if deep:
        import subprocess, tempfile
        article_content = []
        for result in signals.get("pain_points", [])[:3]:
            url = result.get("link", "")
            if not url:
                continue
            try:
                with tempfile.NamedTemporaryFile(mode="w", suffix=".md", delete=False) as tf:
                    tmp_path = tf.name
                r = subprocess.run(
                    ["firecrawl", "scrape", url, "-o", tmp_path, "--formats", "markdown"],
                    capture_output=True, text=True, timeout=30,
                )
                if r.returncode == 0:
                    try:
                        content = Path(tmp_path).read_text()[:5000]  # cap per article
                        article_content.append({
                            "url": url, "title": result.get("title",""), "content": content,
                        })
                    except Exception:
                        pass
                Path(tmp_path).unlink(missing_ok=True)
            except Exception:
                continue
        signals["article_content"] = article_content
    else:
        signals["article_content"] = []

    return signals


# ============================================================
# ClientC V3 scoring — based on the operator's RM_162_Niche_Scoring_V3
# methodology (Fazio offer × Pain Urgency × Saturation × Econ V2 × Svc V3)
# ============================================================

# V3 composite weights (from RM_Niche_Scoring_Prompt_V3.md)
V3_WEIGHTS = {
    "fazio":      0.30,
    "pain":       0.15,
    "saturation": 0.20,
    "econ_v2":    0.20,
    "svc_v3":     0.15,
}


def replication_score(prospect_tam: int) -> int:
    """Derive replication score from Prospect TAM — the operator's V3 scale.

    Small Prospect TAMs cause list exhaustion within months; large ones
    allow campaigns to run indefinitely. This score captures that risk.
    """
    if prospect_tam is None:
        return 5  # unknown — neutral
    tiers = [
        (1_000_000, 10), (500_000, 9), (300_000, 8), (200_000, 7),
        (100_000, 6), (50_000, 5), (30_000, 4), (15_000, 3),
        (5_000, 2),
    ]
    for threshold, score in tiers:
        if prospect_tam >= threshold:
            return score
    return 1


def cycle_score(days: int) -> int:
    """Sales cycle → serviceability points (shorter = better)."""
    if days is None:
        return 5
    if days <= 14:  return 10
    if days <= 30:  return 9
    if days <= 45:  return 8
    if days <= 60:  return 7
    if days <= 90:  return 5
    if days <= 120: return 3
    if days <= 180: return 1
    return 0  # >180 = auto-fail per V3 filter


def dm_reachable_score(dm_reachable: str) -> int:
    """Yes/Partial/No → 0-10 score."""
    m = {"Yes": 10, "Partial": 5, "No": 0}
    return m.get(dm_reachable, 5)


def econ_v2_score(deal_usd: float, ltv_usd: float, gm_pct: float,
                   cushion: int) -> int:
    """V3 Economic Viability formula.

    Econ_V2 = MIN(10, MAX(1, ROUND(
        MIN(GP/15000, 2)
      + MIN(LTV/25000, 2)
      + MIN(Margin/18, 3)
      + MIN((LTV/Deal)/3, 1.5)
      + Cushion × 0.375
    )))
    """
    if not all([deal_usd, ltv_usd, gm_pct]):
        return 5
    gp = deal_usd * (gm_pct / 100)
    score = (
        min(gp / 15000, 2)
        + min(ltv_usd / 25000, 2)
        + min(gm_pct / 18, 3)
        + min((ltv_usd / deal_usd) / 3, 1.5)
        + (cushion or 0) * 0.375
    )
    return int(round(max(1, min(10, score))))


def run_stress_tests(deal_usd: float, gm_pct: float, payment: str,
                      cycle_days: int) -> dict:
    """V3 stress tests — Test A, B, C, D. Cushion = count of tests passed.

    Test A: GP - (2 × CAC) > 0
    Test B: GP_at_30%_conv_drop - (1.43 × CAC) > 0
    Test C: Cashflow risk not HIGH (HIGH = Commission/Net30 AND cycle > 60)
    Test D: GP_at_-20%_deal - CAC > 0
    """
    if not all([deal_usd, gm_pct]):
        return {"cushion": 0, "results": "????", "tests": {}}
    gp = deal_usd * (gm_pct / 100)
    cac_ceiling = min(max(gp * 0.3, 1500), 4500)
    test_a = gp - (2 * cac_ceiling) > 0
    test_b = (gp * 0.7) - (1.43 * cac_ceiling) > 0
    # Cashflow risk HIGH = commission/net30 AND long cycle
    risky_payment = (payment or "").lower() in ("commission", "net 30", "net30")
    test_c = not (risky_payment and (cycle_days or 0) > 60)
    test_d = (gp * 0.8) - cac_ceiling > 0
    results = "".join("P" if t else "F" for t in (test_a, test_b, test_c, test_d))
    return {
        "cushion": sum([test_a, test_b, test_c, test_d]),
        "results": results,  # e.g., "PPPP" = all pass, "PPFP" = test C failed
        "cac_ceiling": round(cac_ceiling),
        "gp_usd": round(gp),
        "tests": {
            "A_gp_minus_2x_cac":   round(gp - 2*cac_ceiling),
            "B_conv_drop_30pct":   round(gp*0.7 - 1.43*cac_ceiling),
            "C_cashflow":          "OK" if test_c else "HIGH",
            "D_deal_down_20pct":   round(gp*0.8 - cac_ceiling),
        },
    }


def svc_v3_score(dm_reachable: str, cycle_days: int, lead_source: int,
                  replication: int) -> float:
    """V3 Serviceability = equal-weight average of 4 sub-scores."""
    return round(
        (dm_reachable_score(dm_reachable) * 0.25)
        + (cycle_score(cycle_days) * 0.25)
        + ((lead_source or 5) * 0.25)
        + ((replication or 5) * 0.25),
        2,
    )


def v3_composite(fazio: float, pain: float, saturation: float,
                  econ_v2: float, svc_v3: float) -> float:
    """V3 composite = weighted sum per V3_WEIGHTS."""
    return round(
        fazio * V3_WEIGHTS["fazio"]
        + pain * V3_WEIGHTS["pain"]
        + saturation * V3_WEIGHTS["saturation"]
        + econ_v2 * V3_WEIGHTS["econ_v2"]
        + svc_v3 * V3_WEIGHTS["svc_v3"],
        2,
    )


def v3_funnel_stage(composite: float, rm_tam: int, cycle_days: int,
                     fazio: float, cushion: int) -> str:
    """Classify niche into V3 funnel stage.

    Stage 1 (Discovery): everything starts here
    Stage 2 (Initial Filter): RM_TAM ≥1000, cycle ≤180d, Fazio ≥6
    Stage 3 (Economic Validation): passes Tests A and D (cushion ≥3 — allowing 1 fail
             but not A or D specifically; we approximate by cushion threshold)
    Stage 4 (Saturation Analysis): composite ≥ 7
    Stage 5 (Final Selection): composite ≥ 8
    """
    if not rm_tam or rm_tam < 1000 or (cycle_days and cycle_days > 180) or (fazio and fazio < 6):
        return "Stage 2 — filtered out (fails initial criteria)"
    if cushion is not None and cushion < 3:
        return "Stage 3 — filtered out (economic validation)"
    if composite >= 8:
        return "Stage 5 — final selection (top-tier candidate)"
    if composite >= 7:
        return "Stage 4 — passed saturation analysis"
    return "Stage 3 — passed economic validation, below top tier"


def prospect_tam_lookup(prospect_description: str, prospect_naics: str = None) -> dict:
    """Look up the CLIENT's prospect TAM (not CLIENT_C's TAM).

    Example: niche="fire protection" → prospect="commercial property managers"
    → NAICS 531312 → BLS lookup → ~15,000 establishments.

    Kimi provides the prospect NAICS in its V3 synthesis. We use that to hit
    the local BLS QCEW cache. No additional API calls needed.
    """
    if not prospect_naics:
        return {"count": None, "naics": None, "source": "no NAICS provided"}
    cache = _load_qcew_cache()
    if prospect_naics in cache:
        e = cache[prospect_naics]
        return {"count": e["establishments"], "naics": prospect_naics,
                "source": f"BLS QCEW 2023 ({e.get('label','')[:40]})"}
    # Hierarchy walk for inexact match
    for trunc_len in (5, 4, 3, 2):
        if len(prospect_naics) > trunc_len and prospect_naics[:trunc_len] in cache:
            e = cache[prospect_naics[:trunc_len]]
            return {"count": e["establishments"], "naics": prospect_naics[:trunc_len],
                    "source": f"BLS QCEW 2023 {trunc_len}-digit parent"}
    return {"count": None, "naics": prospect_naics, "source": "not in BLS cache"}


# ============================================================
# NAICS + Census cross-reference — TAM accuracy corrector
# ============================================================

def infer_naics_code(niche: str) -> dict:
    """Use Kimi to infer the 6-digit NAICS 2017 industry code for a niche.

    NAICS codes are keys into US Census County Business Patterns data, which
    gives authoritative establishment counts. Kimi knows NAICS well.
    Returns {"naics": "238220", "label": "Plumbing, Heating, AC Contractors"}.
    """
    try:
        from llm_router import get_light_client
        client_llm, model = get_light_client()
    except Exception:
        return {"naics": None, "label": None}

    prompt = f"""Return ONLY the most-specific 6-digit NAICS 2017 code for this US business niche, plus the official label. No preamble.

Niche: {niche}

Respond as JSON: {{"naics": "123456", "label": "Official NAICS 2017 Label"}}

If no clean fit, return {{"naics": null, "label": null}}. Do NOT invent codes."""

    try:
        resp = client_llm.messages.create(
            model=model, max_tokens=200,
            messages=[{"role": "user", "content": prompt}],
        )
        text = resp.content[0].text.strip()
        text = re.sub(r"^```(?:json)?\s*", "", text)
        text = re.sub(r"\s*```$", "", text)
        return json.loads(text)
    except Exception:
        return {"naics": None, "label": None}


# BLS QCEW 2023 cache — comprehensive local lookup, zero API dependency.
# Built once from data.bls.gov annual-by-industry CSV. Covers 2,069 NAICS
# codes (2-digit to 6-digit granularity). Kills Census API reliability issues.
# Refresh yearly via:
#   ~/agency-os/03-Resources/naics-reference/refresh_qcew.sh
_QCEW_CACHE = None
_QCEW_CACHE_PATH = Path(__file__).resolve().parent.parent.parent.parent / "03-Resources" / "naics-reference" / "qcew_2023.json"


def _load_qcew_cache() -> dict:
    """Lazy-load the BLS QCEW 2023 JSON cache into memory."""
    global _QCEW_CACHE
    if _QCEW_CACHE is not None:
        return _QCEW_CACHE
    try:
        _QCEW_CACHE = json.loads(_QCEW_CACHE_PATH.read_text())
    except Exception:
        _QCEW_CACHE = {}
    return _QCEW_CACHE


def census_establishment_count(naics_code: str) -> dict:
    """Look up real US establishment count for a NAICS code.

    Try order:
      1. BLS QCEW 2023 local cache (primary — 2,069 NAICS codes, zero API
         dependency, updated annually from data.bls.gov CSV flat files)
      2. Live Census 2022 CBP API (when available, freshest annual)
      3. Live Census 2017 ecnbasic API (stable 5-year Economic Census)

    Kimi may output either 2017 NAICS (e.g. 238220 combined) or 2022 NAICS
    (split into 238221 residential + 238222 nonresidential). Cache handles both:
    if the exact code isn't found, tries the 5-digit parent (e.g. 23822).

    Returns {"count": int, "year": int, "source": str} or
            {"count": None, "year": None, "source": "unavailable"}.
    """
    if not naics_code or not naics_code.isdigit():
        return {"count": None, "year": None, "source": "unavailable"}

    # 1. Try BLS QCEW local cache — primary source
    cache = _load_qcew_cache()
    if naics_code in cache:
        e = cache[naics_code]
        return {
            "count": e["establishments"],
            "year": 2023,
            "source": f"BLS QCEW 2023 ({e.get('label', '')[:40]})",
        }
    # 2017 NAICS codes sometimes differ from 2022 (e.g. 238220 vs 238221/238222).
    # Walk up the hierarchy: 238220 → 23822 (parent) → 2382 (grandparent)
    for trunc_len in (5, 4, 3, 2):
        if len(naics_code) > trunc_len and naics_code[:trunc_len] in cache:
            e = cache[naics_code[:trunc_len]]
            return {
                "count": e["establishments"],
                "year": 2023,
                "source": f"BLS QCEW 2023 ({e.get('label', '')[:40]}, {trunc_len}-digit parent of {naics_code})",
            }

    # 2-3. Fall back to live Census API if cache missed
    for label, year, dataset in [
        ("2022 cbp (live)",      "2022", "cbp"),
        ("2017 ecnbasic (live)", "2017", "ecnbasic"),
    ]:
        try:
            r = requests.get(
                f"https://api.census.gov/data/{year}/{dataset}",
                params={"get": "ESTAB", "for": "us:*", "NAICS2017": naics_code},
                timeout=10,
            )
            if r.status_code != 200:
                continue
            txt = r.text.strip()
            if txt.startswith("Sorry") or not txt.startswith("["):
                continue
            data = r.json()
            if len(data) >= 2 and str(data[1][0]).isdigit():
                return {"count": int(data[1][0]), "year": int(year), "source": label}
        except Exception:
            continue

    return {"count": None, "year": None, "source": "unavailable"}


def naics_cross_reference(niche: str, blitz_count: int = None) -> dict:
    """Combine NAICS inference + Census CBP to compute TAM coverage.

    Returns:
      naics_code: str or None
      naics_label: str or None
      census_establishments: int or None (authoritative US count)
      census_year: int or None
      blitz_coverage_pct: float or None  (blitz_count / census_establishments)
      coverage_note: str  (interpretation: "LinkedIn-representative" vs "trades/local gap")
    """
    naics_info = infer_naics_code(niche)
    naics_code = naics_info.get("naics")
    naics_label = naics_info.get("label")

    census = census_establishment_count(naics_code) if naics_code else {"count": None, "year": None}

    coverage_pct = None
    note = ""
    if census["count"] and blitz_count is not None and blitz_count >= 0:
        coverage_pct = (blitz_count / census["count"]) * 100
        if coverage_pct >= 50:
            note = "Blitz is representative for this vertical (LinkedIn-heavy B2B)."
        elif coverage_pct >= 10:
            note = "Blitz covers a subset — trades / mid-market niche with some LinkedIn gap. TAM is larger than Blitz shows."
        else:
            note = f"⚠ Blitz significantly undercounts (only {coverage_pct:.1f}% of true TAM). LinkedIn-poor vertical. Consider Serper Maps or state licensing registries for fuller discovery."

    return {
        "naics_code": naics_code,
        "naics_label": naics_label,
        "census_establishments": census["count"],
        "census_year": census["year"],
        "census_source": census.get("source", "unavailable"),
        "blitz_coverage_pct": round(coverage_pct, 2) if coverage_pct is not None else None,
        "coverage_note": note,
    }


# ============================================================
# Kimi synthesis
# ============================================================

def _load_framework() -> str:
    """Load the distilled copywriting framework for prompt injection.

    See 03-Resources/copywriting-frameworks/ for MASTER.md (full reference)
    and PROMPT.md (compressed injection version). Returns PROMPT.md content
    or empty string if file missing.
    """
    fw_path = WORKSPACE_ROOT / "03-Resources" / "copywriting-frameworks" / "PROMPT.md"
    try:
        return fw_path.read_text()
    except Exception:
        return ""


def synthesize_brief(niche: str, signals: dict, client: str = None,
                     blitz_tam: dict = None, dm_sample: dict = None,
                     historical: dict = None, naics_ref: dict = None) -> dict:
    """Use Kimi to turn raw search snippets into a structured niche brief.

    Framework-aware (Schwartz awareness/sophistication + Whitman LF8 +
    Masterson lead types + Cialdini weapons). Data-grounded (Blitz TAM,
    empirical decision-maker sample, historical CLIENT_C campaign fit).

    Returns dict with strategic classification + data grounding + multi-signal
    score (0-100) instead of the old single-metric Kimi confidence.
    """
    try:
        from llm_router import get_light_client
        client_llm, model = get_light_client()
    except Exception as e:
        return {"error": f"llm_router unavailable: {e}"}

    market_blob = "\n".join([f"- {s['title']}: {s['snippet']}" for s in signals.get("market", [])])
    pain_blob = "\n".join([f"- {s['title']}: {s['snippet']}" for s in signals.get("pain_points", [])])
    competition_blob = "\n".join([f"- {s['title']}: {s['snippet']}" for s in signals.get("competition", [])])
    reddit_blob = "\n".join([f"- {s['title']}: {s['snippet']}" for s in signals.get("reddit", [])])
    jobs_blob = "\n".join([f"- {s['title']}: {s['snippet']}" for s in signals.get("job_postings", [])])
    reg_blob = "\n".join([f"- {s['title']}: {s['snippet']}" for s in signals.get("regulatory", [])])
    # Deep content (Firecrawl full-page scrape) — cap total to avoid prompt explosion
    deep_blob = ""
    for art in signals.get("article_content", [])[:3]:
        deep_blob += f"\n### ARTICLE: {art.get('title','')} ({art.get('url','')[:80]})\n{art.get('content','')[:4000]}\n"

    context_hint = ""
    if client == "client_c":
        context_hint = "ClientC sells cold email lead gen TO these companies. Does the vertical have businesses that need cold outbound (i.e., B2B services looking for more clients)?"
    elif client == "client_a":
        context_hint = "ClientA provides fire protection services TO these companies. Are these commercial buildings with fire systems?"
    elif client == "client_b":
        context_hint = "ClientB provides cybersecurity consulting TO these companies. Do they handle sensitive data / have compliance obligations?"

    framework = _load_framework()
    framework_block = ""
    if framework:
        framework_block = f"""
COPYWRITING FRAMEWORK (use this to classify the niche):
{framework}
"""

    # Build data-grounding block from Blitz + historical (these override LLM guesses)
    grounding_lines = []
    if blitz_tam and blitz_tam.get("status") == "ok":
        grounding_lines.append(
            f"BLITZ TAM CHECK (real database count): {blitz_tam['tam_count']} US companies match keyword '{blitz_tam.get('keyword_used', niche)}'."
        )
    elif blitz_tam and blitz_tam.get("status") == "no_key":
        grounding_lines.append("BLITZ TAM CHECK: skipped (no API key).")
    else:
        grounding_lines.append("BLITZ TAM CHECK: no results found — market may be too narrow or keyword mismatch.")

    # NAICS / Census cross-reference — TAM reality check
    if naics_ref and naics_ref.get("census_establishments"):
        grounding_lines.append(
            f"NAICS CROSS-REFERENCE: Code {naics_ref['naics_code']} ({naics_ref['naics_label']}) — "
            f"US Census reports {naics_ref['census_establishments']:,} real US establishments ({naics_ref['census_year']}). "
            f"Blitz coverage: {naics_ref.get('blitz_coverage_pct', 'n/a')}% of true TAM. "
            f"{naics_ref.get('coverage_note', '')}"
        )
    elif naics_ref and naics_ref.get("naics_code"):
        grounding_lines.append(
            f"NAICS CROSS-REFERENCE: Code {naics_ref['naics_code']} identified but Census API unreachable — use Blitz TAM alone."
        )

    if dm_sample and dm_sample.get("top_title"):
        title_dist = ", ".join(f"{t['title']} ({t['count']})" for t in dm_sample["titles"][:5])
        grounding_lines.append(
            f"EMPIRICAL DECISION-MAKER SAMPLE ({dm_sample['companies_sampled']} companies queried): top titles — {title_dist}. Most common: {dm_sample['top_title']}."
        )
        # Phase 3: discoverability — the Property Tax Appeal preventer
        disc = dm_sample.get("discoverability_rate")
        fn_rate = dm_sample.get("first_name_rate")
        if disc is not None:
            grounding_lines.append(
                f"ENRICHMENT DISCOVERABILITY: {disc:.0f}% of sampled companies had a findable decision-maker via Blitz; "
                f"{fn_rate:.0f}% of returned persons had populated first_name. "
                + ("⚠ FIRST_NAME RATE BELOW 60% — expect 'Hey ,' disaster like Property Tax Appeal 2026-04-20 if you proceed without a data_quality_check pass." if (fn_rate is not None and fn_rate < 60) else "")
                + ("⚠ DISCOVERABILITY BELOW 40% — Forge will struggle to find emails; consider SKIP." if (disc is not None and disc < 40) else "")
            )
    else:
        grounding_lines.append("EMPIRICAL DECISION-MAKER SAMPLE: not available (use inference + flag as lower-confidence).")

    if historical and historical.get("closest_match"):
        rate_str = f"{historical['match_reply_rate']}% reply rate" if historical.get("match_reply_rate") else "reply rate not recorded"
        grounding_lines.append(
            f"HISTORICAL CLIENT_C FIT: closest past campaign match is '{historical['closest_match']}' ({rate_str}). {historical.get('similarity_reason', '')}. Total past campaigns analyzed: {historical['historical_campaigns']}."
        )
    elif historical and historical.get("historical_campaigns", 0) > 0:
        grounding_lines.append(
            f"HISTORICAL CLIENT_C FIT: {historical['historical_campaigns']} past campaigns analyzed but none overlap keywords with this niche — treat as novel."
        )
    else:
        grounding_lines.append("HISTORICAL CLIENT_C FIT: no past campaign data available for comparison.")

    grounding_block = "\n".join(grounding_lines)

    prompt = f"""You are evaluating whether cold email lead gen for this vertical is worth pursuing.

NICHE: {niche}

{f'CLIENT CONTEXT: {context_hint}' if context_hint else ''}
{framework_block}
DATA GROUNDING (real data — TRUST THIS OVER YOUR TRAINING-DATA INTUITION):
{grounding_block}

MARKET SIGNALS (from web search — use for grounding, don't invent):

Market research snippets:
{market_blob or '(no signals found)'}

Pain points snippets:
{pain_blob or '(no signals found)'}

Competition / existing agencies:
{competition_blob or '(no signals found)'}

Reddit complaint threads (raw unfiltered pain language — source for winning angles):
{reddit_blob or '(no Reddit signals found)'}

Sales/BD job postings in this niche (proxy for companies with growth budget + outbound appetite):
{jobs_blob or '(no hiring signals found)'}

Regulatory deadlines / dated compliance windows (Schwartz Change-Force hooks — use for timing urgency):
{reg_blob or '(no dated regulatory signals found)'}
{"FULL-PAGE ARTICLE CONTENT (deep scrape — richer pain-point language):" + deep_blob if deep_blob else ""}

Output a structured evaluation as JSON with these exact fields:

{{
  "summary": "<1-sentence what this vertical is + rough health of the market>",
  "tam_estimate": "<rough # of US companies fitting this niche — e.g. 'roughly 2,000-5,000 firms' — OK to say 'unknown' if signals are weak>",
  "decision_maker": "<typical title of who buys — e.g. 'Owner or President' for small services, 'VP Sales' for mid-market>",
  "pain_points": ["<3 specific pains the decision-maker feels, based on signals>"],
  "winning_angles": ["<2-3 cold email angles that would likely resonate>"],
  "competition": "<1-sentence: how crowded is this niche with cold-email agencies already targeting it?>",

  "awareness_level": "<Most Aware | Product Aware | Solution Aware | Problem Aware | Unaware — Schwartz's 5 levels, classify the typical prospect>",
  "sophistication_stage": <integer 1-5 — Schwartz's 5 stages of market sophistication. 1=virgin, 5=exhausted>,
  "dominant_life_force": "<one of: Survival | Fear/Pain | Comfort | Superiority | Protection of Loved Ones | Social Approval — Whitman's LF8>",
  "force_type": "<Permanent | Change — is the pain always there (Permanent) or trend/deadline-driven (Change)?>",
  "change_trigger": "<if Change: what specific current event/regulation/trend drives urgency? null if Permanent>",
  "recommended_lead_type": "<Offer | Promise | Problem-Solution | Big Secret | Proclamation | Story — pick using the Awareness×Sophistication matrix>",
  "lead_type_reasoning": "<1 sentence explaining why this lead type fits>",
  "persuasion_stack": ["<2-3 Cialdini weapons most applicable: Reciprocation, Commitment, Social Proof, Liking, Authority, Scarcity>"],

  "v3_rm_acquisition": {{
    "comment": "V3 framework from RM_Niche_Scoring_Prompt_V3. Answers: 'If CLIENT_C signs this niche as a client, how strong is THEIR cold email offer to THEIR prospects?' This is the PRIMARY scoring for CLIENT_C client acquisition decisions.",
    "their_prospect": "<who the CLIENT would be cold-emailing — e.g. 'commercial property managers at 500+ employee companies' for fire-alarm client>",
    "their_prospect_naics": "<6-digit NAICS for the prospect's industry — e.g. '531312' for nonresidential property managers>",
    "their_cold_email_offer": "<the offer the client's cold email would make, under 80 words>",
    "typical_deal_usd": <integer, e.g. 15000>,
    "ltv_usd": <integer, e.g. 45000>,
    "gross_margin_pct": <integer 40-90>,
    "payment": "<Upfront | Monthly | Commission | Net 30 | Milestone>",
    "sales_cycle_days": <integer>,
    "rm_tam": <integer — # of potential CLIENT_C clients in this niche. If different from BLS count above, explain why>,

    "fazio": {{
      "claim": <1-10 — specific quantifiable? ("$50K-500K missed taxes"=10, "help businesses grow"=3)>,
      "risk_reversal": <1-10 — ("no savings no fee"=10, "$50K retainer"=2)>,
      "social_proof": <1-10 — ("saved $200K"=10, "improved morale"=3)>,
      "prospect_math": <1-10 — ("$2M property = ~$80K savings"=10, "better leadership"=2)>,
      "total": <float — average of the 4>
    }},

    "pain_urgency": <1-10 — 10=legally mandated + deadline, 9=IRS/regulatory, 8=actively losing $, 7=probably losing $, 6=ROI but optional, 5=measurable improvement, 4=strategic, 3=nice to have, 2=discretionary>,
    "pain_urgency_rationale": "<1 sentence anchoring score in specific deadlines/penalties/losses>",

    "lead_source": <1-10 — 10=perfect Apollo targeting, 7=targetable+enrichment, 4=hard to identify, 1=nearly impossible>,
    "lead_source_rationale": "<what titles/SIC/filters would you use?>",

    "saturation": <1-10 — 10=zero cold email agencies in this niche, 5=moderate, 1=extreme saturation. Base on competition signals above>,
    "dm_reachable": "<Yes | Partial | No — can the prospect's decision maker be reached via cold email?>"
  }},

  "confidence": <integer 0-10 — legacy scalar, keep for backward compat>,
  "multi_signal_score": {{
    "tam_score": <0-20 — Blitz count: 20 if >10K companies, 15 if 2-10K, 10 if 500-2K, 5 if 200-500, 0 if <200>,
    "decision_maker_fit_score": <0-20 — empirical DM sample: 20 if owner/president dominant, 12 if mixed, 5 if VP-heavy, 0 if unknown>,
    "discoverability_score": <0-20 — from ENRICHMENT DISCOVERABILITY above: 20 if discoverability >=80% AND first_name_rate >=80%, 12 if discoverability 50-80% AND first_name_rate >=60%, 5 if one below threshold, 0 if discoverability <40% OR first_name_rate <40% (fatal — Forge will produce garbage leads)>,
    "competition_score": <0-20 — inverted; 20 = thin cold-email competition, 12 = moderate, 5 = saturated>,
    "historical_fit_score": <0-20 — HISTORICAL CLIENT_C FIT: 20 if match hit >3%, 12 if match hit >1%, 8 if novel niche, 0 if match failed>,
    "total": <sum 0-100>
  }},
  "recommendation": "<one of: SKIP, TEST_50_LEADS, GO>",
  "risk_notes": ["<2-3 things that could make this campaign fail — specific, not generic>"]
}}

CRITICAL RULES:
1. For TAM: USE the Blitz TAM count from DATA GROUNDING. Do NOT make up a number — cite the Blitz figure verbatim.
2. For decision_maker: USE the empirical DM sample from DATA GROUNDING when available. Only infer when no data.
3. Use the framework above to classify awareness_level and sophistication_stage.
4. Apply the decision matrix (awareness × sophistication → lead type) rigorously.
5. If historical CLIENT_C fit data shows a similar niche hit >3%, bump confidence. If it hit <1%, lower confidence.
6. SKIP = TAM <500 OR saturated competition OR decision-maker wrong fit.
7. TEST_50_LEADS = plausible but risky — run seed test. Use when multi_signal_score is 40-70.
8. GO = multi_signal_score >70 AND historical fit aligns AND angles clear.
9. Winning_angles must channel existing desire (per Schwartz) — don't invent new desires.
10. Multi-signal score is the PRIMARY decision input; scalar confidence is legacy.

Output ONLY the JSON. No preamble, no markdown code fences."""

    import time as _time
    last_err = None
    for attempt in range(3):
        try:
            resp = client_llm.messages.create(
                model=model, max_tokens=1500,
                messages=[{"role": "user", "content": prompt}],
            )
            text = resp.content[0].text.strip()
            text = re.sub(r"^```(?:json)?\s*", "", text)
            text = re.sub(r"\s*```$", "", text)
            return json.loads(text)
        except Exception as e:
            last_err = e
            # Rate limit or transient error — back off + retry
            if "rate_limit" in str(e).lower() or "overloaded" in str(e).lower() or "429" in str(e):
                _time.sleep(5 + attempt * 3)
                continue
            # Other error types — don't retry
            break
    return {"error": f"kimi synthesis failed: {last_err}"}


# ============================================================
# Render + save
# ============================================================

def _fmt_int(v):
    """Format a number with commas, or '?' if None/non-numeric."""
    try:
        return f"{int(v):,}"
    except (TypeError, ValueError):
        return str(v) if v else "?"


def render_brief(niche: str, brief: dict, signals: dict, client: str = None,
                 blitz_tam: dict = None, dm_sample: dict = None,
                 historical: dict = None, naics_ref: dict = None) -> str:
    if brief.get("error"):
        return f"# Niche Research FAILED: {niche}\n\nError: {brief['error']}"

    confidence = brief.get("confidence", 0)
    rec = brief.get("recommendation", "UNKNOWN")
    rec_icon = {"SKIP": "🛑", "TEST_50_LEADS": "⚠️", "GO": "✅"}.get(rec, "❓")
    score = brief.get("multi_signal_score", {})
    total_score = score.get("total") if score else None

    v3 = brief.get("v3_rm_acquisition") or {}
    v3_composite_val = v3.get("composite_v3")

    out = f"""# Niche Research: {niche}

**Generated:** {datetime.now(timezone.utc).isoformat()}
**Client context:** {client or "(none — agnostic evaluation)"}
**Recommendation:** {rec_icon} **{rec}** (multi-signal score {total_score if total_score is not None else '?'}/100, legacy confidence {confidence}/10)
**V3 CLIENT_C Acquisition Composite:** **{v3_composite_val if v3_composite_val else '?'}/10** (Fazio × 30% + Pain × 15% + Saturation × 20% + Econ V2 × 20% + Svc V3 × 15%)
**V3 Funnel Stage:** {v3.get('funnel_stage','not computed')}

## TAM reality check (NAICS + Census cross-reference)
- **NAICS code:** {naics_ref.get('naics_code') if naics_ref else 'not identified'} ({naics_ref.get('naics_label') if naics_ref else 'n/a'})
- **US Census establishments:** {f"{naics_ref['census_establishments']:,}" if naics_ref and naics_ref.get('census_establishments') else 'n/a'}  ·  source: {naics_ref.get('census_source','n/a') if naics_ref else 'n/a'}  ·  year: {naics_ref.get('census_year') if naics_ref else 'n/a'}
- **Blitz coverage of true TAM:** {f"{naics_ref['blitz_coverage_pct']}%" if naics_ref and naics_ref.get('blitz_coverage_pct') is not None else 'cannot compute'}
- **Interpretation:** {naics_ref.get('coverage_note', '(no coverage note)') if naics_ref else 'no NAICS data'}

## Multi-signal score breakdown
- **TAM score:** {score.get('tam_score', '?')}/20 (Blitz count: {blitz_tam.get('tam_count') if blitz_tam else 'n/a'}; Census: {f"{naics_ref['census_establishments']:,}" if naics_ref and naics_ref.get('census_establishments') else 'n/a'})
- **Decision-maker fit:** {score.get('decision_maker_fit_score', '?')}/20 (top empirical title: {dm_sample.get('top_title') if dm_sample else 'not sampled'})
- **Discoverability:** {score.get('discoverability_score', '?')}/20 (DM findable: {f"{dm_sample.get('discoverability_rate'):.0f}%" if dm_sample and dm_sample.get('discoverability_rate') is not None else 'n/a'}, first_name populated: {f"{dm_sample.get('first_name_rate'):.0f}%" if dm_sample and dm_sample.get('first_name_rate') is not None else 'n/a'})
- **Competition score:** {score.get('competition_score', '?')}/20 (higher = less saturated)
- **Historical fit:** {score.get('historical_fit_score', '?')}/20 (closest match: {historical.get('closest_match') if historical else 'none'})
- **Total:** **{total_score if total_score is not None else '?'}/100**

## Summary
{brief.get("summary", "(no summary)")}

## Market shape
- **TAM estimate:** {brief.get("tam_estimate", "unknown")}
- **Decision maker:** {brief.get("decision_maker", "unknown")}
- **Competition:** {brief.get("competition", "unknown")}

## Copywriting framework positioning (Schwartz + Whitman + Masterson + Cialdini)
- **Awareness level:** {brief.get("awareness_level", "unknown")}
- **Sophistication stage:** {brief.get("sophistication_stage", "unknown")}/5
- **Dominant Life Force desire:** {brief.get("dominant_life_force", "unknown")}
- **Force type:** {brief.get("force_type", "unknown")}"""
    if brief.get("change_trigger"):
        out += f" — trigger: {brief['change_trigger']}"
    out += f"""
- **Recommended lead type:** {brief.get("recommended_lead_type", "unknown")}
- **Lead reasoning:** {brief.get("lead_type_reasoning", "")}
- **Persuasion stack:** {', '.join(brief.get("persuasion_stack", []))}

## V3 CLIENT_C Acquisition Scoring (Fazio + Pain + Saturation + Econ V2 + Svc V3)

**Their prospect:** {v3.get('their_prospect','?')}
**Their cold email offer:** _{v3.get('their_cold_email_offer','?')}_

| Dimension | Weight | Score | Calculation |
|---|---:|---:|---|
| **Fazio Offer** | 30% | {(v3.get('fazio') or {}).get('total','?')} | Claim {(v3.get('fazio') or {}).get('claim','?')}, Risk Reversal {(v3.get('fazio') or {}).get('risk_reversal','?')}, Social Proof {(v3.get('fazio') or {}).get('social_proof','?')}, Prospect Math {(v3.get('fazio') or {}).get('prospect_math','?')} |
| **Pain Urgency** | 15% | {v3.get('pain_urgency','?')} | {v3.get('pain_urgency_rationale','')} |
| **Saturation** | 20% | {v3.get('saturation','?')} | Cold-email agency competition (higher = less saturated) |
| **Econ V2** | 20% | {v3.get('econ_v2','?')} | GP {v3.get('stress_tests',{}).get('gp_usd','?')}, LTV {v3.get('ltv_usd','?')}, Margin {v3.get('gross_margin_pct','?')}%, Cushion {v3.get('stress_tests',{}).get('cushion','?')}/4 |
| **Svc V3** | 15% | {v3.get('svc_v3','?')} | DM {v3.get('dm_reachable','?')}, Cycle {v3.get('sales_cycle_days','?')}d, Lead Src {v3.get('lead_source','?')}, Replication {v3.get('replication_score','?')} |
| **Composite** | | **{v3.get('composite_v3','?')}** | V3 weighted sum |

**Economics breakdown:**
- Typical deal: ${v3.get('typical_deal_usd','?')}  ·  LTV: ${v3.get('ltv_usd','?')}  ·  Gross margin: {v3.get('gross_margin_pct','?')}%
- Payment: {v3.get('payment','?')}  ·  Sales cycle: {v3.get('sales_cycle_days','?')}d
- CLIENT_C TAM (signable clients): {_fmt_int(v3.get('rm_tam'))}
- **Their Prospect TAM (campaigns CLIENT_C would run for them): {_fmt_int(v3.get('prospect_tam'))}** via NAICS {v3.get('their_prospect_naics','?')} ({v3.get('prospect_tam_source','?')})

**Stress tests:** `{v3.get('stress_tests',{}).get('results','????')}` (cushion {v3.get('stress_tests',{}).get('cushion','?')}/4)
- Test A: GP - 2×CAC = ${v3.get('stress_tests',{}).get('tests',{}).get('A_gp_minus_2x_cac','?')}
- Test B: GP @ -30% conv - 1.43×CAC = ${v3.get('stress_tests',{}).get('tests',{}).get('B_conv_drop_30pct','?')}
- Test C: Cashflow = {v3.get('stress_tests',{}).get('tests',{}).get('C_cashflow','?')}
- Test D: GP @ -20% deal - CAC = ${v3.get('stress_tests',{}).get('tests',{}).get('D_deal_down_20pct','?')}
- CAC ceiling: ${v3.get('stress_tests',{}).get('cac_ceiling','?')}

**Funnel stage:** {v3.get('funnel_stage','not computed')}

**Spreadsheet row** (paste into RM_162_Niche_Scoring_V3 Master Ranking):
```
{niche} | V3: {v3.get('composite_v3','?')} | Fazio: {(v3.get('fazio') or {}).get('total','?')} | Pain: {v3.get('pain_urgency','?')} | Sat: {v3.get('saturation','?')} | Econ V2: {v3.get('econ_v2','?')} | Svc V3: {v3.get('svc_v3','?')} | Lead Src: {v3.get('lead_source','?')} | Repl: {v3.get('replication_score','?')} | Stress: {v3.get('stress_tests',{}).get('results','????')} | Cushion: {v3.get('stress_tests',{}).get('cushion','?')} | Deal: ${v3.get('typical_deal_usd','?')} | LTV: ${v3.get('ltv_usd','?')} | GM: {v3.get('gross_margin_pct','?')}% | GP: ${v3.get('stress_tests',{}).get('gp_usd','?')} | CLIENT_C TAM: {v3.get('rm_tam','?')} | Prospect TAM: {v3.get('prospect_tam','?')} | Cycle: {v3.get('sales_cycle_days','?')}d | Payment: {v3.get('payment','?')} | DM: {v3.get('dm_reachable','?')}
```

---

## Decision-maker pain points
"""
    for p in brief.get("pain_points", []):
        out += f"- {p}\n"

    out += "\n## Cold email angles that would likely work\n"
    for a in brief.get("winning_angles", []):
        out += f"- {a}\n"

    out += "\n## Risk notes (why this might fail)\n"
    for r in brief.get("risk_notes", []):
        out += f"- {r}\n"

    # Decision guidance
    out += "\n---\n\n## What to do with this\n\n"
    if rec == "SKIP":
        out += "**Don't run Forge on this vertical.** Signals suggest low reply rate or wrong fit. Move to a different niche.\n"
    elif rec == "TEST_50_LEADS":
        out += ("**Run a seed test first.** Kick off Forge with `--target 50` to generate ~50 leads, upload to a test campaign, "
                "watch reply rate for 3-5 days. If reply rate >= 1.5% on 50 sends, scale to full 500-1000 run.\n")
    elif rec == "GO":
        out += f"**Proceed with a full Forge run** (target 500-1000 leads). Use the winning angles above in copy. Watch the risk notes during campaign analysis.\n"

    # Raw signals for audit
    out += "\n---\n\n## Raw signals (audit trail)\n\n### Market research\n"
    for s in signals.get("market", []):
        out += f"- **{s['title']}** — {s['snippet']}\n"
    out += "\n### Pain points\n"
    for s in signals.get("pain_points", []):
        out += f"- **{s['title']}** — {s['snippet']}\n"
    out += "\n### Competing agencies\n"
    for s in signals.get("competition", []):
        out += f"- **{s['title']}** — {s['snippet']}\n"
    if signals.get("reddit"):
        out += "\n### Reddit threads (raw complaint language — source for winning angles)\n"
        for s in signals["reddit"]:
            out += f"- **{s['title']}** — {s['snippet']}\n"
    if signals.get("job_postings"):
        out += "\n### Sales/BD job postings (growth-intent signal)\n"
        for s in signals["job_postings"]:
            out += f"- **{s['title']}** — {s['snippet']}\n"
    if signals.get("regulatory"):
        out += "\n### Regulatory / dated deadlines (Schwartz Change-Force hooks)\n"
        for s in signals["regulatory"]:
            out += f"- **{s['title']}** — {s['snippet']}\n"
    if signals.get("article_content"):
        out += "\n### Deep article scrapes (Firecrawl, --deep mode)\n"
        for art in signals["article_content"]:
            out += f"- **{art.get('title','')}** ({art.get('url','')[:80]}) — {len(art.get('content',''))} chars captured\n"

    out += f"\n---\n*Generated by `niche_research.py` — verify before committing Forge budget.*\n"
    return out


# ============================================================
# CLI
# ============================================================

def _log_prediction(niche: str, client: str, brief: dict, blitz_tam: dict,
                    dm_sample: dict, historical: dict, naics_ref: dict = None) -> None:
    """Append the prediction to _predictions.jsonl for future feedback-loop comparison.

    When a campaign in this niche runs, campaign_analyzer can cross-reference
    this file to compute prediction-vs-actual deltas and recalibrate scoring
    weights over time.
    """
    import time
    log_path = RESEARCH_DIR / "_predictions.jsonl"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    score = brief.get("multi_signal_score", {})
    entry = {
        "ts": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "niche": niche,
        "client": client,
        "recommendation": brief.get("recommendation"),
        "confidence": brief.get("confidence"),
        "multi_signal_total": score.get("total"),
        "tam_score": score.get("tam_score"),
        "dm_fit_score": score.get("decision_maker_fit_score"),
        "discoverability_score": score.get("discoverability_score"),
        "competition_score": score.get("competition_score"),
        "historical_fit_score": score.get("historical_fit_score"),
        "blitz_tam_count": blitz_tam.get("tam_count") if blitz_tam else None,
        "naics_code": naics_ref.get("naics_code") if naics_ref else None,
        "naics_label": naics_ref.get("naics_label") if naics_ref else None,
        "census_establishments": naics_ref.get("census_establishments") if naics_ref else None,
        "blitz_coverage_pct": naics_ref.get("blitz_coverage_pct") if naics_ref else None,
        "discoverability_rate": dm_sample.get("discoverability_rate") if dm_sample else None,
        "first_name_rate": dm_sample.get("first_name_rate") if dm_sample else None,
        "top_title": dm_sample.get("top_title") if dm_sample else None,
        "closest_historical": historical.get("closest_match") if historical else None,
        # V3 CLIENT_C acquisition scoring fields
        "v3_composite": ((brief.get("v3_rm_acquisition") or {}).get("composite_v3")),
        "v3_fazio": (((brief.get("v3_rm_acquisition") or {}).get("fazio") or {}).get("total")),
        "v3_pain_urgency": ((brief.get("v3_rm_acquisition") or {}).get("pain_urgency")),
        "v3_saturation": ((brief.get("v3_rm_acquisition") or {}).get("saturation")),
        "v3_econ_v2": ((brief.get("v3_rm_acquisition") or {}).get("econ_v2")),
        "v3_svc": ((brief.get("v3_rm_acquisition") or {}).get("svc_v3")),
        "v3_cushion": (((brief.get("v3_rm_acquisition") or {}).get("stress_tests") or {}).get("cushion")),
        "v3_funnel_stage": ((brief.get("v3_rm_acquisition") or {}).get("funnel_stage")),
        "v3_prospect_tam": ((brief.get("v3_rm_acquisition") or {}).get("prospect_tam")),
        "v3_rm_tam": ((brief.get("v3_rm_acquisition") or {}).get("rm_tam")),
        "v3_typical_deal_usd": ((brief.get("v3_rm_acquisition") or {}).get("typical_deal_usd")),
        "v3_ltv_usd": ((brief.get("v3_rm_acquisition") or {}).get("ltv_usd")),
        # actual_reply_rate gets filled in later by campaign_analyzer
        "actual_reply_rate": None,
    }
    with open(log_path, "a") as f:
        f.write(json.dumps(entry) + "\n")


def _dry_run_estimate(niche: str, deep: bool = False) -> None:
    """Show what API calls + costs would happen, without spending anything."""
    est = {
        "Serper searches × 6 (market/pain/competition/reddit/jobs/regulatory)": "$0.006 (~6 × $0.001)",
        "Blitz /search/companies (TAM check, up to 5 keyword variants)": "$0 (included in plan)",
        "Blitz /search/waterfall-icp-keyword (10 sample companies × 1 cascade each)": "$0 (included in plan)",
        "Kimi NAICS inference (short call)": "~$0.0002",
        "US Census CBP API (NAICS establishment count)": "free",
        "Kimi synthesis (framework + expanded signals + NAICS)": "~$0.002-0.003",
        "File writes + prediction log": "free",
    }
    if deep:
        est["Firecrawl full-page scrape × 3 articles (--deep mode)"] = "~$0.60 (3 × $0.20)"
    print(f"\n=== DRY RUN — niche='{niche}' — mode: {'DEEP' if deep else 'LIGHT'} ===\n")
    print("What would happen (no API calls made):\n")
    for step, cost in est.items():
        print(f"  • {step}")
        print(f"      cost: {cost}")
    if deep:
        print(f"\nEstimated TOTAL: $0.608 - $0.609 per run (Firecrawl dominates)")
        print(f"Runtime: ~30-45 seconds")
    else:
        print(f"\nEstimated TOTAL: $0.008 - $0.010 per run")
        print(f"Runtime: ~10-20 seconds")
    print(f"\nRun without --dry-run to actually execute.\n")


def main():
    ap = argparse.ArgumentParser(description="Pre-Forge vertical validation")
    ap.add_argument("--niche", required=True, help="niche name, e.g. 'commercial HVAC contractors'")
    ap.add_argument("--client", choices=["client_c", "client_a", "client_b"],
                    help="client context for fit evaluation")
    ap.add_argument("--out", help="output markdown file (default: 03-Resources/niche-research/<slug>.md)")
    ap.add_argument("--json", action="store_true", help="also print raw JSON brief")
    ap.add_argument("--dry-run", action="store_true",
                    help="show API calls + estimated cost without executing")
    ap.add_argument("--no-log", action="store_true",
                    help="skip writing prediction to _predictions.jsonl (testing only)")
    ap.add_argument("--deep", action="store_true",
                    help="Phase 2 deep mode: also Firecrawl-scrape top 3 pain-points articles (~$0.60 extra, ~30s slower, dramatically richer pain-point language for Kimi)")
    args = ap.parse_args()

    if args.dry_run:
        _dry_run_estimate(args.niche, deep=args.deep)
        return

    print(f"Researching niche: {args.niche}")
    if args.client:
        print(f"Client context:    {args.client}")

    mode_note = "DEEP (with Firecrawl)" if args.deep else "LIGHT"
    print(f"\nGathering market signals (Serper × 6 + Reddit/jobs/regulatory) — {mode_note} mode...")
    signals = gather_market_signals(args.niche, deep=args.deep)
    total_snippets = sum(len(v) for k, v in signals.items() if k != "article_content")
    articles = len(signals.get("article_content", []))
    print(f"  → {total_snippets} snippets across 6 signal types" + (f" + {articles} full articles scraped" if articles else ""))

    print(f"\nRunning Blitz TAM check...")
    blitz_tam = blitz_tam_check(args.niche)
    if blitz_tam.get("status") == "ok":
        print(f"  → {blitz_tam['tam_count']} US companies match '{blitz_tam.get('keyword_used', args.niche)}' ({len(blitz_tam['sample_companies'])} sampled)")
    elif blitz_tam.get("status") == "no_key":
        print(f"  → skipped (BLITZ_API_KEY not set)")
    else:
        print(f"  → no Blitz results — market may be too narrow for this keyword")

    print(f"\nCross-referencing NAICS + Census for TAM accuracy...")
    naics_ref = naics_cross_reference(args.niche, blitz_count=blitz_tam.get("tam_count"))
    if naics_ref.get("census_establishments"):
        print(f"  → NAICS {naics_ref['naics_code']} ({naics_ref['naics_label']}): {naics_ref['census_establishments']:,} US establishments (Census {naics_ref['census_year']})")
        if naics_ref.get("blitz_coverage_pct") is not None:
            print(f"  → Blitz covers {naics_ref['blitz_coverage_pct']}% of true TAM")
    elif naics_ref.get("naics_code"):
        print(f"  → NAICS {naics_ref['naics_code']} identified; Census API unreachable (using Blitz alone)")
    else:
        print(f"  → no NAICS match found for this niche")

    print(f"\nSampling enrichment quality (Blitz mini-forge) ...")
    # max_lookups=10 uses the full sample for both title distribution AND
    # discoverability/first_name presence (Phase 3 Property-Tax-Appeal preventer)
    dm_sample = enrichment_quality_sample(blitz_tam.get("sample_companies", []), max_lookups=10)
    if dm_sample.get("top_title"):
        disc = dm_sample.get("discoverability_rate")
        fn = dm_sample.get("first_name_rate")
        print(f"  → sampled {dm_sample['companies_sampled']} companies, top title: {dm_sample['top_title']}")
        if disc is not None:
            print(f"  → discoverability: {disc:.0f}% of companies had findable DM")
        if fn is not None:
            warn = "  ⚠ BELOW 60% — expect 'Hey ,' problems" if fn < 60 else ""
            print(f"  → first_name populated: {fn:.0f}% of returned persons{warn}")
    else:
        print(f"  → no empirical sample (falling back to Kimi inference)")

    print(f"\nChecking historical CLIENT_C campaign fit...")
    historical = historical_fit_score(args.niche)
    if historical.get("closest_match"):
        rate_str = f"{historical['match_reply_rate']}% reply rate" if historical.get("match_reply_rate") else "rate not recorded"
        print(f"  → closest past match: {historical['closest_match']} ({rate_str})")
    else:
        print(f"  → no overlapping past campaigns in {historical.get('historical_campaigns', 0)} analyzed")

    print(f"\nSynthesizing brief via Kimi (framework + data grounded + NAICS cross-ref + V3 scoring)...")
    brief = synthesize_brief(args.niche, signals, client=args.client,
                             blitz_tam=blitz_tam, dm_sample=dm_sample,
                             historical=historical, naics_ref=naics_ref)

    # V3 deterministic computations (Kimi provided estimates, Python does math)
    v3 = brief.get("v3_rm_acquisition") or {}
    if v3:
        # Prospect TAM lookup via BLS using Kimi-inferred prospect NAICS
        prospect_tam_result = prospect_tam_lookup("", v3.get("their_prospect_naics"))
        v3["prospect_tam"] = prospect_tam_result.get("count")
        v3["prospect_tam_source"] = prospect_tam_result.get("source", "unknown")

        # Stress tests
        stress = run_stress_tests(
            deal_usd=v3.get("typical_deal_usd"),
            gm_pct=v3.get("gross_margin_pct"),
            payment=v3.get("payment"),
            cycle_days=v3.get("sales_cycle_days"),
        )
        v3["stress_tests"] = stress
        cushion = stress.get("cushion", 0)

        # Econ V2
        v3["econ_v2"] = econ_v2_score(
            deal_usd=v3.get("typical_deal_usd"),
            ltv_usd=v3.get("ltv_usd"),
            gm_pct=v3.get("gross_margin_pct"),
            cushion=cushion,
        )

        # Replication + Svc V3
        repl = replication_score(v3.get("prospect_tam"))
        v3["replication_score"] = repl
        v3["svc_v3"] = svc_v3_score(
            dm_reachable=v3.get("dm_reachable"),
            cycle_days=v3.get("sales_cycle_days"),
            lead_source=v3.get("lead_source"),
            replication=repl,
        )

        # V3 Composite
        fazio_total = (v3.get("fazio") or {}).get("total") or 5
        v3["composite_v3"] = v3_composite(
            fazio=fazio_total,
            pain=v3.get("pain_urgency", 5),
            saturation=v3.get("saturation", 5),
            econ_v2=v3["econ_v2"],
            svc_v3=v3["svc_v3"],
        )

        # Funnel stage
        v3["funnel_stage"] = v3_funnel_stage(
            composite=v3["composite_v3"],
            rm_tam=v3.get("rm_tam") or (blitz_tam.get("tam_count") if blitz_tam else 0),
            cycle_days=v3.get("sales_cycle_days"),
            fazio=fazio_total,
            cushion=cushion,
        )

        brief["v3_rm_acquisition"] = v3
        print(f"\nV3 CLIENT_C acquisition scoring:")
        print(f"  → Composite V3: {v3['composite_v3']}/10   (Fazio {fazio_total:.1f} × 30% + Pain {v3.get('pain_urgency','?')} × 15% + Sat {v3.get('saturation','?')} × 20% + Econ {v3['econ_v2']} × 20% + Svc {v3['svc_v3']} × 15%)")
        print(f"  → Stress tests: {stress['results']} (cushion {cushion}/4)")
        print(f"  → Prospect TAM: {v3.get('prospect_tam') or '?'} (NAICS {v3.get('their_prospect_naics','?')})")
        print(f"  → Funnel: {v3['funnel_stage']}")

    if brief.get("error"):
        print(f"\nFAILED: {brief['error']}")
        sys.exit(1)

    # Render markdown
    md = render_brief(args.niche, brief, signals, client=args.client,
                      blitz_tam=blitz_tam, dm_sample=dm_sample,
                      historical=historical, naics_ref=naics_ref)

    # Save
    if args.out:
        out_path = Path(args.out)
    else:
        slug = re.sub(r"[^a-z0-9]+", "-", args.niche.lower()).strip("-")[:60]
        out_path = RESEARCH_DIR / f"{slug}.md"

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(md)
    print(f"\n✓ Brief saved: {out_path}")

    # Feedback loop — log prediction so campaign_analyzer can cross-reference
    # later with actual reply rate from the same niche
    if not args.no_log:
        try:
            _log_prediction(args.niche, args.client, brief, blitz_tam, dm_sample, historical, naics_ref)
        except Exception as e:
            print(f"  (prediction log skipped: {e})")

    # Summary to stdout
    print(f"\n{'='*60}")
    rec = brief.get("recommendation", "?")
    icon = {"SKIP": "🛑", "TEST_50_LEADS": "⚠️", "GO": "✅"}.get(rec, "?")
    score = brief.get("multi_signal_score", {})
    total = score.get("total", "?")
    print(f"{icon} {rec} — multi-signal {total}/100 (legacy confidence {brief.get('confidence', '?')}/10)")
    print(f"{'='*60}")
    print(f"  {brief.get('summary', '')}")

    if args.json:
        print(f"\n--- JSON ---")
        print(json.dumps(brief, indent=2))


if __name__ == "__main__":
    main()
