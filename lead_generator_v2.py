#!/usr/bin/env python3
"""
Lead Generator v2 — Unified Target-Driven Pipeline
====================================================
0. Pre-filter companies
1. Blitz Discovery (/search/companies)
2. Blitz Enrichment (contacts + emails)
3. Target Check (enough emails?)
4. Backfill (pattern inference → Icypeas/Hunter → Serper geo-grid)
4.5 Pre-Clean (light — save verification credits)
5. Verify ALL (MV + BounceBan)
6. Deliverability Scoring (Tier 1/2/3)
7. Post-Clean (strict — decision makers only)
8. Deduplicate
9. Cache (automatic, cross-client)
10. Export (segmented by tier)

Usage:
    python3 lead_generator_v2.py --client "cost segregation firms" --target 500
    python3 lead_generator_v2.py --client "MSPs" --target 500 --keywords "managed service provider;MSP;IT support"
    python3 lead_generator_v2.py --resume --name costseg-v2
"""

import os
import sys
import time
import logging
import argparse
from datetime import datetime, timezone

# Ensure pipeline dir is on path
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SCRIPT_DIR)

from v2_config import V2Config, SCRIPT_DIR as PIPELINE_DIR, PROJECTS_DIR
from v2_cache import load_cache, save_cache, cache_stats, V2_CACHE_FILE
from v2_checkpoint import (
    save_step, load_step, is_step_complete, load_manifest, clear_checkpoints,
)
from v2_discovery import blitz_discover, prefilter_companies, serper_discover
from v2_enrichment import blitz_enrich, check_target, backfill
from v2_cleaning import pre_clean, post_clean
from v2_scoring import verify_all, score_tiers
from v2_export import deduplicate, export_all

from config import (
    BLITZ_API_KEY, MILLIONVERIFIER_API_KEY, BOUNCEBAN_API_KEY,
    SERPER_API_KEY, ANTHROPIC_API_KEY,
)
from v2_config import AIARK_API_KEY

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("v2")


# ==============================================================================
# UTILS
# ==============================================================================

def slugify(text, max_len=60):
    """Convert text to kebab-case slug."""
    import re
    s = text.lower().strip()
    s = re.sub(r"[^a-z0-9\s-]", "", s)
    s = re.sub(r"[\s_]+", "-", s)
    s = re.sub(r"-+", "-", s).strip("-")
    return s[:max_len]


def resolve_output_dir(cfg):
    """Determine output directory based on client name."""
    if cfg.output_dir:
        return cfg.output_dir

    # Try to find client project folder
    name = cfg.client_name or slugify(cfg.client_description)
    # Check if project folder exists
    for folder in os.listdir(PROJECTS_DIR):
        if folder.lower() == name.lower() or slugify(folder) == slugify(name):
            return os.path.join(PROJECTS_DIR, folder, "lead-runs", name)

    # Default: output in pipeline dir
    return os.path.join(PIPELINE_DIR, "output", name)


# ==============================================================================
# ICP GENERATION
# ==============================================================================

def generate_icp_if_needed(cfg):
    """Generate ICP from client description if no keywords provided."""
    if cfg.keyword_sets:
        log.info(f"  Using {len(cfg.keyword_sets)} custom keyword sets")
        # Still generate ICP for target_roles and classifier keywords
        if ANTHROPIC_API_KEY and cfg.client_description:
            try:
                from icp import generate_icp
                icp = generate_icp(cfg.client_description)
                if icp:
                    cfg.target_roles = icp.get("target_roles", cfg.target_roles)
                    cfg.positive_keywords = icp.get("positive_keywords", [])
                    cfg.negative_keywords = icp.get("negative_keywords", [])
                    cfg.classifier_positive_keywords = icp.get("classifier_positive_keywords", [])
                    cfg.classifier_negative_keywords = icp.get("classifier_negative_keywords", [])
                    cfg.query_exclusion_terms = icp.get("query_exclusion_terms", [])
                    log.info(f"  ICP generated: {len(cfg.target_roles)} target roles")
            except Exception as e:
                log.debug(f"  ICP generation failed: {e}")
        return

    # No keywords — generate everything from client description
    if ANTHROPIC_API_KEY and cfg.client_description:
        try:
            from icp import generate_icp
            log.info(f"  Generating ICP from: \"{cfg.client_description}\"")
            icp = generate_icp(cfg.client_description)
            if icp:
                # Convert queries to keyword sets
                queries = icp.get("queries", [])
                cfg.keyword_sets = [[q] for q in queries]
                cfg.target_roles = icp.get("target_roles", [])
                cfg.positive_keywords = icp.get("positive_keywords", [])
                cfg.negative_keywords = icp.get("negative_keywords", [])
                cfg.classifier_positive_keywords = icp.get("classifier_positive_keywords", [])
                cfg.classifier_negative_keywords = icp.get("classifier_negative_keywords", [])
                cfg.query_exclusion_terms = icp.get("query_exclusion_terms", [])
                log.info(f"  ICP: {len(cfg.keyword_sets)} queries, "
                         f"{len(cfg.target_roles)} target roles")
        except Exception as e:
            log.error(f"  ICP generation failed: {e}")
            log.error(f"  Provide --keywords manually")
            return

    if not cfg.keyword_sets:
        # Last resort: use client description as a single keyword
        cfg.keyword_sets = [[cfg.client_description]]
        log.warning(f"  No ICP or keywords — using client description as search term")

    # Default target roles if not set
    if not cfg.target_roles:
        cfg.target_roles = [
            "Owner", "Founder", "Co-Founder", "CEO", "President",
            "Managing Partner", "Partner", "Principal",
            "Managing Director", "CTO", "COO",
            "VP Sales", "VP Business Development",
            "Director of Sales", "Director of Business Development",
            "Sales Manager", "Operations Manager",
        ]


# ==============================================================================
# MAIN PIPELINE
# ==============================================================================

def run_pipeline(cfg: V2Config):
    """Execute the full v2 pipeline."""
    start_time = time.time()

    # Resolve output directory
    cfg.output_dir = resolve_output_dir(cfg)
    os.makedirs(cfg.output_dir, exist_ok=True)

    log.info("=" * 65)
    log.info("  LEAD GENERATOR v2")
    log.info(f"  Client: {cfg.client_description}")
    log.info(f"  Target: {cfg.target} verified leads")
    log.info(f"  Output: {cfg.output_dir}")
    log.info("=" * 65)

    # API status
    apis = {
        "BlitzAPI":        bool(BLITZ_API_KEY),
        "MillionVerifier": bool(MILLIONVERIFIER_API_KEY),
        "BounceBan":       bool(BOUNCEBAN_API_KEY),
        "Serper":          bool(SERPER_API_KEY),
        "Anthropic":       bool(ANTHROPIC_API_KEY),
    }
    if cfg.aiark:
        apis["AI Ark"] = bool(AIARK_API_KEY)
    for name, ok in apis.items():
        log.info(f"  {name}: {'OK' if ok else 'MISSING'}")

    if not apis["BlitzAPI"]:
        log.error("  BLITZ_API_KEY required for v2 pipeline.")
        return

    # Load cache
    cache = load_cache()
    stats = cache_stats(cache)
    if stats:
        log.info(f"  Cache: {sum(stats.values()):,} entries ({', '.join(f'{k}={v}' for k,v in stats.items())})")

    # Generate ICP / resolve keywords
    generate_icp_if_needed(cfg)

    # ──────────────────────────────────────────────────────────────────────────
    # STEP 1: BLITZ DISCOVERY
    # ──────────────────────────────────────────────────────────────────────────
    if cfg.resume and is_step_complete(cfg.output_dir, "step_1_discovery"):
        companies = load_step(cfg.output_dir, "step_1_discovery")
        log.info(f"\n  [1/10] DISCOVERY — resumed ({len(companies)} companies)")
    else:
        log.info(f"\n  [1/10] BLITZ DISCOVERY")
        companies = blitz_discover(cfg, cache, V2_CACHE_FILE)

        # Step 1b: AI Ark Lookalike Discovery (if --aiark)
        if cfg.aiark and AIARK_API_KEY:
            from v2_aiark import lookalike_discover, check_credits as aiark_credits
            credits = aiark_credits(AIARK_API_KEY)
            log.info(f"\n  [1b/10] AI ARK LOOKALIKE DISCOVERY (credits: {credits:.0f})")

            # Use top Blitz domains as seeds, or user-provided seeds
            seed_domains = cfg.aiark_seeds
            if not seed_domains and companies:
                seed_domains = [c["domain"] for c in companies[:30] if c.get("domain")]

            if seed_domains:
                aiark_companies = lookalike_discover(
                    seed_domains, AIARK_API_KEY,
                    max_pages=20, max_companies=500,
                    cache=cache, cache_file=V2_CACHE_FILE,
                )
                # Merge — deduplicate by domain
                existing_domains = {c.get("domain", "").lower() for c in companies}
                new_from_aiark = [
                    c for c in aiark_companies
                    if c.get("domain", "").lower() not in existing_domains
                ]
                companies.extend(new_from_aiark)
                log.info(f"  AI Ark added {len(new_from_aiark)} new companies "
                         f"(Blitz: {len(existing_domains)}, combined: {len(companies)})")
            else:
                log.info(f"  AI Ark: no seed domains available")
        elif cfg.aiark and not AIARK_API_KEY:
            log.warning("  --aiark requested but AIARK_API_KEY not set in .env")

        # Step 0: Pre-filter
        companies = prefilter_companies(companies, cfg)
        save_step(cfg.output_dir, "step_1_discovery", companies)

    if not companies:
        log.error("  No companies found. Try different keywords.")
        return

    if cfg.dry_run:
        log.info(f"\n  [DRY RUN] {len(companies)} companies discovered. Stopping.")
        from v2_export import _save_csv
        from v2_config import COMPANY_FIELDS
        _save_csv(companies, os.path.join(cfg.output_dir, "companies_dryrun.csv"), COMPANY_FIELDS)
        return

    # ──────────────────────────────────────────────────────────────────────────
    # STEP 2: BLITZ ENRICHMENT
    # ──────────────────────────────────────────────────────────────────────────
    if cfg.resume and is_step_complete(cfg.output_dir, "step_2_enrichment"):
        contacts = load_step(cfg.output_dir, "step_2_enrichment")
        log.info(f"\n  [2/10] ENRICHMENT — resumed ({len(contacts)} contacts)")
    else:
        log.info(f"\n  [2/10] BLITZ ENRICHMENT")
        contacts = blitz_enrich(companies, cfg, cache, V2_CACHE_FILE)
        save_step(cfg.output_dir, "step_2_enrichment", contacts)

    if not contacts:
        log.error("  No contacts enriched.")
        return

    # ──────────────────────────────────────────────────────────────────────────
    # STEP 3: TARGET CHECK
    # ──────────────────────────────────────────────────────────────────────────
    log.info(f"\n  [3/10] TARGET CHECK")
    target_met, estimated, deficit = check_target(contacts, cfg.target)

    # ──────────────────────────────────────────────────────────────────────────
    # STEP 4: BACKFILL (if needed)
    # ──────────────────────────────────────────────────────────────────────────
    if not target_met:
        if cfg.resume and is_step_complete(cfg.output_dir, "step_4_backfill"):
            contacts = load_step(cfg.output_dir, "step_4_backfill")
            log.info(f"\n  [4/10] BACKFILL — resumed ({len(contacts)} contacts)")
        else:
            log.info(f"\n  [4/10] BACKFILL")
            contacts = backfill(companies, contacts, deficit, cfg, cache, V2_CACHE_FILE)
            save_step(cfg.output_dir, "step_4_backfill", contacts)
    else:
        log.info(f"\n  [4/10] BACKFILL — skipped (target met)")

    # ──────────────────────────────────────────────────────────────────────────
    # STEP 4.5: PRE-CLEAN
    # ──────────────────────────────────────────────────────────────────────────
    log.info(f"\n  [4.5/10] PRE-CLEAN")
    contacts, excluded_preclean = pre_clean(contacts, cfg)
    save_step(cfg.output_dir, "step_4_5_preclean", contacts)

    # ──────────────────────────────────────────────────────────────────────────
    # STEP 5: VERIFY
    # ──────────────────────────────────────────────────────────────────────────
    if cfg.resume and is_step_complete(cfg.output_dir, "step_5_verify"):
        contacts = load_step(cfg.output_dir, "step_5_verify")
        log.info(f"\n  [5/10] VERIFY — resumed ({len(contacts)} contacts)")
    else:
        log.info(f"\n  [5/10] VERIFY (MV + BounceBan)")
        if MILLIONVERIFIER_API_KEY:
            contacts = verify_all(contacts, cfg, cache, V2_CACHE_FILE)
        else:
            log.warning("  MILLIONVERIFIER_API_KEY not set — skipping verification")
        save_step(cfg.output_dir, "step_5_verify", contacts)

    # ──────────────────────────────────────────────────────────────────────────
    # STEP 6: DELIVERABILITY SCORING
    # ──────────────────────────────────────────────────────────────────────────
    log.info(f"\n  [6/10] DELIVERABILITY SCORING")
    contacts = score_tiers(contacts)
    save_step(cfg.output_dir, "step_6_scored", contacts)

    # ──────────────────────────────────────────────────────────────────────────
    # STEP 7: POST-CLEAN
    # ──────────────────────────────────────────────────────────────────────────
    log.info(f"\n  [7/10] POST-CLEAN")
    contacts, excluded_postclean = post_clean(contacts, cfg)
    save_step(cfg.output_dir, "step_7_postclean", contacts)

    # ──────────────────────────────────────────────────────────────────────────
    # STEP 8: DEDUPLICATE
    # ──────────────────────────────────────────────────────────────────────────
    log.info(f"\n  [8/10] DEDUPLICATE")
    contacts = deduplicate(contacts, skip_global=cfg.skip_dedup)

    # ──────────────────────────────────────────────────────────────────────────
    # STEP 10: EXPORT
    # ──────────────────────────────────────────────────────────────────────────
    log.info(f"\n  [10/10] EXPORT")
    all_excluded = (excluded_preclean or []) + (excluded_postclean or [])
    summary = export_all(contacts, companies, all_excluded, cfg.output_dir, cfg)

    elapsed = time.time() - start_time
    log.info(f"\n  Pipeline completed in {elapsed/60:.1f} minutes")

    return summary


# ==============================================================================
# CLI
# ==============================================================================

def parse_args():
    parser = argparse.ArgumentParser(
        description="Lead Generator v2 — Unified Target-Driven Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python3 lead_generator_v2.py --client "cost segregation firms" --target 500
  python3 lead_generator_v2.py --client "MSPs" --target 500 --keywords "managed service provider;MSP"
  python3 lead_generator_v2.py --client "hotels in Colorado" --target 300 --geo colorado
  python3 lead_generator_v2.py --resume --name costseg-v2
        """,
    )
    parser.add_argument("--client", type=str, default="",
                        help="Client/industry description for ICP generation")
    parser.add_argument("--target", type=int, default=500,
                        help="Minimum Tier 1 verified leads (default: 500)")
    parser.add_argument("--name", type=str, default="",
                        help="Project name for output folder")
    parser.add_argument("--keywords", type=str, default="",
                        help="Semicolon-separated Blitz keyword sets")
    parser.add_argument("--max-contacts", type=int, default=2,
                        help="Max contacts per company (default: 2)")
    parser.add_argument("--max-companies", type=int, default=2000,
                        help="Cap on Blitz discovery (default: 2000)")
    parser.add_argument("--cities", type=str, default="us_top_30",
                        help="Cities for Serper backfill (semicolon-separated or preset)")
    parser.add_argument("--geo", type=str, default="",
                        help="Filter to geography (e.g., 'colorado', 'US')")
    parser.add_argument("--exclude-industries", type=str, default="",
                        help="Semicolon-separated industries to exclude")
    parser.add_argument("--smb-only", action="store_true",
                        help="Remove enterprises (>500 employees)")
    parser.add_argument("--no-backfill", action="store_true",
                        help="Skip Serper geo-grid backfill")
    parser.add_argument("--no-classify", action="store_true",
                        help="Skip AI company classification")
    parser.add_argument("--threshold", type=int, default=60,
                        help="Classification confidence threshold (default: 60)")
    parser.add_argument("--double-verify", action="store_true", default=True,
                        help="Use BounceBan after MillionVerifier (default: True)")
    parser.add_argument("--no-double-verify", action="store_true",
                        help="Skip BounceBan double-verification")
    parser.add_argument("--skip-dedup", action="store_true",
                        help="Skip global email dedup")
    parser.add_argument("--dry-run", action="store_true",
                        help="Discover companies only, estimate costs")
    parser.add_argument("--resume", action="store_true",
                        help="Resume from last checkpoint")
    parser.add_argument("--output-dir", type=str, default="",
                        help="Override output directory")
    parser.add_argument("--blitz-workers", type=int, default=40,
                        help="Parallel Blitz workers (default: 40)")
    parser.add_argument("--check-credits", action="store_true",
                        help="Show API credit balances and exit")
    parser.add_argument("--no-master", action="store_true",
                        help="Disable master DB pre-run dedup (re-enrich known companies)")
    parser.add_argument("--aiark", action="store_true",
                        help="Use AI Ark for lookalike discovery + email backfill")
    parser.add_argument("--seeds", type=str, default="",
                        help="Seed domains for AI Ark lookalike (semicolon-separated)")
    parser.add_argument("--aiark-budget", type=int, default=500,
                        help="Max AI Ark credits per run (default: 500)")

    args = parser.parse_args()

    if args.check_credits:
        from verification import print_credit_status
        print_credit_status()
        if AIARK_API_KEY:
            from v2_aiark import check_credits as aiark_credits
            credits = aiark_credits(AIARK_API_KEY)
            print(f"\n  AI Ark: {credits:.0f} credits remaining")
        sys.exit(0)

    if not args.client and not args.resume:
        parser.error("--client is required (or use --resume)")

    # Build config
    cfg = V2Config()
    cfg.client_description = args.client
    cfg.client_name = args.name or slugify(args.client)
    cfg.target = args.target
    cfg.max_contacts_per_company = args.max_contacts
    cfg.max_companies = args.max_companies
    cfg.smb_only = args.smb_only
    cfg.classify = not args.no_classify
    cfg.threshold = args.threshold
    cfg.double_verify = args.double_verify and not args.no_double_verify
    cfg.backfill = not args.no_backfill
    cfg.blitz_workers = args.blitz_workers
    cfg.dry_run = args.dry_run
    cfg.resume = args.resume
    cfg.skip_dedup = args.skip_dedup
    cfg.output_dir = args.output_dir
    cfg.aiark = args.aiark
    cfg.aiark_budget = args.aiark_budget
    cfg.use_master = not args.no_master

    # Parse AI Ark seeds
    if args.seeds:
        cfg.aiark_seeds = [s.strip() for s in args.seeds.split(";") if s.strip()]

    # Parse keywords
    if args.keywords:
        cfg.keyword_sets = [[k.strip()] for k in args.keywords.split(";") if k.strip()]

    # Parse cities
    if args.cities:
        cfg.cities = [c.strip() for c in args.cities.split(";") if c.strip()]

    # Parse geo
    cfg.target_geo = args.geo.strip() if args.geo else ""

    # Parse exclude industries
    if args.exclude_industries:
        cfg.exclude_industries = [i.strip() for i in args.exclude_industries.split(";") if i.strip()]

    return cfg


# ==============================================================================
# MAIN
# ==============================================================================

def main():
    cfg = parse_args()
    run_pipeline(cfg)


if __name__ == "__main__":
    main()
