#!/usr/bin/env python3
"""
ClientC Lead Pipeline v6
Full automated B2B lead generation with AI-powered company classification.

Pipeline:
  Client request → Claude ICP (with classifier keywords + query exclusions)
  → Improved query generation → Geo-grid Serper scraping
  → Domain extraction → Filtering → AI Company Classification
    (homepage + /about + /services scrape → keyword scoring → Haiku fallback → confidence score)
  → Tiered contact enrichment (only on classified companies)
  → Post-enrichment title validation
  → Email pattern propagation → Verification → CSV export

Usage:
  export SERPER_API_KEY="..."
  export ANTHROPIC_API_KEY="..."
  export HUNTER_API_KEY="..."           # Optional (fallback enrichment)
  export MILLIONVERIFIER_API_KEY="..."  # Optional (use existing account)

  python3 lead_pipeline_v6.py --client "fire protection company targeting hotels in Colorado"
  python3 lead_pipeline_v6.py --client "MSP targeting IT companies in Florida" --name client_b
  python3 lead_pipeline_v6.py --client "..." --dry-run
  python3 lead_pipeline_v6.py --client "..." --threshold 70
  python3 lead_pipeline_v6.py --client "..." --no-classify  # Skip classification (v5 behavior)
"""

import argparse
import os
import sys
import re

import config
import threading
from config import (
    SERPER_API_KEY, ANTHROPIC_API_KEY, MILLIONVERIFIER_API_KEY, BLITZ_API_KEY,
    CACHE_FILE, CACHE_VERSION, CLASSIFICATION_CACHE_FILE,
    WORKSPACE_ROOT, PROJECTS_DIR,
    CITY_COORDS, CITY_PRESETS,
    CLASSIFIER_CONFIDENCE_THRESHOLD, ENRICHMENT_WORKERS,
    PipelineContext,
)
from cache import load_pipeline_cache, load_classification_cache
from utils import slugify, geocode_city
from icp import generate_icp, expand_queries
from scraping import (
    run_serper_scrape, run_serp_discovery,
    filter_and_deduplicate,
)
from classification import (
    classify_companies, export_classification_log,
    enrich_firmographics, validate_contacts_post_enrichment,
)
from enrichment import (
    enrich_company, propagate_emails,
    classify_email, generate_email,
)
from verification import (
    verify_contacts, flag_catch_all_domains,
    print_credit_status,
)
from export import (
    export_contacts, export_smartlead,
    export_companies, export_domains,
    score_contact, is_bad_title,
    dedup_against_global, save_global_dedup,
    log_run,
)
from cache import cache_key, save_cache
from checkpoint import save_checkpoint, load_checkpoint, clear_checkpoint
from signals import detect_signals, generate_signal_keywords
from title_enrichment import enrich_titles
from name_discovery import discover_names


# ============================================================
# LOCATION RESOLVER
# ============================================================

def resolve_cities(location_str, cli_cities=None):
    """Resolve location to list of cities."""
    if cli_cities:
        return cli_cities

    if not location_str:
        return CITY_PRESETS["us_top_30"]

    loc = location_str.lower().strip()

    for preset_name, preset_cities in CITY_PRESETS.items():
        if preset_name.replace("_", " ") in loc:
            return preset_cities

    state_map = {
        "colorado": "colorado", "georgia": "atlanta_metro",
        "atlanta": "atlanta_metro", "florida": "florida",
    }
    for state, preset in state_map.items():
        if state in loc:
            return CITY_PRESETS[preset]

    if any(kw in loc for kw in ["us", "usa", "national", "nationwide"]):
        return CITY_PRESETS["us_top_30"]

    return CITY_PRESETS["us_top_30"]


# ============================================================
# CLIENT FOLDER MANAGEMENT
# ============================================================

def setup_client_folder(client_name):
    """Create a client folder under 01-Projects/ and return the path."""
    slug = slugify(client_name)
    if not slug:
        slug = "unnamed-client"

    client_dir = os.path.join(PROJECTS_DIR, slug)
    runs_dir = os.path.join(client_dir, "lead-runs")
    os.makedirs(runs_dir, exist_ok=True)

    client_md = os.path.join(client_dir, "CLIENT.md")
    if not os.path.exists(client_md):
        from datetime import datetime
        with open(client_md, "w", encoding="utf-8") as f:
            f.write(f"# {client_name}\n\n")
            f.write(f"## Lead Pipeline\n\n")
            f.write(f"Lead generation runs are stored in `lead-runs/`.\n\n")
            f.write(f"## Notes\n\n")
            f.write(f"- Created: {datetime.now().strftime('%Y-%m-%d')}\n")

    config.OUTPUT_DIR = runs_dir
    return runs_dir


# ============================================================
# MAIN PIPELINE
# ============================================================

def run_pipeline(client_request, cities=None, output_prefix="",
                 dry_run=False, strict_filter=False, no_chains=False, no_emails=False,
                 auto_confirm=False, no_grid=False, no_expand=False, queries=None,
                 client_name=None, discover=False, include_linkedin=False,
                 employee_range=None, grid_mode="full", confirm_icp=False,
                 skip_dedup=False, no_classify=False, threshold=None,
                 enable_signals=False, signals_serp=False,
                 enrich_titles_flag=False, no_apify=False):
    """Full pipeline: ICP → search → filter → classify → enrich → verify → export."""
    from concurrent.futures import ThreadPoolExecutor, as_completed
    from datetime import datetime

    # ---- Setup client output folder ----
    if client_name:
        output_dir = setup_client_folder(client_name)
        print(f"\n  Output folder: {os.path.relpath(output_dir, WORKSPACE_ROOT)}")

    print(f"\n{'='*60}")
    print(f"  ClientC Lead Pipeline v6")
    print(f"{'='*60}")
    print(f"  Client: {client_request}")
    if client_name:
        print(f"  Client folder: 01-Projects/{slugify(client_name)}/lead-runs/")
    print(f"  Dry run: {dry_run}")
    print(f"  Strict filter: {strict_filter}")
    print(f"  Chain filter: {no_chains}")
    print(f"  Email enrichment: {'OFF' if no_emails else 'ON'}")
    print(f"  AI Classification: {'OFF' if no_classify else 'ON'}")
    if not no_classify:
        print(f"  Confidence threshold: {threshold or CLASSIFIER_CONFIDENCE_THRESHOLD}")
    if no_grid:
        print(f"  Geo-grid: OFF (city center only)")
    if no_expand:
        print(f"  Query expansion: OFF")
    if discover:
        print(f"  Discovery mode: SERP web search (not Maps)")
        if include_linkedin:
            print(f"  LinkedIn discovery: ON")
            if employee_range:
                print(f"  Employee filter: {employee_range}")

    # ---- Step 1: Generate ICP ----
    if queries:
        print(f"\n  [1/8] Using custom queries (skipping ICP generation)")
        icp = {
            "queries": queries,
            "target_roles": ["Owner", "CEO", "Founder", "President", "CTO",
                             "VP of Sales", "General Manager", "Managing Partner",
                             "COO", "Principal"],
            "positive_keywords": [],
            "negative_keywords": [],
            "location": "",
        }
    else:
        print(f"\n  [1/8] Generating ICP via Claude...")
        icp = generate_icp(client_request)

    # Initialize pipeline context
    ctx = PipelineContext()
    ctx.positive_keywords = [k.lower() for k in icp.get("positive_keywords", [])]
    ctx.negative_keywords = [k.lower() for k in icp.get("negative_keywords", [])]
    ctx.required_keywords = [k.lower() for k in icp.get("required_keywords", [])]
    ctx.excluded_titles = [k.lower() for k in icp.get("excluded_titles", [])]
    ctx.buyer_keywords = [k.lower() for k in icp.get("buyer_keywords", [])]
    ctx.strict_icp = strict_filter
    ctx.classifier_positive_keywords = [k.lower() for k in icp.get("classifier_positive_keywords", [])]
    ctx.classifier_negative_keywords = [k.lower() for k in icp.get("classifier_negative_keywords", [])]
    ctx.query_exclusion_terms = [k.lower() for k in icp.get("query_exclusion_terms", [])]
    ctx.valid_maps_categories = [k.lower() for k in icp.get("valid_maps_categories", [])]
    ctx.client_summary = icp.get("client_summary", client_request)
    ctx.target_description = icp.get("target_description", "")
    if threshold:
        ctx.classification_threshold = threshold

    # Print ICP
    print(f"\n  ICP Generated:")
    print(f"    Target roles: {', '.join(icp.get('target_roles', [])[:5])}")
    print(f"    Location: {icp.get('location', 'nationwide')}")
    if not no_classify:
        print(f"    Classifier keywords: {len(icp.get('classifier_positive_keywords', []))} positive, "
              f"{len(icp.get('classifier_negative_keywords', []))} negative")
    print(f"    Queries ({len(icp['queries'])}):")
    for q in icp["queries"]:
        print(f"      - {q}")

    # ---- ICP Confirmation ----
    if confirm_icp:
        print(f"\n  Review the ICP above. Continue? (y/n): ", end="", flush=True)
        user_input = input().strip().lower()
        if user_input != "y":
            print("  Cancelled.")
            return

    # ---- Credit check ----
    if not dry_run:
        print_credit_status()

    # ---- Step 2: Expand queries ----
    if no_expand:
        print(f"\n  [2/8] Skipping query expansion (--no-expand)")
        expanded = icp["queries"]
    else:
        print(f"\n  [2/8] Expanding queries...")
        exclusions = ctx.query_exclusion_terms if not no_classify else None
        expanded = expand_queries(icp["queries"], exclusion_terms=exclusions)
    print(f"    Original: {len(icp['queries'])} → Using: {len(expanded)}")
    if ctx.query_exclusion_terms and not no_classify and not no_expand:
        print(f"    Exclusion terms applied: {', '.join(ctx.query_exclusion_terms[:5])}")

    # ---- Step 3: Resolve cities ----
    resolved_cities = resolve_cities(icp.get("location", ""), cities)

    # Validate city coordinates
    valid_cities = []
    for city in resolved_cities:
        if city in CITY_COORDS:
            valid_cities.append(city)
        else:
            coords = geocode_city(city, cache=None, cache_key_func=cache_key, save_cache_func=None)
            if coords:
                CITY_COORDS[city] = coords
                valid_cities.append(city)
            else:
                print(f"    Warning: Could not geocode '{city}'. Skipping.")

    if not valid_cities:
        print("  ERROR: No valid cities found. Cannot proceed.")
        return

    print(f"\n  Cities ({len(valid_cities)}): {', '.join(valid_cities[:5])}")
    if len(valid_cities) > 5:
        print(f"    ... and {len(valid_cities) - 5} more")

    # ---- Step 4: Company discovery ----
    cache = load_pipeline_cache(CACHE_FILE, CACHE_VERSION)

    if discover:
        print(f"\n  [3/8] Running SERP web + LinkedIn company discovery...")
        location_strs = [c.split(",")[1].strip() if "," in c else c for c in valid_cities[:5]]
        location_strs = list(set(location_strs))
        if not location_strs:
            location_strs = [icp.get("location", "")]

        raw_results = run_serp_discovery(
            expanded, location_strs, cache, CACHE_FILE,
            include_linkedin=include_linkedin,
            employee_range=employee_range,
            dry_run=dry_run,
        )

        if dry_run:
            return

        companies = raw_results
        for c in companies:
            if not c.get("domain"):
                from utils import get_domain
                c["domain"] = get_domain(c.get("website", ""))
        companies = [c for c in companies if c.get("domain")]

    else:
        print(f"\n  [3/8] Scraping Google Maps via Serper...")
        raw_results = run_serper_scrape(
            expanded, valid_cities, cache, CACHE_FILE,
            dry_run=dry_run, auto_confirm=auto_confirm,
            no_grid=no_grid, grid_mode=grid_mode,
        )

        if dry_run:
            # Estimate API usage
            est_companies = len(set(r.get("website", "") for r in raw_results if r.get("website"))) if raw_results else len(expanded) * len(valid_cities) * 5
            est_enrichment = int(est_companies * 0.6) if not no_emails else 0
            est_mv_calls = int(est_companies * 0.5) if MILLIONVERIFIER_API_KEY else 0

            print(f"\n  API Usage Estimates:")
            print(f"    Estimated unique companies: ~{est_companies}")
            if not no_emails:
                print(f"    Estimated enrichment API calls: ~{est_enrichment}")
            if MILLIONVERIFIER_API_KEY:
                print(f"    Estimated MillionVerifier calls: ~{est_mv_calls}")
            else:
                print(f"    MillionVerifier: SKIPPED (no API key)")
            return

        print(f"\n  [3b/8] Filtering and deduplicating...")
        companies = filter_and_deduplicate(
            raw_results, strict_filter=strict_filter, no_chains=no_chains,
        )

    scraped_company_count = len(companies)
    print(f"    Unique companies after dedup: {scraped_company_count}")

    # ---- Step 4b: ICP-based filtering ----
    if ctx.positive_keywords or ctx.negative_keywords:
        before = len(companies)
        companies = [c for c in companies if ctx.is_target_company(c)]
        filtered_out = before - len(companies)
        if filtered_out:
            print(f"    ICP filter removed: {filtered_out} companies")
    else:
        filtered_out = 0

    # ---- Step 5: AI Classification ----
    classification_log = []
    classified_rejected = []
    if not no_classify and companies:
        print(f"\n  [4/8] AI Company Classification...")
        cls_cache = load_classification_cache(CLASSIFICATION_CACHE_FILE)
        companies, classified_rejected, classification_log = classify_companies(
            companies, icp,
            threshold=threshold or ctx.classification_threshold,
            classification_cache=cls_cache,
            classification_cache_file=CLASSIFICATION_CACHE_FILE,
        )
        print(f"    Passed: {len(companies)}, Rejected: {len(classified_rejected)}")

        if classification_log:
            prefix = output_prefix + "_" if output_prefix else ""
            export_classification_log(classification_log, f"{prefix}classification_log.csv")
    else:
        print(f"\n  [4/8] Skipping classification")

    if not companies:
        print("\n  No companies passed filtering. Pipeline complete (no output).")
        return

    # ---- Step 4d: Signal Detection ----
    if enable_signals and not dry_run:
        print(f"\n  [4d/8] Detecting buying signals...")
        companies, signal_kws = detect_signals(
            companies, icp,
            use_serp=signals_serp,
            cache=cache, cache_file=CACHE_FILE,
        )
        with_signals = sum(1 for c in companies if c.get("signal_score", 0) > 0)
        tier1 = sum(1 for c in companies if c.get("signal_score", 0) >= 3)
        print(f"    Companies with signals: {with_signals}/{len(companies)}")
        print(f"    Tier 1 (urgent intent): {tier1}")

    # ---- Checkpoint: save classified companies ----
    save_checkpoint("classification", {
        "companies": companies,
        "classification_log": classification_log,
        "classified_rejected": classified_rejected,
        "scraped_company_count": scraped_company_count,
        "filtered_out": filtered_out,
    })

    # ---- Step 5b: Export companies + domains ----
    prefix = output_prefix + "_" if output_prefix else ""
    export_companies(companies, f"{prefix}companies.csv")
    export_domains(companies, f"{prefix}domains.csv")

    if no_emails:
        print(f"\n  Email enrichment skipped (--no-emails).")
        return

    # ---- Step 5c: Firmographic enrichment ----
    print(f"\n  [4c/8] Enriching company firmographics (tech stack, description, socials)...")
    firmo_count = 0
    for company in companies:
        firmo = enrich_firmographics(company, cache, CACHE_FILE)
        if firmo:
            company["tech_stack"] = ", ".join(firmo.get("tech_stack", []))
            company["description"] = firmo.get("description", "")
            for key in ("linkedin_url", "twitter_url", "facebook_url"):
                if firmo.get(key):
                    company[key] = firmo[key]
            if firmo.get("tech_stack"):
                firmo_count += 1
    print(f"    Firmographics: {firmo_count}/{len(companies)} companies with tech stack detected")

    # ---- Step 4e: Blitz Enrichment (if enabled) ----
    target_roles = icp.get("target_roles", [])
    blitz_contacts = []
    blitz_domains_found = set()
    use_blitz = getattr(args, "blitz", False) or (BLITZ_API_KEY and not getattr(args, "no_blitz", False))

    if use_blitz and BLITZ_API_KEY:
        from blitz import blitz_enrich_batch, is_blitz_available
        if is_blitz_available():
            print(f"\n  [4e/8] BlitzAPI enrichment ({len(companies)} companies)...")
            blitz_companies = [{"name": c.get("name", ""), "domain": c.get("domain", "")} for c in companies]

            def _blitz_progress(done, total, found):
                print(f"    [{done}/{total}] Blitz -- {found} contacts found")

            blitz_contacts = blitz_enrich_batch(
                blitz_companies, target_roles,
                max_workers=10, progress_callback=_blitz_progress,
            )
            blitz_domains_found = set(c.get("domain", "") for c in blitz_contacts if c.get("email"))
            print(f"    Blitz found {len(blitz_contacts)} contacts ({len(blitz_domains_found)} domains with emails)")
        else:
            print(f"\n  [4e/8] BlitzAPI not available, skipping...")

    # Filter companies that Blitz already fully enriched (have emails)
    companies_for_waterfall = companies
    if blitz_domains_found:
        companies_for_waterfall = [c for c in companies if c.get("domain", "") not in blitz_domains_found]
        print(f"    Skipping {len(companies) - len(companies_for_waterfall)} companies already enriched by Blitz")

    # ---- Step 6: Contact Enrichment (Parallel) — waterfall for remaining ----
    print(f"\n  [5/8] Enriching contacts ({ENRICHMENT_WORKERS} workers)...")
    all_contacts = list(blitz_contacts)  # Start with Blitz contacts
    stats = {"website": 0, "serp": 0, "icypeas": 0, "hunter": 0, "staff": 0, "blitz": len(blitz_contacts), "none": 0}
    enriched = 0
    _contacts_lock = threading.Lock()
    _stats_lock = threading.Lock()
    _completed = [0]  # mutable counter for closure

    def _enrich_one(company):
        return enrich_company(company, target_roles,
                              cache, CACHE_FILE, ctx, dry_run=dry_run)

    with ThreadPoolExecutor(max_workers=ENRICHMENT_WORKERS) as executor:
        futures = {executor.submit(_enrich_one, c): c for c in companies_for_waterfall}

        for future in as_completed(futures):
            try:
                contacts = future.result()
            except Exception:
                contacts = None

            with _contacts_lock:
                _completed[0] += 1
                if contacts:
                    all_contacts.extend(contacts)
                    enriched += 1
                    sources = set(c.get("source", "") for c in contacts)
                    with _stats_lock:
                        if "website_scrape" in sources:
                            stats["website"] += 1
                        if any("serp" in s for s in sources):
                            stats["serp"] += 1
                        if "icypeas" in sources:
                            stats["icypeas"] += 1
                        if "hunter" in sources:
                            stats["hunter"] += 1
                        if "staff_scrape" in sources:
                            stats["staff"] += 1
                else:
                    with _stats_lock:
                        stats["none"] += 1

                done = _completed[0]
                if done % 25 == 0:
                    print(f"    [{done}/{len(companies_for_waterfall)}] Enriched -- {len(all_contacts)} contacts found")
                    # Incremental checkpoint every 25 companies
                    save_checkpoint("enrichment_partial", {
                        "contacts": all_contacts[:],
                        "progress": done,
                        "total": len(companies),
                    })

    # Save full enrichment checkpoint
    save_checkpoint("enrichment", {"contacts": all_contacts})
    print(f"    Enrichment complete: {len(all_contacts)} contacts from {enriched} companies")

    # ---- Propagate signal data from companies to contacts ----
    if enable_signals:
        company_signals = {c["domain"]: c for c in companies if c.get("signal_score", 0) > 0}
        for contact in all_contacts:
            domain = contact.get("domain", "")
            if domain in company_signals:
                src = company_signals[domain]
                contact["signal_score"] = src.get("signal_score", 0)
                contact["top_signal"] = src.get("top_signal", "")
                contact["top_signal_detail"] = src.get("top_signal_detail", "")
                contact["signal_summary"] = src.get("signal_summary", "")

    # ---- Step 5a: Name Discovery (for contacts with generic emails only) ----
    if enrich_titles_flag and not dry_run:
        print(f"\n  [5a/8] Discovering owner names (Yelp + SOS + SERP)...")
        all_contacts, name_stats = discover_names(
            all_contacts, cache=cache, cache_file=CACHE_FILE,
        )
        print(f"    Names discovered: {name_stats['total']} "
              f"(Yelp: {name_stats['yelp']}, SOS: {name_stats['sos']}, SERP: {name_stats['serp']})")

        # ---- Step 5a2: Title Enrichment (for contacts with names but no titles) ----
        print(f"\n  [5a2/8] Enriching missing titles (SERP{' + Apify' if not no_apify else ''})...")
        all_contacts, title_stats = enrich_titles(
            all_contacts, cache=cache, cache_file=CACHE_FILE,
            use_apify=not no_apify,
        )
        print(f"    Titles filled: {title_stats['total_filled']} "
              f"(SERP: {title_stats['serp']}, Apify: {title_stats['apify']})")

    # ---- Step 5c: Scoring + dedup + title filtering ----
    print(f"\n  [5b/8] Scoring and filtering contacts...")
    pre_score_count = len(all_contacts)
    bad_title_count = 0

    for c in all_contacts:
        s, p = score_contact(c.get("title", ""))
        c["score"] = s
        c["priority"] = p

    # Remove bad titles
    filtered_contacts = []
    for c in all_contacts:
        title = (c.get("title", "") or "").lower()
        if title and is_bad_title(title, ctx):
            bad_title_count += 1
            continue
        filtered_contacts.append(c)
    all_contacts = filtered_contacts

    # Limit contacts per domain
    by_domain = {}
    for c in all_contacts:
        d = c.get("domain", "")
        if d not in by_domain:
            by_domain[d] = []
        by_domain[d].append(c)

    limited_contacts = []
    for domain, contacts_list in by_domain.items():
        contacts_list.sort(key=lambda x: x.get("score", 0), reverse=True)
        picked = []
        picked_priorities = set()
        for c in contacts_list:
            p = c.get("priority", "low")
            if p in ("owner", "executive") and "owner_exec" not in picked_priorities:
                picked.append(c)
                picked_priorities.add("owner_exec")
            elif p == "buyer" and "buyer" not in picked_priorities:
                picked.append(c)
                picked_priorities.add("buyer")
            elif p == "generic" and "generic" not in picked_priorities:
                picked.append(c)
                picked_priorities.add("generic")
            elif len(picked) < 3 and p not in picked_priorities:
                picked.append(c)
                picked_priorities.add(p)
            if len(picked) >= 3:
                break
        if len(picked) < 3:
            for c in contacts_list:
                if c not in picked:
                    picked.append(c)
                if len(picked) >= 3:
                    break
        limited_contacts.extend(picked)

    all_contacts = limited_contacts
    print(f"    Bad titles filtered: {bad_title_count}")
    print(f"    Contacts after scoring + limit: {len(all_contacts)} (from {pre_score_count})")

    # ---- Step 5d: Post-enrichment validation ----
    if not no_classify:
        print(f"\n  [5c/8] Post-enrichment title validation...")
        all_contacts = validate_contacts_post_enrichment(all_contacts, icp, ctx)

    # ---- Step 6: Pattern inference + propagation ----
    print(f"\n  [6/8] Inferring email patterns and propagating...")

    by_domain = {}
    for c in all_contacts:
        d = c.get("domain", "")
        if d not in by_domain:
            by_domain[d] = []
        by_domain[d].append(c)

    propagated = 0
    domains_done = 0
    total_domains = len(by_domain)
    from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeout
    for domain, contacts in by_domain.items():
        before = sum(1 for c in contacts if c.get("email"))
        # Wrap propagation in a timeout to prevent hangs
        try:
            with ThreadPoolExecutor(max_workers=1) as timeout_exec:
                future = timeout_exec.submit(propagate_emails, contacts, domain, ctx)
                future.result(timeout=60)  # 60s max per domain
        except (FuturesTimeout, Exception):
            pass  # Skip hung domains
        after = sum(1 for c in contacts if c.get("email"))
        propagated += (after - before)
        domains_done += 1
        if domains_done % 50 == 0:
            print(f"    [{domains_done}/{total_domains}] Propagated -- {propagated} emails generated so far")

    print(f"    Emails generated via propagation: {propagated}")

    all_contacts = [c for contacts in by_domain.values() for c in contacts]

    # ---- Step 7a: Catch-all domain detection (BEFORE verification) ----
    if MILLIONVERIFIER_API_KEY and not dry_run:
        print(f"\n  [7a/8] Detecting catch-all domains (before verification)...")
        all_contacts, catch_all_map = flag_catch_all_domains(all_contacts, cache=cache, cache_file=CACHE_FILE)
        catch_all_count = sum(1 for c in all_contacts if c.get("catch_all"))
        catch_all_emails = sum(1 for c in all_contacts if c.get("catch_all") and c.get("email"))
        if catch_all_count:
            print(f"    {catch_all_count} contacts on catch-all domains (flagged)")
            print(f"    {catch_all_emails} emails will skip verification (saving MV credits)")

    # ---- Step 7b: Verification (skip catch-all domains) ----
    print(f"\n  [7b/8] Verifying emails...")
    non_catch_all = [c for c in all_contacts if not c.get("catch_all")]
    catch_all_contacts = [c for c in all_contacts if c.get("catch_all")]
    for c in catch_all_contacts:
        if c.get("email"):
            c["verified"] = True
    double_verify = getattr(args, "double_verify", False)
    non_catch_all = verify_contacts(non_catch_all, dry_run=dry_run, double_verify=double_verify)
    all_contacts = non_catch_all + catch_all_contacts

    # ---- Step 7c: Global deduplication ----
    if not skip_dedup:
        all_contacts, dedup_removed = dedup_against_global(all_contacts)
        if dedup_removed:
            print(f"\n  Global dedup: removed {dedup_removed} emails already exported in previous runs")

    # ---- Step 8: Export contacts ----
    print(f"\n  [8/8] Exporting final contacts...")
    final = export_contacts(all_contacts, f"{prefix}contacts_final.csv", ctx=ctx)
    smartlead_rows = export_smartlead(all_contacts, f"{prefix}smartlead_import.csv", ctx=ctx)

    # ---- Save to global dedup tracking ----
    if not skip_dedup:
        save_global_dedup(final, client_name=client_name or "")

    # ---- Final Summary ----
    with_email = sum(1 for c in final if c.get("email"))
    with_name = sum(1 for c in final if c.get("name", "").strip())
    with_phone = sum(1 for c in final if c.get("phone", "").strip())
    personal_count = sum(1 for c in final if c.get("type") == "personal")
    generic_count = sum(1 for c in final if c.get("type") == "generic")
    owner_count = sum(1 for c in final if c.get("priority") == "owner")
    exec_count = sum(1 for c in final if c.get("priority") == "executive")
    buyer_count = sum(1 for c in final if c.get("priority") == "buyer")

    domains_with_personal = sum(1 for s in ctx.domain_state.values() if s.get("has_personal_email"))
    domains_with_generic = sum(1 for s in ctx.domain_state.values() if s.get("has_generic_email"))

    print(f"\n{'='*60}")
    print(f"  PIPELINE COMPLETE (v6)")
    print(f"{'='*60}")
    print(f"  Scraped companies: {scraped_company_count}")
    if classification_log:
        print(f"  Classified companies: {len(classification_log)}")
        print(f"  Classification rejected: {len(classified_rejected)}")
    print(f"  Qualified companies: {len(companies)}")
    if ctx.positive_keywords or ctx.negative_keywords:
        print(f"  Filtered out (ICP scoring): {filtered_out}")
    print(f"  Contacts exported: {with_email}")
    print(f"  Contacts with phone: {with_phone}")
    print(f"  Decision makers: {owner_count + exec_count + buyer_count}")
    print(f"  Smartlead-ready: {len(smartlead_rows)}")
    if enable_signals:
        signal_leads = sum(1 for c in final if c.get("signal_score", 0) > 0)
        print(f"  Signal-based leads: {signal_leads}")
    print(f"")
    print(f"  Contact breakdown:")
    print(f"    Owners: {owner_count}")
    print(f"    Executives: {exec_count}")
    print(f"    Buyers: {buyer_count}")
    print(f"    Generic emails: {generic_count}")
    print(f"    Personal emails: {personal_count}")
    print(f"    Contacts with name: {with_name}")
    print(f"")
    print(f"  Enrichment sources:")
    print(f"    Website scraping: {stats['website']} companies")
    print(f"    Google SERP: {stats['serp']} companies")
    print(f"    Icypeas: {stats['icypeas']} companies")
    print(f"    Hunter: {stats['hunter']} companies")
    print(f"    Staff scraping: {stats['staff']} companies")
    print(f"    No contacts found: {stats['none']} companies")
    print(f"")
    print(f"  Domain coverage:")
    print(f"    Domains with personal emails: {domains_with_personal}")
    print(f"    Domains with generic emails only: {domains_with_generic}")
    print(f"    Emails propagated: {propagated}")
    print(f"")
    output_rel = os.path.relpath(config.OUTPUT_DIR, WORKSPACE_ROOT) if client_name else ""
    print(f"  Files:")
    if client_name:
        print(f"    {output_rel}/{prefix}companies.csv")
        print(f"    {output_rel}/{prefix}domains.csv")
        print(f"    {output_rel}/{prefix}contacts_final.csv")
        print(f"    {output_rel}/{prefix}decision_makers.csv")
        print(f"    {output_rel}/{prefix}smartlead_import.csv")
    else:
        print(f"    {prefix}companies.csv")
        print(f"    {prefix}domains.csv")
        print(f"    {prefix}contacts_final.csv")
        print(f"    {prefix}decision_makers.csv")
        print(f"    {prefix}smartlead_import.csv")
    print(f"{'='*60}\n")

    # ---- Clear checkpoint on success ----
    clear_checkpoint()

    # ---- Run history ----
    log_run(client_name, {
        "client_request": client_request,
        "companies_scraped": scraped_company_count,
        "companies_qualified": len(companies),
        "contacts_exported": with_email,
        "decision_makers": owner_count + exec_count + buyer_count,
        "smartlead_ready": len(smartlead_rows),
        "personal_emails": personal_count,
        "generic_emails": generic_count,
        "sources": dict(stats),
    })


# ============================================================
# CLI
# ============================================================

def build_parser():
    parser = argparse.ArgumentParser(
        description="ClientC Lead Pipeline v6 -- B2B lead generation with AI classification",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Environment variables:
  SERPER_API_KEY           Required
  ANTHROPIC_API_KEY        Required for --client mode and AI classification
  HUNTER_API_KEY           Optional (fallback enrichment)
  MILLIONVERIFIER_API_KEY  Optional (email verification)

Examples:
  python3 lead_pipeline_v6.py --client "fire protection company targeting hotels in Colorado"
  python3 lead_pipeline_v6.py --client "MSP targeting IT companies in FL" --name client_b
  python3 lead_pipeline_v6.py --client "..." --threshold 70
  python3 lead_pipeline_v6.py --client "..." --no-classify
  python3 lead_pipeline_v6.py --client "..." --dry-run
  python3 lead_pipeline_v6.py --clear-cache
        """,
    )
    parser.add_argument("--name", type=str, help="Client name for organizing output")
    parser.add_argument("--client", type=str, help="Natural language client description")
    parser.add_argument("--queries", type=str, help="Semicolon-separated custom queries")
    parser.add_argument("--cities", type=str, help="City preset or semicolon-separated city names")
    parser.add_argument("--output", type=str, default="", help="Output file prefix")
    parser.add_argument("--dry-run", action="store_true", help="Estimate API usage without making calls")
    parser.add_argument("--strict-filter", action="store_true", help="Apply rating/review + strict ICP scoring")
    parser.add_argument("--strict-icp", action="store_true", help="Enable strict ICP qualification scoring")
    parser.add_argument("--no-chains", action="store_true", help="Filter out national chains")
    parser.add_argument("--no-emails", action="store_true", help="Skip enrichment, export companies only")
    parser.add_argument("--no-grid", action="store_true", help="Skip geo-grid, search city center only")
    parser.add_argument("--grid", type=str, default="full", choices=["full", "cross", "center"], help="Grid mode")
    parser.add_argument("--no-expand", action="store_true", help="Skip query expansion")
    parser.add_argument("-y", "--yes", action="store_true", help="Auto-confirm large API credit usage")
    parser.add_argument("--discover", action="store_true", help="Use SERP web + LinkedIn discovery instead of Maps")
    parser.add_argument("--linkedin", action="store_true", help="Include LinkedIn company page discovery")
    parser.add_argument("--employees", type=str, default=None, help="Filter LinkedIn by employee range")
    parser.add_argument("--confirm", action="store_true", help="Pause after ICP generation to review")
    parser.add_argument("--skip-dedup", action="store_true", help="Skip global email deduplication")
    parser.add_argument("--check-credits", action="store_true", help="Check API credit balances and exit")
    parser.add_argument("--clear-cache", action="store_true", help="Delete the cache file and exit")
    parser.add_argument("--no-classify", action="store_true", help="Skip AI company classification")
    parser.add_argument("--threshold", type=int, default=None,
                        help=f"Classification confidence threshold 0-100 (default: {CLASSIFIER_CONFIDENCE_THRESHOLD})")
    parser.add_argument("--clear-classifications", action="store_true", help="Clear the classification cache and exit")
    parser.add_argument("--resume", action="store_true", help="Resume from last checkpoint if available")
    parser.add_argument("--signals", action="store_true", help="Enable buying signal detection (website + maps)")
    parser.add_argument("--signals-serp", action="store_true", help="Enable SERP-based signal detection (costs API credits)")
    parser.add_argument("--verticals", type=str, default=None,
                        help="Semicolon-separated verticals to run (e.g., 'restaurants;hotels;hospitals')")
    parser.add_argument("--enrich-titles", action="store_true",
                        help="Run title enrichment on contacts missing titles (SERP + Apify)")
    parser.add_argument("--no-apify", action="store_true",
                        help="Skip Apify LinkedIn lookups for title enrichment (SERP only, free)")
    parser.add_argument("--blitz", action="store_true",
                        help="Use BlitzAPI for contact enrichment (runs before other enrichment sources)")
    parser.add_argument("--no-blitz", action="store_true",
                        help="Skip BlitzAPI even if BLITZ_API_KEY is set")
    parser.add_argument("--double-verify", action="store_true",
                        help="Run BounceBan after MillionVerifier for double email verification")
    return parser


if __name__ == "__main__":
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    parser = build_parser()
    args = parser.parse_args()

    if args.clear_cache:
        if os.path.exists(CACHE_FILE):
            os.remove(CACHE_FILE)
            print("Pipeline cache cleared.")
        sys.exit(0)

    if args.clear_classifications:
        if os.path.exists(CLASSIFICATION_CACHE_FILE):
            os.remove(CLASSIFICATION_CACHE_FILE)
            print("Classification cache cleared.")
        sys.exit(0)

    if args.check_credits:
        print_credit_status()
        sys.exit(0)

    if not SERPER_API_KEY:
        parser.error("SERPER_API_KEY environment variable is required. "
                      "Set it with: export SERPER_API_KEY='your_key'")

    custom_queries = None
    if args.queries:
        custom_queries = [q.strip() for q in args.queries.split(";") if q.strip()]

    client_request = args.client
    if not client_request and not custom_queries:
        parser.error("Provide --client or --queries argument")
    if not client_request:
        client_request = "(custom queries)"

    cities = None
    if args.cities:
        if args.cities in CITY_PRESETS:
            cities = CITY_PRESETS[args.cities]
        else:
            cities = [c.strip() for c in args.cities.split(";") if c.strip()]

    # Multi-vertical support
    if args.verticals:
        verticals = [v.strip() for v in args.verticals.split(";") if v.strip()]
        print(f"\n  Multi-vertical campaign: {len(verticals)} verticals")
        print(f"  Verticals: {', '.join(verticals)}")
        print(f"  Running each vertical as a separate pipeline pass...\n")

        for i, vertical in enumerate(verticals, 1):
            # Build the client request for this vertical
            # Extract the base client identity from the original request
            vertical_request = f"{client_request} targeting {vertical}"
            vertical_prefix = args.output + "_" + vertical.replace(" ", "_") if args.output else vertical.replace(" ", "_")

            print(f"\n{'='*60}")
            print(f"  VERTICAL {i}/{len(verticals)}: {vertical}")
            print(f"{'='*60}")

            run_pipeline(
                client_request=vertical_request,
                cities=cities,
                output_prefix=vertical_prefix,
                dry_run=args.dry_run,
                strict_filter=args.strict_filter or args.strict_icp,
                no_chains=args.no_chains,
                no_emails=args.no_emails,
                auto_confirm=args.yes,
                no_grid=args.no_grid,
                grid_mode=args.grid,
                no_expand=args.no_expand,
                queries=custom_queries,
                client_name=args.name,
                discover=args.discover,
                include_linkedin=args.linkedin or args.discover,
                employee_range=args.employees,
                confirm_icp=args.confirm,
                skip_dedup=args.skip_dedup,
                no_classify=args.no_classify,
                threshold=args.threshold,
                enable_signals=args.signals or args.signals_serp,
                signals_serp=args.signals_serp,
                enrich_titles_flag=args.enrich_titles,
                no_apify=args.no_apify,
            )

        print(f"\n{'='*60}")
        print(f"  ALL VERTICALS COMPLETE ({len(verticals)} runs)")
        print(f"{'='*60}\n")
    else:
        run_pipeline(
            client_request=client_request,
            cities=cities,
            output_prefix=args.output,
            dry_run=args.dry_run,
            strict_filter=args.strict_filter or args.strict_icp,
            no_chains=args.no_chains,
            no_emails=args.no_emails,
            auto_confirm=args.yes,
            no_grid=args.no_grid,
            grid_mode=args.grid,
            no_expand=args.no_expand,
            queries=custom_queries,
            client_name=args.name,
            discover=args.discover,
            include_linkedin=args.linkedin or args.discover,
            employee_range=args.employees,
            confirm_icp=args.confirm,
            skip_dedup=args.skip_dedup,
            no_classify=args.no_classify,
            threshold=args.threshold,
            enable_signals=args.signals or args.signals_serp,
            signals_serp=args.signals_serp,
            enrich_titles_flag=args.enrich_titles,
            no_apify=args.no_apify,
        )
