#!/usr/bin/env python3
"""
verify_combine.py — Combine results from the three independent niche-fit
verification methods and apply consensus rules.

Methods:
    1. Name-based (verify_niche_fit.py)        -> _niche_fit_cache.json
    2. Website content (verify_niche_fit_website.py) -> _website_verify_cache.json
    3. Blitz LinkedIn data (verify_niche_fit_blitz.py) -> _blitz_verify_cache.json
    + Title red flags (verify_title_redflags.py, regex, not cached)

Consensus rule (default): exclude a lead only if AT LEAST 2 of the 3 LLM
methods agree it's a misfit at their respective confidence thresholds.

Title red flags are always additive (a bad title alone excludes a lead).

Usage:
    python3 tools/verify_combine.py                          # dry-run
    python3 tools/verify_combine.py --commit                 # apply
    python3 tools/verify_combine.py --require 3              # require all 3 methods to agree (stricter)
    python3 tools/verify_combine.py --require 1              # any one method (loosest, not recommended)
"""

import os
import sys
import json
import shutil
import sqlite3
import argparse
from datetime import datetime
from collections import Counter, defaultdict

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.dirname(SCRIPT_DIR)
DB_PATH = os.path.join(ROOT_DIR, "master-leads", "master_leads.db")

NAME_CACHE = os.path.join(SCRIPT_DIR, "_niche_fit_cache.json")
WEB_CACHE = os.path.join(SCRIPT_DIR, "_website_verify_cache.json")
BLITZ_CACHE = os.path.join(SCRIPT_DIR, "_blitz_verify_cache.json")

sys.path.insert(0, SCRIPT_DIR)
from verify_niche_fit import FIT_CRITERIA
from verify_title_redflags import is_bad_title


def load_json(p):
    if os.path.isfile(p):
        try: return json.load(open(p))
        except Exception: return {}
    return {}


def name_key(niche, company, title, domain):
    return f"{niche}|{(company or '').strip().lower()}|{(title or '').strip().lower()}|{(domain or '').strip().lower()}"


def main():
    ap = argparse.ArgumentParser(description="Combine name + website + Blitz verdicts into final exclusion list")
    ap.add_argument("--require", type=int, default=2, choices=[1, 2, 3],
                    help="minimum number of methods that must agree (default 2)")
    ap.add_argument("--name-threshold", type=float, default=0.90)
    ap.add_argument("--website-threshold", type=float, default=0.80)
    ap.add_argument("--blitz-threshold", type=float, default=0.80)
    ap.add_argument("--include-sent", action="store_true", default=True)
    ap.add_argument("--commit", action="store_true")
    args = ap.parse_args()

    name_cache = load_json(NAME_CACHE)
    web_cache = load_json(WEB_CACHE)
    blitz_cache = load_json(BLITZ_CACHE)

    print(f"caches: name={len(name_cache)}  web={len(web_cache)}  blitz={len(blitz_cache)}")

    # Load all candidate leads
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    statuses = ["new"]
    if args.include_sent:
        statuses.append("sent")
    qs = ",".join("?" * len(statuses))
    cur.execute(f"""SELECT id, client, niche, email, first_name, last_name, company, title, domain, status, notes
                    FROM leads WHERE status IN ({qs})""", statuses)
    rows = cur.fetchall()
    print(f"leads checked: {len(rows)}")

    targets = set(FIT_CRITERIA.keys())

    exclude = []           # final list
    vote_counts = Counter()  # how many votes per excluded lead
    title_only = 0

    for r in rows:
        client, niche = r["client"], r["niche"]
        status = r["status"]

        # Title red flag — instant exclude regardless of other votes
        if is_bad_title(r["title"]):
            exclude.append((r, "title", None))
            title_only += 1
            continue

        if (client, niche) not in targets:
            continue  # no fit criteria defined for this bucket

        votes = 0
        reasons = []

        # Name pass
        k = name_key(niche, r["company"], r["title"], r["domain"])
        nv = name_cache.get(k)
        if nv and not nv.get("fit") and nv.get("confidence", 0) >= args.name_threshold:
            votes += 1
            reasons.append(f"name[{nv['confidence']:.2f}]")

        # Website pass
        # Key in web cache is: niche|company|domain|site_text_len — need to match by prefix
        web_verdict = None
        web_prefix = f"{niche}|{(r['company'] or '').strip().lower()}|{(r['domain'] or '').strip().lower()}|"
        for wk, wv in web_cache.items():
            if wk.startswith(web_prefix):
                web_verdict = wv
                break
        if web_verdict and not web_verdict.get("fit") and web_verdict.get("confidence", 0) >= args.website_threshold:
            votes += 1
            reasons.append(f"web[{web_verdict['confidence']:.2f}]")

        # Blitz pass
        # Key in blitz cache is: niche|domain|yes/no
        blitz_verdict = None
        for bk, bv in blitz_cache.items():
            if bk.startswith(f"{niche}|{(r['domain'] or '').strip().lower()}|"):
                blitz_verdict = bv
                break
        if blitz_verdict and not blitz_verdict.get("fit") and blitz_verdict.get("confidence", 0) >= args.blitz_threshold:
            votes += 1
            reasons.append(f"blitz[{blitz_verdict['confidence']:.2f}]")

        if votes >= args.require:
            exclude.append((r, ",".join(reasons), votes))
            vote_counts[votes] += 1

    # Dedup by lead id (title red flags may overlap with consensus)
    seen = set()
    final = []
    for r, reason, votes in exclude:
        if r["id"] in seen: continue
        seen.add(r["id"])
        final.append((r, reason, votes))

    print(f"\n=== CONSENSUS RESULTS ===")
    print(f"require: {args.require} of 3 methods")
    print(f"title red-flags: {title_only}")
    print(f"LLM consensus exclusions: {sum(vote_counts.values())}")
    print(f"vote breakdown:")
    for v, n in sorted(vote_counts.items(), reverse=True):
        print(f"  {v} of 3 agree: {n}")
    print(f"TOTAL to exclude: {len(final)}")

    by_status = Counter(r["status"] for r, _, _ in final)
    print(f"\nby status: {dict(by_status)}")

    by_cn_status = Counter((r["client"], r["niche"], r["status"]) for r, _, _ in final)
    print(f"\nby client/niche/status:")
    for (c, n, s), ct in sorted(by_cn_status.items(), key=lambda x: -x[1])[:25]:
        print(f"  {c:<18}{n:<28}{s:<8}{ct}")

    print(f"\n=== sample exclusions (15) ===")
    for r, reason, votes in final[:15]:
        vote_str = f"{votes}/3" if votes else "title"
        print(f"  [{vote_str}] {r['client']:<18}{r['niche']:<22}{(r['company'] or '')[:30]:<30}{reason[:40]}")

    if not final:
        print("\nnothing to exclude")
        return

    if not args.commit:
        print(f"\nDRY RUN — re-run with --commit to apply")
        return

    bak = f"{DB_PATH}.bak_combine_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    shutil.copy2(DB_PATH, bak)
    print(f"\nbackup: {bak}")

    conn2 = sqlite3.connect(DB_PATH)
    cur2 = conn2.cursor()
    title_ids = [r["id"] for r, reason, _ in final if reason == "title"]
    off_target_ids = [r["id"] for r, reason, _ in final if reason != "title"]
    if title_ids:
        cur2.executemany(
            "UPDATE leads SET status='excluded_bad_title', date_updated=datetime('now') WHERE id=?",
            [(i,) for i in title_ids],
        )
        print(f"  bad_title: {cur2.rowcount}")
    if off_target_ids:
        cur2.executemany(
            "UPDATE leads SET status='excluded_off_target', date_updated=datetime('now') WHERE id=?",
            [(i,) for i in off_target_ids],
        )
        print(f"  off_target: {cur2.rowcount}")
    conn2.commit()
    conn2.close()
    print(f"applied {len(final)} exclusions total")


if __name__ == "__main__":
    main()
