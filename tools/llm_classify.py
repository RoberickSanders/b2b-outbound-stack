#!/usr/bin/env python3
"""
llm_classify.py — Use Claude Haiku to classify ambiguous leads into the correct
niche bucket based on company name + title + optional website snippet.

Cheap (~$0.05 per 1,000 leads), cached, resumable, read-only until the final
commit step.

Usage:
    # Dry-run: classify without writing (shows what it would do)
    python3 tools/llm_classify.py --client client_a --niche other --dry-run

    # Real run: classify and update DB
    python3 tools/llm_classify.py --client client_a --niche other

    # Classify everything ambiguous across all clients
    python3 tools/llm_classify.py --all-ambiguous

    # Use a different model
    python3 tools/llm_classify.py --client client_a --niche other --model claude-haiku-4-5

Buckets available per client (edit ALLOWED_BUCKETS to change):
    client_a: restaurants, hotels, churches, schools, property-management,
                   real-estate-realtors, apartments, medical, assisted-living,
                   daycares, coffee-shops, community, gyms, retail,
                   manufacturing, office, warehouses, auto, storage, other
    client_c: (B2B, niche is the product not the target) — skip
    client_b: (B2B) — skip

Safety:
    - Dry-run by default; requires --commit flag in non-dry-run mode
    - Caches responses in tools/_llm_cache.json to avoid re-billing
    - Parallel with 20 workers
    - Backs up master_leads.db before writing
    - Only touches the `niche` column. Never touches status, email, etc.
"""

import os
import re
import sys
import json
import time
import sqlite3
import argparse
import shutil
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.dirname(SCRIPT_DIR)
DB_PATH = os.path.join(ROOT_DIR, "master-leads", "master_leads.db")
CACHE_PATH = os.path.join(SCRIPT_DIR, "_llm_cache.json")

# Load .env — manual parse because python-dotenv silently fails on some paths here
def _load_env_file(path):
    if not os.path.isfile(path):
        return
    try:
        with open(path) as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                k, v = line.split("=", 1)
                k = k.strip()
                v = v.strip().strip('"').strip("'")
                if k and not os.environ.get(k):
                    os.environ[k] = v
    except Exception:
        pass

for _p in (
    "~/agency-os/.env",
    os.path.abspath(os.path.join(ROOT_DIR, "..", "..", ".env")),
    os.path.join(ROOT_DIR, ".env"),
):
    _load_env_file(_p)

import anthropic

ALLOWED_BUCKETS = {
    "client_a": [
        "restaurants", "hotels", "churches", "schools", "property-management",
        "real-estate-realtors", "apartments", "medical", "assisted-living",
        "daycares", "coffee-shops", "community", "gyms", "retail",
        "manufacturing", "office", "warehouses", "auto", "storage",
        "fitness-recreation", "nonprofit", "professional-services", "other"
    ],
}

DEFAULT_MODEL = "claude-haiku-4-5"

SYSTEM_PROMPT = """You classify small businesses into target niches for a cold email pipeline.

You will be given a company name, a job title (if available), and sometimes a domain. Return ONLY a single JSON object with this exact shape:

{"bucket": "<one of the allowed values>", "confidence": 0.0-1.0, "reason": "<brief, 8 words max>"}

Rules:
- Output ONLY the JSON. No preamble, no trailing text, no markdown.
- bucket MUST be one of the allowed values you are given.
- If unsure or the company could fit multiple, return "other" with low confidence.
- confidence should reflect how certain you are based on the name alone.
- "reason" is a short human-readable hint explaining the choice."""


def build_user_prompt(company, title, domain, allowed):
    lines = [f"Company: {company or '(none)'}"]
    if title:
        lines.append(f"Title: {title}")
    if domain:
        lines.append(f"Domain: {domain}")
    lines.append("")
    lines.append(f"Allowed buckets: {', '.join(allowed)}")
    lines.append("")
    lines.append("Return ONLY the JSON object.")
    return "\n".join(lines)


def load_cache():
    if os.path.isfile(CACHE_PATH):
        try:
            return json.load(open(CACHE_PATH))
        except Exception:
            return {}
    return {}


def save_cache(cache):
    # snapshot to avoid RuntimeError: dict changed size during iteration
    snapshot = dict(cache)
    tmp = CACHE_PATH + ".tmp"
    with open(tmp, "w") as f:
        json.dump(snapshot, f, indent=2)
    os.replace(tmp, CACHE_PATH)


def cache_key(company, title, domain):
    return f"{(company or '').strip().lower()}|{(title or '').strip().lower()}|{(domain or '').strip().lower()}"


def classify_one(client, company, title, domain, allowed, model, cache):
    key = cache_key(company, title, domain)
    if key in cache:
        return cache[key]

    try:
        resp = client.messages.create(
            model=model,
            max_tokens=120,
            system=SYSTEM_PROMPT,
            messages=[{"role": "user", "content": build_user_prompt(company, title, domain, allowed)}],
        )
        raw = resp.content[0].text.strip() if resp.content else ""
        # Strip any accidental wrapper
        m = re.search(r"\{[^}]*\}", raw, re.DOTALL)
        if not m:
            result = {"bucket": "other", "confidence": 0.0, "reason": "no_json"}
        else:
            parsed = json.loads(m.group(0))
            bucket = (parsed.get("bucket") or "other").lower()
            if bucket not in allowed:
                bucket = "other"
            result = {
                "bucket": bucket,
                "confidence": float(parsed.get("confidence", 0)),
                "reason": (parsed.get("reason") or "")[:60],
            }
    except Exception as e:
        result = {"bucket": "other", "confidence": 0.0, "reason": f"err:{str(e)[:30]}"}

    cache[key] = result
    return result


def main():
    ap = argparse.ArgumentParser(description="LLM-classify ambiguous leads into correct niche buckets")
    ap.add_argument("--client", default="client_a")
    ap.add_argument("--niche", help="current niche to reclassify (e.g. other, property-management-unclear)")
    ap.add_argument("--all-ambiguous", action="store_true",
                    help="classify everything in 'other' and '*-unclear' buckets across the client")
    ap.add_argument("--model", default=DEFAULT_MODEL)
    ap.add_argument("--workers", type=int, default=20)
    ap.add_argument("--limit", type=int, default=0, help="max leads to classify this run (0 = all)")
    ap.add_argument("--min-confidence", type=float, default=0.7,
                    help="only move to new bucket if confidence >= this threshold")
    ap.add_argument("--dry-run", action="store_true", default=True)
    ap.add_argument("--commit", action="store_true", help="actually write to DB (disables dry-run)")
    args = ap.parse_args()

    if args.commit:
        args.dry_run = False

    client_name = args.client
    if client_name not in ALLOWED_BUCKETS:
        print(f"ERROR: no allowed buckets for client '{client_name}'")
        sys.exit(2)
    allowed = ALLOWED_BUCKETS[client_name]

    # Pull target leads
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    if args.all_ambiguous:
        cur.execute("""
            SELECT id, company, title, domain, niche
            FROM leads
            WHERE client=? AND status='new'
              AND (niche='other' OR niche LIKE '%-unclear')
        """, (client_name,))
    elif args.niche:
        cur.execute("""
            SELECT id, company, title, domain, niche
            FROM leads
            WHERE client=? AND niche=? AND status='new'
        """, (client_name, args.niche))
    else:
        print("ERROR: pass --niche or --all-ambiguous")
        sys.exit(2)

    rows = cur.fetchall()
    if args.limit:
        rows = rows[:args.limit]
    conn.close()

    if not rows:
        print("nothing to classify")
        return

    print(f"classifying {len(rows)} leads")
    print(f"client: {client_name}")
    print(f"model: {args.model}")
    print(f"dry-run: {args.dry_run}")
    print(f"confidence threshold: {args.min_confidence}")
    print()

    cache = load_cache()
    cache_hits = sum(1 for r in rows if cache_key(r["company"], r["title"], r["domain"]) in cache)
    print(f"cache hits: {cache_hits} / {len(rows)}")

    # Route light classification through llm_router (Kimi K2.6 ~8x cheaper than Haiku).
    # Honor --model if user explicitly set it to a non-default value.
    if args.model == DEFAULT_MODEL:
        import sys as _sys
        _pipe_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        if _pipe_dir not in _sys.path:
            _sys.path.insert(0, _pipe_dir)
        from llm_router import get_light_client
        api, args.model = get_light_client()
    else:
        api = anthropic.Anthropic()
    results = {}  # lead_id -> result

    t0 = time.time()
    done = 0
    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futs = {
            ex.submit(classify_one, api, r["company"], r["title"], r["domain"], allowed, args.model, cache): r
            for r in rows
        }
        for f in as_completed(futs):
            r = futs[f]
            try:
                results[r["id"]] = f.result()
            except Exception as e:
                results[r["id"]] = {"bucket": "other", "confidence": 0.0, "reason": f"err:{str(e)[:30]}"}
            done += 1
            if done % 50 == 0:
                print(f"  {done}/{len(rows)}")
                save_cache(cache)

    save_cache(cache)
    print(f"done in {time.time()-t0:.1f}s")

    # Summarize distribution
    from collections import Counter
    bucket_counts = Counter(v["bucket"] for v in results.values())
    print()
    print("=== bucket distribution ===")
    for b, n in bucket_counts.most_common():
        print(f"  {b:<28}{n}")

    # Determine moves (only when confidence high enough AND bucket differs)
    moves = []
    skipped = 0
    for r in rows:
        res = results[r["id"]]
        if res["confidence"] < args.min_confidence:
            skipped += 1
            continue
        if res["bucket"] == r["niche"]:
            continue
        moves.append((res["bucket"], r["id"], r["company"], r["niche"], res))

    print()
    print(f"moves to apply: {len(moves)}")
    print(f"skipped (low confidence): {skipped}")

    if not moves:
        print("nothing to move")
        return

    # Sample 15
    print("\n=== sample moves ===")
    for new_bucket, lid, comp, old_niche, res in moves[:15]:
        print(f"  [{res['confidence']:.2f}] {(comp or '')[:40]:<40} {old_niche:<22} -> {new_bucket}  ({res['reason']})")

    if args.dry_run:
        print("\nDRY RUN — no DB changes. Re-run with --commit to apply.")
        return

    # Backup
    bak = f"{DB_PATH}.bak_llm_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    shutil.copy2(DB_PATH, bak)
    print(f"\nbackup: {bak}")

    # Apply
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.executemany(
        "UPDATE leads SET niche=?, date_updated=datetime('now') WHERE id=?",
        [(m[0], m[1]) for m in moves],
    )
    conn.commit()
    print(f"applied {cur.rowcount} moves")
    conn.close()


if __name__ == "__main__":
    main()
