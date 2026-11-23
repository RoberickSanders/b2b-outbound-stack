#!/usr/bin/env python3
"""
verify_niche_fit.py — Use Claude Haiku to verify that each lead in a niche
actually FITS the target profile. This is different from llm_classify.py:

  llm_classify.py asks:  "What bucket should this company be in?"
  verify_niche_fit.py asks: "Is this company a reasonable target for niche X?"

For ClientC, niches represent WHO we sell services TO (cost seg buyers,
utility audit candidates, fire protection firms, etc). A hairdresser showing up
in the cost-seg bucket is a bucketizer false positive that LLM verification
catches.

Safety:
- Dry-run by default, requires --commit to write.
- Backs up DB before any write.
- Only moves leads to 'excluded_off_target'. Does NOT change niche, status
  (other than exclude), or verified flags.
- Works on both status='new' AND status='sent' (with --include-sent).
- For sent leads, prints the Smartlead campaigns they're in so user can decide
  to clean them up manually.
- Caches all Haiku calls in tools/_niche_fit_cache.json.

Usage:
    # Dry-run all clients, all niches, active leads only
    python3 tools/verify_niche_fit.py --all

    # Include leads already sent (to audit live campaigns)
    python3 tools/verify_niche_fit.py --all --include-sent

    # Just one client
    python3 tools/verify_niche_fit.py --client client_c --include-sent

    # Just one niche
    python3 tools/verify_niche_fit.py --client client_a --niche schools

    # Commit
    python3 tools/verify_niche_fit.py --all --include-sent --commit
"""

import os
import re
import sys
import json
import time
import shutil
import sqlite3
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.dirname(SCRIPT_DIR)
DB_PATH = os.path.join(ROOT_DIR, "master-leads", "master_leads.db")
CACHE_PATH = os.path.join(SCRIPT_DIR, "_niche_fit_cache.json")

# Load .env
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

DEFAULT_MODEL = "claude-haiku-4-5"

# ============================================================================
# FIT CRITERIA — define what "a good target" looks like for each client+niche
# ============================================================================

FIT_CRITERIA = {
    # ---------------- ClientA (prospect = business with building(s)) ----------------
    ("client_a", "schools"): "K-12 schools, private schools, academies, colleges, universities, charter schools, boarding schools, or educational districts with physical campus buildings",
    ("client_a", "churches"): "Churches, chapels, cathedrals, ministries, parishes, synagogues, mosques, or religious congregations that own/operate a place of worship",
    ("client_a", "property-management"): "Property management companies, HOA management firms, rental property managers, or real estate management firms that manage multiple physical buildings",
    ("client_a", "property-management-unclear"): "Property management companies, HOA management firms, rental property managers, or real estate management firms",
    ("client_a", "real-estate-realtors"): "Real estate agencies, realty firms, real estate brokerages, individual realtors, or real estate sales companies",
    ("client_a", "hotels"): "Hotels, motels, inns, resorts, lodges, hostels, bed & breakfasts, or hospitality businesses with lodging facilities",
    ("client_a", "restaurants"): "Restaurants, cafes, bistros, grills, diners, pubs, taverns, bakeries, pizzerias, or other food-service establishments with a physical location",
    ("client_a", "coffee-shops"): "Coffee shops, cafes, coffee roasters, espresso bars, or similar retail coffee businesses",
    ("client_a", "medical"): "Hospitals, medical clinics, dental practices, physician offices, urgent care, specialty medical practices, or healthcare facilities",
    ("client_a", "assisted-living"): "Assisted living facilities, senior living communities, nursing homes, retirement homes, memory care, or elder care facilities",
    ("client_a", "daycares"): "Childcare centers, daycare facilities, preschools, early learning centers, or kindergarten programs",
    ("client_a", "apartments"): "Apartment complexes, residential communities, lofts, condominiums, townhomes, or multi-unit residential properties",
    ("client_a", "community"): "Community centers, nonprofit community organizations, civic centers, foundations, libraries, or museums that operate a physical building",
    ("client_a", "nonprofit"): "Nonprofit organizations, foundations, or charities that operate from physical facilities",
    ("client_a", "professional-services"): "Professional service firms like law offices, accounting firms, consulting offices, or similar that occupy commercial office space",
    ("client_a", "fitness-recreation"): "Gyms, fitness centers, yoga studios, crossfit boxes, martial arts studios, dance studios, or recreational facilities",
    ("client_a", "gyms"): "Gyms, fitness centers, or workout facilities",
    ("client_a", "gyms-rec"): "Gyms, fitness centers, recreation centers, or sports facilities",
    ("client_a", "retail"): "Retail stores, boutiques, shops, or similar brick-and-mortar retail businesses",
    ("client_a", "manufacturing"): "Manufacturing plants, factories, industrial facilities, fabrication shops, or production facilities",
    ("client_a", "warehouses"): "Warehouses, distribution centers, logistics facilities, or fulfillment centers",
    ("client_a", "auto"): "Auto body shops, car dealerships, automotive service centers, collision centers, or car washes",
    ("client_a", "storage"): "Self-storage facilities or storage unit operators",
    ("client_a", "office"): "Generic office-based businesses occupying commercial office space",
    ("client_a", "hotels-unclear"): "Hotels, motels, inns, resorts, lodges, or similar lodging businesses",
    ("client_a", "other"): "Any brick-and-mortar business with a physical building that needs fire protection services",

    # ---------------- ClientC (prospect = client WHO BUYS the service) ----------------
    ("client_c", "cost-segregation"): "Cost segregation consulting firms, cost seg study providers, specialty tax consultants that conduct cost segregation studies, or engineering-based tax consulting firms (we sell lead gen services TO these cost seg firms). NOT property owners, NOT real estate investors, NOT wealth managers, NOT generic CPA firms without cost seg specialty, NOT financial advisors",
    ("client_c", "utility-audit"): "Utility bill auditing firms, utility cost recovery consultants, energy expense management firms, or utility audit service providers that audit commercial utility bills and recover overcharges on behalf of businesses (we sell lead gen services TO these audit firms). NOT the businesses that HAVE utility bills, NOT electric/gas/water utility companies themselves, NOT wealth managers, NOT financial advisors",
    ("client_c", "fire-protection"): "Fire protection service companies, fire sprinkler contractors, fire alarm installers, or life safety firms (we are doing biz dev targeting these companies as clients)",
    ("client_c", "property-tax-appeal"): "Property tax appeal consulting firms, property tax protest services, ad valorem tax consultants, or commercial property tax reduction specialists that help owners appeal over-assessments (we sell lead gen services TO these consulting firms). NOT commercial property owners, NOT REITs, NOT property management firms, NOT wealth managers",
    ("client_c", "telecom-audit"): "Telecom expense management (TEM) firms, telecom audit consulting companies, telecom bill auditing firms, or wireless expense management consultants that audit multi-location telecom spend and recover overcharges (we sell lead gen services TO these audit firms). NOT telecom carriers themselves, NOT wireless providers, NOT businesses that HAVE telecom bills, NOT wealth managers",
    ("client_c", "telecom-expense"): "Telecom expense management (TEM) firms and telecom audit consulting companies — same ICP as telecom-audit. NOT telecom carriers, NOT businesses that HAVE telecom bills",
    ("client_c", "rd-tax-credit"): "R&D tax credit consulting firms, Section 174 advisory firms, specialty tax credit consultants, or R&D credit study firms that help OTHER companies CLAIM R&D tax credits (we sell lead gen services TO these consulting firms). NOT the companies that NEED R&D credits themselves, NOT wealth management firms, NOT investment advisors, NOT generic CPA firms without R&D specialization, NOT financial planners",
    ("client_c", "ma-advisors"): "M&A advisory firms, business brokers, investment banks, or sell-side advisors helping SMBs exit",
    ("client_c", "osha-compliance"): "OSHA compliance consulting firms, safety consulting companies, EHS consulting firms, or industrial safety service providers that help businesses comply with OSHA regulations (NOT the businesses that NEED OSHA compliance — those are prospects, not CLIENT_C clients)",
    ("client_c", "freight-audit"): "Freight audit and payment companies, freight bill auditing firms, shipping/logistics cost recovery consultants, or parcel audit services that audit and recover overcharges on freight/shipping invoices",
    ("client_c", "sales-tax-recovery"): "Sales tax recovery firms, sales tax compliance consultants, indirect tax consulting companies, or state & local tax (SALT) advisory firms that help businesses recover overpaid sales tax or manage multi-state tax compliance",
    ("client_c", "elevator-inspection"): "Elevator inspection companies, elevator maintenance firms, vertical transportation consultants, escalator service companies, or elevator code compliance inspectors",
    ("client_c", "fire-alarm-inspection"): "Fire alarm inspection and testing companies, fire alarm monitoring firms, fire detection service providers, or fire alarm system contractors (NOT fire sprinkler/suppression — that's a different sub-niche)",
    ("client_c", "workers-comp-recovery"): "Workers compensation premium recovery firms, workers comp audit consulting companies, experience modification factor (EMF/X-Mod) review specialists, or workers comp cost containment consultants that help employers recover overpaid premiums or dispute experience mod calculations (we sell lead gen services TO these recovery firms). NOT insurance brokers, NOT insurance carriers like Texas Mutual or Highview, NOT state rating bureaus like NCCI/NYCIRB/ICRB, NOT claims adjusters or TPA firms, NOT law firms, NOT PEOs or HR firms, NOT medical management services",

    # ---------------- ClientB (prospect = client WHO BUYS cybersecurity/VCISO) ----------------
    ("client_b", "msps"): "Managed Service Providers (MSPs) or IT services firms that manage IT for small/mid businesses and could partner on cybersecurity / VCISO / SOC2 services",
    ("client_b", "fintech"): "Fintech companies, financial technology startups, payment processors, or financial software companies that need SOC2 compliance or cybersecurity services",
    ("client_b", "unknown"): "Any B2B company that could benefit from VCISO, SOC2, or outsourced cybersecurity services",
}


SYSTEM_PROMPT = """You are verifying whether a company is a REASONABLE TARGET for a specific niche in a cold-email pipeline.

You will be given:
- The niche description (what kind of company we want to target)
- The candidate company name and optional title/domain

Return ONLY a single JSON object with this exact shape:

{"fit": true|false, "confidence": 0.0-1.0, "reason": "<brief, 10 words max>"}

Rules:
- Output ONLY the JSON. No preamble, no trailing text, no markdown fences.
- fit=true if the company plausibly matches the niche description.
- fit=false if the company is clearly NOT in the target profile (e.g., a hair salon in a cost-segregation list).
- When in doubt (ambiguous company name, could be either), return fit=true with low confidence. Don't kill ambiguous leads.
- confidence reflects how certain you are.
- "reason" is a short human-readable hint explaining the choice."""


def build_user_prompt(niche_description, company, title, domain):
    lines = [f"Niche target profile: {niche_description}", ""]
    lines.append(f"Company: {company or '(none)'}")
    if title:
        lines.append(f"Title: {title}")
    if domain:
        lines.append(f"Domain: {domain}")
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
    snapshot = dict(cache)
    tmp = CACHE_PATH + ".tmp"
    with open(tmp, "w") as f:
        json.dump(snapshot, f, indent=2)
    os.replace(tmp, CACHE_PATH)


def cache_key(niche, company, title, domain):
    return f"{niche}|{(company or '').strip().lower()}|{(title or '').strip().lower()}|{(domain or '').strip().lower()}"


def verify_one(api, niche, niche_desc, company, title, domain, model, cache):
    key = cache_key(niche, company, title, domain)
    if key in cache:
        return cache[key]
    try:
        resp = api.messages.create(
            model=model,
            max_tokens=100,
            system=SYSTEM_PROMPT,
            messages=[{"role": "user", "content": build_user_prompt(niche_desc, company, title, domain)}],
        )
        raw = resp.content[0].text.strip() if resp.content else ""
        m = re.search(r"\{[^}]*\}", raw, re.DOTALL)
        if not m:
            result = {"fit": True, "confidence": 0.0, "reason": "no_json"}
        else:
            parsed = json.loads(m.group(0))
            result = {
                "fit": bool(parsed.get("fit", True)),
                "confidence": float(parsed.get("confidence", 0)),
                "reason": (parsed.get("reason") or "")[:60],
            }
    except Exception as e:
        # On error, default to FIT=True so we don't mistakenly exclude legitimate leads
        result = {"fit": True, "confidence": 0.0, "reason": f"err:{str(e)[:30]}"}
    cache[key] = result
    return result


def fetch_leads(cur, client, niche, include_sent):
    statuses = ["new"]
    if include_sent:
        statuses.append("sent")
    qs = ",".join("?" * len(statuses))
    cur.execute(f"""SELECT id, email, first_name, last_name, company, title, domain, status, notes
                    FROM leads
                    WHERE client=? AND niche=? AND status IN ({qs})""", (client, niche, *statuses))
    return cur.fetchall()


def main():
    ap = argparse.ArgumentParser(description="LLM niche-fit verification (dry-run by default)")
    ap.add_argument("--all", action="store_true", help="verify all clients + niches with defined criteria")
    ap.add_argument("--client", help="limit to one client")
    ap.add_argument("--niche", help="limit to one niche")
    ap.add_argument("--include-sent", action="store_true", help="also verify leads in status='sent'")
    ap.add_argument("--min-confidence", type=float, default=0.75,
                    help="only exclude if fit=false AND confidence >= this threshold")
    ap.add_argument("--model", default=DEFAULT_MODEL)
    ap.add_argument("--workers", type=int, default=15)
    ap.add_argument("--limit", type=int, default=0, help="max leads per niche (0=all)")
    ap.add_argument("--commit", action="store_true", help="apply changes to DB (default is dry-run)")
    args = ap.parse_args()

    # Determine target (client, niche) pairs
    targets = []
    if args.all:
        targets = list(FIT_CRITERIA.keys())
    elif args.client and args.niche:
        if (args.client, args.niche) in FIT_CRITERIA:
            targets = [(args.client, args.niche)]
        else:
            print(f"ERROR: no fit criteria defined for {args.client}/{args.niche}")
            sys.exit(2)
    elif args.client:
        targets = [k for k in FIT_CRITERIA.keys() if k[0] == args.client]
    else:
        print("ERROR: pass --all, --client, or --client+--niche")
        sys.exit(2)

    print(f"mode: {'COMMIT' if args.commit else 'DRY-RUN'}")
    print(f"include_sent: {args.include_sent}")
    print(f"targets: {len(targets)} (client, niche) pairs")
    print(f"model: {args.model}")
    print(f"min confidence to exclude: {args.min_confidence}")
    print()

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    cache = load_cache()
    # Route light niche-fit classification through llm_router (Kimi K2.6 ~8x cheaper
    # than Haiku). Honor --model if user explicitly set it to a non-default value.
    if args.model == DEFAULT_MODEL:
        import sys as _sys
        _pipe_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        if _pipe_dir not in _sys.path:
            _sys.path.insert(0, _pipe_dir)
        from llm_router import get_light_client
        api, args.model = get_light_client()
    else:
        api = anthropic.Anthropic()

    all_fails = []  # (client, niche, lead_row, result)
    total_checked = 0

    for client, niche in targets:
        rows = fetch_leads(cur, client, niche, args.include_sent)
        if args.limit:
            rows = rows[:args.limit]
        if not rows:
            continue
        niche_desc = FIT_CRITERIA[(client, niche)]
        print(f"=== {client}/{niche}: {len(rows)} leads ===")

        results = {}
        with ThreadPoolExecutor(max_workers=args.workers) as ex:
            futs = {
                ex.submit(verify_one, api, niche, niche_desc, r["company"], r["title"], r["domain"], args.model, cache): r
                for r in rows
            }
            done = 0
            for f in as_completed(futs):
                r = futs[f]
                try:
                    results[r["id"]] = f.result()
                except Exception as e:
                    results[r["id"]] = {"fit": True, "confidence": 0.0, "reason": f"err:{str(e)[:30]}"}
                done += 1
                if done % 200 == 0:
                    save_cache(cache)
        save_cache(cache)
        total_checked += len(rows)

        fails = [
            (r, results[r["id"]])
            for r in rows
            if not results[r["id"]]["fit"] and results[r["id"]]["confidence"] >= args.min_confidence
        ]
        print(f"  fails: {len(fails)} / {len(rows)}")
        for r, res in fails[:10]:
            print(f"    [{res['confidence']:.2f}] {(r['company'] or '')[:40]:<40} {(r['title'] or '')[:20]:<20} {r['status']:<6}  ({res['reason']})")
        if len(fails) > 10:
            print(f"    ... and {len(fails) - 10} more")
        for r, res in fails:
            all_fails.append((client, niche, r, res))

    print()
    print(f"=== SUMMARY ===")
    print(f"leads checked: {total_checked}")
    print(f"fit=false at >={args.min_confidence} confidence: {len(all_fails)}")

    # Group by status
    from collections import Counter
    by_status = Counter()
    by_client_niche_status = Counter()
    for client, niche, r, res in all_fails:
        by_status[r["status"]] += 1
        by_client_niche_status[(client, niche, r["status"])] += 1
    print()
    print("by status:")
    for s, n in by_status.most_common():
        print(f"  {s:<10}{n}")
    print()
    print("by client/niche/status:")
    for (c, n, s), ct in sorted(by_client_niche_status.items(), key=lambda x: -x[1])[:20]:
        print(f"  {c:<18}{n:<28}{s:<8}{ct}")

    # If any fails are status='sent', warn user about live campaign impact
    sent_fails = [x for x in all_fails if x[2]["status"] == "sent"]
    if sent_fails:
        print()
        print(f"!!! {len(sent_fails)} of these are already SENT (in live Smartlead campaigns) !!!")
        print("  Review the 'notes' field to find which campaigns. DB commit will mark them")
        print("  excluded but WILL NOT delete from Smartlead. Run a separate cleanup for that.")

    if not all_fails:
        print("\nnothing to exclude")
        return

    if not args.commit:
        print(f"\nDRY RUN — no DB changes. Re-run with --commit to apply.")
        return

    # Backup
    bak = f"{DB_PATH}.bak_fit_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    shutil.copy2(DB_PATH, bak)
    print(f"\nbackup: {bak}")

    conn2 = sqlite3.connect(DB_PATH)
    cur2 = conn2.cursor()
    ids = [r["id"] for _, _, r, _ in all_fails]
    cur2.executemany(
        "UPDATE leads SET status='excluded_off_target', date_updated=datetime('now') WHERE id=?",
        [(i,) for i in ids],
    )
    conn2.commit()
    print(f"applied {cur2.rowcount} exclusions")
    conn2.close()


if __name__ == "__main__":
    main()
