"""Blitz backfill — enrich existing contacts that are missing names/titles."""
import csv
import sys
import time
import os

from blitz import blitz_enrich_company
from config import BLITZ_API_KEY

BASE = "~/agency-os/01-Projects/client_a/lead-runs"

# Target roles for fire protection clients (flat list — Blitz expects strings)
FIRE_ROLES = [
    "Owner", "Founder", "President",
    "General Manager", "Executive Director", "Director",
    "Facilities Manager", "Operations Manager", "Building Manager",
    "Administrator", "Office Manager", "Business Manager",
]

VERTICALS = ["restaurants", "hotels", "hospitals", "schools", "assisted_living", "churches", "apartment_complexes", "daycare_centers"]

def run():
    if not BLITZ_API_KEY:
        print("No BLITZ_API_KEY set")
        return

    total_enriched = 0
    total_tried = 0

    for v in VERTICALS:
        path = f"{BASE}/{v}/contacts_final.csv"
        if not os.path.exists(path):
            print(f"  {v}: no file, skipping")
            continue

        rows = list(csv.DictReader(open(path)))
        needs_enrichment = []
        complete = []

        for r in rows:
            if not r.get("name", "").strip() or not r.get("title", "").strip():
                needs_enrichment.append(r)
            else:
                complete.append(r)

        if not needs_enrichment:
            print(f"  {v}: all contacts complete, skipping")
            continue

        print(f"\n=== {v.upper()} === {len(needs_enrichment)} to enrich")

        # Group by domain to avoid duplicate Blitz calls
        by_domain = {}
        for r in needs_enrichment:
            d = r.get("domain", r.get("website", "")).strip()
            if d and d not in by_domain:
                by_domain[d] = r.get("company", r.get("company_name", ""))

        enriched = 0
        tried = 0
        for domain, company in by_domain.items():
            tried += 1
            try:
                contacts = blitz_enrich_company(company, domain, FIRE_ROLES)
                if contacts:
                    # Update matching rows with Blitz data
                    for r in needs_enrichment:
                        r_domain = r.get("domain", r.get("website", "")).strip()
                        if r_domain == domain and contacts:
                            best = contacts[0]
                            if best.get("name"):
                                r["name"] = best["name"]
                                parts = best["name"].split(" ", 1)
                                r["first_name"] = parts[0] if parts else ""
                                r["last_name"] = parts[1] if len(parts) > 1 else ""
                            if best.get("title"):
                                r["title"] = best["title"]
                            if best.get("email"):
                                r["email"] = best["email"]
                            enriched += 1
                            break  # One contact per domain
            except Exception as e:
                pass

            if tried % 25 == 0:
                print(f"    [{tried}/{len(by_domain)}] {enriched} enriched so far")
            time.sleep(0.5)  # Rate limit

        total_enriched += enriched
        total_tried += tried
        print(f"    Done: {enriched}/{len(by_domain)} domains enriched")

        # Write back — collect all possible fieldnames across all rows
        all_rows = complete + needs_enrichment
        all_fields = list(dict.fromkeys(
            field for row in all_rows for field in row.keys()
        ))
        with open(path, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=all_fields, extrasaction="ignore")
            w.writeheader()
            w.writerows(all_rows)

    print(f"\n=== TOTAL: {total_enriched} contacts enriched via Blitz out of {total_tried} tried ===")

if __name__ == "__main__":
    run()
