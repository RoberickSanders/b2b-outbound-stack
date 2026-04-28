#!/usr/bin/env python3
"""
signal_permits.py — Find new construction/renovation permits in Colorado.
New construction = guaranteed future fire protection contracts for CLIENT_A.

Sources:
- Denver Open Data Portal (building permits API)
- Serper search for other CO cities' permit databases

Usage:
    python3 tools/signal_permits.py                    # Denver permits, last 90 days
    python3 tools/signal_permits.py --days 30          # Last 30 days
    python3 tools/signal_permits.py --min-value 500000 # Only big projects ($500k+)
"""

import os
import re
import sys
import csv
import json
import time
import argparse
import requests
from datetime import datetime, timedelta

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.dirname(SCRIPT_DIR)

def _load_env_file(path):
    if not os.path.isfile(path):
        return
    try:
        for line in open(path):
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
    os.path.join(ROOT_DIR, ".env"),
):
    _load_env_file(_p)

SERPER_KEY = os.environ.get("SERPER_API_KEY", "")

# Denver Open Data - Building Permits
# https://data.denvergov.org/dataset/city-and-county-of-denver-building-permits
DENVER_PERMITS_API = "https://data.denvergov.org/resource/p2gw-iqyj.json"

# Permit types that signal fire protection need
FIRE_RELEVANT_TYPES = [
    "new construction", "new building", "tenant finish",
    "commercial alteration", "commercial remodel", "commercial addition",
    "mixed use", "multi-family", "hotel", "hospital", "school",
    "restaurant", "assembly", "high rise",
]


def fetch_denver_permits(days=90, min_value=100000):
    """Fetch recent Denver building permits from the open data API."""
    since = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%dT00:00:00.000")

    params = {
        "$where": f"issue_date > '{since}' AND total_project_valuation > {min_value}",
        "$order": "total_project_valuation DESC",
        "$limit": 500,
    }

    try:
        r = requests.get(DENVER_PERMITS_API, params=params, timeout=30)
        if r.status_code == 200:
            return r.json()
    except Exception as e:
        print(f"  Denver API error: {e}")

    return []


def search_co_permits_serper(cities=None, days=30):
    """Search for recent permit data in other CO cities via Serper."""
    if not SERPER_KEY:
        return []

    if not cities:
        cities = ["colorado springs", "aurora", "fort collins", "boulder", "pueblo"]

    results = []
    for city in cities:
        query = f'"{city}" colorado building permit new construction {datetime.now().year}'
        try:
            r = requests.post(
                "https://google.serper.dev/search",
                headers={"X-API-KEY": SERPER_KEY, "Content-Type": "application/json"},
                json={"q": query, "num": 10},
                timeout=15,
            )
            if r.status_code == 200:
                for item in r.json().get("organic", []):
                    results.append({
                        "city": city,
                        "title": item.get("title", ""),
                        "link": item.get("link", ""),
                        "snippet": (item.get("snippet", "") or "")[:300],
                    })
        except Exception:
            pass
        time.sleep(0.3)

    return results


def is_fire_relevant(permit):
    """Check if a permit likely needs fire protection services."""
    text = json.dumps(permit).lower()
    return any(ft in text for ft in FIRE_RELEVANT_TYPES)


def main():
    ap = argparse.ArgumentParser(description="Find CO building permits signaling fire protection need")
    ap.add_argument("--days", type=int, default=90, help="look back N days")
    ap.add_argument("--min-value", type=int, default=100000, help="min project valuation ($)")
    ap.add_argument("--output", help="output CSV path")
    args = ap.parse_args()

    if not args.output:
        args.output = os.path.join(SCRIPT_DIR, f"_signal_permits_{datetime.now().strftime('%Y%m%d')}.csv")

    print(f"=== Denver Building Permits (last {args.days} days, >${args.min_value:,}) ===")
    permits = fetch_denver_permits(days=args.days, min_value=args.min_value)
    print(f"  total permits: {len(permits)}")

    fire_relevant = [p for p in permits if is_fire_relevant(p)]
    print(f"  fire-relevant: {len(fire_relevant)}")

    # Extract key fields
    rows = []
    for p in fire_relevant:
        rows.append({
            "permit_number": p.get("permit_number", ""),
            "address": p.get("full_address") or p.get("address", ""),
            "project_name": p.get("project_name", ""),
            "work_description": (p.get("work_description", "") or "")[:200],
            "permit_type": p.get("permit_type_desc") or p.get("permit_type", ""),
            "valuation": p.get("total_project_valuation", ""),
            "issue_date": p.get("issue_date", "")[:10],
            "contractor": p.get("contractor_company_name", ""),
            "owner": p.get("owner_name") or p.get("building_owner", ""),
            "city": "Denver",
        })

    # Sort by valuation desc
    rows.sort(key=lambda x: float(x.get("valuation") or 0), reverse=True)

    # Write CSV
    if rows:
        fields = list(rows[0].keys())
        with open(args.output, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=fields)
            w.writeheader()
            w.writerows(rows)

    print(f"\n=== TOP FIRE-RELEVANT PERMITS ===")
    for r in rows[:15]:
        val = float(r.get("valuation") or 0)
        print(f"  ${val:>12,.0f}  {r['issue_date']}  {r['permit_type'][:25]:<25}  {r['address'][:40]}")
        if r.get("project_name"):
            print(f"              project: {r['project_name'][:60]}")
        if r.get("contractor"):
            print(f"              contractor: {r['contractor'][:40]}")

    # Also search other CO cities
    print(f"\n=== Searching other CO cities for permit databases ===")
    serper_results = search_co_permits_serper()
    print(f"  found {len(serper_results)} permit-related pages")
    for r in serper_results[:10]:
        print(f"  [{r['city']}] {r['title'][:60]}")
        print(f"    {r['link']}")

    print(f"\n  output: {args.output}")
    print(f"  fire-relevant permits: {len(rows)}")


if __name__ == "__main__":
    main()
