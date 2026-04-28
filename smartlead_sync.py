#!/usr/bin/env python3
"""
Smartlead Sync — Pull all campaigns + leads from Smartlead and ingest into master DB.

Usage:
    python3 smartlead_sync.py              # Pull all campaigns, ingest into master
    python3 smartlead_sync.py --list       # List campaigns without syncing
    python3 smartlead_sync.py --dry-run    # Show what would sync without writing
    python3 smartlead_sync.py --campaign 3094505  # Sync one campaign

Updates status on existing leads:
- status → sent / unsubscribed / bounced based on Smartlead data
- sent_date → campaign created_at or earliest send date
- notes → campaign name
- New leads not in master get added with source='smartlead'
"""

import os
import sys
import time
import argparse
import requests
from datetime import datetime, timezone

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SCRIPT_DIR)

# Load .env
try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(SCRIPT_DIR, "..", "..", ".env"))
    load_dotenv(os.path.join(SCRIPT_DIR, ".env"))
except ImportError:
    pass

API_KEY = os.environ.get("SMARTLEAD_API_KEY", "")
BASE_URL = "https://server.smartlead.ai/api/v1"

from master_db import get_conn, init_db, _now


# ==============================================================================
# API HELPERS
# ==============================================================================

def _get(endpoint, **params):
    """GET request with API key."""
    url = f"{BASE_URL}{endpoint}"
    params["api_key"] = API_KEY
    try:
        r = requests.get(url, params=params, timeout=30)
        if r.status_code == 200:
            return r.json()
        if r.status_code == 429:
            time.sleep(2)
            r = requests.get(url, params=params, timeout=30)
            return r.json() if r.status_code == 200 else None
        print(f"  ⚠️  API error: {r.status_code} - {r.text[:200]}")
        return None
    except Exception as e:
        print(f"  ⚠️  Request failed: {e}")
        return None


def list_campaigns():
    """Fetch all Smartlead campaigns."""
    data = _get("/campaigns/")
    if isinstance(data, list):
        return data
    if isinstance(data, dict) and "data" in data:
        return data["data"]
    return []


def fetch_campaign_leads(campaign_id, verbose=False):
    """Fetch ALL leads for a campaign with pagination."""
    all_leads = []
    offset = 0
    limit = 100

    while True:
        data = _get(f"/campaigns/{campaign_id}/leads", offset=offset, limit=limit)
        if not data:
            break

        # Smartlead returns {total_leads, data, offset, limit}
        if isinstance(data, dict):
            leads = data.get("data", [])
            total = data.get("total_leads", 0)
        elif isinstance(data, list):
            leads = data
            total = len(leads)
        else:
            break

        if not leads:
            break

        all_leads.extend(leads)

        if verbose and len(all_leads) % 500 == 0:
            print(f"    Fetched {len(all_leads)}/{total}...")

        if len(leads) < limit:
            break

        offset += limit
        time.sleep(0.2)  # rate limit respect

    return all_leads


# ==============================================================================
# INGEST INTO MASTER DB
# ==============================================================================

STATUS_MAP = {
    "STARTED": "sent",
    "COMPLETED": "sent",
    "BLOCKED": "bounced",
    "PAUSED": "queued",
    "STOPPED": "queued",
    "IN_PROGRESS": "sent",
}


def _map_smartlead_lead(entry, campaign_name, campaign_id):
    """Map Smartlead lead entry to master DB record."""
    lead = entry.get("lead", {})
    email = (lead.get("email") or "").lower().strip()
    if not email or "@" not in email:
        return None

    raw_status = (entry.get("status") or "").upper()
    status = STATUS_MAP.get(raw_status, "sent")

    if lead.get("is_unsubscribed"):
        status = "unsubscribed"

    # Extract name
    fname = (lead.get("first_name") or "").strip()
    lname = (lead.get("last_name") or "").strip()
    company = (lead.get("company_name") or "").strip()
    phone = (lead.get("phone_number") or "").strip()
    linkedin = (lead.get("linkedin_profile") or "").strip()

    # Extract domain from email
    domain = email.split("@")[1] if "@" in email else ""

    # Parse sent date
    sent_date = entry.get("created_at") or ""

    return {
        "email": email,
        "first_name": fname,
        "last_name": lname,
        "company": company,
        "domain": domain,
        "phone": phone,
        "linkedin_url": linkedin,
        "status": status,
        "sent_date": sent_date,
        "notes": f"Smartlead: {campaign_name} (id={campaign_id})",
    }


def sync_campaign_leads(campaign, dry_run=False, verbose=True):
    """Sync all leads from one campaign into master DB."""
    campaign_id = campaign.get("id")
    campaign_name = campaign.get("name", "")

    if verbose:
        print(f"  → {campaign_name} (id={campaign_id})")

    leads = fetch_campaign_leads(campaign_id, verbose=False)
    if not leads:
        print(f"    No leads found")
        return {"total": 0, "updated": 0, "added": 0, "errors": 0}

    stats = {"total": len(leads), "updated": 0, "added": 0, "errors": 0}

    if dry_run:
        print(f"    {len(leads)} leads (dry-run, not writing)")
        return stats

    conn = get_conn()
    cursor = conn.cursor()

    for entry in leads:
        try:
            mapped = _map_smartlead_lead(entry, campaign_name, campaign_id)
            if not mapped:
                continue

            # Check if exists
            existing = cursor.execute(
                "SELECT id, status FROM leads WHERE email = ?",
                (mapped["email"],)
            ).fetchone()

            if existing:
                # Update status, sent_date, notes (always — Smartlead is source of truth for these)
                # sent_date is WRITE-ONCE: only set if currently empty. Preserves first-send timestamp.
                cursor.execute("""
                    UPDATE leads SET
                        status = ?,
                        sent_date = COALESCE(NULLIF(sent_date, ''), NULLIF(?, '')),
                        notes = CASE
                            WHEN notes IS NULL OR notes = '' THEN ?
                            WHEN notes LIKE '%' || ? || '%' THEN notes
                            ELSE notes || ' | ' || ?
                        END,
                        first_name = COALESCE(NULLIF(?, ''), first_name),
                        last_name = COALESCE(NULLIF(?, ''), last_name),
                        company = COALESCE(NULLIF(?, ''), company),
                        phone = COALESCE(NULLIF(?, ''), phone),
                        linkedin_url = COALESCE(NULLIF(?, ''), linkedin_url),
                        date_updated = ?
                    WHERE id = ?
                """, (
                    mapped["status"], mapped["sent_date"], mapped["notes"],
                    mapped["notes"], mapped["notes"],
                    mapped["first_name"], mapped["last_name"], mapped["company"],
                    mapped["phone"], mapped["linkedin_url"],
                    _now(), existing["id"],
                ))
                stats["updated"] += 1
            else:
                # Insert new
                cursor.execute("""
                    INSERT INTO leads (
                        email, first_name, last_name, company, domain,
                        phone, linkedin_url, source, status, sent_date,
                        notes, date_added, date_updated
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, 'smartlead', ?, ?, ?, ?, ?)
                """, (
                    mapped["email"], mapped["first_name"], mapped["last_name"],
                    mapped["company"], mapped["domain"], mapped["phone"],
                    mapped["linkedin_url"], mapped["status"], mapped["sent_date"],
                    mapped["notes"], _now(), _now(),
                ))
                stats["added"] += 1
        except Exception as e:
            stats["errors"] += 1
            if verbose:
                print(f"    ⚠️  Error on {mapped.get('email', '?') if mapped else '?'}: {e}")

    conn.commit()
    conn.close()

    if verbose:
        print(f"    {stats['total']} leads: {stats['updated']} updated, {stats['added']} added")

    return stats


def sync_all(dry_run=False):
    """Fetch all campaigns and sync each."""
    init_db()

    print(f"Fetching campaign list from Smartlead...")
    campaigns = list_campaigns()

    if not campaigns:
        print("No campaigns found or API error.")
        return

    print(f"Found {len(campaigns)} campaigns")
    print()

    totals = {"total": 0, "updated": 0, "added": 0, "errors": 0}
    for c in campaigns:
        stats = sync_campaign_leads(c, dry_run=dry_run)
        for k in totals:
            totals[k] += stats.get(k, 0)

    print()
    print("=" * 60)
    print("SMARTLEAD SYNC COMPLETE")
    print("=" * 60)
    print(f"  Campaigns synced: {len(campaigns)}")
    print(f"  Total leads:      {totals['total']:,}")
    print(f"  Updated (marked sent/bounced): {totals['updated']:,}")
    print(f"  New (added to master):         {totals['added']:,}")
    if totals["errors"]:
        print(f"  Errors:           {totals['errors']}")


def show_campaigns():
    """List campaigns without syncing."""
    campaigns = list_campaigns()
    if not campaigns:
        print("No campaigns found.")
        return
    print(f"{len(campaigns)} campaigns:")
    for c in campaigns:
        print(f"  {c.get('id'):12s} | {c.get('status',''):10s} | {c.get('name','')}")


# ==============================================================================
# CLI
# ==============================================================================

def main():
    parser = argparse.ArgumentParser(description="Sync Smartlead campaigns to master DB")
    parser.add_argument("--list", action="store_true", help="List campaigns without syncing")
    parser.add_argument("--dry-run", action="store_true", help="Count without writing to DB")
    parser.add_argument("--campaign", type=int, help="Sync one campaign by ID")
    args = parser.parse_args()

    if not API_KEY:
        print("ERROR: SMARTLEAD_API_KEY not set in .env")
        sys.exit(1)

    if args.list:
        show_campaigns()
    elif args.campaign:
        init_db()
        campaigns = list_campaigns()
        match = next((c for c in campaigns if c.get("id") == args.campaign), None)
        if not match:
            print(f"Campaign {args.campaign} not found")
            sys.exit(1)
        sync_campaign_leads(match, dry_run=args.dry_run)
    else:
        sync_all(dry_run=args.dry_run)


if __name__ == "__main__":
    main()
