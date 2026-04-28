#!/usr/bin/env python3.13
"""
rollback.py — Restore Smartlead state from backup files.

Why this exists:
  Every destructive operation in this codebase (lead deletion, sequence
  replacement, bulk status changes) writes a JSON backup BEFORE executing.
  Until 2026-04-20, those backups were untested — nobody knew if restore
  actually worked. This tool closes that gap with a tested restore path.

What it does:
  Reads backup files from 02-Areas/lead-pipeline/logs/ and POSTs the
  original state back to Smartlead. Supports two backup types:
    1. deleted_leads_{campaign}_{timestamp}.json — restores deleted leads
    2. sequences_backup_{campaign}_{timestamp}.json — restores sequence copy

  All restores default to --dry-run (preview only). Passing --execute
  actually writes back to Smartlead.

Usage:
  python3 tools/rollback.py --list                                # show all backups
  python3 tools/rollback.py --file logs/deleted_leads_X.json      # preview (dry-run)
  python3 tools/rollback.py --file logs/deleted_leads_X.json --execute
  python3 tools/rollback.py --campaign 3184163 --latest           # latest backup for campaign
  python3 tools/rollback.py --campaign 3184163 --latest --execute

Exit codes:
  0 — restore completed (or dry-run completed)
  1 — invalid / missing / corrupt backup
  2 — partial failure (some API calls failed)

Standalone tool — does not modify Forge code.
"""

import argparse
import glob
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

SCRIPT_DIR = Path(__file__).resolve().parent
LEAD_PIPELINE_DIR = SCRIPT_DIR.parent
WORKSPACE_ROOT = LEAD_PIPELINE_DIR.parent.parent
LOGS_DIR = LEAD_PIPELINE_DIR / "logs"
sys.path.insert(0, str(LEAD_PIPELINE_DIR))

try:
    from dotenv import load_dotenv
    load_dotenv(WORKSPACE_ROOT / ".env", override=True)
    load_dotenv(LEAD_PIPELINE_DIR / ".env", override=True)
except ImportError:
    pass

SMARTLEAD_KEY = os.environ.get("SMARTLEAD_API_KEY", "")
SMARTLEAD_BASE = "https://server.smartlead.ai/api/v1"


# ============================================================
# HTTP helpers with retry
# ============================================================

def sl_post(path: str, json_body: dict = None, params: dict = None, retries: int = 3, timeout: int = 30):
    merged_params = {"api_key": SMARTLEAD_KEY, **(params or {})}
    for attempt in range(retries + 1):
        try:
            r = requests.post(f"{SMARTLEAD_BASE}{path}", params=merged_params,
                              json=json_body or {}, timeout=timeout)
            if r.status_code == 200:
                return r
            if r.status_code == 429 or r.status_code >= 500:
                time.sleep(2 + attempt * 2); continue
            return r  # non-retryable error — return for caller to inspect
        except Exception:
            if attempt < retries:
                time.sleep(2 + attempt)
    return None


# ============================================================
# Backup discovery
# ============================================================

def list_backups() -> list:
    """Return all backup files sorted newest first."""
    patterns = [
        str(LOGS_DIR / "deleted_leads_*.json"),
        str(LOGS_DIR / "sequences_backup_*.json"),
    ]
    files = []
    for pat in patterns:
        files.extend(glob.glob(pat))
    files.sort(key=os.path.getctime, reverse=True)
    return files


def classify_backup(path: Path) -> str:
    """Return 'leads', 'sequences', or 'unknown' based on filename + contents."""
    name = path.name.lower()
    if name.startswith("deleted_leads_"):
        return "leads"
    if name.startswith("sequences_backup_"):
        return "sequences"
    # Try to infer from contents
    try:
        data = json.loads(path.read_text())
        if isinstance(data, dict) and "leads_deleted" in data:
            return "leads"
        if isinstance(data, list) and data and "seq_number" in data[0]:
            return "sequences"
    except Exception:
        pass
    return "unknown"


def find_latest_for_campaign(campaign_id: int, backup_type: str = None) -> Path:
    """Find the most recent backup for a given campaign_id.

    backup_type: 'leads', 'sequences', or None for any type.
    """
    all_files = list_backups()
    matches = []
    for f in all_files:
        p = Path(f)
        kind = classify_backup(p)
        if backup_type and kind != backup_type:
            continue
        # Check if this backup is for the given campaign
        try:
            data = json.loads(p.read_text())
        except Exception:
            continue
        cid_in_file = None
        if isinstance(data, dict):
            cid_in_file = data.get("campaign_id")
        elif isinstance(data, list) and data:
            cid_in_file = data[0].get("email_campaign_id")
        if cid_in_file == campaign_id:
            matches.append(p)
    return matches[0] if matches else None


# ============================================================
# Restore: deleted leads
# ============================================================

def restore_leads(backup_path: Path, dry_run: bool = True) -> dict:
    """Restore deleted leads from a backup file.

    Backup format (from lead-deletion scripts earlier in this project):
        {
          "campaign_id": 3184163,
          "timestamp": "2026-04-20T...",
          "leads_deleted": [
            {"id": 3614003302, "email": "a@b.com", "company_name": "X",
             "website": "b.com", "first_name": "...", "last_name": "..."},
            ...
          ]
        }
    """
    try:
        data = json.loads(backup_path.read_text())
    except Exception as e:
        return {"status": "error", "error": f"could not parse backup: {e}"}

    campaign_id = data.get("campaign_id")
    deleted_leads = data.get("leads_deleted", [])
    if not campaign_id or not deleted_leads:
        return {"status": "error", "error": "backup missing campaign_id or leads_deleted"}

    print(f"\n{'='*70}")
    print(f"RESTORE LEADS — campaign {campaign_id}")
    print(f"{'='*70}")
    print(f"  Backup file:  {backup_path.name}")
    print(f"  Original timestamp: {data.get('timestamp')}")
    print(f"  Leads to restore:   {len(deleted_leads)}")
    print(f"  Mode:               {'DRY RUN (preview only)' if dry_run else 'EXECUTE (writes to Smartlead)'}")

    # Build the lead_list payload expected by POST /campaigns/{id}/leads
    lead_list = []
    for lead in deleted_leads:
        entry = {
            "email": (lead.get("email") or "").strip().lower(),
            "first_name": (lead.get("first_name") or "").strip(),
            "last_name": (lead.get("last_name") or "").strip(),
            "company_name": (lead.get("company_name") or "").strip(),
            "phone_number": (lead.get("phone_number") or "").strip(),
        }
        if lead.get("website"):
            entry["custom_fields"] = {"website": lead["website"]}
        # Skip leads that would be rejected (no email)
        if entry["email"]:
            lead_list.append(entry)

    print(f"  Valid for restore:  {len(lead_list)} (rest have no email)")

    if dry_run:
        print(f"\n  DRY RUN — no API calls made.")
        print(f"  To execute, re-run with --execute")
        return {"status": "preview", "would_restore": len(lead_list)}

    # Actually restore
    print(f"\n  Restoring in batches of 100...")
    restored = 0
    failed = 0
    for i in range(0, len(lead_list), 100):
        batch = lead_list[i:i+100]
        resp = sl_post(
            f"/campaigns/{campaign_id}/leads",
            json_body={
                "lead_list": batch,
                "settings": {
                    "ignore_global_block_list": False,
                    "ignore_unsubscribe_list": False,
                    "ignore_duplicate_leads_in_other_campaign": False,
                }
            }
        )
        if resp and resp.status_code == 200:
            body = resp.json()
            batch_restored = body.get("upload_count", 0)
            batch_dupes = body.get("already_added_to_campaign", 0)
            restored += batch_restored
            print(f"    batch {i//100+1}: restored={batch_restored} dupes={batch_dupes}")
        else:
            status_code = resp.status_code if resp else "?"
            failed += len(batch)
            print(f"    batch {i//100+1}: FAIL status={status_code}")
        time.sleep(0.5)

    return {"status": "complete", "restored": restored, "failed": failed, "total": len(lead_list)}


# ============================================================
# Restore: sequences
# ============================================================

def restore_sequences(backup_path: Path, dry_run: bool = True, campaign_id: int = None) -> dict:
    """Restore campaign sequences from a backup file.

    Backup format: the raw Smartlead sequences list (from GET /campaigns/{id}/sequences).
    """
    try:
        data = json.loads(backup_path.read_text())
    except Exception as e:
        return {"status": "error", "error": f"could not parse backup: {e}"}

    # Sequences backup is just the list returned by GET /campaigns/{id}/sequences
    if not isinstance(data, list):
        return {"status": "error", "error": "backup is not a sequences list"}

    # Extract campaign_id from first sequence if not provided
    if not campaign_id and data:
        campaign_id = data[0].get("email_campaign_id")
    if not campaign_id:
        # Try to parse from filename like sequences_backup_fireprotection-apr13_20260420_191930.json
        return {"status": "error", "error": "cannot determine campaign_id — pass --campaign explicitly"}

    print(f"\n{'='*70}")
    print(f"RESTORE SEQUENCES — campaign {campaign_id}")
    print(f"{'='*70}")
    print(f"  Backup file:      {backup_path.name}")
    print(f"  Sequences:        {len(data)}")
    print(f"  Mode:             {'DRY RUN' if dry_run else 'EXECUTE'}")
    for s in data:
        subj = (s.get("subject") or "(body only)")[:50]
        delay_raw = s.get("seq_delay_details") or {}
        delay = delay_raw.get("delayInDays") or delay_raw.get("delay_in_days") or 0
        print(f"    Email {s.get('seq_number')}: '{subj}' delay={delay}d")

    # Reshape into what POST /campaigns/{id}/sequences expects.
    # Input shape (from GET): uses delayInDays (camelCase) in seq_delay_details
    # Required shape (for POST): uses delay_in_days (snake_case)
    # This is one of the Smartlead API quirks documented in SOP.md.
    payload_sequences = []
    for s in data:
        delay_raw = s.get("seq_delay_details") or {}
        delay_in_days = delay_raw.get("delay_in_days") or delay_raw.get("delayInDays") or 0
        payload_sequences.append({
            "seq_number": s.get("seq_number"),
            "seq_delay_details": {"delay_in_days": delay_in_days},
            "subject": s.get("subject", ""),
            "email_body": s.get("email_body", ""),
        })

    if dry_run:
        print(f"\n  DRY RUN — no API calls made.")
        print(f"  To execute, re-run with --execute")
        return {"status": "preview", "sequences_to_restore": len(payload_sequences)}

    # Execute
    print(f"\n  Replacing sequences on campaign {campaign_id}...")
    resp = sl_post(
        f"/campaigns/{campaign_id}/sequences",
        json_body={"sequences": payload_sequences},
    )
    if resp and resp.status_code == 200:
        print(f"    ✓ Sequences restored")
        return {"status": "complete", "restored_count": len(payload_sequences)}
    else:
        err = resp.text[:200] if resp else "no response"
        print(f"    ✗ FAIL: {err}")
        return {"status": "failed", "error": err}


# ============================================================
# CLI
# ============================================================

def main():
    ap = argparse.ArgumentParser(
        description="Restore Smartlead state from backup files. Default: dry-run (preview only).",
        epilog="Use --execute to actually write to Smartlead. Without it, all operations are preview-only.",
    )
    group = ap.add_mutually_exclusive_group()
    group.add_argument("--list", action="store_true", help="list all available backup files")
    group.add_argument("--file", help="path to a specific backup file to restore")
    group.add_argument("--campaign", type=int, help="restore the latest backup for this campaign ID")

    ap.add_argument("--type", choices=["leads", "sequences"], help="with --campaign, restrict to this backup type")
    ap.add_argument("--latest", action="store_true", help="with --campaign, use the latest backup (default)")
    ap.add_argument("--execute", action="store_true",
                    help="ACTUALLY write to Smartlead. Without this flag, operations are dry-run.")
    args = ap.parse_args()

    if args.list:
        files = list_backups()
        if not files:
            print("No backups found.")
            return
        print(f"\n{'='*90}")
        print(f"{'Type':<10} {'File':<70} {'Age':<8}")
        print(f"{'='*90}")
        now = datetime.now()
        for f in files:
            p = Path(f)
            kind = classify_backup(p)
            age_sec = now.timestamp() - p.stat().st_ctime
            if age_sec < 3600:
                age = f"{int(age_sec/60)}m"
            elif age_sec < 86400:
                age = f"{int(age_sec/3600)}h"
            else:
                age = f"{int(age_sec/86400)}d"
            print(f"{kind:<10} {p.name[:70]:<70} {age:<8}")
        return

    if not args.file and not args.campaign:
        ap.print_help()
        sys.exit(1)

    # Resolve backup file
    if args.file:
        backup_path = Path(args.file)
    else:
        backup_path = find_latest_for_campaign(args.campaign, args.type)
        if not backup_path:
            print(f"No backup found for campaign {args.campaign}" + (f" (type={args.type})" if args.type else ""))
            sys.exit(1)

    if not backup_path.exists():
        print(f"Backup file not found: {backup_path}")
        sys.exit(1)

    # Classify and dispatch
    kind = classify_backup(backup_path)
    print(f"Backup type detected: {kind}")

    dry_run = not args.execute
    if kind == "leads":
        result = restore_leads(backup_path, dry_run=dry_run)
    elif kind == "sequences":
        result = restore_sequences(backup_path, dry_run=dry_run, campaign_id=args.campaign)
    else:
        print(f"Unknown backup type for {backup_path.name}")
        sys.exit(1)

    print(f"\nResult: {result}")
    if result.get("status") == "error":
        sys.exit(1)
    if result.get("failed", 0) > 0:
        sys.exit(2)


if __name__ == "__main__":
    main()
