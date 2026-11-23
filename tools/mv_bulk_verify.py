#!/usr/bin/env python3
"""
mv_bulk_verify.py — Batch verify emails via MillionVerifier Bulk API.

Instead of verifying one email at a time (0.3s wait between each),
upload a CSV of ALL emails at once. MV processes them in parallel
on their servers. Same credits, 5-10x faster.

Usage:
    # Verify a list of emails
    python3 tools/mv_bulk_verify.py --input emails.csv --output verified.csv

    # Called by forge_enrich when batch mode is enabled
    python3 tools/mv_bulk_verify.py --input candidates.csv --wait

Flow:
  1. Write emails to temp CSV
  2. Upload to MV Bulk API (bulkapi.millionverifier.com)
  3. Poll for completion
  4. Download results
  5. Return verified emails
"""

import os
import sys
import csv
import json
import time
import argparse
import requests
import tempfile
from datetime import datetime

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

MV_KEY = os.environ.get("MILLIONVERIFIER_API_KEY", "")
BULK_BASE = "https://bulkapi.millionverifier.com"


def upload_emails(emails):
    """Upload a list of emails for bulk verification. Returns file_id."""
    if not MV_KEY or not emails:
        return None

    # Write to temp CSV
    tmp = tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False)
    for email in emails:
        tmp.write(f"{email}\n")
    tmp.close()

    try:
        with open(tmp.name, 'rb') as f:
            r = requests.post(
                f"{BULK_BASE}/bulkapi/v2/upload?key={MV_KEY}",
                files={'file_contents': ('emails.csv', f, 'text/plain')},
                timeout=60,
            )
        os.unlink(tmp.name)

        if r.status_code == 200:
            data = r.json()
            file_id = data.get('file_id')
            if file_id:
                print(f"  MV bulk: uploaded {len(emails)} emails (file_id={file_id})")
                return file_id
            else:
                print(f"  MV bulk upload response: {data}")
        else:
            print(f"  MV bulk upload failed: {r.status_code} {r.text[:200]}")
    except Exception as e:
        print(f"  MV bulk upload error: {e}")
        try:
            os.unlink(tmp.name)
        except:
            pass

    return None


def check_progress(file_id):
    """Check if bulk verification is complete. Returns status dict."""
    try:
        r = requests.get(
            f"{BULK_BASE}/bulkapi/v2/fileinfo?key={MV_KEY}&file_id={file_id}",
            timeout=15,
        )
        if r.status_code == 200:
            return r.json()
    except Exception:
        pass
    return None


def download_results(file_id):
    """Download verification results. Returns dict of email → result."""
    try:
        r = requests.get(
            f"{BULK_BASE}/bulkapi/v2/download?key={MV_KEY}&file_id={file_id}",
            timeout=60,
        )
        if r.status_code == 200:
            results = {}
            lines = r.text.strip().split('\n')
            for line in lines:
                parts = line.strip().split(',')
                if len(parts) >= 2:
                    email = parts[0].strip().strip('"')
                    result = parts[1].strip().strip('"').lower()
                    results[email.lower()] = result
            return results
    except Exception as e:
        print(f"  MV bulk download error: {e}")
    return {}


def bulk_verify(emails, max_wait=300):
    """Upload, wait, download. Returns dict of email → 'ok'|'invalid'|etc.

    Args:
        emails: list of email strings
        max_wait: max seconds to wait for processing (default 5 min)
    """
    if not emails or not MV_KEY:
        return {}

    # Upload
    file_id = upload_emails(emails)
    if not file_id:
        return {}

    # Poll for completion
    elapsed = 0
    poll_interval = 5
    while elapsed < max_wait:
        time.sleep(poll_interval)
        elapsed += poll_interval

        status = check_progress(file_id)
        if not status:
            continue

        percent = status.get('percent', 0)
        file_status = status.get('status', '')

        if percent == 100 or file_status == 'finished':
            print(f"  MV bulk: complete ({elapsed}s)")
            return download_results(file_id)

        if elapsed % 30 == 0:
            print(f"  MV bulk: {percent}% ({elapsed}s)", flush=True)

    print(f"  MV bulk: timeout after {max_wait}s")
    # Try to download whatever is done
    return download_results(file_id)


def filter_valid(results):
    """Filter bulk results to only valid emails."""
    valid = ('ok', 'valid', 'good')
    return {email: result for email, result in results.items() if result in valid}


def main():
    ap = argparse.ArgumentParser(description="Batch verify emails via MV Bulk API")
    ap.add_argument("--input", required=True, help="CSV with email column")
    ap.add_argument("--output", help="output CSV with results")
    ap.add_argument("--max-wait", type=int, default=300)
    args = ap.parse_args()

    # Load emails
    emails = []
    with open(args.input) as f:
        reader = csv.DictReader(f)
        for r in reader:
            email = r.get('email', '').strip()
            if email and '@' in email:
                emails.append(email)

    if not emails:
        print("no emails to verify")
        return

    print(f"verifying {len(emails)} emails via MV Bulk API")
    results = bulk_verify(emails, max_wait=args.max_wait)

    valid = filter_valid(results)
    print(f"\nresults: {len(results)} processed, {len(valid)} valid")

    if args.output:
        with open(args.output, 'w', newline='') as f:
            w = csv.writer(f)
            w.writerow(['email', 'result'])
            for email, result in sorted(results.items()):
                w.writerow([email, result])
        print(f"saved to: {args.output}")


if __name__ == "__main__":
    main()
