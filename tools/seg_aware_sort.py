#!/usr/bin/env python3
"""
seg_aware_sort.py — Reorder a Smartlead-bound lead CSV so non-SEG inboxes
send first and SEG-protected inboxes send last.

Why: Secure Email Gateways (Mimecast, Proofpoint, Barracuda, Cisco IronPort,
Forcepoint) flag senders harder than direct Google/Microsoft. Per Oliverify's
playbook on 2.27M emails, sending to non-SEG prospects FIRST builds your sender
reputation; sending to SEG-protected prospects LATER lets that reputation work
for you. Mixing them invites cascade-flag issues where one Mimecast bounce
torches deliverability across your whole Mimecast cohort.

Detection: MX lookup. Cached to disk to avoid re-querying. Misses default to
non-SEG so we never delay sending to a prospect we couldn't classify.

Usage:
    # Sort a lead CSV in place (writes <name>_segsorted.csv next to original)
    python3 tools/seg_aware_sort.py --input leads.csv

    # Replace the file in place
    python3 tools/seg_aware_sort.py --input leads.csv --in-place

    # Dry-run — show classification breakdown, don't write
    python3 tools/seg_aware_sort.py --input leads.csv --dry-run

    # Custom email column name (default 'email')
    python3 tools/seg_aware_sort.py --input leads.csv --email-col Email

Output:
    Same CSV. Reordered: non-SEG rows first (in their original relative order),
    then SEG rows. A new column `seg_provider` is appended showing the detected
    provider (one of: google, microsoft, mimecast, proofpoint, barracuda,
    cisco_ironport, forcepoint, other, unknown).

Caching:
    MX lookups cached to tools/_seg_mx_cache.json. Domain-keyed. Cache shared
    across runs. Delete the file to force fresh lookups.

Rate limit:
    Uses ~0.05s per uncached domain (system `dig` call). 1000 leads with 700
    unique domains = ~35s on a fresh cache. Subsequent runs are instant.
"""

import argparse
import csv
import json
import re
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
CACHE_PATH = SCRIPT_DIR / "_seg_mx_cache.json"


# ─── Provider patterns (matched against MX hostname, lowercased) ────────────
# Order matters for ambiguous matches (e.g. ironport before generic cisco).
SEG_PATTERNS = [
    ("mimecast",        re.compile(r"\bmimecast(?:cloud)?\.com$")),
    ("mimecast",        re.compile(r"\.mimecast\.")),
    ("proofpoint",      re.compile(r"\bpp(?:hosted|e-hosted|ops)\.\w+$")),
    ("proofpoint",      re.compile(r"\bproofpoint\.com$")),
    ("barracuda",       re.compile(r"\b(?:barracudanetworks|cudasvc|barracuda)\.com$")),
    ("cisco_ironport",  re.compile(r"\biphmx\.com$")),
    ("cisco_ironport",  re.compile(r"\bironport\.")),
    ("forcepoint",      re.compile(r"\bmailcontrol\.com$")),
    ("forcepoint",      re.compile(r"\bforcepoint\.")),
]

DIRECT_PATTERNS = [
    ("google",          re.compile(r"(?:^|\.)(?:aspmx\.l\.google\.com|googlemail\.com|google\.com)$")),
    ("google",          re.compile(r"\.aspmx\.l\.google\.com$")),
    ("microsoft",       re.compile(r"\.mail\.protection\.outlook\.com$")),
    ("microsoft",       re.compile(r"\.outlook\.com$")),
]

ALL_PATTERNS = SEG_PATTERNS + DIRECT_PATTERNS

SEG_PROVIDERS = {p for p, _ in SEG_PATTERNS}


# ─── MX lookup ──────────────────────────────────────────────────────────────
def _load_cache() -> dict:
    if CACHE_PATH.is_file():
        try:
            return json.loads(CACHE_PATH.read_text())
        except Exception:
            return {}
    return {}


def _save_cache(cache: dict) -> None:
    CACHE_PATH.write_text(json.dumps(cache, indent=2, sort_keys=True))


def _dig_mx(domain: str, timeout: float = 5.0) -> list[str]:
    """Return list of MX hostnames (lowercased, no trailing dot, no priority)."""
    try:
        result = subprocess.run(
            ["dig", "+short", "+time=3", "+tries=1", "MX", domain],
            capture_output=True, text=True, timeout=timeout, check=False,
        )
        out = (result.stdout or "").strip()
    except (subprocess.TimeoutExpired, FileNotFoundError):
        return []

    hostnames = []
    for line in out.splitlines():
        parts = line.strip().split()
        if len(parts) >= 2:
            host = parts[1].rstrip(".").lower()
            if host:
                hostnames.append(host)
    return hostnames


def classify_mx_hosts(hosts: list[str]) -> str:
    """Return provider label based on MX hostnames. SEG providers win over
    direct providers when both match (Mimecast in front of Google = SEG)."""
    if not hosts:
        return "unknown"
    matched_seg = None
    matched_direct = None
    for h in hosts:
        for label, pat in ALL_PATTERNS:
            if pat.search(h):
                if label in SEG_PROVIDERS:
                    matched_seg = matched_seg or label
                else:
                    matched_direct = matched_direct or label
                break
    return matched_seg or matched_direct or "other"


def lookup_provider(domain: str, cache: dict) -> str:
    domain = domain.lower().strip()
    if not domain:
        return "unknown"
    if domain in cache:
        return cache[domain]
    hosts = _dig_mx(domain)
    label = classify_mx_hosts(hosts)
    cache[domain] = label
    return label


# ─── CSV processing ─────────────────────────────────────────────────────────
def _domain_from_email(email: str) -> str:
    if not email or "@" not in email:
        return ""
    return email.rsplit("@", 1)[-1].strip().lower()


def sort_csv(input_path: Path, output_path: Path, email_col: str,
             dry_run: bool, workers: int) -> dict:
    rows = []
    with input_path.open("r", newline="") as f:
        reader = csv.DictReader(f)
        if email_col not in (reader.fieldnames or []):
            raise SystemExit(f"--email-col {email_col!r} not found. "
                             f"Available: {reader.fieldnames}")
        rows = list(reader)
    fieldnames = list(reader.fieldnames)

    # Unique domains
    domains = sorted({_domain_from_email(r.get(email_col, "")) for r in rows} - {""})
    print(f"  Rows:    {len(rows)}")
    print(f"  Domains: {len(domains)} unique")

    cache = _load_cache()
    pre_cached = sum(1 for d in domains if d in cache)
    print(f"  Cache:   {pre_cached}/{len(domains)} pre-cached")

    # Parallel MX lookups for cache misses
    misses = [d for d in domains if d not in cache]
    if misses:
        print(f"  Looking up {len(misses)} domains (workers={workers})...")
        t0 = time.time()
        with ThreadPoolExecutor(max_workers=workers) as ex:
            futs = {ex.submit(lookup_provider, d, cache): d for d in misses}
            done = 0
            for f in as_completed(futs):
                done += 1
                if done % 100 == 0 or done == len(misses):
                    print(f"    {done}/{len(misses)}  ({time.time()-t0:.1f}s)")
        _save_cache(cache)

    # Tag every row
    breakdown = {}
    for r in rows:
        d = _domain_from_email(r.get(email_col, ""))
        provider = cache.get(d, "unknown")
        r["seg_provider"] = provider
        breakdown[provider] = breakdown.get(provider, 0) + 1

    # Stable sort: non-SEG (and unknown) first, SEG last
    def _key(r):
        return (1 if r.get("seg_provider") in SEG_PROVIDERS else 0)

    rows.sort(key=_key)

    # Print breakdown
    print()
    print("  Provider breakdown (sorted high → low):")
    for p in sorted(breakdown, key=lambda k: -breakdown[k]):
        seg_flag = "(SEG)" if p in SEG_PROVIDERS else ""
        print(f"    {p:18s} {breakdown[p]:>6}   {seg_flag}")
    seg_count = sum(v for k, v in breakdown.items() if k in SEG_PROVIDERS)
    print(f"  Total SEG-protected:  {seg_count} ({100*seg_count/max(len(rows),1):.1f}%)")
    print(f"  Total direct/unknown: {len(rows) - seg_count}")

    if dry_run:
        print("\n  DRY RUN — not writing.")
        return {"rows": len(rows), "seg": seg_count, "breakdown": breakdown}

    if "seg_provider" not in fieldnames:
        fieldnames.append("seg_provider")

    with output_path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    print(f"\n  ✓ Wrote {output_path}  ({len(rows)} rows, non-SEG first, SEG last)")
    return {"rows": len(rows), "seg": seg_count, "breakdown": breakdown,
            "output": str(output_path)}


# ─── CLI ────────────────────────────────────────────────────────────────────
def main():
    ap = argparse.ArgumentParser(description="Sort a Smartlead lead CSV non-SEG-first.")
    ap.add_argument("--input", required=True, help="path to input CSV")
    ap.add_argument("--output", help="path to output CSV (default: <input>_segsorted.csv)")
    ap.add_argument("--in-place", action="store_true", help="overwrite input file")
    ap.add_argument("--email-col", default="email", help="column name holding email (default: email)")
    ap.add_argument("--workers", type=int, default=20, help="concurrent MX lookups (default: 20)")
    ap.add_argument("--dry-run", action="store_true", help="show breakdown without writing")
    args = ap.parse_args()

    input_path = Path(args.input).expanduser().resolve()
    if not input_path.is_file():
        sys.exit(f"input not found: {input_path}")

    if args.in_place:
        output_path = input_path
    elif args.output:
        output_path = Path(args.output).expanduser().resolve()
    else:
        output_path = input_path.with_name(input_path.stem + "_segsorted" + input_path.suffix)

    print(f"seg_aware_sort:  {input_path.name}")
    sort_csv(input_path, output_path, args.email_col, args.dry_run, args.workers)


if __name__ == "__main__":
    main()
