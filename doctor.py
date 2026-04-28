#!/usr/bin/env python3
"""
doctor.py — Read-only system health check for the lead pipeline.

Run before every campaign launch for the next 2 weeks. If it passes clean
14 days in a row, move to Phase 2.

READ-ONLY BY DESIGN:
    - Does NOT write to the DB
    - Does NOT delete cache
    - Does NOT call Smartlead DELETE
    - Only reports. If something is wrong, run the specific fix tool.

Usage:
    python3 doctor.py
    python3 doctor.py --fast   # skip Smartlead live checks
"""

import os
import sys
import csv
import json
import sqlite3
import argparse
from datetime import datetime, timezone

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(SCRIPT_DIR, "master-leads", "master_leads.db")
PROJECTS = os.path.abspath(os.path.join(SCRIPT_DIR, "..", "..", "01-Projects"))

GREEN = "\033[92m"
YELLOW = "\033[93m"
RED = "\033[91m"
RESET = "\033[0m"
BOLD = "\033[1m"


def ok(msg):    print(f"  {GREEN}✓{RESET} {msg}")
def warn(msg):  print(f"  {YELLOW}!{RESET} {msg}")
def fail(msg):  print(f"  {RED}✗{RESET} {msg}")
def header(msg):print(f"\n{BOLD}{msg}{RESET}")


def check_db_connectivity():
    header("[1] DATABASE")
    if not os.path.exists(DB_PATH):
        fail(f"DB not found at {DB_PATH}")
        return None
    try:
        c = sqlite3.connect(DB_PATH)
        cur = c.cursor()
        cur.execute("SELECT COUNT(*) FROM leads")
        n = cur.fetchone()[0]
        ok(f"master_leads.db exists ({n:,} leads)")
        cur.execute("PRAGMA journal_mode")
        jm = cur.fetchone()[0]
        if jm == "wal":
            ok(f"journal_mode = wal")
        else:
            warn(f"journal_mode = {jm} (expected wal)")
        return c
    except Exception as e:
        fail(f"DB error: {e}")
        return None


def check_schema(c):
    header("[2] SCHEMA")
    cur = c.cursor()
    cur.execute("PRAGMA table_info(leads)")
    cols = {r[1] for r in cur.fetchall()}
    required = {"id","email","first_name","last_name","title","company","domain",
                "phone","city","state","industry","source","niche","client","tier",
                "mv_result","bb_result","verified","status","sent_date","date_added",
                "date_updated","notes","city_source"}
    missing = required - cols
    if missing:
        fail(f"missing columns: {missing}")
    else:
        ok(f"all {len(required)} required columns present")


def check_integrity_rules(c):
    header("[3] INTEGRITY RULES")
    cur = c.cursor()
    issues = 0

    # Rule: leads marked sent should have sent_date
    cur.execute("SELECT COUNT(*) FROM leads WHERE status='sent' AND (sent_date IS NULL OR sent_date='')")
    n = cur.fetchone()[0]
    if n == 0:
        ok("all sent leads have sent_date")
    else:
        warn(f"{n} sent leads missing sent_date")
        issues += 1

    # Rule: leads in 'new' should not have sent_date
    cur.execute("SELECT COUNT(*) FROM leads WHERE status='new' AND sent_date IS NOT NULL AND sent_date!=''")
    n = cur.fetchone()[0]
    if n == 0:
        ok("no 'new' leads carry a sent_date")
    else:
        warn(f"{n} new leads have a stray sent_date")
        issues += 1

    # Rule: verified=1 should have mv_result
    cur.execute("SELECT COUNT(*) FROM leads WHERE verified=1 AND (mv_result IS NULL OR mv_result='')")
    n = cur.fetchone()[0]
    if n == 0:
        ok("all verified=1 leads have mv_result")
    else:
        fail(f"{n} verified=1 leads have NO mv_result (trust gap)")
        issues += 1

    # Rule: emails should be unique (case-insensitive)
    cur.execute("""SELECT LOWER(email), COUNT(*) FROM leads
                   GROUP BY LOWER(email) HAVING COUNT(*)>1 LIMIT 5""")
    dups = cur.fetchall()
    if not dups:
        ok("no duplicate emails in DB")
    else:
        fail(f"{len(dups)}+ duplicate emails (sample: {dups[0][0]})")
        issues += 1

    # Rule: city present implies city_source present
    cur.execute("SELECT COUNT(*) FROM leads WHERE city IS NOT NULL AND city!='' AND (city_source IS NULL OR city_source='')")
    n = cur.fetchone()[0]
    if n == 0:
        ok("all rows with city have city_source")
    else:
        warn(f"{n} rows have city but no city_source")
        issues += 1

    return issues


def check_master_csvs_vs_db(c):
    header("[4] CSV vs DB DRIFT (_master/ folders)")
    cur = c.cursor()
    if not os.path.isdir(PROJECTS):
        warn(f"01-Projects not found at {PROJECTS}")
        return

    drift_total = 0
    files_checked = 0
    for client in os.listdir(PROJECTS):
        mdir = os.path.join(PROJECTS, client, "lead-runs", "_master")
        if not os.path.isdir(mdir):
            continue
        for niche in os.listdir(mdir):
            csv_path = os.path.join(mdir, niche, "smartlead_import.csv")
            if not os.path.isfile(csv_path):
                continue
            files_checked += 1
            emails = []
            with open(csv_path) as f:
                for r in csv.DictReader(f):
                    if r.get("email"):
                        emails.append(r["email"].lower().strip())
            if not emails:
                continue
            qs = ",".join("?" * len(emails))
            cur.execute(f"SELECT COUNT(*) FROM leads WHERE LOWER(email) IN ({qs}) AND status IN ('sent','bounced','excluded_unverified','excluded_competitor','excluded_off_target')", emails)
            dirty = cur.fetchone()[0]
            if dirty > 0:
                warn(f"{client}/{niche}: {dirty}/{len(emails)} leads in CSV are already sent/excluded in DB")
                drift_total += dirty
    if files_checked == 0:
        warn("no _master csvs found")
    elif drift_total == 0:
        ok(f"checked {files_checked} _master csvs, zero drift")
    else:
        fail(f"total drift: {drift_total} leads across {files_checked} csvs")


def check_verification_coverage(c):
    header("[5] VERIFICATION COVERAGE")
    cur = c.cursor()
    # Verified = has any mv_result (including 'legacy_trusted' from phase-1 migration)
    # OR has verified=1 (legacy flag that was honored before the migration)
    cur.execute("""SELECT COUNT(*) FROM leads WHERE status='new'
                   AND (mv_result IS NULL OR mv_result='')
                   AND verified != 1""")
    unver = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM leads WHERE status='new'")
    total = cur.fetchone()[0]
    if total == 0:
        warn("no 'new' leads")
        return
    pct = 100 * (total - unver) / total
    if unver == 0:
        ok(f"100% of {total} new leads have mv_result")
    elif pct >= 95:
        warn(f"{pct:.1f}% verified ({unver} unverified out of {total})")
    else:
        fail(f"only {pct:.1f}% verified — re-run verification before launching")


def check_backup_age():
    header("[6] BACKUP")
    bdir = os.path.dirname(DB_PATH)
    baks = sorted([f for f in os.listdir(bdir) if f.startswith("master_leads.db.bak")],
                   key=lambda f: os.path.getmtime(os.path.join(bdir, f)))
    if not baks:
        fail("no backup files found. Run: sqlite3 master_leads.db \".backup master_leads.db.bak_$(date +%Y%m%d_%H%M%S)\"")
        return
    newest = baks[-1]
    path = os.path.join(bdir, newest)
    age_hrs = (datetime.now().timestamp() - os.path.getmtime(path)) / 3600
    if age_hrs < 24:
        ok(f"latest backup: {newest} ({age_hrs:.1f}h ago)")
    elif age_hrs < 168:
        warn(f"latest backup {age_hrs:.0f}h old — consider re-backing up before bulk changes")
    else:
        fail(f"latest backup {age_hrs/24:.1f} days old")


def check_smartlead_sync(fast):
    header("[7] SMARTLEAD SYNC STATUS")
    if fast:
        warn("skipped (--fast)")
        return
    try:
        import requests
        from dotenv import load_dotenv
        load_dotenv(os.path.join(SCRIPT_DIR, ".env"))
        load_dotenv(os.path.join(SCRIPT_DIR, "..", "..", ".env"))
        key = os.environ.get("SMARTLEAD_API_KEY", "")
        if not key:
            warn("SMARTLEAD_API_KEY not set")
            return
        r = requests.get(f"https://server.smartlead.ai/api/v1/campaigns/?api_key={key}", timeout=15)
        camps = r.json()
        active = [c for c in camps if c.get("status") == "ACTIVE"]
        ok(f"{len(camps)} total campaigns, {len(active)} active")
    except Exception as e:
        warn(f"could not reach Smartlead: {e}")


def main():
    ap = argparse.ArgumentParser(description="Lead pipeline health check (read-only)")
    ap.add_argument("--fast", action="store_true", help="skip live Smartlead API checks")
    args = ap.parse_args()

    print(f"{BOLD}Lead Pipeline Doctor — READ-ONLY{RESET}")
    print(f"DB: {DB_PATH}")

    c = check_db_connectivity()
    if not c:
        sys.exit(2)

    check_schema(c)
    issues = check_integrity_rules(c)
    check_master_csvs_vs_db(c)
    check_verification_coverage(c)
    check_backup_age()
    check_smartlead_sync(args.fast)

    print()
    if issues == 0:
        print(f"{GREEN}{BOLD}All integrity rules passed.{RESET}")
        sys.exit(0)
    else:
        print(f"{YELLOW}{BOLD}{issues} integrity warning(s). Review above.{RESET}")
        sys.exit(1)


if __name__ == "__main__":
    main()
