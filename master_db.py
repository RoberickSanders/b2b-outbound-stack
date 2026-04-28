#!/usr/bin/env python3
"""
Master Lead Database — Centralized lead tracking across all clients and sources.

Stores every lead we've ever acquired in a SQLite database so we can:
- Check before every run if we already have a lead (don't re-enrich)
- Query by client, niche, source, date, tier
- Track sent/bounced/replied status over time
- Dedupe across all sources (Blitz, AI Ark, ScraperCity, bought lists, etc.)

Database: master-leads/master_leads.db
Per-client folders in 01-Projects/{client}/lead-runs/ remain unchanged.

Usage:
    python3 master_db.py init                              # Create DB + schema
    python3 master_db.py scan                              # Scan all lead folders + ingest
    python3 master_db.py scan --client client_c    # Scan one client
    python3 master_db.py stats                             # Show database stats
    python3 master_db.py query --niche "cost segregation"  # Query
    python3 master_db.py check emails.csv                  # Check which emails we already have
    python3 master_db.py add contacts.csv --source blitz --niche msps --client client_b
    python3 master_db.py export --niche "fire protection" --out fire.csv
"""

import os
import csv
import sqlite3
import argparse
import glob
import sys
from datetime import datetime, timezone

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
WORKSPACE_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, "..", ".."))
PROJECTS_DIR = os.path.join(WORKSPACE_ROOT, "01-Projects")
MASTER_DIR = os.path.join(SCRIPT_DIR, "master-leads")
DB_PATH = os.path.join(MASTER_DIR, "master_leads.db")


# ==============================================================================
# SCHEMA
# ==============================================================================

SCHEMA = """
CREATE TABLE IF NOT EXISTS leads (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    email TEXT NOT NULL UNIQUE,
    first_name TEXT,
    last_name TEXT,
    title TEXT,
    company TEXT,
    domain TEXT,
    phone TEXT,
    linkedin_url TEXT,
    city TEXT,
    state TEXT,
    industry TEXT,
    source TEXT NOT NULL,           -- blitz | aiark | scrapercity | d7_bulk | manual
    niche TEXT,                     -- cost-seg | msps | fire-protection | etc
    client TEXT,                    -- client_c | client_b | client_a
    tier INTEGER,                   -- 1 | 2 | 3
    mv_result TEXT,
    bb_result TEXT,
    verified INTEGER DEFAULT 0,     -- 0 | 1
    status TEXT DEFAULT 'new',      -- new | queued | sent | bounced | replied | unsubscribed
    sent_date TEXT,
    date_added TEXT,
    date_updated TEXT,
    notes TEXT
);

CREATE INDEX IF NOT EXISTS idx_email ON leads(email);
CREATE INDEX IF NOT EXISTS idx_domain ON leads(domain);
CREATE INDEX IF NOT EXISTS idx_niche ON leads(niche);
CREATE INDEX IF NOT EXISTS idx_client ON leads(client);
CREATE INDEX IF NOT EXISTS idx_source ON leads(source);
CREATE INDEX IF NOT EXISTS idx_status ON leads(status);

CREATE TABLE IF NOT EXISTS ingestion_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    filepath TEXT NOT NULL,
    source TEXT,
    niche TEXT,
    client TEXT,
    rows_total INTEGER,
    rows_new INTEGER,
    rows_updated INTEGER,
    rows_skipped INTEGER,
    ingested_at TEXT
);
"""


# ==============================================================================
# DB HELPERS
# ==============================================================================

def get_conn():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    conn = get_conn()
    conn.executescript(SCHEMA)
    conn.commit()
    conn.close()
    print(f"✓ Database initialized at {DB_PATH}")


def _now():
    return datetime.now(timezone.utc).isoformat()


# ==============================================================================
# INGEST
# ==============================================================================

def _normalize_email(s):
    return (s or "").lower().strip()


def _detect_source_niche_client(filepath):
    """
    Guess source, niche, client from filepath.
    e.g., 01-Projects/client_c/lead-runs/top5-niches-blitz/utility-audit/smartlead_import.csv
    Also detects Desktop/ClientC/... and Desktop/ClientB/...
    """
    p = filepath.lower().replace("\\", "/")
    source = "unknown"
    niche = "unknown"
    client = "unknown"

    # Detect client from 01-Projects path
    if "01-projects/client_c" in p:
        client = "client_c"
    elif "01-projects/client_b" in p:
        client = "client_b"
    elif "01-projects/client_a" in p:
        client = "client_a"
    elif "01-projects/preaction-fire" in p:
        client = "client_a"
    # Detect client from Desktop folder path
    elif "/desktop/client_c/" in p or "/desktop/client_c" in p:
        client = "client_c"
    elif "/desktop/client_b/" in p:
        client = "client_b"
    elif "/desktop/preaction/" in p or "/desktop/client_a/" in p:
        client = "client_a"
    # Generic "leads & pipeline" goes to client_c (user's own bucket)
    elif "/desktop/leads & pipeline/" in p or "/desktop/leads_pipeline/" in p:
        client = "client_c"

    # Detect source
    if "blitz" in p:
        source = "blitz"
    elif "aiark" in p or "ai-ark" in p or "ai_ark" in p:
        source = "aiark"
    elif "scrapercity" in p:
        source = "scrapercity"
    elif "d7_bulk" in p or "d7 bulk" in p:
        source = "d7_bulk"
    elif "master-leads" in p:
        source = "bought_list"

    # Detect niche from folder names
    niche_keywords = {
        "cost-segregation": ["costseg", "cost seg", "cost_segregation"],
        "msps": ["msp", "managed service", "florida_msp"],
        "fire-protection": ["fire-protection", "fire_protection", "fire", "fireprotection"],
        "telecom-audit": ["telecom-audit", "telecom_audit"],
        "telecom-expense": ["telecom-expense", "telecom_expense"],
        "property-tax-appeal": ["property-tax", "property_tax"],
        "utility-audit": ["utility-audit", "utility_audit"],
        "rd-tax-credit": ["rd-tax", "r&d", "rd_tax"],
        "hotels": ["hotel", "ht_", "hotels_"],
        "property-management": ["property management", "pm_", "property_management"],
        "schools": ["school", "sc_companies", "sc_contacts", "sc_domains", "school_districts"],
        "assisted-living": ["assisted_living", "assisted-living", "al_"],
        "restaurants": ["restaurant", "fire_restaurant"],
        "apartment-complexes": ["apartment", "apartment_complex"],
        "warehouses": ["warehouse"],
        "hospitals": ["hospital"],
        "commercial-real-estate": ["commercial_real_estate", "commercial real estate"],
        "industrial-equipment": ["industrial_equipment", "industrial equip"],
        "hvac": ["hvac"],
        "ma-advisors": ["ma_advisors", "ma_campaign", "ma advisors"],
        "fintech": ["fintech"],
        "cybersecurity": ["cybersecurity", "cyber security", "vpc_cyber"],
        "siding-installation": ["siding"],
    }
    for n, keywords in niche_keywords.items():
        if any(k in p for k in keywords):
            niche = n
            break

    return source, niche, client


def _map_row(row, default_source, default_niche, default_client):
    """Map a CSV row (various schemas) to our canonical lead dict."""
    # Try to find email across common column names
    email = ""
    for key in ["email", "Email", "EMAIL", "sc_validated_email"]:
        if key in row and row[key]:
            email = _normalize_email(row[key])
            if email and "@" in email:
                break
    if not email or "@" not in email:
        return None

    def get(*keys):
        for k in keys:
            if k in row and row[k] is not None:
                v = str(row[k]).strip()
                if v and v.lower() not in ("none", "null", ""):
                    return v
        return ""

    return {
        "email": email,
        "first_name": get("first_name", "First Name", "firstName", "first"),
        "last_name": get("last_name", "Last Name", "lastName", "last"),
        "title": get("title", "Title", "position", "Position"),
        "company": get("company", "Company Name", "Company", "organization", "sc_company_name"),
        "domain": get("domain", "Domain", "Company Domain", "company_domain") or (email.split("@")[1] if "@" in email else ""),
        "phone": get("phone", "Phone", "Mobile Number", "Company Phone Number"),
        "linkedin_url": get("linkedin_url", "LinkedIn", "linkedin", "Person LinkedIn"),
        "city": get("city", "City", "sc_company_location"),
        "state": get("state", "State"),
        "industry": get("industry", "Industry", "sc_company_industry"),
        "source": get("source") or default_source,
        "niche": default_niche,
        "client": default_client,
        "tier": get("tier"),
        "mv_result": get("mv_result", "sc_email_status"),
        "bb_result": get("bb_result"),
        "verified": 1 if (get("verified").lower() == "true" or get("mv_result").lower() in ("valid", "ok") or get("sc_email_status").lower() == "valid") else 0,
    }


def ingest_file(filepath, source=None, niche=None, client=None, verbose=False):
    """Ingest a single CSV file into the master database."""
    if not os.path.exists(filepath):
        return {"error": "file not found"}

    # Auto-detect if not provided
    auto_source, auto_niche, auto_client = _detect_source_niche_client(filepath)
    source = source or auto_source
    niche = niche or auto_niche
    client = client or auto_client

    conn = get_conn()
    cursor = conn.cursor()

    total = 0
    new = 0
    updated = 0
    skipped = 0

    try:
        with open(filepath, "r", encoding="utf-8", errors="replace") as f:
            reader = csv.DictReader(f)
            for row in reader:
                total += 1
                mapped = _map_row(row, source, niche, client)
                if not mapped:
                    skipped += 1
                    continue

                # Check if exists
                existing = cursor.execute(
                    "SELECT id, source, niche, verified FROM leads WHERE email = ?",
                    (mapped["email"],)
                ).fetchone()

                if existing:
                    # Update if new info is better (verified > unverified)
                    if mapped["verified"] and not existing["verified"]:
                        cursor.execute("""
                            UPDATE leads SET
                                first_name = COALESCE(NULLIF(?, ''), first_name),
                                last_name = COALESCE(NULLIF(?, ''), last_name),
                                title = COALESCE(NULLIF(?, ''), title),
                                company = COALESCE(NULLIF(?, ''), company),
                                phone = COALESCE(NULLIF(?, ''), phone),
                                linkedin_url = COALESCE(NULLIF(?, ''), linkedin_url),
                                mv_result = ?,
                                bb_result = ?,
                                verified = 1,
                                date_updated = ?
                            WHERE id = ?
                        """, (
                            mapped["first_name"], mapped["last_name"], mapped["title"],
                            mapped["company"], mapped["phone"], mapped["linkedin_url"],
                            mapped["mv_result"], mapped["bb_result"],
                            _now(), existing["id"],
                        ))
                        updated += 1
                    else:
                        skipped += 1
                else:
                    cursor.execute("""
                        INSERT INTO leads (
                            email, first_name, last_name, title, company, domain,
                            phone, linkedin_url, city, state, industry,
                            source, niche, client, tier, mv_result, bb_result, verified,
                            status, date_added, date_updated
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'new', ?, ?)
                    """, (
                        mapped["email"], mapped["first_name"], mapped["last_name"],
                        mapped["title"], mapped["company"], mapped["domain"],
                        mapped["phone"], mapped["linkedin_url"], mapped["city"], mapped["state"],
                        mapped["industry"], mapped["source"], mapped["niche"], mapped["client"],
                        mapped["tier"], mapped["mv_result"], mapped["bb_result"], mapped["verified"],
                        _now(), _now(),
                    ))
                    new += 1

        # Log ingestion
        cursor.execute("""
            INSERT INTO ingestion_log (filepath, source, niche, client,
                                        rows_total, rows_new, rows_updated, rows_skipped, ingested_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (filepath, source, niche, client, total, new, updated, skipped, _now()))

        conn.commit()
    except Exception as e:
        conn.rollback()
        conn.close()
        return {"error": str(e)}

    conn.close()

    result = {
        "filepath": filepath,
        "source": source,
        "niche": niche,
        "client": client,
        "total": total,
        "new": new,
        "updated": updated,
        "skipped": skipped,
    }
    if verbose:
        print(f"  {os.path.basename(filepath)}: {new} new, {updated} updated, {skipped} skipped ({total} total)")
    return result


def scan_all(client=None, verbose=True):
    """
    Scan all known lead folders and ingest any CSVs found.
    Looks in:
      - 01-Projects/*/lead-runs/**/smartlead_import.csv (v2 pipeline outputs)
      - 01-Projects/*/lead-runs/**/contacts_final.csv (v6 pipeline outputs)
      - master-leads/**/*.csv (bought lists — optional)
    """
    init_db()

    files = []

    # v2 pipeline outputs
    pattern = os.path.join(PROJECTS_DIR, "*", "lead-runs", "**", "smartlead_import.csv")
    files.extend(glob.glob(pattern, recursive=True))

    # v2 merged variants
    pattern = os.path.join(PROJECTS_DIR, "*", "lead-runs", "**", "smartlead_merged.csv")
    files.extend(glob.glob(pattern, recursive=True))

    pattern = os.path.join(PROJECTS_DIR, "*", "lead-runs", "**", "smartlead_combined.csv")
    files.extend(glob.glob(pattern, recursive=True))

    # v6 pipeline outputs
    pattern = os.path.join(PROJECTS_DIR, "*", "lead-runs", "**", "contacts_final.csv")
    files.extend(glob.glob(pattern, recursive=True))

    pattern = os.path.join(PROJECTS_DIR, "*", "lead-runs", "**", "final_smartlead_VERIFIED.csv")
    files.extend(glob.glob(pattern, recursive=True))

    # Filter by client if specified
    if client:
        files = [f for f in files if f"/{client}/" in f]

    files = sorted(set(files))

    if not files:
        print("No lead files found.")
        return

    print(f"Found {len(files)} lead files to scan")
    print()

    totals = {"new": 0, "updated": 0, "skipped": 0, "total": 0}
    for fp in files:
        result = ingest_file(fp, verbose=verbose)
        if "error" in result:
            print(f"  ERROR {fp}: {result['error']}")
            continue
        for k in totals:
            totals[k] += result.get(k, 0)

    print()
    print(f"=== SCAN COMPLETE ===")
    print(f"  Files processed: {len(files)}")
    print(f"  Total rows:      {totals['total']:,}")
    print(f"  New leads:       {totals['new']:,}")
    print(f"  Updated leads:   {totals['updated']:,}")
    print(f"  Skipped (dupes): {totals['skipped']:,}")


# ==============================================================================
# QUERY / STATS
# ==============================================================================

def show_stats():
    conn = get_conn()
    cursor = conn.cursor()

    total = cursor.execute("SELECT COUNT(*) FROM leads").fetchone()[0]
    verified = cursor.execute("SELECT COUNT(*) FROM leads WHERE verified = 1").fetchone()[0]

    print(f"=== MASTER LEAD DATABASE ===")
    print(f"  Total leads:     {total:,}")
    print(f"  Verified:        {verified:,}")
    print()

    # By source
    print(f"  By source:")
    for row in cursor.execute("SELECT source, COUNT(*) as cnt FROM leads GROUP BY source ORDER BY cnt DESC"):
        print(f"    {row['source']:15s}  {row['cnt']:,}")
    print()

    # By client
    print(f"  By client:")
    for row in cursor.execute("SELECT client, COUNT(*) as cnt FROM leads GROUP BY client ORDER BY cnt DESC"):
        print(f"    {row['client']:20s}  {row['cnt']:,}")
    print()

    # By niche
    print(f"  By niche:")
    for row in cursor.execute("SELECT niche, COUNT(*) as cnt FROM leads GROUP BY niche ORDER BY cnt DESC"):
        print(f"    {row['niche']:25s}  {row['cnt']:,}")
    print()

    # By status
    print(f"  By status:")
    for row in cursor.execute("SELECT status, COUNT(*) as cnt FROM leads GROUP BY status ORDER BY cnt DESC"):
        print(f"    {row['status']:15s}  {row['cnt']:,}")

    conn.close()


def query_leads(niche=None, source=None, client=None, status=None, limit=100, out=None):
    conn = get_conn()
    cursor = conn.cursor()

    sql = "SELECT * FROM leads WHERE 1=1"
    params = []
    if niche:
        sql += " AND niche LIKE ?"
        params.append(f"%{niche}%")
    if source:
        sql += " AND source = ?"
        params.append(source)
    if client:
        sql += " AND client = ?"
        params.append(client)
    if status:
        sql += " AND status = ?"
        params.append(status)
    sql += f" LIMIT {int(limit)}"

    rows = cursor.execute(sql, params).fetchall()
    conn.close()

    if out:
        if rows:
            fields = rows[0].keys()
            with open(out, "w", newline="", encoding="utf-8") as f:
                w = csv.DictWriter(f, fieldnames=fields)
                w.writeheader()
                w.writerows([dict(r) for r in rows])
            print(f"Exported {len(rows)} leads to {out}")
        else:
            print("No matching leads found.")
    else:
        print(f"Found {len(rows)} leads")
        for r in rows[:20]:
            print(f"  {r['first_name']} {r['last_name']} | {r['title']} | {r['company']} | {r['email']}")
        if len(rows) > 20:
            print(f"  ... and {len(rows) - 20} more")


def check_existing(emails):
    """Given a list of emails, return which are already in the master DB."""
    conn = get_conn()
    cursor = conn.cursor()

    existing = set()
    for email in emails:
        normalized = _normalize_email(email)
        row = cursor.execute(
            "SELECT email FROM leads WHERE email = ?", (normalized,)
        ).fetchone()
        if row:
            existing.add(normalized)

    conn.close()
    return existing


# ==============================================================================
# PIPELINE INTEGRATION — skip known companies, pull from master
# ==============================================================================

def get_known_domains(verified_only=True, niche=None, client=None):
    """
    Return set of domains that already have contacts in the master DB.
    Used by v2 pipeline to skip re-enriching companies we already paid to enrich.
    """
    if not os.path.exists(DB_PATH):
        return set()
    conn = get_conn()
    cursor = conn.cursor()

    sql = "SELECT DISTINCT domain FROM leads WHERE domain IS NOT NULL AND domain != ''"
    params = []
    if verified_only:
        sql += " AND verified = 1"
    if niche:
        sql += " AND niche = ?"
        params.append(niche)
    if client:
        sql += " AND client = ?"
        params.append(client)

    rows = cursor.execute(sql, params).fetchall()
    conn.close()
    return {r["domain"].lower() for r in rows if r["domain"]}


def pull_verified_contacts(niche=None, client=None, domains=None, limit=None):
    """
    Pull existing verified contacts from the master DB for a campaign.
    Used to merge past inventory into a new campaign's output so we reuse assets
    we already paid for instead of re-enriching.
    """
    if not os.path.exists(DB_PATH):
        return []
    conn = get_conn()
    cursor = conn.cursor()

    sql = """
        SELECT email, first_name, last_name, title, company, domain,
               phone, linkedin_url, city, state, industry,
               source, niche, client, tier, mv_result, bb_result, verified, status
        FROM leads
        WHERE verified = 1 AND status = 'new' AND email IS NOT NULL
    """
    params = []
    if niche:
        sql += " AND niche = ?"
        params.append(niche)
    if client:
        sql += " AND client = ?"
        params.append(client)
    if domains:
        placeholders = ",".join("?" for _ in domains)
        sql += f" AND domain IN ({placeholders})"
        params.extend([d.lower() for d in domains])
    if limit:
        sql += f" LIMIT {int(limit)}"

    rows = cursor.execute(sql, params).fetchall()
    conn.close()
    return [dict(r) for r in rows]


# ==============================================================================
# CLI
# ==============================================================================

def main():
    parser = argparse.ArgumentParser(description="Master Lead Database")
    sub = parser.add_subparsers(dest="cmd", required=True)

    sub.add_parser("init", help="Initialize database")

    p_scan = sub.add_parser("scan", help="Scan lead folders and ingest")
    p_scan.add_argument("--client", help="Only scan one client")
    p_scan.add_argument("--quiet", action="store_true")

    sub.add_parser("stats", help="Show database stats")

    p_query = sub.add_parser("query", help="Query leads")
    p_query.add_argument("--niche")
    p_query.add_argument("--source")
    p_query.add_argument("--client")
    p_query.add_argument("--status")
    p_query.add_argument("--limit", type=int, default=100)
    p_query.add_argument("--out", help="Export to CSV")

    p_check = sub.add_parser("check", help="Check which emails already exist")
    p_check.add_argument("csv_file", help="CSV with 'email' column")

    p_add = sub.add_parser("add", help="Ingest a single file")
    p_add.add_argument("csv_file")
    p_add.add_argument("--source")
    p_add.add_argument("--niche")
    p_add.add_argument("--client")

    args = parser.parse_args()

    if args.cmd == "init":
        init_db()
    elif args.cmd == "scan":
        scan_all(client=args.client, verbose=not args.quiet)
    elif args.cmd == "stats":
        show_stats()
    elif args.cmd == "query":
        query_leads(niche=args.niche, source=args.source, client=args.client,
                    status=args.status, limit=args.limit, out=args.out)
    elif args.cmd == "check":
        with open(args.csv_file) as f:
            emails = [r.get("email", "") for r in csv.DictReader(f)]
        existing = check_existing(emails)
        new = set(_normalize_email(e) for e in emails if _normalize_email(e)) - existing
        print(f"Total: {len(emails)}")
        print(f"Already in master: {len(existing)}")
        print(f"New: {len(new)}")
    elif args.cmd == "add":
        init_db()
        result = ingest_file(args.csv_file, source=args.source, niche=args.niche,
                              client=args.client, verbose=True)
        print(result)


if __name__ == "__main__":
    main()
