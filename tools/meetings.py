#!/usr/bin/env python3
"""
meetings.py — Track meetings, proposals, and closed deals.

Finally answers: "which niche/campaign actually makes money?"

Usage:
    # Log a meeting
    python3 tools/meetings.py log --email shannon@alliedfireprotection.com --client client_c --niche fire-protection --date 2026-04-15

    # Log a closed deal
    python3 tools/meetings.py close --email shannon@alliedfireprotection.com --value 2500 --monthly 2500

    # Update meeting status
    python3 tools/meetings.py update --email shannon@alliedfireprotection.com --outcome proposal_sent

    # View pipeline
    python3 tools/meetings.py pipeline
    python3 tools/meetings.py pipeline --client client_a

    # ROI report — THE WHOLE POINT
    python3 tools/meetings.py roi
    python3 tools/meetings.py roi --client client_c

    # List all meetings
    python3 tools/meetings.py list
    python3 tools/meetings.py list --niche fire-protection
"""

import os
import sys
import sqlite3
import argparse
from datetime import datetime

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.dirname(SCRIPT_DIR)
DB_PATH = os.path.join(ROOT_DIR, "master-leads", "master_leads.db")


def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def cmd_log(args):
    """Log a new meeting."""
    conn = get_conn()
    cur = conn.cursor()

    # Try to find lead in DB for auto-fill
    lead = cur.execute("SELECT * FROM leads WHERE LOWER(email)=?",
                       (args.email.lower(),)).fetchone()

    company = args.company or (lead["company"] if lead else "")
    contact = args.contact or (f"{lead['first_name'] or ''} {lead['last_name'] or ''}".strip() if lead else "")
    client = args.client or (lead["client"] if lead else "client_c")
    niche = args.niche or (lead["niche"] if lead else "")
    lead_id = lead["id"] if lead else None

    cur.execute("""INSERT INTO meetings
        (lead_id, email, company, contact_name, meeting_date, meeting_type,
         meeting_notes, client, niche, campaign_name, status, outcome)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
        (lead_id, args.email, company, contact,
         args.date or datetime.now().strftime("%Y-%m-%d"),
         args.type or "discovery", args.notes or "",
         client, niche, args.campaign or "", "scheduled", "pending"))
    conn.commit()

    print(f"✓ Meeting logged: {contact or args.email} @ {company}")
    print(f"  client={client} niche={niche} date={args.date or 'today'}")
    conn.close()


def cmd_update(args):
    """Update a meeting's status or outcome."""
    conn = get_conn()
    cur = conn.cursor()

    updates = []
    params = []
    if args.status:
        updates.append("status=?")
        params.append(args.status)
    if args.outcome:
        updates.append("outcome=?")
        params.append(args.outcome)
    if args.notes:
        updates.append("meeting_notes=?")
        params.append(args.notes)
    updates.append("date_updated=datetime('now')")

    params.append(args.email.lower())
    cur.execute(f"UPDATE meetings SET {','.join(updates)} WHERE LOWER(email)=?", params)
    conn.commit()
    print(f"✓ Updated {cur.rowcount} meeting(s) for {args.email}")
    conn.close()


def cmd_close(args):
    """Mark a meeting as closed-won with deal value."""
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""UPDATE meetings SET
        outcome='closed_won', deal_value=?, monthly_revenue=?,
        close_date=?, status='completed', date_updated=datetime('now')
        WHERE LOWER(email)=?""",
        (args.value, args.monthly or args.value,
         args.date or datetime.now().strftime("%Y-%m-%d"),
         args.email.lower()))
    conn.commit()
    print(f"✓ Deal closed: {args.email} @ ${args.value}")
    conn.close()


def cmd_lost(args):
    """Mark a meeting as closed-lost."""
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""UPDATE meetings SET
        outcome='closed_lost', status='completed',
        meeting_notes=COALESCE(meeting_notes,'') || ' | Lost: ' || ?,
        date_updated=datetime('now')
        WHERE LOWER(email)=?""",
        (args.reason or "no reason given", args.email.lower()))
    conn.commit()
    print(f"✓ Marked lost: {args.email}")
    conn.close()


def cmd_list(args):
    """List meetings."""
    conn = get_conn()
    cur = conn.cursor()

    sql = "SELECT * FROM meetings WHERE 1=1"
    params = []
    if args.client:
        sql += " AND client=?"
        params.append(args.client)
    if args.niche:
        sql += " AND niche=?"
        params.append(args.niche)
    if args.outcome:
        sql += " AND outcome=?"
        params.append(args.outcome)
    sql += " ORDER BY meeting_date DESC"

    rows = cur.execute(sql, params).fetchall()
    if not rows:
        print("No meetings found.")
        return

    print(f"\n{'Date':<12} {'Contact':<22} {'Company':<25} {'Niche':<20} {'Outcome':<15} {'Value':>8}")
    print("-" * 105)
    for r in rows:
        val = f"${r['deal_value']:,.0f}" if r['deal_value'] else ""
        print(f"{(r['meeting_date'] or '?'):<12} {(r['contact_name'] or r['email'] or '?'):<22} "
              f"{(r['company'] or '')[:24]:<25} {(r['niche'] or '')[:19]:<20} "
              f"{(r['outcome'] or 'pending'):<15} {val:>8}")
    conn.close()


def cmd_pipeline(args):
    """Show pipeline summary."""
    conn = get_conn()
    cur = conn.cursor()

    sql = "SELECT * FROM meetings WHERE 1=1"
    params = []
    if args.client:
        sql += " AND client=?"
        params.append(args.client)

    rows = cur.execute(sql, params).fetchall()
    if not rows:
        print("No meetings in pipeline.")
        return

    from collections import Counter
    by_outcome = Counter(r["outcome"] for r in rows)
    by_niche = Counter(r["niche"] for r in rows)
    by_client = Counter(r["client"] for r in rows)

    total_value = sum(r["deal_value"] or 0 for r in rows if r["outcome"] == "closed_won")
    total_monthly = sum(r["monthly_revenue"] or 0 for r in rows if r["outcome"] == "closed_won")

    print(f"\n{'='*50}")
    print(f"  PIPELINE SUMMARY")
    print(f"{'='*50}")
    print(f"  total meetings:    {len(rows)}")
    for outcome, n in by_outcome.most_common():
        print(f"    {outcome:<20} {n}")
    print(f"\n  by niche:")
    for niche, n in by_niche.most_common():
        print(f"    {niche:<20} {n}")
    print(f"\n  by client:")
    for client, n in by_client.most_common():
        print(f"    {client:<20} {n}")
    if total_value:
        print(f"\n  closed revenue:    ${total_value:,.0f}")
        print(f"  monthly recurring: ${total_monthly:,.0f}/mo")
    conn.close()


def cmd_roi(args):
    """THE WHOLE POINT — ROI per niche/campaign.
    Answers: which niche should I double down on?"""
    conn = get_conn()
    cur = conn.cursor()

    # Get lead counts per niche
    lead_sql = """SELECT client, niche, COUNT(*) as total,
                  SUM(CASE WHEN status='sent' THEN 1 ELSE 0 END) as sent
                  FROM leads GROUP BY client, niche"""
    lead_stats = {(r[0], r[1]): {"total": r[2], "sent": r[3]} for r in cur.execute(lead_sql).fetchall()}

    # Get meeting counts per niche
    meeting_sql = "SELECT client, niche, outcome, COUNT(*), SUM(COALESCE(deal_value,0)), SUM(COALESCE(monthly_revenue,0)) FROM meetings GROUP BY client, niche, outcome"
    meetings_raw = cur.execute(meeting_sql).fetchall()

    # Build per-niche stats
    niche_stats = {}
    for client, niche, outcome, count, value, monthly in meetings_raw:
        key = (client, niche)
        if key not in niche_stats:
            leads = lead_stats.get(key, {"total": 0, "sent": 0})
            niche_stats[key] = {"meetings": 0, "closed": 0, "value": 0, "monthly": 0,
                                "total_leads": leads["total"], "sent": leads["sent"]}
        niche_stats[key]["meetings"] += count
        if outcome == "closed_won":
            niche_stats[key]["closed"] += count
            niche_stats[key]["value"] += value or 0
            niche_stats[key]["monthly"] += monthly or 0

    if not niche_stats:
        print("No meeting data yet. Log meetings with: python3 tools/meetings.py log --email ...")
        conn.close()
        return

    print(f"\n{'='*90}")
    print(f"  ROI BY NICHE — Which niches actually make money?")
    print(f"{'='*90}")
    print(f"  {'Client':<18} {'Niche':<22} {'Sent':>6} {'Mtgs':>5} {'Close':>5} {'Rate':>6} {'Revenue':>10} {'MRR':>8}")
    print("-" * 90)

    for (client, niche), s in sorted(niche_stats.items(), key=lambda x: -x[1]["value"]):
        if args.client and client != args.client:
            continue
        mtg_rate = f"{100*s['meetings']/max(s['sent'],1):.1f}%" if s["sent"] else "—"
        close_rate = f"{100*s['closed']/max(s['meetings'],1):.0f}%" if s["meetings"] else "—"
        rev = f"${s['value']:,.0f}" if s["value"] else "—"
        mrr = f"${s['monthly']:,.0f}" if s["monthly"] else "—"
        print(f"  {client:<18} {(niche or '?'):<22} {s['sent'] or 0:>6} {s['meetings']:>5} "
              f"{s['closed']:>5} {close_rate:>6} {rev:>10} {mrr:>8}")

    # Bottom line
    total_rev = sum(s["value"] for s in niche_stats.values())
    total_mrr = sum(s["monthly"] for s in niche_stats.values())
    total_meetings = sum(s["meetings"] for s in niche_stats.values())
    total_closed = sum(s["closed"] for s in niche_stats.values())

    print(f"\n  TOTALS: {total_meetings} meetings → {total_closed} closed → ${total_rev:,.0f} revenue → ${total_mrr:,.0f}/mo MRR")
    conn.close()


def main():
    ap = argparse.ArgumentParser(description="Track meetings and deal outcomes")
    sub = ap.add_subparsers(dest="command")

    # log
    p = sub.add_parser("log", help="Log a new meeting")
    p.add_argument("--email", required=True)
    p.add_argument("--client")
    p.add_argument("--niche")
    p.add_argument("--company")
    p.add_argument("--contact")
    p.add_argument("--date")
    p.add_argument("--type", choices=["discovery", "proposal", "followup"])
    p.add_argument("--notes")
    p.add_argument("--campaign")

    # update
    p = sub.add_parser("update", help="Update meeting status/outcome")
    p.add_argument("--email", required=True)
    p.add_argument("--status", choices=["scheduled", "completed", "no_show", "cancelled"])
    p.add_argument("--outcome", choices=["pending", "interested", "proposal_sent", "closed_won", "closed_lost"])
    p.add_argument("--notes")

    # close
    p = sub.add_parser("close", help="Close a deal")
    p.add_argument("--email", required=True)
    p.add_argument("--value", type=float, required=True, help="deal value in dollars")
    p.add_argument("--monthly", type=float, help="monthly recurring revenue")
    p.add_argument("--date")

    # lost
    p = sub.add_parser("lost", help="Mark deal as lost")
    p.add_argument("--email", required=True)
    p.add_argument("--reason")

    # list
    p = sub.add_parser("list", help="List meetings")
    p.add_argument("--client")
    p.add_argument("--niche")
    p.add_argument("--outcome")

    # pipeline
    p = sub.add_parser("pipeline", help="Pipeline summary")
    p.add_argument("--client")

    # roi
    p = sub.add_parser("roi", help="ROI by niche — which niches make money?")
    p.add_argument("--client")

    args = ap.parse_args()
    if not args.command:
        ap.print_help()
        return

    {"log": cmd_log, "update": cmd_update, "close": cmd_close, "lost": cmd_lost,
     "list": cmd_list, "pipeline": cmd_pipeline, "roi": cmd_roi}[args.command](args)


if __name__ == "__main__":
    main()
