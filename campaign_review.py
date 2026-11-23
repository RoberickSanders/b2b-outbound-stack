#!/usr/bin/env python3
"""
Campaign Review — Smartlead campaign analytics and performance review.

Usage:
    python3 campaign_review.py overview              # All campaigns at a glance
    python3 campaign_review.py campaign <id>         # Deep dive one campaign
    python3 campaign_review.py compare               # Compare campaigns side by side
    python3 campaign_review.py replies               # All positive replies
    python3 campaign_review.py bounces               # Campaigns with bounce issues
    python3 campaign_review.py winners               # Best-performing campaigns
    python3 campaign_review.py losers                # Worst-performing campaigns
    python3 campaign_review.py export --out stats.csv  # Export all stats
"""

import os
import sys
import time
import argparse
import requests
from datetime import datetime, timedelta, timezone

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SCRIPT_DIR)

try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(SCRIPT_DIR, "..", "..", ".env"))
    load_dotenv(os.path.join(SCRIPT_DIR, ".env"))
except ImportError:
    pass

API_KEY = os.environ.get("SMARTLEAD_API_KEY", "")
BASE_URL = "https://server.smartlead.ai/api/v1"


# ==============================================================================
# API HELPERS
# ==============================================================================

def _get(endpoint, **params):
    params["api_key"] = API_KEY
    try:
        r = requests.get(f"{BASE_URL}{endpoint}", params=params, timeout=30)
        if r.status_code == 200:
            return r.json()
        if r.status_code == 429:
            time.sleep(2)
            r = requests.get(f"{BASE_URL}{endpoint}", params=params, timeout=30)
            return r.json() if r.status_code == 200 else None
        return None
    except Exception:
        return None


def list_campaigns():
    data = _get("/campaigns/")
    if isinstance(data, list):
        return data
    return []


def get_campaign_stats(campaign_id, days_back=30):
    """Get campaign statistics from analytics-by-date endpoint."""
    end = datetime.now().strftime("%Y-%m-%d")
    start = (datetime.now() - timedelta(days=days_back)).strftime("%Y-%m-%d")
    return _get(f"/campaigns/{campaign_id}/analytics-by-date",
                start_date=start, end_date=end)


def get_campaign_per_lead_stats(campaign_id, limit=500):
    """Get per-lead statistics (sent, open, click, reply, bounce)."""
    all_stats = []
    offset = 0
    while True:
        data = _get(f"/campaigns/{campaign_id}/statistics", offset=offset, limit=limit)
        if not data:
            break
        if isinstance(data, dict):
            stats = data.get("data", [])
            total = int(data.get("total_stats", 0))
        else:
            stats = data
            total = len(data)
        if not stats:
            break
        all_stats.extend(stats)
        if len(all_stats) >= total:
            break
        offset += limit
        time.sleep(0.1)
    return all_stats


# ==============================================================================
# COMPUTE METRICS
# ==============================================================================

def compute_metrics(stats_dict):
    """Compute rates from raw stats."""
    if not stats_dict:
        return {}
    sent = int(stats_dict.get("sent_count") or 0)
    unique_sent = int(stats_dict.get("unique_sent_count") or 0)
    opens = int(stats_dict.get("unique_open_count") or 0)
    clicks = int(stats_dict.get("unique_click_count") or 0)
    replies = int(stats_dict.get("reply_count") or 0)
    bounces = int(stats_dict.get("bounce_count") or 0)
    unsubs = int(stats_dict.get("unsubscribed_count") or 0)
    total = int(stats_dict.get("total_count") or 0)
    drafted = int(stats_dict.get("drafted_count") or 0)

    def _pct(n, d):
        return (n / d * 100) if d else 0.0

    return {
        "total_leads": total,
        "drafted": drafted,
        "sent": sent,
        "unique_sent": unique_sent,
        "opens": opens,
        "clicks": clicks,
        "replies": replies,
        "bounces": bounces,
        "unsubs": unsubs,
        "open_rate": _pct(opens, unique_sent),
        "click_rate": _pct(clicks, unique_sent),
        "reply_rate": _pct(replies, unique_sent),
        "bounce_rate": _pct(bounces, unique_sent),
        "unsub_rate": _pct(unsubs, unique_sent),
    }


# ==============================================================================
# DISPLAY
# ==============================================================================

def _shorten(s, n):
    s = s or ""
    return s if len(s) <= n else s[:n-1] + "…"


def print_overview():
    """Print overview of all campaigns with key metrics."""
    print("Fetching all campaigns...")
    campaigns = list_campaigns()
    if not campaigns:
        print("No campaigns found.")
        return

    print(f"Found {len(campaigns)} campaigns. Fetching stats...\n")

    rows = []
    for i, c in enumerate(campaigns, 1):
        print(f"  [{i}/{len(campaigns)}] {c.get('name','')[:60]}", end="\r")
        stats = get_campaign_stats(c["id"])
        metrics = compute_metrics(stats) if stats else {}
        rows.append({
            "id": c["id"],
            "name": c.get("name", ""),
            "status": c.get("status", ""),
            **metrics,
        })

    print(" " * 100, end="\r")  # clear progress line

    # Sort by reply rate desc
    rows.sort(key=lambda r: r.get("reply_rate", 0), reverse=True)

    print(f"\n{'='*120}")
    print(f"CAMPAIGN OVERVIEW — {len(rows)} campaigns")
    print(f"{'='*120}")
    print(f"{'Status':10} {'Sent':>7} {'Opens':>7} {'Repl':>5} {'Bnc':>5} {'Open%':>7} {'Rpl%':>6} {'Bnc%':>6}  {'Campaign':50}")
    print("-" * 120)

    totals = {"sent": 0, "opens": 0, "replies": 0, "bounces": 0}
    for r in rows:
        status = r.get("status", "")[:10]
        sent = r.get("unique_sent", 0) or 0
        opens = r.get("opens", 0) or 0
        replies = r.get("replies", 0) or 0
        bounces = r.get("bounces", 0) or 0
        or_ = r.get("open_rate", 0) or 0
        rr = r.get("reply_rate", 0) or 0
        br = r.get("bounce_rate", 0) or 0

        # Color hints via chars (plain text — emoji indicators)
        reply_icon = "🔥" if rr >= 2 else "✓" if rr >= 1 else " "
        bounce_icon = "⚠️ " if br >= 3 else "  "

        print(f"{status:10} {sent:>7} {opens:>7} {replies:>5} {bounces:>5} "
              f"{or_:>6.1f}% {rr:>5.1f}% {br:>5.1f}% {bounce_icon}{_shorten(r['name'], 50)} {reply_icon}")

        totals["sent"] += sent
        totals["opens"] += opens
        totals["replies"] += replies
        totals["bounces"] += bounces

    print("-" * 120)

    # Aggregate totals
    if totals["sent"]:
        overall_or = totals["opens"] / totals["sent"] * 100
        overall_rr = totals["replies"] / totals["sent"] * 100
        overall_br = totals["bounces"] / totals["sent"] * 100
        print(f"{'TOTAL':10} {totals['sent']:>7} {totals['opens']:>7} {totals['replies']:>5} "
              f"{totals['bounces']:>5} {overall_or:>6.1f}% {overall_rr:>5.1f}% {overall_br:>5.1f}%")
    print()

    # Top/bottom performers
    active = [r for r in rows if r.get("unique_sent", 0) >= 50]
    if active:
        best = max(active, key=lambda r: r.get("reply_rate", 0))
        worst_bounce = max(active, key=lambda r: r.get("bounce_rate", 0))
        print(f"  🔥 BEST REPLY RATE:    {best['reply_rate']:.1f}% — {best['name']}")
        print(f"  ⚠️  HIGHEST BOUNCES:    {worst_bounce['bounce_rate']:.1f}% — {worst_bounce['name']}")
        print()


def print_campaign_deep_dive(campaign_id):
    """Detailed view of one campaign."""
    campaigns = list_campaigns()
    match = next((c for c in campaigns if str(c.get("id")) == str(campaign_id)), None)
    if not match:
        print(f"Campaign {campaign_id} not found")
        return

    print(f"\n{'='*80}")
    print(f"CAMPAIGN: {match.get('name','')}")
    print(f"ID: {campaign_id}  |  Status: {match.get('status','')}")
    print(f"{'='*80}\n")

    stats = get_campaign_stats(campaign_id)
    metrics = compute_metrics(stats) if stats else {}

    if metrics:
        print(f"  Total leads:      {metrics['total_leads']:,}")
        print(f"  Drafted:          {metrics['drafted']:,}")
        print(f"  Sent (unique):    {metrics['unique_sent']:,}")
        print(f"  Opens (unique):   {metrics['opens']:,}  ({metrics['open_rate']:.1f}%)")
        print(f"  Clicks (unique):  {metrics['clicks']:,}  ({metrics['click_rate']:.1f}%)")
        print(f"  Replies:          {metrics['replies']:,}  ({metrics['reply_rate']:.1f}%)")
        print(f"  Bounces:          {metrics['bounces']:,}  ({metrics['bounce_rate']:.1f}%)")
        print(f"  Unsubscribes:     {metrics['unsubs']:,}  ({metrics['unsub_rate']:.1f}%)")
        print()

    # Per-lead stats
    print("Fetching per-lead statistics...")
    lead_stats = get_campaign_per_lead_stats(campaign_id)

    if lead_stats:
        # Compute additional metrics
        total_sends = len(lead_stats)
        with_open = sum(1 for s in lead_stats if s.get("open_time"))
        with_reply = sum(1 for s in lead_stats if s.get("reply_time"))
        with_bounce = sum(1 for s in lead_stats if s.get("is_bounced"))
        unsubs = sum(1 for s in lead_stats if s.get("is_unsubscribed"))

        # By sequence step
        by_seq = {}
        for s in lead_stats:
            seq = s.get("sequence_number", 0)
            if seq not in by_seq:
                by_seq[seq] = {"sent": 0, "opens": 0, "replies": 0, "bounces": 0}
            by_seq[seq]["sent"] += 1
            if s.get("open_time"):
                by_seq[seq]["opens"] += 1
            if s.get("reply_time"):
                by_seq[seq]["replies"] += 1
            if s.get("is_bounced"):
                by_seq[seq]["bounces"] += 1

        print(f"\nSequence step breakdown:")
        print(f"  {'Step':6} {'Sent':>7} {'Opens':>7} {'Replies':>8} {'Bounces':>8} {'Open%':>7} {'Rply%':>7}")
        for seq in sorted(by_seq.keys()):
            s = by_seq[seq]
            or_ = s["opens"] / s["sent"] * 100 if s["sent"] else 0
            rr = s["replies"] / s["sent"] * 100 if s["sent"] else 0
            print(f"  #{seq:<5} {s['sent']:>7} {s['opens']:>7} {s['replies']:>8} {s['bounces']:>8} {or_:>6.1f}% {rr:>6.1f}%")

        # Show recent replies if any
        replies = [s for s in lead_stats if s.get("reply_time")]
        if replies:
            print(f"\n  Recent replies ({len(replies)}):")
            for r in replies[:10]:
                print(f"    → {r.get('lead_name','')} ({r.get('lead_email','')})")

        # Show recent bounces
        bounces = [s for s in lead_stats if s.get("is_bounced")]
        if bounces and len(bounces) <= 20:
            print(f"\n  Bounced addresses ({len(bounces)}):")
            for b in bounces[:10]:
                print(f"    ✗ {b.get('lead_email','')}")

    print()


def show_compare():
    """Compare all campaigns in a sortable table."""
    print_overview()  # reuse the overview


def show_replies():
    """Show all replies across all campaigns."""
    campaigns = list_campaigns()
    print(f"\nFetching replies from {len(campaigns)} campaigns...\n")

    all_replies = []
    for i, c in enumerate(campaigns, 1):
        print(f"  [{i}/{len(campaigns)}] {c.get('name','')[:60]}", end="\r")
        stats = get_campaign_per_lead_stats(c["id"])
        for s in stats:
            if s.get("reply_time"):
                all_replies.append({
                    "campaign": c.get("name", ""),
                    "campaign_id": c["id"],
                    "lead_name": s.get("lead_name", ""),
                    "lead_email": s.get("lead_email", ""),
                    "reply_time": s.get("reply_time", ""),
                    "sequence": s.get("sequence_number", 0),
                })

    print(" " * 100, end="\r")
    # Sort by most recent
    all_replies.sort(key=lambda r: r.get("reply_time", ""), reverse=True)

    print(f"\n{'='*100}")
    print(f"ALL REPLIES — {len(all_replies)} total")
    print(f"{'='*100}\n")

    for r in all_replies[:50]:
        date = (r.get("reply_time") or "")[:10]
        print(f"  {date}  {_shorten(r['lead_name'], 25):25} {_shorten(r['lead_email'], 35):35}")
        print(f"              Campaign: {_shorten(r['campaign'], 60)}  (seq #{r['sequence']})")
        print()


def show_bounces():
    """Show campaigns with high bounce rates (>2%)."""
    campaigns = list_campaigns()
    print("Analyzing bounce rates...\n")

    flagged = []
    for c in campaigns:
        stats = get_campaign_stats(c["id"])
        m = compute_metrics(stats) if stats else {}
        if m.get("unique_sent", 0) >= 50 and m.get("bounce_rate", 0) >= 1:
            flagged.append((c, m))

    flagged.sort(key=lambda x: x[1]["bounce_rate"], reverse=True)

    print(f"{'='*100}")
    print(f"BOUNCE RATE ALERT — campaigns with bounce rate ≥ 1%")
    print(f"{'='*100}\n")

    for c, m in flagged:
        icon = "🚨" if m["bounce_rate"] >= 3 else "⚠️ "
        print(f"  {icon} {m['bounce_rate']:>5.1f}%  "
              f"({m['bounces']}/{m['unique_sent']}) — {c.get('name','')}")

    if not flagged:
        print("  ✓ All campaigns under 1% bounce rate")
    print()


def show_winners():
    """Show top-performing campaigns."""
    campaigns = list_campaigns()
    print("Finding winners...\n")

    rows = []
    for c in campaigns:
        stats = get_campaign_stats(c["id"])
        m = compute_metrics(stats) if stats else {}
        if m.get("unique_sent", 0) >= 50:
            rows.append((c, m))

    if not rows:
        print("No campaigns with enough data yet.")
        return

    print(f"{'='*100}")
    print("TOP BY REPLY RATE")
    print(f"{'='*100}")
    for c, m in sorted(rows, key=lambda x: x[1]["reply_rate"], reverse=True)[:10]:
        print(f"  {m['reply_rate']:>5.1f}%  ({m['replies']}/{m['unique_sent']}) — {c.get('name','')}")

    print(f"\n{'='*100}")
    print("TOP BY OPEN RATE")
    print(f"{'='*100}")
    for c, m in sorted(rows, key=lambda x: x[1]["open_rate"], reverse=True)[:10]:
        print(f"  {m['open_rate']:>5.1f}%  ({m['opens']}/{m['unique_sent']}) — {c.get('name','')}")
    print()


def show_losers():
    """Show underperforming campaigns."""
    campaigns = list_campaigns()
    print("Finding underperformers...\n")

    rows = []
    for c in campaigns:
        stats = get_campaign_stats(c["id"])
        m = compute_metrics(stats) if stats else {}
        if m.get("unique_sent", 0) >= 50:
            rows.append((c, m))

    if not rows:
        print("No campaigns with enough data yet.")
        return

    print(f"{'='*100}")
    print("WORST BY REPLY RATE")
    print(f"{'='*100}")
    for c, m in sorted(rows, key=lambda x: x[1]["reply_rate"])[:10]:
        print(f"  {m['reply_rate']:>5.1f}%  ({m['replies']}/{m['unique_sent']}) — {c.get('name','')}")

    print(f"\n{'='*100}")
    print("WORST BY BOUNCE RATE")
    print(f"{'='*100}")
    for c, m in sorted(rows, key=lambda x: x[1]["bounce_rate"], reverse=True)[:10]:
        print(f"  {m['bounce_rate']:>5.1f}%  ({m['bounces']}/{m['unique_sent']}) — {c.get('name','')}")
    print()


def export_stats(out_path):
    """Export all campaign stats to CSV."""
    import csv
    campaigns = list_campaigns()
    print(f"Exporting {len(campaigns)} campaigns to {out_path}...")

    rows = []
    for c in campaigns:
        stats = get_campaign_stats(c["id"])
        m = compute_metrics(stats) if stats else {}
        rows.append({
            "campaign_id": c["id"],
            "name": c.get("name", ""),
            "status": c.get("status", ""),
            **m,
        })

    if rows:
        fields = list(rows[0].keys())
        with open(out_path, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=fields)
            w.writeheader()
            w.writerows(rows)

    print(f"Exported {len(rows)} campaigns.")


# ==============================================================================
# CLI
# ==============================================================================

def main():
    parser = argparse.ArgumentParser(description="Smartlead campaign review")
    sub = parser.add_subparsers(dest="cmd", required=True)

    sub.add_parser("overview", help="All campaigns at a glance")

    p_camp = sub.add_parser("campaign", help="Deep dive on one campaign")
    p_camp.add_argument("campaign_id", type=int)

    sub.add_parser("compare", help="Compare campaigns side by side")
    sub.add_parser("replies", help="All replies across campaigns")
    sub.add_parser("bounces", help="Campaigns with high bounce rates")
    sub.add_parser("winners", help="Top-performing campaigns")
    sub.add_parser("losers", help="Underperforming campaigns")

    p_exp = sub.add_parser("export", help="Export all stats to CSV")
    p_exp.add_argument("--out", default="campaign_stats.csv")

    args = parser.parse_args()

    if not API_KEY:
        print("ERROR: SMARTLEAD_API_KEY not set in .env")
        sys.exit(1)

    if args.cmd == "overview":
        print_overview()
    elif args.cmd == "campaign":
        print_campaign_deep_dive(args.campaign_id)
    elif args.cmd == "compare":
        show_compare()
    elif args.cmd == "replies":
        show_replies()
    elif args.cmd == "bounces":
        show_bounces()
    elif args.cmd == "winners":
        show_winners()
    elif args.cmd == "losers":
        show_losers()
    elif args.cmd == "export":
        export_stats(args.out)


if __name__ == "__main__":
    main()
