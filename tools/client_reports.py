#!/usr/bin/env python3.13
"""
client_reports.py — Weekly performance reports per client.

Why this exists:
  the operator currently generates client reports Friday evening by hand. Takes 3-5 hrs
  across CLIENT_A + CLIENT_B + any others. Sender One and Sender Two want more transparency on
  what's working. Adding "weekly performance reports" justifies a retainer
  bump from $500 → $1,000/mo per client = +$1,000/mo revenue.

Cron schedule (installed 2026-04-20): runs Friday 9am Eastern Time.

What it does:
  For each client (CLIENT_A, CLIENT_B, CLIENT_C-internal), pulls last-7-day Smartlead data:
    - Emails sent per campaign
    - Replies received
    - Meetings booked (from master_leads.db)
    - Bounce rate
    - Top-performing campaign
    - Notable replies (positive sentiment)
  Asks Kimi to write a 3-bullet "what happened / next week" narrative.
  Writes a markdown report to 01-Projects/{client}/reports/YYYY-MM-DD-weekly.md.

  Optional: when Gmail + Drive are wired via Composio, can also email to the
  client contact + upload PDF to client Drive folder. For v1, writes markdown
  locally — you preview, add personal notes, then send yourself.

Usage:
  python3 tools/client_reports.py                       # all clients, last 7 days
  python3 tools/client_reports.py --client client_a
  python3 tools/client_reports.py --days 14             # different window
  python3 tools/client_reports.py --dry-run             # preview to stdout

Standalone tool — does not modify Forge code.
"""

import argparse
import json
import os
import re
import sqlite3
import sys
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests

import requests

# Paths
SCRIPT_DIR = Path(__file__).resolve().parent
LEAD_PIPELINE_DIR = SCRIPT_DIR.parent
WORKSPACE_ROOT = LEAD_PIPELINE_DIR.parent.parent
PROJECTS_DIR = WORKSPACE_ROOT / "01-Projects"
MASTER_DB_PATH = LEAD_PIPELINE_DIR / "master-leads" / "master_leads.db"

# Ensure llm_router is importable from tools/ subdirectory
sys.path.insert(0, str(LEAD_PIPELINE_DIR))

# .env
try:
    from dotenv import load_dotenv
    load_dotenv(WORKSPACE_ROOT / ".env", override=True)
    load_dotenv(LEAD_PIPELINE_DIR / ".env", override=True)
except ImportError:
    pass

SMARTLEAD_KEY = os.environ.get("SMARTLEAD_API_KEY", "")
SMARTLEAD_BASE = "https://server.smartlead.ai/api/v1"


# ============================================================
# Client config
# ============================================================

CLIENT_CONFIG = {
    "client_c": {
        "display_name": "ClientC",
        "name_keywords": ["ClientC", "CLIENT_C ", "RevenueMechanic"],
        "master_db_client": "client_c",
    },
    "client_a": {
        "display_name": "ClientA",
        "name_keywords": ["ClientA", "CLIENT_A "],
        "master_db_client": "client_a",
    },
    "client_b": {
        "display_name": "ClientB",
        "name_keywords": ["ClientB"],
        "master_db_client": "client_b",
    },
}


# ============================================================
# Smartlead HTTP
# ============================================================

def sl_get(path: str, params: dict = None, retries: int = 3, timeout: int = 30):
    params = {"api_key": SMARTLEAD_KEY, **(params or {})}
    for attempt in range(retries + 1):
        try:
            r = requests.get(f"{SMARTLEAD_BASE}{path}", params=params, timeout=timeout)
            if r.status_code == 200 and r.text and r.text.strip().startswith(("{", "[")):
                return r.json()
            if r.status_code == 429 or r.status_code >= 500:
                time.sleep(2 + attempt * 2); continue
            return None
        except Exception:
            if attempt < retries:
                time.sleep(2 + attempt)
    return None


def matches_client(campaign_name: str, keywords: list) -> bool:
    name = (campaign_name or "").lower()
    return any(kw.lower() in name for kw in keywords)


# ============================================================
# Data pull
# ============================================================

def pull_client_campaigns(client_key: str, days_back: int) -> list:
    """Pull all campaigns for one client + their date-windowed analytics."""
    cfg = CLIENT_CONFIG[client_key]
    all_campaigns = sl_get("/campaigns/") or []
    client_campaigns = [c for c in all_campaigns if matches_client(c.get("name", ""), cfg["name_keywords"])]

    # Date range
    end_date = datetime.now(timezone.utc).date()
    start_date = end_date - timedelta(days=days_back)

    enriched = []
    for c in client_campaigns:
        cid = c["id"]
        # Get windowed analytics
        analytics = sl_get(
            f"/campaigns/{cid}/analytics-by-date",
            {"start_date": start_date.isoformat(), "end_date": end_date.isoformat()}
        ) or {}
        sent = int(analytics.get("sent_count", 0) or 0)
        if sent == 0:
            continue  # campaign didn't send during window — skip in report
        enriched.append({
            "id": cid,
            "name": c.get("name", ""),
            "status": c.get("status"),
            "sent": sent,
            "replies": int(analytics.get("reply_count", 0) or 0),
            "bounces": int(analytics.get("bounce_count", 0) or 0),
            "opens": int(analytics.get("open_count", 0) or 0),
        })
    return enriched


def pull_meetings_from_db(client_db_key: str, days_back: int) -> int:
    """Count meetings booked for this client in the window."""
    if not MASTER_DB_PATH.exists():
        return 0
    try:
        conn = sqlite3.connect(MASTER_DB_PATH)
        cur = conn.cursor()
        # Check if meetings table exists (not all DBs have it)
        tables = cur.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
        table_names = {t[0].lower() for t in tables}
        if "meetings" not in table_names:
            conn.close()
            return 0
        cutoff = (datetime.now() - timedelta(days=days_back)).isoformat()
        # Assume meetings table has client + booked_at columns
        cur.execute("SELECT COUNT(*) FROM meetings WHERE client = ? AND booked_at >= ?",
                    (client_db_key, cutoff))
        count = cur.fetchone()[0]
        conn.close()
        return count
    except Exception:
        return 0


# ============================================================
# Narrative via Kimi
# ============================================================

def generate_narrative(client_key: str, campaigns: list, days_back: int, meetings: int) -> str:
    """Kimi writes the 3-bullet 'what happened / next steps' summary."""
    try:
        from llm_router import get_light_client
        client, model = get_light_client()
    except Exception:
        return "(LLM unavailable — skipping narrative)"

    total_sent = sum(c["sent"] for c in campaigns)
    total_replies = sum(c["replies"] for c in campaigns)
    total_bounces = sum(c["bounces"] for c in campaigns)
    reply_rate = (total_replies / total_sent * 100) if total_sent else 0
    bounce_rate = (total_bounces / total_sent * 100) if total_sent else 0

    top_campaigns = sorted(campaigns, key=lambda c: -c["replies"])[:3]
    top_summary = "\n".join(
        f"- {c['name']}: {c['sent']} sent, {c['replies']} replies ({(c['replies']/c['sent']*100 if c['sent'] else 0):.2f}%)"
        for c in top_campaigns
    )

    prompt = f"""You are writing a {days_back}-day performance recap for a lead-gen client, {CLIENT_CONFIG[client_key]['display_name']}.

METRICS ({days_back} days):
  - Total emails sent: {total_sent}
  - Total replies: {total_replies} ({reply_rate:.2f}%)
  - Total bounces: {total_bounces} ({bounce_rate:.2f}%)
  - Meetings booked: {meetings}

TOP 3 CAMPAIGNS:
{top_summary}

Write exactly 3 bullets in this format. Keep it CRISP. No fluff.

## What happened this week
- Bullet 1 (one concrete metric + why it matters)
- Bullet 2 (standout campaign or trend)
- Bullet 3 (challenge or red flag worth knowing)

## Next week
- 2 specific actions you're taking to build on this

Write in the voice of an operator texting a client. No "I'm excited to share" or similar corporate filler. Be direct. Don't restate metrics the client already sees above — draw insight from them."""
    try:
        resp = client.messages.create(
            model=model, max_tokens=800,
            messages=[{"role": "user", "content": prompt}]
        )
        return resp.content[0].text.strip()
    except Exception as e:
        return f"(Narrative generation failed: {e})"


# ============================================================
# Report writer
# ============================================================

def build_report(client_key: str, days_back: int = 7, dry_run: bool = False) -> Path:
    cfg = CLIENT_CONFIG[client_key]
    print(f"\n{'='*70}\n{cfg['display_name']} — last {days_back} days\n{'='*70}")

    campaigns = pull_client_campaigns(client_key, days_back)
    if not campaigns:
        print("  No campaign activity in window. Skipping.")
        return None

    meetings = pull_meetings_from_db(cfg["master_db_client"], days_back)

    total_sent = sum(c["sent"] for c in campaigns)
    total_replies = sum(c["replies"] for c in campaigns)
    total_bounces = sum(c["bounces"] for c in campaigns)
    reply_rate = (total_replies / total_sent * 100) if total_sent else 0
    bounce_rate = (total_bounces / total_sent * 100) if total_sent else 0

    print(f"  Sent:      {total_sent}")
    print(f"  Replies:   {total_replies}  ({reply_rate:.2f}%)")
    print(f"  Bounces:   {total_bounces}  ({bounce_rate:.2f}%)")
    print(f"  Meetings:  {meetings}")
    print(f"  Campaigns: {len(campaigns)} active")

    narrative = generate_narrative(client_key, campaigns, days_back, meetings)
    print(f"\n  Narrative generated ({len(narrative)} chars)")

    # Markdown file
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    md = f"""# Weekly Performance Report — {cfg['display_name']}

**Period:** Last {days_back} days (through {today})
**Generated:** {datetime.now(timezone.utc).isoformat()}

## Headline metrics

| Metric | Value |
|---|---|
| Emails sent | {total_sent:,} |
| Replies | {total_replies} ({reply_rate:.2f}%) |
| Bounces | {total_bounces} ({bounce_rate:.2f}%) |
| Meetings booked | {meetings} |
| Active campaigns | {len(campaigns)} |

## Campaigns this week

| Campaign | Sent | Replies | Reply % | Bounces |
|---|---|---|---|---|
"""
    for c in sorted(campaigns, key=lambda x: -x["sent"]):
        rr = (c["replies"] / c["sent"] * 100) if c["sent"] else 0
        md += f"| {c['name']} | {c['sent']} | {c['replies']} | {rr:.2f}% | {c['bounces']} |\n"

    md += f"\n{narrative}\n"
    md += f"\n---\n*Auto-generated by `client_reports.py` — preview before sending to client.*\n"

    out_dir = PROJECTS_DIR / client_key / "reports"
    out_path = out_dir / f"{today}-weekly.md"

    if dry_run:
        print(f"\n[DRY RUN] would write {out_path}")
        print(f"---\n{md[:2500]}\n---")
        return out_path

    out_dir.mkdir(parents=True, exist_ok=True)
    out_path.write_text(md)
    print(f"  ✓ Saved: {out_path}")
    return out_path


# ============================================================
# Pushover summary (notifies the operator on his phone after report run)
# ============================================================

def send_pushover_summary(out_paths: list, dry_run: bool = False) -> bool:
    """Push a summary to the operator's phone after the weekly run completes.

    No-op if PUSHOVER_USER_KEY / PUSHOVER_APP_TOKEN are not configured. Returns
    True if Pushover accepted the alert. Each report's filesystem path is
    included so the operator can SSH in and review via Tailscale from his phone.
    """
    user_key = os.environ.get("PUSHOVER_USER_KEY", "")
    app_token = os.environ.get("PUSHOVER_APP_TOKEN", "")
    if not user_key or not app_token:
        return False

    if not out_paths:
        title = "📊 Weekly client reports — none generated"
        body = "No reports were produced. Check client_reports_systemd.log for errors."
    else:
        title = f"📊 {len(out_paths)} weekly report(s) ready"
        body_lines = [
            "Reports written. Preview before sending to clients.",
            "",
        ]
        for p in out_paths:
            # Show client name (parent dir 2 up from the file: <client>/reports/)
            try:
                client = p.parent.parent.name
            except Exception:
                client = "?"
            body_lines.append(f"  · {client}: {p.name}")
        body_lines.append("")
        body_lines.append("Read on phone via SSH:")
        body_lines.append("  ssh forge-prod  (Tailscale)")
        body = "\n".join(body_lines)[:1024]

    if dry_run:
        print(f"\n[DRY RUN] would Pushover: {title}\n{body}")
        return True

    try:
        r = requests.post(
            "https://api.pushover.net/1/messages.json",
            data={
                "user": user_key,
                "token": app_token,
                "title": title,
                "message": body,
                "priority": 0,
            },
            timeout=15,
        )
        return r.status_code == 200
    except Exception as e:
        print(f"  [WARN] Pushover send failed: {e}")
        return False


# ============================================================
# CLI
# ============================================================

def main():
    ap = argparse.ArgumentParser(description="Generate weekly client performance reports")
    ap.add_argument("--client", choices=list(CLIENT_CONFIG.keys()),
                    help="only run for one client (default: all)")
    ap.add_argument("--days", type=int, default=7, help="days back to include (default 7)")
    ap.add_argument("--dry-run", action="store_true", help="preview without writing files")
    ap.add_argument("--no-pushover", action="store_true",
                    help="skip Pushover summary at end (default: send)")
    args = ap.parse_args()

    targets = [args.client] if args.client else list(CLIENT_CONFIG.keys())
    out_paths = []
    for c in targets:
        try:
            p = build_report(c, days_back=args.days, dry_run=args.dry_run)
            if p:
                out_paths.append(p)
        except Exception as e:
            print(f"  [ERROR] {c}: {e}")

    print(f"\n{'='*70}\nDONE — {len(out_paths)} report(s) written\n{'='*70}")
    for p in out_paths:
        print(f"  {p}")

    if not args.no_pushover:
        ok = send_pushover_summary(out_paths, dry_run=args.dry_run)
        if ok:
            print("  → Pushover summary sent to phone ✓")
        else:
            print("  → Pushover not configured (skipped)")


if __name__ == "__main__":
    main()
