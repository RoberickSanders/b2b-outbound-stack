#!/usr/bin/env python3
"""
send_audit.py — Daily/weekly/monthly send audit per client/campaign.

Pulls send data from Smartlead's /campaigns/{id}/statistics (lifetime records
with sent_time timestamps), filters client-side by date, groups by day/client/
campaign. Works because Smartlead's documented date-range endpoints are 404s.

Usage:
    # All clients, last 7 days
    python3 tools/send_audit.py

    # Just ClientC, last 31 days
    python3 tools/send_audit.py --client client_c --days 31

    # Today only, broken down per campaign
    python3 tools/send_audit.py --today --by-campaign

    # Yesterday's numbers
    python3 tools/send_audit.py --yesterday

    # Show cap utilization vs theoretical max
    python3 tools/send_audit.py --days 7 --utilization

    # Machine-readable JSON
    python3 tools/send_audit.py --days 7 --json
"""

import argparse
import json
import os
import sys
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from pathlib import Path

import requests

PIPELINE_DIR = Path(__file__).resolve().parent.parent

def _load_env():
    for p in (PIPELINE_DIR.parent.parent / ".env", PIPELINE_DIR / ".env"):
        if not p.exists():
            continue
        for line in p.read_text().splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            k, v = k.strip(), v.strip().strip('"').strip("'")
            if k and not os.environ.get(k):
                os.environ[k] = v

_load_env()
KEY = os.environ.get("SMARTLEAD_API_KEY", "")
BASE = "https://server.smartlead.ai/api/v1"


def client_of(name):
    n = name or ""
    if "ClientA" in n or n.startswith("CLIENT_A"): return "CLIENT_A"
    if "ClientB" in n or n.startswith("CLIENT_B"): return "CLIENT_B"
    return "CLIENT_C"


def _sl_get(path):
    sep = "&" if "?" in path else "?"
    r = requests.get(f"{BASE}{path}{sep}api_key={KEY}", timeout=30,
                     headers={"User-Agent": "audit/1.0"})
    return r.status_code, (r.json() if r.text.strip() and r.text.strip().startswith(("{","[")) else r.text)


def count_sends_in_window(cid, start, end):
    """Paginate /statistics, return (sent, replied, bounced) in window."""
    sent = replied = bounced = 0
    offset = 0
    while True:
        code, d = _sl_get(f"/campaigns/{cid}/statistics?limit=100&offset={offset}")
        if code != 200 or not isinstance(d, dict):
            break
        rows = d.get("data", []) or []
        if not rows:
            break
        stop_early = False
        for rec in rows:
            st = rec.get("sent_time")
            if not st:
                continue
            try:
                dt = datetime.fromisoformat(st.replace("Z", "+00:00"))
            except Exception:
                continue
            if start <= dt < end:
                sent += 1
                if rec.get("reply_time"):
                    replied += 1
                if rec.get("is_bounced"):
                    bounced += 1
            elif dt < start:
                # Records appear roughly in insertion order. If we see records
                # earlier than window start, we're past the relevant data.
                pass  # don't stop early — Smartlead ordering is not reliable
        if len(rows) < 100:
            break
        offset += 100
        # Safety cap to avoid runaway on huge campaigns
        if offset > 10000:
            break
    return sent, replied, bounced


def count_sends_by_day(cid, start, end):
    """Same pagination but return per-day counts (used for daily breakdowns)."""
    by_day = defaultdict(lambda: [0, 0, 0])  # [sent, replied, bounced]
    offset = 0
    while True:
        code, d = _sl_get(f"/campaigns/{cid}/statistics?limit=100&offset={offset}")
        if code != 200 or not isinstance(d, dict):
            break
        rows = d.get("data", []) or []
        if not rows:
            break
        for rec in rows:
            st = rec.get("sent_time")
            if not st:
                continue
            try:
                dt = datetime.fromisoformat(st.replace("Z", "+00:00"))
            except Exception:
                continue
            if start <= dt < end:
                day = dt.date().isoformat()
                by_day[day][0] += 1
                if rec.get("reply_time"):
                    by_day[day][1] += 1
                if rec.get("is_bounced"):
                    by_day[day][2] += 1
        if len(rows) < 100:
            break
        offset += 100
        if offset > 10000:
            break
    return dict(by_day)


def daily_sent_from_mailboxes(cid):
    """Today's sent count from each mailbox's daily_sent_count (authoritative)."""
    code, mbs = _sl_get(f"/campaigns/{cid}/email-accounts")
    if code != 200 or not isinstance(mbs, list):
        return 0
    total = 0
    for m in mbs:
        code_m, det = _sl_get(f"/email-accounts/{m['id']}")
        if code_m == 200 and isinstance(det, dict):
            total += int(det.get("daily_sent_count") or 0)
    return total


def daily_cap_sum(cid):
    code, mbs = _sl_get(f"/campaigns/{cid}/email-accounts")
    if code != 200 or not isinstance(mbs, list):
        return 0
    return sum(int(m.get("message_per_day") or 0) for m in mbs)


def main():
    ap = argparse.ArgumentParser(description="Send audit across Smartlead campaigns.")
    ap.add_argument("--client", choices=["client_a", "client_b", "client_c", "CLIENT_A", "CLIENT_B", "CLIENT_C", "all"], default="all")
    ap.add_argument("--days", type=int, default=7, help="Lookback window in days (default 7)")
    ap.add_argument("--today", action="store_true", help="Today only (ET). Uses mailbox daily_sent_count (authoritative, fast).")
    ap.add_argument("--yesterday", action="store_true", help="Yesterday only (ET)")
    ap.add_argument("--by-campaign", action="store_true", help="Show per-campaign breakdown")
    ap.add_argument("--utilization", action="store_true", help="Show utilization %% vs daily cap (today + yday)")
    ap.add_argument("--json", action="store_true", help="JSON output")
    args = ap.parse_args()

    # Normalize client
    cmap = {"CLIENT_A":"client_a","CLIENT_B":"client_b","CLIENT_C":"client_c"}
    client_filter = cmap.get(args.client, args.client)

    # All campaigns
    code, camps = _sl_get("/campaigns/")
    if code != 200 or not isinstance(camps, list):
        print("ERR: could not list campaigns")
        sys.exit(1)

    # Filter by client
    if client_filter != "all":
        camps = [c for c in camps if client_of(c.get("name", "")).lower() == {"client_a":"paf","client_b":"sc","client_c":"rm"}[client_filter]]

    # ---- FAST PATH: --today ----
    if args.today:
        print(f"TODAY's sends (from mailbox daily_sent_count)")
        totals = defaultdict(int)
        rows = []
        for c in camps:
            if c.get("status") != "ACTIVE":
                continue
            cid = c["id"]
            n = daily_sent_from_mailboxes(cid)
            cap = daily_cap_sum(cid)
            cl = client_of(c.get("name"))
            totals[cl] += n
            rows.append((cid, cl, c["name"], n, cap))
        rows.sort(key=lambda r: -r[3])
        if args.by_campaign:
            print(f"{'ID':>9} | {'Cl':<3} | {'Campaign':<50} | {'sent':>4} | {'cap':>4} | {'util':>4}")
            print("-" * 95)
            for cid, cl, name, n, cap in rows:
                util = f"{int(n/cap*100)}%" if cap else "-"
                print(f"{cid:>9} | {cl:<3} | {name[:50]:<50} | {n:>4} | {cap:>4} | {util:>4}")
        print()
        print(f"TOTALS: CLIENT_A={totals['CLIENT_A']}  CLIENT_C={totals['CLIENT_C']}  CLIENT_B={totals['CLIENT_B']}  ALL={sum(totals.values())}")
        return

    # ---- Window-based path (--days, --yesterday, --week) ----
    import zoneinfo
    ET = zoneinfo.ZoneInfo("America/New_York")
    now_et = datetime.now(ET)

    if args.yesterday:
        end = now_et.replace(hour=0, minute=0, second=0, microsecond=0)
        start = end - timedelta(days=1)
        label = f"YESTERDAY ({start.date()} ET)"
    else:
        end = datetime.now(timezone.utc) + timedelta(days=1)
        start = datetime.now(timezone.utc) - timedelta(days=args.days)
        label = f"LAST {args.days} DAYS (ending {datetime.now(timezone.utc).date()})"

    # Convert start/end to UTC for comparison with sent_time
    start_utc = start.astimezone(timezone.utc) if start.tzinfo else start.replace(tzinfo=timezone.utc)
    end_utc = end.astimezone(timezone.utc) if end.tzinfo else end.replace(tzinfo=timezone.utc)

    print(f"{label} — scanning {len(camps)} campaigns...")
    per_client = defaultdict(lambda: [0, 0, 0])
    per_camp = []
    for c in camps:
        cid = c["id"]
        name = c.get("name", "")
        cl = client_of(name)
        sent, replied, bounced = count_sends_in_window(cid, start_utc, end_utc)
        if sent == 0:
            continue
        per_client[cl][0] += sent
        per_client[cl][1] += replied
        per_client[cl][2] += bounced
        per_camp.append((cid, cl, name, sent, replied, bounced, c.get("status")))

    per_camp.sort(key=lambda r: -r[3])

    out = {
        "label": label,
        "days": args.days,
        "totals": {cl: {"sent": v[0], "replied": v[1], "bounced": v[2]} for cl, v in per_client.items()},
        "grand_total": {
            "sent": sum(v[0] for v in per_client.values()),
            "replied": sum(v[1] for v in per_client.values()),
            "bounced": sum(v[2] for v in per_client.values()),
        },
        "campaigns": [{"id":cid, "client":cl, "name":n, "sent":s, "replied":r, "bounced":b, "status":st} for cid,cl,n,s,r,b,st in per_camp],
    }

    if args.json:
        print(json.dumps(out, indent=2, default=str))
        return

    if args.by_campaign:
        print()
        print(f"{'ID':>9} | {'Status':<8} | {'Cl':<3} | {'Campaign':<50} | {'sent':>5} | {'rep':>4} | {'bnc':>4}")
        print("-" * 100)
        for cid,cl,n,s,r,b,st in per_camp:
            print(f"{cid:>9} | {st:<8} | {cl:<3} | {n[:50]:<50} | {s:>5,} | {r:>4} | {b:>4}")

    print()
    print("=" * 80)
    for cl in ("CLIENT_A", "CLIENT_C", "CLIENT_B"):
        v = per_client.get(cl, [0,0,0])
        if v[0]:
            rate = v[1]/v[0]*100
            bounce = v[2]/v[0]*100
            print(f"  {cl}: {v[0]:>6,} sent  {v[1]:>4} replies ({rate:.2f}%)  {v[2]:>4} bounces ({bounce:.2f}%)")
    print(f"  TOTAL: {out['grand_total']['sent']:>6,} sent  "
          f"{out['grand_total']['replied']:>4} replies  "
          f"{out['grand_total']['bounced']:>4} bounces  "
          f"(avg/day: {out['grand_total']['sent']//max(1,args.days)})")
    print("=" * 80)


if __name__ == "__main__":
    main()
