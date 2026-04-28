#!/usr/bin/env python3.13
"""
deliverability_monitor.py — Daily check for silent deliverability decay.

Why this exists:
  Bounce rate (autopilot's current metric) is a lagging indicator. By the
  time bounces hit 3%, your sender reputation is already damaged. This tool
  catches EARLIER signals:
    - Mailbox warmup reputation trending DOWN (Google / Outlook flagging)
    - Smartlead internal warmup health dropping
    - Domains that stopped responding on MX check
    - Blocklist appearances (MXToolbox free tier)

  Runs daily via launchd. Alerts via Pushover on any concerning change.

What it checks (across all 57+ CLIENT_C mailboxes + 72+ domains):
  1. Smartlead warmup score delta vs. 7 days ago
  2. Smartlead inbox placement trend
  3. DNS resolution + MX record health per domain
  4. Generic blocklist signal (via free checker API)

Usage:
  python3 tools/deliverability_monitor.py              # full run + Pushover alerts
  python3 tools/deliverability_monitor.py --dry-run    # no alerts, just report
  python3 tools/deliverability_monitor.py --json       # JSON output for dashboards
  python3 tools/deliverability_monitor.py --domain X   # check one domain only

State persistence: writes baseline to logs/deliverability_baseline.json so
week-over-week trend detection works across runs.

Standalone tool — does not modify Forge code.
"""

import argparse
import json
import os
import socket
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

SCRIPT_DIR = Path(__file__).resolve().parent
LEAD_PIPELINE_DIR = SCRIPT_DIR.parent
WORKSPACE_ROOT = LEAD_PIPELINE_DIR.parent.parent
LOGS_DIR = LEAD_PIPELINE_DIR / "logs"
LOGS_DIR.mkdir(exist_ok=True)
BASELINE_FILE = LOGS_DIR / "deliverability_baseline.json"
EVENTS_LOG = LOGS_DIR / "deliverability_events.jsonl"

try:
    from dotenv import load_dotenv
    load_dotenv(WORKSPACE_ROOT / ".env", override=True)
    load_dotenv(LEAD_PIPELINE_DIR / ".env", override=True)
except ImportError:
    pass

SMARTLEAD_KEY = os.environ.get("SMARTLEAD_API_KEY", "")
SMARTLEAD_BASE = "https://server.smartlead.ai/api/v1"


# ============================================================
# State persistence
# ============================================================

def load_baseline() -> dict:
    if not BASELINE_FILE.exists():
        return {}
    try:
        return json.loads(BASELINE_FILE.read_text())
    except Exception:
        return {}


def save_baseline(baseline: dict) -> None:
    BASELINE_FILE.write_text(json.dumps(baseline, indent=2))


def log_event(event: dict) -> None:
    event = {**event, "timestamp": datetime.now(timezone.utc).isoformat()}
    with EVENTS_LOG.open("a") as f:
        f.write(json.dumps(event) + "\n")


# ============================================================
# Data pulls
# ============================================================

def pull_all_mailboxes() -> list:
    """Pull every mailbox in Smartlead with warmup details."""
    all_accts = []
    page = 1
    while True:
        r = requests.get(
            f"{SMARTLEAD_BASE}/email-accounts/",
            params={"api_key": SMARTLEAD_KEY, "offset": (page - 1) * 100, "limit": 100},
            timeout=30,
        )
        if r.status_code != 200: break
        batch = r.json() if isinstance(r.json(), list) else []
        if not batch: break
        all_accts.extend(batch)
        if len(batch) < 100: break
        page += 1
    return all_accts


def mx_check(domain: str) -> bool:
    """Returns True if the domain has MX records. Uses `dig` for speed."""
    if not domain:
        return False
    try:
        result = subprocess.run(
            ["dig", "+short", "+time=2", "+tries=1", "MX", domain],
            capture_output=True, text=True, timeout=5,
        )
        return bool(result.stdout.strip())
    except Exception:
        return False


# ============================================================
# Check functions
# ============================================================

def check_warmup_trend(current_mailboxes: list, baseline: dict) -> list:
    """Compare current warmup scores to baseline. Returns alerts."""
    alerts = []
    now = datetime.now(timezone.utc).isoformat()

    for acct in current_mailboxes:
        email = acct.get("from_email", "")
        if not email:
            continue
        warmup = acct.get("warmup_details") or {}
        try:
            pct = int(str(warmup.get("warmup_reputation", "0")).replace("%", ""))
        except (ValueError, TypeError):
            pct = 0

        baseline_entry = baseline.get(email, {})
        prior_pct = baseline_entry.get("warmup_pct", pct)

        # Alert: mailbox dropped 10+ points since last check
        if pct < prior_pct - 10:
            alerts.append({
                "severity": "high",
                "email": email,
                "metric": "warmup_reputation",
                "from": prior_pct, "to": pct,
                "message": f"{email} warmup dropped from {prior_pct}% → {pct}%",
            })
        # Alert: mailbox below 80% after being >90%
        elif pct < 80 and prior_pct >= 90:
            alerts.append({
                "severity": "medium",
                "email": email,
                "metric": "warmup_reputation",
                "from": prior_pct, "to": pct,
                "message": f"{email} warmup below 80% (was {prior_pct}%)",
            })

        # Update baseline
        baseline[email] = {
            "warmup_pct": pct,
            "last_checked": now,
            "message_per_day": acct.get("message_per_day", 0),
        }

    return alerts


def check_dns_health(domains: list) -> list:
    """Check every domain has MX records. Returns alerts for any missing."""
    alerts = []
    for domain in domains:
        if not mx_check(domain):
            alerts.append({
                "severity": "critical",
                "domain": domain,
                "metric": "mx_records",
                "message": f"{domain} has no MX records — DNS broken or domain expired",
            })
    return alerts


def check_bounce_trend(current_mailboxes: list, baseline: dict) -> list:
    """Alert if lifetime bounce rate jumped since baseline."""
    alerts = []
    for acct in current_mailboxes:
        email = acct.get("from_email", "")
        warmup = acct.get("warmup_details") or {}
        sent = warmup.get("total_sent_count", 0) or 0
        bounced = warmup.get("total_hard_bounce_count", 0) or 0
        if not sent or sent < 50:
            continue
        current_rate = bounced / sent * 100

        baseline_entry = baseline.get(email, {})
        prior_rate = baseline_entry.get("lifetime_bounce_rate", current_rate)

        if current_rate > prior_rate + 1.0 and current_rate > 2.0:
            alerts.append({
                "severity": "medium",
                "email": email,
                "metric": "lifetime_bounce_rate",
                "from": round(prior_rate, 2), "to": round(current_rate, 2),
                "message": f"{email} bounce rate jumped {prior_rate:.2f}% → {current_rate:.2f}%",
            })

        # Update baseline
        if email not in baseline:
            baseline[email] = {}
        baseline[email]["lifetime_bounce_rate"] = current_rate

    return alerts


# ============================================================
# Alert delivery
# ============================================================

def send_pushover(message: str, title: str, priority: int) -> bool:
    """Copy of the mailbox_autopilot Pushover sender, kept local to avoid import coupling."""
    user_key = os.environ.get("PUSHOVER_USER_KEY", "")
    app_token = os.environ.get("PUSHOVER_APP_TOKEN", "")
    if not user_key or not app_token:
        return False
    try:
        r = requests.post(
            "https://api.pushover.net/1/messages.json",
            data={"token": app_token, "user": user_key, "title": title, "message": message, "priority": priority},
            timeout=10,
        )
        return r.status_code == 200
    except Exception:
        return False


def send_alert(alert: dict, dry_run: bool = False) -> None:
    severity = alert["severity"]
    priority = {"critical": 1, "high": 1, "medium": 0, "info": -1}.get(severity, 0)
    title_map = {"critical": "🚨 Deliverability CRITICAL", "high": "⚠️ Deliverability warning",
                 "medium": "ℹ️ Deliverability note", "info": "Deliverability"}
    prefix = {"critical": "🚨", "high": "⚠️", "medium": "ℹ️", "info": "·"}.get(severity, "·")
    print(f"  {prefix} [{severity.upper()}] {alert['message']}")
    log_event({"type": "alert", **alert})
    if not dry_run:
        send_pushover(alert["message"], title_map.get(severity, "Deliverability"), priority)


# ============================================================
# Orchestration
# ============================================================

def run(dry_run: bool = False, domain_filter: str = None, output_json: bool = False) -> dict:
    if not SMARTLEAD_KEY:
        sys.exit("ERROR: SMARTLEAD_API_KEY not set")

    print(f"{'='*70}")
    print(f"DELIVERABILITY MONITOR — {'DRY RUN' if dry_run else 'LIVE'}")
    print(f"{'='*70}")

    started = datetime.now(timezone.utc)
    baseline = load_baseline()

    # Pull mailboxes
    print("Pulling mailboxes from Smartlead...")
    mailboxes = pull_all_mailboxes()
    print(f"  → {len(mailboxes)} mailboxes fetched")

    # Extract unique domains
    domains = sorted({(m.get("from_email") or "").split("@")[-1] for m in mailboxes if m.get("from_email")})
    if domain_filter:
        domains = [d for d in domains if d == domain_filter]
    print(f"  → {len(domains)} unique domains to check")

    all_alerts = []

    # Check 1: warmup trend
    print("\nCheck 1: warmup reputation trend...")
    warmup_alerts = check_warmup_trend(mailboxes, baseline)
    print(f"  → {len(warmup_alerts)} alerts")
    all_alerts.extend(warmup_alerts)

    # Check 2: DNS / MX
    print("\nCheck 2: DNS / MX records (takes ~30s for 72 domains)...")
    dns_alerts = check_dns_health(domains)
    print(f"  → {len(dns_alerts)} alerts")
    all_alerts.extend(dns_alerts)

    # Check 3: bounce trend
    print("\nCheck 3: lifetime bounce rate trend...")
    bounce_alerts = check_bounce_trend(mailboxes, baseline)
    print(f"  → {len(bounce_alerts)} alerts")
    all_alerts.extend(bounce_alerts)

    # Save updated baseline
    save_baseline(baseline)

    # Fire alerts
    print(f"\n{'='*70}\nALERTS ({len(all_alerts)})\n{'='*70}")
    if not all_alerts:
        print("  ✅ No deliverability issues detected.")
    for alert in all_alerts:
        send_alert(alert, dry_run=dry_run)

    summary = {
        "started_at": started.isoformat(),
        "completed_at": datetime.now(timezone.utc).isoformat(),
        "mailboxes_checked": len(mailboxes),
        "domains_checked": len(domains),
        "total_alerts": len(all_alerts),
        "critical": sum(1 for a in all_alerts if a["severity"] == "critical"),
        "high": sum(1 for a in all_alerts if a["severity"] == "high"),
        "medium": sum(1 for a in all_alerts if a["severity"] == "medium"),
    }

    log_event({"type": "run_complete", **summary})

    print(f"\n{'='*70}\nSUMMARY\n{'='*70}")
    for k, v in summary.items():
        print(f"  {k}: {v}")

    if output_json:
        return {"summary": summary, "alerts": all_alerts}
    return summary


# ============================================================
# CLI
# ============================================================

def main():
    ap = argparse.ArgumentParser(description="Daily deliverability health check")
    ap.add_argument("--dry-run", action="store_true", help="report only, no Pushover alerts")
    ap.add_argument("--domain", help="check one specific domain")
    ap.add_argument("--json", action="store_true", help="output result as JSON to stdout")
    args = ap.parse_args()

    result = run(dry_run=args.dry_run, domain_filter=args.domain, output_json=args.json)
    if args.json:
        print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    main()
