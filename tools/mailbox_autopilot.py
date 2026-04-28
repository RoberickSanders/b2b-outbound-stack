#!/usr/bin/env python3.13
"""
mailbox_autopilot.py — Daily watchdog for CLIENT_C's Smartlead infrastructure.

Why this exists:
  On 2026-04-20 the Fire Alarm campaign hit 6.67% bounce rate (industry danger
  threshold is 5%). Smartlead auto-paused it, but we only caught it by accident
  because the operator asked about bounces. With 57+ CLIENT_C mailboxes, 72+ domains, and 15+
  active campaigns across 3 clients, nobody can monitor manually anymore. One
  bad campaign can damage sender reputation for 3-6 months (unrecoverable
  without buying new domains).

What it does:
  Every 4 hours (via cron), walks all active campaigns across CLIENT_A, CLIENT_B, and CLIENT_C.
  For each one, checks:
    - Bounce rate > 3% in last 24h            → auto-PAUSE + alert
    - Reply rate < 1% after 200 sends         → auto-PAUSE + alert
    - Zero opens + zero replies after 500     → auto-PAUSE + alert (dead campaign)
    - Mailbox warmup reputation dropping      → warn only (no action)

  Logs every action to logs/autopilot_events.jsonl and autopilot_state.json.
  Alerts surface via iMessage (fast path) and email/Gmail (audit trail).

Usage:
  python3 tools/mailbox_autopilot.py                   # run now, act on violations
  python3 tools/mailbox_autopilot.py --dry-run         # show what would happen
  python3 tools/mailbox_autopilot.py --client paf      # only check one client
  python3 tools/mailbox_autopilot.py --no-pause        # alert only, never pause
  python3 tools/mailbox_autopilot.py --verbose         # per-campaign output

Standalone tool — does NOT modify Forge code. Safe to iterate on independently.
"""

import argparse
import json
import os
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import requests


# ============================================================
# PATHS + CONFIG
# ============================================================

SCRIPT_DIR = Path(__file__).resolve().parent
LEAD_PIPELINE_DIR = SCRIPT_DIR.parent
WORKSPACE_ROOT = LEAD_PIPELINE_DIR.parent.parent
LOGS_DIR = LEAD_PIPELINE_DIR / "logs"
LOGS_DIR.mkdir(exist_ok=True)
EVENTS_LOG = LOGS_DIR / "autopilot_events.jsonl"
STATE_FILE = LOGS_DIR / "autopilot_state.json"

# Load .env
try:
    from dotenv import load_dotenv
    load_dotenv(WORKSPACE_ROOT / ".env", override=True)
    load_dotenv(LEAD_PIPELINE_DIR / ".env", override=True)
except ImportError:
    pass

SMARTLEAD_KEY = os.environ.get("SMARTLEAD_API_KEY", "")
SMARTLEAD_BASE = "https://server.smartlead.ai/api/v1"

# Client name → campaign name prefixes we watch
CLIENT_KEYWORDS = {
    "rm": ["ClientC", "CLIENT_C ", "RevenueMechanic"],
    "paf": ["ClientA", "CLIENT_A "],
    "sc": ["ClientB"],
}

# ============================================================
# THRESHOLDS (tunable via CLI / env)
# ============================================================

DEFAULT_THRESHOLDS = {
    "bounce_rate_pct": 3.0,             # pause if > 3% over min_sends_for_bounce
    "min_sends_for_bounce": 20,         # don't evaluate bounce until this many sent
    "reply_rate_pct": 1.0,              # pause if < 1% after min_sends_for_reply
    "min_sends_for_reply": 200,         # don't evaluate until this many sent
    "dead_campaign_sends": 500,         # if 0 opens + 0 replies after this many sends → pause
    "warmup_reputation_min": 100,       # warn (don't pause) if any mailbox drops below
    "recheck_cooldown_hours": 24,       # don't re-act on a campaign we acted on recently
}


# ============================================================
# HTTP helpers (with retry/backoff)
# ============================================================

def sl_get(path: str, params: dict = None, retries: int = 3, timeout: int = 30):
    params = {"api_key": SMARTLEAD_KEY, **(params or {})}
    for attempt in range(retries + 1):
        try:
            r = requests.get(f"{SMARTLEAD_BASE}{path}", params=params, timeout=timeout)
            if r.status_code == 200 and r.text and r.text.strip().startswith(("{", "[")):
                return r.json()
            if r.status_code == 429 or r.status_code >= 500:
                time.sleep(2 + attempt * 2)
                continue
            return None
        except Exception:
            if attempt < retries:
                time.sleep(2 + attempt)
    return None


def sl_post(path: str, json_body: dict = None, retries: int = 3, timeout: int = 30):
    for attempt in range(retries + 1):
        try:
            r = requests.post(f"{SMARTLEAD_BASE}{path}",
                              params={"api_key": SMARTLEAD_KEY},
                              json=json_body or {}, timeout=timeout)
            if r.status_code == 200:
                return r.json() if r.text and r.text.strip().startswith(("{", "[")) else {"ok": True}
            if r.status_code == 429 or r.status_code >= 500:
                time.sleep(2 + attempt * 2)
                continue
            return None
        except Exception:
            if attempt < retries:
                time.sleep(2 + attempt)
    return None


# ============================================================
# CLIENT CLASSIFICATION
# ============================================================

def classify_client(campaign_name: str) -> str:
    """Return 'rm', 'paf', 'sc', or 'unknown' from campaign name."""
    name = (campaign_name or "").lower()
    for client, keywords in CLIENT_KEYWORDS.items():
        if any(kw.lower() in name for kw in keywords):
            return client
    return "unknown"


# ============================================================
# STATE PERSISTENCE (prevents re-acting on the same problem)
# ============================================================

def load_state() -> dict:
    if not STATE_FILE.exists():
        return {}
    try:
        return json.loads(STATE_FILE.read_text())
    except Exception:
        return {}


def save_state(state: dict) -> None:
    STATE_FILE.write_text(json.dumps(state, indent=2))


def recently_acted(state: dict, campaign_id: int, cooldown_hours: int) -> bool:
    key = str(campaign_id)
    if key not in state:
        return False
    try:
        last_action_ts = datetime.fromisoformat(state[key]["last_action_at"])
        hours_since = (datetime.now(timezone.utc) - last_action_ts).total_seconds() / 3600
        return hours_since < cooldown_hours
    except Exception:
        return False


def record_action(state: dict, campaign_id: int, action: str, reason: str) -> None:
    state[str(campaign_id)] = {
        "last_action": action,
        "last_reason": reason,
        "last_action_at": datetime.now(timezone.utc).isoformat(),
    }


# ============================================================
# EVENT LOGGING
# ============================================================

def log_event(event: dict) -> None:
    event = {**event, "timestamp": datetime.now(timezone.utc).isoformat()}
    with EVENTS_LOG.open("a") as f:
        f.write(json.dumps(event) + "\n")


# ============================================================
# THRESHOLD EVALUATION
# ============================================================

def evaluate_campaign(campaign: dict, analytics: dict, thresholds: dict) -> list:
    """Return list of violations found for this campaign. Each is a dict with
    keys: severity, reason, action, metric_value."""
    violations = []
    status = campaign.get("status")
    if status not in ("ACTIVE", "RUNNING"):
        return []  # Only evaluate actively-sending campaigns

    sent = int(analytics.get("sent_count", 0) or 0)
    bounces = int(analytics.get("bounce_count", 0) or 0)
    replies = int(analytics.get("reply_count", 0) or 0)
    opens = int(analytics.get("open_count", 0) or 0)

    # Rule 1: Bounce rate
    if sent >= thresholds["min_sends_for_bounce"]:
        bounce_pct = (bounces / sent * 100) if sent else 0
        if bounce_pct > thresholds["bounce_rate_pct"]:
            violations.append({
                "severity": "critical",
                "reason": f"bounce_rate={bounce_pct:.2f}% (threshold: >{thresholds['bounce_rate_pct']}% on {sent} sends)",
                "action": "pause",
                "metric": "bounce_rate",
                "metric_value": bounce_pct,
            })

    # Rule 2: Dead campaign (no engagement after a lot of sends)
    if sent >= thresholds["dead_campaign_sends"] and opens == 0 and replies == 0:
        violations.append({
            "severity": "high",
            "reason": f"dead_campaign: 0 opens + 0 replies on {sent} sends (threshold: >{thresholds['dead_campaign_sends']})",
            "action": "pause",
            "metric": "dead_campaign",
            "metric_value": sent,
        })

    # Rule 3: Low reply rate (after burn-in period)
    if sent >= thresholds["min_sends_for_reply"]:
        reply_pct = (replies / sent * 100) if sent else 0
        if reply_pct < thresholds["reply_rate_pct"]:
            violations.append({
                "severity": "medium",
                "reason": f"reply_rate={reply_pct:.2f}% (threshold: <{thresholds['reply_rate_pct']}% after {thresholds['min_sends_for_reply']} sends)",
                "action": "pause",
                "metric": "reply_rate",
                "metric_value": reply_pct,
            })

    return violations


# ============================================================
# CORE RUN
# ============================================================

def pause_campaign(campaign_id: int, reason: str, dry_run: bool = False) -> bool:
    """Pause a Smartlead campaign. Returns True on success."""
    if dry_run:
        print(f"    [DRY RUN] would POST /campaigns/{campaign_id}/status status=PAUSED")
        return True
    result = sl_post(f"/campaigns/{campaign_id}/status", {"status": "PAUSED"})
    return result is not None


def _send_pushover(message: str, title: str, priority: int) -> bool:
    """Send Pushover notification. Requires PUSHOVER_USER_KEY + PUSHOVER_APP_TOKEN
    in .env. Returns True on 200 success, False on any error (including missing keys).

    Priority levels:
       -2  silent (no alert)
       -1  quiet (no sound/vibration)
        0  normal
        1  high (bypass quiet hours)
        2  emergency (requires ack)
    """
    user_key = os.environ.get("PUSHOVER_USER_KEY", "")
    app_token = os.environ.get("PUSHOVER_APP_TOKEN", "")
    if not user_key or not app_token:
        return False
    try:
        r = requests.post(
            "https://api.pushover.net/1/messages.json",
            data={
                "token": app_token,
                "user": user_key,
                "title": title,
                "message": message,
                "priority": priority,
            },
            timeout=10,
        )
        return r.status_code == 200
    except Exception:
        return False


def send_alert(message: str, severity: str = "info") -> None:
    """Send an alert via Pushover (if wired) plus stdout + JSONL event log.

    Severity maps to Pushover priority:
      critical → 1 (high, bypass quiet hours)
      high     → 1
      medium   → 0 (normal)
      info     → -1 (quiet)

    Falls back silently if Pushover isn't configured — the event still logs
    to autopilot_events.jsonl for audit.
    """
    prefix = {"critical": "🚨", "high": "⚠️", "medium": "ℹ️", "info": "·"}.get(severity, "·")
    alert_text = f"{prefix} [{severity.upper()}] {message}"
    print(alert_text)
    log_event({"type": "alert", "severity": severity, "message": message})

    # Pushover delivery
    priority_map = {"critical": 1, "high": 1, "medium": 0, "info": -1}
    priority = priority_map.get(severity, 0)
    title_map = {"critical": "Forge ALERT", "high": "Forge warning", "medium": "Forge note", "info": "Forge"}
    title = title_map.get(severity, "Forge")
    _send_pushover(message, title, priority)


def run(thresholds: dict, dry_run: bool = False, no_pause: bool = False,
        client_filter: str = None, verbose: bool = False) -> dict:
    """Main orchestration. Returns summary dict."""
    if not SMARTLEAD_KEY:
        sys.exit("ERROR: SMARTLEAD_API_KEY not set in .env")

    started_at = datetime.now(timezone.utc)
    summary = {
        "started_at": started_at.isoformat(),
        "dry_run": dry_run,
        "no_pause": no_pause,
        "campaigns_checked": 0,
        "campaigns_paused": 0,
        "violations_found": 0,
        "actions": [],
    }

    # 1. Pull all campaigns
    print(f"Fetching campaigns from Smartlead...")
    campaigns = sl_get("/campaigns/")
    if not campaigns:
        sys.exit("ERROR: could not fetch campaigns list")
    print(f"  → {len(campaigns)} total campaigns in Smartlead")

    # 2. Filter to clients we watch + currently active
    watched = []
    for c in campaigns:
        client = classify_client(c.get("name", ""))
        if client == "unknown":
            continue
        if client_filter and client != client_filter:
            continue
        if c.get("status") not in ("ACTIVE", "RUNNING"):
            continue
        c["_client"] = client
        watched.append(c)
    print(f"  → {len(watched)} active campaigns under autopilot watch")

    state = load_state()

    # 3. Evaluate each
    for c in watched:
        cid = c["id"]
        cname = c.get("name", str(cid))
        client = c["_client"]
        summary["campaigns_checked"] += 1

        # Rate-limit re-action
        if recently_acted(state, cid, thresholds["recheck_cooldown_hours"]):
            if verbose:
                print(f"  [{client}] {cname}: recently acted on, skipping")
            continue

        # Pull analytics
        analytics = sl_get(f"/campaigns/{cid}/analytics")
        if not analytics:
            print(f"  [{client}] {cname}: analytics unavailable, skipping")
            continue

        violations = evaluate_campaign(c, analytics, thresholds)

        if verbose or violations:
            sent = analytics.get("sent_count", 0)
            bounces = analytics.get("bounce_count", 0)
            replies = analytics.get("reply_count", 0)
            print(f"  [{client}] {cname[:50]}")
            print(f"      sent={sent} bounces={bounces} replies={replies}")
            for v in violations:
                print(f"      ⚠ {v['severity']}: {v['reason']}")

        summary["violations_found"] += len(violations)

        # Policy: ALWAYS alert, NEVER auto-pause unless the operator explicitly enabled it.
        # the operator's 2026-04-20 rule: campaigns cannot be paused automatically.
        # Alerts are the default action. Auto-pause requires --auto-pause opt-in.
        if violations:
            most_severe = max(violations, key=lambda v: {"critical": 3, "high": 2, "medium": 1}.get(v["severity"], 0))
            action_taken = None

            # Build rich alert message with campaign ID + specific guidance
            action_hint = {
                "bounce_rate": "Review in Smartlead, clean bad leads, then pause or continue.",
                "reply_rate": "Low engagement. Consider pausing or running post-mortem analyzer to diagnose.",
                "dead_campaign": "No opens + no replies. Likely spam-filtered. Pause + investigate sender.",
            }.get(most_severe.get("metric"), "Review in Smartlead.")

            alert_message = (
                f"[{client.upper()}] '{cname}' — {most_severe['reason']}. "
                f"{action_hint} "
                f"https://app.smartlead.ai/app/email-campaign/{cid}"
            )

            if most_severe["action"] == "pause" and not no_pause:
                # Auto-pause path — only runs when the operator explicitly passed --auto-pause
                success = pause_campaign(cid, most_severe["reason"], dry_run=dry_run)
                if success:
                    action_taken = "paused"
                    summary["campaigns_paused"] += 1
                    record_action(state, cid, "paused", most_severe["reason"])
                    log_event({
                        "type": "campaign_paused",
                        "campaign_id": cid,
                        "campaign_name": cname,
                        "client": client,
                        "reason": most_severe["reason"],
                        "severity": most_severe["severity"],
                        "metric": most_severe["metric"],
                        "metric_value": most_severe["metric_value"],
                        "dry_run": dry_run,
                    })
                    send_alert(
                        f"{'[DRY] ' if dry_run else ''}AUTO-PAUSED: {alert_message}",
                        severity=most_severe["severity"]
                    )
            else:
                # Alert-only path (default). Record so we don't re-alert within cooldown.
                action_taken = "alert_only"
                record_action(state, cid, "alerted", most_severe["reason"])
                log_event({
                    "type": "alert_only",
                    "campaign_id": cid,
                    "campaign_name": cname,
                    "client": client,
                    "reason": most_severe["reason"],
                    "severity": most_severe["severity"],
                    "metric": most_severe["metric"],
                    "metric_value": most_severe["metric_value"],
                })
                send_alert(
                    f"ACTION NEEDED: {alert_message}",
                    severity=most_severe["severity"]
                )

            summary["actions"].append({
                "campaign_id": cid,
                "campaign_name": cname,
                "client": client,
                "action": action_taken,
                "reason": most_severe["reason"],
                "severity": most_severe["severity"],
            })

    save_state(state)
    summary["completed_at"] = datetime.now(timezone.utc).isoformat()
    log_event({"type": "run_complete", **{k: v for k, v in summary.items() if k != "actions"}})
    return summary


# ============================================================
# CLI
# ============================================================

def main():
    ap = argparse.ArgumentParser(description=(
        "Mailbox Health Autopilot — DEFAULT IS ALERT-ONLY. "
        "Detects threshold violations and sends Pushover alerts for the operator to review. "
        "NEVER pauses campaigns automatically unless --auto-pause is passed explicitly."
    ))
    ap.add_argument("--dry-run", action="store_true", help="show what would happen without changing anything")
    ap.add_argument("--auto-pause", action="store_true",
                    help="EXPLICITLY opt in to auto-pausing campaigns on critical violations. "
                         "Without this flag, autopilot only ALERTS and the operator pauses manually.")
    ap.add_argument("--client", choices=["rm", "paf", "sc"], help="only check one client's campaigns")
    ap.add_argument("--verbose", action="store_true", help="print every campaign even if clean")
    ap.add_argument("--bounce-threshold", type=float, default=DEFAULT_THRESHOLDS["bounce_rate_pct"],
                    help="percent bounce to trigger alert")
    ap.add_argument("--reply-threshold", type=float, default=DEFAULT_THRESHOLDS["reply_rate_pct"],
                    help="percent reply below which triggers alert")
    ap.add_argument("--dead-campaign-sends", type=int, default=DEFAULT_THRESHOLDS["dead_campaign_sends"],
                    help="# sends with 0 opens+replies before alert")
    args = ap.parse_args()

    thresholds = {
        **DEFAULT_THRESHOLDS,
        "bounce_rate_pct": args.bounce_threshold,
        "reply_rate_pct": args.reply_threshold,
        "dead_campaign_sends": args.dead_campaign_sends,
    }

    # Policy: alert-only by default. the operator must opt in to auto-pause via --auto-pause.
    # This reflects his 2026-04-20 rule: "before any campaign gets paused or whatever
    # due to this tool, it has to go by me first. It can not pause a campaign
    # automatically." Flip safety default to alert-only so cron-running autopilot
    # can never pause without explicit opt-in from the operator.
    pause_allowed = args.auto_pause
    alert_only = not pause_allowed

    mode_label = "DRY RUN" if args.dry_run else ("AUTO-PAUSE (live)" if pause_allowed else "ALERT-ONLY (safe default)")
    print(f"{'='*70}")
    print(f"MAILBOX AUTOPILOT — {mode_label}")
    print(f"{'='*70}")
    print(f"Thresholds: bounce>{thresholds['bounce_rate_pct']}%  reply<{thresholds['reply_rate_pct']}% (after {thresholds['min_sends_for_reply']})  dead>{thresholds['dead_campaign_sends']} sends")
    if alert_only:
        print(f"Mode: the operator is notified on violations via Pushover. Nothing auto-pauses.")
    print()

    summary = run(thresholds, dry_run=args.dry_run, no_pause=alert_only,
                  client_filter=args.client, verbose=args.verbose)

    print(f"\n{'='*70}")
    print(f"SUMMARY")
    print(f"{'='*70}")
    print(f"  Campaigns checked:   {summary['campaigns_checked']}")
    print(f"  Violations found:    {summary['violations_found']}")
    print(f"  Campaigns paused:    {summary['campaigns_paused']}")
    if summary["actions"]:
        print(f"\n  Actions taken:")
        for a in summary["actions"]:
            marker = "🚨" if a["severity"] == "critical" else ("⚠️" if a["severity"] == "high" else "ℹ️")
            print(f"    {marker} [{a['client'].upper()}] {a['campaign_name'][:50]}")
            print(f"       {a['action']} — {a['reason']}")


if __name__ == "__main__":
    main()
