"""
Mailbox pool helpers — enforces the 14-day warmup rule across all tools.

Hard rule (set 18-Apr-2026): a mailbox is NEVER assigned to a production
campaign until it is at least 14 days old AND at 100% warmup reputation.
Warmup percentage alone is unreliable — it can jump to 100% in 2-3 days
on low activity. The age check is the real safety net.

Use this module anywhere mailboxes get picked for campaigns. Do NOT write
ad-hoc filtering logic in individual scripts.

Usage:
    from mailbox_helpers import pick_mature_mailboxes, MailboxPoolError

    try:
        picks = pick_mature_mailboxes("client_c", count=5)
        for m in picks:
            print(f"{m['email']}  ({m['age_days']}d, {m['warmup_pct']}%)")
    except MailboxPoolError as e:
        print(f"Not enough mature mailboxes: {e}")
"""

import os
import sys
import requests
from datetime import datetime, timezone
from typing import Optional, Iterable

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
WORKSPACE_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, "..", ".."))

# Load .env (same pattern as forge.py / onboard.py)
for _p in (os.path.join(WORKSPACE_ROOT, ".env"), os.path.join(SCRIPT_DIR, ".env")):
    if os.path.isfile(_p):
        for line in open(_p):
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            k, v = k.strip(), v.strip().strip('"').strip("'")
            if k and not os.environ.get(k):
                os.environ[k] = v


# ──────────────────────────────────────────────────────────────────────────
# Config
# ──────────────────────────────────────────────────────────────────────────

MIN_AGE_DAYS = 14
MIN_WARMUP_PCT = 100
SMARTLEAD_BASE = "https://server.smartlead.ai/api/v1"

# Client → list of keyword fragments that match a mailbox's from_email.
# Keep in sync with onboard.py's classify() and forge_dashboard.py's audit.
CLIENT_KEYWORDS = {
    "client_c": ["client_c", "rmechanics", "gorevmechanics",
                         "tryrevmechanics", "getrevmechanics"],
    "client_a":   ["client_a", "preaction"],
    "client_b":  ["client_b", "securecreator"],
}


class MailboxPoolError(RuntimeError):
    """Raised when not enough eligible mailboxes exist for a request."""


# ──────────────────────────────────────────────────────────────────────────
# Smartlead API helpers
# ──────────────────────────────────────────────────────────────────────────

def _smartlead_key() -> str:
    key = os.environ.get("SMARTLEAD_API_KEY", "")
    if not key:
        raise RuntimeError("SMARTLEAD_API_KEY not set in environment or .env")
    return key


def fetch_all_mailboxes() -> list:
    """Paginate through Smartlead's email-accounts endpoint."""
    key = _smartlead_key()
    out, offset = [], 0
    while True:
        r = requests.get(
            f"{SMARTLEAD_BASE}/email-accounts/",
            params={"api_key": key, "limit": 100, "offset": offset},
            timeout=30,
        ).json()
        if not r:
            break
        out.extend(r)
        if len(r) < 100:
            break
        offset += 100
    return out


def fetch_campaign_mailbox_ids(campaign_id: int) -> set:
    """Mailbox ids currently assigned to a given campaign."""
    key = _smartlead_key()
    r = requests.get(
        f"{SMARTLEAD_BASE}/campaigns/{campaign_id}/email-accounts",
        params={"api_key": key},
        timeout=30,
    ).json()
    if not isinstance(r, list):
        return set()
    return {ea.get("email_account_id") or ea.get("id") for ea in r}


def fetch_assigned_mailbox_ids() -> set:
    """Any mailbox id currently assigned to ANY campaign (active or otherwise)."""
    key = _smartlead_key()
    campaigns = requests.get(
        f"{SMARTLEAD_BASE}/campaigns/",
        params={"api_key": key},
        timeout=30,
    ).json()
    if not isinstance(campaigns, list):
        campaigns = campaigns.get("data", [])

    assigned = set()
    for c in campaigns:
        ids = fetch_campaign_mailbox_ids(c["id"])
        assigned.update(ids)
    return assigned


# ──────────────────────────────────────────────────────────────────────────
# Maturity checks
# ──────────────────────────────────────────────────────────────────────────

def _age_days(acct: dict) -> int:
    """Days since mailbox was added to Smartlead. 0 if unparseable."""
    created = acct.get("created_at", "")
    try:
        dt = datetime.fromisoformat(created.replace("Z", "+00:00"))
        return (datetime.now(timezone.utc) - dt).days
    except Exception:
        return 0


def _warmup_pct(acct: dict) -> int:
    """Integer warmup reputation. 0 if unparseable."""
    w = acct.get("warmup_details") or {}
    rep = w.get("warmup_reputation", "0%")
    try:
        return int(rep.strip("%"))
    except (AttributeError, ValueError):
        return 0


def is_mature(acct: dict, min_age_days: int = MIN_AGE_DAYS,
              min_warmup_pct: int = MIN_WARMUP_PCT) -> bool:
    """Hard rule: mailbox must be at least min_age_days old AND at
    min_warmup_pct or higher reputation. Both required."""
    return _age_days(acct) >= min_age_days and _warmup_pct(acct) >= min_warmup_pct


def maturity_report(acct: dict) -> dict:
    """Human-readable snapshot of a single mailbox's maturity state."""
    age = _age_days(acct)
    pct = _warmup_pct(acct)
    return {
        "id": acct.get("id"),
        "email": acct.get("from_email", ""),
        "age_days": age,
        "warmup_pct": pct,
        "mature": age >= MIN_AGE_DAYS and pct >= MIN_WARMUP_PCT,
        "smtp_ok": bool(acct.get("is_smtp_success")),
        "imap_ok": bool(acct.get("is_imap_success")),
    }


# ──────────────────────────────────────────────────────────────────────────
# Client matching
# ──────────────────────────────────────────────────────────────────────────

def _matches_client(email: str, client_key: str) -> bool:
    """Case-insensitive substring match against CLIENT_KEYWORDS[client_key]."""
    email_lower = (email or "").lower()
    for kw in CLIENT_KEYWORDS.get(client_key, []):
        if kw in email_lower:
            return True
    return False


def filter_by_client(accts: Iterable[dict], client_key: str) -> list:
    """Filter a list of mailbox accounts to those matching the client's domains."""
    if client_key not in CLIENT_KEYWORDS:
        raise ValueError(
            f"Unknown client {client_key!r}. Known: {list(CLIENT_KEYWORDS)}"
        )
    return [a for a in accts if _matches_client(a.get("from_email", ""), client_key)]


# ──────────────────────────────────────────────────────────────────────────
# The main helper
# ──────────────────────────────────────────────────────────────────────────

def pick_mature_mailboxes(
    client_key: str,
    count: int,
    *,
    exclude_in_campaign: bool = True,
    min_age_days: int = MIN_AGE_DAYS,
    min_warmup_pct: int = MIN_WARMUP_PCT,
    verbose: bool = True,
) -> list:
    """Pick N mature mailboxes for a client, with the 14-day rule enforced.

    Args:
        client_key: one of "client_c", "client_a", "client_b"
        count: number of mailboxes needed
        exclude_in_campaign: if True (default) skip mailboxes already in ANY
            campaign. Set False to allow re-use across campaigns.
        min_age_days: minimum days since Smartlead creation. Default 14.
        min_warmup_pct: minimum warmup reputation. Default 100.
        verbose: print each picked mailbox with its age + warmup for eyeball check.

    Returns:
        List of mailbox dicts (raw Smartlead account objects), limited to `count`.

    Raises:
        MailboxPoolError: if fewer than `count` eligible mailboxes exist.
            The error message explains why — either too young, not 100%, or
            already in campaigns.
    """
    all_accts = fetch_all_mailboxes()
    client_accts = filter_by_client(all_accts, client_key)

    if exclude_in_campaign:
        assigned = fetch_assigned_mailbox_ids()
        client_accts = [a for a in client_accts if a["id"] not in assigned]

    # Partition into eligible and rejected, so error messages can explain the pool
    eligible, rejected = [], []
    for a in client_accts:
        age = _age_days(a)
        pct = _warmup_pct(a)
        if age >= min_age_days and pct >= min_warmup_pct:
            eligible.append(a)
        else:
            rejected.append({
                "email": a.get("from_email", ""),
                "age_days": age,
                "warmup_pct": pct,
                "reason": (
                    "too young" if age < min_age_days
                    else f"warmup {pct}% (need {min_warmup_pct}+)"
                ),
            })

    if len(eligible) < count:
        # Diagnostic detail for the error
        lines = [f"Need {count} mature {client_key} mailboxes, only {len(eligible)} eligible."]
        lines.append(f"  Rule: age >= {min_age_days}d AND warmup >= {min_warmup_pct}%.")
        if rejected:
            lines.append(f"  {len(rejected)} rejected:")
            for r in rejected[:10]:
                lines.append(f"    {r['email']}  ({r['age_days']}d, {r['warmup_pct']}%, {r['reason']})")
        raise MailboxPoolError("\n".join(lines))

    # Sort eligible by age descending (oldest first = most established reputation)
    eligible.sort(key=_age_days, reverse=True)
    picks = eligible[:count]

    if verbose:
        print(f"\n  Picked {count} mature {client_key} mailboxes:")
        for a in picks:
            print(f"    ✓ {a['from_email']}  ({_age_days(a)}d old, {_warmup_pct(a)}%)")

    return picks


# ──────────────────────────────────────────────────────────────────────────
# Pool stats (for dashboards / audits)
# ──────────────────────────────────────────────────────────────────────────

def get_pool_stats(client_key: Optional[str] = None) -> dict:
    """Summary of mature vs. young vs. in-campaign for one client or all.

    Returns dict keyed by client:
        {
          "client_c": {
            "total": 57, "mature": 30, "young": 2, "in_campaign": 30,
            "available_mature": 25
          },
          ...
        }
    """
    all_accts = fetch_all_mailboxes()
    assigned = fetch_assigned_mailbox_ids()

    clients = [client_key] if client_key else list(CLIENT_KEYWORDS.keys())
    out = {}
    for ck in clients:
        client_accts = filter_by_client(all_accts, ck)
        mature = [a for a in client_accts if is_mature(a)]
        young = [a for a in client_accts if not is_mature(a)]
        in_campaign = [a for a in client_accts if a["id"] in assigned]
        available_mature = [a for a in mature if a["id"] not in assigned]
        out[ck] = {
            "total": len(client_accts),
            "mature": len(mature),
            "young": len(young),
            "in_campaign": len(in_campaign),
            "available_mature": len(available_mature),
        }
    return out


# ──────────────────────────────────────────────────────────────────────────
# CLI — quick pool-check from terminal
# ──────────────────────────────────────────────────────────────────────────

def _cli():
    import argparse
    ap = argparse.ArgumentParser(description="Mailbox pool helper — 14-day maturity check.")
    ap.add_argument("--client", help="Client key (client_c/client_a/client_b)")
    ap.add_argument("--pick", type=int, help="Pick N mature mailboxes (dry-run preview only)")
    ap.add_argument("--stats", action="store_true", help="Show pool stats for all clients")
    args = ap.parse_args()

    if args.stats or (not args.pick and not args.client):
        stats = get_pool_stats(args.client)
        print("\nMAILBOX POOL STATS\n" + "=" * 60)
        for ck, s in stats.items():
            print(f"\n{ck}")
            print(f"  Total:            {s['total']}")
            print(f"  Mature (14d+):    {s['mature']}")
            print(f"  Too young:        {s['young']}")
            print(f"  In campaigns:     {s['in_campaign']}")
            print(f"  Available mature: {s['available_mature']}  ← safe to assign")
        return

    if args.pick:
        if not args.client:
            sys.exit("--pick requires --client")
        try:
            pick_mature_mailboxes(args.client, args.pick)
        except MailboxPoolError as e:
            print(f"\n⛔ {e}")
            sys.exit(1)


if __name__ == "__main__":
    _cli()
