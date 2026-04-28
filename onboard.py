#!/usr/bin/env python3
"""
Onboard — Client mailbox infrastructure automation.

Automates everything AFTER domain purchase at Porkbun:
  1. Import domains to InboxKit (NS delegation to InboxKit's Cloudflare)
  2. Update Porkbun nameservers to the Cloudflare NS InboxKit assigns
  3. Poll InboxKit propagation check
  4. Create mailboxes via /mailboxes/buy (2 per domain, GOOGLE or MICROSOFT)
  5. Export mailboxes to Smartlead sequencer via /sequencers/export
  6. Configure Smartlead warmup (time_to_wait, reply_rate, warmup_enabled)

NOT part of The Forge.

InboxKit enforces one platform per domain (all Google OR all Microsoft on a
single domain). Use --split-provider to diversify across the fleet.

Usage:
    python3 onboard.py --client client_b --domains domains.txt
    python3 onboard.py --client client_b --domains "d1.com,d2.com" --platform MICROSOFT
    python3 onboard.py --client client_c --domains batch.txt --split-provider
    python3 onboard.py --client client_a --check
    python3 onboard.py --audit
    python3 onboard.py --client client_b --dry-run --domains domains.txt

    # Expansion runs also work — existing domains are untouched by the NS import
    # (InboxKit recognizes domains already in the workspace and skips them).
"""

import os
import sys
import json
import time
import argparse
import subprocess
import requests
from datetime import datetime, timezone

# ==============================================================================
# PATHS & ENV
# ==============================================================================

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
WORKSPACE_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, "..", ".."))
PROJECTS_DIR = os.path.join(WORKSPACE_ROOT, "01-Projects")
LOG_FILE = os.path.join(SCRIPT_DIR, "onboard_log.json")

try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(WORKSPACE_ROOT, ".env"), override=True)
    load_dotenv(os.path.join(SCRIPT_DIR, ".env"), override=True)
except ImportError:
    pass

# ==============================================================================
# API CONFIG
# ==============================================================================

PORKBUN_API_KEY = os.environ.get("PORKBUN_API_KEY", "")
PORKBUN_SECRET_KEY = os.environ.get("PORKBUN_SECRET_KEY", "")
PORKBUN_BASE = "https://api.porkbun.com/api/json/v3"

INBOXKIT_API_KEY = os.environ.get("INBOXKIT_API_KEY", "")
INBOXKIT_BASE = "https://api.inboxkit.com/v1/api"
INBOXKIT_WORKSPACE = ""  # populated at runtime

SMARTLEAD_API_KEY = os.environ.get("SMARTLEAD_API_KEY", "")
SMARTLEAD_BASE = "https://server.smartlead.ai/api/v1"


# ==============================================================================
# API HELPERS
# ==============================================================================

def porkbun_post(endpoint, extra_data=None):
    """POST to Porkbun API with auth."""
    url = f"{PORKBUN_BASE}{endpoint}"
    data = {"apikey": PORKBUN_API_KEY, "secretapikey": PORKBUN_SECRET_KEY}
    if extra_data:
        data.update(extra_data)
    try:
        r = requests.post(url, json=data, timeout=30)
        return r.json()
    except Exception as e:
        print(f"  ⚠️  Porkbun request failed: {e}")
        return None


def porkbun_update_nameservers(domain, nameservers):
    """Update nameservers for a Porkbun domain. `nameservers` is a list of hosts."""
    # Porkbun expects {ns: [...]} in the body
    return porkbun_post(f"/domain/updateNs/{domain}", {"ns": nameservers})


def inboxkit_get(endpoint, params=None):
    """GET to InboxKit API."""
    url = f"{INBOXKIT_BASE}{endpoint}"
    headers = {"Authorization": f"Bearer {INBOXKIT_API_KEY}"}
    if INBOXKIT_WORKSPACE:
        headers["X-Workspace-Id"] = INBOXKIT_WORKSPACE
    try:
        r = requests.get(url, headers=headers, params=params, timeout=30)
        if r.status_code == 200:
            return r.json()
        print(f"  ⚠️  InboxKit GET {endpoint}: {r.status_code}")
        return None
    except Exception as e:
        print(f"  ⚠️  InboxKit request failed: {e}")
        return None


def inboxkit_post(endpoint, data):
    """POST to InboxKit API."""
    url = f"{INBOXKIT_BASE}{endpoint}"
    headers = {
        "Authorization": f"Bearer {INBOXKIT_API_KEY}",
        "Content-Type": "application/json",
    }
    if INBOXKIT_WORKSPACE:
        headers["X-Workspace-Id"] = INBOXKIT_WORKSPACE
    try:
        r = requests.post(url, json=data, headers=headers, timeout=60)
        return r.json()
    except Exception as e:
        print(f"  ⚠️  InboxKit POST {endpoint} failed: {e}")
        return None


def smartlead_get(endpoint, **params):
    """GET from Smartlead API."""
    url = f"{SMARTLEAD_BASE}{endpoint}"
    params["api_key"] = SMARTLEAD_API_KEY
    try:
        r = requests.get(url, params=params, timeout=30)
        if r.status_code == 200:
            return r.json()
        if r.status_code == 429:
            time.sleep(2)
            r = requests.get(url, params=params, timeout=30)
            return r.json() if r.status_code == 200 else None
        return None
    except Exception as e:
        print(f"  ⚠️  Smartlead request failed: {e}")
        return None


def smartlead_post(endpoint, data):
    """POST to Smartlead API."""
    url = f"{SMARTLEAD_BASE}{endpoint}"
    try:
        r = requests.post(f"{url}?api_key={SMARTLEAD_API_KEY}", json=data, timeout=30)
        if r.status_code == 200:
            return r.json()
        if r.status_code == 429:
            time.sleep(2)
            r = requests.post(f"{url}?api_key={SMARTLEAD_API_KEY}", json=data, timeout=30)
            return r.json() if r.status_code == 200 else None
        return None
    except Exception as e:
        print(f"  ⚠️  Smartlead POST failed: {e}")
        return None


# ==============================================================================
# PHASE 0: CLIENT FOLDER SETUP
# ==============================================================================

def setup_client_folder(client_name):
    """Create client project folder with CLIENT.md if it doesn't exist."""
    client_dir = os.path.join(PROJECTS_DIR, client_name)
    client_md = os.path.join(client_dir, "CLIENT.md")
    leads_dir = os.path.join(client_dir, "lead-runs")

    if os.path.exists(client_md):
        print(f"  ✓ Client folder exists: {client_dir}")
        return client_dir

    print(f"  → Creating client folder: {client_dir}")
    os.makedirs(leads_dir, exist_ok=True)

    template = f"""# {client_name.title()}

## About
[Client description here]

## Forge Brief
```yaml
sender: {client_name.title()}
service: [Service description]
target_audience: [Target audience]
cta: [Call to action]
usp: [Unique selling proposition]
geography: [Geography]
```

## Lead Pipeline
Lead generation runs stored in `lead-runs/`.

## Notes
- Created: {datetime.now().strftime('%Y-%m-%d')}
"""
    with open(client_md, "w") as f:
        f.write(template)
    print(f"  ✓ Created CLIENT.md (fill in the details)")
    return client_dir


def load_client_brief(client_name):
    """Load sender personas from CLIENT.md."""
    import re

    client_md = os.path.join(PROJECTS_DIR, client_name, "CLIENT.md")
    if not os.path.exists(client_md):
        return None
    with open(client_md) as f:
        content = f.read()

    # Extract sender name
    sender_match = re.search(r"sender:\s*(.+)", content)
    sender = sender_match.group(1).strip() if sender_match else client_name.title()

    # Extract personas from YAML block
    personas = []
    persona_block = re.findall(
        r"-\s*first_name:\s*(.+)\n\s*last_name:\s*(.+)\n\s*username:\s*(.+)",
        content,
    )
    for first, last, username in persona_block:
        personas.append({
            "first_name": first.strip(),
            "last_name": last.strip(),
            "username": username.strip(),
        })

    if not personas:
        # Fallback: generate from sender name
        parts = sender.split()
        if len(parts) >= 2:
            fn, ln = parts[0], parts[-1]
            personas = [
                {"first_name": fn, "last_name": ln, "username": f"{fn.lower()}.{ln.lower()}"},
                {"first_name": fn, "last_name": ln, "username": fn.lower()},
            ]
        else:
            personas = [
                {"first_name": sender, "last_name": "", "username": sender.lower()},
            ]

    return {"sender": sender, "personas": personas, "client_md_path": client_md}


# ==============================================================================
# PHASE 1: PARSE & VALIDATE
# ==============================================================================

def parse_domains(domains_arg):
    """Parse domains from file path or comma-separated string."""
    if os.path.isfile(domains_arg):
        with open(domains_arg) as f:
            domains = [line.strip().lower() for line in f if line.strip() and not line.startswith("#")]
    else:
        domains = [d.strip().lower() for d in domains_arg.split(",") if d.strip()]

    # Clean domains
    cleaned = []
    for d in domains:
        d = d.replace("https://", "").replace("http://", "").replace("www.", "").strip("/")
        if "." in d:
            cleaned.append(d)
    return cleaned


def validate_apis():
    """Validate all 3 API connections. Returns dict of results."""
    global INBOXKIT_WORKSPACE
    results = {"porkbun": False, "inboxkit": False, "smartlead": False}

    # Porkbun
    if PORKBUN_API_KEY and PORKBUN_SECRET_KEY:
        resp = porkbun_post("/ping")
        if resp and resp.get("status") == "SUCCESS":
            results["porkbun"] = True
            print(f"  ✓ Porkbun: connected (IP: {resp.get('yourIp', '?')})")
        else:
            print(f"  ✗ Porkbun: auth failed")
    else:
        print(f"  ✗ Porkbun: missing API keys")

    # InboxKit
    if INBOXKIT_API_KEY:
        resp = inboxkit_get("/workspaces/list")
        if resp and not resp.get("error"):
            workspaces = resp.get("workspaces", [])
            if workspaces:
                INBOXKIT_WORKSPACE = workspaces[0].get("uid", "")
                results["inboxkit"] = True
                print(f"  ✓ InboxKit: connected (workspace: {workspaces[0].get('name', '?')}, {workspaces[0].get('domains', 0)} domains)")
            else:
                print(f"  ✗ InboxKit: no workspaces found")
        else:
            print(f"  ✗ InboxKit: auth failed")
    else:
        print(f"  ✗ InboxKit: missing API key")

    # Smartlead
    if SMARTLEAD_API_KEY:
        resp = smartlead_get("/email-accounts/", limit=1)
        if resp is not None:
            results["smartlead"] = True
            print(f"  ✓ Smartlead: connected")
        else:
            print(f"  ✗ Smartlead: auth failed")
    else:
        print(f"  ✗ Smartlead: missing API key")

    return results


def validate_domain_ownership(domains):
    """Check which domains are owned via Porkbun."""
    resp = porkbun_post("/domain/listAll", {"start": 0})
    if not resp or resp.get("status") != "SUCCESS":
        print(f"  ⚠️  Could not fetch domain list from Porkbun")
        return domains, []

    owned = {d["domain"].lower() for d in resp.get("domains", [])}
    valid = [d for d in domains if d in owned]
    missing = [d for d in domains if d not in owned]

    if missing:
        print(f"  ⚠️  {len(missing)} domains NOT found in Porkbun:")
        for d in missing[:10]:
            print(f"      {d}")

    return valid, missing


# ==============================================================================
# PHASE 2: DOMAIN IMPORT TO INBOXKIT (nameserver delegation)
# ==============================================================================
# InboxKit manages DNS via Cloudflare. To import an externally-registered
# domain (Porkbun/Namecheap/etc.), we:
#   1. POST /v1/api/domains/nameservers — InboxKit assigns Cloudflare NS
#   2. Update the domain's NS at the registrar (Porkbun) to the assigned NS
#   3. Poll /v1/api/domains/nameservers/check-propagation until active
# After propagation, InboxKit owns DNS (MX, SPF, DKIM, DMARC) and can
# provision mailboxes via /v1/api/mailboxes/buy.

def get_existing_dns(domain):
    """Retrieve existing DNS records for a domain (legacy helper, kept for audits)."""
    resp = porkbun_post(f"/dns/retrieve/{domain}")
    if resp and resp.get("status") == "SUCCESS":
        return resp.get("records", [])
    return []


def has_record(records, rtype, content_match=None):
    """Check if a DNS record type already exists (legacy helper)."""
    for r in records:
        if r.get("type") == rtype:
            if content_match is None:
                return True
            if content_match in r.get("content", ""):
                return True
    return False


def import_domains_to_inboxkit(domains, dry_run=False):
    """Register domains with InboxKit and delegate NS from Porkbun → Cloudflare.

    Returns: (imported, ns_updated, propagated, failed)
      - imported: list of dicts {domain, uid, nameservers}
      - ns_updated: list of domains whose NS was set on Porkbun
      - propagated: list of domains whose propagation check passed
      - failed: list of domains that errored anywhere in the flow
    """
    imported, ns_updated, propagated, failed = [], [], [], []

    # Step 1: ask InboxKit to allocate Cloudflare NS for each domain
    if dry_run:
        for d in domains:
            imported.append({"domain": d, "uid": "dry-run", "nameservers": ["ns1.cloudflare.com", "ns2.cloudflare.com"]})
            ns_updated.append(d)
            propagated.append(d)
        return imported, ns_updated, propagated, failed

    print(f"  → Requesting InboxKit nameservers for {len(domains)} domain(s)...")
    resp = inboxkit_post("/domains/nameservers", {"domains": domains, "mask_forwarding": False})
    if not resp or resp.get("error"):
        msg = (resp or {}).get("message", "no response")
        print(f"    ⛔ /domains/nameservers failed: {msg}")
        return imported, ns_updated, propagated, list(domains)

    for row in resp.get("result", []):
        imported.append({
            "domain": row.get("domain") or row.get("name"),
            "uid": row.get("uid"),
            "nameservers": row.get("nameservers", []),
        })

    # Step 2: update each domain's nameservers at Porkbun
    print(f"  → Updating Porkbun nameservers for {len(imported)} domain(s)...")
    for item in imported:
        d = item["domain"]
        ns = item["nameservers"]
        if not ns:
            print(f"    ⚠️  {d}: no nameservers assigned by InboxKit")
            failed.append(d)
            continue
        r = porkbun_update_nameservers(d, ns)
        if r and r.get("status") == "SUCCESS":
            print(f"    ✓ {d} → {', '.join(ns)}")
            ns_updated.append(d)
        else:
            msg = (r or {}).get("message", "no response")
            print(f"    ⚠️  {d} NS update failed: {msg}")
            failed.append(d)

    # Step 3: poll propagation — InboxKit reports status via check-propagation
    if ns_updated:
        print(f"\n  → Polling InboxKit propagation check (up to 10 min)...")
        max_wait = 600
        interval = 30
        elapsed = 0
        while elapsed < max_wait:
            r = inboxkit_post("/domains/nameservers/check-propagation", {"domains": ns_updated})
            if r and not r.get("error"):
                rows = r.get("result", [])
                prop_now = [x.get("name") for x in rows if x.get("propagated")]
                print(f"    {len(prop_now)}/{len(ns_updated)} propagated…")
                if len(prop_now) == len(ns_updated):
                    propagated = prop_now
                    break
            time.sleep(interval)
            elapsed += interval
        else:
            # Take whatever propagated within the window, leave the rest queued
            r = inboxkit_post("/domains/nameservers/check-propagation", {"domains": ns_updated})
            if r and not r.get("error"):
                propagated = [x.get("name") for x in r.get("result", []) if x.get("propagated")]
            not_prop = [d for d in ns_updated if d not in propagated]
            if not_prop:
                print(f"    ⚠️  {len(not_prop)} still propagating: {not_prop}")

    return imported, ns_updated, propagated, failed


# ==============================================================================
# PHASE 3: MAILBOX CREATION (InboxKit)
# ==============================================================================

def get_or_create_sequencer(preferred_name="SmartLead Account"):
    """Find existing Smartlead sequencer (by platform=smartlead) or create one.

    Returns the sequencer UID, or None on failure.
    """
    # Step 1: try to find an existing active Smartlead sequencer
    resp = inboxkit_post("/sequencers/list", {})
    if resp and not resp.get("error"):
        for s in resp.get("data", []) or []:
            if s.get("platform") == "smartlead" and s.get("status") == "active":
                print(f"  ✓ Using existing sequencer: {s.get('name')} ({s.get('uid')[:8]}...)")
                return s.get("uid")

    # Step 2: create one
    data = {
        "name": preferred_name,
        "platform": "smartlead",
        "api_key": SMARTLEAD_API_KEY,
        "sequencer_login": "",
        "sequencer_password": "",
        "enable_warmup": True,
        "warmup_replyrate": 55,
    }
    resp = inboxkit_post("/sequencers/add", data)
    if resp and not resp.get("error") and resp.get("uid"):
        print(f"  ✓ Sequencer created: {resp['uid']}")
        return resp["uid"]
    msg = (resp or {}).get("message", "no response")
    print(f"  ⚠️  Sequencer setup: {msg}")
    return None


def create_mailboxes(domains, sender_info, sequencer_uid=None, dry_run=False,
                     platform="GOOGLE", platform_map=None):
    """Create N mailboxes per domain via InboxKit /mailboxes/buy.

    InboxKit enforces one platform per domain. Two modes:
      - platform="GOOGLE" or "MICROSOFT" — all domains use that platform
      - platform_map={domain: "GOOGLE"|"MICROSOFT"} — per-domain override

    After /mailboxes/buy, all new mailbox UIDs are exported to the Smartlead
    sequencer in one call so InboxKit pushes them as provisioning completes.
    """
    personas = sender_info.get("personas", [])
    if not personas:
        personas = [{"first_name": "Agent", "last_name": "", "username": "agent"}]

    results = {"created": 0, "failed": 0, "details": [], "mailbox_uids": [],
               "expected": []}

    def plat_for(d):
        if platform_map and d in platform_map:
            return platform_map[d].upper()
        return platform.upper()

    # Build payload
    mailboxes = []
    for domain in domains:
        for persona in personas:
            mailboxes.append({
                "domain_name": domain,
                "username": persona["username"],
                "first_name": persona["first_name"],
                "last_name": persona["last_name"],
                "platform": plat_for(domain),
            })

    # Always emit the expected list so Phase 3b recovery knows what to look for
    results["expected"] = [
        {"email": f"{mb['username']}@{mb['domain_name']}", "platform": mb["platform"]}
        for mb in mailboxes
    ]

    if dry_run:
        for mb in mailboxes:
            results["details"].append(
                f"[DRY RUN] {mb['platform']:<10} {mb['username']}@{mb['domain_name']}"
            )
        results["created"] = len(mailboxes)
        return results

    if not mailboxes:
        return results

    print(f"  → Submitting {len(mailboxes)} mailboxes to InboxKit /mailboxes/buy...")
    resp = inboxkit_post("/mailboxes/buy",
                         {"mailboxes": mailboxes, "use_wallet_balance": True})
    if not resp or resp.get("error"):
        msg = (resp or {}).get("message", "no response")
        print(f"    ⛔ /mailboxes/buy failed: {msg}")
        for mb in mailboxes:
            results["details"].append(
                f"✗ {mb['platform']:<10} {mb['username']}@{mb['domain_name']} — {msg}"
            )
            results["failed"] += 1
        return results

    created_mbs = resp.get("mailboxes", []) or []
    for mb in created_mbs:
        uid = mb.get("uid")
        if uid:
            results["mailbox_uids"].append(uid)
        results["created"] += 1
        results["details"].append(
            f"✓ {mb.get('platform','?'):<10} {mb.get('username')}@{mb.get('domain_name')} "
            f"[{mb.get('status','?')}]"
        )

    # Export to Smartlead sequencer so InboxKit pushes them as they provision
    if sequencer_uid and results["mailbox_uids"]:
        print(f"  → Exporting {len(results['mailbox_uids'])} mailboxes to Smartlead sequencer...")
        exp = inboxkit_post("/sequencers/export", {
            "sequencer_uid": sequencer_uid,
            "mailbox_uids": results["mailbox_uids"],
        })
        if exp and not exp.get("error"):
            res = exp.get("results", {})
            print(f"    ✓ {res.get('new_exports_created', 0)} new, "
                  f"{res.get('duplicate_exports_skipped', 0)} duplicates")
        else:
            msg = (exp or {}).get("message", "no response")
            print(f"    ⚠️  Export failed: {msg}")

    return results


# ==============================================================================
# PHASE 3b: STRAGGLER RECOVERY
# ==============================================================================
# InboxKit pushes mailboxes into Smartlead via browser automation (Playwright
# against Smartlead's "Connect Mailbox" UI). This flakes on 5–15% of mailboxes
# — Playwright selector timeouts, Microsoft IMAP basic-auth failures, etc.
# When that happens, InboxKit's retry loop gives up and the mailbox never
# lands in Smartlead.
#
# Recovery: pull the mailbox creds from /mailboxes/show-credentials and POST
# direct to Smartlead's /email-accounts/save with the right SMTP/IMAP config.
# This bypasses the Playwright flow entirely.

GOOGLE_SMTP_CONF = {
    "smtp_host": "smtp.gmail.com", "smtp_port": 465,
    "imap_host": "imap.gmail.com", "imap_port": 993,
}
MICROSOFT_SMTP_CONF = {
    "smtp_host": "smtp.office365.com", "smtp_port": 587,
    "imap_host": "outlook.office365.com", "imap_port": 993,
}


def find_smartlead_mailboxes(domains):
    """Return set of emails currently in Smartlead whose domain is in `domains`."""
    accts = get_all_smartlead_accounts()
    dom_set = {d.lower() for d in domains}
    found = set()
    for a in accts or []:
        email = (a.get("from_email") or "").lower()
        if "@" in email and email.split("@", 1)[1] in dom_set:
            found.add(email)
    return found


def fetch_mailbox_credentials(email):
    """Get (password, app_password, secret) from InboxKit for a mailbox."""
    resp = inboxkit_get("/mailboxes/show-credentials", params={"email": email})
    if not resp or resp.get("error"):
        return None, None, None
    return (resp.get("password") or "",
            resp.get("app_password") or "",
            resp.get("secret") or "")


def smartlead_add_direct(email, platform, password, from_name=None):
    """POST an email account direct to Smartlead, bypassing InboxKit's UI flow."""
    conf = GOOGLE_SMTP_CONF if platform.upper() == "GOOGLE" else MICROSOFT_SMTP_CONF
    payload = {
        "from_name": from_name or email.split("@", 1)[0].replace(".", " ").title(),
        "from_email": email,
        "user_name": email,
        "password": password,
        "smtp_host": conf["smtp_host"],
        "smtp_port": conf["smtp_port"],
        "imap_host": conf["imap_host"],
        "imap_port": conf["imap_port"],
        "max_email_per_day": 20,
        "warmup_enabled": True,
        "total_warmup_per_day": 30,
        "daily_rampup": 5,
        "reply_rate_percentage": 55,
    }
    try:
        r = requests.post(f"{SMARTLEAD_BASE}/email-accounts/save?api_key={SMARTLEAD_API_KEY}",
                          json=payload, timeout=30)
        if r.status_code == 200 and not r.json().get("error"):
            return True, r.json().get("message", "")
        return False, (r.text[:300] if r.status_code != 200 else r.json().get("message", "unknown"))
    except Exception as e:
        return False, str(e)


def recover_stragglers(expected, wait_min=5, dry_run=False):
    """Wait `wait_min` for InboxKit sync, then direct-add any missing mailboxes.

    `expected`: list of dicts {email, platform} for every mailbox we provisioned.
    Returns: dict with counts + list of (email, result).
    """
    results = {"arrived": 0, "recovered": 0, "failed": 0, "details": []}
    if not expected or dry_run:
        return results

    domains = list({e["email"].split("@", 1)[1] for e in expected})

    print(f"  → Waiting {wait_min} min for InboxKit → Smartlead sync...")
    max_wait = wait_min * 60
    interval = 30
    elapsed = 0
    last_count = 0
    while elapsed < max_wait:
        in_sl = find_smartlead_mailboxes(domains)
        expected_set = {e["email"].lower() for e in expected}
        arrived = expected_set & in_sl
        if len(arrived) != last_count:
            print(f"    {len(arrived)}/{len(expected)} in Smartlead…")
            last_count = len(arrived)
        if len(arrived) == len(expected):
            results["arrived"] = len(arrived)
            return results
        time.sleep(interval)
        elapsed += interval

    # Who's missing?
    in_sl = find_smartlead_mailboxes(domains)
    missing = [e for e in expected if e["email"].lower() not in in_sl]
    results["arrived"] = len(expected) - len(missing)

    if not missing:
        return results

    print(f"\n  → {len(missing)} mailbox(es) stuck on InboxKit's Playwright export.")
    print(f"    Fetching creds from /mailboxes/show-credentials and pushing direct...")
    for mb in missing:
        email = mb["email"]
        platform = mb["platform"]
        password, app_password, _ = fetch_mailbox_credentials(email)
        pw = app_password if (platform.upper() == "GOOGLE" and app_password) else password
        if not pw:
            results["failed"] += 1
            results["details"].append((email, "no credentials from InboxKit"))
            print(f"      ✗ {email}: no usable password")
            continue
        ok, msg = smartlead_add_direct(email, platform, pw)
        if ok:
            results["recovered"] += 1
            results["details"].append((email, "recovered"))
            print(f"      ✓ {email} → direct-added to Smartlead")
        else:
            results["failed"] += 1
            results["details"].append((email, msg[:100]))
            print(f"      ✗ {email}: {msg[:150]}")

    return results


# ==============================================================================
# PHASE 4: SMARTLEAD WARMUP CONFIG
# ==============================================================================

def get_all_smartlead_accounts():
    """Fetch all Smartlead email accounts with pagination."""
    all_accounts = []
    offset = 0
    while True:
        resp = smartlead_get("/email-accounts/", limit=100, offset=offset)
        if not resp:
            break
        all_accounts.extend(resp)
        if len(resp) < 100:
            break
        offset += 100
    return all_accounts


def configure_smartlead_warmup(domains, dry_run=False):
    """Find new mailboxes in Smartlead and configure warmup settings."""
    accounts = get_all_smartlead_accounts()
    domain_set = set(d.lower() for d in domains)

    # Find accounts matching our domains
    matching = []
    for a in accounts:
        email = a.get("from_email", "").lower()
        domain = email.split("@")[1] if "@" in email else ""
        if domain in domain_set:
            matching.append(a)

    if not matching:
        print(f"  ⚠️  No matching mailboxes found in Smartlead yet")
        print(f"      InboxKit may still be provisioning. Check back in 10-30 min.")
        return {"configured": 0, "total": 0}

    configured = 0
    for a in matching:
        aid = a["id"]
        email = a["from_email"]

        if dry_run:
            print(f"    [DRY RUN] Would configure: {email}")
            configured += 1
            continue

        # Set time between sends
        r1 = requests.post(
            f"{SMARTLEAD_BASE}/email-accounts/{aid}?api_key={SMARTLEAD_API_KEY}",
            json={"time_to_wait_in_mins": 20}
        )

        # Set warmup
        r2 = requests.post(
            f"{SMARTLEAD_BASE}/email-accounts/{aid}/warmup?api_key={SMARTLEAD_API_KEY}",
            json={"warmup_enabled": True, "reply_rate_percentage": 55}
        )

        ok1 = r1.status_code == 200
        ok2 = r2.status_code == 200

        if ok1 and ok2:
            configured += 1
            print(f"    ✓ {email} — 20min wait, 55% reply rate, warmup ON")
        else:
            print(f"    ⚠️  {email} — partial config (wait={ok1}, warmup={ok2})")

    return {"configured": configured, "total": len(matching)}


# ==============================================================================
# PHASE 5: VERIFY & REPORT
# ==============================================================================

def verify_setup(domains):
    """Verify all mailboxes are healthy."""
    accounts = get_all_smartlead_accounts()
    domain_set = set(d.lower() for d in domains)

    matching = []
    for a in accounts:
        email = a.get("from_email", "").lower()
        domain = email.split("@")[1] if "@" in email else ""
        if domain in domain_set:
            matching.append(a)

    report = {
        "total_mailboxes": len(matching),
        "smtp_ok": 0,
        "imap_ok": 0,
        "warmup_active": 0,
        "issues": [],
    }

    for a in matching:
        email = a["from_email"]
        if a.get("is_smtp_success"):
            report["smtp_ok"] += 1
        else:
            report["issues"].append(f"SMTP broken: {email}")
        if a.get("is_imap_success"):
            report["imap_ok"] += 1
        else:
            report["issues"].append(f"IMAP broken: {email}")
        w = a.get("warmup_details") or {}
        if w.get("status") == "ACTIVE":
            report["warmup_active"] += 1
        else:
            report["issues"].append(f"Warmup not active: {email} ({w.get('status', '?')})")

    return report


def save_onboard_log(client_name, domains, report):
    """Save onboarding record to log file."""
    entry = {
        "client": client_name,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "domains": domains,
        "report": report,
    }

    log = []
    if os.path.exists(LOG_FILE):
        with open(LOG_FILE) as f:
            try:
                log = json.load(f)
            except json.JSONDecodeError:
                log = []

    log.append(entry)
    with open(LOG_FILE, "w") as f:
        json.dump(log, f, indent=2)


# ==============================================================================
# AUDIT MODE
# ==============================================================================

def audit_all():
    """Health check all clients across InboxKit + Smartlead."""
    print("\n" + "=" * 60)
    print("MAILBOX AUDIT — All Clients")
    print("=" * 60)

    accounts = get_all_smartlead_accounts()

    # Classify by client
    from collections import defaultdict, Counter
    by_client = defaultdict(list)
    for a in accounts:
        email = (a.get("from_email") or "").lower()
        if "client_c" in email or "rmechanics" in email or "gorevmechanics" in email or "tryrevmechanics" in email or "getrevmechanics" in email:
            by_client["ClientC"].append(a)
        elif "client_a" in email or "preaction" in email:
            by_client["ClientA"].append(a)
        elif "client_b" in email or "secure" in email:
            by_client["ClientB"].append(a)
        else:
            by_client["Other"].append(a)

    for client, accts in by_client.items():
        print(f"\n--- {client} ({len(accts)} mailboxes) ---")

        smtp_ok = sum(1 for a in accts if a.get("is_smtp_success"))
        imap_ok = sum(1 for a in accts if a.get("is_imap_success"))

        warmup_active = 0
        warmup_scores = []
        in_campaign = 0
        for a in accts:
            w = a.get("warmup_details") or {}
            if w.get("status") == "ACTIVE":
                warmup_active += 1
            rep = w.get("warmup_reputation", "0%")
            try:
                warmup_scores.append(int(rep.strip("%")))
            except (ValueError, AttributeError):
                warmup_scores.append(0)
            if a.get("campaign_count", 0) > 0:
                in_campaign += 1

        available = len(accts) - in_campaign
        avg_warmup = sum(warmup_scores) / len(warmup_scores) if warmup_scores else 0
        low_warmup = sum(1 for s in warmup_scores if s < 90)

        # Check settings
        wait_times = Counter(a.get("minTimeToWaitInMins") for a in accts)
        reply_rates = Counter((a.get("warmup_details") or {}).get("reply_rate") for a in accts)
        daily_limits = Counter(a.get("message_per_day") for a in accts)

        print(f"  SMTP:        {smtp_ok}/{len(accts)} connected")
        print(f"  IMAP:        {imap_ok}/{len(accts)} connected")
        print(f"  Warmup:      {warmup_active}/{len(accts)} active")
        print(f"  Avg warmup:  {avg_warmup:.0f}% (low <90%: {low_warmup})")
        print(f"  In campaign: {in_campaign} | Available: {available}")
        print(f"  Wait time:   {dict(wait_times)}")
        print(f"  Reply rate:  {dict(reply_rates)}")
        print(f"  Daily limit: {dict(daily_limits)}")

        # Flag issues
        issues = []
        if smtp_ok < len(accts):
            issues.append(f"{len(accts) - smtp_ok} SMTP broken")
        if imap_ok < len(accts):
            issues.append(f"{len(accts) - imap_ok} IMAP broken")
        if warmup_active < len(accts):
            issues.append(f"{len(accts) - warmup_active} warmup not active")
        if low_warmup > 0:
            issues.append(f"{low_warmup} below 90% warmup")

        if issues:
            print(f"  ⚠️  Issues: {', '.join(issues)}")
        else:
            print(f"  ✅ All healthy")

    print(f"\n{'=' * 60}")
    print(f"Total: {len(accounts)} mailboxes across {len(by_client)} clients")


def check_client(client_name):
    """Check existing setup for a specific client."""
    print(f"\n{'=' * 60}")
    print(f"CLIENT CHECK — {client_name}")
    print(f"{'=' * 60}")

    accounts = get_all_smartlead_accounts()
    client_lower = client_name.lower()

    matching = [a for a in accounts if client_lower in (a.get("from_email") or "").lower()]

    if not matching:
        print(f"  No mailboxes found for '{client_name}'")
        return

    # Collect unique domains
    domains = set()
    for a in matching:
        email = a.get("from_email", "")
        if "@" in email:
            domains.add(email.split("@")[1])

    print(f"  Domains: {len(domains)}")
    print(f"  Mailboxes: {len(matching)}")
    print(f"  Active/backup: {len(domains)} domains × {len(matching) // max(len(domains), 1)} mailboxes/domain")

    for domain in sorted(domains):
        domain_accounts = [a for a in matching if domain in (a.get("from_email") or "")]
        statuses = []
        for a in domain_accounts:
            w = a.get("warmup_details") or {}
            rep = w.get("warmup_reputation", "?")
            camp = a.get("campaign_count", 0)
            smtp = "✓" if a.get("is_smtp_success") else "✗"
            statuses.append(f"{a['from_email']} | smtp={smtp} warmup={rep} campaigns={camp}")
        print(f"\n  {domain}:")
        for s in statuses:
            print(f"    {s}")


# ==============================================================================
# MAIN ONBOARDING FLOW
# ==============================================================================

def onboard(client_name, domains, dry_run=False, platform="GOOGLE", split_provider=False):
    """Run the full onboarding pipeline.

    platform: 'GOOGLE' or 'MICROSOFT' (InboxKit enforces one platform per domain)
    split_provider: if True, split the fleet ~50/50 between GOOGLE and MICROSOFT
        (odd-index domains Microsoft, even-index Google). Useful for diversifying
        deliverability across provider reputations.
    """
    print(f"\n{'=' * 60}")
    print(f"ONBOARDING — {client_name}")
    print(f"{'=' * 60}")
    if dry_run:
        print("  *** DRY RUN — no changes will be made ***\n")

    # Phase 0: Client folder
    print("\n[Phase 0] Client Folder Setup")
    client_dir = setup_client_folder(client_name)
    brief = load_client_brief(client_name)
    if brief:
        print(f"  ✓ Loaded brief: sender={brief['sender']}")

    # Phase 1: Validate
    print("\n[Phase 1] API Validation")
    api_status = validate_apis()

    if not all(api_status.values()):
        failed = [k for k, v in api_status.items() if not v]
        print(f"\n  ⛔ Cannot proceed — failed APIs: {', '.join(failed)}")
        return

    print(f"\n  Domains provided: {len(domains)}")
    for d in domains:
        print(f"    {d}")

    # Validate domain ownership
    print("\n  Checking Porkbun ownership...")
    valid_domains, missing = validate_domain_ownership(domains)
    if missing and not dry_run:
        print(f"\n  ⛔ {len(missing)} domains not found in Porkbun. Buy them first.")
        return
    if not valid_domains and not dry_run:
        print(f"\n  ⛔ No valid domains to process.")
        return

    # Use valid domains (or all in dry-run)
    work_domains = domains if dry_run else valid_domains

    # Note: we used to split into "active" and "backup" tiers here, but that
    # distinction is purely organizational — the tool does the same work on
    # every domain (DNS, mailbox creation, warmup). Whether a mailbox ends up
    # active (in a campaign) or backup (warmed but idle) is decided later in
    # Smartlead, not at provisioning time. Keeping one count makes the output
    # honest about what onboard.py actually does.
    print(f"\n  Provisioning {len(work_domains)} domain(s)")

    # Phase 2: Import domains to InboxKit (NS delegation to Cloudflare)
    print("\n[Phase 2] Domain Import to InboxKit (NS delegation)")
    imported, ns_updated, propagated, failed_import = import_domains_to_inboxkit(
        work_domains, dry_run=dry_run
    )
    print(f"\n  Import Summary: {len(imported)} registered, {len(ns_updated)} NS-updated, "
          f"{len(propagated)} propagated, {len(failed_import)} failed")

    # Only proceed with domains that actually propagated (otherwise InboxKit
    # rejects the mailbox request with "No domains found")
    if not dry_run and not propagated:
        print(f"  ⛔ No domains propagated yet. Re-run after NS settles (10–60 min).")
        return

    mailbox_domains = propagated if not dry_run else work_domains

    # Phase 3: Mailbox creation
    print("\n[Phase 3] Mailbox Creation (InboxKit /mailboxes/buy)")
    sequencer_uid = get_or_create_sequencer() if not dry_run else "dry-run-seq"

    # Build platform map if splitting providers
    platform_map = None
    if split_provider:
        platform_map = {}
        for i, d in enumerate(mailbox_domains):
            platform_map[d] = "MICROSOFT" if i % 2 else "GOOGLE"
        g = sum(1 for v in platform_map.values() if v == "GOOGLE")
        m = sum(1 for v in platform_map.values() if v == "MICROSOFT")
        print(f"  Split provider: {g} Google + {m} Microsoft domains")

    mailbox_results = create_mailboxes(
        mailbox_domains,
        brief or {"sender": client_name},
        sequencer_uid=sequencer_uid,
        dry_run=dry_run,
        platform=platform,
        platform_map=platform_map,
    )
    print(f"\n  Mailboxes: {mailbox_results['created']} created, {mailbox_results['failed']} failed")
    for detail in mailbox_results["details"]:
        print(f"    {detail}")

    # Phase 3b: straggler recovery
    # InboxKit's Playwright-based export into Smartlead flakes on 5–15% of
    # mailboxes. Wait 5 min for the sync, then direct-POST any missing ones.
    print("\n[Phase 3b] Straggler Recovery (InboxKit → Smartlead sync)")
    recovery = recover_stragglers(
        mailbox_results.get("expected", []),
        wait_min=5,
        dry_run=dry_run,
    )
    if not dry_run:
        total_expected = len(mailbox_results.get("expected", []))
        in_sl = recovery["arrived"] + recovery["recovered"]
        print(f"\n  In Smartlead: {in_sl}/{total_expected} "
              f"(arrived={recovery['arrived']}, recovered={recovery['recovered']}, "
              f"failed={recovery['failed']})")
        if recovery["failed"]:
            print(f"  ⚠️  {recovery['failed']} mailbox(es) could not be recovered. "
                  f"Check /mailboxes/show-credentials + Smartlead UI manually.")

    # Phase 4: Smartlead warmup config
    # Recovery already passes warmup_enabled=true to any recovered mailboxes.
    # configure_smartlead_warmup sets time_to_wait_in_mins and reply_rate_percentage
    # on whatever's actually in Smartlead right now.
    print("\n[Phase 4] Smartlead Warmup Configuration")
    if dry_run:
        expected = mailbox_results.get("created", len(mailbox_domains) * 2)
        print(f"  [DRY RUN] Would configure {expected} mailboxes:")
        print(f"    time_to_wait_in_mins: 20")
        print(f"    reply_rate_percentage: 55")
        print(f"    warmup_enabled: true")
        warmup_result = {"configured": expected, "total": expected}
    else:
        warmup_result = configure_smartlead_warmup(mailbox_domains, dry_run=dry_run)

    print(f"\n  Configured: {warmup_result['configured']}/{warmup_result['total']}")

    # Phase 5: Verify & Report
    print(f"\n[Phase 5] Verification")
    if dry_run:
        print("  [DRY RUN] Skipping verification")
        expected = mailbox_results.get("created", len(mailbox_domains) * 2)
        report = {
            "total_mailboxes": expected,
            "smtp_ok": expected,
            "imap_ok": expected,
            "warmup_active": expected,
            "issues": [],
        }
    else:
        report = verify_setup(mailbox_domains)

    # Summary
    print(f"\n{'=' * 60}")
    if dry_run:
        print(f"DRY RUN SUMMARY — {client_name}")
    else:
        print(f"✓ {client_name.title()} Onboarding Complete")
    print(f"{'=' * 60}")
    print(f"  Domains:   {len(work_domains)} configured")
    print(f"  Mailboxes: {mailbox_results['created']} created")
    print(f"  Warmup:    {warmup_result['configured']}/{warmup_result['total']} configured")
    print(f"  SMTP:      {report['smtp_ok']}/{report['total_mailboxes']}")
    print(f"  IMAP:      {report['imap_ok']}/{report['total_mailboxes']}")
    print(f"  Warmup:    {report['warmup_active']}/{report['total_mailboxes']} active")
    print(f"  Settings:  20min wait, 55% reply rate, 15/day")

    if report["issues"]:
        print(f"\n  ⚠️  Issues ({len(report['issues'])}):")
        for issue in report["issues"][:10]:
            print(f"    {issue}")
    else:
        print(f"\n  ✅ All healthy")

    est_ready = datetime.now().strftime("%Y-%m-%d")
    print(f"  Est. ready: ~3 weeks from now")

    # Save log
    if not dry_run:
        save_onboard_log(client_name, mailbox_domains, report)
        print(f"\n  Log saved to: {LOG_FILE}")

    # Hint to user if some mailboxes aren't in Smartlead yet
    if not dry_run and warmup_result["configured"] < mailbox_results["created"]:
        gap = mailbox_results["created"] - warmup_result["configured"]
        print(f"\n  ℹ️  {gap} mailboxes still provisioning (InboxKit → Smartlead sync pending).")
        print(f"     Re-run `python3 onboard.py --client {client_name} --check` in a few hours.")


# ==============================================================================
# CLI
# ==============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Onboard — Client mailbox infrastructure automation",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python3 onboard.py --client client_b --domains domains.txt
  python3 onboard.py --client client_b --domains "d1.com,d2.com"
  python3 onboard.py --client client_a --check
  python3 onboard.py --audit
  python3 onboard.py --client client_b --dry-run --domains domains.txt
        """,
    )
    parser.add_argument("--client", help="Client name (e.g., client_b)")
    parser.add_argument("--domains", help="Domain list: file path or comma-separated")
    parser.add_argument("--check", action="store_true", help="Check existing setup for a client")
    parser.add_argument("--audit", action="store_true", help="Health check all clients")
    parser.add_argument("--dry-run", action="store_true", help="Show plan without making changes")
    parser.add_argument("--platform", default="GOOGLE", choices=["GOOGLE", "MICROSOFT"],
                        help="Mailbox platform (InboxKit enforces one per domain). Default: GOOGLE")
    parser.add_argument("--split-provider", action="store_true",
                        help="Split fleet ~50/50 between Google and Microsoft (alternating domains)")

    args = parser.parse_args()

    if args.audit:
        print("\n[Validating APIs...]")
        validate_apis()
        audit_all()
        return

    if not args.client and not args.audit:
        parser.print_help()
        return

    if args.check:
        print("\n[Validating APIs...]")
        validate_apis()
        check_client(args.client)
        return

    if not args.domains:
        print("Error: --domains required for onboarding. Provide a file path or comma-separated list.")
        return

    domains = parse_domains(args.domains)
    if not domains:
        print("Error: No valid domains found in input.")
        return

    onboard(args.client, domains, dry_run=args.dry_run,
            platform=args.platform, split_provider=args.split_provider)


if __name__ == "__main__":
    main()
