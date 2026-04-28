#!/usr/bin/env python3
"""
onboard_microsoft.py — M365 variant of onboard.py.

Provisions Microsoft 365 mailboxes for cold email use. Handles:
  - M365 DNS records (MX → outlook.com, SPF → protection.outlook.com, DKIM CNAMEs)
  - InboxKit M365 mailbox creation (provider hint)
  - Smartlead auto-connect + warmup
  - Same verification + reporting as onboard.py

Run alongside onboard.py for a Google + Microsoft split:
    # Run Google batch
    python3 onboard.py --client client_c --domains /tmp/rm_google.txt

    # Run Microsoft batch
    python3 onboard_microsoft.py --client client_c --domains /tmp/rm_m365.txt

⚠️  VERIFICATION NEEDED BEFORE FIRST LIVE RUN:

1. InboxKit M365 support — confirm the provider field name and available
   endpoints. Current code assumes /prewarm/buy-domain accepts a
   `provider: "microsoft"` hint. If InboxKit uses a different endpoint or
   field name, edit the `create_mailboxes_m365` function.

2. M365 DKIM tenant values — DKIM for M365 uses CNAME records pointing to
   your Microsoft 365 tenant. If you don't have an M365 tenant (because
   InboxKit manages the tenancy for you), InboxKit's onboarding will
   either skip DKIM or provide the selectors post-provisioning. Check
   InboxKit's docs for exact selectors.

3. Autodiscover — some M365 setups require autodiscover CNAME. Added as
   a standard record; remove if unneeded.

Run --dry-run first to inspect the plan.
"""

import os
import sys
import time
import argparse
from pathlib import Path

# Reuse helpers from onboard.py — same directory
sys.path.insert(0, str(Path(__file__).resolve().parent))
from onboard import (
    parse_domains,
    validate_apis,
    setup_client_folder,
    load_client_brief,
    validate_domain_ownership,
    get_existing_dns,
    has_record,
    porkbun_post,
    inboxkit_post,
    smartlead_get,
    smartlead_post,
    get_or_create_sequencer,
    get_all_smartlead_accounts,
    configure_smartlead_warmup,
)


# ==============================================================================
# PHASE 2 (M365 variant): DNS CONFIGURATION
# ==============================================================================

def configure_dns_m365(domain, m365_tenant_prefix=None, dry_run=False):
    """Configure DNS for Microsoft 365 email.

    m365_tenant_prefix: your M365 tenant subdomain for DKIM routing
        (e.g., "client_c" → DKIM points to
        selector1-{domain-hyphens}._domainkey.client_c.onmicrosoft.com)
        If None, DKIM CNAMEs are SKIPPED and you'll need to configure them
        post-provisioning when InboxKit tells you the actual selectors.
    """
    existing = get_existing_dns(domain)
    created = []
    skipped = []

    # Domain with dots replaced by hyphens (M365 pattern for routing)
    domain_hyphens = domain.replace(".", "-")

    # M365 standard DNS records for cold email
    records = [
        # MX: single record to M365 edge (priority 0)
        {"type": "MX", "content": f"{domain_hyphens}.mail.protection.outlook.com",
         "prio": 0, "name": ""},

        # SPF
        {"type": "TXT", "content": "v=spf1 include:spf.protection.outlook.com ~all",
         "name": ""},

        # DMARC (same as Google)
        {"type": "TXT", "content": f"v=DMARC1; p=none; rua=mailto:dmarc@{domain}",
         "name": "_dmarc"},

        # Autodiscover (optional but standard for M365)
        {"type": "CNAME", "content": "autodiscover.outlook.com",
         "name": "autodiscover"},
    ]

    # DKIM CNAMEs — only add if tenant prefix is known
    if m365_tenant_prefix:
        records.extend([
            {"type": "CNAME",
             "content": f"selector1-{domain_hyphens}._domainkey.{m365_tenant_prefix}.onmicrosoft.com",
             "name": "selector1._domainkey"},
            {"type": "CNAME",
             "content": f"selector2-{domain_hyphens}._domainkey.{m365_tenant_prefix}.onmicrosoft.com",
             "name": "selector2._domainkey"},
        ])

    for rec in records:
        if has_record(existing, rec["type"], rec["content"]):
            skipped.append(f"{rec['type']} {rec.get('name', '@')} → {rec['content'][:50]}")
            continue

        if dry_run:
            created.append(f"[DRY RUN] {rec['type']} {rec.get('name', '@')} → {rec['content'][:50]}")
            continue

        data = {
            "type": rec["type"],
            "content": rec["content"],
            "ttl": 600,
        }
        if rec.get("name"):
            data["name"] = rec["name"]
        if rec.get("prio") is not None:
            data["prio"] = rec["prio"]

        resp = porkbun_post(f"/dns/create/{domain}", data)
        if resp and resp.get("status") == "SUCCESS":
            created.append(f"{rec['type']} {rec.get('name', '@')} → {rec['content'][:50]}")
        else:
            msg = resp.get("message", "unknown error") if resp else "no response"
            print(f"    ⚠️  Failed: {rec['type']} {rec.get('name', '@')} — {msg}")

    return created, skipped


# ==============================================================================
# PHASE 3 (M365 variant): MAILBOX CREATION via InboxKit
# ==============================================================================

def create_mailboxes_m365(domains, sender_info, sequencer_uid=None, dry_run=False):
    """Create 2 M365 mailboxes per domain via InboxKit.

    ⚠️ VERIFY THE PROVIDER FIELD NAME — InboxKit docs may use:
      - "provider": "microsoft"
      - "email_provider": "m365"
      - "type": "microsoft_365"
    Current code uses the first. Check /prewarm/buy-domain docs if this
    returns an error.
    """
    personas = sender_info.get("personas", [])
    if not personas:
        personas = [{"first_name": "Agent", "last_name": "", "username": "agent"}]

    results = {"created": 0, "failed": 0, "details": []}

    for domain in domains:
        mailboxes = []
        for persona in personas:
            mailboxes.append({
                "username": persona["username"],
                "first_name": persona["first_name"],
                "last_name": persona["last_name"],
            })

        if dry_run:
            for p in personas:
                results["details"].append(f"[DRY RUN] [M365] {p['username']}@{domain}")
            results["created"] += len(personas)
            continue

        payload = {
            "domains": [{
                "domain_name": domain,
                "mailboxes": mailboxes,
                # ⚠️ THIS IS THE KEY M365 FIELD — verify name with InboxKit support
                "provider": "microsoft",
            }],
            "keep_warming": True,
        }
        if sequencer_uid:
            payload["sequencer_uid"] = sequencer_uid

        resp = inboxkit_post("/prewarm/buy-domain", payload)
        if resp and not resp.get("error"):
            count = resp.get("data", {}).get("total_mailboxes", len(personas))
            results["created"] += count
            for p in personas:
                results["details"].append(f"✓ [M365] {p['username']}@{domain}")
        else:
            results["failed"] += len(personas)
            msg = resp.get("message", "unknown") if resp else "no response"
            for p in personas:
                results["details"].append(f"✗ [M365] {p['username']}@{domain} — {msg}")

        time.sleep(1)

    return results


# ==============================================================================
# MAIN
# ==============================================================================

def main():
    ap = argparse.ArgumentParser(
        description="Onboard — M365 variant of client mailbox infrastructure automation"
    )
    ap.add_argument("--client", required=True,
                    help="Client name (e.g., client_c)")
    ap.add_argument("--domains", required=True,
                    help="Domain list: file path or comma-separated")
    ap.add_argument("--m365-tenant-prefix", default=None,
                    help="M365 tenant subdomain for DKIM (e.g., 'mycompany' for mycompany.onmicrosoft.com). If omitted, DKIM CNAMEs are skipped.")
    ap.add_argument("--dry-run", action="store_true",
                    help="Show plan without making changes")
    args = ap.parse_args()

    domains = parse_domains(args.domains)
    if not domains:
        print("No domains provided.")
        sys.exit(1)

    print("=" * 60)
    print(f"Onboard [M365] — {args.client}")
    print(f"Domains: {len(domains)}")
    if args.dry_run:
        print("Mode: DRY RUN (no changes will be made)")
    print("=" * 60)

    # Validate APIs
    print("\n[Phase 1] API Validation")
    apis = validate_apis()
    if not all(apis.values()):
        print("\n✗ One or more APIs failed. Fix before continuing.")
        sys.exit(1)

    # Client folder + brief
    setup_client_folder(args.client)
    sender_info = load_client_brief(args.client)

    # Domain ownership check
    print("\n[Phase 1b] Domain Ownership")
    owned = validate_domain_ownership(domains)
    if not owned:
        print("✗ No valid domains to onboard.")
        sys.exit(1)

    # DNS configuration
    print("\n[Phase 2] DNS Configuration (M365)")
    dns_created = 0
    dns_skipped = 0
    for domain in owned:
        print(f"\n  {domain}:")
        created, skipped = configure_dns_m365(domain, args.m365_tenant_prefix, args.dry_run)
        dns_created += len(created)
        dns_skipped += len(skipped)
        for line in created:
            print(f"    + {line}")
        for line in skipped:
            print(f"    = (exists) {line}")
    print(f"\n  DNS Summary: {dns_created} created, {dns_skipped} already existed")

    # Sequencer
    print("\n[Phase 3a] Smartlead Sequencer")
    seq_uid = None
    if not args.dry_run:
        seq_uid = get_or_create_sequencer()
    else:
        print("  [DRY RUN] Would ensure sequencer exists")

    # Mailbox creation
    print("\n[Phase 3b] Mailbox Creation (InboxKit, M365)")
    mb_results = create_mailboxes_m365(owned, sender_info, seq_uid, args.dry_run)
    print(f"\n  Mailboxes: {mb_results['created']} created, {mb_results['failed']} failed")
    for line in mb_results["details"][:30]:
        print(f"    {line}")

    # Smartlead warmup config
    print("\n[Phase 4] Smartlead Warmup Configuration")
    if not args.dry_run:
        wm = configure_smartlead_warmup(owned, dry_run=False)
        print(f"  Configured: {wm.get('configured', 0)}/{wm.get('total', 0)}")
    else:
        expected = mb_results["created"]
        print(f"  [DRY RUN] Would configure {expected} mailboxes:")
        print(f"    time_to_wait_in_mins: 20")
        print(f"    reply_rate_percentage: 55")
        print(f"    warmup_enabled: true")

    # Summary
    print()
    print("=" * 60)
    print(f"{'DRY RUN ' if args.dry_run else ''}SUMMARY — {args.client} [M365]")
    print("=" * 60)
    print(f"  Domains:   {len(owned)} configured")
    print(f"  Mailboxes: {mb_results['created']} created")
    print()
    if args.dry_run:
        print("  Est. ready: ~2 weeks from live run (Microsoft 365 warmup)")
        print()
        print("  ⚠️  BEFORE RUNNING LIVE:")
        print("    1. Verify InboxKit M365 provider field name in payload")
        print("    2. Confirm InboxKit has M365 tenancy configured on your account")
        print("    3. If you have a dedicated M365 tenant, pass --m365-tenant-prefix")
    else:
        print("  Est. ready: ~2 weeks (warmup via Smartlead)")


if __name__ == "__main__":
    main()
