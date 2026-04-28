"""
forge_campaign_launch.py — Forge Phase 8: Campaign Launch.

Consolidates post_forge_launcher.py's campaign-creation logic into Forge's
native pipeline. Invoked at the end of forge.py's main() after copy gen.

Flow:
  1. Read exported leads from outdir (or CSV)
  2. Look up humanized copy bank for the niche (paf_copy_banks)
     - If no bank, fall back to Forge's generated campaign_copy.md (parse + convert)
  3. Run paf_copy_gate.grade_sequence — HARD BLOCK if fails
  4. Create Smartlead campaign (DRAFTED)
  5. Save sequence
  6. Upload leads (filtered valid)
  7. Find FREE mailboxes per persona (walks ACTIVE/DRAFTED/PAUSED)
  8. Attach mailboxes (ceil(leads/50), capped 1-3)
  9. STOP. Do NOT change status. the operator handles scheduling + settings.

Per PLAYBOOK rule 12 (updated 2026-04-23): no force-pause, no status changes.
Rule 1 still holds: no explicit START via API.

Disable this phase with `forge.py --no-launch`.
"""

import json
import math
import os
import re
import sys
from datetime import datetime
from pathlib import Path

import requests

# Bolt on tools/ path so we can import the quality layer
_THIS_DIR = Path(__file__).resolve().parent
_TOOLS_DIR = _THIS_DIR / "tools"
if str(_TOOLS_DIR) not in sys.path:
    sys.path.insert(0, str(_TOOLS_DIR))

try:
    from paf_copy_banks import get_sequence as _get_copy_bank
except ImportError:
    _get_copy_bank = lambda niche: None

try:
    from paf_copy_gate import grade_sequence as _grade_sequence
except ImportError:
    _grade_sequence = None


SMARTLEAD_BASE = "https://server.smartlead.ai/api/v1"

# Persona → mailbox match hints (from_email substring)
PERSONAS = {
    "client_a": {
        "display": "ClientA",
        "hints": ["senderone", "client_a", "goclient_a"],
    },
    "client_b": {
        "display": "ClientB",
        "hints": ["sendertwo", "client_b"],
    },
    "client_c": {
        "display": "ClientC",
        "hints": ["operator", "robert", "client_c", "revmechanics"],
    },
}

# Niche display names (mirrors NEW_NICHES display fields)
NICHE_DISPLAY = {
    "apartments": "Multifamily Apartments",
    "multifamily-apartment-buildings": "Multifamily Apartments",
    "hotels": "Hotels",
    "hotels-and-motels": "Hotels",
    "warehouses": "Warehouses + Distribution",
    "warehouses-and-distribution-centers": "Warehouses + Distribution",
    "ambulatory-surgical-centers": "Ambulatory Surgical Centers",
    "schools": "Private Schools",
    "private-and-charter-schools": "Private Schools",
    "daycares": "Day Care Centers",
    "day-care-centers": "Day Care Centers",
    "self-storage": "Self-Storage Facilities",
    "self-storage-facilities": "Self-Storage Facilities",
    "manufacturing": "Light Manufacturing",
    "light-manufacturing-and-fabrication": "Light Manufacturing",
    "breweries": "Breweries + Distilleries",
    "breweries-and-distilleries": "Breweries + Distilleries",
    "office": "Commercial Office",
    "property-management": "Property Management",
    "churches": "Churches",
    "restaurants": "Restaurants",
    "medical": "Medical",
    "medical-offices": "Medical",
    "assisted-living": "Assisted Living",
}


# ---------- tiny HTTP helpers ----------
def _sl(method, path, payload=None, timeout=30):
    key = os.environ.get("SMARTLEAD_API_KEY", "")
    if not key:
        raise RuntimeError("SMARTLEAD_API_KEY not set in environment")
    sep = "&" if "?" in path else "?"
    url = f"{SMARTLEAD_BASE}{path}{sep}api_key={key}"
    if method == "GET":
        r = requests.get(url, timeout=timeout, headers={"User-Agent": "forge-launch/1.0"})
    elif method == "POST":
        r = requests.post(url, json=payload or {}, timeout=timeout)
    elif method == "DELETE":
        r = requests.delete(url, timeout=timeout)
    else:
        raise ValueError(f"unsupported method {method}")
    try:
        body = r.json() if r.text.strip() and r.text.strip().startswith(('{', '[')) else r.text
    except Exception:
        body = r.text
    return r.status_code, body


# ---------- free mailbox discovery ----------
def _find_free_mailboxes(persona_key, rep_threshold=95):
    """Return mailbox dicts NOT assigned to any ACTIVE/DRAFTED/PAUSED campaign."""
    persona = PERSONAS.get(persona_key)
    if not persona:
        return []

    # All mailboxes (paginated)
    all_mbs = []
    offset = 0
    while True:
        code, chunk = _sl("GET", f"/email-accounts/?offset={offset}&limit=100")
        if code != 200 or not isinstance(chunk, list):
            break
        if not chunk:
            break
        all_mbs.extend(chunk)
        if len(chunk) < 100:
            break
        offset += 100

    # Busy set: any mailbox currently attached to a non-completed campaign
    code, camps = _sl("GET", "/campaigns/")
    busy = set()
    if code == 200 and isinstance(camps, list):
        for c in camps:
            if c.get("status") in ("ACTIVE", "DRAFTED", "PAUSED"):
                code_m, mbs = _sl("GET", f"/campaigns/{c['id']}/email-accounts")
                if code_m == 200 and isinstance(mbs, list):
                    for m in mbs:
                        busy.add(m["id"])

    # Filter to persona + free + warmup healthy
    free = []
    for m in all_mbs:
        email = (m.get("from_email") or "").lower()
        if not any(h in email for h in persona["hints"]):
            continue
        if m.get("id") in busy:
            continue
        wd = m.get("warmup_details") or {}
        if wd.get("status") != "ACTIVE":
            continue
        try:
            rep = int(str(wd.get("warmup_reputation", "0")).replace("%", ""))
        except Exception:
            rep = 0
        if rep < rep_threshold:
            continue
        free.append(m)
    return free


# ---------- copy loading ----------
def _load_sequence_for_niche(niche_slug, outdir):
    """Prefer humanized bank. Fall back to Forge's campaign_copy.md if no bank."""
    # Try bank first
    banked = _get_copy_bank(niche_slug)
    if banked:
        return banked, "humanized_bank"

    # Fall back: parse Forge's campaign_copy.md (if it exists and is in the
    # right format). For now we treat the absence of a bank as a blocker to
    # force explicit curation. Future work: teach this to parse the markdown.
    copy_md = Path(outdir) / "campaign_copy.md"
    if copy_md.exists():
        # Flagged to the operator — we could parse markdown but prefer hand-curated
        # copy that is already in the ship-gate-approved bank format.
        return None, "fallback_unavailable_needs_bank_entry"

    return None, "no_copy_source"


# ---------- lead filtering ----------
def _valid_leads_from_csv(csv_path):
    """Read Smartlead-ready CSV; return list of dicts suitable for upload."""
    import csv as _csv
    out = []
    email_rx = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
    with open(csv_path, newline="") as f:
        reader = _csv.DictReader(f)
        for row in reader:
            email = (row.get("email") or "").lower().strip()
            if not email_rx.match(email):
                continue
            fn = (row.get("first_name") or "").strip()
            if not fn or fn.lower() in ("or null", "null", ""):
                continue
            entry = {
                "email": email,
                "first_name": fn,
                "last_name": (row.get("last_name") or "").strip(),
                "company_name": (row.get("company") or row.get("company_name") or "").strip(),
            }
            cf = {}
            for k in ("title", "phone", "domain", "city", "state"):
                v = row.get(k)
                if v:
                    cf[k] = v
            if cf:
                entry["custom_fields"] = cf
            out.append(entry)
    return out


# ---------- main launch ----------
def launch_campaign(intent, outdir, skip=False):
    """Phase 8: create Smartlead campaign end-to-end.

    Returns dict: {campaign_id, name, leads_uploaded, mailboxes_attached,
                   gate_score, status, skipped, error}
    """
    result = {
        "campaign_id": None,
        "name": None,
        "leads_uploaded": 0,
        "mailboxes_attached": 0,
        "gate_score": None,
        "status": None,
        "skipped": False,
        "error": None,
    }

    if skip:
        result["skipped"] = True
        result["error"] = "--no-launch flag set"
        return result

    if not os.environ.get("SMARTLEAD_API_KEY"):
        result["error"] = "SMARTLEAD_API_KEY not configured"
        return result

    client = intent.get("client") or "client_c"
    persona = PERSONAS.get(client)
    if not persona:
        result["error"] = f"unknown client/persona: {client}"
        return result

    niche_slug = intent.get("niche") or ""
    niche_display = NICHE_DISPLAY.get(niche_slug) or niche_slug.replace("-", " ").title()
    today = datetime.now().strftime("%d%b%Y")
    name = f"{persona['display']} - {niche_display} - {today}"
    result["name"] = name

    # ---- Step 1: load sequence (humanized bank preferred) ----
    seq, seq_source = _load_sequence_for_niche(niche_slug, outdir)
    if not seq:
        result["error"] = f"no_sequence_available: {seq_source}"
        print(f"  [launch] BLOCKED: no copy bank for niche '{niche_slug}'. "
              f"Add to tools/paf_copy_banks.py before auto-launch can proceed.")
        return result
    print(f"  [launch] copy source: {seq_source}")

    # ---- Step 2: ship gate ----
    if _grade_sequence:
        gate = _grade_sequence(seq, min_score=14)
        result["gate_score"] = gate["score"]
        if not gate["ship_ok"]:
            result["error"] = f"ship_gate_fail: score={gate['score']}/18, flags={len(gate['humanizer_flags'])}"
            print(f"  [launch] BLOCKED by ship gate: score={gate['score']}/18, "
                  f"humanizer_flags={len(gate['humanizer_flags'])}")
            for f in gate["failed"][:3]:
                print(f"    FAIL [{f['rule']}]: {f['note']}")
            return result
        print(f"  [launch] ship gate PASS: {gate['score']}/18, 0 humanizer flags")
    else:
        print("  [launch] WARN: paf_copy_gate not importable, shipping without grade")

    # ---- Step 3: load leads ----
    # Look for the Smartlead-ready CSV that export_results saved
    candidates = list(Path(outdir).glob("*smartlead*.csv")) + list(Path(outdir).glob("*.csv"))
    csv_path = candidates[0] if candidates else None
    if not csv_path:
        result["error"] = f"no_csv_in_outdir: {outdir}"
        return result
    leads = _valid_leads_from_csv(csv_path)
    if not leads:
        result["error"] = "zero_valid_leads_in_csv"
        return result
    print(f"  [launch] valid leads for upload: {len(leads)}")

    # ---- Step 4: create campaign ----
    code, body = _sl("POST", "/campaigns/create", {"name": name})
    if code not in (200, 201):
        result["error"] = f"create_fail_{code}: {str(body)[:200]}"
        return result
    cid = (body.get("id") or body.get("campaign_id") or (body.get("data") or {}).get("id")) if isinstance(body, dict) else None
    if not cid:
        result["error"] = f"no_cid_in_response: {body}"
        return result
    result["campaign_id"] = cid
    print(f"  [launch] created campaign {cid}")

    # ---- Step 5: save sequence ----
    code, _ = _sl("POST", f"/campaigns/{cid}/sequences", seq)
    if code not in (200, 201):
        result["error"] = f"save_sequence_fail_{code}"
        return result

    # ---- Step 6: upload leads (chunks of 100) ----
    uploaded = 0
    for i in range(0, len(leads), 100):
        chunk = leads[i:i+100]
        code, body = _sl("POST", f"/campaigns/{cid}/leads", {"lead_list": chunk})
        if code in (200, 201):
            up = body.get("upload_count", len(chunk)) if isinstance(body, dict) else len(chunk)
            uploaded += up if isinstance(up, int) else 0
    result["leads_uploaded"] = uploaded
    print(f"  [launch] uploaded {uploaded} leads")

    # ---- Step 7: find + attach FREE mailboxes ----
    free = _find_free_mailboxes(client, rep_threshold=95)
    attach_n = max(1, min(3, math.ceil(uploaded / 50)))
    to_attach = [m["id"] for m in free[:attach_n]]
    if to_attach:
        code, _ = _sl("POST", f"/campaigns/{cid}/email-accounts",
                      {"email_account_ids": to_attach})
        if code in (200, 201):
            result["mailboxes_attached"] = len(to_attach)
            print(f"  [launch] attached {len(to_attach)} free mailboxes")
        else:
            print(f"  [launch] WARN mailbox attach HTTP {code}")
    else:
        print(f"  [launch] WARN no free mailboxes for {client}")

    # ---- Step 8: STOP. Do not touch status. Report final. ----
    code, camp = _sl("GET", f"/campaigns/{cid}")
    if code == 200 and isinstance(camp, dict):
        result["status"] = camp.get("status")

    print(f"  [launch] DONE: {cid} | {uploaded} leads | "
          f"{result['mailboxes_attached']} mailboxes | status={result['status']}")
    return result
