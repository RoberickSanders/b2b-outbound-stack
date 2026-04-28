#!/usr/bin/env python3.13
"""
reply_triage.py — Auto-classify inbound replies + draft responses for the operator's review.

Why this exists:
  At 50k sends/mo with 2% reply rate = 1,000 replies/month. the operator personally
  reads + drafts each one (Bryce thread pattern on 2026-04-20). That's
  80-150 hrs/month of his time on reply handling alone as volume scales.

  This agent:
    1. Pulls new replies from Smartlead every run
    2. Classifies via Kimi (hot / question / objection / OOO / not-interested)
    3. Drafts a response via Claude Opus in the right sender voice (CLIENT_C/Sender One/Sender Two)
    4. Pushes each one to Trello as a card in "Replies to Review"
    5. the operator reviews in Trello → moves to "Approved" column → (v2: auto-sends)

  v1 (this file): everything up to Trello card creation. the operator copies/pastes
  the approved draft into Smartlead manually. Kills 80% of the time sink.

  v2 (later): auto-send on Trello column move.

Usage:
  python3 tools/reply_triage.py --dry-run        # preview what would be classified/drafted
  python3 tools/reply_triage.py                  # classify + draft + push to Trello
  python3 tools/reply_triage.py --since 1        # only replies from last 1 day
  python3 tools/reply_triage.py --lead-email X   # process a single specific lead

Trello setup (one-time):
  1. Create a Trello board called "Reply Triage"
  2. Add 3 lists: "New Replies", "Approved", "Sent"
  3. Get your API key + token from https://trello.com/app-key
  4. Add to .env:
       TRELLO_API_KEY=...
       TRELLO_TOKEN=...
       TRELLO_BOARD_ID=...       # board where cards get created
       TRELLO_LIST_ID_NEW=...    # "New Replies" list ID
  5. First run creates cards in "New Replies"

Without Trello env vars, tool falls back to stdout + markdown files in
01-Projects/<client>/reply_drafts/.

Standalone tool — does not modify Forge code.
"""

import argparse
import html
import json
import os
import re
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests

SCRIPT_DIR = Path(__file__).resolve().parent
LEAD_PIPELINE_DIR = SCRIPT_DIR.parent
WORKSPACE_ROOT = LEAD_PIPELINE_DIR.parent.parent
PROJECTS_DIR = WORKSPACE_ROOT / "01-Projects"
LOGS_DIR = LEAD_PIPELINE_DIR / "logs"
LOGS_DIR.mkdir(exist_ok=True)
STATE_FILE = LOGS_DIR / "reply_triage_state.json"
sys.path.insert(0, str(LEAD_PIPELINE_DIR))

try:
    from dotenv import load_dotenv
    load_dotenv(WORKSPACE_ROOT / ".env", override=True)
    load_dotenv(LEAD_PIPELINE_DIR / ".env", override=True)
except ImportError:
    pass

SMARTLEAD_KEY = os.environ.get("SMARTLEAD_API_KEY", "")
SMARTLEAD_BASE = "https://server.smartlead.ai/api/v1"

TRELLO_KEY = os.environ.get("TRELLO_API_KEY", "")
TRELLO_TOKEN = os.environ.get("TRELLO_TOKEN", "")
TRELLO_LIST_ID = os.environ.get("TRELLO_LIST_ID_NEW", "")

# Client identity → sender context for reply drafting
CLIENT_IDENTITIES = {
    "client_c": {
        "name_keywords": ["ClientC", "CLIENT_C ", "RevenueMechanic"],
        "display_name": "ClientC",
        "sender_persona": "Demo Operator (founder). Runs a cold email lead gen agency. Writes concise, direct, no-fluff replies. Uses 'we' when talking about CLIENT_C, 'I' when sharing personal take.",
    },
    "client_a": {
        "name_keywords": ["ClientA", "CLIENT_A "],
        "display_name": "ClientA",
        "sender_persona": "Sender One (owner). Denver commercial fire protection since 2009. NICET-certified. Practical, operator-speak. Signs with 'Sender One, ClientA'. Never fluffy.",
    },
    "client_b": {
        "name_keywords": ["ClientB"],
        "display_name": "ClientB",
        "sender_persona": "Sender Two (founder). Cybersecurity / VCISO / SOC2 consulting. Technical but accessible. Answers compliance questions directly.",
    },
}


def classify_client(campaign_name: str) -> str:
    name = (campaign_name or "").lower()
    for key, cfg in CLIENT_IDENTITIES.items():
        if any(kw.lower() in name for kw in cfg["name_keywords"]):
            return key
    return "unknown"


# ============================================================
# State: track which replies we've already processed
# ============================================================

def load_state() -> dict:
    if not STATE_FILE.exists():
        return {"processed_stats_ids": {}}
    try:
        return json.loads(STATE_FILE.read_text())
    except Exception:
        return {"processed_stats_ids": {}}


def save_state(state: dict) -> None:
    STATE_FILE.write_text(json.dumps(state, indent=2))


# ============================================================
# Smartlead: find + pull replies
# ============================================================

def _strip_html(body: str, max_chars: int = 2000) -> str:
    if not body:
        return ""
    text = re.sub(r"<br\s*/?>", "\n", body, flags=re.IGNORECASE)
    text = re.sub(r"</p>", "\n", text, flags=re.IGNORECASE)
    # Strip inline CSS blocks that leak from Outlook-style email HTML (e.g.,
    # "P {margin-top:0;margin-bottom:0;}" — common when prospects reply from Outlook).
    text = re.sub(r"<style[^>]*>.*?</style>", " ", text, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r"[A-Za-z.#][A-Za-z0-9.#\-_]*\s*\{[^}]*\}", " ", text)
    # Strip HTML tags
    text = re.sub(r"<[^>]+>", "", text)
    # HTML-entity decode (&nbsp;, &amp;, etc.)
    text = html.unescape(text)
    # Collapse whitespace / newlines
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text).strip()
    # Trim quoted reply history
    for marker in ["On Fri,", "On Thu,", "On Wed,", "On Tue,", "On Mon,", "On Sat,", "On Sun,",
                   "\nFrom:", "-- Forwarded", "________________________________",
                   "From: Sender One", "From: Operator", "From: Sender Two"]:
        idx = text.find(marker)
        if idx > 100:
            text = text[:idx].strip()
            break
    return text[:max_chars]


def find_new_replies(since_days: int = 3, email_filter: str = None) -> list:
    """Walk active campaigns, find every replied lead, pull thread, find most recent REPLY.

    Why this approach (vs. stats endpoint's reply_time):
      Smartlead's /statistics endpoint only records the FIRST reply_time per
      stats row. Subsequent messages in an ongoing thread don't update that
      field. So if Bryce replied 5 times today, stats only shows his first
      reply (which could be weeks old). We need to look at /leads (filter by
      lead_category_id) then walk /message-history per lead to find truly
      recent REPLY messages.
    """
    r = requests.get(f"{SMARTLEAD_BASE}/campaigns/", params={"api_key": SMARTLEAD_KEY}, timeout=30)
    if r.status_code != 200:
        return []
    campaigns = r.json()

    cutoff = datetime.now(timezone.utc) - timedelta(days=since_days)
    all_replies = []
    seen_lead_ids = set()  # dedupe — same lead can appear across campaigns

    for c in campaigns:
        if c.get("status") not in ("ACTIVE", "RUNNING", "PAUSED", "COMPLETED"):
            continue
        client_key = classify_client(c.get("name", ""))
        if client_key == "unknown":
            continue
        cid = c["id"]

        # Pull all leads in this campaign, filter to ones with a reply category set
        # lead_category_id is populated by Smartlead for any lead that's replied
        # (categories 1-9 correspond to meeting booked / interested / not now / etc.)
        leads_in_campaign = []
        offset = 0
        while True:
            lr = requests.get(
                f"{SMARTLEAD_BASE}/campaigns/{cid}/leads",
                params={"api_key": SMARTLEAD_KEY, "limit": 100, "offset": offset},
                timeout=30,
            )
            if lr.status_code != 200: break
            batch = lr.json().get("data", [])
            if not batch: break
            leads_in_campaign.extend(batch)
            if len(batch) < 100: break
            offset += 100

        for cl in leads_in_campaign:
            # Only leads that have been categorized (= replied)
            if cl.get("lead_category_id") is None:
                continue
            lead = cl.get("lead") or {}
            lead_email = (lead.get("email") or "").lower()
            lead_id = lead.get("id")
            if not lead_email or not lead_id:
                continue
            if email_filter and lead_email != email_filter.lower():
                continue
            # Dedupe across campaigns
            dedupe_key = f"{lead_id}:{cid}"
            if dedupe_key in seen_lead_ids:
                continue
            seen_lead_ids.add(dedupe_key)

            # Pull full message history
            hist_r = requests.get(
                f"{SMARTLEAD_BASE}/campaigns/{cid}/leads/{lead_id}/message-history",
                params={"api_key": SMARTLEAD_KEY},
                timeout=20,
            )
            if hist_r.status_code != 200:
                continue
            history = hist_r.json().get("history", [])
            # Find the MOST RECENT REPLY (reverse through history)
            reply_msg = None
            for m in reversed(history):
                if m.get("type") == "REPLY":
                    reply_msg = m
                    break
            if not reply_msg:
                continue

            # Check if this latest reply is within our window
            reply_time_str = reply_msg.get("time", "")
            try:
                rt = datetime.fromisoformat(reply_time_str.replace("Z", "+00:00"))
            except Exception:
                continue
            if rt < cutoff:
                continue

            # Use "{lead_id}:{reply_time}" as stable dedupe identifier for state tracking.
            # This way if the prospect replies AGAIN tomorrow, it's a NEW event that
            # needs triage (different reply_time = different dedupe key).
            stable_id = f"{lead_id}:{reply_time_str}"

            all_replies.append({
                "stats_id": stable_id,  # reused field name for state persistence
                "client": client_key,
                "campaign_id": cid,
                "campaign_name": c.get("name", ""),
                "lead_email": lead_email,
                "lead_name": f"{lead.get('first_name','')} {lead.get('last_name','')}".strip(),
                "lead_id": lead_id,
                "reply_text": _strip_html(reply_msg.get("email_body", "")),
                "reply_subject": reply_msg.get("subject", ""),
                "reply_time": reply_time_str,
                "full_history": history,
            })
        time.sleep(0.2)  # be gentle with Smartlead API

    return all_replies


# ============================================================
# Classify + draft via Kimi + Opus
# ============================================================

def classify_reply(reply_text: str) -> dict:
    """Kimi classifies the reply into a category + extracts the core ask/objection."""
    try:
        from llm_router import get_light_client
        client, model = get_light_client()
    except Exception:
        return {"category": "unknown", "urgency": "medium", "summary": "(classifier unavailable)"}

    prompt = f"""Classify this cold-email reply from a prospect.

REPLY TEXT:
{reply_text[:2000]}

Output a JSON object with these keys:
  - "category": one of ["hot", "question", "objection", "oof", "not_interested", "wrong_person", "unsubscribe", "other"]
  - "urgency": one of ["critical", "high", "medium", "low"] — based on how quickly the operator should personally read + respond
  - "summary": 1-sentence paraphrase of what they're asking or saying
  - "intent": what they seem to want ("bid invitation", "more information", "off my list", "forwarded to wrong dept", etc.)

Output ONLY the JSON. No preamble."""
    try:
        resp = client.messages.create(
            model=model, max_tokens=300,
            messages=[{"role": "user", "content": prompt}],
        )
        text = resp.content[0].text.strip()
        text = re.sub(r"^```(?:json)?\s*", "", text)
        text = re.sub(r"\s*```$", "", text)
        return json.loads(text)
    except Exception as e:
        return {"category": "unknown", "urgency": "medium", "summary": f"(classifier err: {e})"}


def draft_reply(reply_item: dict, classification: dict) -> str:
    """Claude Opus drafts a response in the right sender voice.

    Uses heavy model (not Kimi) because voice matching + diplomatic replies are
    higher-stakes than classification. Worth the ~$0.05/reply.
    """
    try:
        from llm_router import get_heavy_client
        client, model = get_heavy_client()
    except Exception:
        return "(heavy client unavailable — cannot draft)"

    client_key = reply_item["client"]
    persona = CLIENT_IDENTITIES.get(client_key, {}).get("sender_persona", "")

    # Build thread context (last 3 messages max, newest first)
    thread_context = ""
    for msg in reply_item.get("full_history", [])[-4:]:
        direction = "CLIENT_C/CLIENT SAID" if msg.get("type") == "SENT" else "PROSPECT SAID"
        body_preview = _strip_html(msg.get("email_body", ""), max_chars=500)
        thread_context += f"\n[{direction}] {body_preview}\n---\n"

    cat = classification.get("category", "unknown")
    summary = classification.get("summary", "")

    prompt = f"""You are drafting a response to a cold-email reply on behalf of {CLIENT_IDENTITIES.get(client_key, {}).get('display_name', client_key)}.

SENDER VOICE:
{persona}

THIS REPLY IS CATEGORIZED AS:
  category: {cat}
  intent:   {classification.get('intent', 'unknown')}
  summary:  {summary}

RECENT THREAD CONTEXT (most recent last):
{thread_context}

PROSPECT'S CURRENT REPLY:
{reply_item['reply_text'][:1500]}

CRITICAL RULES:
1. NEVER invent facts about our services. Only state what's already in the thread or in the sender's known capabilities.
2. Match the sender's voice (concise, operator-speak for Sender One; direct for the operator; technical for Sender Two).
3. Under 100 words. No fluff. No "thanks so much for reaching out."
4. If they asked a QUESTION, answer it directly. If they raised an OBJECTION, acknowledge + respond.
5. If this is an "oof" or "unsubscribe" — output exactly: `SKIP_REPLY`
6. If this is a "wrong_person" — output a short forward request or "SKIP_REPLY"
7. End with a specific next step (reply with X, calendar link, meeting time, send specs, etc.) when appropriate.
8. No em dashes. Use commas, periods, or hyphens with spaces.
9. No signatures — the sender adds their own.

Output ONLY the draft reply body. No preamble, no explanation."""

    try:
        resp = client.messages.create(
            model=model, max_tokens=500,
            messages=[{"role": "user", "content": prompt}],
        )
        return resp.content[0].text.strip()
    except Exception as e:
        return f"(draft failed: {e})"


# ============================================================
# Trello delivery (or fallback to markdown file)
# ============================================================

def send_pushover_preview(reply_item: dict, classification: dict, draft: str,
                           destination_url: str = "", dry_run: bool = False) -> bool:
    """Push the full draft to the operator's phone so he can review + decide.

    Per 2026-04-20 rule: we never auto-send without explicit approval. Pushover
    is one-way (can't accept YES/NO replies), so this is PREVIEW-ONLY:
      - the operator gets the full draft on his lock screen within seconds of triage
      - To approve → tap the Smartlead URL in the notification → paste draft → Send
      - To reject → ignore the notification

    Returns True if Pushover accepted the alert, False otherwise (or if not configured).
    """
    user_key = os.environ.get("PUSHOVER_USER_KEY", "")
    app_token = os.environ.get("PUSHOVER_APP_TOKEN", "")
    if not user_key or not app_token:
        return False

    client_key = reply_item.get("client", "?")
    display = CLIENT_IDENTITIES.get(client_key, {}).get("display_name", client_key)
    cat = classification.get("category", "?").upper()
    urgency = classification.get("urgency", "medium")
    lead = reply_item.get("lead_email", "")
    summary = classification.get("summary", "")

    # Priority: hot/critical → 1 (bypass DnD), otherwise 0
    priority_map = {"critical": 1, "high": 1, "medium": 0, "low": -1}
    priority = priority_map.get(urgency, 0)

    # Pushover body cap is 1024 chars. Budget it carefully:
    #   ~500 for the prospect's actual reply (what they said)
    #   ~400 for the Opus-generated draft (our response)
    #   ~50 for labels + instruction
    prospect_reply_clean = reply_item.get("reply_text", "").strip()
    # Trim prospect text to ~500 chars, ending at a word boundary
    if len(prospect_reply_clean) > 500:
        cut = prospect_reply_clean[:500]
        last_space = cut.rfind(" ")
        prospect_reply_clean = cut[:last_space if last_space > 350 else 500] + "…"

    draft_clean = draft.strip()
    if len(draft_clean) > 400:
        cut = draft_clean[:400]
        last_space = cut.rfind(" ")
        draft_clean = cut[:last_space if last_space > 300 else 400] + "…"

    body_lines = [
        "── THEY SAID ──",
        prospect_reply_clean,
        "",
        "── DRAFT REPLY ──",
        draft_clean,
        "",
        "Tap to open Smartlead → paste draft → send.",
    ]
    body = "\n".join(body_lines)[:1024]

    # Use Smartlead campaign URL if we can — tap-to-jump to the thread
    smartlead_url = f"https://app.smartlead.ai/app/email-campaign/{reply_item.get('campaign_id')}"

    title = f"[{display}] {cat} · {lead[:30]}"

    if dry_run:
        print(f"  [DRY RUN] Would send Pushover preview: '{title}' (priority {priority})")
        return True

    try:
        r = requests.post(
            "https://api.pushover.net/1/messages.json",
            data={
                "token": app_token,
                "user": user_key,
                "title": title,
                "message": body,
                "priority": priority,
                "url": smartlead_url,
                "url_title": "Open Smartlead thread",
            },
            timeout=10,
        )
        return r.status_code == 200
    except Exception:
        return False


def create_trello_card(reply_item: dict, classification: dict, draft: str, dry_run: bool = False) -> str:
    """Create a Trello card for this reply. Returns card URL or empty string."""
    if not (TRELLO_KEY and TRELLO_TOKEN and TRELLO_LIST_ID):
        # Fallback: write a markdown file
        return _fallback_markdown(reply_item, classification, draft, dry_run)

    client_key = reply_item["client"]
    client_name = CLIENT_IDENTITIES.get(client_key, {}).get("display_name", client_key)
    cat = classification.get("category", "?")
    urgency = classification.get("urgency", "medium")
    summary = classification.get("summary", "")

    title = f"[{client_name}] {cat.upper()} · {reply_item.get('lead_email', '')[:40]} · {summary[:60]}"

    description = f"""**Client:** {client_name}
**Campaign:** {reply_item.get('campaign_name', '')}
**From:** {reply_item.get('lead_email', '')}
**Reply received:** {reply_item.get('reply_time', '')}
**Category:** {cat} · **Urgency:** {urgency}
**Kimi summary:** {summary}

---

### Prospect's reply

{reply_item.get('reply_text', '')[:2000]}

---

### Suggested draft (review + edit before sending)

{draft}

---

*Generated by `reply_triage.py`. To approve: move this card to the "Approved" list. To send: copy the draft into Smartlead's reply UI for this thread.*
"""

    if dry_run:
        print(f"  [DRY RUN] Would create Trello card: {title[:80]}")
        return "dry-run"

    r = requests.post(
        "https://api.trello.com/1/cards",
        params={
            "key": TRELLO_KEY,
            "token": TRELLO_TOKEN,
            "idList": TRELLO_LIST_ID,
            "name": title,
            "desc": description,
        },
        timeout=20,
    )
    if r.status_code in (200, 201):
        return r.json().get("shortUrl", "")
    else:
        print(f"  FAIL Trello: {r.status_code} {r.text[:150]}")
        # Fall back to markdown
        return _fallback_markdown(reply_item, classification, draft, dry_run)


def _fallback_markdown(reply_item: dict, classification: dict, draft: str, dry_run: bool) -> str:
    """Write the reply draft to a markdown file if Trello isn't configured."""
    client_key = reply_item["client"]
    out_dir = PROJECTS_DIR / client_key / "reply_drafts"
    out_dir.mkdir(parents=True, exist_ok=True)

    safe_email = re.sub(r"[^a-z0-9]+", "-", (reply_item.get("lead_email") or "unknown").lower()).strip("-")
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    out_path = out_dir / f"{ts}_{safe_email}.md"

    md = f"""# Reply from {reply_item.get('lead_email')}

**Client:** {client_key}
**Campaign:** {reply_item.get('campaign_name')}
**Category:** {classification.get('category')} (urgency: {classification.get('urgency')})
**Summary:** {classification.get('summary')}

## Their reply

{reply_item.get('reply_text', '')[:2000]}

## Suggested draft

{draft}

---
*Generated {datetime.now(timezone.utc).isoformat()} — copy to Smartlead after review.*
"""
    if dry_run:
        print(f"  [DRY RUN] Would write: {out_path}")
        return "dry-run"
    out_path.write_text(md)
    return str(out_path)


# ============================================================
# Orchestration
# ============================================================

def run(since_days: int = 3, dry_run: bool = False, email_filter: str = None) -> dict:
    if not SMARTLEAD_KEY:
        sys.exit("ERROR: SMARTLEAD_API_KEY not set")

    print(f"{'='*70}")
    print(f"REPLY TRIAGE — {'DRY RUN' if dry_run else 'LIVE'}")
    print(f"{'='*70}")

    state = load_state()
    processed = state.setdefault("processed_stats_ids", {})

    print(f"Scanning last {since_days} days for new replies...")
    replies = find_new_replies(since_days=since_days, email_filter=email_filter)
    print(f"  → found {len(replies)} replies in window")

    new_replies = [r for r in replies if r.get("stats_id") not in processed]
    print(f"  → {len(new_replies)} are new (not yet triaged)")

    if not new_replies:
        print("\n✅ No new replies to process.")
        return {"processed": 0, "drafted": 0, "skipped": 0}

    drafted = 0
    skipped = 0

    for i, reply in enumerate(new_replies, 1):
        print(f"\n[{i}/{len(new_replies)}] {reply['lead_email']} ({reply['client']})")

        # 1. Classify via Kimi
        classification = classify_reply(reply["reply_text"])
        cat = classification.get("category", "?")
        print(f"  classification: {cat} (urgency: {classification.get('urgency')})")
        print(f"  summary: {classification.get('summary', '')[:100]}")

        # 2. Skip categories that don't need a reply
        if cat in ("unsubscribe", "oof", "not_interested"):
            print(f"  → skipping (category={cat}, no reply needed)")
            skipped += 1
            processed[reply["stats_id"]] = {"action": "skipped", "reason": cat}
            continue

        # 3. Draft via Opus
        print(f"  drafting response...")
        draft = draft_reply(reply, classification)
        if draft.strip() == "SKIP_REPLY":
            print(f"  → skipped per Opus directive")
            skipped += 1
            processed[reply["stats_id"]] = {"action": "skipped", "reason": "opus_skip"}
            continue

        # 4. Push to Trello (or fallback markdown)
        url = create_trello_card(reply, classification, draft, dry_run=dry_run)
        print(f"  → written to: {url[:100]}")

        # 5. Push a FULL preview to the operator's phone via Pushover so he can review
        #    + decide YES/NO in real time. Tap-to-jump to Smartlead to send.
        pushover_ok = send_pushover_preview(reply, classification, draft,
                                             destination_url=url, dry_run=dry_run)
        if pushover_ok:
            print(f"  → Pushover preview sent to phone ✓")
        else:
            print(f"  → Pushover not configured (would have sent preview)")

        drafted += 1
        processed[reply["stats_id"]] = {
            "action": "drafted",
            "category": cat,
            "destination": url,
            "pushover_sent": pushover_ok,
            "drafted_at": datetime.now(timezone.utc).isoformat(),
        }

    if not dry_run:
        save_state(state)

    summary = {
        "total_new": len(new_replies),
        "drafted": drafted,
        "skipped": skipped,
    }
    print(f"\n{'='*70}\nSUMMARY\n{'='*70}")
    for k, v in summary.items():
        print(f"  {k}: {v}")
    return summary


# ============================================================
# CLI
# ============================================================

def main():
    ap = argparse.ArgumentParser(description="Classify + draft inbound reply responses")
    ap.add_argument("--since", type=int, default=3, help="look back N days for replies (default 3)")
    ap.add_argument("--dry-run", action="store_true", help="preview without writing cards / files")
    ap.add_argument("--lead-email", help="process only this specific lead's reply")
    args = ap.parse_args()

    run(since_days=args.since, dry_run=args.dry_run, email_filter=args.lead_email)


if __name__ == "__main__":
    main()
