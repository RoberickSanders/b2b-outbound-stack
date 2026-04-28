#!/usr/bin/env python3.13
"""
meeting_prep.py — Generate a pre-call prep brief for a prospect.

Why this exists:
  the operator walks into most calls cold. Close rate goes up when you arrive with:
    - What their company does (30 sec of context)
    - Recent news, hiring, or expansion signals
    - Past thread with the prospect (what they said, what Sender One replied)
    - 2-3 likely pain points
    - 3 questions to open the call with
  Building this manually for every call = 15-20 min. This agent generates
  it in 30 seconds, pulling from Smartlead (thread history) + Serper
  (company intel) + Kimi (synthesis).

Usage:
  python3 tools/meeting_prep.py --email bjankowski@pmgdevelop.com
  python3 tools/meeting_prep.py --email X --out prep.md
  python3 tools/meeting_prep.py --lead-id 3588388312

Output:
  A markdown brief printed to stdout, optionally saved to a file. Includes
  company summary, recent signals, full thread history with the prospect,
  and suggested call-opener questions.

Standalone tool — does not modify Forge code.
"""

import argparse
import json
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

import requests

SCRIPT_DIR = Path(__file__).resolve().parent
LEAD_PIPELINE_DIR = SCRIPT_DIR.parent
WORKSPACE_ROOT = LEAD_PIPELINE_DIR.parent.parent
sys.path.insert(0, str(LEAD_PIPELINE_DIR))

try:
    from dotenv import load_dotenv
    load_dotenv(WORKSPACE_ROOT / ".env", override=True)
    load_dotenv(LEAD_PIPELINE_DIR / ".env", override=True)
except ImportError:
    pass

SMARTLEAD_KEY = os.environ.get("SMARTLEAD_API_KEY", "")
SMARTLEAD_BASE = "https://server.smartlead.ai/api/v1"


# ============================================================
# Helpers
# ============================================================

def _strip_html(body: str) -> str:
    """Quick-and-dirty HTML stripper for thread bodies."""
    if not body:
        return ""
    text = re.sub(r"<br\s*/?>", "\n", body, flags=re.IGNORECASE)
    text = re.sub(r"</p>", "\n", text, flags=re.IGNORECASE)
    text = re.sub(r"<[^>]+>", "", text)
    text = re.sub(r"\n{3,}", "\n\n", text).strip()
    return text


def lookup_lead(email: str = None, lead_id: int = None) -> dict:
    """Find a lead in Smartlead by email or by ID. Returns the full lead payload."""
    if email:
        r = requests.get(
            f"{SMARTLEAD_BASE}/leads/",
            params={"api_key": SMARTLEAD_KEY, "email": email},
            timeout=30,
        )
        if r.status_code == 200:
            return r.json()
    # TODO: fallback to /leads/{id} if email not found
    return None


def pull_thread(campaign_id: int, lead_id: int) -> list:
    """Pull the message history between sender and prospect for one campaign."""
    r = requests.get(
        f"{SMARTLEAD_BASE}/campaigns/{campaign_id}/leads/{lead_id}/message-history",
        params={"api_key": SMARTLEAD_KEY},
        timeout=30,
    )
    if r.status_code != 200:
        return []
    return r.json().get("history", [])


# ============================================================
# Brief generation
# ============================================================

def build_brief(lead: dict, thread: list) -> str:
    """Use Kimi to synthesize everything into a 1-page prep brief."""
    try:
        from llm_router import get_light_client
        from deep_research import research_lead, load_cache, save_cache
    except Exception as e:
        return f"(Dependencies not available: {e})"

    first_name = lead.get("first_name", "") or "the prospect"
    last_name = lead.get("last_name", "") or ""
    full_name = f"{first_name} {last_name}".strip()
    company = lead.get("company_name", "") or "(unknown company)"
    domain = (lead.get("website", "") or "").replace("https://", "").replace("http://", "").split("/")[0]
    email = lead.get("email", "")

    # 1. Research company via Deep Research agent
    cache = load_cache()
    research = research_lead(first_name, company, domain, niche_context=None, cache=cache)
    save_cache(cache)

    research_summary = ""
    if research.get("opener"):
        research_summary = f"Personalization angle: {research['opener']}\nSignals: {research.get('signals_used', [])}"
    else:
        research_summary = "No strong personalization signal found."

    # 2. Format the thread
    thread_text = ""
    for i, msg in enumerate(thread, 1):
        direction = msg.get("type", "?")
        sender = msg.get("from", "?")
        time = (msg.get("time", "") or "")[:16].replace("T", " ")
        subject = msg.get("subject", "")
        body = _strip_html(msg.get("email_body", ""))
        # Trim quoted history
        for marker in ["On Fri,", "On Thu,", "On Wed,", "On Tue,", "On Mon,", "\nFrom:", "-- Forwarded"]:
            idx = body.find(marker)
            if idx > 100:
                body = body[:idx].strip()
                break
        thread_text += f"\n[{i}] {direction} · {time}\n"
        thread_text += f"From: {sender}\nSubject: {subject}\n\n{body[:800]}\n"
        thread_text += "---\n"

    # 3. Kimi synthesizes the brief
    client, model = get_light_client()
    prompt = f"""You are preparing a pre-meeting brief for a cold-email agency owner.
The prospect just agreed to a meeting. You have 30 seconds to orient him before the call.

PROSPECT:
  Name:    {full_name}
  Company: {company}
  Domain:  {domain}
  Email:   {email}

COMPANY RESEARCH (from web search — use this as factual grounding):
{research_summary}

FULL EMAIL THREAD HISTORY WITH THIS PROSPECT:
{thread_text if thread_text else "(No thread yet — this is first contact or lead data only.)"}

CRITICAL RULES — DO NOT VIOLATE:
1. **Never invent facts.** If information isn't in the thread or research above, DON'T include it. Say "unknown" or skip the section.
2. **Never hallucinate actions the prospect took** (e.g., "they sent prequal forms", "added us to their portal") unless you can quote the exact thread message that says so.
3. **Quote directly from the thread** when summarizing what they said. If the thread is short or generic, keep the summary short.
4. **Pain points must be INFERRED from the thread's signals + industry research**, not invented. Label them "likely" not "known."
5. If a section would require invention to fill, output "(no strong signal from the thread)".

Output a crisp markdown brief with these exact sections:

## 1-sentence company summary
(what they do, for whom — based ONLY on research above)

## What they said in the thread
(QUOTE OR PARAPHRASE specific messages. If no thread, write "(first contact)".)

## 2-3 likely pain points
(Inferred from research + thread. Label each with 'likely' — don't claim certainty.)

## 3 opening questions to ask
(Open-ended. Avoid yes/no. Tie to what you actually know.)

## 1 thing NOT to do
(ONLY if the thread has a clear signal, e.g. "they pushed back on X, don't repeat it." Otherwise skip this section.)

Be concrete. No filler. Under 250 words total. NEVER fabricate details."""
    try:
        resp = client.messages.create(
            model=model, max_tokens=1200,
            messages=[{"role": "user", "content": prompt}],
        )
        brief_body = resp.content[0].text.strip()
    except Exception as e:
        brief_body = f"(Brief generation failed: {e})"

    # 4. Assemble final output
    out = f"""# Meeting Prep: {full_name} — {company}

**Email:** {email}
**Domain:** {domain or '(not provided)'}
**Generated:** {datetime.now(timezone.utc).isoformat()}
**Research confidence:** {research.get('confidence', 0):.2f}

{brief_body}

---

## Raw thread transcript
{thread_text if thread_text else '(no thread)'}

*Generated by `meeting_prep.py` — review 30 sec before the call.*
"""
    return out


# ============================================================
# CLI
# ============================================================

def main():
    ap = argparse.ArgumentParser(description="Generate pre-call prep brief for a prospect")
    ap.add_argument("--email", help="prospect email")
    ap.add_argument("--lead-id", type=int, help="Smartlead lead ID (alternative to email)")
    ap.add_argument("--out", help="save brief to this file (default: stdout)")
    args = ap.parse_args()

    if not args.email and not args.lead_id:
        ap.print_help()
        sys.exit(1)

    # Lookup
    lead_payload = lookup_lead(email=args.email)
    if not lead_payload:
        print(f"Could not find lead: {args.email or args.lead_id}")
        sys.exit(1)

    # Find the campaign this lead is associated with to pull the thread
    campaign_data = lead_payload.get("lead_campaign_data", [])
    if not campaign_data:
        print("Lead found but no campaign association — cannot pull thread.")
        thread = []
        cid = None
    else:
        cid = campaign_data[0].get("campaign_id")
        lid = lead_payload.get("id")
        thread = pull_thread(cid, int(lid)) if cid else []

    # Build brief
    brief = build_brief(lead_payload, thread)

    if args.out:
        Path(args.out).write_text(brief)
        print(f"✓ Brief saved to {args.out}")
    else:
        print(brief)


if __name__ == "__main__":
    main()
