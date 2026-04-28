#!/usr/bin/env python3.13
"""
campaign_analyzer.py — Learn from every campaign (active or completed).

Why this exists:
  CLIENT_C has run 33+ campaigns in Smartlead. Almost none get analyzed after
  they launch. The same mistakes (banned openers, wrong verticals, wrong CTAs)
  keep recurring because nothing feeds past reply data back into the next
  campaign brief. This tool closes that loop.

What it does:
  Given a campaign_id (or --auto-scan for recently-completed campaigns),
  it pulls every reply thread, classifies each reply via Kimi K2.6, finds
  patterns across variants / sequences / subjects, and writes a short
  markdown "playbook update" brief to
  01-Projects/{client}/campaign_analyses/.

  The brief captures:
    - Headline metrics (sent, replies, meetings, bounces)
    - What the winning subject/variant was
    - Most common reply types (interested, objection, OOO, not-interested)
    - Objection themes ("we already have a vendor", "price too high", etc.)
    - Vertical-fit mismatches ("many replies said they're not X")
    - 3 recommendations for next campaign in same vertical

  Next time you run Forge for that vertical, load the brief as context so the
  copy gen inherits what we learned.

Usage:
  python3 tools/campaign_analyzer.py --campaign 3147070
  python3 tools/campaign_analyzer.py --auto-scan                    # analyze all new-ish campaigns
  python3 tools/campaign_analyzer.py --auto-scan --since 7          # campaigns completed in last 7 days
  python3 tools/campaign_analyzer.py --campaign 3147070 --dry-run   # preview without writing file

Standalone tool — does not modify Forge code.
"""

import argparse
import json
import os
import re
import sys
import time
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests

# Paths
SCRIPT_DIR = Path(__file__).resolve().parent
LEAD_PIPELINE_DIR = SCRIPT_DIR.parent
WORKSPACE_ROOT = LEAD_PIPELINE_DIR.parent.parent
PROJECTS_DIR = WORKSPACE_ROOT / "01-Projects"

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
# Smartlead HTTP helpers
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


# ============================================================
# Client classification (matches autopilot)
# ============================================================

CLIENT_KEYWORDS = {
    "client_c": ["ClientC", "CLIENT_C ", "RevenueMechanic"],
    "client_a": ["ClientA", "CLIENT_A "],
    "client_b": ["ClientB"],
}


def classify_client(campaign_name: str) -> str:
    name = (campaign_name or "").lower()
    for client, kws in CLIENT_KEYWORDS.items():
        if any(kw.lower() in name for kw in kws):
            return client
    return "unknown"


# ============================================================
# Reply text extraction
# ============================================================

_HTML_RE = re.compile(r"<[^>]+>")
_WHITESPACE_RE = re.compile(r"\s+")


def clean_reply_text(raw: str, max_chars: int = 1500) -> str:
    """Strip HTML tags + collapse whitespace + trim quoted reply history."""
    if not raw:
        return ""
    # Strip HTML
    text = _HTML_RE.sub(" ", raw)
    # Normalize whitespace
    text = _WHITESPACE_RE.sub(" ", text).strip()
    # Cut at common quoted-reply markers
    for marker in [
        "On Fri, Apr", "On Thu, Apr", "On Wed, Apr", "On Tue, Apr", "On Mon, Apr",
        "On Fri, Mar", "On Thu, Mar", "On Wed, Mar", "On Tue, Mar", "On Mon, Mar",
        "From: ", "\n\n>",
        "On Fri,", "On Thu,", "On Wed,", "On Tue,", "On Mon,", "On Sat,", "On Sun,",
    ]:
        idx = text.find(marker)
        if idx > 50:  # keep at least the actual reply, not just the quoted part
            text = text[:idx]
            break
    return text[:max_chars].strip()


# ============================================================
# Reply classification via Kimi
# ============================================================

def classify_reply_batch(replies: list) -> list:
    """Classify a batch of replies (up to 20) via one Kimi call.

    Each reply in the list is {'lead_email': str, 'reply_text': str}.
    Returns list of dicts with keys: lead_email, category, objection_theme, sentiment.
    """
    if not replies:
        return []

    try:
        from llm_router import get_light_client
        client, model = get_light_client()
    except Exception:
        return [{"lead_email": r["lead_email"], "category": "unclassified", "objection_theme": None, "sentiment": "unknown"} for r in replies]

    numbered = "\n\n".join([f"#{i+1}: {r['reply_text'][:800]}" for i, r in enumerate(replies)])
    prompt = f"""Classify each of these cold-email replies. For each reply, output a JSON object with:
  - "n": the reply number (#1, #2, etc.)
  - "category": one of [interested, info_request, objection, oof, not_interested, not_the_right_person, wrong_person, unsubscribe, other]
  - "objection_theme": if category is "objection", a 2-4 word theme (e.g., "already has vendor", "price too high", "bad timing"). Otherwise null.
  - "sentiment": one of [positive, neutral, negative]

REPLIES:
{numbered}

Output a JSON array with one object per reply. Order must match input order. No prose before or after the array."""

    try:
        resp = client.messages.create(
            model=model, max_tokens=2000,
            messages=[{"role": "user", "content": prompt}]
        )
        text = resp.content[0].text.strip()
        # Strip markdown fences
        text = re.sub(r"^```(?:json)?\s*", "", text)
        text = re.sub(r"\s*```$", "", text)
        classifications = json.loads(text)
        # Merge with lead emails
        out = []
        for i, c in enumerate(classifications):
            if i < len(replies):
                out.append({
                    "lead_email": replies[i]["lead_email"],
                    "category": c.get("category", "unclassified"),
                    "objection_theme": c.get("objection_theme"),
                    "sentiment": c.get("sentiment", "unknown"),
                })
        return out
    except Exception as e:
        print(f"  [WARN] classification failed: {e}")
        return [{"lead_email": r["lead_email"], "category": "unclassified", "objection_theme": None, "sentiment": "unknown"} for r in replies]


# ============================================================
# Campaign data pull
# ============================================================

def fetch_campaign_data(campaign_id: int) -> dict:
    """Pull everything needed for a post-mortem on one campaign."""
    campaign = sl_get(f"/campaigns/{campaign_id}")
    if not campaign:
        return None
    analytics = sl_get(f"/campaigns/{campaign_id}/analytics") or {}
    sequences = sl_get(f"/campaigns/{campaign_id}/sequences") or []

    # Pull all leads with pagination
    leads = []
    offset = 0
    while True:
        page = sl_get(f"/campaigns/{campaign_id}/leads", {"limit": 100, "offset": offset})
        if not page:
            break
        batch = page.get("data", [])
        if not batch:
            break
        leads.extend(batch)
        if len(batch) < 100:
            break
        offset += 100

    # Pull message history for every lead that has a reply category
    # (Smartlead marks replies via lead_category_id)
    replies = []
    for lead_map in leads:
        lead = lead_map.get("lead") or {}
        email = lead.get("email")
        cat = lead_map.get("lead_category_id")
        # Only pull message history for leads that replied. Categories 1-8 typically
        # indicate some engagement (varies by Smartlead setup).
        if not email or not cat:
            continue
        lead_id = lead.get("id")
        if not lead_id:
            continue
        hist = sl_get(f"/campaigns/{campaign_id}/leads/{lead_id}/message-history")
        if not hist:
            continue
        history = hist.get("history", [])
        # Find first REPLY message
        for msg in history:
            if msg.get("type") == "REPLY":
                raw_body = msg.get("email_body", "")
                replies.append({
                    "lead_email": email,
                    "lead_category_id": cat,
                    "sequence_number": msg.get("sequence_number"),
                    "reply_text": clean_reply_text(raw_body),
                    "reply_time": msg.get("time"),
                })
                break

    return {
        "campaign": campaign,
        "analytics": analytics,
        "sequences": sequences,
        "lead_count": len(leads),
        "replies": replies,
    }


# ============================================================
# Pattern analysis
# ============================================================

def analyze_patterns(data: dict, classifications: list) -> dict:
    """Given campaign data + per-reply classifications, extract patterns."""
    # Count category distribution
    category_counts = Counter(c["category"] for c in classifications)

    # Reply rate by sequence number
    replies_by_seq = Counter(r["sequence_number"] for r in data["replies"])

    # Objection themes
    objection_themes = Counter(
        c["objection_theme"] for c in classifications
        if c.get("objection_theme") and c["category"] == "objection"
    )

    # Sentiment split
    sentiment_counts = Counter(c["sentiment"] for c in classifications)

    # Sequence subjects for reference
    sequence_subjects = {s.get("seq_number"): s.get("subject", "(body only)") for s in data["sequences"]}

    return {
        "category_counts": dict(category_counts),
        "replies_by_sequence": dict(replies_by_seq),
        "objection_themes": dict(objection_themes.most_common(5)),
        "sentiment_counts": dict(sentiment_counts),
        "sequence_subjects": sequence_subjects,
    }


# ============================================================
# Write markdown brief
# ============================================================

def _load_framework() -> str:
    """Load the distilled copywriting framework for framework-aware diagnostics.

    Same source file used by niche_research.py. Returns empty string if
    missing (analyzer still runs, just without framework layer).
    """
    try:
        # This file is at .../02-Areas/lead-pipeline/tools/campaign_analyzer.py
        fw_path = Path(__file__).resolve().parent.parent.parent.parent / "03-Resources" / "copywriting-frameworks" / "PROMPT.md"
        return fw_path.read_text()
    except Exception:
        return ""


def _build_sequence_snapshot(data: dict, max_body_chars: int = 600) -> str:
    """Extract email content from sequences for framework diagnostic context.

    Smartlead A/B variants live in sequence_variants[]; top-level email_body
    may be empty. Check both. Truncate bodies so prompt stays lean.
    """
    lines = []
    for seq in data.get("sequences", []):
        seq_num = seq.get("seq_number", "?")
        subj = seq.get("subject", "") or "(body-only follow-up)"
        body = seq.get("email_body", "") or ""
        if not body.strip():
            for v in seq.get("sequence_variants", []) or []:
                if (v.get("email_body") or "").strip():
                    body = v["email_body"]
                    subj = v.get("subject") or subj
                    break
        # Strip HTML tags crudely so framework diagnostics see the text
        body_text = re.sub(r"<[^>]+>", " ", body).strip()
        body_text = re.sub(r"\s+", " ", body_text)[:max_body_chars]
        lines.append(f"  Email {seq_num}: subject='{subj}'\n    body='{body_text}'")
    return "\n".join(lines) if lines else "(no sequence data)"


def generate_brief_text(data: dict, patterns: dict) -> str:
    """Use Kimi to turn raw patterns into a framework-aware action brief.

    Injects the 6-book copywriting framework so the analyzer can diagnose:
    - Awareness-level mismatches (copy written for wrong awareness stage)
    - Lead-type mismatches (wrong opening structure for the vertical)
    - Missing Cialdini weapons
    - Life Force 8 misalignment
    In addition to the existing 3-section output, returns framework
    diagnostic blocks when useful.
    """
    try:
        from llm_router import get_light_client
        client, model = get_light_client()
    except Exception:
        return "(LLM unavailable — skipped narrative generation)"

    campaign_name = data["campaign"].get("name", "unknown")
    sent = int(data["analytics"].get("sent_count", 0) or 0)
    replies = int(data["analytics"].get("reply_count", 0) or 0)
    bounces = int(data["analytics"].get("bounce_count", 0) or 0)
    reply_rate = (replies / sent * 100) if sent else 0
    bounce_rate = (bounces / sent * 100) if sent else 0

    framework = _load_framework()
    sequence_snapshot = _build_sequence_snapshot(data)

    framework_block = ""
    diagnostic_section = ""
    if framework:
        framework_block = f"""
COPYWRITING FRAMEWORK (for diagnostic reasoning):
{framework}
"""
        diagnostic_section = """
## Framework diagnostics
(Identify which copy framework layer failed. Use the awareness × sophistication matrix, the 6 lead types, and the 6 Cialdini weapons from the framework above to analyze the actual email copy. Be specific: cite the subject line or a phrase and name which layer mismatched.)

- **Awareness level target:** (what level was the copy written for? e.g., Problem Aware)
- **Likely actual prospect awareness:** (what level were prospects probably at? e.g., Unaware)
- **Mismatch identified:** (yes/no + 1-sentence explanation)
- **Lead type used:** (Offer / Promise / Problem-Solution / Big Secret / Proclamation / Story)
- **Lead type appropriateness:** (was this the right lead type given prospect awareness? if not, what would have fit better?)
- **Cialdini weapons detected:** (list: Reciprocation, Commitment, Social Proof, Liking, Authority, Scarcity)
- **Weapons missing that would have lifted reply rate:** (specific recommendation)
- **Life Force 8 desire channeled:** (which one? if none explicit, flag it)
"""

    prompt = f"""You are analyzing a completed cold email campaign for ClientC, a lead gen agency.
{framework_block}
CAMPAIGN: {campaign_name}
METRICS:
  - Sent: {sent}
  - Replies: {replies} ({reply_rate:.2f}%)
  - Bounces: {bounces} ({bounce_rate:.2f}%)

ACTUAL SEQUENCE COPY (for framework diagnosis):
{sequence_snapshot}

REPLY BREAKDOWN:
  Categories: {patterns['category_counts']}
  Sentiment: {patterns['sentiment_counts']}
  Objection themes: {patterns['objection_themes']}
  Replies by sequence: {patterns['replies_by_sequence']}

Output a markdown brief in this exact format:

## What worked
(2-3 bullets on what drove the replies — subject line, sequence number, angle that resonated)

## What didn't
(2-3 bullets on what underperformed — banned openers that slipped through, vertical mismatches, objection recurrence)
{diagnostic_section}
## 3 things to change next time
(3 concrete, specific action items for the next campaign in this vertical — reference the framework diagnostics above when naming fixes, e.g., "switch from Problem-Solution to Story Lead because prospects are Unaware not Problem Aware")

Be specific. Don't say "improve copy" — say "open with a cost-recovery framing instead of generic pain". No filler. No lead-up. Start with "## What worked".
"""
    try:
        resp = client.messages.create(
            model=model, max_tokens=1500,
            messages=[{"role": "user", "content": prompt}]
        )
        return resp.content[0].text.strip()
    except Exception as e:
        return f"(Narrative generation failed: {e})"


def write_postmortem_file(client: str, data: dict, patterns: dict, brief_text: str, dry_run: bool = False) -> Path:
    campaign_name = data["campaign"].get("name", f"campaign-{data['campaign'].get('id')}")
    # Slugify
    slug = re.sub(r"[^a-z0-9]+", "-", campaign_name.lower()).strip("-")[:80]

    # Output path
    out_dir = PROJECTS_DIR / client / "campaign_analyses"
    out_path = out_dir / f"{slug}.md"

    # Compose markdown
    md = f"""# Campaign Analysis: {campaign_name}

**Campaign ID:** {data['campaign'].get('id')}
**Client:** {client}
**Status:** {data['campaign'].get('status')}
**Analyzed:** {datetime.now(timezone.utc).isoformat()}

## Metrics
| Metric | Value |
|---|---|
| Sent | {data['analytics'].get('sent_count', 0)} |
| Replies | {data['analytics'].get('reply_count', 0)} |
| Bounces | {data['analytics'].get('bounce_count', 0)} |
| Opens | {data['analytics'].get('open_count', 0)} |
| Leads in campaign | {data['lead_count']} |

## Reply breakdown

| Category | Count |
|---|---|
"""
    for cat, count in sorted(patterns["category_counts"].items(), key=lambda x: -x[1]):
        md += f"| {cat} | {count} |\n"

    if patterns["objection_themes"]:
        md += "\n## Most common objections\n\n"
        for theme, count in patterns["objection_themes"].items():
            md += f"- **{theme}** ({count}×)\n"

    md += f"\n## Replies by sequence email\n\n"
    for seq, count in sorted(patterns["replies_by_sequence"].items()):
        subject = patterns["sequence_subjects"].get(seq, "(body only)")
        md += f"- Email {seq}: {count} replies  _subject: {subject[:60]}_\n"

    md += f"\n## Sequences used\n\n"
    for seq_num, subject in sorted(patterns["sequence_subjects"].items()):
        md += f"**Email {seq_num}** — {subject}\n\n"

    md += f"\n{brief_text}\n"

    md += f"\n---\n*Generated by `campaign_analyzer.py` — feed this into next Forge run for the same vertical.*\n"

    if dry_run:
        print(f"\n[DRY RUN] would write to {out_path}")
        print(md[:2000])
        return out_path

    out_dir.mkdir(parents=True, exist_ok=True)
    out_path.write_text(md)
    return out_path


# ============================================================
# CLI orchestration
# ============================================================

def analyze_campaign(campaign_id: int, dry_run: bool = False) -> Path:
    print(f"Fetching campaign {campaign_id}...")
    data = fetch_campaign_data(campaign_id)
    if not data:
        print(f"ERROR: could not fetch campaign {campaign_id}")
        return None

    campaign_name = data["campaign"].get("name", "unknown")
    client = classify_client(campaign_name)
    if client == "unknown":
        print(f"ERROR: cannot identify client from campaign name '{campaign_name}'")
        return None

    print(f"  Campaign: {campaign_name}")
    print(f"  Client:   {client}")
    print(f"  Sent:     {data['analytics'].get('sent_count', 0)}")
    print(f"  Replies:  {len(data['replies'])}")

    if not data["replies"]:
        print(f"  No replies to analyze — skipping post-mortem.")
        return None

    # Classify in batches of 20
    print(f"  Classifying {len(data['replies'])} replies via Kimi...")
    all_classifications = []
    for i in range(0, len(data["replies"]), 20):
        batch = data["replies"][i:i+20]
        classifications = classify_reply_batch(batch)
        all_classifications.extend(classifications)

    patterns = analyze_patterns(data, all_classifications)
    print(f"  Pattern summary: {patterns['category_counts']}")

    print(f"  Generating brief...")
    brief_text = generate_brief_text(data, patterns)

    out_path = write_postmortem_file(client, data, patterns, brief_text, dry_run=dry_run)
    if not dry_run:
        print(f"  Saved: {out_path}")
        # Feedback loop — update _predictions.jsonl entry for this niche
        # with the actual reply rate, so niche_research can self-calibrate over time
        try:
            _update_prediction_log(campaign_name, data, patterns)
        except Exception as e:
            print(f"  (prediction log update skipped: {e})")
    return out_path


def _update_prediction_log(campaign_name: str, data: dict, patterns: dict) -> None:
    """Backfill actual_reply_rate into niche_research/_predictions.jsonl.

    When a campaign post-mortem runs, find the matching prediction entry
    (by niche keyword overlap) and update actual_reply_rate. This builds
    the dataset niche_research.py needs to recalibrate its scoring weights.
    """
    log_path = Path(__file__).resolve().parent.parent.parent.parent / "03-Resources" / "niche-research" / "_predictions.jsonl"
    if not log_path.exists():
        return

    sent = int(data["analytics"].get("sent_count", 0) or 0)
    replies = int(data["analytics"].get("reply_count", 0) or 0)
    if sent == 0:
        return
    actual_rate = replies / sent * 100

    # Match by keyword overlap with the campaign name
    campaign_words = set(w.lower() for w in re.findall(r"[a-z]+", campaign_name.lower()) if len(w) >= 4)
    if not campaign_words:
        return

    lines = log_path.read_text().splitlines()
    updated = False
    new_lines = []
    best_match = None
    best_overlap = 0
    for i, ln in enumerate(lines):
        try:
            entry = json.loads(ln)
        except Exception:
            new_lines.append(ln)
            continue
        niche_words = set(w.lower() for w in re.findall(r"[a-z]+", (entry.get("niche") or "").lower()) if len(w) >= 4)
        overlap = len(campaign_words & niche_words)
        if overlap > best_overlap and entry.get("actual_reply_rate") is None:
            best_overlap = overlap
            best_match = i

    if best_match is not None and best_overlap >= 1:
        for i, ln in enumerate(lines):
            try:
                entry = json.loads(ln)
            except Exception:
                new_lines.append(ln)
                continue
            if i == best_match:
                entry["actual_reply_rate"] = round(actual_rate, 2)
                entry["actual_campaign"] = campaign_name
                entry["actual_sent"] = sent
                entry["actual_replies"] = replies
                updated = True
            new_lines.append(json.dumps(entry))

        if updated:
            log_path.write_text("\n".join(new_lines) + "\n")
            print(f"  ✓ Updated prediction log: matched to niche entry with {actual_rate:.2f}% actual reply rate")


def auto_scan(since_days: int = 30, dry_run: bool = False):
    """Find completed campaigns and generate post-mortems for each."""
    print(f"Fetching all campaigns...")
    campaigns = sl_get("/campaigns/")
    if not campaigns:
        sys.exit("ERROR: could not fetch campaign list")

    cutoff = datetime.now(timezone.utc) - timedelta(days=since_days)
    candidates = []
    for c in campaigns:
        status = c.get("status")
        if status != "COMPLETED":
            continue
        created = c.get("created_at", "")
        try:
            created_dt = datetime.fromisoformat(created.replace("Z", "+00:00"))
            if created_dt < cutoff:
                continue
        except Exception:
            pass
        candidates.append(c)

    print(f"  → {len(candidates)} COMPLETED campaigns in last {since_days} days")
    for c in candidates:
        print(f"\n{'='*70}")
        analyze_campaign(c["id"], dry_run=dry_run)


def main():
    ap = argparse.ArgumentParser(description="Campaign post-mortem analyzer")
    ap.add_argument("--campaign", type=int, help="Smartlead campaign ID to analyze")
    ap.add_argument("--auto-scan", action="store_true", help="analyze all recently-completed campaigns")
    ap.add_argument("--since", type=int, default=30, help="# days back for --auto-scan")
    ap.add_argument("--dry-run", action="store_true", help="preview without writing files")
    args = ap.parse_args()

    if args.campaign:
        analyze_campaign(args.campaign, dry_run=args.dry_run)
    elif args.auto_scan:
        auto_scan(since_days=args.since, dry_run=args.dry_run)
    else:
        ap.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
