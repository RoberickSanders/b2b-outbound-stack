#!/usr/bin/env python3
"""
forge_compound.py — Mine winning angles from past campaigns to feed the next one.

Closes the compounding-data feedback loop Oliverify described: every campaign's
result becomes context for the next campaign's prompt. Without this, every new
campaign starts blind.

What it does:
  1. For a given (client, niche), pulls all leads ever sent (leads.status='sent')
  2. Joins to the meetings table to find which leads converted
  3. Aggregates winners by industry, title, company size proxy, and geography
  4. Writes a markdown brief to lookalike-runs/winning-angles/
  5. The brief is meant to be injected into copy-generation prompts so the next
     campaign's offer/angle leans into segments that historically converted

Two output modes:
  - 'brief'  — markdown context doc (default, human + LLM readable)
  - 'json'   — structured payload for tools that ingest programmatically

Schema we rely on (verified 2026-04-25):
  leads:    id, email, first_name, last_name, title, company, domain, phone,
            linkedin_url, city, state, industry, source, niche, client, tier,
            mv_result, status, sent_date, date_added, date_updated, notes
  meetings: id, lead_id, email, company, contact_name, meeting_date,
            meeting_type, client, niche, campaign_name, source_run, status,
            outcome, deal_value, close_date, monthly_revenue, total_revenue

  Positive outcomes treated as "winners":
      meetings.outcome IN ('interested', 'closed_won', 'meeting_booked',
                           'qualified', 'demo_booked', 'proposal_sent')

Usage:
    # Generate a brief for an existing client + niche combo
    python3 tools/forge_compound.py --client client_a --niche restaurants

    # JSON for programmatic injection
    python3 tools/forge_compound.py --client client_c --niche cost-segregation --format json

    # List all (client, niche) combos with conversion data
    python3 tools/forge_compound.py --list

    # Dry run — print to stdout instead of writing the brief file
    python3 tools/forge_compound.py --client client_a --niche churches --dry-run
"""

import argparse
import json
import os
import sqlite3
import sys
from collections import Counter
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
LEAD_PIPELINE_DIR = SCRIPT_DIR.parent
DB_PATH = LEAD_PIPELINE_DIR / "master-leads" / "master_leads.db"
BRIEFS_DIR = LEAD_PIPELINE_DIR / "winning-angles"

POSITIVE_OUTCOMES = (
    "interested", "closed_won", "meeting_booked", "qualified",
    "demo_booked", "proposal_sent",
)


# ─── DB ──────────────────────────────────────────────────────────────────────
@contextmanager
def open_db():
    if not DB_PATH.is_file():
        raise FileNotFoundError(f"master_leads.db not found at {DB_PATH}")
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
    finally:
        conn.close()


def list_combos() -> list[dict]:
    """Return every (client, niche) combo with at least one positive meeting."""
    with open_db() as conn:
        cur = conn.cursor()
        rows = cur.execute(f"""
            SELECT m.client, m.niche,
                   COUNT(*) AS positive_meetings,
                   COUNT(DISTINCT m.email) AS unique_positives,
                   SUM(CASE WHEN m.outcome = 'closed_won' THEN 1 ELSE 0 END) AS closed
              FROM meetings m
             WHERE m.outcome IN ({','.join('?' * len(POSITIVE_OUTCOMES))})
               AND m.client IS NOT NULL AND m.niche IS NOT NULL
             GROUP BY m.client, m.niche
             ORDER BY positive_meetings DESC, m.client, m.niche
        """, POSITIVE_OUTCOMES).fetchall()
    return [dict(r) for r in rows]


def fetch_winners(client: str, niche: str) -> list[dict]:
    """Pull every winning lead's profile for (client, niche).

    Joins meetings to leads on email (since lead_id can be null on imported leads)
    so we get the demographic data for segmentation.
    """
    placeholders = ",".join("?" * len(POSITIVE_OUTCOMES))
    sql = f"""
        SELECT
            m.email, m.outcome, m.campaign_name, m.meeting_date,
            m.deal_value, m.monthly_revenue, m.total_revenue,
            l.title, l.industry, l.company, l.domain, l.city, l.state,
            l.source, l.tier, l.sent_date
          FROM meetings m
          LEFT JOIN leads l
                ON LOWER(l.email) = LOWER(m.email)
               AND l.client = m.client
               AND l.niche  = m.niche
         WHERE m.client = ?
           AND m.niche  = ?
           AND m.outcome IN ({placeholders})
         ORDER BY m.meeting_date DESC
    """
    with open_db() as conn:
        cur = conn.cursor()
        rows = cur.execute(sql, (client, niche, *POSITIVE_OUTCOMES)).fetchall()
    return [dict(r) for r in rows]


def fetch_send_volume(client: str, niche: str) -> dict:
    """Count total leads sent for (client, niche) — denominator for conversion rate."""
    with open_db() as conn:
        cur = conn.cursor()
        sent = cur.execute("""
            SELECT COUNT(*) FROM leads
             WHERE client = ? AND niche = ? AND status = 'sent'
        """, (client, niche)).fetchone()[0]
        total = cur.execute("""
            SELECT COUNT(*) FROM leads
             WHERE client = ? AND niche = ?
        """, (client, niche)).fetchone()[0]
    return {"sent": sent, "total_leads": total}


# ─── Aggregation ─────────────────────────────────────────────────────────────
def _topn(items, n=5):
    """Return top-N as list of (value, count). Drops empty-string keys."""
    counter = Counter(x for x in items if x)
    return counter.most_common(n)


def aggregate(winners: list[dict]) -> dict:
    """Slice winners by segmentation dimensions."""
    return {
        "industries":      _topn([w.get("industry") for w in winners]),
        "titles":          _topn([w.get("title") for w in winners]),
        "states":          _topn([w.get("state") for w in winners]),
        "cities":          _topn([w.get("city") for w in winners]),
        "campaigns":       _topn([w.get("campaign_name") for w in winners]),
        "outcomes":        _topn([w.get("outcome") for w in winners]),
        "sources":         _topn([w.get("source") for w in winners]),
    }


def revenue_summary(winners: list[dict]) -> dict:
    """Aggregate revenue across closed_won winners."""
    closed = [w for w in winners if w.get("outcome") == "closed_won"]
    total_rev = sum((w.get("total_revenue") or 0) for w in closed)
    monthly_rev = sum((w.get("monthly_revenue") or 0) for w in closed)
    return {
        "closed_won_count": len(closed),
        "total_revenue":    round(total_rev, 2),
        "monthly_revenue":  round(monthly_rev, 2),
        "avg_deal_size":    round(total_rev / len(closed), 2) if closed else 0,
    }


# ─── Brief writer ────────────────────────────────────────────────────────────
def render_brief(client: str, niche: str, winners: list[dict],
                 send_volume: dict, agg: dict, rev: dict) -> str:
    today = datetime.now().strftime("%Y-%m-%d")
    sent = send_volume["sent"]
    pos = len(winners)
    rate = (100.0 * pos / sent) if sent else 0.0

    def _fmt_topn(label: str, items: list[tuple[str, int]]) -> str:
        if not items:
            return f"  - {label}: (no data)"
        bullets = "\n".join(f"  - {v}  [{c}]" for v, c in items)
        return f"### {label}\n{bullets}\n"

    exemplars = []
    for w in winners[:8]:
        bits = []
        if w.get("title"):    bits.append(w["title"])
        if w.get("industry"): bits.append(w["industry"])
        loc = " ".join(filter(None, [w.get("city"), w.get("state")]))
        if loc: bits.append(loc)
        if w.get("outcome"):  bits.append(f"({w['outcome']})")
        exemplars.append("  - " + " · ".join(bits))
    exemplar_block = "\n".join(exemplars) if exemplars else "  (none)"

    return f"""# Winning Angles — {client} / {niche}
*Generated {today} by forge_compound.py*

## Headline Metrics
- **Leads sent (status=sent)**: {sent:,}
- **Positive meetings**: {pos}
- **Conversion rate**: {rate:.2f}% (positive_meetings / sent)
- **Closed won**: {rev['closed_won_count']}
- **Revenue from closed deals**: ${rev['total_revenue']:,.2f}
- **MRR from closed deals**: ${rev['monthly_revenue']:,.2f}
- **Avg deal size**: ${rev['avg_deal_size']:,.2f}

## Who Actually Converted

{_fmt_topn("Top industries", agg["industries"])}
{_fmt_topn("Top titles", agg["titles"])}
{_fmt_topn("Top states", agg["states"])}
{_fmt_topn("Top cities", agg["cities"])}

## Highest-Performing Campaigns
{_fmt_topn("Campaign names", agg["campaigns"])}

## Outcome Mix
{_fmt_topn("Outcome", agg["outcomes"])}

## Lead Sources That Produced Winners
{_fmt_topn("Source", agg["sources"])}

## Exemplar Winners (most recent first)
{exemplar_block}

## How to Use This Brief

When generating new copy for **{client} / {niche}**, condition the prompt on
this brief. Lean the offer + angle toward the segments that converted:

1. Subject lines should reference the top industries / titles above.
2. Pain-point language should match what worked, not generic vertical tropes.
3. Risk reversal language should be tested against the top-performing campaigns.
4. Avoid segments with zero positives — they're not "untapped", they're filtered.

To regenerate this brief after new meetings land:
```
python3 tools/forge_compound.py --client {client} --niche {niche}
```
"""


# ─── CLI ─────────────────────────────────────────────────────────────────────
def cmd_list(_args):
    combos = list_combos()
    if not combos:
        print("No (client, niche) combos with positive meetings found.")
        return 0
    print(f"  {'CLIENT':22s} {'NICHE':32s} {'POS':>5}  {'CLOSED':>6}")
    for c in combos:
        print(f"  {c['client']:22s} {c['niche']:32s} "
              f"{c['positive_meetings']:>5}  {c['closed']:>6}")
    return 0


def cmd_run(args):
    if not args.client or not args.niche:
        print("--client and --niche are required (or use --list)", file=sys.stderr)
        return 2

    winners = fetch_winners(args.client, args.niche)
    if not winners:
        print(f"No positive meetings yet for client={args.client!r} niche={args.niche!r}.")
        print("Run a campaign first, log meetings via tools/meetings.py, then re-run.")
        return 1

    send_volume = fetch_send_volume(args.client, args.niche)
    agg = aggregate(winners)
    rev = revenue_summary(winners)

    if args.format == "json":
        payload = {
            "generated_at":  datetime.utcnow().isoformat(),
            "client":        args.client,
            "niche":         args.niche,
            "winners_count": len(winners),
            "send_volume":   send_volume,
            "aggregations":  {k: [{"value": v, "count": c} for v, c in items]
                              for k, items in agg.items()},
            "revenue":       rev,
            "exemplars":     winners[:8],
        }
        out = json.dumps(payload, indent=2, default=str)
    else:
        out = render_brief(args.client, args.niche, winners, send_volume, agg, rev)

    if args.dry_run:
        print(out)
        return 0

    BRIEFS_DIR.mkdir(parents=True, exist_ok=True)
    today = datetime.now().strftime("%Y%m%d")
    ext = "json" if args.format == "json" else "md"
    out_path = BRIEFS_DIR / f"{args.client}-{args.niche}-{today}.{ext}"
    out_path.write_text(out)
    print(f"✓ wrote {out_path}")
    print(f"  winners={len(winners)}  conversion_rate="
          f"{100*len(winners)/max(send_volume['sent'],1):.2f}%  "
          f"closed=${rev['total_revenue']:,.0f}")
    return 0


def main():
    ap = argparse.ArgumentParser(description="Mine winning angles from past campaigns.")
    ap.add_argument("--list", action="store_true",
                    help="list every (client, niche) combo with positives")
    ap.add_argument("--client", help="client slug (client_a | client_b | client_c)")
    ap.add_argument("--niche", help="niche slug (e.g. restaurants, fire-protection, msps)")
    ap.add_argument("--format", choices=("brief", "json"), default="brief",
                    help="output format (default: brief)")
    ap.add_argument("--dry-run", action="store_true",
                    help="print to stdout instead of writing to winning-angles/")
    args = ap.parse_args()

    if args.list:
        sys.exit(cmd_list(args))
    sys.exit(cmd_run(args))


if __name__ == "__main__":
    main()
