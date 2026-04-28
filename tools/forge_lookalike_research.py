#!/usr/bin/env python3
"""
forge_lookalike_research.py — Claude Code subagent-driven lookalike company finder

Architecture (3 stages):

    1. prep   (Python CLI)
       Pulls seed leads from master_leads.db for a given niche, asks Kimi (via
       llm_router) to distill them into a structured "signal profile", and
       writes a spec.json that the slash command will consume.

    2. dispatch (Claude Code slash command)
       Run `/lookalike-research <run_id>` inside an interactive Claude Code
       session. The slash command spawns N parallel Task subagents — each
       gets a unique search angle from the spec and returns lookalike
       company JSON. Results land in results.json.

    3. ingest (Python CLI)
       Reads results.json, dedupes against master_leads.db, applies
       confidence filter, and inserts new companies with
       source='claude_lookalike'. Hands off to the standard Forge
       enrichment waterfall downstream.

Why split it: the Task subagent dispatch only has the "free web research"
property when it runs inside an interactive Claude Code session
(subsidized by the Max plan). DB I/O is faster + safer in pure Python.

Why three subcommands instead of one: each stage has different runtime
characteristics — prep is fast (~30s), dispatch is slow (~5-15min per
20 subagents), ingest is fast (~10s). Keeping them separate means a
crashed dispatch doesn't lose seed work.

Usage:
    # Stage 1 — extract signal profile from a seed niche
    python3 tools/forge_lookalike_research.py prep \\
        --seed-niche paf-medical-denver \\
        --target-niche paf-assisted-living-denver \\
        --client client_a \\
        --geo "Denver metro" \\
        --n-agents 20

    # Stage 2 — in Claude Code interactive session
    /lookalike-research <run_id>

    # Stage 3 — pull the results into master_leads.db
    python3 tools/forge_lookalike_research.py ingest --run-id <run_id>

    # Anytime — see state of a run
    python3 tools/forge_lookalike_research.py status --run-id <run_id>
"""

import argparse
import json
import os
import sqlite3
import sys
import time
import uuid
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path

# ── Path setup (mirrors forge.py) ────────────────────────────────────────────
SCRIPT_DIR = Path(__file__).resolve().parent
LEAD_PIPELINE_DIR = SCRIPT_DIR.parent
WORKSPACE_ROOT = LEAD_PIPELINE_DIR.parent.parent
DB_PATH = LEAD_PIPELINE_DIR / "master-leads" / "master_leads.db"
RUNS_DIR = LEAD_PIPELINE_DIR / "lookalike-runs"

# Add tools/ + lead-pipeline/ to path so we can import llm_router
sys.path.insert(0, str(LEAD_PIPELINE_DIR))
sys.path.insert(0, str(SCRIPT_DIR))


# ── Dotenv loader (copied pattern from forge.py — Forge is hands-off) ───────
def _load_env():
    for _p in (WORKSPACE_ROOT / ".env", LEAD_PIPELINE_DIR / ".env"):
        if _p.is_file():
            for line in _p.read_text().splitlines():
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                k, v = line.split("=", 1)
                k = k.strip()
                v = v.strip().strip('"').strip("'")
                if k and not os.environ.get(k):
                    os.environ[k] = v

_load_env()


# ── Run dir helpers ──────────────────────────────────────────────────────────
def new_run_id(target_niche: str) -> str:
    """Generate a unique run id: {date}-{niche-slug}-{short-uuid}."""
    date_str = datetime.now().strftime("%Y%m%d-%H%M")
    niche_slug = target_niche.lower().replace("_", "-").replace(" ", "-")
    short_id = uuid.uuid4().hex[:6]
    return f"{date_str}-{niche_slug}-{short_id}"


def run_dir(run_id: str) -> Path:
    return RUNS_DIR / run_id


def run_path(run_id: str, filename: str) -> Path:
    return run_dir(run_id) / filename


def ensure_run_dir(run_id: str) -> Path:
    d = run_dir(run_id)
    d.mkdir(parents=True, exist_ok=True)
    return d


def write_json(path: Path, data) -> None:
    path.write_text(json.dumps(data, indent=2, default=str))


def read_json(path: Path):
    if not path.is_file():
        return None
    return json.loads(path.read_text())


# ── DB helpers ───────────────────────────────────────────────────────────────
@contextmanager
def open_db():
    """Context manager around the master DB. Read-only by default callers
    should pass mode='rw' for writes."""
    if not DB_PATH.is_file():
        raise FileNotFoundError(f"master_leads.db not found at {DB_PATH}")
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
    finally:
        conn.close()


def fetch_seed_leads(niche: str, client: str | None, limit: int = 100) -> list[dict]:
    """Pull representative seed leads from master_leads.db for signal extraction.

    TODO: Verify exact column names against current schema. Use
    `sqlite3 master-leads/master_leads.db '.schema leads'` to confirm.
    Likely columns: company_name, domain, niche, client, city, state,
    employee_count_band, industry, website_text_summary.
    """
    with open_db() as conn:
        cur = conn.cursor()
        # TODO: replace with real SELECT — sample structure below
        # WHERE clause should also exclude unverified / status='bad' rows
        sql = """
            SELECT company_name, domain, city, state,
                   employee_count_band, industry, niche, client
              FROM leads
             WHERE niche = ?
               AND (client = ? OR ? IS NULL)
               AND domain IS NOT NULL
               AND status != 'bad'
             ORDER BY RANDOM()
             LIMIT ?
        """
        try:
            rows = cur.execute(sql, (niche, client, client, limit)).fetchall()
        except sqlite3.OperationalError as e:
            print(f"[fetch_seed_leads] schema mismatch — fix column names: {e}",
                  file=sys.stderr)
            return []
        return [dict(r) for r in rows]


def existing_domains_for_dedup() -> set[str]:
    """Return all domains already in master_leads.db, lowercased.

    Used to dedupe lookalike results before insert.
    """
    with open_db() as conn:
        cur = conn.cursor()
        cur.execute("SELECT DISTINCT LOWER(domain) FROM leads WHERE domain IS NOT NULL")
        return {r[0] for r in cur.fetchall() if r[0]}


def insert_lookalike_lead(conn, row: dict) -> bool:
    """Insert a single lookalike lead. Returns True on insert, False on skip.

    TODO: align field list with master_leads.db schema. The minimum we
    care about for downstream Forge enrichment:
      - company_name, domain, niche, client
      - source='claude_lookalike'
      - status='new'
      - notes / tags column with evidence_url + match_reason
    """
    # TODO: real INSERT — sketch only
    # cur = conn.cursor()
    # cur.execute(
    #     '''INSERT OR IGNORE INTO leads (
    #            company_name, domain, niche, client, source, status,
    #            city, state, notes, created_at
    #        ) VALUES (?, ?, ?, ?, 'claude_lookalike', 'new', ?, ?, ?, ?)''',
    #     (
    #         row['company_name'], row['domain'], row['target_niche'],
    #         row['client'], row.get('city'), row.get('state'),
    #         json.dumps({
    #             'evidence_url': row.get('evidence_url'),
    #             'match_reason': row.get('match_reason'),
    #             'confidence': row.get('confidence'),
    #             'discovered_by_agent': row.get('agent_idx'),
    #         }),
    #         datetime.utcnow().isoformat(),
    #     )
    # )
    # return cur.rowcount > 0
    raise NotImplementedError("wire INSERT against current leads schema")


# ── LLM signal extraction ────────────────────────────────────────────────────
def extract_signal_profile(seeds: list[dict], target_niche: str, geo: str | None) -> dict:
    """Pass the seed leads to Kimi (light client) and ask it to distill
    a signal profile that subagents can use to find lookalikes.

    Returns:
        {
          "industry_pattern":      str,   # e.g. "skilled nursing + assisted living, 50-150 beds"
          "size_band":             str,   # e.g. "$5M-$25M revenue"
          "geo_constraint":        str,   # e.g. "Denver metro within 60mi"
          "regulatory_signals":    [str], # e.g. ["NFPA 25 inspection cycle", "CMS Five-Star deficiencies"]
          "trigger_events":        [str], # e.g. ["recent kitchen renovation", "new construction permit"]
          "exclusion_signals":     [str], # e.g. ["already a CLIENT_A customer", "competitor fire-protection company"]
          "search_angles":         [str], # 5-20 distinct ways to find matches (one per subagent)
        }

    TODO: import llm_router.get_light_client() and run the prompt below.
    Per CLAUDE.md, light tasks (classification, extraction) route through
    Kimi K2.6 — do NOT call anthropic.Anthropic() directly here.
    """
    # from llm_router import get_light_client
    # client = get_light_client()
    # prompt = f"""You are extracting a signal profile from {len(seeds)} seed companies.
    # ...
    # """
    # response = client.chat.completions.create(...)
    # profile = json.loads(response.choices[0].message.content)
    # return profile

    # Skeleton — return a placeholder so callers can still write the spec
    return {
        "industry_pattern": "TODO: kimi-extracted",
        "size_band": "TODO",
        "geo_constraint": geo or "TODO",
        "regulatory_signals": [],
        "trigger_events": [],
        "exclusion_signals": [],
        "search_angles": [],
        "_seed_count": len(seeds),
        "_target_niche": target_niche,
        "_status": "skeleton",
    }


# ── Subcommand: prep ─────────────────────────────────────────────────────────
def cmd_prep(args) -> int:
    """Stage 1. Extract signal profile from seed niche, write spec.json."""
    if not args.seed_niche or not args.target_niche or not args.client:
        print("[prep] --seed-niche, --target-niche, --client are required",
              file=sys.stderr)
        return 2

    # 1. Pull seed leads
    print(f"[prep] pulling seeds: niche={args.seed_niche} client={args.client} "
          f"limit={args.seed_limit}")
    seeds = fetch_seed_leads(args.seed_niche, args.client, limit=args.seed_limit)
    if not seeds:
        print(f"[prep] no seed leads found for niche={args.seed_niche!r} "
              f"client={args.client!r} — aborting", file=sys.stderr)
        return 1
    print(f"[prep]   {len(seeds)} seeds loaded")

    # 2. Distill signal profile via Kimi
    print(f"[prep] extracting signal profile (n_agents target={args.n_agents}) ...")
    profile = extract_signal_profile(seeds, args.target_niche, args.geo)

    # 3. If skeleton, warn the user
    if profile.get("_status") == "skeleton":
        print("[prep]   WARNING: extract_signal_profile is a skeleton — "
              "wire llm_router.get_light_client() before relying on this run",
              file=sys.stderr)

    # 4. Build spec
    run_id = args.run_id or new_run_id(args.target_niche)
    ensure_run_dir(run_id)

    spec = {
        "run_id": run_id,
        "created_at": datetime.utcnow().isoformat(),
        "client": args.client,
        "seed_niche": args.seed_niche,
        "target_niche": args.target_niche,
        "geo": args.geo,
        "n_agents": args.n_agents,
        "min_confidence": args.min_confidence,
        "seed_count": len(seeds),
        "seed_sample": seeds[:10],   # first 10 for subagent context
        "signal_profile": profile,
        "output_schema": {
            "company_name": "str",
            "domain": "str (root domain only, no www/https)",
            "city": "str | null",
            "state": "str | null (2-letter)",
            "match_reason": "str (one sentence why this matches the signal profile)",
            "evidence_url": "str (URL where you confirmed the match)",
            "confidence": "float in [0.0, 1.0]",
        },
    }

    spec_path = run_path(run_id, "spec.json")
    if args.dry_run:
        print(f"[prep] DRY RUN — would write spec to {spec_path}")
        print(json.dumps(spec, indent=2, default=str)[:1500] + "\n...")
        return 0

    write_json(spec_path, spec)
    print(f"[prep] spec written → {spec_path}")
    print()
    print(f"  Next: open Claude Code in the workspace and run:")
    print(f"      /lookalike-research {run_id}")
    print()
    print(f"  When the slash command finishes, ingest with:")
    print(f"      python3 tools/forge_lookalike_research.py ingest "
          f"--run-id {run_id}")
    return 0


# ── Subcommand: ingest ───────────────────────────────────────────────────────
def cmd_ingest(args) -> int:
    """Stage 3. Read results.json, dedupe, write to master_leads.db."""
    if not args.run_id:
        print("[ingest] --run-id is required", file=sys.stderr)
        return 2

    spec = read_json(run_path(args.run_id, "spec.json"))
    if not spec:
        print(f"[ingest] spec.json not found for run_id={args.run_id}",
              file=sys.stderr)
        return 1

    results = read_json(run_path(args.run_id, "results.json"))
    if not results:
        print(f"[ingest] results.json not found for run_id={args.run_id} — "
              f"did the slash command finish?", file=sys.stderr)
        return 1

    raw_rows = results.get("companies", [])
    print(f"[ingest] {len(raw_rows)} raw rows from {len(results.get('agents', []))} agents")

    # 1. Confidence filter
    min_conf = args.min_confidence or spec.get("min_confidence", 0.7)
    filtered = [r for r in raw_rows if (r.get("confidence") or 0) >= min_conf]
    print(f"[ingest]   {len(filtered)} pass confidence ≥ {min_conf}")

    # 2. Dedup against master DB
    known = existing_domains_for_dedup()
    new_rows = []
    seen = set()
    for r in filtered:
        d = (r.get("domain") or "").lower().strip().lstrip("www.")
        if not d or d in known or d in seen:
            continue
        seen.add(d)
        r["domain"] = d
        r["target_niche"] = spec["target_niche"]
        r["client"] = spec["client"]
        new_rows.append(r)
    print(f"[ingest]   {len(new_rows)} net-new (after master DB dedup)")

    if args.dry_run:
        print("[ingest] DRY RUN — sample of what would be inserted:")
        for r in new_rows[:5]:
            print(f"  - {r['company_name']:40s}  {r['domain']:30s}  "
                  f"conf={r.get('confidence')}")
        return 0

    # 3. Write
    inserted = 0
    skipped = 0
    with open_db() as conn:
        for r in new_rows:
            try:
                if insert_lookalike_lead(conn, r):
                    inserted += 1
                else:
                    skipped += 1
            except NotImplementedError:
                print("[ingest] insert_lookalike_lead is a skeleton — "
                      "wire INSERT before calling ingest for real",
                      file=sys.stderr)
                return 3
        conn.commit()

    print(f"[ingest] inserted={inserted} skipped={skipped}")

    # 4. Audit trail
    write_json(run_path(args.run_id, "ingested.json"), {
        "ingested_at": datetime.utcnow().isoformat(),
        "inserted": inserted,
        "skipped": skipped,
        "min_confidence": min_conf,
        "rows": new_rows,
    })
    return 0


# ── Subcommand: status ───────────────────────────────────────────────────────
def cmd_status(args) -> int:
    """Inspect a run."""
    if not args.run_id:
        # List all runs
        if not RUNS_DIR.is_dir():
            print("(no runs yet)")
            return 0
        for d in sorted(RUNS_DIR.iterdir()):
            if not d.is_dir():
                continue
            spec = read_json(d / "spec.json") or {}
            results = read_json(d / "results.json")
            ingested = read_json(d / "ingested.json")
            stage = (
                "ingested" if ingested else
                "results-ready" if results else
                "spec-only" if spec else
                "empty"
            )
            print(f"  {d.name:60s}  [{stage}]  "
                  f"{spec.get('client', '?')}/{spec.get('target_niche', '?')}")
        return 0

    d = run_dir(args.run_id)
    if not d.is_dir():
        print(f"run_id not found: {args.run_id}", file=sys.stderr)
        return 1
    for fname in ("spec.json", "results.json", "ingested.json", "log.txt"):
        p = d / fname
        if p.is_file():
            print(f"  ✓ {fname:18s}  {p.stat().st_size:>10,} bytes")
        else:
            print(f"  · {fname:18s}  (missing)")
    return 0


# ── argparse ────────────────────────────────────────────────────────────────
def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Lookalike company finder using Claude Code subagents.",
        epilog="See file docstring for full architecture overview.",
    )
    sub = p.add_subparsers(dest="cmd", required=True)

    # prep
    pp = sub.add_parser("prep", help="Stage 1: extract signal profile from seed niche")
    pp.add_argument("--seed-niche", required=True,
                    help="niche to draw seed leads from (e.g. paf-medical-denver)")
    pp.add_argument("--target-niche", required=True,
                    help="niche tag for the new lookalike leads (e.g. paf-assisted-living-denver)")
    pp.add_argument("--client", required=True,
                    choices=["client_a", "client_b", "client_c"],
                    help="client these leads belong to")
    pp.add_argument("--geo", default=None,
                    help="geographic constraint (e.g. 'Denver metro')")
    pp.add_argument("--n-agents", type=int, default=20,
                    help="number of parallel subagents to dispatch (default 20)")
    pp.add_argument("--seed-limit", type=int, default=100,
                    help="max seed leads to sample for signal extraction (default 100)")
    pp.add_argument("--min-confidence", type=float, default=0.7,
                    help="confidence threshold applied at ingest stage (default 0.7)")
    pp.add_argument("--run-id", default=None,
                    help="override auto-generated run id (rare)")
    pp.add_argument("--dry-run", action="store_true",
                    help="print spec to stdout instead of writing to disk")
    pp.set_defaults(func=cmd_prep)

    # ingest
    pi = sub.add_parser("ingest", help="Stage 3: pull results.json into master_leads.db")
    pi.add_argument("--run-id", required=True)
    pi.add_argument("--min-confidence", type=float, default=None,
                    help="override spec's confidence threshold")
    pi.add_argument("--dry-run", action="store_true",
                    help="show what would be inserted without writing")
    pi.set_defaults(func=cmd_ingest)

    # status
    ps = sub.add_parser("status", help="Inspect a run (omit --run-id to list all)")
    ps.add_argument("--run-id", default=None)
    ps.set_defaults(func=cmd_status)

    return p


def main():
    args = build_parser().parse_args()
    sys.exit(args.func(args))


if __name__ == "__main__":
    main()
