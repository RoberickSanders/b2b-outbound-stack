#!/usr/bin/env python3.13
"""
forge_auto_research.py — Autonomous campaign research + launch loop.

Mirrors Eric Nowoslawski's `auto-research-public` skill (the Karpathy-inspired
autonomous loop) but built on the Forge stack we already have.

What it does (in order, each step gated):
  1. PAST WINNERS   → forge_compound for (client, niche)
                       Mines past meetings/closed deals into a winning-angles
                       brief that primes the next campaign.
  2. LOOKALIKE PREP → forge_lookalike_research prep
                       Pulls seed leads + signal profile, writes spec for the
                       /lookalike-research subagent dispatcher.
  3. DISCOVERY      → forge.py "find me N companies for client/niche"
                       Runs the full discover→enrich→verify pipeline.
  4. LIST QUALITY   → list_quality_scorecard
                       Grades the produced list /A-F against ICP. Auto-pause
                       below configurable threshold (default: B).
  5. COPY GENERATION→ paf_copy_banks template + Nowoslawski subjects
                       Drafts the sequence YAML the launch step will read.
  6. COPY GATE      → paf_copy_gate (18-point) + score_offer (10-point)
                       Both must pass minimum thresholds (gate ≥14/18,
                       offer ≥35/50 a.k.a. B grade).
  7. DRAFTED LAUNCH → forge_campaign_launch.py
                       Creates DRAFTED Smartlead campaign, uploads leads,
                       attaches mailboxes. NEVER auto-STARTs (Forge rule).

Per the Forge operating rules:
  • Never auto-START campaigns. We stop at DRAFTED. the operator clicks Start in UI.
  • Always run data_quality_check before upload (forge_campaign_launch already
    does this internally).
  • Heavy copy stays on Claude Opus 4 via llm_router.

Outputs every run's artifacts (briefs, scorecard, sequence YAML, launch log)
into a single audit trail dir for retrospective.

Usage:
  # Interactive mode (default) — pauses for approval between phases
  python3 tools/forge_auto_research.py --client client_a --niche restaurants

  # Unattended mode (for cron) — runs through to DRAFTED, stops, emails report
  python3 tools/forge_auto_research.py --client client_a --niche restaurants \\
      --unattended --target 200

  # Skip a phase (e.g. you already have winners brief from yesterday)
  python3 tools/forge_auto_research.py --client client_a --niche churches \\
      --skip compound

  # Dry run (plans the steps without executing)
  python3 tools/forge_auto_research.py --client client_c --niche cost-segregation \\
      --dry-run

Output dir:
  02-Areas/lead-pipeline/auto-research-runs/{client}-{niche}-{YYYYMMDD-HHMM}/
    ├── manifest.json          # run config + timing + phase results
    ├── 01-winners.md          # forge_compound brief
    ├── 02-lookalike-spec.json # lookalike spec (if used)
    ├── 03-leads.csv           # forge.py output
    ├── 04-scorecard.md        # list quality scorecard
    ├── 05-sequence.yaml       # generated copy
    ├── 06-gate-report.json    # paf_copy_gate result
    ├── 06-offer-report.json   # score_offer result
    ├── 07-launch-result.json  # campaign id + lead count + mailbox attachments
    └── stderr.log             # combined stderr from all phases

Standalone tool — does not modify Forge core. Imports from tools/ but only for
helper functions (no side effects on Forge state beyond the existing scripts'
own side effects).
"""

import argparse
import json
import os
import shlex
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
LEAD_PIPELINE_DIR = SCRIPT_DIR.parent
WORKSPACE_ROOT = LEAD_PIPELINE_DIR.parent.parent
PYTHON = "/usr/local/bin/python3.13"

RUNS_ROOT = LEAD_PIPELINE_DIR / "auto-research-runs"

# Default minimum thresholds — tunable via flags
DEFAULT_MIN_GATE = 14         # /18 from paf_copy_gate
DEFAULT_MIN_OFFER = 35        # /50 from score_offer (B grade)
DEFAULT_MIN_LIST_GRADE = "B"  # from list_quality_scorecard

PHASES = [
    "compound",   # 1
    "lookalike",  # 2 — optional
    "discover",   # 3
    "score-list", # 4
    "copy",       # 5
    "gate",       # 6
    "launch",     # 7
]


# ============================================================
# Helpers
# ============================================================

def now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def slug_now() -> str:
    return datetime.now().strftime("%Y%m%d-%H%M")


def log(msg: str, run_dir: Path | None = None):
    line = f"[{now_iso()}] {msg}"
    print(line, file=sys.stderr)
    if run_dir is not None:
        with (run_dir / "stderr.log").open("a") as f:
            f.write(line + "\n")


def run_phase(name: str, argv: list[str], run_dir: Path, timeout: int = 1800) -> dict:
    """Execute a phase, capture exit code + stderr, return result dict."""
    log(f"PHASE [{name}] → {' '.join(shlex.quote(a) for a in argv)}", run_dir)
    started = time.time()
    try:
        proc = subprocess.run(
            argv,
            capture_output=True,
            text=True,
            cwd=str(LEAD_PIPELINE_DIR),
            timeout=timeout,
        )
        elapsed = time.time() - started
        result = {
            "phase": name,
            "argv": argv,
            "exit_code": proc.returncode,
            "elapsed_sec": round(elapsed, 1),
            "stdout_tail": proc.stdout[-2000:] if proc.stdout else "",
            "stderr_tail": proc.stderr[-2000:] if proc.stderr else "",
        }
        if proc.stderr:
            with (run_dir / "stderr.log").open("a") as f:
                f.write(f"\n--- {name} stderr ---\n{proc.stderr}\n")
        log(f"PHASE [{name}] exit={proc.returncode} elapsed={result['elapsed_sec']}s",
            run_dir)
        return result
    except subprocess.TimeoutExpired:
        log(f"PHASE [{name}] TIMEOUT after {timeout}s", run_dir)
        return {
            "phase": name,
            "argv": argv,
            "exit_code": 124,
            "elapsed_sec": timeout,
            "stdout_tail": "",
            "stderr_tail": f"TIMEOUT after {timeout}s",
        }


def confirm(prompt: str, unattended: bool = False, default_yes: bool = False) -> bool:
    """Interactive confirmation gate. In unattended mode, uses default_yes."""
    if unattended:
        return default_yes
    suffix = " [Y/n] " if default_yes else " [y/N] "
    try:
        ans = input(prompt + suffix).strip().lower()
    except EOFError:
        return default_yes
    if not ans:
        return default_yes
    return ans in ("y", "yes")


# ============================================================
# Phase implementations
# ============================================================

def phase_compound(client: str, niche: str, run_dir: Path) -> dict:
    """Phase 1 — generate winning-angles brief from past meetings."""
    out_path = run_dir / "01-winners.md"
    argv = [
        PYTHON, str(SCRIPT_DIR / "forge_compound.py"),
        "--client", client,
        "--niche", niche,
    ]
    result = run_phase("compound", argv, run_dir)
    # forge_compound writes to winning-angles/{client}-{niche}-{date}.md;
    # find it and copy/symlink into our run dir.
    angles_dir = LEAD_PIPELINE_DIR / "winning-angles"
    if angles_dir.is_dir():
        candidates = sorted(angles_dir.glob(f"{client}-{niche}-*.md"),
                            key=lambda p: p.stat().st_mtime, reverse=True)
        if candidates:
            out_path.write_text(candidates[0].read_text())
            result["artifact"] = str(out_path)
    return result


def phase_lookalike(client: str, niche: str, run_dir: Path,
                    seed_niche: str | None, n_agents: int,
                    geo: str | None) -> dict:
    """Phase 2 — lookalike spec prep (if requested)."""
    out_spec = run_dir / "02-lookalike-spec.json"
    if not seed_niche:
        log("PHASE [lookalike] skipped — no --seed-niche provided", run_dir)
        return {"phase": "lookalike", "skipped": True}
    argv = [
        PYTHON, str(SCRIPT_DIR / "forge_lookalike_research.py"),
        "prep",
        "--client", client,
        "--seed-niche", seed_niche,
        "--target-niche", niche,
        "--n-agents", str(n_agents),
    ]
    if geo:
        argv += ["--geo", geo]
    result = run_phase("lookalike", argv, run_dir)
    # Find the produced spec
    runs_dir = LEAD_PIPELINE_DIR / "lookalike-runs"
    if runs_dir.is_dir():
        latest = sorted(
            runs_dir.glob("*/spec.json"),
            key=lambda p: p.stat().st_mtime, reverse=True
        )
        if latest:
            out_spec.write_text(latest[0].read_text())
            result["artifact"] = str(out_spec)
            result["next_action"] = (
                f"Run /lookalike-research {latest[0].parent.name} "
                "in an interactive Claude Code session."
            )
    return result


def phase_discover(client: str, niche: str, target: int, run_dir: Path) -> dict:
    """Phase 3 — full Forge discovery pipeline."""
    nl = f"find me {target} {niche} companies for {client}"
    argv = [
        PYTHON, str(LEAD_PIPELINE_DIR / "forge.py"),
        nl,
    ]
    result = run_phase("discover", argv, run_dir, timeout=3600)
    # Forge writes to its own master CSV path; find the most recent for this client
    master_dir = LEAD_PIPELINE_DIR / "_master"
    if master_dir.is_dir():
        candidates = sorted(
            master_dir.glob(f"*{client}*{niche.replace(' ','_')}*.csv"),
            key=lambda p: p.stat().st_mtime, reverse=True
        )
        if candidates:
            (run_dir / "03-leads.csv").write_text(candidates[0].read_text())
            result["artifact"] = str(run_dir / "03-leads.csv")
            result["lead_count"] = sum(1 for _ in candidates[0].open()) - 1
    return result


def phase_score_list(run_dir: Path, min_grade: str) -> dict:
    """Phase 4 — list quality scorecard against the produced CSV."""
    leads_csv = run_dir / "03-leads.csv"
    if not leads_csv.is_file():
        return {"phase": "score-list", "skipped": True,
                "reason": "no leads CSV from discover phase"}
    out_md = run_dir / "04-scorecard.md"
    argv = [
        PYTHON, str(SCRIPT_DIR / "list_quality_scorecard.py"),
        "--csv", str(leads_csv),
        "--out", str(out_md),
        "--min-grade", min_grade,
    ]
    return run_phase("score-list", argv, run_dir)


def phase_copy(client: str, niche: str, run_dir: Path) -> dict:
    """Phase 5 — generate copy via existing copy banks + writer.

    This phase deliberately stops short of full copy generation in unattended
    mode because the cold-email-writer skill is interactive (it asks the user
    for offer specifics, sender persona tone, etc.). In interactive mode we
    just emit instructions + a stub sequence file. The the operator-driven session
    fills it in via the cold-email-writer skill, then resumes with --skip
    compound,lookalike,discover,score-list,copy.
    """
    stub_path = run_dir / "05-sequence.yaml"
    instructions = [
        f"# Auto-research stub for {client} / {niche}",
        f"# Generated {now_iso()}",
        "#",
        "# To complete this run:",
        "#   1. Open this file's directory and the winners brief at 01-winners.md",
        "#   2. Invoke the cold-email-writer skill in Claude Code",
        "#   3. It produces sequence.yaml in the active campaign workspace",
        "#   4. Copy that sequence into 05-sequence.yaml here",
        "#   5. Re-run forge_auto_research.py with --resume {run_id} to pick up",
        "#      at the gate phase",
        "#",
        "client: " + client,
        "niche: " + niche,
        "sequence: []  # cold-email-writer fills this",
    ]
    stub_path.write_text("\n".join(instructions) + "\n")
    return {"phase": "copy", "stub_written": str(stub_path),
            "next_action": "Use cold-email-writer skill, then resume."}


def phase_gate(run_dir: Path, min_gate: int, min_offer: int) -> dict:
    """Phase 6 — run paf_copy_gate + score_offer on the sequence."""
    seq_path = run_dir / "05-sequence.yaml"
    if not seq_path.is_file():
        return {"phase": "gate", "skipped": True, "reason": "no sequence yet"}
    # paf_copy_gate expects JSON; for the auto-research path the user converts
    # the YAML to JSON via the writer skill, so we accept either extension.
    seq_json = run_dir / "05-sequence.json"
    if not seq_json.is_file():
        return {"phase": "gate", "skipped": True,
                "reason": "no 05-sequence.json — writer skill output it as YAML "
                          "but gate needs JSON; convert or rerun."}
    gate_out = run_dir / "06-gate-report.json"
    offer_out = run_dir / "06-offer-report.json"

    gate_argv = [PYTHON, str(SCRIPT_DIR / "paf_copy_gate.py"),
                 str(seq_json), "--json-out", str(gate_out)]
    offer_argv = [PYTHON, str(SCRIPT_DIR / "score_offer.py"),
                  str(seq_json), "--json-out", str(offer_out)]
    gate_res = run_phase("gate-paf", gate_argv, run_dir)
    offer_res = run_phase("gate-offer", offer_argv, run_dir)

    # Parse scores
    gate_score = None
    offer_score = None
    try:
        gate_score = json.loads(gate_out.read_text()).get("score")
    except Exception:
        pass
    try:
        offer_score = json.loads(offer_out.read_text()).get("total_score")
    except Exception:
        pass

    passed = (
        gate_score is not None and gate_score >= min_gate
        and offer_score is not None and offer_score >= min_offer
    )
    return {
        "phase": "gate",
        "gate_score": gate_score,
        "gate_min": min_gate,
        "offer_score": offer_score,
        "offer_min": min_offer,
        "passed": passed,
        "gate_subprocess": gate_res,
        "offer_subprocess": offer_res,
    }


def phase_launch(client: str, niche: str, run_dir: Path,
                 dry_run: bool) -> dict:
    """Phase 7 — DRAFTED launch via forge_campaign_launch.py."""
    leads_csv = run_dir / "03-leads.csv"
    seq_json = run_dir / "05-sequence.json"
    if not leads_csv.is_file() or not seq_json.is_file():
        return {"phase": "launch", "skipped": True,
                "reason": "missing leads or sequence"}
    out_log = run_dir / "07-launch-result.json"
    argv = [
        PYTHON, str(LEAD_PIPELINE_DIR / "forge_campaign_launch.py"),
        "--client", client,
        "--niche", niche,
        "--leads-csv", str(leads_csv),
        "--sequence-json", str(seq_json),
        "--output", str(out_log),
    ]
    if dry_run:
        argv.append("--dry-run")
    return run_phase("launch", argv, run_dir, timeout=900)


# ============================================================
# Main
# ============================================================

def main():
    p = argparse.ArgumentParser(
        prog="forge_auto_research",
        description="Autonomous campaign research + DRAFTED launch loop.")
    p.add_argument("--client", required=True,
                   help="client_a | client_b | client_c")
    p.add_argument("--niche", required=True,
                   help="Target niche (e.g. restaurants, cost-segregation)")
    p.add_argument("--target", type=int, default=200,
                   help="Lead count target for forge.py discovery (default 200)")
    p.add_argument("--seed-niche",
                   help="If set, runs lookalike phase with this seed niche")
    p.add_argument("--n-agents", type=int, default=10,
                   help="Lookalike subagent count (default 10)")
    p.add_argument("--geo", help="Geographic constraint for lookalike phase")
    p.add_argument("--skip", default="",
                   help="Comma-separated phases to skip (e.g. compound,lookalike)")
    p.add_argument("--resume",
                   help="Resume a previous run by ID. Skips already-completed phases.")
    p.add_argument("--unattended", action="store_true",
                   help="Skip approval gates (cron mode). Stops at DRAFTED.")
    p.add_argument("--dry-run", action="store_true",
                   help="Print phase plan and skip side effects")
    p.add_argument("--min-gate", type=int, default=DEFAULT_MIN_GATE,
                   help=f"Min paf_copy_gate score /18 (default {DEFAULT_MIN_GATE})")
    p.add_argument("--min-offer", type=int, default=DEFAULT_MIN_OFFER,
                   help=f"Min score_offer total /50 (default {DEFAULT_MIN_OFFER})")
    p.add_argument("--min-list-grade", default=DEFAULT_MIN_LIST_GRADE,
                   help=f"Min list quality grade A-F (default {DEFAULT_MIN_LIST_GRADE})")

    args = p.parse_args()

    skips = {s.strip() for s in args.skip.split(",") if s.strip()}

    if args.resume:
        run_dir = RUNS_ROOT / args.resume
        if not run_dir.is_dir():
            print(f"resume run not found: {run_dir}", file=sys.stderr)
            return 2
        manifest_path = run_dir / "manifest.json"
        manifest = json.loads(manifest_path.read_text()) if manifest_path.is_file() else {}
        run_id = args.resume
    else:
        run_id = f"{args.client}-{args.niche.replace(' ', '_')}-{slug_now()}"
        run_dir = RUNS_ROOT / run_id
        run_dir.mkdir(parents=True, exist_ok=True)
        manifest = {
            "run_id": run_id,
            "client": args.client,
            "niche": args.niche,
            "target": args.target,
            "seed_niche": args.seed_niche,
            "geo": args.geo,
            "started_at": now_iso(),
            "unattended": args.unattended,
            "dry_run": args.dry_run,
            "min_gate": args.min_gate,
            "min_offer": args.min_offer,
            "min_list_grade": args.min_list_grade,
            "phases": {},
        }

    log(f"=== Auto-research run started: {run_id} ===", run_dir)
    log(f"client={args.client} niche={args.niche} target={args.target}", run_dir)

    if args.dry_run:
        log("DRY RUN — listing planned phases only:", run_dir)
        for ph in PHASES:
            mark = "SKIP" if ph in skips or (ph == "lookalike" and not args.seed_niche) else "RUN"
            log(f"  [{mark}] {ph}", run_dir)
        return 0

    # ─── Phase 1: compound ───────────────────────────────────────────────
    if "compound" not in skips and "compound" not in manifest.get("phases", {}):
        result = phase_compound(args.client, args.niche, run_dir)
        manifest["phases"]["compound"] = result
        if not confirm("Phase 1 (compound) done. Continue to lookalike?",
                       args.unattended, default_yes=True):
            return _save_and_exit(manifest, manifest_path_for(run_dir))

    # ─── Phase 2: lookalike (optional) ───────────────────────────────────
    if "lookalike" not in skips and "lookalike" not in manifest.get("phases", {}):
        result = phase_lookalike(args.client, args.niche, run_dir,
                                 args.seed_niche, args.n_agents, args.geo)
        manifest["phases"]["lookalike"] = result
        if not result.get("skipped"):
            print("\n*** Lookalike spec prepared. To get the actual lookalike "
                  "leads, dispatch the subagents:")
            print(f"    /lookalike-research {Path(result.get('artifact', '')).parent.name}")
            print("    Then run `f lookalike ingest --run-id <id>` to merge into master DB.\n")
        if not confirm("Phase 2 (lookalike) done. Continue to discover?",
                       args.unattended, default_yes=True):
            return _save_and_exit(manifest, manifest_path_for(run_dir))

    # ─── Phase 3: discover ───────────────────────────────────────────────
    if "discover" not in skips and "discover" not in manifest.get("phases", {}):
        result = phase_discover(args.client, args.niche, args.target, run_dir)
        manifest["phases"]["discover"] = result
        if not confirm("Phase 3 (discover) done. Continue to list scoring?",
                       args.unattended, default_yes=True):
            return _save_and_exit(manifest, manifest_path_for(run_dir))

    # ─── Phase 4: score-list ─────────────────────────────────────────────
    if "score-list" not in skips and "score-list" not in manifest.get("phases", {}):
        result = phase_score_list(run_dir, args.min_list_grade)
        manifest["phases"]["score-list"] = result
        if result.get("exit_code", 0) >= 2:
            log("List quality FAIL — stopping. Fix the list before launch.", run_dir)
            return _save_and_exit(manifest, manifest_path_for(run_dir), exit_code=2)
        if not confirm("Phase 4 (score-list) done. Continue to copy generation?",
                       args.unattended, default_yes=True):
            return _save_and_exit(manifest, manifest_path_for(run_dir))

    # ─── Phase 5: copy ───────────────────────────────────────────────────
    if "copy" not in skips and "copy" not in manifest.get("phases", {}):
        result = phase_copy(args.client, args.niche, run_dir)
        manifest["phases"]["copy"] = result
        if args.unattended:
            log("Unattended mode + copy phase needs interactive writer skill. Stopping.",
                run_dir)
            return _save_and_exit(manifest, manifest_path_for(run_dir))
        if not confirm("Phase 5 (copy stub written) — invoke cold-email-writer "
                       "now, then resume with --resume " + run_id + "?",
                       False, default_yes=False):
            return _save_and_exit(manifest, manifest_path_for(run_dir))

    # ─── Phase 6: gate ───────────────────────────────────────────────────
    if "gate" not in skips and "gate" not in manifest.get("phases", {}):
        result = phase_gate(run_dir, args.min_gate, args.min_offer)
        manifest["phases"]["gate"] = result
        if not result.get("passed"):
            log(f"Gate failed: gate={result.get('gate_score')}/{args.min_gate} "
                f"offer={result.get('offer_score')}/{args.min_offer}. STOP.", run_dir)
            return _save_and_exit(manifest, manifest_path_for(run_dir), exit_code=2)
        if not confirm("Phase 6 (gate) passed. Continue to DRAFTED launch?",
                       args.unattended, default_yes=True):
            return _save_and_exit(manifest, manifest_path_for(run_dir))

    # ─── Phase 7: launch (DRAFTED only) ──────────────────────────────────
    if "launch" not in skips and "launch" not in manifest.get("phases", {}):
        result = phase_launch(args.client, args.niche, run_dir, args.dry_run)
        manifest["phases"]["launch"] = result

    manifest["finished_at"] = now_iso()
    return _save_and_exit(manifest, manifest_path_for(run_dir))


def manifest_path_for(run_dir: Path) -> Path:
    return run_dir / "manifest.json"


def _save_and_exit(manifest: dict, manifest_path: Path, exit_code: int = 0) -> int:
    manifest_path.write_text(json.dumps(manifest, indent=2))
    log(f"manifest saved to {manifest_path}", manifest_path.parent)
    log(f"=== Auto-research run ended (exit={exit_code}) ===", manifest_path.parent)
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
