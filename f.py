#!/usr/bin/env python3
"""
f.py — Forge front-door dispatcher.

ONE memorable command for the entire Forge ecosystem. Doesn't replace the
underlying tools — they keep working at their original paths so cron jobs,
launchd jobs, and slash command references stay intact. Adds a single
discoverable surface so you can find the right tool without `ls tools/`.

Add this alias to your shell rc once:

    alias f='/usr/local/bin/python3.13 "~/agency-os/f.py"'

Then anywhere:

    f                          # show the menu
    f launch                   # full campaign launch (Forge core)
    f score sequence.json      # offer scorecard
    f compound --list          # winning-angles list
    f lookalike prep ...       # lookalike research stage 1
    f health                   # doctor
    f today                    # daily snapshot
    f --search mailbox         # find any command matching "mailbox"

All extra args after the subcommand pass through unchanged.
"""

import os
import shlex
import subprocess
import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
PYTHON = "/usr/local/bin/python3.13"


# ─── Registry ───────────────────────────────────────────────────────────────
# Each entry: subcommand → (relative path or shell command, one-line description, category)
COMMANDS = {
    # ─── DISCOVER ──────────────────────────────────────────────────────────
    "find":         ("forge.py",
                     "Full Forge pipeline (discover → enrich → ship). Natural language input.",
                     "discover"),
    "discover-google":  ("tools/discover_google_to_blitz.py",
                         "Google search → Blitz enrichment for niche queries",
                         "discover"),
    "scrape-licenses":  ("tools/scrape_state_licenses.py",
                         "State licensing database scraper (10 states)",
                         "discover"),
    "research-niche":   ("tools/niche_research.py",
                         "Niche-level market research + ICP scoring",
                         "discover"),
    "lookalike":        ("tools/forge_lookalike_research.py",
                         "Lookalike-company finder (3-stage: prep | ingest | status)",
                         "discover"),

    # ─── ENRICH ────────────────────────────────────────────────────────────
    "enrich":           ("tools/forge_enrich.py",
                         "13-step enrichment cascade (free → paid waterfall)",
                         "enrich"),
    "enrich-owner":     ("tools/enrich_owner_search.py",
                         "Google + Haiku owner-name discovery",
                         "enrich"),
    "enrich-retry":     ("tools/enrich_retry.py",
                         "Retry failed enrichments from cache",
                         "enrich"),
    "enrich-second":    ("tools/enrich_second_contact.py",
                         "Find 2nd decision maker at company",
                         "enrich"),
    "enrich-route":     ("tools/enrich_smart_route.py",
                         "MX-based domain routing + niche directory lookup",
                         "enrich"),

    # ─── VERIFY ────────────────────────────────────────────────────────────
    "verify-niche":     ("tools/verify_niche_fit.py",
                         "Name-based niche-fit LLM check",
                         "verify"),
    "verify-website":   ("tools/verify_niche_fit_website.py",
                         "Website-content niche-fit verification",
                         "verify"),
    "verify-titles":    ("tools/verify_title_redflags.py",
                         "Bad-title regex filter",
                         "verify"),
    "verify-combine":   ("tools/verify_combine.py",
                         "Consensus combiner (2-of-3 rule)",
                         "verify"),
    "mv-bulk":          ("tools/mv_bulk_verify.py",
                         "MillionVerifier bulk verification",
                         "verify"),

    # ─── QUALITY GATES ─────────────────────────────────────────────────────
    "gate":             ("tools/paf_copy_gate.py",
                         "Tactical 18-point writing rubric (humanizer + WRITING_RULES)",
                         "quality"),
    "score":            ("tools/score_offer.py",
                         "Strategic 10-component offer scorecard /50 (Oliverify framework)",
                         "quality"),
    "audit-data":       ("tools/data_quality_check.py",
                         "Pre-send audit: first-name %, generic email %, dupes",
                         "quality"),
    "audit-framework":  ("tools/framework_audit.py",
                         "Strategic framework audit on a campaign",
                         "quality"),
    "audit-quality":    ("tools/forge_quality_audit.py",
                         "Comprehensive quality audit",
                         "quality"),
    "audit-send":       ("tools/send_audit.py",
                         "Final pre-send safety audit",
                         "quality"),

    # ─── CAMPAIGN ──────────────────────────────────────────────────────────
    "launch":           ("forge_campaign_launch.py",
                         "Forge Phase 8: create DRAFTED Smartlead campaign + upload + attach mailboxes",
                         "campaign"),
    "topup":            ("tools/campaign_topup.py",
                         "Add fresh leads to an existing Smartlead campaign",
                         "campaign"),
    "edit-campaign":    ("tools/campaign_edit.py",
                         "Edit campaign settings + sequence",
                         "campaign"),
    "review":           ("campaign_review.py",
                         "Manual review pass on campaign state",
                         "campaign"),
    "compound":         ("tools/forge_compound.py",
                         "Mine winning angles from past campaigns → brief for next campaign",
                         "campaign"),
    "auto-research":    ("tools/forge_auto_research.py",
                         "Autonomous loop: compound → lookalike → discover → score → copy → gate → DRAFTED launch",
                         "campaign"),
    "engagers":         ("tools/competitor_engagers.py",
                         "Harvest LinkedIn engagers from competitor company posts (RapidAPI)",
                         "discover"),
    "score-list":       ("tools/list_quality_scorecard.py",
                         "8-dimension list quality scorecard (A-F grade) before send",
                         "quality"),
    "analyze":          ("tools/campaign_analyzer.py",
                         "Reply classification + objection themes + recommendations",
                         "campaign"),
    "post-launch":      ("tools/post_forge_launcher.py",
                         "Legacy post-Forge campaign launcher",
                         "campaign"),
    "signal-launch":    ("tools/signal_campaign_launcher.py",
                         "Signal-driven campaign launcher (job posts, permits)",
                         "campaign"),

    # ─── MAILBOX & DELIVERABILITY ─────────────────────────────────────────
    "autopilot":        ("tools/mailbox_autopilot.py",
                         "Daily mailbox health watchdog (alert-only by default)",
                         "mailbox"),
    "monitor":          ("tools/deliverability_monitor.py",
                         "Daily DNS + bounce trend across all 125+ mailboxes",
                         "mailbox"),
    "onboard":          ("onboard.py",
                         "Onboard new client (Google mailboxes via InboxKit)",
                         "mailbox"),
    "onboard-msft":     ("onboard_microsoft.py",
                         "Onboard new client (Microsoft mailboxes)",
                         "mailbox"),
    "seg-sort":         ("tools/seg_aware_sort.py",
                         "Sort lead CSV: non-SEG (Google/MSFT) first, SEG (Mimecast/PP/Barracuda) last",
                         "mailbox"),

    # ─── REPLIES & MEETINGS ────────────────────────────────────────────────
    "triage":           ("tools/reply_triage.py",
                         "Classify replies via Kimi + draft response in sender voice",
                         "replies"),
    "triage-notify":    ("tools/reply_triage_notify.py",
                         "Push notifications on positive replies (Pushover/Slack)",
                         "replies"),
    "prep":             ("tools/meeting_prep.py",
                         "Pre-call brief: company intel + thread + opening questions",
                         "replies"),
    "meetings":         ("tools/meetings.py",
                         "Meeting + deal tracking + ROI reports (subcommands: log, close, roi)",
                         "replies"),

    # ─── REPORTS & DASHBOARDS ──────────────────────────────────────────────
    "today":            ("tools/today.py",
                         "Daily snapshot",
                         "reports"),
    "dashboard":        ("tools/forge_dashboard.py",
                         "Live Forge dashboard",
                         "reports"),
    "status":           ("tools/status_report.py",
                         "Current pipeline state report",
                         "reports"),
    "client-report":    ("tools/client_reports.py",
                         "Weekly per-client performance report (Friday auto)",
                         "reports"),

    # ─── HEALTH & SAFETY ───────────────────────────────────────────────────
    "health":           ("doctor.py --fast",
                         "Forge 7-category health check",
                         "health"),
    "rollback":         ("tools/rollback.py",
                         "Restore deleted leads/sequences from backup JSONs",
                         "health"),
    "backup":           ("tools/backup_db.sh",
                         "Backup master_leads.db to iCloud (keeps last 30)",
                         "health"),

    # ─── SLASH-COMMAND POINTERS ────────────────────────────────────────────
    # These are slash commands, not CLI scripts. They live in .claude/commands/.
    # Listed here for discoverability — actual invocation is /command in Claude Code.
}

# Slash commands (not invoked through this CLI — listed for discovery only).
SLASH_COMMANDS = {
    "/diagnose-campaign":   "Run 4-point Oliverify diagnostic on a Smartlead campaign id",
    "/lookalike-research":  "Stage 2 of lookalike: dispatch parallel Task subagents",
    "/weekly-rhythm":       "Mon/Wed/Fri/biweekly/monthly/quarterly ops playbook",
    "/proposal":            "Generate Gamma proposal from sales call transcript",
    "/new-client":          "Scaffold a new client folder + CLIENT.md",
    "/new-campaign":        "Scaffold a new campaign with WRITING_RULES gate",
    "/today":               "Today's plan + open work",
    "/RevStart":            "CLIENT_C agency operations boot",
    "/setup-para":          "PARA folder structure setup",
}


# ─── Help renderer ──────────────────────────────────────────────────────────
CATEGORIES = [
    ("discover",  "FIND LEADS"),
    ("enrich",    "ENRICH"),
    ("verify",    "VERIFY"),
    ("quality",   "QUALITY GATES"),
    ("campaign",  "CAMPAIGN OPS"),
    ("mailbox",   "MAILBOX & DELIVERABILITY"),
    ("replies",   "REPLIES & MEETINGS"),
    ("reports",   "REPORTS & DASHBOARDS"),
    ("health",    "HEALTH & SAFETY"),
]


def render_help(filter_term: str | None = None) -> str:
    out = []
    out.append("forge front-door dispatcher  ·  one command for the whole pipeline")
    out.append("")
    if filter_term:
        out.append(f"Filtered by: {filter_term!r}")
        out.append("")
        matches = {k: v for k, v in COMMANDS.items()
                   if filter_term.lower() in k.lower() or filter_term.lower() in v[1].lower()}
        if not matches:
            return "\n".join(out + [f"  (no commands match {filter_term!r})"])
        for cmd, (_, desc, _) in sorted(matches.items()):
            out.append(f"  f {cmd:<22s}  {desc}")
        return "\n".join(out)

    for cat_id, cat_label in CATEGORIES:
        items = [(k, v) for k, v in COMMANDS.items() if v[2] == cat_id]
        if not items:
            continue
        out.append(f"  {cat_label}")
        for cmd, (_, desc, _) in sorted(items, key=lambda x: x[0]):
            out.append(f"    f {cmd:<22s}  {desc}")
        out.append("")

    out.append("  SLASH COMMANDS (Claude Code only — type these directly)")
    for cmd, desc in SLASH_COMMANDS.items():
        out.append(f"    {cmd:<24s}  {desc}")
    out.append("")
    out.append("Tips:")
    out.append("  f <cmd> --help          show that tool's flags")
    out.append("  f --search <term>       fuzzy-search commands by name or description")
    out.append("  f --list                machine-readable list of all subcommands")
    return "\n".join(out)


def render_list() -> str:
    """Machine-readable list (one command per line, tab-separated)."""
    lines = []
    for cmd, (path, desc, cat) in sorted(COMMANDS.items()):
        lines.append(f"{cmd}\t{cat}\t{path}\t{desc}")
    return "\n".join(lines)


# ─── Dispatcher ────────────────────────────────────────────────────────────
def dispatch(cmd: str, extra_args: list[str]) -> int:
    if cmd not in COMMANDS:
        # Try fuzzy match
        candidates = [k for k in COMMANDS if cmd in k or k in cmd]
        if candidates:
            print(f"unknown subcommand: {cmd}", file=sys.stderr)
            print(f"did you mean: {', '.join(candidates[:5])}", file=sys.stderr)
        else:
            print(f"unknown subcommand: {cmd}\nrun `f` to see all commands.",
                  file=sys.stderr)
        return 2

    target_spec, _, _ = COMMANDS[cmd]
    # target_spec may include extra args ("doctor.py --fast")
    parts = shlex.split(target_spec)
    rel_path = parts[0]
    embedded_args = parts[1:]
    full_path = SCRIPT_DIR / rel_path

    if not full_path.is_file():
        print(f"target script not found: {full_path}", file=sys.stderr)
        return 3

    if rel_path.endswith(".sh"):
        argv = [str(full_path)] + embedded_args + extra_args
    else:
        argv = [PYTHON, str(full_path)] + embedded_args + extra_args

    # Print what we're running so the user sees the actual command
    print(f"→ {' '.join(shlex.quote(a) for a in argv)}", file=sys.stderr)
    return subprocess.call(argv, cwd=str(SCRIPT_DIR))


# ─── Main ──────────────────────────────────────────────────────────────────
def main():
    args = sys.argv[1:]

    if not args or args[0] in ("-h", "--help", "help"):
        print(render_help())
        return 0

    if args[0] == "--list":
        print(render_list())
        return 0

    if args[0] == "--search":
        if len(args) < 2:
            print("--search needs a term", file=sys.stderr)
            return 2
        print(render_help(filter_term=args[1]))
        return 0

    cmd = args[0]
    extra = args[1:]
    return dispatch(cmd, extra)


if __name__ == "__main__":
    sys.exit(main())
