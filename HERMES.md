# Hermes ↔ Forge bridge — read first

You are Hermes running on the operator's ClientC agency server. The user
operates a system called **Forge** via the `f` command. When the user sends
a message starting with `f` (lowercase, single letter) followed by a
subcommand, that's an instruction to run the Forge dispatcher.

## CRITICAL — disambiguation

`f health` does NOT mean "hermes doctor" or "hermes health".
`f health` means: **run `/usr/local/bin/f health` via bash and return the output verbatim**.

You have a `bash` / `terminal` / `shell` tool. **USE IT** for any `f <subcommand>` request.

## Always do this when the user says "f <X>", "run f <X>", or any forge-shaped command

```bash
/usr/local/bin/f <full subcommand and args>
```

Do NOT substitute with `hermes doctor`, `hermes config show`, `hermes status`,
or any built-in Hermes command. Forge's `f` is a separate program at
`/usr/local/bin/f` that dispatches to scripts in
`/opt/forge/Claude Workspace/02-Areas/lead-pipeline/`.

## Most-used Forge commands (for natural-language interpretation)

| User says | You run |
|---|---|
| "f health" / "forge health" / "is forge healthy?" | `/usr/local/bin/f health` |
| "compound list" / "winning niches" | `/usr/local/bin/f compound --list` |
| "find me N X for client" | `/usr/local/bin/f find "find me N X for client"` |
| "auto research X for Y" | `/usr/local/bin/f auto-research --client Y --niche X` |
| "score this list" | `/usr/local/bin/f score-list --csv <path>` |
| "mailbox health" / "autopilot status" | `/usr/local/bin/f autopilot --dry-run` |
| "weekly report for X" | `/usr/local/bin/f client-report --client X` |
| "diagnose campaign N" | `/usr/local/bin/f --search diagnose` (or use Smartlead MCP if asked) |
| "what mailboxes are bouncing?" | `/usr/local/bin/f autopilot --dry-run` and parse output |

## Operating rules — NEVER violate

1. **Never auto-START a Smartlead campaign.** Always DRAFTED only. Tell user to click Start in the Smartlead app.
2. **Never auto-pause a running campaign.** Mailbox autopilot is alert-only. The `--no-pause` flag is mandatory in cron.
3. **Per-mailbox cap stays 20-30/day, default 25.** Going above 30 burns inboxes.
4. **Never attach mailboxes <14 days warmed.** Use `pick_mature_mailboxes()` from `mailbox_helpers.py`.

## Clients

- **CLIENT_A** (client_a) — Sender One, fire protection, Denver-focused
- **CLIENT_B** (client_b) — Sender Two, MSP cybersecurity / VCISO / SOC2
- **CLIENT_C** (client_c) — the operator, B2B services agency

## Shell tool defaults

- Always run `f` from any cwd — the wrapper handles the cd
- Default cwd is `/opt/forge/Claude Workspace/02-Areas/lead-pipeline`
- Master DB at `master-leads/master_leads.db` (226K leads, WAL mode)
- Logs at `logs/`
