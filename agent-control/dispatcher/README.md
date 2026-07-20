# Mailbox dispatcher

Automates the human relay in the ORCHESTRATION.md loop. It watches the
`agent-control/inbox` / `agent-control/outbox` JSON files and:

| Event | Action |
| --- | --- |
| `claude-task.json` is `IMPLEMENTATION_READY` and no matching handoff exists | Spawns `claude -p "continue"` headless in the project root (Claude Code CLI reads CLAUDE.md and the inbox itself, so the existing protocol just works) |
| `antigravity-task.json` is `RESEARCH_READY` and no matching handoff exists | Windows toast: open the Antigravity app and type `continue` (GUI app, cannot be spawned) |
| A worker handoff lands for the current revision | Windows toast: open the Codex app to review |
| A task goes `BLOCKED` / `CANCELLED` | Windows toast escalation |
| A headless Claude run exits without a handoff, or exceeds 60 min | Escalates to you; **never auto-retries** |

The dispatcher never writes any mailbox file. Local runtime state lives under
the git-ignored `agent-control/dispatcher/logs/` directory:

- `dispatcher.lock` is held with a non-blocking OS lock, so only one dispatcher
  instance can run for this checkout;
- `attempts/<sha256>.json` durably claims a Claude `(task_id, revision)` before
  launch, so a restart cannot launch the same revision twice;
- `claude-*.log` files are headless run transcripts.

Stopping or restarting is fail-closed. If a revision was claimed but did not
produce a matching handoff, the dispatcher escalates it and will not retry it.
Codex must inspect the log and issue a new inbox revision for any retry.

## Usage

```powershell
# status check (no spawns, no toasts)
.\agent-control\dispatcher\run.ps1 --once --dry-run

# run the loop
.\agent-control\dispatcher\run.ps1
```

Headless run transcripts land in `agent-control/dispatcher/logs/`.

`--once` is accepted only with `--dry-run`; a dispatcher that launches a worker
must remain alive to monitor that process and enforce its timeout.

## Publishing a task safely

Only Codex writes inboxes. Build and validate the complete task in a non-ready
state first. The task must include its routing, scope, output contract, and
stop controls. Make the one-line state change to `IMPLEMENTATION_READY` or
`RESEARCH_READY` the final inbox edit. Never continue editing an already-ready
revision; corrections or retries require a higher revision.

## What stays manual

Antigravity and Codex run as GUI apps, so their turns still need one tap each
("continue" in Antigravity, review poke in Codex). The toast tells you which
app and which task. Commit/push remains Codex-gated per ORCHESTRATION.md.

## One-time setup

Headless Claude cannot answer permission prompts. The dispatcher exposes only
native `Edit`/`Write` plus Bash and passes
`--permission-mode acceptEdits --allowedTools "Bash(rtk:*)"`. Native `Read`,
`Grep`, and `Glob` are deliberately unavailable, so file reads use `rtk read`
and searches use targeted `rtk rg`. Native editing stays enabled because
RTK optimizes shell output, not writes. If a run's log shows a denied tool,
review the exact task scope before changing these fail-closed flags.

Safety properties preserved from ORCHESTRATION.md:

- single writer: the singleton lock and in-process gate permit at most one
  dispatcher and one Claude process per checkout;
- at most one Claude spawn per `(task_id, revision)`, including across
  dispatcher restarts; failures escalate and require a new revision;
- a ready inbox is ignored unless its complete routing/scope envelope validates,
  and duplicate-key or half-written JSON is ignored;
- mailbox files remain the only instruction channel — toasts carry no
  instructions, only "which app to open";
- the dispatcher itself never commits, pushes, or edits mailboxes.
