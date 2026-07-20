#!/usr/bin/env python
"""Mailbox dispatcher for the india-swing multi-model orchestration protocol.

Watches agent-control/inbox/*.json and agent-control/outbox/*.json and:

  * spawns Claude Code headless (``claude -p "continue"``) when
    claude-task.json is IMPLEMENTATION_READY for a (task_id, revision) whose
    outbox handoff has not been written yet;
  * fires a Windows toast when the Antigravity app needs a human tap
    (RESEARCH_READY) -- Antigravity runs as a GUI app and cannot be spawned;
  * fires a Windows toast when a worker handoff lands, meaning the Codex app
    needs a human tap to review;
  * notifies on BLOCKED / CANCELLED states and on failed/timed-out Claude runs.

The dispatcher never writes any mailbox file. A singleton OS lock prevents two
dispatchers from launching workers concurrently, and a durable local attempt
marker prevents a restarted dispatcher from relaunching an already-attempted
(task_id, revision). A run that exits without producing a handoff is escalated
to the human instead of being retried; retry requires a new inbox revision.

Usage:
    python dispatcher.py               # run forever
    python dispatcher.py --once --dry-run  # single non-mutating status poll
    python dispatcher.py --dry-run     # never spawn or toast, just log
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import signal
import shutil
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import IO

# --------------------------------------------------------------------------
# Configuration
# --------------------------------------------------------------------------

ROOT = Path(__file__).resolve().parents[2]  # project root
INBOX = ROOT / "agent-control" / "inbox"
OUTBOX = ROOT / "agent-control" / "outbox"
LOG_DIR = Path(__file__).resolve().parent / "logs"
ATTEMPT_DIR = LOG_DIR / "attempts"
LOCK_PATH = LOG_DIR / "dispatcher.lock"

POLL_SECONDS = 3
REMIND_SECONDS = 15 * 60          # re-toast an unanswered human action
CLAUDE_TIMEOUT_SECONDS = 60 * 60  # kill + escalate a run that exceeds this

CLAUDE_PROMPT = "continue"
CLAUDE_ARGS = [
    "-p", CLAUDE_PROMPT,
    "--model", "sonnet",
    "--effort", "medium",
    "--output-format", "json",
    "--no-session-persistence",
    # Native Read/Grep/Glob are intentionally unavailable. File reads and
    # searches must go through the RTK-filtered Bash allow-rule below, while
    # native Edit/Write remain available because RTK does not optimize writes.
    "--tools", "Bash,Edit,Write",
    "--permission-mode", "acceptEdits",
    "--allowedTools", "Bash(rtk:*)",
]

STOP_STATES = {"BLOCKED", "CANCELLED"}
_TASK_ID = re.compile(r"[A-Z0-9][A-Z0-9._-]{0,127}\Z")

WORKERS = [
    {
        "name": "claude",
        "inbox": INBOX / "claude-task.json",
        "outbox": OUTBOX / "claude-response.json",
        "ready_state": "IMPLEMENTATION_READY",
        "assignee": "CLAUDE",
        "role": "BOUNDED_IMPLEMENTOR",
        "outbox_agent": "CLAUDE",
        "output_file": "agent-control/outbox/claude-response.json",
        "mode": "spawn",
    },
    {
        "name": "antigravity",
        "inbox": INBOX / "antigravity-task.json",
        "outbox": OUTBOX / "antigravity-response.json",
        "ready_state": "RESEARCH_READY",
        "assignee": "ANTIGRAVITY",
        "role": "READ_ONLY_ADVERSARIAL_REVIEWER",
        "outbox_agent": "ANTIGRAVITY",
        "output_file": "agent-control/outbox/antigravity-response.json",
        "mode": "notify",
        "human_hint": "Open the Antigravity app and type: continue",
    },
]

# --------------------------------------------------------------------------
# Small utilities
# --------------------------------------------------------------------------


def log(msg: str) -> None:
    line = f"[{datetime.now():%Y-%m-%d %H:%M:%S}] {msg}"
    print(line, flush=True)
    try:
        with open(LOG_DIR / "dispatcher.log", "a", encoding="utf-8") as fh:
            fh.write(line + "\n")
    except OSError:
        pass


def load_json(path: Path) -> dict | None:
    """Fail-soft reader: a missing or half-written file just skips a cycle."""

    def reject_duplicate_keys(pairs: list[tuple[str, object]]) -> dict:
        result: dict = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("duplicate JSON key")
            result[key] = value
        return result

    try:
        with open(path, encoding="utf-8") as fh:
            data = json.load(fh, object_pairs_hook=reject_duplicate_keys)
        return data if isinstance(data, dict) else None
    except (OSError, ValueError):
        return None


def _nonempty_string_list(value: object) -> bool:
    return (
        type(value) is list
        and bool(value)
        and all(type(item) is str and bool(item.strip()) for item in value)
    )


def validated_key(worker: dict, inbox: dict) -> tuple[str, int] | None:
    """Return a safe task key only for a complete activation envelope.

    The ready-state flip is the dispatch trigger. Requiring the stable routing
    fields here prevents a syntactically valid but only partially drafted JSON
    object from launching a worker.
    """

    task_id = inbox.get("task_id")
    revision = inbox.get("revision")
    project_root = inbox.get("project_root")
    objective = inbox.get("objective")
    if type(inbox.get("schema_version")) is not int or inbox["schema_version"] != 1:
        return None
    if type(task_id) is not str or _TASK_ID.fullmatch(task_id) is None:
        return None
    if type(revision) is not int or revision <= 0:
        return None
    if inbox.get("assignee") != worker["assignee"] or inbox.get("role") != worker["role"]:
        return None
    if (
        type(project_root) is not str
        or not os.path.isabs(project_root)
        or os.path.normcase(os.path.abspath(project_root))
        != os.path.normcase(str(ROOT))
    ):
        return None
    if type(objective) is not str or not objective.strip():
        return None
    if inbox.get("output_file") != worker["output_file"]:
        return None
    if inbox.get("output_schema") != "agent-control/OUTBOX_SCHEMA.md":
        return None
    for field in ("allowed_reads", "allowed_writes", "forbidden_actions"):
        if not _nonempty_string_list(inbox.get(field)):
            return None
    if worker["output_file"] not in inbox["allowed_writes"]:
        return None
    if inbox.get("write_output") is not True or inbox.get("stop_after_handoff") is not True:
        return None
    return (task_id, revision)


def handoff_written(worker: dict, inbox: dict, outbox: dict | None) -> bool:
    """True when the outbox already answers this exact inbox revision."""
    if not outbox:
        return False
    return (
        type(outbox.get("schema_version")) is int
        and outbox.get("schema_version") == 1
        and outbox.get("agent") == worker["outbox_agent"]
        and outbox.get("task_id") == inbox.get("task_id")
        and outbox.get("task_revision") == inbox.get("revision")
        and type(outbox.get("status")) is str
        and outbox["status"].strip().upper() not in ("", "EMPTY")
    )


def attempt_token(key: tuple[str, int]) -> str:
    material = json.dumps(key, ensure_ascii=True, separators=(",", ":")).encode("ascii")
    return hashlib.sha256(material).hexdigest()


def attempt_path(key: tuple[str, int]) -> Path:
    return ATTEMPT_DIR / f"{attempt_token(key)}.json"


def claim_attempt(key: tuple[str, int]) -> bool:
    """Durably claim one revision before spawn; false means already claimed."""

    ATTEMPT_DIR.mkdir(parents=True, exist_ok=True)
    payload = (
        json.dumps(
            {"task_id": key[0], "revision": key[1], "claimed_at": datetime.now().isoformat()},
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        )
        + "\n"
    ).encode("utf-8")
    try:
        descriptor = os.open(
            attempt_path(key), os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600
        )
    except FileExistsError:
        return False
    with os.fdopen(descriptor, "wb") as handle:
        handle.write(payload)
        handle.flush()
        os.fsync(handle.fileno())
    return True


def attempt_claimed(key: tuple[str, int]) -> bool:
    return attempt_path(key).is_file()


def toast(title: str, body: str, dry_run: bool) -> None:
    log(f"NOTIFY: {title} -- {body}")
    if dry_run:
        return
    t = title.replace("'", "''")
    b = body.replace("'", "''")
    ps = (
        "[Windows.UI.Notifications.ToastNotificationManager, "
        "Windows.UI.Notifications, ContentType = WindowsRuntime] | Out-Null; "
        "$x = [Windows.UI.Notifications.ToastNotificationManager]::"
        "GetTemplateContent([Windows.UI.Notifications.ToastTemplateType]::ToastText02); "
        f"$x.GetElementsByTagName('text').Item(0).InnerText = '{t}'; "
        f"$x.GetElementsByTagName('text').Item(1).InnerText = '{b}'; "
        "[Windows.UI.Notifications.ToastNotificationManager]::"
        "CreateToastNotifier('Agent Dispatcher').Show("
        "[Windows.UI.Notifications.ToastNotification]::new($x))"
    )
    try:
        subprocess.Popen(
            ["powershell", "-NoProfile", "-NonInteractive", "-Command", ps],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
    except OSError:
        print("\a", end="", flush=True)  # console bell fallback


def acquire_singleton_lock() -> IO[bytes] | None:
    """Hold a non-blocking one-byte OS lock for the dispatcher's lifetime."""

    LOG_DIR.mkdir(parents=True, exist_ok=True)
    handle = open(LOCK_PATH, "a+b")
    try:
        handle.seek(0, os.SEEK_END)
        if handle.tell() == 0:
            handle.write(b"\0")
            handle.flush()
            os.fsync(handle.fileno())
        handle.seek(0)
        if os.name == "nt":
            import msvcrt

            msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
        else:
            import fcntl

            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except (OSError, IOError):
        handle.close()
        return None
    return handle


def release_singleton_lock(handle: IO[bytes]) -> None:
    try:
        handle.seek(0)
        if os.name == "nt":
            import msvcrt

            msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
        else:
            import fcntl

            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
    finally:
        handle.close()


# --------------------------------------------------------------------------
# Dispatcher process state (attempt claims are durable under dispatcher/logs)
# --------------------------------------------------------------------------


class State:
    def __init__(self) -> None:
        self.spawned: set[tuple] = set()      # revisions we launched Claude for
        self.failed: set[tuple] = set()       # revisions needing human help
        self.notified: dict[tuple, float] = {}  # notification key -> last time
        self.claude_proc: subprocess.Popen | None = None
        self.claude_key: tuple[str, int] | None = None
        self.claude_started: float = 0.0
        self.claude_log_handle: IO[str] | None = None


def notify_once(state: State, key: tuple, title: str, body: str, dry: bool) -> None:
    now = time.time()
    if now - state.notified.get(key, 0.0) < REMIND_SECONDS:
        return
    state.notified[key] = now
    toast(title, body, dry)


# --------------------------------------------------------------------------
# Claude spawning
# --------------------------------------------------------------------------


def clear_claude_process(state: State) -> None:
    if state.claude_log_handle is not None:
        state.claude_log_handle.close()
    state.claude_log_handle = None
    state.claude_proc = None
    state.claude_key = None
    state.claude_started = 0.0


def terminate_process_tree(proc: subprocess.Popen) -> bool:
    """Terminate the exact spawned process tree and confirm it is dead."""

    try:
        if os.name == "nt":
            subprocess.run(
                ["taskkill", "/PID", str(proc.pid), "/T", "/F"],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                check=False,
                timeout=15,
                creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
            )
        else:
            os.killpg(proc.pid, signal.SIGTERM)
        proc.wait(timeout=15)
        return True
    except (OSError, subprocess.TimeoutExpired):
        if os.name != "nt":
            try:
                os.killpg(proc.pid, signal.SIGKILL)
                proc.wait(timeout=5)
                return True
            except (OSError, subprocess.TimeoutExpired):
                pass
        return proc.poll() is not None


def spawn_claude(state: State, key: tuple[str, int], dry: bool) -> None:
    if dry:
        log(f"DRY-RUN: would spawn Claude Code for task={key[0]} rev={key[1]}")
        return
    try:
        claimed = claim_attempt(key)
    except OSError:
        log("ERROR: could not durably claim the Claude task revision; refusing to spawn.")
        state.failed.add(key)
        toast("Dispatcher error", "Could not claim task revision; no worker launched", dry)
        return
    if not claimed:
        state.failed.add(key)
        toast(
            "Claude run needs attention",
            f"Task {key[0]} rev {key[1]} was already attempted. "
            "Create a new revision after review; no automatic retry was made.",
            dry,
        )
        return
    claude_bin = shutil.which("claude")
    if claude_bin is None:
        log("ERROR: 'claude' CLI not found on PATH; cannot spawn.")
        state.failed.add(key)
        toast("Dispatcher error", "claude CLI not found on PATH", dry)
        return
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    run_log = LOG_DIR / (
        f"claude-{attempt_token(key)[:16]}-{datetime.now():%Y%m%d-%H%M%S}.log"
    )
    log(f"Spawning Claude Code for task={key[0]} rev={key[1]} (log: {run_log.name})")
    try:
        log_fh = open(run_log, "x", encoding="utf-8")
    except OSError:
        log("ERROR: Claude transcript could not be opened; refusing to launch.")
        state.failed.add(key)
        toast("Dispatcher error", "Claude transcript could not be opened", dry)
        return
    popen_kwargs: dict[str, object] = {}
    if os.name == "nt":
        popen_kwargs["creationflags"] = (
            getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)
            | getattr(subprocess, "CREATE_NO_WINDOW", 0)
        )
    else:
        popen_kwargs["start_new_session"] = True
    try:
        state.claude_proc = subprocess.Popen(
            [claude_bin, *CLAUDE_ARGS],
            cwd=ROOT,
            stdout=log_fh,
            stderr=subprocess.STDOUT,
            stdin=subprocess.DEVNULL,
            **popen_kwargs,
        )
    except OSError:
        log_fh.close()
        state.failed.add(key)
        toast("Dispatcher error", "Claude process could not be launched", dry)
        return
    state.claude_log_handle = log_fh
    state.claude_key = key
    state.claude_started = time.time()
    state.spawned.add(key)


def check_claude_process(state: State, dry: bool) -> None:
    proc = state.claude_proc
    if proc is None:
        return
    rc = proc.poll()
    if rc is None:
        if time.time() - state.claude_started > CLAUDE_TIMEOUT_SECONDS:
            log(f"Claude run for {state.claude_key} exceeded timeout; terminating.")
            state.failed.add(state.claude_key)
            stopped = terminate_process_tree(proc)
            if stopped:
                toast(
                    "Claude run timed out",
                    f"Task {state.claude_key[0]} rev {state.claude_key[1]} was "
                    "terminated after timeout. Check dispatcher logs.",
                    dry,
                )
                clear_claude_process(state)
            else:
                toast(
                    "Claude termination failed",
                    "The timed-out Claude process may still be alive; no new worker "
                    "will launch until it exits.",
                    dry,
                )
        return
    log(f"Claude process for {state.claude_key} exited with code {rc}.")
    clear_claude_process(state)
    # Whether this run produced a handoff is judged from the outbox on the
    # next poll; an exit without a handoff is marked failed there.


# --------------------------------------------------------------------------
# Main polling logic
# --------------------------------------------------------------------------


def poll(state: State, dry: bool) -> None:
    check_claude_process(state, dry)

    for worker in WORKERS:
        inbox = load_json(worker["inbox"])
        if inbox is None:
            continue
        outbox = load_json(worker["outbox"])
        task_state = inbox.get("state")
        key = validated_key(worker, inbox)
        if key is None:
            if task_state == worker["ready_state"] or task_state in STOP_STATES:
                notify_once(
                    state,
                    ("invalid", worker["name"]),
                    "Invalid dispatcher task",
                    f"{worker['name']} inbox is active but incomplete or malformed; "
                    "no worker was launched.",
                    dry,
                )
            continue

        if task_state in STOP_STATES:
            notify_once(
                state,
                ("stop", worker["name"], key, task_state),
                f"{worker['name']} task {task_state}",
                f"Task {key[0]} rev {key[1]} is {task_state}. Human attention "
                "needed; no worker should improvise a workaround.",
                dry,
            )
            continue

        if task_state != worker["ready_state"]:
            continue  # Codex-internal state; nothing for the dispatcher to do

        if handoff_written(worker, inbox, outbox):
            notify_once(
                state,
                ("codex", worker["name"], key),
                "Codex review needed",
                f"{worker['name']} handed off task {key[0]} rev {key[1]}. "
                "Open the Codex app to review.",
                dry,
            )
            continue

        if worker["mode"] == "notify":
            notify_once(
                state,
                ("human", worker["name"], key),
                f"{worker['name']} turn",
                f"Task {key[0]} rev {key[1]} is {task_state}. "
                + worker.get("human_hint", "Open the app and type: continue"),
                dry,
            )
            continue

        # mode == "spawn" (Claude)
        if state.claude_proc is not None:
            continue  # a run is already in flight; single-writer rule
        if key in state.failed:
            continue  # already escalated to the human; never auto-retry
        if key in state.spawned or attempt_claimed(key):
            # We launched a run for this exact revision, it has exited, and
            # still no handoff exists, or a prior dispatcher claimed it:
            # escalate instead of retrying.
            state.failed.add(key)
            notify_once(
                state,
                ("attempted", worker["name"], key),
                "Claude run needs attention",
                f"Task {key[0]} rev {key[1]} was already attempted without a "
                "matching handoff. Review logs and create a new revision; it was "
                "not launched again.",
                dry,
            )
            continue
        spawn_claude(state, key, dry)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--once", action="store_true", help="single poll, then exit")
    parser.add_argument("--dry-run", action="store_true", help="log only; never spawn or toast")
    args = parser.parse_args()
    if args.once and not args.dry_run:
        parser.error("--once requires --dry-run so launched workers remain monitored")

    LOG_DIR.mkdir(parents=True, exist_ok=True)
    singleton_lock = acquire_singleton_lock()
    if singleton_lock is None:
        print("Another dispatcher instance already holds the project lock.", file=sys.stderr)
        return 2
    log(
        f"Dispatcher started (root={ROOT}, dry_run={args.dry_run}, "
        f"once={args.once}, poll={POLL_SECONDS}s)"
    )

    state = State()
    try:
        while True:
            poll(state, args.dry_run)
            if args.once:
                break
            time.sleep(POLL_SECONDS)
    except KeyboardInterrupt:
        if state.claude_proc is not None and state.claude_proc.poll() is None:
            log("Dispatcher stopping; leaving the in-flight Claude run alive "
                "(its durable attempt claim prevents an automatic duplicate).")
            if state.claude_log_handle is not None:
                state.claude_log_handle.close()
                state.claude_log_handle = None
        log("Dispatcher stopped by user.")
    finally:
        if state.claude_log_handle is not None:
            state.claude_log_handle.close()
            state.claude_log_handle = None
        release_singleton_lock(singleton_lock)
    return 0


if __name__ == "__main__":
    sys.exit(main())
