#!/usr/bin/env python
"""Fail-closed parallel supervisor for isolated Git worktree dispatchers.

Runs in the primary checkout and coordinates up to three of the existing
single-worker mailbox dispatchers (``dispatcher.py``), one per separately
registered Git worktree. Each worktree keeps its own branch, its own
worktree-local ``agent-control`` mailboxes, and its own dispatcher singleton
lock and durable attempt claims, so the one-Claude-per-checkout rule is never
weakened -- parallelism comes only from checkout isolation.

Codex alone provisions worktrees, branches, worktree-local inboxes, and the
runtime plan at ``agent-control/dispatcher/parallel-plan.json``. The
supervisor only validates and reads them, launches and monitors child
dispatcher processes, and writes git-ignored supervisor logs and its
singleton lock. It never writes an inbox or outbox and never commits,
pushes, merges, or deploys.

A ready plan launches either every validated slot or nothing at all:

  * every slot must be a registered worktree sharing the primary
    repository's exact common Git directory, must not be the primary
    checkout, must sit on its exact expected local branch, and must contain
    dispatcher.py plus a complete ready worktree-local Claude inbox whose
    project_root is that worktree;
  * normalized source allowed_writes must be pairwise disjoint across slots,
    including ancestor/descendant overlap; each slot's exact worktree-local
    Claude outbox path is coordination output and excluded from comparison;
  * at most three slots, one child per slot, one launch per supervisor
    lifetime for each plan revision and slot task key;
  * child exits and launch failures are logged and escalated, never
    automatically retried. Retry requires Codex to publish a new revision.

Usage:
    python parallel_dispatcher.py                   # run forever
    python parallel_dispatcher.py --once --dry-run  # single non-mutating validation poll
    python parallel_dispatcher.py --dry-run         # validate and log only; never launch
"""

from __future__ import annotations

import argparse
import itertools
import json
import os
import re
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import IO

# --------------------------------------------------------------------------
# Configuration
# --------------------------------------------------------------------------

ROOT = Path(__file__).resolve().parents[2]  # primary checkout root
PLAN_PATH = Path(__file__).resolve().parent / "parallel-plan.json"
LOG_DIR = Path(__file__).resolve().parent / "logs"
SUPERVISOR_LOCK_PATH = LOG_DIR / "parallel-supervisor.lock"

POLL_SECONDS = 5
REMIND_SECONDS = 15 * 60  # throttle for repeated escalations of one condition

MAX_SLOTS = 3
PLAN_READY_STATE = "READY"
CLAUDE_READY_STATE = "IMPLEMENTATION_READY"
CLAUDE_OUTBOX_REL = "agent-control/outbox/claude-response.json"

_TASK_ID = re.compile(r"[A-Z0-9][A-Z0-9._-]{0,127}\Z")
_SLOT_ID = re.compile(r"[a-z0-9][a-z0-9-]{0,31}\Z")
_BRANCH = re.compile(r"[A-Za-z0-9][A-Za-z0-9._/-]{0,254}\Z")

_PLAN_KEYS = {"schema_version", "plan_revision", "state", "slots"}
_SLOT_KEYS = {"slot_id", "worktree_root", "branch"}

# Static sanitized error messages. Plan, inbox, and filesystem content must
# never be interpolated into an error; only these fixed strings may surface.
_ERR_PLAN_UNREADABLE = (
    "parallel plan file is malformed, contains duplicate keys, or is not a JSON object"
)
_ERR_PLAN_SCHEMA = "parallel plan schema, revision, or state is invalid"
_ERR_PLAN_SLOTS = (
    "parallel plan slots are missing, malformed, or exceed the three-slot ceiling"
)
_ERR_PLAN_DUPLICATE = (
    "parallel plan slot ids, worktree roots, and branches must be unique"
)
_ERR_PLAN_PRIMARY = "parallel plan may not use the primary checkout as a slot"
_ERR_WORKTREE = (
    "slot worktree failed registration, common-git-dir, branch, or dispatcher verification"
)
_ERR_SLOT_INBOX = (
    "slot worktree Claude inbox is missing, malformed, or not a valid ready envelope"
)
_ERR_SCOPE = (
    "slot allowed_writes contain unsafe paths or overlap another slot's source scope"
)
_ERR_LAUNCH = "child dispatcher process could not be launched"


class PlanValidationError(Exception):
    """Raised with a static sanitized message when a plan must not launch."""


class LaunchError(Exception):
    """Raised with a static sanitized message when a child could not start."""


# --------------------------------------------------------------------------
# Small utilities
# --------------------------------------------------------------------------


def log(msg: str) -> None:
    line = f"[{datetime.now():%Y-%m-%d %H:%M:%S}] {msg}"
    print(line, flush=True)
    try:
        with open(LOG_DIR / "parallel-supervisor.log", "a", encoding="utf-8") as fh:
            fh.write(line + "\n")
    except OSError:
        pass


def _canon(path: object) -> str:
    """Canonical absolute case-normalized text form of a filesystem path."""

    return os.path.normcase(os.path.abspath(str(path)))


def _load_json_strict(path: Path, error_message: str) -> dict:
    """Strict loader: duplicate keys, non-objects, and IO errors fail closed."""

    def reject_duplicate_keys(pairs: list[tuple[str, object]]) -> dict:
        result: dict = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("duplicate JSON key")
            result[key] = value
        return result

    try:
        with open(path, encoding="utf-8") as fh:
            document = json.load(fh, object_pairs_hook=reject_duplicate_keys)
    except (OSError, ValueError):
        raise PlanValidationError(error_message) from None
    if type(document) is not dict:
        raise PlanValidationError(error_message)
    return document


def _nonempty_string_list(value: object) -> bool:
    return (
        type(value) is list
        and bool(value)
        and all(type(item) is str and bool(item.strip()) for item in value)
    )


def toast(title: str, body: str, dry_run: bool) -> None:
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
        "CreateToastNotifier('Parallel Supervisor').Show("
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


def acquire_singleton_lock(lock_path: Path) -> IO[bytes] | None:
    """Hold a non-blocking one-byte OS lock for the supervisor's lifetime."""

    lock_path.parent.mkdir(parents=True, exist_ok=True)
    handle = open(lock_path, "a+b")
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
# Injectable boundaries
# --------------------------------------------------------------------------


class WorktreeInspector:
    """Read-only Git inspection boundary; tests inject a fake."""

    def common_git_dir(self, root: Path) -> str | None:
        """Canonical common Git directory of the checkout at ``root``."""

        raise NotImplementedError

    def registered_worktree_roots(self, primary_root: Path) -> list[str] | None:
        """Canonical roots of every worktree registered to the repository."""

        raise NotImplementedError

    def current_branch(self, root: Path) -> str | None:
        """Exact current local branch at ``root``; None when detached."""

        raise NotImplementedError


class GitWorktreeInspector(WorktreeInspector):
    """Production inspector: non-interactive git commands with fixed argv."""

    _GIT_TIMEOUT_SECONDS = 30

    def _run_git(self, argv: tuple[str, ...], cwd: Path) -> str | None:
        env = dict(os.environ)
        env["GIT_TERMINAL_PROMPT"] = "0"
        kwargs: dict[str, object] = {}
        if os.name == "nt":
            kwargs["creationflags"] = getattr(subprocess, "CREATE_NO_WINDOW", 0)
        try:
            completed = subprocess.run(
                list(argv),
                cwd=str(cwd),
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
                timeout=self._GIT_TIMEOUT_SECONDS,
                stdin=subprocess.DEVNULL,
                env=env,
                check=False,
                **kwargs,
            )
        except (OSError, subprocess.TimeoutExpired, ValueError):
            return None
        if completed.returncode != 0:
            return None
        return completed.stdout

    def common_git_dir(self, root: Path) -> str | None:
        out = self._run_git(("git", "rev-parse", "--git-common-dir"), root)
        if out is None:
            return None
        text = out.strip()
        if not text:
            return None
        return _canon(os.path.join(str(root), text))

    def registered_worktree_roots(self, primary_root: Path) -> list[str] | None:
        out = self._run_git(("git", "worktree", "list", "--porcelain"), primary_root)
        if out is None:
            return None
        prefix = "worktree "
        roots = [
            _canon(line[len(prefix):].strip())
            for line in out.splitlines()
            if line.startswith(prefix) and line[len(prefix):].strip()
        ]
        return roots or None

    def current_branch(self, root: Path) -> str | None:
        out = self._run_git(("git", "branch", "--show-current"), root)
        if out is None:
            return None
        branch = out.strip()
        return branch or None


class ProcessLauncher:
    """Child dispatcher process boundary; tests inject a fake."""

    def launch(
        self,
        slot_id: str,
        interpreter: str,
        dispatcher_script: Path,
        worktree_root: Path,
    ) -> object:
        raise NotImplementedError

    def poll(self, handle: object) -> int | None:
        raise NotImplementedError

    def release(self, handle: object) -> None:
        """Detach from a still-running child without terminating it."""


class _ChildHandle:
    __slots__ = ("process", "log_handle")

    def __init__(self, process: subprocess.Popen, log_handle: IO[str]) -> None:
        self.process = process
        self.log_handle = log_handle


class DispatcherProcessLauncher(ProcessLauncher):
    """Production launcher: hidden, session-isolated dispatcher children."""

    def __init__(self, log_dir: Path) -> None:
        self._log_dir = log_dir
        self._sequence = itertools.count(1)

    def launch(
        self,
        slot_id: str,
        interpreter: str,
        dispatcher_script: Path,
        worktree_root: Path,
    ) -> object:
        try:
            self._log_dir.mkdir(parents=True, exist_ok=True)
            log_path = self._log_dir / (
                f"parallel-child-{slot_id}-{datetime.now():%Y%m%d-%H%M%S}"
                f"-{next(self._sequence)}.log"
            )
            log_fh = open(log_path, "x", encoding="utf-8")
        except OSError:
            raise LaunchError(_ERR_LAUNCH) from None
        popen_kwargs: dict[str, object] = {}
        if os.name == "nt":
            popen_kwargs["creationflags"] = (
                getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)
                | getattr(subprocess, "CREATE_NO_WINDOW", 0)
            )
        else:
            popen_kwargs["start_new_session"] = True
        try:
            process = subprocess.Popen(
                [interpreter, str(dispatcher_script)],
                cwd=str(worktree_root),
                stdout=log_fh,
                stderr=subprocess.STDOUT,
                stdin=subprocess.DEVNULL,
                **popen_kwargs,
            )
        except OSError:
            log_fh.close()
            raise LaunchError(_ERR_LAUNCH) from None
        return _ChildHandle(process, log_fh)

    def poll(self, handle: object) -> int | None:
        assert isinstance(handle, _ChildHandle)
        rc = handle.process.poll()
        if rc is not None and handle.log_handle is not None:
            handle.log_handle.close()
            handle.log_handle = None
        return rc

    def release(self, handle: object) -> None:
        assert isinstance(handle, _ChildHandle)
        if handle.log_handle is not None:
            handle.log_handle.close()
            handle.log_handle = None


# --------------------------------------------------------------------------
# Plan and slot validation
# --------------------------------------------------------------------------


@dataclass(frozen=True)
class ValidatedSlot:
    slot_id: str
    root: Path
    branch: str
    task_id: str
    task_revision: int
    source_scopes: tuple[tuple[str, ...], ...]


def _validated_plan_document(document: dict) -> tuple[int, str, list]:
    if set(document) != _PLAN_KEYS:
        raise PlanValidationError(_ERR_PLAN_SCHEMA)
    if type(document["schema_version"]) is not int or document["schema_version"] != 1:
        raise PlanValidationError(_ERR_PLAN_SCHEMA)
    plan_revision = document["plan_revision"]
    if type(plan_revision) is not int or plan_revision <= 0:
        raise PlanValidationError(_ERR_PLAN_SCHEMA)
    state = document["state"]
    if type(state) is not str or not state.strip():
        raise PlanValidationError(_ERR_PLAN_SCHEMA)
    slots = document["slots"]
    if type(slots) is not list or not slots or len(slots) > MAX_SLOTS:
        raise PlanValidationError(_ERR_PLAN_SLOTS)
    return plan_revision, state, slots


def _normalized_scope(raw: object) -> tuple[str, ...] | None:
    """Normalized component tuple of one safe relative repository path."""

    if type(raw) is not str or not raw or raw != raw.strip():
        return None
    if "\x00" in raw or ":" in raw or os.path.isabs(raw):
        return None
    text = raw.replace("\\", "/")
    if text.startswith("/"):
        return None
    parts = text.split("/")
    for part in parts:
        if part in ("", ".", ".."):
            return None
    return tuple(os.path.normcase(part) for part in parts)


def _scopes_overlap(a: tuple[str, ...], b: tuple[str, ...]) -> bool:
    shorter, longer = (a, b) if len(a) <= len(b) else (b, a)
    return longer[: len(shorter)] == shorter


def _validated_slot_task(task: dict, worktree_root: Path) -> tuple[str, int, list[str]]:
    """Exact Claude activation envelope for one worktree-local ready inbox."""

    if type(task.get("schema_version")) is not int or task["schema_version"] != 1:
        raise PlanValidationError(_ERR_SLOT_INBOX)
    task_id = task.get("task_id")
    if type(task_id) is not str or _TASK_ID.fullmatch(task_id) is None:
        raise PlanValidationError(_ERR_SLOT_INBOX)
    revision = task.get("revision")
    if type(revision) is not int or revision <= 0:
        raise PlanValidationError(_ERR_SLOT_INBOX)
    if task.get("state") != CLAUDE_READY_STATE:
        raise PlanValidationError(_ERR_SLOT_INBOX)
    if task.get("assignee") != "CLAUDE" or task.get("role") != "BOUNDED_IMPLEMENTOR":
        raise PlanValidationError(_ERR_SLOT_INBOX)
    project_root = task.get("project_root")
    if (
        type(project_root) is not str
        or not os.path.isabs(project_root)
        or _canon(project_root) != _canon(worktree_root)
    ):
        raise PlanValidationError(_ERR_SLOT_INBOX)
    objective = task.get("objective")
    if type(objective) is not str or not objective.strip():
        raise PlanValidationError(_ERR_SLOT_INBOX)
    if task.get("output_file") != CLAUDE_OUTBOX_REL:
        raise PlanValidationError(_ERR_SLOT_INBOX)
    if task.get("output_schema") != "agent-control/OUTBOX_SCHEMA.md":
        raise PlanValidationError(_ERR_SLOT_INBOX)
    for field in ("allowed_reads", "allowed_writes", "forbidden_actions"):
        if not _nonempty_string_list(task.get(field)):
            raise PlanValidationError(_ERR_SLOT_INBOX)
    if CLAUDE_OUTBOX_REL not in task["allowed_writes"]:
        raise PlanValidationError(_ERR_SLOT_INBOX)
    if task.get("write_output") is not True or task.get("stop_after_handoff") is not True:
        raise PlanValidationError(_ERR_SLOT_INBOX)
    return task_id, revision, task["allowed_writes"]


def _validated_ready_slots(
    raw_slots: list,
    primary_root: Path,
    inspector: WorktreeInspector,
) -> list[ValidatedSlot]:
    """Validate the entire ready set atomically; any failure launches nothing."""

    primary_canonical = _canon(primary_root)

    structured: list[tuple[str, str, str]] = []
    for raw in raw_slots:
        if type(raw) is not dict or set(raw) != _SLOT_KEYS:
            raise PlanValidationError(_ERR_PLAN_SLOTS)
        slot_id = raw["slot_id"]
        root_text = raw["worktree_root"]
        branch = raw["branch"]
        if type(slot_id) is not str or _SLOT_ID.fullmatch(slot_id) is None:
            raise PlanValidationError(_ERR_PLAN_SLOTS)
        if type(root_text) is not str or not os.path.isabs(root_text):
            raise PlanValidationError(_ERR_PLAN_SLOTS)
        if (
            type(branch) is not str
            or _BRANCH.fullmatch(branch) is None
            or ".." in branch
            or branch == "HEAD"
        ):
            raise PlanValidationError(_ERR_PLAN_SLOTS)
        canonical_root = _canon(root_text)
        if canonical_root == primary_canonical:
            raise PlanValidationError(_ERR_PLAN_PRIMARY)
        structured.append((slot_id, canonical_root, branch))

    slot_ids = [entry[0] for entry in structured]
    roots = [entry[1] for entry in structured]
    branches = [entry[2] for entry in structured]
    if (
        len(set(slot_ids)) != len(slot_ids)
        or len(set(roots)) != len(roots)
        or len(set(branches)) != len(branches)
    ):
        raise PlanValidationError(_ERR_PLAN_DUPLICATE)

    primary_common = inspector.common_git_dir(primary_root)
    if primary_common is None:
        raise PlanValidationError(_ERR_WORKTREE)
    registered = inspector.registered_worktree_roots(primary_root)
    if not registered or primary_canonical not in registered:
        raise PlanValidationError(_ERR_WORKTREE)

    validated: list[ValidatedSlot] = []
    for slot_id, canonical_root, branch in structured:
        root = Path(canonical_root)
        if canonical_root not in registered:
            raise PlanValidationError(_ERR_WORKTREE)
        slot_common = inspector.common_git_dir(root)
        if slot_common is None or slot_common != primary_common:
            raise PlanValidationError(_ERR_WORKTREE)
        current = inspector.current_branch(root)
        if current is None or current != branch:
            raise PlanValidationError(_ERR_WORKTREE)
        if not (root / "agent-control" / "dispatcher" / "dispatcher.py").is_file():
            raise PlanValidationError(_ERR_WORKTREE)
        inbox_path = root / "agent-control" / "inbox" / "claude-task.json"
        task = _load_json_strict(inbox_path, _ERR_SLOT_INBOX)
        task_id, task_revision, allowed_writes = _validated_slot_task(task, root)
        scopes: list[tuple[str, ...]] = []
        for entry in allowed_writes:
            if entry == CLAUDE_OUTBOX_REL:
                continue  # coordination output, excluded from overlap comparison
            normalized = _normalized_scope(entry)
            if normalized is None:
                raise PlanValidationError(_ERR_SCOPE)
            scopes.append(normalized)
        validated.append(
            ValidatedSlot(slot_id, root, branch, task_id, task_revision, tuple(scopes))
        )

    for i in range(len(validated)):
        for j in range(i + 1, len(validated)):
            for scope_a in validated[i].source_scopes:
                for scope_b in validated[j].source_scopes:
                    if _scopes_overlap(scope_a, scope_b):
                        raise PlanValidationError(_ERR_SCOPE)
    return validated


# --------------------------------------------------------------------------
# Supervisor
# --------------------------------------------------------------------------


class Supervisor:
    """Validates ready plans atomically and launches at most one child per slot."""

    def __init__(
        self,
        primary_root: Path,
        plan_path: Path,
        inspector: WorktreeInspector,
        launcher: ProcessLauncher,
        *,
        dry_run: bool = False,
        interpreter: str | None = None,
        log_fn=None,
        notify_fn=None,
        clock=time.time,
    ) -> None:
        self._primary_root = Path(primary_root)
        self._plan_path = Path(plan_path)
        self._inspector = inspector
        self._launcher = launcher
        self._dry_run = bool(dry_run)
        self._interpreter = interpreter or sys.executable
        self._log = log_fn or log
        self._notify_fn = notify_fn or (
            lambda title, body: toast(title, body, self._dry_run)
        )
        self._clock = clock
        # (plan_revision, slot_id, task_id, task_revision) launched or failed;
        # one launch per supervisor lifetime for each key, never retried.
        self._attempted: set[tuple[int, str, str, int]] = set()
        self._children: dict[str, tuple[object, tuple[int, str, str, int]]] = {}
        self._notified: dict[object, float] = {}

    def has_live_children(self) -> bool:
        return bool(self._children)

    def _notify_once(self, key: object, title: str, body: str) -> None:
        now = self._clock()
        if now - self._notified.get(key, 0.0) < REMIND_SECONDS:
            return
        self._notified[key] = now
        self._log(f"NOTIFY: {title} -- {body}")
        self._notify_fn(title, body)

    def _check_children(self) -> None:
        for slot_id in sorted(self._children):
            handle, key = self._children[slot_id]
            rc = self._launcher.poll(handle)
            if rc is None:
                continue
            del self._children[slot_id]
            self._log(
                f"child dispatcher for slot {slot_id} exited with code {rc}; "
                "no automatic retry."
            )
            if rc != 0:
                self._notify_once(
                    ("child-failed", key),
                    "Parallel dispatcher child failed",
                    "A child dispatcher exited abnormally. Review supervisor logs; "
                    "no automatic retry was made.",
                )

    def poll_once(self) -> None:
        self._check_children()
        if not self._plan_path.is_file():
            if self._dry_run:
                self._log("DRY-RUN: no parallel plan file present; nothing to do.")
            return
        try:
            document = _load_json_strict(self._plan_path, _ERR_PLAN_UNREADABLE)
            plan_revision, plan_state, raw_slots = _validated_plan_document(document)
        except PlanValidationError as exc:
            self._notify_once(("plan-invalid",), "Parallel plan invalid", str(exc))
            return
        if plan_state != PLAN_READY_STATE:
            if self._dry_run:
                self._log(
                    f"DRY-RUN: parallel plan present but not {PLAN_READY_STATE}; "
                    "nothing to launch."
                )
            return
        try:
            slots = _validated_ready_slots(
                raw_slots, self._primary_root, self._inspector
            )
        except PlanValidationError as exc:
            self._notify_once(
                ("plan-rejected", plan_revision), "Parallel plan rejected", str(exc)
            )
            return
        if self._dry_run:
            self._log(
                f"DRY-RUN: plan revision {plan_revision} validated; would launch "
                f"{len(slots)} child dispatcher(s); launching nothing."
            )
            return
        for slot in slots:
            key = (plan_revision, slot.slot_id, slot.task_id, slot.task_revision)
            if key in self._attempted:
                continue
            if slot.slot_id in self._children:
                continue  # at most one child per slot
            if len(self._children) >= MAX_SLOTS:
                break  # at most three children total
            self._attempted.add(key)
            script = slot.root / "agent-control" / "dispatcher" / "dispatcher.py"
            try:
                handle = self._launcher.launch(
                    slot.slot_id, self._interpreter, script, slot.root
                )
            except (LaunchError, OSError):
                self._log(
                    f"ERROR: child dispatcher for slot {slot.slot_id} could not be "
                    "launched; no automatic retry."
                )
                self._notify_once(
                    ("launch-failed", key),
                    "Parallel dispatcher launch failed",
                    "A child dispatcher could not be launched. Review supervisor "
                    "logs; no automatic retry was made.",
                )
                continue
            self._children[slot.slot_id] = (handle, key)
            self._log(
                f"launched child dispatcher for slot {slot.slot_id} "
                f"(plan rev {plan_revision}, task rev {slot.task_revision})."
            )

    def shutdown(self) -> None:
        """Detach from in-flight children without killing them.

        Each child's own checkout-local singleton lock and durable attempt
        claim prevent duplicate Claude launches after the supervisor exits.
        """

        for slot_id in sorted(self._children):
            handle, _key = self._children.pop(slot_id)
            self._launcher.release(handle)
            self._log(
                f"supervisor stopping; leaving child dispatcher for slot {slot_id} "
                "alive (its own lock and attempt claim prevent duplicates)."
            )


# --------------------------------------------------------------------------
# Entry point
# --------------------------------------------------------------------------


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--once", action="store_true", help="single poll, then exit")
    parser.add_argument(
        "--dry-run", action="store_true", help="validate and log only; never launch"
    )
    args = parser.parse_args(argv)
    if args.once and not args.dry_run:
        parser.error(
            "--once requires --dry-run so launched child dispatchers remain monitored"
        )

    LOG_DIR.mkdir(parents=True, exist_ok=True)
    singleton_lock = acquire_singleton_lock(SUPERVISOR_LOCK_PATH)
    if singleton_lock is None:
        print(
            "Another parallel supervisor instance already holds the lock.",
            file=sys.stderr,
        )
        return 2
    log(
        f"Parallel supervisor started (root={ROOT}, dry_run={args.dry_run}, "
        f"once={args.once}, poll={POLL_SECONDS}s)"
    )
    supervisor = Supervisor(
        ROOT,
        PLAN_PATH,
        GitWorktreeInspector(),
        DispatcherProcessLauncher(LOG_DIR),
        dry_run=args.dry_run,
    )
    try:
        while True:
            supervisor.poll_once()
            if args.once:
                break
            time.sleep(POLL_SECONDS)
    except KeyboardInterrupt:
        log("Parallel supervisor stopped by user.")
    finally:
        supervisor.shutdown()
        release_singleton_lock(singleton_lock)
    return 0


if __name__ == "__main__":
    sys.exit(main())
