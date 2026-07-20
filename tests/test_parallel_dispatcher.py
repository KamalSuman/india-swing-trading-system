from __future__ import annotations

import ast
import contextlib
import io
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

# parallel_dispatcher.py is not a package; import it directly the same way
# tests/test_one_shot_runner.py imports one_shot_runner.
dispatcher_path = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "agent-control", "dispatcher")
)
sys.path.insert(0, dispatcher_path)

import parallel_dispatcher as pd_mod
from parallel_dispatcher import (
    CLAUDE_OUTBOX_REL,
    CLAUDE_READY_STATE,
    MAX_SLOTS,
    PLAN_READY_STATE,
    REMIND_SECONDS,
    LaunchError,
    PlanValidationError,
    ProcessLauncher,
    Supervisor,
    ValidatedSlot,
    WorktreeInspector,
    _normalized_scope,
    _scopes_overlap,
    _validated_plan_document,
    _validated_ready_slots,
    _validated_slot_task,
    acquire_singleton_lock,
    release_singleton_lock,
)


# --------------------------------------------------------------------------
# Shared fixtures
# --------------------------------------------------------------------------


def _valid_task(project_root: str, **overrides: object) -> dict:
    data = {
        "schema_version": 1,
        "task_id": "TASK-001",
        "revision": 1,
        "state": CLAUDE_READY_STATE,
        "assignee": "CLAUDE",
        "role": "BOUNDED_IMPLEMENTOR",
        "project_root": project_root,
        "objective": "do the assigned unit of work",
        "output_file": CLAUDE_OUTBOX_REL,
        "output_schema": "agent-control/OUTBOX_SCHEMA.md",
        "allowed_reads": ["some/file.py"],
        "allowed_writes": ["some/file.py", CLAUDE_OUTBOX_REL],
        "forbidden_actions": ["do not do unrelated things"],
        "write_output": True,
        "stop_after_handoff": True,
    }
    data.update(overrides)
    return data


def _make_slot_fixture(
    root: Path,
    *,
    allowed_writes: list[str] | None = None,
    task_overrides: dict | None = None,
    create_dispatcher: bool = True,
    create_inbox: bool = True,
    inbox_text: str | None = None,
) -> None:
    root.mkdir(parents=True, exist_ok=True)
    dispatcher_dir = root / "agent-control" / "dispatcher"
    dispatcher_dir.mkdir(parents=True, exist_ok=True)
    if create_dispatcher:
        (dispatcher_dir / "dispatcher.py").write_text("# placeholder\n", encoding="utf-8")
    inbox_dir = root / "agent-control" / "inbox"
    inbox_dir.mkdir(parents=True, exist_ok=True)
    if not create_inbox:
        return
    if inbox_text is not None:
        (inbox_dir / "claude-task.json").write_text(inbox_text, encoding="utf-8")
        return
    task = _valid_task(
        str(root),
        allowed_writes=allowed_writes or ["some/file.py", CLAUDE_OUTBOX_REL],
    )
    if task_overrides:
        task.update(task_overrides)
    (inbox_dir / "claude-task.json").write_text(json.dumps(task), encoding="utf-8")


def _write_plan(path: Path, revision: int, state: str, slots: list) -> None:
    path.write_text(
        json.dumps(
            {"schema_version": 1, "plan_revision": revision, "state": state, "slots": slots}
        ),
        encoding="utf-8",
    )


class FakeInspector(WorktreeInspector):
    """In-memory WorktreeInspector; no real git is ever invoked."""

    def __init__(self) -> None:
        self._common_git_dir: dict[str, str] = {}
        self._registered_roots: list[str] | None = None
        self._branch: dict[str, str] = {}

    def set_common_git_dir(self, root: object, value: str) -> None:
        self._common_git_dir[pd_mod._canon(root)] = value

    def set_branch(self, root: object, value: str) -> None:
        self._branch[pd_mod._canon(root)] = value

    def set_registered_roots(self, roots: list) -> None:
        self._registered_roots = [pd_mod._canon(r) for r in roots]

    def common_git_dir(self, root: Path) -> str | None:
        return self._common_git_dir.get(pd_mod._canon(root))

    def registered_worktree_roots(self, primary_root: Path) -> list[str] | None:
        return self._registered_roots

    def current_branch(self, root: Path) -> str | None:
        return self._branch.get(pd_mod._canon(root))


class FakeLauncher(ProcessLauncher):
    """In-memory ProcessLauncher; no real child process is ever spawned."""

    def __init__(self) -> None:
        self.launch_calls: list[dict] = []
        self.handles_by_slot: dict[str, object] = {}
        self.poll_results: dict[object, int | None] = {}
        self.released: list[object] = []
        self.raise_on_launch: BaseException | None = None

    def launch(self, slot_id, interpreter, dispatcher_script, worktree_root):
        self.launch_calls.append(
            {
                "slot_id": slot_id,
                "interpreter": interpreter,
                "dispatcher_script": dispatcher_script,
                "worktree_root": worktree_root,
            }
        )
        if self.raise_on_launch is not None:
            raise self.raise_on_launch
        handle = f"handle-{slot_id}-{len(self.launch_calls)}"
        self.handles_by_slot[slot_id] = handle
        self.poll_results[handle] = None
        return handle

    def poll(self, handle):
        return self.poll_results.get(handle)

    def release(self, handle):
        self.released.append(handle)


class _DispatcherFixtureTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.base = Path(self.temp.name)
        self.primary_root = self.base / "primary"
        self.primary_root.mkdir()
        self.inspector = FakeInspector()
        self.inspector.set_common_git_dir(self.primary_root, "GITDIR")
        self.inspector.set_registered_roots([self.primary_root])

    def tearDown(self) -> None:
        self.temp.cleanup()

    def _register(self, *roots: Path) -> None:
        self.inspector.set_registered_roots([self.primary_root, *roots])

    def _wire_slot(self, root: Path, branch: str, *, common_git_dir: str = "GITDIR") -> None:
        self.inspector.set_common_git_dir(root, common_git_dir)
        self.inspector.set_branch(root, branch)

    def _slot_entry(self, slot_id: str, root: Path, branch: str) -> dict:
        return {"slot_id": slot_id, "worktree_root": str(root), "branch": branch}

    def _make_ready_slot(
        self,
        name: str,
        branch: str,
        *,
        task_overrides: dict | None = None,
        allowed_writes: list[str] | None = None,
    ) -> Path:
        root = self.base / name
        _make_slot_fixture(root, allowed_writes=allowed_writes, task_overrides=task_overrides)
        self._wire_slot(root, branch)
        return root


# --------------------------------------------------------------------------
# Pure helper: _normalized_scope
# --------------------------------------------------------------------------


class NormalizedScopeTests(unittest.TestCase):
    def test_accepts_safe_relative_path(self):
        self.assertEqual(_normalized_scope("src/foo.py"), ("src", "foo.py"))

    def test_backslash_and_forward_slash_normalize_equal(self):
        self.assertEqual(
            _normalized_scope("src\\foo.py"), _normalized_scope("src/foo.py")
        )

    def test_rejects_absolute_path(self):
        self.assertIsNone(_normalized_scope("/etc/passwd"))
        self.assertIsNone(_normalized_scope("C:\\Windows\\System32"))

    def test_rejects_nul_byte(self):
        self.assertIsNone(_normalized_scope("src/foo\x00.py"))

    def test_rejects_colon(self):
        self.assertIsNone(_normalized_scope("src/foo:bar.py"))

    def test_rejects_dot_and_dotdot_components(self):
        self.assertIsNone(_normalized_scope("src/./foo.py"))
        self.assertIsNone(_normalized_scope("src/../foo.py"))
        self.assertIsNone(_normalized_scope("."))
        self.assertIsNone(_normalized_scope(".."))

    def test_rejects_non_str_and_empty(self):
        self.assertIsNone(_normalized_scope(123))
        self.assertIsNone(_normalized_scope(None))
        self.assertIsNone(_normalized_scope(""))

    def test_rejects_untrimmed_whitespace(self):
        self.assertIsNone(_normalized_scope(" src/foo.py"))
        self.assertIsNone(_normalized_scope("src/foo.py "))


# --------------------------------------------------------------------------
# Pure helper: _scopes_overlap
# --------------------------------------------------------------------------


class ScopesOverlapTests(unittest.TestCase):
    def test_identical_scopes_overlap(self):
        self.assertTrue(_scopes_overlap(("a", "b"), ("a", "b")))

    def test_ancestor_descendant_overlap(self):
        self.assertTrue(_scopes_overlap(("a",), ("a", "b")))
        self.assertTrue(_scopes_overlap(("a", "b"), ("a",)))

    def test_disjoint_scopes_do_not_overlap(self):
        self.assertFalse(_scopes_overlap(("a",), ("b",)))
        self.assertFalse(_scopes_overlap(("a", "b"), ("a", "c")))


# --------------------------------------------------------------------------
# Pure helper: _validated_plan_document
# --------------------------------------------------------------------------


class ValidatedPlanDocumentTests(unittest.TestCase):
    def _doc(self, **overrides: object) -> dict:
        doc = {
            "schema_version": 1,
            "plan_revision": 1,
            "state": "READY",
            "slots": [{"slot_id": "a", "worktree_root": "/tmp/a", "branch": "main"}],
        }
        doc.update(overrides)
        return doc

    def test_valid_document_returns_fields(self):
        revision, state, slots = _validated_plan_document(self._doc())
        self.assertEqual(revision, 1)
        self.assertEqual(state, "READY")
        self.assertEqual(len(slots), 1)

    def test_rejects_wrong_key_set(self):
        doc = self._doc()
        doc["extra"] = "nope"
        with self.assertRaises(PlanValidationError):
            _validated_plan_document(doc)

    def test_rejects_missing_key(self):
        doc = self._doc()
        del doc["state"]
        with self.assertRaises(PlanValidationError):
            _validated_plan_document(doc)

    def test_rejects_bad_schema_version(self):
        with self.assertRaises(PlanValidationError):
            _validated_plan_document(self._doc(schema_version=2))
        with self.assertRaises(PlanValidationError):
            _validated_plan_document(self._doc(schema_version="1"))

    def test_rejects_bad_plan_revision(self):
        with self.assertRaises(PlanValidationError):
            _validated_plan_document(self._doc(plan_revision=0))
        with self.assertRaises(PlanValidationError):
            _validated_plan_document(self._doc(plan_revision=-1))
        with self.assertRaises(PlanValidationError):
            _validated_plan_document(self._doc(plan_revision="1"))

    def test_rejects_bad_state(self):
        with self.assertRaises(PlanValidationError):
            _validated_plan_document(self._doc(state=""))
        with self.assertRaises(PlanValidationError):
            _validated_plan_document(self._doc(state=123))

    def test_rejects_slots_not_list(self):
        with self.assertRaises(PlanValidationError):
            _validated_plan_document(self._doc(slots={}))

    def test_rejects_empty_slots(self):
        with self.assertRaises(PlanValidationError):
            _validated_plan_document(self._doc(slots=[]))

    def test_rejects_slots_over_three_slot_ceiling(self):
        slot = {"slot_id": "a", "worktree_root": "/tmp/a", "branch": "main"}
        with self.assertRaises(PlanValidationError):
            _validated_plan_document(self._doc(slots=[slot] * (MAX_SLOTS + 1)))

    def test_accepts_exactly_three_slots(self):
        slots = [
            {"slot_id": f"s{i}", "worktree_root": f"/tmp/{i}", "branch": "main"}
            for i in range(MAX_SLOTS)
        ]
        _validated_plan_document(self._doc(slots=slots))


# --------------------------------------------------------------------------
# Pure helper: _validated_slot_task
# --------------------------------------------------------------------------


class ValidatedSlotTaskTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)

    def tearDown(self) -> None:
        self.temp.cleanup()

    def test_valid_task_returns_fields(self):
        task_id, revision, allowed_writes = _validated_slot_task(
            _valid_task(str(self.root)), self.root
        )
        self.assertEqual(task_id, "TASK-001")
        self.assertEqual(revision, 1)
        self.assertIn(CLAUDE_OUTBOX_REL, allowed_writes)

    def test_rejects_bad_schema_version(self):
        with self.assertRaises(PlanValidationError):
            _validated_slot_task(_valid_task(str(self.root), schema_version=2), self.root)

    def test_rejects_bad_task_id(self):
        with self.assertRaises(PlanValidationError):
            _validated_slot_task(
                _valid_task(str(self.root), task_id="lowercase-not-allowed"), self.root
            )

    def test_rejects_bad_revision(self):
        with self.assertRaises(PlanValidationError):
            _validated_slot_task(_valid_task(str(self.root), revision=0), self.root)
        with self.assertRaises(PlanValidationError):
            _validated_slot_task(_valid_task(str(self.root), revision="1"), self.root)

    def test_rejects_wrong_state(self):
        with self.assertRaises(PlanValidationError):
            _validated_slot_task(_valid_task(str(self.root), state="DRAFT"), self.root)

    def test_rejects_wrong_assignee_or_role(self):
        with self.assertRaises(PlanValidationError):
            _validated_slot_task(_valid_task(str(self.root), assignee="CODEX"), self.root)
        with self.assertRaises(PlanValidationError):
            _validated_slot_task(_valid_task(str(self.root), role="OTHER"), self.root)

    def test_rejects_mismatched_project_root(self):
        with tempfile.TemporaryDirectory() as other:
            with self.assertRaises(PlanValidationError):
                _validated_slot_task(_valid_task(other), self.root)

    def test_rejects_relative_project_root(self):
        with self.assertRaises(PlanValidationError):
            _validated_slot_task(_valid_task("relative/path"), self.root)

    def test_rejects_missing_objective(self):
        with self.assertRaises(PlanValidationError):
            _validated_slot_task(_valid_task(str(self.root), objective=""), self.root)

    def test_rejects_wrong_output_file_or_schema(self):
        with self.assertRaises(PlanValidationError):
            _validated_slot_task(
                _valid_task(str(self.root), output_file="wrong.json"), self.root
            )
        with self.assertRaises(PlanValidationError):
            _validated_slot_task(
                _valid_task(str(self.root), output_schema="wrong.md"), self.root
            )

    def test_rejects_missing_reads_writes_or_forbidden(self):
        for field in ("allowed_reads", "allowed_writes", "forbidden_actions"):
            with self.assertRaises(PlanValidationError):
                _validated_slot_task(
                    _valid_task(str(self.root), **{field: []}), self.root
                )

    def test_rejects_allowed_writes_missing_outbox_path(self):
        with self.assertRaises(PlanValidationError):
            _validated_slot_task(
                _valid_task(str(self.root), allowed_writes=["some/file.py"]), self.root
            )

    def test_rejects_write_output_or_stop_after_handoff_not_true(self):
        with self.assertRaises(PlanValidationError):
            _validated_slot_task(_valid_task(str(self.root), write_output=False), self.root)
        with self.assertRaises(PlanValidationError):
            _validated_slot_task(
                _valid_task(str(self.root), stop_after_handoff=False), self.root
            )


# --------------------------------------------------------------------------
# _validated_ready_slots end-to-end (injected FakeInspector)
# --------------------------------------------------------------------------


class ValidatedReadySlotsTests(_DispatcherFixtureTestCase):
    def test_duplicate_slot_id_rejected(self):
        slots = [
            self._slot_entry("dup", self.base / "a", "feature-a"),
            self._slot_entry("dup", self.base / "b", "feature-b"),
        ]
        with self.assertRaises(PlanValidationError) as caught:
            _validated_ready_slots(slots, self.primary_root, self.inspector)
        self.assertEqual(str(caught.exception), pd_mod._ERR_PLAN_DUPLICATE)

    def test_duplicate_worktree_root_rejected(self):
        root = self.base / "shared-root"
        slots = [
            self._slot_entry("slot-a", root, "feature-a"),
            self._slot_entry("slot-b", root, "feature-b"),
        ]
        with self.assertRaises(PlanValidationError) as caught:
            _validated_ready_slots(slots, self.primary_root, self.inspector)
        self.assertEqual(str(caught.exception), pd_mod._ERR_PLAN_DUPLICATE)

    def test_duplicate_branch_rejected(self):
        slots = [
            self._slot_entry("slot-a", self.base / "a", "shared-branch"),
            self._slot_entry("slot-b", self.base / "b", "shared-branch"),
        ]
        with self.assertRaises(PlanValidationError) as caught:
            _validated_ready_slots(slots, self.primary_root, self.inspector)
        self.assertEqual(str(caught.exception), pd_mod._ERR_PLAN_DUPLICATE)

    def test_primary_checkout_as_slot_rejected(self):
        slots = [self._slot_entry("primary-slot", self.primary_root, "main")]
        with self.assertRaises(PlanValidationError) as caught:
            _validated_ready_slots(slots, self.primary_root, self.inspector)
        self.assertEqual(str(caught.exception), pd_mod._ERR_PLAN_PRIMARY)

    def test_common_git_dir_disagreement_rejected(self):
        root = self._make_ready_slot("mismatched", "feature-x")
        self.inspector.set_common_git_dir(root, "DIFFERENT-GITDIR")
        self._register(root)
        slots = [self._slot_entry("mismatched", root, "feature-x")]
        with self.assertRaises(PlanValidationError) as caught:
            _validated_ready_slots(slots, self.primary_root, self.inspector)
        self.assertEqual(str(caught.exception), pd_mod._ERR_WORKTREE)

    def test_branch_mismatch_rejected(self):
        root = self._make_ready_slot("wrong-branch", "feature-x")
        self.inspector.set_branch(root, "totally-different-branch")
        self._register(root)
        slots = [self._slot_entry("wrong-branch", root, "feature-x")]
        with self.assertRaises(PlanValidationError) as caught:
            _validated_ready_slots(slots, self.primary_root, self.inspector)
        self.assertEqual(str(caught.exception), pd_mod._ERR_WORKTREE)

    def test_missing_dispatcher_script_rejected(self):
        root = self.base / "no-dispatcher"
        _make_slot_fixture(root, create_dispatcher=False)
        self._wire_slot(root, "feature-x")
        self._register(root)
        slots = [self._slot_entry("no-dispatcher", root, "feature-x")]
        with self.assertRaises(PlanValidationError) as caught:
            _validated_ready_slots(slots, self.primary_root, self.inspector)
        self.assertEqual(str(caught.exception), pd_mod._ERR_WORKTREE)

    def test_slot_inbox_missing_rejected(self):
        root = self.base / "no-inbox"
        _make_slot_fixture(root, create_inbox=False)
        self._wire_slot(root, "feature-x")
        self._register(root)
        slots = [self._slot_entry("no-inbox", root, "feature-x")]
        with self.assertRaises(PlanValidationError) as caught:
            _validated_ready_slots(slots, self.primary_root, self.inspector)
        self.assertEqual(str(caught.exception), pd_mod._ERR_SLOT_INBOX)

    def test_slot_inbox_malformed_rejected(self):
        root = self.base / "bad-json"
        _make_slot_fixture(root, inbox_text="{not-json")
        self._wire_slot(root, "feature-x")
        self._register(root)
        slots = [self._slot_entry("bad-json", root, "feature-x")]
        with self.assertRaises(PlanValidationError) as caught:
            _validated_ready_slots(slots, self.primary_root, self.inspector)
        self.assertEqual(str(caught.exception), pd_mod._ERR_SLOT_INBOX)

    def test_slot_inbox_not_ready_state_rejected(self):
        root = self._make_ready_slot(
            "not-ready", "feature-x", task_overrides={"state": "DRAFT"}
        )
        self._register(root)
        slots = [self._slot_entry("not-ready", root, "feature-x")]
        with self.assertRaises(PlanValidationError) as caught:
            _validated_ready_slots(slots, self.primary_root, self.inspector)
        self.assertEqual(str(caught.exception), pd_mod._ERR_SLOT_INBOX)

    def test_slot_inbox_wrong_assignee_role_rejected(self):
        root = self._make_ready_slot(
            "wrong-role", "feature-x", task_overrides={"role": "SOMETHING_ELSE"}
        )
        self._register(root)
        slots = [self._slot_entry("wrong-role", root, "feature-x")]
        with self.assertRaises(PlanValidationError) as caught:
            _validated_ready_slots(slots, self.primary_root, self.inspector)
        self.assertEqual(str(caught.exception), pd_mod._ERR_SLOT_INBOX)

    def test_slot_inbox_wrong_project_root_rejected(self):
        root = self._make_ready_slot(
            "wrong-root",
            "feature-x",
            task_overrides={"project_root": str(self.base / "elsewhere")},
        )
        self._register(root)
        slots = [self._slot_entry("wrong-root", root, "feature-x")]
        with self.assertRaises(PlanValidationError) as caught:
            _validated_ready_slots(slots, self.primary_root, self.inspector)
        self.assertEqual(str(caught.exception), pd_mod._ERR_SLOT_INBOX)

    def test_pairwise_direct_overlap_rejected(self):
        root_a = self._make_ready_slot(
            "ov-a", "feature-ov-a", allowed_writes=["shared/file.py", CLAUDE_OUTBOX_REL]
        )
        root_b = self._make_ready_slot(
            "ov-b", "feature-ov-b", allowed_writes=["shared/file.py", CLAUDE_OUTBOX_REL]
        )
        self._register(root_a, root_b)
        slots = [
            self._slot_entry("ov-a", root_a, "feature-ov-a"),
            self._slot_entry("ov-b", root_b, "feature-ov-b"),
        ]
        with self.assertRaises(PlanValidationError) as caught:
            _validated_ready_slots(slots, self.primary_root, self.inspector)
        self.assertEqual(str(caught.exception), pd_mod._ERR_SCOPE)

    def test_pairwise_ancestor_descendant_overlap_rejected(self):
        root_a = self._make_ready_slot(
            "anc-a",
            "feature-anc-a",
            allowed_writes=["shared/subdir/file.py", CLAUDE_OUTBOX_REL],
        )
        root_b = self._make_ready_slot(
            "anc-b", "feature-anc-b", allowed_writes=["shared/subdir", CLAUDE_OUTBOX_REL]
        )
        self._register(root_a, root_b)
        slots = [
            self._slot_entry("anc-a", root_a, "feature-anc-a"),
            self._slot_entry("anc-b", root_b, "feature-anc-b"),
        ]
        with self.assertRaises(PlanValidationError) as caught:
            _validated_ready_slots(slots, self.primary_root, self.inspector)
        self.assertEqual(str(caught.exception), pd_mod._ERR_SCOPE)

    def test_claude_outbox_path_excluded_from_overlap(self):
        root_a = self._make_ready_slot(
            "out-a", "feature-out-a", allowed_writes=["unique-a.py", CLAUDE_OUTBOX_REL]
        )
        root_b = self._make_ready_slot(
            "out-b", "feature-out-b", allowed_writes=["unique-b.py", CLAUDE_OUTBOX_REL]
        )
        self._register(root_a, root_b)
        slots = [
            self._slot_entry("out-a", root_a, "feature-out-a"),
            self._slot_entry("out-b", root_b, "feature-out-b"),
        ]
        result = _validated_ready_slots(slots, self.primary_root, self.inspector)
        self.assertEqual(len(result), 2)

    def test_fully_valid_multi_slot_plan_returns_validated_slots(self):
        root_a = self._make_ready_slot(
            "full-a",
            "feature-full-a",
            allowed_writes=["a.py", CLAUDE_OUTBOX_REL],
            task_overrides={"task_id": "TASK-FULL-A", "revision": 3},
        )
        root_b = self._make_ready_slot(
            "full-b",
            "feature-full-b",
            allowed_writes=["b.py", CLAUDE_OUTBOX_REL],
            task_overrides={"task_id": "TASK-FULL-B", "revision": 5},
        )
        self._register(root_a, root_b)
        slots = [
            self._slot_entry("full-a", root_a, "feature-full-a"),
            self._slot_entry("full-b", root_b, "feature-full-b"),
        ]
        result = _validated_ready_slots(slots, self.primary_root, self.inspector)
        self.assertEqual(len(result), 2)
        self.assertTrue(all(isinstance(slot, ValidatedSlot) for slot in result))
        by_id = {slot.slot_id: slot for slot in result}
        self.assertEqual(by_id["full-a"].task_id, "TASK-FULL-A")
        self.assertEqual(by_id["full-a"].task_revision, 3)
        self.assertEqual(by_id["full-a"].source_scopes, (("a.py",),))
        self.assertEqual(by_id["full-b"].task_id, "TASK-FULL-B")
        self.assertEqual(by_id["full-b"].task_revision, 5)


# --------------------------------------------------------------------------
# Supervisor.poll_once as an atomic all-or-nothing gate
# --------------------------------------------------------------------------


class PollOnceTests(_DispatcherFixtureTestCase):
    def setUp(self) -> None:
        super().setUp()
        self.plan_path = self.base / "plan.json"
        self.logs: list[str] = []
        self.notifications: list[tuple] = []
        self.launcher = FakeLauncher()

    def _supervisor(self, *, dry_run: bool = False) -> Supervisor:
        return Supervisor(
            self.primary_root,
            self.plan_path,
            self.inspector,
            self.launcher,
            dry_run=dry_run,
            log_fn=self.logs.append,
            notify_fn=lambda title, body: self.notifications.append((title, body)),
            clock=lambda: 1000.0,
        )

    def test_no_plan_file_present_does_nothing(self):
        supervisor = self._supervisor()
        supervisor.poll_once()
        self.assertEqual(self.launcher.launch_calls, [])
        self.assertEqual(self.notifications, [])

    def test_no_plan_file_present_logs_in_dry_run(self):
        supervisor = self._supervisor(dry_run=True)
        supervisor.poll_once()
        self.assertTrue(any("no parallel plan file present" in msg for msg in self.logs))

    def test_plan_file_failing_validation_notifies_and_launches_nothing(self):
        self.plan_path.write_text("{not-json", encoding="utf-8")
        supervisor = self._supervisor()
        supervisor.poll_once()
        self.assertEqual(self.launcher.launch_calls, [])
        self.assertEqual(len(self.notifications), 1)
        self.assertEqual(self.notifications[0][0], "Parallel plan invalid")

    def test_plan_not_ready_launches_nothing(self):
        root_a = self._make_ready_slot("a", "feature-a")
        self._register(root_a)
        _write_plan(self.plan_path, 1, "DRAFT", [self._slot_entry("a", root_a, "feature-a")])
        supervisor = self._supervisor()
        supervisor.poll_once()
        self.assertEqual(self.launcher.launch_calls, [])

    def test_plan_not_ready_logs_in_dry_run(self):
        root_a = self._make_ready_slot("a", "feature-a")
        self._register(root_a)
        _write_plan(self.plan_path, 1, "DRAFT", [self._slot_entry("a", root_a, "feature-a")])
        supervisor = self._supervisor(dry_run=True)
        supervisor.poll_once()
        self.assertTrue(any(f"not {PLAN_READY_STATE}" in msg for msg in self.logs))

    def test_ready_plan_launches_every_slot(self):
        root_a = self._make_ready_slot("a", "feature-a", allowed_writes=["a.py", CLAUDE_OUTBOX_REL])
        root_b = self._make_ready_slot("b", "feature-b", allowed_writes=["b.py", CLAUDE_OUTBOX_REL])
        self._register(root_a, root_b)
        _write_plan(
            self.plan_path,
            1,
            PLAN_READY_STATE,
            [
                self._slot_entry("a", root_a, "feature-a"),
                self._slot_entry("b", root_b, "feature-b"),
            ],
        )
        supervisor = self._supervisor()
        supervisor.poll_once()
        self.assertEqual(len(self.launcher.launch_calls), 2)
        self.assertTrue(supervisor.has_live_children())

    def test_single_invalid_slot_blocks_entire_plan(self):
        root_a = self._make_ready_slot("a", "feature-a", allowed_writes=["a.py", CLAUDE_OUTBOX_REL])
        root_b = self.base / "b"
        _make_slot_fixture(root_b, allowed_writes=["b.py", CLAUDE_OUTBOX_REL])
        # root_b is deliberately never wired (no common_git_dir/branch), so it
        # fails worktree verification while root_a alone would be valid.
        self._register(root_a, root_b)
        _write_plan(
            self.plan_path,
            1,
            PLAN_READY_STATE,
            [
                self._slot_entry("a", root_a, "feature-a"),
                self._slot_entry("b", root_b, "feature-b"),
            ],
        )
        supervisor = self._supervisor()
        supervisor.poll_once()
        self.assertEqual(self.launcher.launch_calls, [])
        self.assertFalse(supervisor.has_live_children())


# --------------------------------------------------------------------------
# Supervisor launch bookkeeping
# --------------------------------------------------------------------------


class LaunchBookkeepingTests(_DispatcherFixtureTestCase):
    def setUp(self) -> None:
        super().setUp()
        self.plan_path = self.base / "plan.json"
        self.logs: list[str] = []
        self.notifications: list[tuple] = []
        self.launcher = FakeLauncher()

    def _supervisor(self) -> Supervisor:
        return Supervisor(
            self.primary_root,
            self.plan_path,
            self.inspector,
            self.launcher,
            log_fn=self.logs.append,
            notify_fn=lambda title, body: self.notifications.append((title, body)),
            clock=lambda: 1000.0,
        )

    def test_repeated_poll_once_does_not_double_launch_same_slot(self):
        root_a = self._make_ready_slot("a", "feature-a", allowed_writes=["a.py", CLAUDE_OUTBOX_REL])
        self._register(root_a)
        _write_plan(
            self.plan_path, 1, PLAN_READY_STATE, [self._slot_entry("a", root_a, "feature-a")]
        )
        supervisor = self._supervisor()
        supervisor.poll_once()
        supervisor.poll_once()
        self.assertEqual(len(self.launcher.launch_calls), 1)

    def test_at_most_three_children_total(self):
        roots = {
            name: self._make_ready_slot(
                name, f"feature-{name}", allowed_writes=[f"{name}.py", CLAUDE_OUTBOX_REL]
            )
            for name in ("a", "b", "c", "d")
        }
        self._register(*roots.values())
        _write_plan(
            self.plan_path,
            1,
            PLAN_READY_STATE,
            [self._slot_entry(n, roots[n], f"feature-{n}") for n in ("a", "b", "c")],
        )
        supervisor = self._supervisor()
        supervisor.poll_once()
        self.assertEqual(len(self.launcher.launch_calls), 3)

        _write_plan(
            self.plan_path,
            2,
            PLAN_READY_STATE,
            [self._slot_entry(n, roots[n], f"feature-{n}") for n in ("a", "b", "d")],
        )
        supervisor.poll_once()
        # a and b are already live (skipped); d cannot launch because three
        # children are already alive (MAX_SLOTS ceiling on live children).
        self.assertEqual(len(self.launcher.launch_calls), 3)
        launched_slots = {call["slot_id"] for call in self.launcher.launch_calls}
        self.assertNotIn("d", launched_slots)

    def test_launch_error_is_logged_escalated_and_not_retried(self):
        root_a = self._make_ready_slot("a", "feature-a", allowed_writes=["a.py", CLAUDE_OUTBOX_REL])
        self._register(root_a)
        _write_plan(
            self.plan_path, 1, PLAN_READY_STATE, [self._slot_entry("a", root_a, "feature-a")]
        )
        self.launcher.raise_on_launch = LaunchError("boom")
        supervisor = self._supervisor()
        supervisor.poll_once()
        self.assertEqual(len(self.launcher.launch_calls), 1)
        self.assertFalse(supervisor.has_live_children())
        self.assertTrue(any("could not be" in msg for msg in self.logs))
        self.assertEqual(len(self.notifications), 1)
        self.assertEqual(self.notifications[0][0], "Parallel dispatcher launch failed")

        supervisor.poll_once()  # same plan revision, launcher still raising
        self.assertEqual(len(self.launcher.launch_calls), 1)  # no retry

    def test_oserror_launch_failure_is_logged_escalated_and_not_retried(self):
        """Distinct from test_launch_error_is_logged_escalated_and_not_retried:
        poll_once's launch try/except catches (LaunchError, OSError), and an
        OSError from the injected launcher must be handled identically to a
        LaunchError -- this is asserted as its own regression, not folded
        into a loop over exception types, so a failure names exactly which
        exception class broke."""

        root_a = self._make_ready_slot("a", "feature-a", allowed_writes=["a.py", CLAUDE_OUTBOX_REL])
        self._register(root_a)
        _write_plan(
            self.plan_path, 1, PLAN_READY_STATE, [self._slot_entry("a", root_a, "feature-a")]
        )
        self.launcher.raise_on_launch = OSError("boom")
        supervisor = self._supervisor()
        supervisor.poll_once()
        self.assertEqual(len(self.launcher.launch_calls), 1)
        self.assertFalse(supervisor.has_live_children())
        self.assertTrue(any("could not be" in msg for msg in self.logs))
        self.assertEqual(len(self.notifications), 1)
        self.assertEqual(self.notifications[0][0], "Parallel dispatcher launch failed")

        supervisor.poll_once()  # same plan revision, launcher still raising
        self.assertEqual(len(self.launcher.launch_calls), 1)  # no retry
        self.assertEqual(len(self.notifications), 1)  # no repeat escalation either

    def test_check_children_reaps_clean_exit_without_escalation(self):
        root_a = self._make_ready_slot("a", "feature-a", allowed_writes=["a.py", CLAUDE_OUTBOX_REL])
        self._register(root_a)
        _write_plan(
            self.plan_path, 1, PLAN_READY_STATE, [self._slot_entry("a", root_a, "feature-a")]
        )
        supervisor = self._supervisor()
        supervisor.poll_once()
        handle = self.launcher.handles_by_slot["a"]
        self.launcher.poll_results[handle] = 0
        self.plan_path.unlink()  # isolate this poll to just _check_children
        supervisor.poll_once()
        self.assertFalse(supervisor.has_live_children())
        self.assertEqual(self.notifications, [])

    def test_check_children_escalates_on_nonzero_exit_and_reap_is_never_retried(self):
        root_a = self._make_ready_slot("a", "feature-a", allowed_writes=["a.py", CLAUDE_OUTBOX_REL])
        self._register(root_a)
        _write_plan(
            self.plan_path, 1, PLAN_READY_STATE, [self._slot_entry("a", root_a, "feature-a")]
        )
        supervisor = self._supervisor()
        supervisor.poll_once()
        self.assertEqual(len(self.launcher.launch_calls), 1)
        handle = self.launcher.handles_by_slot["a"]
        self.launcher.poll_results[handle] = 1

        # The same READY plan (same plan_revision, slot, task, task_revision
        # key) is retained -- not deleted -- across the next poll. The reap
        # itself must escalate exactly once, and the attempted-key bookkeeping
        # must prevent the reaped slot from being relaunched even though the
        # plan on disk is unchanged and still names it.
        supervisor.poll_once()
        self.assertFalse(supervisor.has_live_children())
        self.assertEqual(len(self.notifications), 1)
        self.assertEqual(self.notifications[0][0], "Parallel dispatcher child failed")
        self.assertEqual(len(self.launcher.launch_calls), 1)  # not relaunched

        # A third poll on the still-present, still-READY plan must not
        # relaunch it either, and must not repeat the failure notification.
        supervisor.poll_once()
        self.assertEqual(len(self.launcher.launch_calls), 1)
        self.assertEqual(len(self.notifications), 1)

    def test_shutdown_releases_every_live_child(self):
        root_a = self._make_ready_slot("a", "feature-a", allowed_writes=["a.py", CLAUDE_OUTBOX_REL])
        root_b = self._make_ready_slot("b", "feature-b", allowed_writes=["b.py", CLAUDE_OUTBOX_REL])
        self._register(root_a, root_b)
        _write_plan(
            self.plan_path,
            1,
            PLAN_READY_STATE,
            [
                self._slot_entry("a", root_a, "feature-a"),
                self._slot_entry("b", root_b, "feature-b"),
            ],
        )
        supervisor = self._supervisor()
        supervisor.poll_once()
        self.assertTrue(supervisor.has_live_children())
        supervisor.shutdown()
        self.assertFalse(supervisor.has_live_children())
        self.assertEqual(
            set(self.launcher.released), set(self.launcher.handles_by_slot.values())
        )


# --------------------------------------------------------------------------
# Notification throttle
# --------------------------------------------------------------------------


class NotifyThrottleTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)

    def tearDown(self) -> None:
        self.temp.cleanup()

    def test_repeat_notification_within_window_is_suppressed_then_fires_after(self):
        notifications: list[tuple] = []
        clock_value = {"t": 1000.0}

        def clock() -> float:
            return clock_value["t"]

        supervisor = Supervisor(
            self.root,
            self.root / "plan.json",
            FakeInspector(),
            FakeLauncher(),
            log_fn=lambda msg: None,
            notify_fn=lambda title, body: notifications.append((title, body)),
            clock=clock,
        )
        supervisor._notify_once(("k",), "Title", "Body1")
        self.assertEqual(len(notifications), 1)

        clock_value["t"] += 60
        supervisor._notify_once(("k",), "Title", "Body2")
        self.assertEqual(len(notifications), 1)

        clock_value["t"] += REMIND_SECONDS + 1
        supervisor._notify_once(("k",), "Title", "Body3")
        self.assertEqual(len(notifications), 2)


# --------------------------------------------------------------------------
# Singleton lock (temp-directory paths only)
# --------------------------------------------------------------------------


class SingletonLockTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.lock_path = Path(self.temp.name) / "test.lock"

    def tearDown(self) -> None:
        self.temp.cleanup()

    def test_first_acquire_succeeds(self):
        handle = acquire_singleton_lock(self.lock_path)
        self.assertIsNotNone(handle)
        release_singleton_lock(handle)

    def test_second_concurrent_acquire_fails_without_raising(self):
        handle1 = acquire_singleton_lock(self.lock_path)
        self.assertIsNotNone(handle1)
        handle2 = acquire_singleton_lock(self.lock_path)
        self.assertIsNone(handle2)
        release_singleton_lock(handle1)

    def test_release_then_reacquire_succeeds(self):
        handle1 = acquire_singleton_lock(self.lock_path)
        self.assertIsNotNone(handle1)
        release_singleton_lock(handle1)
        handle2 = acquire_singleton_lock(self.lock_path)
        self.assertIsNotNone(handle2)
        release_singleton_lock(handle2)


# --------------------------------------------------------------------------
# main() argument validation (no real path may be touched)
# --------------------------------------------------------------------------


class MainArgumentValidationTests(unittest.TestCase):
    def test_once_without_dry_run_exits_2_before_touching_filesystem(self):
        # pathlib.Path instances are slotted, so Path.mkdir is patched at the
        # class level (scoped to this `with` block) rather than on the real
        # module-level LOG_DIR instance -- either way, no real directory is
        # ever created, and the mock proves main() never even attempts it.
        with patch("pathlib.Path.mkdir") as mock_mkdir:
            with patch.object(pd_mod, "acquire_singleton_lock") as mock_acquire:
                with contextlib.redirect_stderr(io.StringIO()):
                    with self.assertRaises(SystemExit) as caught:
                        pd_mod.main(["--once"])
        self.assertEqual(caught.exception.code, 2)
        mock_mkdir.assert_not_called()
        mock_acquire.assert_not_called()


# --------------------------------------------------------------------------
# AST-based capability lock
# --------------------------------------------------------------------------


class CapabilityLockTests(unittest.TestCase):
    def _source(self) -> str:
        with open(pd_mod.__file__, "r", encoding="utf-8") as fh:
            return fh.read()

    def test_no_broker_order_network_credential_llm_model_imports(self):
        tree = ast.parse(self._source())
        forbidden_roots = {
            "broker",
            "order",
            "openai",
            "anthropic",
            "requests",
            "urllib",
            "http",
            "socket",
            "ftplib",
            "smtplib",
            "paramiko",
            "boto3",
        }
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    root = alias.name.split(".")[0].lower()
                    self.assertNotIn(root, forbidden_roots, alias.name)
            elif isinstance(node, ast.ImportFrom):
                if node.module:
                    root = node.module.split(".")[0].lower()
                    self.assertNotIn(root, forbidden_roots, node.module)

    def test_subprocess_and_git_invocation_confined_to_declared_functions(self):
        """Locks subprocess/git *invocation* (actual Call nodes, not mere
        attribute/type references like ``subprocess.Popen`` used as a type
        annotation or ``subprocess.TimeoutExpired`` used as an exception
        type) to the module's own currently-declared call sites: the toast()
        notification helper, GitWorktreeInspector._run_git, and
        DispatcherProcessLauncher.launch. This freezes the module's actual
        current capability surface; it does not add a restriction the module
        does not already satisfy.
        """

        tree = ast.parse(self._source())
        allowed = {
            (None, "toast"),
            ("GitWorktreeInspector", "_run_git"),
            ("DispatcherProcessLauncher", "launch"),
        }
        offenders: list[str] = []

        class Visitor(ast.NodeVisitor):
            def __init__(self) -> None:
                self.class_stack: list[str] = []
                self.func_stack: list[str] = []

            def visit_ClassDef(self, node: ast.ClassDef) -> None:
                self.class_stack.append(node.name)
                self.generic_visit(node)
                self.class_stack.pop()

            def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
                self.func_stack.append(node.name)
                self.generic_visit(node)
                self.func_stack.pop()

            def visit_Call(self, node: ast.Call) -> None:
                func = node.func
                is_subprocess_call = (
                    isinstance(func, ast.Attribute)
                    and isinstance(func.value, ast.Name)
                    and func.value.id == "subprocess"
                )
                if is_subprocess_call:
                    current_class = self.class_stack[-1] if self.class_stack else None
                    current_func = self.func_stack[-1] if self.func_stack else None
                    if (current_class, current_func) not in allowed:
                        label = (
                            f"{current_class}.{current_func}"
                            if current_class
                            else str(current_func)
                        )
                        offenders.append(label)
                self.generic_visit(node)

        Visitor().visit(tree)
        self.assertEqual(offenders, [])

    def test_git_command_construction_confined_to_git_worktree_inspector(self):
        """The subprocess-invocation test above already proves git command
        *execution* (the actual subprocess.run call) happens only inside
        GitWorktreeInspector._run_git. This test proves git command
        *construction* -- every occurrence of the literal git-binary string
        "git" that seeds an argv tuple -- appears only inside the
        GitWorktreeInspector class as a whole (its three thin wrapper
        methods, common_git_dir/registered_worktree_roots/current_branch,
        each build one argv tuple and delegate to _run_git). Together the
        two tests prove no fourth site, anywhere in the module, can ever
        construct or execute a git command.
        """

        tree = ast.parse(self._source())
        offenders: list[str] = []

        class Visitor(ast.NodeVisitor):
            def __init__(self) -> None:
                self.class_stack: list[str] = []

            def visit_ClassDef(self, node: ast.ClassDef) -> None:
                self.class_stack.append(node.name)
                self.generic_visit(node)
                self.class_stack.pop()

            def visit_Constant(self, node: ast.Constant) -> None:
                if node.value == "git":
                    current_class = self.class_stack[-1] if self.class_stack else None
                    if current_class != "GitWorktreeInspector":
                        offenders.append(str(current_class))
                self.generic_visit(node)

        Visitor().visit(tree)
        self.assertEqual(offenders, [])


if __name__ == "__main__":
    unittest.main()
