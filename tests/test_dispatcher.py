from __future__ import annotations

import importlib.util
import json
import tempfile
import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DISPATCHER_PATH = PROJECT_ROOT / "agent-control" / "dispatcher" / "dispatcher.py"
SPEC = importlib.util.spec_from_file_location("mailbox_dispatcher", DISPATCHER_PATH)
if SPEC is None or SPEC.loader is None:  # pragma: no cover - import machinery guard
    raise RuntimeError("dispatcher module could not be loaded")
dispatcher = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(dispatcher)


def valid_task(worker: dict) -> dict:
    return {
        "schema_version": 1,
        "revision": 7,
        "task_id": "SAFE-TASK-007",
        "state": worker["ready_state"],
        "assignee": worker["assignee"],
        "role": worker["role"],
        "project_root": str(dispatcher.ROOT),
        "objective": "Perform one bounded task.",
        "allowed_reads": ["src/example.py"],
        "allowed_writes": [worker["output_file"]],
        "forbidden_actions": ["commit or push"],
        "output_file": worker["output_file"],
        "output_schema": "agent-control/OUTBOX_SCHEMA.md",
        "write_output": True,
        "stop_after_handoff": True,
    }


class DispatcherEnvelopeTests(unittest.TestCase):
    def setUp(self) -> None:
        self.worker = dispatcher.WORKERS[0]
        self.task = valid_task(self.worker)

    def test_complete_canonical_envelope_is_accepted(self) -> None:
        self.assertEqual(
            dispatcher.validated_key(self.worker, self.task),
            ("SAFE-TASK-007", 7),
        )

    def test_bool_revision_is_rejected(self) -> None:
        self.task["revision"] = True
        self.assertIsNone(dispatcher.validated_key(self.worker, self.task))

    def test_unsafe_task_id_is_rejected(self) -> None:
        self.task["task_id"] = "../../secret"
        self.assertIsNone(dispatcher.validated_key(self.worker, self.task))

    def test_wrong_project_root_is_rejected(self) -> None:
        self.task["project_root"] = str(PROJECT_ROOT.parent)
        self.assertIsNone(dispatcher.validated_key(self.worker, self.task))

    def test_relative_project_root_is_rejected(self) -> None:
        self.task["project_root"] = "."
        self.assertIsNone(dispatcher.validated_key(self.worker, self.task))

    def test_missing_scope_field_is_rejected(self) -> None:
        del self.task["forbidden_actions"]
        self.assertIsNone(dispatcher.validated_key(self.worker, self.task))

    def test_output_must_be_in_allowed_writes(self) -> None:
        self.task["allowed_writes"] = ["somewhere/else.json"]
        self.assertIsNone(dispatcher.validated_key(self.worker, self.task))

    def test_wrong_role_or_stop_control_is_rejected(self) -> None:
        self.task["role"] = "ORCHESTRATOR"
        self.assertIsNone(dispatcher.validated_key(self.worker, self.task))
        self.task = valid_task(self.worker)
        self.task["stop_after_handoff"] = False
        self.assertIsNone(dispatcher.validated_key(self.worker, self.task))


class DispatcherJsonTests(unittest.TestCase):
    def test_duplicate_json_keys_fail_closed(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "task.json"
            path.write_text('{"state":"DRAFT","state":"IMPLEMENTATION_READY"}')
            self.assertIsNone(dispatcher.load_json(path))


class DispatcherHandoffTests(unittest.TestCase):
    def setUp(self) -> None:
        self.worker = dispatcher.WORKERS[0]
        self.task = valid_task(self.worker)
        self.outbox = {
            "schema_version": 1,
            "task_id": self.task["task_id"],
            "task_revision": self.task["revision"],
            "agent": self.worker["outbox_agent"],
            "status": "COMPLETE",
        }

    def test_exact_matching_handoff_is_accepted(self) -> None:
        self.assertTrue(
            dispatcher.handoff_written(self.worker, self.task, self.outbox)
        )

    def test_wrong_agent_revision_or_empty_status_is_rejected(self) -> None:
        for field, value in (
            ("agent", "ANTIGRAVITY"),
            ("task_revision", 8),
            ("status", "EMPTY"),
        ):
            candidate = dict(self.outbox)
            candidate[field] = value
            self.assertFalse(
                dispatcher.handoff_written(self.worker, self.task, candidate)
            )


class DispatcherAttemptTests(unittest.TestCase):
    def test_attempt_claim_is_durable_and_filename_is_hashed(self) -> None:
        original_attempt_dir = dispatcher.ATTEMPT_DIR
        try:
            with tempfile.TemporaryDirectory() as temp_dir:
                dispatcher.ATTEMPT_DIR = Path(temp_dir)
                key = ("SAFE-TASK-007", 7)
                self.assertTrue(dispatcher.claim_attempt(key))
                self.assertFalse(dispatcher.claim_attempt(key))
                self.assertTrue(dispatcher.attempt_claimed(key))
                claim_path = dispatcher.attempt_path(key)
                self.assertEqual(len(claim_path.stem), 64)
                self.assertNotIn(key[0], claim_path.name)
                payload = json.loads(claim_path.read_text(encoding="utf-8"))
                self.assertEqual(payload["revision"], 7)
        finally:
            dispatcher.ATTEMPT_DIR = original_attempt_dir

    def test_attempt_token_is_stable_and_revision_sensitive(self) -> None:
        self.assertEqual(
            dispatcher.attempt_token(("SAFE-TASK-007", 7)),
            dispatcher.attempt_token(("SAFE-TASK-007", 7)),
        )
        self.assertNotEqual(
            dispatcher.attempt_token(("SAFE-TASK-007", 7)),
            dispatcher.attempt_token(("SAFE-TASK-007", 8)),
        )


if __name__ == "__main__":
    unittest.main()
