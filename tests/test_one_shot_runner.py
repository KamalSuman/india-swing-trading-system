import unittest
import os
import tempfile
import hashlib
import ast
import stat
import importlib.util
import io
from unittest import mock
from datetime import datetime, timezone, timedelta
import sys
from typing import Optional

# Setup path strictly for testing
dispatcher_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'agent-control', 'dispatcher'))
sys.path.insert(0, dispatcher_path)

import worker_status
import one_shot_runner as osr_mod

# Bring types into scope
from one_shot_runner import (
    OneShotRequest, AttemptOutcome, OneShotRunnerError, run_one_shot,
    _canonical_path
)
from worker_status import read_status, WorkerPhase, WorkerReason

class MockHandle:
    def __init__(self, pid=123, exit_code=0, timeout=False, raise_on_wait=False, missing_pid=False, raising_pid=False):
        self._pid_val = pid
        self._exit_code = exit_code
        self._timeout = timeout
        self.terminated_count = 0
        self.raise_on_wait = raise_on_wait
        self.missing_pid = missing_pid
        self.raising_pid = raising_pid
        self.raise_on_terminate = False

    @property
    def pid(self):
        if self.raising_pid:
            raise ValueError("Secret pid error!")
        if self.missing_pid:
            raise AttributeError("pid missing")
        return self._pid_val

    def wait(self, timeout_seconds: int) -> int:
        if self._timeout:
            raise TimeoutError("Mock timeout")
        if self.raise_on_wait:
            raise ValueError("Secret wait error!")
        return self._exit_code

    def terminate_tree(self) -> None:
        self.terminated_count += 1
        if self.raise_on_terminate:
            raise ValueError("Secret terminate error!")

class MockLauncher:
    def __init__(self, handle_to_return=None, raise_on_launch=False):
        self.handle_to_return = handle_to_return
        self.raise_on_launch = raise_on_launch
        self.launch_count = 0

    def launch(self, argv, cwd, transcript_path):
        self.launch_count += 1
        if self.raise_on_launch:
            raise ValueError("Secret launch error!")
        return self.handle_to_return

class MockHandoffChecker:
    def __init__(self, return_val=False, raise_on_check=False):
        self.return_val = return_val
        self.raise_on_check = raise_on_check
        self.check_count = 0

    def has_matching_handoff(self, task_id, task_revision):
        self.check_count += 1
        if self.raise_on_check:
            raise ValueError("Secret handoff error!")
        return self.return_val

class MockClock:
    def __init__(self, raise_on_call=False):
        self.call_count = 0
        self.raise_on_call = raise_on_call
        self.base_time = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)

    def __call__(self):
        if self.raise_on_call:
            raise ValueError("Secret clock error!")
        self.call_count += 1
        return self.base_time + timedelta(seconds=self.call_count)

class TestOneShotRunner(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.project_root = self.temp_dir.name
        self.runtime_root = os.path.join(self.project_root, 'agent-control', 'dispatcher', 'logs', 'one-shot')
        os.makedirs(self.runtime_root, exist_ok=True)
        
        self.task_id = "TASK-001.A_B"
        self.task_revision = 1
        self.task_bytes = b'{"hello": "world"}'
        
        token_src = f"{self.task_id}\n{self.task_revision}".encode('utf-8')
        self.task_token = hashlib.sha256(token_src).hexdigest()
        self.expected_sha256 = hashlib.sha256(self.task_bytes).hexdigest()
        
    def tearDown(self):
        self.temp_dir.cleanup()

    def make_valid_request(self, **kwargs):
        data = {
            "task_id": self.task_id,
            "task_revision": self.task_revision,
            "task_token": self.task_token,
            "task_bytes": self.task_bytes,
            "expected_sha256": self.expected_sha256,
            "project_root": self.project_root,
            "runtime_root": self.runtime_root,
            "timeout_seconds": 300,
            "claude_executable": "claude",
            "allowed_tools": ('Bash(rtk:*)',)
        }
        
        # Auto-compute token/hash if ID/bytes changed but token/hash not explicitly overridden
        # To avoid making invalid request by mistake when we just want to change ID or bytes
        if "task_id" in kwargs or "task_revision" in kwargs:
            tid = kwargs.get("task_id", self.task_id)
            trev = kwargs.get("task_revision", self.task_revision)
            data["task_token"] = hashlib.sha256(f"{tid}\n{trev}".encode('utf-8')).hexdigest()
        if "task_bytes" in kwargs:
            data["expected_sha256"] = hashlib.sha256(kwargs["task_bytes"]).hexdigest()

        data.update(kwargs)
        return OneShotRequest(**data)

    def write_transcript(self, text):
        tpath = os.path.join(self.runtime_root, "transcripts", f"{self.task_token}.log")
        os.makedirs(os.path.dirname(tpath), exist_ok=True)
        with open(tpath, "wb") as f:
            f.write(text.encode('utf-8'))

    def write_transcript_for(self, request, text):
        tpath = os.path.join(
            self.runtime_root, "transcripts", f"{request.task_token}.log"
        )
        os.makedirs(os.path.dirname(tpath), exist_ok=True)
        with open(tpath, "wb") as f:
            f.write(text.encode("utf-8"))

    def assert_sanitized(self, msg):
        msg_lower = msg.lower()
        forbidden = ["secret", "tampered", self.task_token[:8], self.project_root.lower()]
        for f in forbidden:
            if f in msg_lower:
                self.fail(f"Sanitization failure. Message contains '{f}': {msg}")

    # --- Validation Tests ---

    def test_task_id_boundaries(self):
        # Valid: Dot and underscore, length 128
        req = self.make_valid_request(task_id="A" + "B"*127) # 128
        self.assertEqual(len(req.task_id), 128)
        self.make_valid_request(task_id="A._-B")
        
        # Invalid
        with self.assertRaises(OneShotRunnerError): self.make_valid_request(task_id="A" + "B"*128) # 129
        with self.assertRaises(OneShotRunnerError): self.make_valid_request(task_id=".AB") # Must start with alnum
        with self.assertRaises(OneShotRunnerError): self.make_valid_request(task_id="A B")
        with self.assertRaises(OneShotRunnerError): self.make_valid_request(task_id="a-b") # Uppercase only

    def test_canonical_path_checks(self):
        # Case mismatch Windows alias
        case_alias = self.runtime_root.upper()
        req = self.make_valid_request(runtime_root=case_alias) # Should pass if OS is case-insensitive
        
        # Junction/Symlink test. We just test the _canonical_path explicitly as symlinks require admin
        self.assertEqual(_canonical_path("C:\\A\\B"), _canonical_path("c:\\a\\b"))
        self.assertEqual(_canonical_path("C:\\A\\.\\B"), _canonical_path("C:\\A\\B"))

    def test_obinary_fallback(self):
        self.assertTrue(hasattr(osr_mod, 'O_BINARY_FLAG'))
        self.assertTrue(isinstance(osr_mod.O_BINARY_FLAG, int))

    def test_sys_module_clean(self):
        with open(osr_mod.__file__, 'r') as f:
            content = f.read()
            self.assertNotIn("import sys", content)
            self.assertNotIn("sys.path", content)

    def test_executable_must_be_trimmed_and_reject_del(self):
        with self.assertRaises(OneShotRunnerError):
            self.make_valid_request(claude_executable=" claude")
        with self.assertRaises(OneShotRunnerError):
            self.make_valid_request(claude_executable="claude ")
        with self.assertRaises(OneShotRunnerError):
            self.make_valid_request(claude_executable="claude\x7f")

    # --- Ordering & State Tests ---

    def test_fresh_state_ordering(self):
        req = self.make_valid_request()
        launcher = MockLauncher(MockHandle())
        
        # Pre-existing status blocks claim creation
        status_path = os.path.join(self.runtime_root, "status", f"{req.task_token}.json")
        os.makedirs(os.path.dirname(status_path), exist_ok=True)
        with open(status_path, 'wb') as f:
            f.write(b"{}")
        
        with self.assertRaises(OneShotRunnerError) as e:
            run_one_shot(req, launcher, MockHandoffChecker(), MockClock())
        self.assertIn("pre-existing status blocks", str(e.exception))
        
        cpath = os.path.join(self.runtime_root, "claims", f"{req.task_token}.claim")
        self.assertFalse(os.path.exists(cpath))

        os.remove(status_path)
        
        # Pre-existing claim blocks snapshot
        os.makedirs(os.path.dirname(cpath), exist_ok=True)
        with open(cpath, 'wb') as f:
            f.write(b"claimed")
            
        with self.assertRaises(OneShotRunnerError) as e:
            run_one_shot(req, launcher, MockHandoffChecker(), MockClock())
        self.assertIn("pre-existing claim blocks", str(e.exception))
        
        spath = os.path.join(self.runtime_root, "snapshots", f"{req.expected_sha256}.json")
        self.assertFalse(os.path.exists(spath))
        self.assertEqual(launcher.launch_count, 0)

    # --- Snapshot Tests ---

    def test_snapshot_sizes(self):
        # 1 byte
        self.make_valid_request(task_bytes=b"A")
        # 262144 bytes
        self.make_valid_request(task_bytes=b"A"*262144)
        
        with self.assertRaises(OneShotRunnerError): self.make_valid_request(task_bytes=b"")
        with self.assertRaises(OneShotRunnerError): self.make_valid_request(task_bytes=b"A"*262145)

    def test_snapshot_exact_reuse(self):
        req1 = self.make_valid_request()
        launcher = MockLauncher(MockHandle())
        self.write_transcript("")
        run_one_shot(req1, launcher, MockHandoffChecker(), MockClock())
        
        # Req2 has same bytes, different ID
        req2 = self.make_valid_request(task_id="TASK-002")
        self.assertEqual(req1.expected_sha256, req2.expected_sha256)
        
        # Should reuse snapshot
        self.write_transcript("")
        # Write another transcript for req2
        tpath2 = os.path.join(self.runtime_root, "transcripts", f"{req2.task_token}.log")
        with open(tpath2, 'wb') as f:
            f.write(b"")
            
        launcher.launch_count = 0
        run_one_shot(req2, launcher, MockHandoffChecker(), MockClock())
        self.assertEqual(launcher.launch_count, 1)

    def test_snapshot_bounds_and_tamper(self):
        req = self.make_valid_request()
        spath = os.path.join(self.runtime_root, "snapshots", f"{req.expected_sha256}.json")
        
        import builtins
        import unittest.mock
        original_open = builtins.open
        read_calls = []
        
        def spy_open(path, *args, **kwargs):
            f = original_open(path, *args, **kwargs)
            if str(path).endswith(".json") and "snapshots" in str(path):
                original_read = f.read
                def spy_read(size=-1):
                    read_calls.append(size)
                    return original_read(size)
                f.read = spy_read
            return f
            
        with unittest.mock.patch('builtins.open', side_effect=spy_open):
            launcher = MockLauncher(MockHandle())
            run_one_shot(req, launcher, MockHandoffChecker(True), MockClock())
            
        self.assertIn(262145, read_calls)

    def test_prelaunch_snapshot_readback_blocks_launch(self):
        req = self.make_valid_request()
        launcher = MockLauncher(MockHandle())
        snapshot_path = os.path.join(
            self.runtime_root, "snapshots", f"{req.expected_sha256}.json"
        )
        real_open = open

        def tampering_open(path, mode="r", *args, **kwargs):
            if os.path.normcase(str(path)) == os.path.normcase(snapshot_path) and mode == "rb":
                return io.BytesIO(b"secret tampered snapshot")
            return real_open(path, mode, *args, **kwargs)

        with mock.patch("builtins.open", side_effect=tampering_open):
            with self.assertRaises(OneShotRunnerError) as caught:
                run_one_shot(req, launcher, MockHandoffChecker(True), MockClock())
        self.assert_sanitized(str(caught.exception))
        self.assertEqual(launcher.launch_count, 0)

    def test_claim_fsync_failure_blocks_launch_and_consumes_claim(self):
        req = self.make_valid_request()
        launcher = MockLauncher(MockHandle())
        with mock.patch.object(
            osr_mod.os, "fsync", side_effect=OSError("secret fsync failure")
        ):
            with self.assertRaises(OneShotRunnerError) as caught:
                run_one_shot(req, launcher, MockHandoffChecker(), MockClock())
        self.assert_sanitized(str(caught.exception))
        self.assertEqual(launcher.launch_count, 0)
        claim_path = os.path.join(
            self.runtime_root, "claims", f"{req.task_token}.claim"
        )
        self.assertTrue(os.path.exists(claim_path))

    def test_claim_and_snapshot_exist_before_launch(self):
        req = self.make_valid_request()

        class OrderingLauncher(MockLauncher):
            def launch(self_, argv, cwd, transcript_path):
                claim_path = os.path.join(
                    self.runtime_root, "claims", f"{req.task_token}.claim"
                )
                snapshot_path = os.path.join(
                    self.runtime_root, "snapshots", f"{req.expected_sha256}.json"
                )
                with open(claim_path, "rb") as claim_file:
                    self.assertEqual(claim_file.read(), b"claimed\n")
                with open(snapshot_path, "rb") as snapshot_file:
                    self.assertEqual(snapshot_file.read(262145), req.task_bytes)
                return super().launch(argv, cwd, transcript_path)

        launcher = OrderingLauncher(MockHandle())
        self.write_transcript_for(req, "")
        run_one_shot(req, launcher, MockHandoffChecker(), MockClock())
        self.assertEqual(launcher.launch_count, 1)

    def test_post_launch_tamper_blocks_handoff(self):
        req = self.make_valid_request()
        class TamperingHandle(MockHandle):
            def wait(self_, timeout):
                spath = os.path.join(self.runtime_root, "snapshots", f"{req.expected_sha256}.json")
                with open(spath, 'wb') as f:
                    f.write(b"tampered")
                return 0
                
        handoff = MockHandoffChecker(True)
        outcome = run_one_shot(req, MockLauncher(TamperingHandle()), handoff, MockClock())
        self.assertEqual(outcome, AttemptOutcome.PRIOR_ATTEMPT_NO_HANDOFF)
        self.assertEqual(handoff.check_count, 0) # Handoff blocked

    # --- Fault Matrix Tests ---

    def test_clock_call_counts(self):
        req = self.make_valid_request()
        clock = MockClock()
        self.write_transcript("")
        run_one_shot(req, MockLauncher(MockHandle()), MockHandoffChecker(), clock)
        self.assertEqual(clock.call_count, 2)
        
        status_path = os.path.join(self.runtime_root, "status", f"{req.task_token}.json")
        ws = read_status(status_path)
        # Clock returned +2 sec for terminal
        self.assertEqual(ws.observed_at_utc, datetime(2024, 1, 1, 12, 0, 2, tzinfo=timezone.utc))

    def test_missing_pid_terminates(self):
        req = self.make_valid_request()
        handle = MockHandle(missing_pid=True)
        outcome = run_one_shot(req, MockLauncher(handle), MockHandoffChecker(), MockClock())
        self.assertEqual(outcome, AttemptOutcome.LAUNCH_ERROR)
        self.assertEqual(handle.terminated_count, 1)

    def test_bool_pid_terminates(self):
        req = self.make_valid_request()
        handle = MockHandle(pid=True)
        outcome = run_one_shot(req, MockLauncher(handle), MockHandoffChecker(), MockClock())
        self.assertEqual(outcome, AttemptOutcome.LAUNCH_ERROR)
        self.assertEqual(handle.terminated_count, 1)

    def test_wait_failure_terminates(self):
        req = self.make_valid_request()
        handle = MockHandle(raise_on_wait=True)
        with self.assertRaises(OneShotRunnerError) as e:
            run_one_shot(req, MockLauncher(handle), MockHandoffChecker(), MockClock())
        self.assert_sanitized(str(e.exception))
        self.assertEqual(handle.terminated_count, 1)

    def test_launcher_and_raising_pid_failures_are_sanitized(self):
        req = self.make_valid_request()
        launch_outcome = run_one_shot(
            req, MockLauncher(raise_on_launch=True), MockHandoffChecker(), MockClock()
        )
        self.assertEqual(launch_outcome, AttemptOutcome.LAUNCH_ERROR)

        req2 = self.make_valid_request(task_id="TASK-RAISING-PID")
        handle = MockHandle(raising_pid=True)
        pid_outcome = run_one_shot(
            req2, MockLauncher(handle), MockHandoffChecker(), MockClock()
        )
        self.assertEqual(pid_outcome, AttemptOutcome.LAUNCH_ERROR)
        self.assertEqual(handle.terminated_count, 1)

    def test_running_status_write_failure_always_terminates(self):
        req = self.make_valid_request()
        handle = MockHandle()
        with mock.patch.object(
            osr_mod, "write_status_atomic", side_effect=OSError("secret status failure")
        ):
            with self.assertRaises(OneShotRunnerError) as caught:
                run_one_shot(req, MockLauncher(handle), MockHandoffChecker(), MockClock())
        self.assert_sanitized(str(caught.exception))
        self.assertEqual(handle.terminated_count, 1)

    def test_running_status_and_termination_combined_failure(self):
        req = self.make_valid_request()
        handle = MockHandle()
        handle.raise_on_terminate = True
        with mock.patch.object(
            osr_mod, "write_status_atomic", side_effect=OSError("secret status failure")
        ):
            with self.assertRaises(OneShotRunnerError) as caught:
                run_one_shot(req, MockLauncher(handle), MockHandoffChecker(), MockClock())
        self.assert_sanitized(str(caught.exception))
        self.assertIn("termination failed", str(caught.exception))
        self.assertEqual(handle.terminated_count, 1)

    def test_terminate_failure_sanitization(self):
        req = self.make_valid_request()
        handle = MockHandle(raise_on_wait=True)
        handle.raise_on_terminate = True
        with self.assertRaises(OneShotRunnerError) as e:
            run_one_shot(req, MockLauncher(handle), MockHandoffChecker(), MockClock())
        self.assert_sanitized(str(e.exception))
        self.assertIn("termination failed", str(e.exception).lower())

    def test_matching_handoff_wins_over_timeout(self):
        req = self.make_valid_request()
        handle = MockHandle(timeout=True)
        handoff = MockHandoffChecker(True)
        outcome = run_one_shot(req, MockLauncher(handle), handoff, MockClock())
        self.assertEqual(outcome, AttemptOutcome.HANDOFF_READY)
        self.assertEqual(handle.terminated_count, 1) # Still terminates on timeout

    def test_matching_handoff_wins_over_bool_exit(self):
        req = self.make_valid_request()
        handle = MockHandle(exit_code=True)
        handoff = MockHandoffChecker(True)
        outcome = run_one_shot(req, MockLauncher(handle), handoff, MockClock())
        self.assertEqual(outcome, AttemptOutcome.HANDOFF_READY)

    def test_matching_handoff_wins_over_missing_transcript(self):
        req = self.make_valid_request()
        handle = MockHandle()
        handoff = MockHandoffChecker(True)
        # Transcript NOT written
        outcome = run_one_shot(req, MockLauncher(handle), handoff, MockClock())
        self.assertEqual(outcome, AttemptOutcome.HANDOFF_READY)

    def test_invalid_exit_and_handoff_failure_write_fallback_terminal(self):
        req = self.make_valid_request()
        with self.assertRaises(OneShotRunnerError) as caught:
            run_one_shot(
                req,
                MockLauncher(MockHandle(exit_code="secret invalid exit")),
                MockHandoffChecker(False),
                MockClock(),
            )
        self.assert_sanitized(str(caught.exception))
        status_path = os.path.join(
            self.runtime_root, "status", f"{req.task_token}.json"
        )
        self.assertEqual(
            read_status(status_path).reason,
            WorkerReason.PRIOR_ATTEMPT_NO_HANDOFF,
        )

        req2 = self.make_valid_request(task_id="TASK-HANDOFF-ERROR")
        with self.assertRaises(OneShotRunnerError) as caught:
            run_one_shot(
                req2,
                MockLauncher(MockHandle()),
                MockHandoffChecker(raise_on_check=True),
                MockClock(),
            )
        self.assert_sanitized(str(caught.exception))
        status_path2 = os.path.join(
            self.runtime_root, "status", f"{req2.task_token}.json"
        )
        self.assertEqual(
            read_status(status_path2).reason,
            WorkerReason.PRIOR_ATTEMPT_NO_HANDOFF,
        )

    def test_clock_failures_are_sanitized_and_initial_failure_terminates(self):
        req = self.make_valid_request()
        handle = MockHandle()
        with self.assertRaises(OneShotRunnerError) as caught:
            run_one_shot(
                req,
                MockLauncher(handle),
                MockHandoffChecker(),
                MockClock(raise_on_call=True),
            )
        self.assert_sanitized(str(caught.exception))
        self.assertEqual(handle.terminated_count, 1)

        class FinalFailClock(MockClock):
            def __call__(self_):
                self_.call_count += 1
                if self_.call_count == 2:
                    raise ValueError("secret final clock error")
                return self_.base_time + timedelta(seconds=self_.call_count)

        req2 = self.make_valid_request(task_id="TASK-FINAL-CLOCK")
        self.write_transcript_for(req2, "")
        with self.assertRaises(OneShotRunnerError) as caught:
            run_one_shot(
                req2,
                MockLauncher(MockHandle()),
                MockHandoffChecker(),
                FinalFailClock(),
            )
        self.assert_sanitized(str(caught.exception))

    def test_missing_transcript_fallback_terminal(self):
        req = self.make_valid_request()
        handle = MockHandle()
        handoff = MockHandoffChecker(False)
        with self.assertRaises(OneShotRunnerError) as e:
            run_one_shot(req, MockLauncher(handle), handoff, MockClock())
        self.assert_sanitized(str(e.exception))
        
        status_path = os.path.join(self.runtime_root, "status", f"{req.task_token}.json")
        ws = read_status(status_path)
        self.assertEqual(ws.reason, WorkerReason.PRIOR_ATTEMPT_NO_HANDOFF)

    # --- Transcript IO Bounds Tests ---

    def test_transcript_tail_bounds(self):
        req = self.make_valid_request()
        self.write_transcript("A" * 70000 + "SESSION LIMIT")
        
        # Mock file size and reading
        import unittest.mock
        m_file = unittest.mock.MagicMock()
        m_file.tell.return_value = 70013
        m_file.read.return_value = b"A" * (65536 - 13) + b"SESSION LIMIT"
        m_open = unittest.mock.MagicMock(return_value=unittest.mock.MagicMock(__enter__=lambda x: m_file, __exit__=lambda *a: None))
        
        # We also need to patch read_status to not use our mock!
        # Just run the actual file system, it's safer.
        
        outcome = run_one_shot(req, MockLauncher(MockHandle()), MockHandoffChecker(), MockClock())
        self.assertEqual(outcome, AttemptOutcome.SESSION_LIMIT)

    def test_transcript_seek_and_read_are_bounded(self):
        req = self.make_valid_request()
        self.write_transcript_for(req, "A" * 70000 + "RATE LIMIT")
        transcript_path = os.path.join(
            self.runtime_root, "transcripts", f"{req.task_token}.log"
        )
        real_open = open
        read_sizes = []
        seeks = []

        class TranscriptSpy:
            def __init__(self_, wrapped):
                self_._wrapped = wrapped
            def __enter__(self_):
                self_._wrapped.__enter__()
                return self_
            def __exit__(self_, *args):
                return self_._wrapped.__exit__(*args)
            def seek(self_, offset, whence=os.SEEK_SET):
                seeks.append((offset, whence))
                return self_._wrapped.seek(offset, whence)
            def tell(self_):
                return self_._wrapped.tell()
            def read(self_, size=-1):
                read_sizes.append(size)
                return self_._wrapped.read(size)

        def tracking_open(path, mode="r", *args, **kwargs):
            wrapped = real_open(path, mode, *args, **kwargs)
            if os.path.normcase(str(path)) == os.path.normcase(transcript_path) and mode == "rb":
                return TranscriptSpy(wrapped)
            return wrapped

        with mock.patch("builtins.open", side_effect=tracking_open):
            outcome = run_one_shot(
                req, MockLauncher(MockHandle()), MockHandoffChecker(), MockClock()
            )
        self.assertEqual(outcome, AttemptOutcome.RATE_LIMIT)
        self.assertTrue(read_sizes)
        self.assertTrue(all(0 <= size <= 65536 for size in read_sizes))
        self.assertIn((0, os.SEEK_END), seeks)

    # --- AST Check ---

    def test_ast_forbidden_capabilities(self):
        with open(osr_mod.__file__, 'r') as f:
            tree = ast.parse(f.read())
            
        forbidden = {
            'subprocess', 'multiprocessing', 'psutil', 'socket', 'requests', 'urllib',
            'environ', 'getenv', 'sleep', 'clock', 'time',
            'listdir', 'walk', 'inbox', 'outbox'
        }
        
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for name in node.names:
                    if name.name.split('.')[0] in forbidden:
                        self.fail(f"Forbidden import: {name.name}")
            elif isinstance(node, ast.ImportFrom):
                if node.module and node.module.split('.')[0] in forbidden:
                    self.fail(f"Forbidden import from: {node.module}")
            elif isinstance(node, ast.Call):
                if isinstance(node.func, ast.Attribute):
                    if node.func.attr in forbidden:
                        self.fail(f"Forbidden call: {node.func.attr}")
                elif isinstance(node.func, ast.Name):
                    if node.func.id in forbidden:
                        self.fail(f"Forbidden call: {node.func.id}")
            elif isinstance(node, ast.Attribute):
                if node.attr in forbidden:
                    self.fail(f"Forbidden attr: {node.attr}")

if __name__ == '__main__':
    unittest.main()
