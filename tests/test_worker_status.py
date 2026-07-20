import unittest
import os
import tempfile
import json
import ast
from datetime import datetime, timezone, timedelta, tzinfo
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'agent-control', 'dispatcher')))
import worker_status as ws_mod
from worker_status import (
    WorkerStatus, WorkerPhase, WorkerReason, StatusError,
    serialize_status, parse_status, write_status_atomic, read_status, validate_transition
)

class SecretBearingTzInfo(tzinfo):
    def utcoffset(self, dt):
        raise ValueError("Secret database password: password123")
    def tzname(self, dt):
        return "SecretTZ"
    def dst(self, dt):
        return timedelta(0)

class TestWorkerStatus(unittest.TestCase):
    def setUp(self):
        self.valid_token = "a" * 64
        self.now_utc = datetime(2023, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        self.temp_dir = tempfile.TemporaryDirectory()

    def tearDown(self):
        self.temp_dir.cleanup()

    def get_valid_dict(self):
        return {
            "schema_version": 1,
            "task_token": self.valid_token,
            "task_revision": 1,
            "phase": "RUNNING",
            "reason": "NONE",
            "observed_at_utc": "2023-01-01T12:00:00Z",
            "pid": 123,
            "exit_code": None
        }

    def test_round_trip_every_phase_and_reason(self):
        # RUNNING
        ws = WorkerStatus(self.valid_token, 1, WorkerPhase.RUNNING, WorkerReason.NONE, self.now_utc, 123, None)
        self.assertEqual(ws, parse_status(serialize_status(ws)))

        # HANDOFF_READY
        ws = WorkerStatus(self.valid_token, 1, WorkerPhase.HANDOFF_READY, WorkerReason.NONE, self.now_utc, 123, None)
        self.assertEqual(ws, parse_status(serialize_status(ws)))

        # TERMINAL - SUCCESS
        ws = WorkerStatus(self.valid_token, 1, WorkerPhase.TERMINAL, WorkerReason.SUCCESS, self.now_utc, 123, 0)
        self.assertEqual(ws, parse_status(serialize_status(ws)))

        # TERMINAL - PROCESS_EXIT_NONZERO
        ws = WorkerStatus(self.valid_token, 1, WorkerPhase.TERMINAL, WorkerReason.PROCESS_EXIT_NONZERO, self.now_utc, 123, 1)
        self.assertEqual(ws, parse_status(serialize_status(ws)))

        # TERMINAL - LAUNCH_ERROR (null pid, null exit_code)
        ws = WorkerStatus(self.valid_token, 1, WorkerPhase.TERMINAL, WorkerReason.LAUNCH_ERROR, self.now_utc, None, None)
        self.assertEqual(ws, parse_status(serialize_status(ws)))

        # TERMINAL - others (null exit_code)
        reasons = [WorkerReason.SESSION_LIMIT, WorkerReason.RATE_LIMIT, WorkerReason.TIMEOUT, 
                   WorkerReason.STALE_RUNNING_NO_PROCESS, WorkerReason.PRIOR_ATTEMPT_NO_HANDOFF]
        for r in reasons:
            ws = WorkerStatus(self.valid_token, 1, WorkerPhase.TERMINAL, r, self.now_utc, 123, None)
            self.assertEqual(ws, parse_status(serialize_status(ws)))

    def test_reject_oversized_bytes(self):
        oversized = b' ' * 16385
        with self.assertRaises(StatusError):
            parse_status(oversized)

    def test_reject_invalid_utf8(self):
        with self.assertRaises(StatusError):
            parse_status(b'\xff')

    def test_reject_malformed_json(self):
        with self.assertRaises(StatusError):
            parse_status(b'{')

    def test_reject_duplicate_keys(self):
        with self.assertRaises(StatusError):
            parse_status(b'{"schema_version":1, "schema_version":1}')

    def test_reject_floats_and_constants(self):
        valid_dict = self.get_valid_dict()
        valid_dict["pid"] = 1.5
        with self.assertRaises(StatusError):
            parse_status(json.dumps(valid_dict).encode('utf-8'))
        
        # Test explicit constants in json.loads
        constants = ["NaN", "Infinity", "-Infinity"]
        for c in constants:
            # We can't use json.dumps for NaN since python converts it to NaN without quotes,
            # which json standard does not technically support, but python's json.loads allows it.
            json_str = f'{{"schema_version": 1, "task_token": "{self.valid_token}", "task_revision": 1, "phase": "RUNNING", "reason": "NONE", "observed_at_utc": "2023-01-01T12:00:00Z", "pid": {c}, "exit_code": null}}'
            with self.assertRaises(StatusError):
                parse_status(json_str.encode('utf-8'))

    def test_reject_unknown_missing_keys(self):
        valid_dict = self.get_valid_dict()
        d_miss = valid_dict.copy()
        del d_miss["pid"]
        with self.assertRaises(StatusError):
            parse_status(json.dumps(d_miss).encode('utf-8'))
            
        d_unk = valid_dict.copy()
        d_unk["unknown"] = 1
        with self.assertRaises(StatusError):
            parse_status(json.dumps(d_unk).encode('utf-8'))

    def test_reject_wrong_schema_version_and_bools(self):
        valid_dict = self.get_valid_dict()
        valid_dict["schema_version"] = 2
        with self.assertRaises(StatusError):
            parse_status(json.dumps(valid_dict).encode('utf-8'))
            
        valid_dict["schema_version"] = True
        with self.assertRaises(StatusError):
            parse_status(json.dumps(valid_dict).encode('utf-8'))

    def test_reject_bool_integers_in_init(self):
        with self.assertRaises(StatusError):
            WorkerStatus(self.valid_token, True, WorkerPhase.RUNNING, WorkerReason.NONE, self.now_utc, 123, None)
        with self.assertRaises(StatusError):
            WorkerStatus(self.valid_token, 1, WorkerPhase.RUNNING, WorkerReason.NONE, self.now_utc, True, None)
        with self.assertRaises(StatusError):
            WorkerStatus(self.valid_token, 1, WorkerPhase.TERMINAL, WorkerReason.SUCCESS, self.now_utc, 123, False)

    def test_reject_invalid_tokens(self):
        with self.assertRaises(StatusError):
            WorkerStatus("invalid", 1, WorkerPhase.RUNNING, WorkerReason.NONE, self.now_utc, 123, None)

    def test_reject_naive_timestamps(self):
        with self.assertRaises(StatusError):
            WorkerStatus(self.valid_token, 1, WorkerPhase.RUNNING, WorkerReason.NONE, datetime.now(), 123, None)

    def test_tzinfo_secret_exception(self):
        dt = datetime(2023, 1, 1, 12, 0, 0, tzinfo=SecretBearingTzInfo())
        try:
            WorkerStatus(self.valid_token, 1, WorkerPhase.RUNNING, WorkerReason.NONE, dt, 123, None)
            self.fail("Should have raised StatusError")
        except StatusError as e:
            msg = str(e)
            self.assertNotIn("password123", msg)

    def test_observed_at_utc_canonicalization(self):
        valid_dict = self.get_valid_dict()
        
        # Test Space separator
        valid_dict["observed_at_utc"] = "2023-01-01 12:00:00Z"
        with self.assertRaises(StatusError):
            parse_status(json.dumps(valid_dict).encode('utf-8'))
            
        # Test +00:00 suffix
        valid_dict["observed_at_utc"] = "2023-01-01T12:00:00+00:00"
        with self.assertRaises(StatusError):
            parse_status(json.dumps(valid_dict).encode('utf-8'))
            
        # Test lowercase z
        valid_dict["observed_at_utc"] = "2023-01-01T12:00:00z"
        with self.assertRaises(StatusError):
            parse_status(json.dumps(valid_dict).encode('utf-8'))
            
        # Test redundant zero fractional seconds
        valid_dict["observed_at_utc"] = "2023-01-01T12:00:00.000000Z"
        with self.assertRaises(StatusError):
            parse_status(json.dumps(valid_dict).encode('utf-8'))

    def test_invalid_combinations_table(self):
        # RUNNING missing pid, zero pid, negative pid, bool pid, integer exit_code, bool exit_code, wrong reason
        with self.assertRaises(StatusError): WorkerStatus(self.valid_token, 1, WorkerPhase.RUNNING, WorkerReason.NONE, self.now_utc, None, None)
        with self.assertRaises(StatusError): WorkerStatus(self.valid_token, 1, WorkerPhase.RUNNING, WorkerReason.NONE, self.now_utc, 0, None)
        with self.assertRaises(StatusError): WorkerStatus(self.valid_token, 1, WorkerPhase.RUNNING, WorkerReason.NONE, self.now_utc, -1, None)
        with self.assertRaises(StatusError): WorkerStatus(self.valid_token, 1, WorkerPhase.RUNNING, WorkerReason.NONE, self.now_utc, True, None)
        with self.assertRaises(StatusError): WorkerStatus(self.valid_token, 1, WorkerPhase.RUNNING, WorkerReason.NONE, self.now_utc, 123, 0)
        with self.assertRaises(StatusError): WorkerStatus(self.valid_token, 1, WorkerPhase.RUNNING, WorkerReason.NONE, self.now_utc, 123, False)
        with self.assertRaises(StatusError): WorkerStatus(self.valid_token, 1, WorkerPhase.RUNNING, WorkerReason.SUCCESS, self.now_utc, 123, None)

        # HANDOFF_READY missing pid, zero pid, negative pid, bool pid, integer exit_code, bool exit_code, wrong reason
        with self.assertRaises(StatusError): WorkerStatus(self.valid_token, 1, WorkerPhase.HANDOFF_READY, WorkerReason.NONE, self.now_utc, None, None)
        with self.assertRaises(StatusError): WorkerStatus(self.valid_token, 1, WorkerPhase.HANDOFF_READY, WorkerReason.NONE, self.now_utc, 0, None)
        with self.assertRaises(StatusError): WorkerStatus(self.valid_token, 1, WorkerPhase.HANDOFF_READY, WorkerReason.NONE, self.now_utc, -1, None)
        with self.assertRaises(StatusError): WorkerStatus(self.valid_token, 1, WorkerPhase.HANDOFF_READY, WorkerReason.NONE, self.now_utc, True, None)
        with self.assertRaises(StatusError): WorkerStatus(self.valid_token, 1, WorkerPhase.HANDOFF_READY, WorkerReason.NONE, self.now_utc, 123, 0)
        with self.assertRaises(StatusError): WorkerStatus(self.valid_token, 1, WorkerPhase.HANDOFF_READY, WorkerReason.NONE, self.now_utc, 123, False)
        with self.assertRaises(StatusError): WorkerStatus(self.valid_token, 1, WorkerPhase.HANDOFF_READY, WorkerReason.SUCCESS, self.now_utc, 123, None)

        # TERMINAL with NONE
        with self.assertRaises(StatusError): WorkerStatus(self.valid_token, 1, WorkerPhase.TERMINAL, WorkerReason.NONE, self.now_utc, 123, 0)

        # TERMINAL LAUNCH_ERROR requires null pid and null exit_code
        with self.assertRaises(StatusError): WorkerStatus(self.valid_token, 1, WorkerPhase.TERMINAL, WorkerReason.LAUNCH_ERROR, self.now_utc, 123, None)
        with self.assertRaises(StatusError): WorkerStatus(self.valid_token, 1, WorkerPhase.TERMINAL, WorkerReason.LAUNCH_ERROR, self.now_utc, None, 1)

        # SESSION_LIMIT, RATE_LIMIT, TIMEOUT, STALE_RUNNING_NO_PROCESS, PRIOR_ATTEMPT_NO_HANDOFF require null exit_code
        for r in [WorkerReason.SESSION_LIMIT, WorkerReason.RATE_LIMIT, WorkerReason.TIMEOUT, 
                  WorkerReason.STALE_RUNNING_NO_PROCESS, WorkerReason.PRIOR_ATTEMPT_NO_HANDOFF]:
            with self.assertRaises(StatusError): WorkerStatus(self.valid_token, 1, WorkerPhase.TERMINAL, r, self.now_utc, 123, 1)
            # and invalid pid
            with self.assertRaises(StatusError): WorkerStatus(self.valid_token, 1, WorkerPhase.TERMINAL, r, self.now_utc, 0, None)

        # PROCESS_EXIT_NONZERO with null, zero, and bool exit codes
        with self.assertRaises(StatusError): WorkerStatus(self.valid_token, 1, WorkerPhase.TERMINAL, WorkerReason.PROCESS_EXIT_NONZERO, self.now_utc, 123, None)
        with self.assertRaises(StatusError): WorkerStatus(self.valid_token, 1, WorkerPhase.TERMINAL, WorkerReason.PROCESS_EXIT_NONZERO, self.now_utc, 123, 0)
        with self.assertRaises(StatusError): WorkerStatus(self.valid_token, 1, WorkerPhase.TERMINAL, WorkerReason.PROCESS_EXIT_NONZERO, self.now_utc, 123, True)

        # SUCCESS with null, nonzero, and bool exit codes
        with self.assertRaises(StatusError): WorkerStatus(self.valid_token, 1, WorkerPhase.TERMINAL, WorkerReason.SUCCESS, self.now_utc, 123, None)
        with self.assertRaises(StatusError): WorkerStatus(self.valid_token, 1, WorkerPhase.TERMINAL, WorkerReason.SUCCESS, self.now_utc, 123, 1)
        with self.assertRaises(StatusError): WorkerStatus(self.valid_token, 1, WorkerPhase.TERMINAL, WorkerReason.SUCCESS, self.now_utc, 123, False)

    def test_non_utc_timezone_aware(self):
        dt = datetime(2023, 1, 1, 12, 0, 0, tzinfo=timezone(timedelta(hours=5, minutes=30)))
        with self.assertRaises(StatusError):
            WorkerStatus(self.valid_token, 1, WorkerPhase.RUNNING, WorkerReason.NONE, dt, 123, None)

    def test_sanitized_errors(self):
        token = "b" * 64
        try:
            WorkerStatus(token, 1, WorkerPhase.RUNNING, WorkerReason.NONE, self.now_utc, 123, 0)
            self.fail("Should have raised StatusError")
        except StatusError as e:
            self.assertEqual(str(e), "exit_code must be null for active phase")
            self.assertNotIn(token, str(e))
            self.assertNotIn("0", str(e))

        try:
            read_status("/fake/secret/path.json")
            self.fail("Should have raised StatusError")
        except StatusError as e:
            self.assertEqual(str(e), "file read failed")
            self.assertNotIn("/fake", str(e))

        try:
            WorkerStatus(self.valid_token, 1, WorkerPhase.TERMINAL, WorkerReason.PROCESS_EXIT_NONZERO, self.now_utc, 123, 0)
            self.fail("Should have raised")
        except StatusError as e:
            self.assertEqual(str(e), "PROCESS_EXIT_NONZERO requires non-zero exit_code")

        try:
            dt = datetime(2023, 1, 1, 12, 0, 0, tzinfo=SecretBearingTzInfo())
            WorkerStatus(self.valid_token, 1, WorkerPhase.RUNNING, WorkerReason.NONE, dt, 123, None)
            self.fail("Should have raised")
        except StatusError as e:
            self.assertEqual(str(e), "invalid tzinfo behavior")
            self.assertNotIn("password", str(e))

    def test_transitions(self):
        ws_r = WorkerStatus(self.valid_token, 1, WorkerPhase.RUNNING, WorkerReason.NONE, self.now_utc, 123, None)
        ws_h = WorkerStatus(self.valid_token, 1, WorkerPhase.HANDOFF_READY, WorkerReason.NONE, self.now_utc, 123, None)
        ws_t = WorkerStatus(self.valid_token, 1, WorkerPhase.TERMINAL, WorkerReason.SUCCESS, self.now_utc, 123, 0)
        
        ws_r_diff_token = WorkerStatus("b"*64, 1, WorkerPhase.RUNNING, WorkerReason.NONE, self.now_utc, 123, None)
        ws_r_diff_rev = WorkerStatus(self.valid_token, 2, WorkerPhase.RUNNING, WorkerReason.NONE, self.now_utc, 123, None)
        
        dt_later = self.now_utc + timedelta(seconds=1)
        ws_r_diff_time = WorkerStatus(self.valid_token, 1, WorkerPhase.RUNNING, WorkerReason.NONE, dt_later, 123, None)

        # Idempotent
        validate_transition(ws_r, ws_r)
        
        # Identity mismatch
        with self.assertRaises(StatusError):
            validate_transition(ws_r, ws_r_diff_token)
        with self.assertRaises(StatusError):
            validate_transition(ws_r, ws_r_diff_rev)

        # RUNNING self-transition with changed data
        with self.assertRaises(StatusError):
            validate_transition(ws_r, ws_r_diff_time)

        # Invalid inputs
        with self.assertRaises(StatusError):
            validate_transition(ws_r, "not_worker_status")

        # Valid transitions
        validate_transition(ws_r, ws_h)
        validate_transition(ws_r, ws_t)
        validate_transition(ws_h, ws_t)

        # Forbidden transitions
        with self.assertRaises(StatusError):
            validate_transition(ws_h, ws_r)
        with self.assertRaises(StatusError):
            validate_transition(ws_t, ws_r)
        with self.assertRaises(StatusError):
            validate_transition(ws_t, ws_h)

    def test_atomic_write_read_round_trip_and_exact_bytes(self):
        path = os.path.join(self.temp_dir.name, "status.json")
        ws = WorkerStatus(self.valid_token, 1, WorkerPhase.RUNNING, WorkerReason.NONE, self.now_utc, 123, None)
        
        # Exact bytes assertion
        bytes_out = serialize_status(ws)
        # Expected: deterministic sorted-key compact UTF-8 JSON, no trailing newline
        expected = f'{{"exit_code":null,"observed_at_utc":"2023-01-01T12:00:00Z","phase":"RUNNING","pid":123,"reason":"NONE","schema_version":1,"task_revision":1,"task_token":"{self.valid_token}"}}'.encode('utf-8')
        self.assertEqual(bytes_out, expected)
        
        write_status_atomic(path, ws)
        ws2 = read_status(path)
        self.assertEqual(ws, ws2)

    def test_serialize_rejects_invalid_type(self):
        with self.assertRaises(StatusError):
            serialize_status(self.get_valid_dict())

    def test_read_status_truncates_large_file(self):
        path = os.path.join(self.temp_dir.name, "large.json")
        with open(path, 'wb') as f:
            f.write(b' ' * 17000)
        
        with self.assertRaises(StatusError) as cm:
            read_status(path)
        self.assertTrue("payload too large" in str(cm.exception) or "parse failed" in str(cm.exception) or "malformed" in str(cm.exception))

    def test_read_status_exact_read_size(self):
        import unittest.mock
        
        mock_data = b'{"schema_version": 1, "task_token": "' + self.valid_token.encode('utf-8') + b'", "task_revision": 1, "phase": "RUNNING", "reason": "NONE", "observed_at_utc": "2023-01-01T12:00:00Z", "pid": 123, "exit_code": null}'
        m_open = unittest.mock.mock_open(read_data=mock_data)
        
        with unittest.mock.patch('builtins.open', m_open):
            read_status("dummy_path")
            
        m_open().read.assert_called_once_with(16385)

    def test_os_replace_failure_handling(self):
        import unittest.mock
        path = os.path.join(self.temp_dir.name, "status.json")
        ws = WorkerStatus(self.valid_token, 1, WorkerPhase.RUNNING, WorkerReason.NONE, self.now_utc, 123, None)
        write_status_atomic(path, ws) # Initial successful write
        
        ws_new = WorkerStatus(self.valid_token, 1, WorkerPhase.HANDOFF_READY, WorkerReason.NONE, self.now_utc, 123, None)
        with unittest.mock.patch('os.replace', side_effect=OSError("mocked error")):
            with self.assertRaises(StatusError):
                write_status_atomic(path, ws_new)
                
        # Original should be untouched
        self.assertEqual(ws, read_status(path))
        
        # Temp files should be cleaned up
        files = os.listdir(self.temp_dir.name)
        self.assertEqual(len(files), 1)
        self.assertEqual(files[0], "status.json")

    def test_no_forbidden_capabilities(self):
        with open(ws_mod.__file__, 'r') as f:
            tree = ast.parse(f.read())
            
        forbidden_names = {
            'environ', 'getenv', 'system', 'popen', 'subprocess', 'socket', 
            'requests', 'urllib', 'http', 'gcp', 'google', 'multiprocessing', 
            'sys', 'clock', 'now', 'utcnow', 'time', 'sleep', 'inbox', 'outbox', 
            'psutil', 'os.environ', 'getpid', 'kill', 'killpg', 'waitpid', 
            'wait', 'poll', 'Popen'
        }
        
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for name in node.names:
                    if name.name.split('.')[0] in forbidden_names:
                        self.fail(f"Forbidden import: {name.name}")
            elif isinstance(node, ast.ImportFrom):
                if node.module and node.module.split('.')[0] in forbidden_names:
                    self.fail(f"Forbidden import from: {node.module}")
            elif isinstance(node, ast.Call):
                if isinstance(node.func, ast.Attribute):
                    if node.func.attr in forbidden_names:
                        self.fail(f"Forbidden call: {node.func.attr}")
                elif isinstance(node.func, ast.Name):
                    if node.func.id in forbidden_names:
                        self.fail(f"Forbidden call: {node.func.id}")
            elif isinstance(node, ast.Attribute):
                if node.attr in forbidden_names:
                    self.fail(f"Forbidden attribute: {node.attr}")

if __name__ == '__main__':
    unittest.main()
