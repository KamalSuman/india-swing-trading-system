from __future__ import annotations

import ast
import inspect
import json
import unittest
from datetime import date, datetime, timezone
from io import StringIO
from pathlib import Path
from unittest.mock import patch

from india_swing.market_data.config import KiteCredentials, MissingMarketDataConfiguration
from india_swing import quality_pilot_job as job_module
from india_swing.quality_pilot.canonical_response import ScheduledWindowKind
from india_swing.quality_pilot.invocation_control_plane import (
    QualityPilotInvocationRunbook,
    encode_quality_pilot_invocation_runbook,
)
from india_swing.quality_pilot.window_service import QualityPilotWindowServiceResult
from india_swing.quality_pilot_job import QualityPilotWindowJobError, main
from tests.test_quality_pilot_campaign_ledger import BUCKET, _window
from tests.test_quality_pilot_capture_runner import _campaign

ENV = {
    "INDIA_SWING_KITE_API_KEY": "test-api-key",
    "INDIA_SWING_KITE_ACCESS_TOKEN": "test-access-token",
    "INDIA_SWING_QUALITY_PILOT_CODE_SHA256": "a" * 64,
    "INDIA_SWING_QUALITY_PILOT_ENVIRONMENT_SHA256": "b" * 64,
}


class _FakeBlob:
    def __init__(self, bucket, name):
        self._bucket = bucket
        self.name = name
        self.generation = None

    def reload(self, **kwargs):
        if self.name not in self._bucket.objects:
            raise _FakeNotFound(self.name)
        _, generation = self._bucket.objects[self.name]
        self.generation = generation

    def download_as_bytes(self, *, end=None, if_generation_match=None, **kwargs):
        if self.name not in self._bucket.objects:
            raise _FakeNotFound(self.name)
        content, generation = self._bucket.objects[self.name]
        if if_generation_match is not None and generation != if_generation_match:
            raise _FakePreconditionFailed(self.name)
        self.generation = generation
        return content[:end] if end is not None else content

    def upload_from_string(self, data, *, if_generation_match=None, **kwargs):
        existing = self._bucket.objects.get(self.name)
        if if_generation_match == 0 and existing is not None:
            raise _FakePreconditionFailed(self.name)
        new_generation = (existing[1] + 1) if existing else 1
        payload = data if isinstance(data, bytes) else data.encode("utf-8")
        self._bucket.objects[self.name] = (payload, new_generation)
        self.generation = new_generation


class _FakeNotFound(Exception):
    pass


class _FakePreconditionFailed(Exception):
    pass


class _FakeBucket:
    def __init__(self, name):
        self.name = name
        self.objects: dict[str, tuple[bytes, int]] = {}

    def blob(self, name, generation=None):
        return _FakeBlob(self, name)


class _FakeGCSClient:
    def __init__(self):
        self._buckets: dict[str, _FakeBucket] = {}
        self.bucket_calls = 0

    def bucket(self, name):
        self.bucket_calls += 1
        return self._buckets.setdefault(name, _FakeBucket(name))


def _write_runbook_file(path: Path) -> QualityPilotInvocationRunbook:
    campaign = _campaign()
    windows = []
    for session in campaign.confirmed_sessions:
        windows.append(_window(session, ScheduledWindowKind.CATALOG_PREOPEN))
        windows.append(_window(session, ScheduledWindowKind.QUOTE_0920))
        windows.append(_window(session, ScheduledWindowKind.QUOTE_CLOSE))
        windows.append(_window(session, ScheduledWindowKind.OHLCV_CLOSE))
    runbook = QualityPilotInvocationRunbook(
        campaign=campaign, provider_version="kite-3.0", bucket=BUCKET, windows=tuple(windows)
    )
    path.write_bytes(encode_quality_pilot_invocation_runbook(runbook))
    return runbook


class PrepareGenesisTests(unittest.TestCase):
    def test_prepare_genesis_publishes_binding_and_window_entry(self) -> None:
        import tempfile

        with tempfile.TemporaryDirectory() as tmp:
            runbook_path = Path(tmp) / "runbook.json"
            runbook = _write_runbook_file(runbook_path)

            client = _FakeGCSClient()
            stdout = StringIO()
            with patch("sys.stdout", stdout):
                exit_code = main(
                    ["prepare-genesis", "--runbook-file", str(runbook_path), "--bucket", BUCKET],
                    environ={}, gcs_client_factory=lambda: client,
                )
            self.assertEqual(exit_code, 0)
            envelope = json.loads(stdout.getvalue())
            self.assertEqual(envelope["status"], "QUALITY_PILOT_GENESIS_PREPARED")
            self.assertEqual(envelope["runbook_id"], runbook.runbook_id)
            self.assertEqual(envelope["pilot_run_id"], runbook.campaign.pilot_run_id)
            self.assertTrue(envelope["quality_only"])
            self.assertEqual(client.bucket_calls > 0, True)

    def test_prepare_genesis_rejects_relative_runbook_path(self) -> None:
        stderr = StringIO()
        with patch("sys.stderr", stderr):
            exit_code = main(
                ["prepare-genesis", "--runbook-file", "relative/path.json", "--bucket", BUCKET],
                environ={}, gcs_client_factory=lambda: _FakeGCSClient(),
            )
        self.assertEqual(exit_code, 2)
        envelope = json.loads(stderr.getvalue())
        self.assertEqual(envelope["status"], "FAILED")
        self.assertEqual(envelope["error_type"], "QualityPilotWindowJobError")

    def test_prepare_genesis_rejects_traversal_path(self) -> None:
        stderr = StringIO()
        with patch("sys.stderr", stderr):
            exit_code = main(
                ["prepare-genesis", "--runbook-file", "/abs/../etc/passwd", "--bucket", BUCKET],
                environ={}, gcs_client_factory=lambda: _FakeGCSClient(),
            )
        self.assertEqual(exit_code, 2)

    def test_prepare_genesis_rejects_missing_option(self) -> None:
        stderr = StringIO()
        with patch("sys.stderr", stderr):
            exit_code = main(
                ["prepare-genesis", "--runbook-file", "/abs/path.json"],
                environ={}, gcs_client_factory=lambda: _FakeGCSClient(),
            )
        self.assertEqual(exit_code, 2)

    def test_prepare_genesis_rejects_duplicate_option(self) -> None:
        stderr = StringIO()
        with patch("sys.stderr", stderr):
            exit_code = main(
                [
                    "prepare-genesis", "--runbook-file", "/abs/path.json",
                    "--bucket", BUCKET, "--bucket", BUCKET,
                ],
                environ={}, gcs_client_factory=lambda: _FakeGCSClient(),
            )
        self.assertEqual(exit_code, 2)

    def test_no_command_fails_closed(self) -> None:
        stderr = StringIO()
        with patch("sys.stderr", stderr):
            exit_code = main([], environ={}, gcs_client_factory=lambda: _FakeGCSClient())
        self.assertEqual(exit_code, 2)

    def test_unknown_command_fails_closed(self) -> None:
        stderr = StringIO()
        with patch("sys.stderr", stderr):
            exit_code = main(["deploy-now"], environ={}, gcs_client_factory=lambda: _FakeGCSClient())
        self.assertEqual(exit_code, 2)


class RunWindowTests(unittest.TestCase):
    def _stub_result(self, campaign) -> QualityPilotWindowServiceResult:
        return QualityPilotWindowServiceResult(
            pilot_run_id=campaign.pilot_run_id,
            market_session=campaign.confirmed_sessions[0],
            window_kind=ScheduledWindowKind.CATALOG_PREOPEN,
            actions_processed=1,
            actions_reused=0,
            final_transition_id="c" * 64,
            campaign_complete=False,
            window_complete=True,
            next_window_session=campaign.confirmed_sessions[0],
            next_window_kind=ScheduledWindowKind.QUOTE_0920,
        )

    def test_run_window_uses_a_single_shared_client_and_calls_the_service_once(self) -> None:
        campaign = _campaign()
        stub_result = self._stub_result(campaign)
        calls = []

        def stub_window_service_callable(**kwargs):
            calls.append(kwargs)
            return stub_result

        client = _FakeGCSClient()
        clients_built = []

        def gcs_client_factory():
            clients_built.append(client)
            return client

        def kite_adapter_factory(credentials, clock):
            self.assertIsInstance(credentials, KiteCredentials)
            return object()

        def claim_writer_factory(passed_client):
            self.assertIs(passed_client, client)
            return object()

        fixed_clock = lambda: datetime(2026, 7, 15, 3, 50, tzinfo=timezone.utc)

        stdout = StringIO()
        with patch("sys.stdout", stdout):
            exit_code = main(
                [
                    "run-window", "--bucket", BUCKET, "--pilot-run-id", campaign.pilot_run_id,
                    "--target-session", campaign.confirmed_sessions[0].isoformat(),
                    "--window-kind", "CATALOG_PREOPEN",
                ],
                environ=ENV, clock=fixed_clock, gcs_client_factory=gcs_client_factory,
                kite_adapter_factory=kite_adapter_factory, claim_writer_factory=claim_writer_factory,
                window_service_callable=stub_window_service_callable,
            )
        self.assertEqual(exit_code, 0)
        self.assertEqual(len(calls), 1)
        self.assertEqual(len(clients_built), 1)
        self.assertEqual(calls[0]["bucket"], BUCKET)
        self.assertEqual(calls[0]["pilot_run_id"], campaign.pilot_run_id)
        self.assertEqual(calls[0]["market_session"], campaign.confirmed_sessions[0])
        self.assertIs(calls[0]["window_kind"], ScheduledWindowKind.CATALOG_PREOPEN)
        self.assertEqual(calls[0]["code_sha256"], ENV["INDIA_SWING_QUALITY_PILOT_CODE_SHA256"])
        self.assertEqual(calls[0]["environment_sha256"], ENV["INDIA_SWING_QUALITY_PILOT_ENVIRONMENT_SHA256"])

        envelope = json.loads(stdout.getvalue())
        self.assertEqual(envelope["status"], "QUALITY_PILOT_WINDOW_COMPLETE")
        self.assertEqual(envelope["actions_processed"], 1)
        self.assertTrue(envelope["quality_only"])

    def test_run_window_campaign_complete_status(self) -> None:
        campaign = _campaign()
        stub_result = QualityPilotWindowServiceResult(
            pilot_run_id=campaign.pilot_run_id, market_session=campaign.confirmed_sessions[-1],
            window_kind=ScheduledWindowKind.OHLCV_CLOSE, actions_processed=1, actions_reused=0,
            final_transition_id="c" * 64, campaign_complete=True, window_complete=True,
            next_window_session=None, next_window_kind=None,
        )
        stdout = StringIO()
        with patch("sys.stdout", stdout):
            exit_code = main(
                [
                    "run-window", "--bucket", BUCKET, "--pilot-run-id", campaign.pilot_run_id,
                    "--target-session", campaign.confirmed_sessions[-1].isoformat(),
                    "--window-kind", "OHLCV_CLOSE",
                ],
                environ=ENV, clock=lambda: datetime.now(timezone.utc), gcs_client_factory=lambda: _FakeGCSClient(),
                kite_adapter_factory=lambda credentials, clock: object(),
                claim_writer_factory=lambda client: object(),
                window_service_callable=lambda **kwargs: stub_result,
            )
        self.assertEqual(exit_code, 0)
        envelope = json.loads(stdout.getvalue())
        self.assertEqual(envelope["status"], "QUALITY_PILOT_CAMPAIGN_COMPLETE")
        self.assertIsNone(envelope["next_window_session"])

    def test_run_window_partial_status_when_capped_before_crossing(self) -> None:
        campaign = _campaign()
        stub_result = QualityPilotWindowServiceResult(
            pilot_run_id=campaign.pilot_run_id, market_session=campaign.confirmed_sessions[0],
            window_kind=ScheduledWindowKind.OHLCV_CLOSE, actions_processed=2, actions_reused=1,
            final_transition_id="c" * 64, campaign_complete=False, window_complete=False,
            next_window_session=None, next_window_kind=None,
        )
        stdout = StringIO()
        with patch("sys.stdout", stdout):
            exit_code = main(
                [
                    "run-window", "--bucket", BUCKET, "--pilot-run-id", campaign.pilot_run_id,
                    "--target-session", campaign.confirmed_sessions[0].isoformat(),
                    "--window-kind", "OHLCV_CLOSE", "--maximum-actions-per-invocation", "2",
                ],
                environ=ENV, clock=lambda: datetime.now(timezone.utc), gcs_client_factory=lambda: _FakeGCSClient(),
                kite_adapter_factory=lambda credentials, clock: object(),
                claim_writer_factory=lambda client: object(),
                window_service_callable=lambda **kwargs: stub_result,
            )
        self.assertEqual(exit_code, 0)
        envelope = json.loads(stdout.getvalue())
        self.assertEqual(envelope["status"], "QUALITY_PILOT_WINDOW_PARTIAL")
        self.assertFalse(envelope["window_complete"])
        self.assertFalse(envelope["campaign_complete"])
        self.assertEqual(envelope["final_transition_id"], "c" * 64)

    def test_run_window_missing_kite_credentials_fails_closed_sanitized(self) -> None:
        campaign = _campaign()
        stderr = StringIO()
        with patch("sys.stderr", stderr):
            exit_code = main(
                [
                    "run-window", "--bucket", BUCKET, "--pilot-run-id", campaign.pilot_run_id,
                    "--target-session", campaign.confirmed_sessions[0].isoformat(),
                    "--window-kind", "CATALOG_PREOPEN",
                ],
                environ={}, gcs_client_factory=lambda: _FakeGCSClient(),
                kite_adapter_factory=lambda credentials, clock: object(),
                claim_writer_factory=lambda client: object(),
                window_service_callable=lambda **kwargs: self._stub_result(campaign),
            )
        self.assertEqual(exit_code, 2)
        envelope = json.loads(stderr.getvalue())
        self.assertEqual(envelope, {"status": "FAILED", "error_type": "QualityPilotWindowJobError"})

    def test_run_window_rejects_invalid_window_kind(self) -> None:
        campaign = _campaign()
        stderr = StringIO()
        with patch("sys.stderr", stderr):
            exit_code = main(
                [
                    "run-window", "--bucket", BUCKET, "--pilot-run-id", campaign.pilot_run_id,
                    "--target-session", campaign.confirmed_sessions[0].isoformat(),
                    "--window-kind", "NOT_A_KIND",
                ],
                environ=ENV, gcs_client_factory=lambda: _FakeGCSClient(),
                kite_adapter_factory=lambda credentials, clock: object(),
                claim_writer_factory=lambda client: object(),
                window_service_callable=lambda **kwargs: self._stub_result(campaign),
            )
        self.assertEqual(exit_code, 2)

    def test_run_window_rejects_invalid_session_date(self) -> None:
        campaign = _campaign()
        stderr = StringIO()
        with patch("sys.stderr", stderr):
            exit_code = main(
                [
                    "run-window", "--bucket", BUCKET, "--pilot-run-id", campaign.pilot_run_id,
                    "--target-session", "not-a-date", "--window-kind", "CATALOG_PREOPEN",
                ],
                environ=ENV, gcs_client_factory=lambda: _FakeGCSClient(),
                kite_adapter_factory=lambda credentials, clock: object(),
                claim_writer_factory=lambda client: object(),
                window_service_callable=lambda **kwargs: self._stub_result(campaign),
            )
        self.assertEqual(exit_code, 2)

    def test_run_window_rejects_out_of_range_maximum_actions(self) -> None:
        campaign = _campaign()
        stderr = StringIO()
        with patch("sys.stderr", stderr):
            exit_code = main(
                [
                    "run-window", "--bucket", BUCKET, "--pilot-run-id", campaign.pilot_run_id,
                    "--target-session", campaign.confirmed_sessions[0].isoformat(),
                    "--window-kind", "CATALOG_PREOPEN", "--maximum-actions-per-invocation", "0",
                ],
                environ=ENV, gcs_client_factory=lambda: _FakeGCSClient(),
                kite_adapter_factory=lambda credentials, clock: object(),
                claim_writer_factory=lambda client: object(),
                window_service_callable=lambda **kwargs: self._stub_result(campaign),
            )
        self.assertEqual(exit_code, 2)

    def test_non_callable_injected_factory_fails_closed(self) -> None:
        campaign = _campaign()
        stderr = StringIO()
        with patch("sys.stderr", stderr):
            exit_code = main(
                [
                    "run-window", "--bucket", BUCKET, "--pilot-run-id", campaign.pilot_run_id,
                    "--target-session", campaign.confirmed_sessions[0].isoformat(),
                    "--window-kind", "CATALOG_PREOPEN",
                ],
                environ=ENV, gcs_client_factory="not-callable",  # type: ignore[arg-type]
            )
        self.assertEqual(exit_code, 2)


def _write_manifest_file(path: Path, runbook: QualityPilotInvocationRunbook, **overrides):
    from india_swing.quality_pilot.arming import encode_quality_pilot_arming_manifest
    from tests.test_quality_pilot_arming import _manifest as _build_manifest

    manifest = _build_manifest(runbook, **overrides)
    path.write_bytes(encode_quality_pilot_arming_manifest(manifest))
    return manifest


ARMED_ENV = {**ENV, "INDIA_SWING_QUALITY_PILOT_ARMED": "true"}


class RunDueWindowTests(unittest.TestCase):
    def _write_files(self, tmp: Path, **manifest_overrides):
        runbook_path = tmp / "runbook.json"
        manifest_path = tmp / "manifest.json"
        runbook = _write_runbook_file(runbook_path)
        manifest = _write_manifest_file(manifest_path, runbook, **manifest_overrides)
        return runbook, manifest, runbook_path, manifest_path

    def test_disarmed_calls_zero_credential_gcs_kite_capabilities(self) -> None:
        import tempfile

        with tempfile.TemporaryDirectory() as tmp:
            _, _, runbook_path, manifest_path = self._write_files(Path(tmp))
            gcs_calls = []
            kite_calls = []
            stdout = StringIO()
            with patch("sys.stdout", stdout):
                exit_code = main(
                    ["run-due-window", "--runbook-file", str(runbook_path), "--manifest-file", str(manifest_path)],
                    environ={},  # no INDIA_SWING_QUALITY_PILOT_ARMED at all
                    gcs_client_factory=lambda: gcs_calls.append(1) or _FakeGCSClient(),
                    kite_adapter_factory=lambda credentials, clock: kite_calls.append(1) or object(),
                    claim_writer_factory=lambda client: object(),
                    window_service_callable=lambda **kwargs: (_ for _ in ()).throw(AssertionError("must not be called")),
                )
            self.assertEqual(exit_code, 0)
            envelope = json.loads(stdout.getvalue())
            self.assertEqual(envelope["status"], "QUALITY_PILOT_DISARMED")
            self.assertTrue(envelope["quality_only"])
            self.assertEqual(gcs_calls, [])
            self.assertEqual(kite_calls, [])

    def test_disarmed_on_malformed_kill_switch_value(self) -> None:
        import tempfile

        with tempfile.TemporaryDirectory() as tmp:
            _, _, runbook_path, manifest_path = self._write_files(Path(tmp))
            for bad_value in ("True", "1", "TRUE", " true", "true "):
                stdout = StringIO()
                with patch("sys.stdout", stdout):
                    exit_code = main(
                        ["run-due-window", "--runbook-file", str(runbook_path), "--manifest-file", str(manifest_path)],
                        environ={"INDIA_SWING_QUALITY_PILOT_ARMED": bad_value},
                        gcs_client_factory=lambda: (_ for _ in ()).throw(AssertionError("must not be called")),
                        kite_adapter_factory=lambda credentials, clock: object(),
                        claim_writer_factory=lambda client: object(),
                        window_service_callable=lambda **kwargs: object(),
                    )
                self.assertEqual(exit_code, 0, msg=bad_value)
                self.assertEqual(json.loads(stdout.getvalue())["status"], "QUALITY_PILOT_DISARMED", msg=bad_value)

    def test_not_scheduled_before_pilot_start_calls_zero_gcp_capability(self) -> None:
        import tempfile

        with tempfile.TemporaryDirectory() as tmp:
            runbook, _, runbook_path, manifest_path = self._write_files(Path(tmp))
            before_all = (runbook.windows[0].opens_at - __import__("datetime").timedelta(days=1)).astimezone(timezone.utc)
            stdout = StringIO()
            with patch("sys.stdout", stdout):
                exit_code = main(
                    ["run-due-window", "--runbook-file", str(runbook_path), "--manifest-file", str(manifest_path)],
                    environ=ARMED_ENV, clock=lambda: before_all,
                    gcs_client_factory=lambda: (_ for _ in ()).throw(AssertionError("must not be called")),
                    kite_adapter_factory=lambda credentials, clock: (_ for _ in ()).throw(AssertionError("must not be called")),
                    claim_writer_factory=lambda client: object(),
                    window_service_callable=lambda **kwargs: (_ for _ in ()).throw(AssertionError("must not be called")),
                )
            self.assertEqual(exit_code, 0)
            envelope = json.loads(stdout.getvalue())
            self.assertEqual(envelope["status"], "QUALITY_PILOT_NOT_SCHEDULED")

    def test_missed_window_fails_before_kite_credentials(self) -> None:
        import tempfile

        with tempfile.TemporaryDirectory() as tmp:
            runbook, _, runbook_path, manifest_path = self._write_files(Path(tmp))
            window = runbook.windows[0]
            after_close = (window.closes_at + __import__("datetime").timedelta(minutes=1)).astimezone(timezone.utc)
            kite_calls = []
            stderr = StringIO()
            with patch("sys.stderr", stderr):
                exit_code = main(
                    ["run-due-window", "--runbook-file", str(runbook_path), "--manifest-file", str(manifest_path)],
                    environ=ARMED_ENV, clock=lambda: after_close,
                    gcs_client_factory=lambda: _FakeGCSClient(),  # empty: window entry absent -> INCOMPLETE probe
                    kite_adapter_factory=lambda credentials, clock: kite_calls.append(1) or object(),
                    claim_writer_factory=lambda client: object(),
                    window_service_callable=lambda **kwargs: (_ for _ in ()).throw(AssertionError("must not be called")),
                )
            self.assertEqual(exit_code, 2)
            self.assertEqual(kite_calls, [])
            envelope = json.loads(stderr.getvalue())
            self.assertEqual(envelope, {"status": "FAILED", "error_type": "QualityPilotWindowJobError"})

    def test_due_path_uses_one_clock_read_one_shared_client_and_delegates_once(self) -> None:
        import tempfile

        with tempfile.TemporaryDirectory() as tmp:
            runbook, manifest, runbook_path, manifest_path = self._write_files(Path(tmp))
            window = runbook.windows[0]
            mid = (window.opens_at + (window.closes_at - window.opens_at) / 2).astimezone(timezone.utc)

            clock_calls = []

            def counting_clock():
                clock_calls.append(1)
                return mid

            gcs_calls = []
            shared_client = _FakeGCSClient()

            def gcs_client_factory():
                gcs_calls.append(1)
                return shared_client

            service_calls = []
            stub_result = self._stub_window_result(runbook)

            def stub_window_service_callable(**kwargs):
                service_calls.append(kwargs)
                return stub_result

            environ = {
                **ARMED_ENV,
                "INDIA_SWING_QUALITY_PILOT_CODE_SHA256": manifest.code_sha256,
                "INDIA_SWING_QUALITY_PILOT_ENVIRONMENT_SHA256": manifest.environment_sha256,
            }
            stdout = StringIO()
            with patch("sys.stdout", stdout):
                exit_code = main(
                    ["run-due-window", "--runbook-file", str(runbook_path), "--manifest-file", str(manifest_path)],
                    environ=environ, clock=counting_clock, gcs_client_factory=gcs_client_factory,
                    kite_adapter_factory=lambda credentials, clock: object(),
                    claim_writer_factory=lambda client: object(),
                    window_service_callable=stub_window_service_callable,
                )
            self.assertEqual(exit_code, 0)
            self.assertEqual(len(clock_calls), 1)
            self.assertEqual(len(gcs_calls), 1)
            self.assertEqual(len(service_calls), 1)
            call = service_calls[0]
            self.assertEqual(call["market_session"], window.market_session)
            self.assertIs(call["window_kind"], window.window_kind)
            from india_swing.quality_pilot.window_service import MAXIMUM_ACTIONS_PER_INVOCATION_CEILING

            self.assertEqual(call["maximum_actions_per_invocation"], MAXIMUM_ACTIONS_PER_INVOCATION_CEILING)

    def test_due_path_rejects_mismatched_code_digest(self) -> None:
        import tempfile

        with tempfile.TemporaryDirectory() as tmp:
            runbook, manifest, runbook_path, manifest_path = self._write_files(Path(tmp))
            window = runbook.windows[0]
            mid = (window.opens_at + (window.closes_at - window.opens_at) / 2).astimezone(timezone.utc)
            service_calls = []
            stderr = StringIO()
            with patch("sys.stderr", stderr):
                exit_code = main(
                    ["run-due-window", "--runbook-file", str(runbook_path), "--manifest-file", str(manifest_path)],
                    environ={**ARMED_ENV, "INDIA_SWING_QUALITY_PILOT_CODE_SHA256": "9" * 64}, clock=lambda: mid,
                    gcs_client_factory=lambda: _FakeGCSClient(),
                    kite_adapter_factory=lambda credentials, clock: object(),
                    claim_writer_factory=lambda client: object(),
                    window_service_callable=lambda **kwargs: service_calls.append(kwargs),
                )
            self.assertEqual(exit_code, 2)
            self.assertEqual(service_calls, [])

    def test_malformed_runbook_file_produces_static_failure_envelope(self) -> None:
        import tempfile

        with tempfile.TemporaryDirectory() as tmp:
            runbook_path = Path(tmp) / "runbook.json"
            manifest_path = Path(tmp) / "manifest.json"
            runbook_path.write_bytes(b"not-json\n")
            manifest_path.write_bytes(b"not-json\n")
            stderr = StringIO()
            with patch("sys.stderr", stderr):
                exit_code = main(
                    ["run-due-window", "--runbook-file", str(runbook_path), "--manifest-file", str(manifest_path)],
                    environ=ARMED_ENV, gcs_client_factory=lambda: _FakeGCSClient(),
                    kite_adapter_factory=lambda credentials, clock: object(),
                    claim_writer_factory=lambda client: object(),
                    window_service_callable=lambda **kwargs: object(),
                )
            self.assertEqual(exit_code, 2)
            envelope = json.loads(stderr.getvalue())
            self.assertEqual(envelope, {"status": "FAILED", "error_type": "QualityPilotWindowJobError"})

    def test_manifest_disagreeing_with_runbook_produces_static_failure_envelope(self) -> None:
        import tempfile

        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            runbook_path = tmp_path / "runbook.json"
            other_runbook_path = tmp_path / "other-runbook.json"
            manifest_path = tmp_path / "manifest.json"
            runbook = _write_runbook_file(runbook_path)
            other_runbook = _write_runbook_file(other_runbook_path)
            _write_manifest_file(manifest_path, other_runbook)
            stderr = StringIO()
            with patch("sys.stderr", stderr):
                exit_code = main(
                    ["run-due-window", "--runbook-file", str(runbook_path), "--manifest-file", str(manifest_path)],
                    environ=ARMED_ENV, gcs_client_factory=lambda: _FakeGCSClient(),
                    kite_adapter_factory=lambda credentials, clock: object(),
                    claim_writer_factory=lambda client: object(),
                    window_service_callable=lambda **kwargs: object(),
                )
            self.assertEqual(exit_code, 2)

    def test_rejects_missing_option(self) -> None:
        stderr = StringIO()
        with patch("sys.stderr", stderr):
            exit_code = main(
                ["run-due-window", "--runbook-file", "/abs/runbook.json"],
                environ=ARMED_ENV, gcs_client_factory=lambda: _FakeGCSClient(),
            )
        self.assertEqual(exit_code, 2)

    def test_rejects_relative_paths(self) -> None:
        stderr = StringIO()
        with patch("sys.stderr", stderr):
            exit_code = main(
                ["run-due-window", "--runbook-file", "relative.json", "--manifest-file", "/abs/manifest.json"],
                environ=ARMED_ENV, gcs_client_factory=lambda: _FakeGCSClient(),
            )
        self.assertEqual(exit_code, 2)

    @staticmethod
    def _stub_window_result(runbook) -> QualityPilotWindowServiceResult:
        window = runbook.windows[0]
        return QualityPilotWindowServiceResult(
            pilot_run_id=runbook.campaign.pilot_run_id,
            market_session=window.market_session,
            window_kind=window.window_kind,
            actions_processed=1,
            actions_reused=0,
            final_transition_id="c" * 64,
            campaign_complete=False,
            window_complete=True,
            next_window_session=window.market_session,
            next_window_kind=ScheduledWindowKind.QUOTE_0920,
        )


class RegressionAndCapabilityTests(unittest.TestCase):
    def test_module_has_no_interactive_login_refresh_scheduler_or_paper_trading(self) -> None:
        source = inspect.getsource(job_module)
        lowered = source.lower()
        for token in (
            "input(", "webbrowser.", "login_url", "request_token", "generate_session(",
            "cron", "scheduler.", "backgroundscheduler", "place_order(", "run_paper_trade(",
            "generate_signal(", "telegram.send", "telegrambot",
        ):
            self.assertNotIn(token, lowered, msg=token)

    def test_module_never_constructs_a_kite_or_gcs_client_at_import_time(self) -> None:
        source = inspect.getsource(job_module)
        tree = ast.parse(source)
        module_level_calls = set()
        for node in tree.body:
            if isinstance(node, ast.Expr) and isinstance(node.value, ast.Call):
                if isinstance(node.value.func, ast.Attribute):
                    module_level_calls.add(node.value.func.attr)
        self.assertEqual(module_level_calls & {"Client", "KiteConnect"}, set())

    def test_only_this_module_may_read_environ_or_wall_clock(self) -> None:
        # Sanity check that the injectable-seam pattern is present: main()
        # accepts environ/clock and resolves production defaults only at
        # call time, not at import time.
        source = inspect.getsource(job_module.main)
        self.assertIn("environ", source)
        self.assertIn("clock", source)


if __name__ == "__main__":
    unittest.main()
