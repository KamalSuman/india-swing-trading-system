from __future__ import annotations

import hashlib
import io
import json
import shutil
import sys
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from datetime import datetime, timezone
from pathlib import Path

from india_swing.notifications import TelegramDeliveryReceipt, TelegramDeliveryRequest
from india_swing.promoted_operational_hydrated_cloud_control import (
    PromotedOperationalHydratedCloudLaunch,
    encode_promoted_operational_hydrated_cloud_launch,
)
from india_swing.promoted_operational_input_gcs import (
    PromotedOperationalInputRestoreRequest,
)
from india_swing.promoted_operational_persistence import (
    LocalPromotedOperationalAdvisoryOutbox,
    LocalPromotedOperationalTerminalStore,
    build_promoted_operational_advisory,
    build_promoted_operational_terminal_record,
)
from india_swing.promoted_paper_pilot_job import main
from india_swing.promoted_paper_pilot_notification import (
    CompletedPromotedPaperPilotNotification,
    PromotedPaperPilotNotificationClaim,
    PromotedPaperPilotNotificationReceipt,
    build_promoted_paper_pilot_message,
)

from tests import test_promoted_operational_persistence as _persistence_tests


_NOW = datetime(2026, 8, 11, 12, 0, tzinfo=timezone.utc)


def _terminal_and_advisory():
    result = _persistence_tests._complete_no_trade_result()
    advisory = build_promoted_operational_advisory(result)
    terminal = build_promoted_operational_terminal_record(
        result, advisory, None
    )
    return terminal, advisory


def _launch(terminal) -> PromotedOperationalHydratedCloudLaunch:
    assembly_id = "a" * 64
    snapshot_id = "b" * 64
    request = PromotedOperationalInputRestoreRequest(
        bucket="paper-pilot-state-123",
        manifest_object_name=(
            "promoted-operational-input/v1/"
            f"{terminal.target_session.isoformat()}/{assembly_id}/"
            f"manifests/{snapshot_id}.json"
        ),
        generation=3,
        expected_sha256="c" * 64,
        expected_snapshot_id=snapshot_id,
        expected_assembly_spec_id=assembly_id,
        target_session=terminal.target_session,
    )
    return PromotedOperationalHydratedCloudLaunch(
        expected_assembly_spec_id=assembly_id,
        expected_operational_run_spec_id=terminal.spec_id,
        target_session=terminal.target_session,
        state_bucket=request.bucket,
        input_restore=request,
        prior_state_restore=None,
    )


def _hydrated_envelope(launch, terminal, **overrides) -> dict[str, object]:
    publication_id = "d" * 64
    body: dict[str, object] = {
        "action": terminal.action.value,
        "advisory_id": terminal.advisory_id,
        "assembly_spec_id": launch.expected_assembly_spec_id,
        "binding_generation": 4,
        "binding_id": "e" * 64,
        "cloud_control_id": "f" * 64,
        "execution_eligible": False,
        "failure_codes": list(terminal.failure_codes),
        "inner_status": "PROMOTED_OPERATIONAL_JOB_COMPLETE",
        "input_manifest_byte_count": 500,
        "input_manifest_generation": launch.input_restore.generation,
        "input_manifest_object_name": launch.input_restore.manifest_object_name,
        "input_manifest_sha256": launch.input_restore.expected_sha256,
        "input_snapshot_id": launch.input_restore.expected_snapshot_id,
        "launch_id": launch.launch_id,
        "notification_eligible": False,
        "operational_run_spec_id": terminal.spec_id,
        "paper_only": True,
        "preparation_id": terminal.preparation_id,
        "reused_existing_terminal": False,
        "runtime_job_spec_id": "1" * 64,
        "state_manifest_byte_count": 900,
        "state_manifest_generation": 5,
        "state_manifest_object_name": (
            "promoted-operational-state/v1/"
            f"{terminal.target_session.isoformat()}/{terminal.spec_id}/"
            f"manifests/{publication_id}.json"
        ),
        "state_manifest_sha256": "2" * 64,
        "state_publication_id": publication_id,
        "status": "PROMOTED_OPERATIONAL_HYDRATED_CLOUD_JOB_COMPLETE",
        "target_session": terminal.target_session.isoformat(),
        "terminal_id": terminal.terminal_id,
        "terminal_status": terminal.status.value,
    }
    body.update(overrides)
    return body


class _HydratedMain:
    def __init__(self, parent: Path, terminal, advisory, envelope) -> None:
        self.parent = parent
        self.terminal = terminal
        self.advisory = advisory
        self.envelope = envelope
        self.calls = []

    def __call__(self, argv, **kwargs):
        self.calls.append((list(argv), dict(kwargs)))
        LocalPromotedOperationalTerminalStore(
            self.parent / "state" / "terminal"
        ).put(self.terminal)
        LocalPromotedOperationalAdvisoryOutbox(
            self.parent / "state" / "advisory"
        ).put(self.advisory)
        print(
            json.dumps(
                self.envelope,
                separators=(",", ":"),
                sort_keys=True,
            )
        )
        return 0


class _Notification:
    def __init__(self) -> None:
        self.calls = []

    def __call__(self, **kwargs):
        self.calls.append(kwargs)
        terminal = kwargs["terminal"]
        advisory = kwargs["advisory"]
        config = kwargs["config"]
        text = build_promoted_paper_pilot_message(
            advisory=advisory,
            terminal=terminal,
            state_publication_id=kwargs["state_publication_id"],
        )
        request = TelegramDeliveryRequest(
            delivery_key=terminal.terminal_id,
            text=text,
            message_sha256=hashlib.sha256(text.encode()).hexdigest(),
        )
        claim = PromotedPaperPilotNotificationClaim(
            target_session=terminal.target_session,
            operational_run_spec_id=terminal.spec_id,
            terminal_id=terminal.terminal_id,
            advisory_id=advisory.advisory_id,
            state_publication_id=kwargs["state_publication_id"],
            state_manifest_object_name=kwargs["state_manifest_object_name"],
            state_manifest_generation=kwargs["state_manifest_generation"],
            state_manifest_sha256=kwargs["state_manifest_sha256"],
            request_id=request.request_id,
            message_sha256=request.message_sha256,
            chat_binding_id=config.chat_binding_id,
        )
        telegram = TelegramDeliveryReceipt(
            request_id=request.request_id,
            delivery_key=request.delivery_key,
            message_sha256=request.message_sha256,
            chat_binding_id=config.chat_binding_id,
            telegram_message_id=91,
            delivered_at=_NOW,
        )
        receipt = PromotedPaperPilotNotificationReceipt(
            claim_id=claim.claim_id,
            terminal_id=terminal.terminal_id,
            state_publication_id=kwargs["state_publication_id"],
            telegram_receipt=telegram,
        )
        return CompletedPromotedPaperPilotNotification(
            claim=claim, receipt=receipt, replayed=False
        )


class _Transport:
    def post_json(self, **_kwargs):
        raise AssertionError("injected notification owns delivery")


class _ReplacingHydratedMain(_HydratedMain):
    def __call__(self, argv, **kwargs):
        shutil.rmtree(self.parent)
        self.parent.mkdir()
        return super().__call__(argv, **kwargs)


class PromotedPaperPilotJobTests(unittest.TestCase):
    def _run(self, argv, **kwargs):
        stdout = io.StringIO()
        stderr = io.StringIO()
        with redirect_stdout(stdout), redirect_stderr(stderr):
            code = main(argv, **kwargs)
        return code, stdout.getvalue(), stderr.getvalue()

    def test_complete_job_reuses_one_gcs_client_and_notifies_after_inner_success(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            parent = root / "runtime"
            parent.mkdir()
            terminal, advisory = _terminal_and_advisory()
            launch = _launch(terminal)
            launch_file = root / "launch.json"
            launch_file.write_bytes(
                encode_promoted_operational_hydrated_cloud_launch(launch)
            )
            hydrated = _HydratedMain(
                parent,
                terminal,
                advisory,
                _hydrated_envelope(launch, terminal),
            )
            notification = _Notification()
            client = object()
            client_calls = []

            code, stdout, stderr = self._run(
                ["--launch-file", str(launch_file)],
                environ={
                    "INDIA_SWING_TELEGRAM_BOT_TOKEN": "12345:" + "a" * 24,
                    "INDIA_SWING_TELEGRAM_CHAT_ID": "123456",
                    "INDIA_SWING_PAPER_PILOT_STATE_BUCKET": launch.state_bucket,
                },
                runtime_parent=parent,
                clock=lambda: _NOW,
                gcs_client_factory=lambda: client_calls.append(1) or client,
                hydrated_job_main=hydrated,
                telegram_transport=_Transport(),
                notification_callable=notification,
            )
        self.assertEqual(code, 0)
        self.assertEqual(stderr, "")
        body = json.loads(stdout)
        self.assertEqual(body["status"], "PROMOTED_PAPER_PILOT_JOB_COMPLETE")
        self.assertEqual(
            body["inner_status"],
            "PROMOTED_OPERATIONAL_HYDRATED_CLOUD_JOB_COMPLETE",
        )
        self.assertEqual(client_calls, [1])
        self.assertEqual(len(hydrated.calls), 1)
        self.assertEqual(
            hydrated.calls[0][1]["gcs_client_factory"](), client
        )
        self.assertEqual(len(notification.calls), 1)
        self.assertEqual(
            notification.calls[0]["state_publication_id"], "d" * 64
        )

    def test_tampered_inner_envelope_never_reaches_notification(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            parent = root / "runtime"
            parent.mkdir()
            terminal, advisory = _terminal_and_advisory()
            launch = _launch(terminal)
            launch_file = root / "launch.json"
            launch_file.write_bytes(
                encode_promoted_operational_hydrated_cloud_launch(launch)
            )
            hydrated = _HydratedMain(
                parent,
                terminal,
                advisory,
                _hydrated_envelope(launch, terminal, advisory_id="9" * 64),
            )
            notification = _Notification()
            code, stdout, stderr = self._run(
                ["--launch-file", str(launch_file)],
                environ={
                    "INDIA_SWING_TELEGRAM_BOT_TOKEN": "12345:" + "a" * 24,
                    "INDIA_SWING_TELEGRAM_CHAT_ID": "123456",
                    "INDIA_SWING_PAPER_PILOT_STATE_BUCKET": launch.state_bucket,
                },
                runtime_parent=parent,
                gcs_client_factory=lambda: object(),
                hydrated_job_main=hydrated,
                telegram_transport=_Transport(),
                notification_callable=notification,
            )
        self.assertEqual(code, 2)
        self.assertEqual(stdout, "")
        self.assertEqual(
            json.loads(stderr),
            {"error_type": "PromotedPaperPilotJobError", "status": "FAILED"},
        )
        self.assertEqual(notification.calls, [])

    def test_foreign_failure_code_and_manifest_path_are_rejected(self) -> None:
        for overrides in (
            {"failure_codes": ["INVENTED_FAILURE"], "terminal_status": "FAILED"},
            {
                "state_manifest_object_name": (
                    "promoted-operational-state/v1/1999-01-01/"
                    + "7" * 64
                    + "/manifests/"
                    + "d" * 64
                    + ".json"
                )
            },
        ):
            with self.subTest(overrides=overrides), tempfile.TemporaryDirectory() as directory:
                root = Path(directory).resolve()
                parent = root / "runtime"
                parent.mkdir()
                terminal, advisory = _terminal_and_advisory()
                launch = _launch(terminal)
                launch_file = root / "launch.json"
                launch_file.write_bytes(
                    encode_promoted_operational_hydrated_cloud_launch(launch)
                )
                hydrated = _HydratedMain(
                    parent,
                    terminal,
                    advisory,
                    _hydrated_envelope(launch, terminal, **overrides),
                )
                notification = _Notification()
                code, stdout, _stderr = self._run(
                    ["--launch-file", str(launch_file)],
                    environ={
                        "INDIA_SWING_TELEGRAM_BOT_TOKEN": "12345:" + "a" * 24,
                        "INDIA_SWING_TELEGRAM_CHAT_ID": "123456",
                        "INDIA_SWING_PAPER_PILOT_STATE_BUCKET": launch.state_bucket,
                    },
                    runtime_parent=parent,
                    gcs_client_factory=lambda: object(),
                    hydrated_job_main=hydrated,
                    telegram_transport=_Transport(),
                    notification_callable=notification,
                )
            self.assertEqual(code, 2)
            self.assertEqual(stdout, "")
            self.assertEqual(notification.calls, [])

    def test_runtime_parent_replacement_after_inner_run_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            parent = root / "runtime"
            parent.mkdir()
            terminal, advisory = _terminal_and_advisory()
            launch = _launch(terminal)
            launch_file = root / "launch.json"
            launch_file.write_bytes(
                encode_promoted_operational_hydrated_cloud_launch(launch)
            )
            hydrated = _ReplacingHydratedMain(
                parent,
                terminal,
                advisory,
                _hydrated_envelope(launch, terminal),
            )
            notification = _Notification()
            code, stdout, _stderr = self._run(
                ["--launch-file", str(launch_file)],
                environ={
                    "INDIA_SWING_TELEGRAM_BOT_TOKEN": "12345:" + "a" * 24,
                    "INDIA_SWING_TELEGRAM_CHAT_ID": "123456",
                    "INDIA_SWING_PAPER_PILOT_STATE_BUCKET": launch.state_bucket,
                },
                runtime_parent=parent,
                gcs_client_factory=lambda: object(),
                hydrated_job_main=hydrated,
                telegram_transport=_Transport(),
                notification_callable=notification,
            )
        self.assertEqual(code, 2)
        self.assertEqual(stdout, "")
        self.assertEqual(notification.calls, [])

    def test_missing_telegram_configuration_fails_before_gcs_or_inner_job(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            terminal, _advisory = _terminal_and_advisory()
            launch_file = root / "launch.json"
            launch_file.write_bytes(
                encode_promoted_operational_hydrated_cloud_launch(
                    _launch(terminal)
                )
            )
            calls = []
            code, stdout, stderr = self._run(
                ["--launch-file", str(launch_file)],
                environ={},
                gcs_client_factory=lambda: calls.append("gcs"),
                hydrated_job_main=lambda *_a, **_k: calls.append("inner"),
            )
        self.assertEqual(code, 2)
        self.assertEqual(stdout, "")
        self.assertEqual(calls, [])
        self.assertEqual(json.loads(stderr)["status"], "FAILED")

    def test_deployment_bucket_mismatch_fails_before_gcs_or_inner_job(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            terminal, _advisory = _terminal_and_advisory()
            launch_file = root / "launch.json"
            launch_file.write_bytes(
                encode_promoted_operational_hydrated_cloud_launch(
                    _launch(terminal)
                )
            )
            calls = []
            code, stdout, stderr = self._run(
                ["--launch-file", str(launch_file)],
                environ={
                    "INDIA_SWING_TELEGRAM_BOT_TOKEN": "12345:" + "a" * 24,
                    "INDIA_SWING_TELEGRAM_CHAT_ID": "123456",
                    "INDIA_SWING_PAPER_PILOT_STATE_BUCKET": "foreign-bucket-123",
                },
                gcs_client_factory=lambda: calls.append("gcs"),
                hydrated_job_main=lambda *_a, **_k: calls.append("inner"),
            )
        self.assertEqual(code, 2)
        self.assertEqual(stdout, "")
        self.assertEqual(calls, [])
        self.assertEqual(json.loads(stderr)["status"], "FAILED")

    def test_invalid_arguments_emit_only_sanitized_failure(self) -> None:
        code, stdout, stderr = self._run(
            ["--launch-file", "relative-secret-launch.json"]
        )
        self.assertEqual(code, 2)
        self.assertEqual(stdout, "")
        self.assertEqual(
            stderr,
            '{"error_type":"PromotedPaperPilotJobError","status":"FAILED"}\n',
        )


if __name__ == "__main__":
    unittest.main()
