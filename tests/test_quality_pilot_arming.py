from __future__ import annotations

import ast
import inspect
import unittest
from datetime import date, datetime, time, timedelta, timezone, tzinfo

from india_swing.quality_pilot import arming as arming_module
from india_swing.quality_pilot.arming import (
    QualityPilotArmingError,
    QualityPilotArmingManifest,
    QualityPilotArmingSchedule,
    QualityPilotArmingScheduleLane,
    QualityPilotArmingSecretKind,
    QualityPilotArmingSecretReference,
    QualityPilotDueWindowStatus,
    QualityPilotOrderedCompletionProof,
    QualityPilotRunbookDraft,
    QualityPilotWindowCompletionEvidence,
    QualityPilotWindowCompletionProbeResult,
    QualityPilotWindowPosture,
    assess_quality_pilot_window_posture,
    compile_quality_pilot_invocation_runbook,
    decode_quality_pilot_arming_manifest,
    decode_quality_pilot_runbook_draft,
    encode_quality_pilot_arming_manifest,
    encode_quality_pilot_runbook_draft,
    quality_pilot_window_completion_probe_targets,
    select_due_quality_pilot_window,
)
from india_swing.quality_pilot.canonical_response import PILOT_PROTOCOL_SHA256, ScheduledWindowKind
from tests.test_quality_pilot_campaign_ledger import BUCKET, _window
from tests.test_quality_pilot_capture_runner import _campaign

IMAGE_REFERENCE = "asia-south1-docker.pkg.dev/proj/repo/image@sha256:" + "a" * 64
PROJECT_ID = "india-swing-quality"
REGION = "asia-south1"
JOB_NAME = "india-swing-quality-pilot"
RUNTIME_SA = "qp-runtime@india-swing-quality.iam.gserviceaccount.com"
SCHEDULER_SA = "qp-scheduler@india-swing-quality.iam.gserviceaccount.com"

_SAFE_CRON = {
    QualityPilotArmingScheduleLane.CATALOG_PREOPEN: "50 8 * * 1-5",
    QualityPilotArmingScheduleLane.QUOTE_0920: "20 9 * * 1-5",
    QualityPilotArmingScheduleLane.QUOTE_CLOSE: "45 15 * * 1-5",
    QualityPilotArmingScheduleLane.OHLCV_CLOSE: "20 16 * * 1-5",
}


def _all_windows(campaign):
    windows = []
    for session in campaign.confirmed_sessions:
        windows.append(_window(session, ScheduledWindowKind.CATALOG_PREOPEN))
        windows.append(_window(session, ScheduledWindowKind.QUOTE_0920))
        windows.append(_window(session, ScheduledWindowKind.QUOTE_CLOSE))
        windows.append(_window(session, ScheduledWindowKind.OHLCV_CLOSE))
    return windows


def _draft(**overrides):
    campaign = overrides.pop("campaign", None) or _campaign()
    windows = overrides.pop("windows", None) or _all_windows(campaign)
    fields = dict(
        pilot_run_id=campaign.pilot_run_id,
        protocol_sha256=campaign.protocol_sha256,
        confirmed_sessions=campaign.confirmed_sessions,
        calendar_decision_ids=campaign.calendar_decision_ids,
        provider_version="kite-3.0",
        bucket=BUCKET,
        window_timestamps=tuple((w.opens_at, w.closes_at) for w in windows),
    )
    fields.update(overrides)
    return QualityPilotRunbookDraft(**fields)


def _runbook(**overrides):
    draft = _draft(**overrides)
    runbook, encoded = compile_quality_pilot_invocation_runbook(draft)
    return runbook, encoded


def _manifest(runbook, **overrides):
    fields = dict(
        runbook=runbook,
        image_reference=IMAGE_REFERENCE,
        code_sha256="b" * 64,
        environment_sha256="c" * 64,
        gcp_project_id=PROJECT_ID,
        gcp_region=REGION,
        gcp_job_name=JOB_NAME,
        runtime_service_account_email=RUNTIME_SA,
        scheduler_service_account_email=SCHEDULER_SA,
        schedules=tuple(
            QualityPilotArmingSchedule(lane, cron) for lane, cron in _SAFE_CRON.items()
        ),
        secret_references=(
            QualityPilotArmingSecretReference(QualityPilotArmingSecretKind.KITE_API_KEY, "kite-api-key", "3"),
            QualityPilotArmingSecretReference(QualityPilotArmingSecretKind.KITE_ACCESS_TOKEN, "kite-access-token", "12"),
            QualityPilotArmingSecretReference(QualityPilotArmingSecretKind.RUNBOOK, "quality-pilot-runbook", "1"),
        ),
        timeout_seconds=600,
    )
    fields.update(overrides)
    return QualityPilotArmingManifest(**fields)


class RunbookDraftAndCompilerTests(unittest.TestCase):
    def test_happy_path_compiles_exactly_20_sessions_80_windows(self) -> None:
        runbook, encoded = _runbook()
        self.assertEqual(len(runbook.campaign.confirmed_sessions), 20)
        self.assertEqual(len(runbook.windows), 80)
        runbook.verify_content_identity()
        self.assertTrue(encoded.endswith(b"\n"))

    def test_compiled_encoding_is_byte_equal_to_accepted_encoder(self) -> None:
        from india_swing.quality_pilot.invocation_control_plane import encode_quality_pilot_invocation_runbook

        runbook, encoded = _runbook()
        self.assertEqual(encode_quality_pilot_invocation_runbook(runbook), encoded)

    def test_compiled_runbook_round_trips_through_decode(self) -> None:
        from india_swing.quality_pilot.invocation_control_plane import decode_quality_pilot_invocation_runbook

        runbook, encoded = _runbook()
        reloaded = decode_quality_pilot_invocation_runbook(encoded)
        self.assertEqual(reloaded.runbook_id, runbook.runbook_id)

    def test_draft_codec_round_trips_and_rejects_tampered_bytes(self) -> None:
        draft = _draft()
        encoded = encode_quality_pilot_runbook_draft(draft)
        reloaded = decode_quality_pilot_runbook_draft(encoded)
        self.assertEqual(reloaded.pilot_run_id, draft.pilot_run_id)
        # Truncate the trailing bytes so the JSON is syntactically broken.
        with self.assertRaises(QualityPilotArmingError):
            decode_quality_pilot_runbook_draft(encoded[:-20])
        # A structurally valid but bucket-invalid replacement must also fail.
        tampered = encoded.replace(draft.bucket.encode("utf-8"), b"UPPERCASE_NOT_ALLOWED")
        self.assertNotEqual(tampered, encoded)
        with self.assertRaises(QualityPilotArmingError):
            decode_quality_pilot_runbook_draft(tampered)

    def test_rejects_wrong_protocol_hash(self) -> None:
        with self.assertRaises(QualityPilotArmingError):
            _draft(protocol_sha256="9" * 64)

    def test_rejects_wrong_session_count(self) -> None:
        campaign = _campaign()
        with self.assertRaises(QualityPilotArmingError):
            _draft(confirmed_sessions=campaign.confirmed_sessions[:19])

    def test_rejects_wrong_window_count(self) -> None:
        campaign = _campaign()
        windows = _all_windows(campaign)
        with self.assertRaises(QualityPilotArmingError):
            _draft(window_timestamps=tuple((w.opens_at, w.closes_at) for w in windows[:79]))

    def test_rejects_naive_window_timestamp(self) -> None:
        campaign = _campaign()
        windows = _all_windows(campaign)
        pairs = list((w.opens_at, w.closes_at) for w in windows)
        pairs[0] = (pairs[0][0].replace(tzinfo=None), pairs[0][1])
        with self.assertRaises(QualityPilotArmingError):
            _draft(window_timestamps=tuple(pairs))

    def test_compile_rejects_wrong_provider_version(self) -> None:
        with self.assertRaises(QualityPilotArmingError):
            _draft(provider_version="")

    def test_compile_rejects_wrong_bucket(self) -> None:
        with self.assertRaises(QualityPilotArmingError):
            _draft(bucket="Not_A_Valid_Bucket!")

    def test_compile_rejects_wrong_pilot_run_id(self) -> None:
        with self.assertRaises(QualityPilotArmingError):
            _draft(pilot_run_id="not-a-sha256")

    def test_compile_rejects_out_of_gate_window(self) -> None:
        campaign = _campaign()
        windows = _all_windows(campaign)
        pairs = [(w.opens_at, w.closes_at) for w in windows]
        session0 = campaign.confirmed_sessions[0]
        bogus_open = datetime.combine(session0, time(0, 1), tzinfo=timezone(timedelta(hours=5, minutes=30)))
        bogus_close = datetime.combine(session0, time(0, 2), tzinfo=timezone(timedelta(hours=5, minutes=30)))
        pairs[0] = (bogus_open, bogus_close)
        draft = _draft(window_timestamps=tuple(pairs))
        with self.assertRaises(QualityPilotArmingError):
            compile_quality_pilot_invocation_runbook(draft)

    def test_compile_rejects_reordered_sessions_in_window_timestamps(self) -> None:
        campaign = _campaign()
        windows = _all_windows(campaign)
        pairs = [(w.opens_at, w.closes_at) for w in windows]
        pairs[0], pairs[4] = pairs[4], pairs[0]
        draft = _draft(window_timestamps=tuple(pairs))
        with self.assertRaises(QualityPilotArmingError):
            compile_quality_pilot_invocation_runbook(draft)

    def test_decode_draft_rejects_duplicate_keys(self) -> None:
        draft = _draft()
        encoded = encode_quality_pilot_runbook_draft(draft)
        text = encoded.decode("utf-8")
        duplicate_key_bytes = text[:-2] + ',"pilot_run_id":"' + draft.pilot_run_id + '"}\n'
        with self.assertRaises(QualityPilotArmingError):
            decode_quality_pilot_runbook_draft(duplicate_key_bytes.encode("utf-8"))

    def test_decode_draft_rejects_float_literal(self) -> None:
        draft = _draft()
        encoded = encode_quality_pilot_runbook_draft(draft)
        text = encoded.decode("utf-8")
        tampered = text.replace('"provider_version":"kite-3.0"', '"provider_version":1.5')
        with self.assertRaises(QualityPilotArmingError):
            decode_quality_pilot_runbook_draft(tampered.encode("utf-8"))

    def test_decode_draft_rejects_non_canonical_json(self) -> None:
        draft = _draft()
        encoded = encode_quality_pilot_runbook_draft(draft)
        pretty = encoded.decode("utf-8").replace(",", ", ")
        with self.assertRaises(QualityPilotArmingError):
            decode_quality_pilot_runbook_draft(pretty.encode("utf-8"))

    def test_decode_draft_rejects_oversized_input(self) -> None:
        from india_swing.quality_pilot.arming import MAXIMUM_DRAFT_BYTES

        with self.assertRaises(QualityPilotArmingError):
            decode_quality_pilot_runbook_draft(b"x" * (MAXIMUM_DRAFT_BYTES + 1))


class ArmingManifestTests(unittest.TestCase):
    def test_manifest_round_trips_and_verifies_identity(self) -> None:
        runbook, _ = _runbook()
        manifest = _manifest(runbook)
        manifest.verify_content_identity()
        encoded = encode_quality_pilot_arming_manifest(manifest)
        reloaded = decode_quality_pilot_arming_manifest(encoded, runbook=runbook)
        self.assertEqual(reloaded.manifest_id, manifest.manifest_id)

    def test_manifest_exposes_fixed_posture(self) -> None:
        runbook, _ = _runbook()
        manifest = _manifest(runbook)
        self.assertEqual(manifest.tasks, 1)
        self.assertEqual(manifest.parallelism, 1)
        self.assertEqual(manifest.max_retries, 0)
        self.assertFalse(manifest.armed)
        self.assertTrue(manifest.quality_only)
        self.assertFalse(manifest.research_partition_eligible)
        self.assertFalse(manifest.execution_eligible)
        self.assertFalse(manifest.capital_eligible)

    def test_manifest_requires_four_unique_lanes(self) -> None:
        runbook, _ = _runbook()
        with self.assertRaises(QualityPilotArmingError):
            _manifest(
                runbook,
                schedules=(
                    QualityPilotArmingSchedule(QualityPilotArmingScheduleLane.CATALOG_PREOPEN, "50 8 * * 1-5"),
                    QualityPilotArmingSchedule(QualityPilotArmingScheduleLane.CATALOG_PREOPEN, "51 8 * * 1-5"),
                    QualityPilotArmingSchedule(QualityPilotArmingScheduleLane.QUOTE_CLOSE, "45 15 * * 1-5"),
                    QualityPilotArmingSchedule(QualityPilotArmingScheduleLane.OHLCV_CLOSE, "20 16 * * 1-5"),
                ),
            )

    def test_manifest_rejects_cron_outside_gate(self) -> None:
        runbook, _ = _runbook()
        with self.assertRaises(QualityPilotArmingError):
            _manifest(
                runbook,
                schedules=(
                    QualityPilotArmingSchedule(QualityPilotArmingScheduleLane.CATALOG_PREOPEN, "1 0 * * 1-5"),
                    QualityPilotArmingSchedule(QualityPilotArmingScheduleLane.QUOTE_0920, "20 9 * * 1-5"),
                    QualityPilotArmingSchedule(QualityPilotArmingScheduleLane.QUOTE_CLOSE, "45 15 * * 1-5"),
                    QualityPilotArmingSchedule(QualityPilotArmingScheduleLane.OHLCV_CLOSE, "20 16 * * 1-5"),
                ),
            )

    def test_manifest_rejects_pinned_image_without_digest(self) -> None:
        runbook, _ = _runbook()
        with self.assertRaises(QualityPilotArmingError):
            _manifest(runbook, image_reference="asia-south1-docker.pkg.dev/proj/repo/image:latest")

    def test_manifest_rejects_non_numeric_secret_versions(self) -> None:
        for bad_version in ("latest", "0", "+3", " 3", "3 ", "01"):
            with self.assertRaises(QualityPilotArmingError, msg=bad_version):
                QualityPilotArmingSecretReference(QualityPilotArmingSecretKind.KITE_API_KEY, "kite-api-key", bad_version)

    def test_manifest_rejects_duplicate_secret_kinds(self) -> None:
        runbook, _ = _runbook()
        with self.assertRaises(QualityPilotArmingError):
            _manifest(
                runbook,
                secret_references=(
                    QualityPilotArmingSecretReference(QualityPilotArmingSecretKind.KITE_API_KEY, "a", "1"),
                    QualityPilotArmingSecretReference(QualityPilotArmingSecretKind.KITE_API_KEY, "b", "2"),
                    QualityPilotArmingSecretReference(QualityPilotArmingSecretKind.RUNBOOK, "c", "3"),
                ),
            )

    def test_manifest_rejects_malformed_gcp_identities(self) -> None:
        runbook, _ = _runbook()
        with self.assertRaises(QualityPilotArmingError):
            _manifest(runbook, gcp_project_id="INVALID PROJECT")
        with self.assertRaises(QualityPilotArmingError):
            _manifest(runbook, gcp_region="not-a-region")
        with self.assertRaises(QualityPilotArmingError):
            _manifest(runbook, runtime_service_account_email="not-an-email")

    def test_manifest_rejects_out_of_bound_timeout(self) -> None:
        runbook, _ = _runbook()
        with self.assertRaises(QualityPilotArmingError):
            _manifest(runbook, timeout_seconds=30)
        with self.assertRaises(QualityPilotArmingError):
            _manifest(runbook, timeout_seconds=999999)

    def test_decode_rejects_manifest_disagreeing_with_supplied_runbook(self) -> None:
        runbook, _ = _runbook()
        other_runbook, _ = _runbook(bucket="a-different-quality-pilot-bucket")
        manifest = _manifest(runbook)
        encoded = encode_quality_pilot_arming_manifest(manifest)
        with self.assertRaises(QualityPilotArmingError):
            decode_quality_pilot_arming_manifest(encoded, runbook=other_runbook)

    def test_decode_rejects_unknown_keys(self) -> None:
        runbook, _ = _runbook()
        manifest = _manifest(runbook)
        encoded = encode_quality_pilot_arming_manifest(manifest)
        text = encoded.decode("utf-8")[:-2] + ',"extra_field":"x"}\n'
        with self.assertRaises(QualityPilotArmingError):
            decode_quality_pilot_arming_manifest(text.encode("utf-8"), runbook=runbook)

    def test_decode_rejects_mutated_posture_fields(self) -> None:
        runbook, _ = _runbook()
        manifest = _manifest(runbook)
        encoded = encode_quality_pilot_arming_manifest(manifest)
        text = encoded.decode("utf-8").replace('"armed":false', '"armed":true')
        with self.assertRaises(QualityPilotArmingError):
            decode_quality_pilot_arming_manifest(text.encode("utf-8"), runbook=runbook)

    def test_decode_rejects_tampered_manifest_id(self) -> None:
        runbook, _ = _runbook()
        manifest = _manifest(runbook)
        encoded = encode_quality_pilot_arming_manifest(manifest)
        text = encoded.decode("utf-8").replace(manifest.manifest_id, "9" * 64)
        with self.assertRaises(QualityPilotArmingError):
            decode_quality_pilot_arming_manifest(text.encode("utf-8"), runbook=runbook)


def _utc(value: datetime) -> datetime:
    return value.astimezone(timezone.utc)


class DueSelectorTests(unittest.TestCase):
    def setUp(self) -> None:
        self.runbook, _ = _runbook()
        self.windows = self.runbook.windows

    def test_one_instant_inside_each_of_four_windows_is_due(self) -> None:
        for window in self.windows[:4]:
            mid = _utc(window.opens_at + (window.closes_at - window.opens_at) / 2)
            selection = select_due_quality_pilot_window(self.runbook, mid)
            self.assertIs(selection.status, QualityPilotDueWindowStatus.DUE)
            self.assertEqual(selection.market_session, window.market_session)
            self.assertIs(selection.window_kind, window.window_kind)

    def test_exact_opens_and_closes_boundaries_are_due(self) -> None:
        window = self.windows[0]
        self.assertIs(
            select_due_quality_pilot_window(self.runbook, _utc(window.opens_at)).status,
            QualityPilotDueWindowStatus.DUE,
        )
        self.assertIs(
            select_due_quality_pilot_window(self.runbook, _utc(window.closes_at)).status,
            QualityPilotDueWindowStatus.DUE,
        )

    def test_one_second_before_opens_and_after_closes_is_not_scheduled(self) -> None:
        window = self.windows[0]
        before = select_due_quality_pilot_window(self.runbook, _utc(window.opens_at - timedelta(seconds=1)))
        after = select_due_quality_pilot_window(self.runbook, _utc(window.closes_at + timedelta(seconds=1)))
        self.assertIs(before.status, QualityPilotDueWindowStatus.NOT_SCHEDULED)
        self.assertIs(after.status, QualityPilotDueWindowStatus.NOT_SCHEDULED)

    def test_non_session_weekend_date_is_not_scheduled(self) -> None:
        # No window exists for a date outside the 20 confirmed sessions.
        far_future = _utc(self.windows[-1].closes_at + timedelta(days=365))
        selection = select_due_quality_pilot_window(self.runbook, far_future)
        self.assertIs(selection.status, QualityPilotDueWindowStatus.NOT_SCHEDULED)

    def test_first_and_final_session_windows_are_reachable(self) -> None:
        first = self.windows[0]
        final = self.windows[-1]
        self.assertIs(
            select_due_quality_pilot_window(self.runbook, _utc(first.opens_at)).status,
            QualityPilotDueWindowStatus.DUE,
        )
        self.assertIs(
            select_due_quality_pilot_window(self.runbook, _utc(final.opens_at)).status,
            QualityPilotDueWindowStatus.DUE,
        )

    def test_after_final_window_closes_is_not_scheduled(self) -> None:
        final = self.windows[-1]
        selection = select_due_quality_pilot_window(self.runbook, _utc(final.closes_at + timedelta(minutes=1)))
        self.assertIs(selection.status, QualityPilotDueWindowStatus.NOT_SCHEDULED)

    def test_naive_observed_at_is_rejected(self) -> None:
        with self.assertRaises(QualityPilotArmingError):
            select_due_quality_pilot_window(self.runbook, datetime(2026, 1, 1))

    def test_timezone_equivalent_instant_is_due(self) -> None:
        window = self.windows[0]
        utc_equivalent = window.opens_at.astimezone(timezone.utc)
        selection = select_due_quality_pilot_window(self.runbook, utc_equivalent)
        self.assertIs(selection.status, QualityPilotDueWindowStatus.DUE)

    def test_non_utc_offset_representation_is_rejected(self) -> None:
        # Revision-2: observed_at must be an EXACT aware UTC datetime (zero
        # offset) -- an equivalent non-UTC-offset representation (e.g. IST,
        # even naming the identical instant) fails closed rather than being
        # silently normalized, so scheduler posture has one canonical time
        # basis.
        window = self.windows[0]
        ist_equivalent = window.opens_at.astimezone(timezone(timedelta(hours=5, minutes=30)))
        with self.assertRaises(QualityPilotArmingError):
            select_due_quality_pilot_window(self.runbook, ist_equivalent)

    def test_tzinfo_failure_is_translated_to_static_arming_error(self) -> None:
        class _BrokenTimezone(tzinfo):
            def utcoffset(self, dt):
                raise RuntimeError("sensitive timezone failure")

            def dst(self, dt):
                return None

        observed_at = datetime(2026, 1, 1, tzinfo=_BrokenTimezone())
        with self.assertRaises(QualityPilotArmingError) as caught:
            select_due_quality_pilot_window(self.runbook, observed_at)
        self.assertIsNone(caught.exception.__cause__)
        self.assertIsNone(caught.exception.__context__)
        self.assertNotIn("sensitive", str(caught.exception))

    def test_impossible_ambiguous_overlap_is_rejected(self) -> None:
        # A validly-constructed QualityPilotInvocationRunbook can never
        # actually contain two overlapping windows (each window's own
        # opens_at/closes_at must map onto its own session's calendar date,
        # and confirmed_sessions are strictly increasing distinct dates) --
        # this is exactly why the finding calls it an "impossible" overlap.
        # Reproduce the defensive check anyway by constructing two
        # individually-valid ObservationWindowSpec objects for the SAME
        # session that deliberately overlap in time (schedule-gate
        # authorization is a separate check this construction bypasses on
        # purpose), and patching the module's own type-check target so the
        # selector accepts a minimal stand-in carrying them.
        from unittest import mock

        from india_swing.quality_pilot.canonical_response import EndpointFamily, ObservationWindowSpec

        session0 = self.runbook.campaign.confirmed_sessions[0]
        ist = timezone(timedelta(hours=5, minutes=30))
        shared_open = datetime.combine(session0, time(10, 0), tzinfo=ist)
        shared_close = datetime.combine(session0, time(10, 30), tzinfo=ist)
        overlap_a = ObservationWindowSpec(
            pilot_run_id=self.runbook.campaign.pilot_run_id, market_session=session0,
            window_kind=ScheduledWindowKind.CATALOG_PREOPEN, endpoint_family=EndpointFamily.CATALOG,
            opens_at=shared_open, closes_at=shared_close, protocol_sha256=self.runbook.campaign.protocol_sha256,
        )
        overlap_b = ObservationWindowSpec(
            pilot_run_id=self.runbook.campaign.pilot_run_id, market_session=session0,
            window_kind=ScheduledWindowKind.QUOTE_0920, endpoint_family=EndpointFamily.FULL_QUOTE,
            opens_at=shared_open, closes_at=shared_close, protocol_sha256=self.runbook.campaign.protocol_sha256,
        )

        class _FakeRunbook:
            campaign = self.runbook.campaign
            windows = (overlap_a, overlap_b)

            def verify_content_identity(self) -> None:
                return None

        with mock.patch.object(arming_module, "QualityPilotInvocationRunbook", _FakeRunbook):
            with self.assertRaises(QualityPilotArmingError):
                select_due_quality_pilot_window(_FakeRunbook(), _utc(shared_open + timedelta(minutes=10)))


COMPLETE = QualityPilotWindowCompletionProbeResult.COMPLETE
INCOMPLETE = QualityPilotWindowCompletionProbeResult.INCOMPLETE


def _proof(targets, *, incomplete_at: tuple[int, ...] = ()) -> QualityPilotOrderedCompletionProof:
    entries = tuple(
        QualityPilotWindowCompletionEvidence(
            window_id=window.window_id, result=INCOMPLETE if index in incomplete_at else COMPLETE
        )
        for index, window in enumerate(targets)
    )
    return QualityPilotOrderedCompletionProof(entries)


class CompletionProbeTargetAndPostureTests(unittest.TestCase):
    def setUp(self) -> None:
        self.runbook, _ = _runbook()
        self.windows = self.runbook.windows

    def test_before_pilot_start_has_no_probe_targets(self) -> None:
        before_all = _utc(self.windows[0].opens_at - timedelta(days=1))
        self.assertEqual(quality_pilot_window_completion_probe_targets(self.runbook, before_all), ())
        assessment = assess_quality_pilot_window_posture(self.runbook, before_all, None)
        self.assertIs(assessment.posture, QualityPilotWindowPosture.NOT_SCHEDULED)

    def test_before_pilot_start_calls_zero_gcs_meaning_no_targets(self) -> None:
        before_all = _utc(self.windows[0].opens_at - timedelta(days=1))
        targets = quality_pilot_window_completion_probe_targets(self.runbook, before_all)
        self.assertEqual(len(targets), 0)

    def test_before_pilot_start_rejects_malformed_proof_with_static_error(self) -> None:
        before_all = _utc(self.windows[0].opens_at - timedelta(days=1))
        with self.assertRaises(QualityPilotArmingError) as caught:
            assess_quality_pilot_window_posture(self.runbook, before_all, True)  # type: ignore[arg-type]
        self.assertIsNone(caught.exception.__cause__)
        self.assertIsNone(caught.exception.__context__)

    def test_not_scheduled_value_rejects_attached_window_identity(self) -> None:
        with self.assertRaises(QualityPilotArmingError):
            arming_module.QualityPilotWindowPostureAssessment(
                QualityPilotWindowPosture.NOT_SCHEDULED,
                self.windows[0].market_session,
                self.windows[0].window_kind,
            )

    def test_genesis_due_window_targets_is_exactly_itself(self) -> None:
        window = self.windows[0]
        mid = _utc(window.opens_at + (window.closes_at - window.opens_at) / 2)
        targets = quality_pilot_window_completion_probe_targets(self.runbook, mid)
        self.assertEqual(len(targets), 1)
        self.assertEqual(targets[0].window_id, window.window_id)

    def test_genesis_due_window_with_complete_proof_is_already_complete(self) -> None:
        window = self.windows[0]
        mid = _utc(window.opens_at + (window.closes_at - window.opens_at) / 2)
        targets = quality_pilot_window_completion_probe_targets(self.runbook, mid)
        assessment = assess_quality_pilot_window_posture(self.runbook, mid, _proof(targets))
        self.assertIs(assessment.posture, QualityPilotWindowPosture.ALREADY_COMPLETE)

    def test_genesis_due_window_with_incomplete_proof_is_due(self) -> None:
        window = self.windows[0]
        mid = _utc(window.opens_at + (window.closes_at - window.opens_at) / 2)
        targets = quality_pilot_window_completion_probe_targets(self.runbook, mid)
        assessment = assess_quality_pilot_window_posture(self.runbook, mid, _proof(targets, incomplete_at=(0,)))
        self.assertIs(assessment.posture, QualityPilotWindowPosture.DUE)
        self.assertEqual(assessment.market_session, window.market_session)

    def test_later_due_window_targets_is_the_exact_ordered_prefix(self) -> None:
        window4 = self.windows[4]
        mid = _utc(window4.opens_at + (window4.closes_at - window4.opens_at) / 2)
        targets = quality_pilot_window_completion_probe_targets(self.runbook, mid)
        self.assertEqual(len(targets), 5)
        self.assertEqual([w.window_id for w in targets], [w.window_id for w in self.windows[:5]])

    def test_later_due_window_with_adjacent_predecessor_incomplete_is_missed_window_blocked(self) -> None:
        window4 = self.windows[4]
        mid = _utc(window4.opens_at + (window4.closes_at - window4.opens_at) / 2)
        targets = quality_pilot_window_completion_probe_targets(self.runbook, mid)
        assessment = assess_quality_pilot_window_posture(self.runbook, mid, _proof(targets, incomplete_at=(3,)))
        self.assertIs(assessment.posture, QualityPilotWindowPosture.MISSED_WINDOW_BLOCKED)

    def test_later_due_window_with_non_adjacent_earlier_predecessor_incomplete_is_missed_window_blocked(self) -> None:
        # This is exactly the revision-2 correction: window index 0 (a
        # NON-adjacent predecessor of the DUE window at index 4) is
        # incomplete while every more recent predecessor (1, 2, 3) and the
        # current window are complete. Revision 1's single-target design
        # would have missed this entirely, since it only ever probed the
        # DUE window itself.
        window4 = self.windows[4]
        mid = _utc(window4.opens_at + (window4.closes_at - window4.opens_at) / 2)
        targets = quality_pilot_window_completion_probe_targets(self.runbook, mid)
        self.assertEqual(len(targets), 5)
        assessment = assess_quality_pilot_window_posture(self.runbook, mid, _proof(targets, incomplete_at=(0,)))
        self.assertIs(assessment.posture, QualityPilotWindowPosture.MISSED_WINDOW_BLOCKED)

    def test_later_due_window_all_predecessors_complete_current_incomplete_is_due(self) -> None:
        window4 = self.windows[4]
        mid = _utc(window4.opens_at + (window4.closes_at - window4.opens_at) / 2)
        targets = quality_pilot_window_completion_probe_targets(self.runbook, mid)
        assessment = assess_quality_pilot_window_posture(self.runbook, mid, _proof(targets, incomplete_at=(4,)))
        self.assertIs(assessment.posture, QualityPilotWindowPosture.DUE)

    def test_later_due_window_all_complete_including_current_is_already_complete(self) -> None:
        window4 = self.windows[4]
        mid = _utc(window4.opens_at + (window4.closes_at - window4.opens_at) / 2)
        targets = quality_pilot_window_completion_probe_targets(self.runbook, mid)
        assessment = assess_quality_pilot_window_posture(self.runbook, mid, _proof(targets))
        self.assertIs(assessment.posture, QualityPilotWindowPosture.ALREADY_COMPLETE)

    def test_passed_window_without_completion_is_missed_window_blocked(self) -> None:
        window = self.windows[0]
        after = _utc(window.closes_at + timedelta(minutes=1))
        targets = quality_pilot_window_completion_probe_targets(self.runbook, after)
        self.assertEqual(len(targets), 1)
        assessment = assess_quality_pilot_window_posture(self.runbook, after, _proof(targets, incomplete_at=(0,)))
        self.assertIs(assessment.posture, QualityPilotWindowPosture.MISSED_WINDOW_BLOCKED)

    def test_passed_window_with_completion_and_not_final_is_not_scheduled(self) -> None:
        window = self.windows[0]
        after = _utc(window.closes_at + timedelta(minutes=1))
        targets = quality_pilot_window_completion_probe_targets(self.runbook, after)
        assessment = assess_quality_pilot_window_posture(self.runbook, after, _proof(targets))
        self.assertIs(assessment.posture, QualityPilotWindowPosture.NOT_SCHEDULED)

    def test_after_final_window_all_80_complete_is_pilot_complete(self) -> None:
        final = self.windows[-1]
        after = _utc(final.closes_at + timedelta(minutes=1))
        targets = quality_pilot_window_completion_probe_targets(self.runbook, after)
        self.assertEqual(len(targets), 80)
        assessment = assess_quality_pilot_window_posture(self.runbook, after, _proof(targets))
        self.assertIs(assessment.posture, QualityPilotWindowPosture.PILOT_COMPLETE)

    def test_after_final_window_one_of_80_incomplete_is_missed_window_blocked(self) -> None:
        final = self.windows[-1]
        after = _utc(final.closes_at + timedelta(minutes=1))
        targets = quality_pilot_window_completion_probe_targets(self.runbook, after)
        assessment = assess_quality_pilot_window_posture(self.runbook, after, _proof(targets, incomplete_at=(37,)))
        self.assertIs(assessment.posture, QualityPilotWindowPosture.MISSED_WINDOW_BLOCKED)

    def test_a_later_due_window_cannot_bypass_an_earlier_missed_window(self) -> None:
        # Causal assertion (not merely target-selection): a DUE window at
        # index 4 whose non-adjacent predecessor at index 0 is incomplete
        # must resolve to MISSED_WINDOW_BLOCKED even though the DUE
        # window's own entry and every more recent predecessor are
        # complete -- proving the block, not merely which window was named.
        window4 = self.windows[4]
        mid = _utc(window4.opens_at + (window4.closes_at - window4.opens_at) / 2)
        targets = quality_pilot_window_completion_probe_targets(self.runbook, mid)
        assessment = assess_quality_pilot_window_posture(self.runbook, mid, _proof(targets, incomplete_at=(0,)))
        self.assertIs(assessment.posture, QualityPilotWindowPosture.MISSED_WINDOW_BLOCKED)

    def test_proof_rejects_missing_entry(self) -> None:
        window4 = self.windows[4]
        mid = _utc(window4.opens_at + (window4.closes_at - window4.opens_at) / 2)
        targets = quality_pilot_window_completion_probe_targets(self.runbook, mid)
        short_proof = _proof(targets[:-1])
        with self.assertRaises(QualityPilotArmingError):
            assess_quality_pilot_window_posture(self.runbook, mid, short_proof)

    def test_proof_rejects_extra_entry(self) -> None:
        window4 = self.windows[4]
        mid = _utc(window4.opens_at + (window4.closes_at - window4.opens_at) / 2)
        targets = quality_pilot_window_completion_probe_targets(self.runbook, mid)
        extended = targets + (self.windows[5],)
        long_proof = _proof(extended)
        with self.assertRaises(QualityPilotArmingError):
            assess_quality_pilot_window_posture(self.runbook, mid, long_proof)

    def test_proof_rejects_reordered_entries(self) -> None:
        window4 = self.windows[4]
        mid = _utc(window4.opens_at + (window4.closes_at - window4.opens_at) / 2)
        targets = quality_pilot_window_completion_probe_targets(self.runbook, mid)
        reordered = tuple(reversed(targets))
        reordered_proof = _proof(reordered)
        with self.assertRaises(QualityPilotArmingError):
            assess_quality_pilot_window_posture(self.runbook, mid, reordered_proof)

    def test_proof_rejects_duplicate_window_id(self) -> None:
        window = self.windows[0]
        with self.assertRaises(QualityPilotArmingError):
            QualityPilotOrderedCompletionProof(
                (
                    QualityPilotWindowCompletionEvidence(window.window_id, COMPLETE),
                    QualityPilotWindowCompletionEvidence(window.window_id, COMPLETE),
                )
            )

    def test_proof_rejects_foreign_window_evidence(self) -> None:
        window4 = self.windows[4]
        mid = _utc(window4.opens_at + (window4.closes_at - window4.opens_at) / 2)
        targets = quality_pilot_window_completion_probe_targets(self.runbook, mid)
        foreign_proof = QualityPilotOrderedCompletionProof(
            tuple(QualityPilotWindowCompletionEvidence(w.window_id, COMPLETE) for w in targets[:-1])
            + (QualityPilotWindowCompletionEvidence("9" * 64, COMPLETE),)
        )
        with self.assertRaises(QualityPilotArmingError):
            assess_quality_pilot_window_posture(self.runbook, mid, foreign_proof)

    def test_proof_rejects_subclassed_evidence(self) -> None:
        class _SubclassedEvidence(QualityPilotWindowCompletionEvidence):
            pass

        window = self.windows[0]
        subclassed = _SubclassedEvidence(window_id=window.window_id, result=COMPLETE)
        with self.assertRaises(QualityPilotArmingError):
            QualityPilotOrderedCompletionProof((subclassed,))

    def test_proof_rejects_bare_boolean_or_unbound_result(self) -> None:
        window = self.windows[0]
        mid = _utc(window.opens_at + (window.closes_at - window.opens_at) / 2)
        with self.assertRaises(QualityPilotArmingError):
            assess_quality_pilot_window_posture(self.runbook, mid, True)  # type: ignore[arg-type]
        with self.assertRaises(QualityPilotArmingError):
            assess_quality_pilot_window_posture(self.runbook, mid, COMPLETE)  # type: ignore[arg-type]
        with self.assertRaises(QualityPilotArmingError):
            QualityPilotWindowCompletionEvidence(window_id=window.window_id, result=True)  # type: ignore[arg-type]

    def test_proof_type_must_be_exact_never_defaults_to_complete(self) -> None:
        window = self.windows[0]
        mid = _utc(window.opens_at + (window.closes_at - window.opens_at) / 2)
        with self.assertRaises(QualityPilotArmingError):
            assess_quality_pilot_window_posture(self.runbook, mid, "COMPLETE")  # type: ignore[arg-type]
        with self.assertRaises(QualityPilotArmingError):
            assess_quality_pilot_window_posture(self.runbook, mid, None)


class RegressionAndCapabilityTests(unittest.TestCase):
    def test_module_has_no_env_filesystem_clock_network_or_subprocess_capability(self) -> None:
        source = inspect.getsource(arming_module)
        tree = ast.parse(source)
        forbidden_modules = {
            "os", "pathlib", "socket", "subprocess", "requests", "urllib", "httpx",
            "google", "kiteconnect", "time", "random", "threading", "asyncio",
            "sqlite3", "pickle", "shelve",
        }
        imported: set[str] = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imported.update(alias.name.split(".")[0] for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module:
                imported.add(node.module.split(".")[0])
        self.assertEqual(imported & forbidden_modules, set())
        lowered = source.lower()
        for token in (
            "datetime.now(", "utcnow(", "getenv(", "os.environ", "sleep(", "list_blobs(",
            "place_order(", "generate_signal(", "run_paper_trade(", "cloud_run",
            "telegram.send", "telegrambot", "gcloud", "subprocess.",
        ):
            self.assertNotIn(token, lowered, msg=token)

    def test_module_never_calls_a_bucket_listing_or_selects_a_latest_object(self) -> None:
        source = inspect.getsource(arming_module)
        lowered = source.lower()
        for token in ("list_blobs(", "list_objects(", ".blobs(", "select_latest"):
            self.assertNotIn(token, lowered, msg=token)

    def test_only_arming_cli_may_touch_local_files(self) -> None:
        import india_swing.quality_pilot_arming_cli as cli_module

        cli_source = inspect.getsource(cli_module)
        self.assertIn("os.open", cli_source)


if __name__ == "__main__":
    unittest.main()
