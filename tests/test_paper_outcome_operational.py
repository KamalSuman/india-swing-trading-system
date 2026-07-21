from __future__ import annotations

import tempfile
import unittest
import hashlib
import contextlib
import inspect
import io
import json
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from india_swing.outcomes import ReviewClassification
from india_swing.daily_pipeline.acquisition import GCSObjectPayload
from india_swing.daily_pipeline.state_publication import PublishedStateObject
from india_swing.paper_outcomes import (
    LocalPaperOutcomeRunStore,
    PaperOutcomeEvidence,
    PaperOutcomeJobSpec,
    PaperOutcomeOperationalError,
    PaperOutcomePolicy,
    PaperOutcomeStatus,
    PaperOutcomeStateError,
    PaperOutcomeStateArtifactKind,
    decode_paper_outcome_job_spec,
    decode_paper_outcome_record,
    encode_paper_outcome_job_spec,
    encode_paper_outcome_record,
    publish_paper_outcome_state_to_gcs,
    prepare_paper_outcome_job_spec,
    replay_paper_outcome,
    run_paper_outcome_job,
    restore_paper_outcome_state_from_gcs,
)
from india_swing.paper_trades import LocalPaperTradeLedger
from india_swing.paper_outcome_job import main as outcome_job_main
from india_swing.paper_outcome_restore import main as outcome_restore_main
import india_swing.paper_outcome_job as outcome_job_module
import india_swing.paper_outcome_restore as outcome_restore_module
import india_swing.paper_outcomes.gcs_state as outcome_gcs_module
import india_swing.paper_outcomes.operational as outcome_operational_module
from tests.test_paper_outcomes import (
    ISIN,
    _binding,
    _calendar,
    _observation,
    _registration,
)


UTC = timezone.utc


class _EvidenceSource:
    def __init__(self, evidence: PaperOutcomeEvidence) -> None:
        self.evidence = evidence
        self.calls: list[str] = []

    def load(self, spec: PaperOutcomeJobSpec) -> PaperOutcomeEvidence:
        self.calls.append(spec.job_spec_id)
        return self.evidence


class _MemoryState:
    def __init__(self) -> None:
        self.objects: dict[tuple[str, str, int], bytes] = {}
        self.write_calls: list[dict[str, object]] = []
        self.read_calls: list[dict[str, object]] = []

    def create_or_verify(self, **values: object) -> PublishedStateObject:
        self.write_calls.append(values)
        generation = len(self.write_calls)
        payload = values["content_bytes"]
        self.objects[(values["bucket"], values["object_name"], generation)] = payload
        return PublishedStateObject(
            object_name=values["object_name"],
            generation=generation,
            byte_count=len(payload),
            sha256=hashlib.sha256(payload).hexdigest(),
        )

    def read_generation(self, **values: object) -> GCSObjectPayload:
        self.read_calls.append(values)
        payload = self.objects[
            (values["bucket"], values["object_name"], values["generation"])
        ]
        return GCSObjectPayload(
            generation=values["generation"],
            content_bytes=payload[: values["maximum_bytes"] + 1],
        )


def _evidence(observations) -> PaperOutcomeEvidence:
    registration = _registration()
    return PaperOutcomeEvidence(
        registration=registration,
        binding=_binding(registration),
        calendar=_calendar(),
        observations=tuple(observations),
    )


def _spec(evidence: PaperOutcomeEvidence, *, as_of: datetime) -> PaperOutcomeJobSpec:
    policy = PaperOutcomePolicy()
    replay = replay_paper_outcome(
        registration=evidence.registration,
        binding=evidence.binding,
        calendar=evidence.calendar,
        observations=evidence.observations,
        as_of=as_of,
        policy=policy,
    )
    return PaperOutcomeJobSpec(
        registration_id=evidence.registration.registration_id,
        calendar_materialization_id="1" * 64,
        tick_snapshot_id=evidence.binding.tick_snapshot_id,
        historical_artifact_ids=tuple(
            value.artifact_id for value in evidence.observations
        ) or ("2" * 64,),
        series="EQ",
        validated_isin=ISIN,
        as_of=as_of,
        policy=policy,
        expected_replay_id=replay.replay_id,
    )


class PaperOutcomeOperationalTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.ledger = LocalPaperTradeLedger(self.root / "paper")
        self.record_store = LocalPaperOutcomeRunStore(self.root / "paper_outcomes")

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def _run(self, evidence: PaperOutcomeEvidence, spec: PaperOutcomeJobSpec):
        self.ledger.register_value(evidence.registration)
        source = _EvidenceSource(evidence)
        record = run_paper_outcome_job(
            spec=spec,
            evidence_source=source,
            ledger=self.ledger,
            record_store=self.record_store,
        )
        return record, source

    def test_closed_target_is_reconciled_reviewed_and_restart_idempotent(self) -> None:
        calendar = _calendar()
        evidence = _evidence(
            (
                _observation(calendar, date(2026, 1, 2), high="111", low="95"),
                _observation(calendar, date(2026, 1, 3), high="111", low="95"),
            )
        )
        spec = _spec(evidence, as_of=datetime(2026, 1, 10, tzinfo=UTC))

        record, source = self._run(evidence, spec)

        self.assertIs(record.outcome_status, PaperOutcomeStatus.CLOSED)
        self.assertEqual(record.symbol, "INFY")
        self.assertEqual(len(record.event_ids), 2)
        self.assertEqual(record.event_ids, record.appended_event_ids)
        self.assertIsNotNone(record.estimated_net_pnl)
        self.assertGreater(record.estimated_net_pnl, 0)
        self.assertIsNotNone(record.review)
        self.assertIs(
            record.review.classification,
            ReviewClassification.PROFITABLE_OUTCOME,
        )
        self.assertIn("PAPER-ONLY", record.message)
        self.assertEqual(source.calls, [spec.job_spec_id])

        second = run_paper_outcome_job(
            spec=spec,
            evidence_source=source,
            ledger=self.ledger,
            record_store=self.record_store,
        )
        self.assertEqual(second, record)
        self.assertEqual(source.calls, [spec.job_spec_id])
        self.assertEqual(
            tuple(value.event_id for value in self.ledger.list_events(spec.registration_id)),
            record.event_ids,
        )

    def test_spec_preparation_pins_the_exact_replay_without_writing(self) -> None:
        calendar = _calendar()
        evidence = _evidence((_observation(calendar, date(2026, 1, 2)),))
        source = _EvidenceSource(evidence)
        as_of = datetime(2026, 1, 3, tzinfo=UTC)

        spec = prepare_paper_outcome_job_spec(
            registration_id=evidence.registration.registration_id,
            calendar_materialization_id="1" * 64,
            tick_snapshot_id=evidence.binding.tick_snapshot_id,
            historical_artifact_ids=tuple(
                value.artifact_id for value in evidence.observations
            ),
            series="EQ",
            validated_isin=ISIN,
            as_of=as_of,
            policy=PaperOutcomePolicy(),
            evidence_source=source,
        )

        expected = replay_paper_outcome(
            registration=evidence.registration,
            binding=evidence.binding,
            calendar=evidence.calendar,
            observations=evidence.observations,
            as_of=as_of,
            policy=spec.policy,
        )
        self.assertEqual(spec.expected_replay_id, expected.replay_id)
        self.assertEqual(len(source.calls), 1)
        self.assertFalse((self.root / "paper").exists())
        self.assertFalse((self.root / "paper_outcomes").exists())

    def test_gap_stop_gets_tail_review_without_inventing_news(self) -> None:
        calendar = _calendar()
        evidence = _evidence(
            (
                _observation(calendar, date(2026, 1, 2)),
                _observation(
                    calendar,
                    date(2026, 1, 3),
                    open="85",
                    high="86",
                    low="80",
                    close="82",
                ),
            )
        )
        spec = _spec(evidence, as_of=datetime(2026, 1, 10, tzinfo=UTC))

        record, _ = self._run(evidence, spec)

        self.assertLess(record.estimated_net_pnl, 0)
        self.assertIs(
            record.review.classification,
            ReviewClassification.TAIL_OR_GAP_LOSS,
        )
        self.assertTrue(
            any("news attribution" in value for value in record.review.uncertainties)
        )

    def test_waiting_replay_writes_no_ledger_event_and_no_review(self) -> None:
        calendar = _calendar()
        future = _observation(calendar, date(2026, 1, 2))
        as_of = future.knowledge_time - timedelta(seconds=1)
        evidence = _evidence((future,))
        spec = _spec(evidence, as_of=as_of)

        record, _ = self._run(evidence, spec)

        self.assertIs(record.outcome_status, PaperOutcomeStatus.WAITING)
        self.assertEqual(record.event_ids, ())
        self.assertIsNone(record.review)
        self.assertIsNone(record.estimated_net_pnl)

    def test_expected_replay_mismatch_fails_before_ledger_or_record_write(self) -> None:
        calendar = _calendar()
        evidence = _evidence((_observation(calendar, date(2026, 1, 2)),))
        valid = _spec(evidence, as_of=datetime(2026, 1, 3, tzinfo=UTC))
        mismatched = PaperOutcomeJobSpec(
            registration_id=valid.registration_id,
            calendar_materialization_id=valid.calendar_materialization_id,
            tick_snapshot_id=valid.tick_snapshot_id,
            historical_artifact_ids=valid.historical_artifact_ids,
            series=valid.series,
            validated_isin=valid.validated_isin,
            as_of=valid.as_of,
            policy=valid.policy,
            expected_replay_id="f" * 64,
        )
        self.ledger.register_value(evidence.registration)

        with self.assertRaises(PaperOutcomeOperationalError):
            run_paper_outcome_job(
                spec=mismatched,
                evidence_source=_EvidenceSource(evidence),
                ledger=self.ledger,
                record_store=self.record_store,
            )

        self.assertEqual(self.ledger.list_events(valid.registration_id), ())
        with self.assertRaises(PaperOutcomeOperationalError):
            self.record_store.get(mismatched.job_spec_id)

    def test_job_spec_and_record_codecs_are_canonical_and_tamper_evident(self) -> None:
        calendar = _calendar()
        evidence = _evidence(
            (
                _observation(calendar, date(2026, 1, 2)),
                _observation(calendar, date(2026, 1, 3), high="111"),
            )
        )
        spec = _spec(evidence, as_of=datetime(2026, 1, 10, tzinfo=UTC))
        record, _ = self._run(evidence, spec)

        spec_bytes = encode_paper_outcome_job_spec(spec)
        record_bytes = encode_paper_outcome_record(record)
        self.assertEqual(decode_paper_outcome_job_spec(spec_bytes), spec)
        self.assertEqual(decode_paper_outcome_record(record_bytes), record)

        with self.assertRaises(PaperOutcomeOperationalError):
            decode_paper_outcome_job_spec(spec_bytes.replace(b'"EQ"', b'"BE"'))
        with self.assertRaises(PaperOutcomeOperationalError):
            decode_paper_outcome_record(
                record_bytes.replace(b'"INFY"', b'"WIPRO"')
            )

    def test_gcs_publication_is_terminal_last_and_restores_idempotently(self) -> None:
        calendar = _calendar()
        evidence = _evidence(
            (
                _observation(calendar, date(2026, 1, 2)),
                _observation(calendar, date(2026, 1, 3), high="111"),
            )
        )
        spec = _spec(evidence, as_of=datetime(2026, 1, 10, tzinfo=UTC))
        record, _ = self._run(evidence, spec)
        memory = _MemoryState()

        publication = publish_paper_outcome_state_to_gcs(
            record=record,
            bucket="paper-state-bucket",
            writer=memory,
            ledger=self.ledger,
        )

        self.assertEqual(
            memory.write_calls[-1]["object_name"],
            publication.manifest_object.object_name,
        )
        self.assertIn("/manifests/", publication.manifest_object.object_name)

        restored_ledger = LocalPaperTradeLedger(self.root / "restored" / "paper")
        restored_store = LocalPaperOutcomeRunStore(
            self.root / "restored" / "paper_outcomes"
        )
        restored = restore_paper_outcome_state_from_gcs(
            expected_job_spec_id=spec.job_spec_id,
            bucket="paper-state-bucket",
            manifest_object_name=publication.manifest_object.object_name,
            manifest_generation=publication.manifest_object.generation,
            manifest_sha256=publication.manifest_object.sha256,
            reader=memory,
            ledger=restored_ledger,
            record_store=restored_store,
        )
        self.assertEqual(restored, record)
        self.assertEqual(
            tuple(value.event_id for value in restored_ledger.list_events(record.registration_id)),
            record.event_ids,
        )

        second = restore_paper_outcome_state_from_gcs(
            expected_job_spec_id=spec.job_spec_id,
            bucket="paper-state-bucket",
            manifest_object_name=publication.manifest_object.object_name,
            manifest_generation=publication.manifest_object.generation,
            manifest_sha256=publication.manifest_object.sha256,
            reader=memory,
            ledger=restored_ledger,
            record_store=restored_store,
        )
        self.assertEqual(second, record)

    def test_manifest_hash_failure_writes_no_local_state(self) -> None:
        calendar = _calendar()
        evidence = _evidence((_observation(calendar, date(2026, 1, 2)),))
        spec = _spec(evidence, as_of=datetime(2026, 1, 3, tzinfo=UTC))
        record, _ = self._run(evidence, spec)
        memory = _MemoryState()
        publication = publish_paper_outcome_state_to_gcs(
            record=record,
            bucket="paper-state-bucket",
            writer=memory,
            ledger=self.ledger,
        )
        restored_ledger = LocalPaperTradeLedger(self.root / "rejected" / "paper")
        restored_store = LocalPaperOutcomeRunStore(
            self.root / "rejected" / "paper_outcomes"
        )

        with self.assertRaises(PaperOutcomeStateError):
            restore_paper_outcome_state_from_gcs(
                expected_job_spec_id=spec.job_spec_id,
                bucket="paper-state-bucket",
                manifest_object_name=publication.manifest_object.object_name,
                manifest_generation=publication.manifest_object.generation,
                manifest_sha256="0" * 64,
                reader=memory,
                ledger=restored_ledger,
                record_store=restored_store,
            )

        self.assertEqual(len(memory.read_calls), 1)
        with self.assertRaises(Exception):
            restored_ledger.get_registration(record.registration_id)
        with self.assertRaises(Exception):
            restored_store.get(spec.job_spec_id)

    def test_tampered_terminal_object_is_rejected_before_any_restore_write(self) -> None:
        calendar = _calendar()
        evidence = _evidence(
            (
                _observation(calendar, date(2026, 1, 2)),
                _observation(calendar, date(2026, 1, 3), high="111"),
            )
        )
        spec = _spec(evidence, as_of=datetime(2026, 1, 10, tzinfo=UTC))
        record, _ = self._run(evidence, spec)
        memory = _MemoryState()
        publication = publish_paper_outcome_state_to_gcs(
            record=record,
            bucket="paper-state-bucket",
            writer=memory,
            ledger=self.ledger,
        )
        terminal = next(
            value
            for value in publication.manifest.artifacts
            if value.kind is PaperOutcomeStateArtifactKind.RECORD
        )
        key = (
            "paper-state-bucket",
            terminal.published.object_name,
            terminal.published.generation,
        )
        memory.objects[key] = memory.objects[key] + b"tampered"
        restored_ledger = LocalPaperTradeLedger(self.root / "tampered" / "paper")
        restored_store = LocalPaperOutcomeRunStore(
            self.root / "tampered" / "paper_outcomes"
        )

        with self.assertRaises(PaperOutcomeStateError):
            restore_paper_outcome_state_from_gcs(
                expected_job_spec_id=spec.job_spec_id,
                bucket="paper-state-bucket",
                manifest_object_name=publication.manifest_object.object_name,
                manifest_generation=publication.manifest_object.generation,
                manifest_sha256=publication.manifest_object.sha256,
                reader=memory,
                ledger=restored_ledger,
                record_store=restored_store,
            )

        with self.assertRaises(Exception):
            restored_ledger.get_registration(record.registration_id)
        with self.assertRaises(Exception):
            restored_store.get(spec.job_spec_id)

    def test_cli_failures_are_sanitized_and_core_has_no_broker_capability(self) -> None:
        secret = "secret-value-that-must-not-leak"
        for main in (outcome_job_main, outcome_restore_main):
            stdout = io.StringIO()
            stderr = io.StringIO()
            with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
                code = main([], environ={"UNTRUSTED_SECRET": secret})
            self.assertEqual(code, 2)
            self.assertEqual(stdout.getvalue(), "")
            payload = json.loads(stderr.getvalue())
            self.assertEqual(
                payload,
                {"error_type": "PaperOutcomeOperationalError", "status": "FAILED"},
            )
            self.assertNotIn(secret, stderr.getvalue())

        combined = "\n".join(
            inspect.getsource(module)
            for module in (
                outcome_operational_module,
                outcome_gcs_module,
                outcome_job_module,
                outcome_restore_module,
            )
        ).casefold()
        for forbidden in (
            "place_order",
            "modify_order",
            "cancel_order",
            "kiteconnect",
            "list_blobs",
        ):
            self.assertNotIn(forbidden, combined)


if __name__ == "__main__":
    unittest.main()
