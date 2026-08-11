from __future__ import annotations

import hashlib
import tempfile
import unittest
from unittest.mock import patch
from datetime import timedelta
from datetime import date, datetime, timezone
from pathlib import Path

from india_swing.daily_pipeline.acquisition import GCSObjectPayload
from india_swing.daily_pipeline.state_publication import PublishedStateObject
from india_swing.operations.portfolio_store import LocalSwingPortfolioArtifactStore
from india_swing.notifications import TelegramBotConfig
from india_swing.paper_trades import LocalPaperTradeLedger
from india_swing.paper_outcomes import (
    LocalPaperOutcomeRunStore,
    LocalPaperPortfolioStateStore,
    LocalPaperPortfolioRolloverStore,
    PaperPortfolioBatchSpec,
    PaperOutcomeStatus,
    PaperPortfolioRolloverLineage,
    PaperPortfolioRolloverPublicationError,
    PaperPortfolioRolloverRequest,
    PaperPortfolioServiceError,
    decode_paper_portfolio_rollover_publication_manifest,
    prepare_paper_portfolio_rollover_request,
    restore_paper_portfolio_rollover,
    run_paper_portfolio_rollover_service,
    run_paper_portfolio_batch,
    run_paper_portfolio_operational_service,
)
import tests.test_paper_portfolio_rollover as rollover_fixtures
import tests.test_paper_outcome_operational as outcome_operational_fixtures
import tests.test_paper_outcomes as outcome_fixtures


UTC = timezone.utc


class _CreateOnceMemory:
    def __init__(self) -> None:
        self.objects: dict[tuple[str, str, int], bytes] = {}
        self.current: dict[tuple[str, str], PublishedStateObject] = {}
        self.write_calls: list[dict[str, object]] = []
        self.read_calls: list[dict[str, object]] = []

    def create_or_verify(self, **values: object) -> PublishedStateObject:
        self.write_calls.append(values)
        key = (values["bucket"], values["object_name"])
        payload = values["content_bytes"]
        existing = self.current.get(key)
        if existing is not None:
            stored = self.objects[(key[0], key[1], existing.generation)]
            if stored != payload:
                raise ValueError("create-once conflict")
            return existing
        generation = len(self.current) + 1
        published = PublishedStateObject(
            object_name=values["object_name"],
            generation=generation,
            byte_count=len(payload),
            sha256=hashlib.sha256(payload).hexdigest(),
        )
        self.objects[(key[0], key[1], generation)] = payload
        self.current[key] = published
        return published

    def read_generation(self, **values: object) -> GCSObjectPayload:
        self.read_calls.append(values)
        payload = self.objects[
            (values["bucket"], values["object_name"], values["generation"])
        ]
        return GCSObjectPayload(
            generation=values["generation"],
            content_bytes=payload[: values["maximum_bytes"] + 1],
        )


class _TelegramTransport:
    def __init__(self) -> None:
        self.calls = 0

    def post_json(self, **_: object) -> bytes:
        self.calls += 1
        return b'{"ok":true,"result":{"message_id":29}}'


def _open_state_and_mark():
    fixture = rollover_fixtures.PaperPortfolioRolloverTests()
    _, _, state, mark = fixture._open_state_and_mark()
    return state, mark


class PaperPortfolioRolloverOperationalTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.state_root = self.root / "state"
        self.genesis = rollover_fixtures._genesis()
        LocalSwingPortfolioArtifactStore(self.state_root / "portfolio").put(
            self.genesis
        )
        self.state, self.mark = _open_state_and_mark()
        self.request = PaperPortfolioRolloverRequest(
            genesis_artifact_id=self.genesis.artifact_id,
            previous_rollover_id=None,
            marks=(self.mark,),
            as_of=self.state.as_of,
        )
        self.memory = _CreateOnceMemory()

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def _run(self):
        return run_paper_portfolio_rollover_service(
            state=self.state,
            request=self.request,
            state_root=self.state_root,
            bucket="paper-state-bucket",
            writer=self.memory,
        )

    def test_service_persists_publishes_terminal_last_and_retries_idempotently(self) -> None:
        completed = self._run()

        self.assertEqual(completed.request_id, self.request.request_id)
        self.assertEqual(completed.rollover.paper_portfolio_state_id, self.state.state_id)
        self.assertEqual(
            LocalPaperPortfolioRolloverStore(
                self.state_root / "paper_portfolio_rollovers"
            ).get(completed.rollover.rollover_id),
            completed.rollover,
        )
        self.assertEqual(
            LocalSwingPortfolioArtifactStore(self.state_root / "portfolio").get(
                completed.rollover.portfolio_artifact.artifact_id
            ),
            completed.rollover.portfolio_artifact,
        )
        names = [value["object_name"] for value in self.memory.write_calls]
        self.assertIn("/portfolio-artifacts/", names[0])
        self.assertIn("/rollovers/", names[1])
        self.assertIn("/manifests/", names[2])
        self.assertEqual(
            decode_paper_portfolio_rollover_publication_manifest(
                self.memory.objects[
                    (
                        "paper-state-bucket",
                        completed.publication.manifest_object.object_name,
                        completed.publication.manifest_object.generation,
                    )
                ]
            ),
            completed.publication.manifest,
        )

        second = self._run()
        self.assertEqual(second, completed)
        self.assertEqual(len(self.memory.current), 3)

    def test_exact_generation_restore_is_idempotent(self) -> None:
        completed = self._run()
        restored_root = self.root / "restored"
        rollover_store = LocalPaperPortfolioRolloverStore(
            restored_root / "paper_portfolio_rollovers"
        )
        portfolio_store = LocalSwingPortfolioArtifactStore(
            restored_root / "portfolio"
        )
        publication = completed.publication

        restored = restore_paper_portfolio_rollover(
            expected_state_id=self.state.state_id,
            expected_rollover_id=completed.rollover.rollover_id,
            bucket="paper-state-bucket",
            manifest_object_name=publication.manifest_object.object_name,
            manifest_generation=publication.manifest_object.generation,
            manifest_sha256=publication.manifest_object.sha256,
            reader=self.memory,
            rollover_store=rollover_store,
            portfolio_store=portfolio_store,
        )
        second = restore_paper_portfolio_rollover(
            expected_state_id=self.state.state_id,
            expected_rollover_id=completed.rollover.rollover_id,
            bucket="paper-state-bucket",
            manifest_object_name=publication.manifest_object.object_name,
            manifest_generation=publication.manifest_object.generation,
            manifest_sha256=publication.manifest_object.sha256,
            reader=self.memory,
            rollover_store=rollover_store,
            portfolio_store=portfolio_store,
        )

        self.assertEqual(restored, completed.rollover)
        self.assertEqual(second, restored)

    def test_tampered_pinned_object_writes_no_restore_state(self) -> None:
        completed = self._run()
        publication = completed.publication
        target = publication.manifest.rollover_object
        key = ("paper-state-bucket", target.object_name, target.generation)
        self.memory.objects[key] += b"tampered"
        rejected_root = self.root / "rejected"

        with self.assertRaises(PaperPortfolioRolloverPublicationError):
            restore_paper_portfolio_rollover(
                expected_state_id=self.state.state_id,
                expected_rollover_id=completed.rollover.rollover_id,
                bucket="paper-state-bucket",
                manifest_object_name=publication.manifest_object.object_name,
                manifest_generation=publication.manifest_object.generation,
                manifest_sha256=publication.manifest_object.sha256,
                reader=self.memory,
                rollover_store=LocalPaperPortfolioRolloverStore(
                    rejected_root / "paper_portfolio_rollovers"
                ),
                portfolio_store=LocalSwingPortfolioArtifactStore(
                    rejected_root / "portfolio"
                ),
            )

        self.assertFalse(rejected_root.exists())

    def test_request_tampering_and_cutoff_drift_fail_before_publication(self) -> None:
        object.__setattr__(self.request, "as_of", self.state.as_of + timedelta(seconds=1))
        with self.assertRaises(PaperPortfolioServiceError):
            self._run()
        self.assertEqual(self.memory.write_calls, [])

    def test_capability_surface_has_no_listing_or_latest_selection(self) -> None:
        for value in (
            PaperPortfolioRolloverRequest,
            run_paper_portfolio_rollover_service,
            restore_paper_portfolio_rollover,
        ):
            names = tuple(name.casefold() for name in dir(value))
            self.assertFalse(any("list_blob" in name or "latest" in name for name in names))

    def _actual_open_batch(self):
        calendar = outcome_fixtures._calendar()
        observation = outcome_fixtures._observation(calendar, date(2026, 1, 2))
        evidence = outcome_operational_fixtures._evidence((observation,))
        job = outcome_operational_fixtures._spec(
            evidence,
            as_of=datetime(2026, 1, 3, tzinfo=UTC),
        )
        batch = PaperPortfolioBatchSpec(as_of=job.as_of, outcome_jobs=(job,))
        ledger = LocalPaperTradeLedger(self.root / "bridge" / "paper")
        ledger.register_value(evidence.registration)
        source = outcome_operational_fixtures._EvidenceSource(evidence)
        outcome_store = LocalPaperOutcomeRunStore(
            self.root / "bridge" / "paper_outcomes"
        )
        state = run_paper_portfolio_batch(
            spec=batch,
            evidence_source=source,
            ledger=ledger,
            outcome_store=outcome_store,
            portfolio_store=LocalPaperPortfolioStateStore(
                self.root / "bridge" / "paper_portfolio"
            ),
        )
        self.assertIs(state.positions[0].outcome_status, PaperOutcomeStatus.OPEN)
        return state, batch, evidence, source, outcome_store, observation

    def test_exact_terminal_observation_is_sealed_as_the_open_position_mark(self) -> None:
        state, batch, _, source, outcome_store, observation = self._actual_open_batch()
        lineage = PaperPortfolioRolloverLineage(
            genesis_artifact_id=self.genesis.artifact_id,
            previous_rollover_id=None,
        )

        request = prepare_paper_portfolio_rollover_request(
            state=state,
            spec=batch,
            lineage=lineage,
            evidence_source=source,
            outcome_store=outcome_store,
        )

        self.assertEqual(len(request.marks), 1)
        self.assertEqual(request.marks[0].observation_id, observation.observation_id)
        self.assertEqual(request.marks[0].artifact_id, observation.artifact_id)
        self.assertEqual(request.marks[0].close, observation.close)
        self.assertEqual(request.marks[0].knowledge_time, observation.knowledge_time)

    def test_missing_terminal_bar_never_falls_back_to_an_older_price(self) -> None:
        state, batch, evidence, _, outcome_store, _ = self._actual_open_batch()
        missing = outcome_fixtures._observation(
            evidence.calendar,
            date(2026, 1, 3),
            traded=False,
        )
        source = outcome_operational_fixtures._EvidenceSource(
            outcome_operational_fixtures._evidence(
                evidence.observations + (missing,)
            )
        )

        with self.assertRaisesRegex(PaperPortfolioServiceError, "marks"):
            prepare_paper_portfolio_rollover_request(
                state=state,
                spec=batch,
                lineage=PaperPortfolioRolloverLineage(
                    genesis_artifact_id=self.genesis.artifact_id,
                    previous_rollover_id=None,
                ),
                evidence_source=source,
                outcome_store=outcome_store,
            )

    def test_portfolio_operational_service_builds_and_returns_the_sealed_rollover(self) -> None:
        _, batch, evidence, _, _, _ = self._actual_open_batch()
        service_root = self.root / "closed_loop_service"
        evidence_root = self.root / "closed_loop_evidence"
        evidence_root.mkdir()
        ledger = LocalPaperTradeLedger(service_root / "paper")
        ledger.register_value(evidence.registration)
        LocalSwingPortfolioArtifactStore(service_root / "portfolio").put(
            self.genesis
        )
        source = outcome_operational_fixtures._EvidenceSource(evidence)
        writer = _CreateOnceMemory()
        telegram = _TelegramTransport()
        lineage = PaperPortfolioRolloverLineage(
            genesis_artifact_id=self.genesis.artifact_id,
            previous_rollover_id=None,
        )

        with patch(
            "india_swing.paper_outcomes.portfolio_service.LocalPaperOutcomeEvidenceSource",
            return_value=source,
        ):
            completed = run_paper_portfolio_operational_service(
                spec=batch,
                evidence_root=evidence_root,
                state_root=service_root,
                bucket="paper-state-bucket",
                writer=writer,
                telegram_config=TelegramBotConfig(
                    bot_token="12345:" + "a" * 20,
                    chat_id="123456",
                ),
                telegram_transport=telegram,
                clock=lambda: batch.as_of + timedelta(hours=1),
                rollover_lineage=lineage,
            )
            replayed = run_paper_portfolio_operational_service(
                spec=batch,
                evidence_root=evidence_root,
                state_root=service_root,
                bucket="paper-state-bucket",
                writer=writer,
                telegram_config=TelegramBotConfig(
                    bot_token="12345:" + "a" * 20,
                    chat_id="123456",
                ),
                telegram_transport=telegram,
                clock=lambda: batch.as_of + timedelta(hours=2),
                rollover_lineage=lineage,
            )

        self.assertIsNotNone(completed.rollover)
        self.assertIsNotNone(completed.rollover_publication)
        self.assertEqual(
            completed.rollover.paper_portfolio_state_id,
            completed.state.state_id,
        )
        self.assertEqual(
            completed.rollover_request_id,
            PaperPortfolioRolloverRequest(
                genesis_artifact_id=self.genesis.artifact_id,
                previous_rollover_id=None,
                marks=completed.rollover.marks,
                as_of=batch.as_of,
            ).request_id,
        )
        self.assertIn(
            "/manifests/",
            completed.rollover_publication.manifest_object.object_name,
        )
        self.assertEqual(telegram.calls, 1)
        self.assertEqual(replayed, completed)


if __name__ == "__main__":
    unittest.main()
