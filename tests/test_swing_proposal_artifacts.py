from __future__ import annotations

import io
import json
import tempfile
import unittest
from contextlib import redirect_stdout
from dataclasses import replace
from datetime import timedelta, timezone
from decimal import Decimal
from pathlib import Path

from india_swing.operations import (
    LocalSwingOperationalRunStore,
    SwingOperationalError,
    build_stored_swing_operational_run_spec,
    run_and_publish_stored_swing_operation,
)
from india_swing.operations.cli import main as operational_cli
from india_swing.paper_trades import LocalPaperTradeLedger
from india_swing.recommendations import LocalSwingDecisionOutbox
from india_swing.risk.swing_portfolio import SwingPortfolioSnapshot
from india_swing.signals.proposal_artifacts import (
    FixedSwingProposalBatchInputResolver,
    LocalSwingProposalBatchStore,
    SwingProposalArtifactError,
    SwingProposalBatchManifest,
    decode_swing_proposal_manifest,
    encode_swing_proposal_manifest,
    manifest_from_proposal_batch,
    replay_swing_proposal_batch,
)

from tests import test_swing_opportunity_ranking as ranking_fixtures
from tests.test_swing_operational_run import (
    FakePortfolioSource,
    FakeQuoteSource,
    SequenceClock,
)


D = Decimal


class RecordingResolver:
    def __init__(self, batch) -> None:
        self.batch = batch
        self.calls: list[tuple[str, str]] = []

    def get_universe_batch(self, value: str):
        self.calls.append(("universe", value))
        return self.batch.universe_batch

    def get_calendar_snapshot(self, value: str):
        self.calls.append(("calendar", value))
        return self.batch.calendar

    def get_signal_config(self, value: str):
        self.calls.append(("config", value))
        return self.batch.config


class FailingResolver:
    def get_universe_batch(self, value: str):
        raise RuntimeError("secret bucket and credential detail")

    def get_calendar_snapshot(self, value: str):
        raise AssertionError("must not continue after a failed parent")

    def get_signal_config(self, value: str):
        raise AssertionError("must not continue after a failed parent")


class SwingProposalArtifactTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        fixture = ranking_fixtures.SwingOpportunityRankingBatchTests(
            methodName="test_ranks_every_pass_with_explainable_bounded_components"
        )
        fixture.setUp()
        cls.fixture = fixture.fixture
        cls.quote_batch = fixture.quote_batch
        cls.batch = cls.fixture.proposal_batch
        cls.manifest = manifest_from_proposal_batch(cls.batch)
        cls.resolver = FixedSwingProposalBatchInputResolver(
            universe_batch=cls.batch.universe_batch,
            calendar=cls.batch.calendar,
            config=cls.batch.config,
        )

    def test_manifest_round_trip_is_canonical_and_complete(self) -> None:
        payload = encode_swing_proposal_manifest(self.manifest)
        decoded = decode_swing_proposal_manifest(payload)

        self.assertEqual(decoded, self.manifest)
        self.assertEqual(encode_swing_proposal_manifest(decoded), payload)
        self.assertEqual(
            decoded.assembly_ids,
            tuple(value.assembly_id for value in self.batch.universe_batch.assemblies),
        )
        self.assertEqual(
            decoded.proposal_ids,
            tuple(value.proposal_id for value in self.batch.proposals),
        )
        self.assertEqual(
            decoded.veto_ids,
            tuple(value.veto_id for value in self.batch.vetoes),
        )
        decoded.verify_content_identity()

    def test_replay_requests_only_exact_pinned_parent_ids(self) -> None:
        resolver = RecordingResolver(self.batch)
        rebuilt = replay_swing_proposal_batch(self.manifest, resolver)

        self.assertEqual(rebuilt, self.batch)
        self.assertEqual(
            resolver.calls,
            [
                ("universe", self.manifest.universe_batch_id),
                ("calendar", self.manifest.calendar_snapshot_id),
                ("config", self.manifest.signal_config_id),
            ],
        )
        for owner in (resolver, LocalSwingProposalBatchStore):
            self.assertFalse(any("latest" in name.casefold() for name in dir(owner)))

    def test_valid_self_consistent_manifest_cannot_override_replayed_evidence(self) -> None:
        tampered = replace(
            self.manifest,
            proposal_ids=tuple(reversed(self.manifest.proposal_ids)),
        )
        tampered.verify_content_identity()

        with self.assertRaisesRegex(
            SwingProposalArtifactError,
            "replayed proposal batch differs",
        ):
            replay_swing_proposal_batch(tampered, self.resolver)

    def test_resolver_failure_is_sanitized_and_stops_immediately(self) -> None:
        with self.assertRaises(SwingProposalArtifactError) as caught:
            replay_swing_proposal_batch(self.manifest, FailingResolver())
        self.assertEqual(
            str(caught.exception),
            "proposal inputs could not be resolved safely",
        )
        self.assertNotIn("secret", str(caught.exception))

    def test_decoder_rejects_duplicate_keys_floats_unknown_fields_and_bad_identity(self) -> None:
        payload = encode_swing_proposal_manifest(self.manifest)
        raw = json.loads(payload)
        invalid_payloads = [
            b'{"codec_schema_version":"swing-proposal-artifact-json/v1",'
            b'"codec_schema_version":"swing-proposal-artifact-json/v1",'
            b'"manifest":{}}',
            payload.replace(
                f'"scoped_subject_count":{self.manifest.scoped_subject_count}'.encode(),
                f'"scoped_subject_count":{self.manifest.scoped_subject_count}.0'.encode(),
            ),
            payload.replace(
                self.manifest.cutoff.isoformat().encode(),
                self.manifest.cutoff.astimezone(
                    timezone(timedelta(hours=5, minutes=30))
                ).isoformat().encode(),
            ),
        ]
        extra = dict(raw)
        extra["unexpected"] = True
        invalid_payloads.append(json.dumps(extra).encode())
        wrong_id = dict(raw)
        wrong_id["manifest"] = dict(raw["manifest"])
        wrong_id["manifest"]["manifest_id"] = "0" * 64
        invalid_payloads.append(json.dumps(wrong_id).encode())

        for invalid in invalid_payloads:
            with self.subTest(payload=invalid[:80]):
                with self.assertRaises(SwingProposalArtifactError):
                    decode_swing_proposal_manifest(invalid)

    def test_manifest_rejects_bool_counts_duplicates_and_excessive_coverage(self) -> None:
        with self.assertRaises(SwingProposalArtifactError):
            replace(self.manifest, scoped_subject_count=True)
        with self.assertRaises(SwingProposalArtifactError):
            replace(
                self.manifest,
                proposal_ids=(self.manifest.proposal_ids[0],) * 2,
                assembly_ids=(self.manifest.assembly_ids[0],) * 2,
                proposal_subject_count=2,
            )
        with self.assertRaises(SwingProposalArtifactError):
            SwingProposalBatchManifest(
                **{
                    name: getattr(self.manifest, name)
                    for name in (
                        "proposal_batch_id",
                        "universe_batch_id",
                        "universe_snapshot_id",
                        "calendar_snapshot_id",
                        "signal_config_id",
                        "signal_session",
                        "cutoff",
                        "assembly_ids",
                        "proposal_ids",
                        "veto_ids",
                        "proposal_subject_count",
                        "veto_subject_count",
                    )
                },
                scoped_subject_count=10001,
            )

    def test_create_once_store_loads_exact_batch_and_detects_tamper(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            store = LocalSwingProposalBatchStore(Path(directory))
            first = store.publish(self.batch)
            second = store.publish(self.batch)

            self.assertEqual(first, second)
            self.assertEqual(store.list_manifests(), (first,))
            self.assertEqual(store.load(self.batch.batch_id, self.resolver), self.batch)
            self.assertEqual(store.require_persisted(self.batch, self.resolver), self.batch)

            path = store.path_for(self.batch.batch_id)
            path.write_bytes(
                path.read_bytes().replace(
                    self.manifest.calendar_snapshot_id.encode(),
                    ("0" * 64).encode(),
                )
            )
            with self.assertRaises(SwingProposalArtifactError):
                store.get_manifest(self.batch.batch_id)

    def test_store_rejects_unsafe_ids_and_unexpected_file_set(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            store = LocalSwingProposalBatchStore(Path(directory))
            for value in ("../escape", "A" * 64, "0" * 63, object()):
                with self.subTest(value=value):
                    with self.assertRaises(SwingProposalArtifactError):
                        store.path_for(value)
            store.manifests_root.mkdir(parents=True)
            (store.manifests_root / "unexpected.txt").write_text("no", encoding="utf-8")
            with self.assertRaises(SwingProposalArtifactError):
                store.list_manifests()

    def test_operational_service_loads_by_exact_id_and_runs_paper_only(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            proposal_store = LocalSwingProposalBatchStore(root / "proposals")
            proposal_store.publish(self.batch)
            spec = build_stored_swing_operational_run_spec(
                proposal_batch_id=self.batch.batch_id,
                proposal_store=proposal_store,
                proposal_resolver=self.resolver,
                quote_chunk_size=1,
            )
            evaluated_at = self.fixture.evaluated_at
            portfolio = SwingPortfolioSnapshot(
                capital=D("100000"),
                cash_available=D("100000"),
                gross_exposure=D("0"),
                open_risk=D("0"),
                open_positions=0,
                daily_realized_pnl=D("0"),
                pilot_realized_pnl=D("0"),
                as_of=evaluated_at,
            )
            result, record = run_and_publish_stored_swing_operation(
                proposal_batch_id=self.batch.batch_id,
                proposal_store=proposal_store,
                proposal_resolver=self.resolver,
                quote_source=FakeQuoteSource(self.quote_batch),
                portfolio_source=FakePortfolioSource(portfolio),
                clock=SequenceClock(
                    evaluated_at,
                    evaluated_at,
                    evaluated_at,
                ),
                run_store=LocalSwingOperationalRunStore(root / "runs"),
                decision_outbox=LocalSwingDecisionOutbox(root / "outbox"),
                paper_ledger=LocalPaperTradeLedger(root / "paper"),
                quote_chunk_size=1,
            )

            self.assertEqual(spec.proposal_batch.batch_id, self.batch.batch_id)
            self.assertEqual(result.spec.spec_id, spec.spec_id)
            self.assertEqual(record.proposal_batch_id, self.batch.batch_id)
            self.assertFalse(result.execution_eligible)

            with self.assertRaises(SwingOperationalError):
                build_stored_swing_operational_run_spec(
                    proposal_batch_id="0" * 64,
                    proposal_store=proposal_store,
                    proposal_resolver=self.resolver,
                )

    def test_cli_lists_and_shows_only_sanitized_manifest_summary(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            store = LocalSwingProposalBatchStore(root)
            store.publish(self.batch)
            for command in (
                ["proposal-list"],
                ["proposal-show", "--proposal-batch-id", self.batch.batch_id],
            ):
                output = io.StringIO()
                with redirect_stdout(output):
                    code = operational_cli(
                        ["--proposal-root", str(root), *command]
                    )
                self.assertEqual(code, 0)
                value = output.getvalue()
                self.assertIn(self.batch.batch_id, value)
                self.assertNotIn("bars", value)
                self.assertNotIn("api_key", value)


if __name__ == "__main__":
    unittest.main()
