from __future__ import annotations

import ast
import io
import json
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from dataclasses import replace
from datetime import timedelta
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch

from india_swing.identity import content_id
from india_swing.operations import (
    LocalSwingPortfolioArtifactStore,
    SwingOperationalJobError,
    SwingPortfolioArtifactError,
    SwingPortfolioEvidenceBinding,
    SwingPortfolioEvidenceKind,
    SwingPortfolioSnapshotArtifact,
    build_swing_operational_run_spec,
    decode_swing_portfolio_artifact,
    encode_swing_operational_job_spec,
    encode_swing_portfolio_artifact,
    job_spec_from_operational_spec,
    load_swing_operational_job_spec_file,
    parse_swing_operational_job_spec,
    run_swing_operational_job,
)
from india_swing.operational_job import main as operational_job_main
from india_swing.risk.swing_portfolio import SwingPortfolioSnapshot
from india_swing.signals.proposal_artifacts import LocalSwingProposalBatchStore
from india_swing.signals.proposal_parent_store import (
    LocalSwingProposalParentStore,
    publish_swing_proposal_with_parents,
)

from tests import test_swing_opportunity_ranking as ranking_fixtures
from tests.test_market_data import FakeKiteClient, adapter as kite_adapter
from tests.test_swing_operational_run import SequenceClock


D = Decimal
_REPO_ROOT = Path(__file__).resolve().parent.parent


def _raw_quote(value) -> dict[str, object]:
    return {
        "depth": {
            "buy": [
                {
                    "orders": item.orders,
                    "price": str(item.price),
                    "quantity": item.quantity,
                }
                for item in value.depth_buy
            ],
            "sell": [
                {
                    "orders": item.orders,
                    "price": str(item.price),
                    "quantity": item.quantity,
                }
                for item in value.depth_sell
            ],
        },
        "instrument_token": value.instrument_token,
        "last_price": str(value.last_price),
        "last_trade_time": (
            None
            if value.last_trade_time is None
            else value.last_trade_time.replace(tzinfo=None)
        ),
        "lower_circuit_limit": str(value.lower_circuit_limit),
        "timestamp": value.exchange_timestamp.replace(tzinfo=None),
        "upper_circuit_limit": str(value.upper_circuit_limit),
    }


class SwingOperationalJobTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        fixture = ranking_fixtures.SwingOpportunityRankingBatchTests(
            methodName="test_ranks_every_pass_with_explainable_bounded_components"
        )
        fixture.setUp()
        cls.quote_fixture = fixture.fixture
        cls.batch = cls.quote_fixture.proposal_batch
        cls.quote_batch = fixture.quote_batch
        cls.evaluated_at = cls.quote_fixture.evaluated_at
        cls.portfolio = SwingPortfolioSnapshot(
            capital=D("100000"),
            cash_available=D("100000"),
            gross_exposure=D("0"),
            open_risk=D("0"),
            open_positions=0,
            daily_realized_pnl=D("0"),
            pilot_realized_pnl=D("0"),
            as_of=cls.evaluated_at - timedelta(seconds=1),
        )
        cls.portfolio_artifact = cls._portfolio_artifact(cls.portfolio)
        cls.operational_spec = build_swing_operational_run_spec(
            proposal_batch=cls.batch,
        )
        cls.job_spec = job_spec_from_operational_spec(
            operational_spec=cls.operational_spec,
            portfolio_artifact_id=cls.portfolio_artifact.artifact_id,
            portfolio_snapshot_id=cls.portfolio.portfolio_snapshot_id,
        )

    @staticmethod
    def _portfolio_artifact(portfolio: SwingPortfolioSnapshot):
        evidence = tuple(
            SwingPortfolioEvidenceBinding(
                kind=kind,
                evidence_id=content_id(
                    {
                        "kind": kind.value,
                        "portfolio_snapshot_id": portfolio.portfolio_snapshot_id,
                    },
                    length=64,
                ),
                observed_at=portfolio.as_of,
                source_version="test-reconciliation/v1",
            )
            for kind in SwingPortfolioEvidenceKind
        )
        return SwingPortfolioSnapshotArtifact(
            portfolio=portfolio,
            portfolio_snapshot_id=portfolio.portfolio_snapshot_id,
            evidence=evidence,
            reconciled_at=portfolio.as_of,
        )

    def _prepare_state(self, root: Path, *, artifact=None) -> None:
        graph_root = root / "proposal_graph"
        publish_swing_proposal_with_parents(
            batch=self.batch,
            proposal_store=LocalSwingProposalBatchStore(graph_root),
            parent_store=LocalSwingProposalParentStore(graph_root),
        )
        LocalSwingPortfolioArtifactStore(root / "portfolio").put(
            artifact or self.portfolio_artifact
        )

    def _adapter(self, client: FakeKiteClient):
        return kite_adapter(client, clock=lambda: self.evaluated_at)

    def _client(self) -> FakeKiteClient:
        return FakeKiteClient(
            quotes={
                value.listing_key: _raw_quote(value)
                for value in self.quote_batch.quotes
            }
        )

    def test_portfolio_artifact_round_trip_store_and_required_evidence(self) -> None:
        payload = encode_swing_portfolio_artifact(self.portfolio_artifact)
        decoded = decode_swing_portfolio_artifact(payload)
        self.assertEqual(decoded, self.portfolio_artifact)
        self.assertEqual(encode_swing_portfolio_artifact(decoded), payload)
        with tempfile.TemporaryDirectory() as directory:
            store = LocalSwingPortfolioArtifactStore(Path(directory))
            self.assertEqual(store.put(decoded), decoded)
            self.assertEqual(store.put(decoded), decoded)
            self.assertEqual(store.get(decoded.artifact_id), decoded)
        with self.assertRaises(SwingPortfolioArtifactError):
            replace(self.portfolio_artifact, evidence=self.portfolio_artifact.evidence[:-1])

    def test_portfolio_decoder_rejects_duplicate_float_extra_and_identity_tamper(self) -> None:
        payload = encode_swing_portfolio_artifact(self.portfolio_artifact)
        invalid = [
            payload.replace(
                b'{"artifact":',
                b'{"codec_schema_version":"swing-portfolio-artifact-json/v1","artifact":',
                1,
            ),
        ]
        for mutation in ("float", "extra", "identity"):
            raw = json.loads(payload)
            if mutation == "float":
                raw["artifact"]["portfolio"]["open_positions"] = 0.0
            elif mutation == "extra":
                raw["artifact"]["unexpected"] = True
            else:
                raw["artifact"]["artifact_id"] = "0" * 64
            invalid.append(json.dumps(raw, separators=(",", ":"), sort_keys=True).encode())
        for value in invalid:
            with self.subTest(value=value[:80]):
                with self.assertRaises(SwingPortfolioArtifactError):
                    decode_swing_portfolio_artifact(value)

    def test_job_spec_round_trip_file_boundary_and_tamper_rejection(self) -> None:
        payload = encode_swing_operational_job_spec(self.job_spec)
        self.assertEqual(parse_swing_operational_job_spec(payload), self.job_spec)
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory, "job.json").resolve()
            path.write_bytes(payload)
            self.assertEqual(load_swing_operational_job_spec_file(path), self.job_spec)
            path.write_bytes(payload.replace(self.job_spec.job_spec_id.encode(), b"0" * 64))
            with self.assertRaises(Exception):
                load_swing_operational_job_spec_file(path)

    def test_real_read_only_kite_adapter_runs_and_is_idempotent(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            self._prepare_state(root)
            client = self._client()
            record = run_swing_operational_job(
                job_spec=self.job_spec,
                state_root=root,
                kite_adapter=self._adapter(client),
                clock=SequenceClock(
                    self.evaluated_at - timedelta(milliseconds=500),
                    self.evaluated_at,
                    self.evaluated_at + timedelta(milliseconds=500),
                ),
            )
            second = run_swing_operational_job(
                job_spec=self.job_spec,
                state_root=root,
                kite_adapter=self._adapter(client),
                clock=lambda: self.evaluated_at,
            )

            self.assertEqual(record, second)
            self.assertEqual(record.proposal_batch_id, self.batch.batch_id)
            self.assertEqual(
                record.portfolio_snapshot_id,
                self.portfolio.portfolio_snapshot_id,
            )
            self.assertEqual(len(client.quote_calls), 1)
            self.assertTrue((root / "operational").is_dir())
            self.assertTrue((root / "decision_outbox").is_dir())
            self.assertTrue((root / "paper").is_dir())

    def test_idempotent_restart_rejects_missing_terminal_side_effects(self) -> None:
        for missing_artifact in ("notification", "paper_registration"):
            with self.subTest(missing_artifact=missing_artifact):
                with tempfile.TemporaryDirectory() as directory:
                    root = Path(directory).resolve()
                    self._prepare_state(root)
                    client = self._client()
                    record = run_swing_operational_job(
                        job_spec=self.job_spec,
                        state_root=root,
                        kite_adapter=self._adapter(client),
                        clock=SequenceClock(
                            self.evaluated_at - timedelta(milliseconds=500),
                            self.evaluated_at,
                            self.evaluated_at + timedelta(milliseconds=500),
                        ),
                    )
                    self.assertIsNotNone(record.decision_id)
                    self.assertIsNotNone(record.paper_registration_id)
                    if missing_artifact == "notification":
                        path = (
                            root
                            / "decision_outbox"
                            / "notifications"
                            / f"{record.decision_id}.json"
                        )
                    else:
                        path = (
                            root
                            / "paper"
                            / "registrations"
                            / f"{record.paper_registration_id}.json"
                        )
                    path.unlink()

                    with self.assertRaisesRegex(
                        SwingOperationalJobError,
                        "existing terminal side effects are invalid",
                    ):
                        run_swing_operational_job(
                            job_spec=self.job_spec,
                            state_root=root,
                            kite_adapter=self._adapter(client),
                            clock=lambda: self.evaluated_at,
                        )
                    self.assertEqual(len(client.quote_calls), 1)

    def test_missing_stale_or_wrong_portfolio_fails_before_any_quote(self) -> None:
        cases = []
        with tempfile.TemporaryDirectory() as first, tempfile.TemporaryDirectory() as second:
            missing_root = Path(first).resolve()
            graph_root = missing_root / "proposal_graph"
            publish_swing_proposal_with_parents(
                batch=self.batch,
                proposal_store=LocalSwingProposalBatchStore(graph_root),
                parent_store=LocalSwingProposalParentStore(graph_root),
            )
            cases.append((missing_root, self.job_spec))

            stale_root = Path(second).resolve()
            stale_portfolio = replace(
                self.portfolio,
                as_of=self.job_spec.decision_not_before - timedelta(minutes=10),
            )
            stale_artifact = self._portfolio_artifact(stale_portfolio)
            self._prepare_state(stale_root, artifact=stale_artifact)
            stale_spec = replace(
                self.job_spec,
                portfolio_artifact_id=stale_artifact.artifact_id,
                portfolio_snapshot_id=stale_portfolio.portfolio_snapshot_id,
                maximum_portfolio_age_seconds=60,
            )
            cases.append((stale_root, stale_spec))

            for root, spec in cases:
                client = self._client()
                with self.subTest(root=root):
                    with self.assertRaises(SwingOperationalJobError):
                        run_swing_operational_job(
                            job_spec=spec,
                            state_root=root,
                            kite_adapter=self._adapter(client),
                            clock=lambda: self.evaluated_at,
                        )
                    self.assertEqual(client.quote_calls, [])

    def test_policy_or_window_drift_fails_before_any_quote(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            self._prepare_state(root)
            for spec in (
                replace(self.job_spec, quote_policy_id="0" * 64),
                replace(
                    self.job_spec,
                    decision_deadline=self.job_spec.decision_deadline - timedelta(seconds=1),
                ),
            ):
                client = self._client()
                with self.subTest(job_spec_id=spec.job_spec_id):
                    with self.assertRaises(SwingOperationalJobError):
                        run_swing_operational_job(
                            job_spec=spec,
                            state_root=root,
                            kite_adapter=self._adapter(client),
                            clock=lambda: self.evaluated_at,
                        )
                    self.assertEqual(client.quote_calls, [])

    def test_cli_arguments_and_runtime_failures_are_sanitized(self) -> None:
        secret = "distinct-secret-path-and-token"
        stderr = io.StringIO()
        with redirect_stderr(stderr):
            code = operational_job_main(["--unknown", secret], environ={})
        self.assertEqual(code, 2)
        self.assertEqual(
            json.loads(stderr.getvalue()),
            {"error_type": "SwingOperationalJobError", "status": "FAILED"},
        )
        self.assertNotIn(secret, stderr.getvalue())

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            path = root / "job.json"
            path.write_bytes(encode_swing_operational_job_spec(self.job_spec))
            stderr = io.StringIO()
            with (
                patch(
                    "india_swing.operational_job.KiteMarketDataAdapter.from_official_sdk"
                ) as sdk,
                redirect_stderr(stderr),
            ):
                code = operational_job_main(
                    ["--spec-file", str(path), "--state-root", str(root)],
                    environ={
                        "INDIA_SWING_KITE_API_KEY": secret,
                        "INDIA_SWING_KITE_ACCESS_TOKEN": secret,
                    },
                )
            self.assertEqual(code, 2)
            sdk.assert_not_called()
            self.assertNotIn(secret, stderr.getvalue())

            fake_record = type("Record", (), {})()
            fake_record.action = type("Action", (), {"value": "NO_TRADE"})()
            fake_record.failure_codes = ()
            fake_record.message = "Research and paper-trading only. No order will be placed.\n"
            fake_record.record_id = "1" * 64
            fake_record.run_id = "2" * 64
            fake_record.spec_id = self.job_spec.expected_operational_spec_id
            fake_record.status = type("Status", (), {"value": "COMPLETE"})()
            fake_record.target_session = self.job_spec.target_session
            fake_manifest = type("Manifest", (), {})()
            fake_manifest.publication_id = "3" * 64
            fake_manifest_object = type("ManifestObject", (), {})()
            fake_manifest_object.generation = 7
            fake_manifest_object.object_name = "operational-state/manifest.json"
            fake_manifest_object.sha256 = "4" * 64
            fake_publication = type("Publication", (), {})()
            fake_publication.manifest = fake_manifest
            fake_publication.manifest_object = fake_manifest_object
            fake_telegram_receipt = type("TelegramReceipt", (), {})()
            fake_telegram_receipt.receipt_id = "5" * 64
            stdout = io.StringIO()
            stderr = io.StringIO()
            with (
                patch(
                    "india_swing.operational_job.KiteMarketDataAdapter.from_official_sdk",
                    return_value=object(),
                ),
                patch(
                    "india_swing.operational_job.run_swing_operational_job",
                    return_value=fake_record,
                ) as run,
                patch(
                    "india_swing.operational_job.GoogleCloudStorageStateObjectWriter",
                    return_value=object(),
                ),
                patch(
                    "india_swing.operational_job.publish_swing_operational_state_to_gcs",
                    return_value=fake_publication,
                ) as publish,
                patch(
                    "india_swing.operational_job.deliver_telegram_notification",
                    return_value=fake_telegram_receipt,
                ) as deliver,
                redirect_stdout(stdout),
                redirect_stderr(stderr),
            ):
                code = operational_job_main(
                    ["--spec-file", str(path), "--state-root", str(root)],
                    environ={
                        "INDIA_SWING_KITE_API_KEY": secret,
                        "INDIA_SWING_KITE_ACCESS_TOKEN": secret,
                        "INDIA_SWING_OPERATIONAL_STATE_BUCKET": "india-swing-private",
                        "INDIA_SWING_TELEGRAM_BOT_TOKEN": (
                            "12345:abcdefghijklmnopqrstuvwxyz_123456"
                        ),
                        "INDIA_SWING_TELEGRAM_CHAT_ID": "123456789",
                    },
                )
            self.assertEqual(code, 0)
            self.assertEqual(stderr.getvalue(), "")
            self.assertNotIn(secret, stdout.getvalue())
            run.assert_called_once()
            publish.assert_called_once()
            deliver.assert_called_once()

    def test_job_modules_have_no_order_or_dynamic_execution_capability(self) -> None:
        for relative in (
            "src/india_swing/operations/job.py",
            "src/india_swing/operational_job.py",
        ):
            source = (_REPO_ROOT / relative).read_text(encoding="utf-8")
            lowered = source.casefold()
            for forbidden in (
                "place_order",
                "modify_order",
                "cancel_order",
                "pickle",
                "importlib",
                "subprocess",
                "eval(",
                "exec(",
            ):
                self.assertNotIn(forbidden, lowered)
            ast.parse(source)


if __name__ == "__main__":
    unittest.main()
