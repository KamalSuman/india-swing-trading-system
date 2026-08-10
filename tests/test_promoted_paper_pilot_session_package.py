from __future__ import annotations

import ast
import hashlib
import inspect
import json
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from unittest import mock

import india_swing.promoted_paper_pilot_session_package as package_module
import india_swing.promoted_paper_pilot_session_package_cli as package_cli_module
from india_swing._exact_replay import ExactReplayScope
from india_swing.operations.portfolio_store import (
    LocalSwingPortfolioArtifactStore,
    SwingPortfolioEvidenceKind,
    SwingPortfolioSnapshotArtifact,
)
from india_swing.promoted_operational_assembly import (
    PromotedOperationalAssemblySpec,
    PromotedOperationalRuntimeAssembly,
)
from india_swing.promoted_operational_launch import (
    LaunchAllocationPolicyRequest,
    LaunchQuoteGatePolicyRequest,
    LaunchSizingPolicyRequest,
)
from india_swing.promoted_operational_preparation import (
    LocalPromotedOperationalPreparationStore,
    VerifiedPromotedOperationalPreparation,
)
from india_swing.promoted_paper_pilot_session_package import (
    MAXIMUM_SESSION_PACKAGE_REQUEST_BYTES,
    PROMOTED_PAPER_PILOT_SESSION_PACKAGE_REQUEST_SCHEMA_VERSION,
    PromotedPaperPilotSessionPackageError,
    PromotedPaperPilotSessionPackageRequest,
    decode_promoted_paper_pilot_session_package_request,
    encode_promoted_paper_pilot_session_package_request,
    prepare_promoted_paper_pilot_first_session_package,
)
from india_swing.promoted_paper_portfolio_genesis import (
    MANUAL_RECONCILIATION_ACK,
    GenesisEvidenceDescriptor,
    LocalPromotedPortfolioEvidenceArchive,
    PromotedPaperPortfolioGenesisRequest,
    encode_promoted_paper_portfolio_genesis_request,
)

from tests import test_promoted_operational_preparation as _prep_tests

D = Decimal
UTC = timezone.utc
_IST = timezone(timedelta(hours=5, minutes=30))
_QUOTE_SOURCE_ID = "8" * 64


def _decision_window_for(target_session):
    not_before = datetime(
        target_session.year, target_session.month, target_session.day, 9, 15, tzinfo=_IST
    ).astimezone(UTC)
    deadline = datetime(
        target_session.year, target_session.month, target_session.day, 15, 0, tzinfo=_IST
    ).astimezone(UTC)
    return not_before, deadline


def _sizing_policy_request(**overrides) -> LaunchSizingPolicyRequest:
    values = dict(
        per_trade_risk_fraction=D("0.005"),
        maximum_total_open_risk_fraction=D("0.02"),
        maximum_position_notional_fraction=D("0.25"),
        maximum_gross_exposure_fraction=D("0.80"),
        maximum_daily_turnover_participation=D("0.0025"),
        maximum_top_ask_participation=D("0.20"),
        maximum_daily_loss_fraction=D("0.01"),
        maximum_pilot_drawdown_fraction=D("0.02"),
        minimum_net_reward_risk=D("2.50"),
        maximum_open_positions=4,
        maximum_new_positions_per_run=1,
    )
    values.update(overrides)
    return LaunchSizingPolicyRequest(**values)


def _quote_gate_policy_request(**overrides) -> LaunchQuoteGatePolicyRequest:
    values = dict(
        maximum_batch_collection_seconds=15,
        maximum_quote_age_seconds=15,
        maximum_last_trade_age_seconds=300,
        maximum_spread_bps=D("50"),
    )
    values.update(overrides)
    return LaunchQuoteGatePolicyRequest(**values)


def _allocation_policy_request(**overrides) -> LaunchAllocationPolicyRequest:
    sizing = overrides.pop("sizing_policy", None) or _sizing_policy_request()
    values = dict(maximum_portfolio_age_seconds=300, sizing_policy=sizing)
    values.update(overrides)
    return LaunchAllocationPolicyRequest(**values)


def _package_request(
    *, research_run_id: str, target_session, open_listing_keys=(), **overrides
) -> PromotedPaperPilotSessionPackageRequest:
    not_before, deadline = _decision_window_for(target_session)
    values = dict(
        schema_version=PROMOTED_PAPER_PILOT_SESSION_PACKAGE_REQUEST_SCHEMA_VERSION,
        research_run_id=research_run_id,
        target_session=target_session,
        expected_quote_source_id=_QUOTE_SOURCE_ID,
        open_listing_keys=open_listing_keys,
        decision_not_before=not_before,
        decision_deadline=deadline,
        quote_gate_policy=_quote_gate_policy_request(),
        allocation_policy=_allocation_policy_request(),
        maximum_quote_chunk_size=500,
        binding_bucket="test-bucket",
    )
    values.update(overrides)
    return PromotedPaperPilotSessionPackageRequest(**values)


def _genesis_fixture(*, as_of, capital=D("100000")):
    payloads = {
        kind: (kind.value + "\nmanual paper reconciliation\n").encode()
        for kind in SwingPortfolioEvidenceKind
    }
    request = PromotedPaperPortfolioGenesisRequest(
        as_of=as_of,
        capital=capital,
        manual_reconciliation_ack=MANUAL_RECONCILIATION_ACK,
        evidence=tuple(
            GenesisEvidenceDescriptor(
                kind=kind,
                expected_sha256=hashlib.sha256(payloads[kind]).hexdigest(),
                observed_at=as_of - timedelta(seconds=index + 1),
                source_version="manual-paper-reconciliation/v1",
            )
            for index, kind in enumerate(SwingPortfolioEvidenceKind)
        ),
    )
    return request, payloads


class _FakeEngineStores:
    def __init__(self, engine_runs, research_intents):
        self.engine_runs = engine_runs
        self.research_intents = research_intents


class _FakeResearchStores:
    def __init__(self, research_runs, engine_runs, research_intents, replay_scope):
        self.research_runs = research_runs
        self.engine = _FakeEngineStores(engine_runs, research_intents)
        self._replay_scope = replay_scope


class _Fixture:
    """Wires one fully self-consistent, disk-backed preparation/portfolio
    pair around one of the shared, in-memory research/engine/intent-batch
    lineages already built by test_promoted_operational_preparation.py --
    never a real promoted graph/engine/Kite/GCS store."""

    def __init__(self, tmp_path: Path, *, lineage=None) -> None:
        self.tmp_path = tmp_path
        tmp_path.mkdir(parents=True, exist_ok=True)
        self.research_run_manifest, self.engine_run_manifest, self.batch = (
            lineage if lineage is not None else _prep_tests._NONEMPTY_LINEAGE
        )
        self.target_session = self.research_run_manifest.entry_session
        self.replay_scope = ExactReplayScope()
        self.research_stores = _FakeResearchStores(
            _prep_tests._StubResolver(
                {self.research_run_manifest.research_run_id: self.research_run_manifest}
            ),
            _prep_tests._StubResolver({self.engine_run_manifest.run_id: self.engine_run_manifest}),
            _prep_tests._StubResolver({self.batch.batch_id: self.batch}),
            self.replay_scope,
        )
        self.preparations = LocalPromotedOperationalPreparationStore(
            tmp_path / "preparations",
            research_runs=self.research_stores.research_runs,
            engine_runs=self.research_stores.engine.engine_runs,
            research_intents=self.research_stores.engine.research_intents,
            replay_scope=self.replay_scope,
        )
        self.portfolio_root = tmp_path / "portfolio"
        self.portfolio_store = LocalSwingPortfolioArtifactStore(self.portfolio_root)
        self.evidence_archive = LocalPromotedPortfolioEvidenceArchive(self.portfolio_root)

    def decision_window(self):
        return _decision_window_for(self.target_session)

    def package_request(self, **overrides) -> PromotedPaperPilotSessionPackageRequest:
        values = dict(
            research_run_id=self.research_run_manifest.research_run_id,
            target_session=self.target_session,
        )
        values.update(overrides)
        return _package_request(**values)

    def genesis(self, **overrides):
        not_before, _deadline = self.decision_window()
        as_of = overrides.pop("as_of", not_before - timedelta(seconds=1))
        return _genesis_fixture(as_of=as_of, **overrides)

    def run(self, *, package_request=None, genesis_request=None, genesis_payloads=None, output_name="assembly.json"):
        package_request = package_request if package_request is not None else self.package_request()
        if genesis_request is None or genesis_payloads is None:
            built_request, built_payloads = self.genesis()
            genesis_request = genesis_request if genesis_request is not None else built_request
            genesis_payloads = genesis_payloads if genesis_payloads is not None else built_payloads
        return prepare_promoted_paper_pilot_first_session_package(
            package_request=package_request,
            genesis_request=genesis_request,
            genesis_evidence_payloads=genesis_payloads,
            research_stores=self.research_stores,
            preparations=self.preparations,
            evidence_archive=self.evidence_archive,
            portfolio_store=self.portfolio_store,
            output_assembly_spec_file=self.tmp_path / output_name,
        )


class SessionPackageCodecTests(unittest.TestCase):
    def test_round_trip(self) -> None:
        request = _package_request(
            research_run_id="a" * 64, target_session=_prep_tests._ENTRY_SESSION,
        )
        payload = encode_promoted_paper_pilot_session_package_request(request)
        reloaded = decode_promoted_paper_pilot_session_package_request(payload)
        self.assertEqual(reloaded, request)
        self.assertEqual(encode_promoted_paper_pilot_session_package_request(reloaded), payload)

    def test_replay_detects_post_construction_tamper(self) -> None:
        request = _package_request(
            research_run_id="a" * 64, target_session=_prep_tests._ENTRY_SESSION,
        )
        object.__setattr__(request, "binding_bucket", "AB")
        with self.assertRaises(PromotedPaperPilotSessionPackageError):
            request.replay()

    def test_replay_detects_nested_policy_tamper(self) -> None:
        request = _package_request(
            research_run_id="a" * 64, target_session=_prep_tests._ENTRY_SESSION,
        )
        object.__setattr__(request.quote_gate_policy, "maximum_quote_age_seconds", "not-an-int")
        with self.assertRaises(PromotedPaperPilotSessionPackageError):
            request.replay()

    def test_rejects_str_subclass_schema_version_equal_to_the_constant(self) -> None:
        class _StrSubclass(str):
            pass

        with self.assertRaises(PromotedPaperPilotSessionPackageError):
            _package_request(
                research_run_id="a" * 64,
                target_session=_prep_tests._ENTRY_SESSION,
                schema_version=_StrSubclass(PROMOTED_PAPER_PILOT_SESSION_PACKAGE_REQUEST_SCHEMA_VERSION),
            )

    def test_ordinary_str_schema_version_round_trips_byte_identical(self) -> None:
        request = _package_request(
            research_run_id="a" * 64, target_session=_prep_tests._ENTRY_SESSION,
        )
        self.assertIs(type(request.schema_version), str)
        payload = encode_promoted_paper_pilot_session_package_request(request)
        reloaded = decode_promoted_paper_pilot_session_package_request(payload)
        self.assertEqual(encode_promoted_paper_pilot_session_package_request(reloaded), payload)

    def test_decode_rejects_adversarial_malformed_payloads(self) -> None:
        request = _package_request(
            research_run_id="a" * 64, target_session=_prep_tests._ENTRY_SESSION,
        )
        payload = encode_promoted_paper_pilot_session_package_request(request)
        raw = json.loads(payload)

        mutations = []

        def _mutated(mutate):
            value = json.loads(json.dumps(raw))
            mutate(value)
            return json.dumps(value, separators=(",", ":"), sort_keys=True).encode("utf-8")

        mutations.append(("extra_top_key", _mutated(lambda v: v.__setitem__("extra", True))))
        mutations.append(("missing_top_key", _mutated(lambda v: v.pop("binding_bucket"))))
        mutations.append(
            ("extra_nested_key", _mutated(lambda v: v["quote_gate_policy"].__setitem__("extra", 1)))
        )
        mutations.append(
            ("missing_nested_key", _mutated(lambda v: v["allocation_policy"]["sizing_policy"].pop("maximum_open_positions")))
        )
        mutations.append(("bool_as_int", _mutated(lambda v: v.__setitem__("maximum_quote_chunk_size", True))))
        mutations.append(
            ("noncanonical_decimal", _mutated(lambda v: v["quote_gate_policy"].__setitem__("maximum_spread_bps", "50.0")))
        )
        mutations.append(
            ("noncanonical_timestamp", _mutated(lambda v: v.__setitem__("decision_not_before", "2026-07-17T09:15:00+05:30")))
        )
        mutations.append(("noncanonical_session", _mutated(lambda v: v.__setitem__("target_session", "2026/07/17"))))
        mutations.append(("unsorted_listing", _mutated(lambda v: v.__setitem__("open_listing_keys", ["NSE:TCS", "NSE:RELIANCE"]))))
        mutations.append(("duplicate_listing", _mutated(lambda v: v.__setitem__("open_listing_keys", ["NSE:TCS", "NSE:TCS"]))))
        mutations.append(("malformed_listing", _mutated(lambda v: v.__setitem__("open_listing_keys", ["not-a-listing-key"]))))
        mutations.append(("noncanonical_bucket", _mutated(lambda v: v.__setitem__("binding_bucket", "AB"))))
        mutations.append(("malformed_hash", _mutated(lambda v: v.__setitem__("research_run_id", "not-a-sha256"))))
        mutations.append(
            ("duplicate_keys", payload.replace(b'{"allocation_policy"', b'{"schema_version":"x","allocation_policy"', 1))
        )
        mutations.append(("float_literal", payload.replace(b'"maximum_quote_chunk_size":500', b'"maximum_quote_chunk_size":500.0')))
        mutations.append(("nan_literal", payload.replace(b'"maximum_quote_chunk_size":500', b'"maximum_quote_chunk_size":NaN')))
        mutations.append(("invalid_utf8", b"\xff\xfe not utf-8"))
        mutations.append(("empty", b""))
        mutations.append(("oversized", b"{}" + b" " * MAXIMUM_SESSION_PACKAGE_REQUEST_BYTES))
        mutations.append(("noncanonical_json", payload.replace(b",", b", ")))

        for name, candidate in mutations:
            with self.subTest(name=name):
                with self.assertRaises(PromotedPaperPilotSessionPackageError):
                    decode_promoted_paper_pilot_session_package_request(candidate)


class SessionPackageCoordinatorTests(unittest.TestCase):
    def test_exact_research_resolution_and_preparation_lineage(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            result = fx.run()
            self.assertEqual(result.research_run_id, fx.research_run_manifest.research_run_id)
            self.assertEqual(result.candidate_count, len(fx.batch.intents))
            stored = fx.preparations.get(result.preparation_id)
            self.assertEqual(stored.manifest.research_run_id, fx.research_run_manifest.research_run_id)
            self.assertEqual(stored.manifest.target_session, fx.target_session)

    def test_four_evidence_bindings_are_archived_and_bound(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            genesis_request, payloads = fx.genesis()
            result = fx.run(genesis_request=genesis_request, genesis_payloads=payloads)
            artifact = fx.portfolio_store.get(result.portfolio_artifact_id)
            self.assertEqual(len(artifact.evidence), len(SwingPortfolioEvidenceKind))
            for kind in SwingPortfolioEvidenceKind:
                path = fx.evidence_archive.path_for(
                    kind, hashlib.sha256(payloads[kind]).hexdigest()
                )
                self.assertTrue(path.exists())

    def test_empty_genesis_portfolio(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            result = fx.run()
            artifact = fx.portfolio_store.get(result.portfolio_artifact_id)
            self.assertEqual(artifact.portfolio.open_positions, 0)
            self.assertEqual(artifact.portfolio.capital, D("100000"))
            self.assertEqual(artifact.portfolio.cash_available, D("100000"))

    def test_launch_dry_assembly_publishes_assembly_spec(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            result = fx.run()
            output_file = fx.tmp_path / "assembly.json"
            self.assertTrue(output_file.exists())
            self.assertIn(result.assembly_spec_id.encode(), output_file.read_bytes())

    def test_create_once_and_idempotent_same_request_replay(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            package_request = fx.package_request()
            genesis_request, payloads = fx.genesis()
            result1 = fx.run(
                package_request=package_request, genesis_request=genesis_request, genesis_payloads=payloads,
            )
            first_bytes = (fx.tmp_path / "assembly.json").read_bytes()
            result2 = fx.run(
                package_request=package_request, genesis_request=genesis_request, genesis_payloads=payloads,
            )
            self.assertEqual(result1, result2)
            self.assertEqual((fx.tmp_path / "assembly.json").read_bytes(), first_bytes)

    def test_zero_candidate_preparation_produces_an_auditable_package(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve(), lineage=_prep_tests._EMPTY_LINEAGE)
            result = fx.run()
            self.assertEqual(result.candidate_count, 0)


class SessionPackageAdversarialTests(unittest.TestCase):
    def test_foreign_target_session_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            wrong_session = fx.target_session - timedelta(days=1)
            not_before, deadline = _decision_window_for(fx.target_session)
            package_request = fx.package_request(
                target_session=wrong_session, decision_not_before=not_before, decision_deadline=deadline,
            )
            with self.assertRaises(PromotedPaperPilotSessionPackageError):
                fx.run(package_request=package_request)

    def test_stale_portfolio_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            not_before, _deadline = fx.decision_window()
            stale_as_of = not_before - timedelta(hours=6)
            genesis_request, payloads = fx.genesis(as_of=stale_as_of)
            with self.assertRaises(PromotedPaperPilotSessionPackageError):
                fx.run(genesis_request=genesis_request, genesis_payloads=payloads)

    def test_mismatched_evidence_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            genesis_request, payloads = fx.genesis()
            tampered_payloads = dict(payloads)
            tampered_payloads[SwingPortfolioEvidenceKind.BROKER_FUNDS] = b"tampered evidence bytes"
            with self.assertRaises(PromotedPaperPilotSessionPackageError):
                fx.run(genesis_request=genesis_request, genesis_payloads=tampered_payloads)

    def test_tampered_stored_preparation_ancestor_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            result = fx.run()
            stored_path = fx.preparations.path_for(result.preparation_id)
            stored_path.write_bytes(b'{"tampered": true}\n')
            with self.assertRaises(PromotedPaperPilotSessionPackageError):
                fx.run(output_name="assembly-second.json")

    def test_conflicting_existing_output_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            fx.run()
            output_file = fx.tmp_path / "assembly.json"
            original_bytes = output_file.read_bytes()

            other_genesis, other_payloads = fx.genesis(capital=D("999999"))
            with self.assertRaises(PromotedPaperPilotSessionPackageError):
                fx.run(genesis_request=other_genesis, genesis_payloads=other_payloads, output_name="assembly.json")
            self.assertEqual(output_file.read_bytes(), original_bytes)

    def test_portfolio_store_exception_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            with mock.patch.object(
                fx.portfolio_store, "put", side_effect=RuntimeError("boom"),
            ):
                with self.assertRaises(PromotedPaperPilotSessionPackageError):
                    fx.run()

    def test_wrong_type_package_request_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            genesis_request, payloads = fx.genesis()
            with self.assertRaises(PromotedPaperPilotSessionPackageError):
                prepare_promoted_paper_pilot_first_session_package(
                    package_request="not-a-request",
                    genesis_request=genesis_request,
                    genesis_evidence_payloads=payloads,
                    research_stores=fx.research_stores,
                    preparations=fx.preparations,
                    evidence_archive=fx.evidence_archive,
                    portfolio_store=fx.portfolio_store,
                    output_assembly_spec_file=fx.tmp_path / "assembly.json",
                )


class SessionPackageDependencySanitizationTests(unittest.TestCase):
    """Every accepted-dependency result and every subsequent verification/
    property-access step inside prepare_promoted_paper_pilot_first_session_
    package is untrusted. Each case here plants a secret marker in a wrong-
    typed return or a foreign exception at one exact stage/step, and
    requires the raised exception to be exactly
    PromotedPaperPilotSessionPackageError, with only the static message,
    with __cause__ and __context__ both None, and with the marker never
    appearing anywhere in the raised exception's own text."""

    SECRET_MARKER = "SUPER-SECRET-DEPENDENCY-SANITIZATION-MARKER-4kd9"

    def _assert_sanitized(self, action) -> None:
        try:
            action()
        except PromotedPaperPilotSessionPackageError as exc:
            self.assertEqual(str(exc), package_module._ERR)
            self.assertIsNone(exc.__cause__)
            self.assertIsNone(exc.__context__)
            self.assertNotIn(self.SECRET_MARKER, str(exc))
        else:
            self.fail("expected PromotedPaperPilotSessionPackageError")

    def _run(self, fx: "_Fixture", **kwargs):
        return fx.run(**kwargs)

    # -- Stage 1: preparation --------------------------------------------

    def test_preparation_wrong_type_return_is_sanitized(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            with mock.patch.object(
                package_module, "prepare_and_publish", return_value=self.SECRET_MARKER,
            ):
                self._assert_sanitized(lambda: self._run(fx))

    def test_preparation_dependency_exception_is_sanitized(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            with mock.patch.object(
                package_module, "prepare_and_publish",
                side_effect=RuntimeError(f"leaked={self.SECRET_MARKER}"),
            ):
                self._assert_sanitized(lambda: self._run(fx))

    def test_preparation_verify_content_identity_exception_is_sanitized(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            # A wrong-typed return already fails closed at the type check
            # (covered above); this case proves that even an exact-typed,
            # already-published, real preparation is still not trusted --
            # a foreign exception raised during its own verification step
            # is sanitized too.
            real_preparation = _prep_tests.PromotedOperationalPreparationService().prepare(
                research_run_manifest=fx.research_run_manifest,
                engine_run_manifest=fx.engine_run_manifest,
                research_intent_batch=fx.batch,
            )
            with mock.patch.object(
                package_module, "prepare_and_publish", return_value=real_preparation,
            ):
                with mock.patch.object(
                    VerifiedPromotedOperationalPreparation,
                    "verify_content_identity",
                    side_effect=RuntimeError(f"leaked={self.SECRET_MARKER}"),
                ):
                    self._assert_sanitized(lambda: self._run(fx))

    # -- Stage 2: genesis ---------------------------------------------------

    def test_genesis_wrong_type_return_is_sanitized(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            with mock.patch.object(
                package_module, "seal_promoted_paper_portfolio_genesis", return_value=self.SECRET_MARKER,
            ):
                self._assert_sanitized(lambda: self._run(fx))

    def test_genesis_dependency_exception_is_sanitized(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            with mock.patch.object(
                package_module, "seal_promoted_paper_portfolio_genesis",
                side_effect=RuntimeError(f"leaked={self.SECRET_MARKER}"),
            ):
                self._assert_sanitized(lambda: self._run(fx))

    def test_genesis_verify_content_identity_exception_is_sanitized(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            with mock.patch.object(
                SwingPortfolioSnapshotArtifact,
                "verify_content_identity",
                side_effect=RuntimeError(f"leaked={self.SECRET_MARKER}"),
            ):
                self._assert_sanitized(lambda: self._run(fx))

    # -- Stage 3: launch assembly / spec -------------------------------------

    def test_assembly_wrong_type_return_is_sanitized(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            with mock.patch.object(
                package_module, "prepare_promoted_operational_launch", return_value=self.SECRET_MARKER,
            ):
                self._assert_sanitized(lambda: self._run(fx))

    def test_assembly_dependency_exception_is_sanitized(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            with mock.patch.object(
                package_module, "prepare_promoted_operational_launch",
                side_effect=RuntimeError(f"leaked={self.SECRET_MARKER}"),
            ):
                self._assert_sanitized(lambda: self._run(fx))

    def test_assembly_spec_verify_content_identity_exception_is_sanitized(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            with mock.patch.object(
                PromotedOperationalAssemblySpec,
                "verify_content_identity",
                side_effect=RuntimeError(f"leaked={self.SECRET_MARKER}"),
            ):
                self._assert_sanitized(lambda: self._run(fx))

    # -- Stage 4: publication -------------------------------------------------

    def test_publication_dependency_exception_is_sanitized(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            with mock.patch.object(
                package_module, "publish_promoted_operational_launch_assembly_spec_file",
                side_effect=RuntimeError(f"leaked={self.SECRET_MARKER}"),
            ):
                self._assert_sanitized(lambda: self._run(fx))

    # -- Stage 5: result construction -----------------------------------------

    def test_result_construction_exception_is_sanitized(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fx = _Fixture(Path(directory).resolve())
            with mock.patch.object(
                package_module, "PromotedPaperPilotSessionPackageResult",
                side_effect=RuntimeError(f"leaked={self.SECRET_MARKER}"),
            ):
                self._assert_sanitized(lambda: self._run(fx))


class SessionPackageCliTests(unittest.TestCase):
    def _roots(self, tmp_path: Path) -> dict[str, str]:
        names = (
            "reference-root", "identity-evidence-root", "calendar-root", "daily-reports-root",
            "historical-corpus-root", "promoted-root", "graph-publication-root", "engine-run-root",
            "research-run-root",
        )
        return {f"--{name}": str(tmp_path / name) for name in names}

    def test_success_envelope_and_wiring(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            tmp_path = Path(directory).resolve()
            fx = _Fixture(tmp_path / "fixture")
            package_request = fx.package_request()
            genesis_request, payloads = fx.genesis()

            package_request_file = tmp_path / "package-request.json"
            package_request_file.write_bytes(
                encode_promoted_paper_pilot_session_package_request(package_request)
            )
            genesis_request_file = tmp_path / "genesis-request.json"
            genesis_request_file.write_bytes(encode_promoted_paper_portfolio_genesis_request(genesis_request))
            evidence_files = {}
            for kind, payload in payloads.items():
                path = tmp_path / f"{kind.value}.bin"
                path.write_bytes(payload)
                evidence_files[kind] = path
            output_file = tmp_path / "assembly.json"
            operational_preparation_root = tmp_path / "operational-preparation-root"
            portfolio_artifact_root = tmp_path / "portfolio-artifact-root"

            argv = [
                "--package-request-file", str(package_request_file),
                "--reference-root", str(tmp_path / "reference-root"),
                "--identity-evidence-root", str(tmp_path / "identity-evidence-root"),
                "--calendar-root", str(tmp_path / "calendar-root"),
                "--daily-reports-root", str(tmp_path / "daily-reports-root"),
                "--historical-corpus-root", str(tmp_path / "historical-corpus-root"),
                "--promoted-root", str(tmp_path / "promoted-root"),
                "--graph-publication-root", str(tmp_path / "graph-publication-root"),
                "--engine-run-root", str(tmp_path / "engine-run-root"),
                "--research-run-root", str(tmp_path / "research-run-root"),
                "--operational-preparation-root", str(operational_preparation_root),
                "--portfolio-artifact-root", str(portfolio_artifact_root),
                "--genesis-request-file", str(genesis_request_file),
                "--broker-funds-file", str(evidence_files[SwingPortfolioEvidenceKind.BROKER_FUNDS]),
                "--broker-positions-file", str(evidence_files[SwingPortfolioEvidenceKind.BROKER_POSITIONS]),
                "--engine-risk-ledger-file", str(evidence_files[SwingPortfolioEvidenceKind.ENGINE_RISK_LEDGER]),
                "--engine-pnl-ledger-file", str(evidence_files[SwingPortfolioEvidenceKind.ENGINE_PNL_LEDGER]),
                "--output-assembly-spec-file", str(output_file),
            ]

            import io
            from contextlib import redirect_stdout, redirect_stderr

            stdout = io.StringIO()
            stderr = io.StringIO()
            with mock.patch.object(
                package_cli_module,
                "build_promoted_operational_preparation_store",
                return_value=(fx.research_stores, fx.preparations),
            ):
                with redirect_stdout(stdout), redirect_stderr(stderr):
                    code = package_cli_module.main(argv)

            self.assertEqual(code, 0, stderr.getvalue())
            self.assertEqual(stderr.getvalue(), "")
            result = json.loads(stdout.getvalue())
            self.assertEqual(result["status"], "PROMOTED_PAPER_PILOT_SESSION_PACKAGE_READY")
            self.assertEqual(result["research_run_id"], fx.research_run_manifest.research_run_id)
            self.assertEqual(result["target_session"], fx.target_session.isoformat())
            self.assertTrue(result["paper_only"])
            self.assertFalse(result["notification_eligible"])
            self.assertFalse(result["execution_eligible"])
            self.assertEqual(
                set(result),
                {
                    "status", "target_session", "research_run_id", "preparation_id",
                    "portfolio_artifact_id", "portfolio_snapshot_id", "assembly_spec_id",
                    "candidate_count", "open_position_count", "paper_only",
                    "notification_eligible", "execution_eligible",
                },
            )
            self.assertTrue(output_file.exists())

    def test_missing_argument_fails_closed(self) -> None:
        code = package_cli_module.main(["--package-request-file", "/tmp/x.json"])
        self.assertEqual(code, 2)

    def test_unknown_argument_fails_closed(self) -> None:
        code = package_cli_module.main(["--unknown-flag", "x"])
        self.assertEqual(code, 2)

    def test_relative_path_argument_fails_closed(self) -> None:
        code = package_cli_module.main(["--package-request-file", "relative.json"])
        self.assertEqual(code, 2)

    def test_sanitized_failure_envelope_contains_no_leaked_text(self) -> None:
        secret = "SUPER-SECRET-SESSION-PACKAGE-MARKER"
        with tempfile.TemporaryDirectory() as directory:
            tmp_path = Path(directory).resolve()
            missing_file = tmp_path / "missing.json"
            argv = ["--package-request-file", str(missing_file) + secret]
            import io
            from contextlib import redirect_stderr, redirect_stdout

            stdout = io.StringIO()
            stderr = io.StringIO()
            with redirect_stdout(stdout), redirect_stderr(stderr):
                code = package_cli_module.main(argv)
            self.assertEqual(code, 2)
            self.assertEqual(stdout.getvalue(), "")
            self.assertNotIn(secret, stderr.getvalue())
            self.assertEqual(
                json.loads(stderr.getvalue()),
                {"error_type": "PromotedPaperPilotSessionPackageError", "status": "FAILED"},
            )


class RegressionAndCapabilityTests(unittest.TestCase):
    def _assert_no_forbidden_capability(self, module) -> None:
        source = inspect.getsource(module)
        tree = ast.parse(source)
        forbidden_modules = {
            "os", "socket", "subprocess", "requests", "urllib", "httpx", "google",
            "kiteconnect", "time", "threading", "asyncio", "tempfile", "glob", "shutil",
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
            "os.environ", "getenv(", "datetime.now(", "sleep(", "list_blobs(", "list_objects(",
            "storage.client(", "place_order(", "modify_order(", "cancel_order(", "gcloud", "gsutil",
        ):
            self.assertNotIn(token, lowered, msg=token)

    def test_package_module_has_no_forbidden_capability(self) -> None:
        self._assert_no_forbidden_capability(package_module)

    def test_cli_module_has_no_forbidden_capability(self) -> None:
        self._assert_no_forbidden_capability(package_cli_module)

    def test_pyproject_console_script_mapping_is_exact(self) -> None:
        pyproject_path = (
            Path(inspect.getfile(package_module)).resolve().parent.parent.parent / "pyproject.toml"
        )
        text = pyproject_path.read_text(encoding="utf-8")
        self.assertIn(
            'india-swing-promoted-paper-pilot-session-package = '
            '"india_swing.promoted_paper_pilot_session_package_cli:main"',
            text,
        )


if __name__ == "__main__":
    unittest.main()
