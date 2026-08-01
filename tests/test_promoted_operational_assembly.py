from __future__ import annotations

import ast
import inspect
import json
import tempfile
import unittest
from dataclasses import replace as _dc_replace
from datetime import timedelta
from decimal import Decimal
from pathlib import Path

import india_swing.promoted_operational_assembly as _assembly_module
from india_swing.domain.models import INDIA_STANDARD_TIME
from india_swing.operations.portfolio_store import (
    SwingPortfolioSnapshotArtifact,
    SwingPortfolioEvidenceBinding,
    SwingPortfolioEvidenceKind,
)
from india_swing.identity import content_id
from india_swing.paper_trades.store import LocalPaperTradeLedger
from india_swing.promoted_operational_allocation import PromotedOperationalPortfolioContext
from india_swing.promoted_operational_assembly import (
    MAXIMUM_ASSEMBLY_SPEC_BYTES,
    PROMOTED_OPERATIONAL_ASSEMBLY_SPEC_SCHEMA_VERSION,
    PromotedOperationalAssemblyError,
    PromotedOperationalAssemblySpec,
    PromotedOperationalRuntimeAssembly,
    assemble_promoted_operational_runtime_inputs,
    decode_promoted_operational_assembly_spec,
    encode_promoted_operational_assembly_spec,
    load_promoted_operational_assembly_spec_file,
)
from india_swing.promoted_operational_persistence import (
    LocalPromotedOperationalAdvisoryOutbox,
    LocalPromotedOperationalTerminalStore,
)
from india_swing.promoted_operational_runtime import (
    PinnedPromotedOperationalPortfolioSource,
    run_promoted_operational_runtime_job,
)
from india_swing.risk.swing_portfolio import SwingPortfolioSnapshot

from tests import test_promoted_operational_anchored_session as _anchored_tests
from tests import test_promoted_operational_runner as _runner_tests


def _portfolio_snapshot(*, as_of, open_positions: int = 0) -> SwingPortfolioSnapshot:
    return SwingPortfolioSnapshot(
        capital=Decimal("100000"),
        cash_available=Decimal("100000"),
        gross_exposure=Decimal("0"),
        open_risk=Decimal("0"),
        open_positions=open_positions,
        daily_realized_pnl=Decimal("0"),
        pilot_realized_pnl=Decimal("0"),
        as_of=as_of,
    )


def _portfolio_artifact(portfolio: SwingPortfolioSnapshot) -> SwingPortfolioSnapshotArtifact:
    evidence = tuple(
        SwingPortfolioEvidenceBinding(
            kind=kind,
            evidence_id=content_id(
                {"kind": kind.value, "portfolio_snapshot_id": portfolio.portfolio_snapshot_id},
                length=64,
            ),
            observed_at=portfolio.as_of,
            source_version="test-assembly/v1",
        )
        for kind in SwingPortfolioEvidenceKind
    )
    return SwingPortfolioSnapshotArtifact(
        portfolio=portfolio,
        portfolio_snapshot_id=portfolio.portfolio_snapshot_id,
        evidence=evidence,
        reconciled_at=portfolio.as_of,
    )


def _listing_keys(count: int) -> tuple[str, ...]:
    return tuple(sorted(f"NSE:SYM{index:02d}" for index in range(count)))


class _FakePreparationResolver:
    def __init__(self, preparation, *, raise_exc=None) -> None:
        self._preparation = preparation
        self._raise_exc = raise_exc
        self.calls: list[str] = []

    def get(self, preparation_id: str):
        self.calls.append(preparation_id)
        if self._raise_exc is not None:
            raise self._raise_exc
        return self._preparation


class _FakePortfolioArtifactResolver:
    def __init__(self, artifact, *, raise_exc=None) -> None:
        self._artifact = artifact
        self._raise_exc = raise_exc
        self.calls: list[str] = []

    def get(self, artifact_id: str):
        self.calls.append(artifact_id)
        if self._raise_exc is not None:
            raise self._raise_exc
        return self._artifact


class _AssertingPortfolioArtifactResolver:
    def get(self, artifact_id: str):
        raise AssertionError("portfolio_artifact_resolver must not be called")


class PromotedOperationalAssemblyTests(unittest.TestCase):
    def _fixture(self, *, open_positions: int = 0):
        preparation, run_spec = _runner_tests._run_spec(chunk_size=500)
        listing_keys = _listing_keys(open_positions)
        as_of = run_spec.quote_gate_spec.decision_not_before - timedelta(seconds=10)
        portfolio = _portfolio_snapshot(as_of=as_of, open_positions=open_positions)
        artifact = _portfolio_artifact(portfolio)
        spec = PromotedOperationalAssemblySpec(
            preparation_id=preparation.manifest.preparation_id,
            portfolio_artifact_id=artifact.artifact_id,
            expected_portfolio_snapshot_id=portfolio.portfolio_snapshot_id,
            expected_quote_source_id=run_spec.expected_quote_source_id,
            open_listing_keys=listing_keys,
            decision_not_before=run_spec.quote_gate_spec.decision_not_before,
            decision_deadline=run_spec.quote_gate_spec.decision_deadline,
            target_session=preparation.manifest.target_session,
            quote_gate_policy=run_spec.quote_gate_spec.policy,
            allocation_policy=run_spec.allocation_policy,
            maximum_quote_chunk_size=run_spec.maximum_quote_chunk_size,
            binding_bucket="test-bucket",
        )
        preparation_resolver = _FakePreparationResolver(preparation)
        artifact_resolver = _FakePortfolioArtifactResolver(artifact)
        return preparation, run_spec, portfolio, artifact, spec, preparation_resolver, artifact_resolver

    def test_spec_construction_content_identity_and_codec_round_trip(self) -> None:
        _, _, _, _, spec, _, _ = self._fixture()
        self.assertEqual(spec.schema_version, PROMOTED_OPERATIONAL_ASSEMBLY_SPEC_SCHEMA_VERSION)
        spec.verify_content_identity()

        payload = encode_promoted_operational_assembly_spec(spec)
        decoded = decode_promoted_operational_assembly_spec(payload)
        self.assertEqual(decoded, spec)
        self.assertEqual(encode_promoted_operational_assembly_spec(decoded), payload)

        with self.subTest(case="duplicate_key"):
            needle = f'"schema_version":"{PROMOTED_OPERATIONAL_ASSEMBLY_SPEC_SCHEMA_VERSION}",'.encode()
            self.assertEqual(payload.count(needle), 1)
            duplicated = payload.replace(needle, needle + needle, 1)
            with self.assertRaises(PromotedOperationalAssemblyError):
                decode_promoted_operational_assembly_spec(duplicated)

        with self.subTest(case="unknown_key"):
            root = json.loads(payload)
            root["assembly_spec"]["unexpected"] = "x"
            with self.assertRaises(PromotedOperationalAssemblyError):
                decode_promoted_operational_assembly_spec(_reencode(root))

        with self.subTest(case="missing_key"):
            root = json.loads(payload)
            del root["assembly_spec"]["binding_bucket"]
            with self.assertRaises(PromotedOperationalAssemblyError):
                decode_promoted_operational_assembly_spec(_reencode(root))

        with self.subTest(case="unknown_key_nested_policy"):
            root = json.loads(payload)
            root["assembly_spec"]["allocation_policy"]["sizing_policy"]["unexpected"] = "x"
            with self.assertRaises(PromotedOperationalAssemblyError):
                decode_promoted_operational_assembly_spec(_reencode(root))

        text = payload.decode("utf-8")
        id_needle = f'"assembly_spec_id":"{spec.assembly_spec_id}"'
        self.assertIn(id_needle, text)

        with self.subTest(case="float_value"):
            mutated = text.replace(id_needle, '"assembly_spec_id":1.5', 1).encode("utf-8")
            with self.assertRaises(PromotedOperationalAssemblyError):
                decode_promoted_operational_assembly_spec(mutated)

        with self.subTest(case="nan_value"):
            mutated = text.replace(id_needle, '"assembly_spec_id":NaN', 1).encode("utf-8")
            with self.assertRaises(PromotedOperationalAssemblyError):
                decode_promoted_operational_assembly_spec(mutated)

        with self.subTest(case="infinity_value"):
            mutated = text.replace(id_needle, '"assembly_spec_id":Infinity', 1).encode("utf-8")
            with self.assertRaises(PromotedOperationalAssemblyError):
                decode_promoted_operational_assembly_spec(mutated)

        with self.subTest(case="decimal_not_canonical"):
            root = json.loads(payload)
            root["assembly_spec"]["quote_gate_policy"]["maximum_spread_bps"] = "50.00"
            with self.assertRaises(PromotedOperationalAssemblyError):
                decode_promoted_operational_assembly_spec(_reencode(root))

        with self.subTest(case="naive_datetime"):
            root = json.loads(payload)
            root["assembly_spec"]["decision_not_before"] = "2026-01-01T09:15:00"
            with self.assertRaises(PromotedOperationalAssemblyError):
                decode_promoted_operational_assembly_spec(_reencode(root))

        with self.subTest(case="noncanonical_datetime_offset"):
            root = json.loads(payload)
            ist_value = spec.decision_not_before.astimezone(INDIA_STANDARD_TIME)
            root["assembly_spec"]["decision_not_before"] = ist_value.isoformat()
            with self.assertRaises(PromotedOperationalAssemblyError):
                decode_promoted_operational_assembly_spec(_reencode(root))

        with self.subTest(case="direct_construction_rejects_nonzero_offset"):
            ist_value = spec.decision_not_before.astimezone(INDIA_STANDARD_TIME)
            self.assertNotEqual(ist_value.utcoffset(), timedelta(0))
            with self.assertRaises(PromotedOperationalAssemblyError):
                _dc_replace(spec, decision_not_before=ist_value)

        with self.subTest(case="invalid_authority"):
            root = json.loads(payload)
            root["assembly_spec"]["notification_eligible"] = True
            with self.assertRaises(PromotedOperationalAssemblyError):
                decode_promoted_operational_assembly_spec(_reencode(root))

        with self.subTest(case="oversized"):
            oversized = payload + b" " * MAXIMUM_ASSEMBLY_SPEC_BYTES
            with self.assertRaises(PromotedOperationalAssemblyError):
                decode_promoted_operational_assembly_spec(oversized)

        with self.subTest(case="stale_id_field_tampering"):
            root = json.loads(payload)
            root["assembly_spec"]["binding_bucket"] = "a-different-bucket"
            with self.assertRaises(PromotedOperationalAssemblyError):
                decode_promoted_operational_assembly_spec(_reencode(root))

        with self.subTest(case="nested_policy_id_stale"):
            root = json.loads(payload)
            root["assembly_spec"]["allocation_policy"]["sizing_policy"][
                "maximum_open_positions"
            ] = 99
            with self.assertRaises(PromotedOperationalAssemblyError):
                decode_promoted_operational_assembly_spec(_reencode(root))

    def test_spec_construction_rejects_unsorted_duplicate_or_count_mismatched_listing_keys(
        self,
    ) -> None:
        _, run_spec, _, _, base_spec, _, _ = self._fixture(open_positions=2)

        with self.subTest(case="unsorted"):
            reversed_keys = tuple(reversed(base_spec.open_listing_keys))
            self.assertNotEqual(reversed_keys, base_spec.open_listing_keys)
            with self.assertRaises(PromotedOperationalAssemblyError):
                _dc_replace(base_spec, open_listing_keys=reversed_keys)

        with self.subTest(case="duplicate"):
            duplicated = (base_spec.open_listing_keys[0], base_spec.open_listing_keys[0])
            with self.assertRaises(PromotedOperationalAssemblyError):
                _dc_replace(base_spec, open_listing_keys=duplicated)

        with self.subTest(case="malformed_shape"):
            with self.assertRaises(PromotedOperationalAssemblyError):
                _dc_replace(base_spec, open_listing_keys=("BSE:FOO",))

    def test_file_loader_round_trips_and_rejects_unsafe_or_invalid_inputs(self) -> None:
        _, _, _, _, spec, _, _ = self._fixture()
        payload = encode_promoted_operational_assembly_spec(spec)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            good_path = root / "assembly.json"
            good_path.write_bytes(payload)
            self.assertEqual(load_promoted_operational_assembly_spec_file(good_path), spec)

            with self.subTest(case="relative_path"):
                with self.assertRaises(PromotedOperationalAssemblyError):
                    load_promoted_operational_assembly_spec_file(Path("assembly.json"))

            with self.subTest(case="traversing_path"):
                traversing = root / ".." / root.name / "assembly.json"
                with self.assertRaises(PromotedOperationalAssemblyError):
                    load_promoted_operational_assembly_spec_file(traversing)

            with self.subTest(case="concrete_path_subclass_rejected"):

                class _OverridingPath(_assembly_module._CONCRETE_PATH_TYPE):
                    def is_absolute(self) -> bool:
                        raise AssertionError(
                            "overridden path behavior must never be consulted"
                        )

                subclassed_path = _OverridingPath(good_path)
                self.assertIsInstance(subclassed_path, _assembly_module._CONCRETE_PATH_TYPE)
                self.assertIsNot(type(subclassed_path), _assembly_module._CONCRETE_PATH_TYPE)
                with self.assertRaises(PromotedOperationalAssemblyError):
                    load_promoted_operational_assembly_spec_file(subclassed_path)

            with self.subTest(case="missing_path"):
                with self.assertRaises(PromotedOperationalAssemblyError):
                    load_promoted_operational_assembly_spec_file(root / "missing.json")

            with self.subTest(case="directory_path"):
                with self.assertRaises(PromotedOperationalAssemblyError):
                    load_promoted_operational_assembly_spec_file(root)

            with self.subTest(case="tampered_content"):
                tampered_path = root / "tampered.json"
                tampered_path.write_bytes(
                    payload.replace(spec.assembly_spec_id.encode(), b"0" * 64)
                )
                with self.assertRaises(PromotedOperationalAssemblyError):
                    load_promoted_operational_assembly_spec_file(tampered_path)

            with self.subTest(case="oversized_file"):
                oversized_path = root / "oversized.json"
                oversized_path.write_bytes(payload + b" " * MAXIMUM_ASSEMBLY_SPEC_BYTES)
                with self.assertRaises(PromotedOperationalAssemblyError):
                    load_promoted_operational_assembly_spec_file(oversized_path)

            with self.subTest(case="symlink_path"):
                symlink_path = root / "link.json"
                try:
                    symlink_path.symlink_to(good_path)
                except OSError:
                    self.skipTest("symlinks not permitted in this environment")
                with self.assertRaises(PromotedOperationalAssemblyError):
                    load_promoted_operational_assembly_spec_file(symlink_path)

    def test_happy_assembly_exact_calls_and_stable_ids(self) -> None:
        preparation, run_spec, portfolio, artifact, spec, preparation_resolver, artifact_resolver = (
            self._fixture(open_positions=2)
        )

        assembly = assemble_promoted_operational_runtime_inputs(
            spec=spec,
            preparation_resolver=preparation_resolver,
            portfolio_artifact_resolver=artifact_resolver,
        )
        self.assertIs(type(assembly), PromotedOperationalRuntimeAssembly)
        self.assertEqual(preparation_resolver.calls, [spec.preparation_id])
        self.assertEqual(artifact_resolver.calls, [spec.portfolio_artifact_id])
        self.assertIs(assembly.preparation, preparation)
        self.assertIs(assembly.portfolio_artifact, artifact)
        self.assertEqual(assembly.portfolio_context.portfolio, portfolio)
        self.assertEqual(assembly.portfolio_context.source_portfolio_artifact_id, artifact.artifact_id)
        self.assertEqual(assembly.portfolio_context.open_listing_keys, spec.open_listing_keys)
        self.assertEqual(assembly.run_spec.quote_gate_spec.preparation, preparation)
        self.assertEqual(assembly.run_spec.expected_quote_source_id, spec.expected_quote_source_id)
        self.assertEqual(
            assembly.run_spec.expected_portfolio_source_id, artifact.artifact_id
        )
        self.assertEqual(assembly.runtime_job_spec.binding_bucket, spec.binding_bucket)
        self.assertEqual(
            assembly.runtime_job_spec.operational_run_spec_id, assembly.run_spec.spec_id
        )

        second = assemble_promoted_operational_runtime_inputs(
            spec=spec,
            preparation_resolver=_FakePreparationResolver(preparation),
            portfolio_artifact_resolver=_FakePortfolioArtifactResolver(artifact),
        )
        self.assertEqual(second.run_spec.spec_id, assembly.run_spec.spec_id)
        self.assertEqual(second.runtime_job_spec.job_spec_id, assembly.runtime_job_spec.job_spec_id)
        self.assertEqual(
            second.portfolio_context.context_id, assembly.portfolio_context.context_id
        )

    def test_zero_and_nonzero_open_position_counts_require_exact_explicit_keys(self) -> None:
        with self.subTest(case="zero_positions_empty_tuple"):
            _, _, _, _, spec, prep_resolver, artifact_resolver = self._fixture(open_positions=0)
            assembly = assemble_promoted_operational_runtime_inputs(
                spec=spec,
                preparation_resolver=prep_resolver,
                portfolio_artifact_resolver=artifact_resolver,
            )
            self.assertEqual(assembly.portfolio_context.open_listing_keys, ())

        with self.subTest(case="nonzero_positions_require_exact_count"):
            preparation, run_spec, portfolio, artifact, spec, prep_resolver, _ = self._fixture(
                open_positions=2
            )
            mismatched_spec = _dc_replace(spec, open_listing_keys=spec.open_listing_keys[:1])
            with self.assertRaises(PromotedOperationalAssemblyError):
                assemble_promoted_operational_runtime_inputs(
                    spec=mismatched_spec,
                    preparation_resolver=_FakePreparationResolver(preparation),
                    portfolio_artifact_resolver=_FakePortfolioArtifactResolver(artifact),
                )

        with self.subTest(case="zero_position_portfolio_rejects_nonempty_keys"):
            preparation, run_spec, portfolio, artifact, spec, prep_resolver, artifact_resolver = (
                self._fixture(open_positions=0)
            )
            nonempty_spec = _dc_replace(spec, open_listing_keys=_listing_keys(1))
            with self.assertRaises(PromotedOperationalAssemblyError):
                assemble_promoted_operational_runtime_inputs(
                    spec=nonempty_spec,
                    preparation_resolver=_FakePreparationResolver(preparation),
                    portfolio_artifact_resolver=_FakePortfolioArtifactResolver(artifact),
                )

    def test_resolver_failures_wrong_types_tampered_parents_and_stale_portfolio_fail_closed(
        self,
    ) -> None:
        preparation, run_spec, portfolio, artifact, spec, _, _ = self._fixture()

        with self.subTest(case="preparation_resolver_raises"):
            resolver = _FakePreparationResolver(preparation, raise_exc=RuntimeError("boom"))
            asserting_artifacts = _AssertingPortfolioArtifactResolver()
            try:
                assemble_promoted_operational_runtime_inputs(
                    spec=spec,
                    preparation_resolver=resolver,
                    portfolio_artifact_resolver=asserting_artifacts,
                )
                self.fail("expected PromotedOperationalAssemblyError")
            except PromotedOperationalAssemblyError as exc:
                self.assertIsNone(exc.__cause__)
                self.assertIsNone(exc.__context__)
            self.assertEqual(resolver.calls, [spec.preparation_id])

        with self.subTest(case="preparation_wrong_type"):
            resolver = _FakePreparationResolver(object())
            with self.assertRaises(PromotedOperationalAssemblyError):
                assemble_promoted_operational_runtime_inputs(
                    spec=spec,
                    preparation_resolver=resolver,
                    portfolio_artifact_resolver=_AssertingPortfolioArtifactResolver(),
                )

        with self.subTest(case="preparation_id_mismatch"):
            # The resolver returns a preparation whose own ID does not
            # match the ID it was asked to resolve -- a stale/tampered
            # resolver result, not a legitimate absence.
            resolver = _FakePreparationResolver(preparation)
            mismatched_spec = _dc_replace(spec, preparation_id="0" * 64)
            with self.assertRaises(PromotedOperationalAssemblyError):
                assemble_promoted_operational_runtime_inputs(
                    spec=mismatched_spec,
                    preparation_resolver=resolver,
                    portfolio_artifact_resolver=_AssertingPortfolioArtifactResolver(),
                )
            self.assertEqual(resolver.calls, ["0" * 64])

        with self.subTest(case="artifact_resolver_raises"):
            resolver = _FakePortfolioArtifactResolver(artifact, raise_exc=RuntimeError("boom"))
            try:
                assemble_promoted_operational_runtime_inputs(
                    spec=spec,
                    preparation_resolver=_FakePreparationResolver(preparation),
                    portfolio_artifact_resolver=resolver,
                )
                self.fail("expected PromotedOperationalAssemblyError")
            except PromotedOperationalAssemblyError as exc:
                self.assertIsNone(exc.__cause__)
                self.assertIsNone(exc.__context__)

        with self.subTest(case="artifact_wrong_type"):
            resolver = _FakePortfolioArtifactResolver(object())
            with self.assertRaises(PromotedOperationalAssemblyError):
                assemble_promoted_operational_runtime_inputs(
                    spec=spec,
                    preparation_resolver=_FakePreparationResolver(preparation),
                    portfolio_artifact_resolver=resolver,
                )

        with self.subTest(case="artifact_snapshot_mismatch"):
            other_portfolio = _portfolio_snapshot(
                as_of=portfolio.as_of - timedelta(seconds=1), open_positions=0
            )
            other_artifact = _portfolio_artifact(other_portfolio)
            self.assertNotEqual(other_artifact.artifact_id, artifact.artifact_id)
            mismatched_spec = _dc_replace(spec, portfolio_artifact_id=other_artifact.artifact_id)
            with self.assertRaises(PromotedOperationalAssemblyError):
                assemble_promoted_operational_runtime_inputs(
                    spec=mismatched_spec,
                    preparation_resolver=_FakePreparationResolver(preparation),
                    portfolio_artifact_resolver=_FakePortfolioArtifactResolver(artifact),
                )

        with self.subTest(case="future_portfolio"):
            future_portfolio = _portfolio_snapshot(
                as_of=spec.decision_deadline + timedelta(seconds=1), open_positions=0
            )
            future_artifact = _portfolio_artifact(future_portfolio)
            future_spec = _dc_replace(
                spec,
                portfolio_artifact_id=future_artifact.artifact_id,
                expected_portfolio_snapshot_id=future_portfolio.portfolio_snapshot_id,
            )
            with self.assertRaises(PromotedOperationalAssemblyError):
                assemble_promoted_operational_runtime_inputs(
                    spec=future_spec,
                    preparation_resolver=_FakePreparationResolver(preparation),
                    portfolio_artifact_resolver=_FakePortfolioArtifactResolver(future_artifact),
                )

        with self.subTest(case="stale_portfolio"):
            max_age = spec.allocation_policy.maximum_portfolio_age_seconds
            stale_portfolio = _portfolio_snapshot(
                as_of=spec.decision_not_before - timedelta(seconds=max_age + 5),
                open_positions=0,
            )
            stale_artifact = _portfolio_artifact(stale_portfolio)
            stale_spec = _dc_replace(
                spec,
                portfolio_artifact_id=stale_artifact.artifact_id,
                expected_portfolio_snapshot_id=stale_portfolio.portfolio_snapshot_id,
            )
            with self.assertRaises(PromotedOperationalAssemblyError):
                assemble_promoted_operational_runtime_inputs(
                    spec=stale_spec,
                    preparation_resolver=_FakePreparationResolver(preparation),
                    portfolio_artifact_resolver=_FakePortfolioArtifactResolver(stale_artifact),
                )

    def test_runtime_assembly_rejects_tampered_or_mismatched_nested_state(self) -> None:
        preparation, run_spec, portfolio, artifact, spec, preparation_resolver, artifact_resolver = (
            self._fixture(open_positions=1)
        )
        assembly = assemble_promoted_operational_runtime_inputs(
            spec=spec,
            preparation_resolver=preparation_resolver,
            portfolio_artifact_resolver=artifact_resolver,
        )

        accepted = PromotedOperationalRuntimeAssembly(
            assembly_spec=assembly.assembly_spec,
            preparation=assembly.preparation,
            portfolio_artifact=assembly.portfolio_artifact,
            portfolio_context=assembly.portfolio_context,
            run_spec=assembly.run_spec,
            runtime_job_spec=assembly.runtime_job_spec,
        )
        self.assertEqual(accepted, assembly)

        # Every independently self-consistent alternate runtime job spec
        # (built through the real constructor via dataclasses.replace,
        # never object.__setattr__ stale-ID corruption) must still be
        # rejected -- a content-valid runtime job artifact is not trusted
        # merely because its own ID replays.
        mismatched_job_specs = {
            "preparation_id": _dc_replace(assembly.runtime_job_spec, preparation_id="0" * 64),
            "target_session": _dc_replace(
                assembly.runtime_job_spec,
                target_session=assembly.runtime_job_spec.target_session - timedelta(days=1),
                decision_not_before=(
                    assembly.runtime_job_spec.decision_not_before - timedelta(days=1)
                ),
                decision_deadline=(
                    assembly.runtime_job_spec.decision_deadline - timedelta(days=1)
                ),
            ),
            "decision_not_before": _dc_replace(
                assembly.runtime_job_spec,
                decision_not_before=(
                    assembly.runtime_job_spec.decision_not_before + timedelta(seconds=1)
                ),
            ),
            "decision_deadline": _dc_replace(
                assembly.runtime_job_spec,
                decision_deadline=(
                    assembly.runtime_job_spec.decision_deadline + timedelta(seconds=1)
                ),
            ),
            "expected_quote_source_id": _dc_replace(
                assembly.runtime_job_spec, expected_quote_source_id="1" * 64
            ),
            "expected_portfolio_source_id": _dc_replace(
                assembly.runtime_job_spec, expected_portfolio_source_id="2" * 64
            ),
            "expected_portfolio_context_id": _dc_replace(
                assembly.runtime_job_spec, expected_portfolio_context_id="3" * 64
            ),
            "binding_bucket": _dc_replace(
                assembly.runtime_job_spec, binding_bucket="a-different-bucket"
            ),
        }
        for case, mismatched_job_spec in mismatched_job_specs.items():
            with self.subTest(case=case):
                self.assertNotEqual(mismatched_job_spec, assembly.runtime_job_spec)
                try:
                    PromotedOperationalRuntimeAssembly(
                        assembly_spec=assembly.assembly_spec,
                        preparation=assembly.preparation,
                        portfolio_artifact=assembly.portfolio_artifact,
                        portfolio_context=assembly.portfolio_context,
                        run_spec=assembly.run_spec,
                        runtime_job_spec=mismatched_job_spec,
                    )
                    self.fail("expected PromotedOperationalAssemblyError")
                except PromotedOperationalAssemblyError as exc:
                    self.assertIsNone(exc.__cause__)
                    self.assertIsNone(exc.__context__)

        with self.subTest(case="wrong_type"):
            with self.assertRaises(PromotedOperationalAssemblyError):
                PromotedOperationalRuntimeAssembly(
                    assembly_spec=assembly.assembly_spec,
                    preparation=assembly.preparation,
                    portfolio_artifact=assembly.portfolio_artifact,
                    portfolio_context=assembly.portfolio_context,
                    run_spec=assembly.run_spec,
                    runtime_job_spec=object(),
                )

    def test_tampered_preparation_manifest_is_sanitized_before_leak_or_second_resolver_call(
        self,
    ) -> None:
        preparation, run_spec, portfolio, artifact, spec, _, _ = self._fixture()
        # Structural tampering after construction: the manifest attribute
        # itself is replaced with a bare object() that has no nested
        # fields at all. verify_content_identity() must reject this
        # before any manifest.* attribute is ever read.
        object.__setattr__(preparation, "manifest", object())

        resolver = _FakePreparationResolver(preparation)
        asserting_artifact_resolver = _AssertingPortfolioArtifactResolver()
        try:
            assemble_promoted_operational_runtime_inputs(
                spec=spec,
                preparation_resolver=resolver,
                portfolio_artifact_resolver=asserting_artifact_resolver,
            )
            self.fail("expected PromotedOperationalAssemblyError")
        except PromotedOperationalAssemblyError as exc:
            self.assertIsNone(exc.__cause__)
            self.assertIsNone(exc.__context__)
        self.assertEqual(resolver.calls, [spec.preparation_id])

    def test_runtime_assembly_is_compatible_with_the_existing_runtime_orchestrator(self) -> None:
        preparation, run_spec, portfolio, artifact, spec, preparation_resolver, artifact_resolver = (
            self._fixture(open_positions=0)
        )
        assembly = assemble_promoted_operational_runtime_inputs(
            spec=spec,
            preparation_resolver=preparation_resolver,
            portfolio_artifact_resolver=artifact_resolver,
        )

        quote_source = _runner_tests._FakeQuoteSource(
            responder=_runner_tests._sorted_quote_responder(preparation),
            source_id=assembly.run_spec.expected_quote_source_id,
        )
        portfolio_source = PinnedPromotedOperationalPortfolioSource(assembly.portfolio_context)
        clock = _runner_tests._clock(
            assembly.run_spec.quote_gate_spec.decision_not_before + timedelta(seconds=1),
            assembly.run_spec.quote_gate_spec.decision_not_before + timedelta(seconds=2),
            assembly.run_spec.quote_gate_spec.decision_not_before + timedelta(seconds=3),
        )
        backend = _anchored_tests._FakeBindingBackend()
        preflight = _anchored_tests._PermissiveRecordingPreflight()

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            advisory_outbox = LocalPromotedOperationalAdvisoryOutbox(root)
            terminal_store = LocalPromotedOperationalTerminalStore(root / "runner")
            paper_ledger = LocalPaperTradeLedger(root / "paper")

            state = run_promoted_operational_runtime_job(
                job_spec=assembly.runtime_job_spec,
                run_spec=assembly.run_spec,
                quote_source=quote_source,
                portfolio_source=portfolio_source,
                clock=clock,
                advisory_outbox=advisory_outbox,
                terminal_store=terminal_store,
                paper_ledger=paper_ledger,
                binding_writer=backend,
                binding_reader=backend,
                binding_preflight=preflight,
            )
        self.assertEqual(state.job_spec, assembly.runtime_job_spec)
        self.assertFalse(state.anchored.reused_existing_terminal)

    def test_module_has_no_environment_credential_network_broker_subprocess_or_discovery_capability(
        self,
    ) -> None:
        source = inspect.getsource(_assembly_module)
        for forbidden in (
            "import os",
            "os.environ",
            "os.getenv",
            "import google",
            "google.cloud.storage.Client",
            "import requests",
            "import urllib",
            "import http",
            "import socket",
            "telegram",
            "kiteconnect",
            "import subprocess",
            "datetime.now(",
            "datetime.utcnow(",
            ".utcnow(",
            ".today(",
            "time.time(",
            "import random",
            "glob(",
            "iterdir(",
            "listdir(",
            "rglob(",
            "place_order",
            "modify_order",
            "cancel_order",
        ):
            self.assertNotIn(forbidden, source)
        ast.parse(source)
        # The docstring may reference these names in prose (documenting
        # what the assembly output is "suitable for"), but the module
        # must never actually import the execution-triggering functions
        # themselves.
        for forbidden_symbol in (
            "run_promoted_operational_runtime_job",
            "run_publish_and_anchor_promoted_operational_session",
            "run_and_publish_promoted_operational_service",
        ):
            self.assertFalse(hasattr(_assembly_module, forbidden_symbol))


def _reencode(root: dict) -> bytes:
    return (
        json.dumps(root, allow_nan=False, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
        + "\n"
    ).encode("utf-8")


if __name__ == "__main__":
    unittest.main()
