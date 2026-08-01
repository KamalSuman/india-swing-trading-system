from __future__ import annotations

import ast
import inspect
import io
import json
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from unittest import mock

import india_swing.promoted_operational_launch as _launch_module
import india_swing.promoted_operational_launch_cli as _launch_cli_module
from india_swing.operations.portfolio_store import LocalSwingPortfolioArtifactStore
from india_swing.promoted_operational_assembly import (
    MAXIMUM_ASSEMBLY_SPEC_BYTES,
    load_promoted_operational_assembly_spec_file,
)
from india_swing.promoted_operational_launch import (
    MAXIMUM_LAUNCH_REQUEST_BYTES,
    PROMOTED_OPERATIONAL_LAUNCH_REQUEST_SCHEMA_VERSION,
    LaunchAllocationPolicyRequest,
    LaunchQuoteGatePolicyRequest,
    LaunchSizingPolicyRequest,
    PromotedOperationalLaunchError,
    PromotedOperationalLaunchRequest,
    canonical_utc_z_timestamp,
    decode_promoted_operational_launch_request,
    prepare_promoted_operational_launch,
    publish_promoted_operational_launch_assembly_spec_file,
)
from india_swing.promoted_operational_preparation import PromotedOperationalPreparationService

from tests import test_promoted_operational_assembly as _assembly_tests
from tests import test_promoted_operational_preparation as _preparation_tests

D = Decimal
_IST = timezone(timedelta(hours=5, minutes=30))
_QUOTE_SOURCE_ID = "8" * 64


def _reencode(root: dict) -> bytes:
    return (
        json.dumps(root, allow_nan=False, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
        + "\n"
    ).encode("utf-8")


def _build_preparation():
    research_run_manifest, engine_run_manifest, batch = _preparation_tests._NONEMPTY_LINEAGE
    return PromotedOperationalPreparationService().prepare(
        research_run_manifest=research_run_manifest,
        engine_run_manifest=engine_run_manifest,
        research_intent_batch=batch,
    )


def _decision_window_for(target_session):
    not_before = datetime(
        target_session.year, target_session.month, target_session.day, 9, 15, tzinfo=_IST
    ).astimezone(timezone.utc)
    deadline = datetime(
        target_session.year, target_session.month, target_session.day, 15, 0, tzinfo=_IST
    ).astimezone(timezone.utc)
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


def _request_for(preparation, artifact, *, open_listing_keys=()) -> PromotedOperationalLaunchRequest:
    not_before, deadline = _decision_window_for(preparation.manifest.target_session)
    return PromotedOperationalLaunchRequest(
        schema_version=PROMOTED_OPERATIONAL_LAUNCH_REQUEST_SCHEMA_VERSION,
        preparation_id=preparation.manifest.preparation_id,
        portfolio_artifact_id=artifact.artifact_id,
        expected_quote_source_id=_QUOTE_SOURCE_ID,
        open_listing_keys=open_listing_keys,
        decision_not_before=not_before,
        decision_deadline=deadline,
        quote_gate_policy=_quote_gate_policy_request(),
        allocation_policy=LaunchAllocationPolicyRequest(
            maximum_portfolio_age_seconds=300,
            sizing_policy=_sizing_policy_request(),
        ),
        maximum_quote_chunk_size=500,
        binding_bucket="test-bucket",
    )


def _request_body(request: PromotedOperationalLaunchRequest) -> dict[str, object]:
    sizing = request.allocation_policy.sizing_policy
    return {
        "schema_version": request.schema_version,
        "preparation_id": request.preparation_id,
        "portfolio_artifact_id": request.portfolio_artifact_id,
        "expected_quote_source_id": request.expected_quote_source_id,
        "open_listing_keys": list(request.open_listing_keys),
        "decision_not_before": canonical_utc_z_timestamp(request.decision_not_before),
        "decision_deadline": canonical_utc_z_timestamp(request.decision_deadline),
        "quote_gate_policy": {
            "maximum_batch_collection_seconds": (
                request.quote_gate_policy.maximum_batch_collection_seconds
            ),
            "maximum_quote_age_seconds": request.quote_gate_policy.maximum_quote_age_seconds,
            "maximum_last_trade_age_seconds": (
                request.quote_gate_policy.maximum_last_trade_age_seconds
            ),
            "maximum_spread_bps": str(request.quote_gate_policy.maximum_spread_bps),
        },
        "allocation_policy": {
            "maximum_portfolio_age_seconds": request.allocation_policy.maximum_portfolio_age_seconds,
            "sizing_policy": {
                "per_trade_risk_fraction": str(sizing.per_trade_risk_fraction),
                "maximum_total_open_risk_fraction": str(sizing.maximum_total_open_risk_fraction),
                "maximum_position_notional_fraction": str(sizing.maximum_position_notional_fraction),
                "maximum_gross_exposure_fraction": str(sizing.maximum_gross_exposure_fraction),
                "maximum_daily_turnover_participation": str(
                    sizing.maximum_daily_turnover_participation
                ),
                "maximum_top_ask_participation": str(sizing.maximum_top_ask_participation),
                "maximum_daily_loss_fraction": str(sizing.maximum_daily_loss_fraction),
                "maximum_pilot_drawdown_fraction": str(sizing.maximum_pilot_drawdown_fraction),
                "minimum_net_reward_risk": str(sizing.minimum_net_reward_risk),
                "maximum_open_positions": sizing.maximum_open_positions,
                "maximum_new_positions_per_run": sizing.maximum_new_positions_per_run,
            },
        },
        "maximum_quote_chunk_size": request.maximum_quote_chunk_size,
        "binding_bucket": request.binding_bucket,
    }


def _encode_request(request: PromotedOperationalLaunchRequest) -> bytes:
    return _reencode(_request_body(request))


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


class PromotedOperationalLaunchTests(unittest.TestCase):
    def test_strict_decoder_round_trips_and_rejects_malformed_payloads(self) -> None:
        preparation = _build_preparation()
        portfolio = _assembly_tests._portfolio_snapshot(
            as_of=_decision_window_for(preparation.manifest.target_session)[0] - timedelta(seconds=10),
            open_positions=0,
        )
        artifact = _assembly_tests._portfolio_artifact(portfolio)
        request = _request_for(preparation, artifact)
        payload = _encode_request(request)

        decoded = decode_promoted_operational_launch_request(payload)
        self.assertEqual(decoded, request)

        with self.subTest(case="duplicate_key_top_level"):
            needle = b'"maximum_quote_chunk_size":500,'
            self.assertEqual(payload.count(needle), 1)
            duplicated = payload.replace(needle, needle + needle, 1)
            with self.assertRaises(PromotedOperationalLaunchError):
                decode_promoted_operational_launch_request(duplicated)

        with self.subTest(case="duplicate_key_nested"):
            needle = b'"minimum_net_reward_risk":"2.50",'
            self.assertEqual(payload.count(needle), 1)
            duplicated = payload.replace(needle, needle + needle, 1)
            with self.assertRaises(PromotedOperationalLaunchError):
                decode_promoted_operational_launch_request(duplicated)

        with self.subTest(case="unknown_key_top_level"):
            root = json.loads(payload)
            root["unexpected"] = "x"
            with self.assertRaises(PromotedOperationalLaunchError):
                decode_promoted_operational_launch_request(_reencode(root))

        with self.subTest(case="missing_key_nested_sizing"):
            root = json.loads(payload)
            del root["allocation_policy"]["sizing_policy"]["minimum_net_reward_risk"]
            with self.assertRaises(PromotedOperationalLaunchError):
                decode_promoted_operational_launch_request(_reencode(root))

        text = payload.decode("utf-8")
        int_needle = '"maximum_quote_chunk_size":500'
        self.assertIn(int_needle, text)

        with self.subTest(case="float_value"):
            mutated = text.replace(int_needle, '"maximum_quote_chunk_size":500.0', 1).encode()
            with self.assertRaises(PromotedOperationalLaunchError):
                decode_promoted_operational_launch_request(mutated)

        with self.subTest(case="nan_value"):
            mutated = text.replace(int_needle, '"maximum_quote_chunk_size":NaN', 1).encode()
            with self.assertRaises(PromotedOperationalLaunchError):
                decode_promoted_operational_launch_request(mutated)

        with self.subTest(case="infinity_value"):
            mutated = text.replace(int_needle, '"maximum_quote_chunk_size":Infinity', 1).encode()
            with self.assertRaises(PromotedOperationalLaunchError):
                decode_promoted_operational_launch_request(mutated)

        with self.subTest(case="bool_as_int"):
            root = json.loads(payload)
            root["maximum_quote_chunk_size"] = True
            with self.assertRaises(PromotedOperationalLaunchError):
                decode_promoted_operational_launch_request(_reencode(root))

        with self.subTest(case="noncanonical_decimal"):
            root = json.loads(payload)
            root["quote_gate_policy"]["maximum_spread_bps"] = "5E1"
            with self.assertRaises(PromotedOperationalLaunchError):
                decode_promoted_operational_launch_request(_reencode(root))

        with self.subTest(case="noncanonical_timestamp_offset_suffix"):
            root = json.loads(payload)
            root["decision_not_before"] = root["decision_not_before"][:-1] + "+00:00"
            with self.assertRaises(PromotedOperationalLaunchError):
                decode_promoted_operational_launch_request(_reencode(root))

        with self.subTest(case="malformed_listing_keys"):
            root = json.loads(payload)
            root["open_listing_keys"] = ["BSE:FOO"]
            with self.assertRaises(PromotedOperationalLaunchError):
                decode_promoted_operational_launch_request(_reencode(root))

        with self.subTest(case="malformed_preparation_id"):
            root = json.loads(payload)
            root["preparation_id"] = "not-a-sha"
            with self.assertRaises(PromotedOperationalLaunchError):
                decode_promoted_operational_launch_request(_reencode(root))

        with self.subTest(case="malformed_bucket"):
            root = json.loads(payload)
            root["binding_bucket"] = "Invalid_Bucket!"
            with self.assertRaises(PromotedOperationalLaunchError):
                decode_promoted_operational_launch_request(_reencode(root))

        with self.subTest(case="invalid_window_order"):
            root = json.loads(payload)
            root["decision_not_before"], root["decision_deadline"] = (
                root["decision_deadline"],
                root["decision_not_before"],
            )
            with self.assertRaises(PromotedOperationalLaunchError):
                decode_promoted_operational_launch_request(_reencode(root))

        with self.subTest(case="oversized"):
            oversized = payload + b" " * MAXIMUM_LAUNCH_REQUEST_BYTES
            with self.assertRaises(PromotedOperationalLaunchError):
                decode_promoted_operational_launch_request(oversized)

        with self.subTest(case="sanitized_cause_context"):
            try:
                decode_promoted_operational_launch_request(b"not json")
                self.fail("expected PromotedOperationalLaunchError")
            except PromotedOperationalLaunchError as exc:
                self.assertIsNone(exc.__cause__)
                self.assertIsNone(exc.__context__)

    def test_resolver_ordering_and_dry_assembly_reuses_pinned_parents(self) -> None:
        preparation = _build_preparation()
        portfolio = _assembly_tests._portfolio_snapshot(
            as_of=_decision_window_for(preparation.manifest.target_session)[0] - timedelta(seconds=10),
            open_positions=0,
        )
        artifact = _assembly_tests._portfolio_artifact(portfolio)
        request = _request_for(preparation, artifact)

        with self.subTest(case="preparation_resolver_raises_blocks_portfolio_resolver"):
            prep_resolver = _FakePreparationResolver(preparation, raise_exc=RuntimeError("boom"))
            with self.assertRaises(PromotedOperationalLaunchError):
                prepare_promoted_operational_launch(
                    request=request,
                    preparation_resolver=prep_resolver,
                    portfolio_artifact_resolver=_AssertingPortfolioArtifactResolver(),
                )
            self.assertEqual(prep_resolver.calls, [request.preparation_id])

        with self.subTest(case="preparation_id_mismatch_blocks_portfolio_resolver"):
            prep_resolver = _FakePreparationResolver(preparation)
            mismatched_request = PromotedOperationalLaunchRequest(
                schema_version=request.schema_version,
                preparation_id="0" * 64,
                portfolio_artifact_id=request.portfolio_artifact_id,
                expected_quote_source_id=request.expected_quote_source_id,
                open_listing_keys=request.open_listing_keys,
                decision_not_before=request.decision_not_before,
                decision_deadline=request.decision_deadline,
                quote_gate_policy=request.quote_gate_policy,
                allocation_policy=request.allocation_policy,
                maximum_quote_chunk_size=request.maximum_quote_chunk_size,
                binding_bucket=request.binding_bucket,
            )
            with self.assertRaises(PromotedOperationalLaunchError):
                prepare_promoted_operational_launch(
                    request=mismatched_request,
                    preparation_resolver=prep_resolver,
                    portfolio_artifact_resolver=_AssertingPortfolioArtifactResolver(),
                )
            self.assertEqual(prep_resolver.calls, ["0" * 64])

        with self.subTest(case="artifact_resolver_raises_blocks_spec_construction"):
            prep_resolver = _FakePreparationResolver(preparation)
            artifact_resolver = _FakePortfolioArtifactResolver(artifact, raise_exc=RuntimeError("boom"))
            with self.assertRaises(PromotedOperationalLaunchError):
                prepare_promoted_operational_launch(
                    request=request,
                    preparation_resolver=prep_resolver,
                    portfolio_artifact_resolver=artifact_resolver,
                )
            self.assertEqual(prep_resolver.calls, [request.preparation_id])
            self.assertEqual(artifact_resolver.calls, [request.portfolio_artifact_id])

        with self.subTest(case="valid_request_calls_each_resolver_exactly_once"):
            prep_resolver = _FakePreparationResolver(preparation)
            artifact_resolver = _FakePortfolioArtifactResolver(artifact)
            assembly = prepare_promoted_operational_launch(
                request=request,
                preparation_resolver=prep_resolver,
                portfolio_artifact_resolver=artifact_resolver,
            )
            self.assertEqual(prep_resolver.calls, [request.preparation_id])
            self.assertEqual(artifact_resolver.calls, [request.portfolio_artifact_id])
            self.assertIs(assembly.preparation, preparation)
            self.assertIs(assembly.portfolio_artifact, artifact)

    def test_valid_request_derives_ids_exactly_and_is_stable(self) -> None:
        preparation = _build_preparation()
        portfolio = _assembly_tests._portfolio_snapshot(
            as_of=_decision_window_for(preparation.manifest.target_session)[0] - timedelta(seconds=10),
            open_positions=0,
        )
        artifact = _assembly_tests._portfolio_artifact(portfolio)
        request = _request_for(preparation, artifact)

        assembly = prepare_promoted_operational_launch(
            request=request,
            preparation_resolver=_FakePreparationResolver(preparation),
            portfolio_artifact_resolver=_FakePortfolioArtifactResolver(artifact),
        )
        spec = assembly.assembly_spec
        self.assertEqual(spec.target_session, preparation.manifest.target_session)
        self.assertEqual(spec.expected_portfolio_snapshot_id, artifact.portfolio_snapshot_id)
        self.assertEqual(spec.preparation_id, request.preparation_id)
        self.assertEqual(spec.portfolio_artifact_id, request.portfolio_artifact_id)
        self.assertEqual(spec.expected_quote_source_id, request.expected_quote_source_id)
        self.assertIs(spec.paper_only, True)
        self.assertIs(spec.notification_eligible, False)
        self.assertIs(spec.execution_eligible, False)

        second = prepare_promoted_operational_launch(
            request=request,
            preparation_resolver=_FakePreparationResolver(preparation),
            portfolio_artifact_resolver=_FakePortfolioArtifactResolver(artifact),
        )
        self.assertEqual(second.assembly_spec.assembly_spec_id, spec.assembly_spec_id)
        self.assertEqual(
            second.assembly_spec.quote_gate_policy.policy_id, spec.quote_gate_policy.policy_id
        )
        self.assertEqual(
            second.assembly_spec.allocation_policy.allocation_policy_id,
            spec.allocation_policy.allocation_policy_id,
        )

    def test_create_once_output_behavior(self) -> None:
        preparation = _build_preparation()
        portfolio = _assembly_tests._portfolio_snapshot(
            as_of=_decision_window_for(preparation.manifest.target_session)[0] - timedelta(seconds=10),
            open_positions=0,
        )
        artifact = _assembly_tests._portfolio_artifact(portfolio)
        request = _request_for(preparation, artifact)
        assembly = prepare_promoted_operational_launch(
            request=request,
            preparation_resolver=_FakePreparationResolver(preparation),
            portfolio_artifact_resolver=_FakePortfolioArtifactResolver(artifact),
        )
        spec = assembly.assembly_spec

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()

            with self.subTest(case="new_write_and_cold_read"):
                output_path = root / "assembly.json"
                publish_promoted_operational_launch_assembly_spec_file(output_path, spec)
                loaded = load_promoted_operational_assembly_spec_file(output_path)
                self.assertEqual(loaded, spec)

            with self.subTest(case="identical_replay_is_idempotent"):
                output_path = root / "assembly.json"
                publish_promoted_operational_launch_assembly_spec_file(output_path, spec)

            with self.subTest(case="byte_different_conflict_fails_closed"):
                conflict_path = root / "conflict.json"
                conflict_path.write_bytes(b"not the same content at all\n")
                with self.assertRaises(PromotedOperationalLaunchError):
                    publish_promoted_operational_launch_assembly_spec_file(conflict_path, spec)
                self.assertEqual(conflict_path.read_bytes(), b"not the same content at all\n")

            with self.subTest(case="empty_existing_target_fails_closed"):
                empty_path = root / "empty.json"
                empty_path.write_bytes(b"")
                with self.assertRaises(PromotedOperationalLaunchError):
                    publish_promoted_operational_launch_assembly_spec_file(empty_path, spec)

            with self.subTest(case="missing_parent_fails_closed"):
                missing_parent_path = root / "no-such-dir" / "assembly.json"
                with self.assertRaises(PromotedOperationalLaunchError):
                    publish_promoted_operational_launch_assembly_spec_file(missing_parent_path, spec)

            with self.subTest(case="link_parent_fails_closed"):
                real_dir = root / "real-dir"
                real_dir.mkdir()
                link_dir = root / "link-dir"
                try:
                    link_dir.symlink_to(real_dir, target_is_directory=True)
                except OSError:
                    self.skipTest("symlinks not permitted in this environment")
                with self.assertRaises(PromotedOperationalLaunchError):
                    publish_promoted_operational_launch_assembly_spec_file(
                        link_dir / "assembly.json", spec
                    )

            with self.subTest(case="link_target_fails_closed"):
                real_file = root / "real.json"
                publish_promoted_operational_launch_assembly_spec_file(real_file, spec)
                link_target = root / "link.json"
                try:
                    link_target.symlink_to(real_file)
                except OSError:
                    self.skipTest("symlinks not permitted in this environment")
                with self.assertRaises(PromotedOperationalLaunchError):
                    publish_promoted_operational_launch_assembly_spec_file(link_target, spec)

    def test_cli_end_to_end_success_and_output_compatibility(self) -> None:
        preparation = _build_preparation()
        portfolio = _assembly_tests._portfolio_snapshot(
            as_of=_decision_window_for(preparation.manifest.target_session)[0] - timedelta(seconds=10),
            open_positions=0,
        )
        artifact = _assembly_tests._portfolio_artifact(portfolio)
        request = _request_for(preparation, artifact)

        with tempfile.TemporaryDirectory() as directory:
            tmp_path = Path(directory).resolve()
            portfolio_artifact_root = tmp_path / "portfolio-artifact"
            LocalSwingPortfolioArtifactStore(portfolio_artifact_root).put(artifact)

            request_file = tmp_path / "request.json"
            request_file.write_bytes(_encode_request(request))

            output_dir = tmp_path / "output"
            output_dir.mkdir()
            output_file = output_dir / "assembly.json"

            argv = ["--launch-request-file", str(request_file)]
            for option in _launch_cli_module._ROOT_OPTIONS:
                argv.extend([option, str(tmp_path / option.strip("-"))])
            argv.extend(["--portfolio-artifact-root", str(portfolio_artifact_root)])
            argv.extend(["--output-assembly-spec-file", str(output_file)])

            with mock.patch.object(
                _launch_cli_module,
                "build_promoted_operational_preparation_store",
                return_value=(None, _FakePreparationResolver(preparation)),
            ):
                stdout = io.StringIO()
                stderr = io.StringIO()
                with redirect_stdout(stdout), redirect_stderr(stderr):
                    code = _launch_cli_module.main(argv)
                out, err = stdout.getvalue(), stderr.getvalue()

            self.assertEqual(code, 0)
            self.assertEqual(err, "")
            envelope = json.loads(out)
            self.assertEqual(
                set(envelope),
                {
                    "status",
                    "assembly_spec_id",
                    "preparation_id",
                    "portfolio_artifact_id",
                    "portfolio_snapshot_id",
                    "target_session",
                    "decision_not_before",
                    "decision_deadline",
                    "quote_gate_policy_id",
                    "allocation_policy_id",
                    "expected_quote_source_id",
                    "candidate_count",
                    "open_position_count",
                    "paper_only",
                    "notification_eligible",
                    "execution_eligible",
                },
            )
            self.assertEqual(envelope["status"], "PROMOTED_OPERATIONAL_LAUNCH_READY")
            self.assertEqual(envelope["candidate_count"], len(preparation.candidates))
            self.assertEqual(envelope["open_position_count"], 0)
            self.assertIs(envelope["paper_only"], True)
            self.assertIs(envelope["notification_eligible"], False)
            self.assertIs(envelope["execution_eligible"], False)

            spec = load_promoted_operational_assembly_spec_file(output_file)
            self.assertEqual(spec.assembly_spec_id, envelope["assembly_spec_id"])

            with self.subTest(case="parser_rejects_missing_option"):
                broken_argv = list(argv[:-2])
                stdout2 = io.StringIO()
                stderr2 = io.StringIO()
                with redirect_stdout(stdout2), redirect_stderr(stderr2):
                    code2 = _launch_cli_module.main(broken_argv)
                self.assertEqual(code2, 2)
                self.assertEqual(stdout2.getvalue(), "")
                self.assertEqual(
                    json.loads(stderr2.getvalue()),
                    {"error_type": "PromotedOperationalLaunchError", "status": "FAILED"},
                )

    def test_request_graph_replay_rejects_every_post_construction_tamper(self) -> None:
        preparation = _build_preparation()
        portfolio = _assembly_tests._portfolio_snapshot(
            as_of=_decision_window_for(preparation.manifest.target_session)[0] - timedelta(seconds=10),
            open_positions=0,
        )
        artifact = _assembly_tests._portfolio_artifact(portfolio)

        def _assert_rejected(request) -> None:
            prep_resolver = _FakePreparationResolver(preparation)
            with self.assertRaises(PromotedOperationalLaunchError) as ctx:
                prepare_promoted_operational_launch(
                    request=request,
                    preparation_resolver=prep_resolver,
                    portfolio_artifact_resolver=_AssertingPortfolioArtifactResolver(),
                )
            self.assertIsNone(ctx.exception.__cause__)
            self.assertIsNone(ctx.exception.__context__)
            self.assertEqual(prep_resolver.calls, [])

        with self.subTest(case="preparation_id_tampered_to_secret"):
            request = _request_for(preparation, artifact)
            object.__setattr__(request, "preparation_id", "SECRET")
            _assert_rejected(request)

        with self.subTest(case="portfolio_artifact_id_tampered"):
            request = _request_for(preparation, artifact)
            object.__setattr__(request, "portfolio_artifact_id", "not-a-sha")
            _assert_rejected(request)

        with self.subTest(case="decision_timestamp_tampered"):
            request = _request_for(preparation, artifact)
            object.__setattr__(request, "decision_deadline", request.decision_not_before)
            _assert_rejected(request)

        with self.subTest(case="open_listing_keys_tampered"):
            request = _request_for(preparation, artifact)
            object.__setattr__(request, "open_listing_keys", ("BSE:FOO",))
            _assert_rejected(request)

        with self.subTest(case="quote_policy_scalar_tampered"):
            request = _request_for(preparation, artifact)
            object.__setattr__(request.quote_gate_policy, "maximum_spread_bps", "999999")
            _assert_rejected(request)

        with self.subTest(case="sizing_scalar_tampered"):
            request = _request_for(preparation, artifact)
            object.__setattr__(
                request.allocation_policy.sizing_policy, "maximum_open_positions", 4.0
            )
            _assert_rejected(request)

        with self.subTest(case="allocation_age_tampered"):
            request = _request_for(preparation, artifact)
            object.__setattr__(request.allocation_policy, "maximum_portfolio_age_seconds", "300")
            _assert_rejected(request)

        with self.subTest(case="nested_request_replaced_with_object"):
            request = _request_for(preparation, artifact)
            object.__setattr__(request, "quote_gate_policy", object())
            _assert_rejected(request)

    def test_session_gate_rejects_window_off_target_session_before_portfolio_resolver(self) -> None:
        preparation = _build_preparation()
        portfolio = _assembly_tests._portfolio_snapshot(
            as_of=_decision_window_for(preparation.manifest.target_session)[0] - timedelta(seconds=10),
            open_positions=0,
        )
        artifact = _assembly_tests._portfolio_artifact(portfolio)
        request = _request_for(preparation, artifact)
        shifted_request = PromotedOperationalLaunchRequest(
            schema_version=request.schema_version,
            preparation_id=request.preparation_id,
            portfolio_artifact_id=request.portfolio_artifact_id,
            expected_quote_source_id=request.expected_quote_source_id,
            open_listing_keys=request.open_listing_keys,
            decision_not_before=request.decision_not_before + timedelta(days=1),
            decision_deadline=request.decision_deadline + timedelta(days=1),
            quote_gate_policy=request.quote_gate_policy,
            allocation_policy=request.allocation_policy,
            maximum_quote_chunk_size=request.maximum_quote_chunk_size,
            binding_bucket=request.binding_bucket,
        )

        prep_resolver = _FakePreparationResolver(preparation)
        with self.assertRaises(PromotedOperationalLaunchError) as ctx:
            prepare_promoted_operational_launch(
                request=shifted_request,
                preparation_resolver=prep_resolver,
                portfolio_artifact_resolver=_AssertingPortfolioArtifactResolver(),
            )
        self.assertIsNone(ctx.exception.__cause__)
        self.assertIsNone(ctx.exception.__context__)
        self.assertEqual(prep_resolver.calls, [request.preparation_id])

    def test_publisher_output_safety_wrong_type_stale_id_oversized_and_partial_write(self) -> None:
        preparation = _build_preparation()
        portfolio = _assembly_tests._portfolio_snapshot(
            as_of=_decision_window_for(preparation.manifest.target_session)[0] - timedelta(seconds=10),
            open_positions=0,
        )
        artifact = _assembly_tests._portfolio_artifact(portfolio)
        request = _request_for(preparation, artifact)
        assembly = prepare_promoted_operational_launch(
            request=request,
            preparation_resolver=_FakePreparationResolver(preparation),
            portfolio_artifact_resolver=_FakePortfolioArtifactResolver(artifact),
        )
        spec = assembly.assembly_spec
        payload = _launch_module.encode_promoted_operational_assembly_spec(spec)

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()

            with self.subTest(case="wrong_type_spec_rejected"):
                output_path = root / "wrong-type.json"
                with self.assertRaises(PromotedOperationalLaunchError) as ctx:
                    publish_promoted_operational_launch_assembly_spec_file(output_path, object())
                self.assertIsNone(ctx.exception.__cause__)
                self.assertIsNone(ctx.exception.__context__)
                self.assertFalse(output_path.exists())

            with self.subTest(case="stale_id_spec_rejected"):
                original_id = spec.assembly_spec_id
                object.__setattr__(spec, "assembly_spec_id", "0" * 64)
                output_path = root / "stale-id.json"
                try:
                    with self.assertRaises(PromotedOperationalLaunchError) as ctx:
                        publish_promoted_operational_launch_assembly_spec_file(output_path, spec)
                    self.assertIsNone(ctx.exception.__cause__)
                    self.assertIsNone(ctx.exception.__context__)
                    self.assertFalse(output_path.exists())
                finally:
                    object.__setattr__(spec, "assembly_spec_id", original_id)

            with self.subTest(case="oversized_existing_target_fails_closed"):
                oversized_path = root / "oversized.json"
                oversized_bytes = b"x" * (MAXIMUM_ASSEMBLY_SPEC_BYTES + 1)
                oversized_path.write_bytes(oversized_bytes)
                with self.assertRaises(PromotedOperationalLaunchError) as ctx:
                    publish_promoted_operational_launch_assembly_spec_file(oversized_path, spec)
                self.assertIsNone(ctx.exception.__cause__)
                self.assertIsNone(ctx.exception.__context__)
                self.assertEqual(oversized_path.read_bytes(), oversized_bytes)

            with self.subTest(case="partial_write_failure_leaves_poisoned_file_and_retry_fails_closed"):
                output_path = root / "partial.json"

                def _fake_fdopen(descriptor, mode):
                    real_handle = open(descriptor, mode)

                    class _PartialFailingHandle:
                        def __enter__(self):
                            return self

                        def write(self, data):
                            partial = data[: len(data) // 2]
                            real_handle.write(partial)
                            real_handle.flush()
                            raise OSError("simulated partial write failure")

                        def fileno(self):
                            return real_handle.fileno()

                        def __exit__(self, exc_type, exc, tb):
                            real_handle.close()
                            return False

                    return _PartialFailingHandle()

                with mock.patch.object(_launch_module.os, "fdopen", side_effect=_fake_fdopen):
                    with self.assertRaises(PromotedOperationalLaunchError) as ctx:
                        publish_promoted_operational_launch_assembly_spec_file(output_path, spec)
                self.assertIsNone(ctx.exception.__cause__)
                self.assertIsNone(ctx.exception.__context__)
                self.assertTrue(output_path.exists())
                poisoned_bytes = output_path.read_bytes()
                self.assertNotEqual(poisoned_bytes, payload)

                with self.assertRaises(PromotedOperationalLaunchError) as ctx2:
                    publish_promoted_operational_launch_assembly_spec_file(output_path, spec)
                self.assertIsNone(ctx2.exception.__cause__)
                self.assertIsNone(ctx2.exception.__context__)
                self.assertEqual(output_path.read_bytes(), poisoned_bytes)

    def test_module_has_no_discovery_network_runtime_environment_or_clock_capability(self) -> None:
        for module in (_launch_module, _launch_cli_module):
            source = inspect.getsource(module)
            for forbidden in (
                "glob(",
                "iterdir(",
                "listdir(",
                "rglob(",
                "os.environ",
                "os.getenv",
                "datetime.now(",
                "datetime.utcnow(",
                ".utcnow(",
                ".today(",
                "time.time(",
                "import subprocess",
                "subprocess.",
                "import requests",
                "import urllib",
                "import socket",
                "place_order",
                "modify_order",
                "cancel_order",
            ):
                self.assertNotIn(forbidden, source)
            ast.parse(source)
            for forbidden_symbol in (
                "KiteCredentials",
                "KiteMarketDataAdapter",
                "KiteSwingQuoteSource",
                "GoogleCloudStorageStateObjectWriter",
                "GoogleCloudStorageTerminalBindingReader",
                "run_promoted_operational_runtime_job",
                "TelegramBotConfig",
                "deliver_telegram_notification",
            ):
                self.assertFalse(hasattr(module, forbidden_symbol))


if __name__ == "__main__":
    unittest.main()
