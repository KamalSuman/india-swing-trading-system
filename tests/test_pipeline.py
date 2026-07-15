from __future__ import annotations

import json
import tempfile
import unittest
from dataclasses import asdict, replace
from datetime import date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch

from india_swing.audit import AuditExistsError, AuditIntegrityError, AuditWriter
from india_swing.demo import IST, build_demo
from india_swing.domain.models import (
    DecisionAction,
    ResearchVerdict,
    RunStatus,
    Surveillance,
)
from india_swing.reference.context import ReferenceContext, validate_reference_context
from india_swing.reference.calendar import CalendarSnapshot
from india_swing.reference.models import ReferenceReadiness
from india_swing.reference.universe import UniverseDisposition, UniverseSnapshot
from india_swing.risk.engine import RiskEngine


class PipelineIntegrationTests(unittest.TestCase):
    @staticmethod
    def _rebind_universe(snapshot, instruments, reference_context, universe):
        rebound_snapshot = replace(
            snapshot,
            universe_snapshot_id=universe.snapshot_id,
        )
        rebound_instruments = [
            replace(instrument, universe_snapshot_id=universe.snapshot_id)
            for instrument in instruments
        ]
        rebound_context = ReferenceContext(
            calendar=reference_context.calendar,
            universe=universe,
        )
        return rebound_snapshot, rebound_instruments, rebound_context

    def test_provider_cannot_upgrade_synthetic_references_after_validation(self) -> None:
        pipeline, snapshot, instruments, portfolio, reference_context = build_demo()
        original_forecast = pipeline.forecast_provider.forecast

        def mutate_readiness(instrument, current_snapshot):
            object.__setattr__(
                reference_context.calendar,
                "readiness",
                ReferenceReadiness.POINT_IN_TIME_VERIFIED,
            )
            object.__setattr__(
                reference_context.universe,
                "readiness",
                ReferenceReadiness.POINT_IN_TIME_VERIFIED,
            )
            return original_forecast(instrument, current_snapshot)

        with patch.object(
            pipeline.forecast_provider,
            "forecast",
            side_effect=mutate_readiness,
        ):
            result = pipeline.run(
                snapshot,
                instruments,
                portfolio,
                reference_context,
            )

        self.assertIs(result.status, RunStatus.FAILED)
        self.assertIs(result.decision.action, DecisionAction.NO_TRADE)
        self.assertEqual(result.failure_stage, "final_integrity")
        self.assertEqual(result.reference_readiness, "INVALID")
        self.assertFalse(result.decision.execution_eligible)
        self.assertTrue(result.validated_input_fingerprint)
        self.assertNotEqual(
            result.validated_input_fingerprint,
            result.final_input_fingerprint,
        )

    def test_provider_configuration_change_during_run_fails_closed(self) -> None:
        pipeline, snapshot, instruments, portfolio, reference_context = build_demo()
        original_forecast = pipeline.forecast_provider.forecast

        def mutate_signal_configuration(instrument, current_snapshot):
            signals, setup, evidence_ids = pipeline.signal_provider.values[
                "DEMO-SMALL"
            ]
            pipeline.signal_provider.values["DEMO-SMALL"] = (
                signals,
                replace(setup, target=Decimal("116")),
                evidence_ids,
            )
            return original_forecast(instrument, current_snapshot)

        with patch.object(
            pipeline.forecast_provider,
            "forecast",
            side_effect=mutate_signal_configuration,
        ):
            result = pipeline.run(
                snapshot,
                instruments,
                portfolio,
                reference_context,
            )

        self.assertIs(result.status, RunStatus.FAILED)
        self.assertIs(result.decision.action, DecisionAction.NO_TRADE)
        self.assertEqual(result.failure_stage, "final_integrity")
        self.assertNotEqual(
            result.validated_input_fingerprint,
            result.final_input_fingerprint,
        )

    def test_runtime_policy_replacement_cannot_raise_the_risk_limit(self) -> None:
        pipeline, snapshot, instruments, portfolio, reference_context = build_demo()
        original_forecast = pipeline.forecast_provider.forecast

        def replace_declared_policy(instrument, current_snapshot):
            pipeline.policy = replace(
                pipeline.policy,
                per_trade_risk=Decimal("1000"),
                max_open_risk=Decimal("1000"),
                max_position_notional=Decimal("100000"),
                max_gross_exposure=Decimal("100000"),
            )
            return original_forecast(instrument, current_snapshot)

        with patch.object(
            pipeline.forecast_provider,
            "forecast",
            side_effect=replace_declared_policy,
        ):
            result = pipeline.run(
                snapshot,
                instruments,
                portfolio,
                reference_context,
            )

        self.assertIs(result.status, RunStatus.FAILED)
        self.assertIs(result.decision.action, DecisionAction.NO_TRADE)
        self.assertEqual(result.failure_stage, "final_integrity")
        self.assertNotEqual(
            result.validated_input_fingerprint,
            result.final_input_fingerprint,
        )

    def test_injected_alternate_risk_engine_is_never_used(self) -> None:
        pipeline, snapshot, instruments, portfolio, reference_context = build_demo()
        loose_policy = replace(
            pipeline.policy,
            per_trade_risk=Decimal("1000"),
            max_open_risk=Decimal("1000"),
            max_position_notional=Decimal("100000"),
            max_gross_exposure=Decimal("100000"),
        )
        pipeline.risk_engine = RiskEngine(loose_policy)

        result = pipeline.run(
            snapshot,
            instruments,
            portfolio,
            reference_context,
        )

        self.assertIs(result.status, RunStatus.COMPLETE)
        self.assertIs(result.decision.action, DecisionAction.BUY)
        self.assertLessEqual(
            result.decision.planned_max_loss,
            pipeline.policy.per_trade_risk,
        )

    def test_corrupted_snapshot_timestamp_returns_failed_result(self) -> None:
        pipeline, snapshot, instruments, portfolio, reference_context = build_demo()
        object.__setattr__(
            snapshot,
            "decision_time",
            datetime(2026, 7, 15, 17, 0),
        )

        result = pipeline.run(
            snapshot,
            instruments,
            portfolio,
            reference_context,
        )

        self.assertIs(result.status, RunStatus.FAILED)
        self.assertIs(result.decision.action, DecisionAction.NO_TRADE)
        self.assertEqual(result.failure_stage, "snapshot_integrity")
        self.assertEqual(result.reference_readiness, "INVALID")

    def test_corrupted_reference_timestamp_returns_failed_result(self) -> None:
        pipeline, snapshot, instruments, portfolio, reference_context = build_demo()
        object.__setattr__(
            reference_context.calendar,
            "cutoff",
            datetime(2026, 7, 15, 18, 0),
        )

        result = pipeline.run(
            snapshot,
            instruments,
            portfolio,
            reference_context,
        )

        self.assertIs(result.status, RunStatus.FAILED)
        self.assertIs(result.decision.action, DecisionAction.NO_TRADE)
        self.assertEqual(result.failure_stage, "reference_integrity")
        self.assertEqual(result.reference_readiness, "INVALID")

    def test_research_provider_cannot_mutate_a_validated_entry_window(self) -> None:
        pipeline, snapshot, instruments, portfolio, reference_context = build_demo()
        original_assess = pipeline.research_provider.assess

        def mutate_candidate(candidate, current_snapshot):
            assessment = original_assess(candidate, current_snapshot)
            object.__setattr__(
                candidate.setup,
                "earliest_entry_at",
                datetime(2026, 7, 16, 8, 0, tzinfo=IST),
            )
            object.__setattr__(
                candidate.setup,
                "entry_expires_at",
                datetime(2026, 7, 16, 8, 30, tzinfo=IST),
            )
            return assessment

        with patch.object(
            pipeline.research_provider,
            "assess",
            side_effect=mutate_candidate,
        ):
            result = pipeline.run(
                snapshot,
                instruments,
                portfolio,
                reference_context,
            )

        self.assertIs(result.status, RunStatus.FAILED)
        self.assertIs(result.decision.action, DecisionAction.NO_TRADE)
        self.assertEqual(result.failure_stage, "research_integrity")

    def test_provider_outputs_must_report_the_configured_versions(self) -> None:
        pipeline, snapshot, instruments, portfolio, reference_context = build_demo()
        pipeline.forecast_provider.forecasts["DEMO-SMALL"] = replace(
            pipeline.forecast_provider.forecasts["DEMO-SMALL"],
            model_version="forged-model-version",
        )

        result = pipeline.run(snapshot, instruments, portfolio, reference_context)

        self.assertIs(result.status, RunStatus.FAILED)
        self.assertIs(result.decision.action, DecisionAction.NO_TRADE)
        self.assertEqual(result.failure_stage, "candidate_integrity")

    def test_signal_features_cannot_be_swapped_between_instruments(self) -> None:
        pipeline, snapshot, instruments, portfolio, reference_context = build_demo()
        large_signals = pipeline.signal_provider.values["DEMO-LARGE"][0]
        _, small_setup, small_evidence = pipeline.signal_provider.values["DEMO-SMALL"]
        pipeline.signal_provider.values["DEMO-SMALL"] = (
            large_signals,
            small_setup,
            small_evidence,
        )

        result = pipeline.run(snapshot, instruments, portfolio, reference_context)

        self.assertIs(result.status, RunStatus.FAILED)
        self.assertIs(result.decision.action, DecisionAction.NO_TRADE)
        self.assertIn(result.failure_stage, {"candidate_build", "candidate_integrity"})

    def test_cached_outputs_cannot_survive_changed_instrument_market_data(self) -> None:
        pipeline, snapshot, instruments, portfolio, reference_context = build_demo()
        changed_instruments = [
            replace(instruments[0], last_price=Decimal("10000")),
            instruments[1],
        ]

        result = pipeline.run(
            snapshot,
            changed_instruments,
            portfolio,
            reference_context,
        )

        self.assertIs(result.status, RunStatus.FAILED)
        self.assertIs(result.decision.action, DecisionAction.NO_TRADE)
        self.assertIn(result.failure_stage, {"candidate_build", "candidate_integrity"})

    def test_reference_scopes_must_bind_exchange_and_segment(self) -> None:
        pipeline, snapshot, instruments, portfolio, reference_context = build_demo()
        wrong_exchange_calendar = replace(
            reference_context.calendar,
            exchange="BSE",
        )
        invalid_context = ReferenceContext(
            calendar=wrong_exchange_calendar,
            universe=reference_context.universe,
        )

        result = pipeline.run(snapshot, instruments, portfolio, invalid_context)

        self.assertIs(result.status, RunStatus.FAILED)
        self.assertEqual(result.failure_stage, "reference_integrity")

    def test_listing_and_eligibility_must_remain_valid_through_entry_session(self) -> None:
        pipeline, snapshot, instruments, portfolio, reference_context = build_demo()
        small_entry = next(
            entry
            for entry in reference_context.universe.entries
            if entry.listing.tradingsymbol == "DEMO-SMALL"
        )
        expiring_entry = replace(
            small_entry,
            listing=replace(
                small_entry.listing,
                valid_to_exclusive=date(2026, 7, 16),
            ),
            eligibility_refs=tuple(
                replace(
                    state,
                    effective=replace(
                        state.effective,
                        effective_to_exclusive=date(2026, 7, 16),
                    ),
                )
                for state in small_entry.eligibility_refs
            ),
        )
        entries = tuple(
            expiring_entry if entry is small_entry else entry
            for entry in reference_context.universe.entries
        )
        expiring_universe = replace(reference_context.universe, entries=entries)
        expiring_context = ReferenceContext(
            calendar=reference_context.calendar,
            universe=expiring_universe,
        )
        rebound_snapshot = replace(
            snapshot,
            universe_snapshot_id=expiring_universe.snapshot_id,
        )
        rebound_instruments = [
            replace(
                instrument,
                universe_snapshot_id=expiring_universe.snapshot_id,
            )
            for instrument in instruments
        ]

        result = pipeline.run(
            rebound_snapshot,
            rebound_instruments,
            portfolio,
            expiring_context,
        )

        self.assertIs(result.status, RunStatus.FAILED)
        self.assertEqual(result.failure_stage, "reference_integrity")

    def test_adjacent_same_fact_eligibility_rollover_is_entry_valid(self) -> None:
        _, snapshot, instruments, _, reference_context = build_demo()
        small_entry = next(
            entry
            for entry in reference_context.universe.entries
            if entry.listing.tradingsymbol == "DEMO-SMALL"
        )
        state = small_entry.eligibility_refs[0]
        entry_session = date(2026, 7, 16)
        current_state = replace(
            state,
            effective=replace(
                state.effective,
                effective_to_exclusive=entry_session,
            ),
        )
        next_state = replace(
            state,
            effective=replace(
                state.effective,
                reference=replace(
                    state.effective.reference,
                    content_hash="0" * 64,
                ),
                effective_from_session=entry_session,
                effective_to_exclusive=None,
            ),
        )
        rolled_entry = replace(
            small_entry,
            eligibility_refs=tuple(
                sorted(
                    (current_state, next_state),
                    key=lambda item: item.effective.reference.content_hash,
                )
            ),
        )
        entries = tuple(
            rolled_entry if entry is small_entry else entry
            for entry in reference_context.universe.entries
        )
        rolled_universe = replace(reference_context.universe, entries=entries)
        rebound_snapshot, rebound_instruments, rebound_context = self._rebind_universe(
            snapshot,
            instruments,
            reference_context,
            rolled_universe,
        )

        validate_reference_context(
            rebound_snapshot,
            rebound_instruments,
            rebound_context,
        )

    def test_next_session_surveillance_change_blocks_the_pipeline(self) -> None:
        pipeline, snapshot, instruments, portfolio, reference_context = build_demo()
        small_entry = next(
            entry
            for entry in reference_context.universe.entries
            if entry.listing.tradingsymbol == "DEMO-SMALL"
        )
        state = small_entry.eligibility_refs[0]
        entry_session = date(2026, 7, 16)
        current_state = replace(
            state,
            effective=replace(
                state.effective,
                effective_to_exclusive=entry_session,
            ),
        )
        restricted_state = replace(
            state,
            surveillance=Surveillance.GSM,
            effective=replace(
                state.effective,
                reference=replace(
                    state.effective.reference,
                    content_hash="0" * 64,
                ),
                effective_from_session=entry_session,
                effective_to_exclusive=None,
            ),
        )
        changed_entry = replace(
            small_entry,
            eligibility_refs=tuple(
                sorted(
                    (current_state, restricted_state),
                    key=lambda item: item.effective.reference.content_hash,
                )
            ),
        )
        entries = tuple(
            changed_entry if entry is small_entry else entry
            for entry in reference_context.universe.entries
        )
        changed_universe = replace(reference_context.universe, entries=entries)
        rebound_snapshot, rebound_instruments, rebound_context = self._rebind_universe(
            snapshot,
            instruments,
            reference_context,
            changed_universe,
        )

        result = pipeline.run(
            rebound_snapshot,
            rebound_instruments,
            portfolio,
            rebound_context,
        )

        self.assertIs(result.status, RunStatus.FAILED)
        self.assertEqual(result.failure_stage, "reference_integrity")

    def test_pipeline_rejects_collection_only_reference_context(self) -> None:
        pipeline, snapshot, instruments, portfolio, reference_context = build_demo()
        collection_calendar = CalendarSnapshot.create(
            exchange=reference_context.calendar.exchange,
            segment=reference_context.calendar.segment,
            cutoff=reference_context.calendar.cutoff,
            coverage_start=reference_context.calendar.coverage_start,
            coverage_end=reference_context.calendar.coverage_end,
            days=reference_context.calendar.days,
            source_snapshot_ids=reference_context.calendar.source_snapshot_ids,
            readiness=ReferenceReadiness.COLLECTION_ONLY,
        )
        invalid_context = ReferenceContext(
            calendar=collection_calendar,
            universe=reference_context.universe,
        )

        result = pipeline.run(snapshot, instruments, portfolio, invalid_context)

        self.assertIs(result.status, RunStatus.FAILED)
        self.assertEqual(result.failure_stage, "reference_integrity")

    def test_pipeline_rejects_missing_or_unrelated_universe_member(self) -> None:
        pipeline, snapshot, instruments, portfolio, reference_context = build_demo()
        missing_actionable = [
            instrument for instrument in instruments if instrument.symbol != "DEMO-SMALL"
        ]

        missing_result = pipeline.run(
            snapshot,
            missing_actionable,
            portfolio,
            reference_context,
        )
        unrelated = [
            replace(instruments[0], instrument_id="unrelated-instrument"),
            *instruments[1:],
        ]
        unrelated_result = pipeline.run(
            snapshot,
            unrelated,
            portfolio,
            reference_context,
        )

        self.assertEqual(missing_result.failure_stage, "reference_integrity")
        self.assertEqual(unrelated_result.failure_stage, "reference_integrity")

    def test_decision_context_must_match_reference_artifact_ids(self) -> None:
        pipeline, snapshot, instruments, portfolio, reference_context = build_demo()
        mismatched = replace(snapshot, universe_snapshot_id="f" * 64)

        result = pipeline.run(
            mismatched,
            instruments,
            portfolio,
            reference_context,
        )

        self.assertIs(result.status, RunStatus.FAILED)
        self.assertEqual(result.failure_stage, "reference_integrity")

    def test_full_demo_produces_a_sized_buy(self) -> None:
        pipeline, snapshot, instruments, portfolio, reference_context = build_demo()

        result = pipeline.run(snapshot, instruments, portfolio, reference_context)

        self.assertEqual(snapshot.market_session, snapshot.decision_time.date())
        self.assertTrue(
            all(instrument.price_session == snapshot.market_session for instrument in instruments)
        )
        self.assertIs(result.decision.action, DecisionAction.BUY)
        self.assertEqual(result.decision.symbol, "DEMO-SMALL")
        self.assertGreater(result.decision.quantity, 0)
        self.assertGreater(result.decision.planned_max_loss, 0)
        self.assertGreater(result.decision.expected_r, 0)
        self.assertEqual(result.snapshot_id, snapshot.snapshot_id)
        self.assertEqual(result.pipeline_version, pipeline.version)
        self.assertIs(result.status, RunStatus.COMPLETE)
        self.assertEqual(
            result.decision.reference_readiness,
            ReferenceReadiness.SYNTHETIC_TEST.value,
        )
        self.assertFalse(result.decision.execution_eligible)
        result.verify_integrity()

    def test_gsm_instrument_is_excluded_before_candidate_build(self) -> None:
        pipeline, snapshot, instruments, portfolio, reference_context = build_demo()

        result = pipeline.run(snapshot, instruments, portfolio, reference_context)

        gsm_rejections = [
            rejection for rejection in result.rejections if rejection.symbol == "DEMO-GSM"
        ]
        self.assertEqual(len(gsm_rejections), 1)
        self.assertEqual(gsm_rejections[0].stage, "universe")
        self.assertIn("GSM_BLOCKED", gsm_rejections[0].reasons)
        self.assertNotIn(
            "DEMO-GSM",
            {ranked.candidate.instrument.symbol for ranked in result.ranked},
        )
        self.assertNotIn(
            "DEMO-GSM",
            {assessment.symbol for assessment in result.research},
        )

    def test_uncertain_research_cannot_become_a_trade(self) -> None:
        pipeline, snapshot, instruments, portfolio, reference_context = build_demo()
        large_cap = next(
            instrument for instrument in instruments if instrument.symbol == "DEMO-LARGE"
        )

        narrowed_entries = tuple(
            replace(
                entry,
                disposition=UniverseDisposition.EXCLUDED,
                reason_codes=("TEST_SCOPE_EXCLUSION",),
            )
            if entry.listing.tradingsymbol == "DEMO-SMALL"
            else entry
            for entry in reference_context.universe.entries
        )
        narrowed_universe = UniverseSnapshot.create(
            exchange=reference_context.universe.exchange,
            segment=reference_context.universe.segment,
            market_session=reference_context.universe.market_session,
            cutoff=reference_context.universe.cutoff,
            calendar_snapshot_id=reference_context.calendar.snapshot_id,
            universe_rules_version="synthetic-large-only-test/v1",
            selection_key=reference_context.universe.selection_key,
            scoped_source_row_ids=reference_context.universe.scoped_source_row_ids,
            security_master_snapshot_ids=("b" * 64,),
            eligibility_snapshot_ids=("c" * 64,),
            liquidity_snapshot_ids=("d" * 64,),
            readiness=reference_context.universe.readiness,
            entries=narrowed_entries,
        )
        narrowed_context = ReferenceContext(
            calendar=reference_context.calendar,
            universe=narrowed_universe,
        )
        snapshot = replace(snapshot, universe_snapshot_id=narrowed_universe.snapshot_id)
        large_cap = replace(
            large_cap,
            universe_snapshot_id=narrowed_universe.snapshot_id,
        )
        forecast = pipeline.forecast_provider.forecasts["DEMO-LARGE"]
        pipeline.forecast_provider.forecasts["DEMO-LARGE"] = replace(
            forecast,
            universe_snapshot_id=narrowed_universe.snapshot_id,
            data_snapshot_fingerprint=snapshot.content_fingerprint,
            instrument_fingerprint=large_cap.content_fingerprint,
        )
        signals, setup, evidence_ids = pipeline.signal_provider.values["DEMO-LARGE"]
        pipeline.signal_provider.values["DEMO-LARGE"] = (
            replace(
                signals,
                universe_snapshot_id=narrowed_universe.snapshot_id,
                data_snapshot_fingerprint=snapshot.content_fingerprint,
                instrument_fingerprint=large_cap.content_fingerprint,
            ),
            replace(
                setup,
                universe_snapshot_id=narrowed_universe.snapshot_id,
                data_snapshot_fingerprint=snapshot.content_fingerprint,
                instrument_fingerprint=large_cap.content_fingerprint,
            ),
            evidence_ids,
        )
        assessment = pipeline.research_provider.assessments["DEMO-LARGE"]
        pipeline.research_provider.assessments["DEMO-LARGE"] = replace(
            assessment,
            universe_snapshot_id=narrowed_universe.snapshot_id,
            data_snapshot_fingerprint=snapshot.content_fingerprint,
            instrument_fingerprint=large_cap.content_fingerprint,
        )

        result = pipeline.run(snapshot, [large_cap], portfolio, narrowed_context)

        self.assertIs(result.decision.action, DecisionAction.NO_TRADE)
        self.assertIsNone(result.decision.symbol)
        self.assertEqual(len(result.research), 1)
        self.assertIs(result.research[0].verdict, ResearchVerdict.UNCERTAIN)
        risk_rejections = [
            rejection
            for rejection in result.rejections
            if rejection.symbol == "DEMO-LARGE" and rejection.stage == "risk"
        ]
        self.assertEqual(len(risk_rejections), 1)
        self.assertIn("research verdict is UNCERTAIN", risk_rejections[0].reasons)

    def test_audit_record_is_immutable_and_cannot_be_overwritten(self) -> None:
        pipeline, snapshot, instruments, portfolio, reference_context = build_demo()
        result = pipeline.run(snapshot, instruments, portfolio, reference_context)
        writer = AuditWriter()

        with tempfile.TemporaryDirectory() as temp_dir:
            output_dir = Path(temp_dir)
            path = writer.write_pipeline_result(output_dir, result)
            original_bytes = path.read_bytes()
            envelope = json.loads(original_bytes)

            self.assertEqual(envelope["schema_version"], writer.schema_version)
            self.assertEqual(
                envelope["payload"]["result"]["decision"]["action"],
                DecisionAction.BUY.value,
            )
            self.assertEqual(
                envelope["payload"]["result"]["calendar_snapshot_id"],
                reference_context.calendar.snapshot_id,
            )
            self.assertEqual(
                envelope["payload"]["result"]["universe_snapshot_id"],
                reference_context.universe.snapshot_id,
            )
            self.assertEqual(
                envelope["payload"]["result"]["reference_readiness"],
                ReferenceReadiness.SYNTHETIC_TEST.value,
            )
            for name in (
                "trial_id",
                "model_bundle_id",
                "data_content_hash",
                "source_revision",
                "execution_policy_version",
                "cost_schedule_version",
            ):
                self.assertEqual(
                    envelope["payload"]["result"][name],
                    getattr(snapshot, name),
                )
            with self.assertRaisesRegex(AuditExistsError, "already exists"):
                writer.write(output_dir, result.run_id, {"result": "tampered"})

            self.assertEqual(path.read_bytes(), original_bytes)
            self.assertEqual(list(output_dir.iterdir()), [path])

    def test_audit_rejects_a_pipeline_result_mutated_after_finish(self) -> None:
        pipeline, snapshot, instruments, portfolio, reference_context = build_demo()
        result = pipeline.run(snapshot, instruments, portfolio, reference_context)
        object.__setattr__(
            result.ranked[0].candidate.setup,
            "target",
            Decimal("9999"),
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            output_dir = Path(temp_dir)
            with self.assertRaisesRegex(AuditIntegrityError, "embedded integrity"):
                AuditWriter().write(output_dir, result.run_id, {"result": result})
            self.assertEqual(list(output_dir.iterdir()), [])

    def test_audit_filename_must_match_embedded_pipeline_run_id(self) -> None:
        pipeline, snapshot, instruments, portfolio, reference_context = build_demo()
        result = pipeline.run(snapshot, instruments, portfolio, reference_context)

        with tempfile.TemporaryDirectory() as temp_dir:
            output_dir = Path(temp_dir)
            with self.assertRaisesRegex(AuditIntegrityError, "filename run_id"):
                AuditWriter().write(output_dir, "wrong-run-id", {"result": result})
            self.assertEqual(list(output_dir.iterdir()), [])

    def test_audit_rejects_untyped_pipeline_result_lookalikes(self) -> None:
        pipeline, snapshot, instruments, portfolio, reference_context = build_demo()
        result = pipeline.run(snapshot, instruments, portfolio, reference_context)

        with tempfile.TemporaryDirectory() as temp_dir:
            output_dir = Path(temp_dir)
            with self.assertRaisesRegex(AuditIntegrityError, "untyped pipeline"):
                AuditWriter().write(
                    output_dir,
                    result.run_id,
                    {"result": asdict(result)},
                )
            self.assertEqual(list(output_dir.iterdir()), [])

    def test_all_candidates_rejected_returns_no_trade(self) -> None:
        pipeline, snapshot, instruments, portfolio, reference_context = build_demo()
        locked_instruments = [
            replace(instrument, lower_circuit_locked=True) for instrument in instruments
        ]

        result = pipeline.run(
            snapshot,
            locked_instruments,
            portfolio,
            reference_context,
        )

        self.assertIs(result.decision.action, DecisionAction.NO_TRADE)
        self.assertIsNone(result.decision.symbol)
        self.assertEqual(result.decision.quantity, 0)
        self.assertEqual(result.decision.signal_id, f"no-trade-{result.run_id}")
        self.assertEqual(
            result.decision.reasons,
            ("no candidate passed every deterministic gate",),
        )
        self.assertEqual(result.ranked, ())
        self.assertEqual(result.research, ())
        eligibility_rejections = [
            rejection for rejection in result.rejections if rejection.stage == "eligibility"
        ]
        self.assertEqual(len(eligibility_rejections), len(locked_instruments))
        self.assertTrue(
            all(
                "instrument is locked at the lower circuit" in rejection.reasons
                for rejection in eligibility_rejections
            )
        )
        self.assertIs(result.status, RunStatus.COMPLETE)

    def test_adapter_exception_is_failed_run_not_ordinary_no_trade(self) -> None:
        pipeline, snapshot, instruments, portfolio, reference_context = build_demo()

        with patch.object(
            pipeline.forecast_provider,
            "forecast",
            side_effect=RuntimeError("api_key=must-not-appear"),
        ):
            result = pipeline.run(snapshot, instruments, portfolio, reference_context)

        self.assertIs(result.status, RunStatus.FAILED)
        self.assertIs(result.decision.action, DecisionAction.NO_TRADE)
        self.assertEqual(result.failure_stage, "candidate_build")
        self.assertEqual(result.failure_type, "RuntimeError")
        self.assertNotIn("must-not-appear", repr(result))

    def test_lookahead_failure_returns_auditable_failed_result(self) -> None:
        pipeline, snapshot, instruments, portfolio, reference_context = build_demo()
        future_evidence = replace(
            snapshot.evidence[0],
            published_at=snapshot.decision_time + timedelta(minutes=1),
            available_at=snapshot.decision_time + timedelta(minutes=2),
        )
        invalid_snapshot = replace(
            snapshot,
            evidence=(future_evidence, *snapshot.evidence[1:]),
        )

        result = pipeline.run(
            invalid_snapshot,
            instruments,
            portfolio,
            reference_context,
        )

        self.assertIs(result.status, RunStatus.FAILED)
        self.assertIs(result.decision.action, DecisionAction.NO_TRADE)
        self.assertEqual(result.failure_stage, "snapshot_integrity")
        with tempfile.TemporaryDirectory() as temp_dir:
            path = AuditWriter().write(Path(temp_dir), result.run_id, {"result": result})
            self.assertTrue(path.exists())


if __name__ == "__main__":
    unittest.main()
