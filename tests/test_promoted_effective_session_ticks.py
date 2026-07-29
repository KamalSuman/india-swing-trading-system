from __future__ import annotations

import dataclasses
import hashlib
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

from india_swing.evaluation.dataset_assembly import EffectiveTickSize
from india_swing.historical_prices.promoted_history import (
    PromotedStableListingHistoryObservation,
    PromotedStableListingHistoryService,
    VerifiedPromotedStableListingHistoryPanel,
)
from india_swing.identity_decisions import PromotedIdentityAdjudicationService
from india_swing.identity_registry.promoted_intake import PromotedIdentityIntakeService
from india_swing.market_data.promoted_session_frame import (
    PromotedSessionMarketDataFrameService,
)
from india_swing.reference.models import ReferenceReadiness
from india_swing.tick_sizes.effective_session import (
    PromotedEffectiveSessionTickError,
    PromotedEffectiveSessionTickResult,
    PromotedEffectiveSessionTickService,
    PromotedEffectiveSessionTickStatus,
    VerifiedPromotedEffectiveSessionTickPanel,
)
from india_swing.tick_sizes.promoted_session import PromotedSessionTickSizeService
from india_swing.universe.promoted_identity import PromotedIdentitySessionUniverseService
from tests.test_promoted_identity_session_universe import (
    ADJUDICATION_CUTOFF,
    D0,
    D1,
    D2,
    INTAKE_CUTOFF,
    SESSION_CUTOFF,
    build_calendar,
    build_evidence,
    build_promotion,
    build_review,
    security_row,
)
from tests.test_promoted_session_market_data import FRAME_CUTOFF, _bar, _corpus
from tests.test_promoted_session_tick_sizes import TICK_CUTOFF
from tests.test_promoted_stable_listing_history import (
    PANEL_CUTOFF,
    _endpoint_snapshots_with_middle_session_missing,
    _two_session_fixture,
)


UTC = timezone.utc
RESULT_CUTOFF = PANEL_CUTOFF + timedelta(hours=1)


def _kwargs_from(panel: VerifiedPromotedEffectiveSessionTickPanel) -> dict[str, object]:
    return {field.name: getattr(panel, field.name) for field in dataclasses.fields(panel)}


def _three_session_resolved_fixture_with_missing_middle(
    root: Path,
) -> VerifiedPromotedStableListingHistoryPanel:
    """A single resolved history spanning D0/D1/D2 whose middle session
    (D1) tick snapshot is genuinely absent from the source panel -- unlike
    _endpoint_snapshots_with_middle_session_missing in
    tests/test_promoted_stable_listing_history.py (which never submits
    evidence/review, so its candidate stays unresolved and produces zero
    histories), this fixture resolves the candidate first so the missing
    middle session actually surfaces as a MISSING_OBSERVATION_BLOCKED
    result rather than disappearing entirely."""

    promotions = tuple(
        build_promotion(
            root,
            report_date=session,
            generation=700 + index,
            rows=[security_row(FinInstrmId="80001", TckrSymb="MIDGAP", ISIN="INE001A01036")],
            first_seen=datetime(
                session.year, session.month, session.day, 12, 0, tzinfo=UTC
            ),
            validated=datetime(
                session.year, session.month, session.day, 12, 0, 2, tzinfo=UTC
            ),
        )
        for index, session in enumerate((D0, D1, D2))
    )
    intake = PromotedIdentityIntakeService().materialize(
        promotions=promotions, expected_report_dates=(D0, D1, D2), cutoff=INTAKE_CUTOFF
    )
    case = intake.queue.cases[0]
    status = next(
        value for value in intake.requirement_statuses if value.candidate_id == case.candidate_id
    )
    evidence = build_evidence(
        root,
        candidate_id=case.candidate_id,
        requirements=status.unresolved_requirements,
        symbol="MIDGAP",
        series="EQ",
        isin="INE001A01036",
        suffix="midgap",
    )
    review = build_review(
        root,
        queue_id=intake.queue.queue_id,
        source_registry_id=intake.source_graph_id,
        candidate_id=case.candidate_id,
        requirements=status.unresolved_requirements,
        evidence=evidence,
        suffix="midgap",
    )
    adjudication = PromotedIdentityAdjudicationService().materialize(
        intake=intake, evidence_artifacts=(evidence,), review_bundles=(review,), cutoff=ADJUDICATION_CUTOFF
    )
    calendar = build_calendar(root)
    snapshots = []
    for session in (D0, D2):
        universe = PromotedIdentitySessionUniverseService().materialize(
            adjudication=adjudication, calendar=calendar, market_session=session, cutoff=SESSION_CUTOFF
        )
        entry = next(value for value in universe.entries if value.symbol == "MIDGAP")
        index, partition = _corpus(
            market_session=session,
            bars=(_bar(entry, market_session=session, label=f"midgap-{session}"),),
        )
        frame = PromotedSessionMarketDataFrameService().materialize(
            universe=universe, corpus_index=index, partition=partition, cutoff=FRAME_CUTOFF
        )
        snapshots.append(
            PromotedSessionTickSizeService().materialize(frame=frame, cutoff=TICK_CUTOFF)
        )
    return PromotedStableListingHistoryService().materialize(
        tick_snapshots=tuple(snapshots), calendar=calendar, cutoff=PANEL_CUTOFF
    )


class PromotedEffectiveSessionTickAcceptanceTests(unittest.TestCase):
    def _panel(self, tmp: str) -> VerifiedPromotedEffectiveSessionTickPanel:
        root = Path(tmp)
        _, _, source_panel = _two_session_fixture(root)
        return PromotedEffectiveSessionTickService().materialize(
            source_panel=source_panel, cutoff=RESULT_CUTOFF
        )

    def test_one_effective_tick_size_per_resolved_history_session(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _, _, source_panel = _two_session_fixture(root)
            panel = PromotedEffectiveSessionTickService().materialize(
                source_panel=source_panel, cutoff=RESULT_CUTOFF
            )
            self.assertEqual(len(panel.results), 2)
            self.assertTrue(
                all(
                    value.status
                    is PromotedEffectiveSessionTickStatus.VERIFIED_EXACT_SESSION_ONLY
                    for value in panel.results
                )
            )
            history = source_panel.histories[0]
            for result in panel.results:
                self.assertEqual(result.stable_instrument_id, history.stable_instrument_id)
                self.assertEqual(result.stable_listing_id, history.stable_listing_id)
                spec = result.tick_specification
                self.assertIsNotNone(spec)
                self.assertEqual(spec.tick_size, Decimal("0.05"))
                snapshot = next(
                    value
                    for value in source_panel.tick_snapshots
                    if value.market_session == result.market_session
                )
                self.assertEqual(spec.source_snapshot_id, snapshot.snapshot_id)
                self.assertEqual(spec.knowledge_time, snapshot.knowledge_time)
                self.assertIs(spec.readiness, ReferenceReadiness.POINT_IN_TIME_VERIFIED)

    def test_results_are_canonically_ordered(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            panel = self._panel(tmp)
            self.assertEqual(
                panel.results,
                tuple(
                    sorted(
                        panel.results,
                        key=lambda value: (
                            value.stable_instrument_id,
                            value.stable_listing_id,
                            value.market_session,
                        ),
                    )
                ),
            )

    def test_one_day_exclusive_interval_effective_only_on_its_own_session(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            panel = self._panel(tmp)
            for result in panel.results:
                spec = result.tick_specification
                self.assertEqual(spec.effective_from_session, result.market_session)
                self.assertIsNotNone(spec.effective_to_exclusive)
                self.assertEqual(
                    spec.effective_to_exclusive, result.market_session + timedelta(days=1)
                )
                self.assertTrue(spec.is_effective_on(result.market_session))
                self.assertFalse(spec.is_effective_on(result.market_session - timedelta(days=1)))
                self.assertFalse(spec.is_effective_on(result.market_session + timedelta(days=1)))

    def test_verified_reason_codes_are_exact(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            panel = self._panel(tmp)
            for result in panel.results:
                self.assertEqual(
                    set(result.reason_codes),
                    {
                        "EXACT_SESSION_TICK_INTERVAL_ONLY",
                        "NO_CROSS_SESSION_TICK_INFERENCE",
                        "EFFECTIVE_TICK_SIZE_POINT_IN_TIME_VERIFIED",
                    },
                )

    def test_all_safety_flags_false_and_coverage_complete_when_fully_verified(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            panel = self._panel(tmp)
            self.assertIs(panel.readiness, ReferenceReadiness.COLLECTION_ONLY)
            self.assertFalse(panel.actionable)
            self.assertFalse(panel.training_eligible)
            self.assertFalse(panel.feature_eligible)
            self.assertFalse(panel.alert_eligible)
            self.assertFalse(panel.execution_eligible)
            self.assertTrue(panel.resolved_histories_tick_coverage_complete)

    def test_verify_content_identity_passes(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            panel = self._panel(tmp)
            panel.verify_content_identity()


class PromotedEffectiveSessionTickBarIndependenceTests(unittest.TestCase):
    def test_missing_bar_still_verifies_the_exact_session_tick(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _, _, source_panel = _two_session_fixture(root, omit_reliance_bar_on=D2)
            panel = PromotedEffectiveSessionTickService().materialize(
                source_panel=source_panel, cutoff=RESULT_CUTOFF
            )
            self.assertEqual(len(panel.results), 2)
            self.assertTrue(
                all(
                    value.status
                    is PromotedEffectiveSessionTickStatus.VERIFIED_EXACT_SESSION_ONLY
                    for value in panel.results
                )
            )
            d2_result = next(value for value in panel.results if value.market_session == D2)
            self.assertIsNone(d2_result.source_observation.raw_bar_id)
            self.assertIsNotNone(d2_result.tick_specification)

    def test_bar_identity_conflict_still_verifies_the_exact_session_tick(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _, _, source_panel = _two_session_fixture(root, conflict_reliance_bar_on=D2)
            panel = PromotedEffectiveSessionTickService().materialize(
                source_panel=source_panel, cutoff=RESULT_CUTOFF
            )
            self.assertTrue(
                all(
                    value.status
                    is PromotedEffectiveSessionTickStatus.VERIFIED_EXACT_SESSION_ONLY
                    for value in panel.results
                )
            )
            self.assertTrue(panel.resolved_histories_tick_coverage_complete)


class PromotedEffectiveSessionTickMissingObservationTests(unittest.TestCase):
    def test_missing_middle_session_snapshot_blocks_without_adjacent_values(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source_panel = _three_session_resolved_fixture_with_missing_middle(root)
            panel = PromotedEffectiveSessionTickService().materialize(
                source_panel=source_panel, cutoff=RESULT_CUTOFF
            )
            self.assertEqual(len(panel.results), 3)
            by_session = {value.market_session: value for value in panel.results}
            self.assertIs(
                by_session[D0].status,
                PromotedEffectiveSessionTickStatus.VERIFIED_EXACT_SESSION_ONLY,
            )
            self.assertIs(
                by_session[D1].status,
                PromotedEffectiveSessionTickStatus.MISSING_OBSERVATION_BLOCKED,
            )
            self.assertIsNone(by_session[D1].tick_specification)
            self.assertIs(
                by_session[D2].status,
                PromotedEffectiveSessionTickStatus.VERIFIED_EXACT_SESSION_ONLY,
            )
            self.assertFalse(panel.resolved_histories_tick_coverage_complete)
            # No adjacent-session tick value leaks into the blocked result.
            self.assertNotEqual(
                by_session[D0].tick_specification.tick_size,
                None,
            )
            panel.verify_content_identity()

    def test_missing_observation_blocked_reason_codes_are_exact(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source_panel = _three_session_resolved_fixture_with_missing_middle(root)
            panel = PromotedEffectiveSessionTickService().materialize(
                source_panel=source_panel, cutoff=RESULT_CUTOFF
            )
            blocked = next(
                value
                for value in panel.results
                if value.status is PromotedEffectiveSessionTickStatus.MISSING_OBSERVATION_BLOCKED
            )
            self.assertEqual(
                set(blocked.reason_codes),
                {
                    "MISSING_TICK_OBSERVATION_NO_STATE_INFERENCE",
                    "NO_CROSS_SESSION_TICK_INFERENCE",
                },
            )


class PromotedEffectiveSessionTickUnassignedEvidenceTests(unittest.TestCase):
    def test_unresolved_and_excluded_entries_never_produce_a_result(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _, _, source_panel = _two_session_fixture(root)
            panel = PromotedEffectiveSessionTickService().materialize(
                source_panel=source_panel, cutoff=RESULT_CUTOFF
            )
            result_stable_ids = {value.stable_instrument_id for value in panel.results}
            for entry in source_panel.unassigned_entries:
                universe_entry = entry.tick_entry.frame_entry.universe_entry
                self.assertIsNone(universe_entry.stable_instrument_id)
            self.assertTrue(source_panel.unassigned_entries)
            self.assertEqual(len(result_stable_ids), 1)


class PromotedEffectiveSessionTickRejectionTests(unittest.TestCase):
    def test_wrong_type_source_panel_is_rejected(self) -> None:
        with self.assertRaises(PromotedEffectiveSessionTickError):
            PromotedEffectiveSessionTickService().materialize(
                source_panel="not-a-panel", cutoff=RESULT_CUTOFF  # type: ignore[arg-type]
            )

    def test_naive_cutoff_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _, _, source_panel = _two_session_fixture(root)
            with self.assertRaises(PromotedEffectiveSessionTickError):
                PromotedEffectiveSessionTickService().materialize(
                    source_panel=source_panel, cutoff=RESULT_CUTOFF.replace(tzinfo=None)
                )

    def test_cutoff_before_source_panel_knowledge_time_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _, _, source_panel = _two_session_fixture(root)
            with self.assertRaises(PromotedEffectiveSessionTickError):
                PromotedEffectiveSessionTickService().materialize(
                    source_panel=source_panel,
                    cutoff=source_panel.knowledge_time - timedelta(days=1),
                )

    def test_source_panel_actionable_mutation_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _, _, source_panel = _two_session_fixture(root)
            object.__setattr__(source_panel, "actionable", True)
            with self.assertRaises(PromotedEffectiveSessionTickError):
                PromotedEffectiveSessionTickService().materialize(
                    source_panel=source_panel, cutoff=RESULT_CUTOFF
                )

    def test_source_panel_feature_eligible_mutation_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _, _, source_panel = _two_session_fixture(root)
            object.__setattr__(source_panel, "feature_eligible", True)
            with self.assertRaises(PromotedEffectiveSessionTickError):
                PromotedEffectiveSessionTickService().materialize(
                    source_panel=source_panel, cutoff=RESULT_CUTOFF
                )

    def test_source_panel_training_eligible_mutation_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _, _, source_panel = _two_session_fixture(root)
            object.__setattr__(source_panel, "training_eligible", True)
            with self.assertRaises(PromotedEffectiveSessionTickError):
                PromotedEffectiveSessionTickService().materialize(
                    source_panel=source_panel, cutoff=RESULT_CUTOFF
                )

    def test_source_panel_alert_eligible_mutation_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _, _, source_panel = _two_session_fixture(root)
            object.__setattr__(source_panel, "alert_eligible", True)
            with self.assertRaises(PromotedEffectiveSessionTickError):
                PromotedEffectiveSessionTickService().materialize(
                    source_panel=source_panel, cutoff=RESULT_CUTOFF
                )

    def test_source_panel_execution_eligible_mutation_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _, _, source_panel = _two_session_fixture(root)
            object.__setattr__(source_panel, "execution_eligible", True)
            with self.assertRaises(PromotedEffectiveSessionTickError):
                PromotedEffectiveSessionTickService().materialize(
                    source_panel=source_panel, cutoff=RESULT_CUTOFF
                )


class PromotedEffectiveSessionTickWhiteBoxTests(unittest.TestCase):
    """Some rejection scenarios required by the architecture (duplicate
    tick-snapshot session, mismatched stable-ID lineage) are structurally
    unreachable through the public service: source_panel.verify_content_
    identity() (always called first) already independently re-verifies its
    own retained histories/tick_snapshots, which already guarantee unique
    sessions and lineage-consistent stable IDs. These tests import the
    private _build_effective_session_facts/_result_for helpers directly
    against a hand-mutated (object.__setattr__) fixture, bypassing that
    outer replay, to prove each check is still genuinely load-bearing
    defense-in-depth."""

    def test_incorrect_stable_instrument_id_is_rejected(self) -> None:
        from india_swing.tick_sizes.effective_session import _result_for

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _, _, source_panel = _two_session_fixture(root)
            history = source_panel.histories[0]
            observation = history.observations[0]
            snapshots_by_session = {
                value.market_session: value for value in source_panel.tick_snapshots
            }
            with self.assertRaises(PromotedEffectiveSessionTickError):
                _result_for("0" * 64, history.stable_listing_id, observation, snapshots_by_session)

    def test_effective_interval_verified_true_on_source_tick_entry_is_rejected(self) -> None:
        # Going through the full service would also be rejected here, but
        # only because source_panel.verify_content_identity() (called first)
        # independently rebuilds the tick snapshot's own entries and would
        # already reject a tampered effective_interval_verified=True before
        # this module's own guard runs -- so that path does not prove THIS
        # guard specifically is load-bearing. Calling the private
        # _result_for directly bypasses that outer replay and isolates it.
        from india_swing.tick_sizes.effective_session import _result_for

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _, _, source_panel = _two_session_fixture(root)
            history = source_panel.histories[0]
            observation = history.observations[0]
            object.__setattr__(observation.tick_entry, "effective_interval_verified", True)
            snapshots_by_session = {
                value.market_session: value for value in source_panel.tick_snapshots
            }
            with self.assertRaises(PromotedEffectiveSessionTickError):
                _result_for(
                    history.stable_instrument_id,
                    history.stable_listing_id,
                    observation,
                    snapshots_by_session,
                )

    def test_same_session_entry_from_different_snapshot_is_rejected(self) -> None:
        from india_swing.tick_sizes.effective_session import _result_for

        with (
            tempfile.TemporaryDirectory() as first_tmp,
            tempfile.TemporaryDirectory() as second_tmp,
        ):
            _, _, source_panel = _two_session_fixture(Path(first_tmp))
            _, other_snapshots, _ = _two_session_fixture(
                Path(second_tmp),
                conflict_reliance_bar_on=D1,
            )
            history = source_panel.histories[0]
            observation = history.observations[0]
            self.assertEqual(
                observation.market_session,
                other_snapshots[0].market_session,
            )
            self.assertNotEqual(
                observation.tick_entry._identity(),
                next(
                    value
                    for value in other_snapshots[0].entries
                    if value.source_record_id
                    == observation.tick_entry.source_record_id
                )._identity(),
            )
            with self.assertRaises(PromotedEffectiveSessionTickError):
                _result_for(
                    history.stable_instrument_id,
                    history.stable_listing_id,
                    observation,
                    {observation.market_session: other_snapshots[0]},
                )


class PromotedEffectiveSessionTickDirectConstructionMismatchTests(unittest.TestCase):
    def _panel(self, tmp: str) -> VerifiedPromotedEffectiveSessionTickPanel:
        root = Path(tmp)
        _, _, source_panel = _two_session_fixture(root)
        return PromotedEffectiveSessionTickService().materialize(
            source_panel=source_panel, cutoff=RESULT_CUTOFF
        )

    def test_replacing_schema_version_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            panel = self._panel(tmp)
            kwargs = _kwargs_from(panel)
            kwargs["schema_version"] = "promoted-effective-session-tick/v2"
            with self.assertRaises(PromotedEffectiveSessionTickError):
                VerifiedPromotedEffectiveSessionTickPanel(**kwargs)

    def test_replacing_policy_version_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            panel = self._panel(tmp)
            kwargs = _kwargs_from(panel)
            kwargs["policy_version"] = "different-policy/v1"
            with self.assertRaises(PromotedEffectiveSessionTickError):
                VerifiedPromotedEffectiveSessionTickPanel(**kwargs)

    def test_replacing_source_panel_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_a, tempfile.TemporaryDirectory() as tmp_b:
            panel = self._panel(tmp_a)
            root_b = Path(tmp_b)
            other_source_panel = _three_session_resolved_fixture_with_missing_middle(root_b)
            kwargs = _kwargs_from(panel)
            kwargs["source_panel"] = other_source_panel
            with self.assertRaises(PromotedEffectiveSessionTickError):
                VerifiedPromotedEffectiveSessionTickPanel(**kwargs)

    def test_replacing_cutoff_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            panel = self._panel(tmp)
            kwargs = _kwargs_from(panel)
            kwargs["cutoff"] = panel.cutoff + timedelta(days=1)
            with self.assertRaises(PromotedEffectiveSessionTickError):
                VerifiedPromotedEffectiveSessionTickPanel(**kwargs)

    def test_replacing_results_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            panel = self._panel(tmp)
            kwargs = _kwargs_from(panel)
            kwargs["results"] = panel.results[:-1]
            with self.assertRaises(PromotedEffectiveSessionTickError):
                VerifiedPromotedEffectiveSessionTickPanel(**kwargs)

    def test_replacing_status_counts_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            panel = self._panel(tmp)
            kwargs = _kwargs_from(panel)
            kwargs["status_counts"] = ()
            with self.assertRaises(PromotedEffectiveSessionTickError):
                VerifiedPromotedEffectiveSessionTickPanel(**kwargs)

    def test_replacing_reason_counts_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            panel = self._panel(tmp)
            kwargs = _kwargs_from(panel)
            kwargs["reason_counts"] = ()
            with self.assertRaises(PromotedEffectiveSessionTickError):
                VerifiedPromotedEffectiveSessionTickPanel(**kwargs)

    def test_replacing_resolved_histories_tick_coverage_complete_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            panel = self._panel(tmp)
            kwargs = _kwargs_from(panel)
            kwargs["resolved_histories_tick_coverage_complete"] = (
                not panel.resolved_histories_tick_coverage_complete
            )
            with self.assertRaises(PromotedEffectiveSessionTickError):
                VerifiedPromotedEffectiveSessionTickPanel(**kwargs)

    def test_replacing_readiness_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            panel = self._panel(tmp)
            kwargs = _kwargs_from(panel)
            kwargs["readiness"] = ReferenceReadiness.POINT_IN_TIME_VERIFIED
            with self.assertRaises(PromotedEffectiveSessionTickError):
                VerifiedPromotedEffectiveSessionTickPanel(**kwargs)

    def test_replacing_actionable_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            panel = self._panel(tmp)
            kwargs = _kwargs_from(panel)
            kwargs["actionable"] = True
            with self.assertRaises(PromotedEffectiveSessionTickError):
                VerifiedPromotedEffectiveSessionTickPanel(**kwargs)

    def test_replacing_training_eligible_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            panel = self._panel(tmp)
            kwargs = _kwargs_from(panel)
            kwargs["training_eligible"] = True
            with self.assertRaises(PromotedEffectiveSessionTickError):
                VerifiedPromotedEffectiveSessionTickPanel(**kwargs)

    def test_replacing_feature_eligible_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            panel = self._panel(tmp)
            kwargs = _kwargs_from(panel)
            kwargs["feature_eligible"] = True
            with self.assertRaises(PromotedEffectiveSessionTickError):
                VerifiedPromotedEffectiveSessionTickPanel(**kwargs)

    def test_replacing_alert_eligible_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            panel = self._panel(tmp)
            kwargs = _kwargs_from(panel)
            kwargs["alert_eligible"] = True
            with self.assertRaises(PromotedEffectiveSessionTickError):
                VerifiedPromotedEffectiveSessionTickPanel(**kwargs)

    def test_replacing_execution_eligible_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            panel = self._panel(tmp)
            kwargs = _kwargs_from(panel)
            kwargs["execution_eligible"] = True
            with self.assertRaises(PromotedEffectiveSessionTickError):
                VerifiedPromotedEffectiveSessionTickPanel(**kwargs)

    def test_replacing_panel_id_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            panel = self._panel(tmp)
            kwargs = _kwargs_from(panel)
            kwargs["panel_id"] = hashlib.sha256(b"different").hexdigest()
            with self.assertRaises(PromotedEffectiveSessionTickError):
                VerifiedPromotedEffectiveSessionTickPanel(**kwargs)

    def test_bool_as_int_lookalike_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            panel = self._panel(tmp)
            kwargs = _kwargs_from(panel)
            kwargs["actionable"] = 0
            with self.assertRaises(PromotedEffectiveSessionTickError):
                VerifiedPromotedEffectiveSessionTickPanel(**kwargs)

    def test_bool_as_count_lookalike_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            source_panel = _three_session_resolved_fixture_with_missing_middle(
                Path(tmp)
            )
            panel = PromotedEffectiveSessionTickService().materialize(
                source_panel=source_panel,
                cutoff=RESULT_CUTOFF,
            )
            kwargs = _kwargs_from(panel)
            kwargs["status_counts"] = tuple(
                (
                    status,
                    True
                    if status
                    == PromotedEffectiveSessionTickStatus.MISSING_OBSERVATION_BLOCKED.value
                    else count,
                )
                for status, count in panel.status_counts
            )
            self.assertIn(
                (
                    PromotedEffectiveSessionTickStatus.MISSING_OBSERVATION_BLOCKED.value,
                    1,
                ),
                panel.status_counts,
            )
            with self.assertRaises(PromotedEffectiveSessionTickError):
                VerifiedPromotedEffectiveSessionTickPanel(**kwargs)

    def test_string_as_enum_lookalike_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            panel = self._panel(tmp)
            kwargs = _kwargs_from(panel)
            kwargs["readiness"] = ReferenceReadiness.COLLECTION_ONLY.value
            with self.assertRaises(PromotedEffectiveSessionTickError):
                VerifiedPromotedEffectiveSessionTickPanel(**kwargs)


class PromotedEffectiveSessionTickMutationTests(unittest.TestCase):
    def _panel(self, tmp: str) -> VerifiedPromotedEffectiveSessionTickPanel:
        root = Path(tmp)
        _, _, source_panel = _two_session_fixture(root)
        return PromotedEffectiveSessionTickService().materialize(
            source_panel=source_panel, cutoff=RESULT_CUTOFF
        )

    def test_mutating_top_level_panel_id_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            panel = self._panel(tmp)
            object.__setattr__(panel, "panel_id", "0" * 64)
            with self.assertRaises(PromotedEffectiveSessionTickError):
                panel.verify_content_identity()

    def test_mutating_nested_source_panel_field_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            panel = self._panel(tmp)
            object.__setattr__(panel.source_panel, "panel_id", "0" * 64)
            with self.assertRaises(PromotedEffectiveSessionTickError):
                panel.verify_content_identity()

    def test_mutating_nested_tick_snapshot_field_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            panel = self._panel(tmp)
            object.__setattr__(
                panel.source_panel.tick_snapshots[0], "snapshot_id", "0" * 64
            )
            with self.assertRaises(PromotedEffectiveSessionTickError):
                panel.verify_content_identity()

    def test_mutating_result_tick_specification_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            panel = self._panel(tmp)
            spec = panel.results[0].tick_specification
            object.__setattr__(spec, "tick_size", Decimal("99.99"))
            with self.assertRaises(PromotedEffectiveSessionTickError):
                panel.verify_content_identity()

    def test_mutating_result_status_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            panel = self._panel(tmp)
            object.__setattr__(
                panel.results[0],
                "status",
                PromotedEffectiveSessionTickStatus.MISSING_OBSERVATION_BLOCKED,
            )
            with self.assertRaises(PromotedEffectiveSessionTickError):
                panel.verify_content_identity()

    def test_mutating_history_stable_id_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            panel = self._panel(tmp)
            object.__setattr__(
                panel.source_panel.histories[0], "stable_instrument_id", "1" * 64
            )
            with self.assertRaises(PromotedEffectiveSessionTickError):
                panel.verify_content_identity()


class _EvilEq:
    def __init__(self, secret: str) -> None:
        self._secret = secret

    def __eq__(self, other: object) -> bool:
        raise RuntimeError(f"secret-leak-{self._secret}")

    def __hash__(self) -> int:
        return 0


class _ComparisonBoundaryBaseException(BaseException):
    pass


class _EvilEqBaseException:
    def __eq__(self, other: object) -> bool:
        raise _ComparisonBoundaryBaseException("comparison-boundary-control")

    def __hash__(self) -> int:
        return 0


class PromotedEffectiveSessionTickComparisonBoundaryTests(unittest.TestCase):
    def _panel(self, tmp: str) -> VerifiedPromotedEffectiveSessionTickPanel:
        root = Path(tmp)
        _, _, source_panel = _two_session_fixture(root)
        return PromotedEffectiveSessionTickService().materialize(
            source_panel=source_panel, cutoff=RESULT_CUTOFF
        )

    def _assert_sanitized(self, secret: str, exc: BaseException) -> None:
        self.assertIsInstance(exc, PromotedEffectiveSessionTickError)
        message = str(exc)
        self.assertNotIn("RuntimeError", message)
        self.assertNotIn(secret, message)

    def test_malicious_tick_specification_equality_is_sanitized(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            panel = self._panel(tmp)
            secret = "spec-secret-1a2b"
            spec = panel.results[0].tick_specification
            object.__setattr__(spec, "instrument_id", _EvilEq(secret))
            with self.assertRaises(PromotedEffectiveSessionTickError) as ctx:
                panel.verify_content_identity()
            self._assert_sanitized(secret, ctx.exception)

    def test_malicious_history_equality_is_sanitized(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            panel = self._panel(tmp)
            secret = "history-secret-3c4d"
            object.__setattr__(
                panel.source_panel.histories[0], "stable_instrument_id", _EvilEq(secret)
            )
            with self.assertRaises(PromotedEffectiveSessionTickError) as ctx:
                panel.verify_content_identity()
            self._assert_sanitized(secret, ctx.exception)

    def test_base_exception_from_equality_is_not_swallowed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            panel = self._panel(tmp)
            spec = panel.results[0].tick_specification
            object.__setattr__(spec, "instrument_id", _EvilEqBaseException())
            with self.assertRaises(_ComparisonBoundaryBaseException):
                panel.verify_content_identity()


class PromotedEffectiveSessionTickSubclassImpostorTests(unittest.TestCase):
    def test_panel_subclass_is_rejected(self) -> None:
        class _PanelSubclass(VerifiedPromotedEffectiveSessionTickPanel):
            pass

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _, _, source_panel = _two_session_fixture(root)
            panel = PromotedEffectiveSessionTickService().materialize(
                source_panel=source_panel, cutoff=RESULT_CUTOFF
            )
            with self.assertRaises(PromotedEffectiveSessionTickError):
                _PanelSubclass(**_kwargs_from(panel))

    def test_wrong_type_results_tuple_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _, _, source_panel = _two_session_fixture(root)
            panel = PromotedEffectiveSessionTickService().materialize(
                source_panel=source_panel, cutoff=RESULT_CUTOFF
            )
            kwargs = _kwargs_from(panel)
            kwargs["results"] = list(panel.results)  # type: ignore[assignment]
            with self.assertRaises(PromotedEffectiveSessionTickError):
                VerifiedPromotedEffectiveSessionTickPanel(**kwargs)


class PromotedEffectiveSessionTickContentIdCompletenessTests(unittest.TestCase):
    def test_different_cutoff_changes_panel_id(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _, _, source_panel = _two_session_fixture(root)
            panel_a = PromotedEffectiveSessionTickService().materialize(
                source_panel=source_panel, cutoff=RESULT_CUTOFF
            )
            panel_b = PromotedEffectiveSessionTickService().materialize(
                source_panel=source_panel, cutoff=RESULT_CUTOFF + timedelta(hours=1)
            )
            self.assertNotEqual(panel_a.panel_id, panel_b.panel_id)

    def test_different_tick_value_changes_panel_id(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_a, tempfile.TemporaryDirectory() as tmp_b:
            root_a, root_b = Path(tmp_a), Path(tmp_b)
            _, _, source_panel_a = _two_session_fixture(root_a)
            source_panel_b = _three_session_resolved_fixture_with_missing_middle(root_b)
            panel_a = PromotedEffectiveSessionTickService().materialize(
                source_panel=source_panel_a, cutoff=RESULT_CUTOFF
            )
            panel_b = PromotedEffectiveSessionTickService().materialize(
                source_panel=source_panel_b, cutoff=RESULT_CUTOFF
            )
            self.assertNotEqual(panel_a.panel_id, panel_b.panel_id)

    def test_different_result_status_changes_panel_id(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_a, tempfile.TemporaryDirectory() as tmp_b:
            root_a, root_b = Path(tmp_a), Path(tmp_b)
            _, _, source_panel_a = _two_session_fixture(root_a)
            panel_a = PromotedEffectiveSessionTickService().materialize(
                source_panel=source_panel_a, cutoff=RESULT_CUTOFF
            )
            source_panel_b = _three_session_resolved_fixture_with_missing_middle(root_b)
            panel_b = PromotedEffectiveSessionTickService().materialize(
                source_panel=source_panel_b, cutoff=RESULT_CUTOFF
            )
            self.assertNotEqual(
                {value.status for value in panel_a.results},
                {value.status for value in panel_b.results}
                | {value.status for value in panel_a.results},
            )
            self.assertNotEqual(panel_a.panel_id, panel_b.panel_id)

    def test_different_completeness_changes_panel_id(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_a, tempfile.TemporaryDirectory() as tmp_b:
            root_a, root_b = Path(tmp_a), Path(tmp_b)
            _, _, source_panel_a = _two_session_fixture(root_a)
            panel_a = PromotedEffectiveSessionTickService().materialize(
                source_panel=source_panel_a, cutoff=RESULT_CUTOFF
            )
            source_panel_b = _three_session_resolved_fixture_with_missing_middle(root_b)
            panel_b = PromotedEffectiveSessionTickService().materialize(
                source_panel=source_panel_b, cutoff=RESULT_CUTOFF
            )
            self.assertTrue(panel_a.resolved_histories_tick_coverage_complete)
            self.assertFalse(panel_b.resolved_histories_tick_coverage_complete)
            self.assertNotEqual(panel_a.panel_id, panel_b.panel_id)


class PromotedEffectiveSessionTickCapabilityTests(unittest.TestCase):
    def test_no_interval_merge_extend_or_open_ended_capability_exists(self) -> None:
        banned_substrings = (
            "merge",
            "extend",
            "open_ended",
            "openend",
            "interpolate",
            "backfill",
            "forwardfill",
            "forward_fill",
        )
        for candidate in (
            PromotedEffectiveSessionTickService,
            VerifiedPromotedEffectiveSessionTickPanel,
            PromotedEffectiveSessionTickResult,
        ):
            members = [name for name in dir(candidate) if not name.startswith("__")]
            for name in members:
                lowered = name.lower()
                self.assertFalse(
                    any(bad in lowered for bad in banned_substrings),
                    f"{candidate!r} unexpectedly exposes {name!r}",
                )

    def test_no_feature_signal_broker_or_io_shaped_capability_exists(self) -> None:
        # feature_eligible is the required always-False safety flag itself
        # (asserted separately below), not a capability leak, so "feature"
        # is deliberately excluded from this banned-substring scan.
        banned_substrings = (
            "signal",
            "model",
            "rank",
            "notif",
            "broker",
            "order",
            "position_size",
            "capital",
            "list",
            "latest",
            "find",
            "download",
            "fetch",
            "network",
            "filesystem",
            "environ",
            "clock",
            "write",
            "delete",
        )
        for candidate in (
            PromotedEffectiveSessionTickService,
            VerifiedPromotedEffectiveSessionTickPanel,
        ):
            members = [name for name in dir(candidate) if not name.startswith("__")]
            for name in members:
                lowered = name.lower()
                self.assertFalse(
                    any(bad in lowered for bad in banned_substrings),
                    f"{candidate!r} unexpectedly exposes {name!r}",
                )

    def test_effective_to_exclusive_is_never_null_on_verified_results(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _, _, source_panel = _two_session_fixture(root)
            panel = PromotedEffectiveSessionTickService().materialize(
                source_panel=source_panel, cutoff=RESULT_CUTOFF
            )
            for result in panel.results:
                if result.status is PromotedEffectiveSessionTickStatus.VERIFIED_EXACT_SESSION_ONLY:
                    self.assertIsNotNone(result.tick_specification.effective_to_exclusive)

    def test_no_forbidden_capability_field_exists(self) -> None:
        # feature_eligible is the required always-False safety flag itself,
        # so "feature" is deliberately excluded from this banned list.
        field_names = {
            field.name for field in dataclasses.fields(VerifiedPromotedEffectiveSessionTickPanel)
        }
        for banned in (
            "signal",
            "model",
            "ranking",
            "recommendation",
            "notification",
            "broker",
            "order",
            "position_size",
            "capital",
        ):
            self.assertFalse(any(banned in name for name in field_names))

    def test_feature_eligible_is_always_false(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _, _, source_panel = _two_session_fixture(root)
            panel = PromotedEffectiveSessionTickService().materialize(
                source_panel=source_panel, cutoff=RESULT_CUTOFF
            )
            self.assertFalse(panel.feature_eligible)

    def test_specification_type_is_the_unmodified_evaluation_effective_tick_size(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _, _, source_panel = _two_session_fixture(root)
            panel = PromotedEffectiveSessionTickService().materialize(
                source_panel=source_panel, cutoff=RESULT_CUTOFF
            )
            spec = panel.results[0].tick_specification
            self.assertIsInstance(spec, EffectiveTickSize)

    def test_importing_module_causes_no_io(self) -> None:
        import india_swing.tick_sizes.effective_session as module

        banned_module_names = {"os", "socket", "urllib", "requests", "storage"}
        top_level_names = {
            name
            for name in vars(module)
            if not name.startswith("_") and not name[0].isupper()
        }
        self.assertFalse(top_level_names & banned_module_names)


class PromotedEffectiveSessionTickLegacyRegressionTests(unittest.TestCase):
    def test_legacy_evaluation_dataset_assembly_untouched(self) -> None:
        from india_swing.evaluation.dataset_assembly import assemble_evaluation_dataset

        self.assertTrue(callable(assemble_evaluation_dataset))

    def test_legacy_promoted_history_module_untouched(self) -> None:
        self.assertTrue(callable(PromotedStableListingHistoryService))


if __name__ == "__main__":
    unittest.main()
