from __future__ import annotations

import tempfile
import unittest
from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

from india_swing.corporate_actions import CorporateActionSnapshot
from india_swing.daily_reports.artifact_store import LocalDailyBundleArtifactStore
from india_swing.daily_reports.parser import NSE_DAILY_BUNDLE_FILENAME
from india_swing.evaluation import EffectiveTickSize
from india_swing.promotion import (
    PromotionCapability,
    PromotionDecision,
    PromotionEvidence,
    PromotionStage,
    evaluate_promotion,
)
from india_swing.reference import (
    EffectiveExternalRecordRef,
    EligibilityStateRef,
    ExternalRecordRef,
    ListingMapping,
    ListingState,
    ReferenceReadiness,
    UniverseDisposition,
    UniverseEntry,
    UniverseSnapshot,
)
from india_swing.domain.models import Board, Surveillance
from india_swing.signals.input_assembly import (
    SwingInputAssemblyError,
    assemble_alert_swing_inputs,
    assemble_swing_inputs,
)
from tests.test_historical_prices import (
    CUTOFF,
    FIRST_SEEN,
    SESSION,
    VALIDATED,
    _bundle_bytes,
    _clock,
)


UTC = timezone.utc
INSTRUMENT_ID = "1" * 64
LISTING_ID = "2" * 64
MASTER_ID = "3" * 64
ELIGIBILITY_ID = "4" * 64
LIQUIDITY_ID = "5" * 64
SOURCE_ROW_ID = "6" * 64
CORPORATE_SOURCE_ID = "7" * 64
TICK_SOURCE_ID = "8" * 64
IDENTITY_SNAPSHOT_ID = "c" * 64


def _reference(source_id: str, digit: str) -> ExternalRecordRef:
    return ExternalRecordRef(
        event_time=datetime(2020, 1, 1, tzinfo=UTC),
        knowledge_time=FIRST_SEEN - timedelta(hours=1),
        source="VERIFIED_TEST_EVIDENCE",
        content_hash=digit * 64,
        source_snapshot_id=source_id,
    )


def universe(*, cutoff: datetime = VALIDATED + timedelta(minutes=10)) -> UniverseSnapshot:
    entry = UniverseEntry(
        source_record_id=SOURCE_ROW_ID,
        listing=ListingMapping(
            instrument_id=INSTRUMENT_ID,
            listing_id=LISTING_ID,
            exchange="NSE",
            segment="CM",
            tradingsymbol="INFY",
            series="EQ",
            isin="INE009A01021",
            valid_from=date(2020, 1, 1),
            valid_to_exclusive=None,
            reference=_reference(MASTER_ID, "9"),
        ),
        board=Board.MAIN,
        listing_state=ListingState.ACTIVE,
        suspended=False,
        surveillance=Surveillance.NONE,
        disposition=UniverseDisposition.ACTIONABLE,
        reason_codes=(),
        eligibility_refs=(
            EligibilityStateRef(
                effective=EffectiveExternalRecordRef(
                    reference=_reference(ELIGIBILITY_ID, "a"),
                    effective_from_session=date(2020, 1, 1),
                    effective_to_exclusive=None,
                    schema_version="verified-eligibility/v1",
                ),
                instrument_id=INSTRUMENT_ID,
                listing_id=LISTING_ID,
                board=Board.MAIN,
                listing_state=ListingState.ACTIVE,
                suspended=False,
                surveillance=Surveillance.NONE,
            ),
        ),
        liquidity_snapshot_id=LIQUIDITY_ID,
        liquidity_cutoff_session=SESSION,
    )
    return UniverseSnapshot.create(
        exchange="NSE",
        segment="CM",
        market_session=SESSION,
        cutoff=cutoff,
        calendar_snapshot_id="b" * 64,
        universe_rules_version="verified-main-board/v1",
        selection_key="ALL_SCOPED_ROWS",
        scoped_source_row_ids=(SOURCE_ROW_ID,),
        security_master_snapshot_ids=(MASTER_ID,),
        eligibility_snapshot_ids=(ELIGIBILITY_ID,),
        liquidity_snapshot_ids=(LIQUIDITY_ID,),
        readiness=ReferenceReadiness.SYNTHETIC_TEST,
        entries=(entry,),
    )


def actions() -> CorporateActionSnapshot:
    return CorporateActionSnapshot(
        cutoff=VALIDATED + timedelta(minutes=5),
        coverage_start=SESSION,
        coverage_end=SESSION,
        source_artifact_ids=(CORPORATE_SOURCE_ID,),
        events=(),
        readiness=ReferenceReadiness.SYNTHETIC_TEST,
        complete=True,
        actionable=True,
        reason_codes=(),
    )


def tick() -> EffectiveTickSize:
    return EffectiveTickSize(
        instrument_id=INSTRUMENT_ID,
        listing_id=LISTING_ID,
        effective_from_session=date(2020, 1, 1),
        effective_to_exclusive=None,
        tick_size=Decimal("0.05"),
        knowledge_time=FIRST_SEEN - timedelta(hours=1),
        source_snapshot_id=TICK_SOURCE_ID,
        readiness=ReferenceReadiness.SYNTHETIC_TEST,
    )


def promotion(
    raw_id: str,
    universe_id: str,
    action_id: str,
    tick_id: str,
    identity_id: str = IDENTITY_SNAPSHOT_ID,
    *,
    history_start: date = SESSION,
) -> PromotionDecision:
    expected = {
        PromotionCapability.RAW_PRICES: (raw_id,),
        PromotionCapability.UNIVERSE: (universe_id,),
        PromotionCapability.STABLE_IDENTITY: (identity_id,),
        PromotionCapability.CORPORATE_ACTIONS: (action_id,),
        PromotionCapability.TICK_SIZES: (tick_id,),
    }
    evidence = tuple(
        sorted(
            (
                PromotionEvidence(
                    capability=capability,
                    cutoff=CUTOFF,
                    coverage_start=SESSION,
                    coverage_end=SESSION,
                    source_snapshot_ids=expected.get(
                        capability,
                        (f"{list(PromotionCapability).index(capability) + 20:064x}",),
                    ),
                    readiness=ReferenceReadiness.SYNTHETIC_TEST,
                    complete=True,
                    actionable=True,
                    reason_codes=(),
                )
                for capability in PromotionCapability
            ),
            key=lambda value: value.capability.value,
        )
    )
    return evaluate_promotion(
        market_session=SESSION,
        history_start=history_start,
        decision_cutoff=CUTOFF,
        evidence=evidence,
    )


class SwingInputAssemblyTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        root = Path(self.temporary.name)
        source = root / "source" / NSE_DAILY_BUNDLE_FILENAME
        source.parent.mkdir()
        source.write_bytes(_bundle_bytes())
        bundle = LocalDailyBundleArtifactStore(
            root / "daily",
            clock=_clock(FIRST_SEEN, VALIDATED),
        ).import_bundle(source)
        from india_swing.historical_prices import materialize_nse_eod_session

        self.raw = materialize_nse_eod_session(
            bundle,
            market_session=SESSION,
            cutoff=CUTOFF,
        )
        self.universe = universe()
        self.actions = actions()
        self.tick = tick()

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def promoted(self, universe_value: UniverseSnapshot | None = None) -> PromotionDecision:
        universe_value = universe_value or self.universe
        return promotion(
            self.raw.artifact_id,
            universe_value.snapshot_id,
            self.actions.snapshot_id,
            self.tick.specification_id,
        )

    def promoted_with(
        self,
        *,
        raw_id: str | None = None,
        universe_id: str | None = None,
        action_id: str | None = None,
        tick_id: str | None = None,
        identity_id: str | None = None,
        history_start: date | None = None,
    ) -> PromotionDecision:
        return promotion(
            raw_id if raw_id is not None else self.raw.artifact_id,
            universe_id if universe_id is not None else self.universe.snapshot_id,
            action_id if action_id is not None else self.actions.snapshot_id,
            tick_id if tick_id is not None else self.tick.specification_id,
            identity_id if identity_id is not None else IDENTITY_SNAPSHOT_ID,
            history_start=history_start if history_start is not None else SESSION,
        )

    def assemble(
        self,
        *,
        universe_value: UniverseSnapshot | None = None,
        promotion_value: PromotionDecision | None = None,
    ):
        universe_value = universe_value or self.universe
        return assemble_swing_inputs(
            history=(self.raw,),
            universes=(universe_value,),
            stable_identity_snapshot_id=IDENTITY_SNAPSHOT_ID,
            stable_instrument_id=INSTRUMENT_ID,
            stable_listing_id=LISTING_ID,
            signal_session=SESSION,
            cutoff=CUTOFF,
            corporate_actions=self.actions,
            tick_size=self.tick,
            promotion=promotion_value or self.promoted(universe_value),
        )

    def test_builds_exact_promoted_adjusted_signal_history(self) -> None:
        result = self.assemble()

        self.assertEqual(result.raw_artifact_ids, (self.raw.artifact_id,))
        self.assertIs(result.readiness, ReferenceReadiness.SYNTHETIC_TEST)
        self.assertFalse(result.actionable)
        self.assertEqual(result.universe_snapshot_ids, (self.universe.snapshot_id,))
        self.assertEqual(result.identity_bindings[0].raw_bar_id, self.raw.bars[1].bar_id)
        self.assertEqual(
            result.adjusted_history.bars[0].knowledge_time,
            CUTOFF,
        )
        self.assertEqual(
            result.signal_materialization.history.bars[0].evidence_id,
            result.adjusted_history.bars[0].adjusted_bar_id,
        )
        result.verify_content_identity()

    def test_rejects_promotion_bound_to_another_raw_artifact(self) -> None:
        wrong = promotion(
            "f" * 64,
            self.universe.snapshot_id,
            self.actions.snapshot_id,
            self.tick.specification_id,
        )
        with self.assertRaisesRegex(SwingInputAssemblyError, "RAW_PRICES promotion"):
            self.assemble(promotion_value=wrong)

    def test_rejects_forged_alert_blockers_that_do_not_replay(self) -> None:
        valid = self.promoted()
        raw = valid.evidence_for(PromotionCapability.RAW_PRICES)
        blocked_raw = replace(
            raw,
            readiness=ReferenceReadiness.COLLECTION_ONLY,
            complete=False,
            actionable=False,
            reason_codes=("UNVERIFIED",),
        )
        evidence = tuple(
            blocked_raw if value.capability is PromotionCapability.RAW_PRICES else value
            for value in valid.evidence
        )
        forged = PromotionDecision(
            market_session=SESSION,
            history_start=SESSION,
            decision_cutoff=CUTOFF,
            evidence=evidence,
            achieved_stage=PromotionStage.ALERT_ELIGIBLE,
            research_blockers=(),
            backtest_blockers=(),
            alert_blockers=(),
        )
        with self.assertRaisesRegex(SwingInputAssemblyError, "does not replay"):
            self.assemble(promotion_value=forged)

    def test_synthetic_research_inputs_cannot_enter_alert_engine(self) -> None:
        with self.assertRaisesRegex(SwingInputAssemblyError, "cannot enter the alert"):
            assemble_alert_swing_inputs(
                history=(self.raw,),
                universes=(self.universe,),
                stable_identity_snapshot_id=IDENTITY_SNAPSHOT_ID,
                stable_instrument_id=INSTRUMENT_ID,
                stable_listing_id=LISTING_ID,
                signal_session=SESSION,
                cutoff=CUTOFF,
                corporate_actions=self.actions,
                tick_size=self.tick,
                promotion=self.promoted(),
            )

    def test_rejects_non_actionable_signal_session_listing(self) -> None:
        entry = replace(
            self.universe.entries[0],
            disposition=UniverseDisposition.EXCLUDED,
            reason_codes=("SURVEILLANCE_BLOCKED",),
            liquidity_snapshot_id=None,
            liquidity_cutoff_session=None,
        )
        blocked = UniverseSnapshot.create(
            exchange=self.universe.exchange,
            segment=self.universe.segment,
            market_session=self.universe.market_session,
            cutoff=self.universe.cutoff,
            calendar_snapshot_id=self.universe.calendar_snapshot_id,
            universe_rules_version=self.universe.universe_rules_version,
            selection_key=self.universe.selection_key,
            scoped_source_row_ids=self.universe.scoped_source_row_ids,
            security_master_snapshot_ids=self.universe.security_master_snapshot_ids,
            eligibility_snapshot_ids=self.universe.eligibility_snapshot_ids,
            liquidity_snapshot_ids=(),
            readiness=self.universe.readiness,
            entries=(entry,),
        )
        with self.assertRaisesRegex(SwingInputAssemblyError, "not actionable"):
            self.assemble(universe_value=blocked)

    def test_rejects_future_known_universe_snapshot(self) -> None:
        future = universe(cutoff=CUTOFF + timedelta(microseconds=1))
        with self.assertRaisesRegex(SwingInputAssemblyError, "future-known"):
            self.assemble(universe_value=future)

    def test_detects_nested_adjusted_bar_mutation(self) -> None:
        result = self.assemble()
        object.__setattr__(result.adjusted_history.bars[0], "close", Decimal("999"))

        with self.assertRaisesRegex(Exception, "content identity"):
            result.verify_content_identity()

    def test_synthetic_assembly_embeds_exact_promotion_and_verifies_deterministically(
        self,
    ) -> None:
        expected_promotion = self.promoted()
        result = self.assemble(promotion_value=expected_promotion)

        self.assertEqual(result.promotion, expected_promotion)
        self.assertEqual(result.promotion_decision_id, expected_promotion.decision_id)
        self.assertFalse(result.actionable)
        result.verify_content_identity()
        result.verify_content_identity()

    def test_rejects_direct_construction_with_mismatched_duplicated_decision_id(self) -> None:
        result = self.assemble()

        with self.assertRaisesRegex(
            SwingInputAssemblyError, "does not match the assembly lineage"
        ):
            replace(result, promotion_decision_id="d" * 64)

    def test_rejects_promotion_subclass_even_with_identical_content(self) -> None:
        result = self.assemble()

        class _ShapedPromotion(PromotionDecision):
            pass

        shaped = _ShapedPromotion(
            market_session=result.promotion.market_session,
            history_start=result.promotion.history_start,
            decision_cutoff=result.promotion.decision_cutoff,
            evidence=result.promotion.evidence,
            achieved_stage=result.promotion.achieved_stage,
            research_blockers=result.promotion.research_blockers,
            backtest_blockers=result.promotion.backtest_blockers,
            alert_blockers=result.promotion.alert_blockers,
        )
        self.assertEqual(shaped.decision_id, result.promotion.decision_id)

        with self.assertRaisesRegex(SwingInputAssemblyError, "promotion must be exact"):
            replace(result, promotion=shaped)

    def test_rejects_promotion_whose_replay_differs_from_its_own_evidence(self) -> None:
        result = self.assemble()
        mismatched = self.promoted_with(history_start=SESSION - timedelta(days=1))

        with self.assertRaisesRegex(SwingInputAssemblyError, "does not replay"):
            replace(
                result,
                promotion=mismatched,
                promotion_decision_id=mismatched.decision_id,
            )

    def test_rejects_direct_construction_with_wrong_promotion_source_binding(self) -> None:
        result = self.assemble()
        wrong_id = "f" * 64
        overrides = (
            ("RAW_PRICES", {"raw_id": wrong_id}),
            ("UNIVERSE", {"universe_id": wrong_id}),
            ("STABLE_IDENTITY", {"identity_id": wrong_id}),
            ("CORPORATE_ACTIONS", {"action_id": wrong_id}),
            ("TICK_SIZES", {"tick_id": wrong_id}),
        )
        for capability_name, override in overrides:
            with self.subTest(capability=capability_name):
                wrong_promotion = self.promoted_with(**override)
                with self.assertRaisesRegex(
                    SwingInputAssemblyError, f"{capability_name} promotion"
                ):
                    replace(
                        result,
                        promotion=wrong_promotion,
                        promotion_decision_id=wrong_promotion.decision_id,
                    )

    def test_detects_promotion_evidence_mutation_without_disturbing_outer_assembly_id(
        self,
    ) -> None:
        result = self.assemble()
        original_assembly_id = result.assembly_id
        raw_evidence = result.promotion.evidence_for(PromotionCapability.RAW_PRICES)
        object.__setattr__(raw_evidence, "actionable", False)

        self.assertEqual(result.assembly_id, original_assembly_id)
        with self.assertRaisesRegex(Exception, "content identity"):
            result.verify_content_identity()

    def test_cannot_forge_actionable_by_replacing_readiness_or_promotion_status(self) -> None:
        result = self.assemble()

        with self.assertRaisesRegex(
            SwingInputAssemblyError, "point-in-time-verified inputs can be alert actionable"
        ):
            replace(result, actionable=True)

        forged_promotion = replace(
            result.promotion,
            achieved_stage=PromotionStage.ALERT_ELIGIBLE,
            alert_blockers=(),
        )
        with self.assertRaisesRegex(SwingInputAssemblyError, "does not replay"):
            replace(
                result,
                promotion=forged_promotion,
                promotion_decision_id=forged_promotion.decision_id,
            )

        with self.assertRaisesRegex(SwingInputAssemblyError, "cannot enter the alert"):
            assemble_alert_swing_inputs(
                history=(self.raw,),
                universes=(self.universe,),
                stable_identity_snapshot_id=IDENTITY_SNAPSHOT_ID,
                stable_instrument_id=INSTRUMENT_ID,
                stable_listing_id=LISTING_ID,
                signal_session=SESSION,
                cutoff=CUTOFF,
                corporate_actions=self.actions,
                tick_size=self.tick,
                promotion=self.promoted(),
            )


if __name__ == "__main__":
    unittest.main()
