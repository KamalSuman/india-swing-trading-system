from __future__ import annotations

import unittest
from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

from india_swing.corporate_actions import (
    CorporateActionEvent,
    CorporateActionIntegrityError,
    CorporateActionSnapshot,
    CorporateActionStatus,
    CorporateActionType,
    corporate_action_promotion_evidence,
)
from india_swing.promotion import PromotionCapability
from india_swing.reference import ReferenceReadiness


IST = timezone(timedelta(hours=5, minutes=30))
INSTRUMENT_ID = "1" * 64
LISTING_ID = "2" * 64
SOURCE_ID = "3" * 64
SECOND_SOURCE_ID = "4" * 64
ROW_ID = "5" * 64
SECOND_ROW_ID = "6" * 64
ANNOUNCED = datetime(2026, 7, 1, 18, 0, tzinfo=IST)
KNOWN = datetime(2026, 7, 1, 18, 5, tzinfo=IST)
EFFECTIVE = date(2026, 7, 15)


def split_event() -> CorporateActionEvent:
    return CorporateActionEvent(
        stable_instrument_id=INSTRUMENT_ID,
        stable_listing_id=LISTING_ID,
        action_type=CorporateActionType.SPLIT,
        status=CorporateActionStatus.CONFIRMED,
        effective_session=EFFECTIVE,
        announcement_time=ANNOUNCED,
        knowledge_time=KNOWN,
        source_artifact_id=SOURCE_ID,
        source_row_id=ROW_ID,
        pre_action_shares=Decimal("1"),
        post_action_shares=Decimal("2"),
    )


def collection_snapshot(*events: CorporateActionEvent) -> CorporateActionSnapshot:
    return CorporateActionSnapshot(
        cutoff=datetime(2026, 7, 16, 17, 0, tzinfo=IST),
        coverage_start=date(2020, 1, 1),
        coverage_end=date(2026, 7, 16),
        source_artifact_ids=tuple(
            sorted({SOURCE_ID, *(value.source_artifact_id for value in events)})
        ),
        events=tuple(
            sorted(
                events,
                key=lambda value: (
                    value.knowledge_time,
                    value.effective_session,
                    value.event_id,
                ),
            )
        ),
        readiness=ReferenceReadiness.COLLECTION_ONLY,
        complete=False,
        actionable=False,
        reason_codes=("OFFICIAL_IMPORTER_NOT_IMPLEMENTED",),
    )


class CorporateActionTests(unittest.TestCase):
    def test_split_factor_is_explicit_and_content_bound(self) -> None:
        event = split_event()

        self.assertEqual(event.automatic_raw_price_factor, Decimal("0.5"))
        event.verify_content_identity()

    def test_cash_dividend_has_no_unsafe_automatic_factor(self) -> None:
        event = CorporateActionEvent(
            stable_instrument_id=INSTRUMENT_ID,
            stable_listing_id=LISTING_ID,
            action_type=CorporateActionType.CASH_DIVIDEND,
            status=CorporateActionStatus.CONFIRMED,
            effective_session=EFFECTIVE,
            announcement_time=ANNOUNCED,
            knowledge_time=KNOWN,
            source_artifact_id=SOURCE_ID,
            source_row_id=ROW_ID,
            cash_amount_per_share=Decimal("5.50"),
            currency="INR",
        )

        self.assertIsNone(event.automatic_raw_price_factor)

    def test_complex_action_terms_fail_until_a_specific_contract_exists(self) -> None:
        with self.assertRaisesRegex(
            CorporateActionIntegrityError,
            "action-specific terms contract",
        ):
            CorporateActionEvent(
                stable_instrument_id=INSTRUMENT_ID,
                stable_listing_id=LISTING_ID,
                action_type=CorporateActionType.MERGER,
                status=CorporateActionStatus.CONFIRMED,
                effective_session=EFFECTIVE,
                announcement_time=ANNOUNCED,
                knowledge_time=KNOWN,
                source_artifact_id=SOURCE_ID,
                source_row_id=ROW_ID,
                pre_action_shares=Decimal("1"),
            )

    def test_snapshot_rejects_future_knowledge(self) -> None:
        event = replace(
            split_event(),
            knowledge_time=datetime(2026, 7, 17, 9, 0, tzinfo=IST),
        )

        with self.assertRaisesRegex(CorporateActionIntegrityError, "after its cutoff"):
            collection_snapshot(event)

    def test_cancellation_supersedes_without_erasing_original_evidence(self) -> None:
        original = split_event()
        cancellation = CorporateActionEvent(
            stable_instrument_id=INSTRUMENT_ID,
            stable_listing_id=LISTING_ID,
            action_type=CorporateActionType.SPLIT,
            status=CorporateActionStatus.CANCELLED,
            effective_session=EFFECTIVE,
            announcement_time=ANNOUNCED + timedelta(days=1),
            knowledge_time=KNOWN + timedelta(days=1),
            source_artifact_id=SECOND_SOURCE_ID,
            source_row_id=SECOND_ROW_ID,
            supersedes_event_id=original.event_id,
        )
        snapshot = collection_snapshot(original, cancellation)

        self.assertEqual(snapshot.active_events, ())
        self.assertEqual(len(snapshot.events), 2)
        snapshot.verify_content_identity()

    def test_missing_amendment_target_fails_closed(self) -> None:
        cancellation = CorporateActionEvent(
            stable_instrument_id=INSTRUMENT_ID,
            stable_listing_id=LISTING_ID,
            action_type=CorporateActionType.SPLIT,
            status=CorporateActionStatus.CANCELLED,
            effective_session=EFFECTIVE,
            announcement_time=ANNOUNCED,
            knowledge_time=KNOWN,
            source_artifact_id=SOURCE_ID,
            source_row_id=ROW_ID,
            supersedes_event_id="9" * 64,
        )

        with self.assertRaisesRegex(CorporateActionIntegrityError, "target is absent"):
            collection_snapshot(cancellation)

    def test_collection_snapshot_maps_to_non_actionable_promotion_evidence(self) -> None:
        snapshot = collection_snapshot(split_event())
        evidence = corporate_action_promotion_evidence(snapshot)

        self.assertEqual(evidence.capability, PromotionCapability.CORPORATE_ACTIONS)
        self.assertEqual(evidence.source_snapshot_ids, (snapshot.snapshot_id,))
        self.assertFalse(evidence.actionable)
        evidence.verify_content_identity()

    def test_snapshot_content_mutation_is_detected(self) -> None:
        snapshot = collection_snapshot(split_event())
        object.__setattr__(snapshot, "coverage_end", date(2026, 7, 15))

        with self.assertRaises(CorporateActionIntegrityError):
            snapshot.verify_content_identity()


if __name__ == "__main__":
    unittest.main()
