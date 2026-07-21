from __future__ import annotations

import unittest
from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

from india_swing.corporate_actions import (
    CorporateActionEvent,
    CorporateActionSnapshot,
    CorporateActionStatus,
    CorporateActionType,
)
from india_swing.corporate_actions.adjustments import (
    ADJUSTED_PRICE_BASIS,
    PriceAdjustmentError,
    StableRawBarBinding,
    build_adjusted_price_history,
)
from india_swing.daily_reports.models import DailyReportFamily
from india_swing.evaluation.dataset_assembly import EffectiveTickSize
from india_swing.historical_prices.models import PriceRowRef, RawNseEodBar
from india_swing.reference.models import ReferenceReadiness
from india_swing.signals.history_adapter import (
    SwingHistoryAdapterError,
    materialize_swing_history,
)


UTC = timezone.utc
START = date(2026, 1, 1)
SIGNAL = date(2026, 1, 3)
CUTOFF = datetime(2026, 1, 3, 12, 0, tzinfo=UTC)
INSTRUMENT_ID = "1" * 64
LISTING_ID = "2" * 64
SOURCE_ID = "3" * 64
ROW_ID = "4" * 64
TICK_SOURCE_ID = "5" * 64
IDENTITY_SNAPSHOT_ID = "7" * 64


def D(value: str) -> Decimal:
    return Decimal(value)


def raw_bar(
    session: date,
    close: Decimal,
    volume: int,
    row_digit: str,
) -> RawNseEodBar:
    row = PriceRowRef(
        report_ref_id="a" * 64,
        family=DailyReportFamily.UDIFF_BHAVCOPY,
        source_row_number=2,
        row_sha256=row_digit * 64,
        listing_key=("TEST", "EQ"),
    )
    return RawNseEodBar(
        market_session=session,
        financial_instrument_id=100,
        validated_isin="INE009A01021",
        symbol="TEST",
        series="EQ",
        session_id="F1",
        instrument_name="TEST LIMITED",
        open=close,
        high=close + D("1"),
        low=close - D("1"),
        close=close,
        last=close,
        previous_close=close - D("1"),
        volume=volume,
        traded_value=close * volume,
        trade_count=10,
        board_lot_quantity=1,
        full_average_price=None,
        delivery_quantity=None,
        delivery_percent=None,
        knowledge_time=datetime.combine(session, datetime.min.time(), tzinfo=UTC)
        + timedelta(hours=10),
        udiff_row_ref=row,
        full_delivery_row_ref=None,
    )


def raw_bars() -> tuple[RawNseEodBar, ...]:
    return (
        raw_bar(START, D("100"), 100, "b"),
        raw_bar(START + timedelta(days=1), D("105"), 120, "c"),
        raw_bar(SIGNAL, D("55"), 220, "d"),
    )


def identity_bindings(
    source: tuple[RawNseEodBar, ...] | None = None,
) -> tuple[StableRawBarBinding, ...]:
    source = source or raw_bars()
    return tuple(
        StableRawBarBinding(
            market_session=value.market_session,
            raw_bar_id=value.bar_id,
            stable_instrument_id=INSTRUMENT_ID,
            stable_listing_id=LISTING_ID,
            identity_snapshot_id=IDENTITY_SNAPSHOT_ID,
            knowledge_time=value.knowledge_time,
        )
        for value in source
    )


def event(
    *,
    action_type: CorporateActionType = CorporateActionType.SPLIT,
    status: CorporateActionStatus = CorporateActionStatus.CONFIRMED,
    supersedes_event_id: str | None = None,
    knowledge_time: datetime = datetime(2026, 1, 2, 8, 5, tzinfo=UTC),
) -> CorporateActionEvent:
    share_terms = status is CorporateActionStatus.CONFIRMED and action_type in {
        CorporateActionType.SPLIT,
        CorporateActionType.BONUS,
    }
    dividend = status is CorporateActionStatus.CONFIRMED and action_type is CorporateActionType.CASH_DIVIDEND
    return CorporateActionEvent(
        stable_instrument_id=INSTRUMENT_ID,
        stable_listing_id=LISTING_ID,
        action_type=action_type,
        status=status,
        effective_session=SIGNAL,
        announcement_time=knowledge_time - timedelta(minutes=5),
        knowledge_time=knowledge_time,
        source_artifact_id=SOURCE_ID,
        source_row_id=ROW_ID if supersedes_event_id is None else "6" * 64,
        pre_action_shares=D("1") if share_terms else None,
        post_action_shares=D("2") if share_terms else None,
        cash_amount_per_share=D("5") if dividend else None,
        currency="INR" if dividend else None,
        supersedes_event_id=supersedes_event_id,
    )


def action_snapshot(
    *events: CorporateActionEvent,
    cutoff: datetime = CUTOFF,
    readiness: ReferenceReadiness = ReferenceReadiness.SYNTHETIC_TEST,
    complete: bool = True,
    actionable: bool = True,
    reason_codes: tuple[str, ...] = (),
) -> CorporateActionSnapshot:
    return CorporateActionSnapshot(
        cutoff=cutoff,
        coverage_start=START,
        coverage_end=SIGNAL,
        source_artifact_ids=(SOURCE_ID,),
        events=tuple(sorted(events, key=lambda value: (value.knowledge_time, value.effective_session, value.event_id))),
        readiness=readiness,
        complete=complete,
        actionable=actionable,
        reason_codes=reason_codes,
    )


def tick_size(*, knowledge_time: datetime = datetime(2026, 1, 1, 8, 0, tzinfo=UTC), readiness: ReferenceReadiness = ReferenceReadiness.SYNTHETIC_TEST) -> EffectiveTickSize:
    return EffectiveTickSize(
        instrument_id=INSTRUMENT_ID,
        listing_id=LISTING_ID,
        effective_from_session=START,
        effective_to_exclusive=None,
        tick_size=D("0.05"),
        knowledge_time=knowledge_time,
        source_snapshot_id=TICK_SOURCE_ID,
        readiness=readiness,
    )


class CorporateActionAdjustmentTests(unittest.TestCase):
    def test_split_adjusts_only_pre_effective_prices_and_volumes(self) -> None:
        source = raw_bars()
        split = event()
        adjusted = build_adjusted_price_history(
            raw_bars=source,
            identity_bindings=identity_bindings(source),
            stable_instrument_id=INSTRUMENT_ID,
            stable_listing_id=LISTING_ID,
            signal_session=SIGNAL,
            cutoff=CUTOFF,
            corporate_actions=action_snapshot(split),
        )

        self.assertEqual(adjusted.price_basis, ADJUSTED_PRICE_BASIS)
        self.assertEqual(adjusted.bars[0].close, D("50"))
        self.assertEqual(adjusted.bars[0].volume, D("200"))
        self.assertEqual(adjusted.bars[0].traded_value, source[0].traded_value)
        self.assertEqual(adjusted.bars[0].price_factor, D("0.5"))
        self.assertEqual(adjusted.bars[0].applied_event_ids, (split.event_id,))
        self.assertEqual(adjusted.bars[-1].close, D("55"))
        self.assertEqual(adjusted.bars[-1].volume, D("220"))
        self.assertEqual(adjusted.bars[-1].price_factor, D("1"))
        self.assertEqual(adjusted.bars[-1].applied_event_ids, ())

    def test_raw_bars_are_not_rewritten(self) -> None:
        source = raw_bars()
        raw_ids = tuple(value.bar_id for value in source)
        original_closes = tuple(value.close for value in source)

        build_adjusted_price_history(
            raw_bars=source,
            identity_bindings=identity_bindings(source),
            stable_instrument_id=INSTRUMENT_ID,
            stable_listing_id=LISTING_ID,
            signal_session=SIGNAL,
            cutoff=CUTOFF,
            corporate_actions=action_snapshot(event()),
        )

        self.assertEqual(tuple(value.bar_id for value in source), raw_ids)
        self.assertEqual(tuple(value.close for value in source), original_closes)

    def test_materializes_signal_history_and_exact_evidence(self) -> None:
        adjusted = build_adjusted_price_history(
            raw_bars=raw_bars(),
            identity_bindings=identity_bindings(),
            stable_instrument_id=INSTRUMENT_ID,
            stable_listing_id=LISTING_ID,
            signal_session=SIGNAL,
            cutoff=CUTOFF,
            corporate_actions=action_snapshot(event()),
        )
        result = materialize_swing_history(adjusted=adjusted, tick_size=tick_size())

        self.assertEqual(result.history.instrument_id, INSTRUMENT_ID)
        self.assertEqual(result.history.tick_size, D("0.05"))
        self.assertEqual(result.history.bars[0].volume, D("200"))
        self.assertEqual(result.history.adjustment_evidence_id, adjusted.corporate_action_snapshot_id)
        self.assertEqual(
            tuple(value.evidence_id for value in result.evidence),
            tuple(value.evidence_id for value in result.history.bars)
            + (result.history.tick_evidence_id, result.history.adjustment_evidence_id),
        )
        result.verify_content_identity()

    def test_cash_dividend_blocks_until_a_total_return_method_exists(self) -> None:
        with self.assertRaisesRegex(PriceAdjustmentError, "unsupported adjustment"):
            build_adjusted_price_history(
                raw_bars=raw_bars(),
                identity_bindings=identity_bindings(),
                stable_instrument_id=INSTRUMENT_ID,
                stable_listing_id=LISTING_ID,
                signal_session=SIGNAL,
                cutoff=CUTOFF,
                corporate_actions=action_snapshot(event(action_type=CorporateActionType.CASH_DIVIDEND)),
            )

    def test_cancelled_split_is_not_applied(self) -> None:
        original = event()
        cancellation = event(
            status=CorporateActionStatus.CANCELLED,
            supersedes_event_id=original.event_id,
            knowledge_time=datetime(2026, 1, 2, 9, 0, tzinfo=UTC),
        )
        adjusted = build_adjusted_price_history(
            raw_bars=raw_bars(),
            identity_bindings=identity_bindings(),
            stable_instrument_id=INSTRUMENT_ID,
            stable_listing_id=LISTING_ID,
            signal_session=SIGNAL,
            cutoff=CUTOFF,
            corporate_actions=action_snapshot(original, cancellation),
        )

        self.assertTrue(all(value.price_factor == D("1") for value in adjusted.bars))

    def test_rejects_collection_only_corporate_action_snapshot(self) -> None:
        blocked = action_snapshot(
            event(),
            readiness=ReferenceReadiness.COLLECTION_ONLY,
            complete=False,
            actionable=False,
            reason_codes=("COLLECTION_ONLY_SOURCE",),
        )
        with self.assertRaisesRegex(PriceAdjustmentError, "not actionable"):
            build_adjusted_price_history(
                raw_bars=raw_bars(),
                identity_bindings=identity_bindings(),
                stable_instrument_id=INSTRUMENT_ID,
                stable_listing_id=LISTING_ID,
                signal_session=SIGNAL,
                cutoff=CUTOFF,
                corporate_actions=blocked,
            )

    def test_rejects_future_known_corporate_action_snapshot(self) -> None:
        future = action_snapshot(event(), cutoff=CUTOFF + timedelta(minutes=1))
        with self.assertRaisesRegex(PriceAdjustmentError, "future-known"):
            build_adjusted_price_history(
                raw_bars=raw_bars(),
                identity_bindings=identity_bindings(),
                stable_instrument_id=INSTRUMENT_ID,
                stable_listing_id=LISTING_ID,
                signal_session=SIGNAL,
                cutoff=CUTOFF,
                corporate_actions=future,
            )

    def test_rejects_raw_bar_attributed_to_another_stable_identity(self) -> None:
        bindings = list(identity_bindings())
        bindings[0] = replace(bindings[0], stable_instrument_id="8" * 64)

        with self.assertRaisesRegex(PriceAdjustmentError, "identity binding differ"):
            build_adjusted_price_history(
                raw_bars=raw_bars(),
                identity_bindings=tuple(bindings),
                stable_instrument_id=INSTRUMENT_ID,
                stable_listing_id=LISTING_ID,
                signal_session=SIGNAL,
                cutoff=CUTOFF,
                corporate_actions=action_snapshot(event()),
            )

    def test_rejects_future_known_tick_size(self) -> None:
        adjusted = build_adjusted_price_history(
            raw_bars=raw_bars(),
            identity_bindings=identity_bindings(),
            stable_instrument_id=INSTRUMENT_ID,
            stable_listing_id=LISTING_ID,
            signal_session=SIGNAL,
            cutoff=CUTOFF,
            corporate_actions=action_snapshot(event()),
        )
        with self.assertRaisesRegex(SwingHistoryAdapterError, "future-known"):
            materialize_swing_history(
                adjusted=adjusted,
                tick_size=tick_size(knowledge_time=CUTOFF + timedelta(minutes=1)),
            )

    def test_rejects_collection_only_tick_size(self) -> None:
        adjusted = build_adjusted_price_history(
            raw_bars=raw_bars(),
            identity_bindings=identity_bindings(),
            stable_instrument_id=INSTRUMENT_ID,
            stable_listing_id=LISTING_ID,
            signal_session=SIGNAL,
            cutoff=CUTOFF,
            corporate_actions=action_snapshot(event()),
        )
        with self.assertRaisesRegex(SwingHistoryAdapterError, "collection-only"):
            materialize_swing_history(
                adjusted=adjusted,
                tick_size=tick_size(readiness=ReferenceReadiness.COLLECTION_ONLY),
            )

    def test_detects_adjusted_history_mutation(self) -> None:
        adjusted = build_adjusted_price_history(
            raw_bars=raw_bars(),
            identity_bindings=identity_bindings(),
            stable_instrument_id=INSTRUMENT_ID,
            stable_listing_id=LISTING_ID,
            signal_session=SIGNAL,
            cutoff=CUTOFF,
            corporate_actions=action_snapshot(event()),
        )
        object.__setattr__(adjusted.bars[0], "close", D("999"))

        with self.assertRaisesRegex(PriceAdjustmentError, "adjusted bar content identity"):
            adjusted.verify_content_identity()


if __name__ == "__main__":
    unittest.main()
