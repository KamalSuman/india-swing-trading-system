from __future__ import annotations

import dataclasses
import hashlib
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

from india_swing.market_data.historical_corpus import (
    HistoricalEvaluationCorpusBar,
    HistoricalEvaluationCorpusIndex,
    HistoricalEvaluationCorpusSessionPartition,
)
from india_swing.market_data.promoted_session_frame import (
    PromotedSessionBarStatus,
    PromotedSessionMarketDataError,
    PromotedSessionMarketDataFrameService,
    VerifiedPromotedSessionMarketDataFrame,
)
from india_swing.reference.models import ReferenceReadiness
from india_swing.universe import PromotedIdentitySessionDisposition
from tests.test_promoted_identity_session_universe import happy_path_fixture


UTC = timezone.utc
ASSESSED_AT = datetime(2026, 7, 17, 9, 0, tzinfo=UTC)
BUILT_AT = datetime(2026, 7, 17, 10, 0, tzinfo=UTC)
FRAME_CUTOFF = datetime(2026, 7, 17, 11, 0, tzinfo=UTC)


def _id(label: str) -> str:
    return hashlib.sha256(label.encode("utf-8")).hexdigest()


def _bar(
    entry: object,
    *,
    market_session: object,
    label: str,
    isin: str | None = None,
    observed_at: datetime = BUILT_AT - timedelta(minutes=5),
) -> HistoricalEvaluationCorpusBar:
    symbol = getattr(entry, "symbol")
    series = getattr(entry, "series")
    source_isin = getattr(entry, "validated_isin")
    return HistoricalEvaluationCorpusBar(
        session=market_session,
        listing_key=f"NSE:{symbol}",
        series=series,
        isin=isin or source_isin,
        open=Decimal("100.00"),
        high=Decimal("105.00"),
        low=Decimal("99.00"),
        close=Decimal("103.00"),
        volume=1000,
        provider="ZERODHA_KITE",
        request_id=_id(f"{label}-request"),
        binding_id=_id(f"{label}-binding"),
        provider_snapshot_id=_id(f"{label}-provider-snapshot"),
        historical_batch_id=_id(f"{label}-batch"),
        reconciliation_report_id=_id(f"{label}-report"),
        reconciliation_snapshot_id=_id(f"{label}-reconciliation"),
        observed_at=observed_at,
    )
def _corpus(
    *,
    market_session: object,
    bars: tuple[HistoricalEvaluationCorpusBar, ...],
    built_at: datetime = BUILT_AT,
    index_partition: HistoricalEvaluationCorpusSessionPartition | None = None,
) -> tuple[
    HistoricalEvaluationCorpusIndex,
    HistoricalEvaluationCorpusSessionPartition,
]:
    partition = HistoricalEvaluationCorpusSessionPartition(
        market_session=market_session,
        bars=tuple(sorted(bars, key=lambda value: value.listing_lane)),
        source_snapshot_ids=tuple(
            sorted(
                {
                    value.provider_snapshot_id
                    for value in bars
                }
                | {
                    value.reconciliation_snapshot_id
                    for value in bars
                }
            )
        ),
        source_report_ids=tuple(
            sorted({value.reconciliation_report_id for value in bars})
        ),
    )
    retained_partition = index_partition or partition
    index = HistoricalEvaluationCorpusIndex(
        admission_report_id=_id("admission-report"),
        reconciliation_index_id=_id("reconciliation-index"),
        plan_id=_id("plan"),
        progress_id=_id("progress"),
        provider="ZERODHA_KITE",
        connector_version="test/1",
        assessed_at=ASSESSED_AT,
        built_at=built_at,
        partition_ids=(retained_partition.partition_id,),
        partition_sessions=(retained_partition.market_session,),
        all_entry_ids=(_id("admission-entry"),),
        admitted_entry_ids=(_id("admission-entry"),),
        blocked_entry_ids=(),
        disposition_counts=(("ADMITTED", 1),),
        safe_requests_complete=False,
        coverage_complete=False,
    )
    return index, partition


def _happy_frame(root: Path) -> tuple[
    object,
    HistoricalEvaluationCorpusIndex,
    HistoricalEvaluationCorpusSessionPartition,
    VerifiedPromotedSessionMarketDataFrame,
]:
    _, _, _, _, universe = happy_path_fixture(root)
    by_symbol = {entry.symbol: entry for entry in universe.entries}
    orphan_proxy = dataclasses.replace(
        by_symbol["RELIANCE"],
        symbol="ORPHAN",
        validated_isin="INE999A01019",
    )
    bars = (
        _bar(
            by_symbol["RELIANCE"],
            market_session=universe.market_session,
            label="reliance",
        ),
        _bar(
            by_symbol["SMALL1"],
            market_session=universe.market_session,
            label="small",
        ),
        _bar(
            orphan_proxy,
            market_session=universe.market_session,
            label="orphan",
        ),
    )
    index, partition = _corpus(
        market_session=universe.market_session,
        bars=bars,
    )
    frame = PromotedSessionMarketDataFrameService().materialize(
        universe=universe,
        corpus_index=index,
        partition=partition,
        cutoff=FRAME_CUTOFF,
    )
    return universe, index, partition, frame


class PromotedSessionMarketDataAcceptanceTests(unittest.TestCase):
    def test_retains_every_universe_row_and_every_orphan_bar(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            universe, _, partition, frame = _happy_frame(Path(tmp))
            self.assertEqual(
                {entry.source_record_id for entry in frame.entries},
                {entry.source_record_id for entry in universe.entries},
            )
            retained_bar_ids = {
                entry.bar.bar_id for entry in frame.entries if entry.bar is not None
            }
            orphan_bar_ids = {value.bar.bar_id for value in frame.orphan_bars}
            self.assertEqual(
                retained_bar_ids | orphan_bar_ids,
                {value.bar_id for value in partition.bars},
            )
            self.assertFalse(retained_bar_ids & orphan_bar_ids)
            self.assertEqual(len(frame.orphan_bars), 1)
            frame.verify_content_identity()

    def test_resolved_and_unresolved_observations_remain_distinct(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            _, _, _, frame = _happy_frame(Path(tmp))
            by_symbol = {
                entry.universe_entry.symbol: entry for entry in frame.entries
            }
            resolved = by_symbol["RELIANCE"]
            self.assertIs(
                resolved.status,
                PromotedSessionBarStatus.RESOLVED_LISTING_BAR_OBSERVED,
            )
            self.assertTrue(resolved.stable_identity_bound)
            unresolved = by_symbol["SMALL1"]
            self.assertIs(
                unresolved.status,
                PromotedSessionBarStatus.CANDIDATE_BAR_OBSERVED_IDENTITY_UNRESOLVED,
            )
            self.assertFalse(unresolved.stable_identity_bound)
            self.assertIn(
                "CANDIDATE_BAR_OBSERVED_NOT_STABLE_BOUND",
                unresolved.reason_codes,
            )

    def test_missing_bar_makes_no_delisting_or_zero_volume_inference(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            _, _, _, frame = _happy_frame(Path(tmp))
            deleted = next(
                value
                for value in frame.entries
                if value.universe_entry.symbol == "DELISTD"
            )
            self.assertIsNone(deleted.bar)
            self.assertIs(
                deleted.status,
                PromotedSessionBarStatus.IDENTITY_UNRESOLVED_BAR_NOT_OBSERVED,
            )
            self.assertIn(
                "PRICE_BAR_NOT_OBSERVED_NO_STATE_INFERENCE",
                deleted.reason_codes,
            )
            forbidden = ("DELISTED", "SUSPENDED", "ZERO_VOLUME")
            self.assertFalse(
                any(
                    token in reason
                    for reason in deleted.reason_codes
                    for token in forbidden
                )
            )

    def test_collection_only_flags_cannot_authorize_training_or_alerts(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            _, _, _, frame = _happy_frame(Path(tmp))
            self.assertIs(frame.readiness, ReferenceReadiness.COLLECTION_ONLY)
            self.assertFalse(frame.actionable)
            self.assertFalse(frame.training_eligible)
            self.assertFalse(frame.alert_eligible)
            self.assertFalse(frame.execution_eligible)


class PromotedSessionMarketDataConflictTests(unittest.TestCase):
    def test_same_lane_wrong_isin_is_retained_as_conflict(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            _, _, _, _, universe = happy_path_fixture(Path(tmp))
            by_symbol = {entry.symbol: entry for entry in universe.entries}
            wrong_isin = by_symbol["SMALL1"].validated_isin
            self.assertNotEqual(
                wrong_isin,
                by_symbol["RELIANCE"].validated_isin,
            )
            bar = _bar(
                by_symbol["RELIANCE"],
                market_session=universe.market_session,
                label="conflict",
                isin=wrong_isin,
            )
            index, partition = _corpus(
                market_session=universe.market_session,
                bars=(bar,),
            )
            frame = PromotedSessionMarketDataFrameService().materialize(
                universe=universe,
                corpus_index=index,
                partition=partition,
                cutoff=FRAME_CUTOFF,
            )
            reliance = next(
                value
                for value in frame.entries
                if value.universe_entry.symbol == "RELIANCE"
            )
            self.assertIs(
                reliance.status,
                PromotedSessionBarStatus.LANE_BAR_IDENTITY_CONFLICT,
            )
            self.assertFalse(reliance.stable_identity_bound)
            self.assertIn("BAR_IDENTITY_CONFLICT", reliance.reason_codes)

    def test_source_excluded_row_stays_excluded_when_bar_exists(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            _, _, _, _, universe = happy_path_fixture(Path(tmp))
            excluded = next(
                value
                for value in universe.entries
                if value.disposition
                is PromotedIdentitySessionDisposition.EXCLUDED_NON_EQUITY
            )
            bar = _bar(
                excluded,
                market_session=universe.market_session,
                label="excluded",
                isin=excluded.validated_isin or "INE999A01019",
            )
            index, partition = _corpus(
                market_session=universe.market_session,
                bars=(bar,),
            )
            frame = PromotedSessionMarketDataFrameService().materialize(
                universe=universe,
                corpus_index=index,
                partition=partition,
                cutoff=FRAME_CUTOFF,
            )
            result = next(
                value
                for value in frame.entries
                if value.source_record_id == excluded.source_record_id
            )
            self.assertIs(
                result.status,
                PromotedSessionBarStatus.EXCLUDED_SOURCE_BAR_OBSERVED,
            )
            self.assertFalse(result.stable_identity_bound)


class PromotedSessionMarketDataRejectionTests(unittest.TestCase):
    def test_cutoff_before_corpus_build_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            universe, index, partition, _ = _happy_frame(Path(tmp))
            with self.assertRaises(PromotedSessionMarketDataError):
                PromotedSessionMarketDataFrameService().materialize(
                    universe=universe,
                    corpus_index=index,
                    partition=partition,
                    cutoff=BUILT_AT - timedelta(microseconds=1),
                )

    def test_partition_not_bound_by_exact_index_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            universe, _, partition, _ = _happy_frame(Path(tmp))
            first = partition.bars[0]
            alternate_bar = dataclasses.replace(
                first,
                close=Decimal("102.00"),
            )
            _, alternate_partition = _corpus(
                market_session=universe.market_session,
                bars=(alternate_bar,),
            )
            index, _ = _corpus(
                market_session=universe.market_session,
                bars=partition.bars,
                index_partition=alternate_partition,
            )
            with self.assertRaises(PromotedSessionMarketDataError):
                PromotedSessionMarketDataFrameService().materialize(
                    universe=universe,
                    corpus_index=index,
                    partition=partition,
                    cutoff=FRAME_CUTOFF,
                )

    def test_bar_observed_after_corpus_build_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            _, _, _, _, universe = happy_path_fixture(Path(tmp))
            entry = next(
                value for value in universe.entries if value.symbol == "RELIANCE"
            )
            bar = _bar(
                entry,
                market_session=universe.market_session,
                label="future-observation",
                observed_at=BUILT_AT + timedelta(seconds=1),
            )
            index, partition = _corpus(
                market_session=universe.market_session,
                bars=(bar,),
            )
            with self.assertRaises(PromotedSessionMarketDataError):
                PromotedSessionMarketDataFrameService().materialize(
                    universe=universe,
                    corpus_index=index,
                    partition=partition,
                    cutoff=FRAME_CUTOFF,
                )

    def test_naive_cutoff_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            universe, index, partition, _ = _happy_frame(Path(tmp))
            with self.assertRaises(PromotedSessionMarketDataError):
                PromotedSessionMarketDataFrameService().materialize(
                    universe=universe,
                    corpus_index=index,
                    partition=partition,
                    cutoff=FRAME_CUTOFF.replace(tzinfo=None),
                )


class PromotedSessionMarketDataReplayTests(unittest.TestCase):
    def test_direct_construction_with_replaced_frame_id_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            _, _, _, frame = _happy_frame(Path(tmp))
            values = {
                field.name: getattr(frame, field.name)
                for field in dataclasses.fields(frame)
            }
            values["frame_id"] = "0" * 64
            with self.assertRaises(PromotedSessionMarketDataError):
                VerifiedPromotedSessionMarketDataFrame(**values)

    def test_nested_entry_mutation_is_rejected_on_replay(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            _, _, _, frame = _happy_frame(Path(tmp))
            object.__setattr__(
                frame.entries[0],
                "stable_identity_bound",
                not frame.entries[0].stable_identity_bound,
            )
            with self.assertRaises(PromotedSessionMarketDataError):
                frame.verify_content_identity()

    def test_orphan_changes_frame_content_id(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_a, tempfile.TemporaryDirectory() as tmp_b:
            _, _, _, frame_with_orphan = _happy_frame(Path(tmp_a))
            _, _, _, _, universe = happy_path_fixture(Path(tmp_b))
            entry = next(
                value for value in universe.entries if value.symbol == "RELIANCE"
            )
            index, partition = _corpus(
                market_session=universe.market_session,
                bars=(
                    _bar(
                        entry,
                        market_session=universe.market_session,
                        label="reliance-no-orphan",
                    ),
                ),
            )
            frame_without_orphan = PromotedSessionMarketDataFrameService().materialize(
                universe=universe,
                corpus_index=index,
                partition=partition,
                cutoff=FRAME_CUTOFF,
            )
            self.assertNotEqual(
                frame_with_orphan.frame_id,
                frame_without_orphan.frame_id,
            )


if __name__ == "__main__":
    unittest.main()
