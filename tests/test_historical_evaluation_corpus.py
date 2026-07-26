from __future__ import annotations

import json
import os
import tempfile
import unittest
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock, patch

from india_swing.evaluation import (
    EvaluationDatasetAssemblyError,
    HistoricalCorpusAdapterError,
    assemble_evaluation_dataset,
    point_in_time_price_sessions_from_historical_corpus,
)
from india_swing.market_data.backfill import (
    HistoricalBackfillCompletion,
    HistoricalBackfillPlan,
    HistoricalBackfillProgress,
)
from india_swing.market_data.collection import (
    HistoricalReconciliationCollector,
    historical_dataset_name,
)
from india_swing.market_data.dataset_admission import (
    LocalHistoricalDatasetAdmissionReportStore,
    build_historical_dataset_admission_report,
)
from india_swing.market_data.historical_corpus import (
    HISTORICAL_EVALUATION_CORPUS_DATASET,
    INDEX_FILENAME,
    MAXIMUM_BARS_PER_SESSION,
    MAXIMUM_CORPUS_INDEX_BYTES,
    MAXIMUM_CORPUS_PARTITION_BYTES,
    MAXIMUM_SESSIONS_PER_CORPUS,
    PARTITIONS_DIRNAME,
    HistoricalEvaluationCorpusBar,
    HistoricalEvaluationCorpusError,
    HistoricalEvaluationCorpusIndex,
    HistoricalEvaluationCorpusIntegrityError,
    HistoricalEvaluationCorpusService,
    HistoricalEvaluationCorpusSessionPartition,
    LocalHistoricalEvaluationCorpusStore,
    decode_historical_evaluation_corpus_index,
    decode_historical_evaluation_corpus_partition,
    encode_historical_evaluation_corpus_index,
    encode_historical_evaluation_corpus_partition,
)
from india_swing.market_data.models import (
    HistoricalDailyRequest,
    HistoricalInstrumentBinding,
)
from india_swing.market_data.reconciliation import (
    HISTORICAL_RECONCILIATION_DATASET,
    HistoricalCandleReconciliationReport,
    HistoricalCandleReconciliationRow,
    HistoricalReconciliationStatus,
)
from india_swing.market_data.reconciliation_run import (
    HistoricalReconciliationIndex,
    HistoricalReconciliationIndexEntry,
    LocalHistoricalReconciliationIndexStore,
)
from india_swing.market_data.snapshot_store import (
    LocalMarketSnapshotStore,
    StoredMarketSnapshot,
)
from india_swing.market_data.upstox import UPSTOX_PROVIDER
from tests.test_evaluation_dataset_assembly import (
    calendar as assembly_calendar,
    tick_size as assembly_tick_size,
    universe as assembly_universe,
)
from tests.test_upstox_market_data import (
    FakeTransport,
    adapter as upstox_adapter,
    candle_row,
    response,
    success_body,
)


UTC = timezone.utc
SESSION_ONE = date(2026, 7, 14)
SESSION_TWO = date(2026, 7, 15)
REQUESTED_AT = datetime(2026, 7, 20, 8, 0, tzinfo=UTC)
RUN_CLOCK = datetime(2026, 7, 21, 9, 0, tzinfo=UTC)
RECONCILED_AT = datetime(2026, 7, 22, 10, 0, tzinfo=UTC)
ASSESSED_AT = datetime(2026, 7, 23, 10, 0, tzinfo=UTC)
BUILT_AT = datetime(2026, 7, 24, 9, 0, tzinfo=UTC)
CALENDAR_SNAPSHOT_ID = "3" * 64
NSE_ARTIFACT_ID = "9" * 64


# --- lightweight, hand-constructed fixtures ---------------------------------


def _binding(
    *,
    provider_instrument_id: str,
    listing_key: str,
    isin: str,
    series: str = "EQ",
    source_snapshot_ids: tuple[str, ...] = ("a" * 64,),
) -> HistoricalInstrumentBinding:
    return HistoricalInstrumentBinding(
        provider=UPSTOX_PROVIDER,
        provider_instrument_id=provider_instrument_id,
        exchange="NSE",
        listing_key=listing_key,
        security_series=series,
        isin=isin,
        valid_from=date(2020, 1, 1),
        valid_through=date(2026, 12, 31),
        source_snapshot_ids=source_snapshot_ids,
    )


RELIANCE_BINDING = _binding(
    provider_instrument_id="NSE_EQ|INE002A01018",
    listing_key="NSE:RELIANCE",
    isin="INE002A01018",
)
INFY_BINDING = _binding(
    provider_instrument_id="NSE_EQ|INE009A01021",
    listing_key="NSE:INFY",
    isin="INE009A01021",
)


def _request(
    binding: HistoricalInstrumentBinding,
    sessions: tuple[date, ...] = (SESSION_ONE, SESSION_TWO),
) -> HistoricalDailyRequest:
    return HistoricalDailyRequest(
        binding=binding, sessions=sessions, requested_at=REQUESTED_AT
    )


def _consistent_candle_row(session: date, *, close: float):
    return candle_row(
        session,
        open_value=close - 5.0,
        high=close + 10.0,
        low=close - 10.0,
        close=close,
        volume=100,
    )


def _fetch_batch(request: HistoricalDailyRequest, *, closes=(1610.0, 1620.0)):
    body = success_body(
        [
            _consistent_candle_row(SESSION_TWO, close=closes[1]),
            _consistent_candle_row(SESSION_ONE, close=closes[0]),
        ]
    )
    return upstox_adapter(FakeTransport(response(body))).fetch_historical_daily(request)


def _store_batch(snapshot_store: LocalMarketSnapshotStore, batch) -> StoredMarketSnapshot:
    return snapshot_store.put(
        dataset=historical_dataset_name(UPSTOX_PROVIDER),
        selection_key=batch.request.request_id,
        provider=UPSTOX_PROVIDER,
        provider_version=batch.provider_version,
        observed_at=batch.observed_at,
        normalized_payload=batch,
    )


def _match_report(batch, *, artifact_id: str) -> HistoricalCandleReconciliationReport:
    binding = batch.request.binding
    rows = tuple(
        HistoricalCandleReconciliationRow(
            session=candle.session,
            nse_artifact_id=artifact_id,
            nse_bar_id=f"{candle.session.toordinal():064x}",
            status=HistoricalReconciliationStatus.MATCH,
            differences=(),
        )
        for candle in batch.candles
    )
    return HistoricalCandleReconciliationReport(
        historical_batch_id=batch.batch_id,
        historical_request_id=batch.request.request_id,
        provider=UPSTOX_PROVIDER,
        provider_version=batch.provider_version,
        listing_key=binding.listing_key,
        security_series=binding.security_series,
        isin=binding.isin,
        reconciled_at=RECONCILED_AT,
        rows=rows,
        passed=True,
    )


def _plan(
    requests, *, identity_registry_id: str = "2" * 64
) -> HistoricalBackfillPlan:
    ordered = tuple(
        sorted(
            requests,
            key=lambda value: (
                value.sessions[0],
                value.binding.listing_key,
                value.binding.security_series,
                value.binding.isin,
                value.binding.provider_instrument_id,
                value.request_id,
            ),
        )
    )
    return HistoricalBackfillPlan(
        provider=UPSTOX_PROVIDER,
        resolver_version="test-resolver/v1",
        identity_registry_id=identity_registry_id,
        calendar_snapshot_id=CALENDAR_SNAPSHOT_ID,
        coverage_start=SESSION_ONE,
        coverage_end=SESSION_TWO,
        requested_at=REQUESTED_AT,
        requests=ordered,
        issues=(),
    )


def build_two_symbol_fixture(root: Path, *, third_request: bool = False) -> dict:
    """Two ADMITTED symbol lanes across two sessions.

    With ``third_request=True`` a plan request is added that is never
    completed: it must be preserved as a blocked entry in the corpus index's
    accounting without ever contributing a bar.
    """

    snapshot_store = LocalMarketSnapshotStore(root / "market")
    admission_store = LocalHistoricalDatasetAdmissionReportStore(root / "market")
    reconciliation_index_store = LocalHistoricalReconciliationIndexStore(
        root / "market"
    )
    corpus_store = LocalHistoricalEvaluationCorpusStore(root / "market")

    reliance_request = _request(RELIANCE_BINDING)
    infy_request = _request(INFY_BINDING)
    requests = [reliance_request, infy_request]
    if third_request:
        third_binding = _binding(
            provider_instrument_id="NSE_EQ|INE040A01034",
            listing_key="NSE:HDFCBANK",
            isin="INE040A01034",
        )
        requests.append(_request(third_binding))
    plan = _plan(requests)

    reliance_batch = _fetch_batch(reliance_request, closes=(1610.0, 1620.0))
    infy_batch = _fetch_batch(infy_request, closes=(1500.0, 1510.0))
    reliance_stored = _store_batch(snapshot_store, reliance_batch)
    infy_stored = _store_batch(snapshot_store, infy_batch)

    completions = [
        HistoricalBackfillCompletion(
            request_id=reliance_request.request_id,
            snapshot_id=reliance_stored.manifest.snapshot_id,
            completed_at=RUN_CLOCK,
            recovered_existing=False,
        ),
        HistoricalBackfillCompletion(
            request_id=infy_request.request_id,
            snapshot_id=infy_stored.manifest.snapshot_id,
            completed_at=RUN_CLOCK,
            recovered_existing=False,
        ),
    ]
    progress = HistoricalBackfillProgress(
        plan_id=plan.plan_id,
        provider=plan.provider,
        connector_version=reliance_batch.provider_version,
        completions=tuple(sorted(completions, key=lambda value: value.request_id)),
        updated_at=RUN_CLOCK,
    )

    reliance_report = _match_report(reliance_batch, artifact_id="c" * 64)
    infy_report = _match_report(infy_batch, artifact_id="d" * 64)
    reliance_reconciliation = HistoricalReconciliationCollector(snapshot_store).collect(
        reliance_report
    )
    infy_reconciliation = HistoricalReconciliationCollector(snapshot_store).collect(
        infy_report
    )

    admission = build_historical_dataset_admission_report(
        plan=plan,
        progress=progress,
        snapshots=(reliance_stored, infy_stored),
        reconciliations=(reliance_report, infy_report),
        gaps=(),
        gap_adjudication=None,
        assessed_at=ASSESSED_AT,
    )
    stored_admission = admission_store.put(admission)

    index_entries = sorted(
        (
            HistoricalReconciliationIndexEntry(
                request_id=reliance_request.request_id,
                provider_snapshot_id=reliance_stored.manifest.snapshot_id,
                historical_batch_id=reliance_batch.batch_id,
                reconciliation_report_id=reliance_report.report_id,
                reconciliation_snapshot_id=reliance_reconciliation.manifest.snapshot_id,
                reconciled_at=RECONCILED_AT,
                passed=True,
            ),
            HistoricalReconciliationIndexEntry(
                request_id=infy_request.request_id,
                provider_snapshot_id=infy_stored.manifest.snapshot_id,
                historical_batch_id=infy_batch.batch_id,
                reconciliation_report_id=infy_report.report_id,
                reconciliation_snapshot_id=infy_reconciliation.manifest.snapshot_id,
                reconciled_at=RECONCILED_AT,
                passed=True,
            ),
        ),
        key=lambda value: value.request_id,
    )
    reconciliation_index = HistoricalReconciliationIndex(
        plan_id=plan.plan_id,
        progress_id=progress.progress_id,
        provider=UPSTOX_PROVIDER,
        connector_version=progress.connector_version,
        nse_artifact_ids=(NSE_ARTIFACT_ID,),
        prior_index_id=None,
        entries=tuple(index_entries),
        total_completion_count=len(completions),
        updated_at=RECONCILED_AT,
        complete=True,
    )
    stored_index = reconciliation_index_store.put(reconciliation_index)

    return {
        "plan": plan,
        "progress": progress,
        "requests": {"RELIANCE": reliance_request, "INFY": infy_request},
        "batches": {"RELIANCE": reliance_batch, "INFY": infy_batch},
        "stored_snapshots": {"RELIANCE": reliance_stored, "INFY": infy_stored},
        "reports": {"RELIANCE": reliance_report, "INFY": infy_report},
        "reconciliation_snapshots": {
            "RELIANCE": reliance_reconciliation,
            "INFY": infy_reconciliation,
        },
        "admission_report": stored_admission,
        "reconciliation_index": stored_index,
        "admission_store": admission_store,
        "reconciliation_index_store": reconciliation_index_store,
        "snapshot_store": snapshot_store,
        "corpus_store": corpus_store,
    }


def build_service(fixture: dict) -> HistoricalEvaluationCorpusService:
    return HistoricalEvaluationCorpusService(
        admission_store=fixture["admission_store"],
        reconciliation_index_store=fixture["reconciliation_index_store"],
        snapshot_store=fixture["snapshot_store"],
        corpus_store=fixture["corpus_store"],
    )


class _ProxySnapshotStore:
    """Delegates to a real store; injects a forged result for one exact ID."""

    def __init__(self, real_store, forged_by_id: dict[str, StoredMarketSnapshot]):
        self._real_store = real_store
        self._forged_by_id = forged_by_id

    def get(self, dataset: str, snapshot_id: str) -> StoredMarketSnapshot:
        forged = self._forged_by_id.get(snapshot_id)
        if forged is not None:
            return forged
        return self._real_store.get(dataset, snapshot_id)


class _RaisingProxySnapshotStore:
    """Delegates to a real store; raises a hostile exception for one exact (dataset, id).

    Models a genuinely untrusted injected store: the raised exception's text is
    attacker-controlled and must never surface through the service's own
    static sanitized error.
    """

    def __init__(self, real_store, raise_for: tuple[str, str], exception: Exception):
        self._real_store = real_store
        self._raise_for = raise_for
        self._exception = exception

    def get(self, dataset: str, snapshot_id: str) -> StoredMarketSnapshot:
        if (dataset, snapshot_id) == self._raise_for:
            raise self._exception
        return self._real_store.get(dataset, snapshot_id)


def _fabricated_bar(
    *,
    session: date = SESSION_ONE,
    listing_key: str = "NSE:RELIANCE",
    series: str = "EQ",
    isin: str = "INE002A01018",
    close: Decimal = Decimal("1610"),
    volume: int = 100,
    request_id: str = "1" * 64,
    binding_id: str = "2" * 64,
    provider_snapshot_id: str = "3" * 64,
    historical_batch_id: str = "4" * 64,
    reconciliation_report_id: str = "5" * 64,
    reconciliation_snapshot_id: str = "6" * 64,
    observed_at: datetime = RECONCILED_AT,
) -> HistoricalEvaluationCorpusBar:
    return HistoricalEvaluationCorpusBar(
        session=session,
        listing_key=listing_key,
        series=series,
        isin=isin,
        open=Decimal("1600"),
        high=Decimal("1620"),
        low=Decimal("1590"),
        close=close,
        volume=volume,
        provider=UPSTOX_PROVIDER,
        request_id=request_id,
        binding_id=binding_id,
        provider_snapshot_id=provider_snapshot_id,
        historical_batch_id=historical_batch_id,
        reconciliation_report_id=reconciliation_report_id,
        reconciliation_snapshot_id=reconciliation_snapshot_id,
        observed_at=observed_at,
    )


def _fabricated_partition(
    bars, *, session: date = SESSION_ONE
) -> HistoricalEvaluationCorpusSessionPartition:
    sorted_bars = tuple(sorted(bars, key=lambda value: value.listing_lane))
    return HistoricalEvaluationCorpusSessionPartition(
        market_session=session,
        bars=sorted_bars,
        source_snapshot_ids=tuple(
            sorted(
                {
                    *(bar.provider_snapshot_id for bar in sorted_bars),
                    *(bar.reconciliation_snapshot_id for bar in sorted_bars),
                }
            )
        ),
        source_report_ids=tuple(
            sorted({bar.reconciliation_report_id for bar in sorted_bars})
        ),
    )


def _fabricated_index(
    partitions,
    *,
    admission_report_id: str = "7" * 64,
    reconciliation_index_id: str = "8" * 64,
    plan_id: str = "9" * 64,
    progress_id: str = "a" * 64,
    all_entry_ids=("b" * 64,),
    admitted_entry_ids=("b" * 64,),
    blocked_entry_ids=(),
    disposition_counts=(("ADMITTED", 1),),
    safe_requests_complete: bool = True,
    coverage_complete: bool = True,
    built_at: datetime = BUILT_AT,
) -> HistoricalEvaluationCorpusIndex:
    return HistoricalEvaluationCorpusIndex(
        admission_report_id=admission_report_id,
        reconciliation_index_id=reconciliation_index_id,
        plan_id=plan_id,
        progress_id=progress_id,
        provider=UPSTOX_PROVIDER,
        connector_version="test-connector/v1",
        assessed_at=ASSESSED_AT,
        built_at=built_at,
        partition_ids=tuple(value.partition_id for value in partitions),
        partition_sessions=tuple(value.market_session for value in partitions),
        all_entry_ids=all_entry_ids,
        admitted_entry_ids=admitted_entry_ids,
        blocked_entry_ids=blocked_entry_ids,
        disposition_counts=disposition_counts,
        safe_requests_complete=safe_requests_complete,
        coverage_complete=coverage_complete,
    )


# --- model validation --------------------------------------------------------


class CorpusBarModelTests(unittest.TestCase):
    def test_valid_bar_round_trips_its_own_identity(self) -> None:
        bar = _fabricated_bar()
        bar.verify_content_identity()
        self.assertEqual(bar.listing_lane, ("NSE:RELIANCE", "EQ"))

    def test_float_ohlc_is_rejected(self) -> None:
        with self.assertRaises(ValueError):
            _fabricated_bar(close=1610.0)  # type: ignore[arg-type]

    def test_bool_as_int_volume_is_rejected(self) -> None:
        with self.assertRaises(ValueError):
            _fabricated_bar(volume=True)  # type: ignore[arg-type]

    def test_invalid_ohlc_ordering_is_rejected(self) -> None:
        with self.assertRaises(ValueError):
            HistoricalEvaluationCorpusBar(
                session=SESSION_ONE,
                listing_key="NSE:RELIANCE",
                series="EQ",
                isin="INE002A01018",
                open=Decimal("1600"),
                high=Decimal("1500"),
                low=Decimal("1590"),
                close=Decimal("1610"),
                volume=100,
                provider=UPSTOX_PROVIDER,
                request_id="1" * 64,
                binding_id="2" * 64,
                provider_snapshot_id="3" * 64,
                historical_batch_id="4" * 64,
                reconciliation_report_id="5" * 64,
                reconciliation_snapshot_id="6" * 64,
                observed_at=RECONCILED_AT,
            )

    def test_invalid_listing_key_is_rejected(self) -> None:
        with self.assertRaises(ValueError):
            _fabricated_bar(listing_key="BSE:RELIANCE")

    def test_invalid_isin_is_rejected(self) -> None:
        with self.assertRaises(ValueError):
            _fabricated_bar(isin="NOTANISIN")

    def test_malformed_evidence_ids_are_rejected(self) -> None:
        for field_name in (
            "request_id",
            "binding_id",
            "provider_snapshot_id",
            "historical_batch_id",
            "reconciliation_report_id",
            "reconciliation_snapshot_id",
        ):
            with self.subTest(field=field_name):
                with self.assertRaises(ValueError):
                    _fabricated_bar(**{field_name: "not-a-sha256"})

    def test_future_session_relative_to_observation_is_rejected(self) -> None:
        with self.assertRaises(ValueError):
            _fabricated_bar(
                session=date(2030, 1, 1),
                observed_at=RECONCILED_AT,
            )

    def test_naive_observed_at_is_rejected(self) -> None:
        with self.assertRaises(ValueError):
            _fabricated_bar(observed_at=datetime(2026, 7, 22, 10, 0))


class CorpusSessionPartitionModelTests(unittest.TestCase):
    def test_valid_two_lane_partition_round_trips(self) -> None:
        first = _fabricated_bar(listing_key="NSE:RELIANCE")
        second = _fabricated_bar(
            listing_key="NSE:INFY",
            isin="INE009A01021",
            request_id="a" * 64,
            binding_id="b" * 64,
            provider_snapshot_id="c" * 64,
            reconciliation_snapshot_id="d" * 64,
        )
        partition = _fabricated_partition((first, second))
        partition.verify_content_identity()
        self.assertEqual(len(partition.bars), 2)
        self.assertEqual(
            partition.bars, tuple(sorted((first, second), key=lambda v: v.listing_lane))
        )

    def test_empty_bars_is_rejected(self) -> None:
        with self.assertRaises(TypeError):
            HistoricalEvaluationCorpusSessionPartition(
                market_session=SESSION_ONE,
                bars=(),
                source_snapshot_ids=(),
                source_report_ids=(),
            )

    def test_bar_from_another_session_is_rejected(self) -> None:
        bar = _fabricated_bar(session=SESSION_TWO)
        with self.assertRaises(ValueError):
            _fabricated_partition((bar,), session=SESSION_ONE)

    def test_duplicate_symbol_series_lane_is_rejected(self) -> None:
        first = _fabricated_bar(request_id="1" * 64, provider_snapshot_id="3" * 64)
        second = _fabricated_bar(
            request_id="2" * 64,
            binding_id="9" * 64,
            provider_snapshot_id="8" * 64,
            reconciliation_snapshot_id="7" * 64,
        )
        with self.assertRaises(ValueError):
            _fabricated_partition((first, second))

    def test_duplicate_binding_is_rejected(self) -> None:
        first = _fabricated_bar(
            listing_key="NSE:RELIANCE", request_id="1" * 64, binding_id="5" * 64
        )
        second = _fabricated_bar(
            listing_key="NSE:INFY",
            isin="INE009A01021",
            request_id="2" * 64,
            binding_id="5" * 64,
            provider_snapshot_id="8" * 64,
            reconciliation_snapshot_id="7" * 64,
        )
        with self.assertRaises(ValueError):
            _fabricated_partition((first, second))

    def test_duplicate_request_is_rejected(self) -> None:
        first = _fabricated_bar(
            listing_key="NSE:RELIANCE", request_id="1" * 64, binding_id="2" * 64
        )
        second = _fabricated_bar(
            listing_key="NSE:INFY",
            isin="INE009A01021",
            request_id="1" * 64,
            binding_id="9" * 64,
            provider_snapshot_id="8" * 64,
            reconciliation_snapshot_id="7" * 64,
        )
        with self.assertRaises(ValueError):
            _fabricated_partition((first, second))

    def test_overlapping_provider_snapshot_evidence_is_rejected(self) -> None:
        first = _fabricated_bar(
            listing_key="NSE:RELIANCE",
            request_id="1" * 64,
            binding_id="2" * 64,
            provider_snapshot_id="6" * 64,
        )
        second = _fabricated_bar(
            listing_key="NSE:INFY",
            isin="INE009A01021",
            request_id="2" * 64,
            binding_id="9" * 64,
            provider_snapshot_id="6" * 64,
            reconciliation_snapshot_id="7" * 64,
        )
        with self.assertRaises(ValueError):
            _fabricated_partition((first, second))

    def test_fixed_safety_flags_are_enforced(self) -> None:
        bar = _fabricated_bar()
        with self.assertRaises(ValueError):
            HistoricalEvaluationCorpusSessionPartition(
                market_session=SESSION_ONE,
                bars=(bar,),
                source_snapshot_ids=tuple(
                    sorted({bar.provider_snapshot_id, bar.reconciliation_snapshot_id})
                ),
                source_report_ids=(bar.reconciliation_report_id,),
                actionable=True,
            )

    def test_maximum_bars_per_session_exact_at_limit_and_plus_one(self) -> None:
        with patch(
            "india_swing.market_data.historical_corpus.MAXIMUM_BARS_PER_SESSION", 2
        ):
            bars_at_limit = tuple(
                _fabricated_bar(
                    listing_key=f"NSE:SYM{i}",
                    request_id=f"{i + 1:064x}",
                    binding_id=f"{i + 11:064x}",
                    provider_snapshot_id=f"{i + 21:064x}",
                    reconciliation_snapshot_id=f"{i + 31:064x}",
                )
                for i in range(2)
            )
            _fabricated_partition(bars_at_limit)

            bars_over_limit = tuple(
                _fabricated_bar(
                    listing_key=f"NSE:SYM{i}",
                    request_id=f"{i + 1:064x}",
                    binding_id=f"{i + 11:064x}",
                    provider_snapshot_id=f"{i + 21:064x}",
                    reconciliation_snapshot_id=f"{i + 31:064x}",
                )
                for i in range(3)
            )
            with self.assertRaises(TypeError):
                _fabricated_partition(bars_over_limit)


class CorpusIndexModelTests(unittest.TestCase):
    def test_valid_index_round_trips(self) -> None:
        partition = _fabricated_partition((_fabricated_bar(),))
        index = _fabricated_index((partition,))
        index.verify_content_identity()

    def test_admitted_and_blocked_must_be_disjoint(self) -> None:
        partition = _fabricated_partition((_fabricated_bar(),))
        with self.assertRaises(ValueError):
            _fabricated_index(
                (partition,),
                all_entry_ids=("b" * 64,),
                admitted_entry_ids=("b" * 64,),
                blocked_entry_ids=("b" * 64,),
                disposition_counts=(("ADMITTED", 1),),
            )

    def test_admitted_and_blocked_must_exhaust_all_entries(self) -> None:
        partition = _fabricated_partition((_fabricated_bar(),))
        with self.assertRaises(ValueError):
            _fabricated_index(
                (partition,),
                all_entry_ids=("b" * 64, "c" * 64),
                admitted_entry_ids=("b" * 64,),
                blocked_entry_ids=(),
                disposition_counts=(("ADMITTED", 1),),
            )

    def test_disposition_counts_must_agree_with_admitted_count(self) -> None:
        partition = _fabricated_partition((_fabricated_bar(),))
        with self.assertRaises(ValueError):
            _fabricated_index(
                (partition,),
                disposition_counts=(("ADMITTED", 2),),
            )

    def test_coverage_complete_requires_safe_requests_complete(self) -> None:
        partition = _fabricated_partition((_fabricated_bar(),))
        with self.assertRaises(ValueError):
            _fabricated_index(
                (partition,), safe_requests_complete=False, coverage_complete=True
            )

    def test_fixed_safety_flags_are_enforced(self) -> None:
        partition = _fabricated_partition((_fabricated_bar(),))
        with self.assertRaises(ValueError):
            HistoricalEvaluationCorpusIndex(
                admission_report_id="7" * 64,
                reconciliation_index_id="8" * 64,
                plan_id="9" * 64,
                progress_id="a" * 64,
                provider=UPSTOX_PROVIDER,
                connector_version="test-connector/v1",
                assessed_at=ASSESSED_AT,
                built_at=BUILT_AT,
                partition_ids=(partition.partition_id,),
                partition_sessions=(partition.market_session,),
                all_entry_ids=("b" * 64,),
                admitted_entry_ids=("b" * 64,),
                blocked_entry_ids=(),
                disposition_counts=(("ADMITTED", 1),),
                safe_requests_complete=True,
                coverage_complete=True,
                actionable=True,
            )

    def test_maximum_sessions_per_corpus_exact_at_limit_and_plus_one(self) -> None:
        with patch(
            "india_swing.market_data.historical_corpus.MAXIMUM_SESSIONS_PER_CORPUS", 1
        ):
            one_partition = (_fabricated_partition((_fabricated_bar(),)),)
            _fabricated_index(one_partition)

            two_partitions = (
                _fabricated_partition((_fabricated_bar(),), session=SESSION_ONE),
                _fabricated_partition(
                    (
                        _fabricated_bar(
                            session=SESSION_TWO,
                            request_id="c" * 64,
                            binding_id="d" * 64,
                            provider_snapshot_id="e" * 64,
                            reconciliation_snapshot_id="f" * 64,
                        ),
                    ),
                    session=SESSION_TWO,
                ),
            )
            with self.assertRaises(ValueError):
                _fabricated_index(two_partitions)


# --- canonical codec ---------------------------------------------------------


class CorpusCodecTests(unittest.TestCase):
    def test_partition_and_index_round_trip(self) -> None:
        partition = _fabricated_partition((_fabricated_bar(),))
        index = _fabricated_index((partition,))

        partition_payload = encode_historical_evaluation_corpus_partition(partition)
        self.assertEqual(
            decode_historical_evaluation_corpus_partition(partition_payload), partition
        )
        self.assertEqual(
            encode_historical_evaluation_corpus_partition(
                decode_historical_evaluation_corpus_partition(partition_payload)
            ),
            partition_payload,
        )

        index_payload = encode_historical_evaluation_corpus_index(index)
        self.assertEqual(decode_historical_evaluation_corpus_index(index_payload), index)
        self.assertEqual(
            encode_historical_evaluation_corpus_index(
                decode_historical_evaluation_corpus_index(index_payload)
            ),
            index_payload,
        )

    def test_duplicate_json_keys_are_rejected(self) -> None:
        partition = _fabricated_partition((_fabricated_bar(),))
        payload = encode_historical_evaluation_corpus_partition(partition)
        original = json.loads(payload)
        pairs = ",".join(
            f"{json.dumps(key)}:{json.dumps(value)}" for key, value in original.items()
        )
        duplicated = (
            "{" + pairs + f',"partition_id":{json.dumps(original["partition_id"])}' + "}"
        ).encode("utf-8")
        with self.assertRaises(HistoricalEvaluationCorpusIntegrityError):
            decode_historical_evaluation_corpus_partition(duplicated)

        index = _fabricated_index((partition,))
        index_payload = encode_historical_evaluation_corpus_index(index)
        index_original = json.loads(index_payload)
        index_pairs = ",".join(
            f"{json.dumps(key)}:{json.dumps(value)}"
            for key, value in index_original.items()
        )
        index_duplicated = (
            "{" + index_pairs + f',"corpus_id":{json.dumps(index_original["corpus_id"])}' + "}"
        ).encode("utf-8")
        with self.assertRaises(HistoricalEvaluationCorpusIntegrityError):
            decode_historical_evaluation_corpus_index(index_duplicated)

    def test_float_and_nonfinite_tokens_are_rejected(self) -> None:
        partition = _fabricated_partition((_fabricated_bar(),))
        payload = encode_historical_evaluation_corpus_partition(partition)
        text = payload.decode("utf-8")
        self.assertIn('"actionable":false', text)
        for replacement in ('"actionable":0.5', '"actionable":NaN', '"actionable":Infinity'):
            with self.subTest(replacement=replacement):
                corrupted = text.replace('"actionable":false', replacement).encode("utf-8")
                with self.assertRaises(HistoricalEvaluationCorpusIntegrityError):
                    decode_historical_evaluation_corpus_partition(corrupted)

    def test_missing_and_unknown_keys_are_rejected(self) -> None:
        partition = _fabricated_partition((_fabricated_bar(),))
        base = json.loads(encode_historical_evaluation_corpus_partition(partition))

        missing = dict(base)
        del missing["market_session"]
        with self.assertRaises(HistoricalEvaluationCorpusIntegrityError):
            decode_historical_evaluation_corpus_partition(
                json.dumps(missing).encode("utf-8")
            )

        extra = dict(base)
        extra["unexpected"] = "x"
        with self.assertRaises(HistoricalEvaluationCorpusIntegrityError):
            decode_historical_evaluation_corpus_partition(
                json.dumps(extra).encode("utf-8")
            )

    def test_stale_claimed_id_is_rejected(self) -> None:
        partition = _fabricated_partition((_fabricated_bar(),))
        original = json.loads(encode_historical_evaluation_corpus_partition(partition))
        original["partition_id"] = "0" * 64
        with self.assertRaises(HistoricalEvaluationCorpusIntegrityError):
            decode_historical_evaluation_corpus_partition(
                json.dumps(original).encode("utf-8")
            )

    def test_noncanonical_but_equivalent_bytes_are_rejected(self) -> None:
        partition = _fabricated_partition((_fabricated_bar(),))
        canonical = encode_historical_evaluation_corpus_partition(partition)
        value = json.loads(canonical)
        noncanonical = json.dumps(value, indent=2).encode("utf-8")
        self.assertNotEqual(noncanonical, canonical)
        with self.assertRaises(HistoricalEvaluationCorpusIntegrityError):
            decode_historical_evaluation_corpus_partition(noncanonical)

    def test_oversized_partition_and_index_payloads_are_rejected(self) -> None:
        partition = _fabricated_partition((_fabricated_bar(),))
        payload = encode_historical_evaluation_corpus_partition(partition)
        with patch(
            "india_swing.market_data.historical_corpus.MAXIMUM_CORPUS_PARTITION_BYTES",
            len(payload) - 1,
        ):
            with self.assertRaises(HistoricalEvaluationCorpusIntegrityError):
                decode_historical_evaluation_corpus_partition(payload)

        index = _fabricated_index((partition,))
        index_payload = encode_historical_evaluation_corpus_index(index)
        with patch(
            "india_swing.market_data.historical_corpus.MAXIMUM_CORPUS_INDEX_BYTES",
            len(index_payload) - 1,
        ):
            with self.assertRaises(HistoricalEvaluationCorpusIntegrityError):
                decode_historical_evaluation_corpus_index(index_payload)

    def test_non_bytes_and_empty_payloads_are_rejected(self) -> None:
        with self.assertRaises(HistoricalEvaluationCorpusIntegrityError):
            decode_historical_evaluation_corpus_partition("not-bytes")  # type: ignore[arg-type]
        with self.assertRaises(HistoricalEvaluationCorpusIntegrityError):
            decode_historical_evaluation_corpus_partition(b"")


# --- local store --------------------------------------------------------------


class LocalHistoricalEvaluationCorpusStoreTests(unittest.TestCase):
    def test_round_trip_and_idempotent_put(self) -> None:
        partition = _fabricated_partition((_fabricated_bar(),))
        index = _fabricated_index((partition,))
        with tempfile.TemporaryDirectory() as temp_dir:
            store = LocalHistoricalEvaluationCorpusStore(Path(temp_dir))
            stored_first = store.put(index, (partition,))
            stored_second = store.put(index, (partition,))
            loaded_index, loaded_partitions = store.get(index.corpus_id)

        self.assertEqual(stored_first, index)
        self.assertEqual(stored_second, index)
        self.assertEqual(loaded_index, index)
        self.assertEqual(loaded_partitions, (partition,))

    def test_conflicting_content_at_same_path_is_rejected(self) -> None:
        partition = _fabricated_partition((_fabricated_bar(),))
        index = _fabricated_index((partition,))
        with tempfile.TemporaryDirectory() as temp_dir:
            store = LocalHistoricalEvaluationCorpusStore(Path(temp_dir))
            store.put(index, (partition,))
            path = store.dataset_root / index.corpus_id / INDEX_FILENAME
            corrupted = json.loads(path.read_bytes())
            corrupted["assessed_at"] = (
                index.assessed_at + timedelta(seconds=1)
            ).isoformat()
            path.write_text(json.dumps(corrupted, sort_keys=True, separators=(",", ":")) + "\n")

            with self.assertRaises(HistoricalEvaluationCorpusIntegrityError):
                store.put(index, (partition,))
            with self.assertRaises(HistoricalEvaluationCorpusIntegrityError):
                store.get(index.corpus_id)

    def test_not_found_and_invalid_ids_are_distinct(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            store = LocalHistoricalEvaluationCorpusStore(Path(temp_dir))
            with self.assertRaises(ValueError):
                store.get("not-a-hash")
            with self.assertRaises(HistoricalEvaluationCorpusError):
                store.get("a" * 64)

    def test_extra_and_missing_partition_files_are_rejected(self) -> None:
        first = _fabricated_bar()
        second = _fabricated_bar(
            session=SESSION_TWO,
            listing_key="NSE:INFY",
            isin="INE009A01021",
            request_id="c" * 64,
            binding_id="d" * 64,
            provider_snapshot_id="e" * 64,
            reconciliation_snapshot_id="f" * 64,
        )
        partition_one = _fabricated_partition((first,), session=SESSION_ONE)
        partition_two = _fabricated_partition((second,), session=SESSION_TWO)
        index = _fabricated_index(
            (partition_one, partition_two),
            all_entry_ids=("b" * 64, "c" * 64),
            admitted_entry_ids=("b" * 64, "c" * 64),
            disposition_counts=(("ADMITTED", 2),),
        )
        with tempfile.TemporaryDirectory() as temp_dir:
            store = LocalHistoricalEvaluationCorpusStore(Path(temp_dir))
            store.put(index, (partition_one, partition_two))
            partitions_dir = store.dataset_root / index.corpus_id / PARTITIONS_DIRNAME

            extra = partitions_dir / "extra.json"
            extra.write_text("{}", encoding="utf-8")
            with self.assertRaises(HistoricalEvaluationCorpusIntegrityError):
                store.get(index.corpus_id)
            extra.unlink()

            missing = partitions_dir / f"{partition_two.partition_id}.json"
            payload = missing.read_bytes()
            missing.unlink()
            with self.assertRaises(HistoricalEvaluationCorpusIntegrityError):
                store.get(index.corpus_id)
            missing.write_bytes(payload)
            store.get(index.corpus_id)

    def test_unexpected_index_directory_children_are_rejected(self) -> None:
        partition = _fabricated_partition((_fabricated_bar(),))
        index = _fabricated_index((partition,))
        with tempfile.TemporaryDirectory() as temp_dir:
            store = LocalHistoricalEvaluationCorpusStore(Path(temp_dir))
            store.put(index, (partition,))
            (store.dataset_root / index.corpus_id / "unexpected.txt").write_text(
                "x", encoding="utf-8"
            )
            with self.assertRaises(HistoricalEvaluationCorpusIntegrityError):
                store.get(index.corpus_id)

    def test_no_latest_list_find_or_select_operation_exists(self) -> None:
        store = LocalHistoricalEvaluationCorpusStore(Path("unused"))
        for banned in ("latest", "list", "list_all", "find", "select"):
            self.assertFalse(hasattr(store, banned))

    def test_symlink_corpus_directory_is_rejected_when_platform_allows_links(
        self,
    ) -> None:
        partition = _fabricated_partition((_fabricated_bar(),))
        index = _fabricated_index((partition,))
        with tempfile.TemporaryDirectory() as temp_dir:
            store = LocalHistoricalEvaluationCorpusStore(Path(temp_dir))
            store.put(index, (partition,))
            real_dir = store.dataset_root / index.corpus_id
            link_dir = store.dataset_root / "linked"
            try:
                os.symlink(real_dir, link_dir, target_is_directory=True)
            except OSError as exc:
                self.skipTest(f"platform does not permit symlinks: {type(exc).__name__}")

            with self.assertRaises(HistoricalEvaluationCorpusIntegrityError):
                store._read_path(link_dir)

    def test_normal_root_still_supports_idempotent_put_and_get(self) -> None:
        """Control: an ordinary, unlinked root is unaffected by the boundary check."""

        partition = _fabricated_partition((_fabricated_bar(),))
        index = _fabricated_index((partition,))
        with tempfile.TemporaryDirectory() as temp_dir:
            store = LocalHistoricalEvaluationCorpusStore(Path(temp_dir))
            first = store.put(index, (partition,))
            second = store.put(index, (partition,))
            loaded_index, loaded_partitions = store.get(index.corpus_id)
        self.assertEqual(first, index)
        self.assertEqual(second, index)
        self.assertEqual(loaded_index, index)
        self.assertEqual(loaded_partitions, (partition,))

    def test_symlinked_root_is_rejected_before_any_write_or_read(self) -> None:
        partition = _fabricated_partition((_fabricated_bar(),))
        index = _fabricated_index((partition,))
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            real_root = temp_root / "real-store"
            real_root.mkdir()
            linked_root = temp_root / "linked-store"
            try:
                os.symlink(real_root, linked_root, target_is_directory=True)
            except OSError as exc:
                self.skipTest(f"platform does not permit symlinks: {type(exc).__name__}")

            store = LocalHistoricalEvaluationCorpusStore(linked_root)
            with self.assertRaises(HistoricalEvaluationCorpusIntegrityError):
                store.put(index, (partition,))
            self.assertFalse(
                (real_root / HISTORICAL_EVALUATION_CORPUS_DATASET).exists()
            )
            with self.assertRaises(HistoricalEvaluationCorpusIntegrityError):
                store.get(index.corpus_id)

    def test_symlinked_dataset_root_is_rejected_before_any_write_or_read(self) -> None:
        partition = _fabricated_partition((_fabricated_bar(),))
        index = _fabricated_index((partition,))
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            root = temp_root / "root"
            root.mkdir()
            elsewhere = temp_root / "elsewhere"
            elsewhere.mkdir()
            linked_dataset_root = root / HISTORICAL_EVALUATION_CORPUS_DATASET
            try:
                os.symlink(elsewhere, linked_dataset_root, target_is_directory=True)
            except OSError as exc:
                self.skipTest(f"platform does not permit symlinks: {type(exc).__name__}")

            store = LocalHistoricalEvaluationCorpusStore(root)
            with self.assertRaises(HistoricalEvaluationCorpusIntegrityError):
                store.put(index, (partition,))
            # Nothing was ever published through the redirected link.
            self.assertEqual(list(elsewhere.iterdir()), [])
            with self.assertRaises(HistoricalEvaluationCorpusIntegrityError):
                store.get(index.corpus_id)

    def test_windows_junction_dataset_root_is_rejected_when_platform_allows_it(
        self,
    ) -> None:
        import subprocess

        partition = _fabricated_partition((_fabricated_bar(),))
        index = _fabricated_index((partition,))
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            root = temp_root / "root"
            root.mkdir()
            elsewhere = temp_root / "elsewhere-junction-target"
            elsewhere.mkdir()
            linked_dataset_root = root / HISTORICAL_EVALUATION_CORPUS_DATASET
            result = subprocess.run(
                ["cmd", "/c", "mklink", "/J", str(linked_dataset_root), str(elsewhere)],
                capture_output=True,
            )
            if result.returncode != 0:
                self.skipTest("platform does not permit Windows junctions")

            store = LocalHistoricalEvaluationCorpusStore(root)
            with self.assertRaises(HistoricalEvaluationCorpusIntegrityError):
                store.put(index, (partition,))
            self.assertEqual(list(elsewhere.iterdir()), [])
            with self.assertRaises(HistoricalEvaluationCorpusIntegrityError):
                store.get(index.corpus_id)

    def test_atomic_publish_cleans_up_on_failure(self) -> None:
        partition = _fabricated_partition((_fabricated_bar(),))
        index = _fabricated_index((partition,))
        with tempfile.TemporaryDirectory() as temp_dir:
            store = LocalHistoricalEvaluationCorpusStore(Path(temp_dir))
            with patch(
                "india_swing.market_data.historical_corpus.os.replace",
                side_effect=OSError("publish failed"),
            ):
                with self.assertRaisesRegex(OSError, "publish failed"):
                    store.put(index, (partition,))
            leftovers = [
                path
                for path in store.dataset_root.glob("*")
                if path.is_dir() and path.name != index.corpus_id
            ]
        self.assertEqual(leftovers, [])

    def test_partitions_not_matching_index_are_rejected_by_put(self) -> None:
        partition = _fabricated_partition((_fabricated_bar(),), session=SESSION_ONE)
        wrong_session_partition = _fabricated_partition(
            (_fabricated_bar(session=SESSION_TWO),), session=SESSION_TWO
        )
        index = _fabricated_index((partition,))
        with tempfile.TemporaryDirectory() as temp_dir:
            store = LocalHistoricalEvaluationCorpusStore(Path(temp_dir))
            with self.assertRaises(HistoricalEvaluationCorpusError):
                store.put(index, (wrong_session_partition,))


# --- service: build from admission report + reconciliation index ------------


class HistoricalEvaluationCorpusServiceBuildTests(unittest.TestCase):
    def test_two_symbol_two_session_corpus_is_built_and_grouped_cross_sectionally(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            fixture = build_two_symbol_fixture(Path(temp_dir))
            service = build_service(fixture)
            index = service.build(
                admission_report_id=fixture["admission_report"].report_id,
                reconciliation_index_id=fixture["reconciliation_index"].index_id,
                built_at=BUILT_AT,
            )
            loaded_index, partitions = fixture["corpus_store"].get(index.corpus_id)

        self.assertEqual(loaded_index, index)
        self.assertEqual(index.partition_sessions, (SESSION_ONE, SESSION_TWO))
        self.assertEqual(len(partitions), 2)
        self.assertEqual(len(partitions[0].bars), 2)
        self.assertEqual(len(partitions[1].bars), 2)
        self.assertEqual(
            {bar.listing_key for bar in partitions[0].bars}, {"NSE:RELIANCE", "NSE:INFY"}
        )
        self.assertEqual(len(index.all_entry_ids), 2)
        self.assertEqual(len(index.admitted_entry_ids), 2)
        self.assertEqual(index.blocked_entry_ids, ())
        self.assertEqual(dict(index.disposition_counts), {"ADMITTED": 2})
        self.assertTrue(index.safe_requests_complete)
        self.assertTrue(index.coverage_complete)
        self.assertTrue(index.collection_only)
        self.assertFalse(index.actionable)
        self.assertFalse(index.training_eligible)
        self.assertEqual(index.built_at, BUILT_AT)

    def test_build_is_idempotent(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            fixture = build_two_symbol_fixture(Path(temp_dir))
            service = build_service(fixture)
            first = service.build(
                admission_report_id=fixture["admission_report"].report_id,
                reconciliation_index_id=fixture["reconciliation_index"].index_id,
                built_at=BUILT_AT,
            )
            second = service.build(
                admission_report_id=fixture["admission_report"].report_id,
                reconciliation_index_id=fixture["reconciliation_index"].index_id,
                built_at=BUILT_AT,
            )
        self.assertEqual(first, second)

    def test_blocked_entry_is_preserved_without_producing_a_bar(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            fixture = build_two_symbol_fixture(Path(temp_dir), third_request=True)
            service = build_service(fixture)
            index = service.build(
                admission_report_id=fixture["admission_report"].report_id,
                reconciliation_index_id=fixture["reconciliation_index"].index_id,
                built_at=BUILT_AT,
            )
            _loaded_index, partitions = fixture["corpus_store"].get(index.corpus_id)

        self.assertEqual(len(index.all_entry_ids), 3)
        self.assertEqual(len(index.admitted_entry_ids), 2)
        self.assertEqual(len(index.blocked_entry_ids), 1)
        self.assertFalse(index.safe_requests_complete)
        self.assertFalse(index.coverage_complete)
        self.assertEqual(len(partitions), 2)
        for partition in partitions:
            for bar in partition.bars:
                self.assertNotEqual(bar.listing_key, "NSE:HDFCBANK")

    def test_index_report_lineage_mismatch_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            fixture = build_two_symbol_fixture(Path(temp_dir))
            foreign_entry = HistoricalReconciliationIndexEntry(
                request_id="1" * 64,
                provider_snapshot_id="2" * 64,
                historical_batch_id="3" * 64,
                reconciliation_report_id="4" * 64,
                reconciliation_snapshot_id="5" * 64,
                reconciled_at=RECONCILED_AT,
                passed=True,
            )
            foreign_index = HistoricalReconciliationIndex(
                plan_id="9" * 64,
                progress_id="8" * 64,
                provider=UPSTOX_PROVIDER,
                connector_version="foreign-connector/v1",
                nse_artifact_ids=("6" * 64,),
                prior_index_id=None,
                entries=(foreign_entry,),
                total_completion_count=1,
                updated_at=RECONCILED_AT,
                complete=True,
            )
            stored_foreign = fixture["reconciliation_index_store"].put(foreign_index)
            service = build_service(fixture)
            with self.assertRaises(HistoricalEvaluationCorpusError):
                service.build(
                    admission_report_id=fixture["admission_report"].report_id,
                    reconciliation_index_id=stored_foreign.index_id,
                    built_at=BUILT_AT,
                )
            self.assertFalse(
                fixture["corpus_store"].dataset_root.exists()
                and any(fixture["corpus_store"].dataset_root.iterdir())
            )

    def test_admitted_entry_missing_matching_index_evidence_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            fixture = build_two_symbol_fixture(Path(temp_dir))
            report = fixture["admission_report"]
            wrong_entry = HistoricalReconciliationIndexEntry(
                request_id="0" * 64,
                provider_snapshot_id="1" * 64,
                historical_batch_id="2" * 64,
                reconciliation_report_id="3" * 64,
                reconciliation_snapshot_id="4" * 64,
                reconciled_at=RECONCILED_AT,
                passed=True,
            )
            mismatched_index = HistoricalReconciliationIndex(
                plan_id=report.plan_id,
                progress_id=report.progress_id,
                provider=report.provider,
                connector_version=report.connector_version,
                nse_artifact_ids=(NSE_ARTIFACT_ID,),
                prior_index_id=None,
                entries=(wrong_entry,),
                total_completion_count=1,
                updated_at=RECONCILED_AT,
                complete=True,
            )
            stored_mismatched = fixture["reconciliation_index_store"].put(
                mismatched_index
            )
            service = build_service(fixture)
            with self.assertRaises(HistoricalEvaluationCorpusError):
                service.build(
                    admission_report_id=report.report_id,
                    reconciliation_index_id=stored_mismatched.index_id,
                    built_at=BUILT_AT,
                )

    def test_incomplete_reconciliation_index_under_complete_admission_is_rejected(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            fixture = build_two_symbol_fixture(Path(temp_dir))
            report = fixture["admission_report"]
            self.assertTrue(report.coverage_complete)
            index = fixture["reconciliation_index"]
            partial = HistoricalReconciliationIndex(
                plan_id=index.plan_id,
                progress_id=index.progress_id,
                provider=index.provider,
                connector_version=index.connector_version,
                nse_artifact_ids=index.nse_artifact_ids,
                prior_index_id=None,
                entries=index.entries[:1],
                total_completion_count=2,
                updated_at=index.updated_at,
                complete=False,
            )
            stored_partial = fixture["reconciliation_index_store"].put(partial)
            service = build_service(fixture)
            with self.assertRaises(HistoricalEvaluationCorpusError):
                service.build(
                    admission_report_id=report.report_id,
                    reconciliation_index_id=stored_partial.index_id,
                    built_at=BUILT_AT,
                )

    def test_built_at_before_evidence_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            fixture = build_two_symbol_fixture(Path(temp_dir))
            service = build_service(fixture)
            with self.assertRaises(HistoricalEvaluationCorpusError):
                service.build(
                    admission_report_id=fixture["admission_report"].report_id,
                    reconciliation_index_id=fixture["reconciliation_index"].index_id,
                    built_at=RECONCILED_AT - timedelta(days=1),
                )

    def test_malformed_id_arguments_are_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            fixture = build_two_symbol_fixture(Path(temp_dir))
            service = build_service(fixture)
            with self.assertRaises(HistoricalEvaluationCorpusError):
                service.build(
                    admission_report_id="not-a-sha256",
                    reconciliation_index_id=fixture["reconciliation_index"].index_id,
                    built_at=BUILT_AT,
                )
            with self.assertRaises(HistoricalEvaluationCorpusError):
                service.build(
                    admission_report_id=fixture["admission_report"].report_id,
                    reconciliation_index_id="not-a-sha256",
                    built_at=BUILT_AT,
                )


class DuplicateLaneRejectionTests(unittest.TestCase):
    def test_duplicate_session_listing_lane_across_requests_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            snapshot_store = LocalMarketSnapshotStore(root / "market")
            admission_store = LocalHistoricalDatasetAdmissionReportStore(root / "market")
            reconciliation_index_store = LocalHistoricalReconciliationIndexStore(
                root / "market"
            )
            corpus_store = LocalHistoricalEvaluationCorpusStore(root / "market")

            reliance_request_a = _request(RELIANCE_BINDING)
            # A distinct provider routing (different ISIN/provider_instrument_id, so
            # both the plan's own dedup and the fake Upstox adapter accept it) that
            # nevertheless claims the same NSE:RELIANCE/EQ lane -- exactly the
            # provider-identity data-quality collision the corpus must catch.
            duplicate_lane_binding = _binding(
                provider_instrument_id="NSE_EQ|INE040A01034",
                listing_key="NSE:RELIANCE",
                isin="INE040A01034",
                source_snapshot_ids=("b" * 64,),
            )
            reliance_request_b = _request(duplicate_lane_binding)
            plan = _plan([reliance_request_a, reliance_request_b])

            batch_a = _fetch_batch(reliance_request_a, closes=(1610.0, 1620.0))
            batch_b = _fetch_batch(reliance_request_b, closes=(1611.0, 1621.0))
            stored_a = _store_batch(snapshot_store, batch_a)
            stored_b = _store_batch(snapshot_store, batch_b)

            completions = sorted(
                (
                    HistoricalBackfillCompletion(
                        request_id=reliance_request_a.request_id,
                        snapshot_id=stored_a.manifest.snapshot_id,
                        completed_at=RUN_CLOCK,
                        recovered_existing=False,
                    ),
                    HistoricalBackfillCompletion(
                        request_id=reliance_request_b.request_id,
                        snapshot_id=stored_b.manifest.snapshot_id,
                        completed_at=RUN_CLOCK,
                        recovered_existing=False,
                    ),
                ),
                key=lambda value: value.request_id,
            )
            progress = HistoricalBackfillProgress(
                plan_id=plan.plan_id,
                provider=plan.provider,
                connector_version=batch_a.provider_version,
                completions=tuple(completions),
                updated_at=RUN_CLOCK,
            )

            report_a = _match_report(batch_a, artifact_id="c" * 64)
            report_b = _match_report(batch_b, artifact_id="d" * 64)
            reconciliation_a = HistoricalReconciliationCollector(snapshot_store).collect(
                report_a
            )
            reconciliation_b = HistoricalReconciliationCollector(snapshot_store).collect(
                report_b
            )

            admission = build_historical_dataset_admission_report(
                plan=plan,
                progress=progress,
                snapshots=(stored_a, stored_b),
                reconciliations=(report_a, report_b),
                gaps=(),
                gap_adjudication=None,
                assessed_at=ASSESSED_AT,
            )
            stored_admission = admission_store.put(admission)
            self.assertTrue(stored_admission.coverage_complete)

            index_entries = sorted(
                (
                    HistoricalReconciliationIndexEntry(
                        request_id=reliance_request_a.request_id,
                        provider_snapshot_id=stored_a.manifest.snapshot_id,
                        historical_batch_id=batch_a.batch_id,
                        reconciliation_report_id=report_a.report_id,
                        reconciliation_snapshot_id=reconciliation_a.manifest.snapshot_id,
                        reconciled_at=RECONCILED_AT,
                        passed=True,
                    ),
                    HistoricalReconciliationIndexEntry(
                        request_id=reliance_request_b.request_id,
                        provider_snapshot_id=stored_b.manifest.snapshot_id,
                        historical_batch_id=batch_b.batch_id,
                        reconciliation_report_id=report_b.report_id,
                        reconciliation_snapshot_id=reconciliation_b.manifest.snapshot_id,
                        reconciled_at=RECONCILED_AT,
                        passed=True,
                    ),
                ),
                key=lambda value: value.request_id,
            )
            reconciliation_index = HistoricalReconciliationIndex(
                plan_id=plan.plan_id,
                progress_id=progress.progress_id,
                provider=UPSTOX_PROVIDER,
                connector_version=progress.connector_version,
                nse_artifact_ids=(NSE_ARTIFACT_ID,),
                prior_index_id=None,
                entries=tuple(index_entries),
                total_completion_count=2,
                updated_at=RECONCILED_AT,
                complete=True,
            )
            stored_index = reconciliation_index_store.put(reconciliation_index)

            service = HistoricalEvaluationCorpusService(
                admission_store=admission_store,
                reconciliation_index_store=reconciliation_index_store,
                snapshot_store=snapshot_store,
                corpus_store=corpus_store,
            )
            with self.assertRaises(HistoricalEvaluationCorpusError):
                service.build(
                    admission_report_id=stored_admission.report_id,
                    reconciliation_index_id=stored_index.index_id,
                    built_at=BUILT_AT,
                )
            self.assertFalse(corpus_store.dataset_root.exists())


class ForgedProviderAndReconciliationEnvelopeTests(unittest.TestCase):
    def test_wrong_type_provider_snapshot_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            fixture = build_two_symbol_fixture(Path(temp_dir))
            snapshot_id = fixture["stored_snapshots"]["RELIANCE"].manifest.snapshot_id
            proxy = _ProxySnapshotStore(fixture["snapshot_store"], {snapshot_id: object()})
            service = HistoricalEvaluationCorpusService(
                admission_store=fixture["admission_store"],
                reconciliation_index_store=fixture["reconciliation_index_store"],
                snapshot_store=proxy,
                corpus_store=fixture["corpus_store"],
            )
            with self.assertRaises(HistoricalEvaluationCorpusIntegrityError):
                service.build(
                    admission_report_id=fixture["admission_report"].report_id,
                    reconciliation_index_id=fixture["reconciliation_index"].index_id,
                    built_at=BUILT_AT,
                )
            self.assertFalse(fixture["corpus_store"].dataset_root.exists())

    def test_tampered_provider_snapshot_hash_is_rejected(self) -> None:
        from dataclasses import replace

        with tempfile.TemporaryDirectory() as temp_dir:
            fixture = build_two_symbol_fixture(Path(temp_dir))
            stored = fixture["stored_snapshots"]["RELIANCE"]
            forged = replace(stored, payload_bytes=stored.payload_bytes + b"x")
            proxy = _ProxySnapshotStore(
                fixture["snapshot_store"], {stored.manifest.snapshot_id: forged}
            )
            service = HistoricalEvaluationCorpusService(
                admission_store=fixture["admission_store"],
                reconciliation_index_store=fixture["reconciliation_index_store"],
                snapshot_store=proxy,
                corpus_store=fixture["corpus_store"],
            )
            with self.assertRaises(HistoricalEvaluationCorpusIntegrityError):
                service.build(
                    admission_report_id=fixture["admission_report"].report_id,
                    reconciliation_index_id=fixture["reconciliation_index"].index_id,
                    built_at=BUILT_AT,
                )

    def test_wrong_type_reconciliation_snapshot_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            fixture = build_two_symbol_fixture(Path(temp_dir))
            snapshot_id = fixture["reconciliation_snapshots"][
                "RELIANCE"
            ].manifest.snapshot_id
            proxy = _ProxySnapshotStore(fixture["snapshot_store"], {snapshot_id: object()})
            service = HistoricalEvaluationCorpusService(
                admission_store=fixture["admission_store"],
                reconciliation_index_store=fixture["reconciliation_index_store"],
                snapshot_store=proxy,
                corpus_store=fixture["corpus_store"],
            )
            with self.assertRaises(HistoricalEvaluationCorpusIntegrityError):
                service.build(
                    admission_report_id=fixture["admission_report"].report_id,
                    reconciliation_index_id=fixture["reconciliation_index"].index_id,
                    built_at=BUILT_AT,
                )

    def test_tampered_reconciliation_payload_bytes_is_rejected(self) -> None:
        from dataclasses import replace

        with tempfile.TemporaryDirectory() as temp_dir:
            fixture = build_two_symbol_fixture(Path(temp_dir))
            stored = fixture["reconciliation_snapshots"]["RELIANCE"]
            forged = replace(stored, payload_bytes=stored.payload_bytes + b"x")
            proxy = _ProxySnapshotStore(
                fixture["snapshot_store"], {stored.manifest.snapshot_id: forged}
            )
            service = HistoricalEvaluationCorpusService(
                admission_store=fixture["admission_store"],
                reconciliation_index_store=fixture["reconciliation_index_store"],
                snapshot_store=proxy,
                corpus_store=fixture["corpus_store"],
            )
            with self.assertRaises(HistoricalEvaluationCorpusIntegrityError):
                service.build(
                    admission_report_id=fixture["admission_report"].report_id,
                    reconciliation_index_id=fixture["reconciliation_index"].index_id,
                    built_at=BUILT_AT,
                )

class MissingEvidenceSanitizationTests(unittest.TestCase):
    """Every injected-store retrieval failure must fail closed with a static,
    sanitized HistoricalEvaluationCorpusError naming only the evidence
    category -- never the requested ID, dataset, path, nested exception
    type/text, or provider payload -- and must never publish a corpus."""

    def _assert_sanitized_unavailable(
        self,
        service: HistoricalEvaluationCorpusService,
        fixture: dict,
        *,
        admission_report_id: str,
        reconciliation_index_id: str,
        expected_message: str,
        secret_markers: tuple[str, ...],
    ) -> None:
        with self.assertRaises(HistoricalEvaluationCorpusError) as ctx:
            service.build(
                admission_report_id=admission_report_id,
                reconciliation_index_id=reconciliation_index_id,
                built_at=BUILT_AT,
            )
        self.assertNotIsInstance(ctx.exception, HistoricalEvaluationCorpusIntegrityError)
        self.assertEqual(str(ctx.exception), expected_message)
        for marker in secret_markers:
            self.assertNotIn(marker, str(ctx.exception))
        self.assertFalse(
            fixture["corpus_store"].dataset_root.exists()
            and any(fixture["corpus_store"].dataset_root.iterdir())
        )

    def test_missing_admission_report_is_sanitized(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            fixture = build_two_symbol_fixture(Path(temp_dir))
            service = build_service(fixture)
            missing_id = "f" * 64
            self._assert_sanitized_unavailable(
                service,
                fixture,
                admission_report_id=missing_id,
                reconciliation_index_id=fixture["reconciliation_index"].index_id,
                expected_message="admission report evidence is unavailable",
                secret_markers=(missing_id,),
            )

    def test_missing_reconciliation_index_is_sanitized(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            fixture = build_two_symbol_fixture(Path(temp_dir))
            service = build_service(fixture)
            missing_id = "f" * 64
            self._assert_sanitized_unavailable(
                service,
                fixture,
                admission_report_id=fixture["admission_report"].report_id,
                reconciliation_index_id=missing_id,
                expected_message="reconciliation index evidence is unavailable",
                secret_markers=(missing_id,),
            )

    def test_hostile_admission_store_exception_text_does_not_surface(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            fixture = build_two_symbol_fixture(Path(temp_dir))
            secret = "leaked-secret-token-9f8e7d /var/secret/admission.json"
            hostile_store = MagicMock()
            hostile_store.get.side_effect = RuntimeError(secret)
            service = HistoricalEvaluationCorpusService(
                admission_store=hostile_store,
                reconciliation_index_store=fixture["reconciliation_index_store"],
                snapshot_store=fixture["snapshot_store"],
                corpus_store=fixture["corpus_store"],
            )
            self._assert_sanitized_unavailable(
                service,
                fixture,
                admission_report_id=fixture["admission_report"].report_id,
                reconciliation_index_id=fixture["reconciliation_index"].index_id,
                expected_message="admission report evidence is unavailable",
                secret_markers=("leaked-secret-token-9f8e7d", "RuntimeError", "/var/secret"),
            )

    def test_hostile_reconciliation_index_store_exception_text_does_not_surface(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            fixture = build_two_symbol_fixture(Path(temp_dir))
            secret = "leaked-secret-token-9f8e7d /var/secret/index.json"
            hostile_store = MagicMock()
            hostile_store.get.side_effect = RuntimeError(secret)
            service = HistoricalEvaluationCorpusService(
                admission_store=fixture["admission_store"],
                reconciliation_index_store=hostile_store,
                snapshot_store=fixture["snapshot_store"],
                corpus_store=fixture["corpus_store"],
            )
            self._assert_sanitized_unavailable(
                service,
                fixture,
                admission_report_id=fixture["admission_report"].report_id,
                reconciliation_index_id=fixture["reconciliation_index"].index_id,
                expected_message="reconciliation index evidence is unavailable",
                secret_markers=("leaked-secret-token-9f8e7d", "RuntimeError", "/var/secret"),
            )

    def test_missing_provider_snapshot_is_sanitized(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            fixture = build_two_symbol_fixture(Path(temp_dir))
            snapshot_id = fixture["stored_snapshots"]["RELIANCE"].manifest.snapshot_id
            service = HistoricalEvaluationCorpusService(
                admission_store=fixture["admission_store"],
                reconciliation_index_store=fixture["reconciliation_index_store"],
                snapshot_store=LocalMarketSnapshotStore(
                    Path(temp_dir) / "empty-snapshots"
                ),
                corpus_store=fixture["corpus_store"],
            )
            self._assert_sanitized_unavailable(
                service,
                fixture,
                admission_report_id=fixture["admission_report"].report_id,
                reconciliation_index_id=fixture["reconciliation_index"].index_id,
                expected_message="admitted provider snapshot evidence is unavailable",
                secret_markers=(snapshot_id, historical_dataset_name(UPSTOX_PROVIDER)),
            )

    def test_hostile_provider_snapshot_store_exception_text_does_not_surface(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            fixture = build_two_symbol_fixture(Path(temp_dir))
            snapshot_id = fixture["stored_snapshots"]["RELIANCE"].manifest.snapshot_id
            secret = f"leaked-secret-token-9f8e7d {snapshot_id} /var/secret/x"
            proxy = _RaisingProxySnapshotStore(
                fixture["snapshot_store"],
                (historical_dataset_name(UPSTOX_PROVIDER), snapshot_id),
                RuntimeError(secret),
            )
            service = HistoricalEvaluationCorpusService(
                admission_store=fixture["admission_store"],
                reconciliation_index_store=fixture["reconciliation_index_store"],
                snapshot_store=proxy,
                corpus_store=fixture["corpus_store"],
            )
            self._assert_sanitized_unavailable(
                service,
                fixture,
                admission_report_id=fixture["admission_report"].report_id,
                reconciliation_index_id=fixture["reconciliation_index"].index_id,
                expected_message="admitted provider snapshot evidence is unavailable",
                secret_markers=("leaked-secret-token-9f8e7d", snapshot_id, "RuntimeError"),
            )

    def test_missing_reconciliation_snapshot_is_sanitized(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            fixture = build_two_symbol_fixture(Path(temp_dir))
            reconciliation_snapshot_id = fixture["reconciliation_snapshots"][
                "RELIANCE"
            ].manifest.snapshot_id
            secret = f"leaked-secret-token-9f8e7d {reconciliation_snapshot_id}"
            proxy = _RaisingProxySnapshotStore(
                fixture["snapshot_store"],
                (HISTORICAL_RECONCILIATION_DATASET, reconciliation_snapshot_id),
                RuntimeError(secret),
            )
            service = HistoricalEvaluationCorpusService(
                admission_store=fixture["admission_store"],
                reconciliation_index_store=fixture["reconciliation_index_store"],
                snapshot_store=proxy,
                corpus_store=fixture["corpus_store"],
            )
            self._assert_sanitized_unavailable(
                service,
                fixture,
                admission_report_id=fixture["admission_report"].report_id,
                reconciliation_index_id=fixture["reconciliation_index"].index_id,
                expected_message="admitted reconciliation snapshot evidence is unavailable",
                secret_markers=(
                    "leaked-secret-token-9f8e7d",
                    reconciliation_snapshot_id,
                    "RuntimeError",
                    HISTORICAL_RECONCILIATION_DATASET,
                ),
            )


# --- adapter: corpus -> diagnostic PointInTimePriceSession -------------------


class HistoricalCorpusAdapterTests(unittest.TestCase):
    def test_sessions_are_sorted_and_lineage_is_preserved(self) -> None:
        first = _fabricated_bar(session=SESSION_ONE)
        second = _fabricated_bar(
            session=SESSION_TWO,
            request_id="c" * 64,
            binding_id="d" * 64,
            provider_snapshot_id="e" * 64,
            reconciliation_snapshot_id="f" * 64,
            observed_at=RECONCILED_AT,
        )
        partition_two = _fabricated_partition((second,), session=SESSION_TWO)
        partition_one = _fabricated_partition((first,), session=SESSION_ONE)
        # Deliberately out of session order; the index still records them
        # ascending, and the adapter must not depend on partitions() order.
        index = _fabricated_index(
            (partition_one, partition_two),
            all_entry_ids=("b" * 64, "c" * 64),
            admitted_entry_ids=("b" * 64, "c" * 64),
            disposition_counts=(("ADMITTED", 2),),
        )
        sessions = point_in_time_price_sessions_from_historical_corpus(
            index, (partition_one, partition_two)
        )
        self.assertEqual(
            tuple(value.market_session for value in sessions),
            (SESSION_ONE, SESSION_TWO),
        )
        for session, bar in zip(sessions, (first, second)):
            self.assertEqual(session.readiness.value, "COLLECTION_ONLY")
            self.assertFalse(session.actionable)
            self.assertEqual(len(session.bars), 1)
            self.assertEqual(session.bars[0].raw_bar_id, bar.bar_id)
            self.assertEqual(session.bars[0].symbol, "RELIANCE")
            self.assertIn(index.corpus_id, session.source_snapshot_ids)
            self.assertIn(index.admission_report_id, session.source_snapshot_ids)
            self.assertIn(index.reconciliation_index_id, session.source_snapshot_ids)
            self.assertIn(bar.provider_snapshot_id, session.source_snapshot_ids)
            self.assertIn(bar.reconciliation_snapshot_id, session.source_snapshot_ids)
            self.assertIn(bar.reconciliation_report_id, session.source_snapshot_ids)
            self.assertEqual(session.cutoff, index.built_at)
            self.assertEqual(session.knowledge_time, index.built_at)
            self.assertGreaterEqual(session.knowledge_time, index.assessed_at)

    def test_two_corpora_over_the_same_partition_bind_different_session_identity(
        self,
    ) -> None:
        """Same bars/partition, different corpus-level accounting -> different snapshot_id.

        A downstream price-session identity must remain bound to corpus-level
        completeness/blocked-entry accounting, not merely to the bars in one
        partition: two corpora that reference the identical partition but
        differ only in blocked-entry accounting are different evidence and
        must never collide on PointInTimePriceSession.snapshot_id.
        """

        bar = _fabricated_bar()
        partition = _fabricated_partition((bar,))
        complete_index = _fabricated_index(
            (partition,),
            all_entry_ids=("b" * 64,),
            admitted_entry_ids=("b" * 64,),
            blocked_entry_ids=(),
            disposition_counts=(("ADMITTED", 1),),
            safe_requests_complete=True,
            coverage_complete=True,
        )
        partial_index = _fabricated_index(
            (partition,),
            all_entry_ids=("b" * 64, "c" * 64),
            admitted_entry_ids=("b" * 64,),
            blocked_entry_ids=("c" * 64,),
            disposition_counts=(("ADMITTED", 1), ("MISSING_COMPLETION", 1)),
            safe_requests_complete=False,
            coverage_complete=False,
        )
        self.assertNotEqual(complete_index.corpus_id, partial_index.corpus_id)

        complete_sessions = point_in_time_price_sessions_from_historical_corpus(
            complete_index, (partition,)
        )
        partial_sessions = point_in_time_price_sessions_from_historical_corpus(
            partial_index, (partition,)
        )
        self.assertEqual(len(complete_sessions), 1)
        self.assertEqual(len(partial_sessions), 1)
        self.assertIn(complete_index.corpus_id, complete_sessions[0].source_snapshot_ids)
        self.assertIn(partial_index.corpus_id, partial_sessions[0].source_snapshot_ids)
        self.assertNotEqual(
            complete_sessions[0].snapshot_id, partial_sessions[0].snapshot_id
        )

    def test_zero_volume_maps_to_not_tradable(self) -> None:
        bar = _fabricated_bar(volume=0)
        partition = _fabricated_partition((bar,))
        index = _fabricated_index((partition,))
        sessions = point_in_time_price_sessions_from_historical_corpus(
            index, (partition,)
        )
        self.assertFalse(sessions[0].bars[0].tradable)

    def test_positive_volume_maps_to_tradable(self) -> None:
        bar = _fabricated_bar(volume=100)
        partition = _fabricated_partition((bar,))
        index = _fabricated_index((partition,))
        sessions = point_in_time_price_sessions_from_historical_corpus(
            index, (partition,)
        )
        self.assertTrue(sessions[0].bars[0].tradable)

    def test_readiness_and_actionable_cannot_be_overridden(self) -> None:
        import inspect

        signature = inspect.signature(point_in_time_price_sessions_from_historical_corpus)
        self.assertEqual(list(signature.parameters), ["index", "partitions"])

    def test_no_explicit_nontrading_evidence_is_manufactured(self) -> None:
        bar = _fabricated_bar()
        partition = _fabricated_partition((bar,))
        index = _fabricated_index((partition,))
        sessions = point_in_time_price_sessions_from_historical_corpus(
            index, (partition,)
        )
        self.assertEqual(sessions[0].explicit_nontrading_listing_ids, ())

    def test_tampered_index_identity_is_rejected(self) -> None:
        bar = _fabricated_bar()
        partition = _fabricated_partition((bar,))
        index = _fabricated_index((partition,))
        object.__setattr__(index, "coverage_complete", not index.coverage_complete)
        with self.assertRaises(HistoricalCorpusAdapterError):
            point_in_time_price_sessions_from_historical_corpus(index, (partition,))

    def test_partitions_not_matching_index_are_rejected(self) -> None:
        bar = _fabricated_bar()
        partition = _fabricated_partition((bar,))
        index = _fabricated_index((partition,))
        other_bar = _fabricated_bar(
            session=SESSION_TWO,
            request_id="c" * 64,
            binding_id="d" * 64,
            provider_snapshot_id="e" * 64,
            reconciliation_snapshot_id="f" * 64,
        )
        other_partition = _fabricated_partition((other_bar,), session=SESSION_TWO)
        with self.assertRaises(HistoricalCorpusAdapterError):
            point_in_time_price_sessions_from_historical_corpus(index, (other_partition,))

    def test_observation_postdating_built_at_is_rejected(self) -> None:
        bar = _fabricated_bar(observed_at=BUILT_AT + timedelta(days=1))
        partition = _fabricated_partition((bar,))
        index = _fabricated_index((partition,), built_at=BUILT_AT)
        with self.assertRaises(HistoricalCorpusAdapterError):
            point_in_time_price_sessions_from_historical_corpus(index, (partition,))

    def test_adapter_output_remains_rejected_by_assemble_evaluation_dataset(self) -> None:
        bar = _fabricated_bar(
            session=SESSION_ONE, listing_key="NSE:RELIANCE", isin="INE002A01018"
        )
        partition = _fabricated_partition((bar,), session=SESSION_ONE)
        index = _fabricated_index((partition,))
        sessions = point_in_time_price_sessions_from_historical_corpus(
            index, (partition,)
        )
        self.assertEqual(len(sessions), 1)

        cal = assembly_calendar(SESSION_ONE)
        universe = assembly_universe(cal, SESSION_ONE)
        tick = assembly_tick_size()
        with self.assertRaises(EvaluationDatasetAssemblyError):
            assemble_evaluation_dataset(
                calendars=(cal,),
                universes=(universe,),
                price_sessions=sessions,
                tick_sizes=(tick,),
            )


if __name__ == "__main__":
    unittest.main()
