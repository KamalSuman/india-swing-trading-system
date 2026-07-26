"""Adapt a verified historical evaluation corpus into diagnostic price sessions.

The corpus proves only that its bars replay against exact, independently
verified provider and reconciliation evidence -- never that the underlying
provider data was known at its original historical decision cutoff. This
adapter therefore always emits ``EvaluationDataReadiness.COLLECTION_ONLY``
and ``actionable=False``; it accepts no caller override, and its output
remains rejected by ``assemble_evaluation_dataset``.
"""

from __future__ import annotations

from india_swing.market_data.historical_corpus import (
    HistoricalEvaluationCorpusIndex,
    HistoricalEvaluationCorpusSessionPartition,
)

from .dataset_assembly import PointInTimePriceBar, PointInTimePriceSession
from .engine import EvaluationDataReadiness


class HistoricalCorpusAdapterError(ValueError):
    pass


def point_in_time_price_sessions_from_historical_corpus(
    index: HistoricalEvaluationCorpusIndex,
    partitions: tuple[HistoricalEvaluationCorpusSessionPartition, ...],
) -> tuple[PointInTimePriceSession, ...]:
    """Convert one verified corpus and its exact verified partitions.

    ``index`` and every partition are independently re-verified here; the
    caller's own prior verification (for example, from
    ``LocalHistoricalEvaluationCorpusStore.get``) is not trusted on its own.
    """

    if type(index) is not HistoricalEvaluationCorpusIndex:
        raise HistoricalCorpusAdapterError(
            "index must be an exact HistoricalEvaluationCorpusIndex"
        )
    try:
        index.verify_content_identity()
    except (TypeError, ValueError):
        raise HistoricalCorpusAdapterError(
            "historical evaluation corpus index failed identity verification"
        ) from None
    if (
        index.collection_only is not True
        or index.actionable is not False
        or index.training_eligible is not False
    ):
        raise HistoricalCorpusAdapterError("corpus index safety flags are not intact")

    if type(partitions) is not tuple or any(
        type(value) is not HistoricalEvaluationCorpusSessionPartition
        for value in partitions
    ):
        raise HistoricalCorpusAdapterError(
            "partitions must be an exact immutable tuple"
        )
    try:
        for value in partitions:
            value.verify_content_identity()
    except (TypeError, ValueError):
        raise HistoricalCorpusAdapterError(
            "historical evaluation corpus partition failed identity verification"
        ) from None
    if tuple(value.partition_id for value in partitions) != index.partition_ids:
        raise HistoricalCorpusAdapterError(
            "partitions do not match the corpus index exactly"
        )
    if tuple(value.market_session for value in partitions) != index.partition_sessions:
        raise HistoricalCorpusAdapterError(
            "partition sessions do not match the corpus index"
        )

    sessions: list[PointInTimePriceSession] = []
    for partition in partitions:
        if (
            partition.collection_only is not True
            or partition.actionable is not False
            or partition.training_eligible is not False
        ):
            raise HistoricalCorpusAdapterError(
                "corpus partition safety flags are not intact"
            )
        latest_observed_at = max(bar.observed_at for bar in partition.bars)
        if latest_observed_at > index.built_at:
            raise HistoricalCorpusAdapterError(
                "corpus partition observation postdates its built_at cutoff"
            )
        bars = tuple(
            sorted(
                (
                    PointInTimePriceBar(
                        session=bar.session,
                        symbol=bar.listing_key.removeprefix("NSE:"),
                        series=bar.series,
                        isin=bar.isin,
                        open=bar.open,
                        high=bar.high,
                        low=bar.low,
                        close=bar.close,
                        volume=bar.volume,
                        raw_bar_id=bar.bar_id,
                        tradable=bar.volume > 0,
                        lower_circuit_sell_locked=False,
                    )
                    for bar in partition.bars
                ),
                key=lambda value: (value.symbol, value.series),
            )
        )
        source_snapshot_ids = tuple(
            sorted(
                {
                    index.corpus_id,
                    index.admission_report_id,
                    index.reconciliation_index_id,
                    *partition.source_snapshot_ids,
                    *partition.source_report_ids,
                }
            )
        )
        sessions.append(
            PointInTimePriceSession(
                market_session=partition.market_session,
                cutoff=index.built_at,
                # Conservative on purpose: provider observation time alone is not
                # the availability time of reconciled/admitted corpus evidence.
                # built_at is the first timestamp guaranteed to be no earlier than
                # provider observation, reconciliation, admission assessment,
                # reconciliation-index update, and corpus construction.
                knowledge_time=index.built_at,
                source_artifact_id=partition.partition_id,
                source_snapshot_ids=source_snapshot_ids,
                bars=bars,
                explicit_nontrading_listing_ids=(),
                readiness=EvaluationDataReadiness.COLLECTION_ONLY,
                actionable=False,
            )
        )
    return tuple(sorted(sessions, key=lambda value: value.market_session))
