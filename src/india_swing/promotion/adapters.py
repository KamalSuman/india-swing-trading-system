from __future__ import annotations

from india_swing.daily_pipeline.models import DailyPipelineRun
from india_swing.market_data.historical_corpus import (
    HistoricalEvaluationCorpusIndex,
    HistoricalEvaluationCorpusSessionPartition,
)
from india_swing.reference.models import ReferenceReadiness

from .models import PromotionCapability, PromotionEvidence, PromotionIntegrityError


def _collection_evidence(
    run: DailyPipelineRun,
    *,
    capability: PromotionCapability,
    source_snapshot_ids: tuple[str, ...],
    reason_codes: tuple[str, ...],
) -> PromotionEvidence:
    return PromotionEvidence(
        capability=capability,
        cutoff=run.cutoff,
        coverage_start=run.market_session,
        coverage_end=run.market_session,
        source_snapshot_ids=tuple(sorted(set(source_snapshot_ids))),
        readiness=run.readiness,
        complete=False,
        actionable=False,
        reason_codes=tuple(sorted(set(reason_codes))),
    )


def promotion_evidence_from_daily_run(
    run: DailyPipelineRun,
) -> tuple[PromotionEvidence, ...]:
    """Describe exactly what one collection run proves without upgrading it."""

    if type(run) is not DailyPipelineRun:
        raise TypeError("daily pipeline run must be exact")
    run.verify_content_identity()
    evidence = (
        _collection_evidence(
            run,
            capability=PromotionCapability.CALENDAR,
            source_snapshot_ids=(
                run.calendar_materialization_id,
                run.calendar_snapshot_id,
            ),
            reason_codes=("PROVENANCE_UNVERIFIED",),
        ),
        _collection_evidence(
            run,
            capability=PromotionCapability.STABLE_IDENTITY,
            source_snapshot_ids=(
                run.identity_registry_id,
                run.identity_registry_manifest_id,
                run.adjudication_queue_id,
            ),
            reason_codes=("NOT_PROMOTED",),
        ),
        _collection_evidence(
            run,
            capability=PromotionCapability.UNIVERSE,
            source_snapshot_ids=(
                run.current_security_master_artifact_id,
                run.reconciliation_snapshot_id,
            ),
            reason_codes=("NOT_MATERIALIZED",),
        ),
        _collection_evidence(
            run,
            capability=PromotionCapability.RAW_PRICES,
            source_snapshot_ids=(
                run.historical_price_artifact_id,
                run.historical_price_manifest_id,
            ),
            reason_codes=("FINALITY_UNVERIFIED",),
        ),
        _collection_evidence(
            run,
            capability=PromotionCapability.LIQUIDITY,
            source_snapshot_ids=(
                run.historical_price_artifact_id,
                run.reconciliation_snapshot_id,
            ),
            reason_codes=("TRAILING_STATE_NOT_MATERIALIZED",),
        ),
        _collection_evidence(
            run,
            capability=PromotionCapability.SURVEILLANCE,
            source_snapshot_ids=(run.reconciliation_snapshot_id,),
            reason_codes=tuple(
                sorted(
                    {
                        "NOT_PROMOTED",
                        *run.reconciliation_global_reason_codes,
                    }
                )
            ),
        ),
        _collection_evidence(
            run,
            capability=PromotionCapability.EXPLICIT_NONTRADING,
            source_snapshot_ids=(run.reconciliation_snapshot_id,),
            reason_codes=("STATE_NOT_MATERIALIZED",),
        ),
        _collection_evidence(
            run,
            capability=PromotionCapability.RECONCILIATION,
            source_snapshot_ids=(
                run.current_daily_bundle_artifact_id,
                run.current_security_master_artifact_id,
                run.reconciliation_snapshot_id,
            ),
            reason_codes=tuple(
                sorted(
                    {
                        "NOT_PROMOTED",
                        *run.reconciliation_global_reason_codes,
                    }
                )
            ),
        ),
    )
    return tuple(sorted(evidence, key=lambda value: value.capability.value))


class HistoricalCorpusPromotionError(PromotionIntegrityError):
    """A historical-corpus-to-promotion bridge input failed a static safety rule."""


def promotion_evidence_from_historical_corpus(
    index: HistoricalEvaluationCorpusIndex,
    partitions: tuple[HistoricalEvaluationCorpusSessionPartition, ...],
) -> tuple[PromotionEvidence, PromotionEvidence]:
    """Bridge one sealed historical corpus into exactly two promotion capabilities.

    A corpus directly proves only RAW_PRICES and RECONCILIATION evidence, and
    only that its bars replay against exact, independently verified provider
    and reconciliation lineage -- never that the underlying provider data was
    known at its original historical decision cutoff. Both records therefore
    always remain COLLECTION_ONLY/non-actionable and always carry
    PROVENANCE_NOT_POINT_IN_TIME_VERIFIED; no other promotion capability is
    synthesized here.
    """

    if type(index) is not HistoricalEvaluationCorpusIndex:
        raise HistoricalCorpusPromotionError(
            "historical corpus index must be an exact HistoricalEvaluationCorpusIndex"
        )
    try:
        index.verify_content_identity()
    except (TypeError, ValueError):
        raise HistoricalCorpusPromotionError(
            "historical corpus index failed identity verification"
        ) from None
    if (
        index.collection_only is not True
        or index.actionable is not False
        or index.training_eligible is not False
    ):
        raise HistoricalCorpusPromotionError(
            "historical corpus safety flags are not intact"
        )
    if not index.admitted_entry_ids:
        raise HistoricalCorpusPromotionError(
            "historical corpus has no admitted entries to bridge"
        )

    if (
        type(partitions) is not tuple
        or not partitions
        or any(
            type(value) is not HistoricalEvaluationCorpusSessionPartition
            for value in partitions
        )
    ):
        raise HistoricalCorpusPromotionError(
            "historical corpus partitions must be a non-empty exact tuple"
        )
    try:
        for value in partitions:
            value.verify_content_identity()
    except (TypeError, ValueError):
        raise HistoricalCorpusPromotionError(
            "historical corpus partition failed identity verification"
        ) from None
    if tuple(value.partition_id for value in partitions) != index.partition_ids:
        raise HistoricalCorpusPromotionError(
            "historical corpus partitions do not match the corpus index exactly"
        )
    if tuple(value.market_session for value in partitions) != index.partition_sessions:
        raise HistoricalCorpusPromotionError(
            "historical corpus partition sessions do not match the corpus index"
        )
    for value in partitions:
        if (
            value.collection_only is not True
            or value.actionable is not False
            or value.training_eligible is not False
        ):
            raise HistoricalCorpusPromotionError(
                "historical corpus partition safety flags are not intact"
            )

    reason_codes: set[str] = {"PROVENANCE_NOT_POINT_IN_TIME_VERIFIED"}
    if not index.safe_requests_complete:
        reason_codes.add("SAFE_REQUESTS_INCOMPLETE")
    if not index.coverage_complete:
        reason_codes.add("COVERAGE_INCOMPLETE")
    if index.blocked_entry_ids:
        reason_codes.add("BLOCKED_ENTRIES_PRESENT")
    complete = (
        bool(partitions)
        and index.safe_requests_complete
        and index.coverage_complete
        and not index.blocked_entry_ids
    )
    ordered_reason_codes = tuple(sorted(reason_codes))

    coverage_start = index.partition_sessions[0]
    coverage_end = index.partition_sessions[-1]

    raw_prices = PromotionEvidence(
        capability=PromotionCapability.RAW_PRICES,
        cutoff=index.built_at,
        coverage_start=coverage_start,
        coverage_end=coverage_end,
        source_snapshot_ids=tuple(
            sorted({index.corpus_id, index.admission_report_id})
        ),
        readiness=ReferenceReadiness.COLLECTION_ONLY,
        complete=complete,
        actionable=False,
        reason_codes=ordered_reason_codes,
    )
    reconciliation = PromotionEvidence(
        capability=PromotionCapability.RECONCILIATION,
        cutoff=index.built_at,
        coverage_start=coverage_start,
        coverage_end=coverage_end,
        source_snapshot_ids=tuple(
            sorted({index.corpus_id, index.reconciliation_index_id})
        ),
        readiness=ReferenceReadiness.COLLECTION_ONLY,
        complete=complete,
        actionable=False,
        reason_codes=ordered_reason_codes,
    )
    return (raw_prices, reconciliation)
