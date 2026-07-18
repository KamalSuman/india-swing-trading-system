from __future__ import annotations

from india_swing.daily_pipeline.models import DailyPipelineRun

from .models import PromotionCapability, PromotionEvidence


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
