from __future__ import annotations

from datetime import date, datetime

from india_swing.daily_reports.artifact_store import LocalDailyBundleArtifactStore
from india_swing.historical_prices.artifact_store import LocalHistoricalPriceArtifactStore
from india_swing.identity_registry.adjudication_store import LocalIdentityAdjudicationQueueStore
from india_swing.identity_registry.artifact_store import LocalIdentityRegistryStore
from india_swing.reference.calendar import CalendarSnapshot
from india_swing.reference_data.artifact_store import LocalReferenceArtifactStore

from .landing_inputs import LandingObjectReader, acquire_verified_landing_inputs
from .landing_manifest import LandingManifestVerifier, TrustedLandingManifestBinding
from .models import DailyPipelineRun
from .runner import run_daily_pipeline_from_landing_inputs
from .store import LocalDailyPipelineRunStore


class DailyLandingJobError(Exception):
    pass


_ERR_MANIFEST_VERIFICATION = "daily landing job manifest verification failed"
_ERR_ACQUISITION = "daily landing job acquisition failed"


def run_daily_pipeline_from_landing_manifest(
    *,
    manifest_bytes: bytes,
    binding: TrustedLandingManifestBinding,
    reader: LandingObjectReader,
    market_session: date,
    cutoff: datetime,
    calendar_materialization_id: str,
    calendar: CalendarSnapshot,
    previous_run_id: str | None,
    reference_store: LocalReferenceArtifactStore,
    daily_store: LocalDailyBundleArtifactStore,
    historical_store: LocalHistoricalPriceArtifactStore,
    identity_store: LocalIdentityRegistryStore,
    adjudication_store: LocalIdentityAdjudicationQueueStore,
    run_store: LocalDailyPipelineRunStore,
) -> DailyPipelineRun:
    """Composes the existing trust-chain stages into one internal job
    boundary: LandingManifestVerifier.verify -> acquire_verified_landing_inputs
    -> run_daily_pipeline_from_landing_inputs, in that order.

    Every store, the object reader, and every temporal/calendar input remain
    caller-injected. This function never constructs a GCS client, reads an
    environment variable, inspects the clock, lists objects, selects a
    "latest" object, retries, falls back, schedules work, or sends a
    notification. A manifest or binding that fails verification is rejected
    before acquire_verified_landing_inputs is ever called, so no object read
    happens on an invalid manifest; a landing-input acquisition failure is
    rejected before run_daily_pipeline_from_landing_inputs is ever called, so
    no artifact-store mutation happens on a failed acquisition.

    Ordinary failures (never BaseException) at the manifest-verification and
    acquisition trust boundaries are each collapsed into one static,
    stage-specific DailyLandingJobError with chaining suppressed, so neither
    manifest bytes, bucket/object names, generations, hashes, paths, nested
    exception text, nor caller-supplied sentinel values can leak through this
    boundary. Once verified landing inputs exist, this function defers
    entirely to run_daily_pipeline_from_landing_inputs for validation,
    lineage, persistence, and failure semantics; it adds no rollback,
    cleanup, retry, or cross-store transactionality of its own.
    """

    try:
        manifest = LandingManifestVerifier().verify(manifest_bytes, binding)
    except Exception:
        raise DailyLandingJobError(_ERR_MANIFEST_VERIFICATION) from None

    try:
        landing_inputs = acquire_verified_landing_inputs(
            manifest=manifest,
            market_session=market_session,
            run_cutoff=cutoff,
            reader=reader,
        )
    except Exception:
        raise DailyLandingJobError(_ERR_ACQUISITION) from None

    return run_daily_pipeline_from_landing_inputs(
        landing_inputs=landing_inputs,
        market_session=market_session,
        cutoff=cutoff,
        calendar_materialization_id=calendar_materialization_id,
        calendar=calendar,
        previous_run_id=previous_run_id,
        reference_store=reference_store,
        daily_store=daily_store,
        historical_store=historical_store,
        identity_store=identity_store,
        adjudication_store=adjudication_store,
        run_store=run_store,
    )
