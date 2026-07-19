from __future__ import annotations

from datetime import date, datetime

from india_swing.daily_reports.artifact_store import LocalDailyBundleArtifactStore
from india_swing.historical_prices.artifact_store import LocalHistoricalPriceArtifactStore
from india_swing.identity_registry.adjudication_store import LocalIdentityAdjudicationQueueStore
from india_swing.identity_registry.artifact_store import LocalIdentityRegistryStore
from india_swing.reference.calendar import CalendarSnapshot
from india_swing.reference_data.artifact_store import LocalReferenceArtifactStore

from .acquisition import GCSLandingObjectReader, GCSObjectReader, LandingManifestObjectRequest
from .landing_inputs import acquire_verified_landing_inputs
from .landing_manifest import TrustedLandingManifestBinding
from .landing_manifest_acquisition import acquire_verified_landing_manifest
from .models import DailyPipelineRun
from .runner import run_daily_pipeline_from_landing_inputs
from .store import LocalDailyPipelineRunStore


class PinnedGCSLandingJobError(Exception):
    pass


_ERR_MANIFEST_ACQUISITION = "pinned gcs landing job manifest acquisition failed"
_ERR_INPUTS_ACQUISITION = "pinned gcs landing job data object acquisition failed"


def run_daily_pipeline_from_pinned_gcs_manifest(
    *,
    manifest_request: LandingManifestObjectRequest,
    binding: TrustedLandingManifestBinding,
    reader: GCSObjectReader,
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
    """Composes the full pinned-GCS trust chain into one internal job
    boundary: acquire_verified_landing_manifest -> (wrap the same injected
    reader as a GCSLandingObjectReader) -> acquire_verified_landing_inputs
    (with manifest_acquisition set, so the result carries exact manifest
    source provenance) -> run_daily_pipeline_from_landing_inputs, in that
    order.

    Exactly one caller-injected GCSObjectReader is used for all three
    generation-pinned bounded reads issued on a successful run: the landing
    manifest object, then the SECURITY_MASTER object, then the DAILY_BUNDLE
    object, in that order. This function never constructs a GCS/storage
    client, reads an environment variable, inspects the clock, lists
    objects, selects a "latest" object, retries, falls back to a second
    source, schedules work, or sends a notification. It never calls
    run_daily_pipeline_from_landing_manifest: that caller-supplied-bytes
    boundary cannot retain manifest source provenance and would produce
    legacy-v1 lineage instead of the exact v2 source lineage this boundary
    exists to produce.

    A manifest that fails acquisition (invalid request/binding, a reader
    failure, a payload/hash/generation mismatch, or a manifest-verification
    failure) is rejected before either data-object read is attempted, so no
    SECURITY_MASTER or DAILY_BUNDLE read -- and no artifact-store mutation
    -- happens on a failed manifest acquisition. A data-object acquisition
    failure (including a market_session/cutoff mismatch against the
    acquired manifest, or a reader failure on either object) is rejected
    before run_daily_pipeline_from_landing_inputs is ever called, so no
    artifact-store mutation happens on a failed data-object acquisition;
    the existing security-master-before-daily-bundle read order and
    fail-before-second-read behavior are unchanged.

    Ordinary failures (never BaseException) at the manifest-acquisition and
    data-object-acquisition trust boundaries are each collapsed into one
    static, stage-specific PinnedGCSLandingJobError with chaining
    suppressed, so neither bucket/object names, generations, hashes,
    manifest/content bytes, nor nested exception text can leak through this
    boundary. Once VerifiedLandingInputs exists, this function defers
    entirely to run_daily_pipeline_from_landing_inputs for validation,
    lineage, persistence, and failure semantics -- it adds no rollback,
    cleanup, retry, or cross-store transactionality of its own.
    """

    try:
        acquired_manifest = acquire_verified_landing_manifest(manifest_request, binding, reader)
    except Exception:
        raise PinnedGCSLandingJobError(_ERR_MANIFEST_ACQUISITION) from None

    try:
        landing_inputs = acquire_verified_landing_inputs(
            manifest=acquired_manifest.manifest,
            market_session=market_session,
            run_cutoff=cutoff,
            reader=GCSLandingObjectReader(reader),
            manifest_acquisition=acquired_manifest,
        )
    except Exception:
        raise PinnedGCSLandingJobError(_ERR_INPUTS_ACQUISITION) from None

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
