from __future__ import annotations

from india_swing.calendar_data.materialization import CollectionCalendarMaterialization
from india_swing.calendar_data.materialization_store import (
    CalendarMaterializationStoreManifest,
    StoredCalendarMaterialization,
)
from india_swing.daily_reports.artifact_store import LocalDailyBundleArtifactStore
from india_swing.historical_prices.artifact_store import LocalHistoricalPriceArtifactStore
from india_swing.identity_registry.adjudication_store import LocalIdentityAdjudicationQueueStore
from india_swing.identity_registry.artifact_store import LocalIdentityRegistryStore
from india_swing.reference.calendar import CalendarSnapshot
from india_swing.reference_data.artifact_store import LocalReferenceArtifactStore

from .acquisition import GCSObjectReader
from .gcs_landing_job import run_daily_pipeline_from_pinned_gcs_manifest
from .models import DailyPipelineRun
from .pinned_gcs_run_spec import PinnedGCSRunSpec
from .store import LocalDailyPipelineRunStore


class PinnedGCSRunServiceError(Exception):
    pass


_ERR_SPEC = "pinned gcs run service spec is invalid"
_ERR_CALENDAR = "pinned gcs run service calendar materialization is invalid"
_ERR_EXECUTION = "pinned gcs run service execution failed"


def run_daily_pipeline_from_pinned_gcs_run_spec(
    spec: PinnedGCSRunSpec,
    calendar_materialization: StoredCalendarMaterialization,
    *,
    reader: GCSObjectReader,
    reference_store: LocalReferenceArtifactStore,
    daily_store: LocalDailyBundleArtifactStore,
    historical_store: LocalHistoricalPriceArtifactStore,
    identity_store: LocalIdentityRegistryStore,
    adjudication_store: LocalIdentityAdjudicationQueueStore,
    run_store: LocalDailyPipelineRunStore,
) -> DailyPipelineRun:
    """Binds one already-validated PinnedGCSRunSpec to one exact,
    replay-verified StoredCalendarMaterialization and delegates to
    run_daily_pipeline_from_pinned_gcs_manifest.

    This is a pure, dependency-injected application-service boundary: it
    never reads a file, environment variable, or the current clock, never
    constructs a GCS/storage client or any local artifact/run store, never
    lists or selects a "latest" calendar materialization, and never infers
    calendar_materialization_id or previous_run_id. It cannot prove
    authorship of spec or provenance of an arbitrarily caller-constructed
    calendar_materialization; the outer CLI boundary that reads spec
    through a bounded safe-file boundary and obtains
    calendar_materialization only through
    LocalCalendarMaterializationStore.get(spec.calendar_materialization_id)
    remains separate future work.

    spec is never trusted as-is: this function independently reconstructs a
    fresh PinnedGCSRunSpec from its seven retained fields, so a
    post-construction-mutated spec (or a mutated nested manifest_request /
    trusted_binding) cannot bypass PinnedGCSRunSpec's own validation.
    calendar_materialization is required to be exact
    StoredCalendarMaterialization, and its manifest, materialization, and
    calendar_snapshot are each independently content-identity-verified and
    explicitly cross-checked against each other and against
    spec.calendar_materialization_id before any GCS read is attempted; a
    calendar whose cutoff follows the spec cutoff, or whose declared
    calendar session does not include the spec market_session, is
    rejected. Only after every preflight check passes is
    run_daily_pipeline_from_pinned_gcs_manifest called, exactly once, with
    the fresh spec's manifest_request/trusted_binding/market_session/
    cutoff/calendar_materialization_id/previous_run_id, the verified
    calendar_snapshot, and the exact caller-supplied reader/stores.

    Ordinary failures (never BaseException) are collapsed into one of
    three static, sanitized PinnedGCSRunServiceError messages -- one for
    spec reconstruction, one for calendar verification/mismatch, one for
    delegated-job execution -- with chaining suppressed, so no bucket/path/
    hash/ID/date value or nested exception text can leak through this
    boundary. This function adds no retry, rollback, cleanup, alternate
    data source, or partial-success semantics of its own.
    """

    if type(spec) is not PinnedGCSRunSpec:
        raise PinnedGCSRunServiceError(_ERR_SPEC)
    try:
        fresh_spec = PinnedGCSRunSpec(
            schema_version=spec.schema_version,
            manifest_request=spec.manifest_request,
            trusted_binding=spec.trusted_binding,
            market_session=spec.market_session,
            cutoff=spec.cutoff,
            calendar_materialization_id=spec.calendar_materialization_id,
            previous_run_id=spec.previous_run_id,
        )
    except Exception:
        raise PinnedGCSRunServiceError(_ERR_SPEC) from None

    if type(calendar_materialization) is not StoredCalendarMaterialization:
        raise PinnedGCSRunServiceError(_ERR_CALENDAR)
    manifest = calendar_materialization.manifest
    materialization = calendar_materialization.materialization
    if type(manifest) is not CalendarMaterializationStoreManifest:
        raise PinnedGCSRunServiceError(_ERR_CALENDAR)
    if type(materialization) is not CollectionCalendarMaterialization:
        raise PinnedGCSRunServiceError(_ERR_CALENDAR)
    calendar_snapshot = materialization.calendar_snapshot
    if type(calendar_snapshot) is not CalendarSnapshot:
        raise PinnedGCSRunServiceError(_ERR_CALENDAR)

    try:
        manifest.verify_content_identity()
        materialization.verify_content_identity()
        calendar_snapshot.verify_content_identity()
    except Exception:
        raise PinnedGCSRunServiceError(_ERR_CALENDAR) from None

    if (
        manifest.artifact_id != fresh_spec.calendar_materialization_id
        or materialization.materialization_id != fresh_spec.calendar_materialization_id
    ):
        raise PinnedGCSRunServiceError(_ERR_CALENDAR)
    if manifest.calendar_snapshot_id != calendar_snapshot.snapshot_id:
        raise PinnedGCSRunServiceError(_ERR_CALENDAR)
    if (
        manifest.cutoff != materialization.cutoff
        or manifest.coverage_start != materialization.coverage_start
        or manifest.coverage_end != materialization.coverage_end
        or manifest.readiness != materialization.readiness
        or manifest.actionable != materialization.actionable
        or manifest.materialization_schema_version != materialization.schema_version
        or manifest.materialization_policy_version != materialization.policy_version
        or manifest.source_manifests != materialization.source_manifests
        or manifest.observed_evidence_bindings != materialization.observed_evidence_bindings
        or manifest.calendar_snapshot_version != calendar_snapshot.version
    ):
        raise PinnedGCSRunServiceError(_ERR_CALENDAR)

    try:
        calendar_snapshot.require_session(fresh_spec.market_session)
    except Exception:
        raise PinnedGCSRunServiceError(_ERR_CALENDAR) from None
    if calendar_snapshot.cutoff > fresh_spec.cutoff:
        raise PinnedGCSRunServiceError(_ERR_CALENDAR)

    try:
        return run_daily_pipeline_from_pinned_gcs_manifest(
            manifest_request=fresh_spec.manifest_request,
            binding=fresh_spec.trusted_binding,
            reader=reader,
            market_session=fresh_spec.market_session,
            cutoff=fresh_spec.cutoff,
            calendar_materialization_id=fresh_spec.calendar_materialization_id,
            calendar=calendar_snapshot,
            previous_run_id=fresh_spec.previous_run_id,
            reference_store=reference_store,
            daily_store=daily_store,
            historical_store=historical_store,
            identity_store=identity_store,
            adjudication_store=adjudication_store,
            run_store=run_store,
        )
    except Exception:
        raise PinnedGCSRunServiceError(_ERR_EXECUTION) from None
