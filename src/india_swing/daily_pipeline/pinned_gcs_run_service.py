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
from .calendar_materialization_acquisition import (
    AcquiredCalendarMaterialization,
    CalendarMaterializationObjectRequest,
    acquire_calendar_materialization,
)
from .gcs_landing_job import run_daily_pipeline_from_pinned_gcs_manifest
from .models import DailyPipelineRun
from .pinned_gcs_run_spec import (
    PINNED_GCS_RUN_SPEC_SCHEMA_VERSION,
    PINNED_GCS_RUN_SPEC_SCHEMA_VERSION_WITH_CALENDAR,
    PinnedGCSRunSpec,
)
from .store import LocalDailyPipelineRunStore


class PinnedGCSRunServiceError(Exception):
    pass


_ERR_SPEC = "pinned gcs run service spec is invalid"
_ERR_CALENDAR_ACQUISITION = "pinned gcs run service calendar acquisition failed"
_ERR_CALENDAR = "pinned gcs run service calendar materialization is invalid"
_ERR_EXECUTION = "pinned gcs run service execution failed"


def run_daily_pipeline_from_pinned_gcs_run_spec(
    spec: PinnedGCSRunSpec,
    calendar_materialization: StoredCalendarMaterialization | None,
    *,
    reader: GCSObjectReader,
    reference_store: LocalReferenceArtifactStore,
    daily_store: LocalDailyBundleArtifactStore,
    historical_store: LocalHistoricalPriceArtifactStore,
    identity_store: LocalIdentityRegistryStore,
    adjudication_store: LocalIdentityAdjudicationQueueStore,
    run_store: LocalDailyPipelineRunStore,
) -> DailyPipelineRun:
    """Binds one already-validated PinnedGCSRunSpec to its calendar
    materialization and delegates to run_daily_pipeline_from_pinned_gcs_manifest.

    Schema v1 (PINNED_GCS_RUN_SPEC_SCHEMA_VERSION) requires
    calendar_materialization to be an exact, already-replay-verified
    StoredCalendarMaterialization and fresh_spec.calendar_request to be
    None; no calendar-acquisition read is ever issued for v1. Schema v2
    (PINNED_GCS_RUN_SPEC_SCHEMA_VERSION_WITH_CALENDAR) requires
    calendar_materialization to be exactly None and
    fresh_spec.calendar_request to be an exact
    CalendarMaterializationObjectRequest; it calls
    acquire_calendar_materialization(fresh_spec.calendar_request,
    reader=reader) exactly once, using the same caller-injected reader this
    function passes on to the delegated landing job, and never constructs
    a second reader, client, or calendar source. There is never dual
    calendar authority: v2 rejects a supplied local
    StoredCalendarMaterialization, and v1 rejects a spec carrying a
    calendar_request.

    This is a pure, dependency-injected application-service boundary: it
    never reads a file, environment variable, or the current clock, never
    constructs a GCS/storage client or any local artifact/run store, never
    lists or selects a "latest" calendar materialization or object
    generation, and never infers calendar_materialization_id or
    previous_run_id.

    spec is never trusted as-is: this function independently reconstructs a
    fresh PinnedGCSRunSpec from every retained field (including
    calendar_request), so a post-construction-mutated, subclassed, or
    shaped spec cannot bypass PinnedGCSRunSpec's own validation, and this
    happens before any calendar-acquisition read or delegated job call. For
    v2, the acquisition result is likewise never trusted as-is: it is
    independently reconstructed into a fresh AcquiredCalendarMaterialization
    from its own request/observed_generation/observed_sha256/materialization
    fields, so a patched or mutated acquisition result cannot bypass
    AcquiredCalendarMaterialization's own validation.

    For both schemas, the resulting materialization and its calendar
    snapshot are required to be the exact committed types, their content
    identities are independently verified, materialization_id is
    cross-checked against fresh_spec.calendar_materialization_id, the
    snapshot is required to declare fresh_spec.market_session as a
    session, and its cutoff must not exceed fresh_spec.cutoff. Schema v1
    additionally preserves every existing local-store manifest/
    materialization/calendar-snapshot cross-binding check. Schema v2
    additionally requires the acquired request's materialization_id,
    exact field values, observed_generation, and observed_sha256 all
    agree with fresh_spec.calendar_request; neither the externally pinned
    hash nor the internal materialization ID is treated as authorship
    proof by itself. Only after every schema-specific and common check
    passes is run_daily_pipeline_from_pinned_gcs_manifest called, exactly
    once, with the fresh spec's manifest_request/trusted_binding/
    market_session/cutoff/calendar_materialization_id/previous_run_id, the
    verified calendar_snapshot, and the exact caller-supplied reader/
    stores.

    Ordinary failures (never BaseException) are collapsed into one of
    four static, sanitized PinnedGCSRunServiceError messages -- spec
    reconstruction, calendar acquisition, calendar validation/mismatch, and
    delegated-job execution -- so no bucket/path/hash/ID/date value or
    nested exception text can leak through this boundary. There is no
    same-class bare-reraise privilege anywhere in this function: every
    ordinary exception, including one that happens to already be a
    PinnedGCSRunServiceError or CalendarMaterializationAcquisitionError
    (for example an untrusted reader or delegated job deliberately
    injecting one), is caught uniformly, discarded without being retained
    on the fresh error, and the fresh error is raised only after the
    guarding try/except has fully exited -- so both __cause__ and
    __context__ are exactly None, not merely suppressed from display. This
    function adds no retry, rollback, cleanup, alternate data source, or
    partial-success semantics of its own.
    """

    if type(spec) is not PinnedGCSRunSpec:
        raise PinnedGCSRunServiceError(_ERR_SPEC)

    spec_failure = False
    fresh_spec: PinnedGCSRunSpec | None = None
    try:
        fresh_spec = PinnedGCSRunSpec(
            schema_version=spec.schema_version,
            manifest_request=spec.manifest_request,
            trusted_binding=spec.trusted_binding,
            market_session=spec.market_session,
            cutoff=spec.cutoff,
            calendar_materialization_id=spec.calendar_materialization_id,
            previous_run_id=spec.previous_run_id,
            calendar_request=spec.calendar_request,
        )
    except Exception:
        spec_failure = True
    if spec_failure:
        raise PinnedGCSRunServiceError(_ERR_SPEC)

    calendar_snapshot: CalendarSnapshot | None = None

    if fresh_spec.schema_version == PINNED_GCS_RUN_SPEC_SCHEMA_VERSION:
        if fresh_spec.calendar_request is not None:
            raise PinnedGCSRunServiceError(_ERR_CALENDAR)
        if type(calendar_materialization) is not StoredCalendarMaterialization:
            raise PinnedGCSRunServiceError(_ERR_CALENDAR)

        calendar_failure = False
        try:
            manifest = calendar_materialization.manifest
            materialization = calendar_materialization.materialization
            if type(manifest) is not CalendarMaterializationStoreManifest:
                raise PinnedGCSRunServiceError(_ERR_CALENDAR)
            if type(materialization) is not CollectionCalendarMaterialization:
                raise PinnedGCSRunServiceError(_ERR_CALENDAR)
            candidate_snapshot = materialization.calendar_snapshot
            if type(candidate_snapshot) is not CalendarSnapshot:
                raise PinnedGCSRunServiceError(_ERR_CALENDAR)

            manifest.verify_content_identity()
            materialization.verify_content_identity()
            candidate_snapshot.verify_content_identity()

            if (
                manifest.artifact_id != fresh_spec.calendar_materialization_id
                or materialization.materialization_id != fresh_spec.calendar_materialization_id
            ):
                raise PinnedGCSRunServiceError(_ERR_CALENDAR)
            if manifest.calendar_snapshot_id != candidate_snapshot.snapshot_id:
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
                or manifest.calendar_snapshot_version != candidate_snapshot.version
            ):
                raise PinnedGCSRunServiceError(_ERR_CALENDAR)

            candidate_snapshot.require_session(fresh_spec.market_session)
            if candidate_snapshot.cutoff > fresh_spec.cutoff:
                raise PinnedGCSRunServiceError(_ERR_CALENDAR)

            calendar_snapshot = candidate_snapshot
        except Exception:
            calendar_failure = True
        if calendar_failure:
            raise PinnedGCSRunServiceError(_ERR_CALENDAR)

    elif fresh_spec.schema_version == PINNED_GCS_RUN_SPEC_SCHEMA_VERSION_WITH_CALENDAR:
        if calendar_materialization is not None:
            raise PinnedGCSRunServiceError(_ERR_CALENDAR)
        if type(fresh_spec.calendar_request) is not CalendarMaterializationObjectRequest:
            raise PinnedGCSRunServiceError(_ERR_CALENDAR)

        acquisition_failure = False
        acquired: AcquiredCalendarMaterialization | None = None
        try:
            acquired = acquire_calendar_materialization(fresh_spec.calendar_request, reader=reader)
        except Exception:
            acquisition_failure = True
        if acquisition_failure:
            raise PinnedGCSRunServiceError(_ERR_CALENDAR_ACQUISITION)

        calendar_failure = False
        try:
            if type(acquired) is not AcquiredCalendarMaterialization:
                raise PinnedGCSRunServiceError(_ERR_CALENDAR)
            acquired_snapshot = AcquiredCalendarMaterialization(
                request=acquired.request,
                observed_generation=acquired.observed_generation,
                observed_sha256=acquired.observed_sha256,
                materialization=acquired.materialization,
            )

            materialization = acquired_snapshot.materialization
            if type(materialization) is not CollectionCalendarMaterialization:
                raise PinnedGCSRunServiceError(_ERR_CALENDAR)
            candidate_snapshot = materialization.calendar_snapshot
            if type(candidate_snapshot) is not CalendarSnapshot:
                raise PinnedGCSRunServiceError(_ERR_CALENDAR)

            materialization.verify_content_identity()
            candidate_snapshot.verify_content_identity()

            if materialization.materialization_id != fresh_spec.calendar_materialization_id:
                raise PinnedGCSRunServiceError(_ERR_CALENDAR)

            candidate_snapshot.require_session(fresh_spec.market_session)
            if candidate_snapshot.cutoff > fresh_spec.cutoff:
                raise PinnedGCSRunServiceError(_ERR_CALENDAR)

            if (
                acquired_snapshot.request.materialization_id
                != fresh_spec.calendar_materialization_id
                or acquired_snapshot.request != fresh_spec.calendar_request
                or acquired_snapshot.observed_generation != fresh_spec.calendar_request.generation
                or acquired_snapshot.observed_sha256 != fresh_spec.calendar_request.expected_sha256
            ):
                raise PinnedGCSRunServiceError(_ERR_CALENDAR)

            calendar_snapshot = candidate_snapshot
        except Exception:
            calendar_failure = True
        if calendar_failure:
            raise PinnedGCSRunServiceError(_ERR_CALENDAR)

    else:
        raise PinnedGCSRunServiceError(_ERR_SPEC)

    execution_failure = False
    result: DailyPipelineRun | None = None
    try:
        result = run_daily_pipeline_from_pinned_gcs_manifest(
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
        execution_failure = True
    if execution_failure:
        raise PinnedGCSRunServiceError(_ERR_EXECUTION)
    return result
