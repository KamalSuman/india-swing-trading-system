from __future__ import annotations

import stat
from pathlib import Path

from india_swing.calendar_data.materialization_store import LocalCalendarMaterializationStore
from india_swing.daily_reports.artifact_store import LocalDailyBundleArtifactStore
from india_swing.historical_prices.artifact_store import LocalHistoricalPriceArtifactStore
from india_swing.identity_registry.adjudication_store import LocalIdentityAdjudicationQueueStore
from india_swing.identity_registry.artifact_store import LocalIdentityRegistryStore
from india_swing.reference_data.artifact_store import LocalReferenceArtifactStore

from .acquisition import GCSObjectReader
from .models import DailyPipelineRun
from .pinned_gcs_run_service import run_daily_pipeline_from_pinned_gcs_run_spec
from .pinned_gcs_run_spec import (
    MAXIMUM_PINNED_GCS_RUN_SPEC_BYTES,
    PINNED_GCS_RUN_SPEC_SCHEMA_VERSION,
    PINNED_GCS_RUN_SPEC_SCHEMA_VERSION_WITH_CALENDAR,
    PinnedGCSRunSpec,
    parse_pinned_gcs_run_spec,
)
from .pinned_gcs_state_publication_service import (
    CompletedPinnedGCSStatePublication,
    run_daily_pipeline_and_publish_state_from_pinned_gcs_run_spec,
)
from .state_inventory import PipelineStateRoots
from .state_publication import StateObjectWriter
from .store import LocalDailyPipelineRunStore


class PinnedGCSRunFileBoundaryError(Exception):
    pass


_ERR_LOAD = "pinned gcs run spec file could not be loaded"
_ERR_SCHEMA = "pinned gcs run file boundary schema routing failed"
_ERR_CALENDAR = "pinned gcs run file boundary calendar acquisition failed"
_ERR_BINDING = "pinned gcs run file boundary publication root binding failed"
_CONCRETE_PATH_TYPE = type(Path())


def _path_matches_exactly(value: object, expected: Path) -> bool:
    """Returns true only for an exact concrete Path with equal lexical value."""

    return type(value) is _CONCRETE_PATH_TYPE and value == expected


def load_pinned_gcs_run_spec_file(spec_path: str) -> PinnedGCSRunSpec:
    """Reads one caller-named regular file and strictly parses it as a
    PinnedGCSRunSpec.

    spec_path must be a non-empty str containing no NUL character. The
    target is required, via Path.lstat plus the stat module, to exist and
    be a regular file -- a symlink, directory, or other non-regular target
    is rejected without ever being opened. At most
    MAXIMUM_PINNED_GCS_RUN_SPEC_BYTES + 1 bytes are read and handed
    unmodified to parse_pinned_gcs_run_spec, which remains the sole
    authority on spec byte-size and schema validation (schema 1 or 2
    alike). Every ordinary path/filesystem/parse failure (never
    BaseException) -- including one that happens to already be a
    PinnedGCSRunFileBoundaryError -- is caught uniformly, discarded
    without being retained, and collapses into one fresh static,
    sanitized PinnedGCSRunFileBoundaryError raised only after the guarding
    try/except has fully exited, so both __cause__ and __context__ are
    exactly None, not merely suppressed from display.
    """

    if type(spec_path) is not str or not spec_path or "\x00" in spec_path:
        raise PinnedGCSRunFileBoundaryError(_ERR_LOAD)

    load_failure = False
    spec: PinnedGCSRunSpec | None = None
    try:
        path = Path(spec_path)
        status = path.lstat()
        if not stat.S_ISREG(status.st_mode):
            raise PinnedGCSRunFileBoundaryError(_ERR_LOAD)
        with open(path, "rb") as handle:
            spec_bytes = handle.read(MAXIMUM_PINNED_GCS_RUN_SPEC_BYTES + 1)
        spec = parse_pinned_gcs_run_spec(spec_bytes)
    except Exception:
        load_failure = True
    if load_failure:
        raise PinnedGCSRunFileBoundaryError(_ERR_LOAD)
    return spec


def _load_and_reconstruct_spec(spec_path: str) -> PinnedGCSRunSpec:
    """Loads spec_path exactly once via load_pinned_gcs_run_spec_file, then
    independently reconstructs a fresh PinnedGCSRunSpec exactly once from
    every one of the loaded value's retained fields (including
    calendar_request) before any routing decision, root/store binding,
    get call, or service call is made -- so a wrong/subclass/shaped
    loaded spec, a post-construction mutation to any outer field
    (schema_version to bool True/False, an unsupported int, an int
    subclass, or an equality-poisoned object; calendar_materialization_id;
    cutoff; market_session; previous_run_id), or a mutation to any nested
    manifest_request/trusted_binding/calendar_request field, is rejected
    here before it can influence anything downstream. Shared by both
    run_daily_pipeline_from_pinned_gcs_run_spec_file and
    run_daily_pipeline_and_publish_state_from_pinned_gcs_run_spec_file so
    each performs exactly one load and exactly one reconstruction.
    """

    loaded_spec = load_pinned_gcs_run_spec_file(spec_path)

    if type(loaded_spec) is not PinnedGCSRunSpec:
        raise PinnedGCSRunFileBoundaryError(_ERR_SCHEMA)

    reconstruction_failure = False
    spec: PinnedGCSRunSpec | None = None
    try:
        spec = PinnedGCSRunSpec(
            schema_version=loaded_spec.schema_version,
            manifest_request=loaded_spec.manifest_request,
            trusted_binding=loaded_spec.trusted_binding,
            market_session=loaded_spec.market_session,
            cutoff=loaded_spec.cutoff,
            calendar_materialization_id=loaded_spec.calendar_materialization_id,
            previous_run_id=loaded_spec.previous_run_id,
            calendar_request=loaded_spec.calendar_request,
        )
        if type(spec.schema_version) is not int:
            raise PinnedGCSRunFileBoundaryError(_ERR_SCHEMA)
    except Exception:
        reconstruction_failure = True
    if reconstruction_failure:
        raise PinnedGCSRunFileBoundaryError(_ERR_SCHEMA)
    return spec


def _routed_calendar_materialization(
    spec: PinnedGCSRunSpec, calendar_store: LocalCalendarMaterializationStore | None
):
    """Routes the exact reconstructed spec's calendar materialization by
    schema version.

    For PINNED_GCS_RUN_SPEC_SCHEMA_VERSION (v1), calendar_store must be
    exact LocalCalendarMaterializationStore; this calls
    calendar_store.get(spec.calendar_materialization_id) exactly once --
    there is no listing, latest selection, fallback, retry, or inferred
    ID. For PINNED_GCS_RUN_SPEC_SCHEMA_VERSION_WITH_CALENDAR (v2),
    calendar_store may be exactly None or exact
    LocalCalendarMaterializationStore; it is never read from (no get
    call, no other member access) -- this always returns exactly None for
    v2, so the already-integrated pinned-GCS run service remains the sole
    calendar-acquisition authority. Any other schema_version, or any
    other calendar_store value (including a subclass or a shaped proxy),
    is rejected. Shared by both run_daily_pipeline_from_pinned_gcs_run_
    spec_file and run_daily_pipeline_and_publish_state_from_pinned_gcs_
    run_spec_file.
    """

    if spec.schema_version == PINNED_GCS_RUN_SPEC_SCHEMA_VERSION:
        if type(calendar_store) is not LocalCalendarMaterializationStore:
            raise PinnedGCSRunFileBoundaryError(_ERR_SCHEMA)

        get_failure = False
        calendar_materialization = None
        try:
            calendar_materialization = calendar_store.get(spec.calendar_materialization_id)
        except Exception:
            get_failure = True
        if get_failure:
            raise PinnedGCSRunFileBoundaryError(_ERR_CALENDAR)
        return calendar_materialization

    elif spec.schema_version == PINNED_GCS_RUN_SPEC_SCHEMA_VERSION_WITH_CALENDAR:
        if (
            calendar_store is not None
            and type(calendar_store) is not LocalCalendarMaterializationStore
        ):
            raise PinnedGCSRunFileBoundaryError(_ERR_SCHEMA)
        return None

    else:
        raise PinnedGCSRunFileBoundaryError(_ERR_SCHEMA)


def run_daily_pipeline_from_pinned_gcs_run_spec_file(
    spec_path: str,
    *,
    calendar_store: LocalCalendarMaterializationStore | None,
    reader: GCSObjectReader,
    reference_store: LocalReferenceArtifactStore,
    daily_store: LocalDailyBundleArtifactStore,
    historical_store: LocalHistoricalPriceArtifactStore,
    identity_store: LocalIdentityRegistryStore,
    adjudication_store: LocalIdentityAdjudicationQueueStore,
    run_store: LocalDailyPipelineRunStore,
) -> DailyPipelineRun:
    """Loads a PinnedGCSRunSpec from spec_path and routes its calendar
    materialization by exact schema version before delegating exactly
    once to run_daily_pipeline_from_pinned_gcs_run_spec.

    Loading/reconstruction and schema-v1/v2 calendar routing are
    performed by _load_and_reconstruct_spec and
    _routed_calendar_materialization respectively -- the same two private
    helpers run_daily_pipeline_and_publish_state_from_pinned_gcs_run_spec_
    file uses, so both public run entry points share exactly one spec-
    load/reconstruction path and exactly one routing path.

    Every ordinary failure at the file-load, reconstruction/routing, or
    v1 calendar-lookup stage (never BaseException) -- including one that
    happens to already be a PinnedGCSRunFileBoundaryError -- is caught
    uniformly, discarded without being retained, and collapses into one
    fresh, static, stage-specific PinnedGCSRunFileBoundaryError raised
    only after the guarding try/except has fully exited, so both
    __cause__ and __context__ are exactly None. The delegated service
    call itself is never wrapped in a try/except: PinnedGCSRunServiceError
    and any BaseException it raises propagate completely unchanged, for
    both schema versions.
    """

    spec = _load_and_reconstruct_spec(spec_path)
    calendar_materialization = _routed_calendar_materialization(spec, calendar_store)

    return run_daily_pipeline_from_pinned_gcs_run_spec(
        spec,
        calendar_materialization,
        reader=reader,
        reference_store=reference_store,
        daily_store=daily_store,
        historical_store=historical_store,
        identity_store=identity_store,
        adjudication_store=adjudication_store,
        run_store=run_store,
    )


def run_daily_pipeline_and_publish_state_from_pinned_gcs_run_spec_file(
    spec_path: str,
    roots: PipelineStateRoots,
    bucket: str,
    *,
    calendar_store: LocalCalendarMaterializationStore | None,
    reader: GCSObjectReader,
    reference_store: LocalReferenceArtifactStore,
    daily_store: LocalDailyBundleArtifactStore,
    historical_store: LocalHistoricalPriceArtifactStore,
    identity_store: LocalIdentityRegistryStore,
    adjudication_store: LocalIdentityAdjudicationQueueStore,
    run_store: LocalDailyPipelineRunStore,
    writer: StateObjectWriter,
) -> CompletedPinnedGCSStatePublication:
    """Loads a PinnedGCSRunSpec from spec_path, proves every injected
    local store is backed by the corresponding PipelineStateRoots path,
    routes its calendar materialization by exact schema version, and
    delegates exactly once to
    run_daily_pipeline_and_publish_state_from_pinned_gcs_run_spec.

    Uses the same _load_and_reconstruct_spec and
    _routed_calendar_materialization private helpers
    run_daily_pipeline_from_pinned_gcs_run_spec_file uses -- never a
    duplicate load, parse, reconstruction, or routing implementation.
    This function never calls run_daily_pipeline_from_pinned_gcs_run_spec
    directly; it only ever reaches the composed publication service.

    roots must be exact PipelineStateRoots. Between reconstruction and
    routing/get, every injected store is required to be its exact
    committed class (never a subclass or shaped proxy) and lexically
    backed by the matching PipelineStateRoots path using plain Path
    equality on already-constructed path objects -- no root is resolved,
    normalized, case-folded, stat'd, listed, or otherwise touched:
    reference_store.root == roots.reference_data; daily_store.root ==
    roots.daily_reports; historical_store.root == roots.historical_prices
    and historical_store.daily_reports_root == roots.daily_reports;
    identity_store.root == roots.identity_registry and
    identity_store.reference_data_root == roots.reference_data;
    adjudication_store.root == roots.identity_registry and
    adjudication_store.registry_store is the exact identity_store object
    (identity, not equality); run_store.root == roots.daily_pipeline. For
    schema v1, calendar_store must additionally be exact
    LocalCalendarMaterializationStore with calendar_store.root ==
    roots.calendar_data and calendar_store.daily_reports_root ==
    roots.daily_reports. For schema v2, a None calendar_store is valid;
    an exact LocalCalendarMaterializationStore is root-validated the same
    way but never read from during binding. Any mismatch, wrong type,
    subclass, or hostile attribute failure while checking these
    relationships fails closed with the one static _ERR_BINDING message
    before calendar_store.get or either delegated service is ever
    called.

    Only after binding succeeds does routing run (which may perform v1's
    exact one get call), and only after routing succeeds is
    run_daily_pipeline_and_publish_state_from_pinned_gcs_run_spec called,
    exactly once, with the exact reconstructed spec, routed calendar
    materialization, exact roots/bucket, and every reader/store/run_store/
    writer dependency forwarded unchanged by identity. Its exact
    CompletedPinnedGCSStatePublication return value is forwarded
    unchanged; PinnedGCSStatePublicationServiceError and any
    BaseException it raises propagate completely unchanged, never caught
    here.

    Every ordinary failure at the file-load, reconstruction/routing,
    root/store binding, or v1 calendar-lookup stage (never BaseException)
    -- including one that happens to already be a
    PinnedGCSRunFileBoundaryError -- is caught uniformly, discarded
    without being retained, and collapses into one fresh, static,
    stage-specific PinnedGCSRunFileBoundaryError raised only after the
    guarding try/except has fully exited, so both __cause__ and
    __context__ are exactly None.
    """

    spec = _load_and_reconstruct_spec(spec_path)

    roots_validation_failed = False
    verified_roots: PipelineStateRoots | None = None
    try:
        if type(roots) is not PipelineStateRoots:
            raise PinnedGCSRunFileBoundaryError(_ERR_BINDING)
        verified_roots = PipelineStateRoots(
            calendar_data=roots.calendar_data,
            identity_registry=roots.identity_registry,
            historical_prices=roots.historical_prices,
            daily_reports=roots.daily_reports,
            reference_data=roots.reference_data,
            daily_pipeline=roots.daily_pipeline,
        )
    except Exception:
        roots_validation_failed = True
    if roots_validation_failed or verified_roots is None:
        raise PinnedGCSRunFileBoundaryError(_ERR_BINDING)

    binding_failure = False
    try:
        if type(reference_store) is not LocalReferenceArtifactStore:
            raise PinnedGCSRunFileBoundaryError(_ERR_BINDING)
        if not _path_matches_exactly(reference_store.root, verified_roots.reference_data):
            raise PinnedGCSRunFileBoundaryError(_ERR_BINDING)

        if type(daily_store) is not LocalDailyBundleArtifactStore:
            raise PinnedGCSRunFileBoundaryError(_ERR_BINDING)
        if not _path_matches_exactly(daily_store.root, verified_roots.daily_reports):
            raise PinnedGCSRunFileBoundaryError(_ERR_BINDING)

        if type(historical_store) is not LocalHistoricalPriceArtifactStore:
            raise PinnedGCSRunFileBoundaryError(_ERR_BINDING)
        if (
            not _path_matches_exactly(
                historical_store.root, verified_roots.historical_prices
            )
            or not _path_matches_exactly(
                historical_store.daily_reports_root, verified_roots.daily_reports
            )
        ):
            raise PinnedGCSRunFileBoundaryError(_ERR_BINDING)

        if type(identity_store) is not LocalIdentityRegistryStore:
            raise PinnedGCSRunFileBoundaryError(_ERR_BINDING)
        if (
            not _path_matches_exactly(identity_store.root, verified_roots.identity_registry)
            or not _path_matches_exactly(
                identity_store.reference_data_root, verified_roots.reference_data
            )
        ):
            raise PinnedGCSRunFileBoundaryError(_ERR_BINDING)

        if type(adjudication_store) is not LocalIdentityAdjudicationQueueStore:
            raise PinnedGCSRunFileBoundaryError(_ERR_BINDING)
        if (
            not _path_matches_exactly(
                adjudication_store.root, verified_roots.identity_registry
            )
            or adjudication_store.registry_store is not identity_store
        ):
            raise PinnedGCSRunFileBoundaryError(_ERR_BINDING)

        if type(run_store) is not LocalDailyPipelineRunStore:
            raise PinnedGCSRunFileBoundaryError(_ERR_BINDING)
        if not _path_matches_exactly(run_store.root, verified_roots.daily_pipeline):
            raise PinnedGCSRunFileBoundaryError(_ERR_BINDING)

        if spec.schema_version == PINNED_GCS_RUN_SPEC_SCHEMA_VERSION:
            if type(calendar_store) is not LocalCalendarMaterializationStore:
                raise PinnedGCSRunFileBoundaryError(_ERR_BINDING)
            if (
                not _path_matches_exactly(calendar_store.root, verified_roots.calendar_data)
                or not _path_matches_exactly(
                    calendar_store.daily_reports_root, verified_roots.daily_reports
                )
            ):
                raise PinnedGCSRunFileBoundaryError(_ERR_BINDING)
        elif spec.schema_version == PINNED_GCS_RUN_SPEC_SCHEMA_VERSION_WITH_CALENDAR:
            if calendar_store is not None:
                if type(calendar_store) is not LocalCalendarMaterializationStore:
                    raise PinnedGCSRunFileBoundaryError(_ERR_BINDING)
                if (
                    not _path_matches_exactly(calendar_store.root, verified_roots.calendar_data)
                    or not _path_matches_exactly(
                        calendar_store.daily_reports_root, verified_roots.daily_reports
                    )
                ):
                    raise PinnedGCSRunFileBoundaryError(_ERR_BINDING)
        else:
            raise PinnedGCSRunFileBoundaryError(_ERR_BINDING)
    except Exception:
        binding_failure = True
    if binding_failure:
        raise PinnedGCSRunFileBoundaryError(_ERR_BINDING)

    calendar_materialization = _routed_calendar_materialization(spec, calendar_store)

    return run_daily_pipeline_and_publish_state_from_pinned_gcs_run_spec(
        spec,
        calendar_materialization,
        roots,
        bucket,
        reader=reader,
        reference_store=reference_store,
        daily_store=daily_store,
        historical_store=historical_store,
        identity_store=identity_store,
        adjudication_store=adjudication_store,
        run_store=run_store,
        writer=writer,
    )
