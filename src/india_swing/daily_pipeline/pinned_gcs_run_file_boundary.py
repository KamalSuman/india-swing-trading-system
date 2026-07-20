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
from .store import LocalDailyPipelineRunStore


class PinnedGCSRunFileBoundaryError(Exception):
    pass


_ERR_LOAD = "pinned gcs run spec file could not be loaded"
_ERR_SCHEMA = "pinned gcs run file boundary schema routing failed"
_ERR_CALENDAR = "pinned gcs run file boundary calendar acquisition failed"


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

    For PINNED_GCS_RUN_SPEC_SCHEMA_VERSION (v1), calendar_store must be
    exact LocalCalendarMaterializationStore; this function calls
    calendar_store.get(spec.calendar_materialization_id) exactly once --
    there is no listing, latest selection, fallback, retry, or inferred ID
    -- and passes that exact returned StoredCalendarMaterialization
    positionally to the service.

    For PINNED_GCS_RUN_SPEC_SCHEMA_VERSION_WITH_CALENDAR (v2),
    calendar_store may be exactly None or exact
    LocalCalendarMaterializationStore; it is never read from (no get call,
    no other member access) even when the caller supplies an exact,
    unused store instance -- this function always passes exactly None as
    the service's calendar_materialization positional argument for v2, so
    the already-integrated service remains the sole calendar-acquisition
    authority and there is never dual calendar authority. Any other
    calendar_store value for v2 -- including a subclass or a shaped proxy
    -- is rejected before the service is ever called.

    A loaded value that is not exact PinnedGCSRunSpec fails immediately.
    Otherwise, a fresh PinnedGCSRunSpec is independently reconstructed from
    every one of the loaded value's retained fields (including
    calendar_request) before any routing decision, get call, or service
    call is made, and only that reconstructed snapshot is used afterward
    -- so a wrong/subclass/shaped loaded spec, a post-construction
    mutation to any outer field (schema_version to bool True/False, an
    unsupported int, an int subclass, or an equality-poisoned object;
    calendar_materialization_id; cutoff; market_session; previous_run_id),
    or a mutation to any nested manifest_request/trusted_binding/
    calendar_request field, is rejected by this reconstruction before it
    can influence routing or reach calendar_store.get. Routing itself
    checks the reconstructed schema_version's exact type (never relying on
    bool/int equality such as True == 1) before comparing it against the
    two committed schema constants; any other value fails the same way.
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

    loaded_spec = load_pinned_gcs_run_spec_file(spec_path)

    if type(loaded_spec) is not PinnedGCSRunSpec:
        raise PinnedGCSRunFileBoundaryError(_ERR_SCHEMA)

    routing_failure = False
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
        routing_failure = True
    if routing_failure:
        raise PinnedGCSRunFileBoundaryError(_ERR_SCHEMA)

    calendar_materialization = None

    if spec.schema_version == PINNED_GCS_RUN_SPEC_SCHEMA_VERSION:
        if type(calendar_store) is not LocalCalendarMaterializationStore:
            raise PinnedGCSRunFileBoundaryError(_ERR_SCHEMA)

        get_failure = False
        try:
            calendar_materialization = calendar_store.get(spec.calendar_materialization_id)
        except Exception:
            get_failure = True
        if get_failure:
            raise PinnedGCSRunFileBoundaryError(_ERR_CALENDAR)

    elif spec.schema_version == PINNED_GCS_RUN_SPEC_SCHEMA_VERSION_WITH_CALENDAR:
        if (
            calendar_store is not None
            and type(calendar_store) is not LocalCalendarMaterializationStore
        ):
            raise PinnedGCSRunFileBoundaryError(_ERR_SCHEMA)
        calendar_materialization = None

    else:
        raise PinnedGCSRunFileBoundaryError(_ERR_SCHEMA)

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
