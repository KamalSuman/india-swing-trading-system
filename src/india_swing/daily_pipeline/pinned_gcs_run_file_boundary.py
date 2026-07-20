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
    PinnedGCSRunSpec,
    parse_pinned_gcs_run_spec,
)
from .store import LocalDailyPipelineRunStore


class PinnedGCSRunFileBoundaryError(Exception):
    pass


_ERR_LOAD = "pinned gcs run spec file could not be loaded"
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
    authority on spec byte-size and schema validation. Every ordinary
    path/filesystem/parse failure (never BaseException) collapses into one
    static, sanitized PinnedGCSRunFileBoundaryError with chaining
    suppressed.
    """

    if type(spec_path) is not str or not spec_path or "\x00" in spec_path:
        raise PinnedGCSRunFileBoundaryError(_ERR_LOAD)
    try:
        path = Path(spec_path)
        status = path.lstat()
        if not stat.S_ISREG(status.st_mode):
            raise PinnedGCSRunFileBoundaryError(_ERR_LOAD)
        with open(path, "rb") as handle:
            spec_bytes = handle.read(MAXIMUM_PINNED_GCS_RUN_SPEC_BYTES + 1)
        return parse_pinned_gcs_run_spec(spec_bytes)
    except PinnedGCSRunFileBoundaryError:
        raise
    except Exception:
        raise PinnedGCSRunFileBoundaryError(_ERR_LOAD) from None


def run_daily_pipeline_from_pinned_gcs_run_spec_file(
    spec_path: str,
    *,
    calendar_store: LocalCalendarMaterializationStore,
    reader: GCSObjectReader,
    reference_store: LocalReferenceArtifactStore,
    daily_store: LocalDailyBundleArtifactStore,
    historical_store: LocalHistoricalPriceArtifactStore,
    identity_store: LocalIdentityRegistryStore,
    adjudication_store: LocalIdentityAdjudicationQueueStore,
    run_store: LocalDailyPipelineRunStore,
) -> DailyPipelineRun:
    """Loads a PinnedGCSRunSpec from spec_path, acquires its exact pinned
    StoredCalendarMaterialization solely via
    calendar_store.get(spec.calendar_materialization_id), and delegates
    exactly once to run_daily_pipeline_from_pinned_gcs_run_spec with the
    caller-supplied reader/stores unchanged.

    calendar_store must be exact LocalCalendarMaterializationStore. There
    is no listing, latest selection, fallback, retry, or inferred ID: the
    single exact-ID get is the only calendar lookup this function performs.
    An ordinary get failure (never BaseException) collapses into one
    static, sanitized PinnedGCSRunFileBoundaryError with chaining
    suppressed. The delegated call is never wrapped in try/except -- its
    PinnedGCSRunServiceError propagates unchanged, and no BaseException is
    ever intercepted anywhere in this module.
    """

    spec = load_pinned_gcs_run_spec_file(spec_path)

    if type(calendar_store) is not LocalCalendarMaterializationStore:
        raise PinnedGCSRunFileBoundaryError(_ERR_CALENDAR)
    try:
        calendar_materialization = calendar_store.get(spec.calendar_materialization_id)
    except Exception:
        raise PinnedGCSRunFileBoundaryError(_ERR_CALENDAR) from None

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
