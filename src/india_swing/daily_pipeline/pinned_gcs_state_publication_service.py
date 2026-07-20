from __future__ import annotations

from dataclasses import dataclass

from india_swing.calendar_data.materialization_store import StoredCalendarMaterialization
from india_swing.daily_reports.artifact_store import LocalDailyBundleArtifactStore
from india_swing.historical_prices.artifact_store import LocalHistoricalPriceArtifactStore
from india_swing.identity_registry.adjudication_store import LocalIdentityAdjudicationQueueStore
from india_swing.identity_registry.artifact_store import LocalIdentityRegistryStore
from india_swing.reference_data.artifact_store import LocalReferenceArtifactStore

from .acquisition import GCSObjectReader
from .models import DailyPipelineRun
from .pinned_gcs_run_service import run_daily_pipeline_from_pinned_gcs_run_spec
from .pinned_gcs_run_spec import PinnedGCSRunSpec
from .state_inventory import (
    PipelineStateInventory,
    PipelineStateRoots,
    build_pipeline_state_inventory,
)
from .state_publication import (
    CompletedPipelineStatePublication,
    StateObjectWriter,
    _validate_bucket,
    publish_pipeline_state,
)
from .store import LocalDailyPipelineRunStore


_ERROR_INVALID_INPUT = "pinned gcs state publication service input is invalid"
_ERROR_RUN_EXECUTION = "pinned gcs state publication service run execution failed"
_ERROR_INVENTORY_CONSTRUCTION = (
    "pinned gcs state publication service inventory construction failed"
)
_ERROR_PUBLICATION = "pinned gcs state publication service publication failed"
_ERROR_AGGREGATE_VERIFICATION = (
    "pinned gcs state publication service aggregate verification failed"
)


class PinnedGCSStatePublicationServiceError(Exception):
    pass


@dataclass(frozen=True, slots=True)
class CompletedPinnedGCSStatePublication:
    """One independently cross-verified aggregate binding a pinned-GCS
    daily run, its canonical state inventory, and its immutable GCS
    publication together.

    Runtime evidence only: a self-consistent graph is integrity evidence,
    not independent provenance or proof that upstream operator inputs
    were truthful.
    """

    bucket: str
    run: DailyPipelineRun
    inventory: PipelineStateInventory
    publication: CompletedPipelineStatePublication

    def __post_init__(self) -> None:
        if type(self.bucket) is not str:
            raise PinnedGCSStatePublicationServiceError(_ERROR_AGGREGATE_VERIFICATION)
        bucket_failed = False
        try:
            _validate_bucket(self.bucket)
        except Exception:
            bucket_failed = True
        if bucket_failed:
            raise PinnedGCSStatePublicationServiceError(_ERROR_AGGREGATE_VERIFICATION)

        if type(self.run) is not DailyPipelineRun:
            raise PinnedGCSStatePublicationServiceError(_ERROR_AGGREGATE_VERIFICATION)
        run_failed = False
        try:
            self.run.verify_content_identity()
        except Exception:
            run_failed = True
        if run_failed:
            raise PinnedGCSStatePublicationServiceError(_ERROR_AGGREGATE_VERIFICATION)

        if type(self.inventory) is not PipelineStateInventory:
            raise PinnedGCSStatePublicationServiceError(_ERROR_AGGREGATE_VERIFICATION)
        inventory_failed = False
        try:
            self.inventory.verify_content_identity()
        except Exception:
            inventory_failed = True
        if inventory_failed:
            raise PinnedGCSStatePublicationServiceError(_ERROR_AGGREGATE_VERIFICATION)

        if type(self.publication) is not CompletedPipelineStatePublication:
            raise PinnedGCSStatePublicationServiceError(_ERROR_AGGREGATE_VERIFICATION)

        # CompletedPipelineStatePublication intentionally has no verify
        # method of its own; defensively reconstruct a fresh instance from
        # its own current field values, mirroring how state_publication.py
        # and state_inventory.py reconstruct nested values before trusting
        # them.
        reconstruction_failed = False
        reconstructed_publication: CompletedPipelineStatePublication | None = None
        try:
            reconstructed_publication = CompletedPipelineStatePublication(
                manifest=self.publication.manifest,
                publication_object=self.publication.publication_object,
            )
        except Exception:
            reconstruction_failed = True
        if reconstruction_failed or reconstructed_publication is None:
            raise PinnedGCSStatePublicationServiceError(_ERROR_AGGREGATE_VERIFICATION)
        object.__setattr__(self, "publication", reconstructed_publication)

        manifest = self.publication.manifest
        if (
            manifest.run_id != self.run.run_id
            or manifest.previous_run_id != self.run.previous_run_id
            or manifest.market_session != self.run.market_session
            or manifest.cutoff != self.run.cutoff
        ):
            raise PinnedGCSStatePublicationServiceError(_ERROR_AGGREGATE_VERIFICATION)
        if (
            self.inventory.run_id != self.run.run_id
            or self.inventory.previous_run_id != self.run.previous_run_id
            or self.inventory.market_session != self.run.market_session
            or self.inventory.cutoff != self.run.cutoff
        ):
            raise PinnedGCSStatePublicationServiceError(_ERROR_AGGREGATE_VERIFICATION)
        if manifest.inventory_id != self.inventory.inventory_id:
            raise PinnedGCSStatePublicationServiceError(_ERROR_AGGREGATE_VERIFICATION)
        if manifest.bucket != self.bucket:
            raise PinnedGCSStatePublicationServiceError(_ERROR_AGGREGATE_VERIFICATION)


def run_daily_pipeline_and_publish_state_from_pinned_gcs_run_spec(
    spec: PinnedGCSRunSpec,
    calendar_materialization: StoredCalendarMaterialization | None,
    roots: PipelineStateRoots,
    bucket: str,
    *,
    reader: GCSObjectReader,
    reference_store: LocalReferenceArtifactStore,
    daily_store: LocalDailyBundleArtifactStore,
    historical_store: LocalHistoricalPriceArtifactStore,
    identity_store: LocalIdentityRegistryStore,
    adjudication_store: LocalIdentityAdjudicationQueueStore,
    run_store: LocalDailyPipelineRunStore,
    writer: StateObjectWriter,
) -> CompletedPinnedGCSStatePublication:
    """Executes one exact pinned-GCS daily run, builds the canonical
    six-root state inventory for its result, publishes that inventory
    through the immutable state-publication boundary, and returns one
    independently cross-verified aggregate.

    This is a pure composition seam: it never constructs a reader,
    writer, store, client, spec, calendar, or roots object, and never
    derives configuration from the environment or a clock. It delegates
    to run_daily_pipeline_from_pinned_gcs_run_spec, build_pipeline_state_
    inventory, and publish_pipeline_state exactly once each, in that
    order, never duplicating their own inventory walk, canonical codecs,
    GCS create-or-verify logic, or pinned-run validation.

    A successful local DailyPipelineRun is not durable cloud completion:
    if inventory construction or publication fails after the run already
    executed, this function returns no completion and performs no
    rollback, retry, or deletion of anything the run or a partial
    publication attempt already persisted -- only the final immutable
    publication-manifest object, verified by publish_pipeline_state and
    re-verified here, represents durable publication completion.

    Every ordinary failure (never BaseException) collapses to one of five
    static, sanitized PinnedGCSStatePublicationServiceError messages --
    invalid input, run execution, inventory construction, publication, and
    aggregate verification -- so no ID, date, path, bucket, hash,
    generation, content, or nested exception text can leak through this
    boundary, and both __cause__ and __context__ are always None.
    """

    if type(spec) is not PinnedGCSRunSpec:
        raise PinnedGCSStatePublicationServiceError(_ERROR_INVALID_INPUT)
    if type(roots) is not PipelineStateRoots:
        raise PinnedGCSStatePublicationServiceError(_ERROR_INVALID_INPUT)

    input_failed = False
    validated_bucket = ""
    try:
        validated_bucket = _validate_bucket(bucket)
    except Exception:
        input_failed = True
    if input_failed:
        raise PinnedGCSStatePublicationServiceError(_ERROR_INVALID_INPUT)
    bucket = validated_bucket

    run_failed = False
    run: DailyPipelineRun | None = None
    try:
        run = run_daily_pipeline_from_pinned_gcs_run_spec(
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
        if type(run) is not DailyPipelineRun:
            raise PinnedGCSStatePublicationServiceError(_ERROR_RUN_EXECUTION)
        run.verify_content_identity()
    except Exception:
        run_failed = True
    if run_failed:
        raise PinnedGCSStatePublicationServiceError(_ERROR_RUN_EXECUTION)

    inventory_failed = False
    inventory: PipelineStateInventory | None = None
    try:
        inventory = build_pipeline_state_inventory(run, roots)
        if type(inventory) is not PipelineStateInventory:
            raise PinnedGCSStatePublicationServiceError(_ERROR_INVENTORY_CONSTRUCTION)
        inventory.verify_content_identity()
        if (
            inventory.run_id != run.run_id
            or inventory.previous_run_id != run.previous_run_id
            or inventory.market_session != run.market_session
            or inventory.cutoff != run.cutoff
        ):
            raise PinnedGCSStatePublicationServiceError(_ERROR_INVENTORY_CONSTRUCTION)
    except Exception:
        inventory_failed = True
    if inventory_failed:
        raise PinnedGCSStatePublicationServiceError(_ERROR_INVENTORY_CONSTRUCTION)

    publication_failed = False
    publication: CompletedPipelineStatePublication | None = None
    try:
        publication = publish_pipeline_state(run, inventory, roots, bucket, writer)
        if type(publication) is not CompletedPipelineStatePublication:
            raise PinnedGCSStatePublicationServiceError(_ERROR_PUBLICATION)
    except Exception:
        publication_failed = True
    if publication_failed:
        raise PinnedGCSStatePublicationServiceError(_ERROR_PUBLICATION)

    aggregate_failed = False
    completed: CompletedPinnedGCSStatePublication | None = None
    try:
        completed = CompletedPinnedGCSStatePublication(
            bucket=bucket,
            run=run,
            inventory=inventory,
            publication=publication,
        )
    except Exception:
        aggregate_failed = True
    if aggregate_failed or completed is None:
        raise PinnedGCSStatePublicationServiceError(_ERROR_AGGREGATE_VERIFICATION)
    return completed
