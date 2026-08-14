"""Exact-input job service for the first forward-paper operational graph."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from typing import Iterable, Mapping, Protocol

from india_swing.corporate_actions.models import CorporateActionSnapshot
from india_swing.daily_pipeline.state_publication import StateObjectWriter
from india_swing.evaluation.nse_archive_research_dataset import (
    NseArchiveResearchDataset,
)
from india_swing.evaluation.nse_archive_research_price_stream import (
    NseArchiveResearchPriceStreamSession,
    iter_nse_archive_research_price_stream_sessions_from,
)
from india_swing.evaluation.nse_archive_research_identity_checkpoint_runtime import (
    iter_nse_archive_research_price_stream_sessions_from_checkpoint,
)
from india_swing.evaluation.nse_archive_research_identity_checkpoint import (
    NseArchiveResearchIdentityCheckpoint,
)
from india_swing.forward_paper.history import (
    ForwardPaperHistoryWindowSpec,
    ForwardPaperRawHistoryWindow,
    build_forward_paper_raw_history_window,
)
from india_swing.forward_paper.operational import (
    ForwardPaperOperationalResearchGraph,
    _assemble_forward_paper_operational_research_graph_from_verified_inputs,
)
from india_swing.forward_paper.operational_gcs import (
    CompletedForwardPaperOperationalGraphPublication,
    ForwardPaperCorporateActionSnapshotResolver,
    ForwardPaperEffectiveTickPanelResolver,
    _publish_forward_paper_operational_graph_from_verified_graph,
)
from india_swing.identity import content_id
from india_swing.market_data.nse_archive_range import (
    NseHistoricalArchiveSnapshotReader,
)
from india_swing.forward_paper.signal_tick import (
    is_forward_paper_tick_panel,
)


FORWARD_PAPER_OPERATIONAL_JOB_REQUEST_SCHEMA_VERSION = (
    "forward-paper-operational-job-request-v1"
)
FORWARD_PAPER_OPERATIONAL_JOB_RECEIPT_SCHEMA_VERSION = (
    "forward-paper-operational-job-receipt-v1"
)
FORWARD_PAPER_OPERATIONAL_JOB_POLICY_VERSION = "exact-artifacts-paper-research-only-v1"

_SHA256 = re.compile(r"[0-9a-f]{64}")
_BUCKET = re.compile(r"[a-z0-9][a-z0-9._-]{1,61}[a-z0-9]")


class ForwardPaperOperationalJobError(ValueError):
    """One static failure at the exact-artifact job boundary."""


class NseArchiveResearchDatasetResolver(Protocol):
    def get(self, dataset_id: str) -> NseArchiveResearchDataset: ...


class ForwardPaperOperationalStageObserver(Protocol):
    def __call__(
        self,
        stage: str,
        status: str,
        details: Mapping[str, int],
    ) -> None: ...


def _fail(message: str) -> None:
    raise ForwardPaperOperationalJobError(message)


def _sha(value: object) -> str:
    if type(value) is not str or _SHA256.fullmatch(value) is None:
        _fail("forward paper operational job identity is invalid")
    return value


def _observe(
    observer: ForwardPaperOperationalStageObserver | None,
    stage: str,
    status: str,
    **details: int,
) -> None:
    if observer is not None:
        observer(stage, status, details)


def _utc(value: object) -> datetime:
    if (
        type(value) is not datetime
        or value.tzinfo is None
        or value.utcoffset() is None
    ):
        _fail("forward paper operational job cutoff is invalid")
    return value.astimezone(timezone.utc)


@dataclass(frozen=True, slots=True)
class ForwardPaperOperationalJobRequest:
    dataset_id: str
    signal_session: date
    decision_cutoff: datetime
    expected_market_sessions: tuple[date, ...]
    corporate_action_snapshot_id: str
    tick_panel_id: str
    bucket: str
    schema_version: str = FORWARD_PAPER_OPERATIONAL_JOB_REQUEST_SCHEMA_VERSION
    policy_version: str = FORWARD_PAPER_OPERATIONAL_JOB_POLICY_VERSION
    request_id: str = field(init=False)

    def __post_init__(self) -> None:
        self._validate()
        object.__setattr__(self, "decision_cutoff", _utc(self.decision_cutoff))
        object.__setattr__(self, "request_id", self._calculated_id())

    def _spec(self) -> ForwardPaperHistoryWindowSpec:
        return ForwardPaperHistoryWindowSpec(
            dataset_id=self.dataset_id,
            signal_session=self.signal_session,
            decision_cutoff=_utc(self.decision_cutoff),
            expected_market_sessions=self.expected_market_sessions,
        )

    def _validate(self) -> None:
        for value in (
            self.dataset_id,
            self.corporate_action_snapshot_id,
            self.tick_panel_id,
        ):
            _sha(value)
        if type(self.signal_session) is not date:
            _fail("forward paper operational job signal session is invalid")
        _utc(self.decision_cutoff)
        failed = False
        try:
            self._spec().verify_content_identity()
        except Exception:
            failed = True
        if failed:
            _fail("forward paper operational job history specification is invalid")
        if type(self.bucket) is not str or _BUCKET.fullmatch(self.bucket) is None:
            _fail("forward paper operational job bucket is invalid")
        if self.schema_version != FORWARD_PAPER_OPERATIONAL_JOB_REQUEST_SCHEMA_VERSION:
            _fail("forward paper operational job request schema is invalid")
        if self.policy_version != FORWARD_PAPER_OPERATIONAL_JOB_POLICY_VERSION:
            _fail("forward paper operational job policy is invalid")

    @property
    def history_spec(self) -> ForwardPaperHistoryWindowSpec:
        return self._spec()

    def _calculated_id(self) -> str:
        spec = self._spec()
        return content_id(
            {
                "schema": self.schema_version,
                "policy_version": self.policy_version,
                "history_spec_id": spec.spec_id,
                "corporate_action_snapshot_id": self.corporate_action_snapshot_id,
                "tick_panel_id": self.tick_panel_id,
                "bucket": self.bucket,
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        self._validate()
        if self.request_id != self._calculated_id():
            _fail("forward paper operational job request identity failed")


@dataclass(frozen=True, slots=True)
class NseArchiveForwardPaperHistoryBuilder:
    datasets: NseArchiveResearchDatasetResolver
    reader: NseHistoricalArchiveSnapshotReader
    identity_checkpoint: NseArchiveResearchIdentityCheckpoint | None = None

    def sessions(
        self, spec: ForwardPaperHistoryWindowSpec
    ) -> Iterable[NseArchiveResearchPriceStreamSession]:
        if type(spec) is not ForwardPaperHistoryWindowSpec:
            _fail("forward paper operational history specification is invalid")
        failed = False
        dataset = sessions = None
        try:
            spec.verify_content_identity()
            dataset = self.datasets.get(spec.dataset_id)
            if (
                type(dataset) is not NseArchiveResearchDataset
                or dataset.dataset_id != spec.dataset_id
            ):
                raise ValueError
            dataset.verify_content_identity()
            if self.identity_checkpoint is None:
                sessions = iter_nse_archive_research_price_stream_sessions_from(
                    dataset,
                    self.reader,
                    start_session=spec.expected_market_sessions[0],
                )
            else:
                sessions = iter_nse_archive_research_price_stream_sessions_from_checkpoint(
                    dataset,
                    self.reader,
                    start_session=spec.expected_market_sessions[0],
                    checkpoint=self.identity_checkpoint,
                )
        except Exception:
            failed = True
        if failed or sessions is None:
            _fail("forward paper operational raw history reconstruction failed safely")
        return sessions

    def build(self, spec: ForwardPaperHistoryWindowSpec) -> ForwardPaperRawHistoryWindow:
        failed = False
        window = None
        try:
            window = build_forward_paper_raw_history_window(spec, self.sessions(spec))
            if (
                type(window) is not ForwardPaperRawHistoryWindow
                or window.spec.spec_id != spec.spec_id
                or window.window_id != window._calculated_id()
            ):
                raise ValueError
        except Exception:
            failed = True
        if failed or window is None:
            _fail("forward paper operational raw history reconstruction failed safely")
        return window


@dataclass(frozen=True, slots=True)
class ForwardPaperOperationalJobReceipt:
    request: ForwardPaperOperationalJobRequest
    graph: ForwardPaperOperationalResearchGraph
    publication: CompletedForwardPaperOperationalGraphPublication
    schema_version: str = FORWARD_PAPER_OPERATIONAL_JOB_RECEIPT_SCHEMA_VERSION
    policy_version: str = FORWARD_PAPER_OPERATIONAL_JOB_POLICY_VERSION
    receipt_id: str = field(init=False)

    def __post_init__(self) -> None:
        self._validate()
        object.__setattr__(self, "receipt_id", self._calculated_id())

    def _validate(self) -> None:
        if type(self.request) is not ForwardPaperOperationalJobRequest:
            _fail("forward paper operational job receipt request is invalid")
        if type(self.graph) is not ForwardPaperOperationalResearchGraph:
            _fail("forward paper operational job receipt graph is invalid")
        if (
            type(self.publication)
            is not CompletedForwardPaperOperationalGraphPublication
        ):
            _fail("forward paper operational job receipt publication is invalid")
        failed = False
        try:
            self.request.verify_content_identity()
            self.graph.verify_content_identity()
            self.publication.manifest.verify_content_identity()
            CompletedForwardPaperOperationalGraphPublication(
                manifest=self.publication.manifest,
                manifest_object=self.publication.manifest_object,
            )
        except Exception:
            failed = True
        if failed:
            _fail("forward paper operational job receipt evidence failed verification")
        manifest = self.publication.manifest
        if (
            self.graph.source_window.spec.spec_id != self.request.history_spec.spec_id
            or self.graph.corporate_actions.snapshot_id
            != self.request.corporate_action_snapshot_id
            or self.graph.tick_panel.panel_id != self.request.tick_panel_id
            or manifest.graph_id != self.graph.graph_id
            or manifest.bucket != self.request.bucket
            or manifest.source_spec_id != self.request.history_spec.spec_id
        ):
            _fail("forward paper operational job receipt lineage is invalid")
        if self.schema_version != FORWARD_PAPER_OPERATIONAL_JOB_RECEIPT_SCHEMA_VERSION:
            _fail("forward paper operational job receipt schema is invalid")
        if self.policy_version != FORWARD_PAPER_OPERATIONAL_JOB_POLICY_VERSION:
            _fail("forward paper operational job receipt policy is invalid")

    def _calculated_id(self) -> str:
        return content_id(
            {
                "schema": self.schema_version,
                "policy_version": self.policy_version,
                "request_id": self.request.request_id,
                "graph_id": self.graph.graph_id,
                "manifest_id": self.publication.manifest.manifest_id,
                "manifest_object_name": self.publication.manifest_object.object_name,
                "manifest_generation": self.publication.manifest_object.generation,
                "manifest_sha256": self.publication.manifest_object.sha256,
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        self._validate()
        if self.receipt_id != self._calculated_id():
            _fail("forward paper operational job receipt identity failed")

    @classmethod
    def _from_freshly_verified_run(
        cls,
        *,
        request: ForwardPaperOperationalJobRequest,
        graph: ForwardPaperOperationalResearchGraph,
        publication: CompletedForwardPaperOperationalGraphPublication,
    ) -> "ForwardPaperOperationalJobReceipt":
        value = object.__new__(cls)
        object.__setattr__(value, "request", request)
        object.__setattr__(value, "graph", graph)
        object.__setattr__(value, "publication", publication)
        object.__setattr__(
            value,
            "schema_version",
            FORWARD_PAPER_OPERATIONAL_JOB_RECEIPT_SCHEMA_VERSION,
        )
        object.__setattr__(
            value,
            "policy_version",
            FORWARD_PAPER_OPERATIONAL_JOB_POLICY_VERSION,
        )
        object.__setattr__(value, "receipt_id", value._calculated_id())
        return value

    @property
    def collection_only(self) -> bool:
        return True

    @property
    def paper_trade_eligible(self) -> bool:
        return False

    @property
    def notification_eligible(self) -> bool:
        return False

    @property
    def execution_eligible(self) -> bool:
        return False


def run_forward_paper_operational_job(
    *,
    request: ForwardPaperOperationalJobRequest,
    history_builder: NseArchiveForwardPaperHistoryBuilder,
    corporate_actions: ForwardPaperCorporateActionSnapshotResolver,
    tick_panels: ForwardPaperEffectiveTickPanelResolver,
    writer: StateObjectWriter,
    stage_observer: ForwardPaperOperationalStageObserver | None = None,
) -> ForwardPaperOperationalJobReceipt:
    if type(request) is not ForwardPaperOperationalJobRequest:
        _fail("forward paper operational job request is invalid")
    if type(history_builder) is not NseArchiveForwardPaperHistoryBuilder:
        _fail("forward paper operational history builder is invalid")
    request.verify_content_identity()
    failed = False
    source = actions = ticks = graph = publication = None
    try:
        _observe(stage_observer, "history_reconstruction", "started")
        source = history_builder.build(request.history_spec)
        _observe(
            stage_observer,
            "history_reconstruction",
            "completed",
            consumed_session_count=source.consumed_session_count,
            signal_subject_count=source.signal_subject_count,
        )
        _observe(stage_observer, "evidence_resolution", "started")
        actions = corporate_actions.get(request.corporate_action_snapshot_id)
        ticks = tick_panels.get(request.tick_panel_id)
        if (
            type(actions) is not CorporateActionSnapshot
            or actions.snapshot_id != request.corporate_action_snapshot_id
            or not is_forward_paper_tick_panel(ticks)
            or ticks.panel_id != request.tick_panel_id
        ):
            raise ValueError
        actions.verify_content_identity()
        ticks.verify_content_identity()
        _observe(stage_observer, "evidence_resolution", "completed")
        _observe(stage_observer, "graph_assembly", "started")
        graph = _assemble_forward_paper_operational_research_graph_from_verified_inputs(
            source_window=source,
            corporate_actions=actions,
            tick_panel=ticks,
            stage_observer=stage_observer,
        )
        _observe(
            stage_observer,
            "graph_assembly",
            "completed",
            computed_feature_count=(
                graph.technical_feature_window.computed_feature_count
            ),
            blocked_feature_count=(
                graph.technical_feature_window.blocked_feature_count
            ),
        )
        _observe(stage_observer, "publication", "started")
        publication = _publish_forward_paper_operational_graph_from_verified_graph(
            graph=graph,
            bucket=request.bucket,
            writer=writer,
        )
        _observe(stage_observer, "publication", "completed")
    except Exception:
        failed = True
    if failed or graph is None or publication is None:
        _fail("forward paper operational job failed safely")
    return ForwardPaperOperationalJobReceipt._from_freshly_verified_run(
        request=request,
        graph=graph,
        publication=publication,
    )
