"""Exact-input service for one baseline/challenger forward-paper research run."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Mapping, Protocol

from india_swing.daily_pipeline.acquisition import GCSObjectReader
from india_swing.daily_pipeline.state_publication import StateObjectWriter
from india_swing.features.promoted_cross_section import PromotedCrossSectionConfig
from india_swing.identity import content_id

from .operational_gcs import (
    ForwardPaperCorporateActionSnapshotResolver,
    ForwardPaperEffectiveTickPanelResolver,
    ForwardPaperRawHistoryWindowResolver,
    restore_forward_paper_operational_graph,
)
from .research import (
    ForwardPaperBaselineChallengerRun,
    _run_forward_paper_baseline_challenger_research_from_verified_graph,
)
from .research_gcs import (
    CompletedForwardPaperResearchPublication,
    ForwardPaperOperationalManifestPin,
    _publish_forward_paper_research_run_from_verified_run,
)


FORWARD_PAPER_RESEARCH_JOB_REQUEST_SCHEMA_VERSION = (
    "forward-paper-research-job-request-v1"
)
FORWARD_PAPER_RESEARCH_JOB_RECEIPT_SCHEMA_VERSION = (
    "forward-paper-research-job-receipt-v1"
)
FORWARD_PAPER_RESEARCH_JOB_POLICY_VERSION = "exact-pinned-research-only-v1"

_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_BUCKET = re.compile(r"[a-z0-9][a-z0-9._-]{1,61}[a-z0-9]\Z")


class ForwardPaperResearchJobError(ValueError):
    """One static failure at the exact-pinned research job boundary."""


class ForwardPaperResearchStageObserver(Protocol):
    def __call__(
        self,
        stage: str,
        status: str,
        details: Mapping[str, int],
    ) -> None: ...


def _fail(message: str) -> None:
    raise ForwardPaperResearchJobError(message)


def _observe(
    observer: ForwardPaperResearchStageObserver | None,
    stage: str,
    status: str,
    **details: int,
) -> None:
    if observer is not None:
        observer(stage, status, details)


@dataclass(frozen=True, slots=True)
class ForwardPaperResearchJobRequest:
    source_pin: ForwardPaperOperationalManifestPin
    baseline_config: PromotedCrossSectionConfig
    challenger_config: PromotedCrossSectionConfig
    comparison_top_tiers: int
    output_bucket: str
    schema_version: str = FORWARD_PAPER_RESEARCH_JOB_REQUEST_SCHEMA_VERSION
    policy_version: str = FORWARD_PAPER_RESEARCH_JOB_POLICY_VERSION
    request_id: str = field(init=False)

    def __post_init__(self) -> None:
        self._validate()
        object.__setattr__(self, "request_id", self._calculated_id())

    def _validate(self) -> None:
        if (
            type(self.source_pin) is not ForwardPaperOperationalManifestPin
            or type(self.baseline_config) is not PromotedCrossSectionConfig
            or type(self.challenger_config) is not PromotedCrossSectionConfig
            or self.baseline_config.config_id == self.challenger_config.config_id
            or type(self.comparison_top_tiers) is not int
            or isinstance(self.comparison_top_tiers, bool)
            or self.comparison_top_tiers <= 0
            or type(self.output_bucket) is not str
            or _BUCKET.fullmatch(self.output_bucket) is None
            or self.schema_version != FORWARD_PAPER_RESEARCH_JOB_REQUEST_SCHEMA_VERSION
            or self.policy_version != FORWARD_PAPER_RESEARCH_JOB_POLICY_VERSION
        ):
            _fail("forward paper research job request is invalid")
        failed = False
        try:
            self.source_pin.verify_content_identity()
            self.baseline_config.verify_content_identity()
            self.challenger_config.verify_content_identity()
        except Exception:
            failed = True
        if failed:
            _fail("forward paper research job request failed verification")

    def _calculated_id(self) -> str:
        return content_id(
            {
                "schema": self.schema_version,
                "policy_version": self.policy_version,
                "source_pin_id": self.source_pin.pin_id,
                "baseline_config_id": self.baseline_config.config_id,
                "challenger_config_id": self.challenger_config.config_id,
                "comparison_top_tiers": self.comparison_top_tiers,
                "output_bucket": self.output_bucket,
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        self._validate()
        if self.request_id != self._calculated_id():
            _fail("forward paper research job request identity failed")


@dataclass(frozen=True, slots=True)
class ForwardPaperResearchJobReceipt:
    request: ForwardPaperResearchJobRequest
    run: ForwardPaperBaselineChallengerRun
    publication: CompletedForwardPaperResearchPublication
    schema_version: str = FORWARD_PAPER_RESEARCH_JOB_RECEIPT_SCHEMA_VERSION
    policy_version: str = FORWARD_PAPER_RESEARCH_JOB_POLICY_VERSION
    receipt_id: str = field(init=False)

    def __post_init__(self) -> None:
        self._validate()
        object.__setattr__(self, "receipt_id", self._calculated_id())

    def _validate(self) -> None:
        if (
            type(self.request) is not ForwardPaperResearchJobRequest
            or type(self.run) is not ForwardPaperBaselineChallengerRun
            or type(self.publication) is not CompletedForwardPaperResearchPublication
            or self.schema_version != FORWARD_PAPER_RESEARCH_JOB_RECEIPT_SCHEMA_VERSION
            or self.policy_version != FORWARD_PAPER_RESEARCH_JOB_POLICY_VERSION
        ):
            _fail("forward paper research job receipt is invalid")
        failed = False
        try:
            self.request.verify_content_identity()
            self.run.verify_content_identity()
            CompletedForwardPaperResearchPublication(
                manifest=self.publication.manifest,
                manifest_object=self.publication.manifest_object,
            )
        except Exception:
            failed = True
        if failed:
            _fail("forward paper research job receipt failed verification")
        manifest = self.publication.manifest
        if (
            self.run.source_graph.graph_id != self.request.source_pin.expected_graph_id
            or self.run.baseline.config.config_id
            != self.request.baseline_config.config_id
            or self.run.challenger.config.config_id
            != self.request.challenger_config.config_id
            or self.run.comparison_top_tiers != self.request.comparison_top_tiers
            or manifest.run_id != self.run.run_id
            or manifest.source_pin != self.request.source_pin
            or manifest.bucket != self.request.output_bucket
        ):
            _fail("forward paper research job receipt lineage is invalid")

    def _calculated_id(self) -> str:
        return content_id(
            {
                "schema": self.schema_version,
                "policy_version": self.policy_version,
                "request_id": self.request.request_id,
                "run_id": self.run.run_id,
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
            _fail("forward paper research job receipt identity failed")

    @classmethod
    def _from_freshly_verified_run(
        cls,
        *,
        request: ForwardPaperResearchJobRequest,
        run: ForwardPaperBaselineChallengerRun,
        publication: CompletedForwardPaperResearchPublication,
    ) -> "ForwardPaperResearchJobReceipt":
        value = object.__new__(cls)
        for field_name, item in (
            ("request", request),
            ("run", run),
            ("publication", publication),
            ("schema_version", FORWARD_PAPER_RESEARCH_JOB_RECEIPT_SCHEMA_VERSION),
            ("policy_version", FORWARD_PAPER_RESEARCH_JOB_POLICY_VERSION),
        ):
            object.__setattr__(value, field_name, item)
        object.__setattr__(value, "receipt_id", value._calculated_id())
        return value

    @property
    def collection_only(self) -> bool:
        return True

    @property
    def promotion_eligible(self) -> bool:
        return False

    @property
    def paper_trade_eligible(self) -> bool:
        return False

    @property
    def notification_eligible(self) -> bool:
        return False

    @property
    def execution_eligible(self) -> bool:
        return False


def run_forward_paper_research_job(
    *,
    request: ForwardPaperResearchJobRequest,
    reader: GCSObjectReader,
    history_windows: ForwardPaperRawHistoryWindowResolver,
    corporate_actions: ForwardPaperCorporateActionSnapshotResolver,
    tick_panels: ForwardPaperEffectiveTickPanelResolver,
    writer: StateObjectWriter,
    stage_observer: ForwardPaperResearchStageObserver | None = None,
) -> ForwardPaperResearchJobReceipt:
    if type(request) is not ForwardPaperResearchJobRequest:
        _fail("forward paper research job request is invalid")
    request.verify_content_identity()
    graph = run = publication = None
    failed = False
    try:
        pin = request.source_pin
        _observe(stage_observer, "operational_graph_restore", "started")
        graph = restore_forward_paper_operational_graph(
            expected_graph_id=pin.expected_graph_id,
            bucket=pin.bucket,
            manifest_object_name=pin.object_name,
            manifest_generation=pin.generation,
            manifest_sha256=pin.sha256,
            reader=reader,
            history_windows=history_windows,
            corporate_actions=corporate_actions,
            tick_panels=tick_panels,
            stage_observer=stage_observer,
        )
        _observe(
            stage_observer,
            "operational_graph_restore",
            "completed",
            computed_feature_count=graph.technical_feature_window.computed_feature_count,
            blocked_feature_count=graph.technical_feature_window.blocked_feature_count,
        )
        _observe(stage_observer, "baseline_challenger", "started")
        run = _run_forward_paper_baseline_challenger_research_from_verified_graph(
            source_graph=graph,
            baseline_config=request.baseline_config,
            challenger_config=request.challenger_config,
            comparison_top_tiers=request.comparison_top_tiers,
        )
        _observe(
            stage_observer,
            "baseline_challenger",
            "completed",
            baseline_top_count=run.baseline_top_count,
            challenger_top_count=run.challenger_top_count,
            overlap_count=run.overlap_count,
        )
        _observe(stage_observer, "research_publication", "started")
        publication = _publish_forward_paper_research_run_from_verified_run(
            run=run,
            source_pin=request.source_pin,
            bucket=request.output_bucket,
            writer=writer,
        )
        _observe(stage_observer, "research_publication", "completed")
    except Exception:
        failed = True
    if failed or graph is None or run is None or publication is None:
        _fail("forward paper research job failed safely")
    return ForwardPaperResearchJobReceipt._from_freshly_verified_run(
        request=request,
        run=run,
        publication=publication,
    )
