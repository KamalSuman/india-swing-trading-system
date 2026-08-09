"""HYP-002 quality pilot: session bootstrap boundary.

Bridges the chicken-and-egg gap between one already-persisted, successful
pre-open catalog capture and the constant-read
:class:`~india_swing.quality_pilot.resumable_service.QualityPilotResumableCaptureService`:
a session's quote/OHLCV universe is unknowable until its catalog capture
returns, yet :class:`~india_swing.quality_pilot.campaign_ledger
.QualityPilotCampaignPlan` correctly requires all four complete routes for
every included session. This module never recollects the catalog and never
invokes a collector of any kind -- it accepts one already-verified
:class:`~india_swing.quality_pilot.capture_runner.QualityPilotCaptureRunResult`
for the target session's ``CATALOG_PREOPEN`` capture as untrusted input,
derives the frozen K_EQ universe from its immutable payload, deterministically
builds the session's complete quote/OHLCV route, extends the campaign plan by
exactly that one session, rebases the completeness snapshot onto the
extended plan (audit-replaying any completed prior-session state exactly
once at the session boundary), and seals one transition that leaves the
existing resumable service ready to resume at the first 09:20 quote chunk.

This module performs no filesystem, ambient configuration, wall-clock
creation, network/client construction, Cloud SDK, Kite SDK, process,
scheduler, notification, strategy, research-result, trading, risk, or
capital capability of its own. It never lists a bucket, never resolves a
"head" plan or snapshot, and never retries or sleeps. Every artifact this
module produces remains permanently quality-only.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import date
from hashlib import sha256

from india_swing.daily_pipeline.acquisition import GCSObjectReader
from india_swing.daily_pipeline.state_publication import StateObjectWriter
from india_swing.identity import content_id
from india_swing.market_data.models import InstrumentBatch

from .campaign_ledger import QualityPilotCampaignCompletenessLedger, QualityPilotCampaignPlan
from .canonical_response import (
    EXCHANGE_NSE,
    MAXIMUM_QUOTE_REQUEST_KEYS,
    PILOT_PROTOCOL_SHA256,
    PROVIDER_ZERODHA_KITE,
    EndpointFamily,
    ObservationWindowSpec,
    ResponseClassification,
    ScheduledWindowKind,
)
from .capture_runner import (
    QualityPilotCampaignSpec,
    QualityPilotCaptureRunResult,
    QualityPilotCaptureSpec,
)
from .control_plane_store import (
    LoadedQualityPilotControlArtifact,
    LoadedQualityPilotLedgerTransition,
    PinnedQualityPilotControlArtifactRequest,
    PublishedQualityPilotControlArtifact,
    PublishedQualityPilotLedgerTransition,
    QualityPilotCompletenessSnapshot,
    QualityPilotControlArtifactKind,
    QualityPilotLedgerTransition,
    build_quality_pilot_completeness_snapshot,
    encode_quality_pilot_campaign_plan,
    encode_quality_pilot_completeness_snapshot,
    encode_quality_pilot_ledger_transition,
    pinned_quality_pilot_control_artifact_request,
    publish_quality_pilot_control_artifact,
    publish_quality_pilot_ledger_transition,
    read_pinned_quality_pilot_control_artifact,
    read_pinned_quality_pilot_ledger_transition,
)
from .control_plane_store import (
    PinnedQualityPilotLedgerTransitionRequest as _PinnedTransitionRequest,
)
from .resumable_service import audit_replay_quality_pilot_completeness_snapshot


QUALITY_PILOT_SESSION_BOOTSTRAP_REQUEST_SCHEMA_VERSION = "quality_pilot_session_bootstrap_request_v1"
QUALITY_PILOT_SESSION_BOOTSTRAP_RESULT_SCHEMA_VERSION = "quality_pilot_session_bootstrap_result_v1"

_SHA256_PATTERN = re.compile(r"[0-9a-f]{64}\Z")
_BUCKET_PATTERN = re.compile(r"[a-z0-9][a-z0-9\-_.]{1,61}[a-z0-9]\Z")


def _is_sha256(value: object) -> bool:
    return type(value) is str and _SHA256_PATTERN.fullmatch(value) is not None


class QualityPilotSessionBootstrapError(ValueError):
    """A session-bootstrap input, replay, or artifact failed a static trust rule."""


def _fail(message: str) -> None:
    raise QualityPilotSessionBootstrapError(message)


# ---------------------------------------------------------------------------
# Fixed fail-closed posture
# ---------------------------------------------------------------------------

_POSTURE_NAMES = (
    "quality_only",
    "counts_toward_o0",
    "counts_toward_clean_accumulation",
    "research_partition_eligible",
    "training_eligible",
    "feature_eligible",
    "label_eligible",
    "signal_eligible",
    "paper_trade_eligible",
    "notification_eligible",
    "execution_eligible",
    "capital_eligible",
)


def _posture_tree(value: object) -> dict[str, bool]:
    return {name: getattr(value, name) for name in _POSTURE_NAMES}


class _FixedPostureMixin:
    """Read-only, fixed fail-closed posture. Plain ``@property`` for every name,
    plus ``__slots__ = ()`` so no instance ever has a ``__dict__`` -- every
    name is a true data descriptor with no setter, so ``object.__setattr__``
    raises ``AttributeError`` immediately rather than silently succeeding.
    """

    __slots__ = ()

    @property
    def quality_only(self) -> bool:
        return True

    @property
    def counts_toward_o0(self) -> bool:
        return False

    @property
    def counts_toward_clean_accumulation(self) -> bool:
        return False

    @property
    def research_partition_eligible(self) -> bool:
        return False

    @property
    def training_eligible(self) -> bool:
        return False

    @property
    def feature_eligible(self) -> bool:
        return False

    @property
    def label_eligible(self) -> bool:
        return False

    @property
    def signal_eligible(self) -> bool:
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

    @property
    def capital_eligible(self) -> bool:
        return False


# ---------------------------------------------------------------------------
# Small independent-reconstruction helpers
# ---------------------------------------------------------------------------


def _reconstruct_plan_pin(value: object) -> PinnedQualityPilotControlArtifactRequest:
    if type(value) is not PinnedQualityPilotControlArtifactRequest:
        _fail("control artifact pin type is invalid")
    reconstruct_failed = False
    reconstructed: PinnedQualityPilotControlArtifactRequest | None = None
    try:
        reconstructed = PinnedQualityPilotControlArtifactRequest(
            storage_policy_version=value.storage_policy_version,
            protocol_sha256=value.protocol_sha256,
            kind=value.kind,
            pilot_run_id=value.pilot_run_id,
            artifact_id=value.artifact_id,
            bucket=value.bucket,
            object_name=value.object_name,
            generation=value.generation,
            expected_encoded_sha256=value.expected_encoded_sha256,
        )
    except Exception:
        reconstruct_failed = True
    if reconstruct_failed or reconstructed is None:
        _fail("control artifact pin could not be independently reverified")
    return reconstructed


def _reconstruct_transition_pin(value: object) -> _PinnedTransitionRequest:
    if type(value) is not _PinnedTransitionRequest:
        _fail("ledger transition pin type is invalid")
    reconstruct_failed = False
    reconstructed: _PinnedTransitionRequest | None = None
    try:
        reconstructed = _PinnedTransitionRequest(
            storage_policy_version=value.storage_policy_version,
            protocol_sha256=value.protocol_sha256,
            pilot_run_id=value.pilot_run_id,
            plan_id=value.plan_id,
            previous_snapshot_id=value.previous_snapshot_id,
            capture_spec_id=value.capture_spec_id,
            transition_id=value.transition_id,
            bucket=value.bucket,
            object_name=value.object_name,
            generation=value.generation,
            expected_encoded_sha256=value.expected_encoded_sha256,
        )
    except Exception:
        reconstruct_failed = True
    if reconstruct_failed or reconstructed is None:
        _fail("ledger transition pin could not be independently reverified")
    return reconstructed


def _reconstruct_published_control_artifact(value: object) -> PublishedQualityPilotControlArtifact:
    if type(value) is not PublishedQualityPilotControlArtifact:
        _fail("published control artifact type is invalid")
    failed = False
    reconstructed: PublishedQualityPilotControlArtifact | None = None
    try:
        reconstructed = PublishedQualityPilotControlArtifact(
            storage_policy_version=value.storage_policy_version,
            protocol_sha256=value.protocol_sha256,
            kind=value.kind,
            pilot_run_id=value.pilot_run_id,
            artifact_id=value.artifact_id,
            bucket=value.bucket,
            object_name=value.object_name,
            generation=value.generation,
            encoded_byte_count=value.encoded_byte_count,
            encoded_sha256=value.encoded_sha256,
        )
    except Exception:
        failed = True
    if failed or reconstructed is None:
        _fail("published control artifact could not be independently reverified")
    return reconstructed


def _reconstruct_transition(value: object) -> QualityPilotLedgerTransition:
    if type(value) is not QualityPilotLedgerTransition:
        _fail("ledger transition type is invalid")
    failed = False
    reconstructed: QualityPilotLedgerTransition | None = None
    try:
        reconstructed = QualityPilotLedgerTransition(
            protocol_sha256=value.protocol_sha256,
            pilot_run_id=value.pilot_run_id,
            plan_id=value.plan_id,
            previous_snapshot_id=value.previous_snapshot_id,
            capture_spec_id=value.capture_spec_id,
            run_result_id=value.run_result_id,
            next_snapshot=value.next_snapshot,
        )
    except Exception:
        failed = True
    if failed or reconstructed is None:
        _fail("ledger transition could not be independently reverified")
    if value.transition_id != reconstructed.transition_id:
        _fail("ledger transition identity failed independent reverification")
    return reconstructed


def _reconstruct_published_transition(value: object) -> PublishedQualityPilotLedgerTransition:
    if type(value) is not PublishedQualityPilotLedgerTransition:
        _fail("published ledger transition type is invalid")
    failed = False
    reconstructed: PublishedQualityPilotLedgerTransition | None = None
    try:
        reconstructed = PublishedQualityPilotLedgerTransition(
            storage_policy_version=value.storage_policy_version,
            protocol_sha256=value.protocol_sha256,
            pilot_run_id=value.pilot_run_id,
            plan_id=value.plan_id,
            previous_snapshot_id=value.previous_snapshot_id,
            capture_spec_id=value.capture_spec_id,
            transition_id=value.transition_id,
            bucket=value.bucket,
            object_name=value.object_name,
            generation=value.generation,
            encoded_byte_count=value.encoded_byte_count,
            encoded_sha256=value.encoded_sha256,
        )
    except Exception:
        failed = True
    if failed or reconstructed is None:
        _fail("published ledger transition could not be independently reverified")
    return reconstructed


def _reconstruct_plan(value: object) -> QualityPilotCampaignPlan:
    if type(value) is not QualityPilotCampaignPlan:
        _fail("extended campaign plan type is invalid")
    failed = False
    reconstructed: QualityPilotCampaignPlan | None = None
    try:
        reconstructed = QualityPilotCampaignPlan(campaign=value.campaign, capture_specs=value.capture_specs)
    except Exception:
        failed = True
    if failed or reconstructed is None:
        _fail("extended campaign plan could not be independently reverified")
    if value.plan_id != reconstructed.plan_id:
        _fail("extended campaign plan identity failed independent reverification")
    return reconstructed


def _reconstruct_snapshot(value: object) -> QualityPilotCompletenessSnapshot:
    if type(value) is not QualityPilotCompletenessSnapshot:
        _fail("rebased completeness snapshot type is invalid")
    failed = False
    reconstructed: QualityPilotCompletenessSnapshot | None = None
    try:
        reconstructed = QualityPilotCompletenessSnapshot(
            protocol_sha256=value.protocol_sha256,
            plan_id=value.plan_id,
            campaign_id=value.campaign_id,
            pilot_run_id=value.pilot_run_id,
            ledger_id=value.ledger_id,
            evaluated_at=value.evaluated_at,
            bucket=value.bucket,
            status=value.status,
            expected_capture_count=value.expected_capture_count,
            completed_capture_spec_ids=value.completed_capture_spec_ids,
            missing_due_capture_spec_ids=value.missing_due_capture_spec_ids,
            pending_capture_spec_ids=value.pending_capture_spec_ids,
            classification_counts=value.classification_counts,
            completed_session_count=value.completed_session_count,
            fully_due_session_count=value.fully_due_session_count,
            unplanned_due_sessions=value.unplanned_due_sessions,
            future_unplanned_sessions=value.future_unplanned_sessions,
            successful_record_count=value.successful_record_count,
            published_encoded_byte_count=value.published_encoded_byte_count,
            run_result_ids=value.run_result_ids,
            pinned_observations=value.pinned_observations,
            published_observation_byte_counts=value.published_observation_byte_counts,
        )
    except Exception:
        failed = True
    if failed or reconstructed is None:
        _fail("rebased completeness snapshot could not be independently reverified")
    if value.snapshot_id != reconstructed.snapshot_id:
        _fail("rebased completeness snapshot identity failed independent reverification")
    return reconstructed


def _reconstruct_run_result(value: object) -> QualityPilotCaptureRunResult:
    if type(value) is not QualityPilotCaptureRunResult:
        _fail("capture run result type is invalid")
    failed = False
    reconstructed: QualityPilotCaptureRunResult | None = None
    try:
        reconstructed = QualityPilotCaptureRunResult(
            campaign=value.campaign,
            capture_spec=value.capture_spec,
            campaign_id=value.campaign_id,
            capture_spec_id=value.capture_spec_id,
            requested_bucket=value.requested_bucket,
            calendar_decision_id=value.calendar_decision_id,
            observation=value.observation,
            published=value.published,
        )
    except Exception:
        failed = True
    if failed or reconstructed is None:
        _fail("catalog capture run result could not be independently reverified")
    if value.run_result_id != reconstructed.run_result_id:
        _fail("catalog capture run result identity failed independent reverification")
    return reconstructed


def _confirmed_session_calendar_decision_id(campaign: QualityPilotCampaignSpec, session: date) -> str:
    lookup_failed = False
    index = -1
    try:
        index = campaign.confirmed_sessions.index(session)
    except ValueError:
        lookup_failed = True
    if lookup_failed:
        _fail("campaign does not contain the requested session")
    return campaign.calendar_decision_ids[index]


# ---------------------------------------------------------------------------
# QualityPilotSessionBootstrapRequest
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class QualityPilotSessionBootstrapRequest(_FixedPostureMixin):
    """One immutable, independently re-verifiable session-bootstrap request.

    ``prior_plan_pin``/``prior_transition_pin`` are both ``None`` only for
    the campaign's first session (genesis); otherwise both are required.
    There is no caller-supplied universe, token map, prior-snapshot pin, or
    current timestamp -- the universe is derived solely from
    ``catalog_run_result``'s own immutable payload, and the prior snapshot
    is derived solely from the pinned prior transition's own
    ``next_snapshot`` metadata.
    """

    campaign: QualityPilotCampaignSpec
    catalog_run_result: QualityPilotCaptureRunResult
    quote_0920_window: ObservationWindowSpec
    quote_close_window: ObservationWindowSpec
    ohlcv_close_window: ObservationWindowSpec
    bucket: str
    prior_plan_pin: PinnedQualityPilotControlArtifactRequest | None
    prior_transition_pin: _PinnedTransitionRequest | None
    request_id: str = field(init=False)

    def __post_init__(self) -> None:
        self._validate()
        object.__setattr__(self, "request_id", self._calculated_id())

    def _validate(self) -> None:
        if type(self.campaign) is not QualityPilotCampaignSpec:
            _fail("session bootstrap request campaign type is invalid")
        campaign_verify_failed = False
        try:
            self.campaign.verify_content_identity()
        except Exception:
            campaign_verify_failed = True
        if campaign_verify_failed:
            _fail("session bootstrap request campaign failed independent verification")

        catalog_run_result = _reconstruct_run_result(self.catalog_run_result)

        for window, expected_kind, expected_family, label in (
            (self.quote_0920_window, ScheduledWindowKind.QUOTE_0920, EndpointFamily.FULL_QUOTE, "quote_0920_window"),
            (self.quote_close_window, ScheduledWindowKind.QUOTE_CLOSE, EndpointFamily.FULL_QUOTE, "quote_close_window"),
            (self.ohlcv_close_window, ScheduledWindowKind.OHLCV_CLOSE, EndpointFamily.DAILY_OHLCV, "ohlcv_close_window"),
        ):
            if type(window) is not ObservationWindowSpec:
                _fail(f"session bootstrap request {label} type is invalid")
            window_verify_failed = False
            try:
                window.verify_content_identity()
            except Exception:
                window_verify_failed = True
            if window_verify_failed:
                _fail(f"session bootstrap request {label} failed independent verification")
            if window.window_kind is not expected_kind or window.endpoint_family is not expected_family:
                _fail(f"session bootstrap request {label} kind is invalid")
            if window.pilot_run_id != self.campaign.pilot_run_id:
                _fail(f"session bootstrap request {label} pilot run id disagrees with the campaign")
            if window.protocol_sha256 != self.campaign.protocol_sha256:
                _fail(f"session bootstrap request {label} protocol disagrees with the campaign")

        window_ids = {self.quote_0920_window.window_id, self.quote_close_window.window_id, self.ohlcv_close_window.window_id}
        if len(window_ids) != 3:
            _fail("session bootstrap request future windows must be distinct")

        target_session = catalog_run_result.capture_spec.window.market_session
        for window in (self.quote_0920_window, self.quote_close_window, self.ohlcv_close_window):
            if window.market_session != target_session:
                _fail("session bootstrap request future windows must target the catalog's own session")

        if type(self.bucket) is not str or _BUCKET_PATTERN.fullmatch(self.bucket) is None:
            _fail("session bootstrap request bucket is invalid")

        if (self.prior_plan_pin is None) != (self.prior_transition_pin is None):
            _fail("session bootstrap request predecessor pins must both be present or both be absent")
        if self.prior_plan_pin is not None:
            prior_plan_pin = _reconstruct_plan_pin(self.prior_plan_pin)
            prior_transition_pin = _reconstruct_transition_pin(self.prior_transition_pin)
            if prior_plan_pin.kind is not QualityPilotControlArtifactKind.CAMPAIGN_PLAN:
                _fail("session bootstrap request prior plan pin kind is invalid")
            if prior_plan_pin.bucket != self.bucket or prior_transition_pin.bucket != self.bucket:
                _fail("session bootstrap request predecessor pins disagree on bucket")
            if (
                prior_plan_pin.pilot_run_id != self.campaign.pilot_run_id
                or prior_transition_pin.pilot_run_id != self.campaign.pilot_run_id
            ):
                _fail("session bootstrap request predecessor pins disagree on pilot run id")
            if (
                prior_plan_pin.protocol_sha256 != self.campaign.protocol_sha256
                or prior_transition_pin.protocol_sha256 != self.campaign.protocol_sha256
            ):
                _fail("session bootstrap request predecessor pins disagree on protocol")

        if any(getattr(self, name) != (name == "quality_only") for name in _POSTURE_NAMES):
            _fail("session bootstrap request safety posture is invalid")

    def _calculated_id(self) -> str:
        failed = False
        calculated = ""
        try:
            calculated = content_id(
                {
                    "schema": QUALITY_PILOT_SESSION_BOOTSTRAP_REQUEST_SCHEMA_VERSION,
                    "campaign_id": self.campaign.campaign_id,
                    "catalog_run_result_id": self.catalog_run_result.run_result_id,
                    "quote_0920_window": self.quote_0920_window,
                    "quote_close_window": self.quote_close_window,
                    "ohlcv_close_window": self.ohlcv_close_window,
                    "bucket": self.bucket,
                    "prior_plan_pin": self.prior_plan_pin,
                    "prior_transition_pin": self.prior_transition_pin,
                    "posture": _posture_tree(self),
                },
                length=64,
            )
        except Exception:
            failed = True
        if failed:
            _fail("session bootstrap request identity calculation failed")
        return calculated

    def verify_content_identity(self) -> None:
        self._validate()
        if self.request_id != self._calculated_id():
            _fail("session bootstrap request identity failed")


def _reconstruct_request(value: object) -> QualityPilotSessionBootstrapRequest:
    if type(value) is not QualityPilotSessionBootstrapRequest:
        _fail("session bootstrap request type is invalid")
    reconstruct_failed = False
    reconstructed: QualityPilotSessionBootstrapRequest | None = None
    try:
        reconstructed = QualityPilotSessionBootstrapRequest(
            campaign=value.campaign,
            catalog_run_result=value.catalog_run_result,
            quote_0920_window=value.quote_0920_window,
            quote_close_window=value.quote_close_window,
            ohlcv_close_window=value.ohlcv_close_window,
            bucket=value.bucket,
            prior_plan_pin=value.prior_plan_pin,
            prior_transition_pin=value.prior_transition_pin,
        )
    except Exception:
        reconstruct_failed = True
    if reconstruct_failed or reconstructed is None:
        _fail("session bootstrap request could not be independently reverified")
    if value.request_id != reconstructed.request_id:
        _fail("session bootstrap request identity failed independent reverification")
    return reconstructed


# ---------------------------------------------------------------------------
# QualityPilotSessionBootstrapResult
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class QualityPilotSessionBootstrapResult(_FixedPostureMixin):
    """One immutable, independently re-verifiable record of one session bootstrap.

    Carries the full ``request``, ``plan``, and ``snapshot`` records (not
    just opaque IDs/publication metadata) so ``verify_content_identity``
    can independently reconstruct and cross-check every exposed field
    against those full records -- two pieces of untrusted metadata never
    attest to each other without a full-record authority.
    """

    request_id: str
    request: QualityPilotSessionBootstrapRequest
    target_session: date
    catalog_observation_id: str
    catalog_run_result_id: str
    catalog_capture_spec_id: str
    previous_plan_id: str | None
    previous_snapshot_id: str | None
    predecessor_plan: QualityPilotCampaignPlan | None
    predecessor_transition: QualityPilotLedgerTransition | None
    predecessor_snapshot: QualityPilotCompletenessSnapshot | None
    plan_id: str
    plan: QualityPilotCampaignPlan
    published_plan: PublishedQualityPilotControlArtifact
    snapshot_id: str
    snapshot: QualityPilotCompletenessSnapshot
    published_snapshot: PublishedQualityPilotControlArtifact
    transition: QualityPilotLedgerTransition
    transition_id: str
    published_transition: PublishedQualityPilotLedgerTransition
    next_capture_spec_id: str
    result_id: str = field(init=False)

    def __post_init__(self) -> None:
        self._validate()
        object.__setattr__(self, "result_id", self._calculated_id())

    def _validate(self) -> None:
        if not _is_sha256(self.request_id):
            _fail("session bootstrap result request id is invalid")
        if type(self.target_session) is not date:
            _fail("session bootstrap result target session is invalid")
        if not _is_sha256(self.catalog_observation_id):
            _fail("session bootstrap result catalog observation id is invalid")
        if not _is_sha256(self.catalog_run_result_id):
            _fail("session bootstrap result catalog run result id is invalid")
        if not _is_sha256(self.catalog_capture_spec_id):
            _fail("session bootstrap result catalog capture spec id is invalid")
        if (self.previous_plan_id is None) != (self.previous_snapshot_id is None):
            _fail("session bootstrap result previous plan/snapshot ids must both be present or both be absent")
        if self.previous_plan_id is not None and not _is_sha256(self.previous_plan_id):
            _fail("session bootstrap result previous plan id is invalid")
        if self.previous_snapshot_id is not None and not _is_sha256(self.previous_snapshot_id):
            _fail("session bootstrap result previous snapshot id is invalid")
        if not _is_sha256(self.plan_id):
            _fail("session bootstrap result plan id is invalid")
        if not _is_sha256(self.snapshot_id):
            _fail("session bootstrap result snapshot id is invalid")
        if not _is_sha256(self.transition_id):
            _fail("session bootstrap result transition id is invalid")
        if not _is_sha256(self.next_capture_spec_id):
            _fail("session bootstrap result next capture spec id is invalid")

        # --- Independently reconstruct/reverify the full request and
        # require it to be the exact authority for this request_id. ---
        request = _reconstruct_request(self.request)
        if request.request_id != self.request_id:
            _fail("session bootstrap result request id disagrees with its own request record")

        # --- catalog run result: cross-check the result's own copy
        # against the exact copy carried inside the verified request,
        # then derive every catalog-* field solely from that record. ---
        catalog_run_result = _reconstruct_run_result(request.catalog_run_result)
        if catalog_run_result.run_result_id != self.catalog_run_result_id:
            _fail("session bootstrap result catalog run result disagrees with its own request")
        catalog_spec = catalog_run_result.capture_spec
        if catalog_spec.window.market_session != self.target_session:
            _fail("session bootstrap result target session disagrees with the catalog run result")
        if catalog_run_result.observation.observation_id != self.catalog_observation_id:
            _fail("session bootstrap result catalog observation id disagrees with the catalog run result")
        if catalog_spec.capture_spec_id != self.catalog_capture_spec_id:
            _fail("session bootstrap result catalog capture spec id disagrees with the catalog run result")

        # --- Predecessor lineage: genesis carries no predecessor records;
        # extension must carry and independently pin-verify the exact
        # predecessor plan/transition/snapshot already loaded by the
        # service, and derive previous_plan_id/previous_snapshot_id
        # solely from those records -- never from a bare stored id alone. ---
        if request.prior_plan_pin is None:
            if (
                self.previous_plan_id is not None
                or self.previous_snapshot_id is not None
                or self.predecessor_plan is not None
                or self.predecessor_transition is not None
                or self.predecessor_snapshot is not None
            ):
                _fail("session bootstrap result predecessor state disagrees with a genesis request")
            predecessor_plan = None
            predecessor_snapshot = None
        else:
            if (
                self.predecessor_plan is None
                or self.predecessor_transition is None
                or self.predecessor_snapshot is None
            ):
                _fail("session bootstrap result predecessor records are required for an extension request")

            prior_plan_pin = _reconstruct_plan_pin(request.prior_plan_pin)
            predecessor_plan = _reconstruct_plan(self.predecessor_plan)
            if predecessor_plan.plan_id != prior_plan_pin.artifact_id:
                _fail("session bootstrap result predecessor plan disagrees with the request predecessor pin")
            predecessor_plan_encode_failed = False
            predecessor_plan_bytes = b""
            try:
                predecessor_plan_bytes = encode_quality_pilot_campaign_plan(predecessor_plan)
            except Exception:
                predecessor_plan_encode_failed = True
            if predecessor_plan_encode_failed:
                _fail("session bootstrap result predecessor plan could not be canonically encoded")
            if prior_plan_pin.expected_encoded_sha256 != sha256(predecessor_plan_bytes).hexdigest():
                _fail("session bootstrap result predecessor plan hash disagrees with the request predecessor pin")

            prior_transition_pin = _reconstruct_transition_pin(request.prior_transition_pin)
            predecessor_transition = _reconstruct_transition(self.predecessor_transition)
            if (
                predecessor_transition.transition_id != prior_transition_pin.transition_id
                or predecessor_transition.protocol_sha256 != prior_transition_pin.protocol_sha256
                or predecessor_transition.pilot_run_id != prior_transition_pin.pilot_run_id
                or predecessor_transition.plan_id != prior_transition_pin.plan_id
                or predecessor_transition.previous_snapshot_id != prior_transition_pin.previous_snapshot_id
                or predecessor_transition.capture_spec_id != prior_transition_pin.capture_spec_id
            ):
                _fail("session bootstrap result predecessor transition disagrees with the request predecessor pin")
            predecessor_transition_encode_failed = False
            predecessor_transition_bytes = b""
            try:
                predecessor_transition_bytes = encode_quality_pilot_ledger_transition(predecessor_transition)
            except Exception:
                predecessor_transition_encode_failed = True
            if predecessor_transition_encode_failed:
                _fail("session bootstrap result predecessor transition could not be canonically encoded")
            if prior_transition_pin.expected_encoded_sha256 != sha256(predecessor_transition_bytes).hexdigest():
                _fail("session bootstrap result predecessor transition hash disagrees with the request predecessor pin")

            predecessor_snapshot = _reconstruct_snapshot(self.predecessor_snapshot)
            reconstructed_predecessor_next_snapshot = _reconstruct_published_control_artifact(
                predecessor_transition.next_snapshot
            )
            if (
                predecessor_snapshot.snapshot_id != reconstructed_predecessor_next_snapshot.artifact_id
                or predecessor_snapshot.bucket != reconstructed_predecessor_next_snapshot.bucket
            ):
                _fail("session bootstrap result predecessor snapshot disagrees with its own transition")
            predecessor_snapshot_encode_failed = False
            predecessor_snapshot_bytes = b""
            try:
                predecessor_snapshot_bytes = encode_quality_pilot_completeness_snapshot(predecessor_snapshot)
            except Exception:
                predecessor_snapshot_encode_failed = True
            if predecessor_snapshot_encode_failed:
                _fail("session bootstrap result predecessor snapshot could not be canonically encoded")
            if (
                reconstructed_predecessor_next_snapshot.encoded_byte_count != len(predecessor_snapshot_bytes)
                or reconstructed_predecessor_next_snapshot.encoded_sha256
                != sha256(predecessor_snapshot_bytes).hexdigest()
            ):
                _fail("session bootstrap result predecessor snapshot hash disagrees with its own transition")

            if self.previous_plan_id != predecessor_plan.plan_id:
                _fail("session bootstrap result previous plan id disagrees with the predecessor plan")
            if self.previous_snapshot_id != reconstructed_predecessor_next_snapshot.artifact_id:
                _fail("session bootstrap result previous snapshot id disagrees with the predecessor transition")

            # QualityPilotCampaignPlan carries no bucket field of its own;
            # its publication-level bucket is pin-checked above.
            if predecessor_snapshot.bucket != request.bucket:
                _fail("session bootstrap result predecessor snapshot bucket disagrees with the request bucket")
            if reconstructed_predecessor_next_snapshot.bucket != request.bucket:
                _fail("session bootstrap result predecessor transition bucket disagrees with the request bucket")
            if prior_plan_pin.bucket != request.bucket or prior_transition_pin.bucket != request.bucket:
                _fail("session bootstrap result predecessor pins disagree with the request bucket")

        # --- Independently reconstruct/reverify the full extended plan
        # and rebased snapshot. ---
        plan = _reconstruct_plan(self.plan)
        if plan.plan_id != self.plan_id:
            _fail("session bootstrap result plan id disagrees with its own plan record")
        snapshot = _reconstruct_snapshot(self.snapshot)
        if snapshot.snapshot_id != self.snapshot_id:
            _fail("session bootstrap result snapshot id disagrees with its own snapshot record")
        if snapshot.plan_id != plan.plan_id:
            _fail("session bootstrap result snapshot plan id disagrees with its own plan record")
        if plan.campaign.campaign_id != request.campaign.campaign_id:
            _fail("session bootstrap result plan campaign disagrees with its own request")
        if not plan.planned_sessions or plan.planned_sessions[-1] != self.target_session:
            _fail("session bootstrap result plan session lineage disagrees with its target session")

        # --- Extension: the extended plan/snapshot must be the exact
        # continuation of the predecessor plan/snapshot, never a same-
        # campaign plan with a different universe/spec prefix. ---
        if predecessor_plan is not None:
            predecessor_length = len(predecessor_plan.capture_specs)
            if plan.capture_specs[:predecessor_length] != predecessor_plan.capture_specs:
                _fail("session bootstrap result plan is not an exact continuation of the predecessor plan")
            if plan.planned_sessions[:-1] != predecessor_plan.planned_sessions:
                _fail("session bootstrap result plan sessions are not an exact continuation of the predecessor plan")
            expected_predecessor_completed = predecessor_snapshot.completed_capture_spec_ids + (
                self.catalog_capture_spec_id,
            )
            if snapshot.completed_capture_spec_ids != expected_predecessor_completed:
                _fail("session bootstrap result snapshot completed prefix is not an exact continuation of the predecessor snapshot")

        completed_prefix_length = len(snapshot.completed_capture_spec_ids)
        if completed_prefix_length == 0 or completed_prefix_length > len(plan.capture_specs):
            _fail("session bootstrap result snapshot completed prefix is invalid")
        expected_completed_ids = tuple(spec.capture_spec_id for spec in plan.capture_specs[:completed_prefix_length])
        if snapshot.completed_capture_spec_ids != expected_completed_ids:
            _fail("session bootstrap result snapshot completed prefix disagrees with its own plan")
        if snapshot.completed_capture_spec_ids[-1] != self.catalog_capture_spec_id:
            _fail("session bootstrap result snapshot completed prefix does not end at the catalog capture")

        if completed_prefix_length >= len(plan.capture_specs):
            _fail("session bootstrap result plan has no next capture spec")
        derived_next_spec = plan.capture_specs[completed_prefix_length]
        if (
            derived_next_spec.window.window_kind is not ScheduledWindowKind.QUOTE_0920
            or derived_next_spec.window.market_session != self.target_session
        ):
            _fail("session bootstrap result next capture spec is not the target session's first 09:20 quote chunk")
        if self.next_capture_spec_id != derived_next_spec.capture_spec_id:
            _fail("session bootstrap result next capture spec id disagrees with its own plan")

        # --- Canonical-encode the full plan/snapshot and require the
        # published metadata to match those exact canonical bytes. ---
        published_plan = _reconstruct_published_control_artifact(self.published_plan)
        if published_plan.kind is not QualityPilotControlArtifactKind.CAMPAIGN_PLAN:
            _fail("session bootstrap result published plan must be a campaign plan")
        if published_plan.artifact_id != self.plan_id:
            _fail("session bootstrap result published plan id disagrees")

        published_snapshot = _reconstruct_published_control_artifact(self.published_snapshot)
        if published_snapshot.kind is not QualityPilotControlArtifactKind.COMPLETENESS_LEDGER:
            _fail("session bootstrap result published snapshot must be a completeness ledger")
        if published_snapshot.artifact_id != self.snapshot_id:
            _fail("session bootstrap result published snapshot id disagrees")
        if published_snapshot.pilot_run_id != published_plan.pilot_run_id:
            _fail("session bootstrap result published artifacts disagree on pilot run id")
        if published_snapshot.bucket != published_plan.bucket:
            _fail("session bootstrap result published artifacts disagree on bucket")

        # Every current-lineage bucket must equal request.bucket, not
        # merely agree with other untrusted publication metadata -- an
        # attacker moving every publication to the same different bucket
        # together must still be rejected.
        if snapshot.bucket != request.bucket:
            _fail("session bootstrap result snapshot bucket disagrees with the request bucket")
        if published_plan.bucket != request.bucket:
            _fail("session bootstrap result published plan bucket disagrees with the request bucket")
        if published_snapshot.bucket != request.bucket:
            _fail("session bootstrap result published snapshot bucket disagrees with the request bucket")

        # Never trust published_plan/published_snapshot's own byte
        # count/hash as given -- independently recompute the canonical
        # bytes of the reconstructed plan/snapshot records themselves and
        # require an exact match. No two untrusted metadata objects may
        # attest to each other without this full-record authority.
        plan_encode_failed = False
        expected_plan_bytes = b""
        try:
            expected_plan_bytes = encode_quality_pilot_campaign_plan(plan)
        except Exception:
            plan_encode_failed = True
        if plan_encode_failed:
            _fail("session bootstrap result plan could not be canonically encoded")
        if published_plan.encoded_byte_count != len(expected_plan_bytes):
            _fail("session bootstrap result published plan byte count disagrees with its canonical bytes")
        if published_plan.encoded_sha256 != sha256(expected_plan_bytes).hexdigest():
            _fail("session bootstrap result published plan hash disagrees with its canonical bytes")

        snapshot_encode_failed = False
        expected_snapshot_bytes = b""
        try:
            expected_snapshot_bytes = encode_quality_pilot_completeness_snapshot(snapshot)
        except Exception:
            snapshot_encode_failed = True
        if snapshot_encode_failed:
            _fail("session bootstrap result snapshot could not be canonically encoded")
        if published_snapshot.encoded_byte_count != len(expected_snapshot_bytes):
            _fail("session bootstrap result published snapshot byte count disagrees with its canonical bytes")
        if published_snapshot.encoded_sha256 != sha256(expected_snapshot_bytes).hexdigest():
            _fail("session bootstrap result published snapshot hash disagrees with its canonical bytes")

        transition = _reconstruct_transition(self.transition)
        if transition.transition_id != self.transition_id:
            _fail("session bootstrap result transition id disagrees")
        if transition.protocol_sha256 != plan.campaign.protocol_sha256:
            _fail("session bootstrap result transition protocol disagrees with its own plan")
        if transition.pilot_run_id != plan.campaign.pilot_run_id:
            _fail("session bootstrap result transition pilot run id disagrees with its own plan")
        if transition.plan_id != self.plan_id:
            _fail("session bootstrap result transition plan id disagrees")
        if transition.previous_snapshot_id != self.previous_snapshot_id:
            _fail("session bootstrap result transition predecessor disagrees")
        if transition.capture_spec_id != self.catalog_capture_spec_id:
            _fail("session bootstrap result transition capture spec id disagrees")
        if transition.run_result_id != self.catalog_run_result_id:
            _fail("session bootstrap result transition run result id disagrees")

        reconstructed_next_snapshot = _reconstruct_published_control_artifact(transition.next_snapshot)
        if reconstructed_next_snapshot.artifact_id != self.snapshot_id:
            _fail("session bootstrap result transition next snapshot id disagrees")
        if (
            reconstructed_next_snapshot.artifact_id != published_snapshot.artifact_id
            or reconstructed_next_snapshot.object_name != published_snapshot.object_name
            or reconstructed_next_snapshot.bucket != published_snapshot.bucket
            or reconstructed_next_snapshot.generation != published_snapshot.generation
            or reconstructed_next_snapshot.encoded_byte_count != published_snapshot.encoded_byte_count
            or reconstructed_next_snapshot.encoded_sha256 != published_snapshot.encoded_sha256
        ):
            _fail("session bootstrap result transition next snapshot disagrees with the published snapshot")
        if reconstructed_next_snapshot.bucket != request.bucket:
            _fail("session bootstrap result transition next snapshot bucket disagrees with the request bucket")

        published_transition = _reconstruct_published_transition(self.published_transition)
        if published_transition.transition_id != transition.transition_id:
            _fail("session bootstrap result published transition id disagrees")
        if published_transition.plan_id != transition.plan_id:
            _fail("session bootstrap result published transition plan id disagrees")
        if published_transition.previous_snapshot_id != transition.previous_snapshot_id:
            _fail("session bootstrap result published transition predecessor disagrees")
        if published_transition.capture_spec_id != transition.capture_spec_id:
            _fail("session bootstrap result published transition capture spec id disagrees")
        if published_transition.pilot_run_id != transition.pilot_run_id:
            _fail("session bootstrap result published transition pilot run id disagrees")
        if published_transition.protocol_sha256 != transition.protocol_sha256:
            _fail("session bootstrap result published transition protocol disagrees")
        if published_transition.bucket != reconstructed_next_snapshot.bucket:
            _fail("session bootstrap result published artifacts disagree on bucket")
        if published_transition.bucket != request.bucket:
            _fail("session bootstrap result published transition bucket disagrees with the request bucket")

        # Never trust the published transition's own byte count/hash as
        # given -- independently recompute the canonical bytes of the
        # reconstructed transition record itself and require an exact match.
        transition_encode_failed = False
        expected_transition_bytes = b""
        try:
            expected_transition_bytes = encode_quality_pilot_ledger_transition(transition)
        except Exception:
            transition_encode_failed = True
        if transition_encode_failed:
            _fail("session bootstrap result transition could not be canonically encoded")
        if published_transition.encoded_byte_count != len(expected_transition_bytes):
            _fail("session bootstrap result published transition byte count disagrees with its canonical bytes")
        if published_transition.encoded_sha256 != sha256(expected_transition_bytes).hexdigest():
            _fail("session bootstrap result published transition hash disagrees with its canonical bytes")

        if any(getattr(self, name) != (name == "quality_only") for name in _POSTURE_NAMES):
            _fail("session bootstrap result safety posture is invalid")

    def _calculated_id(self) -> str:
        failed = False
        calculated = ""
        try:
            calculated = content_id(
                {
                    "schema": QUALITY_PILOT_SESSION_BOOTSTRAP_RESULT_SCHEMA_VERSION,
                    "request_id": self.request_id,
                    "request": self.request,
                    "target_session": self.target_session,
                    "catalog_observation_id": self.catalog_observation_id,
                    "catalog_run_result_id": self.catalog_run_result_id,
                    "catalog_capture_spec_id": self.catalog_capture_spec_id,
                    "previous_plan_id": self.previous_plan_id,
                    "previous_snapshot_id": self.previous_snapshot_id,
                    "predecessor_plan": self.predecessor_plan,
                    "predecessor_transition": self.predecessor_transition,
                    "predecessor_snapshot": self.predecessor_snapshot,
                    "plan_id": self.plan_id,
                    "plan": self.plan,
                    "published_plan": self.published_plan,
                    "snapshot_id": self.snapshot_id,
                    "snapshot": self.snapshot,
                    "published_snapshot": self.published_snapshot,
                    "transition": self.transition,
                    "transition_id": self.transition_id,
                    "published_transition": self.published_transition,
                    "next_capture_spec_id": self.next_capture_spec_id,
                    "posture": _posture_tree(self),
                },
                length=64,
            )
        except Exception:
            failed = True
        if failed:
            _fail("session bootstrap result identity calculation failed")
        return calculated

    def verify_content_identity(self) -> None:
        self._validate()
        if self.result_id != self._calculated_id():
            _fail("session bootstrap result identity failed")


# ---------------------------------------------------------------------------
# K_EQ universe derivation and route construction
# ---------------------------------------------------------------------------


def _derive_k_eq_universe(payload: InstrumentBatch) -> tuple:
    """Frozen v2 estimand: exchange exactly NSE and instrument_type exactly EQ.

    Deliberately does not infer main-board status, series, SME status,
    issuer identity, ISIN continuity, free float, sector, current index
    membership, or survivorship. The accepted canonical_response.py layer
    already requires every catalog instrument in a valid SUCCESS payload to
    satisfy the strictly narrower ``is_nse_eq_record`` (which additionally
    requires ``segment == NSE``) before the payload can pass observation
    verification at all -- this filter is independently re-applied here
    using only the two fields the frozen estimand actually names, rather
    than trusting that upstream enforcement alone.
    """

    if type(payload) is not InstrumentBatch:
        _fail("catalog payload type is invalid")
    eligible = tuple(
        instrument
        for instrument in payload.instruments
        if instrument.exchange == EXCHANGE_NSE and instrument.instrument_type == "EQ"
    )
    if not eligible:
        _fail("catalog payload contains an empty eligible K_EQ universe")
    sorted_eligible = tuple(sorted(eligible, key=lambda item: item.listing_key))
    listing_keys = tuple(item.listing_key for item in sorted_eligible)
    tokens = tuple(item.instrument_token for item in sorted_eligible)
    if len(set(listing_keys)) != len(listing_keys):
        _fail("catalog payload eligible universe contains duplicate listing keys")
    if any(type(token) is bool or type(token) is not int or token <= 0 for token in tokens):
        _fail("catalog payload eligible universe contains an invalid provider instrument token")
    if len(set(tokens)) != len(tokens):
        _fail("catalog payload eligible universe contains duplicate provider instrument tokens")
    return sorted_eligible


def _chunk_keys(keys: tuple, maximum_keys: int) -> tuple:
    if not keys:
        _fail("session route requires a non-empty universe to chunk")
    chunks = []
    for start in range(0, len(keys), maximum_keys):
        chunks.append(keys[start : start + maximum_keys])
    return tuple(chunks)


def _build_quote_specs(
    campaign: QualityPilotCampaignSpec,
    window: ObservationWindowSpec,
    keys: tuple,
    provider_version: str,
    protocol_sha256: str,
) -> tuple:
    chunks = _chunk_keys(keys, MAXIMUM_QUOTE_REQUEST_KEYS)
    chunk_count = len(chunks)
    build_failed = False
    specs = []
    try:
        for index, chunk in enumerate(chunks, start=1):
            specs.append(
                QualityPilotCaptureSpec(
                    campaign=campaign,
                    window=window,
                    provider=PROVIDER_ZERODHA_KITE,
                    provider_version=provider_version,
                    requested_keys=chunk,
                    provider_instrument_token=None,
                    chunk_index=index,
                    chunk_count=chunk_count,
                    protocol_sha256=protocol_sha256,
                )
            )
    except Exception:
        build_failed = True
    if build_failed:
        _fail("session route quote specs could not be constructed")
    return tuple(specs)


def _build_ohlcv_specs(
    campaign: QualityPilotCampaignSpec,
    window: ObservationWindowSpec,
    sorted_instruments: tuple,
    provider_version: str,
    protocol_sha256: str,
) -> tuple:
    chunk_count = len(sorted_instruments)
    build_failed = False
    specs = []
    try:
        for index, instrument in enumerate(sorted_instruments, start=1):
            specs.append(
                QualityPilotCaptureSpec(
                    campaign=campaign,
                    window=window,
                    provider=PROVIDER_ZERODHA_KITE,
                    provider_version=provider_version,
                    requested_keys=(instrument.listing_key,),
                    provider_instrument_token=instrument.instrument_token,
                    chunk_index=index,
                    chunk_count=chunk_count,
                    protocol_sha256=protocol_sha256,
                )
            )
    except Exception:
        build_failed = True
    if build_failed:
        _fail("session route OHLCV specs could not be constructed")
    return tuple(specs)


# ---------------------------------------------------------------------------
# QualityPilotSessionBootstrapService
# ---------------------------------------------------------------------------


class QualityPilotSessionBootstrapService:
    """Stateless orchestration: one catalog result in, one extended plan sealed.

    Holds no campaign progress of its own. Never invokes a collector --
    ``catalog_run_result`` is the sole, already-persisted source of the
    session's catalog outcome, treated throughout as untrusted input.
    """

    def run(
        self,
        request: QualityPilotSessionBootstrapRequest,
        reader: GCSObjectReader,
        writer: StateObjectWriter,
    ) -> QualityPilotSessionBootstrapResult:
        if type(request) is not QualityPilotSessionBootstrapRequest:
            _fail("session bootstrap request must be an exact QualityPilotSessionBootstrapRequest")
        verified_request = _reconstruct_request(request)
        campaign = verified_request.campaign
        bucket = verified_request.bucket

        catalog_run_result = _reconstruct_run_result(verified_request.catalog_run_result)
        if catalog_run_result.campaign_id != campaign.campaign_id:
            _fail("catalog run result campaign disagrees with the request campaign")
        if catalog_run_result.requested_bucket != bucket:
            _fail("catalog run result bucket disagrees with the request bucket")
        catalog_spec = catalog_run_result.capture_spec
        if (
            catalog_spec.window.window_kind is not ScheduledWindowKind.CATALOG_PREOPEN
            or catalog_spec.window.endpoint_family is not EndpointFamily.CATALOG
        ):
            _fail("catalog run result is not a CATALOG_PREOPEN capture")
        if catalog_spec.chunk_index != 1 or catalog_spec.chunk_count != 1:
            _fail("catalog run result is not a 1-of-1 chunk")
        if catalog_run_result.observation.request.response_classification is not ResponseClassification.SUCCESS:
            _fail("catalog run result is not a successful capture")
        if type(catalog_run_result.observation.payload) is not InstrumentBatch:
            _fail("catalog run result payload is not an exact InstrumentBatch")

        target_session = catalog_spec.window.market_session
        expected_calendar_decision_id = _confirmed_session_calendar_decision_id(campaign, target_session)
        if catalog_run_result.calendar_decision_id != expected_calendar_decision_id:
            _fail("catalog run result calendar decision disagrees with the campaign session")

        prior_plan: QualityPilotCampaignPlan | None = None
        prior_transition: QualityPilotLedgerTransition | None = None
        prior_snapshot: QualityPilotCompletenessSnapshot | None = None
        prior_capture_specs: tuple = ()

        if verified_request.prior_plan_pin is None:
            if target_session != campaign.confirmed_sessions[0]:
                _fail("genesis session bootstrap target must be the campaign's first confirmed session")
        else:
            plan_load_failed = False
            loaded_plan: object = None
            try:
                loaded_plan = read_pinned_quality_pilot_control_artifact(verified_request.prior_plan_pin, reader)
            except Exception:
                plan_load_failed = True
            if plan_load_failed:
                _fail("prior campaign plan could not be loaded")
            if (
                type(loaded_plan) is not LoadedQualityPilotControlArtifact
                or type(loaded_plan.artifact) is not QualityPilotCampaignPlan
            ):
                _fail("prior campaign plan artifact type is invalid")
            prior_plan = loaded_plan.artifact
            prior_plan_verify_failed = False
            try:
                prior_plan.verify_content_identity()
            except Exception:
                prior_plan_verify_failed = True
            if prior_plan_verify_failed:
                _fail("prior campaign plan failed independent verification")
            if prior_plan.campaign.campaign_id != campaign.campaign_id:
                _fail("prior campaign plan campaign disagrees with the request campaign")
            if not prior_plan.planned_sessions:
                _fail("prior campaign plan has no planned sessions")
            if prior_plan.planned_sessions != campaign.confirmed_sessions[: len(prior_plan.planned_sessions)]:
                _fail("prior campaign plan sessions are not a canonical campaign prefix")
            if target_session != campaign.confirmed_sessions[len(prior_plan.planned_sessions)]:
                _fail("target session is not the exact next campaign session after the prior plan")

            transition_load_failed = False
            loaded_transition: object = None
            try:
                loaded_transition = read_pinned_quality_pilot_ledger_transition(
                    verified_request.prior_transition_pin, reader
                )
            except Exception:
                transition_load_failed = True
            if transition_load_failed:
                _fail("prior ledger transition could not be loaded")
            if type(loaded_transition) is not LoadedQualityPilotLedgerTransition:
                _fail("prior ledger transition type is invalid")
            prior_transition = loaded_transition.transition
            prior_transition_verify_failed = False
            try:
                prior_transition.verify_content_identity()
            except Exception:
                prior_transition_verify_failed = True
            if prior_transition_verify_failed:
                _fail("prior ledger transition failed independent verification")
            if (
                prior_transition.plan_id != prior_plan.plan_id
                or prior_transition.pilot_run_id != campaign.pilot_run_id
                or prior_transition.protocol_sha256 != campaign.protocol_sha256
            ):
                _fail("prior ledger transition lineage disagrees with the prior plan")

            snapshot_pin_failed = False
            prior_snapshot_pin: object = None
            try:
                prior_snapshot_pin = pinned_quality_pilot_control_artifact_request(prior_transition.next_snapshot)
            except Exception:
                snapshot_pin_failed = True
            if snapshot_pin_failed:
                _fail("prior transition next snapshot metadata could not be pinned")
            snapshot_load_failed = False
            loaded_snapshot: object = None
            try:
                loaded_snapshot = read_pinned_quality_pilot_control_artifact(prior_snapshot_pin, reader)
            except Exception:
                snapshot_load_failed = True
            if snapshot_load_failed:
                _fail("prior completeness snapshot could not be loaded")
            if (
                type(loaded_snapshot) is not LoadedQualityPilotControlArtifact
                or type(loaded_snapshot.artifact) is not QualityPilotCompletenessSnapshot
            ):
                _fail("prior completeness snapshot artifact type is invalid")
            prior_snapshot = loaded_snapshot.artifact
            prior_snapshot_verify_failed = False
            try:
                prior_snapshot.verify_content_identity()
            except Exception:
                prior_snapshot_verify_failed = True
            if prior_snapshot_verify_failed:
                _fail("prior completeness snapshot failed independent verification")
            if (
                prior_snapshot.snapshot_id != prior_transition.next_snapshot.artifact_id
                or prior_snapshot.plan_id != prior_plan.plan_id
                or prior_snapshot.pilot_run_id != campaign.pilot_run_id
                or prior_snapshot.protocol_sha256 != campaign.protocol_sha256
                or prior_snapshot.bucket != bucket
            ):
                _fail("prior completeness snapshot lineage disagrees with its transition or plan")

            expected_prior_ids = tuple(spec.capture_spec_id for spec in prior_plan.capture_specs)
            if (
                prior_snapshot.completed_capture_spec_ids != expected_prior_ids
                or prior_snapshot.missing_due_capture_spec_ids != ()
                or prior_snapshot.pending_capture_spec_ids != ()
                or prior_snapshot.unplanned_due_sessions != ()
            ):
                _fail("prior campaign plan is not fully completed")

            audit_replay_failed = False
            prior_run_results: tuple = ()
            try:
                prior_run_results = audit_replay_quality_pilot_completeness_snapshot(
                    prior_plan, prior_snapshot, reader
                )
            except Exception:
                audit_replay_failed = True
            if audit_replay_failed:
                _fail("prior completeness snapshot could not be independently audit-replayed")
            prior_capture_specs = prior_plan.capture_specs

        # --- K_EQ universe and complete session route ---
        sorted_instruments = _derive_k_eq_universe(catalog_run_result.observation.payload)
        listing_keys = tuple(item.listing_key for item in sorted_instruments)
        provider_version = catalog_spec.provider_version
        protocol_sha256 = catalog_spec.protocol_sha256

        quote_0920_specs = _build_quote_specs(
            campaign, verified_request.quote_0920_window, listing_keys, provider_version, protocol_sha256
        )
        quote_close_specs = _build_quote_specs(
            campaign, verified_request.quote_close_window, listing_keys, provider_version, protocol_sha256
        )
        ohlcv_specs = _build_ohlcv_specs(
            campaign, verified_request.ohlcv_close_window, sorted_instruments, provider_version, protocol_sha256
        )

        new_route = (catalog_spec,) + quote_0920_specs + quote_close_specs + ohlcv_specs

        # --- Extended plan ---
        extended_plan_failed = False
        extended_plan: QualityPilotCampaignPlan | None = None
        try:
            extended_plan = QualityPilotCampaignPlan(
                campaign=campaign, capture_specs=prior_capture_specs + new_route
            )
        except Exception:
            extended_plan_failed = True
        if extended_plan_failed or extended_plan is None:
            _fail("extended campaign plan could not be constructed")
        if extended_plan.planned_sessions[-1] != target_session:
            _fail("extended campaign plan does not include the target session")

        # --- Rebased completeness snapshot ---
        if prior_plan is None:
            all_run_results = (catalog_run_result,)
        else:
            all_run_results = prior_run_results + (catalog_run_result,)
        evaluated_at = catalog_run_result.observation.request.request_ended_at

        rebase_failed = False
        rebased_ledger = None
        try:
            rebased_ledger = QualityPilotCampaignCompletenessLedger(
                extended_plan, all_run_results, evaluated_at, bucket
            )
        except Exception:
            rebase_failed = True
        if rebase_failed or rebased_ledger is None:
            _fail("rebased completeness ledger could not be constructed")

        snapshot_build_failed = False
        rebased_snapshot: QualityPilotCompletenessSnapshot | None = None
        try:
            rebased_snapshot = build_quality_pilot_completeness_snapshot(rebased_ledger)
        except Exception:
            snapshot_build_failed = True
        if snapshot_build_failed or rebased_snapshot is None:
            _fail("rebased completeness snapshot could not be constructed")

        expected_completed_ids = tuple(spec.capture_spec_id for spec in extended_plan.capture_specs[: len(all_run_results)])
        if rebased_snapshot.completed_capture_spec_ids != expected_completed_ids:
            _fail("rebased completeness snapshot completed captures disagree with the extension")
        if rebased_snapshot.completed_capture_spec_ids[-1] != catalog_spec.capture_spec_id:
            _fail("rebased completeness snapshot does not treat the catalog capture as completed")
        expected_new_pending = tuple(spec.capture_spec_id for spec in new_route[1:])
        if set(expected_new_pending) - set(rebased_snapshot.pending_capture_spec_ids):
            _fail("rebased completeness snapshot does not treat the new session's routes as pending")

        # --- Publish extended plan ---
        plan_encode_failed = False
        plan_bytes = b""
        try:
            plan_bytes = encode_quality_pilot_campaign_plan(extended_plan)
        except Exception:
            plan_encode_failed = True
        if plan_encode_failed:
            _fail("extended campaign plan could not be canonically encoded")

        publish_plan_failed = False
        published_plan: object = None
        try:
            published_plan = publish_quality_pilot_control_artifact(extended_plan, bucket, writer)
        except Exception:
            publish_plan_failed = True
        if publish_plan_failed:
            _fail("extended campaign plan could not be published")
        if (
            type(published_plan) is not PublishedQualityPilotControlArtifact
            or published_plan.kind is not QualityPilotControlArtifactKind.CAMPAIGN_PLAN
            or published_plan.artifact_id != extended_plan.plan_id
            or published_plan.pilot_run_id != campaign.pilot_run_id
            or published_plan.protocol_sha256 != campaign.protocol_sha256
            or published_plan.bucket != bucket
            or published_plan.encoded_byte_count != len(plan_bytes)
            or published_plan.encoded_sha256 != sha256(plan_bytes).hexdigest()
        ):
            _fail("published extended campaign plan failed verification")

        # --- Publish rebased snapshot ---
        snapshot_encode_failed = False
        snapshot_bytes = b""
        try:
            snapshot_bytes = encode_quality_pilot_completeness_snapshot(rebased_snapshot)
        except Exception:
            snapshot_encode_failed = True
        if snapshot_encode_failed:
            _fail("rebased completeness snapshot could not be canonically encoded")

        publish_snapshot_failed = False
        published_snapshot: object = None
        try:
            published_snapshot = publish_quality_pilot_control_artifact(rebased_snapshot, bucket, writer)
        except Exception:
            publish_snapshot_failed = True
        if publish_snapshot_failed:
            _fail("rebased completeness snapshot could not be published")
        if (
            type(published_snapshot) is not PublishedQualityPilotControlArtifact
            or published_snapshot.kind is not QualityPilotControlArtifactKind.COMPLETENESS_LEDGER
            or published_snapshot.artifact_id != rebased_snapshot.snapshot_id
            or published_snapshot.pilot_run_id != campaign.pilot_run_id
            or published_snapshot.protocol_sha256 != campaign.protocol_sha256
            or published_snapshot.bucket != bucket
            or published_snapshot.encoded_byte_count != len(snapshot_bytes)
            or published_snapshot.encoded_sha256 != sha256(snapshot_bytes).hexdigest()
        ):
            _fail("published rebased completeness snapshot failed verification")

        # --- Seal the terminal transition ---
        previous_snapshot_id = prior_snapshot.snapshot_id if prior_snapshot is not None else None
        transition_failed = False
        transition_value: QualityPilotLedgerTransition | None = None
        try:
            transition_value = QualityPilotLedgerTransition(
                protocol_sha256=campaign.protocol_sha256,
                pilot_run_id=campaign.pilot_run_id,
                plan_id=extended_plan.plan_id,
                previous_snapshot_id=previous_snapshot_id,
                capture_spec_id=catalog_spec.capture_spec_id,
                run_result_id=catalog_run_result.run_result_id,
                next_snapshot=published_snapshot,
            )
        except Exception:
            transition_failed = True
        if transition_failed or transition_value is None:
            _fail("ledger transition could not be constructed")

        transition_encode_failed = False
        transition_bytes = b""
        try:
            transition_bytes = encode_quality_pilot_ledger_transition(transition_value)
        except Exception:
            transition_encode_failed = True
        if transition_encode_failed:
            _fail("ledger transition could not be canonically encoded for verification")

        publish_transition_failed = False
        published_transition: object = None
        try:
            published_transition = publish_quality_pilot_ledger_transition(transition_value, bucket, writer)
        except Exception:
            publish_transition_failed = True
        if publish_transition_failed:
            _fail("ledger transition could not be sealed")
        if (
            type(published_transition) is not PublishedQualityPilotLedgerTransition
            or published_transition.transition_id != transition_value.transition_id
            or published_transition.plan_id != extended_plan.plan_id
            or published_transition.previous_snapshot_id != previous_snapshot_id
            or published_transition.capture_spec_id != catalog_spec.capture_spec_id
            or published_transition.bucket != bucket
            or published_transition.encoded_byte_count != len(transition_bytes)
            or published_transition.encoded_sha256 != sha256(transition_bytes).hexdigest()
        ):
            _fail("published ledger transition failed verification")

        next_capture_spec_id = quote_0920_specs[0].capture_spec_id

        result_failed = False
        result: QualityPilotSessionBootstrapResult | None = None
        try:
            result = QualityPilotSessionBootstrapResult(
                request_id=verified_request.request_id,
                request=verified_request,
                target_session=target_session,
                catalog_observation_id=catalog_run_result.observation.observation_id,
                catalog_run_result_id=catalog_run_result.run_result_id,
                catalog_capture_spec_id=catalog_spec.capture_spec_id,
                previous_plan_id=(prior_plan.plan_id if prior_plan is not None else None),
                previous_snapshot_id=previous_snapshot_id,
                predecessor_plan=prior_plan,
                predecessor_transition=prior_transition,
                predecessor_snapshot=prior_snapshot,
                plan_id=extended_plan.plan_id,
                plan=extended_plan,
                published_plan=published_plan,
                snapshot_id=rebased_snapshot.snapshot_id,
                snapshot=rebased_snapshot,
                published_snapshot=published_snapshot,
                transition=transition_value,
                transition_id=transition_value.transition_id,
                published_transition=published_transition,
                next_capture_spec_id=next_capture_spec_id,
            )
        except Exception:
            result_failed = True
        if result_failed or result is None:
            _fail("session bootstrap result could not be constructed")
        return result
