"""HYP-002 quality pilot: pure, restart-safe capture orchestration service.

Advances one exact HYP-002 campaign by exactly one capture spec per call,
either as genesis (no predecessor) or as a resume from one exact,
generation-pinned, sealed predecessor transition. The operational hot path
never reloads prior observations: it trusts only the sealed transition's own
``next_snapshot`` metadata to locate the exact predecessor completeness
snapshot, reads that snapshot once, and advances it incrementally by exactly
the one new capture outcome -- so its read count and the incremental
snapshot derivation are both independent of how many captures came before.
Full prior-observation replay remains available as a separate, explicitly
named audit operation (:func:`audit_replay_quality_pilot_completeness_snapshot`)
that the operational service never calls.

The accepted
:class:`~india_swing.quality_pilot.capture_runner.QualityPilotCaptureRunner`
remains sole authority for collector invocation, observation construction,
and observation publication; this module adds only the replay-free resume
gate, the next-spec selection gate, the time-window gate, the incremental
completeness-snapshot advancement, and the predecessor-to-successor
transition seal described in
:mod:`india_swing.quality_pilot.control_plane_store`.

This module performs no filesystem, ambient configuration, wall-clock
creation, network/client construction, Cloud SDK, Kite SDK, process,
scheduler, notification, strategy, research-result, trading, risk, or
capital capability of its own. It never lists a bucket, never resolves a
"head" plan or snapshot, and never retries or sleeps -- every read/write
happens at most once per call, through the injected Protocol boundaries
(reader, collector, writer). Every artifact this module produces remains
permanently quality-only.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import datetime, time, timezone
from hashlib import sha256

from india_swing.daily_pipeline.acquisition import GCSObjectReader
from india_swing.daily_pipeline.state_publication import StateObjectWriter
from india_swing.domain.models import INDIA_STANDARD_TIME
from india_swing.identity import content_id

from .campaign_ledger import (
    QUALITY_PILOT_COMPLETENESS_LEDGER_SCHEMA_VERSION,
    CampaignCompletenessStatus,
    QualityPilotCampaignCompletenessLedger,
    QualityPilotClassificationCount,
)
from .canonical_response import ResponseClassification
from .capture_runner import (
    CONFIRMED_SESSION_COUNT,
    QualityPilotCampaignSpec,
    QualityPilotCaptureRunner,
    QualityPilotCaptureRunResult,
    QualityPilotCollector,
)
from .control_plane_store import (
    LoadedQualityPilotControlArtifact,
    LoadedQualityPilotLedgerTransition,
    PinnedQualityPilotControlArtifactRequest,
    PublishedQualityPilotControlArtifact,
    PublishedQualityPilotLedgerTransition,
    QualityPilotCampaignPlan,
    QualityPilotCompletenessSnapshot,
    QualityPilotControlArtifactKind,
    QualityPilotLedgerTransition,
    build_quality_pilot_completeness_snapshot,
    encode_quality_pilot_completeness_snapshot,
    encode_quality_pilot_ledger_transition,
    publish_quality_pilot_control_artifact,
    publish_quality_pilot_ledger_transition,
    read_pinned_quality_pilot_control_artifact,
    read_pinned_quality_pilot_ledger_transition,
)
from .control_plane_store import (
    PinnedQualityPilotLedgerTransitionRequest as _PinnedTransitionRequest,
)
from .observation_store import (
    LoadedQualityPilotObservation,
    PinnedQualityPilotObservationRequest,
    PublishedQualityPilotObservation,
    QUALITY_OBSERVATION_STORE_POLICY_VERSION,
    read_pinned_quality_pilot_observation,
)


QUALITY_PILOT_RESUMABLE_REQUEST_SCHEMA_VERSION = "quality_pilot_resumable_capture_request_v1"
QUALITY_PILOT_RESUMABLE_RESULT_SCHEMA_VERSION = "quality_pilot_resumable_capture_result_v1"

_SHA256_PATTERN = re.compile(r"[0-9a-f]{64}\Z")


def _is_sha256(value: object) -> bool:
    return type(value) is str and _SHA256_PATTERN.fullmatch(value) is not None


class QualityPilotResumableServiceError(ValueError):
    """A resumable quality-pilot capture request, replay, or artifact failed a static trust rule."""


def _fail(message: str) -> None:
    raise QualityPilotResumableServiceError(message)


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


def _control_artifact_pin_from_published(
    published: PublishedQualityPilotControlArtifact,
) -> PinnedQualityPilotControlArtifactRequest:
    """Deterministically derive a control-artifact pin from an exact, already
    independently reverified publication (e.g. a sealed transition's own
    ``next_snapshot`` metadata) -- the sole predecessor-snapshot authority
    for a resumed capture."""

    reconstructed = _reconstruct_published_control_artifact(published)
    return PinnedQualityPilotControlArtifactRequest(
        storage_policy_version=reconstructed.storage_policy_version,
        protocol_sha256=reconstructed.protocol_sha256,
        kind=reconstructed.kind,
        pilot_run_id=reconstructed.pilot_run_id,
        artifact_id=reconstructed.artifact_id,
        bucket=reconstructed.bucket,
        object_name=reconstructed.object_name,
        generation=reconstructed.generation,
        expected_encoded_sha256=reconstructed.encoded_sha256,
    )


def _confirmed_session_calendar_decision_id(campaign: QualityPilotCampaignSpec, session) -> str:
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
# QualityPilotResumableCaptureRequest
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class QualityPilotResumableCaptureRequest(_FixedPostureMixin):
    """One immutable, independently re-verifiable resumable capture request.

    ``predecessor_transition_pin`` is ``None`` only for a genesis call. When
    present, the pinned, sealed transition's own ``next_snapshot`` metadata
    is the sole authority for which predecessor completeness snapshot this
    call resumes from -- never a caller-supplied snapshot pin directly.
    """

    plan_pin: PinnedQualityPilotControlArtifactRequest
    predecessor_transition_pin: _PinnedTransitionRequest | None
    target_capture_spec_id: str
    invocation_at: datetime
    request_id: str = field(init=False)

    def __post_init__(self) -> None:
        self._validate()
        object.__setattr__(self, "request_id", self._calculated_id())

    def _validate(self) -> None:
        plan_pin = _reconstruct_plan_pin(self.plan_pin)
        if plan_pin.kind is not QualityPilotControlArtifactKind.CAMPAIGN_PLAN:
            _fail("resumable capture request plan pin kind is invalid")
        if self.predecessor_transition_pin is not None:
            transition_pin = _reconstruct_transition_pin(self.predecessor_transition_pin)
            if transition_pin.bucket != plan_pin.bucket:
                _fail(
                    "resumable capture request predecessor transition pin bucket disagrees with the plan pin"
                )
            if transition_pin.pilot_run_id != plan_pin.pilot_run_id:
                _fail(
                    "resumable capture request predecessor transition pin pilot run id disagrees with the plan pin"
                )
            if transition_pin.protocol_sha256 != plan_pin.protocol_sha256:
                _fail(
                    "resumable capture request predecessor transition pin protocol disagrees with the plan pin"
                )
        if not _is_sha256(self.target_capture_spec_id):
            _fail("resumable capture request target capture spec id is invalid")
        if type(self.invocation_at) is not datetime:
            _fail("resumable capture request invocation_at is invalid")
        aware_failed = False
        offset = None
        try:
            offset = self.invocation_at.utcoffset()
        except Exception:
            aware_failed = True
        if aware_failed or offset is None:
            _fail("resumable capture request invocation_at must be timezone-aware")
        if any(getattr(self, name) != (name == "quality_only") for name in _POSTURE_NAMES):
            _fail("resumable capture request safety posture is invalid")

    def _calculated_id(self) -> str:
        failed = False
        calculated = ""
        try:
            calculated = content_id(
                {
                    "schema": QUALITY_PILOT_RESUMABLE_REQUEST_SCHEMA_VERSION,
                    "plan_pin": self.plan_pin,
                    "predecessor_transition_pin": self.predecessor_transition_pin,
                    "target_capture_spec_id": self.target_capture_spec_id,
                    "invocation_at": self.invocation_at.astimezone(timezone.utc),
                    "posture": _posture_tree(self),
                },
                length=64,
            )
        except Exception:
            failed = True
        if failed:
            _fail("resumable capture request identity calculation failed")
        return calculated

    def verify_content_identity(self) -> None:
        self._validate()
        if self.request_id != self._calculated_id():
            _fail("resumable capture request identity failed")


def _reconstruct_request(value: object) -> QualityPilotResumableCaptureRequest:
    if type(value) is not QualityPilotResumableCaptureRequest:
        _fail("resumable capture request type is invalid")
    reconstruct_failed = False
    reconstructed: QualityPilotResumableCaptureRequest | None = None
    try:
        reconstructed = QualityPilotResumableCaptureRequest(
            plan_pin=value.plan_pin,
            predecessor_transition_pin=value.predecessor_transition_pin,
            target_capture_spec_id=value.target_capture_spec_id,
            invocation_at=value.invocation_at,
        )
    except Exception:
        reconstruct_failed = True
    if reconstruct_failed or reconstructed is None:
        _fail("resumable capture request could not be independently reverified")
    if value.request_id != reconstructed.request_id:
        _fail("resumable capture request identity failed independent reverification")
    return reconstructed


# ---------------------------------------------------------------------------
# QualityPilotResumableCaptureResult
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class QualityPilotResumableCaptureResult(_FixedPostureMixin):
    """One immutable, independently re-verifiable record of one completed resumable step.

    Carries the exact, full :class:`QualityPilotLedgerTransition` record (not
    only its opaque publication metadata) so every field of the result can
    be cross-checked against one single inseparable lineage: a result built
    from one branch's run/snapshot but another branch's transition fails
    both construction and :meth:`verify_content_identity`.
    """

    request_id: str
    plan_id: str
    previous_snapshot_id: str | None
    capture_spec_id: str
    run_result_id: str
    snapshot_id: str
    published_snapshot: PublishedQualityPilotControlArtifact
    transition: QualityPilotLedgerTransition
    transition_id: str
    published_transition: PublishedQualityPilotLedgerTransition
    service_result_id: str = field(init=False)

    def __post_init__(self) -> None:
        self._validate()
        object.__setattr__(self, "service_result_id", self._calculated_id())

    def _validate(self) -> None:
        if not _is_sha256(self.request_id):
            _fail("resumable capture result request id is invalid")
        if not _is_sha256(self.plan_id):
            _fail("resumable capture result plan id is invalid")
        if self.previous_snapshot_id is not None and not _is_sha256(self.previous_snapshot_id):
            _fail("resumable capture result previous snapshot id is invalid")
        if not _is_sha256(self.capture_spec_id):
            _fail("resumable capture result capture spec id is invalid")
        if not _is_sha256(self.run_result_id):
            _fail("resumable capture result run result id is invalid")
        if not _is_sha256(self.snapshot_id):
            _fail("resumable capture result snapshot id is invalid")
        if not _is_sha256(self.transition_id):
            _fail("resumable capture result transition id is invalid")

        published_snapshot = _reconstruct_published_control_artifact(self.published_snapshot)
        if published_snapshot.kind is not QualityPilotControlArtifactKind.COMPLETENESS_LEDGER:
            _fail("resumable capture result published snapshot must be a completeness ledger")
        if published_snapshot.artifact_id != self.snapshot_id:
            _fail("resumable capture result published snapshot id disagrees")

        transition = _reconstruct_transition(self.transition)
        if transition.transition_id != self.transition_id:
            _fail("resumable capture result transition id disagrees")
        if transition.plan_id != self.plan_id:
            _fail("resumable capture result transition plan id disagrees")
        if transition.previous_snapshot_id != self.previous_snapshot_id:
            _fail("resumable capture result transition predecessor disagrees")
        if transition.capture_spec_id != self.capture_spec_id:
            _fail("resumable capture result transition capture spec id disagrees")
        if transition.run_result_id != self.run_result_id:
            _fail("resumable capture result transition run result id disagrees")

        reconstructed_next_snapshot = _reconstruct_published_control_artifact(transition.next_snapshot)
        if reconstructed_next_snapshot.artifact_id != self.snapshot_id:
            _fail("resumable capture result transition next snapshot id disagrees")
        if (
            reconstructed_next_snapshot.artifact_id != published_snapshot.artifact_id
            or reconstructed_next_snapshot.object_name != published_snapshot.object_name
            or reconstructed_next_snapshot.bucket != published_snapshot.bucket
            or reconstructed_next_snapshot.generation != published_snapshot.generation
            or reconstructed_next_snapshot.encoded_byte_count != published_snapshot.encoded_byte_count
            or reconstructed_next_snapshot.encoded_sha256 != published_snapshot.encoded_sha256
        ):
            _fail("resumable capture result transition next snapshot disagrees with the published snapshot")

        published_transition = _reconstruct_published_transition(self.published_transition)
        if published_transition.transition_id != transition.transition_id:
            _fail("resumable capture result published transition id disagrees")
        if published_transition.plan_id != transition.plan_id:
            _fail("resumable capture result published transition plan id disagrees")
        if published_transition.previous_snapshot_id != transition.previous_snapshot_id:
            _fail("resumable capture result published transition predecessor disagrees")
        if published_transition.capture_spec_id != transition.capture_spec_id:
            _fail("resumable capture result published transition capture spec id disagrees")
        if published_transition.pilot_run_id != transition.pilot_run_id:
            _fail("resumable capture result published transition pilot run id disagrees")
        if published_transition.protocol_sha256 != transition.protocol_sha256:
            _fail("resumable capture result published transition protocol disagrees")
        if published_transition.bucket != reconstructed_next_snapshot.bucket:
            _fail("resumable capture result published artifacts disagree on bucket")

        # The published transition's byte count and hash are never trusted as
        # given -- an attacker who swaps in a forged PublishedQualityPilotLedgerTransition
        # (same route/identity fields, but a fabricated encoded_byte_count/
        # encoded_sha256) must be caught here by comparing against the exact
        # canonical bytes of the independently reconstructed transition
        # record itself, not merely against another copy of the same
        # untrusted metadata.
        transition_encode_failed = False
        expected_transition_bytes = b""
        try:
            expected_transition_bytes = encode_quality_pilot_ledger_transition(transition)
        except Exception:
            transition_encode_failed = True
        if transition_encode_failed:
            _fail("resumable capture result transition could not be canonically encoded")
        if published_transition.encoded_byte_count != len(expected_transition_bytes):
            _fail("resumable capture result published transition byte count disagrees with its canonical bytes")
        if published_transition.encoded_sha256 != sha256(expected_transition_bytes).hexdigest():
            _fail("resumable capture result published transition hash disagrees with its canonical bytes")

        if any(getattr(self, name) != (name == "quality_only") for name in _POSTURE_NAMES):
            _fail("resumable capture result safety posture is invalid")

    def _calculated_id(self) -> str:
        failed = False
        calculated = ""
        try:
            calculated = content_id(
                {
                    "schema": QUALITY_PILOT_RESUMABLE_RESULT_SCHEMA_VERSION,
                    "request_id": self.request_id,
                    "plan_id": self.plan_id,
                    "previous_snapshot_id": self.previous_snapshot_id,
                    "capture_spec_id": self.capture_spec_id,
                    "run_result_id": self.run_result_id,
                    "snapshot_id": self.snapshot_id,
                    "published_snapshot": self.published_snapshot,
                    "transition": self.transition,
                    "transition_id": self.transition_id,
                    "published_transition": self.published_transition,
                    "posture": _posture_tree(self),
                },
                length=64,
            )
        except Exception:
            failed = True
        if failed:
            _fail("resumable capture result identity calculation failed")
        return calculated

    def verify_content_identity(self) -> None:
        self._validate()
        if self.service_result_id != self._calculated_id():
            _fail("resumable capture result identity failed")


# ---------------------------------------------------------------------------
# Incremental completeness-snapshot advancement (the hot-path derivation)
# ---------------------------------------------------------------------------


def _advance_completeness_snapshot(
    plan: QualityPilotCampaignPlan,
    predecessor: QualityPilotCompletenessSnapshot | None,
    new_run: QualityPilotCaptureRunResult,
) -> QualityPilotCompletenessSnapshot:
    """Deterministically extend a compact snapshot by exactly one new outcome.

    Uses the identical field definitions as
    ``QualityPilotCampaignCompletenessLedger`` /
    ``build_quality_pilot_completeness_snapshot`` -- proven byte-for-byte and
    ID-equivalent to that full-ledger derivation by the accompanying
    regression tests -- without ever reloading a prior observation. Every
    read this function performs is zero: it operates only on the already
    in-memory ``plan`` (one prior read), the already-verified ``predecessor``
    snapshot (one prior read), and the freshly returned ``new_run``.
    """

    evaluated_at = new_run.observation.request.request_ended_at
    normalize_failed = False
    evaluated_utc: datetime | None = None
    try:
        evaluated_utc = evaluated_at.astimezone(timezone.utc)
    except Exception:
        normalize_failed = True
    if normalize_failed or evaluated_utc is None:
        _fail("next completeness snapshot evaluated_at could not be normalized")

    prior_completed = predecessor.completed_capture_spec_ids if predecessor is not None else ()
    prior_run_result_ids = predecessor.run_result_ids if predecessor is not None else ()
    prior_pins = predecessor.pinned_observations if predecessor is not None else ()
    prior_byte_counts = predecessor.published_observation_byte_counts if predecessor is not None else ()
    prior_successful_record_count = predecessor.successful_record_count if predecessor is not None else 0
    prior_published_byte_count = predecessor.published_encoded_byte_count if predecessor is not None else 0
    prior_classification_counts = (
        {item.classification: item.count for item in predecessor.classification_counts}
        if predecessor is not None
        else {}
    )

    new_pin_failed = False
    new_pin: PinnedQualityPilotObservationRequest | None = None
    try:
        new_pin = PinnedQualityPilotObservationRequest(
            bucket=new_run.published.bucket,
            object_name=new_run.published.object_name,
            generation=new_run.published.generation,
            expected_encoded_sha256=new_run.published.encoded_sha256,
            expected_observation_id=new_run.published.observation_id,
            pilot_run_id=new_run.published.pilot_run_id,
            market_session=new_run.published.market_session,
            window_kind=new_run.published.window_kind,
            endpoint_family=new_run.published.endpoint_family,
            chunk_index=new_run.published.chunk_index,
            chunk_count=new_run.published.chunk_count,
        )
    except Exception:
        new_pin_failed = True
    if new_pin_failed or new_pin is None:
        _fail("next completeness snapshot observation pin could not be constructed")

    completed_capture_spec_ids = prior_completed + (new_run.capture_spec_id,)
    completed_set = set(completed_capture_spec_ids)
    run_result_ids = prior_run_result_ids + (new_run.run_result_id,)
    pinned_observations = prior_pins + (new_pin,)
    published_observation_byte_counts = prior_byte_counts + (new_run.published.encoded_byte_count,)
    successful_record_count = prior_successful_record_count + new_run.observation.record_count
    published_encoded_byte_count = prior_published_byte_count + new_run.published.encoded_byte_count

    counts = dict(prior_classification_counts)
    for classification in ResponseClassification:
        counts.setdefault(classification, 0)
    counts[new_run.observation.request.response_classification] += 1
    classification_counts = tuple(
        QualityPilotClassificationCount(classification, counts[classification])
        for classification in ResponseClassification
    )

    expected_specs = plan.capture_specs
    due_ids: list[str] = []
    pending_ids: list[str] = []
    deadline_failed = False
    for spec in expected_specs:
        due = False
        try:
            due = spec.window.closes_at <= evaluated_utc
        except Exception:
            deadline_failed = True
        if due:
            due_ids.append(spec.capture_spec_id)
        else:
            pending_ids.append(spec.capture_spec_id)
    if deadline_failed:
        _fail("next completeness snapshot could not evaluate a capture deadline")
    missing_due_capture_spec_ids = tuple(value for value in due_ids if value not in completed_set)
    pending_capture_spec_ids = tuple(value for value in pending_ids if value not in completed_set)
    due_set = set(due_ids)

    unplanned_due_sessions: list = []
    future_unplanned_sessions: list = []
    planned_session_set = set(plan.planned_sessions)
    for session in plan.campaign.confirmed_sessions:
        if session in planned_session_set:
            continue
        deadline_normalize_failed = False
        deadline = None
        try:
            deadline = datetime.combine(session, time(9, 25), tzinfo=INDIA_STANDARD_TIME).astimezone(
                timezone.utc
            )
        except Exception:
            deadline_normalize_failed = True
        if deadline_normalize_failed:
            _fail("next completeness snapshot could not evaluate an unplanned session deadline")
        if deadline <= evaluated_utc:
            unplanned_due_sessions.append(session)
        else:
            future_unplanned_sessions.append(session)

    completed_by_session = {session: 0 for session in plan.campaign.confirmed_sessions}
    expected_by_session = {session: 0 for session in plan.campaign.confirmed_sessions}
    due_by_session = {session: 0 for session in plan.campaign.confirmed_sessions}
    for spec in expected_specs:
        session = spec.window.market_session
        expected_by_session[session] += 1
        if spec.capture_spec_id in completed_set:
            completed_by_session[session] += 1
        if spec.capture_spec_id in due_set:
            due_by_session[session] += 1
    completed_session_count = sum(
        completed_by_session[session] == expected_by_session[session] for session in plan.planned_sessions
    )
    fully_due_session_count = sum(
        due_by_session[session] == expected_by_session[session] for session in plan.planned_sessions
    )

    if missing_due_capture_spec_ids or unplanned_due_sessions:
        status = CampaignCompletenessStatus.DUE_INCOMPLETE
    elif (
        len(plan.planned_sessions) == CONFIRMED_SESSION_COUNT
        and len(completed_capture_spec_ids) == len(expected_specs)
    ):
        status = CampaignCompletenessStatus.OUTCOMES_COMPLETE
    elif not completed_capture_spec_ids:
        status = CampaignCompletenessStatus.NOT_STARTED
    else:
        status = CampaignCompletenessStatus.IN_PROGRESS

    posture = {name: (name == "quality_only") for name in _POSTURE_NAMES}

    ledger_id_failed = False
    ledger_id = ""
    try:
        ledger_id = content_id(
            {
                "schema": QUALITY_PILOT_COMPLETENESS_LEDGER_SCHEMA_VERSION,
                "plan_id": plan.plan_id,
                "evaluated_at": evaluated_utc,
                "bucket": new_run.requested_bucket,
                "status": status.value,
                "completed_capture_spec_ids": completed_capture_spec_ids,
                "missing_due_capture_spec_ids": missing_due_capture_spec_ids,
                "pending_capture_spec_ids": pending_capture_spec_ids,
                "classification_counts": tuple(
                    (item.classification.value, item.count) for item in classification_counts
                ),
                "completed_session_count": completed_session_count,
                "fully_due_session_count": fully_due_session_count,
                "unplanned_due_sessions": tuple(unplanned_due_sessions),
                "future_unplanned_sessions": tuple(future_unplanned_sessions),
                "successful_record_count": successful_record_count,
                "published_object_count": len(completed_capture_spec_ids),
                "published_encoded_byte_count": published_encoded_byte_count,
                "run_result_ids": run_result_ids,
                "posture": posture,
            },
            length=64,
        )
    except Exception:
        ledger_id_failed = True
    if ledger_id_failed:
        _fail("next completeness ledger identity could not be computed")

    build_failed = False
    snapshot: QualityPilotCompletenessSnapshot | None = None
    try:
        snapshot = QualityPilotCompletenessSnapshot(
            protocol_sha256=plan.campaign.protocol_sha256,
            plan_id=plan.plan_id,
            campaign_id=plan.campaign.campaign_id,
            pilot_run_id=plan.campaign.pilot_run_id,
            ledger_id=ledger_id,
            evaluated_at=evaluated_at,
            bucket=new_run.requested_bucket,
            status=status,
            expected_capture_count=len(expected_specs),
            completed_capture_spec_ids=completed_capture_spec_ids,
            missing_due_capture_spec_ids=missing_due_capture_spec_ids,
            pending_capture_spec_ids=pending_capture_spec_ids,
            classification_counts=classification_counts,
            completed_session_count=completed_session_count,
            fully_due_session_count=fully_due_session_count,
            unplanned_due_sessions=tuple(unplanned_due_sessions),
            future_unplanned_sessions=tuple(future_unplanned_sessions),
            successful_record_count=successful_record_count,
            published_encoded_byte_count=published_encoded_byte_count,
            run_result_ids=run_result_ids,
            pinned_observations=pinned_observations,
            published_observation_byte_counts=published_observation_byte_counts,
        )
    except Exception:
        build_failed = True
    if build_failed or snapshot is None:
        _fail("next completeness snapshot could not be constructed")
    return snapshot


# ---------------------------------------------------------------------------
# Audit-only full replay (never called by QualityPilotResumableCaptureService.run)
# ---------------------------------------------------------------------------


def _reconstruct_prior_run_results(
    plan: QualityPilotCampaignPlan,
    snapshot: QualityPilotCompletenessSnapshot,
    reader: GCSObjectReader,
) -> tuple[QualityPilotCaptureRunResult, ...]:
    count = len(snapshot.completed_capture_spec_ids)
    if (
        len(snapshot.pinned_observations) != count
        or len(snapshot.run_result_ids) != count
        or len(snapshot.published_observation_byte_counts) != count
        or count > len(plan.capture_specs)
    ):
        _fail("predecessor snapshot lineage lengths disagree")

    results: list[QualityPilotCaptureRunResult] = []
    for index in range(count):
        spec = plan.capture_specs[index]
        if spec.capture_spec_id != snapshot.completed_capture_spec_ids[index]:
            _fail("predecessor completed capture spec is not a canonical plan prefix")

        pin = snapshot.pinned_observations[index]
        if type(pin) is not PinnedQualityPilotObservationRequest:
            _fail("predecessor observation pin type is invalid")

        load_failed = False
        loaded: object = None
        try:
            loaded = read_pinned_quality_pilot_observation(pin, reader)
        except Exception:
            load_failed = True
        if load_failed:
            _fail("predecessor observation could not be reloaded")
        if type(loaded) is not LoadedQualityPilotObservation:
            _fail("predecessor observation type is invalid")
        observation = loaded.observation

        byte_count = snapshot.published_observation_byte_counts[index]
        publish_reconstruct_failed = False
        published: PublishedQualityPilotObservation | None = None
        try:
            published = PublishedQualityPilotObservation(
                storage_policy_version=QUALITY_OBSERVATION_STORE_POLICY_VERSION,
                protocol_sha256=observation.request.protocol_sha256,
                observation_id=pin.expected_observation_id,
                pilot_run_id=pin.pilot_run_id,
                market_session=pin.market_session,
                window_kind=pin.window_kind,
                endpoint_family=pin.endpoint_family,
                chunk_index=pin.chunk_index,
                chunk_count=pin.chunk_count,
                bucket=pin.bucket,
                object_name=pin.object_name,
                generation=pin.generation,
                encoded_byte_count=byte_count,
                encoded_sha256=pin.expected_encoded_sha256,
            )
        except Exception:
            publish_reconstruct_failed = True
        if publish_reconstruct_failed or published is None:
            _fail("predecessor published observation could not be reconstructed")

        calendar_decision_id = _confirmed_session_calendar_decision_id(
            plan.campaign, spec.window.market_session
        )

        run_result_failed = False
        run_result: QualityPilotCaptureRunResult | None = None
        try:
            run_result = QualityPilotCaptureRunResult(
                campaign=plan.campaign,
                capture_spec=spec,
                campaign_id=plan.campaign.campaign_id,
                capture_spec_id=spec.capture_spec_id,
                requested_bucket=snapshot.bucket,
                calendar_decision_id=calendar_decision_id,
                observation=observation,
                published=published,
            )
        except Exception:
            run_result_failed = True
        if run_result_failed or run_result is None:
            _fail("predecessor capture run result could not be reconstructed")
        if run_result.run_result_id != snapshot.run_result_ids[index]:
            _fail("predecessor capture run result identity disagrees with its snapshot")
        results.append(run_result)
    return tuple(results)


def audit_replay_quality_pilot_completeness_snapshot(
    plan: QualityPilotCampaignPlan,
    snapshot: QualityPilotCompletenessSnapshot,
    reader: GCSObjectReader,
) -> tuple[QualityPilotCaptureRunResult, ...]:
    """Full prior-observation reload and independent replay verification.

    Reloads every pinned observation behind ``snapshot`` (one read each),
    reconstructs every prior :class:`QualityPilotCaptureRunResult`, rebuilds
    the completeness ledger and its compact snapshot from that replay, and
    requires the rebuilt snapshot to reproduce ``snapshot`` byte-for-byte.

    This is a periodic/final quality-audit operation only, with cost
    proportional to the number of completed captures. It is never called by
    :meth:`QualityPilotResumableCaptureService.run`, whose hot path instead
    trusts the sealed predecessor transition plus its own snapshot in
    constant reads.
    """

    if type(plan) is not QualityPilotCampaignPlan:
        _fail("audit replay plan type is invalid")
    plan_verify_failed = False
    try:
        plan.verify_content_identity()
    except Exception:
        plan_verify_failed = True
    if plan_verify_failed:
        _fail("audit replay plan failed independent verification")

    if type(snapshot) is not QualityPilotCompletenessSnapshot:
        _fail("audit replay snapshot type is invalid")
    snapshot_verify_failed = False
    try:
        snapshot.verify_content_identity()
    except Exception:
        snapshot_verify_failed = True
    if snapshot_verify_failed:
        _fail("audit replay snapshot failed independent verification")

    prior_run_results = _reconstruct_prior_run_results(plan, snapshot, reader)

    ledger_failed = False
    ledger: QualityPilotCampaignCompletenessLedger | None = None
    try:
        ledger = QualityPilotCampaignCompletenessLedger(
            plan, prior_run_results, snapshot.evaluated_at, snapshot.bucket
        )
    except Exception:
        ledger_failed = True
    if ledger_failed or ledger is None:
        _fail("audit replay ledger could not be independently rebuilt")

    rebuild_failed = False
    rebuilt: QualityPilotCompletenessSnapshot | None = None
    try:
        rebuilt = build_quality_pilot_completeness_snapshot(ledger)
    except Exception:
        rebuild_failed = True
    if rebuild_failed or rebuilt is None:
        _fail("audit replay snapshot could not be independently rebuilt")

    encode_failed = False
    rebuilt_bytes = b""
    original_bytes = b""
    try:
        rebuilt_bytes = encode_quality_pilot_completeness_snapshot(rebuilt)
        original_bytes = encode_quality_pilot_completeness_snapshot(snapshot)
    except Exception:
        encode_failed = True
    if encode_failed or rebuilt.snapshot_id != snapshot.snapshot_id or rebuilt_bytes != original_bytes:
        _fail("audit replay could not reproduce the sealed snapshot")

    return prior_run_results


# ---------------------------------------------------------------------------
# QualityPilotResumableCaptureService
# ---------------------------------------------------------------------------


class QualityPilotResumableCaptureService:
    """Stateless, restart-safe orchestration: one replay-free resume, one capture, one seal.

    Holds no campaign progress of its own -- every call to :meth:`run` is an
    independent, fully self-verifying transformation from one pinned plan
    (and optional pinned predecessor transition) to one sealed transition.
    The read count for a resume is exactly three (plan, transition,
    snapshot), independent of how many captures came before it.
    """

    def run(
        self,
        request: QualityPilotResumableCaptureRequest,
        reader: GCSObjectReader,
        collector: QualityPilotCollector,
        writer: StateObjectWriter,
    ) -> QualityPilotResumableCaptureResult:
        if type(request) is not QualityPilotResumableCaptureRequest:
            _fail("resumable capture request must be an exact QualityPilotResumableCaptureRequest")
        verified_request = _reconstruct_request(request)

        plan_load_failed = False
        loaded_plan: object = None
        try:
            loaded_plan = read_pinned_quality_pilot_control_artifact(verified_request.plan_pin, reader)
        except Exception:
            plan_load_failed = True
        if plan_load_failed:
            _fail("campaign plan could not be loaded")
        if (
            type(loaded_plan) is not LoadedQualityPilotControlArtifact
            or type(loaded_plan.artifact) is not QualityPilotCampaignPlan
        ):
            _fail("campaign plan artifact type is invalid")
        plan = loaded_plan.artifact
        plan_verify_failed = False
        try:
            plan.verify_content_identity()
        except Exception:
            plan_verify_failed = True
        if plan_verify_failed:
            _fail("campaign plan failed independent verification")
        if (
            plan.campaign.protocol_sha256 != verified_request.plan_pin.protocol_sha256
            or plan.campaign.pilot_run_id != verified_request.plan_pin.pilot_run_id
            or plan.plan_id != verified_request.plan_pin.artifact_id
        ):
            _fail("campaign plan lineage disagrees with its pin")
        if len(plan.capture_specs) == 0:
            _fail("campaign plan is empty")

        bucket = verified_request.plan_pin.bucket

        previous_snapshot_id: str | None
        predecessor_evaluated_at: datetime | None
        predecessor_snapshot: QualityPilotCompletenessSnapshot | None
        predecessor_completed_ids: tuple[str, ...]

        if verified_request.predecessor_transition_pin is None:
            if plan.capture_specs[0].capture_spec_id != verified_request.target_capture_spec_id:
                _fail("genesis target capture spec must be the first spec in the plan")
            previous_snapshot_id = None
            predecessor_evaluated_at = None
            predecessor_snapshot = None
            predecessor_completed_ids = ()
        else:
            transition_load_failed = False
            loaded_transition: object = None
            try:
                loaded_transition = read_pinned_quality_pilot_ledger_transition(
                    verified_request.predecessor_transition_pin, reader
                )
            except Exception:
                transition_load_failed = True
            if transition_load_failed:
                _fail("predecessor transition could not be loaded")
            if type(loaded_transition) is not LoadedQualityPilotLedgerTransition:
                _fail("predecessor transition type is invalid")
            transition = loaded_transition.transition
            transition_verify_failed = False
            try:
                transition.verify_content_identity()
            except Exception:
                transition_verify_failed = True
            if transition_verify_failed:
                _fail("predecessor transition failed independent verification")
            if (
                transition.plan_id != plan.plan_id
                or transition.pilot_run_id != plan.campaign.pilot_run_id
                or transition.protocol_sha256 != plan.campaign.protocol_sha256
            ):
                _fail("predecessor transition lineage disagrees with the plan")

            predecessor_snapshot_pin = _control_artifact_pin_from_published(transition.next_snapshot)
            snapshot_load_failed = False
            loaded_snapshot: object = None
            try:
                loaded_snapshot = read_pinned_quality_pilot_control_artifact(predecessor_snapshot_pin, reader)
            except Exception:
                snapshot_load_failed = True
            if snapshot_load_failed:
                _fail("predecessor snapshot could not be loaded")
            if (
                type(loaded_snapshot) is not LoadedQualityPilotControlArtifact
                or type(loaded_snapshot.artifact) is not QualityPilotCompletenessSnapshot
            ):
                _fail("predecessor snapshot artifact type is invalid")
            predecessor_snapshot = loaded_snapshot.artifact
            predecessor_verify_failed = False
            try:
                predecessor_snapshot.verify_content_identity()
            except Exception:
                predecessor_verify_failed = True
            if predecessor_verify_failed:
                _fail("predecessor snapshot failed independent verification")
            if (
                predecessor_snapshot.snapshot_id != transition.next_snapshot.artifact_id
                or predecessor_snapshot.plan_id != plan.plan_id
                or predecessor_snapshot.pilot_run_id != plan.campaign.pilot_run_id
                or predecessor_snapshot.protocol_sha256 != plan.campaign.protocol_sha256
                or predecessor_snapshot.bucket != bucket
            ):
                _fail("predecessor snapshot lineage disagrees with its transition or plan")

            expected_ids = tuple(spec.capture_spec_id for spec in plan.capture_specs)
            predecessor_completed_ids = predecessor_snapshot.completed_capture_spec_ids
            if predecessor_completed_ids != expected_ids[: len(predecessor_completed_ids)]:
                _fail("predecessor completed captures are not a canonical plan prefix")
            if len(predecessor_completed_ids) >= len(expected_ids):
                _fail("campaign plan has no next capture spec after its predecessor")
            if verified_request.target_capture_spec_id != expected_ids[len(predecessor_completed_ids)]:
                _fail("target capture spec is not the exact next spec after the predecessor")

            previous_snapshot_id = predecessor_snapshot.snapshot_id
            predecessor_evaluated_at = predecessor_snapshot.evaluated_at

        target_index = len(predecessor_completed_ids)
        target_spec = plan.capture_specs[target_index]
        if target_spec.capture_spec_id != verified_request.target_capture_spec_id:
            _fail("target capture spec disagrees with the plan")

        invocation_at = verified_request.invocation_at
        window_gate_failed = False
        inside_window = False
        precedes_predecessor = False
        try:
            invocation_utc = invocation_at.astimezone(timezone.utc)
            opens_utc = target_spec.window.opens_at.astimezone(timezone.utc)
            closes_utc = target_spec.window.closes_at.astimezone(timezone.utc)
            inside_window = opens_utc <= invocation_utc <= closes_utc
            if predecessor_evaluated_at is not None:
                precedes_predecessor = invocation_utc < predecessor_evaluated_at.astimezone(timezone.utc)
        except Exception:
            window_gate_failed = True
        if window_gate_failed:
            _fail("invocation time could not be normalized")
        if precedes_predecessor:
            _fail("invocation time precedes the predecessor evaluation time")
        if not inside_window:
            _fail("invocation time lies outside the target capture window")

        run_failed = False
        run_result: object = None
        try:
            run_result = QualityPilotCaptureRunner().run(target_spec, collector, bucket, writer)
        except Exception:
            run_failed = True
        if run_failed:
            _fail("capture run failed")
        if type(run_result) is not QualityPilotCaptureRunResult:
            _fail("capture run result type is invalid")

        lineage_check_failed = False
        observation_started_before_invocation = False
        try:
            observation_started_before_invocation = (
                run_result.observation.request.request_started_at < invocation_at.astimezone(timezone.utc)
            )
        except Exception:
            lineage_check_failed = True
        if lineage_check_failed:
            _fail("capture run observation timestamp could not be verified")
        if observation_started_before_invocation:
            _fail("capture run observation predates its invocation")
        if (
            run_result.capture_spec_id != target_spec.capture_spec_id
            or run_result.campaign_id != plan.campaign.campaign_id
            or run_result.requested_bucket != bucket
        ):
            _fail("capture run lineage disagrees with the selected target spec")

        next_snapshot_value = _advance_completeness_snapshot(plan, predecessor_snapshot, run_result)

        publish_snapshot_failed = False
        published_snapshot: object = None
        try:
            published_snapshot = publish_quality_pilot_control_artifact(next_snapshot_value, bucket, writer)
        except Exception:
            publish_snapshot_failed = True
        if publish_snapshot_failed:
            _fail("next completeness snapshot could not be published")
        if (
            type(published_snapshot) is not PublishedQualityPilotControlArtifact
            or published_snapshot.kind is not QualityPilotControlArtifactKind.COMPLETENESS_LEDGER
            or published_snapshot.artifact_id != next_snapshot_value.snapshot_id
            or published_snapshot.pilot_run_id != plan.campaign.pilot_run_id
            or published_snapshot.protocol_sha256 != plan.campaign.protocol_sha256
            or published_snapshot.bucket != bucket
        ):
            _fail("published next snapshot failed verification")

        transition_failed = False
        transition_value: QualityPilotLedgerTransition | None = None
        try:
            transition_value = QualityPilotLedgerTransition(
                protocol_sha256=plan.campaign.protocol_sha256,
                pilot_run_id=plan.campaign.pilot_run_id,
                plan_id=plan.plan_id,
                previous_snapshot_id=previous_snapshot_id,
                capture_spec_id=target_spec.capture_spec_id,
                run_result_id=run_result.run_result_id,
                next_snapshot=published_snapshot,
            )
        except Exception:
            transition_failed = True
        if transition_failed or transition_value is None:
            _fail("ledger transition could not be constructed")

        publish_transition_failed = False
        published_transition: object = None
        try:
            published_transition = publish_quality_pilot_ledger_transition(transition_value, bucket, writer)
        except Exception:
            publish_transition_failed = True
        if publish_transition_failed:
            _fail("ledger transition could not be sealed")

        # Never rely only on publish_quality_pilot_ledger_transition's own
        # internal hash/byte-count check -- independently recompute the
        # transition's exact canonical bytes here and require the returned
        # publication to match them before this run is allowed to succeed.
        transition_encode_failed = False
        transition_bytes = b""
        try:
            transition_bytes = encode_quality_pilot_ledger_transition(transition_value)
        except Exception:
            transition_encode_failed = True
        if transition_encode_failed:
            _fail("ledger transition could not be canonically encoded for verification")

        if (
            type(published_transition) is not PublishedQualityPilotLedgerTransition
            or published_transition.transition_id != transition_value.transition_id
            or published_transition.plan_id != plan.plan_id
            or published_transition.previous_snapshot_id != previous_snapshot_id
            or published_transition.capture_spec_id != target_spec.capture_spec_id
            or published_transition.bucket != bucket
            or published_transition.encoded_byte_count != len(transition_bytes)
            or published_transition.encoded_sha256 != sha256(transition_bytes).hexdigest()
        ):
            _fail("published ledger transition failed verification")

        result_failed = False
        result: QualityPilotResumableCaptureResult | None = None
        try:
            result = QualityPilotResumableCaptureResult(
                request_id=verified_request.request_id,
                plan_id=plan.plan_id,
                previous_snapshot_id=previous_snapshot_id,
                capture_spec_id=target_spec.capture_spec_id,
                run_result_id=run_result.run_result_id,
                snapshot_id=next_snapshot_value.snapshot_id,
                published_snapshot=published_snapshot,
                transition=transition_value,
                transition_id=transition_value.transition_id,
                published_transition=published_transition,
            )
        except Exception:
            result_failed = True
        if result_failed or result is None:
            _fail("resumable capture result could not be constructed")
        return result
