from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from enum import Enum
from hashlib import sha256

from india_swing.daily_pipeline.acquisition import GCSObjectPayload, GCSObjectReader
from india_swing.daily_pipeline.state_publication import PublishedStateObject, StateObjectWriter
from india_swing.identity import content_id

from .campaign_ledger import (
    CampaignCompletenessStatus,
    QualityPilotCampaignCompletenessLedger,
    QualityPilotCampaignPlan,
    QualityPilotClassificationCount,
)
from .canonical_response import (
    PILOT_PROTOCOL_SHA256,
    EndpointFamily,
    ObservationWindowSpec,
    ResponseClassification,
    ScheduledWindowKind,
)
from .capture_runner import QualityPilotCampaignSpec, QualityPilotCaptureSpec
from .observation_store import PinnedQualityPilotObservationRequest


QUALITY_PILOT_CONTROL_STORE_POLICY_VERSION = "quality_pilot_control_store_v1"
QUALITY_PILOT_PLAN_WIRE_SCHEMA_VERSION = "quality_pilot_campaign_plan_wire_v1"
QUALITY_PILOT_LEDGER_SNAPSHOT_SCHEMA_VERSION = "quality_pilot_ledger_snapshot_v1"
QUALITY_PILOT_LEDGER_TRANSITION_SCHEMA_VERSION = "quality_pilot_ledger_transition_v1"
QUALITY_PILOT_CONTROL_CONTENT_TYPE = "application/json"
MAXIMUM_PLAN_BYTES = 256 * 1024 * 1024
MAXIMUM_LEDGER_SNAPSHOT_BYTES = 128 * 1024 * 1024
MAXIMUM_TRANSITION_BYTES = 64 * 1024

_SHA256_PATTERN = re.compile(r"[0-9a-f]{64}\Z")
_BUCKET_PATTERN = re.compile(r"[a-z0-9][a-z0-9\-_.]{1,61}[a-z0-9]\Z")
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


class QualityPilotControlStoreError(ValueError):
    """A quality-pilot control artifact failed a static trust rule."""


def _fail(message: str) -> None:
    raise QualityPilotControlStoreError(message)


def _is_sha256(value: object) -> bool:
    return type(value) is str and _SHA256_PATTERN.fullmatch(value) is not None


def _posture_tree(value: object) -> dict[str, bool]:
    return {name: getattr(value, name) for name in _POSTURE_NAMES}


class _FixedPostureMixin:
    """Read-only, fixed fail-closed posture. ``__slots__ = ()`` combined with a
    true ``@property`` for every name means no instance ever has a ``__dict__``
    entry or any other storage for these names -- ``object.__setattr__`` on any
    of them raises ``AttributeError`` immediately, it can never silently
    succeed and be read back later.
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


class QualityPilotControlArtifactKind(Enum):
    CAMPAIGN_PLAN = "CAMPAIGN_PLAN"
    COMPLETENESS_LEDGER = "COMPLETENESS_LEDGER"


def _maximum_bytes(kind: QualityPilotControlArtifactKind) -> int:
    if kind is QualityPilotControlArtifactKind.CAMPAIGN_PLAN:
        return MAXIMUM_PLAN_BYTES
    if kind is QualityPilotControlArtifactKind.COMPLETENESS_LEDGER:
        return MAXIMUM_LEDGER_SNAPSHOT_BYTES
    _fail("control artifact kind is invalid")


def _canonical_json_bytes(tree: object) -> bytes:
    failed = False
    encoded = b""
    try:
        encoded = (
            json.dumps(
                tree,
                ensure_ascii=False,
                allow_nan=False,
                separators=(",", ":"),
                sort_keys=True,
            )
            + "\n"
        ).encode("utf-8")
    except Exception:
        failed = True
    if failed:
        _fail("control artifact could not be canonically encoded")
    return encoded


def _reject_float(_: str) -> object:
    _fail("control artifact numbers must be exact integers")


def _reject_constant(_: str) -> object:
    _fail("control artifact contains a non-finite number")


def _reject_duplicate_pairs(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            _fail("control artifact contains a duplicate key")
        result[key] = value
    return result


def _parse_json(content_bytes: object, maximum_bytes: int) -> dict[str, object]:
    if type(content_bytes) is not bytes:
        _fail("control artifact content must be exact bytes")
    if not content_bytes or len(content_bytes) > maximum_bytes:
        _fail("control artifact content size is invalid")
    decode_failed = False
    text = ""
    try:
        text = content_bytes.decode("utf-8", errors="strict")
    except Exception:
        decode_failed = True
    if decode_failed:
        _fail("control artifact is not strict UTF-8")
    parse_failed = False
    value: object = None
    try:
        value = json.loads(
            text,
            object_pairs_hook=_reject_duplicate_pairs,
            parse_float=_reject_float,
            parse_constant=_reject_constant,
        )
    except QualityPilotControlStoreError:
        raise
    except Exception:
        parse_failed = True
    if parse_failed:
        _fail("control artifact is not valid JSON")
    if type(value) is not dict:
        _fail("control artifact root must be an exact object")
    return value


def _exact_dict(value: object, keys: set[str], message: str) -> dict[str, object]:
    if type(value) is not dict or set(value) != keys:
        _fail(message)
    return value


def _exact_list(value: object, message: str) -> list[object]:
    if type(value) is not list:
        _fail(message)
    return value


def _text(value: object, message: str) -> str:
    if type(value) is not str or not value:
        _fail(message)
    return value


def _integer(value: object, message: str, *, minimum: int = 0) -> int:
    if type(value) is not int or value < minimum:
        _fail(message)
    return value


def _date_text(value: object, message: str) -> date:
    if type(value) is not str:
        _fail(message)
    failed = False
    parsed: date | None = None
    try:
        parsed = date.fromisoformat(value)
    except Exception:
        failed = True
    if failed or parsed is None:
        _fail(message)
    if parsed.isoformat() != value:
        _fail(message)
    return parsed


def _datetime_text(value: object, message: str) -> datetime:
    if type(value) is not str:
        _fail(message)
    failed = False
    parsed: datetime | None = None
    offset = None
    try:
        parsed = datetime.fromisoformat(value)
        offset = parsed.utcoffset()
    except Exception:
        failed = True
    if failed or parsed is None:
        _fail(message)
    if offset is None or parsed.isoformat() != value:
        _fail(message)
    return parsed


def _enum(value: object, enum_type: type[Enum], message: str) -> Enum:
    if type(value) is not str:
        _fail(message)
    failed = False
    parsed: Enum | None = None
    try:
        parsed = enum_type(value)
    except Exception:
        failed = True
    if failed or parsed is None:
        _fail(message)
    return parsed


def _campaign_tree(value: QualityPilotCampaignSpec) -> dict[str, object]:
    return {
        "calendar_decision_ids": list(value.calendar_decision_ids),
        "campaign_id": value.campaign_id,
        "confirmed_sessions": [item.isoformat() for item in value.confirmed_sessions],
        "pilot_run_id": value.pilot_run_id,
        "protocol_sha256": value.protocol_sha256,
    }


def _window_tree(value: ObservationWindowSpec) -> dict[str, object]:
    return json.loads(value.canonical_json())


def _capture_spec_tree(value: QualityPilotCaptureSpec) -> dict[str, object]:
    return {
        "capture_spec_id": value.capture_spec_id,
        "chunk_count": value.chunk_count,
        "chunk_index": value.chunk_index,
        "protocol_sha256": value.protocol_sha256,
        "provider": value.provider,
        "provider_instrument_token": value.provider_instrument_token,
        "provider_version": value.provider_version,
        "requested_keys": list(value.requested_keys),
        "window": _window_tree(value.window),
    }


def _plan_tree(value: QualityPilotCampaignPlan) -> dict[str, object]:
    return {
        "campaign": _campaign_tree(value.campaign),
        "capture_specs": [_capture_spec_tree(item) for item in value.capture_specs],
        "plan_id": value.plan_id,
        "planned_sessions": [item.isoformat() for item in value.planned_sessions],
        "posture": _posture_tree(value),
        "schema_version": QUALITY_PILOT_PLAN_WIRE_SCHEMA_VERSION,
    }


def encode_quality_pilot_campaign_plan(plan: QualityPilotCampaignPlan) -> bytes:
    if type(plan) is not QualityPilotCampaignPlan:
        _fail("campaign plan type is invalid")
    failed = False
    try:
        plan.verify_content_identity()
    except Exception:
        failed = True
    if failed:
        _fail("campaign plan failed independent verification")
    encoded = _canonical_json_bytes(_plan_tree(plan))
    if len(encoded) > MAXIMUM_PLAN_BYTES:
        _fail("campaign plan encoding exceeds its bounded size")
    return encoded


def _decode_campaign(value: object) -> QualityPilotCampaignSpec:
    record = _exact_dict(
        value,
        {"calendar_decision_ids", "campaign_id", "confirmed_sessions", "pilot_run_id", "protocol_sha256"},
        "campaign record has an invalid shape",
    )
    sessions = tuple(
        _date_text(item, "campaign session is invalid")
        for item in _exact_list(record["confirmed_sessions"], "campaign sessions are invalid")
    )
    decisions = tuple(
        _text(item, "campaign decision id is invalid")
        for item in _exact_list(record["calendar_decision_ids"], "campaign decisions are invalid")
    )
    failed = False
    campaign: QualityPilotCampaignSpec | None = None
    try:
        campaign = QualityPilotCampaignSpec(
            pilot_run_id=_text(record["pilot_run_id"], "campaign pilot run id is invalid"),
            protocol_sha256=_text(record["protocol_sha256"], "campaign protocol is invalid"),
            confirmed_sessions=sessions,
            calendar_decision_ids=decisions,
        )
    except Exception:
        failed = True
    if failed or campaign is None:
        _fail("campaign record failed reconstruction")
    if campaign.campaign_id != record["campaign_id"]:
        _fail("campaign record identity failed")
    return campaign


def _decode_capture_spec(value: object, campaign: QualityPilotCampaignSpec) -> QualityPilotCaptureSpec:
    record = _exact_dict(
        value,
        {"capture_spec_id", "chunk_count", "chunk_index", "protocol_sha256", "provider", "provider_instrument_token", "provider_version", "requested_keys", "window"},
        "capture spec record has an invalid shape",
    )
    failed = False
    spec: QualityPilotCaptureSpec | None = None
    try:
        window = ObservationWindowSpec._decode(record["window"])
        token = record["provider_instrument_token"]
        if token is not None and type(token) is not int:
            _fail("capture spec token is invalid")
        spec = QualityPilotCaptureSpec(
            campaign=campaign,
            window=window,
            provider=_text(record["provider"], "capture spec provider is invalid"),
            provider_version=_text(record["provider_version"], "capture spec provider version is invalid"),
            requested_keys=tuple(
                _text(item, "capture spec key is invalid")
                for item in _exact_list(record["requested_keys"], "capture spec keys are invalid")
            ),
            provider_instrument_token=token,
            chunk_index=_integer(record["chunk_index"], "capture spec chunk index is invalid", minimum=1),
            chunk_count=_integer(record["chunk_count"], "capture spec chunk count is invalid", minimum=1),
            protocol_sha256=_text(record["protocol_sha256"], "capture spec protocol is invalid"),
        )
    except QualityPilotControlStoreError:
        raise
    except Exception:
        failed = True
    if failed or spec is None:
        _fail("capture spec record failed reconstruction")
    if spec.capture_spec_id != record["capture_spec_id"]:
        _fail("capture spec record identity failed")
    return spec


def decode_quality_pilot_campaign_plan(content_bytes: bytes) -> QualityPilotCampaignPlan:
    root = _parse_json(content_bytes, MAXIMUM_PLAN_BYTES)
    record = _exact_dict(
        root,
        {"campaign", "capture_specs", "plan_id", "planned_sessions", "posture", "schema_version"},
        "campaign plan wire shape is invalid",
    )
    if record["schema_version"] != QUALITY_PILOT_PLAN_WIRE_SCHEMA_VERSION:
        _fail("campaign plan wire schema is invalid")
    campaign = _decode_campaign(record["campaign"])
    specs = tuple(
        _decode_capture_spec(item, campaign)
        for item in _exact_list(record["capture_specs"], "campaign plan specs are invalid")
    )
    failed = False
    plan: QualityPilotCampaignPlan | None = None
    try:
        plan = QualityPilotCampaignPlan(campaign=campaign, capture_specs=specs)
    except Exception:
        failed = True
    if failed or plan is None:
        _fail("campaign plan failed reconstruction")
    planned = tuple(
        _date_text(item, "campaign planned session is invalid")
        for item in _exact_list(record["planned_sessions"], "campaign planned sessions are invalid")
    )
    if (
        plan.plan_id != record["plan_id"]
        or plan.planned_sessions != planned
        or record["posture"] != _posture_tree(plan)
        or encode_quality_pilot_campaign_plan(plan) != content_bytes
    ):
        _fail("campaign plan wire identity failed")
    return plan


@dataclass(frozen=True, slots=True)
class QualityPilotCompletenessSnapshot(_FixedPostureMixin):
    protocol_sha256: str
    plan_id: str
    campaign_id: str
    pilot_run_id: str
    ledger_id: str
    evaluated_at: datetime
    bucket: str
    status: CampaignCompletenessStatus
    expected_capture_count: int
    completed_capture_spec_ids: tuple[str, ...]
    missing_due_capture_spec_ids: tuple[str, ...]
    pending_capture_spec_ids: tuple[str, ...]
    classification_counts: tuple[QualityPilotClassificationCount, ...]
    completed_session_count: int
    fully_due_session_count: int
    unplanned_due_sessions: tuple[date, ...]
    future_unplanned_sessions: tuple[date, ...]
    successful_record_count: int
    published_encoded_byte_count: int
    run_result_ids: tuple[str, ...]
    pinned_observations: tuple[PinnedQualityPilotObservationRequest, ...]
    published_observation_byte_counts: tuple[int, ...]
    snapshot_id: str = field(init=False)

    def __post_init__(self) -> None:
        self._validate()
        object.__setattr__(self, "snapshot_id", self._calculated_id())

    def _validate(self) -> None:
        if self.protocol_sha256 != PILOT_PROTOCOL_SHA256:
            _fail("ledger snapshot protocol hash is invalid")
        for value in (self.plan_id, self.campaign_id, self.pilot_run_id, self.ledger_id):
            if not _is_sha256(value):
                _fail("ledger snapshot identity field is invalid")
        if type(self.evaluated_at) is not datetime or self.evaluated_at.utcoffset() is None:
            _fail("ledger snapshot evaluated_at is invalid")
        if type(self.bucket) is not str or _BUCKET_PATTERN.fullmatch(self.bucket) is None:
            _fail("ledger snapshot bucket is invalid")
        if type(self.status) is not CampaignCompletenessStatus:
            _fail("ledger snapshot status is invalid")
        integer_fields = (
            self.expected_capture_count,
            self.completed_session_count,
            self.fully_due_session_count,
            self.successful_record_count,
            self.published_encoded_byte_count,
        )
        if any(type(value) is not int or value < 0 for value in integer_fields):
            _fail("ledger snapshot count is invalid")
        id_groups = (
            self.completed_capture_spec_ids,
            self.missing_due_capture_spec_ids,
            self.pending_capture_spec_ids,
            self.run_result_ids,
        )
        if any(type(group) is not tuple or any(not _is_sha256(item) for item in group) for group in id_groups):
            _fail("ledger snapshot id collection is invalid")
        if any(len(group) != len(set(group)) for group in id_groups):
            _fail("ledger snapshot id collection contains duplicates")
        if set(self.completed_capture_spec_ids) & set(self.missing_due_capture_spec_ids):
            _fail("ledger snapshot capture states overlap")
        if set(self.completed_capture_spec_ids) & set(self.pending_capture_spec_ids):
            _fail("ledger snapshot capture states overlap")
        if len(self.completed_capture_spec_ids) + len(self.missing_due_capture_spec_ids) + len(self.pending_capture_spec_ids) != self.expected_capture_count:
            _fail("ledger snapshot capture counts disagree")
        expected_classes = tuple(ResponseClassification)
        classifications_valid = type(self.classification_counts) is tuple
        if classifications_valid:
            classifications_valid = all(
                type(item) is QualityPilotClassificationCount
                for item in self.classification_counts
            )
        if classifications_valid:
            classifications_valid = (
                tuple(item.classification for item in self.classification_counts)
                == expected_classes
                and sum(item.count for item in self.classification_counts)
                == len(self.completed_capture_spec_ids)
            )
        if not classifications_valid:
            _fail("ledger snapshot classifications disagree")
        date_groups = (self.unplanned_due_sessions, self.future_unplanned_sessions)
        if any(
            type(group) is not tuple
            or any(type(item) is not date for item in group)
            or group != tuple(sorted(set(group)))
            for group in date_groups
        ):
            _fail("ledger snapshot unplanned sessions are invalid")
        if set(self.unplanned_due_sessions) & set(self.future_unplanned_sessions):
            _fail("ledger snapshot unplanned session states overlap")
        if self.completed_session_count > 20 or self.fully_due_session_count > 20:
            _fail("ledger snapshot session count exceeds the campaign")
        if type(self.pinned_observations) is not tuple or len(self.pinned_observations) != len(self.completed_capture_spec_ids):
            _fail("ledger snapshot observation pins disagree")
        for pin in self.pinned_observations:
            if type(pin) is not PinnedQualityPilotObservationRequest:
                _fail("ledger snapshot observation pin type is invalid")
            pin_failed = False
            try:
                pin._validate()
            except Exception:
                pin_failed = True
            if pin_failed:
                _fail("ledger snapshot observation pin failed verification")
            if pin.bucket != self.bucket or pin.pilot_run_id != self.pilot_run_id:
                _fail("ledger snapshot observation pin lineage disagrees")
        if len(self.run_result_ids) != len(self.pinned_observations):
            _fail("ledger snapshot run results disagree with observation pins")
        if (
            type(self.published_observation_byte_counts) is not tuple
            or len(self.published_observation_byte_counts) != len(self.pinned_observations)
            or any(
                type(value) is not int or value <= 0
                for value in self.published_observation_byte_counts
            )
            or sum(self.published_observation_byte_counts)
            != self.published_encoded_byte_count
        ):
            _fail("ledger snapshot published byte lineage disagrees")
        observation_ids = tuple(
            pin.expected_observation_id for pin in self.pinned_observations
        )
        object_names = tuple(pin.object_name for pin in self.pinned_observations)
        if len(observation_ids) != len(set(observation_ids)) or len(object_names) != len(set(object_names)):
            _fail("ledger snapshot observation pins contain duplicates")
        if self.pinned_observations and self.published_encoded_byte_count <= 0:
            _fail("ledger snapshot published bytes disagree with its pins")
        if self.status is CampaignCompletenessStatus.NOT_STARTED and self.completed_capture_spec_ids:
            _fail("ledger snapshot status disagrees with completed captures")
        if self.status is CampaignCompletenessStatus.DUE_INCOMPLETE and not (
            self.missing_due_capture_spec_ids or self.unplanned_due_sessions
        ):
            _fail("ledger snapshot due-incomplete status has no due gap")
        if self.status is CampaignCompletenessStatus.OUTCOMES_COMPLETE and (
            len(self.completed_capture_spec_ids) != self.expected_capture_count
            or self.missing_due_capture_spec_ids
            or self.pending_capture_spec_ids
            or self.unplanned_due_sessions
            or self.future_unplanned_sessions
        ):
            _fail("ledger snapshot complete status disagrees with its outcomes")
        if any(getattr(self, name) != (name == "quality_only") for name in _POSTURE_NAMES):
            _fail("ledger snapshot safety posture is invalid")

    def _calculated_id(self) -> str:
        failed = False
        calculated = ""
        try:
            calculated = content_id(
                _snapshot_tree(self, include_snapshot_id=False), length=64
            )
        except Exception:
            failed = True
        if failed:
            _fail("ledger snapshot identity calculation failed")
        return calculated

    def verify_content_identity(self) -> None:
        self._validate()
        if self.snapshot_id != self._calculated_id():
            _fail("ledger snapshot identity failed")


def _pin_tree(value: PinnedQualityPilotObservationRequest) -> dict[str, object]:
    return {
        "bucket": value.bucket,
        "chunk_count": value.chunk_count,
        "chunk_index": value.chunk_index,
        "endpoint_family": value.endpoint_family.value,
        "expected_encoded_sha256": value.expected_encoded_sha256,
        "expected_observation_id": value.expected_observation_id,
        "generation": value.generation,
        "market_session": value.market_session.isoformat(),
        "object_name": value.object_name,
        "pilot_run_id": value.pilot_run_id,
        "window_kind": value.window_kind.value,
    }


def _snapshot_tree(value: QualityPilotCompletenessSnapshot, *, include_snapshot_id: bool) -> dict[str, object]:
    tree: dict[str, object] = {
        "bucket": value.bucket,
        "campaign_id": value.campaign_id,
        "classification_counts": [
            {"classification": item.classification.value, "count": item.count}
            for item in value.classification_counts
        ],
        "completed_capture_spec_ids": list(value.completed_capture_spec_ids),
        "completed_session_count": value.completed_session_count,
        "evaluated_at": value.evaluated_at.astimezone(timezone.utc).isoformat(),
        "expected_capture_count": value.expected_capture_count,
        "fully_due_session_count": value.fully_due_session_count,
        "future_unplanned_sessions": [item.isoformat() for item in value.future_unplanned_sessions],
        "ledger_id": value.ledger_id,
        "missing_due_capture_spec_ids": list(value.missing_due_capture_spec_ids),
        "pending_capture_spec_ids": list(value.pending_capture_spec_ids),
        "pilot_run_id": value.pilot_run_id,
        "pinned_observations": [_pin_tree(item) for item in value.pinned_observations],
        "plan_id": value.plan_id,
        "posture": _posture_tree(value),
        "protocol_sha256": value.protocol_sha256,
        "published_encoded_byte_count": value.published_encoded_byte_count,
        "published_observation_byte_counts": list(
            value.published_observation_byte_counts
        ),
        "run_result_ids": list(value.run_result_ids),
        "schema_version": QUALITY_PILOT_LEDGER_SNAPSHOT_SCHEMA_VERSION,
        "status": value.status.value,
        "successful_record_count": value.successful_record_count,
        "unplanned_due_sessions": [item.isoformat() for item in value.unplanned_due_sessions],
    }
    if include_snapshot_id:
        tree["snapshot_id"] = value.snapshot_id
    return tree


def build_quality_pilot_completeness_snapshot(ledger: QualityPilotCampaignCompletenessLedger) -> QualityPilotCompletenessSnapshot:
    if type(ledger) is not QualityPilotCampaignCompletenessLedger:
        _fail("completeness ledger type is invalid")
    failed = False
    try:
        ledger.verify_content_identity()
    except Exception:
        failed = True
    if failed:
        _fail("completeness ledger failed independent verification")
    pins = tuple(
        PinnedQualityPilotObservationRequest(
            bucket=run.published.bucket,
            object_name=run.published.object_name,
            generation=run.published.generation,
            expected_encoded_sha256=run.published.encoded_sha256,
            expected_observation_id=run.published.observation_id,
            pilot_run_id=run.published.pilot_run_id,
            market_session=run.published.market_session,
            window_kind=run.published.window_kind,
            endpoint_family=run.published.endpoint_family,
            chunk_index=run.published.chunk_index,
            chunk_count=run.published.chunk_count,
        )
        for run in ledger.run_results
    )
    return QualityPilotCompletenessSnapshot(
        protocol_sha256=ledger.plan.campaign.protocol_sha256,
        plan_id=ledger.plan.plan_id,
        campaign_id=ledger.plan.campaign.campaign_id,
        pilot_run_id=ledger.plan.campaign.pilot_run_id,
        ledger_id=ledger.ledger_id,
        evaluated_at=ledger.evaluated_at,
        bucket=ledger.bucket,
        status=ledger.status,
        expected_capture_count=ledger.expected_capture_count,
        completed_capture_spec_ids=ledger.completed_capture_spec_ids,
        missing_due_capture_spec_ids=ledger.missing_due_capture_spec_ids,
        pending_capture_spec_ids=ledger.pending_capture_spec_ids,
        classification_counts=ledger.classification_counts,
        completed_session_count=ledger.completed_session_count,
        fully_due_session_count=ledger.fully_due_session_count,
        unplanned_due_sessions=ledger.unplanned_due_sessions,
        future_unplanned_sessions=ledger.future_unplanned_sessions,
        successful_record_count=ledger.successful_record_count,
        published_encoded_byte_count=ledger.published_encoded_byte_count,
        run_result_ids=tuple(run.run_result_id for run in ledger.run_results),
        pinned_observations=pins,
        published_observation_byte_counts=tuple(
            run.published.encoded_byte_count for run in ledger.run_results
        ),
    )


def encode_quality_pilot_completeness_snapshot(snapshot: QualityPilotCompletenessSnapshot) -> bytes:
    if type(snapshot) is not QualityPilotCompletenessSnapshot:
        _fail("ledger snapshot type is invalid")
    snapshot.verify_content_identity()
    encoded = _canonical_json_bytes(_snapshot_tree(snapshot, include_snapshot_id=True))
    if len(encoded) > MAXIMUM_LEDGER_SNAPSHOT_BYTES:
        _fail("ledger snapshot encoding exceeds its bounded size")
    return encoded


def _decode_pin(value: object) -> PinnedQualityPilotObservationRequest:
    record = _exact_dict(
        value,
        {"bucket", "chunk_count", "chunk_index", "endpoint_family", "expected_encoded_sha256", "expected_observation_id", "generation", "market_session", "object_name", "pilot_run_id", "window_kind"},
        "ledger observation pin shape is invalid",
    )
    failed = False
    pin: PinnedQualityPilotObservationRequest | None = None
    try:
        pin = PinnedQualityPilotObservationRequest(
            bucket=_text(record["bucket"], "ledger observation pin bucket is invalid"),
            object_name=_text(record["object_name"], "ledger observation pin object is invalid"),
            generation=_integer(record["generation"], "ledger observation pin generation is invalid", minimum=1),
            expected_encoded_sha256=_text(record["expected_encoded_sha256"], "ledger observation pin hash is invalid"),
            expected_observation_id=_text(record["expected_observation_id"], "ledger observation pin id is invalid"),
            pilot_run_id=_text(record["pilot_run_id"], "ledger observation pin run is invalid"),
            market_session=_date_text(record["market_session"], "ledger observation pin session is invalid"),
            window_kind=_enum(record["window_kind"], ScheduledWindowKind, "ledger observation pin window is invalid"),
            endpoint_family=_enum(record["endpoint_family"], EndpointFamily, "ledger observation pin endpoint is invalid"),
            chunk_index=_integer(record["chunk_index"], "ledger observation pin chunk is invalid", minimum=1),
            chunk_count=_integer(record["chunk_count"], "ledger observation pin chunk count is invalid", minimum=1),
        )
    except QualityPilotControlStoreError:
        raise
    except Exception:
        failed = True
    if failed or pin is None:
        _fail("ledger observation pin failed reconstruction")
    return pin


def decode_quality_pilot_completeness_snapshot(content_bytes: bytes) -> QualityPilotCompletenessSnapshot:
    root = _parse_json(content_bytes, MAXIMUM_LEDGER_SNAPSHOT_BYTES)
    keys = {
        "bucket", "campaign_id", "classification_counts", "completed_capture_spec_ids",
        "completed_session_count", "evaluated_at", "expected_capture_count",
        "fully_due_session_count", "future_unplanned_sessions", "ledger_id",
        "missing_due_capture_spec_ids", "pending_capture_spec_ids", "pilot_run_id",
        "pinned_observations", "plan_id", "posture", "published_encoded_byte_count",
        "published_observation_byte_counts",
        "protocol_sha256", "run_result_ids", "schema_version", "snapshot_id", "status",
        "successful_record_count", "unplanned_due_sessions",
    }
    record = _exact_dict(root, keys, "ledger snapshot wire shape is invalid")
    if record["schema_version"] != QUALITY_PILOT_LEDGER_SNAPSHOT_SCHEMA_VERSION:
        _fail("ledger snapshot wire schema is invalid")
    classifications: list[QualityPilotClassificationCount] = []
    for item in _exact_list(record["classification_counts"], "ledger classifications are invalid"):
        row = _exact_dict(item, {"classification", "count"}, "ledger classification shape is invalid")
        classifications.append(
            QualityPilotClassificationCount(
                classification=_enum(row["classification"], ResponseClassification, "ledger classification is invalid"),
                count=_integer(row["count"], "ledger classification count is invalid"),
            )
        )
    def ids(name: str) -> tuple[str, ...]:
        return tuple(
            _text(item, "ledger snapshot id is invalid")
            for item in _exact_list(record[name], "ledger snapshot ids are invalid")
        )
    failed = False
    snapshot: QualityPilotCompletenessSnapshot | None = None
    try:
        snapshot = QualityPilotCompletenessSnapshot(
            protocol_sha256=_text(record["protocol_sha256"], "ledger protocol hash is invalid"),
            plan_id=_text(record["plan_id"], "ledger plan id is invalid"),
            campaign_id=_text(record["campaign_id"], "ledger campaign id is invalid"),
            pilot_run_id=_text(record["pilot_run_id"], "ledger pilot run id is invalid"),
            ledger_id=_text(record["ledger_id"], "ledger id is invalid"),
            evaluated_at=_datetime_text(record["evaluated_at"], "ledger evaluated_at is invalid"),
            bucket=_text(record["bucket"], "ledger bucket is invalid"),
            status=_enum(record["status"], CampaignCompletenessStatus, "ledger status is invalid"),
            expected_capture_count=_integer(record["expected_capture_count"], "ledger expected count is invalid"),
            completed_capture_spec_ids=ids("completed_capture_spec_ids"),
            missing_due_capture_spec_ids=ids("missing_due_capture_spec_ids"),
            pending_capture_spec_ids=ids("pending_capture_spec_ids"),
            classification_counts=tuple(classifications),
            completed_session_count=_integer(record["completed_session_count"], "ledger completed session count is invalid"),
            fully_due_session_count=_integer(record["fully_due_session_count"], "ledger due session count is invalid"),
            unplanned_due_sessions=tuple(_date_text(item, "ledger unplanned session is invalid") for item in _exact_list(record["unplanned_due_sessions"], "ledger unplanned sessions are invalid")),
            future_unplanned_sessions=tuple(_date_text(item, "ledger future session is invalid") for item in _exact_list(record["future_unplanned_sessions"], "ledger future sessions are invalid")),
            successful_record_count=_integer(record["successful_record_count"], "ledger record count is invalid"),
            published_encoded_byte_count=_integer(record["published_encoded_byte_count"], "ledger byte count is invalid"),
            run_result_ids=ids("run_result_ids"),
            pinned_observations=tuple(_decode_pin(item) for item in _exact_list(record["pinned_observations"], "ledger observation pins are invalid")),
            published_observation_byte_counts=tuple(
                _integer(item, "ledger published observation byte count is invalid", minimum=1)
                for item in _exact_list(
                    record["published_observation_byte_counts"],
                    "ledger published observation byte counts are invalid",
                )
            ),
        )
    except QualityPilotControlStoreError:
        raise
    except Exception:
        failed = True
    if failed or snapshot is None:
        _fail("ledger snapshot failed reconstruction")
    if (
        snapshot.snapshot_id != record["snapshot_id"]
        or record["posture"] != _posture_tree(snapshot)
        or encode_quality_pilot_completeness_snapshot(snapshot) != content_bytes
    ):
        _fail("ledger snapshot wire identity failed")
    return snapshot


def canonical_quality_pilot_control_object_name(kind: QualityPilotControlArtifactKind, pilot_run_id: str, artifact_id: str) -> str:
    if type(kind) is not QualityPilotControlArtifactKind or not _is_sha256(pilot_run_id) or not _is_sha256(artifact_id):
        _fail("control artifact route is invalid")
    lane = "plans" if kind is QualityPilotControlArtifactKind.CAMPAIGN_PLAN else "ledgers"
    return f"quality-pilot/v1/{pilot_run_id}/control/{lane}/{artifact_id}.json"


@dataclass(frozen=True, slots=True)
class PublishedQualityPilotControlArtifact(_FixedPostureMixin):
    storage_policy_version: str
    protocol_sha256: str
    kind: QualityPilotControlArtifactKind
    pilot_run_id: str
    artifact_id: str
    bucket: str
    object_name: str
    generation: int
    encoded_byte_count: int
    encoded_sha256: str

    def __post_init__(self) -> None:
        if self.storage_policy_version != QUALITY_PILOT_CONTROL_STORE_POLICY_VERSION:
            _fail("published control artifact storage policy is invalid")
        if self.protocol_sha256 != PILOT_PROTOCOL_SHA256:
            _fail("published control artifact protocol hash is invalid")
        if type(self.kind) is not QualityPilotControlArtifactKind:
            _fail("published control artifact kind is invalid")
        if not _is_sha256(self.pilot_run_id) or not _is_sha256(self.artifact_id):
            _fail("published control artifact identity is invalid")
        if type(self.bucket) is not str or _BUCKET_PATTERN.fullmatch(self.bucket) is None:
            _fail("published control artifact bucket is invalid")
        if self.object_name != canonical_quality_pilot_control_object_name(self.kind, self.pilot_run_id, self.artifact_id):
            _fail("published control artifact route is invalid")
        if type(self.generation) is not int or self.generation <= 0:
            _fail("published control artifact generation is invalid")
        if type(self.encoded_byte_count) is not int or self.encoded_byte_count <= 0 or self.encoded_byte_count > _maximum_bytes(self.kind):
            _fail("published control artifact byte count is invalid")
        if not _is_sha256(self.encoded_sha256):
            _fail("published control artifact hash is invalid")


def publish_quality_pilot_control_artifact(artifact: object, bucket: str, writer: StateObjectWriter) -> PublishedQualityPilotControlArtifact:
    if type(artifact) is QualityPilotCampaignPlan:
        kind = QualityPilotControlArtifactKind.CAMPAIGN_PLAN
        pilot_run_id = artifact.campaign.pilot_run_id
        artifact_id = artifact.plan_id
        content_bytes = encode_quality_pilot_campaign_plan(artifact)
    elif type(artifact) is QualityPilotCompletenessSnapshot:
        kind = QualityPilotControlArtifactKind.COMPLETENESS_LEDGER
        pilot_run_id = artifact.pilot_run_id
        artifact_id = artifact.snapshot_id
        content_bytes = encode_quality_pilot_completeness_snapshot(artifact)
    else:
        _fail("control artifact type is invalid")
    if type(bucket) is not str or _BUCKET_PATTERN.fullmatch(bucket) is None:
        _fail("control artifact bucket is invalid")
    object_name = canonical_quality_pilot_control_object_name(kind, pilot_run_id, artifact_id)
    expected_hash = sha256(content_bytes).hexdigest()
    failed = False
    published: object = None
    try:
        published = writer.create_or_verify(
            bucket=bucket,
            object_name=object_name,
            content_bytes=content_bytes,
            content_type=QUALITY_PILOT_CONTROL_CONTENT_TYPE,
            maximum_bytes=_maximum_bytes(kind),
        )
    except Exception:
        failed = True
    if failed or type(published) is not PublishedStateObject:
        _fail("control artifact writer failed")
    if (
        published.object_name != object_name
        or published.byte_count != len(content_bytes)
        or published.sha256 != expected_hash
    ):
        _fail("control artifact writer result failed verification")
    return PublishedQualityPilotControlArtifact(
        storage_policy_version=QUALITY_PILOT_CONTROL_STORE_POLICY_VERSION,
        protocol_sha256=PILOT_PROTOCOL_SHA256,
        kind=kind,
        pilot_run_id=pilot_run_id,
        artifact_id=artifact_id,
        bucket=bucket,
        object_name=object_name,
        generation=published.generation,
        encoded_byte_count=len(content_bytes),
        encoded_sha256=expected_hash,
    )


@dataclass(frozen=True, slots=True)
class PinnedQualityPilotControlArtifactRequest:
    storage_policy_version: str
    protocol_sha256: str
    kind: QualityPilotControlArtifactKind
    pilot_run_id: str
    artifact_id: str
    bucket: str
    object_name: str
    generation: int
    expected_encoded_sha256: str

    def __post_init__(self) -> None:
        if self.storage_policy_version != QUALITY_PILOT_CONTROL_STORE_POLICY_VERSION:
            _fail("pinned control artifact storage policy is invalid")
        if self.protocol_sha256 != PILOT_PROTOCOL_SHA256:
            _fail("pinned control artifact protocol hash is invalid")
        if type(self.kind) is not QualityPilotControlArtifactKind:
            _fail("pinned control artifact kind is invalid")
        if not _is_sha256(self.pilot_run_id) or not _is_sha256(self.artifact_id):
            _fail("pinned control artifact identity is invalid")
        if type(self.bucket) is not str or _BUCKET_PATTERN.fullmatch(self.bucket) is None:
            _fail("pinned control artifact bucket is invalid")
        if self.object_name != canonical_quality_pilot_control_object_name(self.kind, self.pilot_run_id, self.artifact_id):
            _fail("pinned control artifact route is invalid")
        if type(self.generation) is not int or self.generation <= 0:
            _fail("pinned control artifact generation is invalid")
        if not _is_sha256(self.expected_encoded_sha256):
            _fail("pinned control artifact hash is invalid")


@dataclass(frozen=True, slots=True)
class LoadedQualityPilotControlArtifact:
    artifact: QualityPilotCampaignPlan | QualityPilotCompletenessSnapshot
    request: PinnedQualityPilotControlArtifactRequest


def read_pinned_quality_pilot_control_artifact(request: PinnedQualityPilotControlArtifactRequest, reader: GCSObjectReader) -> LoadedQualityPilotControlArtifact:
    if type(request) is not PinnedQualityPilotControlArtifactRequest:
        _fail("pinned control artifact request type is invalid")
    failed = False
    payload: object = None
    try:
        payload = reader.read_generation(
            bucket=request.bucket,
            object_name=request.object_name,
            generation=request.generation,
            maximum_bytes=_maximum_bytes(request.kind),
        )
    except Exception:
        failed = True
    if failed or type(payload) is not GCSObjectPayload:
        _fail("control artifact reader failed")
    if type(payload.generation) is not int or payload.generation != request.generation:
        _fail("control artifact reader generation failed verification")
    content_bytes = payload.content_bytes
    if type(content_bytes) is not bytes or not content_bytes or len(content_bytes) > _maximum_bytes(request.kind):
        _fail("control artifact reader content failed verification")
    if sha256(content_bytes).hexdigest() != request.expected_encoded_sha256:
        _fail("control artifact reader hash failed verification")
    artifact = (
        decode_quality_pilot_campaign_plan(content_bytes)
        if request.kind is QualityPilotControlArtifactKind.CAMPAIGN_PLAN
        else decode_quality_pilot_completeness_snapshot(content_bytes)
    )
    observed_id = artifact.plan_id if type(artifact) is QualityPilotCampaignPlan else artifact.snapshot_id
    observed_pilot_run_id = artifact.campaign.pilot_run_id if type(artifact) is QualityPilotCampaignPlan else artifact.pilot_run_id
    observed_protocol = artifact.campaign.protocol_sha256 if type(artifact) is QualityPilotCampaignPlan else artifact.protocol_sha256
    if (
        observed_id != request.artifact_id
        or observed_pilot_run_id != request.pilot_run_id
        or observed_protocol != request.protocol_sha256
    ):
        _fail("control artifact reader lineage failed verification")
    return LoadedQualityPilotControlArtifact(artifact=artifact, request=request)


# ---------------------------------------------------------------------------
# Ledger transition -- one create-only predecessor-to-successor seal
# ---------------------------------------------------------------------------
#
# A transition's object path is derived only from (pilot_run_id, plan_id,
# predecessor token, capture_spec_id) -- deliberately NOT content-addressed
# by the new result. Every concurrently attempted successor for the same
# predecessor and exact next spec therefore collides at exactly one
# create-only path: the first writer to seal that path wins, and any
# different successor's distinct bytes fail the create_or_verify hash/byte
# comparison below, exactly like every other publish boundary in this
# module. This is the single mechanism that makes concurrent branches
# resolve to one accepted campaign head.


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


def pinned_quality_pilot_control_artifact_request(
    published: PublishedQualityPilotControlArtifact,
) -> PinnedQualityPilotControlArtifactRequest:
    """Deterministically derive a pin from an exact, independently reverified publication.

    Symmetric with :func:`pinned_quality_pilot_ledger_transition_request` --
    the published value is treated as untrusted and reconstructed/reverified
    first via :func:`_reconstruct_published_control_artifact`.
    """

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


def canonical_quality_pilot_transition_object_name(
    pilot_run_id: str, plan_id: str, predecessor_token: str, capture_spec_id: str
) -> str:
    if not _is_sha256(pilot_run_id) or not _is_sha256(plan_id) or not _is_sha256(capture_spec_id):
        _fail("ledger transition route is invalid")
    if predecessor_token != "genesis" and not _is_sha256(predecessor_token):
        _fail("ledger transition predecessor token is invalid")
    return (
        f"quality-pilot/v1/{pilot_run_id}/control/transitions/"
        f"{plan_id}/{predecessor_token}/{capture_spec_id}.json"
    )


def _predecessor_token(previous_snapshot_id: str | None) -> str:
    return "genesis" if previous_snapshot_id is None else previous_snapshot_id


@dataclass(frozen=True, slots=True)
class QualityPilotLedgerTransition(_FixedPostureMixin):
    """One immutable, independently re-verifiable predecessor-to-successor seal.

    Binds the exact prior snapshot (``None`` only for genesis), the exact
    capture spec resolved by this transition, the exact capture run result,
    and the already-published next completeness snapshot's full storage
    lineage. Deliberately does not itself pick a "head" artifact -- that
    is purely a consequence of which transition object, if any, occupies
    this deterministic create-only path.
    """

    protocol_sha256: str
    pilot_run_id: str
    plan_id: str
    previous_snapshot_id: str | None
    capture_spec_id: str
    run_result_id: str
    next_snapshot: PublishedQualityPilotControlArtifact
    transition_id: str = field(init=False)

    def __post_init__(self) -> None:
        self._validate()
        object.__setattr__(self, "transition_id", self._calculated_id())

    def _validate(self) -> None:
        if self.protocol_sha256 != PILOT_PROTOCOL_SHA256:
            _fail("ledger transition protocol hash is invalid")
        if not _is_sha256(self.pilot_run_id):
            _fail("ledger transition pilot run id is invalid")
        if not _is_sha256(self.plan_id):
            _fail("ledger transition plan id is invalid")
        if self.previous_snapshot_id is not None and not _is_sha256(self.previous_snapshot_id):
            _fail("ledger transition previous snapshot id is invalid")
        if not _is_sha256(self.capture_spec_id):
            _fail("ledger transition capture spec id is invalid")
        if not _is_sha256(self.run_result_id):
            _fail("ledger transition run result id is invalid")
        next_snapshot = _reconstruct_published_control_artifact(self.next_snapshot)
        if next_snapshot.kind is not QualityPilotControlArtifactKind.COMPLETENESS_LEDGER:
            _fail("ledger transition next snapshot must be a completeness ledger artifact")
        if next_snapshot.protocol_sha256 != self.protocol_sha256:
            _fail("ledger transition next snapshot protocol hash disagrees")
        if next_snapshot.pilot_run_id != self.pilot_run_id:
            _fail("ledger transition next snapshot pilot run id disagrees")
        if self.previous_snapshot_id is not None and next_snapshot.artifact_id == self.previous_snapshot_id:
            _fail("ledger transition next snapshot cannot equal its own predecessor")
        if any(getattr(self, name) != (name == "quality_only") for name in _POSTURE_NAMES):
            _fail("ledger transition safety posture is invalid")

    def _calculated_id(self) -> str:
        failed = False
        calculated = ""
        try:
            calculated = content_id(
                {
                    "schema": QUALITY_PILOT_LEDGER_TRANSITION_SCHEMA_VERSION,
                    "protocol_sha256": self.protocol_sha256,
                    "pilot_run_id": self.pilot_run_id,
                    "plan_id": self.plan_id,
                    "previous_snapshot_id": self.previous_snapshot_id,
                    "capture_spec_id": self.capture_spec_id,
                    "run_result_id": self.run_result_id,
                    "next_snapshot": self.next_snapshot,
                    "posture": _posture_tree(self),
                },
                length=64,
            )
        except Exception:
            failed = True
        if failed:
            _fail("ledger transition identity calculation failed")
        return calculated

    def verify_content_identity(self) -> None:
        self._validate()
        if self.transition_id != self._calculated_id():
            _fail("ledger transition identity failed")


def _next_snapshot_tree(value: PublishedQualityPilotControlArtifact) -> dict[str, object]:
    return {
        "artifact_id": value.artifact_id,
        "bucket": value.bucket,
        "encoded_byte_count": value.encoded_byte_count,
        "encoded_sha256": value.encoded_sha256,
        "generation": value.generation,
        "kind": value.kind.value,
        "object_name": value.object_name,
        "pilot_run_id": value.pilot_run_id,
        "protocol_sha256": value.protocol_sha256,
        "storage_policy_version": value.storage_policy_version,
    }


def _transition_tree(value: QualityPilotLedgerTransition, *, include_transition_id: bool) -> dict[str, object]:
    tree: dict[str, object] = {
        "capture_spec_id": value.capture_spec_id,
        "next_snapshot": _next_snapshot_tree(value.next_snapshot),
        "pilot_run_id": value.pilot_run_id,
        "plan_id": value.plan_id,
        "posture": _posture_tree(value),
        "previous_snapshot_id": value.previous_snapshot_id,
        "protocol_sha256": value.protocol_sha256,
        "run_result_id": value.run_result_id,
        "schema_version": QUALITY_PILOT_LEDGER_TRANSITION_SCHEMA_VERSION,
    }
    if include_transition_id:
        tree["transition_id"] = value.transition_id
    return tree


def encode_quality_pilot_ledger_transition(value: QualityPilotLedgerTransition) -> bytes:
    if type(value) is not QualityPilotLedgerTransition:
        _fail("ledger transition type is invalid")
    value.verify_content_identity()
    encoded = _canonical_json_bytes(_transition_tree(value, include_transition_id=True))
    if len(encoded) > MAXIMUM_TRANSITION_BYTES:
        _fail("ledger transition encoding exceeds its bounded size")
    return encoded


_TRANSITION_NEXT_SNAPSHOT_KEYS = {
    "artifact_id", "bucket", "encoded_byte_count", "encoded_sha256", "generation",
    "kind", "object_name", "pilot_run_id", "protocol_sha256", "storage_policy_version",
}
_TRANSITION_KEYS = {
    "capture_spec_id", "next_snapshot", "pilot_run_id", "plan_id", "posture",
    "previous_snapshot_id", "protocol_sha256", "run_result_id", "schema_version",
    "transition_id",
}


def _decode_next_snapshot(value: object) -> PublishedQualityPilotControlArtifact:
    record = _exact_dict(value, _TRANSITION_NEXT_SNAPSHOT_KEYS, "ledger transition next snapshot shape is invalid")
    failed = False
    result: PublishedQualityPilotControlArtifact | None = None
    try:
        result = PublishedQualityPilotControlArtifact(
            storage_policy_version=_text(record["storage_policy_version"], "ledger transition next snapshot storage policy is invalid"),
            protocol_sha256=_text(record["protocol_sha256"], "ledger transition next snapshot protocol is invalid"),
            kind=_enum(record["kind"], QualityPilotControlArtifactKind, "ledger transition next snapshot kind is invalid"),
            pilot_run_id=_text(record["pilot_run_id"], "ledger transition next snapshot pilot run id is invalid"),
            artifact_id=_text(record["artifact_id"], "ledger transition next snapshot artifact id is invalid"),
            bucket=_text(record["bucket"], "ledger transition next snapshot bucket is invalid"),
            object_name=_text(record["object_name"], "ledger transition next snapshot object name is invalid"),
            generation=_integer(record["generation"], "ledger transition next snapshot generation is invalid", minimum=1),
            encoded_byte_count=_integer(record["encoded_byte_count"], "ledger transition next snapshot byte count is invalid", minimum=1),
            encoded_sha256=_text(record["encoded_sha256"], "ledger transition next snapshot hash is invalid"),
        )
    except QualityPilotControlStoreError:
        raise
    except Exception:
        failed = True
    if failed or result is None:
        _fail("ledger transition next snapshot failed reconstruction")
    return result


def decode_quality_pilot_ledger_transition(content_bytes: bytes) -> QualityPilotLedgerTransition:
    root = _parse_json(content_bytes, MAXIMUM_TRANSITION_BYTES)
    record = _exact_dict(root, _TRANSITION_KEYS, "ledger transition wire shape is invalid")
    if record["schema_version"] != QUALITY_PILOT_LEDGER_TRANSITION_SCHEMA_VERSION:
        _fail("ledger transition wire schema is invalid")
    previous = record["previous_snapshot_id"]
    if previous is not None and type(previous) is not str:
        _fail("ledger transition previous snapshot id is invalid")
    next_snapshot = _decode_next_snapshot(record["next_snapshot"])
    failed = False
    transition: QualityPilotLedgerTransition | None = None
    try:
        transition = QualityPilotLedgerTransition(
            protocol_sha256=_text(record["protocol_sha256"], "ledger transition protocol is invalid"),
            pilot_run_id=_text(record["pilot_run_id"], "ledger transition pilot run id is invalid"),
            plan_id=_text(record["plan_id"], "ledger transition plan id is invalid"),
            previous_snapshot_id=previous,
            capture_spec_id=_text(record["capture_spec_id"], "ledger transition capture spec id is invalid"),
            run_result_id=_text(record["run_result_id"], "ledger transition run result id is invalid"),
            next_snapshot=next_snapshot,
        )
    except QualityPilotControlStoreError:
        raise
    except Exception:
        failed = True
    if failed or transition is None:
        _fail("ledger transition failed reconstruction")
    if (
        transition.transition_id != record["transition_id"]
        or record["posture"] != _posture_tree(transition)
        or encode_quality_pilot_ledger_transition(transition) != content_bytes
    ):
        _fail("ledger transition wire identity failed")
    return transition


@dataclass(frozen=True, slots=True)
class PublishedQualityPilotLedgerTransition(_FixedPostureMixin):
    """One immutable, independently re-verifiable record of a sealed transition."""

    storage_policy_version: str
    protocol_sha256: str
    pilot_run_id: str
    plan_id: str
    previous_snapshot_id: str | None
    capture_spec_id: str
    transition_id: str
    bucket: str
    object_name: str
    generation: int
    encoded_byte_count: int
    encoded_sha256: str

    def __post_init__(self) -> None:
        if self.storage_policy_version != QUALITY_PILOT_CONTROL_STORE_POLICY_VERSION:
            _fail("published ledger transition storage policy is invalid")
        if self.protocol_sha256 != PILOT_PROTOCOL_SHA256:
            _fail("published ledger transition protocol hash is invalid")
        if not _is_sha256(self.pilot_run_id) or not _is_sha256(self.plan_id) or not _is_sha256(self.capture_spec_id) or not _is_sha256(self.transition_id):
            _fail("published ledger transition identity is invalid")
        if self.previous_snapshot_id is not None and not _is_sha256(self.previous_snapshot_id):
            _fail("published ledger transition previous snapshot id is invalid")
        if type(self.bucket) is not str or _BUCKET_PATTERN.fullmatch(self.bucket) is None:
            _fail("published ledger transition bucket is invalid")
        expected_object_name = canonical_quality_pilot_transition_object_name(
            self.pilot_run_id, self.plan_id, _predecessor_token(self.previous_snapshot_id), self.capture_spec_id
        )
        if self.object_name != expected_object_name:
            _fail("published ledger transition route is invalid")
        if type(self.generation) is not int or self.generation <= 0:
            _fail("published ledger transition generation is invalid")
        if (
            type(self.encoded_byte_count) is not int
            or self.encoded_byte_count <= 0
            or self.encoded_byte_count > MAXIMUM_TRANSITION_BYTES
        ):
            _fail("published ledger transition byte count is invalid")
        if not _is_sha256(self.encoded_sha256):
            _fail("published ledger transition hash is invalid")


def publish_quality_pilot_ledger_transition(
    transition: QualityPilotLedgerTransition, bucket: str, writer: StateObjectWriter
) -> PublishedQualityPilotLedgerTransition:
    """Seal one predecessor-to-successor transition at its one create-only path.

    Calls ``writer.create_or_verify`` exactly once. A byte-identical reseal
    of the same transition is idempotent (the writer returns the same
    stored generation/hash for identical bytes at the same path). A
    different successor attempting the same path receives back the
    *already-stored* object's hash/byte count from the writer, which this
    function then finds disagrees with its own freshly encoded bytes, and
    fails closed -- never overwriting, retrying, or selecting a winner
    itself.
    """

    if type(transition) is not QualityPilotLedgerTransition:
        _fail("ledger transition type is invalid")
    content_bytes = encode_quality_pilot_ledger_transition(transition)
    if type(bucket) is not str or _BUCKET_PATTERN.fullmatch(bucket) is None:
        _fail("ledger transition bucket is invalid")
    if bucket != transition.next_snapshot.bucket:
        _fail("ledger transition bucket disagrees with its next snapshot")
    object_name = canonical_quality_pilot_transition_object_name(
        transition.pilot_run_id,
        transition.plan_id,
        _predecessor_token(transition.previous_snapshot_id),
        transition.capture_spec_id,
    )
    expected_hash = sha256(content_bytes).hexdigest()
    failed = False
    published: object = None
    try:
        published = writer.create_or_verify(
            bucket=bucket,
            object_name=object_name,
            content_bytes=content_bytes,
            content_type=QUALITY_PILOT_CONTROL_CONTENT_TYPE,
            maximum_bytes=MAXIMUM_TRANSITION_BYTES,
        )
    except Exception:
        failed = True
    if failed or type(published) is not PublishedStateObject:
        _fail("ledger transition writer failed")
    if (
        published.object_name != object_name
        or published.byte_count != len(content_bytes)
        or published.sha256 != expected_hash
    ):
        _fail("ledger transition writer result failed verification")
    return PublishedQualityPilotLedgerTransition(
        storage_policy_version=QUALITY_PILOT_CONTROL_STORE_POLICY_VERSION,
        protocol_sha256=PILOT_PROTOCOL_SHA256,
        pilot_run_id=transition.pilot_run_id,
        plan_id=transition.plan_id,
        previous_snapshot_id=transition.previous_snapshot_id,
        capture_spec_id=transition.capture_spec_id,
        transition_id=transition.transition_id,
        bucket=bucket,
        object_name=object_name,
        generation=published.generation,
        encoded_byte_count=len(content_bytes),
        encoded_sha256=expected_hash,
    )


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


@dataclass(frozen=True, slots=True)
class PinnedQualityPilotLedgerTransitionRequest:
    """One immutable, exact pin of a single sealed ledger transition to reload.

    Never accepts an absent generation -- ``generation`` is a required
    positive integer field, and the exact canonical transition object name
    is independently derived and required to match ``object_name``.
    """

    storage_policy_version: str
    protocol_sha256: str
    pilot_run_id: str
    plan_id: str
    previous_snapshot_id: str | None
    capture_spec_id: str
    transition_id: str
    bucket: str
    object_name: str
    generation: int
    expected_encoded_sha256: str

    def __post_init__(self) -> None:
        self._validate()

    def _validate(self) -> None:
        if self.storage_policy_version != QUALITY_PILOT_CONTROL_STORE_POLICY_VERSION:
            _fail("pinned ledger transition storage policy is invalid")
        if self.protocol_sha256 != PILOT_PROTOCOL_SHA256:
            _fail("pinned ledger transition protocol hash is invalid")
        if not _is_sha256(self.pilot_run_id) or not _is_sha256(self.plan_id) or not _is_sha256(self.capture_spec_id):
            _fail("pinned ledger transition identity is invalid")
        if self.previous_snapshot_id is not None and not _is_sha256(self.previous_snapshot_id):
            _fail("pinned ledger transition previous snapshot id is invalid")
        if not _is_sha256(self.transition_id):
            _fail("pinned ledger transition id is invalid")
        if type(self.bucket) is not str or _BUCKET_PATTERN.fullmatch(self.bucket) is None:
            _fail("pinned ledger transition bucket is invalid")
        expected_object_name = canonical_quality_pilot_transition_object_name(
            self.pilot_run_id,
            self.plan_id,
            _predecessor_token(self.previous_snapshot_id),
            self.capture_spec_id,
        )
        if self.object_name != expected_object_name:
            _fail("pinned ledger transition route is invalid")
        if type(self.generation) is not int or self.generation <= 0:
            _fail("pinned ledger transition generation is invalid")
        if not _is_sha256(self.expected_encoded_sha256):
            _fail("pinned ledger transition hash is invalid")


def pinned_quality_pilot_ledger_transition_request(
    published: PublishedQualityPilotLedgerTransition,
) -> PinnedQualityPilotLedgerTransitionRequest:
    """Deterministically derive a pin from an exact, independently reverified publication."""

    reconstructed = _reconstruct_published_transition(published)
    return PinnedQualityPilotLedgerTransitionRequest(
        storage_policy_version=reconstructed.storage_policy_version,
        protocol_sha256=reconstructed.protocol_sha256,
        pilot_run_id=reconstructed.pilot_run_id,
        plan_id=reconstructed.plan_id,
        previous_snapshot_id=reconstructed.previous_snapshot_id,
        capture_spec_id=reconstructed.capture_spec_id,
        transition_id=reconstructed.transition_id,
        bucket=reconstructed.bucket,
        object_name=reconstructed.object_name,
        generation=reconstructed.generation,
        expected_encoded_sha256=reconstructed.encoded_sha256,
    )


@dataclass(frozen=True, slots=True)
class LoadedQualityPilotLedgerTransition:
    """One immutable, independently re-verified reload of a pinned ledger transition."""

    transition: QualityPilotLedgerTransition
    request: PinnedQualityPilotLedgerTransitionRequest

    def __post_init__(self) -> None:
        self._validate()

    def _validate(self) -> None:
        if type(self.transition) is not QualityPilotLedgerTransition:
            _fail("loaded ledger transition type is invalid")
        verify_failed = False
        try:
            self.transition.verify_content_identity()
        except Exception:
            verify_failed = True
        if verify_failed:
            _fail("loaded ledger transition failed independent verification")
        if type(self.request) is not PinnedQualityPilotLedgerTransitionRequest:
            _fail("loaded ledger transition request type is invalid")
        self.request._validate()
        if self.transition.transition_id != self.request.transition_id:
            _fail("loaded ledger transition id disagrees with its pinned request")
        if self.transition.pilot_run_id != self.request.pilot_run_id:
            _fail("loaded ledger transition pilot run id disagrees with its pinned request")
        if self.transition.plan_id != self.request.plan_id:
            _fail("loaded ledger transition plan id disagrees with its pinned request")
        if self.transition.previous_snapshot_id != self.request.previous_snapshot_id:
            _fail("loaded ledger transition predecessor disagrees with its pinned request")
        if self.transition.capture_spec_id != self.request.capture_spec_id:
            _fail("loaded ledger transition capture spec id disagrees with its pinned request")
        if self.transition.protocol_sha256 != self.request.protocol_sha256:
            _fail("loaded ledger transition protocol hash disagrees with its pinned request")


def read_pinned_quality_pilot_ledger_transition(
    request: PinnedQualityPilotLedgerTransitionRequest, reader: GCSObjectReader
) -> LoadedQualityPilotLedgerTransition:
    """Reload exactly one pinned, sealed ledger transition and replay-verify its content.

    Calls ``reader.read_generation`` exactly once with the pinned bucket/
    object/generation and the fixed transition byte ceiling. No listing, no
    "head" resolution, no retry.
    """

    if type(request) is not PinnedQualityPilotLedgerTransitionRequest:
        _fail("pinned ledger transition request must be exact")

    read_failed = False
    payload: object = None
    try:
        payload = reader.read_generation(
            bucket=request.bucket,
            object_name=request.object_name,
            generation=request.generation,
            maximum_bytes=MAXIMUM_TRANSITION_BYTES,
        )
    except Exception:
        read_failed = True
    if read_failed:
        _fail("pinned ledger transition could not be read")

    if type(payload) is not GCSObjectPayload:
        _fail("ledger transition reader result type is invalid")
    if type(payload.generation) is not int:
        _fail("ledger transition reader result generation is invalid")
    if payload.generation != request.generation:
        _fail("ledger transition reader result generation disagrees with the pinned request")
    if type(payload.content_bytes) is not bytes or len(payload.content_bytes) == 0:
        _fail("ledger transition reader result content is invalid")
    if len(payload.content_bytes) > MAXIMUM_TRANSITION_BYTES:
        _fail("ledger transition reader result content exceeds the maximum encoded byte ceiling")
    actual_sha256 = sha256(payload.content_bytes).hexdigest()
    if actual_sha256 != request.expected_encoded_sha256:
        _fail("ledger transition reader result content does not match its expected sha256")

    decode_failed = False
    transition: QualityPilotLedgerTransition | None = None
    try:
        transition = decode_quality_pilot_ledger_transition(payload.content_bytes)
    except Exception:
        decode_failed = True
    if decode_failed or transition is None:
        _fail("ledger transition could not be replay-verified")

    return LoadedQualityPilotLedgerTransition(transition=transition, request=request)
