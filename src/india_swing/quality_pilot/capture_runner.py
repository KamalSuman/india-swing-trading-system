"""HYP-002 quality pilot: deterministic 20-session capture orchestration core.

Binds one exact, founder-authorized 20-confirmed-session campaign and its
pre-admitted calendar-decision references, accepts one explicit endpoint
capture specification, invokes one injected collector exactly once, and
publishes the resulting canonical_response_v1 observation through the
accepted immutable observation store. Every artifact this module produces
remains permanently quality-only: ineligible for O0, research partitions,
features, labels, signals, paper trades, notifications, execution, or
capital.

This module performs no filesystem, environment, clock, network, Kite SDK,
GCP SDK, broker, scheduler, or process capability of its own. It never
retries, sleeps, lists, discovers, or selects a "latest" artifact -- the
collector is called at most once per run, and the writer is called at most
once through the accepted :func:`~india_swing.quality_pilot.observation_store
.publish_quality_pilot_observation` boundary. The canonical response layer in
:mod:`india_swing.quality_pilot.canonical_response` remains the final
authority for record coverage, session/finality agreement, catalog cohort,
and response-classification/endpoint compatibility -- this module never
duplicates those private rules, it only calls into the accepted constructors
and converts any rejection into its own sanitized, static error.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Protocol

from india_swing.daily_pipeline.state_publication import StateObjectWriter
from india_swing.identity import content_id
from india_swing.market_data.models import (
    DailyCandleBatch,
    FullQuoteBatch,
    InstrumentBatch,
    require_canonical_listing_keys,
)

from .canonical_response import (
    EXCHANGE_NSE,
    MAXIMUM_CHUNK_COUNT,
    MAXIMUM_QUOTE_REQUEST_KEYS,
    MAXIMUM_TEXT_FIELD_LENGTH,
    PILOT_PROTOCOL_SHA256,
    PROVIDER_ZERODHA_KITE,
    EndpointFamily,
    ObservationRequestIdentity,
    ObservationWindowSpec,
    QualityPilotObservation,
    ResponseClassification,
    ScheduledWindowKind,
    encode_observation,
    replay_verify,
)
from .observation_store import PublishedQualityPilotObservation, publish_quality_pilot_observation


# ---------------------------------------------------------------------------
# Pinned constants
# ---------------------------------------------------------------------------

QUALITY_PILOT_CAMPAIGN_SCHEMA_VERSION = "quality_pilot_campaign_v1"
QUALITY_PILOT_CAPTURE_SPEC_SCHEMA_VERSION = "quality_pilot_capture_spec_v1"
QUALITY_PILOT_CAPTURE_RUN_RESULT_SCHEMA_VERSION = "quality_pilot_capture_run_result_v1"

CONFIRMED_SESSION_COUNT = 20

_SHA256_PATTERN = re.compile(r"[0-9a-f]{64}\Z")


def _is_sha256(value: object) -> bool:
    return type(value) is str and _SHA256_PATTERN.fullmatch(value) is not None


def _is_nonempty_bounded_text(value: object, *, maximum_length: int) -> bool:
    return (
        type(value) is str
        and 0 < len(value) <= maximum_length
        and value == value.strip()
        and all(ord(character) >= 32 for character in value)
    )


class QualityPilotCaptureRunnerError(ValueError):
    """A quality-pilot capture-runner input, capability, or artifact failed a static safety rule."""


def _fail(message: str) -> None:
    raise QualityPilotCaptureRunnerError(message)


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


class _FixedPostureMixin:
    """Read-only, fixed fail-closed posture for values with no content identity of their own.

    Plain properties, never dataclass fields -- no per-instance state.
    """

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
# QualityPilotCampaignSpec
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class QualityPilotCampaignSpec:
    """One exact, founder-authorized 20-confirmed-session HYP-002 campaign.

    ``calendar_decision_ids`` are caller-supplied references to separately
    admitted calendar decisions, positionally paired to ``confirmed_sessions``
    -- this module never creates, validates, or materializes calendar
    evidence of its own.
    """

    pilot_run_id: str
    protocol_sha256: str
    confirmed_sessions: tuple[date, ...]
    calendar_decision_ids: tuple[str, ...]
    quality_only: bool = field(init=False)
    counts_toward_o0: bool = field(init=False)
    counts_toward_clean_accumulation: bool = field(init=False)
    research_partition_eligible: bool = field(init=False)
    training_eligible: bool = field(init=False)
    feature_eligible: bool = field(init=False)
    label_eligible: bool = field(init=False)
    signal_eligible: bool = field(init=False)
    paper_trade_eligible: bool = field(init=False)
    notification_eligible: bool = field(init=False)
    execution_eligible: bool = field(init=False)
    capital_eligible: bool = field(init=False)
    campaign_id: str = field(init=False)

    def __post_init__(self) -> None:
        for name in _POSTURE_NAMES:
            object.__setattr__(self, name, name == "quality_only")
        self._validate()
        object.__setattr__(self, "campaign_id", self._calculated_id())

    def _validate(self) -> None:
        if not _is_sha256(self.pilot_run_id):
            _fail("campaign pilot run id is invalid")
        if self.protocol_sha256 != PILOT_PROTOCOL_SHA256:
            _fail("campaign protocol hash is invalid")
        if (
            type(self.confirmed_sessions) is not tuple
            or len(self.confirmed_sessions) != CONFIRMED_SESSION_COUNT
            or any(type(value) is not date for value in self.confirmed_sessions)
            or self.confirmed_sessions != tuple(sorted(set(self.confirmed_sessions)))
            or len(set(self.confirmed_sessions)) != CONFIRMED_SESSION_COUNT
        ):
            _fail(
                "campaign confirmed_sessions must be exactly "
                f"{CONFIRMED_SESSION_COUNT} strictly increasing unique dates"
            )
        if (
            type(self.calendar_decision_ids) is not tuple
            or len(self.calendar_decision_ids) != CONFIRMED_SESSION_COUNT
            or any(not _is_sha256(value) for value in self.calendar_decision_ids)
            or len(set(self.calendar_decision_ids)) != CONFIRMED_SESSION_COUNT
        ):
            _fail(
                "campaign calendar_decision_ids must be exactly "
                f"{CONFIRMED_SESSION_COUNT} unique lowercase sha256 ids"
            )
        if any(getattr(self, name) != (name == "quality_only") for name in _POSTURE_NAMES):
            _fail("campaign safety posture is invalid")

    def _calculated_id(self) -> str:
        return content_id(
            {
                "schema": QUALITY_PILOT_CAMPAIGN_SCHEMA_VERSION,
                "pilot_run_id": self.pilot_run_id,
                "protocol_sha256": self.protocol_sha256,
                "confirmed_sessions": self.confirmed_sessions,
                "calendar_decision_ids": self.calendar_decision_ids,
                "posture": {name: getattr(self, name) for name in _POSTURE_NAMES},
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        self._validate()
        if not _is_sha256(self.campaign_id):
            _fail("campaign identity is malformed")
        calculation_failed = False
        calculated = ""
        try:
            calculated = self._calculated_id()
        except Exception:
            calculation_failed = True
        if calculation_failed or self.campaign_id != calculated:
            _fail("campaign identity failed")


def _confirmed_session_calendar_decision_id(
    campaign: QualityPilotCampaignSpec, session: date
) -> str:
    lookup_failed = False
    index = -1
    try:
        index = campaign.confirmed_sessions.index(session)
    except ValueError:
        lookup_failed = True
    if lookup_failed:
        _fail("campaign does not contain the requested session")
    return campaign.calendar_decision_ids[index]


def _reconstruct_campaign(value: object) -> QualityPilotCampaignSpec:
    if type(value) is not QualityPilotCampaignSpec:
        _fail("campaign type is invalid")
    reconstruction_failed = False
    reconstructed: QualityPilotCampaignSpec | None = None
    try:
        reconstructed = QualityPilotCampaignSpec(
            pilot_run_id=value.pilot_run_id,
            protocol_sha256=value.protocol_sha256,
            confirmed_sessions=value.confirmed_sessions,
            calendar_decision_ids=value.calendar_decision_ids,
        )
    except Exception:
        reconstruction_failed = True
    if reconstruction_failed or reconstructed is None:
        _fail("campaign could not be independently reverified")
    if not _is_sha256(value.campaign_id) or value.campaign_id != reconstructed.campaign_id:
        _fail("campaign identity failed independent reverification")
    return reconstructed


def _reconstruct_window(value: object) -> ObservationWindowSpec:
    if type(value) is not ObservationWindowSpec:
        _fail("capture window type is invalid")
    reconstruction_failed = False
    reconstructed: ObservationWindowSpec | None = None
    try:
        reconstructed = ObservationWindowSpec(
            pilot_run_id=value.pilot_run_id,
            market_session=value.market_session,
            window_kind=value.window_kind,
            endpoint_family=value.endpoint_family,
            opens_at=value.opens_at,
            closes_at=value.closes_at,
            protocol_sha256=value.protocol_sha256,
        )
    except Exception:
        reconstruction_failed = True
    if reconstruction_failed or reconstructed is None:
        _fail("capture window could not be independently reverified")
    if type(value.window_id) is not str or not _is_sha256(value.window_id):
        _fail("capture window identity is malformed")
    if value.window_id != reconstructed.window_id:
        _fail("capture window identity failed independent reverification")
    return reconstructed


# ---------------------------------------------------------------------------
# QualityPilotCaptureSpec
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class QualityPilotCaptureSpec:
    """One exact, immutable capture request for one campaign session/window.

    Enforces every endpoint shape (CATALOG/FULL_QUOTE/DAILY_OHLCV) before
    any collector can be invoked. The window-kind/endpoint-family pairing
    itself remains authoritative inside :class:`ObservationWindowSpec` and
    is independently reverified here via ``window.verify_content_identity``,
    never remapped by a local table.
    """

    campaign: QualityPilotCampaignSpec
    window: ObservationWindowSpec
    provider: str
    provider_version: str
    requested_keys: tuple[str, ...]
    provider_instrument_token: int | None
    chunk_index: int
    chunk_count: int
    protocol_sha256: str
    quality_only: bool = field(init=False)
    counts_toward_o0: bool = field(init=False)
    counts_toward_clean_accumulation: bool = field(init=False)
    research_partition_eligible: bool = field(init=False)
    training_eligible: bool = field(init=False)
    feature_eligible: bool = field(init=False)
    label_eligible: bool = field(init=False)
    signal_eligible: bool = field(init=False)
    paper_trade_eligible: bool = field(init=False)
    notification_eligible: bool = field(init=False)
    execution_eligible: bool = field(init=False)
    capital_eligible: bool = field(init=False)
    capture_spec_id: str = field(init=False)

    def __post_init__(self) -> None:
        for name in _POSTURE_NAMES:
            object.__setattr__(self, name, name == "quality_only")
        self._validate()
        object.__setattr__(self, "capture_spec_id", self._calculated_id())

    def _validate(self) -> None:
        campaign = _reconstruct_campaign(self.campaign)
        window = _reconstruct_window(self.window)
        if self.protocol_sha256 != PILOT_PROTOCOL_SHA256:
            _fail("capture spec protocol hash is invalid")
        if campaign.protocol_sha256 != self.protocol_sha256:
            _fail("capture spec protocol hash disagrees with its campaign")
        if window.protocol_sha256 != self.protocol_sha256:
            _fail("capture spec protocol hash disagrees with its window")
        if window.pilot_run_id != campaign.pilot_run_id:
            _fail("capture spec window pilot run id disagrees with its campaign")
        if window.market_session not in campaign.confirmed_sessions:
            _fail("capture spec window session is not one of the campaign's confirmed sessions")
        if self.provider != PROVIDER_ZERODHA_KITE:
            _fail("capture spec provider is invalid")
        if not _is_nonempty_bounded_text(
            self.provider_version, maximum_length=MAXIMUM_TEXT_FIELD_LENGTH
        ):
            _fail("capture spec provider version is invalid")
        if type(self.chunk_index) is bool or type(self.chunk_index) is not int or self.chunk_index < 1:
            _fail("capture spec chunk_index is invalid")
        if (
            type(self.chunk_count) is bool
            or type(self.chunk_count) is not int
            or self.chunk_count < 1
            or self.chunk_count > MAXIMUM_CHUNK_COUNT
        ):
            _fail("capture spec chunk_count is invalid")
        if self.chunk_index > self.chunk_count:
            _fail("capture spec chunk_index exceeds chunk_count")
        self._validate_endpoint_shape()
        if any(getattr(self, name) != (name == "quality_only") for name in _POSTURE_NAMES):
            _fail("capture spec safety posture is invalid")

    def _validate_endpoint_shape(self) -> None:
        family = self.window.endpoint_family
        if family is EndpointFamily.CATALOG:
            if self.requested_keys != ():
                _fail("catalog capture spec must not carry requested keys")
            if self.provider_instrument_token is not None:
                _fail("catalog capture spec must not carry a provider instrument token")
            if self.chunk_index != 1 or self.chunk_count != 1:
                _fail("catalog capture spec must use chunk 1-of-1")
        elif family is EndpointFamily.FULL_QUOTE:
            keys_invalid = False
            try:
                require_canonical_listing_keys(
                    self.requested_keys, maximum_keys=MAXIMUM_QUOTE_REQUEST_KEYS
                )
            except Exception:
                keys_invalid = True
            if keys_invalid:
                _fail("full quote capture spec requested keys are invalid")
            if self.provider_instrument_token is not None:
                _fail("full quote capture spec must not carry a provider instrument token")
        else:
            keys_invalid = False
            try:
                require_canonical_listing_keys(self.requested_keys, maximum_keys=1)
            except Exception:
                keys_invalid = True
            if keys_invalid or len(self.requested_keys) != 1:
                _fail("daily OHLCV capture spec must name exactly one canonical listing key")
            if (
                type(self.provider_instrument_token) is bool
                or type(self.provider_instrument_token) is not int
                or self.provider_instrument_token <= 0
            ):
                _fail(
                    "daily OHLCV capture spec requires a positive exact provider instrument token"
                )
            # Each OHLCV provider request carries exactly one key. The shared
            # chunk route still identifies that key's position in the full
            # session plan; it is not collapsed to 1-of-1.

    def _calculated_id(self) -> str:
        return content_id(
            {
                "schema": QUALITY_PILOT_CAPTURE_SPEC_SCHEMA_VERSION,
                "campaign_id": self.campaign.campaign_id,
                "window_id": self.window.window_id,
                "provider": self.provider,
                "provider_version": self.provider_version,
                "requested_keys": self.requested_keys,
                "provider_instrument_token": self.provider_instrument_token,
                "chunk_index": self.chunk_index,
                "chunk_count": self.chunk_count,
                "protocol_sha256": self.protocol_sha256,
                "posture": {name: getattr(self, name) for name in _POSTURE_NAMES},
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        self._validate()
        if not _is_sha256(self.capture_spec_id):
            _fail("capture spec identity is malformed")
        calculation_failed = False
        calculated = ""
        try:
            calculated = self._calculated_id()
        except Exception:
            calculation_failed = True
        if calculation_failed or self.capture_spec_id != calculated:
            _fail("capture spec identity failed")


def _reconstruct_capture_spec(value: object) -> QualityPilotCaptureSpec:
    if type(value) is not QualityPilotCaptureSpec:
        _fail("capture spec must be an exact QualityPilotCaptureSpec")
    campaign = _reconstruct_campaign(value.campaign)
    window = _reconstruct_window(value.window)
    reconstruction_failed = False
    reconstructed: QualityPilotCaptureSpec | None = None
    try:
        reconstructed = QualityPilotCaptureSpec(
            campaign=campaign,
            window=window,
            provider=value.provider,
            provider_version=value.provider_version,
            requested_keys=value.requested_keys,
            provider_instrument_token=value.provider_instrument_token,
            chunk_index=value.chunk_index,
            chunk_count=value.chunk_count,
            protocol_sha256=value.protocol_sha256,
        )
    except Exception:
        reconstruction_failed = True
    if reconstruction_failed or reconstructed is None:
        _fail("capture spec could not be independently reverified")
    if not _is_sha256(value.capture_spec_id):
        _fail("capture spec identity is malformed")
    if value.capture_spec_id != reconstructed.capture_spec_id:
        _fail("capture spec identity failed independent reverification")
    return reconstructed


# ---------------------------------------------------------------------------
# QualityPilotCollectionResult
# ---------------------------------------------------------------------------

_ENDPOINT_PAYLOAD_TYPE = {
    EndpointFamily.CATALOG: InstrumentBatch,
    EndpointFamily.FULL_QUOTE: FullQuoteBatch,
    EndpointFamily.DAILY_OHLCV: DailyCandleBatch,
}

_KNOWN_PAYLOAD_TYPES = (InstrumentBatch, FullQuoteBatch, DailyCandleBatch)


@dataclass(frozen=True, slots=True)
class QualityPilotCollectionResult(_FixedPostureMixin):
    """One immutable, self-consistent snapshot of untrusted collector output.

    Carries no content identity of its own -- it is reconstructed from
    primitive fields and exact nested types by the runner before any
    further field of it is trusted, defeating a frozen-dataclass
    ``object.__setattr__`` tamper on a caller-returned instance.
    """

    request_started_at: datetime
    request_ended_at: datetime
    response_classification: ResponseClassification
    payload: InstrumentBatch | FullQuoteBatch | DailyCandleBatch | None

    def __post_init__(self) -> None:
        self._validate()

    def _validate(self) -> None:
        if type(self.request_started_at) is not datetime:
            _fail("collection result request_started_at is invalid")
        if type(self.request_ended_at) is not datetime:
            _fail("collection result request_ended_at is invalid")
        time_validation_failed = False
        start_offset = None
        end_offset = None
        ended_before_started = False
        try:
            start_offset = self.request_started_at.utcoffset()
            end_offset = self.request_ended_at.utcoffset()
            ended_before_started = self.request_started_at > self.request_ended_at
        except Exception:
            time_validation_failed = True
        if time_validation_failed or start_offset is None:
            _fail("collection result request_started_at is invalid")
        if end_offset is None:
            _fail("collection result request_ended_at is invalid")
        if ended_before_started:
            _fail("collection result started_at must not be after ended_at")
        if type(self.response_classification) is not ResponseClassification:
            _fail("collection result response classification is invalid")
        if self.response_classification is ResponseClassification.SUCCESS:
            if type(self.payload) not in _KNOWN_PAYLOAD_TYPES:
                _fail("collection result success payload type is invalid")
        else:
            if self.payload is not None:
                _fail("collection result non-success payload must be null")
        if any(getattr(self, name) != (name == "quality_only") for name in _POSTURE_NAMES):
            _fail("collection result safety posture is invalid")


class QualityPilotCollector(Protocol):
    def collect(self, spec: QualityPilotCaptureSpec) -> QualityPilotCollectionResult: ...


def _reconstruct_observation(value: object) -> QualityPilotObservation:
    if type(value) is not QualityPilotObservation:
        _fail("run result observation type is invalid")
    reconstruction_failed = False
    reconstructed: QualityPilotObservation | None = None
    try:
        encoded = encode_observation(value)
        reconstructed = replay_verify(encoded)
    except Exception:
        reconstruction_failed = True
    if reconstruction_failed or reconstructed is None:
        _fail("run result observation could not be independently reverified")
    if not _is_sha256(value.observation_id):
        _fail("run result observation identity is malformed")
    if value.observation_id != reconstructed.observation_id:
        _fail("run result observation identity failed independent reverification")
    return reconstructed


# ---------------------------------------------------------------------------
# QualityPilotCaptureRunResult
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class QualityPilotCaptureRunResult(_FixedPostureMixin):
    """One immutable, independently re-verifiable record of a completed capture run."""

    campaign: QualityPilotCampaignSpec
    capture_spec: QualityPilotCaptureSpec
    campaign_id: str
    capture_spec_id: str
    requested_bucket: str
    calendar_decision_id: str
    observation: QualityPilotObservation
    published: PublishedQualityPilotObservation
    run_result_id: str = field(init=False)

    def __post_init__(self) -> None:
        self._validate()
        object.__setattr__(self, "run_result_id", self._calculated_id())

    def _validate(self) -> None:
        campaign = _reconstruct_campaign(self.campaign)
        capture_spec = _reconstruct_capture_spec(self.capture_spec)
        if not _is_sha256(self.campaign_id) or self.campaign_id != campaign.campaign_id:
            _fail("run result campaign id is invalid")
        if not _is_sha256(self.capture_spec_id) or self.capture_spec_id != capture_spec.capture_spec_id:
            _fail("run result capture spec id is invalid")
        if capture_spec.campaign.campaign_id != campaign.campaign_id:
            _fail("run result capture spec campaign disagrees with its campaign")
        if type(self.requested_bucket) is not str or not self.requested_bucket:
            _fail("run result requested bucket is invalid")
        if not _is_sha256(self.calendar_decision_id):
            _fail("run result calendar decision id is invalid")
        expected_calendar_decision_id = _confirmed_session_calendar_decision_id(
            campaign, capture_spec.window.market_session
        )
        if self.calendar_decision_id != expected_calendar_decision_id:
            _fail("run result calendar decision id disagrees with its campaign session")

        observation = _reconstruct_observation(self.observation)
        if observation.window.window_id != capture_spec.window.window_id:
            _fail("run result observation window disagrees with its capture spec")
        if observation.request.provider != capture_spec.provider:
            _fail("run result observation provider disagrees with its capture spec")
        if observation.request.provider_version != capture_spec.provider_version:
            _fail("run result observation provider version disagrees with its capture spec")
        if observation.request.requested_keys != capture_spec.requested_keys:
            _fail("run result observation keys disagree with its capture spec")
        if observation.request.chunk_index != capture_spec.chunk_index:
            _fail("run result observation chunk index disagrees with its capture spec")
        if observation.request.chunk_count != capture_spec.chunk_count:
            _fail("run result observation chunk count disagrees with its capture spec")
        if (
            capture_spec.window.endpoint_family is EndpointFamily.DAILY_OHLCV
            and observation.request.response_classification is ResponseClassification.SUCCESS
            and (
                type(observation.payload) is not DailyCandleBatch
                or type(observation.payload.instrument_token) is not int
                or observation.payload.instrument_token
                != capture_spec.provider_instrument_token
            )
        ):
            _fail("run result OHLCV token disagrees with its capture spec")
        if type(self.published) is not PublishedQualityPilotObservation:
            _fail("run result published observation type is invalid")

        reconstruct_failed = False
        reconstructed_published: PublishedQualityPilotObservation | None = None
        try:
            reconstructed_published = PublishedQualityPilotObservation(
                storage_policy_version=self.published.storage_policy_version,
                protocol_sha256=self.published.protocol_sha256,
                observation_id=self.published.observation_id,
                pilot_run_id=self.published.pilot_run_id,
                market_session=self.published.market_session,
                window_kind=self.published.window_kind,
                endpoint_family=self.published.endpoint_family,
                chunk_index=self.published.chunk_index,
                chunk_count=self.published.chunk_count,
                bucket=self.published.bucket,
                object_name=self.published.object_name,
                generation=self.published.generation,
                encoded_byte_count=self.published.encoded_byte_count,
                encoded_sha256=self.published.encoded_sha256,
            )
        except Exception:
            reconstruct_failed = True
        if reconstruct_failed or reconstructed_published is None:
            _fail("run result published observation could not be independently reverified")

        if reconstructed_published.protocol_sha256 != campaign.protocol_sha256:
            _fail("run result published protocol hash disagrees with its campaign")
        if reconstructed_published.bucket != self.requested_bucket:
            _fail("run result published bucket disagrees with its requested bucket")
        if reconstructed_published.observation_id != observation.observation_id:
            _fail("run result published observation id disagrees with its observation")
        if reconstructed_published.pilot_run_id != observation.window.pilot_run_id:
            _fail("run result published pilot run id disagrees with its observation")
        if reconstructed_published.market_session != observation.window.market_session:
            _fail("run result published market session disagrees with its observation")
        if reconstructed_published.window_kind != observation.window.window_kind:
            _fail("run result published window kind disagrees with its observation")
        if reconstructed_published.endpoint_family != observation.window.endpoint_family:
            _fail("run result published endpoint family disagrees with its observation")
        if reconstructed_published.chunk_index != observation.request.chunk_index:
            _fail("run result published chunk index disagrees with its observation")
        if reconstructed_published.chunk_count != observation.request.chunk_count:
            _fail("run result published chunk count disagrees with its observation")
        if any(getattr(self, name) != (name == "quality_only") for name in _POSTURE_NAMES):
            _fail("run result safety posture is invalid")

    def _calculated_id(self) -> str:
        return content_id(
            {
                "schema": QUALITY_PILOT_CAPTURE_RUN_RESULT_SCHEMA_VERSION,
                "campaign_id": self.campaign_id,
                "capture_spec_id": self.capture_spec_id,
                "requested_bucket": self.requested_bucket,
                "calendar_decision_id": self.calendar_decision_id,
                "observation_id": self.observation.observation_id,
                "published_bucket": self.published.bucket,
                "published_object_name": self.published.object_name,
                "published_generation": self.published.generation,
                "published_encoded_sha256": self.published.encoded_sha256,
                "posture": {name: getattr(self, name) for name in _POSTURE_NAMES},
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        self._validate()
        if not _is_sha256(self.run_result_id):
            _fail("run result identity is malformed")
        calculation_failed = False
        calculated = ""
        try:
            calculated = self._calculated_id()
        except Exception:
            calculation_failed = True
        if calculation_failed or self.run_result_id != calculated:
            _fail("run result identity failed")


# ---------------------------------------------------------------------------
# QualityPilotCaptureRunner
# ---------------------------------------------------------------------------


class QualityPilotCaptureRunner:
    """Stateless orchestration core: one collector call, one publication.

    Holds no campaign progress, session counter, or completeness state of
    its own -- every call to :meth:`run` is an independent, fully
    self-verifying transformation from one pinned capture spec to one
    published observation.
    """

    def run(
        self,
        spec: QualityPilotCaptureSpec,
        collector: QualityPilotCollector,
        bucket: str,
        writer: StateObjectWriter,
    ) -> QualityPilotCaptureRunResult:
        verified_spec = _reconstruct_capture_spec(spec)

        collect_failed = False
        raw_result: object = None
        try:
            raw_result = collector.collect(verified_spec)
        except Exception:
            collect_failed = True
        if collect_failed:
            _fail("collector could not produce a collection result")

        if type(raw_result) is not QualityPilotCollectionResult:
            _fail("collector result type is invalid")

        reconstruct_failed = False
        result: QualityPilotCollectionResult | None = None
        try:
            result = QualityPilotCollectionResult(
                request_started_at=raw_result.request_started_at,
                request_ended_at=raw_result.request_ended_at,
                response_classification=raw_result.response_classification,
                payload=raw_result.payload,
            )
        except Exception:
            reconstruct_failed = True
        if reconstruct_failed or result is None:
            _fail("collector result could not be independently reverified")

        window = verified_spec.window
        interval_validation_failed = False
        outside_window = False
        try:
            outside_window = (
                result.request_started_at < window.opens_at
                or result.request_ended_at > window.closes_at
            )
        except Exception:
            interval_validation_failed = True
        if interval_validation_failed or outside_window:
            _fail("collection result interval does not lie inside its capture window")

        if result.response_classification is ResponseClassification.SUCCESS:
            expected_payload_type = _ENDPOINT_PAYLOAD_TYPE[window.endpoint_family]
            if type(result.payload) is not expected_payload_type:
                _fail("collection result payload type does not match its endpoint")
            if (
                window.endpoint_family is EndpointFamily.DAILY_OHLCV
                and (
                    type(result.payload.instrument_token) is not int
                    or result.payload.instrument_token != verified_spec.provider_instrument_token
                )
            ):
                _fail("collection result instrument token does not match its capture spec")

        request_failed = False
        request: ObservationRequestIdentity | None = None
        try:
            request = ObservationRequestIdentity(
                provider=verified_spec.provider,
                provider_version=verified_spec.provider_version,
                endpoint_family=window.endpoint_family,
                exchange=EXCHANGE_NSE,
                window_id=window.window_id,
                requested_session=window.market_session,
                requested_keys=verified_spec.requested_keys,
                requested_range_start=(
                    window.market_session
                    if window.endpoint_family is EndpointFamily.DAILY_OHLCV
                    else None
                ),
                requested_range_end=(
                    window.market_session
                    if window.endpoint_family is EndpointFamily.DAILY_OHLCV
                    else None
                ),
                request_started_at=result.request_started_at,
                request_ended_at=result.request_ended_at,
                chunk_index=verified_spec.chunk_index,
                chunk_count=verified_spec.chunk_count,
                response_classification=result.response_classification,
                protocol_sha256=verified_spec.protocol_sha256,
            )
        except Exception:
            request_failed = True
        if request_failed or request is None:
            _fail("observation request identity could not be constructed")

        observation_failed = False
        observation: QualityPilotObservation | None = None
        try:
            observation = QualityPilotObservation(
                window=window,
                request=request,
                payload=result.payload,
                corrects_observation_id=None,
            )
        except Exception:
            observation_failed = True
        if observation_failed or observation is None:
            _fail("observation could not be constructed")

        publish_failed = False
        published: PublishedQualityPilotObservation | None = None
        try:
            published = publish_quality_pilot_observation(observation, bucket, writer)
        except Exception:
            publish_failed = True
        if publish_failed or published is None:
            _fail("observation could not be published")

        if type(published) is not PublishedQualityPilotObservation:
            _fail("published observation type is invalid")
        if published.observation_id != observation.observation_id:
            _fail("published observation id disagrees with the constructed observation")
        if published.pilot_run_id != window.pilot_run_id:
            _fail("published observation pilot run id disagrees with its window")
        if published.market_session != window.market_session:
            _fail("published observation market session disagrees with its window")
        if published.window_kind != window.window_kind:
            _fail("published observation window kind disagrees with its window")
        if published.endpoint_family != window.endpoint_family:
            _fail("published observation endpoint family disagrees with its window")
        if (
            published.chunk_index != verified_spec.chunk_index
            or published.chunk_count != verified_spec.chunk_count
        ):
            _fail("published observation chunk route disagrees with its capture spec")
        if published.bucket != bucket:
            _fail("published observation bucket disagrees with the requested bucket")

        calendar_decision_id = _confirmed_session_calendar_decision_id(
            verified_spec.campaign, window.market_session
        )

        return QualityPilotCaptureRunResult(
            campaign=verified_spec.campaign,
            capture_spec=verified_spec,
            campaign_id=verified_spec.campaign.campaign_id,
            capture_spec_id=verified_spec.capture_spec_id,
            requested_bucket=bucket,
            calendar_decision_id=calendar_decision_id,
            observation=observation,
            published=published,
        )
