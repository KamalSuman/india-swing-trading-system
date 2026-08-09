"""HYP-002 quality pilot: invocation control plane.

Defines the immutable wire records and deterministic object routes that let
``window_service.py`` process one scheduled window per invocation, at most
once per capture step, with restart-safe resumption:

- :class:`QualityPilotInvocationRunbook` -- one campaign plus the exact 80
  (20 sessions x 4 windows) caller-pinned ``ObservationWindowSpec`` values,
  in canonical session/kind order.
- :class:`QualityPilotActionBinding` -- one deterministic unit of work
  (a catalog bootstrap or one resumable capture step), embedding the exact
  runbook and either genesis/extension predecessor pins (catalog) or an
  exact plan/predecessor-transition pin plus target spec id (resumable).
- :class:`QualityPilotWindowEntry` -- one deterministic-path pointer (keyed
  only by pilot_run_id/market_session/window_kind) at the first action
  binding of one window.
- :class:`QualityPilotActionClaim` -- one strict create-once claim proving
  a specific process attempted one action, published before any
  collector/API call.
- :class:`QualityPilotCompletionReceipt` -- the terminal success boundary
  for one action, binding its claim, its domain result identity, and
  either its successor action-binding pin (same or next window) or
  ``None`` at exact campaign completion.

This module performs no environment, filesystem, network, GCP/Kite SDK
construction, clock read, sleep, retry, listing, or "latest" selection of
its own -- every write goes through an injected ``StateObjectWriter`` or
strict ``QualityPilotClaimWriter``, and every read of an object whose
generation is not already known goes through an injected
``QualityPilotCurrentObjectReader`` that reloads then pins one exact
generation before downloading, never lists a bucket, and never treats a
non-NotFound failure as absence.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, field
from datetime import date, datetime
from enum import Enum
from typing import Protocol

from india_swing.daily_pipeline.acquisition import GCSObjectPayload, GCSObjectReader
from india_swing.daily_pipeline.state_publication import PublishedStateObject, StateObjectWriter
from india_swing.identity import content_id

from .canonical_response import (
    PILOT_PROTOCOL_SHA256,
    PROVIDER_ZERODHA_KITE,
    EndpointFamily,
    ObservationWindowSpec,
    ScheduledWindowKind,
)
from .campaign_ledger import is_window_inside_authorized_schedule
from .capture_runner import CONFIRMED_SESSION_COUNT, QualityPilotCampaignSpec, QualityPilotCaptureSpec
from .control_plane_store import (
    PinnedQualityPilotControlArtifactRequest,
    PinnedQualityPilotLedgerTransitionRequest,
    QualityPilotControlArtifactKind,
)

try:
    from google.api_core.exceptions import NotFound, PreconditionFailed
except ImportError:  # pragma: no cover - optional dependency
    NotFound = None
    PreconditionFailed = None


QUALITY_PILOT_INVOCATION_STORAGE_POLICY_VERSION = "quality_pilot_invocation_control_plane_v1"
QUALITY_PILOT_RUNBOOK_SCHEMA_VERSION = "quality_pilot_invocation_runbook_v1"
QUALITY_PILOT_ACTION_BINDING_SCHEMA_VERSION = "quality_pilot_action_binding_v1"
QUALITY_PILOT_WINDOW_ENTRY_SCHEMA_VERSION = "quality_pilot_window_entry_v1"
QUALITY_PILOT_ACTION_CLAIM_SCHEMA_VERSION = "quality_pilot_action_claim_v1"
QUALITY_PILOT_COMPLETION_RECEIPT_SCHEMA_VERSION = "quality_pilot_completion_receipt_v1"
QUALITY_PILOT_INVOCATION_CONTENT_TYPE = "application/json"

MAXIMUM_RUNBOOK_BYTES = 4 * 1024 * 1024
MAXIMUM_ACTION_BINDING_BYTES = 64 * 1024
MAXIMUM_WINDOW_ENTRY_BYTES = 16 * 1024
MAXIMUM_ACTION_CLAIM_BYTES = 16 * 1024
MAXIMUM_COMPLETION_RECEIPT_BYTES = 32 * 1024
_MAXIMUM_GENERATION = 9_223_372_036_854_775_807

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

_WINDOW_KIND_ORDER = (
    ScheduledWindowKind.CATALOG_PREOPEN,
    ScheduledWindowKind.QUOTE_0920,
    ScheduledWindowKind.QUOTE_CLOSE,
    ScheduledWindowKind.OHLCV_CLOSE,
)


class QualityPilotInvocationControlPlaneError(ValueError):
    """An invocation-control-plane input, wire record, or route failed a static trust rule."""


class QualityPilotClaimConflictError(QualityPilotInvocationControlPlaneError):
    """The exact create-once claim path already holds an object.

    Distinct from every other failure: a conflict here means a process may
    have crashed after issuing the provider request and recollecting would
    violate the one-scheduled-request protocol. Callers must fail closed
    with no collector/API call, never retry, and never overwrite.
    """


def _fail(message: str) -> None:
    raise QualityPilotInvocationControlPlaneError(message)


def _claim_conflict(message: str) -> None:
    raise QualityPilotClaimConflictError(message)


def _is_sha256(value: object) -> bool:
    return type(value) is str and _SHA256_PATTERN.fullmatch(value) is not None


def _posture_tree(value: object) -> dict[str, bool]:
    return {name: getattr(value, name) for name in _POSTURE_NAMES}


def _validate_posture(value: object) -> None:
    if any(getattr(value, name) != (name == "quality_only") for name in _POSTURE_NAMES):
        _fail("invocation control plane safety posture is invalid")


class _FixedPostureMixin:
    """Read-only, fixed fail-closed posture. ``__slots__ = ()`` plus a true
    ``@property`` for every name means no instance ever has a ``__dict__``
    entry for these names, so ``object.__setattr__`` raises immediately.
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
# Canonical JSON codec primitives (mirrors control_plane_store.py exactly)
# ---------------------------------------------------------------------------


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
        _fail("invocation control plane record could not be canonically encoded")
    return encoded


def _reject_float(_: str) -> object:
    _fail("invocation control plane numbers must be exact integers")


def _reject_constant(_: str) -> object:
    _fail("invocation control plane contains a non-finite number")


def _reject_duplicate_pairs(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            _fail("invocation control plane record contains a duplicate key")
        result[key] = value
    return result


def _parse_json(content_bytes: object, maximum_bytes: int) -> dict[str, object]:
    if type(content_bytes) is not bytes:
        _fail("invocation control plane content must be exact bytes")
    if not content_bytes or len(content_bytes) > maximum_bytes:
        _fail("invocation control plane content size is invalid")
    decode_failed = False
    text = ""
    try:
        text = content_bytes.decode("utf-8", errors="strict")
    except Exception:
        decode_failed = True
    if decode_failed:
        _fail("invocation control plane content is not strict UTF-8")
    parse_failed = False
    value: object = None
    try:
        value = json.loads(
            text,
            object_pairs_hook=_reject_duplicate_pairs,
            parse_float=_reject_float,
            parse_constant=_reject_constant,
        )
    except QualityPilotInvocationControlPlaneError:
        raise
    except Exception:
        parse_failed = True
    if parse_failed:
        _fail("invocation control plane content is not valid JSON")
    if type(value) is not dict:
        _fail("invocation control plane root must be an exact object")
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
    if type(value) is bool or type(value) is not int or value < minimum:
        _fail(message)
    return value


def _optional_text(value: object, message: str) -> str | None:
    if value is None:
        return None
    return _text(value, message)


def _sha256_field(value: object, message: str) -> str:
    if not _is_sha256(value):
        _fail(message)
    return value


def _optional_sha256_field(value: object, message: str) -> str | None:
    if value is None:
        return None
    return _sha256_field(value, message)


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


def _validate_bucket(value: object) -> str:
    if type(value) is not str or _BUCKET_PATTERN.fullmatch(value) is None:
        _fail("invocation control plane bucket is invalid")
    return value


# ---------------------------------------------------------------------------
# Small independent-reconstruction helpers for embedded/foreign records
# ---------------------------------------------------------------------------


def _reconstruct_campaign(value: object) -> QualityPilotCampaignSpec:
    if type(value) is not QualityPilotCampaignSpec:
        _fail("invocation runbook campaign type is invalid")
    failed = False
    try:
        value.verify_content_identity()
    except Exception:
        failed = True
    if failed:
        _fail("invocation runbook campaign failed independent verification")
    return value


def _reconstruct_window(value: object) -> ObservationWindowSpec:
    if type(value) is not ObservationWindowSpec:
        _fail("invocation runbook window type is invalid")
    failed = False
    try:
        value.verify_content_identity()
    except Exception:
        failed = True
    if failed:
        _fail("invocation runbook window failed independent verification")
    return value


def _reconstruct_plan_pin(value: object) -> PinnedQualityPilotControlArtifactRequest:
    if type(value) is not PinnedQualityPilotControlArtifactRequest:
        _fail("control artifact pin type is invalid")
    failed = False
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
        failed = True
    if failed or reconstructed is None:
        _fail("control artifact pin could not be independently reverified")
    return reconstructed


def _reconstruct_transition_pin(value: object) -> PinnedQualityPilotLedgerTransitionRequest:
    if type(value) is not PinnedQualityPilotLedgerTransitionRequest:
        _fail("ledger transition pin type is invalid")
    failed = False
    reconstructed: PinnedQualityPilotLedgerTransitionRequest | None = None
    try:
        reconstructed = PinnedQualityPilotLedgerTransitionRequest(
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
        failed = True
    if failed or reconstructed is None:
        _fail("ledger transition pin could not be independently reverified")
    return reconstructed


def _plan_pin_tree(value: PinnedQualityPilotControlArtifactRequest | None) -> dict[str, object] | None:
    if value is None:
        return None
    return {
        "storage_policy_version": value.storage_policy_version,
        "protocol_sha256": value.protocol_sha256,
        "kind": value.kind.value,
        "pilot_run_id": value.pilot_run_id,
        "artifact_id": value.artifact_id,
        "bucket": value.bucket,
        "object_name": value.object_name,
        "generation": value.generation,
        "expected_encoded_sha256": value.expected_encoded_sha256,
    }


def _transition_pin_tree(value: PinnedQualityPilotLedgerTransitionRequest | None) -> dict[str, object] | None:
    if value is None:
        return None
    return {
        "storage_policy_version": value.storage_policy_version,
        "protocol_sha256": value.protocol_sha256,
        "pilot_run_id": value.pilot_run_id,
        "plan_id": value.plan_id,
        "previous_snapshot_id": value.previous_snapshot_id,
        "capture_spec_id": value.capture_spec_id,
        "transition_id": value.transition_id,
        "bucket": value.bucket,
        "object_name": value.object_name,
        "generation": value.generation,
        "expected_encoded_sha256": value.expected_encoded_sha256,
    }


def _decode_plan_pin(value: object) -> PinnedQualityPilotControlArtifactRequest | None:
    if value is None:
        return None
    record = _exact_dict(
        value,
        {
            "storage_policy_version", "protocol_sha256", "kind", "pilot_run_id",
            "artifact_id", "bucket", "object_name", "generation", "expected_encoded_sha256",
        },
        "control artifact pin record has an invalid shape",
    )
    failed = False
    pin: PinnedQualityPilotControlArtifactRequest | None = None
    try:
        pin = PinnedQualityPilotControlArtifactRequest(
            storage_policy_version=_text(record["storage_policy_version"], "control artifact pin policy is invalid"),
            protocol_sha256=_text(record["protocol_sha256"], "control artifact pin protocol is invalid"),
            kind=_enum(record["kind"], QualityPilotControlArtifactKind, "control artifact pin kind is invalid"),
            pilot_run_id=_sha256_field(record["pilot_run_id"], "control artifact pin pilot run id is invalid"),
            artifact_id=_sha256_field(record["artifact_id"], "control artifact pin artifact id is invalid"),
            bucket=_validate_bucket(record["bucket"]),
            object_name=_text(record["object_name"], "control artifact pin object name is invalid"),
            generation=_integer(record["generation"], "control artifact pin generation is invalid", minimum=1),
            expected_encoded_sha256=_sha256_field(record["expected_encoded_sha256"], "control artifact pin hash is invalid"),
        )
    except QualityPilotInvocationControlPlaneError:
        raise
    except Exception:
        failed = True
    if failed or pin is None:
        _fail("control artifact pin record failed reconstruction")
    return pin


def _decode_transition_pin(value: object) -> PinnedQualityPilotLedgerTransitionRequest | None:
    if value is None:
        return None
    record = _exact_dict(
        value,
        {
            "storage_policy_version", "protocol_sha256", "pilot_run_id", "plan_id",
            "previous_snapshot_id", "capture_spec_id", "transition_id", "bucket",
            "object_name", "generation", "expected_encoded_sha256",
        },
        "ledger transition pin record has an invalid shape",
    )
    failed = False
    pin: PinnedQualityPilotLedgerTransitionRequest | None = None
    try:
        pin = PinnedQualityPilotLedgerTransitionRequest(
            storage_policy_version=_text(record["storage_policy_version"], "ledger transition pin policy is invalid"),
            protocol_sha256=_text(record["protocol_sha256"], "ledger transition pin protocol is invalid"),
            pilot_run_id=_sha256_field(record["pilot_run_id"], "ledger transition pin pilot run id is invalid"),
            plan_id=_sha256_field(record["plan_id"], "ledger transition pin plan id is invalid"),
            previous_snapshot_id=_optional_sha256_field(record["previous_snapshot_id"], "ledger transition pin predecessor is invalid"),
            capture_spec_id=_sha256_field(record["capture_spec_id"], "ledger transition pin capture spec id is invalid"),
            transition_id=_sha256_field(record["transition_id"], "ledger transition pin transition id is invalid"),
            bucket=_validate_bucket(record["bucket"]),
            object_name=_text(record["object_name"], "ledger transition pin object name is invalid"),
            generation=_integer(record["generation"], "ledger transition pin generation is invalid", minimum=1),
            expected_encoded_sha256=_sha256_field(record["expected_encoded_sha256"], "ledger transition pin hash is invalid"),
        )
    except QualityPilotInvocationControlPlaneError:
        raise
    except Exception:
        failed = True
    if failed or pin is None:
        _fail("ledger transition pin record failed reconstruction")
    return pin


# ---------------------------------------------------------------------------
# QualityPilotInvocationRunbook
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class QualityPilotInvocationRunbook(_FixedPostureMixin):
    """One immutable, independently re-verifiable production job runbook.

    Carries the exact campaign, provider version, target bucket, and the
    exact four scheduled windows for every one of the campaign's 20
    confirmed sessions, in canonical session-then-kind order. Never invents
    a session, calendar, cutoff, or timestamp -- every window is caller-
    pinned and independently reverified.
    """

    campaign: QualityPilotCampaignSpec
    provider_version: str
    bucket: str
    windows: tuple[ObservationWindowSpec, ...]
    runbook_id: str = field(init=False)

    def __post_init__(self) -> None:
        self._validate()
        object.__setattr__(self, "runbook_id", self._calculated_id())

    def _validate(self) -> None:
        campaign = _reconstruct_campaign(self.campaign)
        if type(self.provider_version) is not str or not (0 < len(self.provider_version) <= 128):
            _fail("invocation runbook provider version is invalid")
        _validate_bucket(self.bucket)
        expected_count = CONFIRMED_SESSION_COUNT * len(_WINDOW_KIND_ORDER)
        if type(self.windows) is not tuple or len(self.windows) != expected_count:
            _fail("invocation runbook window count is invalid")
        seen_ids: set[str] = set()
        for index, window in enumerate(self.windows):
            verified = _reconstruct_window(window)
            session_index, kind_index = divmod(index, len(_WINDOW_KIND_ORDER))
            if verified.market_session != campaign.confirmed_sessions[session_index]:
                _fail("invocation runbook window session is out of canonical order")
            if verified.window_kind is not _WINDOW_KIND_ORDER[kind_index]:
                _fail("invocation runbook window kind is out of canonical order")
            if verified.pilot_run_id != campaign.pilot_run_id:
                _fail("invocation runbook window pilot run id disagrees with the campaign")
            if verified.protocol_sha256 != campaign.protocol_sha256:
                _fail("invocation runbook window protocol disagrees with the campaign")
            if not is_window_inside_authorized_schedule(verified):
                _fail("invocation runbook window falls outside its authorized schedule gate")
            if verified.window_id in seen_ids:
                _fail("invocation runbook contains a duplicate window")
            seen_ids.add(verified.window_id)
        _validate_posture(self)

    def _calculated_id(self) -> str:
        failed = False
        calculated = ""
        try:
            calculated = content_id(
                {
                    "schema": QUALITY_PILOT_RUNBOOK_SCHEMA_VERSION,
                    "campaign_id": self.campaign.campaign_id,
                    "provider_version": self.provider_version,
                    "bucket": self.bucket,
                    "window_ids": tuple(window.window_id for window in self.windows),
                    "posture": _posture_tree(self),
                },
                length=64,
            )
        except Exception:
            failed = True
        if failed:
            _fail("invocation runbook identity calculation failed")
        return calculated

    def verify_content_identity(self) -> None:
        self._validate()
        if self.runbook_id != self._calculated_id():
            _fail("invocation runbook identity failed")


def _reconstruct_runbook(value: object) -> QualityPilotInvocationRunbook:
    if type(value) is not QualityPilotInvocationRunbook:
        _fail("invocation runbook type is invalid")
    failed = False
    reconstructed: QualityPilotInvocationRunbook | None = None
    try:
        reconstructed = QualityPilotInvocationRunbook(
            campaign=value.campaign,
            provider_version=value.provider_version,
            bucket=value.bucket,
            windows=value.windows,
        )
    except Exception:
        failed = True
    if failed or reconstructed is None:
        _fail("invocation runbook could not be independently reverified")
    if value.runbook_id != reconstructed.runbook_id:
        _fail("invocation runbook identity failed independent reverification")
    return reconstructed


def catalog_capture_spec_for_session(
    runbook: QualityPilotInvocationRunbook, market_session: date
) -> QualityPilotCaptureSpec:
    """Deterministically derive the exact 1-of-1 CATALOG_PREOPEN capture spec
    for one confirmed session from the runbook alone. Never recollects or
    invents a window -- the window is the runbook's own pinned record."""

    verified = _reconstruct_runbook(runbook)
    index_failed = False
    session_index = -1
    try:
        session_index = verified.campaign.confirmed_sessions.index(market_session)
    except ValueError:
        index_failed = True
    if index_failed:
        _fail("catalog capture spec session is not part of the runbook campaign")
    window = verified.windows[session_index * len(_WINDOW_KIND_ORDER)]
    if window.window_kind is not ScheduledWindowKind.CATALOG_PREOPEN:
        _fail("catalog capture spec window is not CATALOG_PREOPEN")

    build_failed = False
    spec: QualityPilotCaptureSpec | None = None
    try:
        spec = QualityPilotCaptureSpec(
            campaign=verified.campaign,
            window=window,
            provider=PROVIDER_ZERODHA_KITE,
            provider_version=verified.provider_version,
            requested_keys=(),
            provider_instrument_token=None,
            chunk_index=1,
            chunk_count=1,
            protocol_sha256=verified.campaign.protocol_sha256,
        )
    except Exception:
        build_failed = True
    if build_failed or spec is None:
        _fail("catalog capture spec could not be constructed")
    return spec


# ---------------------------------------------------------------------------
# QualityPilotActionBinding
# ---------------------------------------------------------------------------


class QualityPilotActionKind(Enum):
    CATALOG_BOOTSTRAP = "CATALOG_BOOTSTRAP"
    RESUMABLE_CAPTURE = "RESUMABLE_CAPTURE"


@dataclass(frozen=True, slots=True)
class QualityPilotActionBinding(_FixedPostureMixin):
    """One immutable, independently re-verifiable unit of scheduled work.

    Embeds the exact runbook and identifies exactly one confirmed
    session/window plus either genesis/extension predecessor pins for a
    catalog bootstrap or an exact plan/predecessor-transition pin and
    target capture-spec id for one resumable capture step.
    """

    runbook: QualityPilotInvocationRunbook
    action_kind: QualityPilotActionKind
    market_session: date
    window_kind: ScheduledWindowKind
    prior_plan_pin: PinnedQualityPilotControlArtifactRequest | None
    prior_transition_pin: PinnedQualityPilotLedgerTransitionRequest | None
    plan_pin: PinnedQualityPilotControlArtifactRequest | None
    predecessor_transition_pin: PinnedQualityPilotLedgerTransitionRequest | None
    target_capture_spec_id: str | None
    action_id: str = field(init=False)

    def __post_init__(self) -> None:
        self._validate()
        object.__setattr__(self, "action_id", self._calculated_id())

    def _validate(self) -> None:
        runbook = _reconstruct_runbook(self.runbook)
        if type(self.action_kind) is not QualityPilotActionKind:
            _fail("action binding kind is invalid")
        if type(self.market_session) is not date or self.market_session not in runbook.campaign.confirmed_sessions:
            _fail("action binding session is not part of the runbook campaign")
        if type(self.window_kind) is not ScheduledWindowKind:
            _fail("action binding window kind is invalid")

        if self.action_kind is QualityPilotActionKind.CATALOG_BOOTSTRAP:
            if self.window_kind is not ScheduledWindowKind.CATALOG_PREOPEN:
                _fail("catalog action binding window kind must be CATALOG_PREOPEN")
            if (
                self.plan_pin is not None
                or self.predecessor_transition_pin is not None
                or self.target_capture_spec_id is not None
            ):
                _fail("catalog action binding must not carry resumable fields")
            if (self.prior_plan_pin is None) != (self.prior_transition_pin is None):
                _fail("catalog action binding predecessor pins must both be present or both be absent")
            is_first_session = self.market_session == runbook.campaign.confirmed_sessions[0]
            if is_first_session and self.prior_plan_pin is not None:
                _fail("catalog action binding for the first session must not carry predecessor pins")
            if not is_first_session and self.prior_plan_pin is None:
                _fail("catalog action binding for a non-first session requires predecessor pins")
            if self.prior_plan_pin is not None:
                prior_plan_pin = _reconstruct_plan_pin(self.prior_plan_pin)
                prior_transition_pin = _reconstruct_transition_pin(self.prior_transition_pin)
                if prior_plan_pin.kind is not QualityPilotControlArtifactKind.CAMPAIGN_PLAN:
                    _fail("catalog action binding prior plan pin kind is invalid")
                if (
                    prior_plan_pin.pilot_run_id != runbook.campaign.pilot_run_id
                    or prior_transition_pin.pilot_run_id != runbook.campaign.pilot_run_id
                ):
                    _fail("catalog action binding predecessor pins disagree with the runbook pilot run id")
                if (
                    prior_plan_pin.protocol_sha256 != runbook.campaign.protocol_sha256
                    or prior_transition_pin.protocol_sha256 != runbook.campaign.protocol_sha256
                ):
                    _fail("catalog action binding predecessor pins disagree with the runbook protocol")
                if prior_plan_pin.bucket != runbook.bucket or prior_transition_pin.bucket != runbook.bucket:
                    _fail("catalog action binding predecessor pins disagree with the runbook bucket")
                if prior_transition_pin.plan_id != prior_plan_pin.artifact_id:
                    _fail("catalog action binding predecessor pins disagree on plan lineage")
        else:
            if self.window_kind is ScheduledWindowKind.CATALOG_PREOPEN:
                _fail("resumable action binding window kind must not be CATALOG_PREOPEN")
            if self.prior_plan_pin is not None or self.prior_transition_pin is not None:
                _fail("resumable action binding must not carry catalog predecessor pins")
            if self.plan_pin is None or self.predecessor_transition_pin is None:
                _fail("resumable action binding requires an exact plan pin and predecessor transition pin")
            plan_pin = _reconstruct_plan_pin(self.plan_pin)
            predecessor_transition_pin = _reconstruct_transition_pin(self.predecessor_transition_pin)
            if plan_pin.kind is not QualityPilotControlArtifactKind.CAMPAIGN_PLAN:
                _fail("resumable action binding plan pin kind is invalid")
            if (
                plan_pin.pilot_run_id != runbook.campaign.pilot_run_id
                or predecessor_transition_pin.pilot_run_id != runbook.campaign.pilot_run_id
            ):
                _fail("resumable action binding pins disagree with the runbook pilot run id")
            if (
                plan_pin.protocol_sha256 != runbook.campaign.protocol_sha256
                or predecessor_transition_pin.protocol_sha256 != runbook.campaign.protocol_sha256
            ):
                _fail("resumable action binding pins disagree with the runbook protocol")
            if plan_pin.bucket != runbook.bucket or predecessor_transition_pin.bucket != runbook.bucket:
                _fail("resumable action binding pins disagree with the runbook bucket")
            if predecessor_transition_pin.plan_id != plan_pin.artifact_id:
                _fail("resumable action binding pins disagree on plan lineage")
            if not _is_sha256(self.target_capture_spec_id):
                _fail("resumable action binding target capture spec id is invalid")

        _validate_posture(self)

    def _calculated_id(self) -> str:
        failed = False
        calculated = ""
        try:
            calculated = content_id(
                {
                    "schema": QUALITY_PILOT_ACTION_BINDING_SCHEMA_VERSION,
                    "runbook_id": self.runbook.runbook_id,
                    "action_kind": self.action_kind.value,
                    "market_session": self.market_session,
                    "window_kind": self.window_kind.value,
                    "prior_plan_pin": self.prior_plan_pin,
                    "prior_transition_pin": self.prior_transition_pin,
                    "plan_pin": self.plan_pin,
                    "predecessor_transition_pin": self.predecessor_transition_pin,
                    "target_capture_spec_id": self.target_capture_spec_id,
                    "posture": _posture_tree(self),
                },
                length=64,
            )
        except Exception:
            failed = True
        if failed:
            _fail("action binding identity calculation failed")
        return calculated

    def verify_content_identity(self) -> None:
        self._validate()
        if self.action_id != self._calculated_id():
            _fail("action binding identity failed")


def _reconstruct_action_binding(value: object) -> QualityPilotActionBinding:
    if type(value) is not QualityPilotActionBinding:
        _fail("action binding type is invalid")
    failed = False
    reconstructed: QualityPilotActionBinding | None = None
    try:
        reconstructed = QualityPilotActionBinding(
            runbook=value.runbook,
            action_kind=value.action_kind,
            market_session=value.market_session,
            window_kind=value.window_kind,
            prior_plan_pin=value.prior_plan_pin,
            prior_transition_pin=value.prior_transition_pin,
            plan_pin=value.plan_pin,
            predecessor_transition_pin=value.predecessor_transition_pin,
            target_capture_spec_id=value.target_capture_spec_id,
        )
    except Exception:
        failed = True
    if failed or reconstructed is None:
        _fail("action binding could not be independently reverified")
    if value.action_id != reconstructed.action_id:
        _fail("action binding identity failed independent reverification")
    return reconstructed


def _runbook_tree(value: QualityPilotInvocationRunbook) -> dict[str, object]:
    return {
        "schema_version": QUALITY_PILOT_RUNBOOK_SCHEMA_VERSION,
        "campaign": {
            "calendar_decision_ids": list(value.campaign.calendar_decision_ids),
            "campaign_id": value.campaign.campaign_id,
            "confirmed_sessions": [item.isoformat() for item in value.campaign.confirmed_sessions],
            "pilot_run_id": value.campaign.pilot_run_id,
            "protocol_sha256": value.campaign.protocol_sha256,
        },
        "provider_version": value.provider_version,
        "bucket": value.bucket,
        "windows": [json.loads(window.canonical_json()) for window in value.windows],
        "runbook_id": value.runbook_id,
        "posture": _posture_tree(value),
    }


def encode_quality_pilot_invocation_runbook(runbook: QualityPilotInvocationRunbook) -> bytes:
    if type(runbook) is not QualityPilotInvocationRunbook:
        _fail("invocation runbook type is invalid")
    failed = False
    try:
        runbook.verify_content_identity()
    except Exception:
        failed = True
    if failed:
        _fail("invocation runbook failed independent verification")
    encoded = _canonical_json_bytes(_runbook_tree(runbook))
    if len(encoded) > MAXIMUM_RUNBOOK_BYTES:
        _fail("invocation runbook encoding exceeds its bounded size")
    return encoded


def _decode_campaign(value: object) -> QualityPilotCampaignSpec:
    record = _exact_dict(
        value,
        {"calendar_decision_ids", "campaign_id", "confirmed_sessions", "pilot_run_id", "protocol_sha256"},
        "runbook campaign record has an invalid shape",
    )
    sessions = tuple(
        _date_text(item, "runbook campaign session is invalid")
        for item in _exact_list(record["confirmed_sessions"], "runbook campaign sessions are invalid")
    )
    decisions = tuple(
        _text(item, "runbook campaign decision id is invalid")
        for item in _exact_list(record["calendar_decision_ids"], "runbook campaign decisions are invalid")
    )
    failed = False
    campaign: QualityPilotCampaignSpec | None = None
    try:
        campaign = QualityPilotCampaignSpec(
            pilot_run_id=_text(record["pilot_run_id"], "runbook campaign pilot run id is invalid"),
            protocol_sha256=_text(record["protocol_sha256"], "runbook campaign protocol is invalid"),
            confirmed_sessions=sessions,
            calendar_decision_ids=decisions,
        )
    except Exception:
        failed = True
    if failed or campaign is None:
        _fail("runbook campaign record failed reconstruction")
    if campaign.campaign_id != record["campaign_id"]:
        _fail("runbook campaign record identity failed")
    return campaign


def decode_quality_pilot_invocation_runbook(content_bytes: bytes) -> QualityPilotInvocationRunbook:
    root = _parse_json(content_bytes, MAXIMUM_RUNBOOK_BYTES)
    record = _exact_dict(
        root,
        {"schema_version", "campaign", "provider_version", "bucket", "windows", "runbook_id", "posture"},
        "invocation runbook wire shape is invalid",
    )
    if record["schema_version"] != QUALITY_PILOT_RUNBOOK_SCHEMA_VERSION:
        _fail("invocation runbook wire schema is invalid")
    campaign = _decode_campaign(record["campaign"])
    decode_failed = False
    windows: tuple[ObservationWindowSpec, ...] = ()
    try:
        windows = tuple(
            ObservationWindowSpec._decode(item)
            for item in _exact_list(record["windows"], "invocation runbook windows are invalid")
        )
    except QualityPilotInvocationControlPlaneError:
        raise
    except Exception:
        decode_failed = True
    if decode_failed:
        _fail("invocation runbook windows failed reconstruction")
    failed = False
    runbook: QualityPilotInvocationRunbook | None = None
    try:
        runbook = QualityPilotInvocationRunbook(
            campaign=campaign,
            provider_version=_text(record["provider_version"], "invocation runbook provider version is invalid"),
            bucket=_validate_bucket(record["bucket"]),
            windows=windows,
        )
    except Exception:
        failed = True
    if failed or runbook is None:
        _fail("invocation runbook failed reconstruction")
    if (
        runbook.runbook_id != record["runbook_id"]
        or record["posture"] != _posture_tree(runbook)
        or encode_quality_pilot_invocation_runbook(runbook) != content_bytes
    ):
        _fail("invocation runbook wire identity failed")
    return runbook


def _action_binding_tree(value: QualityPilotActionBinding) -> dict[str, object]:
    return {
        "schema_version": QUALITY_PILOT_ACTION_BINDING_SCHEMA_VERSION,
        "runbook": _runbook_tree(value.runbook),
        "action_kind": value.action_kind.value,
        "market_session": value.market_session.isoformat(),
        "window_kind": value.window_kind.value,
        "prior_plan_pin": _plan_pin_tree(value.prior_plan_pin),
        "prior_transition_pin": _transition_pin_tree(value.prior_transition_pin),
        "plan_pin": _plan_pin_tree(value.plan_pin),
        "predecessor_transition_pin": _transition_pin_tree(value.predecessor_transition_pin),
        "target_capture_spec_id": value.target_capture_spec_id,
        "action_id": value.action_id,
        "posture": _posture_tree(value),
    }


def encode_quality_pilot_action_binding(binding: QualityPilotActionBinding) -> bytes:
    if type(binding) is not QualityPilotActionBinding:
        _fail("action binding type is invalid")
    failed = False
    try:
        binding.verify_content_identity()
    except Exception:
        failed = True
    if failed:
        _fail("action binding failed independent verification")
    encoded = _canonical_json_bytes(_action_binding_tree(binding))
    if len(encoded) > MAXIMUM_ACTION_BINDING_BYTES:
        _fail("action binding encoding exceeds its bounded size")
    return encoded


def decode_quality_pilot_action_binding(content_bytes: bytes) -> QualityPilotActionBinding:
    root = _parse_json(content_bytes, MAXIMUM_ACTION_BINDING_BYTES)
    record = _exact_dict(
        root,
        {
            "schema_version", "runbook", "action_kind", "market_session", "window_kind",
            "prior_plan_pin", "prior_transition_pin", "plan_pin", "predecessor_transition_pin",
            "target_capture_spec_id", "action_id", "posture",
        },
        "action binding wire shape is invalid",
    )
    if record["schema_version"] != QUALITY_PILOT_ACTION_BINDING_SCHEMA_VERSION:
        _fail("action binding wire schema is invalid")

    runbook_record = _exact_dict(
        record["runbook"],
        {"schema_version", "campaign", "provider_version", "bucket", "windows", "runbook_id", "posture"},
        "action binding runbook wire shape is invalid",
    )
    campaign = _decode_campaign(runbook_record["campaign"])
    decode_failed = False
    windows: tuple[ObservationWindowSpec, ...] = ()
    try:
        windows = tuple(
            ObservationWindowSpec._decode(item)
            for item in _exact_list(runbook_record["windows"], "action binding runbook windows are invalid")
        )
    except QualityPilotInvocationControlPlaneError:
        raise
    except Exception:
        decode_failed = True
    if decode_failed:
        _fail("action binding runbook windows failed reconstruction")
    runbook_failed = False
    runbook: QualityPilotInvocationRunbook | None = None
    try:
        runbook = QualityPilotInvocationRunbook(
            campaign=campaign,
            provider_version=_text(runbook_record["provider_version"], "action binding runbook provider version is invalid"),
            bucket=_validate_bucket(runbook_record["bucket"]),
            windows=windows,
        )
    except Exception:
        runbook_failed = True
    if runbook_failed or runbook is None or runbook.runbook_id != runbook_record["runbook_id"]:
        _fail("action binding runbook failed reconstruction")

    failed = False
    binding: QualityPilotActionBinding | None = None
    try:
        binding = QualityPilotActionBinding(
            runbook=runbook,
            action_kind=_enum(record["action_kind"], QualityPilotActionKind, "action binding kind is invalid"),
            market_session=_date_text(record["market_session"], "action binding session is invalid"),
            window_kind=_enum(record["window_kind"], ScheduledWindowKind, "action binding window kind is invalid"),
            prior_plan_pin=_decode_plan_pin(record["prior_plan_pin"]),
            prior_transition_pin=_decode_transition_pin(record["prior_transition_pin"]),
            plan_pin=_decode_plan_pin(record["plan_pin"]),
            predecessor_transition_pin=_decode_transition_pin(record["predecessor_transition_pin"]),
            target_capture_spec_id=_optional_sha256_field(record["target_capture_spec_id"], "action binding target capture spec id is invalid"),
        )
    except QualityPilotInvocationControlPlaneError:
        raise
    except Exception:
        failed = True
    if failed or binding is None:
        _fail("action binding failed reconstruction")
    if (
        binding.action_id != record["action_id"]
        or record["posture"] != _posture_tree(binding)
        or encode_quality_pilot_action_binding(binding) != content_bytes
    ):
        _fail("action binding wire identity failed")
    return binding


def canonical_quality_pilot_action_binding_object_name(pilot_run_id: str, action_id: str) -> str:
    if not _is_sha256(pilot_run_id) or not _is_sha256(action_id):
        _fail("action binding route is invalid")
    return f"quality-pilot/v1/{pilot_run_id}/invocations/actions/{action_id}.json"


@dataclass(frozen=True, slots=True)
class PublishedQualityPilotActionBinding(_FixedPostureMixin):
    storage_policy_version: str
    protocol_sha256: str
    pilot_run_id: str
    action_id: str
    bucket: str
    object_name: str
    generation: int
    encoded_byte_count: int
    encoded_sha256: str

    def __post_init__(self) -> None:
        if self.storage_policy_version != QUALITY_PILOT_INVOCATION_STORAGE_POLICY_VERSION:
            _fail("published action binding storage policy is invalid")
        if self.protocol_sha256 != PILOT_PROTOCOL_SHA256:
            _fail("published action binding protocol hash is invalid")
        if not _is_sha256(self.pilot_run_id) or not _is_sha256(self.action_id):
            _fail("published action binding identity is invalid")
        _validate_bucket(self.bucket)
        if self.object_name != canonical_quality_pilot_action_binding_object_name(self.pilot_run_id, self.action_id):
            _fail("published action binding route is invalid")
        if type(self.generation) is not int or self.generation <= 0:
            _fail("published action binding generation is invalid")
        if type(self.encoded_byte_count) is not int or not (0 < self.encoded_byte_count <= MAXIMUM_ACTION_BINDING_BYTES):
            _fail("published action binding byte count is invalid")
        if not _is_sha256(self.encoded_sha256):
            _fail("published action binding hash is invalid")


def _reconstruct_published_action_binding(value: object) -> PublishedQualityPilotActionBinding:
    if type(value) is not PublishedQualityPilotActionBinding:
        _fail("published action binding type is invalid")
    failed = False
    reconstructed: PublishedQualityPilotActionBinding | None = None
    try:
        reconstructed = PublishedQualityPilotActionBinding(
            storage_policy_version=value.storage_policy_version,
            protocol_sha256=value.protocol_sha256,
            pilot_run_id=value.pilot_run_id,
            action_id=value.action_id,
            bucket=value.bucket,
            object_name=value.object_name,
            generation=value.generation,
            encoded_byte_count=value.encoded_byte_count,
            encoded_sha256=value.encoded_sha256,
        )
    except Exception:
        failed = True
    if failed or reconstructed is None:
        _fail("published action binding could not be independently reverified")
    return reconstructed


def publish_quality_pilot_action_binding(
    binding: QualityPilotActionBinding, writer: StateObjectWriter
) -> PublishedQualityPilotActionBinding:
    if type(binding) is not QualityPilotActionBinding:
        _fail("action binding type is invalid")
    pilot_run_id = binding.runbook.campaign.pilot_run_id
    bucket = binding.runbook.bucket
    content_bytes = encode_quality_pilot_action_binding(binding)
    object_name = canonical_quality_pilot_action_binding_object_name(pilot_run_id, binding.action_id)
    expected_hash = hashlib.sha256(content_bytes).hexdigest()
    failed = False
    published: object = None
    try:
        published = writer.create_or_verify(
            bucket=bucket,
            object_name=object_name,
            content_bytes=content_bytes,
            content_type=QUALITY_PILOT_INVOCATION_CONTENT_TYPE,
            maximum_bytes=MAXIMUM_ACTION_BINDING_BYTES,
        )
    except Exception:
        failed = True
    if failed or type(published) is not PublishedStateObject:
        _fail("action binding writer failed")
    if (
        published.object_name != object_name
        or published.byte_count != len(content_bytes)
        or published.sha256 != expected_hash
    ):
        _fail("action binding writer result failed verification")
    return PublishedQualityPilotActionBinding(
        storage_policy_version=QUALITY_PILOT_INVOCATION_STORAGE_POLICY_VERSION,
        protocol_sha256=PILOT_PROTOCOL_SHA256,
        pilot_run_id=pilot_run_id,
        action_id=binding.action_id,
        bucket=bucket,
        object_name=object_name,
        generation=published.generation,
        encoded_byte_count=len(content_bytes),
        encoded_sha256=expected_hash,
    )


@dataclass(frozen=True, slots=True)
class PinnedQualityPilotActionBindingRequest:
    storage_policy_version: str
    protocol_sha256: str
    pilot_run_id: str
    action_id: str
    bucket: str
    object_name: str
    generation: int
    expected_encoded_sha256: str

    def __post_init__(self) -> None:
        if self.storage_policy_version != QUALITY_PILOT_INVOCATION_STORAGE_POLICY_VERSION:
            _fail("action binding pin storage policy is invalid")
        if self.protocol_sha256 != PILOT_PROTOCOL_SHA256:
            _fail("action binding pin protocol hash is invalid")
        if not _is_sha256(self.pilot_run_id) or not _is_sha256(self.action_id):
            _fail("action binding pin identity is invalid")
        _validate_bucket(self.bucket)
        if self.object_name != canonical_quality_pilot_action_binding_object_name(self.pilot_run_id, self.action_id):
            _fail("action binding pin route is invalid")
        if type(self.generation) is not int or self.generation <= 0:
            _fail("action binding pin generation is invalid")
        if not _is_sha256(self.expected_encoded_sha256):
            _fail("action binding pin hash is invalid")


def pinned_quality_pilot_action_binding_request(
    published: PublishedQualityPilotActionBinding,
) -> PinnedQualityPilotActionBindingRequest:
    reconstructed = _reconstruct_published_action_binding(published)
    return PinnedQualityPilotActionBindingRequest(
        storage_policy_version=reconstructed.storage_policy_version,
        protocol_sha256=reconstructed.protocol_sha256,
        pilot_run_id=reconstructed.pilot_run_id,
        action_id=reconstructed.action_id,
        bucket=reconstructed.bucket,
        object_name=reconstructed.object_name,
        generation=reconstructed.generation,
        expected_encoded_sha256=reconstructed.encoded_sha256,
    )


@dataclass(frozen=True, slots=True)
class LoadedQualityPilotActionBinding:
    binding: QualityPilotActionBinding
    request: PinnedQualityPilotActionBindingRequest


def read_pinned_quality_pilot_action_binding(
    request: PinnedQualityPilotActionBindingRequest, reader: GCSObjectReader
) -> LoadedQualityPilotActionBinding:
    if type(request) is not PinnedQualityPilotActionBindingRequest:
        _fail("action binding pin request type is invalid")
    failed = False
    payload: object = None
    try:
        payload = reader.read_generation(
            bucket=request.bucket,
            object_name=request.object_name,
            generation=request.generation,
            maximum_bytes=MAXIMUM_ACTION_BINDING_BYTES,
        )
    except Exception:
        failed = True
    if failed or type(payload) is not GCSObjectPayload:
        _fail("action binding reader failed")
    if type(payload.generation) is not int or payload.generation != request.generation:
        _fail("action binding reader generation failed verification")
    content_bytes = payload.content_bytes
    if type(content_bytes) is not bytes or not content_bytes or len(content_bytes) > MAXIMUM_ACTION_BINDING_BYTES:
        _fail("action binding reader content failed verification")
    if hashlib.sha256(content_bytes).hexdigest() != request.expected_encoded_sha256:
        _fail("action binding reader hash failed verification")
    binding = decode_quality_pilot_action_binding(content_bytes)
    if (
        binding.action_id != request.action_id
        or binding.runbook.campaign.pilot_run_id != request.pilot_run_id
        or binding.runbook.bucket != request.bucket
    ):
        _fail("action binding reader lineage failed verification")
    return LoadedQualityPilotActionBinding(binding=binding, request=request)


# ---------------------------------------------------------------------------
# Current-object reader (reload-then-pin) -- used for window entries and
# completion receipts, whose generation is not known in advance.
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class ObservedQualityPilotObject:
    object_name: str
    generation: int
    byte_count: int
    sha256: str
    content_bytes: bytes

    def __post_init__(self) -> None:
        if type(self.object_name) is not str or not self.object_name:
            _fail("observed invocation object name is invalid")
        if type(self.generation) is bool or type(self.generation) is not int or not (1 <= self.generation <= _MAXIMUM_GENERATION):
            _fail("observed invocation object generation is invalid")
        if type(self.content_bytes) is not bytes or not self.content_bytes:
            _fail("observed invocation object content is invalid")
        if type(self.byte_count) is not int or self.byte_count != len(self.content_bytes):
            _fail("observed invocation object byte count is invalid")
        if type(self.sha256) is not str or self.sha256 != hashlib.sha256(self.content_bytes).hexdigest():
            _fail("observed invocation object hash is invalid")


class QualityPilotCurrentObjectReader(Protocol):
    """Reads exactly one deterministic object name at its currently observed
    generation, then re-pins that generation before trusting the downloaded
    bytes. Never lists a bucket and never selects a "latest" object."""

    def read_current(
        self, *, bucket: str, object_name: str, maximum_bytes: int
    ) -> ObservedQualityPilotObject: ...

    def read_current_optional(
        self, *, bucket: str, object_name: str, maximum_bytes: int
    ) -> ObservedQualityPilotObject | None:
        """Identical to ``read_current`` except it returns ``None`` ONLY
        when the exact object is proven not to exist. Every other failure
        must still raise, never be treated as absence."""
        ...


class GoogleCloudStorageQualityPilotCurrentObjectReader:
    """Production QualityPilotCurrentObjectReader backed by google-cloud-storage.

    The constructor requires an already-constructed client; it never
    imports google.cloud.storage, never constructs a client itself, and
    never falls back to an ambient default client.
    """

    def __init__(self, client: object) -> None:
        if client is None:
            _fail("invocation control plane GCS client is required")
        self._client = client

    def _validated_read_request(self, *, bucket: str, object_name: str, maximum_bytes: int) -> str:
        bucket = _validate_bucket(bucket)
        if type(object_name) is not str or not object_name:
            _fail("invocation control plane object name is invalid")
        if type(maximum_bytes) is not int or maximum_bytes <= 0:
            _fail("invocation control plane maximum bytes is invalid")
        return bucket

    def _reload_and_observe_generation(self, *, blob: object) -> int:
        blob.reload(retry=None)
        observed_generation = blob.generation
        if type(observed_generation) is bool or type(observed_generation) is not int or not (1 <= observed_generation <= _MAXIMUM_GENERATION):
            _fail("invocation control plane observed generation is invalid")
        return observed_generation

    def _pin_and_verify(
        self, *, bucket: str, object_name: str, maximum_bytes: int, observed_generation: int
    ) -> ObservedQualityPilotObject:
        pinned_blob = self._client.bucket(bucket).blob(object_name, generation=observed_generation)
        downloaded = pinned_blob.download_as_bytes(
            end=maximum_bytes, raw_download=True, if_generation_match=observed_generation, retry=None
        )
        pinned_generation = pinned_blob.generation
        if type(pinned_generation) is bool or type(pinned_generation) is not int or pinned_generation != observed_generation:
            _fail("invocation control plane pinned generation disagrees with the observed generation")
        if type(downloaded) is not bytes or not (0 < len(downloaded) <= maximum_bytes):
            _fail("invocation control plane downloaded content is invalid")
        return ObservedQualityPilotObject(
            object_name=object_name,
            generation=observed_generation,
            byte_count=len(downloaded),
            sha256=hashlib.sha256(downloaded).hexdigest(),
            content_bytes=downloaded,
        )

    def read_current(self, *, bucket: str, object_name: str, maximum_bytes: int) -> ObservedQualityPilotObject:
        bucket = self._validated_read_request(bucket=bucket, object_name=object_name, maximum_bytes=maximum_bytes)
        construct_failed = False
        blob: object = None
        try:
            blob = self._client.bucket(bucket).blob(object_name)
        except Exception:
            construct_failed = True
        if construct_failed or blob is None:
            _fail("invocation control plane blob handle could not be constructed")

        observe_failed = False
        observed_generation: int | None = None
        try:
            observed_generation = self._reload_and_observe_generation(blob=blob)
        except Exception:
            observe_failed = True
        if observe_failed or observed_generation is None:
            _fail("invocation control plane object could not be observed")

        pin_failed = False
        observed: ObservedQualityPilotObject | None = None
        try:
            observed = self._pin_and_verify(
                bucket=bucket, object_name=object_name, maximum_bytes=maximum_bytes, observed_generation=observed_generation
            )
        except Exception:
            pin_failed = True
        if pin_failed or observed is None:
            _fail("invocation control plane object could not be downloaded")
        return observed

    def read_current_optional(
        self, *, bucket: str, object_name: str, maximum_bytes: int
    ) -> ObservedQualityPilotObject | None:
        bucket = self._validated_read_request(bucket=bucket, object_name=object_name, maximum_bytes=maximum_bytes)
        construct_failed = False
        blob: object = None
        try:
            blob = self._client.bucket(bucket).blob(object_name)
        except Exception:
            construct_failed = True
        if construct_failed or blob is None:
            _fail("invocation control plane blob handle could not be constructed")

        not_found = False
        observe_failed = False
        observed_generation: int | None = None
        try:
            observed_generation = self._reload_and_observe_generation(blob=blob)
        except Exception as error:
            if NotFound is not None and isinstance(error, NotFound):
                not_found = True
            else:
                observe_failed = True
        if not_found:
            return None
        if observe_failed or observed_generation is None:
            _fail("invocation control plane object could not be observed")

        pin_failed = False
        observed: ObservedQualityPilotObject | None = None
        try:
            observed = self._pin_and_verify(
                bucket=bucket, object_name=object_name, maximum_bytes=maximum_bytes, observed_generation=observed_generation
            )
        except Exception:
            pin_failed = True
        if pin_failed or observed is None:
            _fail("invocation control plane object could not be downloaded")
        return observed


# ---------------------------------------------------------------------------
# QualityPilotWindowEntry
# ---------------------------------------------------------------------------


def canonical_quality_pilot_window_entry_object_name(
    pilot_run_id: str, market_session: date, window_kind: ScheduledWindowKind
) -> str:
    if not _is_sha256(pilot_run_id):
        _fail("window entry route pilot run id is invalid")
    if type(market_session) is not date:
        _fail("window entry route session is invalid")
    if type(window_kind) is not ScheduledWindowKind:
        _fail("window entry route kind is invalid")
    return f"quality-pilot/v1/{pilot_run_id}/invocations/windows/{market_session.isoformat()}/{window_kind.value}.json"


@dataclass(frozen=True, slots=True)
class QualityPilotWindowEntry(_FixedPostureMixin):
    pilot_run_id: str
    market_session: date
    window_kind: ScheduledWindowKind
    action_binding_pin: PinnedQualityPilotActionBindingRequest
    window_entry_id: str = field(init=False)

    def __post_init__(self) -> None:
        self._validate()
        object.__setattr__(self, "window_entry_id", self._calculated_id())

    def _validate(self) -> None:
        if not _is_sha256(self.pilot_run_id):
            _fail("window entry pilot run id is invalid")
        if type(self.market_session) is not date:
            _fail("window entry session is invalid")
        if type(self.window_kind) is not ScheduledWindowKind:
            _fail("window entry kind is invalid")
        pin = _reconstruct_action_binding_pin(self.action_binding_pin)
        if pin.pilot_run_id != self.pilot_run_id:
            _fail("window entry pin disagrees on pilot run id")
        _validate_posture(self)

    def _calculated_id(self) -> str:
        failed = False
        calculated = ""
        try:
            calculated = content_id(
                {
                    "schema": QUALITY_PILOT_WINDOW_ENTRY_SCHEMA_VERSION,
                    "pilot_run_id": self.pilot_run_id,
                    "market_session": self.market_session,
                    "window_kind": self.window_kind.value,
                    "action_binding_pin": self.action_binding_pin,
                    "posture": _posture_tree(self),
                },
                length=64,
            )
        except Exception:
            failed = True
        if failed:
            _fail("window entry identity calculation failed")
        return calculated

    def verify_content_identity(self) -> None:
        self._validate()
        if self.window_entry_id != self._calculated_id():
            _fail("window entry identity failed")


def _reconstruct_action_binding_pin(value: object) -> PinnedQualityPilotActionBindingRequest:
    if type(value) is not PinnedQualityPilotActionBindingRequest:
        _fail("action binding pin type is invalid")
    failed = False
    reconstructed: PinnedQualityPilotActionBindingRequest | None = None
    try:
        reconstructed = PinnedQualityPilotActionBindingRequest(
            storage_policy_version=value.storage_policy_version,
            protocol_sha256=value.protocol_sha256,
            pilot_run_id=value.pilot_run_id,
            action_id=value.action_id,
            bucket=value.bucket,
            object_name=value.object_name,
            generation=value.generation,
            expected_encoded_sha256=value.expected_encoded_sha256,
        )
    except Exception:
        failed = True
    if failed or reconstructed is None:
        _fail("action binding pin could not be independently reverified")
    return reconstructed


def _action_binding_pin_tree(value: PinnedQualityPilotActionBindingRequest | None) -> dict[str, object] | None:
    if value is None:
        return None
    return {
        "storage_policy_version": value.storage_policy_version,
        "protocol_sha256": value.protocol_sha256,
        "pilot_run_id": value.pilot_run_id,
        "action_id": value.action_id,
        "bucket": value.bucket,
        "object_name": value.object_name,
        "generation": value.generation,
        "expected_encoded_sha256": value.expected_encoded_sha256,
    }


def _decode_action_binding_pin(value: object) -> PinnedQualityPilotActionBindingRequest | None:
    if value is None:
        return None
    record = _exact_dict(
        value,
        {"storage_policy_version", "protocol_sha256", "pilot_run_id", "action_id", "bucket", "object_name", "generation", "expected_encoded_sha256"},
        "action binding pin record has an invalid shape",
    )
    failed = False
    pin: PinnedQualityPilotActionBindingRequest | None = None
    try:
        pin = PinnedQualityPilotActionBindingRequest(
            storage_policy_version=_text(record["storage_policy_version"], "action binding pin policy is invalid"),
            protocol_sha256=_text(record["protocol_sha256"], "action binding pin protocol is invalid"),
            pilot_run_id=_sha256_field(record["pilot_run_id"], "action binding pin pilot run id is invalid"),
            action_id=_sha256_field(record["action_id"], "action binding pin action id is invalid"),
            bucket=_validate_bucket(record["bucket"]),
            object_name=_text(record["object_name"], "action binding pin object name is invalid"),
            generation=_integer(record["generation"], "action binding pin generation is invalid", minimum=1),
            expected_encoded_sha256=_sha256_field(record["expected_encoded_sha256"], "action binding pin hash is invalid"),
        )
    except QualityPilotInvocationControlPlaneError:
        raise
    except Exception:
        failed = True
    if failed or pin is None:
        _fail("action binding pin record failed reconstruction")
    return pin


def _window_entry_tree(value: QualityPilotWindowEntry) -> dict[str, object]:
    return {
        "schema_version": QUALITY_PILOT_WINDOW_ENTRY_SCHEMA_VERSION,
        "pilot_run_id": value.pilot_run_id,
        "market_session": value.market_session.isoformat(),
        "window_kind": value.window_kind.value,
        "action_binding_pin": _action_binding_pin_tree(value.action_binding_pin),
        "window_entry_id": value.window_entry_id,
        "posture": _posture_tree(value),
    }


def encode_quality_pilot_window_entry(entry: QualityPilotWindowEntry) -> bytes:
    if type(entry) is not QualityPilotWindowEntry:
        _fail("window entry type is invalid")
    failed = False
    try:
        entry.verify_content_identity()
    except Exception:
        failed = True
    if failed:
        _fail("window entry failed independent verification")
    encoded = _canonical_json_bytes(_window_entry_tree(entry))
    if len(encoded) > MAXIMUM_WINDOW_ENTRY_BYTES:
        _fail("window entry encoding exceeds its bounded size")
    return encoded


def decode_quality_pilot_window_entry(content_bytes: bytes) -> QualityPilotWindowEntry:
    root = _parse_json(content_bytes, MAXIMUM_WINDOW_ENTRY_BYTES)
    record = _exact_dict(
        root,
        {"schema_version", "pilot_run_id", "market_session", "window_kind", "action_binding_pin", "window_entry_id", "posture"},
        "window entry wire shape is invalid",
    )
    if record["schema_version"] != QUALITY_PILOT_WINDOW_ENTRY_SCHEMA_VERSION:
        _fail("window entry wire schema is invalid")
    failed = False
    entry: QualityPilotWindowEntry | None = None
    try:
        entry = QualityPilotWindowEntry(
            pilot_run_id=_sha256_field(record["pilot_run_id"], "window entry pilot run id is invalid"),
            market_session=_date_text(record["market_session"], "window entry session is invalid"),
            window_kind=_enum(record["window_kind"], ScheduledWindowKind, "window entry kind is invalid"),
            action_binding_pin=_decode_action_binding_pin(record["action_binding_pin"]),
        )
    except QualityPilotInvocationControlPlaneError:
        raise
    except Exception:
        failed = True
    if failed or entry is None:
        _fail("window entry failed reconstruction")
    if (
        entry.window_entry_id != record["window_entry_id"]
        or record["posture"] != _posture_tree(entry)
        or encode_quality_pilot_window_entry(entry) != content_bytes
    ):
        _fail("window entry wire identity failed")
    return entry


def publish_quality_pilot_window_entry(entry: QualityPilotWindowEntry, bucket: str, writer: StateObjectWriter) -> PublishedStateObject:
    if type(entry) is not QualityPilotWindowEntry:
        _fail("window entry type is invalid")
    bucket = _validate_bucket(bucket)
    content_bytes = encode_quality_pilot_window_entry(entry)
    object_name = canonical_quality_pilot_window_entry_object_name(entry.pilot_run_id, entry.market_session, entry.window_kind)
    expected_hash = hashlib.sha256(content_bytes).hexdigest()
    failed = False
    published: object = None
    try:
        published = writer.create_or_verify(
            bucket=bucket,
            object_name=object_name,
            content_bytes=content_bytes,
            content_type=QUALITY_PILOT_INVOCATION_CONTENT_TYPE,
            maximum_bytes=MAXIMUM_WINDOW_ENTRY_BYTES,
        )
    except Exception:
        failed = True
    if failed or type(published) is not PublishedStateObject:
        _fail("window entry writer failed")
    if published.object_name != object_name or published.byte_count != len(content_bytes) or published.sha256 != expected_hash:
        _fail("window entry writer result failed verification")
    return published


def load_current_quality_pilot_window_entry(
    *, pilot_run_id: str, market_session: date, window_kind: ScheduledWindowKind, bucket: str, reader: QualityPilotCurrentObjectReader
) -> QualityPilotWindowEntry:
    object_name = canonical_quality_pilot_window_entry_object_name(pilot_run_id, market_session, window_kind)
    bucket = _validate_bucket(bucket)
    read_failed = False
    observed: object = None
    try:
        observed = reader.read_current(bucket=bucket, object_name=object_name, maximum_bytes=MAXIMUM_WINDOW_ENTRY_BYTES)
    except Exception:
        read_failed = True
    if read_failed or type(observed) is not ObservedQualityPilotObject:
        _fail("window entry could not be loaded")
    entry = decode_quality_pilot_window_entry(observed.content_bytes)
    if entry.pilot_run_id != pilot_run_id or entry.market_session != market_session or entry.window_kind is not window_kind:
        _fail("window entry lineage disagrees with the requested route")
    return entry


@dataclass(frozen=True, slots=True)
class LoadedQualityPilotWindowEntry:
    entry: QualityPilotWindowEntry
    request: "PinnedQualityPilotWindowEntryRequest"


def read_pinned_quality_pilot_window_entry(
    request: "PinnedQualityPilotWindowEntryRequest", reader: GCSObjectReader
) -> LoadedQualityPilotWindowEntry:
    """Load and independently reverify one window entry at its exact
    already-known pinned generation. Distinct from
    ``load_current_quality_pilot_window_entry``, which reloads-then-pins an
    UNKNOWN generation -- this is used only when a caller already holds an
    exact ``PinnedQualityPilotWindowEntryRequest`` (e.g. from a completion
    receipt's own ``next_window_entry_pin``)."""

    if type(request) is not PinnedQualityPilotWindowEntryRequest:
        _fail("window entry pin request type is invalid")
    failed = False
    payload: object = None
    try:
        payload = reader.read_generation(
            bucket=request.bucket, object_name=request.object_name,
            generation=request.generation, maximum_bytes=MAXIMUM_WINDOW_ENTRY_BYTES,
        )
    except Exception:
        failed = True
    if failed or type(payload) is not GCSObjectPayload:
        _fail("window entry reader failed")
    if type(payload.generation) is not int or payload.generation != request.generation:
        _fail("window entry reader generation failed verification")
    content_bytes = payload.content_bytes
    if type(content_bytes) is not bytes or not content_bytes or len(content_bytes) > MAXIMUM_WINDOW_ENTRY_BYTES:
        _fail("window entry reader content failed verification")
    if hashlib.sha256(content_bytes).hexdigest() != request.expected_encoded_sha256:
        _fail("window entry reader hash failed verification")
    entry = decode_quality_pilot_window_entry(content_bytes)
    if (
        entry.pilot_run_id != request.pilot_run_id
        or entry.market_session != request.market_session
        or entry.window_kind is not request.window_kind
    ):
        _fail("window entry reader lineage failed verification")
    return LoadedQualityPilotWindowEntry(entry=entry, request=request)


# ---------------------------------------------------------------------------
# QualityPilotActionClaim
# ---------------------------------------------------------------------------


def canonical_quality_pilot_claim_object_name(pilot_run_id: str, action_id: str) -> str:
    if not _is_sha256(pilot_run_id) or not _is_sha256(action_id):
        _fail("action claim route is invalid")
    return f"quality-pilot/v1/{pilot_run_id}/invocations/claims/{action_id}.json"


@dataclass(frozen=True, slots=True)
class QualityPilotActionClaim(_FixedPostureMixin):
    pilot_run_id: str
    action_id: str
    invocation_at: datetime
    code_sha256: str
    environment_sha256: str
    claim_id: str = field(init=False)

    def __post_init__(self) -> None:
        self._validate()
        object.__setattr__(self, "claim_id", self._calculated_id())

    def _validate(self) -> None:
        if not _is_sha256(self.pilot_run_id):
            _fail("action claim pilot run id is invalid")
        if not _is_sha256(self.action_id):
            _fail("action claim action id is invalid")
        if type(self.invocation_at) is not datetime or self.invocation_at.tzinfo is None or self.invocation_at.utcoffset() is None:
            _fail("action claim invocation time is invalid")
        if not _is_sha256(self.code_sha256):
            _fail("action claim code digest is invalid")
        if not _is_sha256(self.environment_sha256):
            _fail("action claim environment digest is invalid")
        _validate_posture(self)

    def _calculated_id(self) -> str:
        failed = False
        calculated = ""
        try:
            calculated = content_id(
                {
                    "schema": QUALITY_PILOT_ACTION_CLAIM_SCHEMA_VERSION,
                    "pilot_run_id": self.pilot_run_id,
                    "action_id": self.action_id,
                    "invocation_at": self.invocation_at,
                    "code_sha256": self.code_sha256,
                    "environment_sha256": self.environment_sha256,
                    "posture": _posture_tree(self),
                },
                length=64,
            )
        except Exception:
            failed = True
        if failed:
            _fail("action claim identity calculation failed")
        return calculated

    def verify_content_identity(self) -> None:
        self._validate()
        if self.claim_id != self._calculated_id():
            _fail("action claim identity failed")


def _claim_tree(value: QualityPilotActionClaim) -> dict[str, object]:
    return {
        "schema_version": QUALITY_PILOT_ACTION_CLAIM_SCHEMA_VERSION,
        "pilot_run_id": value.pilot_run_id,
        "action_id": value.action_id,
        "invocation_at": value.invocation_at.isoformat(),
        "code_sha256": value.code_sha256,
        "environment_sha256": value.environment_sha256,
        "claim_id": value.claim_id,
        "posture": _posture_tree(value),
    }


def encode_quality_pilot_action_claim(claim: QualityPilotActionClaim) -> bytes:
    if type(claim) is not QualityPilotActionClaim:
        _fail("action claim type is invalid")
    failed = False
    try:
        claim.verify_content_identity()
    except Exception:
        failed = True
    if failed:
        _fail("action claim failed independent verification")
    encoded = _canonical_json_bytes(_claim_tree(claim))
    if len(encoded) > MAXIMUM_ACTION_CLAIM_BYTES:
        _fail("action claim encoding exceeds its bounded size")
    return encoded


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


def decode_quality_pilot_action_claim(content_bytes: bytes) -> QualityPilotActionClaim:
    root = _parse_json(content_bytes, MAXIMUM_ACTION_CLAIM_BYTES)
    record = _exact_dict(
        root,
        {"schema_version", "pilot_run_id", "action_id", "invocation_at", "code_sha256", "environment_sha256", "claim_id", "posture"},
        "action claim wire shape is invalid",
    )
    if record["schema_version"] != QUALITY_PILOT_ACTION_CLAIM_SCHEMA_VERSION:
        _fail("action claim wire schema is invalid")
    failed = False
    claim: QualityPilotActionClaim | None = None
    try:
        claim = QualityPilotActionClaim(
            pilot_run_id=_sha256_field(record["pilot_run_id"], "action claim pilot run id is invalid"),
            action_id=_sha256_field(record["action_id"], "action claim action id is invalid"),
            invocation_at=_datetime_text(record["invocation_at"], "action claim invocation time is invalid"),
            code_sha256=_sha256_field(record["code_sha256"], "action claim code digest is invalid"),
            environment_sha256=_sha256_field(record["environment_sha256"], "action claim environment digest is invalid"),
        )
    except QualityPilotInvocationControlPlaneError:
        raise
    except Exception:
        failed = True
    if failed or claim is None:
        _fail("action claim failed reconstruction")
    if (
        claim.claim_id != record["claim_id"]
        or record["posture"] != _posture_tree(claim)
        or encode_quality_pilot_action_claim(claim) != content_bytes
    ):
        _fail("action claim wire identity failed")
    return claim


class QualityPilotClaimWriter(Protocol):
    """Strict create-once writer: exactly one upload attempt with
    ``if_generation_match=0``. A conflict must raise
    :class:`QualityPilotClaimConflictError` distinctly from every other
    failure -- an existing byte-identical claim cannot prove that the
    current process created it, so this port is never idempotent."""

    def claim(
        self, *, bucket: str, object_name: str, content_bytes: bytes, content_type: str, maximum_bytes: int
    ) -> PublishedStateObject: ...


class GoogleCloudStorageQualityPilotClaimWriter:
    """Production QualityPilotClaimWriter backed by google-cloud-storage."""

    def __init__(self, client: object) -> None:
        if client is None:
            _fail("invocation control plane GCS client is required")
        self._client = client

    def claim(
        self, *, bucket: str, object_name: str, content_bytes: bytes, content_type: str, maximum_bytes: int
    ) -> PublishedStateObject:
        bucket = _validate_bucket(bucket)
        if type(object_name) is not str or not object_name:
            _fail("action claim object name is invalid")
        if type(content_type) is not str or not content_type:
            _fail("action claim content type is invalid")
        if type(maximum_bytes) is not int or maximum_bytes <= 0:
            _fail("action claim maximum bytes is invalid")
        if type(content_bytes) is not bytes or not (0 < len(content_bytes) <= maximum_bytes):
            _fail("action claim content is invalid")

        blob: object = None
        construct_failed = False
        try:
            blob = self._client.bucket(bucket).blob(object_name)
        except Exception:
            construct_failed = True
        if construct_failed or blob is None:
            _fail("action claim blob handle could not be constructed")

        conflict = False
        upload_failed = False
        try:
            blob.upload_from_string(content_bytes, content_type=content_type, if_generation_match=0, retry=None)
        except Exception as error:
            if PreconditionFailed is not None and isinstance(error, PreconditionFailed):
                conflict = True
            else:
                upload_failed = True
        if conflict:
            _claim_conflict("action claim already exists at its exact create-once path")
        if upload_failed:
            _fail("action claim writer failed")

        generation = blob.generation
        if type(generation) is bool or type(generation) is not int or generation <= 0:
            _fail("action claim writer returned an invalid generation")
        return PublishedStateObject(
            object_name=object_name,
            generation=generation,
            byte_count=len(content_bytes),
            sha256=hashlib.sha256(content_bytes).hexdigest(),
        )


@dataclass(frozen=True, slots=True)
class PublishedQualityPilotActionClaim(_FixedPostureMixin):
    storage_policy_version: str
    protocol_sha256: str
    pilot_run_id: str
    action_id: str
    claim_id: str
    bucket: str
    object_name: str
    generation: int
    encoded_byte_count: int
    encoded_sha256: str

    def __post_init__(self) -> None:
        if self.storage_policy_version != QUALITY_PILOT_INVOCATION_STORAGE_POLICY_VERSION:
            _fail("published action claim storage policy is invalid")
        if self.protocol_sha256 != PILOT_PROTOCOL_SHA256:
            _fail("published action claim protocol hash is invalid")
        if not _is_sha256(self.pilot_run_id) or not _is_sha256(self.action_id) or not _is_sha256(self.claim_id):
            _fail("published action claim identity is invalid")
        _validate_bucket(self.bucket)
        if self.object_name != canonical_quality_pilot_claim_object_name(self.pilot_run_id, self.action_id):
            _fail("published action claim route is invalid")
        if type(self.generation) is not int or self.generation <= 0:
            _fail("published action claim generation is invalid")
        if type(self.encoded_byte_count) is not int or not (0 < self.encoded_byte_count <= MAXIMUM_ACTION_CLAIM_BYTES):
            _fail("published action claim byte count is invalid")
        if not _is_sha256(self.encoded_sha256):
            _fail("published action claim hash is invalid")


def publish_quality_pilot_action_claim(
    claim: QualityPilotActionClaim, bucket: str, claim_writer: QualityPilotClaimWriter
) -> PublishedQualityPilotActionClaim:
    if type(claim) is not QualityPilotActionClaim:
        _fail("action claim type is invalid")
    bucket = _validate_bucket(bucket)
    failed = False
    try:
        claim.verify_content_identity()
    except Exception:
        failed = True
    if failed:
        _fail("action claim failed independent verification")

    content_bytes = encode_quality_pilot_action_claim(claim)
    object_name = canonical_quality_pilot_claim_object_name(claim.pilot_run_id, claim.action_id)
    expected_hash = hashlib.sha256(content_bytes).hexdigest()

    write_conflict = False
    write_failed = False
    published: object = None
    try:
        published = claim_writer.claim(
            bucket=bucket,
            object_name=object_name,
            content_bytes=content_bytes,
            content_type=QUALITY_PILOT_INVOCATION_CONTENT_TYPE,
            maximum_bytes=MAXIMUM_ACTION_CLAIM_BYTES,
        )
    except QualityPilotClaimConflictError:
        write_conflict = True
    except Exception:
        write_failed = True
    if write_conflict:
        _claim_conflict("action claim already exists")
    if write_failed or type(published) is not PublishedStateObject:
        _fail("action claim writer failed")
    if published.object_name != object_name or published.byte_count != len(content_bytes) or published.sha256 != expected_hash:
        _fail("action claim writer result failed verification")

    return PublishedQualityPilotActionClaim(
        storage_policy_version=QUALITY_PILOT_INVOCATION_STORAGE_POLICY_VERSION,
        protocol_sha256=PILOT_PROTOCOL_SHA256,
        pilot_run_id=claim.pilot_run_id,
        action_id=claim.action_id,
        claim_id=claim.claim_id,
        bucket=bucket,
        object_name=object_name,
        generation=published.generation,
        encoded_byte_count=len(content_bytes),
        encoded_sha256=expected_hash,
    )


@dataclass(frozen=True, slots=True)
class PinnedQualityPilotActionClaimRequest:
    storage_policy_version: str
    protocol_sha256: str
    pilot_run_id: str
    action_id: str
    claim_id: str
    bucket: str
    object_name: str
    generation: int
    expected_encoded_sha256: str

    def __post_init__(self) -> None:
        if self.storage_policy_version != QUALITY_PILOT_INVOCATION_STORAGE_POLICY_VERSION:
            _fail("action claim pin storage policy is invalid")
        if self.protocol_sha256 != PILOT_PROTOCOL_SHA256:
            _fail("action claim pin protocol hash is invalid")
        if not _is_sha256(self.pilot_run_id) or not _is_sha256(self.action_id) or not _is_sha256(self.claim_id):
            _fail("action claim pin identity is invalid")
        _validate_bucket(self.bucket)
        if self.object_name != canonical_quality_pilot_claim_object_name(self.pilot_run_id, self.action_id):
            _fail("action claim pin route is invalid")
        if type(self.generation) is not int or self.generation <= 0:
            _fail("action claim pin generation is invalid")
        if not _is_sha256(self.expected_encoded_sha256):
            _fail("action claim pin hash is invalid")


def _reconstruct_action_claim_pin(value: object) -> PinnedQualityPilotActionClaimRequest:
    if type(value) is not PinnedQualityPilotActionClaimRequest:
        _fail("action claim pin type is invalid")
    failed = False
    reconstructed: PinnedQualityPilotActionClaimRequest | None = None
    try:
        reconstructed = PinnedQualityPilotActionClaimRequest(
            storage_policy_version=value.storage_policy_version,
            protocol_sha256=value.protocol_sha256,
            pilot_run_id=value.pilot_run_id,
            action_id=value.action_id,
            claim_id=value.claim_id,
            bucket=value.bucket,
            object_name=value.object_name,
            generation=value.generation,
            expected_encoded_sha256=value.expected_encoded_sha256,
        )
    except Exception:
        failed = True
    if failed or reconstructed is None:
        _fail("action claim pin could not be independently reverified")
    return reconstructed


def _reconstruct_published_claim(value: object) -> PublishedQualityPilotActionClaim:
    if type(value) is not PublishedQualityPilotActionClaim:
        _fail("published action claim type is invalid")
    failed = False
    reconstructed: PublishedQualityPilotActionClaim | None = None
    try:
        reconstructed = PublishedQualityPilotActionClaim(
            storage_policy_version=value.storage_policy_version,
            protocol_sha256=value.protocol_sha256,
            pilot_run_id=value.pilot_run_id,
            action_id=value.action_id,
            claim_id=value.claim_id,
            bucket=value.bucket,
            object_name=value.object_name,
            generation=value.generation,
            encoded_byte_count=value.encoded_byte_count,
            encoded_sha256=value.encoded_sha256,
        )
    except Exception:
        failed = True
    if failed or reconstructed is None:
        _fail("published action claim could not be independently reverified")
    return reconstructed


def pinned_quality_pilot_action_claim_request(
    published: PublishedQualityPilotActionClaim,
) -> PinnedQualityPilotActionClaimRequest:
    reconstructed = _reconstruct_published_claim(published)
    return PinnedQualityPilotActionClaimRequest(
        storage_policy_version=reconstructed.storage_policy_version,
        protocol_sha256=reconstructed.protocol_sha256,
        pilot_run_id=reconstructed.pilot_run_id,
        action_id=reconstructed.action_id,
        claim_id=reconstructed.claim_id,
        bucket=reconstructed.bucket,
        object_name=reconstructed.object_name,
        generation=reconstructed.generation,
        expected_encoded_sha256=reconstructed.encoded_sha256,
    )


@dataclass(frozen=True, slots=True)
class LoadedQualityPilotActionClaim:
    claim: QualityPilotActionClaim
    request: PinnedQualityPilotActionClaimRequest


def read_pinned_quality_pilot_action_claim(
    request: PinnedQualityPilotActionClaimRequest, reader: GCSObjectReader
) -> LoadedQualityPilotActionClaim:
    if type(request) is not PinnedQualityPilotActionClaimRequest:
        _fail("action claim pin request type is invalid")
    failed = False
    payload: object = None
    try:
        payload = reader.read_generation(
            bucket=request.bucket, object_name=request.object_name,
            generation=request.generation, maximum_bytes=MAXIMUM_ACTION_CLAIM_BYTES,
        )
    except Exception:
        failed = True
    if failed or type(payload) is not GCSObjectPayload:
        _fail("action claim reader failed")
    if type(payload.generation) is not int or payload.generation != request.generation:
        _fail("action claim reader generation failed verification")
    content_bytes = payload.content_bytes
    if type(content_bytes) is not bytes or not content_bytes or len(content_bytes) > MAXIMUM_ACTION_CLAIM_BYTES:
        _fail("action claim reader content failed verification")
    if hashlib.sha256(content_bytes).hexdigest() != request.expected_encoded_sha256:
        _fail("action claim reader hash failed verification")
    claim = decode_quality_pilot_action_claim(content_bytes)
    if (
        claim.pilot_run_id != request.pilot_run_id
        or claim.action_id != request.action_id
        or claim.claim_id != request.claim_id
    ):
        _fail("action claim reader lineage failed verification")
    return LoadedQualityPilotActionClaim(claim=claim, request=request)


# ---------------------------------------------------------------------------
# PinnedQualityPilotWindowEntryRequest (verification-only pin; a completion
# receipt binds this to prove exactly which window entry it published when
# crossing into a new window -- it is never read back to route a load).
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class PinnedQualityPilotWindowEntryRequest:
    storage_policy_version: str
    protocol_sha256: str
    pilot_run_id: str
    market_session: date
    window_kind: ScheduledWindowKind
    bucket: str
    object_name: str
    generation: int
    expected_encoded_sha256: str

    def __post_init__(self) -> None:
        if self.storage_policy_version != QUALITY_PILOT_INVOCATION_STORAGE_POLICY_VERSION:
            _fail("window entry pin storage policy is invalid")
        if self.protocol_sha256 != PILOT_PROTOCOL_SHA256:
            _fail("window entry pin protocol hash is invalid")
        if not _is_sha256(self.pilot_run_id):
            _fail("window entry pin pilot run id is invalid")
        if type(self.market_session) is not date:
            _fail("window entry pin session is invalid")
        if type(self.window_kind) is not ScheduledWindowKind:
            _fail("window entry pin kind is invalid")
        _validate_bucket(self.bucket)
        if self.object_name != canonical_quality_pilot_window_entry_object_name(
            self.pilot_run_id, self.market_session, self.window_kind
        ):
            _fail("window entry pin route is invalid")
        if type(self.generation) is not int or self.generation <= 0:
            _fail("window entry pin generation is invalid")
        if not _is_sha256(self.expected_encoded_sha256):
            _fail("window entry pin hash is invalid")


def pinned_quality_pilot_window_entry_request(
    entry: QualityPilotWindowEntry, bucket: str, published: PublishedStateObject
) -> PinnedQualityPilotWindowEntryRequest:
    if type(entry) is not QualityPilotWindowEntry:
        _fail("window entry type is invalid")
    failed = False
    try:
        entry.verify_content_identity()
    except Exception:
        failed = True
    if failed:
        _fail("window entry failed independent verification")
    bucket = _validate_bucket(bucket)
    if type(published) is not PublishedStateObject:
        _fail("published window entry type is invalid")
    object_name = canonical_quality_pilot_window_entry_object_name(entry.pilot_run_id, entry.market_session, entry.window_kind)
    if published.object_name != object_name:
        _fail("published window entry route disagrees with the window entry")
    return PinnedQualityPilotWindowEntryRequest(
        storage_policy_version=QUALITY_PILOT_INVOCATION_STORAGE_POLICY_VERSION,
        protocol_sha256=PILOT_PROTOCOL_SHA256,
        pilot_run_id=entry.pilot_run_id,
        market_session=entry.market_session,
        window_kind=entry.window_kind,
        bucket=bucket,
        object_name=object_name,
        generation=published.generation,
        expected_encoded_sha256=published.sha256,
    )


def _window_entry_pin_tree(value: PinnedQualityPilotWindowEntryRequest | None) -> dict[str, object] | None:
    if value is None:
        return None
    return {
        "storage_policy_version": value.storage_policy_version,
        "protocol_sha256": value.protocol_sha256,
        "pilot_run_id": value.pilot_run_id,
        "market_session": value.market_session.isoformat(),
        "window_kind": value.window_kind.value,
        "bucket": value.bucket,
        "object_name": value.object_name,
        "generation": value.generation,
        "expected_encoded_sha256": value.expected_encoded_sha256,
    }


def _decode_window_entry_pin(value: object) -> PinnedQualityPilotWindowEntryRequest | None:
    if value is None:
        return None
    record = _exact_dict(
        value,
        {
            "storage_policy_version", "protocol_sha256", "pilot_run_id", "market_session",
            "window_kind", "bucket", "object_name", "generation", "expected_encoded_sha256",
        },
        "window entry pin record has an invalid shape",
    )
    failed = False
    pin: PinnedQualityPilotWindowEntryRequest | None = None
    try:
        pin = PinnedQualityPilotWindowEntryRequest(
            storage_policy_version=_text(record["storage_policy_version"], "window entry pin policy is invalid"),
            protocol_sha256=_text(record["protocol_sha256"], "window entry pin protocol is invalid"),
            pilot_run_id=_sha256_field(record["pilot_run_id"], "window entry pin pilot run id is invalid"),
            market_session=_date_text(record["market_session"], "window entry pin session is invalid"),
            window_kind=_enum(record["window_kind"], ScheduledWindowKind, "window entry pin kind is invalid"),
            bucket=_validate_bucket(record["bucket"]),
            object_name=_text(record["object_name"], "window entry pin object name is invalid"),
            generation=_integer(record["generation"], "window entry pin generation is invalid", minimum=1),
            expected_encoded_sha256=_sha256_field(record["expected_encoded_sha256"], "window entry pin hash is invalid"),
        )
    except QualityPilotInvocationControlPlaneError:
        raise
    except Exception:
        failed = True
    if failed or pin is None:
        _fail("window entry pin record failed reconstruction")
    return pin


# ---------------------------------------------------------------------------
# QualityPilotCompletionReceipt
# ---------------------------------------------------------------------------


def canonical_quality_pilot_completion_object_name(pilot_run_id: str, action_id: str) -> str:
    if not _is_sha256(pilot_run_id) or not _is_sha256(action_id):
        _fail("completion receipt route is invalid")
    return f"quality-pilot/v1/{pilot_run_id}/invocations/completions/{action_id}.json"


@dataclass(frozen=True, slots=True)
class QualityPilotCompletionReceipt(_FixedPostureMixin):
    """The terminal success boundary for exactly one sealed action.

    ``successor_action_binding_pin`` is ``None`` only at exact campaign
    completion (the final capture spec of the final confirmed session).
    ``next_window_entry_pin`` is present only when this completion crosses
    into a different window than the one it sealed; a same-window
    continuation carries a successor binding pin but no new window entry.
    """

    pilot_run_id: str
    action_id: str
    action_kind: QualityPilotActionKind
    claim_pin: PinnedQualityPilotActionClaimRequest
    outcome_plan_pin: PinnedQualityPilotControlArtifactRequest
    outcome_transition_pin: PinnedQualityPilotLedgerTransitionRequest
    outcome_snapshot_pin: PinnedQualityPilotControlArtifactRequest
    successor_action_binding_pin: PinnedQualityPilotActionBindingRequest | None
    next_window_entry_pin: PinnedQualityPilotWindowEntryRequest | None
    campaign_complete: bool
    final_transition_id: str = field(init=False)
    receipt_id: str = field(init=False)

    def __post_init__(self) -> None:
        self._validate()
        object.__setattr__(self, "final_transition_id", self.outcome_transition_pin.transition_id)
        object.__setattr__(self, "receipt_id", self._calculated_id())

    def _validate(self) -> None:
        if not _is_sha256(self.pilot_run_id):
            _fail("completion receipt pilot run id is invalid")
        if not _is_sha256(self.action_id):
            _fail("completion receipt action id is invalid")
        if type(self.action_kind) is not QualityPilotActionKind:
            _fail("completion receipt action kind is invalid")

        claim_pin = _reconstruct_action_claim_pin(self.claim_pin)
        if claim_pin.pilot_run_id != self.pilot_run_id or claim_pin.action_id != self.action_id:
            _fail("completion receipt claim pin disagrees with its own action")

        plan_pin = _reconstruct_plan_pin(self.outcome_plan_pin)
        if plan_pin.kind is not QualityPilotControlArtifactKind.CAMPAIGN_PLAN:
            _fail("completion receipt outcome plan pin kind is invalid")
        if plan_pin.pilot_run_id != self.pilot_run_id:
            _fail("completion receipt outcome plan pin disagrees on pilot run id")

        transition_pin = _reconstruct_transition_pin(self.outcome_transition_pin)
        if transition_pin.pilot_run_id != self.pilot_run_id:
            _fail("completion receipt outcome transition pin disagrees on pilot run id")
        if transition_pin.plan_id != plan_pin.artifact_id:
            _fail("completion receipt outcome transition pin disagrees on plan lineage")

        snapshot_pin = _reconstruct_plan_pin(self.outcome_snapshot_pin)
        if snapshot_pin.kind is not QualityPilotControlArtifactKind.COMPLETENESS_LEDGER:
            _fail("completion receipt outcome snapshot pin kind is invalid")
        if snapshot_pin.pilot_run_id != self.pilot_run_id:
            _fail("completion receipt outcome snapshot pin disagrees on pilot run id")

        if type(self.campaign_complete) is not bool:
            _fail("completion receipt campaign-complete flag is invalid")
        if self.campaign_complete and self.successor_action_binding_pin is not None:
            _fail("completion receipt at campaign completion must not carry a successor")
        if not self.campaign_complete and self.successor_action_binding_pin is None:
            _fail("completion receipt short of campaign completion requires a successor")
        if self.successor_action_binding_pin is not None:
            successor_pin = _reconstruct_action_binding_pin(self.successor_action_binding_pin)
            if successor_pin.pilot_run_id != self.pilot_run_id:
                _fail("completion receipt successor disagrees on pilot run id")
        if self.next_window_entry_pin is not None:
            if self.successor_action_binding_pin is None:
                _fail("completion receipt cannot cross into a new window without a successor")
            pin = _reconstruct_window_entry_pin(self.next_window_entry_pin)
            if pin.pilot_run_id != self.pilot_run_id:
                _fail("completion receipt next window entry disagrees on pilot run id")
        _validate_posture(self)

    def _calculated_id(self) -> str:
        failed = False
        calculated = ""
        try:
            calculated = content_id(
                {
                    "schema": QUALITY_PILOT_COMPLETION_RECEIPT_SCHEMA_VERSION,
                    "pilot_run_id": self.pilot_run_id,
                    "action_id": self.action_id,
                    "action_kind": self.action_kind.value,
                    "claim_pin": self.claim_pin,
                    "outcome_plan_pin": self.outcome_plan_pin,
                    "outcome_transition_pin": self.outcome_transition_pin,
                    "outcome_snapshot_pin": self.outcome_snapshot_pin,
                    "successor_action_binding_pin": self.successor_action_binding_pin,
                    "next_window_entry_pin": self.next_window_entry_pin,
                    "campaign_complete": self.campaign_complete,
                    "final_transition_id": self.final_transition_id,
                    "posture": _posture_tree(self),
                },
                length=64,
            )
        except Exception:
            failed = True
        if failed:
            _fail("completion receipt identity calculation failed")
        return calculated

    def verify_content_identity(self) -> None:
        self._validate()
        if (
            self.final_transition_id != self.outcome_transition_pin.transition_id
            or self.receipt_id != self._calculated_id()
        ):
            _fail("completion receipt identity failed")


def _reconstruct_window_entry_pin(value: object) -> PinnedQualityPilotWindowEntryRequest:
    if type(value) is not PinnedQualityPilotWindowEntryRequest:
        _fail("window entry pin type is invalid")
    failed = False
    reconstructed: PinnedQualityPilotWindowEntryRequest | None = None
    try:
        reconstructed = PinnedQualityPilotWindowEntryRequest(
            storage_policy_version=value.storage_policy_version,
            protocol_sha256=value.protocol_sha256,
            pilot_run_id=value.pilot_run_id,
            market_session=value.market_session,
            window_kind=value.window_kind,
            bucket=value.bucket,
            object_name=value.object_name,
            generation=value.generation,
            expected_encoded_sha256=value.expected_encoded_sha256,
        )
    except Exception:
        failed = True
    if failed or reconstructed is None:
        _fail("window entry pin could not be independently reverified")
    return reconstructed


def _action_claim_pin_tree(value: PinnedQualityPilotActionClaimRequest) -> dict[str, object]:
    return {
        "storage_policy_version": value.storage_policy_version,
        "protocol_sha256": value.protocol_sha256,
        "pilot_run_id": value.pilot_run_id,
        "action_id": value.action_id,
        "claim_id": value.claim_id,
        "bucket": value.bucket,
        "object_name": value.object_name,
        "generation": value.generation,
        "expected_encoded_sha256": value.expected_encoded_sha256,
    }


def _decode_action_claim_pin(value: object) -> PinnedQualityPilotActionClaimRequest:
    record = _exact_dict(
        value,
        {
            "storage_policy_version", "protocol_sha256", "pilot_run_id", "action_id",
            "claim_id", "bucket", "object_name", "generation", "expected_encoded_sha256",
        },
        "action claim pin record has an invalid shape",
    )
    failed = False
    pin: PinnedQualityPilotActionClaimRequest | None = None
    try:
        pin = PinnedQualityPilotActionClaimRequest(
            storage_policy_version=_text(record["storage_policy_version"], "action claim pin policy is invalid"),
            protocol_sha256=_text(record["protocol_sha256"], "action claim pin protocol is invalid"),
            pilot_run_id=_sha256_field(record["pilot_run_id"], "action claim pin pilot run id is invalid"),
            action_id=_sha256_field(record["action_id"], "action claim pin action id is invalid"),
            claim_id=_sha256_field(record["claim_id"], "action claim pin claim id is invalid"),
            bucket=_validate_bucket(record["bucket"]),
            object_name=_text(record["object_name"], "action claim pin object name is invalid"),
            generation=_integer(record["generation"], "action claim pin generation is invalid", minimum=1),
            expected_encoded_sha256=_sha256_field(record["expected_encoded_sha256"], "action claim pin hash is invalid"),
        )
    except QualityPilotInvocationControlPlaneError:
        raise
    except Exception:
        failed = True
    if failed or pin is None:
        _fail("action claim pin record failed reconstruction")
    return pin


def _reconstruct_completion_receipt(value: object) -> QualityPilotCompletionReceipt:
    if type(value) is not QualityPilotCompletionReceipt:
        _fail("completion receipt type is invalid")
    failed = False
    reconstructed: QualityPilotCompletionReceipt | None = None
    try:
        reconstructed = QualityPilotCompletionReceipt(
            pilot_run_id=value.pilot_run_id,
            action_id=value.action_id,
            action_kind=value.action_kind,
            claim_pin=value.claim_pin,
            outcome_plan_pin=value.outcome_plan_pin,
            outcome_transition_pin=value.outcome_transition_pin,
            outcome_snapshot_pin=value.outcome_snapshot_pin,
            successor_action_binding_pin=value.successor_action_binding_pin,
            next_window_entry_pin=value.next_window_entry_pin,
            campaign_complete=value.campaign_complete,
        )
    except Exception:
        failed = True
    if failed or reconstructed is None:
        _fail("completion receipt could not be independently reverified")
    if value.receipt_id != reconstructed.receipt_id:
        _fail("completion receipt identity failed independent reverification")
    return reconstructed


def _completion_receipt_tree(value: QualityPilotCompletionReceipt) -> dict[str, object]:
    return {
        "schema_version": QUALITY_PILOT_COMPLETION_RECEIPT_SCHEMA_VERSION,
        "pilot_run_id": value.pilot_run_id,
        "action_id": value.action_id,
        "action_kind": value.action_kind.value,
        "claim_pin": _action_claim_pin_tree(value.claim_pin),
        "outcome_plan_pin": _plan_pin_tree(value.outcome_plan_pin),
        "outcome_transition_pin": _transition_pin_tree(value.outcome_transition_pin),
        "outcome_snapshot_pin": _plan_pin_tree(value.outcome_snapshot_pin),
        "successor_action_binding_pin": _action_binding_pin_tree(value.successor_action_binding_pin),
        "next_window_entry_pin": _window_entry_pin_tree(value.next_window_entry_pin),
        "campaign_complete": value.campaign_complete,
        "final_transition_id": value.final_transition_id,
        "receipt_id": value.receipt_id,
        "posture": _posture_tree(value),
    }


def encode_quality_pilot_completion_receipt(receipt: QualityPilotCompletionReceipt) -> bytes:
    if type(receipt) is not QualityPilotCompletionReceipt:
        _fail("completion receipt type is invalid")
    failed = False
    try:
        receipt.verify_content_identity()
    except Exception:
        failed = True
    if failed:
        _fail("completion receipt failed independent verification")
    encoded = _canonical_json_bytes(_completion_receipt_tree(receipt))
    if len(encoded) > MAXIMUM_COMPLETION_RECEIPT_BYTES:
        _fail("completion receipt encoding exceeds its bounded size")
    return encoded


def decode_quality_pilot_completion_receipt(content_bytes: bytes) -> QualityPilotCompletionReceipt:
    root = _parse_json(content_bytes, MAXIMUM_COMPLETION_RECEIPT_BYTES)
    record = _exact_dict(
        root,
        {
            "schema_version", "pilot_run_id", "action_id", "action_kind", "claim_pin",
            "outcome_plan_pin", "outcome_transition_pin", "outcome_snapshot_pin",
            "successor_action_binding_pin", "next_window_entry_pin",
            "campaign_complete", "final_transition_id", "receipt_id", "posture",
        },
        "completion receipt wire shape is invalid",
    )
    if record["schema_version"] != QUALITY_PILOT_COMPLETION_RECEIPT_SCHEMA_VERSION:
        _fail("completion receipt wire schema is invalid")
    campaign_complete = record["campaign_complete"]
    if type(campaign_complete) is not bool:
        _fail("completion receipt campaign-complete flag is invalid")
    failed = False
    receipt: QualityPilotCompletionReceipt | None = None
    try:
        receipt = QualityPilotCompletionReceipt(
            pilot_run_id=_sha256_field(record["pilot_run_id"], "completion receipt pilot run id is invalid"),
            action_id=_sha256_field(record["action_id"], "completion receipt action id is invalid"),
            action_kind=_enum(record["action_kind"], QualityPilotActionKind, "completion receipt action kind is invalid"),
            claim_pin=_decode_action_claim_pin(record["claim_pin"]),
            outcome_plan_pin=_decode_plan_pin(record["outcome_plan_pin"]),
            outcome_transition_pin=_decode_transition_pin(record["outcome_transition_pin"]),
            outcome_snapshot_pin=_decode_plan_pin(record["outcome_snapshot_pin"]),
            successor_action_binding_pin=_decode_action_binding_pin(record["successor_action_binding_pin"]),
            next_window_entry_pin=_decode_window_entry_pin(record["next_window_entry_pin"]),
            campaign_complete=campaign_complete,
        )
    except QualityPilotInvocationControlPlaneError:
        raise
    except Exception:
        failed = True
    if failed or receipt is None:
        _fail("completion receipt failed reconstruction")
    if (
        receipt.receipt_id != record["receipt_id"]
        or receipt.final_transition_id != record["final_transition_id"]
        or record["posture"] != _posture_tree(receipt)
        or encode_quality_pilot_completion_receipt(receipt) != content_bytes
    ):
        _fail("completion receipt wire identity failed")
    return receipt


def publish_quality_pilot_completion_receipt(
    receipt: QualityPilotCompletionReceipt, bucket: str, writer: StateObjectWriter
) -> PublishedStateObject:
    if type(receipt) is not QualityPilotCompletionReceipt:
        _fail("completion receipt type is invalid")
    bucket = _validate_bucket(bucket)
    content_bytes = encode_quality_pilot_completion_receipt(receipt)
    object_name = canonical_quality_pilot_completion_object_name(receipt.pilot_run_id, receipt.action_id)
    expected_hash = hashlib.sha256(content_bytes).hexdigest()
    failed = False
    published: object = None
    try:
        published = writer.create_or_verify(
            bucket=bucket,
            object_name=object_name,
            content_bytes=content_bytes,
            content_type=QUALITY_PILOT_INVOCATION_CONTENT_TYPE,
            maximum_bytes=MAXIMUM_COMPLETION_RECEIPT_BYTES,
        )
    except Exception:
        failed = True
    if failed or type(published) is not PublishedStateObject:
        _fail("completion receipt writer failed")
    if published.object_name != object_name or published.byte_count != len(content_bytes) or published.sha256 != expected_hash:
        _fail("completion receipt writer result failed verification")
    return published


def load_optional_quality_pilot_completion_receipt(
    *, pilot_run_id: str, action_id: str, bucket: str, reader: QualityPilotCurrentObjectReader
) -> QualityPilotCompletionReceipt | None:
    """Load and independently reverify the terminal completion receipt for
    one exact action, or ``None`` only when it is proven not to exist yet."""

    object_name = canonical_quality_pilot_completion_object_name(pilot_run_id, action_id)
    bucket = _validate_bucket(bucket)
    read_failed = False
    observed: object = None
    try:
        observed = reader.read_current_optional(bucket=bucket, object_name=object_name, maximum_bytes=MAXIMUM_COMPLETION_RECEIPT_BYTES)
    except Exception:
        read_failed = True
    if read_failed:
        _fail("completion receipt could not be loaded")
    if observed is None:
        return None
    if type(observed) is not ObservedQualityPilotObject:
        _fail("completion receipt reader returned an invalid result")
    receipt = decode_quality_pilot_completion_receipt(observed.content_bytes)
    if receipt.pilot_run_id != pilot_run_id or receipt.action_id != action_id:
        _fail("completion receipt lineage disagrees with the requested route")
    return receipt
