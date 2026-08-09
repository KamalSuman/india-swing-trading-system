"""HYP-002 quality pilot: pure deterministic arming/deployment control core.

This module is the offline/operator-facing layer that sits above the
already-accepted ``QualityPilotInvocationRunbook``/``QualityPilotWindowService``
composition. It never reproduces capture, canonicalization, lineage,
classification, campaign-ledger, position, signal, or trading logic -- it
only compiles, arms, and schedules the exact same runbook those modules
already own.

Everything here is a pure function/value: no environment variable, filesystem
path, wall clock, network call, SDK, subprocess, GCP client, credential,
notification, paper trade, strategy, signal, order, or capital capability is
imported or reachable from this module. ``observed_at`` and every timestamp
in a compiled runbook are always caller-supplied aware datetimes; this module
never reads the wall clock or a local/system calendar.

Four pieces live here:

- ``QualityPilotRunbookDraft`` plus its own strict canonical-JSON codec: a
  caller-supplied draft that compiles byte-identically into the accepted
  ``QualityPilotInvocationRunbook`` via ``compile_quality_pilot_invocation_runbook``.
  The existing ``QualityPilotCampaignSpec``/``ObservationWindowSpec``/
  ``QualityPilotInvocationRunbook`` constructors and
  ``is_window_inside_authorized_schedule`` gate remain the sole identity and
  schedule authorities; this module never infers a session, holiday,
  calendar decision, window timestamp, provider version, or bucket.
- ``QualityPilotArmingManifest``: an immutable preparation record binding a
  compiled runbook to a digest-pinned deployment identity, four fixed
  scheduler lanes, and secret *references* only. It never contains a secret
  value and never claims ``armed=true`` -- it is preparation evidence, not
  deployment authority.
- ``select_due_quality_pilot_window``/``assess_quality_pilot_window_posture``:
  pure clock-driven window selection and a fail-closed missed-window
  posture, so a scheduler-fired invocation can decide whether to run, no-op,
  or block before it ever reads a credential. ``observed_at`` must be an
  exact aware UTC datetime (zero offset) -- an equivalent non-UTC
  representation fails closed, so posture has one canonical time basis.
- ``quality_pilot_window_completion_probe_targets``: tells a caller with I/O
  capability (``quality_pilot_job.py``) the exact canonical ordered prefix
  of windows whose completion evidence to independently verify -- every
  window up to and including a currently-DUE window, or every window whose
  ``closes_at`` has passed otherwise -- before calling
  ``assess_quality_pilot_window_posture`` with a matching
  ``QualityPilotOrderedCompletionProof`` binding one
  ``QualityPilotWindowCompletionEvidence`` to each named window in the
  identical order. A DUE posture therefore proves every predecessor window
  is independently COMPLETE, not merely the current one; this module never
  performs that read itself.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from enum import Enum

from india_swing.identity import content_id

from .campaign_ledger import is_window_inside_authorized_schedule
from .canonical_response import (
    PILOT_PROTOCOL_SHA256,
    EndpointFamily,
    ObservationWindowSpec,
    ScheduledWindowKind,
)
from .capture_runner import CONFIRMED_SESSION_COUNT, QualityPilotCampaignSpec
from .invocation_control_plane import (
    QualityPilotInvocationRunbook,
    decode_quality_pilot_invocation_runbook,
    encode_quality_pilot_invocation_runbook,
)

QUALITY_PILOT_ARMING_DRAFT_SCHEMA_VERSION = "quality_pilot_runbook_draft_v1"
QUALITY_PILOT_ARMING_MANIFEST_SCHEMA_VERSION = "quality_pilot_arming_manifest_v1"
MAXIMUM_DRAFT_BYTES = 1 * 1024 * 1024
MAXIMUM_MANIFEST_BYTES = 64 * 1024

_SHA256_PATTERN = re.compile(r"[0-9a-f]{64}\Z")
_BUCKET_PATTERN = re.compile(r"[a-z0-9][a-z0-9\-_.]{1,61}[a-z0-9]\Z")
_PROJECT_PATTERN = re.compile(r"[a-z][a-z0-9\-]{4,28}[a-z0-9]\Z")
_REGION_PATTERN = re.compile(r"[a-z]+-[a-z]+[0-9]\Z")
_JOB_PATTERN = re.compile(r"[a-z][a-z0-9\-]{0,61}[a-z0-9]\Z")
_SERVICE_ACCOUNT_PATTERN = re.compile(
    r"[a-z][a-z0-9\-]{0,61}[a-z0-9]@[a-z][a-z0-9\-]{4,28}[a-z0-9]\.iam\.gserviceaccount\.com\Z"
)
_IMAGE_DIGEST_PATTERN = re.compile(r"[^\s@]+@sha256:[0-9a-f]{64}\Z")
_SECRET_ID_PATTERN = re.compile(r"[a-zA-Z][a-zA-Z0-9\-_]{0,254}\Z")
_SECRET_VERSION_PATTERN = re.compile(r"[1-9][0-9]{0,9}\Z")

# Matches canonical_response.py's own private ``_IST`` constant exactly --
# used only to normalize an already caller-pinned aware datetime onto its
# IST wall clock for a cron-safety probe; never to derive or widen a
# schedule gate time of its own.
_IST = timezone(timedelta(hours=5, minutes=30))

_WINDOW_KIND_ORDER = (
    ScheduledWindowKind.CATALOG_PREOPEN,
    ScheduledWindowKind.QUOTE_0920,
    ScheduledWindowKind.QUOTE_CLOSE,
    ScheduledWindowKind.OHLCV_CLOSE,
)
_WINDOW_KIND_ENDPOINT_FAMILY = {
    ScheduledWindowKind.CATALOG_PREOPEN: EndpointFamily.CATALOG,
    ScheduledWindowKind.QUOTE_0920: EndpointFamily.FULL_QUOTE,
    ScheduledWindowKind.QUOTE_CLOSE: EndpointFamily.FULL_QUOTE,
    ScheduledWindowKind.OHLCV_CLOSE: EndpointFamily.DAILY_OHLCV,
}
_EXPECTED_WINDOW_COUNT = CONFIRMED_SESSION_COUNT * len(_WINDOW_KIND_ORDER)


class QualityPilotArmingError(ValueError):
    """A draft, manifest, schedule, or posture input failed a fixed check."""


def _fail(message: str) -> None:
    raise QualityPilotArmingError(message)


def _is_sha256(value: object) -> bool:
    return type(value) is str and _SHA256_PATTERN.fullmatch(value) is not None


def _reject_float(_text: str) -> None:
    _fail("arming input rejects float literals")


def _reject_constant(_text: str) -> None:
    _fail("arming input rejects NaN/Infinity literals")


def _reject_duplicate_pairs(pairs: list) -> dict:
    result: dict = {}
    for key, value in pairs:
        if key in result:
            _fail("arming input contains a duplicate key")
        result[key] = value
    return result


def _canonical_json_bytes(tree: object) -> bytes:
    failed = False
    encoded = b""
    try:
        encoded = (
            json.dumps(tree, ensure_ascii=False, allow_nan=False, separators=(",", ":"), sort_keys=True) + "\n"
        ).encode("utf-8")
    except Exception:
        failed = True
    if failed:
        _fail("arming input could not be canonically encoded")
    return encoded


def _parse_json(content_bytes: object, maximum_bytes: int) -> dict:
    if type(content_bytes) is not bytes:
        _fail("arming input content must be exact bytes")
    if not content_bytes or len(content_bytes) > maximum_bytes:
        _fail("arming input content size is invalid")
    decode_failed = False
    text = ""
    try:
        text = content_bytes.decode("utf-8", errors="strict")
    except Exception:
        decode_failed = True
    if decode_failed:
        _fail("arming input content is not strict UTF-8")
    parse_failed = False
    value: object = None
    try:
        value = json.loads(
            text, object_pairs_hook=_reject_duplicate_pairs, parse_float=_reject_float, parse_constant=_reject_constant
        )
    except QualityPilotArmingError:
        raise
    except Exception:
        parse_failed = True
    if parse_failed:
        _fail("arming input content is not valid JSON")
    if type(value) is not dict:
        _fail("arming input root must be an exact object")
    return value


def _exact_dict(value: object, keys: set, message: str) -> dict:
    if type(value) is not dict or set(value) != keys:
        _fail(message)
    return value


def _exact_list(value: object, message: str) -> list:
    if type(value) is not list:
        _fail(message)
    return value


def _text(value: object, message: str) -> str:
    if type(value) is not str or not value:
        _fail(message)
    return value


def _integer(value: object, message: str, *, minimum: int) -> int:
    if type(value) is bool or type(value) is not int or value < minimum:
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
    if failed or parsed is None or parsed.isoformat() != value:
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
    if failed or parsed is None or offset is None or parsed.isoformat() != value:
        _fail(message)
    return parsed


# ---------------------------------------------------------------------------
# QualityPilotRunbookDraft
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class QualityPilotRunbookDraft:
    """One strict, fully caller-supplied set of primitives that compiles
    byte-identically into a ``QualityPilotInvocationRunbook``. Carries
    exactly 20 strictly increasing confirmed sessions, 20 calendar decision
    ids, and 80 caller-supplied ``(opens_at, closes_at)`` timestamp pairs in
    canonical session-then-kind order -- one pair per one of the campaign's
    80 scheduled windows. Never infers a session, holiday, calendar
    decision, window timestamp, provider version, or bucket."""

    pilot_run_id: str
    protocol_sha256: str
    confirmed_sessions: tuple[date, ...]
    calendar_decision_ids: tuple[str, ...]
    provider_version: str
    bucket: str
    window_timestamps: tuple[tuple[datetime, datetime], ...]

    def __post_init__(self) -> None:
        if not _is_sha256(self.pilot_run_id):
            _fail("runbook draft pilot run id is invalid")
        if self.protocol_sha256 != PILOT_PROTOCOL_SHA256:
            _fail("runbook draft protocol hash is invalid")
        if (
            type(self.confirmed_sessions) is not tuple
            or len(self.confirmed_sessions) != CONFIRMED_SESSION_COUNT
            or any(type(value) is not date for value in self.confirmed_sessions)
        ):
            _fail(f"runbook draft must carry exactly {CONFIRMED_SESSION_COUNT} confirmed sessions")
        if (
            type(self.calendar_decision_ids) is not tuple
            or len(self.calendar_decision_ids) != CONFIRMED_SESSION_COUNT
            or any(not _is_sha256(value) for value in self.calendar_decision_ids)
        ):
            _fail(f"runbook draft must carry exactly {CONFIRMED_SESSION_COUNT} calendar decision ids")
        if type(self.provider_version) is not str or not (0 < len(self.provider_version) <= 128):
            _fail("runbook draft provider version is invalid")
        if type(self.bucket) is not str or _BUCKET_PATTERN.fullmatch(self.bucket) is None:
            _fail("runbook draft bucket is invalid")
        if type(self.window_timestamps) is not tuple or len(self.window_timestamps) != _EXPECTED_WINDOW_COUNT:
            _fail(f"runbook draft must carry exactly {_EXPECTED_WINDOW_COUNT} window timestamp pairs")
        for pair in self.window_timestamps:
            if (
                type(pair) is not tuple
                or len(pair) != 2
                or type(pair[0]) is not datetime
                or type(pair[1]) is not datetime
                or pair[0].utcoffset() is None
                or pair[1].utcoffset() is None
            ):
                _fail("runbook draft window timestamp pair is invalid")


def compile_quality_pilot_invocation_runbook(
    draft: QualityPilotRunbookDraft,
) -> tuple[QualityPilotInvocationRunbook, bytes]:
    """Compile one draft into an accepted ``QualityPilotInvocationRunbook``.

    Constructs ``QualityPilotCampaignSpec``, ``ObservationWindowSpec``, and
    ``QualityPilotInvocationRunbook`` from the draft's exact caller-supplied
    fields only -- their own identities and
    ``is_window_inside_authorized_schedule`` gate remain the sole
    authorities. The compiled output is independently re-encoded/decoded and
    compared byte-for-byte before returning; any disagreement, or any
    malformed nested value, fails closed with one static sanitized error.
    Returns ``(runbook, canonical_encoded_bytes)``.
    """

    if type(draft) is not QualityPilotRunbookDraft:
        _fail("runbook draft type is invalid")

    build_failed = False
    runbook: QualityPilotInvocationRunbook | None = None
    encoded: bytes = b""
    try:
        campaign = QualityPilotCampaignSpec(
            pilot_run_id=draft.pilot_run_id,
            protocol_sha256=draft.protocol_sha256,
            confirmed_sessions=draft.confirmed_sessions,
            calendar_decision_ids=draft.calendar_decision_ids,
        )
        windows: list[ObservationWindowSpec] = []
        for session_index, session in enumerate(draft.confirmed_sessions):
            for kind_index, kind in enumerate(_WINDOW_KIND_ORDER):
                slot = session_index * len(_WINDOW_KIND_ORDER) + kind_index
                opens_at, closes_at = draft.window_timestamps[slot]
                windows.append(
                    ObservationWindowSpec(
                        pilot_run_id=draft.pilot_run_id,
                        market_session=session,
                        window_kind=kind,
                        endpoint_family=_WINDOW_KIND_ENDPOINT_FAMILY[kind],
                        opens_at=opens_at,
                        closes_at=closes_at,
                        protocol_sha256=draft.protocol_sha256,
                    )
                )
        runbook = QualityPilotInvocationRunbook(
            campaign=campaign, provider_version=draft.provider_version, bucket=draft.bucket, windows=tuple(windows),
        )
        encoded = encode_quality_pilot_invocation_runbook(runbook)
        reloaded = decode_quality_pilot_invocation_runbook(encoded)
        if reloaded.runbook_id != runbook.runbook_id or encode_quality_pilot_invocation_runbook(reloaded) != encoded:
            build_failed = True
    except QualityPilotArmingError:
        raise
    except Exception:
        build_failed = True
    if build_failed or runbook is None or not encoded:
        _fail("runbook draft could not be compiled into an accepted runbook")
    return runbook, encoded


def _draft_tree(draft: QualityPilotRunbookDraft) -> dict:
    return {
        "schema_version": QUALITY_PILOT_ARMING_DRAFT_SCHEMA_VERSION,
        "pilot_run_id": draft.pilot_run_id,
        "protocol_sha256": draft.protocol_sha256,
        "confirmed_sessions": [item.isoformat() for item in draft.confirmed_sessions],
        "calendar_decision_ids": list(draft.calendar_decision_ids),
        "provider_version": draft.provider_version,
        "bucket": draft.bucket,
        "window_timestamps": [[pair[0].isoformat(), pair[1].isoformat()] for pair in draft.window_timestamps],
    }


def encode_quality_pilot_runbook_draft(draft: QualityPilotRunbookDraft) -> bytes:
    if type(draft) is not QualityPilotRunbookDraft:
        _fail("runbook draft type is invalid")
    encoded = _canonical_json_bytes(_draft_tree(draft))
    if len(encoded) > MAXIMUM_DRAFT_BYTES:
        _fail("runbook draft encoding exceeds its bounded size")
    return encoded


def decode_quality_pilot_runbook_draft(content_bytes: bytes) -> QualityPilotRunbookDraft:
    root = _parse_json(content_bytes, MAXIMUM_DRAFT_BYTES)
    record = _exact_dict(
        root,
        {
            "schema_version", "pilot_run_id", "protocol_sha256", "confirmed_sessions", "calendar_decision_ids",
            "provider_version", "bucket", "window_timestamps",
        },
        "runbook draft wire shape is invalid",
    )
    if record["schema_version"] != QUALITY_PILOT_ARMING_DRAFT_SCHEMA_VERSION:
        _fail("runbook draft wire schema is invalid")
    sessions = tuple(
        _date_text(item, "runbook draft session is invalid")
        for item in _exact_list(record["confirmed_sessions"], "runbook draft sessions are invalid")
    )
    decisions = tuple(
        _text(item, "runbook draft calendar decision id is invalid")
        for item in _exact_list(record["calendar_decision_ids"], "runbook draft calendar decisions are invalid")
    )
    pairs: list[tuple[datetime, datetime]] = []
    for item in _exact_list(record["window_timestamps"], "runbook draft window timestamps are invalid"):
        if type(item) is not list or len(item) != 2:
            _fail("runbook draft window timestamp pair is invalid")
        pairs.append(
            (
                _datetime_text(item[0], "runbook draft window opens_at is invalid"),
                _datetime_text(item[1], "runbook draft window closes_at is invalid"),
            )
        )
    build_failed = False
    draft: QualityPilotRunbookDraft | None = None
    try:
        draft = QualityPilotRunbookDraft(
            pilot_run_id=_text(record["pilot_run_id"], "runbook draft pilot run id is invalid"),
            protocol_sha256=_text(record["protocol_sha256"], "runbook draft protocol is invalid"),
            confirmed_sessions=sessions,
            calendar_decision_ids=decisions,
            provider_version=_text(record["provider_version"], "runbook draft provider version is invalid"),
            bucket=_text(record["bucket"], "runbook draft bucket is invalid"),
            window_timestamps=tuple(pairs),
        )
    except QualityPilotArmingError:
        raise
    except Exception:
        build_failed = True
    if build_failed or draft is None:
        _fail("runbook draft failed reconstruction")
    if encode_quality_pilot_runbook_draft(draft) != content_bytes:
        _fail("runbook draft wire identity failed")
    return draft


# ---------------------------------------------------------------------------
# QualityPilotArmingManifest
# ---------------------------------------------------------------------------


class QualityPilotArmingSecretKind(Enum):
    KITE_API_KEY = "KITE_API_KEY"
    KITE_ACCESS_TOKEN = "KITE_ACCESS_TOKEN"
    RUNBOOK = "RUNBOOK"


@dataclass(frozen=True, slots=True)
class QualityPilotArmingSecretReference:
    """A Secret Manager secret *reference* only: a resource-safe secret id
    and a canonical positive-integer version string. Never a secret value,
    never ``"latest"``, never signed/zero/whitespace-padded."""

    kind: QualityPilotArmingSecretKind
    secret_id: str
    version: str

    def __post_init__(self) -> None:
        if type(self.kind) is not QualityPilotArmingSecretKind:
            _fail("arming secret reference kind is invalid")
        if type(self.secret_id) is not str or _SECRET_ID_PATTERN.fullmatch(self.secret_id) is None:
            _fail("arming secret reference id is invalid")
        if type(self.version) is not str or _SECRET_VERSION_PATTERN.fullmatch(self.version) is None:
            _fail("arming secret reference version must be a canonical positive integer string")


class QualityPilotArmingScheduleLane(Enum):
    CATALOG_PREOPEN = "CATALOG_PREOPEN"
    QUOTE_0920 = "QUOTE_0920"
    QUOTE_CLOSE = "QUOTE_CLOSE"
    OHLCV_CLOSE = "OHLCV_CLOSE"


_LANE_TO_WINDOW_KIND = {
    QualityPilotArmingScheduleLane.CATALOG_PREOPEN: ScheduledWindowKind.CATALOG_PREOPEN,
    QualityPilotArmingScheduleLane.QUOTE_0920: ScheduledWindowKind.QUOTE_0920,
    QualityPilotArmingScheduleLane.QUOTE_CLOSE: ScheduledWindowKind.QUOTE_CLOSE,
    QualityPilotArmingScheduleLane.OHLCV_CLOSE: ScheduledWindowKind.OHLCV_CLOSE,
}

_CRON_FIELD_PATTERN = re.compile(r"\*|[0-9]{1,2}(-[0-9]{1,2})?(,[0-9]{1,2}(-[0-9]{1,2})?)*")


def _parse_cron_minute_hour(cron_expression: str) -> tuple[int, int]:
    """Parse a strict 5-field cron expression and return its exact
    ``(hour, minute)`` -- both fields must be single literal integers (never
    a wildcard, range, list, or step), day-of-month and month must be
    ``*``, and weekday must be a syntactically valid field. Never rounds,
    widens, or resolves a range to a single instant."""

    if type(cron_expression) is not str:
        _fail("arming schedule cron expression is invalid")
    fields = cron_expression.split(" ")
    if len(fields) != 5:
        _fail("arming schedule cron expression must have exactly 5 fields")
    minute_field, hour_field, day_field, month_field, weekday_field = fields
    if not minute_field.isdigit() or not hour_field.isdigit():
        _fail("arming schedule cron minute/hour must be single exact integers")
    minute = int(minute_field)
    hour = int(hour_field)
    if not (0 <= minute <= 59) or not (0 <= hour <= 23):
        _fail("arming schedule cron minute/hour is out of range")
    if day_field != "*" or month_field != "*":
        _fail("arming schedule cron day-of-month/month must be exactly '*'")
    if _CRON_FIELD_PATTERN.fullmatch(weekday_field) is None:
        _fail("arming schedule cron weekday field is invalid")
    return hour, minute


def _cron_is_safely_inside_gate(*, lane: QualityPilotArmingScheduleLane, cron_expression: str, runbook: QualityPilotInvocationRunbook) -> bool:
    """Independently reuse ``is_window_inside_authorized_schedule`` -- the
    sole schedule-gate authority -- to prove one cron-pinned invocation time
    falls safely (with at least one minute of margin before the gate's own
    upper bound) inside the accepted window for its lane, in the runbook's
    own timezone-aware terms. Never widens or hardcodes a gate time here."""

    hour, minute = _parse_cron_minute_hour(cron_expression)
    probe_session = runbook.campaign.confirmed_sessions[0]
    kind = _LANE_TO_WINDOW_KIND[lane]
    probe_opens_at = None
    probe_failed = False
    try:
        reference = next(w for w in runbook.windows if w.market_session == probe_session and w.window_kind is kind)
        # A wire-decoded ``opens_at`` may carry a UTC (or any equivalent)
        # tzinfo rather than IST, so its own wall-clock hour/minute cannot
        # be trusted directly -- normalize to IST first, then replace the
        # cron-pinned hour/minute on THAT IST wall clock.
        ist_opens_at = reference.opens_at.astimezone(_IST)
        probe_opens_at = ist_opens_at.replace(hour=hour, minute=minute, second=0, microsecond=0)
        probe_closes_at = probe_opens_at + timedelta(minutes=1)
        probe = ObservationWindowSpec(
            pilot_run_id=reference.pilot_run_id, market_session=probe_session, window_kind=kind,
            endpoint_family=_WINDOW_KIND_ENDPOINT_FAMILY[kind], opens_at=probe_opens_at, closes_at=probe_closes_at,
            protocol_sha256=reference.protocol_sha256,
        )
    except Exception:
        probe_failed = True
    if probe_failed or probe_opens_at is None:
        return False
    return is_window_inside_authorized_schedule(probe)


@dataclass(frozen=True, slots=True)
class QualityPilotArmingSchedule:
    lane: QualityPilotArmingScheduleLane
    cron_expression: str

    def __post_init__(self) -> None:
        if type(self.lane) is not QualityPilotArmingScheduleLane:
            _fail("arming schedule lane is invalid")
        _parse_cron_minute_hour(self.cron_expression)


@dataclass(frozen=True, slots=True)
class QualityPilotArmingManifest:
    """One immutable preparation record. Binds an exact compiled runbook to
    a digest-pinned deployment identity, four fixed unique scheduler lanes
    each independently proven to invoke safely inside the runbook's own
    accepted schedule gate, and secret *references* only.

    ``armed=True`` is never a field here -- this manifest is preparation
    evidence only and grants no deployment authority."""

    runbook: QualityPilotInvocationRunbook
    image_reference: str
    code_sha256: str
    environment_sha256: str
    gcp_project_id: str
    gcp_region: str
    gcp_job_name: str
    runtime_service_account_email: str
    scheduler_service_account_email: str
    schedules: tuple[QualityPilotArmingSchedule, ...]
    secret_references: tuple[QualityPilotArmingSecretReference, ...]
    timeout_seconds: int
    manifest_id: str = field(init=False)

    def __post_init__(self) -> None:
        self._validate()
        object.__setattr__(self, "manifest_id", self._calculated_id())

    def _validate(self) -> None:
        if type(self.runbook) is not QualityPilotInvocationRunbook:
            _fail("arming manifest runbook type is invalid")
        runbook_failed = False
        try:
            self.runbook.verify_content_identity()
        except Exception:
            runbook_failed = True
        if runbook_failed:
            _fail("arming manifest runbook failed independent verification")
        if type(self.image_reference) is not str or _IMAGE_DIGEST_PATTERN.fullmatch(self.image_reference) is None:
            _fail("arming manifest image reference must be an exact digest-pinned reference")
        if not _is_sha256(self.code_sha256) or not _is_sha256(self.environment_sha256):
            _fail("arming manifest code/environment digests are invalid")
        if type(self.gcp_project_id) is not str or _PROJECT_PATTERN.fullmatch(self.gcp_project_id) is None:
            _fail("arming manifest GCP project id is invalid")
        if type(self.gcp_region) is not str or _REGION_PATTERN.fullmatch(self.gcp_region) is None:
            _fail("arming manifest GCP region is invalid")
        if type(self.gcp_job_name) is not str or _JOB_PATTERN.fullmatch(self.gcp_job_name) is None:
            _fail("arming manifest GCP job name is invalid")
        if (
            type(self.runtime_service_account_email) is not str
            or _SERVICE_ACCOUNT_PATTERN.fullmatch(self.runtime_service_account_email) is None
        ):
            _fail("arming manifest runtime service account is invalid")
        if (
            type(self.scheduler_service_account_email) is not str
            or _SERVICE_ACCOUNT_PATTERN.fullmatch(self.scheduler_service_account_email) is None
        ):
            _fail("arming manifest scheduler service account is invalid")
        if type(self.timeout_seconds) is bool or type(self.timeout_seconds) is not int or not (60 <= self.timeout_seconds <= 3600):
            _fail("arming manifest timeout is invalid")

        if type(self.schedules) is not tuple or len(self.schedules) != len(QualityPilotArmingScheduleLane):
            _fail("arming manifest must carry exactly one schedule per fixed lane")
        for item in self.schedules:
            if type(item) is not QualityPilotArmingSchedule:
                _fail("arming manifest schedule type is invalid")
        lanes = tuple(item.lane for item in self.schedules)
        if set(lanes) != set(QualityPilotArmingScheduleLane) or len(set(lanes)) != len(lanes):
            _fail("arming manifest schedule lanes must be the four fixed unique lanes")
        for item in self.schedules:
            if not _cron_is_safely_inside_gate(lane=item.lane, cron_expression=item.cron_expression, runbook=self.runbook):
                _fail("arming manifest schedule cron does not fall safely inside its accepted window")

        if type(self.secret_references) is not tuple or len(self.secret_references) != len(QualityPilotArmingSecretKind):
            _fail("arming manifest must carry exactly one secret reference per fixed kind")
        for item in self.secret_references:
            if type(item) is not QualityPilotArmingSecretReference:
                _fail("arming manifest secret reference type is invalid")
        kinds = tuple(item.kind for item in self.secret_references)
        if set(kinds) != set(QualityPilotArmingSecretKind) or len(set(kinds)) != len(kinds):
            _fail("arming manifest secret references must cover the three fixed kinds exactly once")

    def _calculated_id(self) -> str:
        failed = False
        calculated = ""
        try:
            calculated = content_id(
                {
                    "schema": QUALITY_PILOT_ARMING_MANIFEST_SCHEMA_VERSION,
                    "runbook_id": self.runbook.runbook_id,
                    "pilot_run_id": self.runbook.campaign.pilot_run_id,
                    "protocol_sha256": self.runbook.campaign.protocol_sha256,
                    "bucket": self.runbook.bucket,
                    "image_reference": self.image_reference,
                    "code_sha256": self.code_sha256,
                    "environment_sha256": self.environment_sha256,
                    "gcp_project_id": self.gcp_project_id,
                    "gcp_region": self.gcp_region,
                    "gcp_job_name": self.gcp_job_name,
                    "runtime_service_account_email": self.runtime_service_account_email,
                    "scheduler_service_account_email": self.scheduler_service_account_email,
                    "schedules": tuple(sorted((item.lane.value, item.cron_expression) for item in self.schedules)),
                    "secret_references": tuple(
                        sorted((item.kind.value, item.secret_id, item.version) for item in self.secret_references)
                    ),
                    "timeout_seconds": self.timeout_seconds,
                    "tasks": 1,
                    "parallelism": 1,
                    "max_retries": 0,
                    "armed": False,
                },
                length=64,
            )
        except Exception:
            failed = True
        if failed:
            _fail("arming manifest identity calculation failed")
        return calculated

    def verify_content_identity(self) -> None:
        self._validate()
        if self.manifest_id != self._calculated_id():
            _fail("arming manifest identity failed")

    @property
    def runbook_id(self) -> str:
        return self.runbook.runbook_id

    @property
    def pilot_run_id(self) -> str:
        return self.runbook.campaign.pilot_run_id

    @property
    def protocol_sha256(self) -> str:
        return self.runbook.campaign.protocol_sha256

    @property
    def bucket(self) -> str:
        return self.runbook.bucket

    @property
    def tasks(self) -> int:
        return 1

    @property
    def parallelism(self) -> int:
        return 1

    @property
    def max_retries(self) -> int:
        return 0

    @property
    def armed(self) -> bool:
        return False

    @property
    def quality_only(self) -> bool:
        return True

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


def _manifest_tree(manifest: QualityPilotArmingManifest) -> dict:
    return {
        "schema_version": QUALITY_PILOT_ARMING_MANIFEST_SCHEMA_VERSION,
        "runbook_id": manifest.runbook_id,
        "pilot_run_id": manifest.pilot_run_id,
        "protocol_sha256": manifest.protocol_sha256,
        "bucket": manifest.bucket,
        "image_reference": manifest.image_reference,
        "code_sha256": manifest.code_sha256,
        "environment_sha256": manifest.environment_sha256,
        "gcp_project_id": manifest.gcp_project_id,
        "gcp_region": manifest.gcp_region,
        "gcp_job_name": manifest.gcp_job_name,
        "runtime_service_account_email": manifest.runtime_service_account_email,
        "scheduler_service_account_email": manifest.scheduler_service_account_email,
        "schedules": [
            {"lane": item.lane.value, "cron_expression": item.cron_expression}
            for item in sorted(manifest.schedules, key=lambda item: item.lane.value)
        ],
        "secret_references": [
            {"kind": item.kind.value, "secret_id": item.secret_id, "version": item.version}
            for item in sorted(manifest.secret_references, key=lambda item: item.kind.value)
        ],
        "timeout_seconds": manifest.timeout_seconds,
        "tasks": manifest.tasks,
        "parallelism": manifest.parallelism,
        "max_retries": manifest.max_retries,
        "armed": manifest.armed,
        "manifest_id": manifest.manifest_id,
    }


def encode_quality_pilot_arming_manifest(manifest: QualityPilotArmingManifest) -> bytes:
    if type(manifest) is not QualityPilotArmingManifest:
        _fail("arming manifest type is invalid")
    manifest_failed = False
    try:
        manifest.verify_content_identity()
    except Exception:
        manifest_failed = True
    if manifest_failed:
        _fail("arming manifest failed independent verification")
    encoded = _canonical_json_bytes(_manifest_tree(manifest))
    if len(encoded) > MAXIMUM_MANIFEST_BYTES:
        _fail("arming manifest encoding exceeds its bounded size")
    return encoded


def decode_quality_pilot_arming_manifest(
    content_bytes: bytes, *, runbook: QualityPilotInvocationRunbook
) -> QualityPilotArmingManifest:
    """Decode one arming manifest's wire bytes against a *separately
    supplied* runbook. The manifest wire form binds only ``runbook_id`` (and
    the redundant pilot/protocol/bucket fields), never the runbook's full
    content -- callers load the manifest and its exact mounted runbook as
    two separate files and this function requires their full identities to
    agree before reconstructing anything, exactly mirroring how
    ``quality_pilot_job.py``'s ``run-due-window`` command is required to
    verify them."""

    if type(runbook) is not QualityPilotInvocationRunbook:
        _fail("arming manifest decode requires an exact runbook type")
    runbook_failed = False
    try:
        runbook.verify_content_identity()
    except Exception:
        runbook_failed = True
    if runbook_failed:
        _fail("arming manifest decode runbook failed independent verification")

    root = _parse_json(content_bytes, MAXIMUM_MANIFEST_BYTES)
    record = _exact_dict(
        root,
        {
            "schema_version", "runbook_id", "pilot_run_id", "protocol_sha256", "bucket", "image_reference",
            "code_sha256", "environment_sha256", "gcp_project_id", "gcp_region", "gcp_job_name",
            "runtime_service_account_email", "scheduler_service_account_email", "schedules", "secret_references",
            "timeout_seconds", "tasks", "parallelism", "max_retries", "armed", "manifest_id",
        },
        "arming manifest wire shape is invalid",
    )
    if record["schema_version"] != QUALITY_PILOT_ARMING_MANIFEST_SCHEMA_VERSION:
        _fail("arming manifest wire schema is invalid")
    if (
        record["runbook_id"] != runbook.runbook_id
        or record["pilot_run_id"] != runbook.campaign.pilot_run_id
        or record["protocol_sha256"] != runbook.campaign.protocol_sha256
        or record["bucket"] != runbook.bucket
    ):
        _fail("arming manifest disagrees with its separately supplied runbook")

    schedules: list[QualityPilotArmingSchedule] = []
    for item in _exact_list(record["schedules"], "arming manifest schedules are invalid"):
        entry = _exact_dict(item, {"lane", "cron_expression"}, "arming manifest schedule entry is invalid")
        lane_failed = False
        lane: QualityPilotArmingScheduleLane | None = None
        try:
            lane = QualityPilotArmingScheduleLane(entry["lane"])
        except Exception:
            lane_failed = True
        if lane_failed or lane is None:
            _fail("arming manifest schedule lane is invalid")
        schedules.append(
            QualityPilotArmingSchedule(
                lane=lane, cron_expression=_text(entry["cron_expression"], "arming manifest cron expression is invalid")
            )
        )

    secret_references: list[QualityPilotArmingSecretReference] = []
    for item in _exact_list(record["secret_references"], "arming manifest secret references are invalid"):
        entry = _exact_dict(item, {"kind", "secret_id", "version"}, "arming manifest secret reference entry is invalid")
        kind_failed = False
        kind: QualityPilotArmingSecretKind | None = None
        try:
            kind = QualityPilotArmingSecretKind(entry["kind"])
        except Exception:
            kind_failed = True
        if kind_failed or kind is None:
            _fail("arming manifest secret reference kind is invalid")
        secret_references.append(
            QualityPilotArmingSecretReference(
                kind=kind,
                secret_id=_text(entry["secret_id"], "arming manifest secret id is invalid"),
                version=_text(entry["version"], "arming manifest secret version is invalid"),
            )
        )

    if record["tasks"] != 1 or record["parallelism"] != 1 or record["max_retries"] != 0 or record["armed"] is not False:
        _fail("arming manifest fixed posture fields disagree with their required values")

    build_failed = False
    manifest: QualityPilotArmingManifest | None = None
    try:
        manifest = QualityPilotArmingManifest(
            runbook=runbook,
            image_reference=_text(record["image_reference"], "arming manifest image reference is invalid"),
            code_sha256=_text(record["code_sha256"], "arming manifest code digest is invalid"),
            environment_sha256=_text(record["environment_sha256"], "arming manifest environment digest is invalid"),
            gcp_project_id=_text(record["gcp_project_id"], "arming manifest project id is invalid"),
            gcp_region=_text(record["gcp_region"], "arming manifest region is invalid"),
            gcp_job_name=_text(record["gcp_job_name"], "arming manifest job name is invalid"),
            runtime_service_account_email=_text(
                record["runtime_service_account_email"], "arming manifest runtime service account is invalid"
            ),
            scheduler_service_account_email=_text(
                record["scheduler_service_account_email"], "arming manifest scheduler service account is invalid"
            ),
            schedules=tuple(schedules),
            secret_references=tuple(secret_references),
            timeout_seconds=_integer(record["timeout_seconds"], "arming manifest timeout is invalid", minimum=60),
        )
    except QualityPilotArmingError:
        raise
    except Exception:
        build_failed = True
    if build_failed or manifest is None:
        _fail("arming manifest failed reconstruction")
    if manifest.manifest_id != record["manifest_id"] or encode_quality_pilot_arming_manifest(manifest) != content_bytes:
        _fail("arming manifest wire identity failed")
    return manifest


# ---------------------------------------------------------------------------
# Due-window selection and missed-window posture
# ---------------------------------------------------------------------------


class QualityPilotDueWindowStatus(Enum):
    DUE = "DUE"
    NOT_SCHEDULED = "NOT_SCHEDULED"


@dataclass(frozen=True, slots=True)
class QualityPilotDueWindowSelection:
    status: QualityPilotDueWindowStatus
    market_session: date | None
    window_kind: ScheduledWindowKind | None

    def __post_init__(self) -> None:
        if type(self.status) is not QualityPilotDueWindowStatus:
            _fail("due window selection status is invalid")
        if self.status is QualityPilotDueWindowStatus.DUE:
            if type(self.market_session) is not date or type(self.window_kind) is not ScheduledWindowKind:
                _fail("due window selection must carry an exact session/kind when DUE")
        elif self.market_session is not None or self.window_kind is not None:
            _fail("due window selection must carry no session/kind when NOT_SCHEDULED")


def _require_runbook_and_observed_at(runbook: object, observed_at: object) -> None:
    if type(runbook) is not QualityPilotInvocationRunbook:
        _fail("arming due-window runbook type is invalid")
    runbook_failed = False
    try:
        runbook.verify_content_identity()
    except Exception:
        runbook_failed = True
    if runbook_failed:
        _fail("arming due-window runbook failed independent verification")
    offset_failed = False
    observed_offset = None
    if type(observed_at) is datetime:
        try:
            observed_offset = observed_at.utcoffset()
        except Exception:
            offset_failed = True
    if type(observed_at) is not datetime or offset_failed or observed_offset != timedelta(0):
        _fail("arming due-window observed_at must be an exact aware UTC datetime with zero offset")


def select_due_quality_pilot_window(
    runbook: QualityPilotInvocationRunbook, observed_at: datetime
) -> QualityPilotDueWindowSelection:
    """Return exactly one due session/window when ``observed_at`` lies
    inside one of the runbook's exact windows (``opens_at <= observed_at <=
    closes_at``); ``NOT_SCHEDULED`` otherwise. Fails closed on an impossible
    ambiguous overlap rather than picking one arbitrarily. Never rounds,
    widens a window, chooses the latest, synthesizes a date, or consults a
    local/system calendar."""

    _require_runbook_and_observed_at(runbook, observed_at)
    matches = tuple(window for window in runbook.windows if window.opens_at <= observed_at <= window.closes_at)
    if len(matches) > 1:
        _fail("arming due-window selection found an impossible ambiguous window overlap")
    if not matches:
        return QualityPilotDueWindowSelection(QualityPilotDueWindowStatus.NOT_SCHEDULED, None, None)
    window = matches[0]
    return QualityPilotDueWindowSelection(QualityPilotDueWindowStatus.DUE, window.market_session, window.window_kind)


class QualityPilotWindowCompletionProbeResult(Enum):
    """The caller's already-obtained answer to "does an independently
    verified terminal completion exist for the exact window named by
    ``quality_pilot_window_completion_probe_target``". This module never
    performs that read itself."""

    COMPLETE = "COMPLETE"
    INCOMPLETE = "INCOMPLETE"


def quality_pilot_window_completion_probe_targets(
    runbook: QualityPilotInvocationRunbook, observed_at: datetime
) -> tuple[ObservationWindowSpec, ...]:
    """Return the exact canonical ordered prefix of windows whose completion
    state the caller must independently verify before calling
    ``assess_quality_pilot_window_posture`` with a matching
    ``QualityPilotOrderedCompletionProof``. Empty only when ``observed_at``
    is before the campaign's very first window (the pilot has not started,
    so no completion evidence can exist yet).

    When a window is currently DUE, the prefix is every window up to and
    including that DUE window itself, in canonical order -- proving every
    predecessor is complete before the current window can be considered,
    and letting the current window's own entry distinguish DUE from a
    redundant ALREADY_COMPLETE replay. Otherwise the prefix is every window
    whose ``closes_at`` is at or before ``observed_at`` (which, because
    confirmed sessions are strictly increasing distinct dates, is always an
    exact contiguous canonical-order prefix), so a genuinely missed window
    anywhere in that prefix -- adjacent or not -- is provably included."""

    _require_runbook_and_observed_at(runbook, observed_at)
    selection = select_due_quality_pilot_window(runbook, observed_at)
    if selection.status is QualityPilotDueWindowStatus.DUE:
        due_index = next(
            i for i, w in enumerate(runbook.windows)
            if w.market_session == selection.market_session and w.window_kind is selection.window_kind
        )
        return runbook.windows[: due_index + 1]
    passed_indices = [i for i, window in enumerate(runbook.windows) if window.closes_at <= observed_at]
    if not passed_indices:
        return ()
    return runbook.windows[: max(passed_indices) + 1]


@dataclass(frozen=True, slots=True)
class QualityPilotWindowCompletionEvidence:
    """One immutable, independently bound completion-probe result for
    exactly one named window. Never a bare boolean -- ``window_id`` binds
    this evidence to the exact window it attests, so it can never be
    silently reused, reordered, or applied to a different window."""

    window_id: str
    result: QualityPilotWindowCompletionProbeResult

    def __post_init__(self) -> None:
        if not _is_sha256(self.window_id):
            _fail("completion evidence window id is invalid")
        if type(self.result) is not QualityPilotWindowCompletionProbeResult:
            _fail("completion evidence result is invalid")


@dataclass(frozen=True, slots=True)
class QualityPilotOrderedCompletionProof:
    """One immutable, exact ordered tuple of completion evidence, one entry
    per window ``quality_pilot_window_completion_probe_targets`` names, in
    the identical order, with no duplicate window id. Structural shape is
    validated here; agreement with the exact required target tuple is
    independently re-checked by ``assess_quality_pilot_window_posture``."""

    entries: tuple[QualityPilotWindowCompletionEvidence, ...]

    def __post_init__(self) -> None:
        if type(self.entries) is not tuple:
            _fail("completion proof entries must be an exact tuple")
        for item in self.entries:
            if type(item) is not QualityPilotWindowCompletionEvidence:
                _fail("completion proof entry type is invalid")
        window_ids = tuple(item.window_id for item in self.entries)
        if len(window_ids) != len(set(window_ids)):
            _fail("completion proof contains a duplicate window id")


class QualityPilotWindowPosture(Enum):
    DUE = "DUE"
    NOT_SCHEDULED = "NOT_SCHEDULED"
    ALREADY_COMPLETE = "ALREADY_COMPLETE"
    MISSED_WINDOW_BLOCKED = "MISSED_WINDOW_BLOCKED"
    PILOT_COMPLETE = "PILOT_COMPLETE"


@dataclass(frozen=True, slots=True)
class QualityPilotWindowPostureAssessment:
    posture: QualityPilotWindowPosture
    market_session: date | None
    window_kind: ScheduledWindowKind | None

    def __post_init__(self) -> None:
        if type(self.posture) is not QualityPilotWindowPosture:
            _fail("window posture assessment posture is invalid")
        if self.posture is QualityPilotWindowPosture.NOT_SCHEDULED:
            if self.market_session is not None or self.window_kind is not None:
                _fail("not-scheduled window posture must not carry a session or window kind")
            return
        if type(self.market_session) is not date or type(self.window_kind) is not ScheduledWindowKind:
            _fail("window posture assessment must carry an exact session/kind for this posture")


def assess_quality_pilot_window_posture(
    runbook: QualityPilotInvocationRunbook,
    observed_at: datetime,
    proof: QualityPilotOrderedCompletionProof | None,
) -> QualityPilotWindowPostureAssessment:
    """Distinguish DUE, NOT_SCHEDULED, ALREADY_COMPLETE, MISSED_WINDOW_BLOCKED,
    and PILOT_COMPLETE from an exact ordered-prefix completion proof.

    ``proof`` must bind evidence to exactly the ordered window tuple
    ``quality_pilot_window_completion_probe_targets`` returns -- same
    length, same order, same window ids; missing, extra, reordered,
    duplicate, or foreign-window evidence fails closed. ``None`` is valid
    only when the target tuple is itself empty (before the pilot starts).

    For a DUE window, every predecessor entry must be COMPLETE before the
    current window can be DUE at all; if any predecessor is INCOMPLETE the
    result is MISSED_WINDOW_BLOCKED regardless of the current window's own
    state. Only once every predecessor is COMPLETE does the current
    window's own entry decide DUE (INCOMPLETE) vs ALREADY_COMPLETE
    (COMPLETE). Between windows, every entry in the passed-window prefix
    must be COMPLETE or the result is MISSED_WINDOW_BLOCKED; after the
    final window this means PILOT_COMPLETE requires all 80 windows
    COMPLETE. This never silently advances to a later window/session."""

    _require_runbook_and_observed_at(runbook, observed_at)
    targets = quality_pilot_window_completion_probe_targets(runbook, observed_at)

    if not targets:
        if proof is not None:
            if type(proof) is not QualityPilotOrderedCompletionProof or proof.entries:
                _fail("window posture assessment received completion evidence with no probe targets")
        return QualityPilotWindowPostureAssessment(QualityPilotWindowPosture.NOT_SCHEDULED, None, None)

    if type(proof) is not QualityPilotOrderedCompletionProof:
        _fail("window posture assessment requires an exact ordered completion proof")
    expected_ids = tuple(window.window_id for window in targets)
    actual_ids = tuple(entry.window_id for entry in proof.entries)
    if actual_ids != expected_ids:
        _fail("window posture assessment completion proof disagrees with the exact required target windows")

    boundary = targets[-1]
    selection = select_due_quality_pilot_window(runbook, observed_at)

    if selection.status is QualityPilotDueWindowStatus.DUE:
        predecessor_entries = proof.entries[:-1]
        current_entry = proof.entries[-1]
        if any(entry.result is QualityPilotWindowCompletionProbeResult.INCOMPLETE for entry in predecessor_entries):
            return QualityPilotWindowPostureAssessment(
                QualityPilotWindowPosture.MISSED_WINDOW_BLOCKED, boundary.market_session, boundary.window_kind
            )
        if current_entry.result is QualityPilotWindowCompletionProbeResult.INCOMPLETE:
            return QualityPilotWindowPostureAssessment(
                QualityPilotWindowPosture.DUE, selection.market_session, selection.window_kind
            )
        return QualityPilotWindowPostureAssessment(
            QualityPilotWindowPosture.ALREADY_COMPLETE, boundary.market_session, boundary.window_kind
        )

    if any(entry.result is QualityPilotWindowCompletionProbeResult.INCOMPLETE for entry in proof.entries):
        return QualityPilotWindowPostureAssessment(
            QualityPilotWindowPosture.MISSED_WINDOW_BLOCKED, boundary.market_session, boundary.window_kind
        )
    final_window = runbook.windows[-1]
    if boundary.window_id == final_window.window_id:
        return QualityPilotWindowPostureAssessment(
            QualityPilotWindowPosture.PILOT_COMPLETE, boundary.market_session, boundary.window_kind
        )
    return QualityPilotWindowPostureAssessment(QualityPilotWindowPosture.NOT_SCHEDULED, None, None)
