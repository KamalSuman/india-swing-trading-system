from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from decimal import Decimal
from enum import Enum

from india_swing.identity import content_id
from india_swing.reference.models import ReferenceReadiness


CORPORATE_ACTION_EVENT_SCHEMA_VERSION = "corporate-action-event/v1"
CORPORATE_ACTION_SNAPSHOT_SCHEMA_VERSION = "corporate-action-snapshot/v1"
CORPORATE_ACTION_POLICY_VERSION = "point-in-time-announcement-ledger/v1"

_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_REASON_CODE = re.compile(r"[A-Z][A-Z0-9_]{2,127}\Z")
ZERO = Decimal("0")


class CorporateActionIntegrityError(ValueError):
    pass


class CorporateActionType(str, Enum):
    SPLIT = "SPLIT"
    BONUS = "BONUS"
    CASH_DIVIDEND = "CASH_DIVIDEND"
    RIGHTS = "RIGHTS"
    MERGER = "MERGER"
    DEMERGER = "DEMERGER"
    SYMBOL_CHANGE = "SYMBOL_CHANGE"
    ISIN_CHANGE = "ISIN_CHANGE"
    DELISTING = "DELISTING"


class CorporateActionStatus(str, Enum):
    CONFIRMED = "CONFIRMED"
    CANCELLED = "CANCELLED"


def _sha(value: str, name: str) -> None:
    if not isinstance(value, str) or _SHA256.fullmatch(value) is None:
        raise CorporateActionIntegrityError(f"{name} must be a full lowercase SHA-256")


def _aware_utc(value: datetime, name: str) -> datetime:
    if not isinstance(value, datetime):
        raise TypeError(f"{name} must be a datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        raise CorporateActionIntegrityError(f"{name} must be timezone-aware")
    return value.astimezone(timezone.utc)


def _positive_decimal(value: Decimal | None, name: str) -> None:
    if type(value) is not Decimal or not value.is_finite() or value <= ZERO:
        raise CorporateActionIntegrityError(f"{name} must be a positive Decimal")


def _reason_codes(values: tuple[str, ...]) -> None:
    if type(values) is not tuple or values != tuple(sorted(set(values))):
        raise CorporateActionIntegrityError("reason codes must be sorted and unique")
    if any(_REASON_CODE.fullmatch(value) is None for value in values):
        raise CorporateActionIntegrityError("corporate-action reason code is invalid")


@dataclass(frozen=True, slots=True)
class CorporateActionEvent:
    stable_instrument_id: str
    stable_listing_id: str | None
    action_type: CorporateActionType
    status: CorporateActionStatus
    effective_session: date
    announcement_time: datetime
    knowledge_time: datetime
    source_artifact_id: str
    source_row_id: str
    pre_action_shares: Decimal | None = None
    post_action_shares: Decimal | None = None
    cash_amount_per_share: Decimal | None = None
    currency: str | None = None
    supersedes_event_id: str | None = None
    schema_version: str = CORPORATE_ACTION_EVENT_SCHEMA_VERSION
    event_id: str = field(init=False)

    def __post_init__(self) -> None:
        _sha(self.stable_instrument_id, "stable_instrument_id")
        if self.stable_listing_id is not None:
            _sha(self.stable_listing_id, "stable_listing_id")
        if type(self.action_type) is not CorporateActionType:
            raise TypeError("corporate action type must be exact")
        if type(self.status) is not CorporateActionStatus:
            raise TypeError("corporate action status must be exact")
        if type(self.effective_session) is not date:
            raise TypeError("corporate action effective_session must be a date")
        object.__setattr__(
            self,
            "announcement_time",
            _aware_utc(self.announcement_time, "announcement_time"),
        )
        object.__setattr__(
            self,
            "knowledge_time",
            _aware_utc(self.knowledge_time, "knowledge_time"),
        )
        if self.knowledge_time < self.announcement_time:
            raise CorporateActionIntegrityError(
                "corporate action cannot be known before its announcement"
            )
        _sha(self.source_artifact_id, "source_artifact_id")
        _sha(self.source_row_id, "source_row_id")
        if self.supersedes_event_id is not None:
            _sha(self.supersedes_event_id, "supersedes_event_id")

        supplied_terms = (
            self.pre_action_shares,
            self.post_action_shares,
            self.cash_amount_per_share,
            self.currency,
        )
        if self.status is CorporateActionStatus.CANCELLED:
            if self.supersedes_event_id is None:
                raise CorporateActionIntegrityError(
                    "a cancellation must supersede an earlier corporate-action event"
                )
            if any(value is not None for value in supplied_terms):
                raise CorporateActionIntegrityError(
                    "a cancellation must not repeat superseded economic terms"
                )
        elif self.action_type in {
            CorporateActionType.SPLIT,
            CorporateActionType.BONUS,
        }:
            _positive_decimal(self.pre_action_shares, "pre_action_shares")
            _positive_decimal(self.post_action_shares, "post_action_shares")
            if self.pre_action_shares == self.post_action_shares:
                raise CorporateActionIntegrityError(
                    "share action must change the outstanding share ratio"
                )
            if self.cash_amount_per_share is not None or self.currency is not None:
                raise CorporateActionIntegrityError(
                    "split and bonus events cannot carry cash terms"
                )
        elif self.action_type is CorporateActionType.CASH_DIVIDEND:
            _positive_decimal(self.cash_amount_per_share, "cash_amount_per_share")
            if self.currency != "INR":
                raise CorporateActionIntegrityError(
                    "cash dividends currently require canonical INR terms"
                )
            if self.pre_action_shares is not None or self.post_action_shares is not None:
                raise CorporateActionIntegrityError(
                    "cash dividends cannot carry share-ratio terms"
                )
        elif any(value is not None for value in supplied_terms):
            raise CorporateActionIntegrityError(
                "complex actions require a future action-specific terms contract"
            )
        if self.schema_version != CORPORATE_ACTION_EVENT_SCHEMA_VERSION:
            raise CorporateActionIntegrityError("unsupported corporate-action event schema")
        object.__setattr__(self, "event_id", self._calculated_id())

    @property
    def automatic_raw_price_factor(self) -> Decimal | None:
        """Return the mechanical ex-date price factor; dividends remain unsupported."""

        if self.status is not CorporateActionStatus.CONFIRMED or self.action_type not in {
            CorporateActionType.SPLIT,
            CorporateActionType.BONUS,
        }:
            return None
        assert self.pre_action_shares is not None
        assert self.post_action_shares is not None
        return self.pre_action_shares / self.post_action_shares

    def _calculated_id(self) -> str:
        return content_id(
            {
                "schema_version": self.schema_version,
                "stable_instrument_id": self.stable_instrument_id,
                "stable_listing_id": self.stable_listing_id,
                "action_type": self.action_type,
                "status": self.status,
                "effective_session": self.effective_session,
                "announcement_time": self.announcement_time,
                "knowledge_time": self.knowledge_time,
                "source_artifact_id": self.source_artifact_id,
                "source_row_id": self.source_row_id,
                "pre_action_shares": self.pre_action_shares,
                "post_action_shares": self.post_action_shares,
                "cash_amount_per_share": self.cash_amount_per_share,
                "currency": self.currency,
                "supersedes_event_id": self.supersedes_event_id,
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        if self.event_id != self._calculated_id():
            raise CorporateActionIntegrityError(
                "corporate-action event content identity failed"
            )


@dataclass(frozen=True, slots=True)
class CorporateActionSnapshot:
    cutoff: datetime
    coverage_start: date
    coverage_end: date
    source_artifact_ids: tuple[str, ...]
    events: tuple[CorporateActionEvent, ...]
    readiness: ReferenceReadiness
    complete: bool
    actionable: bool
    reason_codes: tuple[str, ...]
    policy_version: str = CORPORATE_ACTION_POLICY_VERSION
    schema_version: str = CORPORATE_ACTION_SNAPSHOT_SCHEMA_VERSION
    snapshot_id: str = field(init=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "cutoff", _aware_utc(self.cutoff, "snapshot cutoff"))
        if type(self.coverage_start) is not date or type(self.coverage_end) is not date:
            raise TypeError("corporate-action coverage boundaries must be dates")
        if self.coverage_end < self.coverage_start:
            raise CorporateActionIntegrityError(
                "corporate-action coverage interval must be positive"
            )
        if (
            type(self.source_artifact_ids) is not tuple
            or not self.source_artifact_ids
            or self.source_artifact_ids != tuple(sorted(set(self.source_artifact_ids)))
        ):
            raise CorporateActionIntegrityError(
                "source artifact IDs must be non-empty, sorted, and unique"
            )
        for value in self.source_artifact_ids:
            _sha(value, "source_artifact_id")
        if (
            type(self.events) is not tuple
            or any(type(value) is not CorporateActionEvent for value in self.events)
            or self.events
            != tuple(
                sorted(
                    self.events,
                    key=lambda value: (
                        value.knowledge_time,
                        value.effective_session,
                        value.event_id,
                    ),
                )
            )
        ):
            raise CorporateActionIntegrityError(
                "corporate-action events must be exact and knowledge-time ordered"
            )
        if len({value.event_id for value in self.events}) != len(self.events):
            raise CorporateActionIntegrityError("corporate-action event IDs must be unique")
        by_id = {value.event_id: value for value in self.events}
        superseded_ids: set[str] = set()
        for value in self.events:
            value.verify_content_identity()
            if value.knowledge_time > self.cutoff:
                raise CorporateActionIntegrityError(
                    "snapshot contains corporate-action evidence known after its cutoff"
                )
            if value.source_artifact_id not in self.source_artifact_ids:
                raise CorporateActionIntegrityError(
                    "corporate-action event source is absent from snapshot lineage"
                )
            if not self.coverage_start <= value.effective_session <= self.coverage_end:
                raise CorporateActionIntegrityError(
                    "corporate-action event lies outside snapshot coverage"
                )
            if value.supersedes_event_id is not None:
                target = by_id.get(value.supersedes_event_id)
                if target is None:
                    raise CorporateActionIntegrityError(
                        "corporate-action amendment target is absent"
                    )
                if (
                    target.knowledge_time >= value.knowledge_time
                    or target.stable_instrument_id != value.stable_instrument_id
                ):
                    raise CorporateActionIntegrityError(
                        "corporate-action amendment lineage is inconsistent"
                    )
                if target.event_id in superseded_ids:
                    raise CorporateActionIntegrityError(
                        "one corporate-action event cannot have competing amendments"
                    )
                superseded_ids.add(target.event_id)
        if type(self.readiness) is not ReferenceReadiness:
            raise TypeError("corporate-action readiness must be exact")
        if type(self.complete) is not bool or type(self.actionable) is not bool:
            raise TypeError("corporate-action completeness and actionability must be bool")
        _reason_codes(self.reason_codes)
        if self.actionable and (
            not self.complete
            or self.reason_codes
            or self.readiness is ReferenceReadiness.COLLECTION_ONLY
        ):
            raise CorporateActionIntegrityError(
                "actionable corporate actions must be complete, unblocked, and verified"
            )
        if not self.actionable and not self.reason_codes:
            raise CorporateActionIntegrityError(
                "non-actionable corporate-action snapshots require reason codes"
            )
        if (
            self.policy_version != CORPORATE_ACTION_POLICY_VERSION
            or self.schema_version != CORPORATE_ACTION_SNAPSHOT_SCHEMA_VERSION
        ):
            raise CorporateActionIntegrityError(
                "unsupported corporate-action snapshot contract"
            )
        object.__setattr__(self, "snapshot_id", self._calculated_id())

    @property
    def active_events(self) -> tuple[CorporateActionEvent, ...]:
        superseded = {
            value.supersedes_event_id
            for value in self.events
            if value.supersedes_event_id is not None
        }
        return tuple(
            value
            for value in self.events
            if value.event_id not in superseded
            and value.status is CorporateActionStatus.CONFIRMED
        )

    def effective_events_on(self, session: date) -> tuple[CorporateActionEvent, ...]:
        if type(session) is not date:
            raise TypeError("corporate-action lookup session must be a date")
        return tuple(
            value for value in self.active_events if value.effective_session == session
        )

    def _calculated_id(self) -> str:
        return content_id(
            {
                "schema_version": self.schema_version,
                "policy_version": self.policy_version,
                "cutoff": self.cutoff,
                "coverage_start": self.coverage_start,
                "coverage_end": self.coverage_end,
                "source_artifact_ids": self.source_artifact_ids,
                "events": self.events,
                "readiness": self.readiness,
                "complete": self.complete,
                "actionable": self.actionable,
                "reason_codes": self.reason_codes,
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        for value in self.events:
            if type(value) is not CorporateActionEvent:
                raise CorporateActionIntegrityError(
                    "corporate-action snapshot graph contains invalid data"
                )
            value.verify_content_identity()
        if self.snapshot_id != self._calculated_id():
            raise CorporateActionIntegrityError(
                "corporate-action snapshot content identity failed"
            )
