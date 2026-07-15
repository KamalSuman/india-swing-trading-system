from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, datetime
from enum import Enum


_SHA256 = re.compile(r"[0-9a-f]{64}\Z")


def _require_aware(value: datetime, field_name: str) -> None:
    if not isinstance(value, datetime):
        raise TypeError(f"{field_name} must be a datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")


def _require_sha256(value: str, field_name: str) -> None:
    if not isinstance(value, str) or _SHA256.fullmatch(value) is None:
        raise ValueError(f"{field_name} must be a full lowercase SHA-256 identifier")


class ReferenceReadiness(str, Enum):
    COLLECTION_ONLY = "COLLECTION_ONLY"
    POINT_IN_TIME_VERIFIED = "POINT_IN_TIME_VERIFIED"
    SYNTHETIC_TEST = "SYNTHETIC_TEST"


@dataclass(frozen=True, slots=True)
class ExternalRecordRef:
    """Lineage for one external reference-data record.

    ``event_time`` describes when the underlying event occurred or is scheduled
    to occur. ``knowledge_time`` is the earliest supportable time at which this
    exact record vintage was available. A future scheduled event may therefore
    have a knowledge time before its event time.
    """

    event_time: datetime
    knowledge_time: datetime
    source: str
    content_hash: str
    source_snapshot_id: str

    def __post_init__(self) -> None:
        _require_aware(self.event_time, "external_record.event_time")
        _require_aware(self.knowledge_time, "external_record.knowledge_time")
        if not isinstance(self.source, str) or not self.source.strip():
            raise ValueError("external_record.source is required")
        _require_sha256(self.content_hash, "external_record.content_hash")
        _require_sha256(
            self.source_snapshot_id,
            "external_record.source_snapshot_id",
        )


@dataclass(frozen=True, slots=True)
class EffectiveExternalRecordRef:
    """An external record plus the exchange-session interval it governs.

    Source ``event_time`` and ``knowledge_time`` remain untouched.  The
    half-open effective interval is separate because, for example, an NSE
    report produced for day D may govern trading only from the next session.
    """

    reference: ExternalRecordRef
    effective_from_session: date
    effective_to_exclusive: date | None
    schema_version: str

    def __post_init__(self) -> None:
        if type(self.reference) is not ExternalRecordRef:
            raise TypeError(
                "effective_record.reference must be an exact ExternalRecordRef"
            )
        if type(self.effective_from_session) is not date:
            raise TypeError("effective_from_session must be a date")
        if self.effective_to_exclusive is not None:
            if type(self.effective_to_exclusive) is not date:
                raise TypeError("effective_to_exclusive must be a date or None")
            if self.effective_to_exclusive <= self.effective_from_session:
                raise ValueError("effective record interval must be positive and half-open")
        if not isinstance(self.schema_version, str) or not self.schema_version.strip():
            raise ValueError("effective record schema_version is required")

    def is_effective_on(self, session: date) -> bool:
        if type(session) is not date:
            raise TypeError("effective record session must be a date")
        return self.effective_from_session <= session and (
            self.effective_to_exclusive is None
            or session < self.effective_to_exclusive
        )
