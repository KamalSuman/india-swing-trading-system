from __future__ import annotations

import json
import os
import shutil
import tempfile
from dataclasses import dataclass, field, fields
from datetime import date, datetime, timezone
from enum import Enum
from pathlib import Path

from india_swing._filesystem import (
    FileLockUnavailable,
    FileSafetyError,
    advisory_file_lock,
    read_stable_regular_file,
)
from india_swing.historical_prices.models import NseEodSessionArtifact
from india_swing.identity import content_id

from .backfill_gaps import (
    HistoricalBackfillGapClassification,
    HistoricalBackfillSessionGapEvidence,
)
from .models import (
    LISTING_KEY_PATTERN,
    MARKET_DATA_PROVIDER_PATTERN,
    NSE_EQUITY_ISIN_PATTERN,
    NSE_SECURITY_SERIES_PATTERN,
    SHA256_IDENTIFIER,
)


HISTORICAL_GAP_ADJUDICATION_SCHEMA_VERSION = "historical-gap-adjudication-report/v1"
HISTORICAL_GAP_ADJUDICATION_POLICY_VERSION = (
    "nse-eod-pinned-traded-row-assessment/v1"
)
HISTORICAL_GAP_ADJUDICATION_CODEC_VERSION = (
    "historical-gap-adjudication-report-json/v1"
)
HISTORICAL_GAP_ADJUDICATION_DATASET = "historical-gap-adjudication-reports"
REPORT_FILENAME = "report.json"
MAXIMUM_GAP_ADJUDICATION_REPORT_BYTES = 32 * 1024 * 1024


class HistoricalGapAdjudicationError(ValueError):
    pass


class HistoricalGapAdjudicationIntegrityError(HistoricalGapAdjudicationError):
    pass


class HistoricalGapNseStatus(str, Enum):
    """Only a comparison against one pinned TRADED_ROWS_ONLY artifact.

    NSE_TRADED_BAR_ABSENT means the exact identity is absent from that one
    artifact -- never a claim of no trading, suspension, delisting, or an
    invalid listing. EXACT_TRADED_BAR_PRESENT means the bar exists in the
    pinned artifact -- it does not by itself prove the provider token,
    provider availability, or identity routing caused the original gap.
    """

    EXACT_TRADED_BAR_PRESENT = "EXACT_TRADED_BAR_PRESENT"
    RELATED_NSE_IDENTITY_CONFLICT = "RELATED_NSE_IDENTITY_CONFLICT"
    NSE_TRADED_BAR_ABSENT = "NSE_TRADED_BAR_ABSENT"


class HistoricalGapAdjudicationAction(str, Enum):
    """Work-routing metadata only. An action never authorizes anything."""

    REVIEW_PINNED_NSE_BAR_FOR_DATASET_USE = "REVIEW_PINNED_NSE_BAR_FOR_DATASET_USE"
    REVIEW_POINT_IN_TIME_IDENTITY = "REVIEW_POINT_IN_TIME_IDENTITY"
    OBTAIN_LISTING_OR_ALTERNATE_PROVIDER_EVIDENCE = (
        "OBTAIN_LISTING_OR_ALTERNATE_PROVIDER_EVIDENCE"
    )


_ACTION_BY_STATUS = {
    HistoricalGapNseStatus.EXACT_TRADED_BAR_PRESENT: (
        HistoricalGapAdjudicationAction.REVIEW_PINNED_NSE_BAR_FOR_DATASET_USE
    ),
    HistoricalGapNseStatus.RELATED_NSE_IDENTITY_CONFLICT: (
        HistoricalGapAdjudicationAction.REVIEW_POINT_IN_TIME_IDENTITY
    ),
    HistoricalGapNseStatus.NSE_TRADED_BAR_ABSENT: (
        HistoricalGapAdjudicationAction.OBTAIN_LISTING_OR_ALTERNATE_PROVIDER_EVIDENCE
    ),
}


def _sha256(value: object, field_name: str) -> None:
    if type(value) is not str or SHA256_IDENTIFIER.fullmatch(value) is None:
        raise ValueError(f"{field_name} must be a lowercase SHA-256")


def _provider(value: object, field_name: str) -> None:
    if type(value) is not str or MARKET_DATA_PROVIDER_PATTERN.fullmatch(value) is None:
        raise ValueError(f"{field_name} must be canonical uppercase provider text")


def _utc(value: object, field_name: str) -> datetime:
    if type(value) is not datetime:
        raise TypeError(f"{field_name} must be an exact datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
    return value.astimezone(timezone.utc)


@dataclass(frozen=True, slots=True)
class HistoricalGapAdjudicationEntry:
    """One gap assessed against pinned NSE EOD evidence for its exact session.

    Never resolves, deletes, or mutates the gap it assesses; never claims the
    absence status proves zero trading, suspension, or delisting.
    """

    gap_evidence_id: str
    plan_id: str
    request_id: str
    original_classification: HistoricalBackfillGapClassification
    provider: str
    provider_version: str
    provider_instrument_id: str
    listing_key: str
    security_series: str
    isin: str
    session: date
    response_observed_at: datetime
    normalized_response_sha256: str
    nse_artifact_id: str
    nse_knowledge_time: datetime
    status: HistoricalGapNseStatus
    exact_nse_bar_id: str | None
    related_nse_bar_ids: tuple[str, ...]
    action: HistoricalGapAdjudicationAction
    collection_only: bool = True
    actionable: bool = False
    gap_resolved: bool = False
    training_eligible: bool = False
    entry_id: str = field(init=False)

    def __post_init__(self) -> None:
        self._validate()
        object.__setattr__(
            self,
            "response_observed_at",
            _utc(self.response_observed_at, "entry response_observed_at"),
        )
        object.__setattr__(
            self,
            "nse_knowledge_time",
            _utc(self.nse_knowledge_time, "entry nse_knowledge_time"),
        )
        object.__setattr__(self, "entry_id", self._calculated_id())

    def _validate(self) -> None:
        _sha256(self.gap_evidence_id, "entry gap_evidence_id")
        _sha256(self.plan_id, "entry plan_id")
        _sha256(self.request_id, "entry request_id")
        if type(self.original_classification) is not HistoricalBackfillGapClassification:
            raise TypeError("entry original_classification must be exact")
        _provider(self.provider, "entry provider")
        if (
            type(self.provider_version) is not str
            or not self.provider_version
            or len(self.provider_version) > 128
        ):
            raise ValueError("entry provider_version must be bounded text")
        if (
            type(self.provider_instrument_id) is not str
            or not self.provider_instrument_id
            or self.provider_instrument_id != self.provider_instrument_id.strip()
            or len(self.provider_instrument_id) > 128
        ):
            raise ValueError("entry provider_instrument_id must be bounded canonical text")
        if (
            type(self.listing_key) is not str
            or LISTING_KEY_PATTERN.fullmatch(self.listing_key) is None
        ):
            raise ValueError("entry listing_key must be canonical NSE:TRADINGSYMBOL text")
        if (
            type(self.security_series) is not str
            or NSE_SECURITY_SERIES_PATTERN.fullmatch(self.security_series) is None
        ):
            raise ValueError("entry security_series must be canonical NSE series text")
        if type(self.isin) is not str or NSE_EQUITY_ISIN_PATTERN.fullmatch(self.isin) is None:
            raise ValueError("entry isin must be a canonical Indian equity ISIN")
        if type(self.session) is not date:
            raise TypeError("entry session must be an exact date")
        _utc(self.response_observed_at, "entry response_observed_at")
        if (
            type(self.normalized_response_sha256) is not str
            or SHA256_IDENTIFIER.fullmatch(self.normalized_response_sha256) is None
        ):
            raise ValueError("entry normalized_response_sha256 must be a lowercase SHA-256")
        _sha256(self.nse_artifact_id, "entry nse_artifact_id")
        _utc(self.nse_knowledge_time, "entry nse_knowledge_time")
        if type(self.status) is not HistoricalGapNseStatus:
            raise TypeError("entry status must be exact")
        if self.exact_nse_bar_id is not None:
            _sha256(self.exact_nse_bar_id, "entry exact_nse_bar_id")
        if (
            type(self.related_nse_bar_ids) is not tuple
            or self.related_nse_bar_ids != tuple(sorted(set(self.related_nse_bar_ids)))
        ):
            raise ValueError("entry related_nse_bar_ids must be a sorted unique tuple")
        for value in self.related_nse_bar_ids:
            _sha256(value, "entry related_nse_bar_ids item")
        if type(self.action) is not HistoricalGapAdjudicationAction:
            raise TypeError("entry action must be exact")
        if self.action != _ACTION_BY_STATUS[self.status]:
            raise ValueError("entry action disagrees with its deterministic status mapping")
        if self.status is HistoricalGapNseStatus.EXACT_TRADED_BAR_PRESENT:
            if self.exact_nse_bar_id is None or self.related_nse_bar_ids:
                raise ValueError("exact-bar entry shape is invalid")
        elif self.status is HistoricalGapNseStatus.RELATED_NSE_IDENTITY_CONFLICT:
            if self.exact_nse_bar_id is not None or not self.related_nse_bar_ids:
                raise ValueError("conflicted entry shape is invalid")
        elif self.exact_nse_bar_id is not None or self.related_nse_bar_ids:
            raise ValueError("absent-bar entry shape is invalid")
        if self.collection_only is not True:
            raise ValueError("gap adjudication entries must remain collection-only")
        if self.actionable is not False:
            raise ValueError("gap adjudication entries cannot authorize trading")
        if self.gap_resolved is not False:
            raise ValueError("gap adjudication cannot resolve a gap")
        if self.training_eligible is not False:
            raise ValueError("gap adjudication entries cannot be training-eligible")

    def _calculated_id(self) -> str:
        return content_id(
            {
                item.name: getattr(self, item.name)
                for item in fields(self)
                if item.name != "entry_id"
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        self._validate()
        if self.entry_id != self._calculated_id():
            raise HistoricalGapAdjudicationIntegrityError(
                "gap adjudication entry identity failed"
            )


@dataclass(frozen=True, slots=True)
class HistoricalGapAdjudicationReport:
    """One sealed, plan-scoped, non-actionable assessment of unresolved gaps.

    This report never adjudicates a gap resolved, never proves a session had
    no trading, and never authorizes training, signals, or capital.
    """

    plan_id: str
    adjudicated_at: datetime
    nse_artifact_ids: tuple[str, ...]
    entries: tuple[HistoricalGapAdjudicationEntry, ...]
    collection_only: bool = True
    actionable: bool = False
    gaps_resolved: bool = False
    training_eligible: bool = False
    schema_version: str = HISTORICAL_GAP_ADJUDICATION_SCHEMA_VERSION
    policy_version: str = HISTORICAL_GAP_ADJUDICATION_POLICY_VERSION
    codec_version: str = HISTORICAL_GAP_ADJUDICATION_CODEC_VERSION
    report_id: str = field(init=False)

    def __post_init__(self) -> None:
        self._validate()
        object.__setattr__(
            self,
            "adjudicated_at",
            _utc(self.adjudicated_at, "report adjudicated_at"),
        )
        object.__setattr__(self, "report_id", self._calculated_id())

    def _validate(self) -> None:
        _sha256(self.plan_id, "report plan_id")
        _utc(self.adjudicated_at, "report adjudicated_at")
        if type(self.nse_artifact_ids) is not tuple or not self.nse_artifact_ids:
            raise TypeError("report nse_artifact_ids must be a non-empty exact tuple")
        for value in self.nse_artifact_ids:
            _sha256(value, "report nse_artifact_ids entry")
        if len(set(self.nse_artifact_ids)) != len(self.nse_artifact_ids):
            raise ValueError("report nse_artifact_ids must be unique")
        if type(self.entries) is not tuple or not self.entries or any(
            type(value) is not HistoricalGapAdjudicationEntry for value in self.entries
        ):
            raise TypeError("report entries must be a non-empty exact immutable tuple")
        if self.entries != tuple(
            sorted(self.entries, key=lambda value: (value.request_id, value.session))
        ):
            raise ValueError("report entries must be request/session-ordered")
        if len({(value.request_id, value.session) for value in self.entries}) != len(
            self.entries
        ):
            raise ValueError("report entries must be unique by request/session")
        session_artifact_ids: dict[date, str] = {}
        for entry in self.entries:
            entry.verify_content_identity()
            if entry.plan_id != self.plan_id:
                raise HistoricalGapAdjudicationIntegrityError(
                    "entry plan_id disagrees with report plan_id"
                )
            existing_artifact_id = session_artifact_ids.get(entry.session)
            if (
                existing_artifact_id is not None
                and existing_artifact_id != entry.nse_artifact_id
            ):
                raise HistoricalGapAdjudicationIntegrityError(
                    "entries disagree about the NSE artifact for one session"
                )
            session_artifact_ids[entry.session] = entry.nse_artifact_id
            if (
                entry.response_observed_at > self.adjudicated_at
                or entry.nse_knowledge_time > self.adjudicated_at
            ):
                raise HistoricalGapAdjudicationIntegrityError(
                    "report adjudicated_at cannot precede its own entry evidence"
                )
        expected_artifact_ids = tuple(
            session_artifact_ids[session] for session in sorted(session_artifact_ids)
        )
        if self.nse_artifact_ids != expected_artifact_ids:
            raise HistoricalGapAdjudicationIntegrityError(
                "report nse_artifact_ids must exactly match entry session "
                "ownership in ascending session order"
            )
        if self.collection_only is not True:
            raise ValueError("gap adjudication reports must remain collection-only")
        if self.actionable is not False:
            raise ValueError("gap adjudication reports cannot authorize trading")
        if self.gaps_resolved is not False:
            raise ValueError("gap adjudication reports cannot resolve gaps")
        if self.training_eligible is not False:
            raise ValueError("gap adjudication reports cannot be training-eligible")
        if (
            self.schema_version != HISTORICAL_GAP_ADJUDICATION_SCHEMA_VERSION
            or self.policy_version != HISTORICAL_GAP_ADJUDICATION_POLICY_VERSION
            or self.codec_version != HISTORICAL_GAP_ADJUDICATION_CODEC_VERSION
        ):
            raise ValueError("unsupported historical gap adjudication contract")

    @property
    def gap_count(self) -> int:
        return len(self.entries)

    def _calculated_id(self) -> str:
        return content_id(
            {
                item.name: getattr(self, item.name)
                for item in fields(self)
                if item.name != "report_id"
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        self._validate()
        if self.report_id != self._calculated_id():
            raise HistoricalGapAdjudicationIntegrityError(
                "gap adjudication report identity failed"
            )


def _classify(
    gap: HistoricalBackfillSessionGapEvidence,
    artifact: NseEodSessionArtifact,
) -> tuple[HistoricalGapNseStatus, str | None, tuple[str, ...]]:
    symbol = gap.listing_key.removeprefix("NSE:")
    exact_matches = tuple(
        bar
        for bar in artifact.bars
        if bar.symbol == symbol
        and bar.series == gap.security_series
        and bar.validated_isin == gap.isin
    )
    if len(exact_matches) > 1:
        raise HistoricalGapAdjudicationIntegrityError(
            "NSE artifact contains ambiguous exact-bar ownership for one gap identity"
        )
    related_matches = tuple(
        bar
        for bar in artifact.bars
        if bar.validated_isin == gap.isin
        or (bar.symbol == symbol and bar.series == gap.security_series)
    )
    if len(exact_matches) == 1 and len(related_matches) == 1:
        return HistoricalGapNseStatus.EXACT_TRADED_BAR_PRESENT, exact_matches[0].bar_id, ()
    if related_matches:
        related_ids = tuple(sorted({bar.bar_id for bar in related_matches}))
        return HistoricalGapNseStatus.RELATED_NSE_IDENTITY_CONFLICT, None, related_ids
    return HistoricalGapNseStatus.NSE_TRADED_BAR_ABSENT, None, ()


def _entry_for_gap(
    gap: HistoricalBackfillSessionGapEvidence,
    artifact: NseEodSessionArtifact,
) -> HistoricalGapAdjudicationEntry:
    status, exact_bar_id, related_bar_ids = _classify(gap, artifact)
    return HistoricalGapAdjudicationEntry(
        gap_evidence_id=gap.evidence_id,
        plan_id=gap.plan_id,
        request_id=gap.request_id,
        original_classification=gap.classification,
        provider=gap.provider,
        provider_version=gap.provider_version,
        provider_instrument_id=gap.provider_instrument_id,
        listing_key=gap.listing_key,
        security_series=gap.security_series,
        isin=gap.isin,
        session=gap.session,
        response_observed_at=gap.response_observed_at,
        normalized_response_sha256=gap.normalized_response_sha256,
        nse_artifact_id=artifact.artifact_id,
        nse_knowledge_time=artifact.knowledge_time,
        status=status,
        exact_nse_bar_id=exact_bar_id,
        related_nse_bar_ids=related_bar_ids,
        action=_ACTION_BY_STATUS[status],
    )


def build_historical_gap_adjudication_report(
    *,
    gaps: tuple[HistoricalBackfillSessionGapEvidence, ...],
    nse_artifacts: tuple[NseEodSessionArtifact, ...],
    adjudicated_at: datetime,
) -> HistoricalGapAdjudicationReport:
    """Assess every gap against its exact-session pinned NSE artifact.

    Never deletes, resolves, or mutates a gap; never synthesizes a candle;
    never claims absence from a TRADED_ROWS_ONLY artifact proves zero trading.
    """

    if type(gaps) is not tuple or not gaps or any(
        type(value) is not HistoricalBackfillSessionGapEvidence for value in gaps
    ):
        raise HistoricalGapAdjudicationError(
            "gaps must be a non-empty exact immutable tuple"
        )
    for gap in gaps:
        gap.verify_content_identity()
    plan_id = gaps[0].plan_id
    if any(value.plan_id != plan_id for value in gaps):
        raise HistoricalGapAdjudicationError("gaps must all belong to one exact plan")
    if gaps != tuple(
        sorted(gaps, key=lambda value: (value.request_id, value.session))
    ):
        raise HistoricalGapAdjudicationError(
            "gaps must be sorted by (request_id, session)"
        )
    if len({(value.request_id, value.session) for value in gaps}) != len(gaps):
        raise HistoricalGapAdjudicationError(
            "gaps must be unique by (request_id, session)"
        )

    if type(nse_artifacts) is not tuple or not nse_artifacts or any(
        type(value) is not NseEodSessionArtifact for value in nse_artifacts
    ):
        raise HistoricalGapAdjudicationError(
            "nse_artifacts must be a non-empty exact immutable tuple"
        )
    for artifact in nse_artifacts:
        artifact.verify_content_identity()
    by_session = {value.market_session: value for value in nse_artifacts}
    if len(by_session) != len(nse_artifacts):
        raise HistoricalGapAdjudicationError("nse_artifacts must be session-unique")
    gap_sessions = {value.session for value in gaps}
    if set(by_session) != gap_sessions:
        raise HistoricalGapAdjudicationError(
            "nse_artifacts sessions must exactly equal gap sessions"
        )

    adjudicated_at = _utc(adjudicated_at, "gap adjudication adjudicated_at")
    if any(adjudicated_at < value.response_observed_at for value in gaps) or any(
        adjudicated_at < value.knowledge_time for value in nse_artifacts
    ):
        raise HistoricalGapAdjudicationError(
            "gap adjudication cannot precede required evidence"
        )

    entries = tuple(_entry_for_gap(gap, by_session[gap.session]) for gap in gaps)
    nse_artifact_ids = tuple(
        by_session[session].artifact_id for session in sorted(by_session)
    )
    return HistoricalGapAdjudicationReport(
        plan_id=plan_id,
        adjudicated_at=adjudicated_at,
        nse_artifact_ids=nse_artifact_ids,
        entries=entries,
    )


def _entry_value(entry: HistoricalGapAdjudicationEntry) -> dict[str, object]:
    return {
        "entry_id": entry.entry_id,
        "gap_evidence_id": entry.gap_evidence_id,
        "plan_id": entry.plan_id,
        "request_id": entry.request_id,
        "original_classification": entry.original_classification.value,
        "provider": entry.provider,
        "provider_version": entry.provider_version,
        "provider_instrument_id": entry.provider_instrument_id,
        "listing_key": entry.listing_key,
        "security_series": entry.security_series,
        "isin": entry.isin,
        "session": entry.session.isoformat(),
        "response_observed_at": entry.response_observed_at.isoformat(),
        "normalized_response_sha256": entry.normalized_response_sha256,
        "nse_artifact_id": entry.nse_artifact_id,
        "nse_knowledge_time": entry.nse_knowledge_time.isoformat(),
        "status": entry.status.value,
        "exact_nse_bar_id": entry.exact_nse_bar_id,
        "related_nse_bar_ids": list(entry.related_nse_bar_ids),
        "action": entry.action.value,
        "collection_only": entry.collection_only,
        "actionable": entry.actionable,
        "gap_resolved": entry.gap_resolved,
        "training_eligible": entry.training_eligible,
    }


_EXPECTED_ENTRY_KEYS = {
    "entry_id",
    "gap_evidence_id",
    "plan_id",
    "request_id",
    "original_classification",
    "provider",
    "provider_version",
    "provider_instrument_id",
    "listing_key",
    "security_series",
    "isin",
    "session",
    "response_observed_at",
    "normalized_response_sha256",
    "nse_artifact_id",
    "nse_knowledge_time",
    "status",
    "exact_nse_bar_id",
    "related_nse_bar_ids",
    "action",
    "collection_only",
    "actionable",
    "gap_resolved",
    "training_eligible",
}

_EXPECTED_REPORT_KEYS = {
    "schema_version",
    "policy_version",
    "codec_version",
    "report_id",
    "plan_id",
    "adjudicated_at",
    "nse_artifact_ids",
    "entries",
    "collection_only",
    "actionable",
    "gaps_resolved",
    "training_eligible",
}


def encode_historical_gap_adjudication_report(
    report: HistoricalGapAdjudicationReport,
) -> bytes:
    if type(report) is not HistoricalGapAdjudicationReport:
        raise TypeError("report must be an exact HistoricalGapAdjudicationReport")
    report.verify_content_identity()
    value = {
        "schema_version": report.schema_version,
        "policy_version": report.policy_version,
        "codec_version": report.codec_version,
        "report_id": report.report_id,
        "plan_id": report.plan_id,
        "adjudicated_at": report.adjudicated_at.isoformat(),
        "nse_artifact_ids": list(report.nse_artifact_ids),
        "entries": [_entry_value(item) for item in report.entries],
        "collection_only": report.collection_only,
        "actionable": report.actionable,
        "gaps_resolved": report.gaps_resolved,
        "training_eligible": report.training_eligible,
    }
    return (
        json.dumps(
            value,
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        )
        + "\n"
    ).encode("utf-8")


def _reject_duplicate_keys(pairs: list[tuple[str, object]]) -> dict[str, object]:
    value: dict[str, object] = {}
    for key, item in pairs:
        if key in value:
            raise HistoricalGapAdjudicationError(
                "gap adjudication report contains duplicate JSON keys"
            )
        value[key] = item
    return value


def decode_historical_gap_adjudication_report(
    payload: bytes,
) -> HistoricalGapAdjudicationReport:
    try:
        if type(payload) is not bytes:
            raise TypeError
        if not payload:
            raise ValueError
        if len(payload) > MAXIMUM_GAP_ADJUDICATION_REPORT_BYTES:
            raise ValueError
        root = json.loads(
            payload.decode("utf-8"),
            object_pairs_hook=_reject_duplicate_keys,
            parse_float=lambda _: (_ for _ in ()).throw(ValueError()),
            parse_constant=lambda _: (_ for _ in ()).throw(ValueError()),
        )
        if type(root) is not dict or set(root) != _EXPECTED_REPORT_KEYS:
            raise ValueError
        raw_entries = root["entries"]
        if type(raw_entries) is not list:
            raise ValueError
        entries: list[HistoricalGapAdjudicationEntry] = []
        for value in raw_entries:
            if type(value) is not dict or set(value) != _EXPECTED_ENTRY_KEYS:
                raise ValueError
            entry = HistoricalGapAdjudicationEntry(
                gap_evidence_id=value["gap_evidence_id"],
                plan_id=value["plan_id"],
                request_id=value["request_id"],
                original_classification=HistoricalBackfillGapClassification(
                    value["original_classification"]
                ),
                provider=value["provider"],
                provider_version=value["provider_version"],
                provider_instrument_id=value["provider_instrument_id"],
                listing_key=value["listing_key"],
                security_series=value["security_series"],
                isin=value["isin"],
                session=date.fromisoformat(value["session"]),
                response_observed_at=datetime.fromisoformat(
                    value["response_observed_at"]
                ),
                normalized_response_sha256=value["normalized_response_sha256"],
                nse_artifact_id=value["nse_artifact_id"],
                nse_knowledge_time=datetime.fromisoformat(
                    value["nse_knowledge_time"]
                ),
                status=HistoricalGapNseStatus(value["status"]),
                exact_nse_bar_id=value["exact_nse_bar_id"],
                related_nse_bar_ids=tuple(value["related_nse_bar_ids"]),
                action=HistoricalGapAdjudicationAction(value["action"]),
                collection_only=value["collection_only"],
                actionable=value["actionable"],
                gap_resolved=value["gap_resolved"],
                training_eligible=value["training_eligible"],
            )
            if value["entry_id"] != entry.entry_id:
                raise ValueError
            entries.append(entry)
        report = HistoricalGapAdjudicationReport(
            plan_id=root["plan_id"],
            adjudicated_at=datetime.fromisoformat(root["adjudicated_at"]),
            nse_artifact_ids=tuple(root["nse_artifact_ids"]),
            entries=tuple(entries),
            collection_only=root["collection_only"],
            actionable=root["actionable"],
            gaps_resolved=root["gaps_resolved"],
            training_eligible=root["training_eligible"],
            schema_version=root["schema_version"],
            policy_version=root["policy_version"],
            codec_version=root["codec_version"],
        )
        if root["report_id"] != report.report_id:
            raise ValueError
        if payload != encode_historical_gap_adjudication_report(report):
            raise ValueError
        return report
    except HistoricalGapAdjudicationError:
        raise
    except (
        AttributeError,
        KeyError,
        TypeError,
        ValueError,
        UnicodeDecodeError,
        json.JSONDecodeError,
    ):
        raise HistoricalGapAdjudicationIntegrityError(
            "stored gap adjudication report is invalid"
        ) from None


def _is_link_like(path: Path) -> bool:
    is_junction = getattr(path, "is_junction", None)
    return path.is_symlink() or bool(is_junction and is_junction())


def _write_fsynced(path: Path, payload: bytes) -> None:
    with path.open("xb") as handle:
        handle.write(payload)
        handle.flush()
        os.fsync(handle.fileno())


class LocalHistoricalGapAdjudicationReportStore:
    """Create-once local store; exposes only exact-ID get, never a listing."""

    def __init__(self, root: Path) -> None:
        self.root = Path(root)

    @property
    def dataset_root(self) -> Path:
        return self.root / HISTORICAL_GAP_ADJUDICATION_DATASET

    def put(
        self,
        report: HistoricalGapAdjudicationReport,
    ) -> HistoricalGapAdjudicationReport:
        if type(report) is not HistoricalGapAdjudicationReport:
            raise TypeError("report must be an exact HistoricalGapAdjudicationReport")
        report.verify_content_identity()
        payload = encode_historical_gap_adjudication_report(report)
        if len(payload) > MAXIMUM_GAP_ADJUDICATION_REPORT_BYTES:
            raise HistoricalGapAdjudicationError(
                "gap adjudication report exceeds its size limit"
            )
        self.dataset_root.mkdir(parents=True, exist_ok=True)
        target = self.dataset_root / report.report_id
        lock = self.dataset_root / ".gap-adjudication-reports.lock"
        try:
            with advisory_file_lock(lock):
                if target.exists():
                    existing = self._read_path(target)
                    if existing != report:
                        raise HistoricalGapAdjudicationIntegrityError(
                            "report ID already stores different content"
                        )
                    return existing
                temporary = Path(
                    tempfile.mkdtemp(
                        prefix=".gap-adjudication-",
                        dir=self.dataset_root,
                    )
                )
                try:
                    _write_fsynced(temporary / REPORT_FILENAME, payload)
                    os.replace(temporary, target)
                except Exception:
                    shutil.rmtree(temporary, ignore_errors=True)
                    raise
        except (FileLockUnavailable, FileSafetyError):
            raise HistoricalGapAdjudicationIntegrityError(
                "gap adjudication report store is unavailable"
            ) from None
        return self._read_path(target)

    def get(self, report_id: str) -> HistoricalGapAdjudicationReport:
        _sha256(report_id, "gap adjudication report_id")
        target = self.dataset_root / report_id
        if not target.exists():
            raise HistoricalGapAdjudicationError(
                "gap adjudication report was not found"
            )
        return self._read_path(target)

    def _read_path(self, target: Path) -> HistoricalGapAdjudicationReport:
        try:
            if not target.is_dir() or _is_link_like(target):
                raise HistoricalGapAdjudicationIntegrityError(
                    "gap adjudication report path is invalid"
                )
            children = tuple(target.iterdir())
            if (
                {value.name for value in children} != {REPORT_FILENAME}
                or any(
                    _is_link_like(value) or not value.is_file()
                    for value in children
                )
            ):
                raise HistoricalGapAdjudicationIntegrityError(
                    "gap adjudication report directory is invalid"
                )
            payload = read_stable_regular_file(
                target / REPORT_FILENAME,
                maximum_bytes=MAXIMUM_GAP_ADJUDICATION_REPORT_BYTES,
            )
            report = decode_historical_gap_adjudication_report(payload)
            if (
                target.name != report.report_id
                or payload != encode_historical_gap_adjudication_report(report)
            ):
                raise HistoricalGapAdjudicationIntegrityError(
                    "gap adjudication report storage identity failed"
                )
            return report
        except HistoricalGapAdjudicationIntegrityError:
            raise
        except (FileSafetyError, OSError):
            raise HistoricalGapAdjudicationIntegrityError(
                "gap adjudication report could not be read safely"
            ) from None
