from __future__ import annotations

import json
import os
import re
import tempfile
from collections import defaultdict
from collections.abc import Callable
from dataclasses import dataclass, field, fields
from datetime import date, datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Protocol

from india_swing.domain.models import INDIA_STANDARD_TIME
from india_swing.identity import content_id
from india_swing.identity_decisions import (
    ADJUDICATED_IDENTITY_POLICY_VERSION,
    STABLE_INSTRUMENT_ID_SCHEME,
    STABLE_LISTING_ID_SCHEME,
    AdjudicatedIdentitySnapshot,
)
from india_swing.identity_registry import build_identity_adjudication_queue
from india_swing.identity_registry.models import (
    CrossVintageIdentityRegistry,
    IdentityCandidateBasis,
    IdentityCandidateStatus,
    IdentityObservation,
)
from india_swing.reference.calendar import CalendarSnapshot
from india_swing.reference_data.artifact_store import (
    verify_stored_reference_provenance,
)
from india_swing.reference_data.models import StoredReferenceArtifact

from .collection import (
    HistoricalMarketDataCollector,
    historical_dataset_name,
)
from .models import (
    HistoricalDailyCandleBatch,
    HistoricalDailyRequest,
    HistoricalInstrumentBinding,
    MARKET_DATA_PROVIDER_PATTERN,
    NSE_EQUITY_ISIN_PATTERN,
    NSE_SECURITY_SERIES_PATTERN,
    SHA256_IDENTIFIER,
)
from .provider import HistoricalDailyDataConnector
from .snapshot_store import (
    LocalMarketSnapshotStore,
    StoredMarketSnapshot,
)
from .upstox_instruments import UpstoxNseInstrumentCatalog


HISTORICAL_BACKFILL_PLAN_SCHEMA_VERSION = "historical-backfill-plan/v2"
HISTORICAL_BACKFILL_PROGRESS_SCHEMA_VERSION = "historical-backfill-progress/v1"
HISTORICAL_BACKFILL_STATE_DATASET = "historical-backfill-state"
UPSTOX_ISIN_RESOLVER_VERSION = "upstox-nse-eq-isin/v1"
UPSTOX_CATALOG_RESOLVER_VERSION = "upstox-nse-pinned-catalog/v1"
PROGRESS_FILENAME = "progress.json"
SUPPORTED_SWING_SECURITY_SERIES = frozenset({"EQ", "SM"})

_CANONICAL_TEXT = re.compile(r"[A-Za-z0-9][A-Za-z0-9._/@:+\-]{0,127}\Z")


class HistoricalBackfillError(ValueError):
    pass


class HistoricalBackfillIntegrityError(HistoricalBackfillError):
    pass


class HistoricalBackfillStateError(HistoricalBackfillError):
    pass


class HistoricalBackfillIssueCode(str, Enum):
    MISSING_SECURITY_MASTER_VINTAGE = "MISSING_SECURITY_MASTER_VINTAGE"
    NON_SESSION_SECURITY_MASTER = "NON_SESSION_SECURITY_MASTER"
    CONFLICTING_IDENTITY = "CONFLICTING_IDENTITY"
    UNVALIDATED_IDENTIFIER = "UNVALIDATED_IDENTIFIER"
    DELETED_SECURITY = "DELETED_SECURITY"
    INELIGIBLE_NORMAL_MARKET = "INELIGIBLE_NORMAL_MARKET"
    PROVIDER_KEY_UNAVAILABLE = "PROVIDER_KEY_UNAVAILABLE"
    PROVIDER_CATALOG_ABSENT = "PROVIDER_CATALOG_ABSENT"
    AMBIGUOUS_PROVIDER_KEY = "AMBIGUOUS_PROVIDER_KEY"
    UNSUPPORTED_LISTING_LANE = "UNSUPPORTED_LISTING_LANE"


def _utc(value: datetime, field_name: str) -> datetime:
    if not isinstance(value, datetime):
        raise TypeError(f"{field_name} must be a datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
    return value.astimezone(timezone.utc)


def _sha256(value: str, field_name: str) -> None:
    if type(value) is not str or SHA256_IDENTIFIER.fullmatch(value) is None:
        raise ValueError(f"{field_name} must be a lowercase SHA-256")


def _provider(value: str) -> None:
    if type(value) is not str or MARKET_DATA_PROVIDER_PATTERN.fullmatch(value) is None:
        raise ValueError("provider must be canonical uppercase provider text")


class ProviderInstrumentResolver(Protocol):
    @property
    def provider(self) -> str: ...

    @property
    def resolver_version(self) -> str: ...

    def resolve(self, observation: IdentityObservation) -> str: ...

    def resolve_isin(self, isin: str) -> str: ...


class UpstoxIsinInstrumentResolver:
    """Derive the documented Upstox NSE equity key from a validated ISIN."""

    @property
    def provider(self) -> str:
        return "UPSTOX"

    @property
    def resolver_version(self) -> str:
        return UPSTOX_ISIN_RESOLVER_VERSION

    def resolve(self, observation: IdentityObservation) -> str:
        if type(observation) is not IdentityObservation:
            raise TypeError("observation must be an exact IdentityObservation")
        observation.verify_content_identity()
        if observation.validated_isin is None:
            raise HistoricalBackfillIntegrityError(
                "provider instrument resolution requires a validated ISIN"
            )
        return self.resolve_isin(observation.validated_isin)

    def resolve_isin(self, isin: str) -> str:
        if (
            type(isin) is not str
            or NSE_EQUITY_ISIN_PATTERN.fullmatch(isin) is None
        ):
            raise HistoricalBackfillIntegrityError(
                "provider instrument resolution requires a validated ISIN"
            )
        return f"NSE_EQ|{isin}"


class UpstoxCatalogInstrumentResolver:
    """Validate Upstox ISIN routing through a sealed current BOD catalog.

    The catalog is provider-routing evidence observed at collection time. NSE
    point-in-time security masters remain the authority for historical universe
    membership, so a catalog miss is reported rather than silently deleting the
    historical listing.
    """

    def __init__(self, catalog: UpstoxNseInstrumentCatalog) -> None:
        if type(catalog) is not UpstoxNseInstrumentCatalog:
            raise TypeError("catalog must be an exact UpstoxNseInstrumentCatalog")
        catalog.verify_content_identity()
        self.catalog = catalog
        by_isin: dict[str, list[str]] = defaultdict(list)
        for value in catalog.instruments:
            by_isin[value.isin].append(value.instrument_key)
        self._by_isin = {
            key: tuple(sorted(set(values)))
            for key, values in by_isin.items()
        }

    @property
    def provider(self) -> str:
        return "UPSTOX"

    @property
    def resolver_version(self) -> str:
        return (
            f"{UPSTOX_CATALOG_RESOLVER_VERSION}@sha256:"
            f"{self.catalog.catalog_id}"
        )

    @property
    def knowledge_time(self) -> datetime:
        return self.catalog.observed_at

    def resolve(self, observation: IdentityObservation) -> str:
        if type(observation) is not IdentityObservation:
            raise TypeError("observation must be an exact IdentityObservation")
        observation.verify_content_identity()
        if observation.validated_isin is None:
            raise HistoricalBackfillIntegrityError(
                "provider instrument resolution requires a validated ISIN"
            )
        return self.resolve_isin(observation.validated_isin)

    def resolve_isin(self, isin: str) -> str:
        if (
            type(isin) is not str
            or NSE_EQUITY_ISIN_PATTERN.fullmatch(isin) is None
        ):
            raise HistoricalBackfillIntegrityError(
                "provider instrument resolution requires a validated ISIN"
            )
        expected = f"NSE_EQ|{isin}"
        matches = self._by_isin.get(isin, ())
        if matches and matches != (expected,):
            raise HistoricalBackfillIntegrityError(
                "pinned Upstox catalog contains inconsistent ISIN routing"
            )
        return expected

    def catalog_contains(self, observation: IdentityObservation) -> bool:
        if type(observation) is not IdentityObservation:
            raise TypeError("observation must be an exact IdentityObservation")
        observation.verify_content_identity()
        return observation.validated_isin is not None and (
            self.catalog_contains_isin(observation.validated_isin)
        )

    def catalog_contains_isin(self, isin: str) -> bool:
        if (
            type(isin) is not str
            or NSE_EQUITY_ISIN_PATTERN.fullmatch(isin) is None
        ):
            raise HistoricalBackfillIntegrityError(
                "catalog membership requires a validated ISIN"
            )
        return isin in self._by_isin


@dataclass(frozen=True, slots=True)
class HistoricalBackfillIssue:
    code: HistoricalBackfillIssueCode
    affected_dates: tuple[date, ...]
    observation_ids: tuple[str, ...] = ()
    issue_id: str = field(init=False)

    def __post_init__(self) -> None:
        if type(self.code) is not HistoricalBackfillIssueCode:
            raise TypeError("backfill issue code must be exact")
        if (
            type(self.affected_dates) is not tuple
            or not self.affected_dates
            or any(type(value) is not date for value in self.affected_dates)
            or self.affected_dates != tuple(sorted(set(self.affected_dates)))
        ):
            raise ValueError("affected_dates must be sorted unique exact dates")
        if (
            type(self.observation_ids) is not tuple
            or self.observation_ids != tuple(sorted(set(self.observation_ids)))
        ):
            raise ValueError("issue observation IDs must be sorted and unique")
        for value in self.observation_ids:
            _sha256(value, "issue observation ID")
        object.__setattr__(self, "issue_id", self._calculated_id())

    def _calculated_id(self) -> str:
        return content_id(
            {
                "schema": HISTORICAL_BACKFILL_PLAN_SCHEMA_VERSION,
                "code": self.code,
                "affected_dates": self.affected_dates,
                "observation_ids": self.observation_ids,
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        if self.issue_id != self._calculated_id():
            raise HistoricalBackfillIntegrityError(
                "historical backfill issue identity failed"
            )

    @property
    def blocks_collection(self) -> bool:
        return self.code not in {
            HistoricalBackfillIssueCode.DELETED_SECURITY,
            HistoricalBackfillIssueCode.INELIGIBLE_NORMAL_MARKET,
            HistoricalBackfillIssueCode.PROVIDER_CATALOG_ABSENT,
            HistoricalBackfillIssueCode.UNSUPPORTED_LISTING_LANE,
        }


def _request_sort_key(request: HistoricalDailyRequest) -> tuple[object, ...]:
    return (
        request.sessions[0],
        request.binding.listing_key,
        request.binding.security_series,
        request.binding.isin,
        request.binding.provider_instrument_id,
        request.request_id,
    )


@dataclass(frozen=True, slots=True)
class HistoricalBackfillPlan:
    provider: str
    resolver_version: str
    identity_registry_id: str
    calendar_snapshot_id: str
    coverage_start: date
    coverage_end: date
    requested_at: datetime
    requests: tuple[HistoricalDailyRequest, ...]
    issues: tuple[HistoricalBackfillIssue, ...]
    identity_snapshot_id: str | None = None
    collection_only: bool = True
    schema_version: str = HISTORICAL_BACKFILL_PLAN_SCHEMA_VERSION
    plan_id: str = field(init=False)

    def __post_init__(self) -> None:
        _provider(self.provider)
        if (
            type(self.resolver_version) is not str
            or _CANONICAL_TEXT.fullmatch(self.resolver_version) is None
        ):
            raise ValueError("resolver_version must be bounded canonical text")
        _sha256(self.identity_registry_id, "identity_registry_id")
        if self.identity_snapshot_id is not None:
            _sha256(self.identity_snapshot_id, "identity_snapshot_id")
        _sha256(self.calendar_snapshot_id, "calendar_snapshot_id")
        if type(self.coverage_start) is not date or type(self.coverage_end) is not date:
            raise TypeError("plan coverage bounds must be exact dates")
        if self.coverage_end < self.coverage_start:
            raise ValueError("plan coverage interval is reversed")
        object.__setattr__(
            self,
            "requested_at",
            _utc(self.requested_at, "backfill plan requested_at"),
        )
        if self.schema_version != HISTORICAL_BACKFILL_PLAN_SCHEMA_VERSION:
            raise ValueError("unsupported historical backfill plan schema")
        if self.collection_only is not True:
            raise ValueError("historical backfill plans must remain collection-only")
        if type(self.requests) is not tuple or any(
            type(value) is not HistoricalDailyRequest for value in self.requests
        ):
            raise TypeError("plan requests must be an exact immutable tuple")
        if self.requests != tuple(sorted(self.requests, key=_request_sort_key)):
            raise ValueError("plan requests must be deterministically sorted")
        if len({value.request_id for value in self.requests}) != len(self.requests):
            raise ValueError("plan request IDs must be unique")

        covered_provider_sessions: set[tuple[str, date]] = set()
        for request in self.requests:
            request.verify_content_identity()
            if (
                request.binding.provider != self.provider
                or request.requested_at != self.requested_at
                or request.sessions[0] < self.coverage_start
                or request.sessions[-1] > self.coverage_end
            ):
                raise ValueError("historical request disagrees with its plan")
            for session in request.sessions:
                key = (request.binding.provider_instrument_id, session)
                if key in covered_provider_sessions:
                    raise ValueError(
                        "provider instrument/session occurs in multiple plan requests"
                    )
                covered_provider_sessions.add(key)

        if type(self.issues) is not tuple or any(
            type(value) is not HistoricalBackfillIssue for value in self.issues
        ):
            raise TypeError("plan issues must be an exact immutable tuple")
        if tuple(value.issue_id for value in self.issues) != tuple(
            sorted({value.issue_id for value in self.issues})
        ):
            raise ValueError("plan issues must be sorted and unique")
        for issue in self.issues:
            issue.verify_content_identity()
            if (
                issue.affected_dates[0] < self.coverage_start
                or issue.affected_dates[-1] > self.coverage_end
            ):
                raise ValueError("plan issue lies outside requested coverage")
        object.__setattr__(self, "plan_id", self._calculated_id())

    @property
    def safe_request_count(self) -> int:
        return len(self.requests)

    @property
    def safe_session_count(self) -> int:
        return sum(len(value.sessions) for value in self.requests)

    @property
    def has_coverage_issues(self) -> bool:
        return bool(self.issues)

    @property
    def blocking_issue_count(self) -> int:
        return sum(value.blocks_collection for value in self.issues)

    @property
    def exclusion_issue_count(self) -> int:
        return sum(
            value.code
            in {
                HistoricalBackfillIssueCode.DELETED_SECURITY,
                HistoricalBackfillIssueCode.INELIGIBLE_NORMAL_MARKET,
                HistoricalBackfillIssueCode.UNSUPPORTED_LISTING_LANE,
            }
            for value in self.issues
        )

    @property
    def warning_issue_count(self) -> int:
        return sum(
            value.code is HistoricalBackfillIssueCode.PROVIDER_CATALOG_ABSENT
            for value in self.issues
        )

    @property
    def has_blocking_issues(self) -> bool:
        return any(value.blocks_collection for value in self.issues)

    def _calculated_id(self) -> str:
        return content_id(
            {
                item.name: getattr(self, item.name)
                for item in fields(self)
                if item.name != "plan_id"
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        for request in self.requests:
            request.verify_content_identity()
        for issue in self.issues:
            issue.verify_content_identity()
        if self.plan_id != self._calculated_id():
            raise HistoricalBackfillIntegrityError(
                "historical backfill plan identity failed"
            )


def _issue(
    code: HistoricalBackfillIssueCode,
    dates: tuple[date, ...],
    observations: tuple[IdentityObservation, ...] = (),
) -> HistoricalBackfillIssue:
    return HistoricalBackfillIssue(
        code=code,
        affected_dates=dates,
        observation_ids=tuple(sorted(value.observation_id for value in observations)),
    )


def _runs(
    values: tuple[date, ...],
    session_position: dict[date, int],
) -> tuple[tuple[date, ...], ...]:
    if not values:
        return ()
    result: list[tuple[date, ...]] = []
    current = [values[0]]
    for value in values[1:]:
        if session_position[value] == session_position[current[-1]] + 1:
            current.append(value)
        else:
            result.append(tuple(current))
            current = [value]
    result.append(tuple(current))
    return tuple(result)


def build_historical_backfill_plan(
    *,
    registry: CrossVintageIdentityRegistry,
    security_master_sources: tuple[StoredReferenceArtifact, ...],
    calendar: CalendarSnapshot,
    resolver: ProviderInstrumentResolver,
    coverage_start: date,
    coverage_end: date,
    requested_at: datetime,
    identity_snapshot: AdjudicatedIdentitySnapshot | None = None,
) -> HistoricalBackfillPlan:
    """Build only exact positive-observation runs; never interpolate absence."""

    if type(registry) is not CrossVintageIdentityRegistry:
        raise TypeError("registry must be an exact CrossVintageIdentityRegistry")
    if type(calendar) is not CalendarSnapshot:
        raise TypeError("calendar must be an exact CalendarSnapshot")
    registry.verify_content_identity()
    calendar.verify_content_identity()
    if (
        type(security_master_sources) is not tuple
        or not security_master_sources
        or any(
            type(value) is not StoredReferenceArtifact
            for value in security_master_sources
        )
    ):
        raise TypeError(
            "security_master_sources must be a non-empty exact artifact tuple"
        )
    for source in security_master_sources:
        verify_stored_reference_provenance(source)
    if (
        tuple(
            value.manifest.artifact_id for value in security_master_sources
        )
        != registry.source_artifact_ids
        or tuple(value.manifest for value in security_master_sources)
        != registry.source_manifests
    ):
        raise HistoricalBackfillError(
            "security-master source lineage disagrees with the identity registry"
        )
    records_by_source = {
        source.manifest.artifact_id: {
            value.source_record_id: value for value in source.parsed.records
        }
        for source in security_master_sources
    }
    source_record_by_observation = {}
    for observation in registry.observations:
        record = records_by_source.get(
            observation.source_artifact_id,
            {},
        ).get(observation.source_record_id)
        if (
            record is None
            or record.normalized_row_sha256
            != observation.normalized_row_sha256
            or record.financial_instrument_id
            != observation.financial_instrument_id
            or record.ticker_symbol != observation.ticker_symbol
            or record.security_series != observation.security_series
            or record.instrument_name != observation.instrument_name
            or record.raw_source_identifier
            != observation.raw_source_identifier
            or record.validated_isin != observation.validated_isin
            or record.delete_flag != observation.delete_flag
        ):
            raise HistoricalBackfillError(
                "security-master source record disagrees with identity observation"
            )
        source_record_by_observation[observation.observation_id] = record
    if (calendar.exchange, calendar.segment) != ("NSE", "CM"):
        raise HistoricalBackfillError("historical backfill calendar must be NSE CM")
    if type(coverage_start) is not date or type(coverage_end) is not date:
        raise TypeError("coverage bounds must be exact dates")
    if (
        coverage_end < coverage_start
        or coverage_start < calendar.coverage_start
        or coverage_end > calendar.coverage_end
    ):
        raise HistoricalBackfillError(
            "historical backfill coverage is outside the pinned calendar"
        )
    requested_at = _utc(requested_at, "historical backfill requested_at")
    if registry.cutoff > requested_at or calendar.cutoff > requested_at:
        raise HistoricalBackfillError(
            "historical backfill input was not known at requested_at"
        )
    if coverage_end >= requested_at.astimezone(INDIA_STANDARD_TIME).date():
        raise HistoricalBackfillError(
            "historical backfill coverage cannot include current or future dates"
        )
    observations_by_id = {
        value.observation_id: value for value in registry.observations
    }
    candidates_by_id = {
        value.candidate_id: value for value in registry.candidates
    }
    candidate_id_by_observation = {
        observation_id: candidate.candidate_id
        for candidate in registry.candidates
        for observation_id in candidate.observation_ids
    }
    adjudicated_by_observation = {}
    if identity_snapshot is not None:
        if type(identity_snapshot) is not AdjudicatedIdentitySnapshot:
            raise TypeError(
                "identity_snapshot must be an exact AdjudicatedIdentitySnapshot"
            )
        identity_snapshot.verify_content_identity()
        if (
            identity_snapshot.source_registry_id != registry.registry_id
            or identity_snapshot.cutoff > requested_at
            or identity_snapshot.knowledge_time > requested_at
            or {
                value.candidate_id for value in identity_snapshot.resolutions
            }
            != set(candidates_by_id)
        ):
            raise HistoricalBackfillError(
                "adjudicated identity snapshot lineage is incompatible with the plan"
            )
        resolutions = {
            value.candidate_id: value
            for value in identity_snapshot.resolutions
        }
        queue = build_identity_adjudication_queue(registry)
        if identity_snapshot.source_queue_id != queue.queue_id:
            raise HistoricalBackfillError(
                "adjudicated identity snapshot targets another queue"
            )
        cases = {value.candidate_id: value for value in queue.cases}
        for candidate_id, resolution in resolutions.items():
            case = cases[candidate_id]
            if (
                resolution.required_requirements != case.requirements
                or len(resolution.accepted_decision_ids)
                != len(case.requirements)
                - len(resolution.missing_requirements)
                or resolution.rejected_decision_ids
                and resolution.stable_instrument_id is not None
            ):
                raise HistoricalBackfillError(
                    "adjudicated identity resolution disagrees with its queue"
                )
        for value in identity_snapshot.listing_observations:
            observation = observations_by_id.get(value.source_observation_id)
            candidate_id = candidate_id_by_observation.get(
                value.source_observation_id
            )
            candidate = candidates_by_id.get(value.candidate_id)
            resolution = resolutions.get(value.candidate_id)
            expected_instrument_id = content_id(
                {
                    "scheme": STABLE_INSTRUMENT_ID_SCHEME,
                    "exchange": "NSE",
                    "segment": "CM",
                    "validated_isin": value.isin,
                },
                length=64,
            )
            expected_listing_id = content_id(
                {
                    "scheme": STABLE_LISTING_ID_SCHEME,
                    "stable_instrument_id": expected_instrument_id,
                    "exchange": "NSE",
                    "segment": "CM",
                    "series": value.series,
                },
                length=64,
            )
            if (
                observation is None
                or candidate is None
                or candidate_id != value.candidate_id
                or resolution is None
                or resolution.blocker_codes
                or resolution.missing_requirements
                or resolution.rejected_decision_ids
                or len(resolution.accepted_decision_ids)
                != len(resolution.required_requirements)
                or resolution.stable_instrument_id
                != value.stable_instrument_id
                or value.stable_instrument_id != expected_instrument_id
                or value.stable_listing_id != expected_listing_id
                or observation.claimed_report_date != value.effective_on
                or observation.ticker_symbol != value.symbol
                or observation.security_series != value.series
                or value.source_observation_id in adjudicated_by_observation
                or (
                    (
                        candidate.basis
                        is not IdentityCandidateBasis.VALIDATED_ISIN
                        or candidate.status is IdentityCandidateStatus.CONFLICT
                    )
                    and identity_snapshot.policy_version
                    != ADJUDICATED_IDENTITY_POLICY_VERSION
                )
                or (
                    (
                        candidate.basis
                        is not IdentityCandidateBasis.VALIDATED_ISIN
                        or candidate.status is IdentityCandidateStatus.CONFLICT
                    )
                    and (
                        not identity_snapshot.evidence_artifact_ids
                        or not identity_snapshot.review_bundle_ids
                    )
                )
            ):
                raise HistoricalBackfillError(
                    "adjudicated listing evidence disagrees with the identity registry"
                )
            adjudicated_by_observation[value.source_observation_id] = value
    try:
        provider = resolver.provider
        resolver_version = resolver.resolver_version
    except Exception:
        raise HistoricalBackfillError(
            "historical provider resolver metadata is unavailable"
        ) from None
    _provider(provider)
    if (
        type(resolver_version) is not str
        or _CANONICAL_TEXT.fullmatch(resolver_version) is None
    ):
        raise HistoricalBackfillError(
            "historical provider resolver version is invalid"
        )
    resolver_knowledge_time = getattr(resolver, "knowledge_time", None)
    if resolver_knowledge_time is not None:
        try:
            resolver_knowledge_time = _utc(
                resolver_knowledge_time,
                "historical provider resolver knowledge_time",
            )
        except (TypeError, ValueError):
            raise HistoricalBackfillError(
                "historical provider resolver knowledge time is invalid"
            ) from None
        if resolver_knowledge_time > requested_at:
            raise HistoricalBackfillError(
                "historical provider resolver was not known at requested_at"
            )

    selected_days = tuple(
        value
        for value in calendar.days
        if coverage_start <= value.day <= coverage_end
    )
    sessions = tuple(value.day for value in selected_days if value.is_session)
    session_position = {value: index for index, value in enumerate(sessions)}
    session_set = set(sessions)
    manifest_dates = {
        value.claimed_report_date
        for value in registry.source_manifests
        if coverage_start <= value.claimed_report_date <= coverage_end
    }
    observations_by_date: dict[date, tuple[IdentityObservation, ...]] = {
        value: registry.observations_on_claimed_date(value)
        for value in manifest_dates
    }
    candidates_by_observation = {
        observation_id: candidate
        for candidate in registry.candidates
        for observation_id in candidate.observation_ids
    }

    issues: dict[str, HistoricalBackfillIssue] = {}
    missing_sessions = tuple(value for value in sessions if value not in manifest_dates)
    for run in _runs(missing_sessions, session_position):
        value = _issue(
            HistoricalBackfillIssueCode.MISSING_SECURITY_MASTER_VINTAGE,
            run,
        )
        issues[value.issue_id] = value
    for value in sorted(manifest_dates - session_set):
        issue = _issue(
            HistoricalBackfillIssueCode.NON_SESSION_SECURITY_MASTER,
            (value,),
        )
        issues[issue.issue_id] = issue

    resolved: dict[
        tuple[date, str],
        list[tuple[IdentityObservation, str, str]],
    ] = defaultdict(list)
    hard_conflicts_by_session: dict[date, set[str]] = defaultdict(set)
    for session in sessions:
        eligible = tuple(
            value
            for value in observations_by_date.get(session, ())
            if (
                value.delete_flag == "N"
                and source_record_by_observation[
                    value.observation_id
                ].market_eligibility[0].status
                == 6
                and source_record_by_observation[
                    value.observation_id
                ].market_eligibility[0].eligible
                and value.security_series in SUPPORTED_SWING_SECURITY_SERIES
                and re.fullmatch(
                    r"[A-Z0-9][A-Z0-9&\-]{0,31}",
                    value.ticker_symbol,
                )
                is not None
            )
        )
        by_isin_series: dict[
            tuple[str, str], list[IdentityObservation]
        ] = defaultdict(list)
        by_financial_id: dict[int, list[IdentityObservation]] = defaultdict(list)
        by_listing_key: dict[str, list[IdentityObservation]] = defaultdict(list)
        for observation in eligible:
            if observation.validated_isin is not None:
                by_isin_series[
                    (
                        observation.validated_isin,
                        observation.security_series,
                    )
                ].append(observation)
            by_financial_id[observation.financial_instrument_id].append(
                observation
            )
            by_listing_key[observation.listing_key].append(observation)
        for values in by_isin_series.values():
            if len(values) > 1:
                hard_conflicts_by_session[session].update(
                    value.observation_id for value in values
                )
        for values in (
            *by_financial_id.values(),
            *by_listing_key.values(),
        ):
            if len({value.identifier_key for value in values}) > 1:
                hard_conflicts_by_session[session].update(
                    value.observation_id for value in values
                )

    rejected_observations: set[str] = set()
    for session in sessions:
        for observation in observations_by_date.get(session, ()):
            candidate = candidates_by_observation[observation.observation_id]
            adjudicated = adjudicated_by_observation.get(
                observation.observation_id
            )
            if observation.delete_flag != "N":
                issue = _issue(
                    HistoricalBackfillIssueCode.DELETED_SECURITY,
                    (session,),
                    (observation,),
                )
                issues[issue.issue_id] = issue
                rejected_observations.add(observation.observation_id)
                continue
            if (
                observation.security_series
                not in SUPPORTED_SWING_SECURITY_SERIES
                or re.fullmatch(
                    r"[A-Z0-9][A-Z0-9&\-]{0,31}",
                    observation.ticker_symbol,
                )
                is None
            ):
                issue = _issue(
                    HistoricalBackfillIssueCode.UNSUPPORTED_LISTING_LANE,
                    (session,),
                    (observation,),
                )
                issues[issue.issue_id] = issue
                rejected_observations.add(observation.observation_id)
                continue
            if (
                source_record_by_observation[
                    observation.observation_id
                ].market_eligibility[0].status
                != 6
                or not source_record_by_observation[
                    observation.observation_id
                ].market_eligibility[0].eligible
            ):
                issue = _issue(
                    HistoricalBackfillIssueCode.INELIGIBLE_NORMAL_MARKET,
                    (session,),
                    (observation,),
                )
                issues[issue.issue_id] = issue
                rejected_observations.add(observation.observation_id)
                continue
            if (
                observation.observation_id
                in hard_conflicts_by_session.get(session, set())
                and adjudicated is None
            ):
                issue = _issue(
                    HistoricalBackfillIssueCode.CONFLICTING_IDENTITY,
                    (session,),
                    (observation,),
                )
                issues[issue.issue_id] = issue
                rejected_observations.add(observation.observation_id)
                continue
            effective_isin = (
                adjudicated.isin
                if adjudicated is not None
                else observation.validated_isin
            )
            if (
                effective_isin is None
                or (
                    candidate.basis
                    is not IdentityCandidateBasis.VALIDATED_ISIN
                    and adjudicated is None
                )
            ):
                issue = _issue(
                    HistoricalBackfillIssueCode.UNVALIDATED_IDENTIFIER,
                    (session,),
                    (observation,),
                )
                issues[issue.issue_id] = issue
                rejected_observations.add(observation.observation_id)
                continue
            if (
                NSE_EQUITY_ISIN_PATTERN.fullmatch(
                    effective_isin
                )
                is None
                or NSE_SECURITY_SERIES_PATTERN.fullmatch(
                    observation.security_series
                )
                is None
            ):
                issue = _issue(
                    HistoricalBackfillIssueCode.UNSUPPORTED_LISTING_LANE,
                    (session,),
                    (observation,),
                )
                issues[issue.issue_id] = issue
                rejected_observations.add(observation.observation_id)
                continue
            try:
                if effective_isin == observation.validated_isin:
                    provider_key = resolver.resolve(observation)
                else:
                    resolve_isin = getattr(resolver, "resolve_isin", None)
                    if not callable(resolve_isin):
                        raise HistoricalBackfillIntegrityError(
                            "provider resolver cannot use adjudicated ISIN evidence"
                        )
                    provider_key = resolve_isin(effective_isin)
            except Exception:
                issue = _issue(
                    HistoricalBackfillIssueCode.PROVIDER_KEY_UNAVAILABLE,
                    (session,),
                    (observation,),
                )
                issues[issue.issue_id] = issue
                rejected_observations.add(observation.observation_id)
                continue
            if (
                type(provider_key) is not str
                or not provider_key
                or provider_key != provider_key.strip()
                or len(provider_key) > 128
            ):
                issue = _issue(
                    HistoricalBackfillIssueCode.PROVIDER_KEY_UNAVAILABLE,
                    (session,),
                    (observation,),
                )
                issues[issue.issue_id] = issue
                rejected_observations.add(observation.observation_id)
                continue
            catalog_contains = getattr(resolver, "catalog_contains", None)
            if callable(catalog_contains):
                try:
                    if effective_isin == observation.validated_isin:
                        present = catalog_contains(observation)
                    else:
                        contains_isin = getattr(
                            resolver,
                            "catalog_contains_isin",
                            None,
                        )
                        if not callable(contains_isin):
                            raise HistoricalBackfillIntegrityError(
                                "provider catalog cannot use adjudicated ISIN evidence"
                            )
                        present = contains_isin(effective_isin)
                except Exception:
                    issue = _issue(
                        HistoricalBackfillIssueCode.PROVIDER_KEY_UNAVAILABLE,
                        (session,),
                        (observation,),
                    )
                    issues[issue.issue_id] = issue
                    rejected_observations.add(observation.observation_id)
                    continue
                if type(present) is not bool:
                    issue = _issue(
                        HistoricalBackfillIssueCode.PROVIDER_KEY_UNAVAILABLE,
                        (session,),
                        (observation,),
                    )
                    issues[issue.issue_id] = issue
                    rejected_observations.add(observation.observation_id)
                    continue
                if not present:
                    issue = _issue(
                        HistoricalBackfillIssueCode.PROVIDER_CATALOG_ABSENT,
                        (session,),
                        (observation,),
                    )
                    issues[issue.issue_id] = issue
            resolved[(session, provider_key)].append(
                (observation, provider_key, effective_isin)
            )

    accepted: list[tuple[IdentityObservation, str, str]] = []
    for (session, _), values in sorted(resolved.items()):
        if len(values) != 1:
            observations = tuple(item[0] for item in values)
            issue = _issue(
                HistoricalBackfillIssueCode.AMBIGUOUS_PROVIDER_KEY,
                (session,),
                observations,
            )
            issues[issue.issue_id] = issue
            rejected_observations.update(value.observation_id for value in observations)
            continue
        if values[0][0].observation_id not in rejected_observations:
            accepted.append(values[0])

    lanes: dict[
        tuple[str, str, str, str, str],
        list[IdentityObservation],
    ] = defaultdict(list)
    for observation, provider_key, effective_isin in accepted:
        candidate = candidates_by_observation[observation.observation_id]
        lane = (
            candidate.candidate_id,
            observation.ticker_symbol,
            observation.security_series,
            effective_isin,
            provider_key,
        )
        lanes[lane].append(observation)

    requests: list[HistoricalDailyRequest] = []
    for (
        _,
        ticker_symbol,
        security_series,
        isin,
        provider_key,
    ), observations in sorted(lanes.items()):
        by_session = {value.claimed_report_date: value for value in observations}
        lane_sessions = tuple(sorted(by_session))
        for run in _runs(lane_sessions, session_position):
            run_observations = tuple(by_session[value] for value in run)
            binding = HistoricalInstrumentBinding(
                provider=provider,
                provider_instrument_id=provider_key,
                exchange="NSE",
                listing_key=f"NSE:{ticker_symbol}",
                security_series=security_series,
                isin=isin,
                valid_from=run[0],
                valid_through=run[-1],
                source_snapshot_ids=tuple(
                    sorted(
                        {
                            registry.registry_id,
                            calendar.snapshot_id,
                            *(
                                (identity_snapshot.snapshot_id,)
                                if identity_snapshot is not None
                                else ()
                            ),
                            *(
                                value.source_artifact_id
                                for value in run_observations
                            ),
                        }
                    )
                ),
            )
            requests.append(
                HistoricalDailyRequest(
                    binding=binding,
                    sessions=run,
                    requested_at=requested_at,
                )
            )

    return HistoricalBackfillPlan(
        provider=provider,
        resolver_version=resolver_version,
        identity_registry_id=registry.registry_id,
        calendar_snapshot_id=calendar.snapshot_id,
        coverage_start=coverage_start,
        coverage_end=coverage_end,
        requested_at=requested_at,
        requests=tuple(sorted(requests, key=_request_sort_key)),
        issues=tuple(issues[value] for value in sorted(issues)),
        identity_snapshot_id=(
            identity_snapshot.snapshot_id
            if identity_snapshot is not None
            else None
        ),
    )


@dataclass(frozen=True, slots=True)
class HistoricalBackfillCompletion:
    request_id: str
    snapshot_id: str
    completed_at: datetime
    recovered_existing: bool
    completion_id: str = field(init=False)

    def __post_init__(self) -> None:
        _sha256(self.request_id, "completion request_id")
        _sha256(self.snapshot_id, "completion snapshot_id")
        object.__setattr__(
            self,
            "completed_at",
            _utc(self.completed_at, "completion completed_at"),
        )
        if type(self.recovered_existing) is not bool:
            raise TypeError("recovered_existing must be bool")
        object.__setattr__(self, "completion_id", self._calculated_id())

    def _calculated_id(self) -> str:
        return content_id(
            {
                "schema": HISTORICAL_BACKFILL_PROGRESS_SCHEMA_VERSION,
                "request_id": self.request_id,
                "snapshot_id": self.snapshot_id,
                "completed_at": self.completed_at,
                "recovered_existing": self.recovered_existing,
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        if self.completion_id != self._calculated_id():
            raise HistoricalBackfillIntegrityError(
                "historical backfill completion identity failed"
            )


@dataclass(frozen=True, slots=True)
class HistoricalBackfillProgress:
    plan_id: str
    provider: str
    connector_version: str
    completions: tuple[HistoricalBackfillCompletion, ...]
    updated_at: datetime
    schema_version: str = HISTORICAL_BACKFILL_PROGRESS_SCHEMA_VERSION
    progress_id: str = field(init=False)

    def __post_init__(self) -> None:
        _sha256(self.plan_id, "progress plan_id")
        _provider(self.provider)
        if (
            type(self.connector_version) is not str
            or not self.connector_version
            or len(self.connector_version) > 128
        ):
            raise ValueError("connector_version must be bounded text")
        if self.schema_version != HISTORICAL_BACKFILL_PROGRESS_SCHEMA_VERSION:
            raise ValueError("unsupported historical backfill progress schema")
        if type(self.completions) is not tuple or any(
            type(value) is not HistoricalBackfillCompletion
            for value in self.completions
        ):
            raise TypeError("progress completions must be an exact tuple")
        if tuple(value.request_id for value in self.completions) != tuple(
            sorted({value.request_id for value in self.completions})
        ):
            raise ValueError("progress completions must be request-sorted and unique")
        for completion in self.completions:
            completion.verify_content_identity()
        object.__setattr__(
            self,
            "updated_at",
            _utc(self.updated_at, "progress updated_at"),
        )
        if any(value.completed_at > self.updated_at for value in self.completions):
            raise ValueError("progress cannot predate a completion")
        object.__setattr__(self, "progress_id", self._calculated_id())

    def _calculated_id(self) -> str:
        return content_id(
            {
                item.name: getattr(self, item.name)
                for item in fields(self)
                if item.name != "progress_id"
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        for value in self.completions:
            value.verify_content_identity()
        if self.progress_id != self._calculated_id():
            raise HistoricalBackfillIntegrityError(
                "historical backfill progress identity failed"
            )


def _progress_value(progress: HistoricalBackfillProgress) -> dict[str, object]:
    return {
        "schema_version": progress.schema_version,
        "progress_id": progress.progress_id,
        "plan_id": progress.plan_id,
        "provider": progress.provider,
        "connector_version": progress.connector_version,
        "updated_at": progress.updated_at.isoformat(),
        "completions": [
            {
                "completion_id": value.completion_id,
                "request_id": value.request_id,
                "snapshot_id": value.snapshot_id,
                "completed_at": value.completed_at.isoformat(),
                "recovered_existing": value.recovered_existing,
            }
            for value in progress.completions
        ],
    }


def _reject_duplicate_pairs(
    pairs: list[tuple[str, object]],
) -> dict[str, object]:
    value: dict[str, object] = {}
    for key, item in pairs:
        if key in value:
            raise HistoricalBackfillStateError(
                "historical backfill state contains duplicate keys"
            )
        value[key] = item
    return value


def _progress_from_bytes(payload: bytes) -> HistoricalBackfillProgress:
    try:
        root = json.loads(
            payload.decode("utf-8"),
            object_pairs_hook=_reject_duplicate_pairs,
            parse_constant=lambda _: (_ for _ in ()).throw(ValueError()),
        )
        expected_root = {
            "schema_version",
            "progress_id",
            "plan_id",
            "provider",
            "connector_version",
            "updated_at",
            "completions",
        }
        if type(root) is not dict or set(root) != expected_root:
            raise ValueError
        values = root["completions"]
        if type(values) is not list:
            raise ValueError
        completions: list[HistoricalBackfillCompletion] = []
        expected_completion = {
            "completion_id",
            "request_id",
            "snapshot_id",
            "completed_at",
            "recovered_existing",
        }
        claimed_completion_ids: list[str] = []
        for value in values:
            if type(value) is not dict or set(value) != expected_completion:
                raise ValueError
            claimed_completion_ids.append(value["completion_id"])
            completions.append(
                HistoricalBackfillCompletion(
                    request_id=value["request_id"],
                    snapshot_id=value["snapshot_id"],
                    completed_at=datetime.fromisoformat(value["completed_at"]),
                    recovered_existing=value["recovered_existing"],
                )
            )
        claimed_progress_id = root["progress_id"]
        progress = HistoricalBackfillProgress(
            plan_id=root["plan_id"],
            provider=root["provider"],
            connector_version=root["connector_version"],
            completions=tuple(completions),
            updated_at=datetime.fromisoformat(root["updated_at"]),
            schema_version=root["schema_version"],
        )
        if (
            claimed_completion_ids
            != [value.completion_id for value in progress.completions]
            or claimed_progress_id != progress.progress_id
        ):
            raise ValueError
        return progress
    except HistoricalBackfillStateError:
        raise
    except (AttributeError, TypeError, ValueError, UnicodeDecodeError, json.JSONDecodeError):
        raise HistoricalBackfillStateError(
            "historical backfill state is malformed"
        ) from None


class LocalHistoricalBackfillProgressStore:
    """Atomic single-runner progress store; Cloud deployment needs CAS writes."""

    def __init__(self, root: Path) -> None:
        self.root = Path(root)

    def path_for(self, plan_id: str) -> Path:
        _sha256(plan_id, "plan_id")
        return self.root / HISTORICAL_BACKFILL_STATE_DATASET / plan_id / PROGRESS_FILENAME

    def load(self, plan_id: str) -> HistoricalBackfillProgress | None:
        path = self.path_for(plan_id)
        if not path.exists():
            return None
        if not path.is_file():
            raise HistoricalBackfillStateError(
                "historical backfill state path is not a file"
            )
        progress = _progress_from_bytes(path.read_bytes())
        if progress.plan_id != plan_id:
            raise HistoricalBackfillStateError(
                "historical backfill state plan mismatch"
            )
        return progress

    def save(
        self,
        progress: HistoricalBackfillProgress,
    ) -> HistoricalBackfillProgress:
        if type(progress) is not HistoricalBackfillProgress:
            raise TypeError("progress must be exact HistoricalBackfillProgress")
        progress.verify_content_identity()
        existing = self.load(progress.plan_id)
        if existing is not None:
            old = {value.request_id: value for value in existing.completions}
            new = {value.request_id: value for value in progress.completions}
            if (
                existing.provider != progress.provider
                or existing.connector_version != progress.connector_version
                or existing.updated_at > progress.updated_at
                or any(new.get(key) != value for key, value in old.items())
            ):
                raise HistoricalBackfillStateError(
                    "historical backfill progress cannot regress or change lineage"
                )

        path = self.path_for(progress.plan_id)
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = (
            json.dumps(
                _progress_value(progress),
                ensure_ascii=False,
                allow_nan=False,
                separators=(",", ":"),
                sort_keys=True,
            )
            + "\n"
        ).encode("utf-8")
        descriptor, temporary_name = tempfile.mkstemp(
            dir=path.parent,
            prefix=".progress.",
            suffix=".tmp",
        )
        temporary = Path(temporary_name)
        try:
            with os.fdopen(descriptor, "wb") as handle:
                handle.write(payload)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, path)
            try:
                directory = os.open(
                    path.parent,
                    os.O_RDONLY | getattr(os, "O_DIRECTORY", 0),
                )
            except OSError:
                directory = None
            if directory is not None:
                try:
                    os.fsync(directory)
                except OSError:
                    pass
                finally:
                    os.close(directory)
        finally:
            if temporary.exists():
                temporary.unlink()
        loaded = self.load(progress.plan_id)
        if loaded != progress:
            raise HistoricalBackfillStateError(
                "historical backfill state failed write verification"
            )
        return loaded


class HistoricalBackfillRunner:
    def __init__(
        self,
        connector: HistoricalDailyDataConnector,
        snapshot_store: LocalMarketSnapshotStore,
        progress_store: LocalHistoricalBackfillProgressStore,
        *,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self.connector = connector
        self.snapshot_store = snapshot_store
        self.progress_store = progress_store
        self.clock = clock or (lambda: datetime.now(timezone.utc))
        self.collector = HistoricalMarketDataCollector(connector, snapshot_store)

    def run(
        self,
        plan: HistoricalBackfillPlan,
        *,
        maximum_requests: int | None = None,
    ) -> HistoricalBackfillProgress:
        if type(plan) is not HistoricalBackfillPlan:
            raise TypeError("plan must be an exact HistoricalBackfillPlan")
        plan.verify_content_identity()
        if maximum_requests is not None and (
            type(maximum_requests) is not int or maximum_requests <= 0
        ):
            raise ValueError("maximum_requests must be a positive exact integer")
        if (
            self.connector.provider != plan.provider
            or type(self.connector.provider_version) is not str
            or not self.connector.provider_version
        ):
            raise HistoricalBackfillError(
                "historical connector does not match the backfill plan"
            )

        progress = self.progress_store.load(plan.plan_id)
        if progress is None:
            progress = HistoricalBackfillProgress(
                plan_id=plan.plan_id,
                provider=plan.provider,
                connector_version=self.connector.provider_version,
                completions=(),
                updated_at=self._now(plan.requested_at),
            )
            progress = self.progress_store.save(progress)
        self._verify_progress(plan, progress)

        completions = {value.request_id: value for value in progress.completions}
        processed = 0
        requests_by_id = {value.request_id: value for value in plan.requests}
        for request_id, completion in completions.items():
            self._verify_stored_completion(
                requests_by_id[request_id],
                completion,
                progress.connector_version,
            )

        for request in plan.requests:
            if request.request_id in completions:
                continue
            if maximum_requests is not None and processed >= maximum_requests:
                break
            stored = self._recover_existing(request)
            recovered = stored is not None
            if stored is None:
                stored = self.collector.collect(request)
            completed_at = self._now(stored.manifest.observed_at)
            completion = HistoricalBackfillCompletion(
                request_id=request.request_id,
                snapshot_id=stored.manifest.snapshot_id,
                completed_at=completed_at,
                recovered_existing=recovered,
            )
            completions[request.request_id] = completion
            progress = HistoricalBackfillProgress(
                plan_id=plan.plan_id,
                provider=plan.provider,
                connector_version=self.connector.provider_version,
                completions=tuple(completions[value] for value in sorted(completions)),
                updated_at=completed_at,
            )
            progress = self.progress_store.save(progress)
            processed += 1
        return progress

    @staticmethod
    def is_complete(
        plan: HistoricalBackfillPlan,
        progress: HistoricalBackfillProgress,
    ) -> bool:
        plan.verify_content_identity()
        progress.verify_content_identity()
        if (
            progress.plan_id != plan.plan_id
            or progress.provider != plan.provider
        ):
            raise HistoricalBackfillStateError(
                "historical backfill progress belongs to another plan"
            )
        return {value.request_id for value in plan.requests} == {
            value.request_id for value in progress.completions
        }

    def _verify_progress(
        self,
        plan: HistoricalBackfillPlan,
        progress: HistoricalBackfillProgress,
    ) -> None:
        progress.verify_content_identity()
        request_ids = {value.request_id for value in plan.requests}
        if (
            progress.plan_id != plan.plan_id
            or progress.provider != plan.provider
            or progress.connector_version != self.connector.provider_version
            or any(
                value.request_id not in request_ids
                for value in progress.completions
            )
        ):
            raise HistoricalBackfillStateError(
                "historical backfill progress disagrees with plan or connector"
            )

    def _verify_stored_completion(
        self,
        request: HistoricalDailyRequest,
        completion: HistoricalBackfillCompletion,
        connector_version: str,
    ) -> StoredMarketSnapshot:
        dataset = historical_dataset_name(self.connector.provider)
        try:
            stored = self.snapshot_store.get(dataset, completion.snapshot_id)
        except Exception:
            raise HistoricalBackfillStateError(
                "completed historical backfill snapshot is unavailable"
            ) from None
        self._require_matching_snapshot(request, stored, connector_version)
        return stored

    def _recover_existing(
        self,
        request: HistoricalDailyRequest,
    ) -> StoredMarketSnapshot | None:
        dataset = historical_dataset_name(self.connector.provider)
        try:
            values = self.snapshot_store.find_by_selection_key(
                dataset,
                request.request_id,
            )
        except Exception:
            raise HistoricalBackfillStateError(
                "historical backfill snapshot recovery failed"
            ) from None
        matching: list[StoredMarketSnapshot] = []
        for stored in values:
            payload = stored.normalized_payload
            if (
                type(payload) is HistoricalDailyCandleBatch
                and payload.provider_version == self.connector.provider_version
            ):
                self._require_matching_snapshot(
                    request,
                    stored,
                    self.connector.provider_version,
                )
                matching.append(stored)
        return matching[0] if matching else None

    def _require_matching_snapshot(
        self,
        request: HistoricalDailyRequest,
        stored: StoredMarketSnapshot,
        connector_version: str,
    ) -> None:
        payload = stored.normalized_payload
        if type(payload) is not HistoricalDailyCandleBatch:
            raise HistoricalBackfillStateError(
                "historical backfill snapshot has the wrong payload type"
            )
        try:
            payload.verify_content_identity()
        except (TypeError, ValueError):
            raise HistoricalBackfillStateError(
                "historical backfill snapshot identity failed"
            ) from None
        if (
            stored.manifest.selection_key != request.request_id
            or stored.manifest.provider != self.connector.provider
            or stored.manifest.provider_version != connector_version
            or payload.request.request_id != request.request_id
            or payload.provider != self.connector.provider
            or payload.provider_version != connector_version
        ):
            raise HistoricalBackfillStateError(
                "historical backfill snapshot lineage mismatch"
            )

    def _now(self, not_before: datetime) -> datetime:
        value = _utc(self.clock(), "historical backfill clock")
        if value < _utc(not_before, "historical backfill not_before"):
            raise HistoricalBackfillStateError(
                "historical backfill clock moved behind durable lineage"
            )
        return value
