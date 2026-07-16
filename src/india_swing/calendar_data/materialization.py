from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
import re

from india_swing.calendar_evidence.models import ObservedMarketDateArtifact
from india_swing.identity import content_id
from india_swing.reference.calendar import (
    INDIA_STANDARD_TIME,
    CalendarDay,
    CalendarDayKind,
    CalendarIntegrityError,
    CalendarSnapshot,
    SessionWindow,
    SessionWindowPhase,
)
from india_swing.reference.models import ExternalRecordRef, ReferenceReadiness

from .artifact_store import verify_stored_calendar_source_provenance
from .models import (
    CalendarDeclaredDayKind,
    CalendarEvent,
    CalendarEventType,
    CalendarSourceArtifactManifest,
    CalendarSourceArtifactIntegrityError,
    CalendarWeekday,
    DeclaredSessionWindow,
    StoredCalendarSourceArtifact,
)


CALENDAR_MATERIALIZATION_SCHEMA_VERSION = "nse-cm-calendar-materialization/v1"
CALENDAR_MATERIALIZATION_POLICY_VERSION = (
    "nse-cm-explicit-event-graph-collection-only/v1"
)

_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_STATE_TYPES = frozenset(
    {
        CalendarEventType.BASE_WEEKLY_SCHEDULE,
        CalendarEventType.DATE_CLOSED,
        CalendarEventType.DATE_SESSION_REPLACED,
    }
)
_TERMINAL_TYPES = frozenset(
    {
        CalendarEventType.DATE_CLOSED,
        CalendarEventType.DATE_SESSION_REPLACED,
    }
)
_WEEKDAYS = tuple(CalendarWeekday)


class CalendarMaterializationError(ValueError):
    pass


class CalendarMaterializationIntegrityError(CalendarMaterializationError):
    pass


def _require_sha256(value: str, field_name: str) -> None:
    if not isinstance(value, str) or _SHA256.fullmatch(value) is None:
        raise ValueError(f"{field_name} must be a full lowercase SHA-256")


def _utc(value: datetime, field_name: str) -> datetime:
    if not isinstance(value, datetime):
        raise TypeError(f"{field_name} must be a datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
    return value.astimezone(timezone.utc)


def _manifest_sort_key(
    value: CalendarSourceArtifactManifest,
) -> tuple[str, str]:
    return (value.artifact_id, value.manifest_id)


@dataclass(frozen=True, slots=True)
class CalendarDayResolution:
    day: date
    state_chain_event_ids: tuple[str, ...]
    non_executable_event_ids: tuple[str, ...]
    applied_event_ids: tuple[str, ...]
    source_artifact_ids: tuple[str, ...]
    source_manifest_ids: tuple[str, ...]
    source_snapshot_id: str
    resolution_id: str = field(init=False)

    def __post_init__(self) -> None:
        if type(self.day) is not date:
            raise TypeError("calendar-day resolution day must be a date")
        for values, name, required in (
            (self.state_chain_event_ids, "state_chain_event_ids", True),
            (self.non_executable_event_ids, "non_executable_event_ids", False),
            (self.applied_event_ids, "applied_event_ids", True),
            (self.source_artifact_ids, "source_artifact_ids", True),
            (self.source_manifest_ids, "source_manifest_ids", True),
        ):
            if type(values) is not tuple or (required and not values):
                raise TypeError(f"{name} must be an immutable tuple")
            for value in values:
                _require_sha256(value, name)
        if not self.state_chain_event_ids:
            raise ValueError("state chain must contain a base event")
        if len(set(self.state_chain_event_ids)) != len(self.state_chain_event_ids):
            raise ValueError("state chain event IDs must be unique")
        if tuple(sorted(set(self.non_executable_event_ids))) != (
            self.non_executable_event_ids
        ):
            raise ValueError("non-executable event IDs must be unique and sorted")
        if tuple(sorted(set(self.applied_event_ids))) != self.applied_event_ids:
            raise ValueError("applied event IDs must be unique and sorted")
        if set(self.applied_event_ids) != set(self.state_chain_event_ids).union(
            self.non_executable_event_ids
        ):
            raise ValueError("applied event IDs do not match the resolved event graph")
        for values, name in (
            (self.source_artifact_ids, "source artifact IDs"),
            (self.source_manifest_ids, "source manifest IDs"),
        ):
            if tuple(sorted(set(values))) != values:
                raise ValueError(f"{name} must be unique and sorted")
        _require_sha256(self.source_snapshot_id, "source_snapshot_id")
        object.__setattr__(self, "resolution_id", self._calculated_resolution_id())

    def _calculated_resolution_id(self) -> str:
        return content_id(
            {
                "schema": "nse-cm-calendar-day-resolution/v1",
                "day": self.day,
                "state_chain_event_ids": self.state_chain_event_ids,
                "non_executable_event_ids": self.non_executable_event_ids,
                "applied_event_ids": self.applied_event_ids,
                "source_artifact_ids": self.source_artifact_ids,
                "source_manifest_ids": self.source_manifest_ids,
                "source_snapshot_id": self.source_snapshot_id,
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        if self.resolution_id != self._calculated_resolution_id():
            raise CalendarMaterializationIntegrityError(
                "calendar-day resolution identity verification failed"
            )


@dataclass(frozen=True, slots=True)
class ObservedDateEvidenceBinding:
    artifact_id: str
    cutoff: datetime
    knowledge_time: datetime
    source_bundle_artifact_id: str
    source_bundle_manifest_id: str
    observed_dates: tuple[date, ...]
    binding_id: str = field(init=False)

    def __post_init__(self) -> None:
        for value, name in (
            (self.artifact_id, "observed artifact ID"),
            (self.source_bundle_artifact_id, "observed bundle artifact ID"),
            (self.source_bundle_manifest_id, "observed bundle manifest ID"),
        ):
            _require_sha256(value, name)
        object.__setattr__(
            self,
            "cutoff",
            _utc(self.cutoff, "observed evidence cutoff"),
        )
        object.__setattr__(
            self,
            "knowledge_time",
            _utc(self.knowledge_time, "observed evidence knowledge_time"),
        )
        if self.knowledge_time > self.cutoff:
            raise ValueError("observed evidence knowledge time follows its cutoff")
        if type(self.observed_dates) is not tuple or not self.observed_dates:
            raise TypeError("observed dates must be a non-empty immutable tuple")
        if any(type(value) is not date for value in self.observed_dates):
            raise TypeError("observed dates must contain exact dates")
        if tuple(sorted(set(self.observed_dates))) != self.observed_dates:
            raise ValueError("observed dates must be unique and sorted")
        object.__setattr__(self, "binding_id", self._calculated_binding_id())

    def _calculated_binding_id(self) -> str:
        return content_id(
            {
                "schema": "nse-cm-observed-date-binding/v1",
                "artifact_id": self.artifact_id,
                "cutoff": self.cutoff,
                "knowledge_time": self.knowledge_time,
                "source_bundle_artifact_id": self.source_bundle_artifact_id,
                "source_bundle_manifest_id": self.source_bundle_manifest_id,
                "observed_dates": self.observed_dates,
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        if self.binding_id != self._calculated_binding_id():
            raise CalendarMaterializationIntegrityError(
                "observed-date binding identity verification failed"
            )


@dataclass(frozen=True, slots=True)
class CollectionCalendarMaterialization:
    exchange: str
    segment: str
    cutoff: datetime
    coverage_start: date
    coverage_end: date
    source_manifests: tuple[CalendarSourceArtifactManifest, ...]
    day_resolutions: tuple[CalendarDayResolution, ...]
    observed_evidence_bindings: tuple[ObservedDateEvidenceBinding, ...]
    calendar_snapshot: CalendarSnapshot
    readiness: ReferenceReadiness = ReferenceReadiness.COLLECTION_ONLY
    actionable: bool = False
    schema_version: str = CALENDAR_MATERIALIZATION_SCHEMA_VERSION
    policy_version: str = CALENDAR_MATERIALIZATION_POLICY_VERSION
    materialization_id: str = field(init=False)

    def __post_init__(self) -> None:
        if (self.exchange, self.segment) != ("NSE", "CM"):
            raise ValueError("calendar materialization is pinned to NSE CM")
        object.__setattr__(self, "cutoff", _utc(self.cutoff, "materialization cutoff"))
        if type(self.coverage_start) is not date or type(self.coverage_end) is not date:
            raise TypeError("materialization coverage bounds must be dates")
        if self.coverage_end < self.coverage_start:
            raise ValueError("materialization coverage end precedes its start")
        if type(self.source_manifests) is not tuple or not self.source_manifests:
            raise TypeError("calendar materialization requires source manifests")
        if any(
            type(value) is not CalendarSourceArtifactManifest
            for value in self.source_manifests
        ):
            raise TypeError("source manifests must contain exact manifest values")
        if tuple(sorted(self.source_manifests, key=_manifest_sort_key)) != (
            self.source_manifests
        ):
            raise ValueError("source manifests must be deterministically sorted")
        if len({value.artifact_id for value in self.source_manifests}) != len(
            self.source_manifests
        ):
            raise ValueError("calendar source artifact lineage must be unique")
        if len({value.manifest_id for value in self.source_manifests}) != len(
            self.source_manifests
        ):
            raise ValueError("calendar source manifest lineage must be unique")
        if any(value.validated_at > self.cutoff for value in self.source_manifests):
            raise ValueError("calendar source validation follows the materialization cutoff")

        if type(self.day_resolutions) is not tuple:
            raise TypeError("day resolutions must be an immutable tuple")
        expected_count = (self.coverage_end - self.coverage_start).days + 1
        if len(self.day_resolutions) != expected_count:
            raise ValueError("day resolutions must cover every requested date")
        for index, resolution in enumerate(self.day_resolutions):
            if type(resolution) is not CalendarDayResolution:
                raise TypeError("day resolutions must contain exact values")
            if resolution.day != self.coverage_start + timedelta(days=index):
                raise ValueError("day resolutions must be ordered and contiguous")
            resolution.verify_content_identity()

        if type(self.observed_evidence_bindings) is not tuple or any(
            type(value) is not ObservedDateEvidenceBinding
            for value in self.observed_evidence_bindings
        ):
            raise TypeError("observed evidence bindings must be an exact tuple")
        if tuple(
            sorted(
                self.observed_evidence_bindings,
                key=lambda value: value.artifact_id,
            )
        ) != self.observed_evidence_bindings:
            raise ValueError("observed evidence bindings must be sorted")
        if len(
            {value.artifact_id for value in self.observed_evidence_bindings}
        ) != len(self.observed_evidence_bindings):
            raise ValueError("observed evidence artifact lineage must be unique")
        for binding in self.observed_evidence_bindings:
            binding.verify_content_identity()
            if binding.knowledge_time > self.cutoff:
                raise ValueError("observed evidence was unknown at the cutoff")
            if binding.cutoff > self.cutoff:
                raise ValueError("observed evidence cutoff follows the materialization cutoff")

        if type(self.calendar_snapshot) is not CalendarSnapshot:
            raise TypeError("calendar_snapshot must be an exact CalendarSnapshot")
        self.calendar_snapshot.verify_content_identity()
        if (
            self.calendar_snapshot.exchange,
            self.calendar_snapshot.segment,
            self.calendar_snapshot.cutoff,
            self.calendar_snapshot.coverage_start,
            self.calendar_snapshot.coverage_end,
            self.calendar_snapshot.readiness,
        ) != (
            self.exchange,
            self.segment,
            self.cutoff,
            self.coverage_start,
            self.coverage_end,
            ReferenceReadiness.COLLECTION_ONLY,
        ):
            raise ValueError("calendar snapshot disagrees with its materialization")
        expected_snapshot_ids = tuple(
            sorted({value.source_snapshot_id for value in self.day_resolutions})
        )
        if self.calendar_snapshot.source_snapshot_ids != expected_snapshot_ids:
            raise ValueError("calendar snapshot source lineage disagrees with resolutions")
        for resolution, calendar_day in zip(
            self.day_resolutions,
            self.calendar_snapshot.days,
            strict=True,
        ):
            if (
                calendar_day.day != resolution.day
                or calendar_day.reference.source_snapshot_id
                != resolution.source_snapshot_id
                or calendar_day.data_ready_at is not None
            ):
                raise ValueError("calendar day disagrees with its schedule-only resolution")

        event_to_manifest: dict[str, CalendarSourceArtifactManifest] = {}
        for manifest in self.source_manifests:
            for event_id in manifest.event_ids:
                if event_id in event_to_manifest:
                    raise ValueError("event ID occurs in multiple source manifests")
                event_to_manifest[event_id] = manifest
        used_artifacts: set[str] = set()
        for resolution in self.day_resolutions:
            if any(event_id not in event_to_manifest for event_id in resolution.applied_event_ids):
                raise ValueError("applied event is absent from source manifests")
            expected_artifacts = tuple(
                sorted(
                    {
                        event_to_manifest[event_id].artifact_id
                        for event_id in resolution.applied_event_ids
                    }
                )
            )
            expected_manifests = tuple(
                sorted(
                    {
                        event_to_manifest[event_id].manifest_id
                        for event_id in resolution.applied_event_ids
                    }
                )
            )
            if (
                resolution.source_artifact_ids != expected_artifacts
                or resolution.source_manifest_ids != expected_manifests
            ):
                raise ValueError("calendar-day source lineage is incomplete")
            used_artifacts.update(expected_artifacts)
        if used_artifacts != {value.artifact_id for value in self.source_manifests}:
            raise ValueError("calendar materialization contains an unused source artifact")

        if (
            self.readiness is not ReferenceReadiness.COLLECTION_ONLY
            or type(self.actionable) is not bool
            or self.actionable
        ):
            raise ValueError("calendar materialization must remain collection-only")
        if self.schema_version != CALENDAR_MATERIALIZATION_SCHEMA_VERSION:
            raise ValueError("unsupported calendar materialization schema")
        if self.policy_version != CALENDAR_MATERIALIZATION_POLICY_VERSION:
            raise ValueError("unsupported calendar materialization policy")
        object.__setattr__(
            self,
            "materialization_id",
            self._calculated_materialization_id(),
        )

    def _calculated_materialization_id(self) -> str:
        return content_id(
            {
                "schema_version": self.schema_version,
                "policy_version": self.policy_version,
                "exchange": self.exchange,
                "segment": self.segment,
                "cutoff": self.cutoff,
                "coverage_start": self.coverage_start,
                "coverage_end": self.coverage_end,
                "source_manifests": self.source_manifests,
                "day_resolutions": self.day_resolutions,
                "observed_evidence_bindings": self.observed_evidence_bindings,
                "calendar_snapshot": self.calendar_snapshot,
                "readiness": self.readiness,
                "actionable": self.actionable,
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        self.calendar_snapshot.verify_content_identity()
        for value in self.day_resolutions:
            value.verify_content_identity()
        for value in self.observed_evidence_bindings:
            value.verify_content_identity()
        if self.materialization_id != self._calculated_materialization_id():
            raise CalendarMaterializationIntegrityError(
                "calendar materialization content identity verification failed"
            )


@dataclass(frozen=True, slots=True)
class _EventSource:
    event: CalendarEvent
    artifact: StoredCalendarSourceArtifact


def _applies_base(event: CalendarEvent, day: date) -> bool:
    return (
        event.event_type is CalendarEventType.BASE_WEEKLY_SCHEDULE
        and event.effective_from is not None
        and event.effective_to_exclusive is not None
        and event.effective_from <= day < event.effective_to_exclusive
    )


def _applies_state(event: CalendarEvent, day: date) -> bool:
    if event.event_type is CalendarEventType.BASE_WEEKLY_SCHEDULE:
        return _applies_base(event, day)
    return event.event_type in _TERMINAL_TYPES and event.effective_date == day


def _validate_event_graph(event_by_id: dict[str, _EventSource]) -> None:
    for source in event_by_id.values():
        event = source.event
        event.verify_content_identity()
        for predecessor_id in event.supersedes_event_ids:
            if predecessor_id not in event_by_id:
                raise CalendarMaterializationIntegrityError(
                    "calendar event supersedes an unknown event"
                )

    visiting: set[str] = set()
    visited: set[str] = set()

    def visit(event_id: str) -> None:
        if event_id in visiting:
            raise CalendarMaterializationIntegrityError(
                "calendar supersedes graph contains a cycle"
            )
        if event_id in visited:
            return
        visiting.add(event_id)
        for predecessor_id in event_by_id[event_id].event.supersedes_event_ids:
            visit(predecessor_id)
        visiting.remove(event_id)
        visited.add(event_id)

    for event_id in event_by_id:
        visit(event_id)

    successors: dict[tuple[date, str], list[str]] = {}
    for source in event_by_id.values():
        event = source.event
        if event.event_type not in _TERMINAL_TYPES:
            continue
        assert event.effective_date is not None
        if len(event.supersedes_event_ids) != 1:
            raise CalendarMaterializationIntegrityError(
                "state override must form one explicit supersedes chain"
            )
        predecessor = event_by_id[event.supersedes_event_ids[0]].event
        if predecessor.event_type not in _STATE_TYPES or not _applies_state(
            predecessor,
            event.effective_date,
        ):
            raise CalendarMaterializationIntegrityError(
                "state override does not reference an applicable state event"
            )
        predecessor_id = event.supersedes_event_ids[0]
        successors.setdefault(
            (event.effective_date, predecessor_id),
            [],
        ).append(event.event_id)
    if any(len(values) != 1 for values in successors.values()):
        raise CalendarMaterializationIntegrityError(
            "calendar date has competing state overrides"
        )

    for source in event_by_id.values():
        event = source.event
        if event.event_type is not CalendarEventType.NON_EXECUTABLE_ACTIVITY:
            continue
        assert event.effective_date is not None
        for predecessor_id in event.supersedes_event_ids:
            predecessor = event_by_id[predecessor_id].event
            if predecessor.event_type not in _STATE_TYPES or not _applies_state(
                predecessor,
                event.effective_date,
            ):
                raise CalendarMaterializationIntegrityError(
                    "non-executable activity references an inapplicable state event"
                )


def _window(day: date, value: DeclaredSessionWindow) -> SessionWindow:
    return SessionWindow(
        opens_at=datetime.combine(day, value.opens, tzinfo=INDIA_STANDARD_TIME),
        closes_at=datetime.combine(day, value.closes, tzinfo=INDIA_STANDARD_TIME),
        phase=SessionWindowPhase(value.phase.value),
    )


def _closed_kind(value: CalendarDeclaredDayKind) -> CalendarDayKind:
    return CalendarDayKind(value.value)


def materialize_collection_calendar(
    *,
    sources: tuple[StoredCalendarSourceArtifact, ...],
    coverage_start: date,
    coverage_end: date,
    cutoff: datetime,
    observed_date_artifacts: tuple[ObservedMarketDateArtifact, ...] = (),
) -> CollectionCalendarMaterialization:
    """Resolve a bounded calendar without promoting manual evidence to trading use."""

    if type(coverage_start) is not date or type(coverage_end) is not date:
        raise TypeError("coverage bounds must be exact dates")
    if coverage_end < coverage_start:
        raise CalendarMaterializationIntegrityError(
            "coverage end precedes coverage start"
        )
    cutoff_utc = _utc(cutoff, "cutoff")
    if type(sources) is not tuple or not sources:
        raise TypeError("sources must be a non-empty immutable tuple")
    if any(type(value) is not StoredCalendarSourceArtifact for value in sources):
        raise TypeError("sources must contain exact stored calendar artifacts")

    canonical_sources = tuple(
        sorted(sources, key=lambda value: _manifest_sort_key(value.manifest))
    )
    if len({value.manifest.artifact_id for value in canonical_sources}) != len(
        canonical_sources
    ):
        raise CalendarMaterializationIntegrityError("duplicate calendar source input")
    for source in canonical_sources:
        try:
            verify_stored_calendar_source_provenance(source)
        except (CalendarSourceArtifactIntegrityError, TypeError) as exc:
            raise CalendarMaterializationIntegrityError(
                "calendar source failed sealed provenance verification"
            ) from exc
        if source.manifest.validated_at > cutoff_utc:
            raise CalendarMaterializationIntegrityError(
                "calendar source was not validated by the requested cutoff"
            )

    event_by_id: dict[str, _EventSource] = {}
    for source in canonical_sources:
        for event in source.parsed.events:
            if event.event_id in event_by_id:
                raise CalendarMaterializationIntegrityError(
                    "calendar event ID is duplicated across source artifacts"
                )
            event_by_id[event.event_id] = _EventSource(event, source)
    _validate_event_graph(event_by_id)

    state_overrides_by_date: dict[date, list[_EventSource]] = {}
    for source in event_by_id.values():
        if source.event.event_type in _TERMINAL_TYPES:
            assert source.event.effective_date is not None
            state_overrides_by_date.setdefault(
                source.event.effective_date,
                [],
            ).append(source)
    activities_by_date: dict[date, list[_EventSource]] = {}
    for source in event_by_id.values():
        event = source.event
        if event.event_type is CalendarEventType.NON_EXECUTABLE_ACTIVITY:
            assert event.effective_date is not None
            activities_by_date.setdefault(event.effective_date, []).append(source)

    calendar_days: list[CalendarDay] = []
    resolutions: list[CalendarDayResolution] = []
    day = coverage_start
    while day <= coverage_end:
        bases = [
            source
            for source in event_by_id.values()
            if _applies_base(source.event, day)
        ]
        if len(bases) != 1:
            raise CalendarMaterializationIntegrityError(
                "each covered date requires exactly one applicable base schedule"
            )
        base = bases[0]
        weekday = _WEEKDAYS[day.weekday()]
        if weekday in base.event.weekdays:
            kind = CalendarDayKind.REGULAR
            declared_windows = list(base.event.windows)
        else:
            kind = CalendarDayKind.WEEKEND
            declared_windows = []

        state_chain = [base.event.event_id]
        overrides = state_overrides_by_date.get(day, [])
        child_by_predecessor: dict[str, _EventSource] = {}
        for override in overrides:
            predecessor_id = override.event.supersedes_event_ids[0]
            if predecessor_id in child_by_predecessor:
                raise CalendarMaterializationIntegrityError(
                    "calendar date has competing state overrides"
                )
            child_by_predecessor[predecessor_id] = override
        current_id = base.event.event_id
        while current_id in child_by_predecessor:
            child = child_by_predecessor[current_id]
            state_chain.append(child.event.event_id)
            current_id = child.event.event_id
        if len(state_chain) != len(overrides) + 1:
            raise CalendarMaterializationIntegrityError(
                "state overrides do not form one chain from the applicable base"
            )
        if len(state_chain) > 1:
            terminal = event_by_id[state_chain[-1]]
            if terminal.event.event_type is CalendarEventType.DATE_CLOSED:
                assert terminal.event.day_kind is not None
                kind = _closed_kind(terminal.event.day_kind)
                declared_windows = []
            else:
                kind = CalendarDayKind.SPECIAL
                declared_windows = list(terminal.event.windows)

        activities = tuple(
            sorted(
                activities_by_date.get(day, []),
                key=lambda value: value.event.event_id,
            )
        )
        if kind in {CalendarDayKind.REGULAR, CalendarDayKind.SPECIAL}:
            for activity in activities:
                declared_windows.extend(activity.event.windows)
        declared_windows.sort(key=lambda value: (value.opens, value.closes, value.phase.value))
        try:
            windows = tuple(_window(day, value) for value in declared_windows)
        except (TypeError, ValueError, CalendarIntegrityError) as exc:
            raise CalendarMaterializationIntegrityError(
                "resolved session windows are invalid or overlapping"
            ) from exc

        non_executable_ids = tuple(value.event.event_id for value in activities)
        applied_ids = tuple(sorted(set(state_chain).union(non_executable_ids)))
        applied_sources = tuple(event_by_id[event_id].artifact for event_id in applied_ids)
        artifact_ids = tuple(
            sorted({value.manifest.artifact_id for value in applied_sources})
        )
        manifest_ids = tuple(
            sorted({value.manifest.manifest_id for value in applied_sources})
        )
        source_snapshot_id = content_id(
            {
                "schema": "nse-cm-calendar-day-source/v1",
                "day": day,
                "applied_event_ids": applied_ids,
                "artifact_ids": artifact_ids,
                "manifest_ids": manifest_ids,
            },
            length=64,
        )
        resolution = CalendarDayResolution(
            day=day,
            state_chain_event_ids=tuple(state_chain),
            non_executable_event_ids=non_executable_ids,
            applied_event_ids=applied_ids,
            source_artifact_ids=artifact_ids,
            source_manifest_ids=manifest_ids,
            source_snapshot_id=source_snapshot_id,
        )
        knowledge_time = max(value.manifest.validated_at for value in applied_sources)
        day_content_hash = content_id(
            {
                "schema": "nse-cm-calendar-resolved-day/v1",
                "day": day,
                "kind": kind,
                "windows": windows,
                "resolution_id": resolution.resolution_id,
            },
            length=64,
        )
        reference = ExternalRecordRef(
            event_time=datetime.combine(day, datetime.min.time(), tzinfo=INDIA_STANDARD_TIME),
            knowledge_time=knowledge_time,
            source="NSE_CM_CALENDAR_EVENT_GRAPH_COLLECTION_ONLY",
            content_hash=day_content_hash,
            source_snapshot_id=source_snapshot_id,
        )
        try:
            calendar_day = CalendarDay(
                day=day,
                kind=kind,
                reference=reference,
                session_windows=windows,
                data_ready_at=None,
            )
        except (TypeError, ValueError, CalendarIntegrityError) as exc:
            raise CalendarMaterializationIntegrityError(
                "resolved calendar day violates the reference calendar contract"
            ) from exc
        calendar_days.append(calendar_day)
        resolutions.append(resolution)
        day += timedelta(days=1)

    if type(observed_date_artifacts) is not tuple or any(
        type(value) is not ObservedMarketDateArtifact
        for value in observed_date_artifacts
    ):
        raise TypeError("observed_date_artifacts must be an exact immutable tuple")
    if len({value.artifact_id for value in observed_date_artifacts}) != len(
        observed_date_artifacts
    ):
        raise CalendarMaterializationIntegrityError(
            "duplicate observed-date evidence input"
        )
    days_by_date = {value.day: value for value in calendar_days}
    bindings: list[ObservedDateEvidenceBinding] = []
    for evidence in observed_date_artifacts:
        evidence.verify_content_identity()
        if (evidence.exchange, evidence.segment) != ("NSE", "CM"):
            raise CalendarMaterializationIntegrityError(
                "observed-date evidence has another market scope"
            )
        if evidence.knowledge_time > cutoff_utc:
            raise CalendarMaterializationIntegrityError(
                "observed-date evidence was not known by the cutoff"
            )
        for observed_day in evidence.observed_dates:
            resolved = days_by_date.get(observed_day)
            if resolved is None:
                raise CalendarMaterializationIntegrityError(
                    "observed-date evidence lies outside materialized coverage"
                )
            if not resolved.is_session:
                raise CalendarMaterializationIntegrityError(
                    "positive traded-date evidence contradicts a closed calendar date"
                )
        bindings.append(
            ObservedDateEvidenceBinding(
                artifact_id=evidence.artifact_id,
                cutoff=evidence.cutoff,
                knowledge_time=evidence.knowledge_time,
                source_bundle_artifact_id=evidence.source_bundle_artifact_id,
                source_bundle_manifest_id=evidence.source_bundle_manifest_id,
                observed_dates=evidence.observed_dates,
            )
        )

    snapshot = CalendarSnapshot.create(
        exchange="NSE",
        segment="CM",
        cutoff=cutoff_utc,
        coverage_start=coverage_start,
        coverage_end=coverage_end,
        days=tuple(calendar_days),
        source_snapshot_ids=tuple(
            sorted({value.source_snapshot_id for value in resolutions})
        ),
        readiness=ReferenceReadiness.COLLECTION_ONLY,
    )
    try:
        return CollectionCalendarMaterialization(
            exchange="NSE",
            segment="CM",
            cutoff=cutoff_utc,
            coverage_start=coverage_start,
            coverage_end=coverage_end,
            source_manifests=tuple(value.manifest for value in canonical_sources),
            day_resolutions=tuple(resolutions),
            observed_evidence_bindings=tuple(
                sorted(bindings, key=lambda value: value.artifact_id)
            ),
            calendar_snapshot=snapshot,
        )
    except (TypeError, ValueError, CalendarIntegrityError) as exc:
        raise CalendarMaterializationIntegrityError(
            "calendar materialization failed its final integrity contract"
        ) from exc
