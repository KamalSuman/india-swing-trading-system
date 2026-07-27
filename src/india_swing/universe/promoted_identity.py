from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, datetime, timezone
from enum import Enum

from india_swing.identity import content_id
from india_swing.identity_decisions.models import IdentityResolutionBlocker
from india_swing.identity_decisions.promoted_materialize import (
    VerifiedPromotedIdentityAdjudication,
)
from india_swing.identity_registry import IdentityAdjudicationRequirement
from india_swing.reference.calendar import (
    CalendarCoverageError,
    CalendarIntegrityError,
    NotTradingSessionError,
)
from india_swing.reference.models import ReferenceReadiness
from india_swing.reference_data.models import SourceRowDisposition
from india_swing.reference_data.acquisition_promotion import (
    VerifiedReferenceArtifactPromotion,
)
from india_swing.calendar_data import CollectionCalendarMaterialization


class PromotedIdentitySessionUniverseError(ValueError):
    pass


_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_REASON = re.compile(r"[A-Z][A-Z0-9_]{2,127}\Z")

PROMOTED_IDENTITY_SESSION_UNIVERSE_SCHEMA_VERSION = (
    "promoted-identity-session-universe/v1"
)
PROMOTED_IDENTITY_SESSION_UNIVERSE_POLICY_VERSION = (
    "promoted-identity-session-universe/no-market-cap-cutoff-v1"
)

_MANDATORY_RESOLVED_REASONS = (
    "COLLECTION_ONLY_IDENTITY_EVIDENCE",
    "POINT_IN_TIME_LISTING_INTERVAL_UNVERIFIED",
    "BOARD_CLASSIFICATION_UNVERIFIED",
    "SURVEILLANCE_STATE_UNAVAILABLE",
    "LIQUIDITY_UNAVAILABLE",
)

_ERR_TYPE = "promoted identity session universe type is invalid"
_ERR_SCHEMA_VERSION = (
    "promoted identity session universe schema version is unsupported"
)
_ERR_ADJUDICATION = "promoted identity session universe adjudication is invalid"
_ERR_ADJUDICATION_VERIFY = (
    "promoted identity session universe could not verify the source adjudication"
)
_ERR_ADJUDICATION_STATE = (
    "promoted identity session universe adjudication is not collection-only"
)
_ERR_CALENDAR = "promoted identity session universe calendar is invalid"
_ERR_CALENDAR_VERIFY = (
    "promoted identity session universe could not verify the source calendar"
)
_ERR_CALENDAR_STATE = (
    "promoted identity session universe calendar is not collection-only"
)
_ERR_CALENDAR_SCOPE = (
    "promoted identity session universe calendar is not pinned to NSE CM"
)
_ERR_SESSION = "promoted identity session universe market session is invalid"
_ERR_CUTOFF = "promoted identity session universe cutoff is invalid"
_ERR_CUTOFF_BEFORE_KNOWLEDGE = (
    "promoted identity session universe cutoff precedes an authoritative input time"
)
_ERR_SESSION_COVERAGE = (
    "promoted identity session universe market session is not a trading session"
)
_ERR_PROMOTION_SELECTION = (
    "promoted identity session universe could not select exactly one promotion "
    "for the requested session"
)
_ERR_GRAPH = (
    "promoted identity session universe could not build its session entries"
)
_ERR_DERIVED_FIELD = "promoted identity session universe derived field is invalid"
_ERR_COMPARISON_FAILED = (
    "promoted identity session universe retained content could not be verified"
)
_ERR_UNIVERSE_ID = (
    "promoted identity session universe identifier disagrees with independently "
    "recomputed content"
)


def _utc(value: datetime) -> datetime:
    if not isinstance(value, datetime):
        raise PromotedIdentitySessionUniverseError(_ERR_CUTOFF)
    if value.tzinfo is None or value.utcoffset() is None:
        raise PromotedIdentitySessionUniverseError(_ERR_CUTOFF)
    return value.astimezone(timezone.utc)


class PromotedIdentitySessionDisposition(str, Enum):
    IDENTITY_RESOLVED_COLLECTION_ONLY = "IDENTITY_RESOLVED_COLLECTION_ONLY"
    IDENTITY_UNRESOLVED = "IDENTITY_UNRESOLVED"
    EXCLUDED_NON_EQUITY = "EXCLUDED_NON_EQUITY"
    EXCLUDED_TEST_SECURITY = "EXCLUDED_TEST_SECURITY"
    EXCLUDED_ALTERNATIVE_VENUE = "EXCLUDED_ALTERNATIVE_VENUE"


_EXCLUDED_DISPOSITIONS = {
    SourceRowDisposition.EXCLUDED_NON_EQUITY: (
        PromotedIdentitySessionDisposition.EXCLUDED_NON_EQUITY
    ),
    SourceRowDisposition.EXCLUDED_TEST_SECURITY: (
        PromotedIdentitySessionDisposition.EXCLUDED_TEST_SECURITY
    ),
    SourceRowDisposition.EXCLUDED_ALTERNATIVE_VENUE: (
        PromotedIdentitySessionDisposition.EXCLUDED_ALTERNATIVE_VENUE
    ),
}


@dataclass(frozen=True, slots=True)
class PromotedIdentitySessionEntry:
    """One immutable disposition for exactly one row of the selected promoted
    security master. Every row of the selected session's source master
    receives exactly one entry -- retained equities are always either
    IDENTITY_RESOLVED_COLLECTION_ONLY or IDENTITY_UNRESOLVED (never dropped
    because identity is unresolved), and excluded rows carry their exact
    source-exclusion disposition with no identity/candidate/resolution/
    stable-ID fields at all.
    """

    source_artifact_id: str
    source_manifest_id: str
    source_record_id: str
    financial_instrument_id: int
    symbol: str
    series: str
    validated_isin: str | None
    source_disposition: SourceRowDisposition
    disposition: PromotedIdentitySessionDisposition
    permitted_to_trade: int
    normal_market_status: int
    normal_market_eligible: bool
    delete_flag: str
    listing_timestamp: int
    removal_timestamp: int
    readmission_timestamp: int
    candidate_id: str | None
    stable_instrument_id: str | None
    stable_listing_id: str | None
    required_requirements: tuple[IdentityAdjudicationRequirement, ...]
    missing_requirements: tuple[IdentityAdjudicationRequirement, ...]
    blocker_codes: tuple[IdentityResolutionBlocker, ...]
    reason_codes: tuple[str, ...]

    def __post_init__(self) -> None:
        for value, name in (
            (self.source_artifact_id, "source_artifact_id"),
            (self.source_manifest_id, "source_manifest_id"),
            (self.source_record_id, "source_record_id"),
        ):
            if type(value) is not str or _SHA256.fullmatch(value) is None:
                raise PromotedIdentitySessionUniverseError(_ERR_GRAPH)
        if (
            type(self.financial_instrument_id) is not int
            or self.financial_instrument_id <= 0
        ):
            raise PromotedIdentitySessionUniverseError(_ERR_GRAPH)
        if type(self.symbol) is not str or not self.symbol:
            raise PromotedIdentitySessionUniverseError(_ERR_GRAPH)
        if type(self.series) is not str or not self.series:
            raise PromotedIdentitySessionUniverseError(_ERR_GRAPH)
        if self.validated_isin is not None and type(self.validated_isin) is not str:
            raise PromotedIdentitySessionUniverseError(_ERR_GRAPH)
        if type(self.source_disposition) is not SourceRowDisposition:
            raise PromotedIdentitySessionUniverseError(_ERR_GRAPH)
        if type(self.disposition) is not PromotedIdentitySessionDisposition:
            raise PromotedIdentitySessionUniverseError(_ERR_GRAPH)
        if type(self.permitted_to_trade) is not int or self.permitted_to_trade not in (
            0,
            1,
            2,
        ):
            raise PromotedIdentitySessionUniverseError(_ERR_GRAPH)
        if (
            type(self.normal_market_status) is not int
            or not 1 <= self.normal_market_status <= 6
        ):
            raise PromotedIdentitySessionUniverseError(_ERR_GRAPH)
        if type(self.normal_market_eligible) is not bool:
            raise PromotedIdentitySessionUniverseError(_ERR_GRAPH)
        if self.delete_flag not in ("N", "Y"):
            raise PromotedIdentitySessionUniverseError(_ERR_GRAPH)
        for value in (
            self.listing_timestamp,
            self.removal_timestamp,
            self.readmission_timestamp,
        ):
            if type(value) is not int or value < 0:
                raise PromotedIdentitySessionUniverseError(_ERR_GRAPH)
        for value in (self.candidate_id, self.stable_instrument_id, self.stable_listing_id):
            if value is not None and (
                type(value) is not str or _SHA256.fullmatch(value) is None
            ):
                raise PromotedIdentitySessionUniverseError(_ERR_GRAPH)
        for values, expected_type in (
            (self.required_requirements, IdentityAdjudicationRequirement),
            (self.missing_requirements, IdentityAdjudicationRequirement),
        ):
            if type(values) is not tuple or any(
                type(value) is not expected_type for value in values
            ):
                raise PromotedIdentitySessionUniverseError(_ERR_GRAPH)
        if type(self.blocker_codes) is not tuple or any(
            type(value) is not IdentityResolutionBlocker for value in self.blocker_codes
        ):
            raise PromotedIdentitySessionUniverseError(_ERR_GRAPH)
        if (
            type(self.reason_codes) is not tuple
            or not self.reason_codes
            or self.reason_codes != tuple(sorted(set(self.reason_codes)))
            or any(_REASON.fullmatch(value) is None for value in self.reason_codes)
        ):
            raise PromotedIdentitySessionUniverseError(_ERR_GRAPH)

        is_resolved = (
            self.disposition
            is PromotedIdentitySessionDisposition.IDENTITY_RESOLVED_COLLECTION_ONLY
        )
        is_unresolved = (
            self.disposition is PromotedIdentitySessionDisposition.IDENTITY_UNRESOLVED
        )
        if is_resolved:
            if (
                self.source_disposition
                is not SourceRowDisposition.RETAINED_UNVERIFIED_EQUITY
                or self.candidate_id is None
                or self.stable_instrument_id is None
                or self.stable_listing_id is None
                or self.missing_requirements
                or self.blocker_codes
            ):
                raise PromotedIdentitySessionUniverseError(_ERR_GRAPH)
        elif is_unresolved:
            if (
                self.source_disposition
                is not SourceRowDisposition.RETAINED_UNVERIFIED_EQUITY
                or self.candidate_id is None
                or self.stable_instrument_id is not None
                or self.stable_listing_id is not None
            ):
                raise PromotedIdentitySessionUniverseError(_ERR_GRAPH)
        else:
            if (
                _EXCLUDED_DISPOSITIONS.get(self.source_disposition)
                is not self.disposition
                or self.candidate_id is not None
                or self.stable_instrument_id is not None
                or self.stable_listing_id is not None
                or self.required_requirements
                or self.missing_requirements
                or self.blocker_codes
            ):
                raise PromotedIdentitySessionUniverseError(_ERR_GRAPH)
        if self.reason_codes != _reason_codes_for(
            self.disposition,
            missing_requirements=self.missing_requirements,
            blocker_codes=self.blocker_codes,
        ):
            raise PromotedIdentitySessionUniverseError(_ERR_GRAPH)

    def _identity(self) -> dict[str, object]:
        return {
            "source_artifact_id": self.source_artifact_id,
            "source_manifest_id": self.source_manifest_id,
            "source_record_id": self.source_record_id,
            "financial_instrument_id": self.financial_instrument_id,
            "symbol": self.symbol,
            "series": self.series,
            "validated_isin": self.validated_isin,
            "source_disposition": self.source_disposition,
            "disposition": self.disposition,
            "permitted_to_trade": self.permitted_to_trade,
            "normal_market_status": self.normal_market_status,
            "normal_market_eligible": self.normal_market_eligible,
            "delete_flag": self.delete_flag,
            "listing_timestamp": self.listing_timestamp,
            "removal_timestamp": self.removal_timestamp,
            "readmission_timestamp": self.readmission_timestamp,
            "candidate_id": self.candidate_id,
            "stable_instrument_id": self.stable_instrument_id,
            "stable_listing_id": self.stable_listing_id,
            "required_requirements": self.required_requirements,
            "missing_requirements": self.missing_requirements,
            "blocker_codes": self.blocker_codes,
            "reason_codes": self.reason_codes,
        }


def _reason_codes_for(
    disposition: PromotedIdentitySessionDisposition,
    *,
    missing_requirements: tuple[IdentityAdjudicationRequirement, ...] = (),
    blocker_codes: tuple[IdentityResolutionBlocker, ...] = (),
) -> tuple[str, ...]:
    if disposition in _EXCLUDED_DISPOSITIONS.values():
        return (f"SOURCE_EXCLUSION_{disposition.value}",)
    codes = set(_MANDATORY_RESOLVED_REASONS)
    if disposition is PromotedIdentitySessionDisposition.IDENTITY_UNRESOLVED:
        codes.add("STABLE_IDENTITY_UNRESOLVED")
        codes.update(
            f"UNRESOLVED_REQUIREMENT_{value.value}" for value in missing_requirements
        )
        codes.update(
            f"RESOLUTION_BLOCKER_{value.value}" for value in blocker_codes
        )
    return tuple(sorted(codes))


def _build_entries(
    adjudication: VerifiedPromotedIdentityAdjudication,
    promotion: VerifiedReferenceArtifactPromotion,
    market_session: date,
) -> tuple[PromotedIdentitySessionEntry, ...]:
    manifest = promotion.artifact.manifest
    observations_by_key = {}
    for value in adjudication.intake.observations:
        key = (
            value.source_artifact_id,
            value.source_manifest_id,
            value.source_record_id,
        )
        if key in observations_by_key:
            raise PromotedIdentitySessionUniverseError(_ERR_GRAPH)
        observations_by_key[key] = value

    candidate_by_observation = {}
    for candidate in adjudication.intake.candidates:
        for observation_id in candidate.observation_ids:
            if observation_id in candidate_by_observation:
                raise PromotedIdentitySessionUniverseError(_ERR_GRAPH)
            candidate_by_observation[observation_id] = candidate

    resolution_by_candidate = {}
    for value in adjudication.snapshot.resolutions:
        if value.candidate_id in resolution_by_candidate:
            raise PromotedIdentitySessionUniverseError(_ERR_GRAPH)
        resolution_by_candidate[value.candidate_id] = value

    listing_by_observation = {}
    for value in adjudication.snapshot.listing_observations:
        if value.source_observation_id in listing_by_observation:
            raise PromotedIdentitySessionUniverseError(_ERR_GRAPH)
        listing_by_observation[value.source_observation_id] = value

    entries: list[PromotedIdentitySessionEntry] = []
    for record in promotion.artifact.parsed.records:
        if record.disposition is SourceRowDisposition.RETAINED_UNVERIFIED_EQUITY:
            key = (manifest.artifact_id, manifest.manifest_id, record.source_record_id)
            observation = observations_by_key.get(key)
            if observation is None:
                raise PromotedIdentitySessionUniverseError(_ERR_GRAPH)
            candidate = candidate_by_observation.get(observation.observation_id)
            if candidate is None:
                raise PromotedIdentitySessionUniverseError(_ERR_GRAPH)
            resolution = resolution_by_candidate.get(candidate.candidate_id)
            if resolution is None:
                raise PromotedIdentitySessionUniverseError(_ERR_GRAPH)
            listing = listing_by_observation.get(observation.observation_id)
            if resolution.stable_instrument_id is not None:
                if (
                    listing is None
                    or listing.stable_instrument_id != resolution.stable_instrument_id
                    or listing.candidate_id != candidate.candidate_id
                    or listing.symbol != observation.ticker_symbol
                    or listing.series != observation.security_series
                    or observation.validated_isin is None
                    or listing.isin != observation.validated_isin
                    or listing.effective_on != market_session
                ):
                    raise PromotedIdentitySessionUniverseError(_ERR_GRAPH)
                disposition = (
                    PromotedIdentitySessionDisposition.IDENTITY_RESOLVED_COLLECTION_ONLY
                )
                entries.append(
                    PromotedIdentitySessionEntry(
                        source_artifact_id=manifest.artifact_id,
                        source_manifest_id=manifest.manifest_id,
                        source_record_id=record.source_record_id,
                        financial_instrument_id=record.financial_instrument_id,
                        symbol=record.ticker_symbol,
                        series=record.security_series,
                        validated_isin=record.validated_isin,
                        source_disposition=record.disposition,
                        disposition=disposition,
                        permitted_to_trade=record.permitted_to_trade,
                        normal_market_status=record.market_eligibility[0].status,
                        normal_market_eligible=record.market_eligibility[0].eligible,
                        delete_flag=record.delete_flag,
                        listing_timestamp=record.listing_timestamp,
                        removal_timestamp=record.removal_timestamp,
                        readmission_timestamp=record.readmission_timestamp,
                        candidate_id=candidate.candidate_id,
                        stable_instrument_id=resolution.stable_instrument_id,
                        stable_listing_id=listing.stable_listing_id,
                        required_requirements=resolution.required_requirements,
                        missing_requirements=(),
                        blocker_codes=(),
                        reason_codes=_reason_codes_for(disposition),
                    )
                )
            else:
                if listing is not None:
                    raise PromotedIdentitySessionUniverseError(_ERR_GRAPH)
                disposition = PromotedIdentitySessionDisposition.IDENTITY_UNRESOLVED
                entries.append(
                    PromotedIdentitySessionEntry(
                        source_artifact_id=manifest.artifact_id,
                        source_manifest_id=manifest.manifest_id,
                        source_record_id=record.source_record_id,
                        financial_instrument_id=record.financial_instrument_id,
                        symbol=record.ticker_symbol,
                        series=record.security_series,
                        validated_isin=record.validated_isin,
                        source_disposition=record.disposition,
                        disposition=disposition,
                        permitted_to_trade=record.permitted_to_trade,
                        normal_market_status=record.market_eligibility[0].status,
                        normal_market_eligible=record.market_eligibility[0].eligible,
                        delete_flag=record.delete_flag,
                        listing_timestamp=record.listing_timestamp,
                        removal_timestamp=record.removal_timestamp,
                        readmission_timestamp=record.readmission_timestamp,
                        candidate_id=candidate.candidate_id,
                        stable_instrument_id=None,
                        stable_listing_id=None,
                        required_requirements=resolution.required_requirements,
                        missing_requirements=resolution.missing_requirements,
                        blocker_codes=resolution.blocker_codes,
                        reason_codes=_reason_codes_for(
                            disposition,
                            missing_requirements=resolution.missing_requirements,
                            blocker_codes=resolution.blocker_codes,
                        ),
                    )
                )
        else:
            disposition = _EXCLUDED_DISPOSITIONS.get(record.disposition)
            if disposition is None:
                raise PromotedIdentitySessionUniverseError(_ERR_GRAPH)
            key = (manifest.artifact_id, manifest.manifest_id, record.source_record_id)
            if key in observations_by_key:
                raise PromotedIdentitySessionUniverseError(_ERR_GRAPH)
            entries.append(
                PromotedIdentitySessionEntry(
                    source_artifact_id=manifest.artifact_id,
                    source_manifest_id=manifest.manifest_id,
                    source_record_id=record.source_record_id,
                    financial_instrument_id=record.financial_instrument_id,
                    symbol=record.ticker_symbol,
                    series=record.security_series,
                    validated_isin=record.validated_isin,
                    source_disposition=record.disposition,
                    disposition=disposition,
                    permitted_to_trade=record.permitted_to_trade,
                    normal_market_status=record.market_eligibility[0].status,
                    normal_market_eligible=record.market_eligibility[0].eligible,
                    delete_flag=record.delete_flag,
                    listing_timestamp=record.listing_timestamp,
                    removal_timestamp=record.removal_timestamp,
                    readmission_timestamp=record.readmission_timestamp,
                    candidate_id=None,
                    stable_instrument_id=None,
                    stable_listing_id=None,
                    required_requirements=(),
                    missing_requirements=(),
                    blocker_codes=(),
                    reason_codes=_reason_codes_for(disposition),
                )
            )
    if len(entries) != len(promotion.artifact.parsed.records) or len(
        {value.source_record_id for value in entries}
    ) != len(entries):
        raise PromotedIdentitySessionUniverseError(_ERR_GRAPH)
    return tuple(sorted(entries, key=lambda value: value.source_record_id))


def _universe_identity(
    *,
    adjudication_id: str,
    snapshot_id: str,
    intake_id: str,
    source_graph_id: str,
    queue_id: str,
    materialization_id: str,
    calendar_snapshot_id: str,
    market_session: date,
    cutoff: datetime,
    knowledge_time: datetime,
    selected_promotion_id: str,
    selected_join_id: str,
    selected_source_artifact_id: str,
    selected_source_manifest_id: str,
    selected_raw_sha256: str,
    selected_normalized_sha256: str,
    entries: tuple[PromotedIdentitySessionEntry, ...],
    disposition_counts: tuple[tuple[str, int], ...],
    reason_counts: tuple[tuple[str, int], ...],
    readiness: ReferenceReadiness,
    actionable: bool,
    execution_eligible: bool,
) -> dict[str, object]:
    return {
        "schema_version": PROMOTED_IDENTITY_SESSION_UNIVERSE_SCHEMA_VERSION,
        "policy_version": PROMOTED_IDENTITY_SESSION_UNIVERSE_POLICY_VERSION,
        "adjudication_id": adjudication_id,
        "snapshot_id": snapshot_id,
        "intake_id": intake_id,
        "source_graph_id": source_graph_id,
        "queue_id": queue_id,
        "materialization_id": materialization_id,
        "calendar_snapshot_id": calendar_snapshot_id,
        "market_session": market_session,
        "cutoff": cutoff,
        "knowledge_time": knowledge_time,
        "selected_promotion_id": selected_promotion_id,
        "selected_join_id": selected_join_id,
        "selected_source_artifact_id": selected_source_artifact_id,
        "selected_source_manifest_id": selected_source_manifest_id,
        "selected_raw_sha256": selected_raw_sha256,
        "selected_normalized_sha256": selected_normalized_sha256,
        "entry_identities": tuple(value._identity() for value in entries),
        "disposition_counts": disposition_counts,
        "reason_counts": reason_counts,
        "readiness": readiness,
        "actionable": actionable,
        "execution_eligible": execution_eligible,
    }


@dataclass(frozen=True)
class _SessionUniverseFacts:
    """Plain normalized facts returned by the single strict decoder. Never a
    VerifiedPromotedIdentitySessionUniverse."""

    cutoff: datetime
    knowledge_time: datetime
    selected_promotion_id: str
    selected_join_id: str
    selected_source_artifact_id: str
    selected_source_manifest_id: str
    selected_raw_sha256: str
    selected_normalized_sha256: str
    entries: tuple[PromotedIdentitySessionEntry, ...]
    disposition_counts: tuple[tuple[str, int], ...]
    reason_counts: tuple[tuple[str, int], ...]
    readiness: ReferenceReadiness
    actionable: bool
    execution_eligible: bool
    universe_id: str


def _build_session_universe_facts(
    adjudication: VerifiedPromotedIdentityAdjudication,
    calendar: CollectionCalendarMaterialization,
    market_session: date,
    cutoff: datetime,
) -> _SessionUniverseFacts:
    """The single strict session-universe derivation routine.

    Requires exact VerifiedPromotedIdentityAdjudication/
    CollectionCalendarMaterialization/date/aware-datetime inputs,
    independently replays both input content identities, requires both
    inputs to remain collection-only and non-actionable, requires cutoff at
    or after both adjudication.snapshot.knowledge_time and calendar.cutoff,
    requires market_session to resolve to an executable trading session in
    the calendar, selects exactly one intake promotion whose
    verified_report_date equals market_session, and builds exactly one
    PromotedIdentitySessionEntry for every parsed source row in that
    promotion's sealed artifact. Never constructs
    VerifiedPromotedIdentitySessionUniverse.

    The retained adjudication/calendar/promotion graph is treated as
    untrusted at every replay: every step below deliberately catches
    ordinary ``Exception`` (never ``BaseException``, so
    ``KeyboardInterrupt``/``SystemExit`` still propagate) so a malformed
    nested field fails closed with one static sanitized error instead of
    leaking a raw nested exception.
    """

    if type(adjudication) is not VerifiedPromotedIdentityAdjudication:
        raise PromotedIdentitySessionUniverseError(_ERR_ADJUDICATION)
    if type(calendar) is not CollectionCalendarMaterialization:
        raise PromotedIdentitySessionUniverseError(_ERR_CALENDAR)
    if type(market_session) is not date:
        raise PromotedIdentitySessionUniverseError(_ERR_SESSION)

    cutoff = _utc(cutoff)

    try:
        adjudication.verify_content_identity()
    except Exception:
        raise PromotedIdentitySessionUniverseError(_ERR_ADJUDICATION_VERIFY) from None
    try:
        calendar.verify_content_identity()
    except Exception:
        raise PromotedIdentitySessionUniverseError(_ERR_CALENDAR_VERIFY) from None

    if (
        adjudication.readiness is not ReferenceReadiness.COLLECTION_ONLY
        or adjudication.actionable is not False
    ):
        raise PromotedIdentitySessionUniverseError(_ERR_ADJUDICATION_STATE)
    if (
        calendar.readiness is not ReferenceReadiness.COLLECTION_ONLY
        or calendar.actionable is not False
    ):
        raise PromotedIdentitySessionUniverseError(_ERR_CALENDAR_STATE)
    if (calendar.exchange, calendar.segment) != ("NSE", "CM"):
        raise PromotedIdentitySessionUniverseError(_ERR_CALENDAR_SCOPE)

    if cutoff < adjudication.snapshot.knowledge_time or cutoff < calendar.cutoff:
        raise PromotedIdentitySessionUniverseError(_ERR_CUTOFF_BEFORE_KNOWLEDGE)

    if market_session < calendar.coverage_start or market_session > calendar.coverage_end:
        raise PromotedIdentitySessionUniverseError(_ERR_SESSION_COVERAGE)
    try:
        calendar.calendar_snapshot.require_session(market_session)
    except (NotTradingSessionError, CalendarCoverageError, CalendarIntegrityError):
        raise PromotedIdentitySessionUniverseError(_ERR_SESSION_COVERAGE) from None
    except Exception:
        raise PromotedIdentitySessionUniverseError(_ERR_SESSION_COVERAGE) from None

    matches = tuple(
        value
        for value in adjudication.intake.promotions
        if value.verified_report_date == market_session
    )
    if len(matches) != 1:
        raise PromotedIdentitySessionUniverseError(_ERR_PROMOTION_SELECTION)
    promotion = matches[0]

    try:
        entries = _build_entries(adjudication, promotion, market_session)
    except PromotedIdentitySessionUniverseError:
        raise
    except Exception:
        raise PromotedIdentitySessionUniverseError(_ERR_GRAPH) from None

    manifest = promotion.artifact.manifest
    knowledge_time = max(
        adjudication.snapshot.knowledge_time, promotion.knowledge_time, calendar.cutoff
    )
    readiness = ReferenceReadiness.COLLECTION_ONLY
    actionable = False
    execution_eligible = False

    disposition_totals: dict[str, int] = {}
    reason_totals: dict[str, int] = {}
    for entry in entries:
        disposition_totals[entry.disposition.value] = (
            disposition_totals.get(entry.disposition.value, 0) + 1
        )
        for reason in entry.reason_codes:
            reason_totals[reason] = reason_totals.get(reason, 0) + 1
    disposition_counts = tuple(sorted(disposition_totals.items()))
    reason_counts = tuple(sorted(reason_totals.items()))

    universe_id = content_id(
        _universe_identity(
            adjudication_id=adjudication.adjudication_id,
            snapshot_id=adjudication.snapshot.snapshot_id,
            intake_id=adjudication.intake.intake_id,
            source_graph_id=adjudication.intake.source_graph_id,
            queue_id=adjudication.intake.queue.queue_id,
            materialization_id=calendar.materialization_id,
            calendar_snapshot_id=calendar.calendar_snapshot.snapshot_id,
            market_session=market_session,
            cutoff=cutoff,
            knowledge_time=knowledge_time,
            selected_promotion_id=promotion.promotion_id,
            selected_join_id=promotion.join.join_id,
            selected_source_artifact_id=manifest.artifact_id,
            selected_source_manifest_id=manifest.manifest_id,
            selected_raw_sha256=manifest.raw_sha256,
            selected_normalized_sha256=manifest.normalized_sha256,
            entries=entries,
            disposition_counts=disposition_counts,
            reason_counts=reason_counts,
            readiness=readiness,
            actionable=actionable,
            execution_eligible=execution_eligible,
        ),
        length=64,
    )

    return _SessionUniverseFacts(
        cutoff=cutoff,
        knowledge_time=knowledge_time,
        selected_promotion_id=promotion.promotion_id,
        selected_join_id=promotion.join.join_id,
        selected_source_artifact_id=manifest.artifact_id,
        selected_source_manifest_id=manifest.manifest_id,
        selected_raw_sha256=manifest.raw_sha256,
        selected_normalized_sha256=manifest.normalized_sha256,
        entries=entries,
        disposition_counts=disposition_counts,
        reason_counts=reason_counts,
        readiness=readiness,
        actionable=actionable,
        execution_eligible=execution_eligible,
        universe_id=universe_id,
    )


@dataclass(frozen=True, slots=True)
class VerifiedPromotedIdentitySessionUniverse:
    """Immutable, content-addressed session-specific NSE CM collection
    universe bridging one VerifiedPromotedIdentityAdjudication and one exact
    CollectionCalendarMaterialization.

    Applies no market-cap, index-membership, price, liquidity, volume, or
    model-score cutoff: every row of the exactly-selected promoted security
    master receives exactly one entry. Stable identity, when resolved,
    remains a research identifier only -- ``readiness`` is always
    COLLECTION_ONLY, ``actionable`` is always False, and
    ``execution_eligible`` is always False regardless of stable-ID coverage.
    __post_init__ calls verify_content_identity(), which requires the exact
    concrete type (rejecting subclasses/impostors even when every retained
    field is otherwise valid) and independently replays the complete
    derivation, so direct construction with a mismatched value or
    post-construction mutation anywhere in the retained graph fails closed
    with one static sanitized PromotedIdentitySessionUniverseError.
    """

    schema_version: str
    policy_version: str
    adjudication: VerifiedPromotedIdentityAdjudication
    calendar: CollectionCalendarMaterialization
    market_session: date
    cutoff: datetime
    knowledge_time: datetime
    selected_promotion_id: str
    selected_join_id: str
    selected_source_artifact_id: str
    selected_source_manifest_id: str
    selected_raw_sha256: str
    selected_normalized_sha256: str
    entries: tuple[PromotedIdentitySessionEntry, ...]
    disposition_counts: tuple[tuple[str, int], ...]
    reason_counts: tuple[tuple[str, int], ...]
    readiness: ReferenceReadiness
    actionable: bool
    execution_eligible: bool
    universe_id: str

    def __post_init__(self) -> None:
        self.verify_content_identity()

    def verify_content_identity(self) -> None:
        if type(self) is not VerifiedPromotedIdentitySessionUniverse:
            raise PromotedIdentitySessionUniverseError(_ERR_TYPE)
        if (
            type(self.schema_version) is not str
            or self.schema_version != PROMOTED_IDENTITY_SESSION_UNIVERSE_SCHEMA_VERSION
        ):
            raise PromotedIdentitySessionUniverseError(_ERR_SCHEMA_VERSION)
        if (
            type(self.policy_version) is not str
            or self.policy_version != PROMOTED_IDENTITY_SESSION_UNIVERSE_POLICY_VERSION
        ):
            raise PromotedIdentitySessionUniverseError(_ERR_SCHEMA_VERSION)
        if type(self.adjudication) is not VerifiedPromotedIdentityAdjudication:
            raise PromotedIdentitySessionUniverseError(_ERR_ADJUDICATION)
        if type(self.calendar) is not CollectionCalendarMaterialization:
            raise PromotedIdentitySessionUniverseError(_ERR_CALENDAR)
        if type(self.market_session) is not date:
            raise PromotedIdentitySessionUniverseError(_ERR_SESSION)
        if type(self.cutoff) is not datetime:
            raise PromotedIdentitySessionUniverseError(_ERR_CUTOFF)
        if type(self.knowledge_time) is not datetime:
            raise PromotedIdentitySessionUniverseError(_ERR_DERIVED_FIELD)
        for value in (
            self.selected_promotion_id,
            self.selected_join_id,
            self.selected_source_artifact_id,
            self.selected_source_manifest_id,
            self.selected_raw_sha256,
            self.selected_normalized_sha256,
        ):
            if type(value) is not str or _SHA256.fullmatch(value) is None:
                raise PromotedIdentitySessionUniverseError(_ERR_DERIVED_FIELD)
        if type(self.entries) is not tuple or any(
            type(value) is not PromotedIdentitySessionEntry for value in self.entries
        ):
            raise PromotedIdentitySessionUniverseError(_ERR_DERIVED_FIELD)
        for values in (self.disposition_counts, self.reason_counts):
            if type(values) is not tuple or any(
                type(pair) is not tuple or len(pair) != 2 for pair in values
            ):
                raise PromotedIdentitySessionUniverseError(_ERR_DERIVED_FIELD)
        if (
            type(self.readiness) is not ReferenceReadiness
            or self.readiness is not ReferenceReadiness.COLLECTION_ONLY
        ):
            raise PromotedIdentitySessionUniverseError(_ERR_DERIVED_FIELD)
        if type(self.actionable) is not bool or self.actionable is not False:
            raise PromotedIdentitySessionUniverseError(_ERR_DERIVED_FIELD)
        if (
            type(self.execution_eligible) is not bool
            or self.execution_eligible is not False
        ):
            raise PromotedIdentitySessionUniverseError(_ERR_DERIVED_FIELD)
        if type(self.universe_id) is not str or _SHA256.fullmatch(self.universe_id) is None:
            raise PromotedIdentitySessionUniverseError(_ERR_UNIVERSE_ID)

        try:
            facts = _build_session_universe_facts(
                self.adjudication, self.calendar, self.market_session, self.cutoff
            )
        except PromotedIdentitySessionUniverseError:
            raise
        except Exception:
            raise PromotedIdentitySessionUniverseError(_ERR_DERIVED_FIELD) from None

        # Every comparison below may recursively invoke a retained nested
        # value's own __eq__, so a single fail-closed boundary wraps all of
        # them: an already-raised PromotedIdentitySessionUniverseError from
        # an intentional mismatch below propagates unchanged, while any
        # other ordinary Exception raised by equality itself (never
        # BaseException/KeyboardInterrupt/SystemExit) is translated to one
        # static sanitized message.
        try:
            if self.cutoff != facts.cutoff:
                raise PromotedIdentitySessionUniverseError(_ERR_DERIVED_FIELD)
            if self.knowledge_time != facts.knowledge_time:
                raise PromotedIdentitySessionUniverseError(_ERR_DERIVED_FIELD)
            if self.selected_promotion_id != facts.selected_promotion_id:
                raise PromotedIdentitySessionUniverseError(_ERR_DERIVED_FIELD)
            if self.selected_join_id != facts.selected_join_id:
                raise PromotedIdentitySessionUniverseError(_ERR_DERIVED_FIELD)
            if self.selected_source_artifact_id != facts.selected_source_artifact_id:
                raise PromotedIdentitySessionUniverseError(_ERR_DERIVED_FIELD)
            if self.selected_source_manifest_id != facts.selected_source_manifest_id:
                raise PromotedIdentitySessionUniverseError(_ERR_DERIVED_FIELD)
            if self.selected_raw_sha256 != facts.selected_raw_sha256:
                raise PromotedIdentitySessionUniverseError(_ERR_DERIVED_FIELD)
            if self.selected_normalized_sha256 != facts.selected_normalized_sha256:
                raise PromotedIdentitySessionUniverseError(_ERR_DERIVED_FIELD)
            if self.entries != facts.entries:
                raise PromotedIdentitySessionUniverseError(_ERR_DERIVED_FIELD)
            if self.disposition_counts != facts.disposition_counts:
                raise PromotedIdentitySessionUniverseError(_ERR_DERIVED_FIELD)
            if self.reason_counts != facts.reason_counts:
                raise PromotedIdentitySessionUniverseError(_ERR_DERIVED_FIELD)
            if self.universe_id != facts.universe_id:
                raise PromotedIdentitySessionUniverseError(_ERR_UNIVERSE_ID)
        except PromotedIdentitySessionUniverseError:
            raise
        except Exception:
            raise PromotedIdentitySessionUniverseError(_ERR_COMPARISON_FAILED) from None


class PromotedIdentitySessionUniverseService:
    """Materializes one session-specific, small-cap-inclusive NSE CM
    collection universe from a VerifiedPromotedIdentityAdjudication and an
    exact CollectionCalendarMaterialization.

    Never infers listing intervals, delisting, suspension, board,
    surveillance, liquidity, or corporate-action state from source presence/
    absence, timestamps, delete flags, or stable identity. Never applies a
    market-cap, price, volume, liquidity, index-membership, or model-score
    filter. Never selects a latest/nearest source or calendar value.
    """

    def materialize(
        self,
        *,
        adjudication: VerifiedPromotedIdentityAdjudication,
        calendar: CollectionCalendarMaterialization,
        market_session: date,
        cutoff: datetime,
    ) -> VerifiedPromotedIdentitySessionUniverse:
        facts = _build_session_universe_facts(
            adjudication, calendar, market_session, cutoff
        )
        return VerifiedPromotedIdentitySessionUniverse(
            schema_version=PROMOTED_IDENTITY_SESSION_UNIVERSE_SCHEMA_VERSION,
            policy_version=PROMOTED_IDENTITY_SESSION_UNIVERSE_POLICY_VERSION,
            adjudication=adjudication,
            calendar=calendar,
            market_session=market_session,
            cutoff=facts.cutoff,
            knowledge_time=facts.knowledge_time,
            selected_promotion_id=facts.selected_promotion_id,
            selected_join_id=facts.selected_join_id,
            selected_source_artifact_id=facts.selected_source_artifact_id,
            selected_source_manifest_id=facts.selected_source_manifest_id,
            selected_raw_sha256=facts.selected_raw_sha256,
            selected_normalized_sha256=facts.selected_normalized_sha256,
            entries=facts.entries,
            disposition_counts=facts.disposition_counts,
            reason_counts=facts.reason_counts,
            readiness=facts.readiness,
            actionable=facts.actionable,
            execution_eligible=facts.execution_eligible,
            universe_id=facts.universe_id,
        )
