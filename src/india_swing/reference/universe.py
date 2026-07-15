from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import date, datetime
from enum import Enum

from india_swing.domain.models import Board, INDIA_STANDARD_TIME, Surveillance
from india_swing.identity import content_id

from .models import (
    EffectiveExternalRecordRef,
    ExternalRecordRef,
    ReferenceReadiness,
)


UNIVERSE_SCHEMA_VERSION = "reference-universe/v4"
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_ISIN = re.compile(r"[A-Z]{2}[A-Z0-9]{9}[0-9]\Z")
_REASON_CODE = re.compile(r"[A-Z][A-Z0-9_]{1,63}\Z")


class UniverseIntegrityError(ValueError):
    pass


class UniverseDisposition(str, Enum):
    ACTIONABLE = "ACTIONABLE"
    WATCH_ONLY = "WATCH_ONLY"
    EXCLUDED = "EXCLUDED"
    UNVERIFIED = "UNVERIFIED"


class ListingState(str, Enum):
    ACTIVE = "ACTIVE"
    DELISTED = "DELISTED"
    UNKNOWN = "UNKNOWN"


def _require_sha256(value: str, field_name: str) -> None:
    if not isinstance(value, str) or _SHA256.fullmatch(value) is None:
        raise ValueError(f"{field_name} must be a full lowercase SHA-256 identifier")


def _require_date(value: date, field_name: str) -> None:
    if type(value) is not date:
        raise TypeError(f"{field_name} must be a date")


def _require_aware(value: datetime, field_name: str) -> None:
    if not isinstance(value, datetime):
        raise TypeError(f"{field_name} must be a datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")


def _required_text(value: str, field_name: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} is required")


def _reference_sort_key(
    state: EligibilityStateRef,
) -> tuple[str, str, datetime, date, date, str, str, str, str, str, str, str]:
    effective = state.effective
    reference = effective.reference
    return (
        reference.source_snapshot_id,
        reference.content_hash,
        reference.knowledge_time,
        effective.effective_from_session,
        effective.effective_to_exclusive or date.max,
        effective.schema_version,
        state.instrument_id,
        state.listing_id,
        state.board.value,
        state.listing_state.value,
        str(state.suspended),
        state.surveillance.value,
    )


@dataclass(frozen=True, slots=True)
class EligibilityStateRef:
    """Effective-dated eligibility facts supported by one source record."""

    effective: EffectiveExternalRecordRef
    instrument_id: str
    listing_id: str
    board: Board
    listing_state: ListingState
    suspended: bool | None
    surveillance: Surveillance

    def __post_init__(self) -> None:
        if type(self.effective) is not EffectiveExternalRecordRef:
            raise TypeError(
                "eligibility_state.effective must be an exact effective record"
            )
        _required_text(self.instrument_id, "eligibility_state.instrument_id")
        _required_text(self.listing_id, "eligibility_state.listing_id")
        if not isinstance(self.board, Board):
            raise TypeError("eligibility_state.board must be a Board")
        if not isinstance(self.listing_state, ListingState):
            raise TypeError(
                "eligibility_state.listing_state must be a ListingState"
            )
        if self.suspended is not None and type(self.suspended) is not bool:
            raise TypeError("eligibility_state.suspended must be bool or None")
        if not isinstance(self.surveillance, Surveillance):
            raise TypeError(
                "eligibility_state.surveillance must be a Surveillance"
            )


@dataclass(frozen=True, slots=True)
class ListingMapping:
    """Validity-dated symbol mapping for an opaque internal instrument identity.

    ``instrument_id`` must be assigned by a separately audited identity registry.
    This model deliberately does not derive it from a ticker, ISIN, or Kite token.
    """

    instrument_id: str
    listing_id: str
    exchange: str
    segment: str
    tradingsymbol: str
    series: str
    isin: str | None
    valid_from: date
    valid_to_exclusive: date | None
    reference: ExternalRecordRef

    def __post_init__(self) -> None:
        for name in (
            "instrument_id",
            "listing_id",
            "exchange",
            "segment",
            "tradingsymbol",
            "series",
        ):
            _required_text(getattr(self, name), f"listing.{name}")
        for name in ("exchange", "segment", "tradingsymbol", "series"):
            value = getattr(self, name)
            if value != value.strip().upper():
                raise ValueError(f"listing.{name} must be normalized uppercase text")
        if self.isin is not None and _ISIN.fullmatch(self.isin) is None:
            raise ValueError("listing.isin must be an uppercase 12-character ISIN")
        _require_date(self.valid_from, "listing.valid_from")
        if self.valid_to_exclusive is not None:
            _require_date(self.valid_to_exclusive, "listing.valid_to_exclusive")
            if self.valid_to_exclusive <= self.valid_from:
                raise ValueError("listing validity interval must be positive and half-open")
        if type(self.reference) is not ExternalRecordRef:
            raise TypeError("listing.reference must be an exact ExternalRecordRef")

    @property
    def listing_key(self) -> str:
        return f"{self.exchange}:{self.segment}:{self.tradingsymbol}:{self.series}"

    def is_valid_on(self, session: date) -> bool:
        _require_date(session, "listing session")
        return self.valid_from <= session and (
            self.valid_to_exclusive is None or session < self.valid_to_exclusive
        )


@dataclass(frozen=True, slots=True)
class UniverseEntry:
    source_record_id: str
    listing: ListingMapping
    board: Board
    listing_state: ListingState
    suspended: bool | None
    surveillance: Surveillance
    disposition: UniverseDisposition
    reason_codes: tuple[str, ...]
    eligibility_refs: tuple[EligibilityStateRef, ...]
    liquidity_snapshot_id: str | None = None
    liquidity_cutoff_session: date | None = None

    def __post_init__(self) -> None:
        _require_sha256(self.source_record_id, "universe_entry.source_record_id")
        if type(self.listing) is not ListingMapping:
            raise TypeError("universe_entry.listing must be an exact ListingMapping")
        if not isinstance(self.board, Board):
            raise TypeError("universe_entry.board must be a Board")
        if not isinstance(self.listing_state, ListingState):
            raise TypeError("universe_entry.listing_state must be a ListingState")
        if self.suspended is not None and type(self.suspended) is not bool:
            raise TypeError("universe_entry.suspended must be bool or None")
        if not isinstance(self.surveillance, Surveillance):
            raise TypeError("universe_entry.surveillance must be a Surveillance")
        if not isinstance(self.disposition, UniverseDisposition):
            raise TypeError("universe_entry.disposition must be a UniverseDisposition")
        if type(self.reason_codes) is not tuple:
            raise TypeError("universe_entry.reason_codes must be an immutable tuple")
        if tuple(sorted(set(self.reason_codes))) != self.reason_codes:
            raise UniverseIntegrityError("reason codes must be unique and sorted")
        if any(_REASON_CODE.fullmatch(code) is None for code in self.reason_codes):
            raise UniverseIntegrityError("reason codes must be normalized machine codes")
        if type(self.eligibility_refs) is not tuple or any(
            type(reference) is not EligibilityStateRef
            for reference in self.eligibility_refs
        ):
            raise TypeError(
                "eligibility_refs must be an immutable eligibility-state tuple"
            )
        if tuple(sorted(self.eligibility_refs, key=_reference_sort_key)) != self.eligibility_refs:
            raise UniverseIntegrityError("eligibility_refs must be sorted")
        if len({_reference_sort_key(ref) for ref in self.eligibility_refs}) != len(
            self.eligibility_refs
        ):
            raise UniverseIntegrityError("eligibility_refs cannot contain duplicates")
        if any(
            reference.instrument_id != self.listing.instrument_id
            or reference.listing_id != self.listing.listing_id
            for reference in self.eligibility_refs
        ):
            raise UniverseIntegrityError(
                "eligibility reference subject does not match the universe entry"
            )
        effective_order = sorted(
            self.eligibility_refs,
            key=lambda state: (
                state.effective.effective_from_session,
                state.effective.effective_to_exclusive or date.max,
                _reference_sort_key(state),
            ),
        )
        for previous, current in zip(effective_order, effective_order[1:]):
            previous_end = previous.effective.effective_to_exclusive
            if (
                previous_end is None
                or current.effective.effective_from_session < previous_end
            ):
                raise UniverseIntegrityError(
                    "eligibility effective intervals cannot overlap"
                )
        if self.liquidity_snapshot_id is not None:
            _require_sha256(
                self.liquidity_snapshot_id,
                "universe_entry.liquidity_snapshot_id",
            )
        if self.liquidity_cutoff_session is not None:
            _require_date(
                self.liquidity_cutoff_session,
                "universe_entry.liquidity_cutoff_session",
            )
        if (self.liquidity_snapshot_id is None) != (
            self.liquidity_cutoff_session is None
        ):
            raise UniverseIntegrityError(
                "liquidity snapshot ID and cutoff session must be supplied together"
            )

        if self.disposition is UniverseDisposition.ACTIONABLE:
            if self.board is not Board.MAIN:
                raise UniverseIntegrityError("only verified main-board entries can be actionable")
            if self.listing_state is not ListingState.ACTIVE or self.suspended is not False:
                raise UniverseIntegrityError("actionable entries must be active and unsuspended")
            if self.surveillance is not Surveillance.NONE:
                raise UniverseIntegrityError(
                    "actionable entries require a verified no-surveillance state"
                )
            if self.reason_codes:
                raise UniverseIntegrityError("actionable entries cannot have exclusion reasons")
            if not self.eligibility_refs:
                raise UniverseIntegrityError("actionable entries require eligibility lineage")
            if self.liquidity_snapshot_id is None:
                raise UniverseIntegrityError("actionable entries require liquidity lineage")
        elif not self.reason_codes:
            raise UniverseIntegrityError("non-actionable entries require reason codes")

        if self.disposition is UniverseDisposition.WATCH_ONLY and self.board is not Board.SME:
            raise UniverseIntegrityError("watch-only disposition is reserved for verified SME")
        if (
            self.board is Board.UNKNOWN
            or self.listing_state is ListingState.UNKNOWN
            or self.suspended is None
            or self.surveillance is Surveillance.UNKNOWN
        ) and self.disposition is not UniverseDisposition.UNVERIFIED:
            raise UniverseIntegrityError("unknown reference facts must remain unverified")

    def eligibility_state_on(self, session: date) -> EligibilityStateRef:
        """Resolve the single eligibility fact set governing an exchange session."""

        _require_date(session, "eligibility session")
        matches = tuple(
            state
            for state in self.eligibility_refs
            if state.effective.is_effective_on(session)
        )
        if len(matches) != 1:
            raise UniverseIntegrityError(
                "exactly one eligibility state must be effective for the requested session"
            )
        return matches[0]


@dataclass(frozen=True, slots=True)
class UniverseSnapshot:
    exchange: str
    segment: str
    market_session: date
    cutoff: datetime
    calendar_snapshot_id: str
    universe_rules_version: str
    selection_key: str
    scoped_source_row_ids: tuple[str, ...]
    security_master_snapshot_ids: tuple[str, ...]
    eligibility_snapshot_ids: tuple[str, ...]
    liquidity_snapshot_ids: tuple[str, ...]
    readiness: ReferenceReadiness
    entries: tuple[UniverseEntry, ...]
    schema_version: str = UNIVERSE_SCHEMA_VERSION
    selected_records_hash: str = field(init=False)
    snapshot_id: str = field(init=False)

    @classmethod
    def create(
        cls,
        *,
        exchange: str,
        segment: str,
        market_session: date,
        cutoff: datetime,
        calendar_snapshot_id: str,
        universe_rules_version: str,
        selection_key: str,
        scoped_source_row_ids: tuple[str, ...],
        security_master_snapshot_ids: tuple[str, ...],
        eligibility_snapshot_ids: tuple[str, ...],
        liquidity_snapshot_ids: tuple[str, ...],
        readiness: ReferenceReadiness,
        entries: tuple[UniverseEntry, ...],
        schema_version: str = UNIVERSE_SCHEMA_VERSION,
    ) -> UniverseSnapshot:
        return cls(
            exchange=exchange,
            segment=segment,
            market_session=market_session,
            cutoff=cutoff,
            calendar_snapshot_id=calendar_snapshot_id,
            universe_rules_version=universe_rules_version,
            selection_key=selection_key,
            scoped_source_row_ids=scoped_source_row_ids,
            security_master_snapshot_ids=security_master_snapshot_ids,
            eligibility_snapshot_ids=eligibility_snapshot_ids,
            liquidity_snapshot_ids=liquidity_snapshot_ids,
            readiness=readiness,
            entries=entries,
            schema_version=schema_version,
        )

    def __post_init__(self) -> None:
        for name in ("exchange", "segment"):
            value = getattr(self, name)
            _required_text(value, f"universe.{name}")
            if value != value.strip().upper():
                raise UniverseIntegrityError(
                    f"universe.{name} must be normalized uppercase text"
                )
        _require_date(self.market_session, "universe.market_session")
        _require_aware(self.cutoff, "universe.cutoff")
        _require_sha256(self.calendar_snapshot_id, "universe.calendar_snapshot_id")
        _required_text(self.universe_rules_version, "universe.universe_rules_version")
        _required_text(self.selection_key, "universe.selection_key")
        if self.schema_version != UNIVERSE_SCHEMA_VERSION:
            raise UniverseIntegrityError("unsupported universe schema version")
        if not isinstance(self.readiness, ReferenceReadiness):
            raise TypeError("universe readiness must be a ReferenceReadiness")
        if self.readiness is ReferenceReadiness.POINT_IN_TIME_VERIFIED:
            raise UniverseIntegrityError(
                "point-in-time universe verification is unavailable until the official artifact importer exists"
            )

        id_groups = (
            ("scoped_source_row_ids", self.scoped_source_row_ids, True),
            ("security_master_snapshot_ids", self.security_master_snapshot_ids, True),
            ("eligibility_snapshot_ids", self.eligibility_snapshot_ids, False),
            ("liquidity_snapshot_ids", self.liquidity_snapshot_ids, False),
        )
        for name, values, required in id_groups:
            if type(values) is not tuple:
                raise TypeError(f"universe.{name} must be an immutable tuple")
            if required and not values:
                raise UniverseIntegrityError(f"universe.{name} is required")
            if tuple(sorted(set(values))) != values:
                raise UniverseIntegrityError(f"universe.{name} must be unique and sorted")
            for value in values:
                _require_sha256(value, f"universe.{name}")
        if type(self.entries) is not tuple or any(
            type(entry) is not UniverseEntry for entry in self.entries
        ):
            raise TypeError(
                "universe.entries must be an immutable exact UniverseEntry tuple"
            )
        if tuple(sorted(self.entries, key=lambda item: item.source_record_id)) != self.entries:
            raise UniverseIntegrityError("universe entries must be sorted by source_record_id")
        entry_row_ids = tuple(entry.source_record_id for entry in self.entries)
        if entry_row_ids != self.scoped_source_row_ids:
            raise UniverseIntegrityError(
                "every scoped security-master row requires exactly one disposition entry"
            )

        instrument_ids = [entry.listing.instrument_id for entry in self.entries]
        if len(instrument_ids) != len(set(instrument_ids)):
            raise UniverseIntegrityError("universe contains duplicate stable instrument IDs")
        listing_keys = [entry.listing.listing_key for entry in self.entries]
        if len(listing_keys) != len(set(listing_keys)):
            raise UniverseIntegrityError("universe contains duplicate active listing keys")

        for entry in self.entries:
            if entry.listing.exchange != self.exchange:
                raise UniverseIntegrityError(
                    "universe contains a listing from another exchange"
                )
            if entry.listing.segment != self.segment:
                raise UniverseIntegrityError(
                    "universe contains a listing from another segment"
                )
            if not entry.listing.is_valid_on(self.market_session):
                raise UniverseIntegrityError("universe contains a listing invalid for its session")
            references = (entry.listing.reference,) + tuple(
                state.effective.reference for state in entry.eligibility_refs
            )
            if any(reference.knowledge_time > self.cutoff for reference in references):
                raise UniverseIntegrityError("universe contains a record known after its cutoff")
            if entry.eligibility_refs:
                signal_state = entry.eligibility_state_on(self.market_session)
                if (
                    signal_state.board,
                    signal_state.listing_state,
                    signal_state.suspended,
                    signal_state.surveillance,
                ) != (
                    entry.board,
                    entry.listing_state,
                    entry.suspended,
                    entry.surveillance,
                ):
                    raise UniverseIntegrityError(
                        "effective eligibility facts do not match the universe session state"
                    )
            if (
                entry.listing.reference.source_snapshot_id
                not in self.security_master_snapshot_ids
            ):
                raise UniverseIntegrityError(
                    "listing reference is absent from security-master lineage"
                )
            if any(
                state.effective.reference.source_snapshot_id
                not in self.eligibility_snapshot_ids
                for state in entry.eligibility_refs
            ):
                raise UniverseIntegrityError(
                    "eligibility reference is absent from universe lineage"
                )
            if entry.liquidity_snapshot_id is not None:
                if entry.liquidity_snapshot_id not in self.liquidity_snapshot_ids:
                    raise UniverseIntegrityError(
                        "liquidity reference is absent from universe lineage"
                    )
                assert entry.liquidity_cutoff_session is not None
                if entry.liquidity_cutoff_session > self.market_session:
                    raise UniverseIntegrityError(
                        "liquidity cutoff cannot follow the universe session"
                    )

        if self.readiness is ReferenceReadiness.COLLECTION_ONLY and any(
            entry.disposition is not UniverseDisposition.UNVERIFIED
            for entry in self.entries
        ):
            raise UniverseIntegrityError(
                "collection-only universes can contain only unverified entries"
            )
        if self.readiness is ReferenceReadiness.POINT_IN_TIME_VERIFIED and any(
            entry.disposition is UniverseDisposition.UNVERIFIED for entry in self.entries
        ):
            raise UniverseIntegrityError(
                "point-in-time-verified universes cannot contain unverified entries"
            )
        if any(
            entry.disposition is UniverseDisposition.ACTIONABLE for entry in self.entries
        ) and self.readiness not in {
            ReferenceReadiness.POINT_IN_TIME_VERIFIED,
            ReferenceReadiness.SYNTHETIC_TEST,
        }:
            raise UniverseIntegrityError("actionable entries require verified readiness")

        selected_records_hash = self._calculated_selected_records_hash()
        object.__setattr__(self, "selected_records_hash", selected_records_hash)
        snapshot_id = self._calculated_snapshot_id(selected_records_hash)
        object.__setattr__(self, "snapshot_id", snapshot_id)

    def _calculated_selected_records_hash(self) -> str:
        return content_id(
            {
                "scoped_source_row_ids": self.scoped_source_row_ids,
                "entries": self.entries,
            },
            length=64,
        )

    def _calculated_snapshot_id(self, selected_records_hash: str) -> str:
        return content_id(
            {
                "schema_version": self.schema_version,
                "exchange": self.exchange,
                "segment": self.segment,
                "market_session": self.market_session,
                "cutoff": self.cutoff,
                "calendar_snapshot_id": self.calendar_snapshot_id,
                "universe_rules_version": self.universe_rules_version,
                "selection_key": self.selection_key,
                "scoped_source_row_ids": self.scoped_source_row_ids,
                "security_master_snapshot_ids": self.security_master_snapshot_ids,
                "eligibility_snapshot_ids": self.eligibility_snapshot_ids,
                "liquidity_snapshot_ids": self.liquidity_snapshot_ids,
                "readiness": self.readiness,
                "selected_records_hash": selected_records_hash,
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        """Detect any mutation or deserialization bypass after construction."""

        if any(
            type(entry) is not UniverseEntry
            or type(entry.listing) is not ListingMapping
            or type(entry.listing.reference) is not ExternalRecordRef
            or any(
                type(state) is not EligibilityStateRef
                or type(state.effective) is not EffectiveExternalRecordRef
                or type(state.effective.reference) is not ExternalRecordRef
                for state in entry.eligibility_refs
            )
            for entry in self.entries
        ):
            raise UniverseIntegrityError(
                "universe reference graph must contain only exact audited types"
            )
        expected_records_hash = self._calculated_selected_records_hash()
        expected_snapshot_id = self._calculated_snapshot_id(expected_records_hash)
        if (
            self.selected_records_hash != expected_records_hash
            or self.snapshot_id != expected_snapshot_id
        ):
            raise UniverseIntegrityError("universe content identity verification failed")

    @property
    def source_record_count(self) -> int:
        return len(self.scoped_source_row_ids)

    @property
    def actionable_entries(self) -> tuple[UniverseEntry, ...]:
        return tuple(
            entry
            for entry in self.entries
            if entry.disposition is UniverseDisposition.ACTIONABLE
        )
