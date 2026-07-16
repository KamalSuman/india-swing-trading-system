from __future__ import annotations

from dataclasses import dataclass

from india_swing.domain.models import (
    Board,
    DataSnapshot,
    InstrumentSnapshot,
    Surveillance,
)

from .calendar import CalendarIntegrityError, CalendarSnapshot
from .models import ReferenceReadiness
from .universe import ListingState, UniverseIntegrityError, UniverseSnapshot


class ReferenceContextError(ValueError):
    pass


@dataclass(frozen=True, slots=True)
class ReferenceContext:
    calendar: CalendarSnapshot
    universe: UniverseSnapshot

    def __post_init__(self) -> None:
        if type(self.calendar) is not CalendarSnapshot:
            raise TypeError("reference calendar must be an exact CalendarSnapshot")
        if type(self.universe) is not UniverseSnapshot:
            raise TypeError("reference universe must be an exact UniverseSnapshot")


def validate_reference_context(
    snapshot: DataSnapshot,
    instruments: list[InstrumentSnapshot],
    context: ReferenceContext,
) -> None:
    if type(snapshot) is not DataSnapshot:
        raise ReferenceContextError("decision snapshot must be an exact DataSnapshot")
    if type(context) is not ReferenceContext:
        raise ReferenceContextError("reference context must be an exact ReferenceContext")
    if type(instruments) is not list or any(
        type(instrument) is not InstrumentSnapshot for instrument in instruments
    ):
        raise ReferenceContextError(
            "scan instruments must be a list of exact InstrumentSnapshot values"
        )
    calendar = context.calendar
    universe = context.universe
    if type(calendar) is not CalendarSnapshot:
        raise ReferenceContextError(
            "reference calendar must remain an exact CalendarSnapshot"
        )
    if type(universe) is not UniverseSnapshot:
        raise ReferenceContextError(
            "reference universe must remain an exact UniverseSnapshot"
        )

    if calendar.readiness is ReferenceReadiness.COLLECTION_ONLY:
        raise ReferenceContextError("collection-only calendar is not decision eligible")
    if universe.readiness is ReferenceReadiness.COLLECTION_ONLY:
        raise ReferenceContextError("collection-only universe is not decision eligible")
    if (
        calendar.readiness is ReferenceReadiness.POINT_IN_TIME_VERIFIED
        or universe.readiness is ReferenceReadiness.POINT_IN_TIME_VERIFIED
    ):
        raise ReferenceContextError(
            "point-in-time decisions remain locked until the official importer exists"
        )
    try:
        calendar.verify_content_identity()
        universe.verify_content_identity()
    except (CalendarIntegrityError, UniverseIntegrityError, TypeError, ValueError) as exc:
        raise ReferenceContextError("reference content identity verification failed") from exc
    if calendar.readiness is not universe.readiness:
        raise ReferenceContextError("calendar and universe readiness levels disagree")
    if (calendar.exchange, calendar.segment) != (universe.exchange, universe.segment):
        raise ReferenceContextError(
            "calendar and universe exchange/segment scopes disagree"
        )
    if calendar.readiness is ReferenceReadiness.SYNTHETIC_TEST and not snapshot.trial_id.startswith(
        "synthetic-"
    ):
        raise ReferenceContextError("synthetic reference data requires a synthetic trial")
    if universe.calendar_snapshot_id != calendar.snapshot_id:
        raise ReferenceContextError("universe references a different calendar snapshot")
    if snapshot.calendar_version != calendar.version:
        raise ReferenceContextError("decision calendar version does not match its artifact")
    if snapshot.universe_snapshot_id != universe.snapshot_id:
        raise ReferenceContextError("decision universe ID does not match its artifact")
    if universe.market_session != snapshot.market_session:
        raise ReferenceContextError("decision and universe market sessions disagree")
    if universe.cutoff != snapshot.decision_time:
        raise ReferenceContextError("universe cutoff must equal the decision cutoff")
    if calendar.cutoff > snapshot.decision_time:
        raise ReferenceContextError("calendar vintage was unavailable at decision time")

    try:
        signal_session = calendar.require_session(snapshot.market_session)
    except CalendarIntegrityError as exc:
        raise ReferenceContextError(
            "decision market_session is not an eligible calendar session"
        ) from exc
    if signal_session.data_ready_at is None:
        raise ReferenceContextError(
            "decision calendar lacks a separately sourced data-finality policy"
        )
    if snapshot.session_finalized_at < signal_session.data_ready_at:
        raise ReferenceContextError(
            "decision finality precedes the calendar session data-ready time"
        )
    try:
        entry_session = calendar.next_session(snapshot.market_session)
    except CalendarIntegrityError as exc:
        raise ReferenceContextError(
            "calendar does not cover the next eligible entry session"
        ) from exc

    instrument_ids = [instrument.instrument_id for instrument in instruments]
    if len(instrument_ids) != len(set(instrument_ids)):
        raise ReferenceContextError("scan instruments contain duplicate stable IDs")
    entry_by_id = {entry.listing.instrument_id: entry for entry in universe.entries}
    supplied_ids = set(instrument_ids)
    actionable_ids = {
        entry.listing.instrument_id for entry in universe.actionable_entries
    }
    if supplied_ids != actionable_ids:
        raise ReferenceContextError(
            "scan instruments must exactly equal actionable universe members"
        )

    for instrument in instruments:
        try:
            instrument.verify_content_identity()
        except ValueError as exc:
            raise ReferenceContextError(
                "instrument content identity verification failed"
            ) from exc
        entry = entry_by_id[instrument.instrument_id]
        if instrument.universe_snapshot_id != universe.snapshot_id:
            raise ReferenceContextError("instrument carries a different universe snapshot ID")
        if (instrument.exchange, instrument.segment) != (
            universe.exchange,
            universe.segment,
        ):
            raise ReferenceContextError(
                "instrument exchange/segment does not match the universe"
            )
        if instrument.listing_id != entry.listing.listing_id:
            raise ReferenceContextError("instrument listing ID does not match the universe")
        if instrument.symbol != entry.listing.tradingsymbol:
            raise ReferenceContextError("instrument symbol does not match its validity mapping")
        if instrument.board is not entry.board:
            raise ReferenceContextError("instrument board does not match the universe")
        if instrument.active is not (entry.listing_state is ListingState.ACTIVE):
            raise ReferenceContextError("instrument active state does not match the universe")
        if instrument.suspended is not entry.suspended:
            raise ReferenceContextError("instrument suspension state does not match the universe")
        if instrument.surveillance is not entry.surveillance:
            raise ReferenceContextError("instrument surveillance does not match the universe")
        if not entry.listing.is_valid_on(entry_session.day):
            raise ReferenceContextError(
                "instrument listing mapping does not remain valid for entry session"
            )
        try:
            entry_state = entry.eligibility_state_on(entry_session.day)
        except UniverseIntegrityError as exc:
            raise ReferenceContextError(
                "instrument eligibility lineage does not resolve for entry session"
            ) from exc
        if (
            entry_state.board is not Board.MAIN
            or entry_state.listing_state is not ListingState.ACTIVE
            or entry_state.suspended is not False
            or entry_state.surveillance is not Surveillance.NONE
        ):
            raise ReferenceContextError(
                "instrument is not eligible on the next entry session"
            )
