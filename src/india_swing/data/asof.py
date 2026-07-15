from __future__ import annotations

from india_swing.domain.models import Candidate, DataSnapshot, INDIA_STANDARD_TIME
from india_swing.reference.calendar import CalendarIntegrityError, CalendarSnapshot


class DataIntegrityError(ValueError):
    """Raised when required evidence is absent or internally inconsistent."""


class LookaheadViolation(DataIntegrityError):
    """Raised when a decision attempts to use information unavailable at its cutoff."""


def validate_snapshot(snapshot: DataSnapshot) -> None:
    try:
        snapshot.verify_content_identity()
    except (TypeError, ValueError) as exc:
        raise DataIntegrityError("data snapshot content identity verification failed") from exc
    future = [
        item.evidence_id
        for item in snapshot.evidence
        if item.available_at > snapshot.decision_time
    ]
    if future:
        joined = ", ".join(sorted(future))
        raise LookaheadViolation(f"snapshot contains future evidence: {joined}")


def validate_candidate(
    candidate: Candidate,
    snapshot: DataSnapshot,
    calendar: CalendarSnapshot | None = None,
) -> None:
    try:
        candidate.instrument.verify_content_identity()
    except ValueError as exc:
        raise DataIntegrityError(
            "instrument snapshot content identity verification failed"
        ) from exc
    if candidate.instrument.universe_snapshot_id != snapshot.universe_snapshot_id:
        raise DataIntegrityError(
            "instrument universe snapshot does not match the decision snapshot"
        )
    for component_name, component in (
        ("forecast", candidate.forecast),
        ("signals", candidate.signals),
        ("setup", candidate.setup),
    ):
        if component.universe_snapshot_id != snapshot.universe_snapshot_id:
            raise DataIntegrityError(
                f"{component_name} universe snapshot does not match the decision snapshot"
            )
        if component.data_snapshot_id != snapshot.snapshot_id:
            raise DataIntegrityError(
                f"{component_name} data snapshot does not match the decision snapshot"
            )
        if component.data_snapshot_fingerprint != snapshot.content_fingerprint:
            raise DataIntegrityError(
                f"{component_name} snapshot content does not match the decision snapshot"
            )
        if component.instrument_fingerprint != candidate.instrument.content_fingerprint:
            raise DataIntegrityError(
                f"{component_name} instrument content does not match its market snapshot"
            )
    if candidate.instrument.price_session != snapshot.market_session:
        raise DataIntegrityError("instrument price session does not match the signal session")
    if candidate.instrument.data_available_at < snapshot.session_finalized_at:
        raise DataIntegrityError(
            "same-session EOD price data cannot be available before session finalization"
        )
    if candidate.instrument.data_available_at > snapshot.decision_time:
        raise LookaheadViolation(
            f"{candidate.instrument.symbol} market data was unavailable at decision time"
        )
    if candidate.forecast.as_of > snapshot.decision_time:
        raise LookaheadViolation(
            f"{candidate.instrument.symbol} forecast uses an as_of after decision time"
        )
    if candidate.forecast.as_of < snapshot.decision_time:
        raise DataIntegrityError(
            f"{candidate.instrument.symbol} forecast is stale for the decision cutoff"
        )
    if candidate.setup.decision_time != snapshot.decision_time:
        raise DataIntegrityError("setup decision_time must equal snapshot decision_time")
    if candidate.setup.earliest_entry_at <= snapshot.decision_time:
        raise LookaheadViolation("entry must occur strictly after the decision cutoff")
    if (
        candidate.setup.earliest_entry_at.astimezone(INDIA_STANDARD_TIME).date()
        <= snapshot.market_session
    ):
        raise LookaheadViolation("entry must occur in a session after the signal market session")
    if calendar is not None:
        entry_session = candidate.setup.earliest_entry_at.astimezone(
            INDIA_STANDARD_TIME
        ).date()
        try:
            expected_entry = calendar.next_session(snapshot.market_session)
            entry_day = calendar.require_session(entry_session)
            if entry_session != expected_entry.day:
                raise DataIntegrityError(
                    "entry must use the next eligible exchange session"
                )
            if candidate.setup.entry_expires_at is not None:
                expiry_session = candidate.setup.entry_expires_at.astimezone(
                    INDIA_STANDARD_TIME
                ).date()
                if expiry_session != entry_session:
                    raise DataIntegrityError(
                        "entry and expiry must use the same exchange session"
                    )
                entry_day.require_same_session_window(
                    candidate.setup.earliest_entry_at,
                    candidate.setup.entry_expires_at,
                )
            elif entry_day.session_window_containing(
                candidate.setup.earliest_entry_at
            ) is None:
                raise DataIntegrityError(
                    "entry time falls outside an executable exchange session window"
                )
            calendar.advance_sessions(
                entry_session,
                max(
                    candidate.setup.max_holding_sessions,
                    candidate.forecast.horizon_sessions,
                ),
            )
        except CalendarIntegrityError as exc:
            raise DataIntegrityError("candidate violates the versioned trading calendar") from exc

    evidence = {item.evidence_id: item for item in snapshot.evidence}
    missing = sorted(set(candidate.evidence_ids) - evidence.keys())
    if missing:
        raise DataIntegrityError(f"candidate references missing evidence: {', '.join(missing)}")
    future = sorted(
        evidence[item_id].evidence_id
        for item_id in candidate.evidence_ids
        if evidence[item_id].available_at > snapshot.decision_time
    )
    if future:
        raise LookaheadViolation(f"candidate uses future evidence: {', '.join(future)}")
