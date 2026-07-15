from __future__ import annotations

from india_swing.domain.models import Candidate, DataSnapshot


class DataIntegrityError(ValueError):
    """Raised when required evidence is absent or internally inconsistent."""


class LookaheadViolation(DataIntegrityError):
    """Raised when a decision attempts to use information unavailable at its cutoff."""


def validate_snapshot(snapshot: DataSnapshot) -> None:
    future = [
        item.evidence_id
        for item in snapshot.evidence
        if item.available_at > snapshot.decision_time
    ]
    if future:
        joined = ", ".join(sorted(future))
        raise LookaheadViolation(f"snapshot contains future evidence: {joined}")


def validate_candidate(candidate: Candidate, snapshot: DataSnapshot) -> None:
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
    if candidate.setup.decision_time != snapshot.decision_time:
        raise DataIntegrityError("setup decision_time must equal snapshot decision_time")
    if candidate.setup.earliest_entry_at <= snapshot.decision_time:
        raise LookaheadViolation("entry must occur strictly after the decision cutoff")
    if candidate.setup.earliest_entry_at.date() <= snapshot.market_session:
        raise LookaheadViolation("entry must occur in a session after the signal market session")

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
