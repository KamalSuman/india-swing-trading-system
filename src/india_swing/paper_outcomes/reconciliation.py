from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

from india_swing.paper_trades import (
    LocalPaperTradeLedger,
    PaperTradeEvent,
    PaperTradeEventType,
)

from .models import PaperOutcomeExitReason, PaperOutcomeIntegrityError, PaperOutcomeReplay, PaperOutcomeStatus


class PaperOutcomeReconciliationError(PaperOutcomeIntegrityError):
    pass


class ReconciliationStatus(str, Enum):
    NO_CHANGE = "NO_CHANGE"
    RECONCILED = "RECONCILED"


@dataclass(frozen=True, slots=True)
class ReconciliationResult:
    registration_id: str
    replay_id: str
    status: ReconciliationStatus
    events: tuple[PaperTradeEvent, ...]
    appended_event_ids: tuple[str, ...]


_EXIT_REASON_CODE = {
    PaperOutcomeExitReason.STOP: "STOP_EXIT",
    PaperOutcomeExitReason.TARGET: "TARGET_EXIT",
    PaperOutcomeExitReason.TIME: "TIME_EXIT",
}


def _lineage(replay: PaperOutcomeReplay) -> dict:
    return {
        "replay_id": replay.replay_id,
        "outcome_policy_id": replay.policy_id,
        "instrument_binding_id": replay.binding_id,
        "calendar_snapshot_id": replay.calendar_snapshot_id,
    }


def _entry_expectation(replay: PaperOutcomeReplay) -> dict:
    entry = replay.entry
    return {
        "event_type": PaperTradeEventType.ENTRY_RECORDED,
        "occurred_at": entry.observed_at,
        "observed_price": entry.price,
        "evidence_id": entry.evidence_id,
        "reason_code": None,
        "market_session": entry.market_session,
        **_lineage(replay),
    }


def _exit_expectation(replay: PaperOutcomeReplay) -> dict:
    exit_fill = replay.exit
    reason_code = _EXIT_REASON_CODE.get(exit_fill.reason)
    if reason_code is None:
        raise PaperOutcomeReconciliationError("closed replay exit reason is invalid")
    return {
        "event_type": PaperTradeEventType.EXIT_RECORDED,
        "occurred_at": exit_fill.observed_at,
        "observed_price": exit_fill.price,
        "evidence_id": exit_fill.evidence_id,
        "reason_code": reason_code,
        "market_session": exit_fill.market_session,
        **_lineage(replay),
    }


def _expected_events(replay: PaperOutcomeReplay) -> tuple[dict, ...]:
    if replay.status is PaperOutcomeStatus.EXPIRED:
        return (
            {
                "event_type": PaperTradeEventType.EXPIRED,
                "occurred_at": replay.as_of,
                "observed_price": None,
                "evidence_id": None,
                "reason_code": "ENTRY_WINDOW_EXPIRED_UNFILLED",
                "market_session": None,
                **_lineage(replay),
            },
        )
    if replay.status is PaperOutcomeStatus.OPEN:
        return (_entry_expectation(replay),)
    if replay.status is PaperOutcomeStatus.CLOSED:
        return (_entry_expectation(replay), _exit_expectation(replay))
    raise PaperOutcomeReconciliationError("replay status cannot be reconciled")


def _matches(have: PaperTradeEvent, want: dict, *, ignore_replay_id: bool = False) -> bool:
    if have.event_type is not want["event_type"]:
        return False
    for key, value in want.items():
        if key == "event_type":
            continue
        if ignore_replay_id and key == "replay_id":
            continue
        if getattr(have, key) != value:
            return False
    return True


def reconcile_paper_outcome(
    *,
    ledger: LocalPaperTradeLedger,
    replay: PaperOutcomeReplay,
) -> ReconciliationResult:
    """Append the ledger events implied by one exact, verified replay.

    A prefix operation: WAITING and BLOCKED never write. An existing exact
    event prefix is idempotent; a mismatching prefix fails closed without
    appending anything.

    The stored `ENTRY_RECORDED` event's `replay_id` is exempt from the prefix
    match: it continues to identify whichever earlier replay first caused
    that create-once event, and is never rewritten or relabeled. Every other
    entry field -- `occurred_at`, `observed_price`, `evidence_id`,
    `reason_code`, `market_session`, `outcome_policy_id`,
    `instrument_binding_id`, and `calendar_snapshot_id` -- must still match
    exactly. This lets an entry persisted from an earlier `OPEN` replay
    evolve into a later `CLOSED` replay for the same registration by
    appending only the exit, which is normal forward lifecycle evolution
    rather than tampering. An existing `EXIT_RECORDED` or `EXPIRED` terminal
    event still requires an exact match on every field, including
    `replay_id`: a later replay can never replace or reinterpret an existing
    terminal outcome.
    """

    if type(ledger) is not LocalPaperTradeLedger or type(replay) is not PaperOutcomeReplay:
        raise PaperOutcomeReconciliationError("reconciliation inputs must be exact")
    try:
        replay.verify_content_identity()
    except Exception:
        raise PaperOutcomeReconciliationError("replay identity verification failed") from None

    if replay.status in (PaperOutcomeStatus.WAITING, PaperOutcomeStatus.BLOCKED):
        try:
            existing = ledger.list_events(replay.registration_id)
        except Exception:
            raise PaperOutcomeReconciliationError("replay registration could not be loaded") from None
        return ReconciliationResult(
            registration_id=replay.registration_id,
            replay_id=replay.replay_id,
            status=ReconciliationStatus.NO_CHANGE,
            events=existing,
            appended_event_ids=(),
        )

    try:
        registration = ledger.get_registration(replay.registration_id)
    except Exception:
        raise PaperOutcomeReconciliationError("replay registration could not be loaded") from None

    expected = _expected_events(replay)
    existing = ledger.list_events(registration.registration_id)
    if len(existing) > len(expected) or any(
        not _matches(
            have,
            want,
            ignore_replay_id=want["event_type"] is PaperTradeEventType.ENTRY_RECORDED,
        )
        for have, want in zip(existing, expected)
    ):
        raise PaperOutcomeReconciliationError("ledger event chain does not match this replay")

    appended: list[PaperTradeEvent] = []
    for want in expected[len(existing):]:
        try:
            event = ledger.append(registration_id=registration.registration_id, **want)
        except Exception:
            raise PaperOutcomeReconciliationError("paper ledger append failed") from None
        appended.append(event)

    events = existing + tuple(appended)
    status = ReconciliationStatus.RECONCILED if appended else ReconciliationStatus.NO_CHANGE
    return ReconciliationResult(
        registration_id=registration.registration_id,
        replay_id=replay.replay_id,
        status=status,
        events=events,
        appended_event_ids=tuple(event.event_id for event in appended),
    )
