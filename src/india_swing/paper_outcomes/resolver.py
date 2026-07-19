from __future__ import annotations

from datetime import datetime, timezone, timedelta
from decimal import Decimal

from india_swing.execution import (
    LimitEntryOrder,
    ProtectiveExitOrder,
    SimulationBar,
    simulate_limit_entry,
    simulate_protective_exit,
    simulate_time_exit,
)
from india_swing.execution.simulator import ExitReason
from india_swing.paper_trades import PaperTradeRegistration
from india_swing.reference.calendar import CalendarSnapshot

from .models import (
    DEFAULT_PAPER_OUTCOME_BLOCKERS,
    PaperInstrumentBinding,
    PaperOutcomeError,
    PaperOutcomeExitReason,
    PaperOutcomeFill,
    PaperOutcomeIntegrityError,
    PaperOutcomeObservation,
    PaperOutcomePolicy,
    PaperOutcomeReplay,
    PaperOutcomeStatus,
)


IST = timezone(timedelta(hours=5, minutes=30))


def _simulation_bar(value: PaperOutcomeObservation) -> SimulationBar:
    if not value.traded:
        raise PaperOutcomeError("missing observation cannot become a simulation bar")
    return SimulationBar(
        session=value.market_session,
        symbol=value.symbol,
        open=value.open,
        high=value.high,
        low=value.low,
        close=value.close,
        volume=value.volume,
    )


def _fill(value, observation: PaperOutcomeObservation) -> PaperOutcomeFill:
    reason = None
    if value.exit_reason is not None:
        reason = PaperOutcomeExitReason(value.exit_reason.value)
    return PaperOutcomeFill(
        market_session=value.session,
        observed_at=observation.knowledge_time,
        price=value.fill_price,
        evidence_id=observation.observation_id,
        reason=reason,
    )


def replay_paper_outcome(
    *,
    registration: PaperTradeRegistration,
    binding: PaperInstrumentBinding,
    calendar: CalendarSnapshot,
    observations: tuple[PaperOutcomeObservation, ...],
    as_of,
    policy: PaperOutcomePolicy | None = None,
) -> PaperOutcomeReplay:
    if (
        type(registration) is not PaperTradeRegistration
        or type(binding) is not PaperInstrumentBinding
        or type(calendar) is not CalendarSnapshot
    ):
        raise PaperOutcomeError("paper outcome identity inputs must be exact")
    if policy is None:
        policy = PaperOutcomePolicy()
    if type(policy) is not PaperOutcomePolicy:
        raise PaperOutcomeError("paper outcome policy must be exact")
    try:
        registration.verify_content_identity()
        binding.verify_content_identity()
        policy.verify_content_identity()
        calendar.verify_content_identity()
    except Exception:
        raise PaperOutcomeIntegrityError("paper outcome identity verification failed") from None
    if binding.registration_id != registration.registration_id or binding.symbol != registration.symbol:
        raise PaperOutcomeError("paper outcome binding differs from registration")
    if binding.tick_knowledge_time > registration.decision_time:
        raise PaperOutcomeError("tick evidence was unknown at decision time")
    if any(
        price % binding.tick_size != 0
        for price in (registration.entry_low, registration.entry_high, registration.stop, registration.target)
    ):
        raise PaperOutcomeError("paper outcome levels differ from bound tick size")
    if type(observations) is not tuple or any(type(value) is not PaperOutcomeObservation for value in observations):
        raise PaperOutcomeError("paper observations must be an exact tuple")
    if observations != tuple(sorted(observations, key=lambda value: value.market_session)):
        raise PaperOutcomeError("paper observations must be session ordered")
    if len({value.market_session for value in observations}) != len(observations):
        raise PaperOutcomeError("paper observations contain duplicate sessions")
    calendar_ids = {value.calendar_snapshot_id for value in observations}
    if calendar_ids and calendar_ids != {calendar.snapshot_id}:
        raise PaperOutcomeError("paper observations use different calendars")
    for value in observations:
        value.verify_content_identity()
        if (value.symbol, value.series, value.validated_isin) != (
            binding.symbol,
            binding.series,
            binding.validated_isin,
        ):
            raise PaperOutcomeError("paper observation belongs to another listing")
    if type(as_of) is not datetime or as_of.tzinfo is None:
        raise PaperOutcomeError("paper outcome as_of must be an aware datetime")
    as_of = as_of.astimezone(timezone.utc)
    if as_of < registration.decision_time:
        raise PaperOutcomeError("paper outcome as_of predates the decision")
    if calendar.cutoff > as_of:
        raise PaperOutcomeError("calendar snapshot was unavailable at replay as_of")
    available = tuple(value for value in observations if value.knowledge_time <= as_of)
    source_ids = tuple(value.observation_id for value in available)

    def result(status, reason, entry=None, exit_fill=None):
        return PaperOutcomeReplay(
            registration_id=registration.registration_id,
            binding_id=binding.binding_id,
            policy_id=policy.policy_id,
            calendar_snapshot_id=calendar.snapshot_id,
            as_of=as_of,
            status=status,
            entry=entry,
            exit=exit_fill,
            reason_code=reason,
            source_observation_ids=source_ids,
            blockers=DEFAULT_PAPER_OUTCOME_BLOCKERS,
        )

    first_session = registration.earliest_entry_at.astimezone(IST).date()
    expiry_session = registration.entry_expires_at.astimezone(IST).date()
    calendar_sessions = tuple(
        value.day
        for value in calendar.days
        if value.is_session and value.day >= first_session
    )
    entry_sessions = tuple(
        value for value in calendar_sessions if value <= expiry_session
    )
    if not entry_sessions:
        raise PaperOutcomeError("entry window contains no calendar session")
    available_sessions = tuple(value.market_session for value in available)
    relevant_available = tuple(
        value for value in available_sessions if value >= first_session
    )
    if relevant_available:
        expected_prefix = calendar_sessions[: len(relevant_available)]
        if relevant_available != expected_prefix:
            raise PaperOutcomeError("paper observations contain a calendar-session gap")
    eligible = tuple(
        value
        for value in available
        if first_session <= value.market_session <= expiry_session
    )
    order = LimitEntryOrder(
        symbol=registration.symbol,
        signal_session=registration.decision_time.astimezone(IST).date(),
        first_eligible_session=first_session,
        expiry_session=expiry_session,
        quantity=registration.quantity,
        limit_price=registration.entry_high,
        tick_size=binding.tick_size,
        maximum_participation=policy.maximum_participation,
    )
    entry_fill = None
    entry_observation = None
    for observation in eligible:
        if not observation.traded or observation.open < registration.entry_low:
            continue
        candidate = simulate_limit_entry(
            order,
            _simulation_bar(observation),
            slippage_bps=policy.slippage_bps,
        )
        if candidate is not None and candidate.fill_price >= registration.entry_low:
            entry_fill = candidate
            entry_observation = observation
            break
    if entry_fill is None:
        if not set(entry_sessions).issubset(available_sessions):
            return result(PaperOutcomeStatus.WAITING, "WAITING_FOR_ENTRY_EVIDENCE")
        return result(PaperOutcomeStatus.EXPIRED, "ENTRY_WINDOW_EXPIRED_UNFILLED")

    assert entry_observation is not None
    entry = _fill(entry_fill, entry_observation)
    from_entry = tuple(value for value in available if value.market_session >= entry_fill.session)
    entry_calendar_index = calendar_sessions.index(entry_fill.session)
    horizon_sessions = calendar_sessions[
        entry_calendar_index : entry_calendar_index + registration.max_holding_sessions
    ]
    if len(horizon_sessions) < registration.max_holding_sessions:
        return result(PaperOutcomeStatus.BLOCKED, "CALENDAR_COVERAGE_INSUFFICIENT", entry)
    by_session = {value.market_session: value for value in from_entry}
    horizon = tuple(
        by_session[value] for value in horizon_sessions if value in by_session
    )
    exit_order = ProtectiveExitOrder(
        symbol=registration.symbol,
        quantity=registration.quantity,
        entry_session=entry_fill.session,
        entry_price=entry_fill.fill_price,
        stop_price=registration.stop,
        target_price=registration.target,
        tick_size=binding.tick_size,
        maximum_participation=policy.maximum_participation,
    )
    for observation in horizon:
        if not observation.traded:
            continue
        exit_fill = simulate_protective_exit(
            exit_order,
            _simulation_bar(observation),
            slippage_bps=policy.slippage_bps,
        )
        if exit_fill is not None:
            reason = "STOP_EXIT" if exit_fill.exit_reason is ExitReason.STOP else "TARGET_EXIT"
            return result(
                PaperOutcomeStatus.CLOSED,
                reason,
                entry,
                _fill(exit_fill, observation),
            )
    if len(horizon) < len(horizon_sessions):
        return result(PaperOutcomeStatus.OPEN, "HOLDING_HORIZON_NOT_MATURE", entry)
    last = horizon[-1]
    if not last.traded:
        return result(PaperOutcomeStatus.BLOCKED, "HORIZON_EXIT_BAR_MISSING", entry)
    time_fill = simulate_time_exit(
        exit_order,
        _simulation_bar(last),
        slippage_bps=policy.slippage_bps,
    )
    if time_fill is None:
        return result(PaperOutcomeStatus.BLOCKED, "HORIZON_EXIT_NOT_FILLABLE", entry)
    return result(
        PaperOutcomeStatus.CLOSED,
        "TIME_EXIT",
        entry,
        _fill(time_fill, last),
    )
