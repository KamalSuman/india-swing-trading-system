from __future__ import annotations

from dataclasses import dataclass

from india_swing.domain.models import (
    Board,
    DataSnapshot,
    InstrumentSnapshot,
    RiskPolicy,
    Surveillance,
)


@dataclass(frozen=True, slots=True)
class EligibilityResult:
    actionable: bool
    watch_only: bool
    reasons: tuple[str, ...]


def evaluate_eligibility(
    instrument: InstrumentSnapshot,
    policy: RiskPolicy,
    snapshot: DataSnapshot,
) -> EligibilityResult:
    reasons: list[str] = []
    watch_only = instrument.board is Board.SME

    if instrument.board is Board.UNKNOWN:
        reasons.append("instrument board is unknown")
    if not instrument.active:
        reasons.append("instrument is not active")
    if instrument.suspended:
        reasons.append("instrument is suspended")
    if watch_only:
        reasons.append("SME instruments are watch-only in the pilot")
    if instrument.surveillance in policy.banned_surveillance:
        reasons.append(f"surveillance category {instrument.surveillance.value} is blocked")
    if instrument.surveillance is Surveillance.UNKNOWN:
        reasons.append("surveillance status is unknown")
    if instrument.lower_circuit_locked:
        reasons.append("instrument is locked at the lower circuit")
    if instrument.history_sessions < policy.min_history_sessions:
        reasons.append("insufficient price history")
    if instrument.quoted_spread_bps > policy.max_spread_bps:
        reasons.append("quoted spread exceeds the pilot limit")
    if instrument.median_daily_traded_value <= 0:
        reasons.append("median traded value is unavailable")
    if instrument.price_session != snapshot.market_session:
        reasons.append("price data is not from the required market session")
    if instrument.data_available_at < snapshot.session_finalized_at:
        reasons.append("EOD price data predates session finalization")
    if instrument.data_available_at > snapshot.decision_time:
        reasons.append("market data was unavailable at the decision cutoff")
    if instrument.universe_snapshot_id != snapshot.universe_snapshot_id:
        reasons.append("instrument belongs to a different universe snapshot")

    return EligibilityResult(actionable=not reasons, watch_only=watch_only, reasons=tuple(reasons))
