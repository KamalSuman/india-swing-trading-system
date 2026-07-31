"""Pure, proposal-independent quote-quality evaluation shared by every
pre-entry quote gate in this codebase.

Evaluates one exact ``KiteFullQuote`` against one exact decision window and
one exact evaluation instant: decision-window closure, quote/last-trade
timeliness, two-sided depth, spread, and circuit lock. It never inspects or
modifies a price level, a proposal, or a research intent, and it never
selects, sizes, or executes anything -- it only classifies one already-
acquired quote as clean or flagged, returning canonical sorted unique string
reason codes plus the observed spread. A listing-key mismatch or any
malformed parameter is an integrity error (raised), never folded into the
returned reason codes.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal

from india_swing.market_data.models import KiteFullQuote


ZERO = Decimal("0")

REASON_DECISION_WINDOW_NOT_OPEN = "ENTRY_WINDOW_NOT_OPEN"
REASON_DECISION_WINDOW_EXPIRED = "ENTRY_WINDOW_EXPIRED"
REASON_QUOTE_OUTSIDE_WINDOW = "QUOTE_OUTSIDE_ENTRY_WINDOW"
REASON_QUOTE_STALE = "QUOTE_STALE"
REASON_LAST_TRADE_TIME_MISSING = "LAST_TRADE_TIME_MISSING"
REASON_LAST_TRADE_OUTSIDE_WINDOW = "LAST_TRADE_OUTSIDE_ENTRY_WINDOW"
REASON_LAST_TRADE_STALE = "LAST_TRADE_STALE"
REASON_TWO_SIDED_DEPTH_MISSING = "TWO_SIDED_DEPTH_MISSING"
REASON_SPREAD_UNAVAILABLE = "SPREAD_UNAVAILABLE"
REASON_SPREAD_ABOVE_POLICY_MAX = "SPREAD_ABOVE_POLICY_MAX"
REASON_CIRCUIT_LOCKED = "CIRCUIT_LOCKED"


class QuoteQualityError(ValueError):
    pass


def _require_aware_utc(value: object, message: str) -> datetime:
    if type(value) is not datetime:
        raise QuoteQualityError(message)
    try:
        offset = value.utcoffset()
    except Exception:
        raise QuoteQualityError(message) from None
    if value.tzinfo is None or offset is None:
        raise QuoteQualityError(message)
    return value.astimezone(timezone.utc)


@dataclass(frozen=True, slots=True)
class QuoteQualityResult:
    """Immutable outcome of one pure quote-quality evaluation."""

    reason_codes: tuple[str, ...]
    observed_spread_bps: Decimal | None


def evaluate_quote_quality(
    *,
    quote: KiteFullQuote,
    expected_listing_key: str,
    decision_not_before: datetime,
    decision_deadline: datetime,
    evaluated_at: datetime,
    maximum_quote_age_seconds: int,
    maximum_last_trade_age_seconds: int,
    maximum_spread_bps: Decimal,
) -> QuoteQualityResult:
    """Pure, replay-verifiable proposal-independent quote-quality check.

    Never modifies ``quote`` or a price level.
    """

    if type(quote) is not KiteFullQuote:
        raise QuoteQualityError("quote must be an exact KiteFullQuote")
    if type(expected_listing_key) is not str or not expected_listing_key:
        raise QuoteQualityError("expected_listing_key must be non-empty text")
    if quote.listing_key != expected_listing_key:
        raise QuoteQualityError(
            "quote listing key does not match the expected subject"
        )

    not_before = _require_aware_utc(
        decision_not_before, "decision_not_before must be timezone-aware"
    )
    deadline = _require_aware_utc(
        decision_deadline, "decision_deadline must be timezone-aware"
    )
    evaluated = _require_aware_utc(evaluated_at, "evaluated_at must be timezone-aware")
    if not_before >= deadline:
        raise QuoteQualityError(
            "decision_not_before must be strictly before decision_deadline"
        )

    for name, value in (
        ("maximum_quote_age_seconds", maximum_quote_age_seconds),
        ("maximum_last_trade_age_seconds", maximum_last_trade_age_seconds),
    ):
        if type(value) is not int or value <= 0:
            raise QuoteQualityError(f"{name} must be a positive exact integer")
    if (
        type(maximum_spread_bps) is not Decimal
        or not maximum_spread_bps.is_finite()
        or maximum_spread_bps <= ZERO
    ):
        raise QuoteQualityError("maximum_spread_bps must be a positive finite Decimal")

    reasons: set[str] = set()

    if evaluated < not_before:
        reasons.add(REASON_DECISION_WINDOW_NOT_OPEN)
    if evaluated > deadline:
        reasons.add(REASON_DECISION_WINDOW_EXPIRED)

    exchange_timestamp = quote.exchange_timestamp
    if exchange_timestamp < not_before or exchange_timestamp > deadline:
        reasons.add(REASON_QUOTE_OUTSIDE_WINDOW)
    quote_age_seconds = (evaluated - exchange_timestamp).total_seconds()
    if quote_age_seconds < 0 or quote_age_seconds > maximum_quote_age_seconds:
        reasons.add(REASON_QUOTE_STALE)

    if quote.last_trade_time is None:
        reasons.add(REASON_LAST_TRADE_TIME_MISSING)
    else:
        if quote.last_trade_time < not_before or quote.last_trade_time > deadline:
            reasons.add(REASON_LAST_TRADE_OUTSIDE_WINDOW)
        last_trade_age_seconds = (evaluated - quote.last_trade_time).total_seconds()
        if (
            last_trade_age_seconds < 0
            or last_trade_age_seconds > maximum_last_trade_age_seconds
        ):
            reasons.add(REASON_LAST_TRADE_STALE)

    observed_spread_bps = quote.spread_bps
    if not quote.has_two_sided_depth:
        reasons.add(REASON_TWO_SIDED_DEPTH_MISSING)
    if observed_spread_bps is None:
        reasons.add(REASON_SPREAD_UNAVAILABLE)
    elif observed_spread_bps > maximum_spread_bps:
        reasons.add(REASON_SPREAD_ABOVE_POLICY_MAX)

    if quote.at_lower_circuit or quote.at_upper_circuit:
        reasons.add(REASON_CIRCUIT_LOCKED)

    return QuoteQualityResult(
        reason_codes=tuple(sorted(reasons)),
        observed_spread_bps=observed_spread_bps,
    )
