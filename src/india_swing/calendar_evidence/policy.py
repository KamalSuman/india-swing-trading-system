from __future__ import annotations

from datetime import date, datetime, time

from india_swing.domain.models import INDIA_STANDARD_TIME


# This is a conservative collection guard, not a claim about the exchange's
# actual close or dissemination time.  It prevents a manually supplied final
# Bhavcopy from becoming evidence before the stated India trade date is
# substantially complete.  A verified calendar/data-ready policy will replace
# this fixed boundary in the actionable pipeline.
CONSERVATIVE_FINAL_REPORT_NOT_BEFORE_IST = time(20, 0)


def final_report_not_before(trade_date: date) -> datetime:
    if type(trade_date) is not date:
        raise TypeError("trade_date must be a date")
    return datetime.combine(
        trade_date,
        CONSERVATIVE_FINAL_REPORT_NOT_BEFORE_IST,
        tzinfo=INDIA_STANDARD_TIME,
    )
