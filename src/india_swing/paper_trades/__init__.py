"""Append-only, non-executable paper-trade outcome records."""

from .models import (
    PaperTradeConflict,
    PaperTradeError,
    PaperTradeEvent,
    PaperTradeEventType,
    PaperTradeIntegrityError,
    PaperTradeRegistration,
    PaperTradeStatus,
    PaperTradeSummary,
    registration_from_shadow_alert,
)
from .store import LocalPaperTradeLedger, validate_paper_trade_history

__all__ = (
    "LocalPaperTradeLedger",
    "PaperTradeConflict",
    "PaperTradeError",
    "PaperTradeEvent",
    "PaperTradeEventType",
    "PaperTradeIntegrityError",
    "PaperTradeRegistration",
    "PaperTradeStatus",
    "PaperTradeSummary",
    "registration_from_shadow_alert",
    "validate_paper_trade_history",
)
