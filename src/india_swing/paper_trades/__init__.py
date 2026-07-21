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
from .store import (
    LocalPaperTradeLedger,
    decode_paper_trade_registration,
    decode_paper_trade_event,
    encode_paper_trade_event,
    encode_paper_trade_registration,
    validate_paper_trade_history,
)

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
    "decode_paper_trade_registration",
    "decode_paper_trade_event",
    "encode_paper_trade_event",
    "encode_paper_trade_registration",
    "registration_from_shadow_alert",
    "validate_paper_trade_history",
)
