"""Forward-paper research bridge: raw current-cross-section history windows.

Collection-only. Not model, backtest, signal, alert, paper-trade, or
execution input.
"""

from .history import (
    FORWARD_PAPER_HISTORY_CANDIDATE_SCHEMA_VERSION,
    FORWARD_PAPER_HISTORY_POLICY_VERSION,
    FORWARD_PAPER_HISTORY_VETO_SCHEMA_VERSION,
    FORWARD_PAPER_HISTORY_WINDOW_SESSION_COUNT,
    FORWARD_PAPER_HISTORY_WINDOW_SPEC_SCHEMA_VERSION,
    FORWARD_PAPER_RAW_HISTORY_WINDOW_SCHEMA_VERSION,
    ForwardPaperHistoryCandidate,
    ForwardPaperHistoryError,
    ForwardPaperHistoryOutcome,
    ForwardPaperHistoryVeto,
    ForwardPaperHistoryVetoReason,
    ForwardPaperHistoryWindowSpec,
    ForwardPaperRawHistoryWindow,
    build_forward_paper_raw_history_window,
)

__all__ = [
    "FORWARD_PAPER_HISTORY_CANDIDATE_SCHEMA_VERSION",
    "FORWARD_PAPER_HISTORY_POLICY_VERSION",
    "FORWARD_PAPER_HISTORY_VETO_SCHEMA_VERSION",
    "FORWARD_PAPER_HISTORY_WINDOW_SESSION_COUNT",
    "FORWARD_PAPER_HISTORY_WINDOW_SPEC_SCHEMA_VERSION",
    "FORWARD_PAPER_RAW_HISTORY_WINDOW_SCHEMA_VERSION",
    "ForwardPaperHistoryCandidate",
    "ForwardPaperHistoryError",
    "ForwardPaperHistoryOutcome",
    "ForwardPaperHistoryVeto",
    "ForwardPaperHistoryVetoReason",
    "ForwardPaperHistoryWindowSpec",
    "ForwardPaperRawHistoryWindow",
    "build_forward_paper_raw_history_window",
]
