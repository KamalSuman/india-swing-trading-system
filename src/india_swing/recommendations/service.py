from __future__ import annotations

from datetime import datetime

from india_swing.market_data.models import FullQuoteBatch
from india_swing.risk.swing_portfolio import (
    SwingPortfolioSizingPolicy,
    SwingPortfolioSnapshot,
    assemble_swing_portfolio_sizing_batch,
)
from india_swing.signals.opportunity_ranking import (
    SwingOpportunityRankingPolicy,
    assemble_swing_opportunity_ranking_batch,
)
from india_swing.signals.proposal_batch import SwingProposalBatch
from india_swing.signals.quote_gate import (
    SwingQuoteGatePolicy,
    assemble_swing_quote_gate_batch,
)

from .models import (
    SwingDecisionError,
    SwingDecisionPackage,
    assemble_swing_daily_decision,
    package_swing_decision,
)


def build_swing_decision_package(
    *,
    proposal_batch: SwingProposalBatch,
    quote_batch: FullQuoteBatch,
    portfolio: SwingPortfolioSnapshot,
    evaluated_at: datetime,
    quote_policy: SwingQuoteGatePolicy | None = None,
    ranking_policy: SwingOpportunityRankingPolicy | None = None,
    sizing_policy: SwingPortfolioSizingPolicy | None = None,
) -> SwingDecisionPackage:
    """Run the complete deterministic quote-to-notification decision path.

    This boundary is pure and capability-free: callers must supply an already
    captured immutable quote batch and an explicit portfolio snapshot. It never
    obtains credentials, reads a latest object, sends a notification, or places
    an order.
    """

    if type(proposal_batch) is not SwingProposalBatch:
        raise SwingDecisionError("proposal_batch must be exact")
    if type(quote_batch) is not FullQuoteBatch:
        raise SwingDecisionError("quote_batch must be exact")
    if type(portfolio) is not SwingPortfolioSnapshot:
        raise SwingDecisionError("portfolio must be exact")
    if type(evaluated_at) is not datetime:
        raise SwingDecisionError("evaluated_at must be exact")
    try:
        quote_gate = assemble_swing_quote_gate_batch(
            proposal_batch=proposal_batch,
            quote_batch=quote_batch,
            evaluated_at=evaluated_at,
            policy=quote_policy,
        )
        ranking = assemble_swing_opportunity_ranking_batch(
            quote_gate_batch=quote_gate,
            policy=ranking_policy,
        )
        sizing = assemble_swing_portfolio_sizing_batch(
            ranking_batch=ranking,
            portfolio=portfolio,
            policy=sizing_policy,
        )
        decision = assemble_swing_daily_decision(sizing_batch=sizing)
        return package_swing_decision(decision)
    except SwingDecisionError:
        raise
    except Exception:
        raise SwingDecisionError("swing decision package assembly failed") from None
