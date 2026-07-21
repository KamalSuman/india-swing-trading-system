from __future__ import annotations

from collections.abc import Callable
from datetime import datetime

from india_swing.market_data.models import MAXIMUM_QUOTE_KEYS
from india_swing.paper_trades.store import LocalPaperTradeLedger
from india_swing.recommendations.store import LocalSwingDecisionOutbox
from india_swing.risk.swing_portfolio import SwingPortfolioSizingPolicy
from india_swing.signals.opportunity_ranking import SwingOpportunityRankingPolicy
from india_swing.signals.proposal_artifacts import (
    LocalSwingProposalBatchStore,
    SwingProposalArtifactError,
    SwingProposalBatchInputResolver,
)
from india_swing.signals.quote_gate import SwingQuoteGatePolicy

from .models import (
    SwingOperationalError,
    SwingOperationalRunRecord,
    SwingOperationalRunResult,
    SwingOperationalRunSpec,
    build_swing_operational_run_spec,
)
from .runner import SwingPortfolioSource, SwingQuoteSource
from .store import LocalSwingOperationalRunStore, run_and_publish_swing_operation


def build_stored_swing_operational_run_spec(
    *,
    proposal_batch_id: str,
    proposal_store: LocalSwingProposalBatchStore,
    proposal_resolver: SwingProposalBatchInputResolver,
    quote_policy: SwingQuoteGatePolicy | None = None,
    ranking_policy: SwingOpportunityRankingPolicy | None = None,
    sizing_policy: SwingPortfolioSizingPolicy | None = None,
    quote_chunk_size: int = MAXIMUM_QUOTE_KEYS,
) -> SwingOperationalRunSpec:
    """Load one explicitly identified proposal artifact and build its run spec."""

    if type(proposal_store) is not LocalSwingProposalBatchStore:
        raise SwingOperationalError("proposal_store must be exact")
    try:
        proposal_batch = proposal_store.load(proposal_batch_id, proposal_resolver)
    except SwingProposalArtifactError:
        raise SwingOperationalError("proposal artifact could not be loaded safely") from None
    return build_swing_operational_run_spec(
        proposal_batch=proposal_batch,
        quote_policy=quote_policy,
        ranking_policy=ranking_policy,
        sizing_policy=sizing_policy,
        quote_chunk_size=quote_chunk_size,
    )


def run_and_publish_stored_swing_operation(
    *,
    proposal_batch_id: str,
    proposal_store: LocalSwingProposalBatchStore,
    proposal_resolver: SwingProposalBatchInputResolver,
    quote_source: SwingQuoteSource,
    portfolio_source: SwingPortfolioSource,
    clock: Callable[[], datetime],
    run_store: LocalSwingOperationalRunStore,
    decision_outbox: LocalSwingDecisionOutbox,
    paper_ledger: LocalPaperTradeLedger | None = None,
    quote_policy: SwingQuoteGatePolicy | None = None,
    ranking_policy: SwingOpportunityRankingPolicy | None = None,
    sizing_policy: SwingPortfolioSizingPolicy | None = None,
    quote_chunk_size: int = MAXIMUM_QUOTE_KEYS,
) -> tuple[SwingOperationalRunResult, SwingOperationalRunRecord]:
    """Cloud-job boundary: exact-ID load, acquire, decide, publish, and seal."""

    spec = build_stored_swing_operational_run_spec(
        proposal_batch_id=proposal_batch_id,
        proposal_store=proposal_store,
        proposal_resolver=proposal_resolver,
        quote_policy=quote_policy,
        ranking_policy=ranking_policy,
        sizing_policy=sizing_policy,
        quote_chunk_size=quote_chunk_size,
    )
    return run_and_publish_swing_operation(
        spec=spec,
        quote_source=quote_source,
        portfolio_source=portfolio_source,
        clock=clock,
        run_store=run_store,
        decision_outbox=decision_outbox,
        paper_ledger=paper_ledger,
    )
