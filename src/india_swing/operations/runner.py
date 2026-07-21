from __future__ import annotations

from collections.abc import Callable
from datetime import datetime
from typing import Protocol

from india_swing.identity import content_id
from india_swing.market_data.kite import KiteMarketDataAdapter
from india_swing.market_data.models import FullQuoteBatch
from india_swing.recommendations.models import SwingDecisionAction
from india_swing.recommendations.service import build_swing_decision_package
from india_swing.risk.swing_portfolio import SwingPortfolioSnapshot

from .models import (
    SwingOperationalFailureCode,
    SwingOperationalRunResult,
    SwingOperationalRunSpec,
    SwingOperationalStatus,
    paper_registration_from_decision,
)


class SwingQuoteSource(Protocol):
    @property
    def source_id(self) -> str: ...

    def fetch_full_quotes(self, listing_keys: tuple[str, ...]) -> FullQuoteBatch: ...


class SwingPortfolioSource(Protocol):
    @property
    def source_id(self) -> str: ...

    def read_portfolio(self) -> SwingPortfolioSnapshot: ...


class _QuoteCoverageError(ValueError):
    pass


class KiteSwingQuoteSource:
    """Read-only operational wrapper around the pinned Kite market-data adapter."""

    def __init__(self, adapter: KiteMarketDataAdapter) -> None:
        if type(adapter) is not KiteMarketDataAdapter:
            raise TypeError("adapter must be an exact KiteMarketDataAdapter")
        self.adapter = adapter
        self._source_id = content_id(adapter.identity_material, length=64)

    @property
    def source_id(self) -> str:
        return self._source_id

    def fetch_full_quotes(self, listing_keys: tuple[str, ...]) -> FullQuoteBatch:
        return self.adapter.fetch_full_quotes(listing_keys)


class FixedSwingPortfolioSource:
    """Explicit manual/snapshot portfolio source; never reads a broker or environment."""

    def __init__(self, portfolio: SwingPortfolioSnapshot, *, source_version: str) -> None:
        if type(portfolio) is not SwingPortfolioSnapshot:
            raise TypeError("portfolio must be exact")
        portfolio.verify_content_identity()
        if type(source_version) is not str or not source_version.strip():
            raise ValueError("source_version is required")
        self.portfolio = portfolio
        self._source_id = content_id(
            {
                "kind": "FIXED_PORTFOLIO_SNAPSHOT",
                "source_version": source_version,
                "portfolio_snapshot_id": portfolio.portfolio_snapshot_id,
            },
            length=64,
        )

    @property
    def source_id(self) -> str:
        return self._source_id

    def read_portfolio(self) -> SwingPortfolioSnapshot:
        self.portfolio.verify_content_identity()
        return self.portfolio


def _failure(
    *,
    spec: SwingOperationalRunSpec,
    quote_source_id: str,
    portfolio_source_id: str,
    started_at: datetime,
    completed_at: datetime,
    codes: tuple[SwingOperationalFailureCode, ...],
    evaluated_at: datetime | None = None,
    quote_batch: FullQuoteBatch | None = None,
    portfolio: SwingPortfolioSnapshot | None = None,
) -> SwingOperationalRunResult:
    return SwingOperationalRunResult(
        spec=spec,
        quote_source_id=quote_source_id,
        portfolio_source_id=portfolio_source_id,
        started_at=started_at,
        completed_at=max(started_at, completed_at),
        status=SwingOperationalStatus.FAILED,
        action=SwingDecisionAction.NO_TRADE,
        failure_codes=tuple(sorted(set(codes), key=lambda value: value.value)),
        evaluated_at=evaluated_at,
        quote_batch=quote_batch,
        portfolio=portfolio,
    )


def _read_time(clock: Callable[[], datetime]) -> datetime:
    value = clock()
    if type(value) is not datetime or value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("operational clock must return an aware exact datetime")
    return value


def _completion_time(clock: Callable[[], datetime], floor: datetime) -> tuple[datetime, bool]:
    try:
        value = _read_time(clock)
    except Exception:
        return floor, True
    if value < floor:
        return floor, True
    return value, False


def _collect_quotes(
    *,
    spec: SwingOperationalRunSpec,
    source: SwingQuoteSource,
) -> FullQuoteBatch:
    listing_keys = tuple(
        sorted(f"NSE:{proposal.symbol}" for proposal in spec.proposal_batch.proposals)
    )
    if not listing_keys:
        raise _QuoteCoverageError("operational proposal batch has no quoteable subjects")
    batches: list[FullQuoteBatch] = []
    for offset in range(0, len(listing_keys), spec.quote_chunk_size):
        requested = listing_keys[offset : offset + spec.quote_chunk_size]
        batch = source.fetch_full_quotes(requested)
        try:
            if type(batch) is not FullQuoteBatch:
                raise TypeError("quote source returned the wrong type")
            batch.verify_content_identity()
            if batch.requested_keys != requested:
                raise ValueError("quote source returned different coverage")
        except Exception:
            raise _QuoteCoverageError("quote chunk coverage is invalid") from None
        batches.append(batch)
    provider_versions = {value.provider_version for value in batches}
    if len(provider_versions) != 1:
        raise _QuoteCoverageError("quote chunks use inconsistent provider versions")
    try:
        return FullQuoteBatch(
            requested_keys=listing_keys,
            requested_at=min(value.requested_at for value in batches),
            observed_at=max(value.observed_at for value in batches),
            provider_version=batches[0].provider_version,
            quotes=tuple(quote for batch in batches for quote in batch.quotes),
        )
    except Exception:
        raise _QuoteCoverageError("aggregated quote coverage is invalid") from None


def execute_swing_operational_run(
    *,
    spec: SwingOperationalRunSpec,
    quote_source: SwingQuoteSource,
    portfolio_source: SwingPortfolioSource,
    clock: Callable[[], datetime],
) -> SwingOperationalRunResult:
    """Acquire exact read-only inputs and produce a complete or failed NO_TRADE run."""

    if type(spec) is not SwingOperationalRunSpec:
        raise TypeError("spec must be exact")
    spec.verify_content_identity()
    quote_source_id = quote_source.source_id
    portfolio_source_id = portfolio_source.source_id
    started_at = _read_time(clock)
    if started_at < spec.decision_not_before:
        return _failure(
            spec=spec,
            quote_source_id=quote_source_id,
            portfolio_source_id=portfolio_source_id,
            started_at=started_at,
            completed_at=started_at,
            codes=(SwingOperationalFailureCode.START_BEFORE_WINDOW,),
        )
    if started_at > spec.decision_deadline:
        return _failure(
            spec=spec,
            quote_source_id=quote_source_id,
            portfolio_source_id=portfolio_source_id,
            started_at=started_at,
            completed_at=started_at,
            codes=(SwingOperationalFailureCode.START_AFTER_DEADLINE,),
        )

    try:
        quote_batch = _collect_quotes(spec=spec, source=quote_source)
    except Exception as error:
        completed_at, clock_failed = _completion_time(clock, started_at)
        codes = [
            SwingOperationalFailureCode.QUOTE_COVERAGE_INVALID
            if type(error) is _QuoteCoverageError
            else SwingOperationalFailureCode.QUOTE_ACQUISITION_FAILED
        ]
        if clock_failed:
            codes.append(SwingOperationalFailureCode.CLOCK_NON_MONOTONIC)
        return _failure(
            spec=spec,
            quote_source_id=quote_source_id,
            portfolio_source_id=portfolio_source_id,
            started_at=started_at,
            completed_at=completed_at,
            codes=tuple(codes),
        )

    try:
        portfolio = portfolio_source.read_portfolio()
        if type(portfolio) is not SwingPortfolioSnapshot:
            raise TypeError("portfolio source returned the wrong type")
        portfolio.verify_content_identity()
    except Exception:
        completed_at, clock_failed = _completion_time(clock, started_at)
        codes = [SwingOperationalFailureCode.PORTFOLIO_ACQUISITION_FAILED]
        if clock_failed:
            codes.append(SwingOperationalFailureCode.CLOCK_NON_MONOTONIC)
        return _failure(
            spec=spec,
            quote_source_id=quote_source_id,
            portfolio_source_id=portfolio_source_id,
            started_at=started_at,
            completed_at=completed_at,
            codes=tuple(codes),
            quote_batch=quote_batch,
        )

    try:
        evaluated_at = _read_time(clock)
    except Exception:
        return _failure(
            spec=spec,
            quote_source_id=quote_source_id,
            portfolio_source_id=portfolio_source_id,
            started_at=started_at,
            completed_at=started_at,
            codes=(SwingOperationalFailureCode.CLOCK_NON_MONOTONIC,),
            quote_batch=quote_batch,
            portfolio=portfolio,
        )
    if (
        evaluated_at < started_at
        or evaluated_at < quote_batch.observed_at
        or evaluated_at < portfolio.as_of
    ):
        return _failure(
            spec=spec,
            quote_source_id=quote_source_id,
            portfolio_source_id=portfolio_source_id,
            started_at=started_at,
            completed_at=started_at,
            codes=(SwingOperationalFailureCode.CLOCK_NON_MONOTONIC,),
            evaluated_at=evaluated_at,
            quote_batch=quote_batch,
            portfolio=portfolio,
        )
    if evaluated_at > spec.decision_deadline:
        return _failure(
            spec=spec,
            quote_source_id=quote_source_id,
            portfolio_source_id=portfolio_source_id,
            started_at=started_at,
            completed_at=evaluated_at,
            codes=(SwingOperationalFailureCode.EVALUATION_AFTER_DEADLINE,),
            evaluated_at=evaluated_at,
            quote_batch=quote_batch,
            portfolio=portfolio,
        )

    try:
        package = build_swing_decision_package(
            proposal_batch=spec.proposal_batch,
            quote_batch=quote_batch,
            portfolio=portfolio,
            evaluated_at=evaluated_at,
            quote_policy=spec.quote_policy,
            ranking_policy=spec.ranking_policy,
            sizing_policy=spec.sizing_policy,
        )
        registration = paper_registration_from_decision(
            package=package,
            source_run_id=spec.spec_id,
        )
    except Exception:
        completed_at, clock_failed = _completion_time(clock, evaluated_at)
        codes = [SwingOperationalFailureCode.DECISION_ASSEMBLY_FAILED]
        if clock_failed:
            codes.append(SwingOperationalFailureCode.CLOCK_NON_MONOTONIC)
        return _failure(
            spec=spec,
            quote_source_id=quote_source_id,
            portfolio_source_id=portfolio_source_id,
            started_at=started_at,
            completed_at=completed_at,
            codes=tuple(codes),
            evaluated_at=evaluated_at,
            quote_batch=quote_batch,
            portfolio=portfolio,
        )

    completed_at, clock_failed = _completion_time(clock, evaluated_at)
    if clock_failed:
        return _failure(
            spec=spec,
            quote_source_id=quote_source_id,
            portfolio_source_id=portfolio_source_id,
            started_at=started_at,
            completed_at=completed_at,
            codes=(SwingOperationalFailureCode.CLOCK_NON_MONOTONIC,),
            evaluated_at=evaluated_at,
            quote_batch=quote_batch,
            portfolio=portfolio,
        )
    if completed_at > spec.decision_deadline:
        return _failure(
            spec=spec,
            quote_source_id=quote_source_id,
            portfolio_source_id=portfolio_source_id,
            started_at=started_at,
            completed_at=completed_at,
            codes=(SwingOperationalFailureCode.EVALUATION_AFTER_DEADLINE,),
            evaluated_at=evaluated_at,
            quote_batch=quote_batch,
            portfolio=portfolio,
        )
    return SwingOperationalRunResult(
        spec=spec,
        quote_source_id=quote_source_id,
        portfolio_source_id=portfolio_source_id,
        started_at=started_at,
        completed_at=completed_at,
        status=SwingOperationalStatus.COMPLETE,
        action=package.decision.action,
        failure_codes=(),
        evaluated_at=evaluated_at,
        quote_batch=quote_batch,
        portfolio=portfolio,
        decision_package=package,
        paper_registration=registration,
    )
