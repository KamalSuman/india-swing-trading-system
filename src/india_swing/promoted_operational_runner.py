"""Restart-neutral orchestration core for the promoted paper-operation seam.

Binds one exact quote-gate specification, allocation policy, expected quote/
portfolio source identity, and a Kite-safe quote-chunk ceiling, acquires only
exact read-only inputs through two injected ports plus an injected clock, and
runs the existing pure promoted quote-gate -> allocation -> decision-package
chain exactly once in order. Produces one content-addressed ``COMPLETE`` or
sanitized ``FAILED`` result and nothing else: no persistence, notification,
broker order, paper-ledger registration, environment/credential read, GCP, or
network-client construction exists anywhere in this module. Never reranks a
candidate, mutates a price, widens a policy, resizes a quantity, or treats a
veto as a runner failure.
"""

from __future__ import annotations

import re
from collections.abc import Callable
from dataclasses import dataclass, field, fields
from datetime import datetime, timezone
from enum import Enum
from typing import Protocol

from india_swing.identity import content_id
from india_swing.market_data.models import FullQuoteBatch
from india_swing.promoted_operational_allocation import (
    PromotedOperationalAllocationPolicy,
    PromotedOperationalPortfolioContext,
    VerifiedPromotedOperationalAllocationBatch,
    assemble_promoted_operational_allocation_batch,
)
from india_swing.promoted_operational_decision import (
    PromotedOperationalDecisionPackage,
    assemble_promoted_operational_decision_package,
)
from india_swing.promoted_operational_quote_gate import (
    PromotedOperationalQuoteGateSpec,
    VerifiedPromotedOperationalQuoteGateBatch,
    evaluate_promoted_operational_quote_gate,
)


_SHA256 = re.compile(r"[0-9a-f]{64}\Z")

PROMOTED_OPERATIONAL_RUN_SPEC_SCHEMA_VERSION = "promoted-operational-run-spec/v1"
PROMOTED_OPERATIONAL_RUN_RESULT_SCHEMA_VERSION = "promoted-operational-run-result/v1"

_MINIMUM_QUOTE_CHUNK_SIZE = 1
_MAXIMUM_QUOTE_CHUNK_SIZE = 500


class PromotedOperationalRunnerError(ValueError):
    pass


_ERR_TYPE = "promoted operational runner type is invalid"
_ERR_SPEC = "promoted operational run spec is invalid"
_ERR_AUTHORITY = "promoted operational runner authority flags are invalid"
_ERR_RESULT = "promoted operational run result is invalid"


class PromotedOperationalRunStatus(str, Enum):
    COMPLETE = "COMPLETE"
    FAILED = "FAILED"


class PromotedOperationalRunFailureCode(str, Enum):
    START_BEFORE_WINDOW = "START_BEFORE_WINDOW"
    START_AFTER_DEADLINE = "START_AFTER_DEADLINE"
    SOURCE_IDENTITY_INVALID = "SOURCE_IDENTITY_INVALID"
    QUOTE_ACQUISITION_FAILED = "QUOTE_ACQUISITION_FAILED"
    QUOTE_COVERAGE_INVALID = "QUOTE_COVERAGE_INVALID"
    PORTFOLIO_ACQUISITION_FAILED = "PORTFOLIO_ACQUISITION_FAILED"
    CLOCK_NON_MONOTONIC = "CLOCK_NON_MONOTONIC"
    EVALUATION_AFTER_DEADLINE = "EVALUATION_AFTER_DEADLINE"
    QUOTE_GATE_FAILED = "QUOTE_GATE_FAILED"
    ALLOCATION_FAILED = "ALLOCATION_FAILED"
    DECISION_ASSEMBLY_FAILED = "DECISION_ASSEMBLY_FAILED"
    COMPLETION_AFTER_DEADLINE = "COMPLETION_AFTER_DEADLINE"


_FAILURE_CODE_VALUES = frozenset(value.value for value in PromotedOperationalRunFailureCode)


class PromotedOperationalQuoteSource(Protocol):
    """Read-only port: exposes only a pinned identity and exact-coverage
    full-quote reads. Never a broker order, write, or credential surface."""

    @property
    def source_id(self) -> str: ...

    def fetch_full_quotes(self, exact_listing_keys: tuple[str, ...]) -> FullQuoteBatch: ...


class PromotedOperationalPortfolioSource(Protocol):
    """Read-only port: exposes only a pinned identity and one exact
    ``PromotedOperationalPortfolioContext`` read. Never infers open listings
    or synthesizes portfolio evidence -- that binding is the source's own
    responsibility."""

    @property
    def source_id(self) -> str: ...

    def read_portfolio_context(self) -> PromotedOperationalPortfolioContext: ...


def _require_aware_utc(value: object) -> datetime:
    if type(value) is not datetime:
        raise PromotedOperationalRunnerError(_ERR_RESULT)
    try:
        offset = value.utcoffset()
    except Exception:
        raise PromotedOperationalRunnerError(_ERR_RESULT) from None
    if value.tzinfo is None or offset is None:
        raise PromotedOperationalRunnerError(_ERR_RESULT)
    return value.astimezone(timezone.utc)


@dataclass(frozen=True, slots=True)
class PromotedOperationalRunSpec:
    """One exact, content-addressed binding of a quote-gate spec, an
    allocation policy, the two pinned source identities, and a Kite-safe
    quote-chunk ceiling. Grants no authority: ``paper_only`` is always true
    and both ``notification_eligible``/``execution_eligible`` are always
    false."""

    quote_gate_spec: PromotedOperationalQuoteGateSpec
    allocation_policy: PromotedOperationalAllocationPolicy
    expected_quote_source_id: str
    expected_portfolio_source_id: str
    maximum_quote_chunk_size: int
    paper_only: bool = True
    notification_eligible: bool = False
    execution_eligible: bool = False
    schema_version: str = PROMOTED_OPERATIONAL_RUN_SPEC_SCHEMA_VERSION
    spec_id: str = field(init=False)

    def __post_init__(self) -> None:
        if type(self.quote_gate_spec) is not PromotedOperationalQuoteGateSpec:
            raise PromotedOperationalRunnerError(_ERR_TYPE)
        self.quote_gate_spec.verify_content_identity()
        if type(self.allocation_policy) is not PromotedOperationalAllocationPolicy:
            raise PromotedOperationalRunnerError(_ERR_TYPE)
        self.allocation_policy.verify_content_identity()
        for value in (self.expected_quote_source_id, self.expected_portfolio_source_id):
            if type(value) is not str or _SHA256.fullmatch(value) is None:
                raise PromotedOperationalRunnerError(_ERR_SPEC)
        if (
            type(self.maximum_quote_chunk_size) is not int
            or not (
                _MINIMUM_QUOTE_CHUNK_SIZE
                <= self.maximum_quote_chunk_size
                <= _MAXIMUM_QUOTE_CHUNK_SIZE
            )
        ):
            raise PromotedOperationalRunnerError(_ERR_SPEC)
        if (
            self.paper_only is not True
            or self.notification_eligible is not False
            or self.execution_eligible is not False
        ):
            raise PromotedOperationalRunnerError(_ERR_AUTHORITY)
        if self.schema_version != PROMOTED_OPERATIONAL_RUN_SPEC_SCHEMA_VERSION:
            raise PromotedOperationalRunnerError(_ERR_SPEC)
        object.__setattr__(self, "spec_id", self._calculated_id())

    def _identity(self) -> dict[str, object]:
        return {
            value.name: getattr(self, value.name)
            for value in fields(self)
            if value.name != "spec_id"
        }

    def _calculated_id(self) -> str:
        return content_id(self._identity(), length=64)

    def verify_content_identity(self) -> None:
        if type(self) is not PromotedOperationalRunSpec:
            raise PromotedOperationalRunnerError(_ERR_TYPE)
        self.quote_gate_spec.verify_content_identity()
        self.allocation_policy.verify_content_identity()
        try:
            fresh = PromotedOperationalRunSpec(**self._identity())
        except PromotedOperationalRunnerError:
            raise
        except Exception:
            raise PromotedOperationalRunnerError(_ERR_TYPE) from None
        if self.spec_id != fresh.spec_id:
            raise PromotedOperationalRunnerError(_ERR_TYPE)


@dataclass(frozen=True, slots=True)
class PromotedOperationalRunResult:
    """Exact, content-addressed terminal result for one promoted operational
    run. ``COMPLETE`` requires no failures and the complete chain through
    one decision package. ``FAILED`` requires at least one sanitized failure
    code, no decision package, and retains only the exact verified prefix of
    artifacts produced before termination -- never exception text,
    credentials, URLs, payload reprs, or source-provided messages."""

    spec: PromotedOperationalRunSpec
    quote_source_id: str | None
    portfolio_source_id: str | None
    started_at: datetime
    evaluated_at: datetime | None
    completed_at: datetime
    status: PromotedOperationalRunStatus
    failure_codes: tuple[str, ...]
    quote_batch: FullQuoteBatch | None
    portfolio_context: PromotedOperationalPortfolioContext | None
    quote_gate_batch: VerifiedPromotedOperationalQuoteGateBatch | None
    allocation_batch: VerifiedPromotedOperationalAllocationBatch | None
    decision_package: PromotedOperationalDecisionPackage | None
    paper_only: bool
    notification_eligible: bool
    execution_eligible: bool
    schema_version: str = PROMOTED_OPERATIONAL_RUN_RESULT_SCHEMA_VERSION
    result_id: str = field(init=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "started_at", _require_aware_utc(self.started_at))
        object.__setattr__(self, "completed_at", _require_aware_utc(self.completed_at))
        if self.evaluated_at is not None:
            object.__setattr__(self, "evaluated_at", _require_aware_utc(self.evaluated_at))
        if self.schema_version != PROMOTED_OPERATIONAL_RUN_RESULT_SCHEMA_VERSION:
            raise PromotedOperationalRunnerError(_ERR_RESULT)
        self._verify()
        object.__setattr__(self, "result_id", self._calculated_id())

    def _verify(self) -> None:
        if type(self.spec) is not PromotedOperationalRunSpec:
            raise PromotedOperationalRunnerError(_ERR_TYPE)
        self.spec.verify_content_identity()
        if self.schema_version != PROMOTED_OPERATIONAL_RUN_RESULT_SCHEMA_VERSION:
            raise PromotedOperationalRunnerError(_ERR_RESULT)

        for value in (self.quote_source_id, self.portfolio_source_id):
            if value is not None and (type(value) is not str or _SHA256.fullmatch(value) is None):
                raise PromotedOperationalRunnerError(_ERR_RESULT)

        if type(self.status) is not PromotedOperationalRunStatus:
            raise PromotedOperationalRunnerError(_ERR_TYPE)
        if type(self.failure_codes) is not tuple or any(
            type(value) is not str or value not in _FAILURE_CODE_VALUES
            for value in self.failure_codes
        ):
            raise PromotedOperationalRunnerError(_ERR_RESULT)
        if self.failure_codes != tuple(sorted(set(self.failure_codes))):
            raise PromotedOperationalRunnerError(_ERR_RESULT)
        if (
            self.paper_only is not True
            or self.notification_eligible is not False
            or self.execution_eligible is not False
        ):
            raise PromotedOperationalRunnerError(_ERR_AUTHORITY)

        started_at = _require_aware_utc(self.started_at)
        completed_at = _require_aware_utc(self.completed_at)
        if completed_at < started_at:
            raise PromotedOperationalRunnerError(_ERR_RESULT)
        clock_violation = (
            PromotedOperationalRunFailureCode.CLOCK_NON_MONOTONIC.value in self.failure_codes
        )
        evaluated_at = None
        if self.evaluated_at is not None:
            evaluated_at = _require_aware_utc(self.evaluated_at)
            if completed_at < evaluated_at:
                raise PromotedOperationalRunnerError(_ERR_RESULT)
            # A CLOCK_NON_MONOTONIC failure exists precisely to retain, for
            # audit, an evaluated_at that violated ordering against
            # started_at or an acquired artifact's own timestamp -- so that
            # specific ordering is not re-enforced here when the code is
            # present; every other invariant in this method still is.
            if not clock_violation and evaluated_at < started_at:
                raise PromotedOperationalRunnerError(_ERR_RESULT)

        # Source-identity pinning truth (both IDs must equal the spec's pins
        # on every result except one whose codes include
        # START_BEFORE_WINDOW/START_AFTER_DEADLINE/SOURCE_IDENTITY_INVALID)
        # is enforced precisely, per failure code, in the stage-prefix truth
        # table below -- not here, so it is checked exactly once.

        candidates_present = bool(self.spec.quote_gate_spec.preparation.candidates)

        # Artifact prefixes are contiguous, not merely ordered by their
        # deepest retained layer. A nonempty preparation always acquires its
        # quote batch before portfolio context; a zero-candidate preparation
        # never has a quote batch at any stage.
        if self.portfolio_context is not None and (
            (candidates_present and self.quote_batch is None)
            or (not candidates_present and self.quote_batch is not None)
        ):
            raise PromotedOperationalRunnerError(_ERR_RESULT)

        if self.quote_batch is not None:
            if type(self.quote_batch) is not FullQuoteBatch:
                raise PromotedOperationalRunnerError(_ERR_TYPE)
            self.quote_batch.verify_content_identity()
            expected_keys = tuple(
                sorted(self.spec.quote_gate_spec.preparation.manifest.listing_keys)
            )
            if self.quote_batch.requested_keys != expected_keys:
                raise PromotedOperationalRunnerError(_ERR_RESULT)
            if (
                not clock_violation
                and evaluated_at is not None
                and evaluated_at < self.quote_batch.observed_at
            ):
                raise PromotedOperationalRunnerError(_ERR_RESULT)

        if self.portfolio_context is not None:
            if type(self.portfolio_context) is not PromotedOperationalPortfolioContext:
                raise PromotedOperationalRunnerError(_ERR_TYPE)
            self.portfolio_context.verify_content_identity()
            if (
                self.portfolio_context.source_portfolio_artifact_id
                != self.spec.expected_portfolio_source_id
            ):
                raise PromotedOperationalRunnerError(_ERR_RESULT)
            if (
                not clock_violation
                and evaluated_at is not None
                and evaluated_at < self.portfolio_context.portfolio.as_of
            ):
                raise PromotedOperationalRunnerError(_ERR_RESULT)

        if self.quote_gate_batch is not None:
            if self.portfolio_context is None:
                raise PromotedOperationalRunnerError(_ERR_RESULT)
            if candidates_present and self.quote_batch is None:
                raise PromotedOperationalRunnerError(_ERR_RESULT)
            if not candidates_present and self.quote_batch is not None:
                raise PromotedOperationalRunnerError(_ERR_RESULT)
            if type(self.quote_gate_batch) is not VerifiedPromotedOperationalQuoteGateBatch:
                raise PromotedOperationalRunnerError(_ERR_TYPE)
            self.quote_gate_batch.verify_content_identity()
            if self.quote_gate_batch.spec.spec_id != self.spec.quote_gate_spec.spec_id:
                raise PromotedOperationalRunnerError(_ERR_RESULT)
            if self.quote_gate_batch.quote_batch != self.quote_batch:
                raise PromotedOperationalRunnerError(_ERR_RESULT)
            if evaluated_at is None or self.quote_gate_batch.evaluated_at != evaluated_at:
                raise PromotedOperationalRunnerError(_ERR_RESULT)

        if self.allocation_batch is not None:
            if self.quote_gate_batch is None:
                raise PromotedOperationalRunnerError(_ERR_RESULT)
            if type(self.allocation_batch) is not VerifiedPromotedOperationalAllocationBatch:
                raise PromotedOperationalRunnerError(_ERR_TYPE)
            self.allocation_batch.verify_content_identity()
            if self.allocation_batch.quote_gate_batch.batch_id != self.quote_gate_batch.batch_id:
                raise PromotedOperationalRunnerError(_ERR_RESULT)
            if (
                self.allocation_batch.portfolio_context.context_id
                != self.portfolio_context.context_id
            ):
                raise PromotedOperationalRunnerError(_ERR_RESULT)
            if (
                self.allocation_batch.allocation_policy.allocation_policy_id
                != self.spec.allocation_policy.allocation_policy_id
            ):
                raise PromotedOperationalRunnerError(_ERR_RESULT)

        if self.decision_package is not None:
            if self.allocation_batch is None:
                raise PromotedOperationalRunnerError(_ERR_RESULT)
            if type(self.decision_package) is not PromotedOperationalDecisionPackage:
                raise PromotedOperationalRunnerError(_ERR_TYPE)
            self.decision_package.verify_content_identity()
            if (
                self.decision_package.decision.allocation_batch.allocation_batch_id
                != self.allocation_batch.allocation_batch_id
            ):
                raise PromotedOperationalRunnerError(_ERR_RESULT)

        not_before = self.spec.quote_gate_spec.decision_not_before
        deadline = self.spec.quote_gate_spec.decision_deadline
        started_in_window = not_before <= started_at <= deadline
        both_sources_pinned = (
            self.quote_source_id == self.spec.expected_quote_source_id
            and self.portfolio_source_id == self.spec.expected_portfolio_source_id
        )
        depth = 0
        if self.quote_batch is not None:
            depth = 1
        if self.portfolio_context is not None:
            depth = 2
        if self.quote_gate_batch is not None:
            depth = 3
        if self.allocation_batch is not None:
            depth = 4
        if self.decision_package is not None:
            depth = 5

        if self.status is PromotedOperationalRunStatus.COMPLETE:
            if (
                self.failure_codes
                or depth != 5
                or evaluated_at is None
                or evaluated_at > deadline
                or not started_in_window
                or not both_sources_pinned
                or completed_at > deadline
            ):
                raise PromotedOperationalRunnerError(_ERR_RESULT)
            return

        if not self.failure_codes or self.decision_package is not None:
            raise PromotedOperationalRunnerError(_ERR_RESULT)

        codes = set(self.failure_codes)
        primary_codes = codes - {PromotedOperationalRunFailureCode.CLOCK_NON_MONOTONIC.value}
        if len(primary_codes) > 1:
            raise PromotedOperationalRunnerError(_ERR_RESULT)
        primary = next(iter(primary_codes), None)

        F = PromotedOperationalRunFailureCode
        clock_compatible_primary_codes = {
            F.QUOTE_ACQUISITION_FAILED.value,
            F.QUOTE_COVERAGE_INVALID.value,
            F.PORTFOLIO_ACQUISITION_FAILED.value,
            F.QUOTE_GATE_FAILED.value,
            F.ALLOCATION_FAILED.value,
            F.DECISION_ASSEMBLY_FAILED.value,
        }
        if (
            clock_violation
            and primary is not None
            and primary not in clock_compatible_primary_codes
        ):
            raise PromotedOperationalRunnerError(_ERR_RESULT)
        if clock_violation:
            if primary in {
                F.QUOTE_ACQUISITION_FAILED.value,
                F.QUOTE_COVERAGE_INVALID.value,
                F.PORTFOLIO_ACQUISITION_FAILED.value,
            }:
                expected_clock_failure_completion = started_at
            elif primary in {
                F.QUOTE_GATE_FAILED.value,
                F.ALLOCATION_FAILED.value,
                F.DECISION_ASSEMBLY_FAILED.value,
            }:
                expected_clock_failure_completion = evaluated_at
            elif depth == 2:
                expected_clock_failure_completion = max(
                    started_at, evaluated_at or started_at
                )
            else:
                expected_clock_failure_completion = evaluated_at
            if (
                expected_clock_failure_completion is None
                or completed_at != expected_clock_failure_completion
            ):
                raise PromotedOperationalRunnerError(_ERR_RESULT)
        if primary == F.START_BEFORE_WINDOW.value:
            if (
                clock_violation
                or depth != 0
                or evaluated_at is not None
                or self.quote_source_id is not None
                or self.portfolio_source_id is not None
                or started_at >= not_before
                or completed_at != started_at
            ):
                raise PromotedOperationalRunnerError(_ERR_RESULT)
        elif primary == F.START_AFTER_DEADLINE.value:
            if (
                clock_violation
                or depth != 0
                or evaluated_at is not None
                or self.quote_source_id is not None
                or self.portfolio_source_id is not None
                or started_at <= deadline
                or completed_at != started_at
            ):
                raise PromotedOperationalRunnerError(_ERR_RESULT)
        elif primary == F.SOURCE_IDENTITY_INVALID.value:
            if (
                clock_violation
                or depth != 0
                or evaluated_at is not None
                or not started_in_window
                or both_sources_pinned
                or completed_at != started_at
            ):
                raise PromotedOperationalRunnerError(_ERR_RESULT)
        elif primary in (F.QUOTE_ACQUISITION_FAILED.value, F.QUOTE_COVERAGE_INVALID.value):
            if (
                depth != 0
                or evaluated_at is not None
                or not started_in_window
                or not both_sources_pinned
            ):
                raise PromotedOperationalRunnerError(_ERR_RESULT)
        elif primary == F.PORTFOLIO_ACQUISITION_FAILED.value:
            expected_depth = 1 if candidates_present else 0
            if (
                depth != expected_depth
                or evaluated_at is not None
                or not started_in_window
                or not both_sources_pinned
            ):
                raise PromotedOperationalRunnerError(_ERR_RESULT)
        elif primary in (F.EVALUATION_AFTER_DEADLINE.value, F.QUOTE_GATE_FAILED.value):
            if (
                depth != 2
                or evaluated_at is None
                or not started_in_window
                or not both_sources_pinned
            ):
                raise PromotedOperationalRunnerError(_ERR_RESULT)
            if primary == F.EVALUATION_AFTER_DEADLINE.value and evaluated_at <= deadline:
                raise PromotedOperationalRunnerError(_ERR_RESULT)
            if (
                primary == F.EVALUATION_AFTER_DEADLINE.value
                and completed_at != evaluated_at
            ):
                raise PromotedOperationalRunnerError(_ERR_RESULT)
            if primary == F.QUOTE_GATE_FAILED.value and evaluated_at > deadline:
                raise PromotedOperationalRunnerError(_ERR_RESULT)
        elif primary == F.ALLOCATION_FAILED.value:
            if (
                depth != 3
                or evaluated_at is None
                or evaluated_at > deadline
                or not started_in_window
                or not both_sources_pinned
            ):
                raise PromotedOperationalRunnerError(_ERR_RESULT)
        elif primary in (F.DECISION_ASSEMBLY_FAILED.value, F.COMPLETION_AFTER_DEADLINE.value):
            if (
                depth != 4
                or evaluated_at is None
                or evaluated_at > deadline
                or not started_in_window
                or not both_sources_pinned
            ):
                raise PromotedOperationalRunnerError(_ERR_RESULT)
            if primary == F.COMPLETION_AFTER_DEADLINE.value and completed_at <= deadline:
                raise PromotedOperationalRunnerError(_ERR_RESULT)
        elif primary is None:
            # CLOCK_NON_MONOTONIC alone: only the evaluation prefix (quote +
            # portfolio, no quote-gate batch) or the completion prefix
            # (through allocation, no decision package) are reachable.
            if (
                not clock_violation
                or depth not in (2, 4)
                or not started_in_window
                or not both_sources_pinned
            ):
                raise PromotedOperationalRunnerError(_ERR_RESULT)
            if depth == 4 and (evaluated_at is None or evaluated_at > deadline):
                raise PromotedOperationalRunnerError(_ERR_RESULT)
        else:
            raise PromotedOperationalRunnerError(_ERR_RESULT)

    def _identity(self) -> dict[str, object]:
        return {
            value.name: getattr(self, value.name)
            for value in fields(self)
            if value.name != "result_id"
        }

    def _calculated_id(self) -> str:
        return content_id(self._identity(), length=64)

    def verify_content_identity(self) -> None:
        self._verify()
        if self.result_id != self._calculated_id():
            raise PromotedOperationalRunnerError(_ERR_RESULT)

    @property
    def complete(self) -> bool:
        return self.status is PromotedOperationalRunStatus.COMPLETE

    @property
    def execution_eligible(self) -> bool:
        return False


class _QuoteCoverageError(ValueError):
    pass


def _safe_source_id(source: object) -> str | None:
    try:
        value = source.source_id
    except Exception:
        return None
    if type(value) is not str or _SHA256.fullmatch(value) is None:
        return None
    return value


def _read_time(clock: Callable[[], datetime]) -> datetime:
    try:
        value = clock()
    except Exception:
        raise PromotedOperationalRunnerError("operational clock failed") from None
    if type(value) is not datetime or value.tzinfo is None or value.utcoffset() is None:
        raise PromotedOperationalRunnerError(
            "operational clock must return an aware exact datetime"
        )
    return value.astimezone(timezone.utc)


def _completion_time(
    clock: Callable[[], datetime], floor: datetime
) -> tuple[datetime, bool]:
    try:
        value = _read_time(clock)
    except Exception:
        return floor, True
    if value < floor:
        return floor, True
    return value, False


def _collect_quotes(
    *, spec: PromotedOperationalRunSpec, source: PromotedOperationalQuoteSource
) -> FullQuoteBatch:
    listing_keys = tuple(sorted(spec.quote_gate_spec.preparation.manifest.listing_keys))
    chunk_size = spec.maximum_quote_chunk_size
    batches: list[FullQuoteBatch] = []
    for offset in range(0, len(listing_keys), chunk_size):
        requested = listing_keys[offset : offset + chunk_size]
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


def _collect_portfolio(
    *, spec: PromotedOperationalRunSpec, source: PromotedOperationalPortfolioSource
) -> PromotedOperationalPortfolioContext:
    context = source.read_portfolio_context()
    if type(context) is not PromotedOperationalPortfolioContext:
        raise TypeError("portfolio source returned the wrong type")
    context.verify_content_identity()
    if context.source_portfolio_artifact_id != spec.expected_portfolio_source_id:
        raise ValueError("portfolio artifact does not match the pinned source")
    return context


def _failure(
    *,
    spec: PromotedOperationalRunSpec,
    quote_source_id: str | None,
    portfolio_source_id: str | None,
    started_at: datetime,
    completed_at: datetime,
    codes: tuple[PromotedOperationalRunFailureCode, ...],
    evaluated_at: datetime | None = None,
    quote_batch: FullQuoteBatch | None = None,
    portfolio_context: PromotedOperationalPortfolioContext | None = None,
    quote_gate_batch: VerifiedPromotedOperationalQuoteGateBatch | None = None,
    allocation_batch: VerifiedPromotedOperationalAllocationBatch | None = None,
) -> PromotedOperationalRunResult:
    return PromotedOperationalRunResult(
        spec=spec,
        quote_source_id=quote_source_id,
        portfolio_source_id=portfolio_source_id,
        started_at=started_at,
        evaluated_at=evaluated_at,
        completed_at=max(started_at, completed_at, evaluated_at or started_at),
        status=PromotedOperationalRunStatus.FAILED,
        failure_codes=tuple(sorted({value.value for value in codes})),
        quote_batch=quote_batch,
        portfolio_context=portfolio_context,
        quote_gate_batch=quote_gate_batch,
        allocation_batch=allocation_batch,
        decision_package=None,
        paper_only=True,
        notification_eligible=False,
        execution_eligible=False,
    )


def execute_promoted_operational_run(
    *,
    spec: PromotedOperationalRunSpec,
    quote_source: PromotedOperationalQuoteSource,
    portfolio_source: PromotedOperationalPortfolioSource,
    clock: Callable[[], datetime],
) -> PromotedOperationalRunResult:
    """Acquire exact read-only inputs through the two injected ports and run
    the existing pure quote-gate -> allocation -> decision-package chain
    exactly once, returning one content-addressed COMPLETE or sanitized
    FAILED result. Raises ``PromotedOperationalRunnerError`` only when the
    very first clock read is untrustworthy -- every later failure becomes a
    FAILED result instead."""

    if type(spec) is not PromotedOperationalRunSpec:
        raise PromotedOperationalRunnerError(_ERR_TYPE)
    spec.verify_content_identity()

    started_at = _read_time(clock)

    # Window is checked strictly before either source_id property is ever
    # touched: source properties are untrusted capabilities, and an
    # out-of-window run must reject without exercising them at all.
    if started_at < spec.quote_gate_spec.decision_not_before:
        return _failure(
            spec=spec,
            quote_source_id=None,
            portfolio_source_id=None,
            started_at=started_at,
            completed_at=started_at,
            codes=(PromotedOperationalRunFailureCode.START_BEFORE_WINDOW,),
        )
    if started_at > spec.quote_gate_spec.decision_deadline:
        return _failure(
            spec=spec,
            quote_source_id=None,
            portfolio_source_id=None,
            started_at=started_at,
            completed_at=started_at,
            codes=(PromotedOperationalRunFailureCode.START_AFTER_DEADLINE,),
        )

    quote_source_id = _safe_source_id(quote_source)
    portfolio_source_id = _safe_source_id(portfolio_source)
    if (
        quote_source_id != spec.expected_quote_source_id
        or portfolio_source_id != spec.expected_portfolio_source_id
    ):
        return _failure(
            spec=spec,
            quote_source_id=quote_source_id,
            portfolio_source_id=portfolio_source_id,
            started_at=started_at,
            completed_at=started_at,
            codes=(PromotedOperationalRunFailureCode.SOURCE_IDENTITY_INVALID,),
        )

    candidates_present = bool(spec.quote_gate_spec.preparation.candidates)
    quote_batch: FullQuoteBatch | None = None
    if candidates_present:
        try:
            quote_batch = _collect_quotes(spec=spec, source=quote_source)
        except Exception as error:
            completed_at, clock_failed = _completion_time(clock, started_at)
            codes = [
                PromotedOperationalRunFailureCode.QUOTE_COVERAGE_INVALID
                if type(error) is _QuoteCoverageError
                else PromotedOperationalRunFailureCode.QUOTE_ACQUISITION_FAILED
            ]
            if clock_failed:
                codes.append(PromotedOperationalRunFailureCode.CLOCK_NON_MONOTONIC)
            return _failure(
                spec=spec,
                quote_source_id=quote_source_id,
                portfolio_source_id=portfolio_source_id,
                started_at=started_at,
                completed_at=completed_at,
                codes=tuple(codes),
            )

    try:
        portfolio_context = _collect_portfolio(spec=spec, source=portfolio_source)
    except Exception:
        completed_at, clock_failed = _completion_time(clock, started_at)
        codes = [PromotedOperationalRunFailureCode.PORTFOLIO_ACQUISITION_FAILED]
        if clock_failed:
            codes.append(PromotedOperationalRunFailureCode.CLOCK_NON_MONOTONIC)
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
            codes=(PromotedOperationalRunFailureCode.CLOCK_NON_MONOTONIC,),
            quote_batch=quote_batch,
            portfolio_context=portfolio_context,
        )

    lower_bounds = [started_at, portfolio_context.portfolio.as_of]
    if quote_batch is not None:
        lower_bounds.append(quote_batch.observed_at)
    if evaluated_at < max(lower_bounds):
        return _failure(
            spec=spec,
            quote_source_id=quote_source_id,
            portfolio_source_id=portfolio_source_id,
            started_at=started_at,
            completed_at=started_at,
            codes=(PromotedOperationalRunFailureCode.CLOCK_NON_MONOTONIC,),
            evaluated_at=evaluated_at,
            quote_batch=quote_batch,
            portfolio_context=portfolio_context,
        )
    if evaluated_at > spec.quote_gate_spec.decision_deadline:
        return _failure(
            spec=spec,
            quote_source_id=quote_source_id,
            portfolio_source_id=portfolio_source_id,
            started_at=started_at,
            completed_at=evaluated_at,
            codes=(PromotedOperationalRunFailureCode.EVALUATION_AFTER_DEADLINE,),
            evaluated_at=evaluated_at,
            quote_batch=quote_batch,
            portfolio_context=portfolio_context,
        )

    try:
        quote_gate_batch = evaluate_promoted_operational_quote_gate(
            spec=spec.quote_gate_spec, quote_batch=quote_batch, evaluated_at=evaluated_at
        )
    except Exception:
        completed_at, clock_failed = _completion_time(clock, evaluated_at)
        codes = [PromotedOperationalRunFailureCode.QUOTE_GATE_FAILED]
        if clock_failed:
            codes.append(PromotedOperationalRunFailureCode.CLOCK_NON_MONOTONIC)
        return _failure(
            spec=spec,
            quote_source_id=quote_source_id,
            portfolio_source_id=portfolio_source_id,
            started_at=started_at,
            completed_at=completed_at,
            codes=tuple(codes),
            evaluated_at=evaluated_at,
            quote_batch=quote_batch,
            portfolio_context=portfolio_context,
        )

    try:
        allocation_batch = assemble_promoted_operational_allocation_batch(
            quote_gate_batch=quote_gate_batch,
            portfolio_context=portfolio_context,
            allocation_policy=spec.allocation_policy,
        )
    except Exception:
        completed_at, clock_failed = _completion_time(clock, evaluated_at)
        codes = [PromotedOperationalRunFailureCode.ALLOCATION_FAILED]
        if clock_failed:
            codes.append(PromotedOperationalRunFailureCode.CLOCK_NON_MONOTONIC)
        return _failure(
            spec=spec,
            quote_source_id=quote_source_id,
            portfolio_source_id=portfolio_source_id,
            started_at=started_at,
            completed_at=completed_at,
            codes=tuple(codes),
            evaluated_at=evaluated_at,
            quote_batch=quote_batch,
            portfolio_context=portfolio_context,
            quote_gate_batch=quote_gate_batch,
        )

    try:
        decision_package = assemble_promoted_operational_decision_package(
            allocation_batch=allocation_batch
        )
    except Exception:
        completed_at, clock_failed = _completion_time(clock, evaluated_at)
        codes = [PromotedOperationalRunFailureCode.DECISION_ASSEMBLY_FAILED]
        if clock_failed:
            codes.append(PromotedOperationalRunFailureCode.CLOCK_NON_MONOTONIC)
        return _failure(
            spec=spec,
            quote_source_id=quote_source_id,
            portfolio_source_id=portfolio_source_id,
            started_at=started_at,
            completed_at=completed_at,
            codes=tuple(codes),
            evaluated_at=evaluated_at,
            quote_batch=quote_batch,
            portfolio_context=portfolio_context,
            quote_gate_batch=quote_gate_batch,
            allocation_batch=allocation_batch,
        )

    completed_at, clock_failed = _completion_time(clock, evaluated_at)
    if clock_failed:
        return _failure(
            spec=spec,
            quote_source_id=quote_source_id,
            portfolio_source_id=portfolio_source_id,
            started_at=started_at,
            completed_at=completed_at,
            codes=(PromotedOperationalRunFailureCode.CLOCK_NON_MONOTONIC,),
            evaluated_at=evaluated_at,
            quote_batch=quote_batch,
            portfolio_context=portfolio_context,
            quote_gate_batch=quote_gate_batch,
            allocation_batch=allocation_batch,
        )
    if completed_at > spec.quote_gate_spec.decision_deadline:
        return _failure(
            spec=spec,
            quote_source_id=quote_source_id,
            portfolio_source_id=portfolio_source_id,
            started_at=started_at,
            completed_at=completed_at,
            codes=(PromotedOperationalRunFailureCode.COMPLETION_AFTER_DEADLINE,),
            evaluated_at=evaluated_at,
            quote_batch=quote_batch,
            portfolio_context=portfolio_context,
            quote_gate_batch=quote_gate_batch,
            allocation_batch=allocation_batch,
        )

    return PromotedOperationalRunResult(
        spec=spec,
        quote_source_id=quote_source_id,
        portfolio_source_id=portfolio_source_id,
        started_at=started_at,
        evaluated_at=evaluated_at,
        completed_at=completed_at,
        status=PromotedOperationalRunStatus.COMPLETE,
        failure_codes=(),
        quote_batch=quote_batch,
        portfolio_context=portfolio_context,
        quote_gate_batch=quote_gate_batch,
        allocation_batch=allocation_batch,
        decision_package=decision_package,
        paper_only=True,
        notification_eligible=False,
        execution_eligible=False,
    )
