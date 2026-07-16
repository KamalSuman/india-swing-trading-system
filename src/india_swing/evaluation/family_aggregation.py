from __future__ import annotations

import re
from dataclasses import dataclass, field
from decimal import Decimal
from math import comb

from india_swing.identity import content_id

from .baseline_store import LocalDeterministicComparisonRunStore
from .baselines import DeterministicComparisonRun
from .trial_store import LocalTrialRegistry


FOLD_SIGN_HOLM_POLICY = "holm-familywise-primary-fold-sign-v1"
FOLD_SIGN_HOLM_ALPHA = Decimal("0.05")
ZERO = Decimal("0")
ONE = Decimal("1")
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")


class TrialFamilyAggregationError(ValueError):
    pass


def _sha(value: str, name: str) -> None:
    if not isinstance(value, str) or _SHA256.fullmatch(value) is None:
        raise TrialFamilyAggregationError(f"{name} must be a full lowercase SHA-256")


def _exact_sign_tail(wins: int, sample_count: int) -> Decimal:
    if type(wins) is not int or type(sample_count) is not int:
        raise TrialFamilyAggregationError("sign-test counts must be integers")
    if sample_count <= 0 or wins < 0 or wins > sample_count:
        raise TrialFamilyAggregationError("sign-test counts are invalid")
    numerator = sum(comb(sample_count, value) for value in range(wins, sample_count + 1))
    return Decimal(numerator) / Decimal(2**sample_count)


@dataclass(frozen=True, slots=True)
class FamilyTrialDecision:
    trial_id: str
    comparison_id: str
    fold_count: int
    base_wins: int
    stressed_wins: int
    base_p_value: Decimal
    stressed_p_value: Decimal
    raw_p_value: Decimal
    holm_rank: int
    holm_threshold: Decimal
    hypothesis_rejected: bool
    comparison_passed: bool
    eligible: bool
    decision_id: str = field(init=False)

    def __post_init__(self) -> None:
        _sha(self.trial_id, "trial_id")
        _sha(self.comparison_id, "comparison_id")
        if type(self.fold_count) is not int or self.fold_count <= 0:
            raise TrialFamilyAggregationError("fold_count must be positive")
        for value, name in (
            (self.base_wins, "base_wins"),
            (self.stressed_wins, "stressed_wins"),
        ):
            if type(value) is not int or value < 0 or value > self.fold_count:
                raise TrialFamilyAggregationError(f"{name} is invalid")
        for value, name in (
            (self.base_p_value, "base_p_value"),
            (self.stressed_p_value, "stressed_p_value"),
            (self.raw_p_value, "raw_p_value"),
            (self.holm_threshold, "holm_threshold"),
        ):
            if type(value) is not Decimal or not value.is_finite() or not ZERO <= value <= ONE:
                raise TrialFamilyAggregationError(f"{name} must be between zero and one")
        if self.raw_p_value != max(self.base_p_value, self.stressed_p_value):
            raise TrialFamilyAggregationError("raw p-value must be the conservative scenario maximum")
        if (
            self.base_p_value != _exact_sign_tail(self.base_wins, self.fold_count)
            or self.stressed_p_value
            != _exact_sign_tail(self.stressed_wins, self.fold_count)
        ):
            raise TrialFamilyAggregationError("p-values differ from exact fold-sign evidence")
        if type(self.holm_rank) is not int or self.holm_rank <= 0:
            raise TrialFamilyAggregationError("holm_rank must be positive")
        for value, name in (
            (self.hypothesis_rejected, "hypothesis_rejected"),
            (self.comparison_passed, "comparison_passed"),
            (self.eligible, "eligible"),
        ):
            if type(value) is not bool:
                raise TrialFamilyAggregationError(f"{name} must be bool")
        if self.eligible != (self.hypothesis_rejected and self.comparison_passed):
            raise TrialFamilyAggregationError("eligible status differs from statistical and trade gates")
        object.__setattr__(self, "decision_id", self._calculated_id())

    def _calculated_id(self) -> str:
        return content_id(
            {
                "schema": "family-trial-holm-decision/v1",
                "trial_id": self.trial_id,
                "comparison_id": self.comparison_id,
                "fold_count": self.fold_count,
                "base_wins": self.base_wins,
                "stressed_wins": self.stressed_wins,
                "base_p_value": self.base_p_value,
                "stressed_p_value": self.stressed_p_value,
                "raw_p_value": self.raw_p_value,
                "holm_rank": self.holm_rank,
                "holm_threshold": self.holm_threshold,
                "hypothesis_rejected": self.hypothesis_rejected,
                "comparison_passed": self.comparison_passed,
                "eligible": self.eligible,
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        if self.decision_id != self._calculated_id():
            raise TrialFamilyAggregationError("family trial decision identity failed")


@dataclass(frozen=True, slots=True)
class TrialFamilyEvaluationAggregate:
    strategy_family_id: str
    policy: str
    alpha: Decimal
    registered_trial_ids: tuple[str, ...]
    decisions: tuple[FamilyTrialDecision, ...]
    eligible_trial_ids: tuple[str, ...] = field(init=False)
    passed: bool = field(init=False)
    aggregate_id: str = field(init=False)

    def __post_init__(self) -> None:
        if not isinstance(self.strategy_family_id, str) or not self.strategy_family_id.strip():
            raise TrialFamilyAggregationError("strategy_family_id is required")
        if self.policy != FOLD_SIGN_HOLM_POLICY or self.alpha != FOLD_SIGN_HOLM_ALPHA:
            raise TrialFamilyAggregationError("unsupported familywise policy or alpha")
        if (
            type(self.registered_trial_ids) is not tuple
            or not self.registered_trial_ids
            or self.registered_trial_ids != tuple(sorted(set(self.registered_trial_ids)))
        ):
            raise TrialFamilyAggregationError("registered trial IDs must be sorted and unique")
        for value in self.registered_trial_ids:
            _sha(value, "registered_trial_id")
        if (
            type(self.decisions) is not tuple
            or not self.decisions
            or self.decisions
            != tuple(sorted(self.decisions, key=lambda item: (item.holm_rank, item.trial_id)))
        ):
            raise TrialFamilyAggregationError("family decisions must be ordered by Holm rank")
        if {item.trial_id for item in self.decisions} != set(self.registered_trial_ids):
            raise TrialFamilyAggregationError("family decisions must cover every registered trial")
        for expected_rank, decision in enumerate(self.decisions, start=1):
            if type(decision) is not FamilyTrialDecision:
                raise TrialFamilyAggregationError("family decisions must contain exact values")
            decision.verify_content_identity()
            if decision.holm_rank != expected_rank:
                raise TrialFamilyAggregationError("Holm ranks must be consecutive")
        if self.decisions != tuple(
            sorted(self.decisions, key=lambda item: (item.raw_p_value, item.trial_id))
        ):
            raise TrialFamilyAggregationError("Holm decisions are not p-value ordered")
        continue_rejecting = True
        family_size = len(self.decisions)
        for rank, decision in enumerate(self.decisions, start=1):
            threshold = self.alpha / Decimal(family_size - rank + 1)
            rejected = continue_rejecting and decision.raw_p_value <= threshold
            if decision.holm_threshold != threshold or decision.hypothesis_rejected != rejected:
                raise TrialFamilyAggregationError("Holm decision differs from step-down policy")
            if not rejected:
                continue_rejecting = False
        eligible = tuple(sorted(item.trial_id for item in self.decisions if item.eligible))
        object.__setattr__(self, "eligible_trial_ids", eligible)
        object.__setattr__(self, "passed", bool(eligible))
        object.__setattr__(self, "aggregate_id", self._calculated_id())

    def _calculated_id(self) -> str:
        return content_id(
            {
                "schema": "trial-family-evaluation-aggregate/v1",
                "strategy_family_id": self.strategy_family_id,
                "policy": self.policy,
                "alpha": self.alpha,
                "registered_trial_ids": self.registered_trial_ids,
                "decisions": self.decisions,
                "eligible_trial_ids": self.eligible_trial_ids,
                "passed": self.passed,
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        if self.aggregate_id != self._calculated_id():
            raise TrialFamilyAggregationError("family aggregate content identity failed")


class TrialFamilyEvaluationAggregator:
    """Exact fold-sign evidence followed by Holm step-down across a full trial family."""

    def __init__(
        self,
        registry: LocalTrialRegistry,
        run_store: LocalDeterministicComparisonRunStore,
    ) -> None:
        if type(registry) is not LocalTrialRegistry:
            raise TypeError("registry must be exact")
        if type(run_store) is not LocalDeterministicComparisonRunStore:
            raise TypeError("run_store must be exact")
        self.registry = registry
        self.run_store = run_store

    def aggregate(
        self,
        *,
        strategy_family_id: str,
        runs: tuple[DeterministicComparisonRun, ...],
    ) -> TrialFamilyEvaluationAggregate:
        registrations = self.registry.registrations_for_family(strategy_family_id)
        if not registrations:
            raise TrialFamilyAggregationError("trial family is not registered")
        if any(value.multiple_testing_policy != FOLD_SIGN_HOLM_POLICY for value in registrations):
            raise TrialFamilyAggregationError(
                "every family registration must preregister the supported Holm fold-sign policy"
            )
        if type(runs) is not tuple or any(type(value) is not DeterministicComparisonRun for value in runs):
            raise TrialFamilyAggregationError("runs must be an exact tuple")
        registration_ids = {value.trial_id for value in registrations}
        run_ids = {value.comparison.trial_id for value in runs}
        if len(run_ids) != len(runs) or run_ids != registration_ids:
            raise TrialFamilyAggregationError(
                "runs must cover every registered family trial exactly once"
            )
        raw: list[tuple[Decimal, str, DeterministicComparisonRun, int, int, Decimal, Decimal]] = []
        for run in runs:
            self.run_store.require_persisted(run)
            registration = self.registry.require_registered(run.comparison.trial_id)
            if (
                registration.strategy_family_id != strategy_family_id
                or registration.primary_metric != run.comparison.primary_metric
            ):
                raise TrialFamilyAggregationError("run differs from its family registration")
            if any(
                "stressed_primary_excess" not in dict(summary.comparison_metrics)
                for summary in run.fold_summaries
            ):
                raise TrialFamilyAggregationError("Holm fold-sign policy requires stressed folds")
            fold_count = len(run.fold_summaries)
            base_wins = sum(
                dict(summary.comparison_metrics)["base_primary_excess"] > ZERO
                for summary in run.fold_summaries
            )
            stressed_wins = sum(
                dict(summary.comparison_metrics)["stressed_primary_excess"] > ZERO
                for summary in run.fold_summaries
            )
            base_p = _exact_sign_tail(base_wins, fold_count)
            stressed_p = _exact_sign_tail(stressed_wins, fold_count)
            raw_p = max(base_p, stressed_p)
            raw.append(
                (
                    raw_p,
                    registration.trial_id,
                    run,
                    base_wins,
                    stressed_wins,
                    base_p,
                    stressed_p,
                )
            )
        raw.sort(key=lambda item: (item[0], item[1]))
        decisions: list[FamilyTrialDecision] = []
        continue_rejecting = True
        family_size = len(raw)
        for rank, (
            raw_p,
            trial_id,
            run,
            base_wins,
            stressed_wins,
            base_p,
            stressed_p,
        ) in enumerate(raw, start=1):
            threshold = FOLD_SIGN_HOLM_ALPHA / Decimal(family_size - rank + 1)
            rejected = continue_rejecting and raw_p <= threshold
            if not rejected:
                continue_rejecting = False
            decisions.append(
                FamilyTrialDecision(
                    trial_id=trial_id,
                    comparison_id=run.comparison.comparison_id,
                    fold_count=len(run.fold_summaries),
                    base_wins=base_wins,
                    stressed_wins=stressed_wins,
                    base_p_value=base_p,
                    stressed_p_value=stressed_p,
                    raw_p_value=raw_p,
                    holm_rank=rank,
                    holm_threshold=threshold,
                    hypothesis_rejected=rejected,
                    comparison_passed=run.comparison.passed,
                    eligible=rejected and run.comparison.passed,
                )
            )
        return TrialFamilyEvaluationAggregate(
            strategy_family_id=strategy_family_id,
            policy=FOLD_SIGN_HOLM_POLICY,
            alpha=FOLD_SIGN_HOLM_ALPHA,
            registered_trial_ids=tuple(sorted(registration_ids)),
            decisions=tuple(decisions),
        )
