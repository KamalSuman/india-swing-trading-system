from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from decimal import Decimal
from enum import Enum

from india_swing.identity import content_id

from .models import MINIMUM_SWING_LABEL_HORIZON_SESSIONS


TRIAL_REGISTRATION_SCHEMA_VERSION = "research-trial-registration/v1"
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_SOURCE_COMMIT = re.compile(r"[0-9a-f]{7,64}\Z")


class TrialRegistrationError(ValueError):
    pass


class TrialRegistrationIntegrityError(TrialRegistrationError):
    pass


class TrialStage(str, Enum):
    EXPLORATORY = "EXPLORATORY"
    CONFIRMATORY = "CONFIRMATORY"


def _required_text(value: str, name: str) -> None:
    if (
        not isinstance(value, str)
        or not value
        or value != value.strip()
        or any(ord(character) < 32 for character in value)
    ):
        raise TrialRegistrationError(f"{name} must be canonical non-empty text")


def _sha(value: str, name: str) -> None:
    if not isinstance(value, str) or _SHA256.fullmatch(value) is None:
        raise TrialRegistrationError(f"{name} must be a full lowercase SHA-256")


def _id_tuple(values: tuple[str, ...], name: str) -> None:
    if (
        type(values) is not tuple
        or not values
        or values != tuple(sorted(set(values)))
    ):
        raise TrialRegistrationError(f"{name} must be a sorted unique tuple")
    for value in values:
        _sha(value, name)


def _finite_decimal(value: Decimal, name: str, *, positive: bool = False) -> None:
    if type(value) is not Decimal or not value.is_finite():
        raise TrialRegistrationError(f"{name} must be a finite Decimal")
    if positive and value <= 0:
        raise TrialRegistrationError(f"{name} must be positive")


@dataclass(frozen=True, slots=True)
class TrialRegistration:
    registered_at: datetime
    stage: TrialStage
    hypothesis: str
    strategy_family_id: str
    parent_trial_id: str | None
    evaluation_start: date
    evaluation_end: date
    universe_snapshot_ids: tuple[str, ...]
    data_snapshot_ids: tuple[str, ...]
    split_plan_id: str
    label_horizon_sessions: int
    benchmark_id: str
    primary_metric: str
    secondary_metrics: tuple[str, ...]
    model_bundle_id: str
    source_commit: str
    dependency_hash: str
    configuration_hash: str
    exclusions_hash: str
    risk_policy_hash: str
    execution_policy_version: str
    execution_policy_hash: str
    cost_schedule_version: str
    cost_schedule_hash: str
    base_slippage_bps: Decimal
    stressed_slippage_bps: Decimal | None
    pass_thresholds: tuple[tuple[str, Decimal], ...]
    multiple_testing_policy: str
    random_seed: int
    repetition_count: int
    holdout_id: str | None
    holdout_sealed: bool
    synthetic: bool = False
    schema_version: str = TRIAL_REGISTRATION_SCHEMA_VERSION
    trial_id: str = field(init=False)

    def __post_init__(self) -> None:
        if not isinstance(self.registered_at, datetime):
            raise TrialRegistrationError("registered_at must be a datetime")
        if self.registered_at.tzinfo is None or self.registered_at.utcoffset() is None:
            raise TrialRegistrationError("registered_at must be timezone-aware")
        object.__setattr__(
            self,
            "registered_at",
            self.registered_at.astimezone(timezone.utc),
        )
        if type(self.stage) is not TrialStage:
            raise TrialRegistrationError("stage must be an exact TrialStage")
        for value, name in (
            (self.hypothesis, "hypothesis"),
            (self.strategy_family_id, "strategy_family_id"),
            (self.benchmark_id, "benchmark_id"),
            (self.primary_metric, "primary_metric"),
            (self.model_bundle_id, "model_bundle_id"),
            (self.execution_policy_version, "execution_policy_version"),
            (self.cost_schedule_version, "cost_schedule_version"),
            (self.multiple_testing_policy, "multiple_testing_policy"),
        ):
            _required_text(value, name)
        if self.parent_trial_id is not None:
            _sha(self.parent_trial_id, "parent_trial_id")
        if (
            type(self.evaluation_start) is not date
            or type(self.evaluation_end) is not date
            or self.evaluation_end < self.evaluation_start
        ):
            raise TrialRegistrationError("evaluation dates are invalid")
        _id_tuple(self.universe_snapshot_ids, "universe_snapshot_ids")
        _id_tuple(self.data_snapshot_ids, "data_snapshot_ids")
        for value, name in (
            (self.split_plan_id, "split_plan_id"),
            (self.dependency_hash, "dependency_hash"),
            (self.configuration_hash, "configuration_hash"),
            (self.exclusions_hash, "exclusions_hash"),
            (self.risk_policy_hash, "risk_policy_hash"),
            (self.execution_policy_hash, "execution_policy_hash"),
            (self.cost_schedule_hash, "cost_schedule_hash"),
        ):
            _sha(value, name)
        if not isinstance(self.source_commit, str) or _SOURCE_COMMIT.fullmatch(
            self.source_commit
        ) is None:
            raise TrialRegistrationError("source_commit must be a lowercase git hash")
        if (
            type(self.label_horizon_sessions) is not int
            or self.label_horizon_sessions
            < MINIMUM_SWING_LABEL_HORIZON_SESSIONS
        ):
            raise TrialRegistrationError(
                "label horizon must be at least ten trading sessions"
            )
        if (
            type(self.secondary_metrics) is not tuple
            or self.secondary_metrics != tuple(sorted(set(self.secondary_metrics)))
            or self.primary_metric in self.secondary_metrics
        ):
            raise TrialRegistrationError(
                "secondary metrics must be sorted, unique, and exclude the primary metric"
            )
        for value in self.secondary_metrics:
            _required_text(value, "secondary metric")
        _finite_decimal(self.base_slippage_bps, "base_slippage_bps")
        if self.stressed_slippage_bps is not None:
            _finite_decimal(
                self.stressed_slippage_bps,
                "stressed_slippage_bps",
                positive=True,
            )
        if (
            type(self.pass_thresholds) is not tuple
            or not self.pass_thresholds
            or tuple(name for name, _ in self.pass_thresholds)
            != tuple(sorted({name for name, _ in self.pass_thresholds}))
        ):
            raise TrialRegistrationError(
                "pass thresholds must be a non-empty sorted unique tuple"
            )
        for name, value in self.pass_thresholds:
            _required_text(name, "threshold metric")
            _finite_decimal(value, f"threshold {name}")
        if self.primary_metric not in {name for name, _ in self.pass_thresholds}:
            raise TrialRegistrationError("primary metric requires a pass threshold")
        if type(self.random_seed) is not int or self.random_seed < 0:
            raise TrialRegistrationError("random_seed must be a non-negative integer")
        if type(self.repetition_count) is not int or self.repetition_count <= 0:
            raise TrialRegistrationError("repetition_count must be positive")
        if type(self.holdout_sealed) is not bool or type(self.synthetic) is not bool:
            raise TrialRegistrationError("holdout_sealed and synthetic must be bool")
        if self.holdout_id is not None:
            _sha(self.holdout_id, "holdout_id")
        if self.holdout_sealed != (self.holdout_id is not None):
            raise TrialRegistrationError(
                "holdout ID and sealed state must be declared together"
            )
        if not self.synthetic and self.base_slippage_bps <= 0:
            raise TrialRegistrationError(
                "reportable trials cannot assume zero base slippage"
            )
        if self.stage is TrialStage.CONFIRMATORY:
            if not self.holdout_sealed:
                raise TrialRegistrationError(
                    "confirmatory trial requires a sealed holdout"
                )
            if (
                self.stressed_slippage_bps is None
                or self.stressed_slippage_bps <= self.base_slippage_bps
            ):
                raise TrialRegistrationError(
                    "confirmatory trial requires stressed slippage above base"
                )
        if self.schema_version != TRIAL_REGISTRATION_SCHEMA_VERSION:
            raise TrialRegistrationError("unsupported trial-registration schema")
        object.__setattr__(self, "trial_id", self._calculated_id())

    def _calculated_id(self) -> str:
        return content_id(
            {
                "schema_version": self.schema_version,
                "registered_at": self.registered_at,
                "stage": self.stage,
                "hypothesis": self.hypothesis,
                "strategy_family_id": self.strategy_family_id,
                "parent_trial_id": self.parent_trial_id,
                "evaluation_start": self.evaluation_start,
                "evaluation_end": self.evaluation_end,
                "universe_snapshot_ids": self.universe_snapshot_ids,
                "data_snapshot_ids": self.data_snapshot_ids,
                "split_plan_id": self.split_plan_id,
                "label_horizon_sessions": self.label_horizon_sessions,
                "benchmark_id": self.benchmark_id,
                "primary_metric": self.primary_metric,
                "secondary_metrics": self.secondary_metrics,
                "model_bundle_id": self.model_bundle_id,
                "source_commit": self.source_commit,
                "dependency_hash": self.dependency_hash,
                "configuration_hash": self.configuration_hash,
                "exclusions_hash": self.exclusions_hash,
                "risk_policy_hash": self.risk_policy_hash,
                "execution_policy_version": self.execution_policy_version,
                "execution_policy_hash": self.execution_policy_hash,
                "cost_schedule_version": self.cost_schedule_version,
                "cost_schedule_hash": self.cost_schedule_hash,
                "base_slippage_bps": self.base_slippage_bps,
                "stressed_slippage_bps": self.stressed_slippage_bps,
                "pass_thresholds": self.pass_thresholds,
                "multiple_testing_policy": self.multiple_testing_policy,
                "random_seed": self.random_seed,
                "repetition_count": self.repetition_count,
                "holdout_id": self.holdout_id,
                "holdout_sealed": self.holdout_sealed,
                "synthetic": self.synthetic,
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        if self.trial_id != self._calculated_id():
            raise TrialRegistrationIntegrityError(
                "trial registration content identity failed"
            )
