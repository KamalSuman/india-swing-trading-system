from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from enum import Enum

from india_swing.identity import content_id
from india_swing.reference.models import ReferenceReadiness


PROMOTION_POLICY_VERSION = "point-in-time-data-promotion/v1"
PROMOTION_DECISION_SCHEMA_VERSION = "promotion-decision/v1"
PROMOTION_EVIDENCE_SCHEMA_VERSION = "promotion-evidence/v1"

_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_REASON_CODE = re.compile(r"[A-Z][A-Z0-9_]{2,127}\Z")


class PromotionIntegrityError(ValueError):
    pass


class PromotionStage(str, Enum):
    COLLECTION_ONLY = "COLLECTION_ONLY"
    RESEARCH_ELIGIBLE = "RESEARCH_ELIGIBLE"
    BACKTEST_ELIGIBLE = "BACKTEST_ELIGIBLE"
    ALERT_ELIGIBLE = "ALERT_ELIGIBLE"


class PromotionCapability(str, Enum):
    CALENDAR = "CALENDAR"
    STABLE_IDENTITY = "STABLE_IDENTITY"
    UNIVERSE = "UNIVERSE"
    RAW_PRICES = "RAW_PRICES"
    CORPORATE_ACTIONS = "CORPORATE_ACTIONS"
    LIQUIDITY = "LIQUIDITY"
    SURVEILLANCE = "SURVEILLANCE"
    TICK_SIZES = "TICK_SIZES"
    EXPLICIT_NONTRADING = "EXPLICIT_NONTRADING"
    RECONCILIATION = "RECONCILIATION"
    MODEL_VALIDATION = "MODEL_VALIDATION"
    RISK_POLICY = "RISK_POLICY"
    SHADOW_OPERATIONS = "SHADOW_OPERATIONS"


def _aware_utc(value: datetime, name: str) -> datetime:
    if not isinstance(value, datetime):
        raise TypeError(f"{name} must be a datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        raise PromotionIntegrityError(f"{name} must be timezone-aware")
    return value.astimezone(timezone.utc)


def _sha(value: str, name: str) -> None:
    if not isinstance(value, str) or _SHA256.fullmatch(value) is None:
        raise PromotionIntegrityError(f"{name} must be a full lowercase SHA-256")


def _reason_codes(values: tuple[str, ...], name: str) -> None:
    if type(values) is not tuple or values != tuple(sorted(set(values))):
        raise PromotionIntegrityError(f"{name} must be a sorted unique tuple")
    if any(_REASON_CODE.fullmatch(value) is None for value in values):
        raise PromotionIntegrityError(f"{name} contains an invalid reason code")


@dataclass(frozen=True, slots=True)
class PromotionEvidence:
    capability: PromotionCapability
    cutoff: datetime
    coverage_start: date
    coverage_end: date
    source_snapshot_ids: tuple[str, ...]
    readiness: ReferenceReadiness
    complete: bool
    actionable: bool
    reason_codes: tuple[str, ...]
    schema_version: str = PROMOTION_EVIDENCE_SCHEMA_VERSION
    evidence_id: str = field(init=False)

    def __post_init__(self) -> None:
        if type(self.capability) is not PromotionCapability:
            raise TypeError("promotion capability must be exact")
        object.__setattr__(self, "cutoff", _aware_utc(self.cutoff, "evidence cutoff"))
        if type(self.coverage_start) is not date or type(self.coverage_end) is not date:
            raise TypeError("evidence coverage boundaries must be dates")
        if self.coverage_end < self.coverage_start:
            raise PromotionIntegrityError("evidence coverage interval must be positive")
        if (
            type(self.source_snapshot_ids) is not tuple
            or not self.source_snapshot_ids
            or self.source_snapshot_ids != tuple(sorted(set(self.source_snapshot_ids)))
        ):
            raise PromotionIntegrityError(
                "evidence source_snapshot_ids must be non-empty, sorted, and unique"
            )
        for value in self.source_snapshot_ids:
            _sha(value, "evidence source_snapshot_id")
        if type(self.readiness) is not ReferenceReadiness:
            raise TypeError("evidence readiness must be exact")
        if type(self.complete) is not bool or type(self.actionable) is not bool:
            raise TypeError("evidence completeness and actionability must be bool")
        _reason_codes(self.reason_codes, "evidence reason_codes")
        if self.actionable and (
            not self.complete
            or self.reason_codes
            or self.readiness is ReferenceReadiness.COLLECTION_ONLY
        ):
            raise PromotionIntegrityError(
                "actionable evidence must be complete, unblocked, and verified"
            )
        if not self.actionable and not self.reason_codes:
            raise PromotionIntegrityError("non-actionable evidence requires reason codes")
        if self.schema_version != PROMOTION_EVIDENCE_SCHEMA_VERSION:
            raise PromotionIntegrityError("unsupported promotion-evidence schema")
        object.__setattr__(self, "evidence_id", self._calculated_id())

    def _calculated_id(self) -> str:
        return content_id(
            {
                "schema_version": self.schema_version,
                "capability": self.capability,
                "cutoff": self.cutoff,
                "coverage_start": self.coverage_start,
                "coverage_end": self.coverage_end,
                "source_snapshot_ids": self.source_snapshot_ids,
                "readiness": self.readiness,
                "complete": self.complete,
                "actionable": self.actionable,
                "reason_codes": self.reason_codes,
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        if self.evidence_id != self._calculated_id():
            raise PromotionIntegrityError("promotion evidence content identity failed")


@dataclass(frozen=True, slots=True)
class PromotionDecision:
    market_session: date
    history_start: date
    decision_cutoff: datetime
    evidence: tuple[PromotionEvidence, ...]
    achieved_stage: PromotionStage
    research_blockers: tuple[str, ...]
    backtest_blockers: tuple[str, ...]
    alert_blockers: tuple[str, ...]
    policy_version: str = PROMOTION_POLICY_VERSION
    schema_version: str = PROMOTION_DECISION_SCHEMA_VERSION
    decision_id: str = field(init=False)

    def __post_init__(self) -> None:
        if type(self.market_session) is not date or type(self.history_start) is not date:
            raise TypeError("promotion session boundaries must be dates")
        if self.history_start > self.market_session:
            raise PromotionIntegrityError("promotion history cannot start after its session")
        object.__setattr__(
            self,
            "decision_cutoff",
            _aware_utc(self.decision_cutoff, "promotion decision_cutoff"),
        )
        if (
            type(self.evidence) is not tuple
            or any(type(value) is not PromotionEvidence for value in self.evidence)
            or self.evidence
            != tuple(sorted(self.evidence, key=lambda value: value.capability.value))
        ):
            raise PromotionIntegrityError(
                "promotion evidence must be exact and capability ordered"
            )
        if len({value.capability for value in self.evidence}) != len(self.evidence):
            raise PromotionIntegrityError("promotion evidence capabilities must be unique")
        for value in self.evidence:
            value.verify_content_identity()
        if type(self.achieved_stage) is not PromotionStage:
            raise TypeError("promotion achieved_stage must be exact")
        for values, name in (
            (self.research_blockers, "research_blockers"),
            (self.backtest_blockers, "backtest_blockers"),
            (self.alert_blockers, "alert_blockers"),
        ):
            _reason_codes(values, name)
        expected_stage = (
            PromotionStage.ALERT_ELIGIBLE
            if not self.alert_blockers
            else PromotionStage.BACKTEST_ELIGIBLE
            if not self.backtest_blockers
            else PromotionStage.RESEARCH_ELIGIBLE
            if not self.research_blockers
            else PromotionStage.COLLECTION_ONLY
        )
        if self.achieved_stage is not expected_stage:
            raise PromotionIntegrityError("achieved stage disagrees with blocker sets")
        if (
            self.policy_version != PROMOTION_POLICY_VERSION
            or self.schema_version != PROMOTION_DECISION_SCHEMA_VERSION
        ):
            raise PromotionIntegrityError("unsupported promotion decision contract")
        object.__setattr__(self, "decision_id", self._calculated_id())

    @property
    def research_eligible(self) -> bool:
        return not self.research_blockers

    @property
    def backtest_eligible(self) -> bool:
        return not self.backtest_blockers

    @property
    def alert_eligible(self) -> bool:
        return not self.alert_blockers

    def evidence_for(self, capability: PromotionCapability) -> PromotionEvidence:
        matches = tuple(value for value in self.evidence if value.capability is capability)
        if len(matches) != 1:
            raise PromotionIntegrityError(
                f"exactly one {capability.value} evidence record is required"
            )
        return matches[0]

    def _calculated_id(self) -> str:
        return content_id(
            {
                "schema_version": self.schema_version,
                "policy_version": self.policy_version,
                "market_session": self.market_session,
                "history_start": self.history_start,
                "decision_cutoff": self.decision_cutoff,
                "evidence": self.evidence,
                "achieved_stage": self.achieved_stage,
                "research_blockers": self.research_blockers,
                "backtest_blockers": self.backtest_blockers,
                "alert_blockers": self.alert_blockers,
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        for value in self.evidence:
            if type(value) is not PromotionEvidence:
                raise PromotionIntegrityError("promotion evidence graph contains invalid data")
            value.verify_content_identity()
        if self.decision_id != self._calculated_id():
            raise PromotionIntegrityError("promotion decision content identity failed")
