"""Deterministic technical features for 60-bar forward-paper inputs.

The established promoted technical kernel is reused, not reimplemented.  This
module pins the only allowed compatibility configuration: 60 bars contain 59
session-to-session return intervals, so the longest return is explicitly 59
while drawdown consumes all 60 bars. Tick size is deliberately signal-session
only: the engine does not invent historical tick facts merely to calculate a
diagnostic change count. Results remain
collection-only and cannot authorize ranking, alerts, paper trades, or orders.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from enum import Enum
from typing import Union

from india_swing.features.promoted_technical import (
    PromotedTechnicalFeatureConfig,
    PromotedTechnicalFeatureVector,
    _DegenerateInput,
    _compute_vector,
)
from india_swing.identity import content_id

from .feature_inputs import (
    ForwardPaperFeatureInputCandidate,
    ForwardPaperFeatureInputVeto,
    ForwardPaperFeatureInputWindow,
)


FORWARD_PAPER_TECHNICAL_FEATURE_POLICY_VERSION = (
    "forward-paper-technical-feature/60-bars-signal-tick-only-v2"
)
FORWARD_PAPER_TECHNICAL_FEATURE_RESULT_SCHEMA_VERSION = (
    "forward-paper-technical-feature-result/v1"
)
FORWARD_PAPER_TECHNICAL_FEATURE_WINDOW_SCHEMA_VERSION = (
    "forward-paper-technical-feature-window/v1"
)

FORWARD_PAPER_TECHNICAL_FEATURE_CONFIG = PromotedTechnicalFeatureConfig(
    minimum_history_sessions=60,
    short_return_sessions=5,
    medium_return_sessions=20,
    long_return_sessions=59,
    short_trend_sessions=20,
    long_trend_sessions=50,
    atr_sessions=14,
    volatility_sessions=20,
    liquidity_sessions=20,
    breakout_sessions=20,
    drawdown_sessions=60,
    contraction_short_sessions=5,
    contraction_long_sessions=20,
    tick_history_sessions=1,
    annualization_sessions=252,
)

_SHA256 = re.compile(r"[0-9a-f]{64}\Z")


class ForwardPaperTechnicalFeatureError(ValueError):
    """A forward-paper feature graph failed a static safety rule."""


def _fail(message: str) -> None:
    raise ForwardPaperTechnicalFeatureError(message)


def _sha(value: object) -> bool:
    return type(value) is str and _SHA256.fullmatch(value) is not None


class ForwardPaperTechnicalFeatureStatus(Enum):
    FEATURE_VECTOR_COMPUTED_COLLECTION_ONLY = (
        "FEATURE_VECTOR_COMPUTED_COLLECTION_ONLY"
    )
    SOURCE_INPUT_VETO = "SOURCE_INPUT_VETO"
    DEGENERATE_INPUT_VETO = "DEGENERATE_INPUT_VETO"


@dataclass(frozen=True, slots=True)
class ForwardPaperTechnicalFeatureResult:
    source_outcome: Union[
        ForwardPaperFeatureInputCandidate,
        ForwardPaperFeatureInputVeto,
    ]
    status: ForwardPaperTechnicalFeatureStatus
    observed_history_sessions: int
    feature_vector: PromotedTechnicalFeatureVector | None
    result_id: str = field(init=False)

    def __post_init__(self) -> None:
        self._validate()
        object.__setattr__(self, "result_id", self._calculated_id())

    def _validate(self) -> None:
        if type(self.status) is not ForwardPaperTechnicalFeatureStatus:
            _fail("forward paper technical feature result status is invalid")
        if type(self.source_outcome) not in (
            ForwardPaperFeatureInputCandidate,
            ForwardPaperFeatureInputVeto,
        ):
            _fail("forward paper technical feature result source is invalid")
        verification_failed = False
        try:
            self.source_outcome.verify_content_identity()
            if self.feature_vector is not None:
                self.feature_vector.verify_content_identity()
        except Exception:
            verification_failed = True
        if verification_failed:
            _fail("forward paper technical feature result evidence failed verification")
        if (
            type(self.observed_history_sessions) is not int
            or isinstance(self.observed_history_sessions, bool)
            or self.observed_history_sessions < 0
        ):
            _fail("forward paper technical feature observed history is invalid")
        computed = (
            self.status
            is ForwardPaperTechnicalFeatureStatus.FEATURE_VECTOR_COMPUTED_COLLECTION_ONLY
        )
        if computed:
            if (
                type(self.source_outcome) is not ForwardPaperFeatureInputCandidate
                or type(self.feature_vector) is not PromotedTechnicalFeatureVector
                or self.observed_history_sessions != 60
            ):
                _fail("forward paper technical feature computed result is invalid")
            source = self.source_outcome
            vector = self.feature_vector
            if (
                vector.source_history_id != source.candidate_id
                or vector.config_id
                != FORWARD_PAPER_TECHNICAL_FEATURE_CONFIG.config_id
                or vector.stable_instrument_id != source.stable_instrument_id
                or vector.stable_listing_id != source.stable_listing_id
                or vector.signal_session != source.signal_session
                or vector.input_bar_ids
                != tuple(value.input_bar_id for value in source.bars)
                or vector.knowledge_time
                != max(value.knowledge_time for value in source.bars)
            ):
                _fail("forward paper technical feature vector lineage is invalid")
        else:
            if self.feature_vector is not None:
                _fail("forward paper technical feature veto carries a vector")
            if (
                self.status is ForwardPaperTechnicalFeatureStatus.SOURCE_INPUT_VETO
                and (
                    type(self.source_outcome) is not ForwardPaperFeatureInputVeto
                    or self.observed_history_sessions != 0
                )
            ):
                _fail("forward paper technical source veto is invalid")
            if (
                self.status is ForwardPaperTechnicalFeatureStatus.DEGENERATE_INPUT_VETO
                and (
                    type(self.source_outcome) is not ForwardPaperFeatureInputCandidate
                    or self.observed_history_sessions != len(self.source_outcome.bars)
                )
            ):
                _fail("forward paper technical degenerate veto is invalid")

    @property
    def source_outcome_id(self) -> str:
        if type(self.source_outcome) is ForwardPaperFeatureInputCandidate:
            return self.source_outcome.candidate_id
        return self.source_outcome.veto_id

    def _calculated_id(self) -> str:
        return content_id(
            {
                "schema": FORWARD_PAPER_TECHNICAL_FEATURE_RESULT_SCHEMA_VERSION,
                "policy_version": FORWARD_PAPER_TECHNICAL_FEATURE_POLICY_VERSION,
                "source_outcome_id": self.source_outcome_id,
                "status": self.status,
                "config_id": FORWARD_PAPER_TECHNICAL_FEATURE_CONFIG.config_id,
                "observed_history_sessions": self.observed_history_sessions,
                "feature_id": (
                    None if self.feature_vector is None else self.feature_vector.feature_id
                ),
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        self._validate()
        if self.result_id != self._calculated_id():
            _fail("forward paper technical feature result failed verification")

    @classmethod
    def _from_freshly_verified_derivation(
        cls,
        *,
        source_outcome: Union[
            ForwardPaperFeatureInputCandidate,
            ForwardPaperFeatureInputVeto,
        ],
        status: ForwardPaperTechnicalFeatureStatus,
        observed_history_sessions: int,
        feature_vector: PromotedTechnicalFeatureVector | None,
    ) -> "ForwardPaperTechnicalFeatureResult":
        value = object.__new__(cls)
        object.__setattr__(value, "source_outcome", source_outcome)
        object.__setattr__(value, "status", status)
        object.__setattr__(value, "observed_history_sessions", observed_history_sessions)
        object.__setattr__(value, "feature_vector", feature_vector)
        object.__setattr__(value, "result_id", value._calculated_id())
        return value


@dataclass(frozen=True, slots=True)
class ForwardPaperTechnicalFeatureWindow:
    source_window: ForwardPaperFeatureInputWindow
    config: PromotedTechnicalFeatureConfig
    results: tuple[ForwardPaperTechnicalFeatureResult, ...]
    computed_feature_count: int
    blocked_feature_count: int
    resolved_histories_feature_complete: bool
    window_id: str = field(init=False)

    def __post_init__(self) -> None:
        self._validate()
        object.__setattr__(self, "window_id", self._calculated_id())

    def _validate(self) -> None:
        if type(self.source_window) is not ForwardPaperFeatureInputWindow:
            _fail("forward paper technical feature window source is invalid")
        if (
            type(self.config) is not PromotedTechnicalFeatureConfig
            or self.config.config_id
            != FORWARD_PAPER_TECHNICAL_FEATURE_CONFIG.config_id
        ):
            _fail("forward paper technical feature configuration is invalid")
        verification_failed = False
        try:
            self.source_window.verify_content_identity()
            self.config.verify_content_identity()
        except Exception:
            verification_failed = True
        if verification_failed:
            _fail("forward paper technical feature window evidence failed verification")
        if type(self.results) is not tuple or len(self.results) != len(
            self.source_window.outcomes
        ):
            _fail("forward paper technical feature window results are invalid")
        computed = blocked = 0
        cutoff = (
            self.source_window.source_window.source_window.spec.decision_cutoff
        )
        for source, result in zip(self.source_window.outcomes, self.results, strict=True):
            if type(result) is not ForwardPaperTechnicalFeatureResult:
                _fail("forward paper technical feature window result is invalid")
            result.verify_content_identity()
            source_id = (
                source.candidate_id
                if type(source) is ForwardPaperFeatureInputCandidate
                else source.veto_id
            )
            if result.source_outcome_id != source_id or result.source_outcome is not source:
                _fail("forward paper technical feature window lineage is invalid")
            if result.feature_vector is not None and result.feature_vector.cutoff != cutoff:
                _fail("forward paper technical feature window cutoff is invalid")
            if (
                result.status
                is ForwardPaperTechnicalFeatureStatus.FEATURE_VECTOR_COMPUTED_COLLECTION_ONLY
            ):
                computed += 1
            else:
                blocked += 1
        if (
            self.computed_feature_count != computed
            or self.blocked_feature_count != blocked
            or self.resolved_histories_feature_complete is not (blocked == 0)
        ):
            _fail("forward paper technical feature window derived state is invalid")

    def _calculated_id(self) -> str:
        return content_id(
            {
                "schema": FORWARD_PAPER_TECHNICAL_FEATURE_WINDOW_SCHEMA_VERSION,
                "policy_version": FORWARD_PAPER_TECHNICAL_FEATURE_POLICY_VERSION,
                "source_window_id": self.source_window.window_id,
                "config_id": self.config.config_id,
                "result_ids": tuple(value.result_id for value in self.results),
                "computed_feature_count": self.computed_feature_count,
                "blocked_feature_count": self.blocked_feature_count,
                "resolved_histories_feature_complete": (
                    self.resolved_histories_feature_complete
                ),
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        self._validate()
        if self.window_id != self._calculated_id():
            _fail("forward paper technical feature window failed verification")

    @classmethod
    def _from_freshly_verified_derivation(
        cls,
        *,
        source_window: ForwardPaperFeatureInputWindow,
        config: PromotedTechnicalFeatureConfig,
        results: tuple[ForwardPaperTechnicalFeatureResult, ...],
        computed_feature_count: int,
        blocked_feature_count: int,
        resolved_histories_feature_complete: bool,
    ) -> "ForwardPaperTechnicalFeatureWindow":
        value = object.__new__(cls)
        for name, item in (
            ("source_window", source_window),
            ("config", config),
            ("results", results),
            ("computed_feature_count", computed_feature_count),
            ("blocked_feature_count", blocked_feature_count),
            (
                "resolved_histories_feature_complete",
                resolved_histories_feature_complete,
            ),
        ):
            object.__setattr__(value, name, item)
        object.__setattr__(value, "window_id", value._calculated_id())
        return value

    @property
    def collection_only(self) -> bool:
        return True

    @property
    def training_eligible(self) -> bool:
        return False

    @property
    def feature_eligible(self) -> bool:
        return False

    @property
    def label_eligible(self) -> bool:
        return False

    @property
    def ranking_eligible(self) -> bool:
        return False

    @property
    def alert_eligible(self) -> bool:
        return False

    @property
    def paper_trade_eligible(self) -> bool:
        return False

    @property
    def notification_eligible(self) -> bool:
        return False

    @property
    def execution_eligible(self) -> bool:
        return False


def _build_forward_paper_technical_feature_window(
    *,
    source_window: ForwardPaperFeatureInputWindow,
    verify_inputs: bool,
) -> ForwardPaperTechnicalFeatureWindow:
    """Compute descriptive features under the single pinned 60-bar policy."""

    if type(source_window) is not ForwardPaperFeatureInputWindow:
        _fail("forward paper technical feature source window is invalid")
    if verify_inputs:
        verification_failed = False
        try:
            source_window.verify_content_identity()
            FORWARD_PAPER_TECHNICAL_FEATURE_CONFIG.verify_content_identity()
        except Exception:
            verification_failed = True
        if verification_failed:
            _fail("forward paper technical feature input failed verification")
    cutoff = source_window.source_window.source_window.spec.decision_cutoff
    results: list[ForwardPaperTechnicalFeatureResult] = []
    computed = blocked = 0
    for source in source_window.outcomes:
        if type(source) is ForwardPaperFeatureInputVeto:
            results.append(
                ForwardPaperTechnicalFeatureResult._from_freshly_verified_derivation(
                    source_outcome=source,
                    status=ForwardPaperTechnicalFeatureStatus.SOURCE_INPUT_VETO,
                    observed_history_sessions=0,
                    feature_vector=None,
                )
            )
            blocked += 1
            continue
        degenerate = False
        failed = False
        vector = None
        try:
            vector = _compute_vector(
                source,
                FORWARD_PAPER_TECHNICAL_FEATURE_CONFIG,
                cutoff,
            )
        except _DegenerateInput:
            degenerate = True
        except Exception:
            failed = True
        if failed:
            _fail("forward paper technical feature calculation failed")
        if degenerate:
            results.append(
                ForwardPaperTechnicalFeatureResult._from_freshly_verified_derivation(
                    source_outcome=source,
                    status=ForwardPaperTechnicalFeatureStatus.DEGENERATE_INPUT_VETO,
                    observed_history_sessions=len(source.bars),
                    feature_vector=None,
                )
            )
            blocked += 1
            continue
        if type(vector) is not PromotedTechnicalFeatureVector:
            _fail("forward paper technical feature calculation returned invalid output")
        results.append(
            ForwardPaperTechnicalFeatureResult._from_freshly_verified_derivation(
                source_outcome=source,
                status=(
                    ForwardPaperTechnicalFeatureStatus
                    .FEATURE_VECTOR_COMPUTED_COLLECTION_ONLY
                ),
                observed_history_sessions=len(source.bars),
                feature_vector=vector,
            )
        )
        computed += 1
    return ForwardPaperTechnicalFeatureWindow._from_freshly_verified_derivation(
        source_window=source_window,
        config=FORWARD_PAPER_TECHNICAL_FEATURE_CONFIG,
        results=tuple(results),
        computed_feature_count=computed,
        blocked_feature_count=blocked,
        resolved_histories_feature_complete=blocked == 0,
    )


def build_forward_paper_technical_feature_window(
    *, source_window: ForwardPaperFeatureInputWindow
) -> ForwardPaperTechnicalFeatureWindow:
    """Compute features after independently verifying the input window."""

    return _build_forward_paper_technical_feature_window(
        source_window=source_window,
        verify_inputs=True,
    )


def _build_forward_paper_technical_feature_window_from_verified_inputs(
    *, source_window: ForwardPaperFeatureInputWindow
) -> ForwardPaperTechnicalFeatureWindow:
    return _build_forward_paper_technical_feature_window(
        source_window=source_window,
        verify_inputs=False,
    )
