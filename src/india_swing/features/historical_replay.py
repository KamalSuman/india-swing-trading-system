"""Session-keyed historical replay of promoted features and cross-sections."""

from __future__ import annotations

import re
from dataclasses import dataclass, field, fields
from datetime import date, datetime, timezone
from enum import Enum
from typing import Protocol

from india_swing.evaluation.promoted_feature_inputs import (
    VerifiedPromotedFeatureInputPanel,
)
from india_swing.features.promoted_cross_section import (
    PromotedCrossSectionConfig,
    PromotedCrossSectionService,
    VerifiedPromotedCrossSectionPanel,
)
from india_swing.features.promoted_technical import (
    PromotedTechnicalFeatureConfig,
    PromotedTechnicalFeatureService,
    VerifiedPromotedTechnicalFeaturePanel,
)
from india_swing.identity import content_id
from india_swing.reference.models import ReferenceReadiness


class PromotedHistoricalReplayError(ValueError):
    pass


PROMOTED_HISTORICAL_REPLAY_SCHEMA_VERSION = "promoted-historical-replay/v1"
PROMOTED_HISTORICAL_REPLAY_POLICY_VERSION = (
    "promoted-historical-replay/exact-session-create-once-v1"
)

_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_REASON = re.compile(r"[A-Z][A-Z0-9_]{2,127}\Z")
_ERR_TYPE = "promoted historical replay type is invalid"
_ERR_INPUT = "promoted historical replay input is invalid"
_ERR_VERIFY = "promoted historical replay input could not be verified"
_ERR_CUTOFF = "promoted historical replay cutoff is invalid"
_ERR_FUTURE = "promoted historical replay contains future-known evidence"
_ERR_GRAPH = "promoted historical replay graph is invalid"
_ERR_ID = "promoted historical replay identifier is invalid"

_COMMON_REASONS = {
    "COLLECTION_ONLY_NO_DECISION_AUTHORITY",
    "EXACT_SESSION_INPUT_NO_LATEST_SELECTION",
    "REPLAY_OUTPUTS_CREATE_ONCE",
}


def _utc(value: datetime) -> datetime:
    if type(value) is not datetime:
        raise PromotedHistoricalReplayError(_ERR_CUTOFF)
    try:
        offset = value.utcoffset()
    except Exception:
        raise PromotedHistoricalReplayError(_ERR_CUTOFF) from None
    if value.tzinfo is None or offset is None:
        raise PromotedHistoricalReplayError(_ERR_CUTOFF)
    return value.astimezone(timezone.utc)


def _sha(value: object) -> bool:
    return type(value) is str and _SHA256.fullmatch(value) is not None


class PromotedTechnicalFeatureWriter(Protocol):
    def put(
        self,
        value: VerifiedPromotedTechnicalFeaturePanel,
    ) -> VerifiedPromotedTechnicalFeaturePanel: ...


class PromotedCrossSectionWriter(Protocol):
    def put(
        self,
        value: VerifiedPromotedCrossSectionPanel,
    ) -> VerifiedPromotedCrossSectionPanel: ...


@dataclass(frozen=True, slots=True)
class PromotedHistoricalReplayInput:
    market_session: date
    source_panel: VerifiedPromotedFeatureInputPanel
    technical_config: PromotedTechnicalFeatureConfig
    cross_section_config: PromotedCrossSectionConfig
    cutoff: datetime
    input_id: str = field(init=False)

    def __post_init__(self) -> None:
        if (
            type(self.market_session) is not date
            or type(self.source_panel) is not VerifiedPromotedFeatureInputPanel
            or type(self.technical_config) is not PromotedTechnicalFeatureConfig
            or type(self.cross_section_config) is not PromotedCrossSectionConfig
        ):
            raise PromotedHistoricalReplayError(_ERR_INPUT)
        cutoff = _utc(self.cutoff)
        try:
            self.source_panel.verify_content_identity()
            self.technical_config.verify_content_identity()
            self.cross_section_config.verify_content_identity()
        except Exception:
            raise PromotedHistoricalReplayError(_ERR_VERIFY) from None
        if (
            self.source_panel.adjustment_panel.signal_session
            != self.market_session
            or cutoff < max(
                self.source_panel.cutoff,
                self.source_panel.knowledge_time,
            )
        ):
            raise PromotedHistoricalReplayError(_ERR_FUTURE)
        if cutoff != self.cutoff:
            object.__setattr__(self, "cutoff", cutoff)
        object.__setattr__(self, "input_id", self._calculated_id())

    def _identity(self) -> dict[str, object]:
        return {
            "market_session": self.market_session,
            "source_panel_id": self.source_panel.panel_id,
            "technical_config_id": self.technical_config.config_id,
            "cross_section_config_id": self.cross_section_config.config_id,
            "cutoff": self.cutoff,
        }

    def _calculated_id(self) -> str:
        return content_id(
            {
                "schema": "promoted-historical-replay-input/v1",
                **self._identity(),
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        expected = PromotedHistoricalReplayInput(
            market_session=self.market_session,
            source_panel=self.source_panel,
            technical_config=self.technical_config,
            cross_section_config=self.cross_section_config,
            cutoff=self.cutoff,
        )
        if self.input_id != expected.input_id:
            raise PromotedHistoricalReplayError(_ERR_ID)


class PromotedHistoricalReplayStatus(str, Enum):
    SESSION_REPLAYED_RESOLVED_COLLECTION_ONLY = (
        "SESSION_REPLAYED_RESOLVED_COLLECTION_ONLY"
    )
    SESSION_REPLAYED_WITH_BLOCKERS = "SESSION_REPLAYED_WITH_BLOCKERS"
    SESSION_REPLAYED_SOURCE_UNIVERSE_INCOMPLETE = (
        "SESSION_REPLAYED_SOURCE_UNIVERSE_INCOMPLETE"
    )


def _reasons(status: PromotedHistoricalReplayStatus) -> tuple[str, ...]:
    specific = {
        PromotedHistoricalReplayStatus.SESSION_REPLAYED_RESOLVED_COLLECTION_ONLY: {
            "SESSION_RESOLVED_SUBSET_AND_SOURCE_UNIVERSE_COMPLETE",
        },
        PromotedHistoricalReplayStatus.SESSION_REPLAYED_WITH_BLOCKERS: {
            "SESSION_RETAINS_EXPLICIT_BLOCKED_HISTORIES",
        },
        PromotedHistoricalReplayStatus.SESSION_REPLAYED_SOURCE_UNIVERSE_INCOMPLETE: {
            "SESSION_SOURCE_UNIVERSE_INCOMPLETE",
        },
    }[status]
    result = tuple(sorted(_COMMON_REASONS | specific))
    if any(type(value) is not str or _REASON.fullmatch(value) is None for value in result):
        raise PromotedHistoricalReplayError(_ERR_GRAPH)
    return result


@dataclass(frozen=True, slots=True)
class PromotedHistoricalReplayResult:
    market_session: date
    input_id: str
    technical_panel_id: str
    cross_section_panel_id: str
    status: PromotedHistoricalReplayStatus
    technical_blocked_history_count: int
    cross_section_blocked_history_count: int
    unassigned_entry_count: int
    orphan_bar_count: int
    reason_codes: tuple[str, ...]
    result_id: str = field(init=False)

    def __post_init__(self) -> None:
        if (
            type(self.market_session) is not date
            or any(
                not _sha(value)
                for value in (
                    self.input_id,
                    self.technical_panel_id,
                    self.cross_section_panel_id,
                )
            )
            or type(self.status) is not PromotedHistoricalReplayStatus
            or any(
                type(value) is not int or value < 0
                for value in (
                    self.technical_blocked_history_count,
                    self.cross_section_blocked_history_count,
                    self.unassigned_entry_count,
                    self.orphan_bar_count,
                )
            )
            or self.reason_codes != _reasons(self.status)
        ):
            raise PromotedHistoricalReplayError(_ERR_GRAPH)
        if (
            self.status
            is PromotedHistoricalReplayStatus.SESSION_REPLAYED_RESOLVED_COLLECTION_ONLY
            and (
                self.technical_blocked_history_count != 0
                or self.cross_section_blocked_history_count != 0
                or self.unassigned_entry_count != 0
                or self.orphan_bar_count != 0
            )
        ):
            raise PromotedHistoricalReplayError(_ERR_GRAPH)
        if (
            self.status
            is PromotedHistoricalReplayStatus.SESSION_REPLAYED_WITH_BLOCKERS
            and self.technical_blocked_history_count == 0
            and self.cross_section_blocked_history_count == 0
        ):
            raise PromotedHistoricalReplayError(_ERR_GRAPH)
        if (
            self.status
            is PromotedHistoricalReplayStatus.SESSION_REPLAYED_SOURCE_UNIVERSE_INCOMPLETE
            and (
                self.technical_blocked_history_count != 0
                or self.cross_section_blocked_history_count != 0
                or (
                    self.unassigned_entry_count == 0
                    and self.orphan_bar_count == 0
                )
            )
        ):
            raise PromotedHistoricalReplayError(_ERR_GRAPH)
        object.__setattr__(self, "result_id", self._calculated_id())

    def _identity(self) -> dict[str, object]:
        return {
            value.name: getattr(self, value.name)
            for value in fields(self)
            if value.name != "result_id"
        }

    def _calculated_id(self) -> str:
        return content_id(
            {
                "schema": "promoted-historical-replay-result/v1",
                **self._identity(),
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        expected = PromotedHistoricalReplayResult(**self._identity())
        if self.result_id != expected.result_id:
            raise PromotedHistoricalReplayError(_ERR_ID)


@dataclass(frozen=True, slots=True)
class PromotedHistoricalReplayRun:
    schema_version: str
    policy_version: str
    inputs: tuple[PromotedHistoricalReplayInput, ...]
    results: tuple[PromotedHistoricalReplayResult, ...]
    replayed_session_count: int
    blocked_session_count: int
    source_universe_incomplete_session_count: int
    readiness: ReferenceReadiness
    actionable: bool
    training_eligible: bool
    ranking_eligible: bool
    alert_eligible: bool
    execution_eligible: bool
    run_id: str = field(init=False)

    def __post_init__(self) -> None:
        if (
            self.schema_version != PROMOTED_HISTORICAL_REPLAY_SCHEMA_VERSION
            or self.policy_version != PROMOTED_HISTORICAL_REPLAY_POLICY_VERSION
            or type(self.inputs) is not tuple
            or not self.inputs
            or any(
                type(value) is not PromotedHistoricalReplayInput
                for value in self.inputs
            )
            or type(self.results) is not tuple
            or len(self.results) != len(self.inputs)
            or any(
                type(value) is not PromotedHistoricalReplayResult
                for value in self.results
            )
            or any(
                type(value) is not int or value < 0
                for value in (
                    self.replayed_session_count,
                    self.blocked_session_count,
                    self.source_universe_incomplete_session_count,
                )
            )
            or type(self.readiness) is not ReferenceReadiness
            or any(
                type(value) is not bool
                for value in (
                    self.actionable,
                    self.training_eligible,
                    self.ranking_eligible,
                    self.alert_eligible,
                    self.execution_eligible,
                )
            )
        ):
            raise PromotedHistoricalReplayError(_ERR_GRAPH)
        sessions = tuple(value.market_session for value in self.inputs)
        if sessions != tuple(sorted(set(sessions))):
            raise PromotedHistoricalReplayError(_ERR_GRAPH)
        try:
            for value in self.inputs:
                value.verify_content_identity()
            for value in self.results:
                value.verify_content_identity()
        except Exception:
            raise PromotedHistoricalReplayError(_ERR_GRAPH) from None
        if (
            tuple(value.market_session for value in self.results) != sessions
            or tuple(value.input_id for value in self.results)
            != tuple(value.input_id for value in self.inputs)
            or self.replayed_session_count != len(self.results)
            or self.blocked_session_count
            != sum(
                value.status
                is PromotedHistoricalReplayStatus.SESSION_REPLAYED_WITH_BLOCKERS
                for value in self.results
            )
            or self.source_universe_incomplete_session_count
            != sum(
                value.status
                is (
                    PromotedHistoricalReplayStatus
                    .SESSION_REPLAYED_SOURCE_UNIVERSE_INCOMPLETE
                )
                for value in self.results
            )
            or self.readiness is not ReferenceReadiness.COLLECTION_ONLY
            or any(
                value is not False
                for value in (
                    self.actionable,
                    self.training_eligible,
                    self.ranking_eligible,
                    self.alert_eligible,
                    self.execution_eligible,
                )
            )
        ):
            raise PromotedHistoricalReplayError(_ERR_GRAPH)
        object.__setattr__(self, "run_id", self._calculated_id())

    def _identity(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "policy_version": self.policy_version,
            "input_ids": tuple(value.input_id for value in self.inputs),
            "result_ids": tuple(value.result_id for value in self.results),
            "replayed_session_count": self.replayed_session_count,
            "blocked_session_count": self.blocked_session_count,
            "source_universe_incomplete_session_count": (
                self.source_universe_incomplete_session_count
            ),
            "readiness": self.readiness,
            "actionable": self.actionable,
            "training_eligible": self.training_eligible,
            "ranking_eligible": self.ranking_eligible,
            "alert_eligible": self.alert_eligible,
            "execution_eligible": self.execution_eligible,
        }

    def _calculated_id(self) -> str:
        return content_id(
            {
                "schema": PROMOTED_HISTORICAL_REPLAY_SCHEMA_VERSION,
                **self._identity(),
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        expected = PromotedHistoricalReplayRun(
            schema_version=self.schema_version,
            policy_version=self.policy_version,
            inputs=self.inputs,
            results=self.results,
            replayed_session_count=self.replayed_session_count,
            blocked_session_count=self.blocked_session_count,
            source_universe_incomplete_session_count=(
                self.source_universe_incomplete_session_count
            ),
            readiness=self.readiness,
            actionable=self.actionable,
            training_eligible=self.training_eligible,
            ranking_eligible=self.ranking_eligible,
            alert_eligible=self.alert_eligible,
            execution_eligible=self.execution_eligible,
        )
        if self.run_id != expected.run_id:
            raise PromotedHistoricalReplayError(_ERR_ID)


class PromotedHistoricalReplayService:
    """Replays exact session inputs and publishes create-once outputs."""

    def run(
        self,
        *,
        inputs: tuple[PromotedHistoricalReplayInput, ...],
        technical_store: PromotedTechnicalFeatureWriter,
        cross_section_store: PromotedCrossSectionWriter,
    ) -> PromotedHistoricalReplayRun:
        if (
            type(inputs) is not tuple
            or not inputs
            or any(
                type(value) is not PromotedHistoricalReplayInput
                for value in inputs
            )
        ):
            raise PromotedHistoricalReplayError(_ERR_INPUT)
        sessions = tuple(value.market_session for value in inputs)
        if sessions != tuple(sorted(set(sessions))):
            raise PromotedHistoricalReplayError(_ERR_INPUT)
        try:
            for value in inputs:
                value.verify_content_identity()
        except Exception:
            raise PromotedHistoricalReplayError(_ERR_VERIFY) from None

        results: list[PromotedHistoricalReplayResult] = []
        for value in inputs:
            technical = PromotedTechnicalFeatureService().materialize(
                source_panel=value.source_panel,
                config=value.technical_config,
                cutoff=value.cutoff,
            )
            stored_technical = technical_store.put(technical)
            if (
                type(stored_technical)
                is not VerifiedPromotedTechnicalFeaturePanel
                or stored_technical.panel_id != technical.panel_id
            ):
                raise PromotedHistoricalReplayError(_ERR_GRAPH)
            cross_section = PromotedCrossSectionService().materialize(
                source_panel=stored_technical,
                config=value.cross_section_config,
                cutoff=value.cutoff,
            )
            stored_cross_section = cross_section_store.put(cross_section)
            if (
                type(stored_cross_section)
                is not VerifiedPromotedCrossSectionPanel
                or stored_cross_section.panel_id != cross_section.panel_id
            ):
                raise PromotedHistoricalReplayError(_ERR_GRAPH)

            if (
                stored_technical.blocked_history_count > 0
                or stored_cross_section.blocked_history_count > 0
            ):
                status = (
                    PromotedHistoricalReplayStatus
                    .SESSION_REPLAYED_WITH_BLOCKERS
                )
            elif not stored_cross_section.source_universe_cross_section_complete:
                status = (
                    PromotedHistoricalReplayStatus
                    .SESSION_REPLAYED_SOURCE_UNIVERSE_INCOMPLETE
                )
            else:
                status = (
                    PromotedHistoricalReplayStatus
                    .SESSION_REPLAYED_RESOLVED_COLLECTION_ONLY
                )
            results.append(
                PromotedHistoricalReplayResult(
                    market_session=value.market_session,
                    input_id=value.input_id,
                    technical_panel_id=stored_technical.panel_id,
                    cross_section_panel_id=stored_cross_section.panel_id,
                    status=status,
                    technical_blocked_history_count=(
                        stored_technical.blocked_history_count
                    ),
                    cross_section_blocked_history_count=(
                        stored_cross_section.blocked_history_count
                    ),
                    unassigned_entry_count=(
                        stored_cross_section.unassigned_entry_count
                    ),
                    orphan_bar_count=stored_cross_section.orphan_bar_count,
                    reason_codes=_reasons(status),
                )
            )
        result_tuple = tuple(results)
        return PromotedHistoricalReplayRun(
            schema_version=PROMOTED_HISTORICAL_REPLAY_SCHEMA_VERSION,
            policy_version=PROMOTED_HISTORICAL_REPLAY_POLICY_VERSION,
            inputs=inputs,
            results=result_tuple,
            replayed_session_count=len(result_tuple),
            blocked_session_count=sum(
                value.status
                is PromotedHistoricalReplayStatus.SESSION_REPLAYED_WITH_BLOCKERS
                for value in result_tuple
            ),
            source_universe_incomplete_session_count=sum(
                value.status
                is (
                    PromotedHistoricalReplayStatus
                    .SESSION_REPLAYED_SOURCE_UNIVERSE_INCOMPLETE
                )
                for value in result_tuple
            ),
            readiness=ReferenceReadiness.COLLECTION_ONLY,
            actionable=False,
            training_eligible=False,
            ranking_eligible=False,
            alert_eligible=False,
            execution_eligible=False,
        )
