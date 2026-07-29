"""Durable exact-ID evidence for promoted experiment readiness audits."""

from __future__ import annotations

import json
import os
import re
import stat
import tempfile
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path

from india_swing._filesystem import (
    FileLockUnavailable,
    FileSafetyError,
    advisory_file_lock,
    read_stable_regular_file,
)
from india_swing.evaluation.dataset_assembly import (
    AssembledEvaluationDataset,
)
from india_swing.evaluation.engine import EvaluationDataReadiness
from india_swing.evaluation.models import (
    EVALUATION_SPLIT_SCHEMA_VERSION,
    PurgedWalkForwardPlan,
    SplitMethod,
    WalkForwardFold,
)
from india_swing.evaluation.promoted_experiment_assembly import (
    PROMOTED_EXPERIMENT_READINESS_POLICY_VERSION,
    PROMOTED_EXPERIMENT_READINESS_SCHEMA_VERSION,
    ExactCrossSectionResolver,
    PromotedExperimentAssemblyError,
    PromotedExperimentReadinessConfig,
    PromotedExperimentReadinessIssue,
    PromotedExperimentReadinessIssueCode,
    PromotedExperimentReadinessReport,
    PromotedExperimentReadinessService,
    render_promoted_experiment_readiness,
)
from india_swing.evaluation.promoted_walk_forward import (
    PromotedFoldCrossSectionBinding,
)
from india_swing.features.historical_replay import (
    PROMOTED_HISTORICAL_REPLAY_POLICY_VERSION,
    PROMOTED_HISTORICAL_REPLAY_SCHEMA_VERSION,
    PromotedHistoricalReplayRun,
    PromotedHistoricalReplayStatus,
    reconstruct_promoted_historical_replay_run,
)
from india_swing.identity import content_id
from india_swing.reference.models import ReferenceReadiness


PROMOTED_EXPERIMENT_EVIDENCE_STORE_SCHEMA_VERSION = (
    "local-promoted-experiment-readiness-evidence/v1"
)
PROMOTED_REPLAY_PROJECTION_SCHEMA_VERSION = (
    "promoted-historical-replay-projection/v1"
)
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_MAXIMUM_EVIDENCE_BYTES = 16 * 1024 * 1024


class PromotedExperimentEvidenceStoreError(
    PromotedExperimentAssemblyError
):
    pass


class PromotedExperimentEvidenceConflict(
    PromotedExperimentEvidenceStoreError
):
    pass


class PromotedExperimentEvidenceNotFound(
    PromotedExperimentEvidenceStoreError
):
    pass


def _sha(value: object) -> bool:
    return type(value) is str and _SHA256.fullmatch(value) is not None


def _is_link_like(path: Path) -> bool:
    try:
        status = os.lstat(path)
    except OSError:
        return path.is_symlink()
    return path.is_symlink() or bool(
        getattr(status, "st_file_attributes", 0)
        & getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400)
    )


def _unique_object(
    pairs: list[tuple[str, object]],
) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise PromotedExperimentEvidenceConflict(
                "stored readiness evidence contains duplicate JSON keys"
            )
        result[key] = value
    return result


def _reject_number(_: str) -> object:
    raise PromotedExperimentEvidenceConflict(
        "stored readiness evidence contains a forbidden number"
    )


def _object(
    value: object,
    expected: set[str],
) -> dict[str, object]:
    if type(value) is not dict or set(value) != expected:
        raise PromotedExperimentEvidenceConflict(
            "stored readiness evidence has invalid fields"
        )
    return value


def _tuple(value: object) -> tuple[object, ...]:
    if type(value) is not list:
        raise PromotedExperimentEvidenceConflict(
            "stored readiness evidence has an invalid sequence"
        )
    return tuple(value)


def _date(value: object) -> date:
    if type(value) is not str:
        raise PromotedExperimentEvidenceConflict(
            "stored readiness evidence has an invalid date"
        )
    try:
        parsed = date.fromisoformat(value)
    except ValueError:
        raise PromotedExperimentEvidenceConflict(
            "stored readiness evidence has an invalid date"
        ) from None
    if parsed.isoformat() != value:
        raise PromotedExperimentEvidenceConflict(
            "stored readiness evidence has an invalid date"
        )
    return parsed


def _dates(value: object) -> tuple[date, ...]:
    return tuple(_date(item) for item in _tuple(value))


@dataclass(frozen=True, slots=True)
class PromotedHistoricalReplayProjection:
    run_id: str
    input_ids: tuple[str, ...]
    result_ids: tuple[str, ...]
    market_sessions: tuple[date, ...]
    technical_panel_ids: tuple[str, ...]
    cross_section_panel_ids: tuple[str, ...]
    statuses: tuple[PromotedHistoricalReplayStatus, ...]
    replayed_session_count: int
    blocked_session_count: int
    source_universe_incomplete_session_count: int
    schema_version: str = PROMOTED_REPLAY_PROJECTION_SCHEMA_VERSION
    projection_id: str = field(init=False)

    def __post_init__(self) -> None:
        sequences = (
            self.input_ids,
            self.result_ids,
            self.market_sessions,
            self.technical_panel_ids,
            self.cross_section_panel_ids,
            self.statuses,
        )
        if (
            self.schema_version
            != PROMOTED_REPLAY_PROJECTION_SCHEMA_VERSION
            or not _sha(self.run_id)
            or any(type(value) is not tuple for value in sequences)
            or not self.market_sessions
            or any(len(value) != len(self.market_sessions) for value in sequences)
            or self.market_sessions
            != tuple(sorted(set(self.market_sessions)))
            or any(type(value) is not date for value in self.market_sessions)
            or any(
                not _sha(value)
                for values in (
                    self.input_ids,
                    self.result_ids,
                    self.technical_panel_ids,
                    self.cross_section_panel_ids,
                )
                for value in values
            )
            or any(
                type(value) is not PromotedHistoricalReplayStatus
                for value in self.statuses
            )
            or any(
                type(value) is not int or value < 0
                for value in (
                    self.replayed_session_count,
                    self.blocked_session_count,
                    self.source_universe_incomplete_session_count,
                )
            )
            or self.replayed_session_count != len(self.market_sessions)
            or self.blocked_session_count
            != sum(
                value
                is PromotedHistoricalReplayStatus
                .SESSION_REPLAYED_WITH_BLOCKERS
                for value in self.statuses
            )
            or self.source_universe_incomplete_session_count
            != sum(
                value
                is PromotedHistoricalReplayStatus
                .SESSION_REPLAYED_SOURCE_UNIVERSE_INCOMPLETE
                for value in self.statuses
            )
            or self.run_id != self._calculated_run_id()
        ):
            raise PromotedExperimentEvidenceStoreError(
                "promoted replay projection graph is invalid"
            )
        object.__setattr__(
            self,
            "projection_id",
            self._calculated_projection_id(),
        )

    @classmethod
    def from_run(
        cls,
        run: PromotedHistoricalReplayRun,
    ) -> PromotedHistoricalReplayProjection:
        if type(run) is not PromotedHistoricalReplayRun:
            raise TypeError("promoted replay run must be exact")
        try:
            run.verify_content_identity()
        except Exception:
            raise PromotedExperimentEvidenceStoreError(
                "promoted replay run verification failed"
            ) from None
        return cls(
            run_id=run.run_id,
            input_ids=tuple(value.input_id for value in run.inputs),
            result_ids=tuple(value.result_id for value in run.results),
            market_sessions=tuple(
                value.market_session for value in run.results
            ),
            technical_panel_ids=tuple(
                value.technical_panel_id for value in run.results
            ),
            cross_section_panel_ids=tuple(
                value.cross_section_panel_id for value in run.results
            ),
            statuses=tuple(value.status for value in run.results),
            replayed_session_count=run.replayed_session_count,
            blocked_session_count=run.blocked_session_count,
            source_universe_incomplete_session_count=(
                run.source_universe_incomplete_session_count
            ),
        )

    def _calculated_run_id(self) -> str:
        return content_id(
            {
                "schema": PROMOTED_HISTORICAL_REPLAY_SCHEMA_VERSION,
                "schema_version": (
                    PROMOTED_HISTORICAL_REPLAY_SCHEMA_VERSION
                ),
                "policy_version": (
                    PROMOTED_HISTORICAL_REPLAY_POLICY_VERSION
                ),
                "input_ids": self.input_ids,
                "result_ids": self.result_ids,
                "replayed_session_count": self.replayed_session_count,
                "blocked_session_count": self.blocked_session_count,
                "source_universe_incomplete_session_count": (
                    self.source_universe_incomplete_session_count
                ),
                "readiness": ReferenceReadiness.COLLECTION_ONLY,
                "actionable": False,
                "training_eligible": False,
                "ranking_eligible": False,
                "alert_eligible": False,
                "execution_eligible": False,
            },
            length=64,
        )

    def _calculated_projection_id(self) -> str:
        return content_id(
            {
                "schema": PROMOTED_REPLAY_PROJECTION_SCHEMA_VERSION,
                "run_id": self.run_id,
                "input_ids": self.input_ids,
                "result_ids": self.result_ids,
                "market_sessions": self.market_sessions,
                "technical_panel_ids": self.technical_panel_ids,
                "cross_section_panel_ids": self.cross_section_panel_ids,
                "statuses": self.statuses,
                "replayed_session_count": self.replayed_session_count,
                "blocked_session_count": self.blocked_session_count,
                "source_universe_incomplete_session_count": (
                    self.source_universe_incomplete_session_count
                ),
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        if (
            self.run_id != self._calculated_run_id()
            or self.projection_id != self._calculated_projection_id()
        ):
            raise PromotedExperimentEvidenceStoreError(
                "promoted replay projection identity failed"
            )

    def reconstruct(
        self,
        cross_section_resolver: ExactCrossSectionResolver,
    ) -> PromotedHistoricalReplayRun:
        if not callable(getattr(cross_section_resolver, "get", None)):
            raise PromotedExperimentEvidenceStoreError(
                "promoted replay resolver is invalid"
            )
        try:
            panels = tuple(
                cross_section_resolver.get(panel_id)
                for panel_id in self.cross_section_panel_ids
            )
            run = reconstruct_promoted_historical_replay_run(panels)
            projection = PromotedHistoricalReplayProjection.from_run(
                run
            )
        except Exception:
            raise PromotedExperimentEvidenceStoreError(
                "promoted replay reconstruction failed"
            ) from None
        if projection != self:
            raise PromotedExperimentEvidenceStoreError(
                "promoted replay reconstruction differs"
            )
        return run


@dataclass(frozen=True, slots=True)
class PromotedExperimentReadinessEvidence:
    config: PromotedExperimentReadinessConfig
    split_plan: PurgedWalkForwardPlan
    dataset_assembly_id: str
    dataset_id: str
    replay_projections: tuple[
        PromotedHistoricalReplayProjection,
        ...,
    ]
    report: PromotedExperimentReadinessReport
    schema_version: str = (
        PROMOTED_EXPERIMENT_EVIDENCE_STORE_SCHEMA_VERSION
    )
    evidence_id: str = field(init=False)

    def __post_init__(self) -> None:
        if (
            self.schema_version
            != PROMOTED_EXPERIMENT_EVIDENCE_STORE_SCHEMA_VERSION
            or type(self.config) is not PromotedExperimentReadinessConfig
            or type(self.split_plan) is not PurgedWalkForwardPlan
            or not _sha(self.dataset_assembly_id)
            or not _sha(self.dataset_id)
            or type(self.replay_projections) is not tuple
            or not self.replay_projections
            or any(
                type(value) is not PromotedHistoricalReplayProjection
                for value in self.replay_projections
            )
            or tuple(value.run_id for value in self.replay_projections)
            != tuple(
                sorted(
                    {
                        value.run_id
                        for value in self.replay_projections
                    }
                )
            )
            or type(self.report) is not PromotedExperimentReadinessReport
            or self.report.config_id != self.config.config_id
            or self.report.split_plan_id != self.split_plan.plan_id
            or self.report.dataset_id != self.dataset_id
            or self.report.replay_run_ids
            != tuple(value.run_id for value in self.replay_projections)
            or self.report.total_session_count
            != len(self.split_plan.ordered_sessions)
            or self.report.fold_count != len(self.split_plan.folds)
        ):
            raise PromotedExperimentEvidenceStoreError(
                "promoted readiness evidence graph is invalid"
            )
        try:
            self.config.verify_content_identity()
            self.split_plan.verify_content_identity()
            self.report.verify_content_identity()
            for value in self.replay_projections:
                value.verify_content_identity()
        except Exception:
            raise PromotedExperimentEvidenceStoreError(
                "promoted readiness evidence verification failed"
            ) from None
        object.__setattr__(self, "evidence_id", self._calculated_id())

    def _calculated_id(self) -> str:
        return content_id(
            {
                "schema": (
                    PROMOTED_EXPERIMENT_EVIDENCE_STORE_SCHEMA_VERSION
                ),
                "config_id": self.config.config_id,
                "split_plan_id": self.split_plan.plan_id,
                "dataset_assembly_id": self.dataset_assembly_id,
                "dataset_id": self.dataset_id,
                "replay_projection_ids": tuple(
                    value.projection_id
                    for value in self.replay_projections
                ),
                "report_id": self.report.report_id,
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        self.config.verify_content_identity()
        self.split_plan.verify_content_identity()
        self.report.verify_content_identity()
        for value in self.replay_projections:
            value.verify_content_identity()
        if self.evidence_id != self._calculated_id():
            raise PromotedExperimentEvidenceStoreError(
                "promoted readiness evidence identity failed"
            )


def _fold_value(value: WalkForwardFold) -> dict[str, object]:
    return {
        "fold_id": value.fold_id,
        "training_sessions": tuple(
            session.isoformat() for session in value.training_sessions
        ),
        "validation_sessions": tuple(
            session.isoformat() for session in value.validation_sessions
        ),
        "test_sessions": tuple(
            session.isoformat() for session in value.test_sessions
        ),
    }


def _plan_value(value: PurgedWalkForwardPlan) -> dict[str, object]:
    return {
        "calendar_version": value.calendar_version,
        "ordered_sessions": tuple(
            session.isoformat() for session in value.ordered_sessions
        ),
        "label_horizon_sessions": value.label_horizon_sessions,
        "embargo_sessions": value.embargo_sessions,
        "folds": tuple(_fold_value(fold) for fold in value.folds),
        "split_method": value.split_method.value,
        "schema_version": value.schema_version,
        "plan_id": value.plan_id,
    }


def _config_value(
    value: PromotedExperimentReadinessConfig,
) -> dict[str, object]:
    return {
        "minimum_total_sessions": value.minimum_total_sessions,
        "minimum_fold_count": value.minimum_fold_count,
        "minimum_test_sessions_per_fold": (
            value.minimum_test_sessions_per_fold
        ),
        "minimum_instrument_count": value.minimum_instrument_count,
        "minimum_session_eligible_instruments": (
            value.minimum_session_eligible_instruments
        ),
        "required_dataset_readiness": (
            value.required_dataset_readiness.value
        ),
        "schema_version": value.schema_version,
        "policy_version": value.policy_version,
        "config_id": value.config_id,
    }


def _projection_value(
    value: PromotedHistoricalReplayProjection,
) -> dict[str, object]:
    return {
        "run_id": value.run_id,
        "input_ids": value.input_ids,
        "result_ids": value.result_ids,
        "market_sessions": tuple(
            session.isoformat() for session in value.market_sessions
        ),
        "technical_panel_ids": value.technical_panel_ids,
        "cross_section_panel_ids": value.cross_section_panel_ids,
        "statuses": tuple(status.value for status in value.statuses),
        "replayed_session_count": value.replayed_session_count,
        "blocked_session_count": value.blocked_session_count,
        "source_universe_incomplete_session_count": (
            value.source_universe_incomplete_session_count
        ),
        "schema_version": value.schema_version,
        "projection_id": value.projection_id,
    }


def _issue_value(
    value: PromotedExperimentReadinessIssue,
) -> dict[str, object]:
    return {
        "code": value.code.value,
        "affected_sessions": tuple(
            session.isoformat() for session in value.affected_sessions
        ),
        "observed_count": value.observed_count,
        "required_count": value.required_count,
        "issue_id": value.issue_id,
    }


def _binding_value(
    value: PromotedFoldCrossSectionBinding,
) -> dict[str, object]:
    return {
        "fold_id": value.fold_id,
        "signal_session": value.signal_session.isoformat(),
        "cross_section_panel_id": value.cross_section_panel_id,
        "binding_id": value.binding_id,
    }


def _report_value(
    value: PromotedExperimentReadinessReport,
) -> dict[str, object]:
    return {
        "schema_version": value.schema_version,
        "policy_version": value.policy_version,
        "config_id": value.config_id,
        "split_plan_id": value.split_plan_id,
        "dataset_id": value.dataset_id,
        "replay_run_ids": value.replay_run_ids,
        "total_session_count": value.total_session_count,
        "fold_count": value.fold_count,
        "instrument_count": value.instrument_count,
        "minimum_observed_session_universe": (
            value.minimum_observed_session_universe
        ),
        "issues": tuple(_issue_value(item) for item in value.issues),
        "bindings": tuple(
            _binding_value(item) for item in value.bindings
        ),
        "ready": value.ready,
        "evaluation_eligible": value.evaluation_eligible,
        "actionable": value.actionable,
        "alert_eligible": value.alert_eligible,
        "execution_eligible": value.execution_eligible,
        "report_id": value.report_id,
    }


def encode_promoted_experiment_readiness_evidence(
    value: PromotedExperimentReadinessEvidence,
) -> bytes:
    if type(value) is not PromotedExperimentReadinessEvidence:
        raise TypeError("promoted readiness evidence must be exact")
    value.verify_content_identity()
    payload = {
        "store_schema_version": (
            PROMOTED_EXPERIMENT_EVIDENCE_STORE_SCHEMA_VERSION
        ),
        "evidence": {
            "config": _config_value(value.config),
            "split_plan": _plan_value(value.split_plan),
            "dataset_assembly_id": value.dataset_assembly_id,
            "dataset_id": value.dataset_id,
            "replay_projections": tuple(
                _projection_value(item)
                for item in value.replay_projections
            ),
            "report": _report_value(value.report),
            "schema_version": value.schema_version,
            "evidence_id": value.evidence_id,
        },
    }
    return (
        json.dumps(
            payload,
            separators=(",", ":"),
            sort_keys=True,
            ensure_ascii=True,
            allow_nan=False,
        )
        + "\n"
    ).encode("utf-8")


def _decode_fold(value: object) -> WalkForwardFold:
    raw = _object(
        value,
        {
            "fold_id",
            "training_sessions",
            "validation_sessions",
            "test_sessions",
        },
    )
    fold = WalkForwardFold(
        training_sessions=_dates(raw["training_sessions"]),
        validation_sessions=_dates(raw["validation_sessions"]),
        test_sessions=_dates(raw["test_sessions"]),
    )
    if raw["fold_id"] != fold.fold_id:
        raise PromotedExperimentEvidenceConflict(
            "stored readiness fold identity differs"
        )
    return fold


def _decode_plan(value: object) -> PurgedWalkForwardPlan:
    raw = _object(
        value,
        {
            "calendar_version",
            "ordered_sessions",
            "label_horizon_sessions",
            "embargo_sessions",
            "folds",
            "split_method",
            "schema_version",
            "plan_id",
        },
    )
    try:
        plan = PurgedWalkForwardPlan(
            calendar_version=raw["calendar_version"],
            ordered_sessions=_dates(raw["ordered_sessions"]),
            label_horizon_sessions=raw["label_horizon_sessions"],
            embargo_sessions=raw["embargo_sessions"],
            folds=tuple(_decode_fold(item) for item in _tuple(raw["folds"])),
            split_method=SplitMethod(raw["split_method"]),
            schema_version=raw["schema_version"],
        )
    except PromotedExperimentEvidenceConflict:
        raise
    except Exception:
        raise PromotedExperimentEvidenceConflict(
            "stored readiness split plan is invalid"
        ) from None
    if raw["plan_id"] != plan.plan_id:
        raise PromotedExperimentEvidenceConflict(
            "stored readiness split-plan identity differs"
        )
    return plan


def _decode_config(
    value: object,
) -> PromotedExperimentReadinessConfig:
    raw = _object(
        value,
        {
            "minimum_total_sessions",
            "minimum_fold_count",
            "minimum_test_sessions_per_fold",
            "minimum_instrument_count",
            "minimum_session_eligible_instruments",
            "required_dataset_readiness",
            "schema_version",
            "policy_version",
            "config_id",
        },
    )
    try:
        config = PromotedExperimentReadinessConfig(
            minimum_total_sessions=raw["minimum_total_sessions"],
            minimum_fold_count=raw["minimum_fold_count"],
            minimum_test_sessions_per_fold=(
                raw["minimum_test_sessions_per_fold"]
            ),
            minimum_instrument_count=raw["minimum_instrument_count"],
            minimum_session_eligible_instruments=(
                raw["minimum_session_eligible_instruments"]
            ),
            required_dataset_readiness=EvaluationDataReadiness(
                raw["required_dataset_readiness"]
            ),
            schema_version=raw["schema_version"],
            policy_version=raw["policy_version"],
        )
    except Exception:
        raise PromotedExperimentEvidenceConflict(
            "stored readiness configuration is invalid"
        ) from None
    if raw["config_id"] != config.config_id:
        raise PromotedExperimentEvidenceConflict(
            "stored readiness configuration identity differs"
        )
    return config


def _decode_projection(
    value: object,
) -> PromotedHistoricalReplayProjection:
    raw = _object(
        value,
        {
            "run_id",
            "input_ids",
            "result_ids",
            "market_sessions",
            "technical_panel_ids",
            "cross_section_panel_ids",
            "statuses",
            "replayed_session_count",
            "blocked_session_count",
            "source_universe_incomplete_session_count",
            "schema_version",
            "projection_id",
        },
    )
    try:
        projection = PromotedHistoricalReplayProjection(
            run_id=raw["run_id"],
            input_ids=_tuple(raw["input_ids"]),
            result_ids=_tuple(raw["result_ids"]),
            market_sessions=_dates(raw["market_sessions"]),
            technical_panel_ids=_tuple(raw["technical_panel_ids"]),
            cross_section_panel_ids=_tuple(
                raw["cross_section_panel_ids"]
            ),
            statuses=tuple(
                PromotedHistoricalReplayStatus(item)
                for item in _tuple(raw["statuses"])
            ),
            replayed_session_count=raw["replayed_session_count"],
            blocked_session_count=raw["blocked_session_count"],
            source_universe_incomplete_session_count=(
                raw["source_universe_incomplete_session_count"]
            ),
            schema_version=raw["schema_version"],
        )
    except Exception:
        raise PromotedExperimentEvidenceConflict(
            "stored replay projection is invalid"
        ) from None
    if raw["projection_id"] != projection.projection_id:
        raise PromotedExperimentEvidenceConflict(
            "stored replay projection identity differs"
        )
    return projection


def _decode_issue(
    value: object,
) -> PromotedExperimentReadinessIssue:
    raw = _object(
        value,
        {
            "code",
            "affected_sessions",
            "observed_count",
            "required_count",
            "issue_id",
        },
    )
    try:
        issue = PromotedExperimentReadinessIssue(
            code=PromotedExperimentReadinessIssueCode(raw["code"]),
            affected_sessions=_dates(raw["affected_sessions"]),
            observed_count=raw["observed_count"],
            required_count=raw["required_count"],
        )
    except Exception:
        raise PromotedExperimentEvidenceConflict(
            "stored readiness issue is invalid"
        ) from None
    if raw["issue_id"] != issue.issue_id:
        raise PromotedExperimentEvidenceConflict(
            "stored readiness issue identity differs"
        )
    return issue


def _decode_binding(
    value: object,
) -> PromotedFoldCrossSectionBinding:
    raw = _object(
        value,
        {
            "fold_id",
            "signal_session",
            "cross_section_panel_id",
            "binding_id",
        },
    )
    try:
        binding = PromotedFoldCrossSectionBinding(
            fold_id=raw["fold_id"],
            signal_session=_date(raw["signal_session"]),
            cross_section_panel_id=raw["cross_section_panel_id"],
        )
    except Exception:
        raise PromotedExperimentEvidenceConflict(
            "stored readiness binding is invalid"
        ) from None
    if raw["binding_id"] != binding.binding_id:
        raise PromotedExperimentEvidenceConflict(
            "stored readiness binding identity differs"
        )
    return binding


def _decode_report(
    value: object,
) -> PromotedExperimentReadinessReport:
    raw = _object(
        value,
        {
            "schema_version",
            "policy_version",
            "config_id",
            "split_plan_id",
            "dataset_id",
            "replay_run_ids",
            "total_session_count",
            "fold_count",
            "instrument_count",
            "minimum_observed_session_universe",
            "issues",
            "bindings",
            "ready",
            "evaluation_eligible",
            "actionable",
            "alert_eligible",
            "execution_eligible",
            "report_id",
        },
    )
    try:
        report = PromotedExperimentReadinessReport(
            schema_version=raw["schema_version"],
            policy_version=raw["policy_version"],
            config_id=raw["config_id"],
            split_plan_id=raw["split_plan_id"],
            dataset_id=raw["dataset_id"],
            replay_run_ids=_tuple(raw["replay_run_ids"]),
            total_session_count=raw["total_session_count"],
            fold_count=raw["fold_count"],
            instrument_count=raw["instrument_count"],
            minimum_observed_session_universe=(
                raw["minimum_observed_session_universe"]
            ),
            issues=tuple(
                _decode_issue(item) for item in _tuple(raw["issues"])
            ),
            bindings=tuple(
                _decode_binding(item)
                for item in _tuple(raw["bindings"])
            ),
            ready=raw["ready"],
            evaluation_eligible=raw["evaluation_eligible"],
            actionable=raw["actionable"],
            alert_eligible=raw["alert_eligible"],
            execution_eligible=raw["execution_eligible"],
        )
    except PromotedExperimentEvidenceConflict:
        raise
    except Exception:
        raise PromotedExperimentEvidenceConflict(
            "stored readiness report is invalid"
        ) from None
    if raw["report_id"] != report.report_id:
        raise PromotedExperimentEvidenceConflict(
            "stored readiness report identity differs"
        )
    return report


def decode_promoted_experiment_readiness_evidence(
    payload: bytes,
) -> PromotedExperimentReadinessEvidence:
    if (
        type(payload) is not bytes
        or not payload
        or len(payload) > _MAXIMUM_EVIDENCE_BYTES
    ):
        raise PromotedExperimentEvidenceConflict(
            "stored readiness evidence bytes are invalid"
        )
    try:
        decoded = json.loads(
            payload.decode("utf-8"),
            object_pairs_hook=_unique_object,
            parse_float=_reject_number,
            parse_constant=_reject_number,
        )
        root = _object(decoded, {"store_schema_version", "evidence"})
        if (
            root["store_schema_version"]
            != PROMOTED_EXPERIMENT_EVIDENCE_STORE_SCHEMA_VERSION
        ):
            raise PromotedExperimentEvidenceConflict(
                "stored readiness evidence schema is unsupported"
            )
        raw = _object(
            root["evidence"],
            {
                "config",
                "split_plan",
                "dataset_assembly_id",
                "dataset_id",
                "replay_projections",
                "report",
                "schema_version",
                "evidence_id",
            },
        )
        evidence = PromotedExperimentReadinessEvidence(
            config=_decode_config(raw["config"]),
            split_plan=_decode_plan(raw["split_plan"]),
            dataset_assembly_id=raw["dataset_assembly_id"],
            dataset_id=raw["dataset_id"],
            replay_projections=tuple(
                _decode_projection(item)
                for item in _tuple(raw["replay_projections"])
            ),
            report=_decode_report(raw["report"]),
            schema_version=raw["schema_version"],
        )
    except PromotedExperimentEvidenceConflict:
        raise
    except Exception:
        raise PromotedExperimentEvidenceConflict(
            "stored readiness evidence is invalid"
        ) from None
    if (
        raw["evidence_id"] != evidence.evidence_id
        or encode_promoted_experiment_readiness_evidence(evidence)
        != payload
    ):
        raise PromotedExperimentEvidenceConflict(
            "stored readiness evidence is not canonical"
        )
    return evidence


def render_promoted_experiment_readiness_evidence(
    evidence: PromotedExperimentReadinessEvidence,
) -> str:
    if type(evidence) is not PromotedExperimentReadinessEvidence:
        raise TypeError("promoted readiness evidence must be exact")
    evidence.verify_content_identity()
    return "\n".join(
        (
            "# Persisted promoted readiness evidence",
            "",
            f"- Evidence ID: `{evidence.evidence_id}`",
            (
                "- Dataset assembly ID: "
                f"`{evidence.dataset_assembly_id}`"
            ),
            f"- Replay runs: {len(evidence.replay_projections)}",
            "",
            render_promoted_experiment_readiness(evidence.report),
        )
    )


class LocalPromotedExperimentReadinessEvidenceStore:
    """Publishes immutable readiness evidence after rerunning the audit."""

    def __init__(self, root: Path) -> None:
        self.root = Path(root)

    @property
    def evidence_root(self) -> Path:
        return self.root / "promoted-experiment-readiness"

    def path_for(self, evidence_id: str) -> Path:
        if not _sha(evidence_id):
            raise PromotedExperimentEvidenceStoreError(
                "promoted readiness evidence ID is invalid"
            )
        return self.evidence_root / f"{evidence_id}.json"

    def publish(
        self,
        *,
        config: PromotedExperimentReadinessConfig,
        split_plan: PurgedWalkForwardPlan,
        assembled_dataset: AssembledEvaluationDataset,
        replay_runs: tuple[PromotedHistoricalReplayRun, ...],
        cross_section_resolver: ExactCrossSectionResolver,
    ) -> PromotedExperimentReadinessEvidence:
        if type(assembled_dataset) is not AssembledEvaluationDataset:
            raise TypeError("assembled evaluation dataset must be exact")
        try:
            assembled_dataset.verify_content_identity()
            report = PromotedExperimentReadinessService().audit(
                config=config,
                split_plan=split_plan,
                dataset=assembled_dataset.dataset,
                instruments=assembled_dataset.instruments,
                replay_runs=replay_runs,
                cross_section_resolver=cross_section_resolver,
            )
            projections = tuple(
                sorted(
                    (
                        PromotedHistoricalReplayProjection.from_run(run)
                        for run in replay_runs
                    ),
                    key=lambda value: value.run_id,
                )
            )
            evidence = PromotedExperimentReadinessEvidence(
                config=config,
                split_plan=split_plan,
                dataset_assembly_id=assembled_dataset.assembly_id,
                dataset_id=assembled_dataset.dataset.dataset_id,
                replay_projections=projections,
                report=report,
            )
        except PromotedExperimentAssemblyError:
            raise
        except Exception:
            raise PromotedExperimentEvidenceStoreError(
                "promoted readiness evidence publication failed"
            ) from None
        return self._put(evidence)

    def reaudit(
        self,
        *,
        evidence_id: str,
        assembled_dataset: AssembledEvaluationDataset,
        cross_section_resolver: ExactCrossSectionResolver,
    ) -> PromotedExperimentReadinessEvidence:
        if type(assembled_dataset) is not AssembledEvaluationDataset:
            raise TypeError("assembled evaluation dataset must be exact")
        evidence = self.get(evidence_id)
        try:
            assembled_dataset.verify_content_identity()
            if (
                assembled_dataset.assembly_id
                != evidence.dataset_assembly_id
                or assembled_dataset.dataset.dataset_id
                != evidence.dataset_id
            ):
                raise PromotedExperimentEvidenceStoreError(
                    "promoted readiness dataset lineage differs"
                )
            replay_runs = tuple(
                projection.reconstruct(cross_section_resolver)
                for projection in evidence.replay_projections
            )
            report = PromotedExperimentReadinessService().audit(
                config=evidence.config,
                split_plan=evidence.split_plan,
                dataset=assembled_dataset.dataset,
                instruments=assembled_dataset.instruments,
                replay_runs=replay_runs,
                cross_section_resolver=cross_section_resolver,
            )
        except PromotedExperimentEvidenceStoreError:
            raise
        except Exception:
            raise PromotedExperimentEvidenceStoreError(
                "promoted readiness source reaudit failed"
            ) from None
        if report != evidence.report:
            raise PromotedExperimentEvidenceConflict(
                "promoted readiness source reaudit differs"
            )
        return evidence

    def _put(
        self,
        evidence: PromotedExperimentReadinessEvidence,
    ) -> PromotedExperimentReadinessEvidence:
        evidence.verify_content_identity()
        payload = encode_promoted_experiment_readiness_evidence(evidence)
        self.evidence_root.mkdir(parents=True, exist_ok=True)
        if _is_link_like(self.evidence_root):
            raise PromotedExperimentEvidenceConflict(
                "promoted readiness evidence root cannot be a link"
            )
        target = self.path_for(evidence.evidence_id)
        try:
            with advisory_file_lock(
                self.evidence_root / ".readiness-evidence.lock"
            ):
                if target.exists():
                    stored = self.get(evidence.evidence_id)
                    if stored != evidence:
                        raise PromotedExperimentEvidenceConflict(
                            "promoted readiness evidence ID is occupied"
                        )
                    return stored
                descriptor, temporary_name = tempfile.mkstemp(
                    prefix=".readiness-",
                    suffix=".tmp",
                    dir=self.evidence_root,
                )
                temporary = Path(temporary_name)
                try:
                    with os.fdopen(descriptor, "wb") as handle:
                        handle.write(payload)
                        handle.flush()
                        os.fsync(handle.fileno())
                    os.link(temporary, target)
                finally:
                    temporary.unlink(missing_ok=True)
        except PromotedExperimentEvidenceConflict:
            raise
        except (FileLockUnavailable, FileSafetyError, OSError):
            raise PromotedExperimentEvidenceConflict(
                "promoted readiness evidence store is unavailable"
            ) from None
        return self.get(evidence.evidence_id)

    def get(
        self,
        evidence_id: str,
    ) -> PromotedExperimentReadinessEvidence:
        path = self.path_for(evidence_id)
        if not path.exists():
            raise PromotedExperimentEvidenceNotFound(
                "promoted readiness evidence was not found"
            )
        try:
            payload = read_stable_regular_file(
                path,
                maximum_bytes=_MAXIMUM_EVIDENCE_BYTES,
            )
            evidence = decode_promoted_experiment_readiness_evidence(
                payload
            )
        except PromotedExperimentEvidenceConflict:
            raise
        except FileSafetyError:
            raise PromotedExperimentEvidenceConflict(
                "promoted readiness evidence could not be read safely"
            ) from None
        if evidence.evidence_id != evidence_id:
            raise PromotedExperimentEvidenceConflict(
                "promoted readiness evidence differs from its path"
            )
        return evidence
