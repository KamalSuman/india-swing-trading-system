"""Fail-closed readiness audit for promoted historical experiments."""

from __future__ import annotations

import re
from dataclasses import dataclass, field, fields
from datetime import date
from enum import Enum
from typing import Protocol

from india_swing.evaluation.baselines import PointInTimeInstrument
from india_swing.evaluation.engine import (
    EvaluationDataReadiness,
    EvaluationDataset,
)
from india_swing.evaluation.models import PurgedWalkForwardPlan
from india_swing.evaluation.promoted_walk_forward import (
    PromotedFoldCrossSectionBinding,
)
from india_swing.features.historical_replay import (
    PromotedHistoricalReplayRun,
    PromotedHistoricalReplayStatus,
)
from india_swing.features.promoted_cross_section import (
    VerifiedPromotedCrossSectionPanel,
)
from india_swing.identity import content_id


class PromotedExperimentAssemblyError(ValueError):
    pass


PROMOTED_EXPERIMENT_READINESS_SCHEMA_VERSION = (
    "promoted-experiment-readiness/v1"
)
PROMOTED_EXPERIMENT_READINESS_POLICY_VERSION = (
    "promoted-experiment-readiness/point-in-time-broad-universe-v1"
)
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")


class ExactCrossSectionResolver(Protocol):
    def get(
        self,
        panel_id: str,
    ) -> VerifiedPromotedCrossSectionPanel: ...


@dataclass(frozen=True, slots=True)
class PromotedExperimentReadinessConfig:
    minimum_total_sessions: int = 756
    minimum_fold_count: int = 6
    minimum_test_sessions_per_fold: int = 20
    minimum_instrument_count: int = 500
    minimum_session_eligible_instruments: int = 500
    required_dataset_readiness: EvaluationDataReadiness = (
        EvaluationDataReadiness.POINT_IN_TIME_VERIFIED
    )
    schema_version: str = PROMOTED_EXPERIMENT_READINESS_SCHEMA_VERSION
    policy_version: str = PROMOTED_EXPERIMENT_READINESS_POLICY_VERSION
    config_id: str = field(init=False)

    def __post_init__(self) -> None:
        integer_names = (
            "minimum_total_sessions",
            "minimum_fold_count",
            "minimum_test_sessions_per_fold",
            "minimum_instrument_count",
            "minimum_session_eligible_instruments",
        )
        if (
            any(
                type(getattr(self, name)) is not int
                or getattr(self, name) <= 0
                for name in integer_names
            )
            or type(self.required_dataset_readiness)
            is not EvaluationDataReadiness
            or self.required_dataset_readiness
            not in {
                EvaluationDataReadiness.POINT_IN_TIME_VERIFIED,
                EvaluationDataReadiness.SYNTHETIC,
            }
            or self.schema_version
            != PROMOTED_EXPERIMENT_READINESS_SCHEMA_VERSION
            or self.policy_version
            != PROMOTED_EXPERIMENT_READINESS_POLICY_VERSION
        ):
            raise PromotedExperimentAssemblyError(
                "promoted experiment readiness configuration is invalid"
            )
        object.__setattr__(self, "config_id", self._calculated_id())

    def _identity(self) -> dict[str, object]:
        return {
            value.name: getattr(self, value.name)
            for value in fields(self)
            if value.name != "config_id"
        }

    def _calculated_id(self) -> str:
        return content_id(
            {
                "schema": PROMOTED_EXPERIMENT_READINESS_SCHEMA_VERSION,
                **self._identity(),
                "fold_signal_policy": "FIRST_TEST_SESSION_ONLY",
                "entry_session_policy": "SECOND_TEST_SESSION_ONLY",
                "cross_section_selection": "EXACT_ID_NO_LATEST",
                "partial_experiment_policy": "DISALLOWED",
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        expected = PromotedExperimentReadinessConfig(
            **self._identity()
        )
        if self.config_id != expected.config_id:
            raise PromotedExperimentAssemblyError(
                "promoted readiness config identity failed"
            )


class PromotedExperimentReadinessIssueCode(str, Enum):
    DATASET_READINESS_MISMATCH = "DATASET_READINESS_MISMATCH"
    DATASET_CALENDAR_MISMATCH = "DATASET_CALENDAR_MISMATCH"
    INSUFFICIENT_TOTAL_SESSIONS = "INSUFFICIENT_TOTAL_SESSIONS"
    INSUFFICIENT_FOLD_COUNT = "INSUFFICIENT_FOLD_COUNT"
    TEST_WINDOW_TOO_SHORT = "TEST_WINDOW_TOO_SHORT"
    INSUFFICIENT_INSTRUMENT_COUNT = "INSUFFICIENT_INSTRUMENT_COUNT"
    SESSION_UNIVERSE_TOO_SMALL = "SESSION_UNIVERSE_TOO_SMALL"
    INSTRUMENT_UNIVERSE_BINDING_INVALID = (
        "INSTRUMENT_UNIVERSE_BINDING_INVALID"
    )
    MISSING_FOLD_REPLAY = "MISSING_FOLD_REPLAY"
    DUPLICATE_FOLD_REPLAY = "DUPLICATE_FOLD_REPLAY"
    REPLAY_WITH_BLOCKERS = "REPLAY_WITH_BLOCKERS"
    REPLAY_SOURCE_UNIVERSE_INCOMPLETE = (
        "REPLAY_SOURCE_UNIVERSE_INCOMPLETE"
    )
    CROSS_SECTION_UNAVAILABLE = "CROSS_SECTION_UNAVAILABLE"
    CROSS_SECTION_BINDING_INVALID = "CROSS_SECTION_BINDING_INVALID"
    CROSS_SECTION_WITH_BLOCKERS = "CROSS_SECTION_WITH_BLOCKERS"
    CROSS_SECTION_SOURCE_UNIVERSE_INCOMPLETE = (
        "CROSS_SECTION_SOURCE_UNIVERSE_INCOMPLETE"
    )


@dataclass(frozen=True, slots=True)
class PromotedExperimentReadinessIssue:
    code: PromotedExperimentReadinessIssueCode
    affected_sessions: tuple[date, ...]
    observed_count: int | None
    required_count: int | None
    issue_id: str = field(init=False)

    def __post_init__(self) -> None:
        if (
            type(self.code) is not PromotedExperimentReadinessIssueCode
            or type(self.affected_sessions) is not tuple
            or self.affected_sessions
            != tuple(sorted(set(self.affected_sessions)))
            or any(
                type(value) is not date
                for value in self.affected_sessions
            )
            or (
                self.observed_count is not None
                and (
                    type(self.observed_count) is not int
                    or self.observed_count < 0
                )
            )
            or (
                self.required_count is not None
                and (
                    type(self.required_count) is not int
                    or self.required_count <= 0
                )
            )
            or (
                (self.observed_count is None)
                != (self.required_count is None)
            )
        ):
            raise PromotedExperimentAssemblyError(
                "promoted readiness issue is invalid"
            )
        object.__setattr__(
            self,
            "issue_id",
            content_id(
                {
                    "schema": "promoted-readiness-issue/v1",
                    "code": self.code,
                    "affected_sessions": self.affected_sessions,
                    "observed_count": self.observed_count,
                    "required_count": self.required_count,
                },
                length=64,
            ),
        )

    def verify_content_identity(self) -> None:
        expected = PromotedExperimentReadinessIssue(
            code=self.code,
            affected_sessions=self.affected_sessions,
            observed_count=self.observed_count,
            required_count=self.required_count,
        )
        if self.issue_id != expected.issue_id:
            raise PromotedExperimentAssemblyError(
                "promoted readiness issue identity failed"
            )


@dataclass(frozen=True, slots=True)
class PromotedExperimentReadinessReport:
    schema_version: str
    policy_version: str
    config_id: str
    split_plan_id: str
    dataset_id: str
    replay_run_ids: tuple[str, ...]
    total_session_count: int
    fold_count: int
    instrument_count: int
    minimum_observed_session_universe: int
    issues: tuple[PromotedExperimentReadinessIssue, ...]
    bindings: tuple[PromotedFoldCrossSectionBinding, ...]
    ready: bool
    evaluation_eligible: bool
    actionable: bool
    alert_eligible: bool
    execution_eligible: bool
    report_id: str = field(init=False)

    def __post_init__(self) -> None:
        if (
            self.schema_version
            != PROMOTED_EXPERIMENT_READINESS_SCHEMA_VERSION
            or self.policy_version
            != PROMOTED_EXPERIMENT_READINESS_POLICY_VERSION
            or any(
                type(value) is not str
                or _SHA256.fullmatch(value) is None
                for value in (
                    self.config_id,
                    self.split_plan_id,
                    self.dataset_id,
                )
            )
            or type(self.replay_run_ids) is not tuple
            or not self.replay_run_ids
            or self.replay_run_ids
            != tuple(sorted(set(self.replay_run_ids)))
            or any(
                type(value) is not str
                or _SHA256.fullmatch(value) is None
                for value in self.replay_run_ids
            )
            or any(
                type(value) is not int or value < 0
                for value in (
                    self.total_session_count,
                    self.fold_count,
                    self.instrument_count,
                    self.minimum_observed_session_universe,
                )
            )
            or type(self.issues) is not tuple
            or any(
                type(value) is not PromotedExperimentReadinessIssue
                for value in self.issues
            )
            or tuple(value.issue_id for value in self.issues)
            != tuple(sorted({value.issue_id for value in self.issues}))
            or type(self.bindings) is not tuple
            or any(
                type(value) is not PromotedFoldCrossSectionBinding
                for value in self.bindings
            )
            or len({value.binding_id for value in self.bindings})
            != len(self.bindings)
            or len({value.fold_id for value in self.bindings})
            != len(self.bindings)
            or len({value.signal_session for value in self.bindings})
            != len(self.bindings)
            or type(self.ready) is not bool
            or type(self.evaluation_eligible) is not bool
            or self.evaluation_eligible != self.ready
            or self.ready != (not self.issues)
            or (self.ready and len(self.bindings) != self.fold_count)
            or (not self.ready and self.bindings)
            or self.actionable is not False
            or self.alert_eligible is not False
            or self.execution_eligible is not False
        ):
            raise PromotedExperimentAssemblyError(
                "promoted readiness report graph is invalid"
            )
        for value in self.issues:
            value.verify_content_identity()
        for value in self.bindings:
            value.verify_content_identity()
        object.__setattr__(self, "report_id", self._calculated_id())

    def _calculated_id(self) -> str:
        return content_id(
            {
                "schema": PROMOTED_EXPERIMENT_READINESS_SCHEMA_VERSION,
                "policy_version": self.policy_version,
                "config_id": self.config_id,
                "split_plan_id": self.split_plan_id,
                "dataset_id": self.dataset_id,
                "replay_run_ids": self.replay_run_ids,
                "total_session_count": self.total_session_count,
                "fold_count": self.fold_count,
                "instrument_count": self.instrument_count,
                "minimum_observed_session_universe": (
                    self.minimum_observed_session_universe
                ),
                "issue_ids": tuple(
                    value.issue_id for value in self.issues
                ),
                "binding_ids": tuple(
                    value.binding_id for value in self.bindings
                ),
                "ready": self.ready,
                "evaluation_eligible": self.evaluation_eligible,
                "actionable": self.actionable,
                "alert_eligible": self.alert_eligible,
                "execution_eligible": self.execution_eligible,
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        if self.report_id != self._calculated_id():
            raise PromotedExperimentAssemblyError(
                "promoted readiness report identity failed"
            )
        for value in self.issues:
            value.verify_content_identity()
        for value in self.bindings:
            value.verify_content_identity()


def _count_issue(
    code: PromotedExperimentReadinessIssueCode,
    *,
    observed: int,
    required: int,
    sessions: tuple[date, ...] = (),
) -> PromotedExperimentReadinessIssue:
    return PromotedExperimentReadinessIssue(
        code=code,
        affected_sessions=sessions,
        observed_count=observed,
        required_count=required,
    )


def _plain_issue(
    code: PromotedExperimentReadinessIssueCode,
    *sessions: date,
) -> PromotedExperimentReadinessIssue:
    return PromotedExperimentReadinessIssue(
        code=code,
        affected_sessions=tuple(sorted(set(sessions))),
        observed_count=None,
        required_count=None,
    )


def _session_universe_counts(
    sessions: tuple[date, ...],
    instruments: tuple[PointInTimeInstrument, ...],
) -> tuple[tuple[date, int], ...]:
    return tuple(
        (
            session,
            sum(
                session in instrument.eligible_sessions
                for instrument in instruments
            ),
        )
        for session in sessions
    )


class PromotedExperimentReadinessService:
    """Audits exact historical evidence; it never starts an experiment."""

    def audit(
        self,
        *,
        config: PromotedExperimentReadinessConfig,
        split_plan: PurgedWalkForwardPlan,
        dataset: EvaluationDataset,
        instruments: tuple[PointInTimeInstrument, ...],
        replay_runs: tuple[PromotedHistoricalReplayRun, ...],
        cross_section_resolver: ExactCrossSectionResolver,
    ) -> PromotedExperimentReadinessReport:
        if (
            type(config) is not PromotedExperimentReadinessConfig
            or type(split_plan) is not PurgedWalkForwardPlan
            or type(dataset) is not EvaluationDataset
            or type(instruments) is not tuple
            or not instruments
            or any(
                type(value) is not PointInTimeInstrument
                for value in instruments
            )
            or instruments
            != tuple(sorted(instruments, key=lambda value: value.symbol))
            or type(replay_runs) is not tuple
            or not replay_runs
            or any(
                type(value) is not PromotedHistoricalReplayRun
                for value in replay_runs
            )
            or not callable(getattr(cross_section_resolver, "get", None))
        ):
            raise PromotedExperimentAssemblyError(
                "promoted readiness audit input is invalid"
            )
        try:
            config.verify_content_identity()
            split_plan.verify_content_identity()
            dataset.verify_content_identity()
            for value in instruments:
                value.verify_content_identity()
            for value in replay_runs:
                value.verify_content_identity()
        except Exception:
            raise PromotedExperimentAssemblyError(
                "promoted readiness source verification failed"
            ) from None

        issues: list[PromotedExperimentReadinessIssue] = []
        if dataset.readiness is not config.required_dataset_readiness:
            issues.append(
                _plain_issue(
                    PromotedExperimentReadinessIssueCode
                    .DATASET_READINESS_MISMATCH
                )
            )
        if dataset.sessions != split_plan.ordered_sessions:
            issues.append(
                _plain_issue(
                    PromotedExperimentReadinessIssueCode
                    .DATASET_CALENDAR_MISMATCH
                )
            )
        if len(dataset.sessions) < config.minimum_total_sessions:
            issues.append(
                _count_issue(
                    PromotedExperimentReadinessIssueCode
                    .INSUFFICIENT_TOTAL_SESSIONS,
                    observed=len(dataset.sessions),
                    required=config.minimum_total_sessions,
                )
            )
        if len(split_plan.folds) < config.minimum_fold_count:
            issues.append(
                _count_issue(
                    PromotedExperimentReadinessIssueCode
                    .INSUFFICIENT_FOLD_COUNT,
                    observed=len(split_plan.folds),
                    required=config.minimum_fold_count,
                )
            )
        short_folds = tuple(
            fold.test_sessions[0]
            for fold in split_plan.folds
            if len(fold.test_sessions)
            < config.minimum_test_sessions_per_fold
        )
        if short_folds:
            minimum_observed = min(
                len(fold.test_sessions) for fold in split_plan.folds
            )
            issues.append(
                _count_issue(
                    PromotedExperimentReadinessIssueCode
                    .TEST_WINDOW_TOO_SHORT,
                    observed=minimum_observed,
                    required=config.minimum_test_sessions_per_fold,
                    sessions=short_folds,
                )
            )
        if len(instruments) < config.minimum_instrument_count:
            issues.append(
                _count_issue(
                    PromotedExperimentReadinessIssueCode
                    .INSUFFICIENT_INSTRUMENT_COUNT,
                    observed=len(instruments),
                    required=config.minimum_instrument_count,
                )
            )

        session_counts = _session_universe_counts(
            dataset.sessions,
            instruments,
        )
        minimum_session_universe = min(
            count for _, count in session_counts
        )
        small_sessions = tuple(
            session
            for session, count in session_counts
            if count < config.minimum_session_eligible_instruments
        )
        if small_sessions:
            issues.append(
                _count_issue(
                    PromotedExperimentReadinessIssueCode
                    .SESSION_UNIVERSE_TOO_SMALL,
                    observed=minimum_session_universe,
                    required=(
                        config.minimum_session_eligible_instruments
                    ),
                    sessions=small_sessions,
                )
            )

        dataset_universe_ids = set(dataset.universe_snapshot_ids)
        observed_universe_ids: set[str] = set()
        session_universe_ids: dict[date, set[str]] = {
            session: set() for session in dataset.sessions
        }
        invalid_instrument_sessions: set[date] = set()
        for instrument in instruments:
            bindings = dict(instrument.eligibility_bindings)
            for session in instrument.eligible_sessions:
                snapshot_id = (
                    bindings.get(session)
                    if bindings
                    else instrument.universe_snapshot_id
                )
                if type(snapshot_id) is str:
                    observed_universe_ids.add(snapshot_id)
                    if session in session_universe_ids:
                        session_universe_ids[session].add(snapshot_id)
                if (
                    session not in dataset.sessions
                    or snapshot_id not in dataset_universe_ids
                    or (
                        config.required_dataset_readiness
                        is EvaluationDataReadiness.POINT_IN_TIME_VERIFIED
                        and not bindings
                    )
                ):
                    invalid_instrument_sessions.add(session)
        invalid_instrument_sessions.update(
            session
            for session, snapshot_ids in session_universe_ids.items()
            if len(snapshot_ids) != 1
        )
        if observed_universe_ids != dataset_universe_ids:
            invalid_instrument_sessions.update(dataset.sessions)
        if invalid_instrument_sessions:
            issues.append(
                _plain_issue(
                    PromotedExperimentReadinessIssueCode
                    .INSTRUMENT_UNIVERSE_BINDING_INVALID,
                    *invalid_instrument_sessions,
                )
            )

        replay_results_by_session: dict[date, list[object]] = {}
        for run in replay_runs:
            for result in run.results:
                replay_results_by_session.setdefault(
                    result.market_session,
                    [],
                ).append(result)

        candidate_bindings: list[
            PromotedFoldCrossSectionBinding
        ] = []
        for fold in split_plan.folds:
            signal_session = fold.test_sessions[0]
            matches = replay_results_by_session.get(signal_session, [])
            if not matches:
                issues.append(
                    _plain_issue(
                        PromotedExperimentReadinessIssueCode
                        .MISSING_FOLD_REPLAY,
                        signal_session,
                    )
                )
                continue
            if len(matches) != 1:
                issues.append(
                    _count_issue(
                        PromotedExperimentReadinessIssueCode
                        .DUPLICATE_FOLD_REPLAY,
                        observed=len(matches),
                        required=1,
                        sessions=(signal_session,),
                    )
                )
                continue
            replay_result = matches[0]
            if (
                replay_result.status
                is PromotedHistoricalReplayStatus
                .SESSION_REPLAYED_WITH_BLOCKERS
            ):
                issues.append(
                    _plain_issue(
                        PromotedExperimentReadinessIssueCode
                        .REPLAY_WITH_BLOCKERS,
                        signal_session,
                    )
                )
            elif (
                replay_result.status
                is PromotedHistoricalReplayStatus
                .SESSION_REPLAYED_SOURCE_UNIVERSE_INCOMPLETE
            ):
                issues.append(
                    _plain_issue(
                        PromotedExperimentReadinessIssueCode
                        .REPLAY_SOURCE_UNIVERSE_INCOMPLETE,
                        signal_session,
                    )
                )
            try:
                panel = cross_section_resolver.get(
                    replay_result.cross_section_panel_id
                )
            except Exception:
                issues.append(
                    _plain_issue(
                        PromotedExperimentReadinessIssueCode
                        .CROSS_SECTION_UNAVAILABLE,
                        signal_session,
                    )
                )
                continue
            try:
                panel_universe_ids = {
                    (
                        result.source_result.source_result
                        .source_adjustment_result.identity_bindings[-1]
                        .identity_snapshot_id
                    )
                    for result in panel.results
                }
                valid_panel = (
                    type(panel) is VerifiedPromotedCrossSectionPanel
                    and panel.panel_id
                    == replay_result.cross_section_panel_id
                    and panel.source_panel.panel_id
                    == replay_result.technical_panel_id
                    and panel.source_panel.blocked_history_count
                    == replay_result.technical_blocked_history_count
                    and panel.blocked_history_count
                    == replay_result.cross_section_blocked_history_count
                    and panel.unassigned_entry_count
                    == replay_result.unassigned_entry_count
                    and panel.orphan_bar_count
                    == replay_result.orphan_bar_count
                    and (
                        panel.source_panel.source_panel.adjustment_panel
                        .signal_session
                    )
                    == signal_session
                    and panel_universe_ids
                    == session_universe_ids.get(signal_session, set())
                )
                if valid_panel:
                    panel.verify_content_identity()
            except Exception:
                valid_panel = False
            if not valid_panel:
                issues.append(
                    _plain_issue(
                        PromotedExperimentReadinessIssueCode
                        .CROSS_SECTION_BINDING_INVALID,
                        signal_session,
                    )
                )
                continue
            if panel.blocked_history_count > 0:
                issues.append(
                    _plain_issue(
                        PromotedExperimentReadinessIssueCode
                        .CROSS_SECTION_WITH_BLOCKERS,
                        signal_session,
                    )
                )
            if not panel.source_universe_cross_section_complete:
                issues.append(
                    _plain_issue(
                        PromotedExperimentReadinessIssueCode
                        .CROSS_SECTION_SOURCE_UNIVERSE_INCOMPLETE,
                        signal_session,
                    )
                )
            if (
                replay_result.status
                is PromotedHistoricalReplayStatus
                .SESSION_REPLAYED_RESOLVED_COLLECTION_ONLY
                and panel.blocked_history_count == 0
                and panel.source_universe_cross_section_complete
            ):
                candidate_bindings.append(
                    PromotedFoldCrossSectionBinding(
                        fold_id=fold.fold_id,
                        signal_session=signal_session,
                        cross_section_panel_id=panel.panel_id,
                    )
                )

        ordered_issues = tuple(
            sorted(
                {
                    value.issue_id: value for value in issues
                }.values(),
                key=lambda value: value.issue_id,
            )
        )
        ready = not ordered_issues
        bindings = tuple(candidate_bindings) if ready else ()
        replay_ids = tuple(
            sorted({value.run_id for value in replay_runs})
        )
        return PromotedExperimentReadinessReport(
            schema_version=PROMOTED_EXPERIMENT_READINESS_SCHEMA_VERSION,
            policy_version=PROMOTED_EXPERIMENT_READINESS_POLICY_VERSION,
            config_id=config.config_id,
            split_plan_id=split_plan.plan_id,
            dataset_id=dataset.dataset_id,
            replay_run_ids=replay_ids,
            total_session_count=len(dataset.sessions),
            fold_count=len(split_plan.folds),
            instrument_count=len(instruments),
            minimum_observed_session_universe=(
                minimum_session_universe
            ),
            issues=ordered_issues,
            bindings=bindings,
            ready=ready,
            evaluation_eligible=ready,
            actionable=False,
            alert_eligible=False,
            execution_eligible=False,
        )


def render_promoted_experiment_readiness(
    report: PromotedExperimentReadinessReport,
) -> str:
    if type(report) is not PromotedExperimentReadinessReport:
        raise TypeError("report must be exact")
    report.verify_content_identity()
    lines = [
        "# Promoted experiment readiness",
        "",
        f"- Report ID: `{report.report_id}`",
        f"- Split plan ID: `{report.split_plan_id}`",
        f"- Dataset ID: `{report.dataset_id}`",
        f"- Sessions: {report.total_session_count}",
        f"- Folds: {report.fold_count}",
        f"- Instruments: {report.instrument_count}",
        (
            "- Minimum eligible instruments in any session: "
            f"{report.minimum_observed_session_universe}"
        ),
        f"- Offline evaluation ready: `{'YES' if report.ready else 'NO'}`",
        "",
        "## Blocking evidence",
        "",
    ]
    if not report.issues:
        lines.append("No blocking evidence.")
    else:
        lines.extend(
            (
                "| Code | Affected sessions | Observed | Required |",
                "|---|---:|---:|---:|",
            )
        )
        for issue in report.issues:
            if not issue.affected_sessions:
                session_text = "n/a"
            elif len(issue.affected_sessions) == 1:
                session_text = issue.affected_sessions[0].isoformat()
            else:
                session_text = (
                    f"{len(issue.affected_sessions)} "
                    f"({issue.affected_sessions[0].isoformat()} to "
                    f"{issue.affected_sessions[-1].isoformat()})"
                )
            lines.append(
                "| "
                + " | ".join(
                    (
                        issue.code.value,
                        session_text,
                        (
                            "n/a"
                            if issue.observed_count is None
                            else str(issue.observed_count)
                        ),
                        (
                            "n/a"
                            if issue.required_count is None
                            else str(issue.required_count)
                        ),
                    )
                )
                + " |"
            )
    lines.extend(
        (
            "",
            "## Authority boundary",
            "",
            (
                "Readiness authorizes only the offline preregistered "
                "experiment. It never authorizes alerts, broker access, "
                "orders, deployment, or real capital."
            ),
            "",
        )
    )
    return "\n".join(lines)
