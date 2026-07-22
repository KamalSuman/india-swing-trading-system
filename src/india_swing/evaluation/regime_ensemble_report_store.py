from __future__ import annotations

import json
import os
import re
import stat
import tempfile
from dataclasses import fields
from decimal import Decimal, InvalidOperation
from enum import Enum
from pathlib import Path

from india_swing._filesystem import (
    FileLockUnavailable,
    FileSafetyError,
    advisory_file_lock,
    read_stable_regular_file,
)

from .baseline_store import LocalDeterministicComparisonRunStore
from .baselines import DeterministicComparisonRun
from .engine import EvaluationDataReadiness
from .regime_ensemble_report import (
    REGIME_ENSEMBLE_EVALUATION_REPORT_SCHEMA_VERSION,
    RegimeEnsembleEvaluationReport,
    RegimeEnsembleReportError,
    build_regime_ensemble_evaluation_report,
)
from .regime_ensemble_run import RegimeEnsembleEvaluationRun


REGIME_ENSEMBLE_REPORT_STORE_SCHEMA_VERSION = "local-regime-ensemble-evaluation-report/v1"
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_MAX_REPORT_BYTES = 512 * 1024


class RegimeEnsembleReportStoreConflict(RegimeEnsembleReportError):
    pass


class RegimeEnsembleReportStoreNotFound(RegimeEnsembleReportError):
    pass


def _is_link_like(path: Path) -> bool:
    try:
        status = os.lstat(path)
    except OSError:
        return path.is_symlink()
    return path.is_symlink() or bool(
        getattr(status, "st_file_attributes", 0)
        & getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400)
    )


def _json_value(value: object) -> object:
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, tuple):
        return [_json_value(item) for item in value]
    if value is None or isinstance(value, (str, int, bool)):
        return value
    raise TypeError(f"unsupported regime-ensemble-report value: {type(value).__name__}")


def encode_regime_ensemble_evaluation_report(
    report: RegimeEnsembleEvaluationReport,
) -> bytes:
    if type(report) is not RegimeEnsembleEvaluationReport:
        raise TypeError("report must be exact")
    report.verify_content_identity()
    payload = {
        "store_schema_version": REGIME_ENSEMBLE_REPORT_STORE_SCHEMA_VERSION,
        "report": {
            item.name: _json_value(getattr(report, item.name)) for item in fields(report)
        },
    }
    return (
        json.dumps(
            payload,
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        )
        + "\n"
    ).encode("utf-8")


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise RegimeEnsembleReportStoreConflict("report contains a duplicate JSON key")
        result[key] = value
    return result


def _object(value: object, expected: set[str], name: str) -> dict[str, object]:
    if type(value) is not dict or set(value) != expected:
        raise RegimeEnsembleReportStoreConflict(f"stored {name} has invalid fields")
    return value


def _decimal(value: object, name: str) -> Decimal:
    if type(value) is not str:
        raise RegimeEnsembleReportStoreConflict(f"stored {name} must be a Decimal string")
    try:
        result = Decimal(value)
    except InvalidOperation as exc:
        raise RegimeEnsembleReportStoreConflict(f"stored {name} is invalid") from exc
    if not result.is_finite():
        raise RegimeEnsembleReportStoreConflict(f"stored {name} must be finite")
    return result


def _decode_metrics(value: object, name: str) -> tuple[tuple[str, Decimal], ...]:
    if type(value) is not list:
        raise RegimeEnsembleReportStoreConflict(f"stored {name} must be a list")
    metrics: list[tuple[str, Decimal]] = []
    for item in value:
        if type(item) is not list or len(item) != 2 or type(item[0]) is not str:
            raise RegimeEnsembleReportStoreConflict(f"stored {name} entry is invalid")
        metrics.append((item[0], _decimal(item[1], name)))
    return tuple(metrics)


def decode_regime_ensemble_evaluation_report(
    payload: bytes,
) -> RegimeEnsembleEvaluationReport:
    try:
        raw = json.loads(
            payload.decode("utf-8", errors="strict"),
            object_pairs_hook=_unique_object,
            parse_float=lambda _: (_ for _ in ()).throw(ValueError()),
            parse_constant=lambda _: (_ for _ in ()).throw(ValueError()),
        )
        envelope = _object(raw, {"store_schema_version", "report"}, "report envelope")
        if envelope["store_schema_version"] != REGIME_ENSEMBLE_REPORT_STORE_SCHEMA_VERSION:
            raise RegimeEnsembleReportStoreConflict("unsupported report store schema")
        value = _object(
            envelope["report"],
            {item.name for item in fields(RegimeEnsembleEvaluationReport)},
            "evaluation report",
        )
        stored_id = value["report_id"]
        if type(value["fold_summary_ids"]) is not list:
            raise RegimeEnsembleReportStoreConflict("stored fold_summary_ids must be a list")
        if type(value["caveats"]) is not list:
            raise RegimeEnsembleReportStoreConflict("stored caveats must be a list")
        strategy_stressed = value["strategy_stressed_metrics"]
        benchmark_stressed = value["benchmark_stressed_metrics"]
        report = RegimeEnsembleEvaluationReport(
            trial_id=value["trial_id"],
            evaluation_run_id=value["evaluation_run_id"],
            intent_run_id=value["intent_run_id"],
            deterministic_run_id=value["deterministic_run_id"],
            comparison_id=value["comparison_id"],
            config_id=value["config_id"],
            benchmark_id=value["benchmark_id"],
            split_plan_id=value["split_plan_id"],
            dataset_id=value["dataset_id"],
            dataset_readiness=EvaluationDataReadiness(value["dataset_readiness"]),
            fold_count=value["fold_count"],
            proposal_count=value["proposal_count"],
            strategy_decision_count=value["strategy_decision_count"],
            selected_intent_count=value["selected_intent_count"],
            strategy_trade_count=value["strategy_trade_count"],
            benchmark_trade_count=value["benchmark_trade_count"],
            primary_metric=value["primary_metric"],
            strategy_base_metrics=_decode_metrics(
                value["strategy_base_metrics"], "strategy_base_metrics"
            ),
            benchmark_base_metrics=_decode_metrics(
                value["benchmark_base_metrics"], "benchmark_base_metrics"
            ),
            comparison_metrics=_decode_metrics(
                value["comparison_metrics"], "comparison_metrics"
            ),
            strategy_stressed_metrics=(
                None
                if strategy_stressed is None
                else _decode_metrics(strategy_stressed, "strategy_stressed_metrics")
            ),
            benchmark_stressed_metrics=(
                None
                if benchmark_stressed is None
                else _decode_metrics(benchmark_stressed, "benchmark_stressed_metrics")
            ),
            fold_summary_ids=tuple(value["fold_summary_ids"]),
            passed=value["passed"],
            outperformed=value["outperformed"],
            caveats=tuple(value["caveats"]),
            schema_version=value["schema_version"],
        )
        if report.report_id != stored_id:
            raise RegimeEnsembleReportStoreConflict("stored report ID differs from content")
        return report
    except RegimeEnsembleReportStoreConflict:
        raise
    except (
        InvalidOperation,
        KeyError,
        TypeError,
        ValueError,
        UnicodeDecodeError,
        json.JSONDecodeError,
    ) as exc:
        raise RegimeEnsembleReportStoreConflict("stored evaluation report is invalid") from exc


class LocalRegimeEnsembleEvaluationReportStore:
    """One create-once compact evaluation report per trial.

    Constructed around the exact existing LocalDeterministicComparisonRunStore
    sharing its evidence root: publishing persists the deterministic run
    first (through the approved store) and only then, terminal-last,
    publishes the derived report. Reading independently reconstructs the
    deterministic run from disk and cross-checks every report field derivable
    from it. There is no list/latest selection API.
    """

    def __init__(self, deterministic_run_store: LocalDeterministicComparisonRunStore) -> None:
        if type(deterministic_run_store) is not LocalDeterministicComparisonRunStore:
            raise TypeError("deterministic_run_store must be exact")
        self.deterministic_run_store = deterministic_run_store
        self.root = deterministic_run_store.root

    @property
    def reports_root(self) -> Path:
        return self.root / "regime_ensemble_reports"

    def _path(self, trial_id: str) -> Path:
        if _SHA256.fullmatch(trial_id) is None:
            raise RegimeEnsembleReportError("trial_id must be a full lowercase SHA-256")
        return self.reports_root / f"{trial_id}.json"

    def publish(
        self, evaluation_run: RegimeEnsembleEvaluationRun
    ) -> RegimeEnsembleEvaluationReport:
        if type(evaluation_run) is not RegimeEnsembleEvaluationRun:
            raise TypeError("evaluation_run must be exact")
        evaluation_run.verify_content_identity()
        trial_id = evaluation_run.registration.trial_id

        persisted_comparison_run = self.deterministic_run_store.publish(
            evaluation_run.comparison_run
        )
        if persisted_comparison_run != evaluation_run.comparison_run:
            raise RegimeEnsembleReportStoreConflict(
                "persisted deterministic run differs from the evaluation run"
            )

        report = build_regime_ensemble_evaluation_report(evaluation_run)
        self.reports_root.mkdir(parents=True, exist_ok=True)
        if _is_link_like(self.reports_root):
            raise RegimeEnsembleReportStoreConflict("report root cannot be a link")
        target = self._path(trial_id)
        payload = encode_regime_ensemble_evaluation_report(report)
        try:
            with advisory_file_lock(self.reports_root / ".regime-ensemble-reports.lock"):
                if target.exists():
                    stored = self.get(trial_id)
                    if stored != report:
                        raise RegimeEnsembleReportStoreConflict(
                            "trial already stores a different evaluation report"
                        )
                    return stored
                descriptor, temporary_name = tempfile.mkstemp(
                    prefix=".regime-ensemble-report-", suffix=".tmp", dir=self.reports_root
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
        except (FileLockUnavailable, FileSafetyError) as exc:
            raise RegimeEnsembleReportStoreConflict(
                "evaluation report store unavailable"
            ) from exc
        return self.require_persisted(evaluation_run)

    def get(self, trial_id: str) -> RegimeEnsembleEvaluationReport:
        deterministic_run = self.deterministic_run_store.get(trial_id)
        path = self._path(trial_id)
        if not path.exists():
            raise RegimeEnsembleReportStoreNotFound(trial_id)
        if not path.is_file() or _is_link_like(path):
            raise RegimeEnsembleReportStoreConflict("evaluation report must be a regular file")
        try:
            payload = read_stable_regular_file(path, maximum_bytes=_MAX_REPORT_BYTES)
        except FileSafetyError as exc:
            raise RegimeEnsembleReportStoreConflict(
                "evaluation report could not be read safely"
            ) from exc
        report = decode_regime_ensemble_evaluation_report(payload)
        self._validate_against_deterministic_run(trial_id, report, deterministic_run)
        return report

    @staticmethod
    def _validate_against_deterministic_run(
        trial_id: str,
        report: RegimeEnsembleEvaluationReport,
        deterministic_run: DeterministicComparisonRun,
    ) -> None:
        comparison = deterministic_run.comparison
        expected_stressed_strategy = (
            None
            if comparison.strategy_stressed is None
            else comparison.strategy_stressed.metrics
        )
        expected_stressed_benchmark = (
            None
            if comparison.benchmark_stressed is None
            else comparison.benchmark_stressed.metrics
        )
        if (
            report.trial_id != trial_id
            or report.deterministic_run_id != deterministic_run.run_id
            or report.comparison_id != comparison.comparison_id
            or report.config_id != deterministic_run.strategy_batch.generator_id
            or report.benchmark_id != deterministic_run.benchmark_batch.generator_id
            or report.split_plan_id != deterministic_run.strategy_batch.split_plan_id
            or report.dataset_id != comparison.strategy_base.dataset_id
            or report.fold_count != len(deterministic_run.fold_summaries)
            or report.strategy_decision_count != len(deterministic_run.strategy_batch.decisions)
            or report.selected_intent_count != len(deterministic_run.strategy_batch.intents)
            or report.strategy_trade_count != len(comparison.strategy_base.trades)
            or report.benchmark_trade_count != len(comparison.benchmark_base.trades)
            or report.primary_metric != comparison.primary_metric
            or report.strategy_base_metrics != comparison.strategy_base.metrics
            or report.benchmark_base_metrics != comparison.benchmark_base.metrics
            or report.comparison_metrics != comparison.comparison_metrics
            or report.strategy_stressed_metrics != expected_stressed_strategy
            or report.benchmark_stressed_metrics != expected_stressed_benchmark
            or report.fold_summary_ids
            != tuple(item.summary_id for item in deterministic_run.fold_summaries)
            or report.passed != comparison.passed
            or report.outperformed
            != all(item.outperformed for item in deterministic_run.fold_summaries)
        ):
            raise RegimeEnsembleReportStoreConflict(
                "stored evaluation report differs from its persisted deterministic run"
            )

    def require_persisted(
        self, evaluation_run: RegimeEnsembleEvaluationRun
    ) -> RegimeEnsembleEvaluationReport:
        if type(evaluation_run) is not RegimeEnsembleEvaluationRun:
            raise TypeError("evaluation_run must be exact")
        evaluation_run.verify_content_identity()
        expected = build_regime_ensemble_evaluation_report(evaluation_run)
        stored = self.get(evaluation_run.registration.trial_id)
        if stored != expected:
            raise RegimeEnsembleReportStoreConflict(
                "persisted evaluation report differs from the evaluation run"
            )
        return stored
