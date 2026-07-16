from __future__ import annotations

import json
import os
import re
import stat
import tempfile
from decimal import Decimal, InvalidOperation
from pathlib import Path

from india_swing._filesystem import (
    FileLockUnavailable,
    FileSafetyError,
    advisory_file_lock,
    read_stable_regular_file,
)

from .engine import (
    TrialEvaluationComparisonResult,
    TrialEvaluationError,
)
from .result_store import LocalTrialEvaluationResultStore
from .trial_store import LocalTrialRegistry


TRIAL_EVALUATION_COMPARISON_STORE_SCHEMA_VERSION = "local-trial-comparison/v1"
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_MAX_BYTES = 1024 * 1024


class TrialEvaluationComparisonConflict(TrialEvaluationError):
    pass


class TrialEvaluationComparisonNotFound(TrialEvaluationError):
    pass


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise TrialEvaluationComparisonConflict(
                "comparison contains a duplicate JSON key"
            )
        result[key] = value
    return result


def _is_link_like(path: Path) -> bool:
    try:
        status = os.lstat(path)
    except OSError:
        return path.is_symlink()
    return path.is_symlink() or bool(
        getattr(status, "st_file_attributes", 0)
        & getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400)
    )


class LocalTrialEvaluationComparisonStore:
    """Create-once comparison references backed by full scenario-result artifacts."""

    def __init__(
        self,
        root: Path,
        registry: LocalTrialRegistry,
        result_store: LocalTrialEvaluationResultStore,
    ) -> None:
        self.root = Path(root)
        if type(registry) is not LocalTrialRegistry:
            raise TypeError("registry must be exact")
        if type(result_store) is not LocalTrialEvaluationResultStore:
            raise TypeError("result_store must be exact")
        self.registry = registry
        self.result_store = result_store

    @property
    def comparisons_root(self) -> Path:
        return self.root / "comparisons"

    def _path(self, trial_id: str, comparison_id: str) -> Path:
        if _SHA256.fullmatch(trial_id) is None or _SHA256.fullmatch(comparison_id) is None:
            raise TrialEvaluationError("trial and comparison IDs must be full SHA-256")
        return self.comparisons_root / trial_id / f"{comparison_id}.json"

    def _validate_binding(self, comparison: TrialEvaluationComparisonResult) -> None:
        registration = self.registry.require_registered(comparison.trial_id)
        if (
            comparison.strategy_id != registration.model_bundle_id
            or comparison.benchmark_id != registration.benchmark_id
            or comparison.primary_metric != registration.primary_metric
            or comparison.base_slippage_bps != registration.base_slippage_bps
            or comparison.stressed_slippage_bps != registration.stressed_slippage_bps
        ):
            raise TrialEvaluationComparisonConflict(
                "comparison does not match registered strategy, benchmark, or slippage"
            )

    @staticmethod
    def _references(comparison: TrialEvaluationComparisonResult) -> dict[str, object]:
        return {
            "trial_id": comparison.trial_id,
            "strategy_id": comparison.strategy_id,
            "benchmark_id": comparison.benchmark_id,
            "primary_metric": comparison.primary_metric,
            "base_slippage_bps": str(comparison.base_slippage_bps),
            "stressed_slippage_bps": (
                None
                if comparison.stressed_slippage_bps is None
                else str(comparison.stressed_slippage_bps)
            ),
            "strategy_base_result_id": comparison.strategy_base.result_id,
            "benchmark_base_result_id": comparison.benchmark_base.result_id,
            "strategy_stressed_result_id": (
                None
                if comparison.strategy_stressed is None
                else comparison.strategy_stressed.result_id
            ),
            "benchmark_stressed_result_id": (
                None
                if comparison.benchmark_stressed is None
                else comparison.benchmark_stressed.result_id
            ),
            "comparison_metrics": [
                [name, str(value)] for name, value in comparison.comparison_metrics
            ],
            "passed": comparison.passed,
            "comparison_id": comparison.comparison_id,
        }

    def publish(
        self,
        comparison: TrialEvaluationComparisonResult,
    ) -> TrialEvaluationComparisonResult:
        if type(comparison) is not TrialEvaluationComparisonResult:
            raise TypeError("comparison must be exact")
        comparison.verify_content_identity()
        self._validate_binding(comparison)
        for result in (
            comparison.strategy_base,
            comparison.benchmark_base,
            comparison.strategy_stressed,
            comparison.benchmark_stressed,
        ):
            if result is not None:
                self.result_store.publish(result)
        trial_dir = self.comparisons_root / comparison.trial_id
        trial_dir.mkdir(parents=True, exist_ok=True)
        if _is_link_like(self.comparisons_root) or _is_link_like(trial_dir):
            raise TrialEvaluationComparisonConflict("comparison path cannot be a link")
        target = self._path(comparison.trial_id, comparison.comparison_id)
        payload = (
            json.dumps(
                {
                    "store_schema_version": TRIAL_EVALUATION_COMPARISON_STORE_SCHEMA_VERSION,
                    "comparison": self._references(comparison),
                },
                ensure_ascii=False,
                allow_nan=False,
                separators=(",", ":"),
                sort_keys=True,
            )
            + "\n"
        ).encode("utf-8")
        try:
            with advisory_file_lock(self.comparisons_root / ".comparisons.lock"):
                if target.exists():
                    stored = self.get(comparison.trial_id, comparison.comparison_id)
                    if stored != comparison:
                        raise TrialEvaluationComparisonConflict(
                            "stored comparison differs from proposed content"
                        )
                    return stored
                descriptor, temporary_name = tempfile.mkstemp(
                    prefix=".comparison-", suffix=".tmp", dir=trial_dir
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
            raise TrialEvaluationComparisonConflict("comparison store unavailable") from exc
        return self.get(comparison.trial_id, comparison.comparison_id)

    def get(self, trial_id: str, comparison_id: str) -> TrialEvaluationComparisonResult:
        self.registry.require_registered(trial_id)
        path = self._path(trial_id, comparison_id)
        if not path.exists():
            raise TrialEvaluationComparisonNotFound(comparison_id)
        if not path.is_file() or _is_link_like(path):
            raise TrialEvaluationComparisonConflict("comparison must be a regular file")
        try:
            raw = json.loads(
                read_stable_regular_file(path, maximum_bytes=_MAX_BYTES).decode("utf-8"),
                object_pairs_hook=_unique_object,
                parse_float=lambda _: (_ for _ in ()).throw(ValueError()),
                parse_constant=lambda _: (_ for _ in ()).throw(ValueError()),
            )
            if (
                type(raw) is not dict
                or set(raw) != {"store_schema_version", "comparison"}
                or raw["store_schema_version"]
                != TRIAL_EVALUATION_COMPARISON_STORE_SCHEMA_VERSION
                or type(raw["comparison"]) is not dict
            ):
                raise ValueError
            value = raw["comparison"]
            if set(value) != set(self._references_placeholder()):
                raise ValueError
            stressed_strategy = value["strategy_stressed_result_id"]
            stressed_benchmark = value["benchmark_stressed_result_id"]
            comparison = TrialEvaluationComparisonResult(
                trial_id=value["trial_id"],
                strategy_id=value["strategy_id"],
                benchmark_id=value["benchmark_id"],
                primary_metric=value["primary_metric"],
                base_slippage_bps=self._decimal(value["base_slippage_bps"]),
                stressed_slippage_bps=(
                    None
                    if value["stressed_slippage_bps"] is None
                    else self._decimal(value["stressed_slippage_bps"])
                ),
                strategy_base=self.result_store.get(
                    trial_id, value["strategy_base_result_id"]
                ),
                benchmark_base=self.result_store.get(
                    trial_id, value["benchmark_base_result_id"]
                ),
                strategy_stressed=(
                    None
                    if stressed_strategy is None
                    else self.result_store.get(trial_id, stressed_strategy)
                ),
                benchmark_stressed=(
                    None
                    if stressed_benchmark is None
                    else self.result_store.get(trial_id, stressed_benchmark)
                ),
            )
            stored_metrics = tuple(
                (name, self._decimal(metric))
                for name, metric in value["comparison_metrics"]
            )
            if (
                comparison.comparison_metrics != stored_metrics
                or comparison.passed != value["passed"]
                or comparison.comparison_id != value["comparison_id"]
                or comparison.trial_id != trial_id
                or comparison.comparison_id != comparison_id
            ):
                raise TrialEvaluationComparisonConflict(
                    "stored comparison does not match generated content"
                )
            self._validate_binding(comparison)
            return comparison
        except TrialEvaluationError:
            raise
        except (
            FileSafetyError,
            InvalidOperation,
            TypeError,
            ValueError,
            KeyError,
            json.JSONDecodeError,
        ) as exc:
            raise TrialEvaluationComparisonConflict("stored comparison is invalid") from exc

    @staticmethod
    def _decimal(value: object) -> Decimal:
        if type(value) is not str:
            raise ValueError
        result = Decimal(value)
        if not result.is_finite():
            raise ValueError
        return result

    @staticmethod
    def _references_placeholder() -> dict[str, object]:
        return {
            "trial_id": None,
            "strategy_id": None,
            "benchmark_id": None,
            "primary_metric": None,
            "base_slippage_bps": None,
            "stressed_slippage_bps": None,
            "strategy_base_result_id": None,
            "benchmark_base_result_id": None,
            "strategy_stressed_result_id": None,
            "benchmark_stressed_result_id": None,
            "comparison_metrics": None,
            "passed": None,
            "comparison_id": None,
        }

    def require_persisted(
        self, comparison: TrialEvaluationComparisonResult
    ) -> TrialEvaluationComparisonResult:
        stored = self.get(comparison.trial_id, comparison.comparison_id)
        if stored != comparison:
            raise TrialEvaluationComparisonConflict("persisted comparison differs")
        return stored
