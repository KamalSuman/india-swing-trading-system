from __future__ import annotations

import json
import os
import re
import stat
import tempfile
from dataclasses import fields, is_dataclass
from datetime import date
from decimal import Decimal, InvalidOperation
from enum import Enum
from pathlib import Path

from india_swing._filesystem import (
    FileLockUnavailable,
    FileSafetyError,
    advisory_file_lock,
    read_stable_regular_file,
)
from india_swing.execution.simulator import LimitEntryOrder

from .baselines import (
    DeterministicBaselineError,
    DeterministicComparisonRun,
    FoldComparisonSummary,
    GeneratedIntentBatch,
    GeneratedIntentRole,
    GeneratedSignalDecision,
)
from .comparison_store import LocalTrialEvaluationComparisonStore
from .engine import EvaluationTradeIntent
from .trial_store import LocalTrialRegistry


GENERATED_INTENT_BATCH_STORE_SCHEMA_VERSION = "local-generated-intent-batch/v1"
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_MAX_BATCH_BYTES = 128 * 1024 * 1024
_MAX_RUN_BYTES = 32 * 1024 * 1024


class GeneratedIntentBatchConflict(DeterministicBaselineError):
    pass


class GeneratedIntentBatchNotFound(DeterministicBaselineError):
    pass


class DeterministicComparisonRunConflict(DeterministicBaselineError):
    pass


class DeterministicComparisonRunNotFound(DeterministicBaselineError):
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
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if is_dataclass(value):
        return {item.name: _json_value(getattr(value, item.name)) for item in fields(value)}
    if isinstance(value, tuple):
        return [_json_value(item) for item in value]
    if value is None or isinstance(value, (str, int, bool)):
        return value
    raise TypeError(f"unsupported generated-batch value: {type(value).__name__}")


def encode_generated_intent_batch(batch: GeneratedIntentBatch) -> bytes:
    if type(batch) is not GeneratedIntentBatch:
        raise TypeError("batch must be exact")
    batch.verify_content_identity()
    return (
        json.dumps(
            {
                "store_schema_version": GENERATED_INTENT_BATCH_STORE_SCHEMA_VERSION,
                "batch": _json_value(batch),
            },
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
            raise GeneratedIntentBatchConflict("batch contains a duplicate JSON key")
        result[key] = value
    return result


def _object(value: object, expected: set[str], name: str) -> dict[str, object]:
    if type(value) is not dict or set(value) != expected:
        raise GeneratedIntentBatchConflict(f"stored {name} has invalid fields")
    return value


def _decimal(value: object, name: str) -> Decimal:
    if type(value) is not str:
        raise GeneratedIntentBatchConflict(f"stored {name} must be a Decimal string")
    try:
        result = Decimal(value)
    except InvalidOperation as exc:
        raise GeneratedIntentBatchConflict(f"stored {name} is invalid") from exc
    if not result.is_finite():
        raise GeneratedIntentBatchConflict(f"stored {name} must be finite")
    return result


def _date(value: object, name: str) -> date:
    if type(value) is not str:
        raise GeneratedIntentBatchConflict(f"stored {name} must be an ISO date")
    try:
        result = date.fromisoformat(value)
    except ValueError as exc:
        raise GeneratedIntentBatchConflict(f"stored {name} is invalid") from exc
    if result.isoformat() != value:
        raise GeneratedIntentBatchConflict(f"stored {name} is not canonical")
    return result


def _decode_order(value: object) -> LimitEntryOrder:
    raw = _object(value, {item.name for item in fields(LimitEntryOrder)}, "entry order")
    stored_id = raw["order_id"]
    order = LimitEntryOrder(
        symbol=raw["symbol"],
        signal_session=_date(raw["signal_session"], "order.signal_session"),
        first_eligible_session=_date(
            raw["first_eligible_session"], "order.first_eligible_session"
        ),
        expiry_session=_date(raw["expiry_session"], "order.expiry_session"),
        quantity=raw["quantity"],
        limit_price=_decimal(raw["limit_price"], "order.limit_price"),
        tick_size=_decimal(raw["tick_size"], "order.tick_size"),
        maximum_participation=_decimal(
            raw["maximum_participation"], "order.maximum_participation"
        ),
    )
    if order.order_id != stored_id:
        raise GeneratedIntentBatchConflict("stored entry-order ID differs from content")
    return order


def _decode_intent(value: object) -> EvaluationTradeIntent:
    raw = _object(
        value,
        {item.name for item in fields(EvaluationTradeIntent)},
        "trade intent",
    )
    stored_id = raw["intent_id"]
    intent = EvaluationTradeIntent(
        signal_id=raw["signal_id"],
        universe_snapshot_id=raw["universe_snapshot_id"],
        isin=raw["isin"],
        entry_order=_decode_order(raw["entry_order"]),
        stop_price=_decimal(raw["stop_price"], "intent.stop_price"),
        target_price=_decimal(raw["target_price"], "intent.target_price"),
        max_holding_sessions=raw["max_holding_sessions"],
    )
    if intent.intent_id != stored_id:
        raise GeneratedIntentBatchConflict("stored intent ID differs from content")
    return intent


def _decode_decision(value: object) -> GeneratedSignalDecision:
    raw = _object(
        value,
        {item.name for item in fields(GeneratedSignalDecision)},
        "signal decision",
    )
    stored_id = raw["decision_id"]
    evidence = raw["evidence_bar_ids"]
    if type(evidence) is not list:
        raise GeneratedIntentBatchConflict("stored decision evidence must be a list")
    decision = GeneratedSignalDecision(
        generator_id=raw["generator_id"],
        role=GeneratedIntentRole(raw["role"]),
        fold_id=raw["fold_id"],
        signal_session=_date(raw["signal_session"], "decision.signal_session"),
        instrument_id=raw["instrument_id"],
        symbol=raw["symbol"],
        score_name=raw["score_name"],
        score=None if raw["score"] is None else _decimal(raw["score"], "decision.score"),
        selected=raw["selected"],
        reason=raw["reason"],
        evidence_bar_ids=tuple(evidence),
    )
    if decision.decision_id != stored_id:
        raise GeneratedIntentBatchConflict("stored decision ID differs from content")
    return decision


def decode_generated_intent_batch(payload: bytes) -> GeneratedIntentBatch:
    try:
        raw = json.loads(
            payload.decode("utf-8"),
            object_pairs_hook=_unique_object,
            parse_float=lambda _: (_ for _ in ()).throw(ValueError()),
            parse_constant=lambda _: (_ for _ in ()).throw(ValueError()),
        )
        envelope = _object(raw, {"store_schema_version", "batch"}, "batch envelope")
        if envelope["store_schema_version"] != GENERATED_INTENT_BATCH_STORE_SCHEMA_VERSION:
            raise GeneratedIntentBatchConflict("unsupported batch store schema")
        value = _object(
            envelope["batch"],
            {item.name for item in fields(GeneratedIntentBatch)},
            "intent batch",
        )
        if type(value["source_snapshot_ids"]) is not list:
            raise GeneratedIntentBatchConflict("stored source snapshots must be a list")
        if type(value["decisions"]) is not list or type(value["intents"]) is not list:
            raise GeneratedIntentBatchConflict("stored batch collections must be lists")
        stored_id = value["batch_id"]
        batch = GeneratedIntentBatch(
            generator_id=value["generator_id"],
            role=GeneratedIntentRole(value["role"]),
            split_plan_id=value["split_plan_id"],
            source_snapshot_ids=tuple(value["source_snapshot_ids"]),
            decisions=tuple(_decode_decision(item) for item in value["decisions"]),
            intents=tuple(_decode_intent(item) for item in value["intents"]),
        )
        if batch.batch_id != stored_id:
            raise GeneratedIntentBatchConflict("stored batch ID differs from content")
        return batch
    except GeneratedIntentBatchConflict:
        raise
    except (
        InvalidOperation,
        KeyError,
        TypeError,
        ValueError,
        UnicodeDecodeError,
        json.JSONDecodeError,
    ) as exc:
        raise GeneratedIntentBatchConflict("stored generated batch is invalid") from exc


class LocalGeneratedIntentBatchStore:
    """One create-once strategy and benchmark batch per registered trial."""

    def __init__(self, root: Path, registry: LocalTrialRegistry) -> None:
        self.root = Path(root)
        if type(registry) is not LocalTrialRegistry:
            raise TypeError("registry must be exact")
        self.registry = registry

    @property
    def batches_root(self) -> Path:
        return self.root / "intent_batches"

    def _trial_dir(self, trial_id: str) -> Path:
        if _SHA256.fullmatch(trial_id) is None:
            raise DeterministicBaselineError("trial_id must be a full lowercase SHA-256")
        return self.batches_root / trial_id

    def _path(self, trial_id: str, role: GeneratedIntentRole) -> Path:
        if type(role) is not GeneratedIntentRole:
            raise TypeError("role must be exact")
        return self._trial_dir(trial_id) / f"{role.value.casefold()}.json"

    def _validate_binding(self, trial_id: str, batch: GeneratedIntentBatch) -> None:
        registration = self.registry.require_registered(trial_id)
        expected_generator = (
            registration.model_bundle_id
            if batch.role is GeneratedIntentRole.STRATEGY
            else registration.benchmark_id
        )
        if (
            batch.generator_id != expected_generator
            or batch.split_plan_id != registration.split_plan_id
            or batch.source_snapshot_ids != registration.data_snapshot_ids
        ):
            raise GeneratedIntentBatchConflict(
                "generated batch does not match registered generator, split, or data"
            )

    def publish(self, trial_id: str, batch: GeneratedIntentBatch) -> GeneratedIntentBatch:
        if type(batch) is not GeneratedIntentBatch:
            raise TypeError("batch must be exact")
        batch.verify_content_identity()
        self._validate_binding(trial_id, batch)
        self.batches_root.mkdir(parents=True, exist_ok=True)
        trial_dir = self._trial_dir(trial_id)
        trial_dir.mkdir(exist_ok=True)
        if _is_link_like(self.batches_root) or _is_link_like(trial_dir):
            raise GeneratedIntentBatchConflict("generated-batch path cannot be a link")
        target = self._path(trial_id, batch.role)
        payload = encode_generated_intent_batch(batch)
        try:
            with advisory_file_lock(self.batches_root / ".intent-batches.lock"):
                if target.exists():
                    stored = self.get(trial_id, batch.role)
                    if stored != batch:
                        raise GeneratedIntentBatchConflict(
                            "trial role already stores a different generated batch"
                        )
                    return stored
                descriptor, temporary_name = tempfile.mkstemp(
                    prefix=".intent-batch-", suffix=".tmp", dir=trial_dir
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
            raise GeneratedIntentBatchConflict("generated-batch store unavailable") from exc
        return self.get(trial_id, batch.role)

    def get(self, trial_id: str, role: GeneratedIntentRole) -> GeneratedIntentBatch:
        self.registry.require_registered(trial_id)
        path = self._path(trial_id, role)
        if not path.exists():
            raise GeneratedIntentBatchNotFound(f"{trial_id}:{role.value}")
        if not path.is_file() or _is_link_like(path):
            raise GeneratedIntentBatchConflict("generated batch must be a regular file")
        try:
            payload = read_stable_regular_file(path, maximum_bytes=_MAX_BATCH_BYTES)
        except FileSafetyError as exc:
            raise GeneratedIntentBatchConflict("generated batch could not be read safely") from exc
        batch = decode_generated_intent_batch(payload)
        if batch.role is not role:
            raise GeneratedIntentBatchConflict("generated-batch role path differs from content")
        self._validate_binding(trial_id, batch)
        return batch

    def require_persisted(
        self, trial_id: str, batch: GeneratedIntentBatch
    ) -> GeneratedIntentBatch:
        stored = self.get(trial_id, batch.role)
        if stored != batch:
            raise GeneratedIntentBatchConflict("persisted generated batch differs")
        return stored


class LocalDeterministicComparisonRunStore:
    """One create-once run manifest per trial, backed by full component artifacts."""

    def __init__(
        self,
        batch_store: LocalGeneratedIntentBatchStore,
        comparison_store: LocalTrialEvaluationComparisonStore,
    ) -> None:
        if type(batch_store) is not LocalGeneratedIntentBatchStore:
            raise TypeError("batch_store must be exact")
        if type(comparison_store) is not LocalTrialEvaluationComparisonStore:
            raise TypeError("comparison_store must be exact")
        self.batch_store = batch_store
        self.comparison_store = comparison_store
        if batch_store.root.resolve() != comparison_store.root.resolve():
            raise ValueError("batch and comparison stores must share one evidence root")
        self.root = batch_store.root

    @property
    def runs_root(self) -> Path:
        return self.root / "deterministic_runs"

    def _path(self, trial_id: str) -> Path:
        if _SHA256.fullmatch(trial_id) is None:
            raise DeterministicBaselineError("trial_id must be a full lowercase SHA-256")
        return self.runs_root / f"{trial_id}.json"

    @staticmethod
    def _manifest(run: DeterministicComparisonRun) -> dict[str, object]:
        return {
            "store_schema_version": "local-deterministic-comparison-run/v1",
            "trial_id": run.comparison.trial_id,
            "run_id": run.run_id,
            "strategy_batch_id": run.strategy_batch.batch_id,
            "benchmark_batch_id": run.benchmark_batch.batch_id,
            "comparison_id": run.comparison.comparison_id,
            "fold_summaries": _json_value(run.fold_summaries),
        }

    @staticmethod
    def _decode_metrics(value: object, name: str) -> tuple[tuple[str, Decimal], ...]:
        if type(value) is not list:
            raise DeterministicComparisonRunConflict(f"stored {name} must be a list")
        metrics: list[tuple[str, Decimal]] = []
        for item in value:
            if type(item) is not list or len(item) != 2 or type(item[0]) is not str:
                raise DeterministicComparisonRunConflict(f"stored {name} entry is invalid")
            metrics.append((item[0], _decimal(item[1], name)))
        return tuple(metrics)

    @classmethod
    def _decode_summary(cls, value: object) -> FoldComparisonSummary:
        raw = _object(
            value,
            {item.name for item in fields(FoldComparisonSummary)},
            "fold summary",
        )
        stored_metrics = cls._decode_metrics(
            raw["comparison_metrics"], "comparison_metrics"
        )
        stored_outperformed = raw["outperformed"]
        stored_id = raw["summary_id"]
        summary = FoldComparisonSummary(
            fold_id=raw["fold_id"],
            first_session=_date(raw["first_session"], "summary.first_session"),
            last_session=_date(raw["last_session"], "summary.last_session"),
            primary_metric=raw["primary_metric"],
            strategy_base_metrics=cls._decode_metrics(
                raw["strategy_base_metrics"], "strategy_base_metrics"
            ),
            benchmark_base_metrics=cls._decode_metrics(
                raw["benchmark_base_metrics"], "benchmark_base_metrics"
            ),
            strategy_stressed_metrics=(
                None
                if raw["strategy_stressed_metrics"] is None
                else cls._decode_metrics(
                    raw["strategy_stressed_metrics"], "strategy_stressed_metrics"
                )
            ),
            benchmark_stressed_metrics=(
                None
                if raw["benchmark_stressed_metrics"] is None
                else cls._decode_metrics(
                    raw["benchmark_stressed_metrics"], "benchmark_stressed_metrics"
                )
            ),
        )
        if (
            summary.comparison_metrics != stored_metrics
            or summary.outperformed != stored_outperformed
            or summary.summary_id != stored_id
        ):
            raise DeterministicComparisonRunConflict(
                "stored fold summary differs from content"
            )
        return summary

    @staticmethod
    def _validate_intent_lineage(run: DeterministicComparisonRun) -> None:
        pairs = (
            (run.strategy_batch, run.comparison.strategy_base),
            (run.benchmark_batch, run.comparison.benchmark_base),
            (run.strategy_batch, run.comparison.strategy_stressed),
            (run.benchmark_batch, run.comparison.benchmark_stressed),
        )
        for batch, result in pairs:
            if result is None:
                continue
            allowed = {intent.intent_id for intent in batch.intents}
            observed = {trade.intent_id for trade in result.trades}
            if not observed.issubset(allowed):
                raise GeneratedIntentBatchConflict(
                    "evaluation contains a trade outside its generated batch"
                )

    def publish(self, run: DeterministicComparisonRun) -> DeterministicComparisonRun:
        if type(run) is not DeterministicComparisonRun:
            raise TypeError("run must be exact")
        run.verify_content_identity()
        trial_id = run.comparison.trial_id
        self._validate_intent_lineage(run)
        self.batch_store.publish(trial_id, run.strategy_batch)
        self.batch_store.publish(trial_id, run.benchmark_batch)
        self.comparison_store.publish(run.comparison)
        self.runs_root.mkdir(parents=True, exist_ok=True)
        if _is_link_like(self.runs_root):
            raise DeterministicComparisonRunConflict("deterministic-run root cannot be a link")
        target = self._path(trial_id)
        payload = (
            json.dumps(
                self._manifest(run),
                ensure_ascii=False,
                allow_nan=False,
                separators=(",", ":"),
                sort_keys=True,
            )
            + "\n"
        ).encode("utf-8")
        try:
            with advisory_file_lock(self.runs_root / ".deterministic-runs.lock"):
                if target.exists():
                    stored = self.get(trial_id)
                    if stored != run:
                        raise DeterministicComparisonRunConflict(
                            "trial already stores a different deterministic run"
                        )
                    return stored
                descriptor, temporary_name = tempfile.mkstemp(
                    prefix=".deterministic-run-", suffix=".tmp", dir=self.runs_root
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
            raise DeterministicComparisonRunConflict(
                "deterministic-run store unavailable"
            ) from exc
        return self.require_persisted(run)

    def get(self, trial_id: str) -> DeterministicComparisonRun:
        self.batch_store.registry.require_registered(trial_id)
        path = self._path(trial_id)
        if not path.exists():
            raise DeterministicComparisonRunNotFound(trial_id)
        if not path.is_file() or _is_link_like(path):
            raise DeterministicComparisonRunConflict(
                "deterministic run must be a regular file"
            )
        try:
            raw = json.loads(
                read_stable_regular_file(path, maximum_bytes=_MAX_RUN_BYTES).decode(
                    "utf-8"
                ),
                object_pairs_hook=_unique_object,
                parse_float=lambda _: (_ for _ in ()).throw(ValueError()),
                parse_constant=lambda _: (_ for _ in ()).throw(ValueError()),
            )
            expected = {
                "store_schema_version",
                "trial_id",
                "run_id",
                "strategy_batch_id",
                "benchmark_batch_id",
                "comparison_id",
                "fold_summaries",
            }
            if type(raw) is not dict or set(raw) != expected:
                raise ValueError
            if raw["store_schema_version"] != "local-deterministic-comparison-run/v1":
                raise ValueError
            if type(raw["fold_summaries"]) is not list:
                raise ValueError
            strategy_batch = self.batch_store.get(
                trial_id, GeneratedIntentRole.STRATEGY
            )
            benchmark_batch = self.batch_store.get(
                trial_id, GeneratedIntentRole.BENCHMARK
            )
            comparison = self.comparison_store.get(trial_id, raw["comparison_id"])
            run = DeterministicComparisonRun(
                strategy_batch=strategy_batch,
                benchmark_batch=benchmark_batch,
                comparison=comparison,
                fold_summaries=tuple(
                    self._decode_summary(value) for value in raw["fold_summaries"]
                ),
            )
            if (
                raw["trial_id"] != trial_id
                or raw["strategy_batch_id"] != strategy_batch.batch_id
                or raw["benchmark_batch_id"] != benchmark_batch.batch_id
                or raw["comparison_id"] != comparison.comparison_id
                or raw["run_id"] != run.run_id
            ):
                raise DeterministicComparisonRunConflict(
                    "stored deterministic-run manifest differs from content"
                )
            return run
        except DeterministicBaselineError:
            raise
        except (
            FileSafetyError,
            InvalidOperation,
            KeyError,
            TypeError,
            ValueError,
            UnicodeDecodeError,
            json.JSONDecodeError,
        ) as exc:
            raise DeterministicComparisonRunConflict(
                "stored deterministic run is invalid"
            ) from exc

    def require_persisted(
        self, run: DeterministicComparisonRun
    ) -> DeterministicComparisonRun:
        if type(run) is not DeterministicComparisonRun:
            raise TypeError("run must be exact")
        run.verify_content_identity()
        trial_id = run.comparison.trial_id
        self._validate_intent_lineage(run)
        stored = self.get(trial_id)
        if stored != run:
            raise DeterministicComparisonRunConflict(
                "persisted deterministic run differs"
            )
        self.batch_store.require_persisted(trial_id, run.strategy_batch)
        self.batch_store.require_persisted(trial_id, run.benchmark_batch)
        self.comparison_store.require_persisted(run.comparison)
        return stored
