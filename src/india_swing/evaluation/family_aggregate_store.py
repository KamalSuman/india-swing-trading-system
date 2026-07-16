from __future__ import annotations

import json
import os
import stat
import tempfile
from dataclasses import fields, is_dataclass
from decimal import Decimal, InvalidOperation
from enum import Enum
from pathlib import Path

from india_swing._filesystem import (
    FileLockUnavailable,
    FileSafetyError,
    advisory_file_lock,
    read_stable_regular_file,
)
from india_swing.identity import content_id

from .baseline_store import LocalDeterministicComparisonRunStore
from .baselines import DeterministicComparisonRun
from .family_aggregation import (
    FOLD_SIGN_HOLM_ALPHA,
    FOLD_SIGN_HOLM_POLICY,
    FamilyTrialDecision,
    TrialFamilyAggregationError,
    TrialFamilyEvaluationAggregate,
    TrialFamilyEvaluationAggregator,
)
from .trial_store import LocalTrialRegistry


TRIAL_FAMILY_AGGREGATE_STORE_SCHEMA_VERSION = "local-trial-family-aggregate/v1"
_MAX_AGGREGATE_BYTES = 16 * 1024 * 1024


class TrialFamilyAggregateConflict(TrialFamilyAggregationError):
    pass


class TrialFamilyAggregateNotFound(TrialFamilyAggregationError):
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
    if is_dataclass(value):
        return {item.name: _json_value(getattr(value, item.name)) for item in fields(value)}
    if isinstance(value, tuple):
        return [_json_value(item) for item in value]
    if value is None or isinstance(value, (str, int, bool)):
        return value
    raise TypeError(f"unsupported family-aggregate value: {type(value).__name__}")


def encode_trial_family_aggregate(
    aggregate: TrialFamilyEvaluationAggregate,
) -> bytes:
    if type(aggregate) is not TrialFamilyEvaluationAggregate:
        raise TypeError("aggregate must be exact")
    aggregate.verify_content_identity()
    return (
        json.dumps(
            {
                "store_schema_version": TRIAL_FAMILY_AGGREGATE_STORE_SCHEMA_VERSION,
                "aggregate": _json_value(aggregate),
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
            raise TrialFamilyAggregateConflict("aggregate contains a duplicate JSON key")
        result[key] = value
    return result


def _object(value: object, expected: set[str], name: str) -> dict[str, object]:
    if type(value) is not dict or set(value) != expected:
        raise TrialFamilyAggregateConflict(f"stored {name} has invalid fields")
    return value


def _decimal(value: object, name: str) -> Decimal:
    if type(value) is not str:
        raise TrialFamilyAggregateConflict(f"stored {name} must be a Decimal string")
    try:
        result = Decimal(value)
    except InvalidOperation as exc:
        raise TrialFamilyAggregateConflict(f"stored {name} is invalid") from exc
    if not result.is_finite():
        raise TrialFamilyAggregateConflict(f"stored {name} must be finite")
    return result


def _decode_decision(value: object) -> FamilyTrialDecision:
    raw = _object(
        value,
        {item.name for item in fields(FamilyTrialDecision)},
        "family decision",
    )
    stored_id = raw["decision_id"]
    decision = FamilyTrialDecision(
        trial_id=raw["trial_id"],
        comparison_id=raw["comparison_id"],
        fold_count=raw["fold_count"],
        base_wins=raw["base_wins"],
        stressed_wins=raw["stressed_wins"],
        base_p_value=_decimal(raw["base_p_value"], "base_p_value"),
        stressed_p_value=_decimal(raw["stressed_p_value"], "stressed_p_value"),
        raw_p_value=_decimal(raw["raw_p_value"], "raw_p_value"),
        holm_rank=raw["holm_rank"],
        holm_threshold=_decimal(raw["holm_threshold"], "holm_threshold"),
        hypothesis_rejected=raw["hypothesis_rejected"],
        comparison_passed=raw["comparison_passed"],
        eligible=raw["eligible"],
    )
    if decision.decision_id != stored_id:
        raise TrialFamilyAggregateConflict("stored family decision ID differs from content")
    return decision


def decode_trial_family_aggregate(payload: bytes) -> TrialFamilyEvaluationAggregate:
    try:
        raw = json.loads(
            payload.decode("utf-8"),
            object_pairs_hook=_unique_object,
            parse_float=lambda _: (_ for _ in ()).throw(ValueError()),
            parse_constant=lambda _: (_ for _ in ()).throw(ValueError()),
        )
        envelope = _object(
            raw, {"store_schema_version", "aggregate"}, "aggregate envelope"
        )
        if envelope["store_schema_version"] != TRIAL_FAMILY_AGGREGATE_STORE_SCHEMA_VERSION:
            raise TrialFamilyAggregateConflict("unsupported family-aggregate store schema")
        value = _object(
            envelope["aggregate"],
            {item.name for item in fields(TrialFamilyEvaluationAggregate)},
            "family aggregate",
        )
        if (
            type(value["registered_trial_ids"]) is not list
            or type(value["decisions"]) is not list
            or type(value["eligible_trial_ids"]) is not list
        ):
            raise TrialFamilyAggregateConflict("stored aggregate collections must be lists")
        stored_eligible = tuple(value["eligible_trial_ids"])
        stored_passed = value["passed"]
        stored_id = value["aggregate_id"]
        aggregate = TrialFamilyEvaluationAggregate(
            strategy_family_id=value["strategy_family_id"],
            policy=value["policy"],
            alpha=_decimal(value["alpha"], "alpha"),
            registered_trial_ids=tuple(value["registered_trial_ids"]),
            decisions=tuple(_decode_decision(item) for item in value["decisions"]),
        )
        if (
            aggregate.eligible_trial_ids != stored_eligible
            or aggregate.passed != stored_passed
            or aggregate.aggregate_id != stored_id
        ):
            raise TrialFamilyAggregateConflict("stored family aggregate differs from content")
        return aggregate
    except TrialFamilyAggregateConflict:
        raise
    except (
        InvalidOperation,
        KeyError,
        TypeError,
        ValueError,
        UnicodeDecodeError,
        json.JSONDecodeError,
    ) as exc:
        raise TrialFamilyAggregateConflict("stored family aggregate is invalid") from exc


class LocalTrialFamilyAggregateStore:
    """Create once for each exact registered-trial family snapshot."""

    def __init__(
        self,
        root: Path,
        registry: LocalTrialRegistry,
        run_store: LocalDeterministicComparisonRunStore,
    ) -> None:
        self.root = Path(root)
        if type(registry) is not LocalTrialRegistry:
            raise TypeError("registry must be exact")
        if type(run_store) is not LocalDeterministicComparisonRunStore:
            raise TypeError("run_store must be exact")
        self.registry = registry
        self.run_store = run_store

    @property
    def aggregates_root(self) -> Path:
        return self.root / "family_aggregates"

    @staticmethod
    def _family_key(strategy_family_id: str) -> str:
        if not isinstance(strategy_family_id, str) or not strategy_family_id.strip():
            raise TrialFamilyAggregationError("strategy_family_id is required")
        return content_id(
            {"schema": "trial-family-path-key/v1", "strategy_family_id": strategy_family_id},
            length=64,
        )

    @staticmethod
    def _snapshot_key(
        strategy_family_id: str, registered_trial_ids: tuple[str, ...]
    ) -> str:
        return content_id(
            {
                "schema": "trial-family-snapshot-key/v1",
                "strategy_family_id": strategy_family_id,
                "policy": FOLD_SIGN_HOLM_POLICY,
                "alpha": FOLD_SIGN_HOLM_ALPHA,
                "registered_trial_ids": registered_trial_ids,
            },
            length=64,
        )

    def _family_dir(self, strategy_family_id: str) -> Path:
        return self.aggregates_root / self._family_key(strategy_family_id)

    def _path(
        self, strategy_family_id: str, registered_trial_ids: tuple[str, ...]
    ) -> Path:
        return self._family_dir(strategy_family_id) / (
            self._snapshot_key(strategy_family_id, registered_trial_ids) + ".json"
        )

    def _validate_registrations(self, aggregate: TrialFamilyEvaluationAggregate) -> None:
        family = self.registry.registrations_for_family(aggregate.strategy_family_id)
        by_id = {value.trial_id: value for value in family}
        if any(value not in by_id for value in aggregate.registered_trial_ids):
            raise TrialFamilyAggregateConflict("aggregate references an unregistered family trial")
        if any(
            by_id[value].multiple_testing_policy != aggregate.policy
            for value in aggregate.registered_trial_ids
        ):
            raise TrialFamilyAggregateConflict("aggregate policy differs from registration")

    def publish(
        self,
        aggregate: TrialFamilyEvaluationAggregate,
        *,
        runs: tuple[DeterministicComparisonRun, ...],
    ) -> TrialFamilyEvaluationAggregate:
        if type(aggregate) is not TrialFamilyEvaluationAggregate:
            raise TypeError("aggregate must be exact")
        aggregate.verify_content_identity()
        expected = TrialFamilyEvaluationAggregator(
            self.registry, self.run_store
        ).aggregate(
            strategy_family_id=aggregate.strategy_family_id,
            runs=runs,
        )
        if expected != aggregate:
            raise TrialFamilyAggregateConflict(
                "family aggregate differs from persisted run evidence"
            )
        self._validate_registrations(aggregate)
        self.aggregates_root.mkdir(parents=True, exist_ok=True)
        family_dir = self._family_dir(aggregate.strategy_family_id)
        family_dir.mkdir(exist_ok=True)
        if _is_link_like(self.aggregates_root) or _is_link_like(family_dir):
            raise TrialFamilyAggregateConflict("family-aggregate path cannot be a link")
        target = self._path(
            aggregate.strategy_family_id, aggregate.registered_trial_ids
        )
        payload = encode_trial_family_aggregate(aggregate)
        try:
            with advisory_file_lock(self.aggregates_root / ".family-aggregates.lock"):
                if target.exists():
                    stored = self.get(
                        aggregate.strategy_family_id, aggregate.registered_trial_ids
                    )
                    if stored != aggregate:
                        raise TrialFamilyAggregateConflict(
                            "family snapshot already stores a different aggregate"
                        )
                    return stored
                descriptor, temporary_name = tempfile.mkstemp(
                    prefix=".family-aggregate-", suffix=".tmp", dir=family_dir
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
            raise TrialFamilyAggregateConflict("family-aggregate store unavailable") from exc
        return self.get(aggregate.strategy_family_id, aggregate.registered_trial_ids)

    def get(
        self,
        strategy_family_id: str,
        registered_trial_ids: tuple[str, ...],
    ) -> TrialFamilyEvaluationAggregate:
        path = self._path(strategy_family_id, registered_trial_ids)
        if not path.exists():
            raise TrialFamilyAggregateNotFound(strategy_family_id)
        if not path.is_file() or _is_link_like(path):
            raise TrialFamilyAggregateConflict("family aggregate must be a regular file")
        try:
            payload = read_stable_regular_file(path, maximum_bytes=_MAX_AGGREGATE_BYTES)
        except FileSafetyError as exc:
            raise TrialFamilyAggregateConflict("family aggregate could not be read safely") from exc
        aggregate = decode_trial_family_aggregate(payload)
        if (
            aggregate.strategy_family_id != strategy_family_id
            or aggregate.registered_trial_ids != registered_trial_ids
        ):
            raise TrialFamilyAggregateConflict("family-aggregate path differs from content")
        self._validate_registrations(aggregate)
        return aggregate

    def require_persisted(
        self, aggregate: TrialFamilyEvaluationAggregate
    ) -> TrialFamilyEvaluationAggregate:
        stored = self.get(
            aggregate.strategy_family_id, aggregate.registered_trial_ids
        )
        if stored != aggregate:
            raise TrialFamilyAggregateConflict("persisted family aggregate differs")
        return stored
