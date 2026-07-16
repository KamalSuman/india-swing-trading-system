from __future__ import annotations

import json
import os
import re
import stat
import tempfile
from dataclasses import fields
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from enum import Enum
from pathlib import Path

from india_swing._filesystem import (
    FileLockUnavailable,
    FileSafetyError,
    advisory_file_lock,
    read_stable_regular_file,
)

from .trials import (
    TrialRegistration,
    TrialRegistrationError,
    TrialRegistrationIntegrityError,
    TrialStage,
)


TRIAL_REGISTRY_STORE_SCHEMA_VERSION = "local-trial-registry/v1"
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_MAX_REGISTRATION_BYTES = 4 * 1024 * 1024


class TrialNotRegistered(TrialRegistrationError):
    pass


class TrialRegistryConflict(TrialRegistrationError):
    pass


def _is_link_like(path: Path) -> bool:
    try:
        status = os.lstat(path)
    except OSError:
        return path.is_symlink()
    reparse_attribute = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400)
    return path.is_symlink() or bool(
        getattr(status, "st_file_attributes", 0) & reparse_attribute
    )


def _json_value(value: object) -> object:
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, tuple):
        return [_json_value(item) for item in value]
    if value is None or isinstance(value, (str, int, bool)):
        return value
    raise TypeError(f"unsupported trial-registry value: {type(value).__name__}")


def encode_trial_registration(registration: TrialRegistration) -> bytes:
    if type(registration) is not TrialRegistration:
        raise TypeError("registration must be an exact TrialRegistration")
    registration.verify_content_identity()
    payload = {
        "schema_version": TRIAL_REGISTRY_STORE_SCHEMA_VERSION,
        "registration": {
            item.name: _json_value(getattr(registration, item.name))
            for item in fields(TrialRegistration)
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
            raise TrialRegistrationIntegrityError(
                "trial registration contains a duplicate JSON key"
            )
        result[key] = value
    return result


def decode_trial_registration(payload: bytes) -> TrialRegistration:
    try:
        value = json.loads(
            payload.decode("utf-8", errors="strict"),
            object_pairs_hook=_unique_object,
            parse_float=lambda _: (_ for _ in ()).throw(ValueError()),
            parse_constant=lambda _: (_ for _ in ()).throw(ValueError()),
        )
        if (
            type(value) is not dict
            or set(value) != {"schema_version", "registration"}
            or value["schema_version"] != TRIAL_REGISTRY_STORE_SCHEMA_VERSION
            or type(value["registration"]) is not dict
        ):
            raise ValueError
        raw = value["registration"]
        if set(raw) != {item.name for item in fields(TrialRegistration)}:
            raise ValueError
        if (
            type(raw["base_slippage_bps"]) is not str
            or raw["stressed_slippage_bps"] is not None
            and type(raw["stressed_slippage_bps"]) is not str
            or type(raw["pass_thresholds"]) is not list
            or any(
                type(item) is not list
                or len(item) != 2
                or type(item[0]) is not str
                or type(item[1]) is not str
                for item in raw["pass_thresholds"]
            )
        ):
            raise ValueError
        stored_trial_id = raw["trial_id"]
        registration = TrialRegistration(
            registered_at=datetime.fromisoformat(raw["registered_at"]),
            stage=TrialStage(raw["stage"]),
            hypothesis=raw["hypothesis"],
            strategy_family_id=raw["strategy_family_id"],
            parent_trial_id=raw["parent_trial_id"],
            evaluation_start=date.fromisoformat(raw["evaluation_start"]),
            evaluation_end=date.fromisoformat(raw["evaluation_end"]),
            universe_snapshot_ids=tuple(raw["universe_snapshot_ids"]),
            data_snapshot_ids=tuple(raw["data_snapshot_ids"]),
            split_plan_id=raw["split_plan_id"],
            label_horizon_sessions=raw["label_horizon_sessions"],
            benchmark_id=raw["benchmark_id"],
            primary_metric=raw["primary_metric"],
            secondary_metrics=tuple(raw["secondary_metrics"]),
            model_bundle_id=raw["model_bundle_id"],
            source_commit=raw["source_commit"],
            dependency_hash=raw["dependency_hash"],
            configuration_hash=raw["configuration_hash"],
            exclusions_hash=raw["exclusions_hash"],
            risk_policy_hash=raw["risk_policy_hash"],
            execution_policy_version=raw["execution_policy_version"],
            execution_policy_hash=raw["execution_policy_hash"],
            cost_schedule_version=raw["cost_schedule_version"],
            cost_schedule_hash=raw["cost_schedule_hash"],
            base_slippage_bps=Decimal(raw["base_slippage_bps"]),
            stressed_slippage_bps=(
                None
                if raw["stressed_slippage_bps"] is None
                else Decimal(raw["stressed_slippage_bps"])
            ),
            pass_thresholds=tuple(
                (name, Decimal(threshold))
                for name, threshold in raw["pass_thresholds"]
            ),
            multiple_testing_policy=raw["multiple_testing_policy"],
            random_seed=raw["random_seed"],
            repetition_count=raw["repetition_count"],
            holdout_id=raw["holdout_id"],
            holdout_sealed=raw["holdout_sealed"],
            synthetic=raw["synthetic"],
            schema_version=raw["schema_version"],
        )
        if registration.trial_id != stored_trial_id:
            raise TrialRegistrationIntegrityError(
                "stored trial ID does not match registration content"
            )
        registration.verify_content_identity()
        return registration
    except TrialRegistrationIntegrityError:
        raise
    except (
        InvalidOperation,
        KeyError,
        TypeError,
        ValueError,
        UnicodeDecodeError,
        json.JSONDecodeError,
    ) as exc:
        raise TrialRegistrationIntegrityError(
            "stored trial registration is invalid"
        ) from exc


class LocalTrialRegistry:
    """Create-once local registrations; outcomes and holdout access are separate events."""

    def __init__(self, root: Path) -> None:
        self.root = Path(root)

    def _path(self, trial_id: str) -> Path:
        if not isinstance(trial_id, str) or _SHA256.fullmatch(trial_id) is None:
            raise TrialRegistrationError(
                "trial_id must be a full lowercase SHA-256"
            )
        return self.root / f"{trial_id}.json"

    def register(self, registration: TrialRegistration) -> TrialRegistration:
        if type(registration) is not TrialRegistration:
            raise TypeError("registration must be an exact TrialRegistration")
        registration.verify_content_identity()
        payload = encode_trial_registration(registration)
        self.root.mkdir(parents=True, exist_ok=True)
        if _is_link_like(self.root):
            raise TrialRegistrationIntegrityError("trial-registry root cannot be a link")
        target = self._path(registration.trial_id)
        lock = self.root / ".trial-registry.lock"
        try:
            with advisory_file_lock(lock):
                if target.exists():
                    existing = self.get(registration.trial_id)
                    if existing != registration:
                        raise TrialRegistryConflict(
                            "trial ID already stores different registration content"
                        )
                    return existing
                family = self.registrations_for_family(
                    registration.strategy_family_id
                )
                if family:
                    parents = {value.trial_id: value for value in family}
                    parent = parents.get(registration.parent_trial_id)
                    if parent is None:
                        raise TrialRegistryConflict(
                            "a successor trial must reference a registered parent in its family"
                        )
                    if parent.registered_at >= registration.registered_at:
                        raise TrialRegistryConflict(
                            "successor registration must follow its registered parent"
                        )
                elif registration.parent_trial_id is not None:
                    raise TrialRegistryConflict(
                        "initial family trial cannot reference an unregistered parent"
                    )
                descriptor, temporary_name = tempfile.mkstemp(
                    prefix=".trial-registration-",
                    suffix=".tmp",
                    dir=self.root,
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
            raise TrialRegistryConflict("trial registry is currently unavailable") from exc
        return self.get(registration.trial_id)

    def get(self, trial_id: str) -> TrialRegistration:
        path = self._path(trial_id)
        if not path.exists():
            raise TrialNotRegistered(f"trial is not registered: {trial_id}")
        if _is_link_like(path) or not path.is_file():
            raise TrialRegistrationIntegrityError(
                "trial registration path must be a regular non-link file"
            )
        try:
            payload = read_stable_regular_file(
                path,
                maximum_bytes=_MAX_REGISTRATION_BYTES,
            )
        except FileSafetyError as exc:
            raise TrialRegistrationIntegrityError(
                "trial registration could not be read safely"
            ) from exc
        registration = decode_trial_registration(payload)
        if registration.trial_id != trial_id or path.stem != trial_id:
            raise TrialRegistrationIntegrityError(
                "trial registration path identity mismatch"
            )
        return registration

    def require_registered(self, trial_id: str) -> TrialRegistration:
        return self.get(trial_id)

    def registrations_for_family(
        self,
        strategy_family_id: str,
    ) -> tuple[TrialRegistration, ...]:
        if not isinstance(strategy_family_id, str) or not strategy_family_id.strip():
            raise TrialRegistrationError("strategy_family_id is required")
        if not self.root.exists():
            return ()
        if not self.root.is_dir() or _is_link_like(self.root):
            raise TrialRegistrationIntegrityError(
                "trial-registry root must be a regular directory"
            )
        registrations = tuple(
            self.get(path.stem)
            for path in sorted(self.root.glob("*.json"), key=lambda value: value.name)
        )
        return tuple(
            sorted(
                (
                    value
                    for value in registrations
                    if value.strategy_family_id == strategy_family_id
                ),
                key=lambda value: (value.registered_at, value.trial_id),
            )
        )
