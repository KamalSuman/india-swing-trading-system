"""Canonical create-once storage for promoted research-intent batches."""

from __future__ import annotations

import json
import os
import re
import stat
import tempfile
from dataclasses import dataclass, fields
from datetime import date
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Protocol

from india_swing._filesystem import (
    FileLockUnavailable,
    FileSafetyError,
    advisory_file_lock,
    read_stable_regular_file,
)
from india_swing.features.promoted_cross_section import (
    VerifiedPromotedCrossSectionPanel,
)
from india_swing.forecasting.regime_ensemble import MarketRegime

from .promoted_intents import (
    PromotedIntentError,
    PromotedIntentPolicyConfig,
    PromotedResearchIntentService,
    VerifiedPromotedResearchIntentBatch,
)


class PromotedIntentStoreError(PromotedIntentError):
    pass


class PromotedIntentStoreConflict(PromotedIntentStoreError):
    pass


class PromotedIntentStoreNotFound(PromotedIntentStoreError):
    pass


PROMOTED_INTENT_STORE_SCHEMA_VERSION = "promoted-intent-store/v1"
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_MAXIMUM_BYTES = 16 * 1024 * 1024


class ExactCrossSectionResolver(Protocol):
    def get(
        self,
        panel_id: str,
    ) -> VerifiedPromotedCrossSectionPanel: ...


@dataclass(frozen=True, slots=True)
class DecodedPromotedIntentRecord:
    source_panel_id: str
    config: PromotedIntentPolicyConfig
    entry_session: date
    initial_capital: Decimal
    batch_id: str

    def __post_init__(self) -> None:
        if (
            _SHA256.fullmatch(self.source_panel_id) is None
            or type(self.config) is not PromotedIntentPolicyConfig
            or type(self.entry_session) is not date
            or type(self.initial_capital) is not Decimal
            or not self.initial_capital.is_finite()
            or self.initial_capital <= 0
            or _SHA256.fullmatch(self.batch_id) is None
        ):
            raise PromotedIntentStoreConflict(
                "stored promoted-intent record is invalid"
            )


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
            raise PromotedIntentStoreConflict(
                "stored promoted-intent manifest has duplicate keys"
            )
        result[key] = value
    return result


def _object(
    value: object,
    expected: set[str],
    name: str,
) -> dict[str, object]:
    if type(value) is not dict or set(value) != expected:
        raise PromotedIntentStoreConflict(
            f"stored promoted-intent {name} has invalid fields"
        )
    return value


def _decimal(value: object, name: str) -> Decimal:
    if type(value) is not str:
        raise PromotedIntentStoreConflict(
            f"stored promoted-intent {name} must be a Decimal string"
        )
    try:
        result = Decimal(value)
    except InvalidOperation:
        raise PromotedIntentStoreConflict(
            f"stored promoted-intent {name} is invalid"
        ) from None
    if not result.is_finite():
        raise PromotedIntentStoreConflict(
            f"stored promoted-intent {name} must be finite"
        )
    return result


def _date(value: object, name: str) -> date:
    if type(value) is not str:
        raise PromotedIntentStoreConflict(
            f"stored promoted-intent {name} must be an ISO date"
        )
    try:
        result = date.fromisoformat(value)
    except ValueError:
        raise PromotedIntentStoreConflict(
            f"stored promoted-intent {name} is invalid"
        ) from None
    if result.isoformat() != value:
        raise PromotedIntentStoreConflict(
            f"stored promoted-intent {name} is not canonical"
        )
    return result


_DECIMAL_CONFIG_FIELDS = {
    "gross_exposure_fraction",
    "portfolio_risk_fraction",
    "minimum_ensemble_score",
    "minimum_median_traded_value",
    "minimum_signal_traded_value_ratio",
    "maximum_tick_fraction",
    "minimum_average_true_range_ticks",
    "maximum_annualized_volatility",
    "maximum_zero_volume_fraction",
    "stop_atr_multiple",
    "minimum_net_reward_risk",
    "round_trip_cost_buffer_fraction",
    "maximum_participation",
}


def _config_value(config: PromotedIntentPolicyConfig) -> dict[str, object]:
    result: dict[str, object] = {}
    for item in fields(PromotedIntentPolicyConfig):
        value = getattr(config, item.name)
        if item.name in _DECIMAL_CONFIG_FIELDS:
            result[item.name] = str(value)
        elif item.name == "allowed_regimes":
            result[item.name] = [regime.value for regime in value]
        else:
            result[item.name] = value
    return result


def _decode_config(value: object) -> PromotedIntentPolicyConfig:
    expected = {item.name for item in fields(PromotedIntentPolicyConfig)}
    raw = _object(value, expected, "configuration")
    allowed = raw["allowed_regimes"]
    if type(allowed) is not list:
        raise PromotedIntentStoreConflict(
            "stored promoted-intent allowed regimes must be a list"
        )
    stored_id = raw["config_id"]
    values: dict[str, object] = {
        name: _decimal(raw[name], name)
        for name in _DECIMAL_CONFIG_FIELDS
    }
    values.update(
        {
            "maximum_positions": raw["maximum_positions"],
            "maximum_holding_sessions": raw[
                "maximum_holding_sessions"
            ],
            "allowed_regimes": tuple(
                MarketRegime(item) for item in allowed
            ),
            "policy_version": raw["policy_version"],
            "schema_version": raw["schema_version"],
        }
    )
    config = PromotedIntentPolicyConfig(**values)
    if config.config_id != stored_id:
        raise PromotedIntentStoreConflict(
            "stored promoted-intent config ID differs from content"
        )
    return config


def _batch_projection(
    batch: VerifiedPromotedResearchIntentBatch,
) -> dict[str, object]:
    return {
        "batch_id": batch.batch_id,
        "signal_session": batch.signal_session.isoformat(),
        "selected_count": batch.selected_count,
        "blocked_count": batch.blocked_count,
        "source_universe_complete": batch.source_universe_complete,
        "decision_ids": [
            value.decision_id for value in batch.decisions
        ],
        "research_intent_ids": [
            value.research_intent_id for value in batch.intents
        ],
        "readiness": batch.readiness.value,
        "actionable": batch.actionable,
        "alert_eligible": batch.alert_eligible,
        "execution_eligible": batch.execution_eligible,
    }


def encode_promoted_intent_batch(
    batch: VerifiedPromotedResearchIntentBatch,
    config: PromotedIntentPolicyConfig,
) -> bytes:
    if (
        type(batch) is not VerifiedPromotedResearchIntentBatch
        or type(config) is not PromotedIntentPolicyConfig
        or batch.config_id != config.config_id
    ):
        raise PromotedIntentStoreConflict(
            "promoted-intent publication binding is invalid"
        )
    batch.verify_content_identity()
    config.verify_content_identity()
    return (
        json.dumps(
            {
                "store_schema_version": (
                    PROMOTED_INTENT_STORE_SCHEMA_VERSION
                ),
                "source_panel_id": batch.source_panel_id,
                "config": _config_value(config),
                "entry_session": batch.entry_session.isoformat(),
                "initial_capital": str(batch.initial_capital),
                "batch": _batch_projection(batch),
            },
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        )
        + "\n"
    ).encode("utf-8")


def decode_promoted_intent_record(
    payload: bytes,
) -> DecodedPromotedIntentRecord:
    try:
        raw = json.loads(
            payload.decode("utf-8"),
            object_pairs_hook=_unique_object,
            parse_float=lambda _: (_ for _ in ()).throw(ValueError()),
            parse_constant=lambda _: (_ for _ in ()).throw(ValueError()),
        )
        envelope = _object(
            raw,
            {
                "store_schema_version",
                "source_panel_id",
                "config",
                "entry_session",
                "initial_capital",
                "batch",
            },
            "envelope",
        )
        if (
            envelope["store_schema_version"]
            != PROMOTED_INTENT_STORE_SCHEMA_VERSION
        ):
            raise PromotedIntentStoreConflict(
                "stored promoted-intent schema is unsupported"
            )
        batch = _object(
            envelope["batch"],
            {
                "batch_id",
                "signal_session",
                "selected_count",
                "blocked_count",
                "source_universe_complete",
                "decision_ids",
                "research_intent_ids",
                "readiness",
                "actionable",
                "alert_eligible",
                "execution_eligible",
            },
            "batch projection",
        )
        for name in ("decision_ids", "research_intent_ids"):
            values = batch[name]
            if (
                type(values) is not list
                or any(
                    type(value) is not str
                    or _SHA256.fullmatch(value) is None
                    for value in values
                )
            ):
                raise PromotedIntentStoreConflict(
                    f"stored promoted-intent {name} is invalid"
                )
        if (
            type(batch["selected_count"]) is not int
            or batch["selected_count"] < 0
            or type(batch["blocked_count"]) is not int
            or batch["blocked_count"] < 0
            or type(batch["source_universe_complete"]) is not bool
            or batch["readiness"] != "COLLECTION_ONLY"
            or batch["actionable"] is not False
            or batch["alert_eligible"] is not False
            or batch["execution_eligible"] is not False
        ):
            raise PromotedIntentStoreConflict(
                "stored promoted-intent safety projection is invalid"
            )
        _date(batch["signal_session"], "signal_session")
        return DecodedPromotedIntentRecord(
            source_panel_id=envelope["source_panel_id"],
            config=_decode_config(envelope["config"]),
            entry_session=_date(
                envelope["entry_session"],
                "entry_session",
            ),
            initial_capital=_decimal(
                envelope["initial_capital"],
                "initial_capital",
            ),
            batch_id=batch["batch_id"],
        )
    except PromotedIntentStoreConflict:
        raise
    except (
        KeyError,
        TypeError,
        ValueError,
        UnicodeDecodeError,
        json.JSONDecodeError,
    ):
        raise PromotedIntentStoreConflict(
            "stored promoted-intent manifest is invalid"
        ) from None


class LocalPromotedResearchIntentStore:
    """Replays exact upstream content on every read; no latest lookup exists."""

    def __init__(
        self,
        root: Path,
        cross_section_resolver: ExactCrossSectionResolver,
    ) -> None:
        self.root = Path(root)
        if not callable(getattr(cross_section_resolver, "get", None)):
            raise TypeError("cross_section_resolver must expose exact get")
        self.cross_section_resolver = cross_section_resolver

    @property
    def batches_root(self) -> Path:
        return self.root / "promoted_research_intents"

    def path_for(self, batch_id: str) -> Path:
        if (
            type(batch_id) is not str
            or _SHA256.fullmatch(batch_id) is None
        ):
            raise PromotedIntentStoreError(
                "batch_id must be a full lowercase SHA-256"
            )
        return self.batches_root / f"{batch_id}.json"

    def _replay(
        self,
        record: DecodedPromotedIntentRecord,
    ) -> VerifiedPromotedResearchIntentBatch:
        try:
            source_panel = self.cross_section_resolver.get(
                record.source_panel_id
            )
        except Exception:
            raise PromotedIntentStoreConflict(
                "promoted-intent source panel could not be resolved"
            ) from None
        if (
            type(source_panel) is not VerifiedPromotedCrossSectionPanel
            or source_panel.panel_id != record.source_panel_id
        ):
            raise PromotedIntentStoreConflict(
                "promoted-intent source panel differs from binding"
            )
        try:
            batch = PromotedResearchIntentService().generate(
                source_panel=source_panel,
                config=record.config,
                entry_session=record.entry_session,
                initial_capital=record.initial_capital,
            )
        except Exception:
            raise PromotedIntentStoreConflict(
                "promoted-intent replay failed"
            ) from None
        if batch.batch_id != record.batch_id:
            raise PromotedIntentStoreConflict(
                "promoted-intent replay differs from stored ID"
            )
        return batch

    def put(
        self,
        batch: VerifiedPromotedResearchIntentBatch,
        config: PromotedIntentPolicyConfig,
    ) -> VerifiedPromotedResearchIntentBatch:
        payload = encode_promoted_intent_batch(batch, config)
        record = decode_promoted_intent_record(payload)
        replayed = self._replay(record)
        if replayed != batch:
            raise PromotedIntentStoreConflict(
                "promoted-intent publication differs from replay"
            )
        target = self.path_for(batch.batch_id)
        self.batches_root.mkdir(parents=True, exist_ok=True)
        if _is_link_like(self.batches_root):
            raise PromotedIntentStoreConflict(
                "promoted-intent store path cannot be a link"
            )
        try:
            with advisory_file_lock(
                self.batches_root / ".promoted-intents.lock"
            ):
                if target.exists():
                    stored = self.get(batch.batch_id)
                    if stored != batch:
                        raise PromotedIntentStoreConflict(
                            "batch ID already stores different content"
                        )
                    return stored
                descriptor, temporary_name = tempfile.mkstemp(
                    prefix=".promoted-intent-",
                    suffix=".tmp",
                    dir=self.batches_root,
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
            raise PromotedIntentStoreConflict(
                "promoted-intent store unavailable"
            ) from exc
        return self.get(batch.batch_id)

    def get(
        self,
        batch_id: str,
    ) -> VerifiedPromotedResearchIntentBatch:
        path = self.path_for(batch_id)
        if not path.exists():
            raise PromotedIntentStoreNotFound(batch_id)
        if not path.is_file() or _is_link_like(path):
            raise PromotedIntentStoreConflict(
                "promoted-intent artifact must be a regular file"
            )
        try:
            payload = read_stable_regular_file(
                path,
                maximum_bytes=_MAXIMUM_BYTES,
            )
        except FileSafetyError as exc:
            raise PromotedIntentStoreConflict(
                "promoted-intent artifact could not be read safely"
            ) from exc
        record = decode_promoted_intent_record(payload)
        if record.batch_id != batch_id:
            raise PromotedIntentStoreConflict(
                "promoted-intent path differs from content"
            )
        batch = self._replay(record)
        canonical = encode_promoted_intent_batch(batch, record.config)
        if canonical != payload:
            raise PromotedIntentStoreConflict(
                "promoted-intent manifest is noncanonical"
            )
        return batch

    def require_persisted(
        self,
        batch: VerifiedPromotedResearchIntentBatch,
    ) -> VerifiedPromotedResearchIntentBatch:
        if type(batch) is not VerifiedPromotedResearchIntentBatch:
            raise TypeError("batch must be exact")
        batch.verify_content_identity()
        stored = self.get(batch.batch_id)
        if stored != batch:
            raise PromotedIntentStoreConflict(
                "persisted promoted-intent batch differs"
            )
        return stored
