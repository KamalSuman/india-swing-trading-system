"""Deterministic, content-addressed assembly layer immediately below the
future promoted operational CLI.

Starting only from one strict, content-addressed
``PromotedOperationalAssemblySpec`` plus two minimal exact-ID resolver
ports, reconstructs the already-existing ``SwingQuoteGatePolicy``,
``PromotedOperationalAllocationPolicy``, ``PromotedOperationalQuoteGateSpec``,
``PromotedOperationalRunSpec``, ``PromotedOperationalPortfolioContext``, and
``PromotedOperationalRuntimeJobSpec`` -- never duplicating their financial
algorithms, risk rules, or identity formulas, and never serializing the
full nested engine graph. Returns one independently cross-checked
``PromotedOperationalRuntimeAssembly`` suitable for the already-accepted
``run_promoted_operational_runtime_job``.

This module never constructs a real storage/broker/Telegram client, never
reads an environment variable, credential, or the current time, never
calls Kite/GCS/Telegram, never mutates a store, and never executes a
runtime session. A CLI and the actual execution of the assembled runtime
remain the next, separately authorized increment.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field, fields
from datetime import date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Protocol

from india_swing._filesystem import read_stable_regular_file
from india_swing.domain.models import INDIA_STANDARD_TIME
from india_swing.identity import content_id
from india_swing.operations.portfolio_store import SwingPortfolioSnapshotArtifact
from india_swing.promoted_operational_allocation import (
    PromotedOperationalAllocationPolicy,
    PromotedOperationalPortfolioContext,
    _canonical_listing_keys,
)
from india_swing.promoted_operational_preparation import VerifiedPromotedOperationalPreparation
from india_swing.promoted_operational_quote_gate import PromotedOperationalQuoteGateSpec
from india_swing.promoted_operational_runner import (
    PromotedOperationalRunSpec,
    _MAXIMUM_QUOTE_CHUNK_SIZE,
    _MINIMUM_QUOTE_CHUNK_SIZE,
)
from india_swing.promoted_operational_runtime import (
    PromotedOperationalRuntimeJobSpec,
    build_promoted_operational_runtime_job_spec,
)
from india_swing.risk.swing_portfolio import SwingPortfolioSizingPolicy
from india_swing.signals.quote_gate import SwingQuoteGatePolicy


class PromotedOperationalAssemblyError(ValueError):
    pass


_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_BUCKET = re.compile(r"[a-z0-9][a-z0-9._-]{1,61}[a-z0-9]\Z")
_ERR_ASSEMBLY = "promoted operational assembly call is invalid"

PROMOTED_OPERATIONAL_ASSEMBLY_SPEC_SCHEMA_VERSION = "promoted-operational-assembly-spec/v1"
_ASSEMBLY_SPEC_CODEC_SCHEMA_VERSION = "promoted-operational-assembly-spec-json/v1"
MAXIMUM_ASSEMBLY_SPEC_BYTES = 16 * 1024

_CONCRETE_PATH_TYPE = type(Path())


def _require_literal_utc(value: object) -> datetime:
    """Require an exact ``datetime`` with a literal zero UTC offset,
    never normalizing a non-UTC-but-aware representation (e.g. IST) via
    ``astimezone`` -- this module defines its own copy (rather than
    reusing ``promoted_operational_runtime``'s private helper of the same
    shape) so a malformed datetime here always raises this module's own
    static ``PromotedOperationalAssemblyError``, never a foreign nested
    exception type."""

    if type(value) is not datetime:
        raise PromotedOperationalAssemblyError(_ERR_ASSEMBLY)
    offset_failed = False
    offset = None
    try:
        offset = value.utcoffset()
    except Exception:
        offset_failed = True
    if offset_failed:
        raise PromotedOperationalAssemblyError(_ERR_ASSEMBLY)
    if value.tzinfo is None or offset != timedelta(0):
        raise PromotedOperationalAssemblyError(_ERR_ASSEMBLY)
    return value


@dataclass(frozen=True, slots=True)
class PromotedOperationalAssemblySpec:
    """One exact, content-addressed launch binding of parent IDs, the
    exact decision window, the exact quote/allocation policies, the
    quote-source ID, explicit open-listing keys, the chunk ceiling, the
    terminal-binding bucket, and fixed paper-only authority.

    This is a binding, not a second engine spec: it never copies
    candidates, market facts, prices, forecasts, quantities, stops,
    targets, or research intent content. ``open_listing_keys`` exists
    because ``SwingPortfolioSnapshot`` intentionally retains only an
    open-position *count*, not listing identities -- this spec binds the
    exact, explicit, canonical evidence for what those open positions
    are; it is never inferred from candidates, symbols, filesystem
    contents, or the portfolio count itself.
    """

    preparation_id: str
    portfolio_artifact_id: str
    expected_portfolio_snapshot_id: str
    expected_quote_source_id: str
    open_listing_keys: tuple[str, ...]
    decision_not_before: datetime
    decision_deadline: datetime
    target_session: date
    quote_gate_policy: SwingQuoteGatePolicy
    allocation_policy: PromotedOperationalAllocationPolicy
    maximum_quote_chunk_size: int
    binding_bucket: str
    paper_only: bool = True
    notification_eligible: bool = False
    execution_eligible: bool = False
    schema_version: str = PROMOTED_OPERATIONAL_ASSEMBLY_SPEC_SCHEMA_VERSION
    assembly_spec_id: str = field(init=False)

    def __post_init__(self) -> None:
        if self.schema_version != PROMOTED_OPERATIONAL_ASSEMBLY_SPEC_SCHEMA_VERSION:
            raise PromotedOperationalAssemblyError(_ERR_ASSEMBLY)
        for value in (
            self.preparation_id,
            self.portfolio_artifact_id,
            self.expected_portfolio_snapshot_id,
            self.expected_quote_source_id,
        ):
            if type(value) is not str or _SHA256.fullmatch(value) is None:
                raise PromotedOperationalAssemblyError(_ERR_ASSEMBLY)
        if not _canonical_listing_keys(self.open_listing_keys):
            raise PromotedOperationalAssemblyError(_ERR_ASSEMBLY)

        not_before = _require_literal_utc(self.decision_not_before)
        deadline = _require_literal_utc(self.decision_deadline)
        if not_before >= deadline:
            raise PromotedOperationalAssemblyError(_ERR_ASSEMBLY)
        if type(self.target_session) is not date:
            raise PromotedOperationalAssemblyError(_ERR_ASSEMBLY)
        if (
            not_before.astimezone(INDIA_STANDARD_TIME).date() != self.target_session
            or deadline.astimezone(INDIA_STANDARD_TIME).date() != self.target_session
        ):
            raise PromotedOperationalAssemblyError(_ERR_ASSEMBLY)

        if type(self.quote_gate_policy) is not SwingQuoteGatePolicy:
            raise PromotedOperationalAssemblyError(_ERR_ASSEMBLY)
        quote_gate_policy_failed = False
        try:
            self.quote_gate_policy.verify_content_identity()
        except Exception:
            quote_gate_policy_failed = True
        if quote_gate_policy_failed:
            raise PromotedOperationalAssemblyError(_ERR_ASSEMBLY)

        if type(self.allocation_policy) is not PromotedOperationalAllocationPolicy:
            raise PromotedOperationalAssemblyError(_ERR_ASSEMBLY)
        allocation_policy_failed = False
        try:
            self.allocation_policy.verify_content_identity()
        except Exception:
            allocation_policy_failed = True
        if allocation_policy_failed:
            raise PromotedOperationalAssemblyError(_ERR_ASSEMBLY)

        if type(self.maximum_quote_chunk_size) is not int or not (
            _MINIMUM_QUOTE_CHUNK_SIZE <= self.maximum_quote_chunk_size <= _MAXIMUM_QUOTE_CHUNK_SIZE
        ):
            raise PromotedOperationalAssemblyError(_ERR_ASSEMBLY)

        if type(self.binding_bucket) is not str or _BUCKET.fullmatch(self.binding_bucket) is None:
            raise PromotedOperationalAssemblyError(_ERR_ASSEMBLY)

        if (
            self.paper_only is not True
            or self.notification_eligible is not False
            or self.execution_eligible is not False
        ):
            raise PromotedOperationalAssemblyError(_ERR_ASSEMBLY)

        object.__setattr__(self, "assembly_spec_id", self._calculated_id())

    def _identity(self) -> dict[str, object]:
        return {
            item.name: getattr(self, item.name)
            for item in fields(self)
            if item.name != "assembly_spec_id"
        }

    def _calculated_id(self) -> str:
        return content_id(self._identity(), length=64)

    def verify_content_identity(self) -> None:
        """Reconstruct a fresh instance from this object's own retained
        field values -- rerunning every validation in ``__post_init__``,
        including replaying both nested policy objects -- and require its
        re-derived ID to match. Never merely compares a caller-supplied
        hash."""

        if type(self) is not PromotedOperationalAssemblySpec:
            raise PromotedOperationalAssemblyError(_ERR_ASSEMBLY)
        reconstruct_failed = False
        reraise: PromotedOperationalAssemblyError | None = None
        fresh: PromotedOperationalAssemblySpec | None = None
        try:
            fresh = PromotedOperationalAssemblySpec(**self._identity())
        except PromotedOperationalAssemblyError as error:
            reraise = error
        except Exception:
            reconstruct_failed = True
        if reraise is not None:
            raise reraise
        if reconstruct_failed or fresh is None:
            raise PromotedOperationalAssemblyError(_ERR_ASSEMBLY)
        if self.assembly_spec_id != fresh.assembly_spec_id:
            raise PromotedOperationalAssemblyError(_ERR_ASSEMBLY)


def _decimal_str(value: object) -> str:
    if type(value) is not Decimal or not value.is_finite():
        raise PromotedOperationalAssemblyError(_ERR_ASSEMBLY)
    return str(value)


def _parse_canonical_decimal(value: object) -> Decimal:
    if type(value) is not str:
        raise ValueError("not a canonical decimal string")
    result = Decimal(value)
    if not result.is_finite() or str(result) != value:
        raise ValueError("not a canonical decimal string")
    return result


_SIZING_POLICY_KEYS = frozenset(
    {
        "per_trade_risk_fraction",
        "maximum_total_open_risk_fraction",
        "maximum_position_notional_fraction",
        "maximum_gross_exposure_fraction",
        "maximum_daily_turnover_participation",
        "maximum_top_ask_participation",
        "maximum_daily_loss_fraction",
        "maximum_pilot_drawdown_fraction",
        "minimum_net_reward_risk",
        "maximum_open_positions",
        "maximum_new_positions_per_run",
        "policy_version",
        "policy_id",
    }
)


def _sizing_policy_body(value: SwingPortfolioSizingPolicy) -> dict[str, object]:
    return {
        "per_trade_risk_fraction": _decimal_str(value.per_trade_risk_fraction),
        "maximum_total_open_risk_fraction": _decimal_str(value.maximum_total_open_risk_fraction),
        "maximum_position_notional_fraction": _decimal_str(
            value.maximum_position_notional_fraction
        ),
        "maximum_gross_exposure_fraction": _decimal_str(value.maximum_gross_exposure_fraction),
        "maximum_daily_turnover_participation": _decimal_str(
            value.maximum_daily_turnover_participation
        ),
        "maximum_top_ask_participation": _decimal_str(value.maximum_top_ask_participation),
        "maximum_daily_loss_fraction": _decimal_str(value.maximum_daily_loss_fraction),
        "maximum_pilot_drawdown_fraction": _decimal_str(value.maximum_pilot_drawdown_fraction),
        "minimum_net_reward_risk": _decimal_str(value.minimum_net_reward_risk),
        "maximum_open_positions": value.maximum_open_positions,
        "maximum_new_positions_per_run": value.maximum_new_positions_per_run,
        "policy_version": value.policy_version,
        "policy_id": value.policy_id,
    }


def _decode_sizing_policy(raw: object) -> SwingPortfolioSizingPolicy:
    if type(raw) is not dict or set(raw) != _SIZING_POLICY_KEYS:
        raise PromotedOperationalAssemblyError(_ERR_ASSEMBLY)
    construction_failed = False
    value: SwingPortfolioSizingPolicy | None = None
    try:
        value = SwingPortfolioSizingPolicy(
            per_trade_risk_fraction=_parse_canonical_decimal(raw["per_trade_risk_fraction"]),
            maximum_total_open_risk_fraction=_parse_canonical_decimal(
                raw["maximum_total_open_risk_fraction"]
            ),
            maximum_position_notional_fraction=_parse_canonical_decimal(
                raw["maximum_position_notional_fraction"]
            ),
            maximum_gross_exposure_fraction=_parse_canonical_decimal(
                raw["maximum_gross_exposure_fraction"]
            ),
            maximum_daily_turnover_participation=_parse_canonical_decimal(
                raw["maximum_daily_turnover_participation"]
            ),
            maximum_top_ask_participation=_parse_canonical_decimal(
                raw["maximum_top_ask_participation"]
            ),
            maximum_daily_loss_fraction=_parse_canonical_decimal(
                raw["maximum_daily_loss_fraction"]
            ),
            maximum_pilot_drawdown_fraction=_parse_canonical_decimal(
                raw["maximum_pilot_drawdown_fraction"]
            ),
            minimum_net_reward_risk=_parse_canonical_decimal(raw["minimum_net_reward_risk"]),
            maximum_open_positions=raw["maximum_open_positions"],
            maximum_new_positions_per_run=raw["maximum_new_positions_per_run"],
            policy_version=raw["policy_version"],
        )
    except Exception:
        construction_failed = True
    if construction_failed or value is None:
        raise PromotedOperationalAssemblyError(_ERR_ASSEMBLY)
    if value.policy_id != raw["policy_id"]:
        raise PromotedOperationalAssemblyError(_ERR_ASSEMBLY)
    return value


_QUOTE_GATE_POLICY_KEYS = frozenset(
    {
        "maximum_batch_collection_seconds",
        "maximum_quote_age_seconds",
        "maximum_last_trade_age_seconds",
        "maximum_spread_bps",
        "policy_version",
        "policy_id",
    }
)


def _quote_gate_policy_body(value: SwingQuoteGatePolicy) -> dict[str, object]:
    return {
        "maximum_batch_collection_seconds": value.maximum_batch_collection_seconds,
        "maximum_quote_age_seconds": value.maximum_quote_age_seconds,
        "maximum_last_trade_age_seconds": value.maximum_last_trade_age_seconds,
        "maximum_spread_bps": _decimal_str(value.maximum_spread_bps),
        "policy_version": value.policy_version,
        "policy_id": value.policy_id,
    }


def _decode_quote_gate_policy(raw: object) -> SwingQuoteGatePolicy:
    if type(raw) is not dict or set(raw) != _QUOTE_GATE_POLICY_KEYS:
        raise PromotedOperationalAssemblyError(_ERR_ASSEMBLY)
    construction_failed = False
    value: SwingQuoteGatePolicy | None = None
    try:
        value = SwingQuoteGatePolicy(
            maximum_batch_collection_seconds=raw["maximum_batch_collection_seconds"],
            maximum_quote_age_seconds=raw["maximum_quote_age_seconds"],
            maximum_last_trade_age_seconds=raw["maximum_last_trade_age_seconds"],
            maximum_spread_bps=_parse_canonical_decimal(raw["maximum_spread_bps"]),
            policy_version=raw["policy_version"],
        )
    except Exception:
        construction_failed = True
    if construction_failed or value is None:
        raise PromotedOperationalAssemblyError(_ERR_ASSEMBLY)
    if value.policy_id != raw["policy_id"]:
        raise PromotedOperationalAssemblyError(_ERR_ASSEMBLY)
    return value


_ALLOCATION_POLICY_KEYS = frozenset(
    {
        "sizing_policy",
        "maximum_portfolio_age_seconds",
        "paper_only",
        "notification_eligible",
        "execution_eligible",
        "allocation_policy_id",
    }
)


def _allocation_policy_body(value: PromotedOperationalAllocationPolicy) -> dict[str, object]:
    return {
        "sizing_policy": _sizing_policy_body(value.policy),
        "maximum_portfolio_age_seconds": value.maximum_portfolio_age_seconds,
        "paper_only": value.paper_only,
        "notification_eligible": value.notification_eligible,
        "execution_eligible": value.execution_eligible,
        "allocation_policy_id": value.allocation_policy_id,
    }


def _decode_allocation_policy(raw: object) -> PromotedOperationalAllocationPolicy:
    if type(raw) is not dict or set(raw) != _ALLOCATION_POLICY_KEYS:
        raise PromotedOperationalAssemblyError(_ERR_ASSEMBLY)
    sizing_policy = _decode_sizing_policy(raw["sizing_policy"])
    construction_failed = False
    value: PromotedOperationalAllocationPolicy | None = None
    try:
        value = PromotedOperationalAllocationPolicy(
            policy=sizing_policy,
            maximum_portfolio_age_seconds=raw["maximum_portfolio_age_seconds"],
            paper_only=raw["paper_only"],
            notification_eligible=raw["notification_eligible"],
            execution_eligible=raw["execution_eligible"],
        )
    except Exception:
        construction_failed = True
    if construction_failed or value is None:
        raise PromotedOperationalAssemblyError(_ERR_ASSEMBLY)
    if value.allocation_policy_id != raw["allocation_policy_id"]:
        raise PromotedOperationalAssemblyError(_ERR_ASSEMBLY)
    return value


_ENVELOPE_KEYS = frozenset({"codec_schema_version", "assembly_spec"})
_BODY_KEYS = frozenset(
    {
        "schema_version",
        "preparation_id",
        "portfolio_artifact_id",
        "expected_portfolio_snapshot_id",
        "expected_quote_source_id",
        "open_listing_keys",
        "decision_not_before",
        "decision_deadline",
        "target_session",
        "quote_gate_policy",
        "allocation_policy",
        "maximum_quote_chunk_size",
        "binding_bucket",
        "paper_only",
        "notification_eligible",
        "execution_eligible",
        "assembly_spec_id",
    }
)


def _assembly_spec_body(value: PromotedOperationalAssemblySpec) -> dict[str, object]:
    return {
        "schema_version": value.schema_version,
        "preparation_id": value.preparation_id,
        "portfolio_artifact_id": value.portfolio_artifact_id,
        "expected_portfolio_snapshot_id": value.expected_portfolio_snapshot_id,
        "expected_quote_source_id": value.expected_quote_source_id,
        "open_listing_keys": list(value.open_listing_keys),
        "decision_not_before": value.decision_not_before.isoformat(),
        "decision_deadline": value.decision_deadline.isoformat(),
        "target_session": value.target_session.isoformat(),
        "quote_gate_policy": _quote_gate_policy_body(value.quote_gate_policy),
        "allocation_policy": _allocation_policy_body(value.allocation_policy),
        "maximum_quote_chunk_size": value.maximum_quote_chunk_size,
        "binding_bucket": value.binding_bucket,
        "paper_only": value.paper_only,
        "notification_eligible": value.notification_eligible,
        "execution_eligible": value.execution_eligible,
        "assembly_spec_id": value.assembly_spec_id,
    }


def encode_promoted_operational_assembly_spec(value: PromotedOperationalAssemblySpec) -> bytes:
    if type(value) is not PromotedOperationalAssemblySpec:
        raise PromotedOperationalAssemblyError(_ERR_ASSEMBLY)
    value.verify_content_identity()
    payload = (
        json.dumps(
            {
                "codec_schema_version": _ASSEMBLY_SPEC_CODEC_SCHEMA_VERSION,
                "assembly_spec": _assembly_spec_body(value),
            },
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )
        + "\n"
    ).encode("utf-8")
    if len(payload) > MAXIMUM_ASSEMBLY_SPEC_BYTES:
        raise PromotedOperationalAssemblyError(_ERR_ASSEMBLY)
    return payload


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise PromotedOperationalAssemblyError(_ERR_ASSEMBLY)
        result[key] = value
    return result


def _reject_number(_token: str) -> object:
    raise PromotedOperationalAssemblyError(_ERR_ASSEMBLY)


def decode_promoted_operational_assembly_spec(payload: bytes) -> PromotedOperationalAssemblySpec:
    if (
        type(payload) is not bytes
        or not payload
        or len(payload) > MAXIMUM_ASSEMBLY_SPEC_BYTES
    ):
        raise PromotedOperationalAssemblyError(_ERR_ASSEMBLY)

    decode_failed = False
    text = ""
    try:
        text = payload.decode("utf-8", errors="strict")
    except UnicodeDecodeError:
        decode_failed = True
    if decode_failed:
        raise PromotedOperationalAssemblyError(_ERR_ASSEMBLY)

    parse_failed = False
    reraise: PromotedOperationalAssemblyError | None = None
    root: object = None
    try:
        root = json.loads(
            text,
            object_pairs_hook=_unique_object,
            parse_float=_reject_number,
            parse_constant=_reject_number,
        )
    except PromotedOperationalAssemblyError as error:
        reraise = error
    except (json.JSONDecodeError, RecursionError):
        parse_failed = True
    if reraise is not None:
        raise reraise
    if parse_failed:
        raise PromotedOperationalAssemblyError(_ERR_ASSEMBLY)

    if (
        type(root) is not dict
        or set(root) != _ENVELOPE_KEYS
        or root["codec_schema_version"] != _ASSEMBLY_SPEC_CODEC_SCHEMA_VERSION
    ):
        raise PromotedOperationalAssemblyError(_ERR_ASSEMBLY)
    raw = root["assembly_spec"]
    if type(raw) is not dict or set(raw) != _BODY_KEYS:
        raise PromotedOperationalAssemblyError(_ERR_ASSEMBLY)

    quote_gate_policy_failed = False
    quote_gate_policy: SwingQuoteGatePolicy | None = None
    try:
        quote_gate_policy = _decode_quote_gate_policy(raw["quote_gate_policy"])
    except Exception:
        quote_gate_policy_failed = True
    if quote_gate_policy_failed or quote_gate_policy is None:
        raise PromotedOperationalAssemblyError(_ERR_ASSEMBLY)

    allocation_policy_failed = False
    allocation_policy: PromotedOperationalAllocationPolicy | None = None
    try:
        allocation_policy = _decode_allocation_policy(raw["allocation_policy"])
    except Exception:
        allocation_policy_failed = True
    if allocation_policy_failed or allocation_policy is None:
        raise PromotedOperationalAssemblyError(_ERR_ASSEMBLY)

    construction_failed = False
    value: PromotedOperationalAssemblySpec | None = None
    try:
        value = PromotedOperationalAssemblySpec(
            schema_version=raw["schema_version"],
            preparation_id=raw["preparation_id"],
            portfolio_artifact_id=raw["portfolio_artifact_id"],
            expected_portfolio_snapshot_id=raw["expected_portfolio_snapshot_id"],
            expected_quote_source_id=raw["expected_quote_source_id"],
            open_listing_keys=tuple(raw["open_listing_keys"]),
            decision_not_before=datetime.fromisoformat(raw["decision_not_before"]),
            decision_deadline=datetime.fromisoformat(raw["decision_deadline"]),
            target_session=date.fromisoformat(raw["target_session"]),
            quote_gate_policy=quote_gate_policy,
            allocation_policy=allocation_policy,
            maximum_quote_chunk_size=raw["maximum_quote_chunk_size"],
            binding_bucket=raw["binding_bucket"],
            paper_only=raw["paper_only"],
            notification_eligible=raw["notification_eligible"],
            execution_eligible=raw["execution_eligible"],
        )
    except Exception:
        construction_failed = True
    if construction_failed or value is None:
        raise PromotedOperationalAssemblyError(_ERR_ASSEMBLY)
    if value.assembly_spec_id != raw["assembly_spec_id"]:
        raise PromotedOperationalAssemblyError(_ERR_ASSEMBLY)
    if encode_promoted_operational_assembly_spec(value) != payload:
        raise PromotedOperationalAssemblyError(_ERR_ASSEMBLY)
    return value


def load_promoted_operational_assembly_spec_file(
    path: Path,
) -> PromotedOperationalAssemblySpec:
    """Load and strictly decode one assembly spec from a concrete,
    absolute, non-traversing local path. ``path`` must be exactly the
    platform's concrete ``pathlib.Path`` subclass -- an arbitrary ``Path``
    subclass instance is rejected before any of its (possibly overridden)
    path methods or filesystem behavior can be used. No filesystem
    discovery, list/latest/nearest/glob behavior, symlink following,
    fallback, or default path exists here -- ``read_stable_regular_file``
    already rejects symlinks/reparse points and concurrent mutation."""

    if type(path) is not _CONCRETE_PATH_TYPE:
        raise PromotedOperationalAssemblyError(_ERR_ASSEMBLY)
    if not path.is_absolute() or ".." in path.parts:
        raise PromotedOperationalAssemblyError(_ERR_ASSEMBLY)

    read_failed = False
    payload = b""
    try:
        payload = read_stable_regular_file(path, maximum_bytes=MAXIMUM_ASSEMBLY_SPEC_BYTES)
    except Exception:
        read_failed = True
    if read_failed:
        raise PromotedOperationalAssemblyError(_ERR_ASSEMBLY)

    decode_failed = False
    value: PromotedOperationalAssemblySpec | None = None
    try:
        value = decode_promoted_operational_assembly_spec(payload)
    except Exception:
        decode_failed = True
    if decode_failed or value is None:
        raise PromotedOperationalAssemblyError(_ERR_ASSEMBLY)
    return value


class PromotedOperationalPreparationResolver(Protocol):
    """Minimal read-only port: exact-ID lookup only. No list/latest/
    nearest/find/glob API exists here or is ever assumed."""

    def get(self, preparation_id: str) -> VerifiedPromotedOperationalPreparation: ...


class SwingPortfolioArtifactResolver(Protocol):
    """Minimal read-only port: exact-ID lookup only. No list/latest/
    nearest/find/glob API exists here or is ever assumed."""

    def get(self, artifact_id: str) -> SwingPortfolioSnapshotArtifact: ...


@dataclass(frozen=True, slots=True)
class PromotedOperationalRuntimeAssembly:
    """Fixed-shape result of one assembly call: the exact assembly spec,
    its two resolved parents, and the three reconstructed engine-layer
    objects the runtime orchestrator requires.

    ``__post_init__`` independently replays every retained object and
    cross-checks all available parent IDs, target session, decision
    window, both policies, source IDs, explicit open-listing keys, chunk
    size, bucket, and paper-only authority -- it introduces no new
    financial value anywhere.
    """

    assembly_spec: PromotedOperationalAssemblySpec
    preparation: VerifiedPromotedOperationalPreparation
    portfolio_artifact: SwingPortfolioSnapshotArtifact
    portfolio_context: PromotedOperationalPortfolioContext
    run_spec: PromotedOperationalRunSpec
    runtime_job_spec: PromotedOperationalRuntimeJobSpec

    def __post_init__(self) -> None:
        if (
            type(self.assembly_spec) is not PromotedOperationalAssemblySpec
            or type(self.preparation) is not VerifiedPromotedOperationalPreparation
            or type(self.portfolio_artifact) is not SwingPortfolioSnapshotArtifact
            or type(self.portfolio_context) is not PromotedOperationalPortfolioContext
            or type(self.run_spec) is not PromotedOperationalRunSpec
            or type(self.runtime_job_spec) is not PromotedOperationalRuntimeJobSpec
        ):
            raise PromotedOperationalAssemblyError(_ERR_ASSEMBLY)

        replay_failed = False
        try:
            self.assembly_spec.verify_content_identity()
            self.preparation.verify_content_identity()
            self.portfolio_artifact.verify_content_identity()
            self.portfolio_context.verify_content_identity()
            self.run_spec.verify_content_identity()
            self.runtime_job_spec.verify_content_identity()
        except Exception:
            replay_failed = True
        if replay_failed:
            raise PromotedOperationalAssemblyError(_ERR_ASSEMBLY)

        cross_check_failed = False
        mismatch = True
        try:
            manifest = self.preparation.manifest
            quote_gate_spec = self.run_spec.quote_gate_spec
            job_spec = self.runtime_job_spec
            mismatch = (
                self.assembly_spec.preparation_id != manifest.preparation_id
                or self.assembly_spec.preparation_id
                != quote_gate_spec.preparation.manifest.preparation_id
                or self.assembly_spec.preparation_id != job_spec.preparation_id
                or self.assembly_spec.target_session != manifest.target_session
                or self.assembly_spec.target_session != job_spec.target_session
                or self.assembly_spec.portfolio_artifact_id != self.portfolio_artifact.artifact_id
                or self.assembly_spec.expected_portfolio_snapshot_id
                != self.portfolio_artifact.portfolio_snapshot_id
                or self.assembly_spec.expected_portfolio_snapshot_id
                != self.portfolio_context.portfolio.portfolio_snapshot_id
                or self.assembly_spec.open_listing_keys != self.portfolio_context.open_listing_keys
                or self.portfolio_artifact.artifact_id
                != self.portfolio_context.source_portfolio_artifact_id
                or self.assembly_spec.decision_not_before != quote_gate_spec.decision_not_before
                or self.assembly_spec.decision_not_before != job_spec.decision_not_before
                or self.assembly_spec.decision_deadline != quote_gate_spec.decision_deadline
                or self.assembly_spec.decision_deadline != job_spec.decision_deadline
                or self.assembly_spec.quote_gate_policy.policy_id != quote_gate_spec.policy.policy_id
                or self.assembly_spec.allocation_policy.allocation_policy_id
                != self.run_spec.allocation_policy.allocation_policy_id
                or self.assembly_spec.expected_quote_source_id
                != self.run_spec.expected_quote_source_id
                or self.assembly_spec.expected_quote_source_id != job_spec.expected_quote_source_id
                or self.portfolio_context.source_portfolio_artifact_id
                != self.run_spec.expected_portfolio_source_id
                or self.portfolio_context.source_portfolio_artifact_id
                != job_spec.expected_portfolio_source_id
                or self.portfolio_artifact.artifact_id != job_spec.expected_portfolio_source_id
                or self.portfolio_context.context_id != job_spec.expected_portfolio_context_id
                or self.assembly_spec.maximum_quote_chunk_size
                != self.run_spec.maximum_quote_chunk_size
                or self.assembly_spec.binding_bucket != job_spec.binding_bucket
                or self.run_spec.spec_id != job_spec.operational_run_spec_id
                or self.assembly_spec.paper_only is not self.run_spec.paper_only
                or self.assembly_spec.paper_only is not job_spec.paper_only
                or self.assembly_spec.notification_eligible is not self.run_spec.notification_eligible
                or self.assembly_spec.notification_eligible is not job_spec.notification_eligible
                or self.assembly_spec.execution_eligible is not self.run_spec.execution_eligible
                or self.assembly_spec.execution_eligible is not job_spec.execution_eligible
            )
        except Exception:
            cross_check_failed = True
        if cross_check_failed or mismatch:
            raise PromotedOperationalAssemblyError(_ERR_ASSEMBLY)


def assemble_promoted_operational_runtime_inputs(
    *,
    spec: PromotedOperationalAssemblySpec,
    preparation_resolver: PromotedOperationalPreparationResolver,
    portfolio_artifact_resolver: SwingPortfolioArtifactResolver,
) -> PromotedOperationalRuntimeAssembly:
    """Reconstruct every downstream engine-layer object from ``spec`` and
    its two exactly-resolved parents.

    ``spec`` is independently re-verified before either resolver is ever
    called. ``preparation_resolver.get(spec.preparation_id)`` is called
    exactly once; only after its exact result is verified and its
    ``target_session`` is confirmed to agree with ``spec`` does
    ``portfolio_artifact_resolver.get(spec.portfolio_artifact_id)`` get
    called, exactly once. Portfolio freshness (``as_of`` within
    ``[decision_not_before - maximum_portfolio_age_seconds,
    decision_deadline]``) and the explicit open-listing-key count (must
    equal ``portfolio.open_positions``) are both required before any
    child object is constructed. Any resolver exception, wrong return
    type, mismatched/tampered parent, stale/future portfolio, or
    construction failure fails closed under the static assembly error --
    never retried, never falling back, never discovering another
    artifact, and never calling either resolver more than once.
    """

    if type(spec) is not PromotedOperationalAssemblySpec:
        raise PromotedOperationalAssemblyError(_ERR_ASSEMBLY)
    spec_verify_failed = False
    try:
        spec.verify_content_identity()
    except Exception:
        spec_verify_failed = True
    if spec_verify_failed:
        raise PromotedOperationalAssemblyError(_ERR_ASSEMBLY)

    preparation_failed = False
    preparation: VerifiedPromotedOperationalPreparation | None = None
    try:
        preparation = preparation_resolver.get(spec.preparation_id)
    except Exception:
        preparation_failed = True
    if preparation_failed:
        raise PromotedOperationalAssemblyError(_ERR_ASSEMBLY)
    if type(preparation) is not VerifiedPromotedOperationalPreparation:
        raise PromotedOperationalAssemblyError(_ERR_ASSEMBLY)

    preparation_verify_failed = False
    try:
        preparation.verify_content_identity()
    except Exception:
        preparation_verify_failed = True
    if preparation_verify_failed:
        raise PromotedOperationalAssemblyError(_ERR_ASSEMBLY)

    # Only after a successful replay may nested manifest fields be read --
    # and even then, wrapped: a malformed/tampered manifest (e.g. its own
    # ``manifest`` attribute structurally replaced) must never leak a
    # foreign AttributeError or any other exception out of this boundary.
    preparation_identity_failed = False
    preparation_identity_mismatch = True
    try:
        preparation_identity_mismatch = (
            preparation.manifest.preparation_id != spec.preparation_id
            or preparation.manifest.target_session != spec.target_session
        )
    except Exception:
        preparation_identity_failed = True
    if preparation_identity_failed or preparation_identity_mismatch:
        raise PromotedOperationalAssemblyError(_ERR_ASSEMBLY)

    artifact_failed = False
    artifact: SwingPortfolioSnapshotArtifact | None = None
    try:
        artifact = portfolio_artifact_resolver.get(spec.portfolio_artifact_id)
    except Exception:
        artifact_failed = True
    if artifact_failed:
        raise PromotedOperationalAssemblyError(_ERR_ASSEMBLY)
    if (
        type(artifact) is not SwingPortfolioSnapshotArtifact
        or artifact.artifact_id != spec.portfolio_artifact_id
        or artifact.portfolio_snapshot_id != spec.expected_portfolio_snapshot_id
    ):
        raise PromotedOperationalAssemblyError(_ERR_ASSEMBLY)
    artifact_verify_failed = False
    try:
        artifact.verify_content_identity()
    except Exception:
        artifact_verify_failed = True
    if artifact_verify_failed:
        raise PromotedOperationalAssemblyError(_ERR_ASSEMBLY)

    portfolio = artifact.portfolio
    freshness_failed = False
    fresh = False
    try:
        earliest_allowed = spec.decision_not_before - timedelta(
            seconds=spec.allocation_policy.maximum_portfolio_age_seconds
        )
        fresh = earliest_allowed <= portfolio.as_of <= spec.decision_deadline
    except Exception:
        freshness_failed = True
    if freshness_failed or not fresh:
        raise PromotedOperationalAssemblyError(_ERR_ASSEMBLY)
    if len(spec.open_listing_keys) != portfolio.open_positions:
        raise PromotedOperationalAssemblyError(_ERR_ASSEMBLY)

    build_failed = False
    context: PromotedOperationalPortfolioContext | None = None
    run_spec: PromotedOperationalRunSpec | None = None
    runtime_job_spec: PromotedOperationalRuntimeJobSpec | None = None
    try:
        context = PromotedOperationalPortfolioContext(
            portfolio=portfolio,
            source_portfolio_artifact_id=artifact.artifact_id,
            open_listing_keys=spec.open_listing_keys,
        )
        quote_gate_spec = PromotedOperationalQuoteGateSpec(
            preparation=preparation,
            decision_not_before=spec.decision_not_before,
            decision_deadline=spec.decision_deadline,
            policy=spec.quote_gate_policy,
            paper_only=True,
            notification_eligible=False,
            execution_eligible=False,
        )
        run_spec = PromotedOperationalRunSpec(
            quote_gate_spec=quote_gate_spec,
            allocation_policy=spec.allocation_policy,
            expected_quote_source_id=spec.expected_quote_source_id,
            expected_portfolio_source_id=context.source_portfolio_artifact_id,
            maximum_quote_chunk_size=spec.maximum_quote_chunk_size,
        )
        runtime_job_spec = build_promoted_operational_runtime_job_spec(
            run_spec=run_spec, portfolio_context=context, binding_bucket=spec.binding_bucket
        )
    except Exception:
        build_failed = True
    if build_failed or context is None or run_spec is None or runtime_job_spec is None:
        raise PromotedOperationalAssemblyError(_ERR_ASSEMBLY)

    assembly_failed = False
    assembly: PromotedOperationalRuntimeAssembly | None = None
    try:
        assembly = PromotedOperationalRuntimeAssembly(
            assembly_spec=spec,
            preparation=preparation,
            portfolio_artifact=artifact,
            portfolio_context=context,
            run_spec=run_spec,
            runtime_job_spec=runtime_job_spec,
        )
    except Exception:
        assembly_failed = True
    if assembly_failed or assembly is None:
        raise PromotedOperationalAssemblyError(_ERR_ASSEMBLY)
    return assembly
