"""Deterministic, offline session launch-spec preparer.

Turns one strict, human-authored ``PromotedOperationalLaunchRequest`` plus
one exact promoted preparation and one exact portfolio artifact into the
canonical ``PromotedOperationalAssemblySpec`` file consumed by
``india-swing-promoted-operational-job``. Resolves parents only by exact
ID, derives every redundant lineage/session/snapshot/policy ID through
the already-accepted constructors (``SwingQuoteGatePolicy``,
``SwingPortfolioSizingPolicy``, ``PromotedOperationalAllocationPolicy``,
``PromotedOperationalAssemblySpec``), validates the complete assembly via
the unchanged ``assemble_promoted_operational_runtime_inputs`` before any
output is published, and never executes the promoted engine.

This module has no clock, environment, credential, or Kite/GCP/Telegram/
network/broker/runtime/deployment capability. It reads only the explicit
local stores/files a caller injects and writes only one explicit local
assembly-spec file, using a bounded create-once writer that never
truncates, overwrites, replaces, or deletes.
"""

from __future__ import annotations

import json
import os
import re
import stat
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Protocol

from india_swing._filesystem import read_stable_regular_file
from india_swing.domain.models import INDIA_STANDARD_TIME
from india_swing.operations.portfolio_store import SwingPortfolioSnapshotArtifact
from india_swing.promoted_operational_allocation import (
    PromotedOperationalAllocationPolicy,
    _canonical_listing_keys,
)
from india_swing.promoted_operational_assembly import (
    MAXIMUM_ASSEMBLY_SPEC_BYTES,
    PromotedOperationalAssemblySpec,
    PromotedOperationalRuntimeAssembly,
    _CONCRETE_PATH_TYPE,
    assemble_promoted_operational_runtime_inputs,
    decode_promoted_operational_assembly_spec,
    encode_promoted_operational_assembly_spec,
)
from india_swing.promoted_operational_preparation import VerifiedPromotedOperationalPreparation
from india_swing.risk.swing_portfolio import SwingPortfolioSizingPolicy
from india_swing.signals.quote_gate import SwingQuoteGatePolicy


class PromotedOperationalLaunchError(ValueError):
    pass


_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_BUCKET = re.compile(r"[a-z0-9][a-z0-9._-]{1,61}[a-z0-9]\Z")
_ERR_LAUNCH = "promoted operational launch call is invalid"

PROMOTED_OPERATIONAL_LAUNCH_REQUEST_SCHEMA_VERSION = "promoted-operational-launch-request/v1"
MAXIMUM_LAUNCH_REQUEST_BYTES = 64 * 1024


def _require_literal_utc(value: object) -> datetime:
    """Require an exact ``datetime`` with a literal zero UTC offset, never
    normalizing a non-UTC-but-aware representation -- this module defines
    its own copy (rather than importing another module's private helper
    of the same shape) so a malformed datetime here always raises this
    module's own static ``PromotedOperationalLaunchError``."""

    if type(value) is not datetime:
        raise PromotedOperationalLaunchError(_ERR_LAUNCH)
    offset_failed = False
    offset = None
    try:
        offset = value.utcoffset()
    except Exception:
        offset_failed = True
    if offset_failed:
        raise PromotedOperationalLaunchError(_ERR_LAUNCH)
    if value.tzinfo is None or offset != timedelta(0):
        raise PromotedOperationalLaunchError(_ERR_LAUNCH)
    return value


def canonical_utc_z_timestamp(value: datetime) -> str:
    """Canonical UTC timestamp representation: ISO-8601 with a literal
    trailing ``Z`` (never ``+00:00``)."""

    text = _require_literal_utc(value).isoformat()
    if not text.endswith("+00:00"):
        raise PromotedOperationalLaunchError(_ERR_LAUNCH)
    return text[: -len("+00:00")] + "Z"


def _parse_canonical_z_timestamp(value: object) -> datetime:
    if type(value) is not str or not value.endswith("Z"):
        raise ValueError("not a canonical timestamp")
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None or parsed.utcoffset() != timedelta(0):
        raise ValueError("not a canonical timestamp")
    if canonical_utc_z_timestamp(parsed) != value:
        raise ValueError("not a canonical timestamp")
    return parsed


def _parse_strict_int(value: object) -> int:
    if type(value) is not int:
        raise ValueError("not a canonical integer")
    return value


def _parse_canonical_decimal(value: object) -> Decimal:
    if type(value) is not str:
        raise ValueError("not a canonical decimal string")
    result = Decimal(value)
    if not result.is_finite() or str(result) != value:
        raise ValueError("not a canonical decimal string")
    return result


@dataclass(frozen=True, slots=True)
class LaunchQuoteGatePolicyRequest:
    maximum_batch_collection_seconds: int
    maximum_quote_age_seconds: int
    maximum_last_trade_age_seconds: int
    maximum_spread_bps: Decimal

    def __post_init__(self) -> None:
        for value in (
            self.maximum_batch_collection_seconds,
            self.maximum_quote_age_seconds,
            self.maximum_last_trade_age_seconds,
        ):
            if type(value) is not int:
                raise PromotedOperationalLaunchError(_ERR_LAUNCH)
        if type(self.maximum_spread_bps) is not Decimal:
            raise PromotedOperationalLaunchError(_ERR_LAUNCH)

    def replay(self) -> None:
        """Reconstruct a fresh exact instance from every retained field
        (rerunning __post_init__) and require equality -- catches a
        post-construction ``object.__setattr__`` tamper of any single
        field."""

        if type(self) is not LaunchQuoteGatePolicyRequest:
            raise PromotedOperationalLaunchError(_ERR_LAUNCH)
        reconstruct_failed = False
        reraise: PromotedOperationalLaunchError | None = None
        fresh: LaunchQuoteGatePolicyRequest | None = None
        try:
            fresh = LaunchQuoteGatePolicyRequest(
                maximum_batch_collection_seconds=self.maximum_batch_collection_seconds,
                maximum_quote_age_seconds=self.maximum_quote_age_seconds,
                maximum_last_trade_age_seconds=self.maximum_last_trade_age_seconds,
                maximum_spread_bps=self.maximum_spread_bps,
            )
        except PromotedOperationalLaunchError as error:
            reraise = error
        except Exception:
            reconstruct_failed = True
        if reraise is not None:
            raise reraise
        if reconstruct_failed or fresh is None or fresh != self:
            raise PromotedOperationalLaunchError(_ERR_LAUNCH)


@dataclass(frozen=True, slots=True)
class LaunchSizingPolicyRequest:
    per_trade_risk_fraction: Decimal
    maximum_total_open_risk_fraction: Decimal
    maximum_position_notional_fraction: Decimal
    maximum_gross_exposure_fraction: Decimal
    maximum_daily_turnover_participation: Decimal
    maximum_top_ask_participation: Decimal
    maximum_daily_loss_fraction: Decimal
    maximum_pilot_drawdown_fraction: Decimal
    minimum_net_reward_risk: Decimal
    maximum_open_positions: int
    maximum_new_positions_per_run: int

    def __post_init__(self) -> None:
        for value in (
            self.per_trade_risk_fraction,
            self.maximum_total_open_risk_fraction,
            self.maximum_position_notional_fraction,
            self.maximum_gross_exposure_fraction,
            self.maximum_daily_turnover_participation,
            self.maximum_top_ask_participation,
            self.maximum_daily_loss_fraction,
            self.maximum_pilot_drawdown_fraction,
            self.minimum_net_reward_risk,
        ):
            if type(value) is not Decimal:
                raise PromotedOperationalLaunchError(_ERR_LAUNCH)
        for value in (self.maximum_open_positions, self.maximum_new_positions_per_run):
            if type(value) is not int:
                raise PromotedOperationalLaunchError(_ERR_LAUNCH)

    def replay(self) -> None:
        """Reconstruct a fresh exact instance from every retained field
        (rerunning __post_init__) and require equality -- catches a
        post-construction ``object.__setattr__`` tamper of any single
        field."""

        if type(self) is not LaunchSizingPolicyRequest:
            raise PromotedOperationalLaunchError(_ERR_LAUNCH)
        reconstruct_failed = False
        reraise: PromotedOperationalLaunchError | None = None
        fresh: LaunchSizingPolicyRequest | None = None
        try:
            fresh = LaunchSizingPolicyRequest(
                per_trade_risk_fraction=self.per_trade_risk_fraction,
                maximum_total_open_risk_fraction=self.maximum_total_open_risk_fraction,
                maximum_position_notional_fraction=self.maximum_position_notional_fraction,
                maximum_gross_exposure_fraction=self.maximum_gross_exposure_fraction,
                maximum_daily_turnover_participation=self.maximum_daily_turnover_participation,
                maximum_top_ask_participation=self.maximum_top_ask_participation,
                maximum_daily_loss_fraction=self.maximum_daily_loss_fraction,
                maximum_pilot_drawdown_fraction=self.maximum_pilot_drawdown_fraction,
                minimum_net_reward_risk=self.minimum_net_reward_risk,
                maximum_open_positions=self.maximum_open_positions,
                maximum_new_positions_per_run=self.maximum_new_positions_per_run,
            )
        except PromotedOperationalLaunchError as error:
            reraise = error
        except Exception:
            reconstruct_failed = True
        if reraise is not None:
            raise reraise
        if reconstruct_failed or fresh is None or fresh != self:
            raise PromotedOperationalLaunchError(_ERR_LAUNCH)


@dataclass(frozen=True, slots=True)
class LaunchAllocationPolicyRequest:
    maximum_portfolio_age_seconds: int
    sizing_policy: LaunchSizingPolicyRequest

    def __post_init__(self) -> None:
        if type(self.maximum_portfolio_age_seconds) is not int:
            raise PromotedOperationalLaunchError(_ERR_LAUNCH)
        if type(self.sizing_policy) is not LaunchSizingPolicyRequest:
            raise PromotedOperationalLaunchError(_ERR_LAUNCH)

    def replay(self) -> None:
        """Replay the nested sizing policy first, then reconstruct a fresh
        exact instance from every retained field and require equality."""

        if type(self) is not LaunchAllocationPolicyRequest:
            raise PromotedOperationalLaunchError(_ERR_LAUNCH)
        nested_failed = False
        try:
            self.sizing_policy.replay()
        except Exception:
            nested_failed = True
        if nested_failed:
            raise PromotedOperationalLaunchError(_ERR_LAUNCH)
        reconstruct_failed = False
        reraise: PromotedOperationalLaunchError | None = None
        fresh: LaunchAllocationPolicyRequest | None = None
        try:
            fresh = LaunchAllocationPolicyRequest(
                maximum_portfolio_age_seconds=self.maximum_portfolio_age_seconds,
                sizing_policy=self.sizing_policy,
            )
        except PromotedOperationalLaunchError as error:
            reraise = error
        except Exception:
            reconstruct_failed = True
        if reraise is not None:
            raise reraise
        if reconstruct_failed or fresh is None or fresh != self:
            raise PromotedOperationalLaunchError(_ERR_LAUNCH)


@dataclass(frozen=True, slots=True)
class PromotedOperationalLaunchRequest:
    """One strict, human-authored request: every quote-gate, sizing,
    portfolio-age, and chunk value is an explicit launch control -- never
    inferred from market data or candidates. No optional/default field
    exists. Never carries a caller-supplied policy ID/version, authority
    flag, target session, portfolio snapshot ID, or assembly spec ID --
    all of those are derived exclusively from verified accepted parents
    and constructors."""

    schema_version: str
    preparation_id: str
    portfolio_artifact_id: str
    expected_quote_source_id: str
    open_listing_keys: tuple[str, ...]
    decision_not_before: datetime
    decision_deadline: datetime
    quote_gate_policy: LaunchQuoteGatePolicyRequest
    allocation_policy: LaunchAllocationPolicyRequest
    maximum_quote_chunk_size: int
    binding_bucket: str

    def __post_init__(self) -> None:
        if self.schema_version != PROMOTED_OPERATIONAL_LAUNCH_REQUEST_SCHEMA_VERSION:
            raise PromotedOperationalLaunchError(_ERR_LAUNCH)
        for value in (
            self.preparation_id,
            self.portfolio_artifact_id,
            self.expected_quote_source_id,
        ):
            if type(value) is not str or _SHA256.fullmatch(value) is None:
                raise PromotedOperationalLaunchError(_ERR_LAUNCH)
        if not _canonical_listing_keys(self.open_listing_keys):
            raise PromotedOperationalLaunchError(_ERR_LAUNCH)

        not_before = _require_literal_utc(self.decision_not_before)
        deadline = _require_literal_utc(self.decision_deadline)
        if not_before >= deadline:
            raise PromotedOperationalLaunchError(_ERR_LAUNCH)

        if type(self.quote_gate_policy) is not LaunchQuoteGatePolicyRequest:
            raise PromotedOperationalLaunchError(_ERR_LAUNCH)
        if type(self.allocation_policy) is not LaunchAllocationPolicyRequest:
            raise PromotedOperationalLaunchError(_ERR_LAUNCH)
        if type(self.maximum_quote_chunk_size) is not int:
            raise PromotedOperationalLaunchError(_ERR_LAUNCH)
        if type(self.binding_bucket) is not str or _BUCKET.fullmatch(self.binding_bucket) is None:
            raise PromotedOperationalLaunchError(_ERR_LAUNCH)

    def replay(self) -> None:
        """Replay both nested policy requests first, then reconstruct a
        fresh exact instance from every retained top-level field and
        require equality -- catches a post-construction
        ``object.__setattr__`` tamper of any single field anywhere in the
        request graph."""

        if type(self) is not PromotedOperationalLaunchRequest:
            raise PromotedOperationalLaunchError(_ERR_LAUNCH)
        nested_failed = False
        try:
            self.quote_gate_policy.replay()
            self.allocation_policy.replay()
        except Exception:
            nested_failed = True
        if nested_failed:
            raise PromotedOperationalLaunchError(_ERR_LAUNCH)
        reconstruct_failed = False
        reraise: PromotedOperationalLaunchError | None = None
        fresh: PromotedOperationalLaunchRequest | None = None
        try:
            fresh = PromotedOperationalLaunchRequest(
                schema_version=self.schema_version,
                preparation_id=self.preparation_id,
                portfolio_artifact_id=self.portfolio_artifact_id,
                expected_quote_source_id=self.expected_quote_source_id,
                open_listing_keys=self.open_listing_keys,
                decision_not_before=self.decision_not_before,
                decision_deadline=self.decision_deadline,
                quote_gate_policy=self.quote_gate_policy,
                allocation_policy=self.allocation_policy,
                maximum_quote_chunk_size=self.maximum_quote_chunk_size,
                binding_bucket=self.binding_bucket,
            )
        except PromotedOperationalLaunchError as error:
            reraise = error
        except Exception:
            reconstruct_failed = True
        if reraise is not None:
            raise reraise
        if reconstruct_failed or fresh is None or fresh != self:
            raise PromotedOperationalLaunchError(_ERR_LAUNCH)


_TOP_KEYS = frozenset(
    {
        "schema_version",
        "preparation_id",
        "portfolio_artifact_id",
        "expected_quote_source_id",
        "open_listing_keys",
        "decision_not_before",
        "decision_deadline",
        "quote_gate_policy",
        "allocation_policy",
        "maximum_quote_chunk_size",
        "binding_bucket",
    }
)
_QUOTE_GATE_KEYS = frozenset(
    {
        "maximum_batch_collection_seconds",
        "maximum_quote_age_seconds",
        "maximum_last_trade_age_seconds",
        "maximum_spread_bps",
    }
)
_ALLOCATION_KEYS = frozenset({"maximum_portfolio_age_seconds", "sizing_policy"})
_SIZING_KEYS = frozenset(
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
    }
)


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise PromotedOperationalLaunchError(_ERR_LAUNCH)
        result[key] = value
    return result


def _reject_number(_token: str) -> object:
    raise PromotedOperationalLaunchError(_ERR_LAUNCH)


def decode_promoted_operational_launch_request(payload: bytes) -> PromotedOperationalLaunchRequest:
    if (
        type(payload) is not bytes
        or not payload
        or len(payload) > MAXIMUM_LAUNCH_REQUEST_BYTES
    ):
        raise PromotedOperationalLaunchError(_ERR_LAUNCH)

    decode_failed = False
    text = ""
    try:
        text = payload.decode("utf-8", errors="strict")
    except UnicodeDecodeError:
        decode_failed = True
    if decode_failed:
        raise PromotedOperationalLaunchError(_ERR_LAUNCH)

    parse_failed = False
    reraise: PromotedOperationalLaunchError | None = None
    root: object = None
    try:
        root = json.loads(
            text,
            object_pairs_hook=_unique_object,
            parse_float=_reject_number,
            parse_constant=_reject_number,
        )
    except PromotedOperationalLaunchError as error:
        reraise = error
    except (json.JSONDecodeError, RecursionError):
        parse_failed = True
    if reraise is not None:
        raise reraise
    if parse_failed:
        raise PromotedOperationalLaunchError(_ERR_LAUNCH)

    if type(root) is not dict or set(root) != _TOP_KEYS:
        raise PromotedOperationalLaunchError(_ERR_LAUNCH)
    quote_gate_raw = root["quote_gate_policy"]
    if type(quote_gate_raw) is not dict or set(quote_gate_raw) != _QUOTE_GATE_KEYS:
        raise PromotedOperationalLaunchError(_ERR_LAUNCH)
    allocation_raw = root["allocation_policy"]
    if type(allocation_raw) is not dict or set(allocation_raw) != _ALLOCATION_KEYS:
        raise PromotedOperationalLaunchError(_ERR_LAUNCH)
    sizing_raw = allocation_raw["sizing_policy"]
    if type(sizing_raw) is not dict or set(sizing_raw) != _SIZING_KEYS:
        raise PromotedOperationalLaunchError(_ERR_LAUNCH)
    open_listing_keys_raw = root["open_listing_keys"]
    if type(open_listing_keys_raw) is not list:
        raise PromotedOperationalLaunchError(_ERR_LAUNCH)

    construction_failed = False
    request: PromotedOperationalLaunchRequest | None = None
    try:
        quote_gate_policy = LaunchQuoteGatePolicyRequest(
            maximum_batch_collection_seconds=_parse_strict_int(
                quote_gate_raw["maximum_batch_collection_seconds"]
            ),
            maximum_quote_age_seconds=_parse_strict_int(quote_gate_raw["maximum_quote_age_seconds"]),
            maximum_last_trade_age_seconds=_parse_strict_int(
                quote_gate_raw["maximum_last_trade_age_seconds"]
            ),
            maximum_spread_bps=_parse_canonical_decimal(quote_gate_raw["maximum_spread_bps"]),
        )
        sizing_policy = LaunchSizingPolicyRequest(
            per_trade_risk_fraction=_parse_canonical_decimal(sizing_raw["per_trade_risk_fraction"]),
            maximum_total_open_risk_fraction=_parse_canonical_decimal(
                sizing_raw["maximum_total_open_risk_fraction"]
            ),
            maximum_position_notional_fraction=_parse_canonical_decimal(
                sizing_raw["maximum_position_notional_fraction"]
            ),
            maximum_gross_exposure_fraction=_parse_canonical_decimal(
                sizing_raw["maximum_gross_exposure_fraction"]
            ),
            maximum_daily_turnover_participation=_parse_canonical_decimal(
                sizing_raw["maximum_daily_turnover_participation"]
            ),
            maximum_top_ask_participation=_parse_canonical_decimal(
                sizing_raw["maximum_top_ask_participation"]
            ),
            maximum_daily_loss_fraction=_parse_canonical_decimal(
                sizing_raw["maximum_daily_loss_fraction"]
            ),
            maximum_pilot_drawdown_fraction=_parse_canonical_decimal(
                sizing_raw["maximum_pilot_drawdown_fraction"]
            ),
            minimum_net_reward_risk=_parse_canonical_decimal(sizing_raw["minimum_net_reward_risk"]),
            maximum_open_positions=_parse_strict_int(sizing_raw["maximum_open_positions"]),
            maximum_new_positions_per_run=_parse_strict_int(
                sizing_raw["maximum_new_positions_per_run"]
            ),
        )
        allocation_policy = LaunchAllocationPolicyRequest(
            maximum_portfolio_age_seconds=_parse_strict_int(
                allocation_raw["maximum_portfolio_age_seconds"]
            ),
            sizing_policy=sizing_policy,
        )
        request = PromotedOperationalLaunchRequest(
            schema_version=root["schema_version"],
            preparation_id=root["preparation_id"],
            portfolio_artifact_id=root["portfolio_artifact_id"],
            expected_quote_source_id=root["expected_quote_source_id"],
            open_listing_keys=tuple(open_listing_keys_raw),
            decision_not_before=_parse_canonical_z_timestamp(root["decision_not_before"]),
            decision_deadline=_parse_canonical_z_timestamp(root["decision_deadline"]),
            quote_gate_policy=quote_gate_policy,
            allocation_policy=allocation_policy,
            maximum_quote_chunk_size=_parse_strict_int(root["maximum_quote_chunk_size"]),
            binding_bucket=root["binding_bucket"],
        )
    except Exception:
        construction_failed = True
    if construction_failed or request is None:
        raise PromotedOperationalLaunchError(_ERR_LAUNCH)
    return request


class PromotedOperationalPreparationResolver(Protocol):
    def get(self, preparation_id: str) -> VerifiedPromotedOperationalPreparation: ...


class SwingPortfolioArtifactResolver(Protocol):
    def get(self, artifact_id: str) -> SwingPortfolioSnapshotArtifact: ...


class _PinnedPreparationResolver:
    """In-memory resolver wrapping one already-resolved exact parent, so
    the mandatory dry assembly never re-reads the durable preparation
    store a second time."""

    def __init__(self, preparation: VerifiedPromotedOperationalPreparation) -> None:
        self._preparation = preparation

    def get(self, preparation_id: str) -> VerifiedPromotedOperationalPreparation:
        verify_failed = False
        try:
            self._preparation.verify_content_identity()
        except Exception:
            verify_failed = True
        if verify_failed:
            raise PromotedOperationalLaunchError(_ERR_LAUNCH)
        if self._preparation.manifest.preparation_id != preparation_id:
            raise PromotedOperationalLaunchError(_ERR_LAUNCH)
        return self._preparation


class _PinnedPortfolioArtifactResolver:
    """In-memory resolver wrapping one already-resolved exact parent, so
    the mandatory dry assembly never re-reads the durable portfolio-
    artifact store a second time."""

    def __init__(self, artifact: SwingPortfolioSnapshotArtifact) -> None:
        self._artifact = artifact

    def get(self, artifact_id: str) -> SwingPortfolioSnapshotArtifact:
        verify_failed = False
        try:
            self._artifact.verify_content_identity()
        except Exception:
            verify_failed = True
        if verify_failed:
            raise PromotedOperationalLaunchError(_ERR_LAUNCH)
        if self._artifact.artifact_id != artifact_id:
            raise PromotedOperationalLaunchError(_ERR_LAUNCH)
        return self._artifact


def prepare_promoted_operational_launch(
    *,
    request: PromotedOperationalLaunchRequest,
    preparation_resolver: PromotedOperationalPreparationResolver,
    portfolio_artifact_resolver: SwingPortfolioArtifactResolver,
) -> PromotedOperationalRuntimeAssembly:
    """Resolve exact parents, derive every redundant ID through the
    accepted constructors, and independently prove the resulting spec is
    fully assemblable before ever returning it.

    ``preparation_resolver.get`` is called exactly once; every failure
    (wrong type, exception, tampered content, requested/returned ID
    mismatch, or a session that disagrees with the request) fails closed
    before ``portfolio_artifact_resolver.get`` is ever called.
    ``portfolio_artifact_resolver.get`` is then called exactly once, with
    the identical fail-closed treatment. The mandatory dry assembly
    (``assemble_promoted_operational_runtime_inputs``) is then called
    exactly once using in-memory wrappers pinned to the two already-
    resolved parents -- never re-reading either durable store.
    """

    if type(request) is not PromotedOperationalLaunchRequest:
        raise PromotedOperationalLaunchError(_ERR_LAUNCH)

    replay_failed = False
    try:
        request.replay()
    except Exception:
        replay_failed = True
    if replay_failed:
        raise PromotedOperationalLaunchError(_ERR_LAUNCH)

    preparation_failed = False
    preparation: VerifiedPromotedOperationalPreparation | None = None
    try:
        preparation = preparation_resolver.get(request.preparation_id)
    except Exception:
        preparation_failed = True
    if preparation_failed:
        raise PromotedOperationalLaunchError(_ERR_LAUNCH)
    if type(preparation) is not VerifiedPromotedOperationalPreparation:
        raise PromotedOperationalLaunchError(_ERR_LAUNCH)
    preparation_verify_failed = False
    try:
        preparation.verify_content_identity()
    except Exception:
        preparation_verify_failed = True
    if preparation_verify_failed:
        raise PromotedOperationalLaunchError(_ERR_LAUNCH)

    preparation_identity_failed = False
    preparation_mismatch = True
    target_session = None
    try:
        preparation_mismatch = preparation.manifest.preparation_id != request.preparation_id
        target_session = preparation.manifest.target_session
    except Exception:
        preparation_identity_failed = True
    if preparation_identity_failed or preparation_mismatch:
        raise PromotedOperationalLaunchError(_ERR_LAUNCH)

    session_gate_failed = False
    session_mismatch = True
    try:
        not_before_session = request.decision_not_before.astimezone(INDIA_STANDARD_TIME).date()
        deadline_session = request.decision_deadline.astimezone(INDIA_STANDARD_TIME).date()
        session_mismatch = (
            not_before_session != target_session or deadline_session != target_session
        )
    except Exception:
        session_gate_failed = True
    if session_gate_failed or session_mismatch:
        raise PromotedOperationalLaunchError(_ERR_LAUNCH)

    artifact_failed = False
    artifact: SwingPortfolioSnapshotArtifact | None = None
    try:
        artifact = portfolio_artifact_resolver.get(request.portfolio_artifact_id)
    except Exception:
        artifact_failed = True
    if artifact_failed:
        raise PromotedOperationalLaunchError(_ERR_LAUNCH)
    if type(artifact) is not SwingPortfolioSnapshotArtifact:
        raise PromotedOperationalLaunchError(_ERR_LAUNCH)
    artifact_verify_failed = False
    try:
        artifact.verify_content_identity()
    except Exception:
        artifact_verify_failed = True
    if artifact_verify_failed:
        raise PromotedOperationalLaunchError(_ERR_LAUNCH)

    artifact_identity_failed = False
    artifact_mismatch = True
    portfolio_snapshot_id = None
    try:
        artifact_mismatch = artifact.artifact_id != request.portfolio_artifact_id
        portfolio_snapshot_id = artifact.portfolio_snapshot_id
    except Exception:
        artifact_identity_failed = True
    if artifact_identity_failed or artifact_mismatch:
        raise PromotedOperationalLaunchError(_ERR_LAUNCH)

    build_failed = False
    spec: PromotedOperationalAssemblySpec | None = None
    try:
        quote_gate_policy = SwingQuoteGatePolicy(
            maximum_batch_collection_seconds=request.quote_gate_policy.maximum_batch_collection_seconds,
            maximum_quote_age_seconds=request.quote_gate_policy.maximum_quote_age_seconds,
            maximum_last_trade_age_seconds=request.quote_gate_policy.maximum_last_trade_age_seconds,
            maximum_spread_bps=request.quote_gate_policy.maximum_spread_bps,
        )
        sizing_policy = SwingPortfolioSizingPolicy(
            per_trade_risk_fraction=request.allocation_policy.sizing_policy.per_trade_risk_fraction,
            maximum_total_open_risk_fraction=(
                request.allocation_policy.sizing_policy.maximum_total_open_risk_fraction
            ),
            maximum_position_notional_fraction=(
                request.allocation_policy.sizing_policy.maximum_position_notional_fraction
            ),
            maximum_gross_exposure_fraction=(
                request.allocation_policy.sizing_policy.maximum_gross_exposure_fraction
            ),
            maximum_daily_turnover_participation=(
                request.allocation_policy.sizing_policy.maximum_daily_turnover_participation
            ),
            maximum_top_ask_participation=(
                request.allocation_policy.sizing_policy.maximum_top_ask_participation
            ),
            maximum_daily_loss_fraction=(
                request.allocation_policy.sizing_policy.maximum_daily_loss_fraction
            ),
            maximum_pilot_drawdown_fraction=(
                request.allocation_policy.sizing_policy.maximum_pilot_drawdown_fraction
            ),
            minimum_net_reward_risk=request.allocation_policy.sizing_policy.minimum_net_reward_risk,
            maximum_open_positions=request.allocation_policy.sizing_policy.maximum_open_positions,
            maximum_new_positions_per_run=(
                request.allocation_policy.sizing_policy.maximum_new_positions_per_run
            ),
        )
        allocation_policy = PromotedOperationalAllocationPolicy(
            policy=sizing_policy,
            maximum_portfolio_age_seconds=request.allocation_policy.maximum_portfolio_age_seconds,
        )
        spec = PromotedOperationalAssemblySpec(
            preparation_id=request.preparation_id,
            portfolio_artifact_id=request.portfolio_artifact_id,
            expected_portfolio_snapshot_id=portfolio_snapshot_id,
            expected_quote_source_id=request.expected_quote_source_id,
            open_listing_keys=request.open_listing_keys,
            decision_not_before=request.decision_not_before,
            decision_deadline=request.decision_deadline,
            target_session=target_session,
            quote_gate_policy=quote_gate_policy,
            allocation_policy=allocation_policy,
            maximum_quote_chunk_size=request.maximum_quote_chunk_size,
            binding_bucket=request.binding_bucket,
        )
    except Exception:
        build_failed = True
    if build_failed or spec is None:
        raise PromotedOperationalLaunchError(_ERR_LAUNCH)

    dry_assembly_failed = False
    assembly: PromotedOperationalRuntimeAssembly | None = None
    try:
        assembly = assemble_promoted_operational_runtime_inputs(
            spec=spec,
            preparation_resolver=_PinnedPreparationResolver(preparation),
            portfolio_artifact_resolver=_PinnedPortfolioArtifactResolver(artifact),
        )
    except Exception:
        dry_assembly_failed = True
    if dry_assembly_failed or assembly is None or assembly.assembly_spec != spec:
        raise PromotedOperationalLaunchError(_ERR_LAUNCH)

    return assembly


def _is_link_like(path: Path) -> bool:
    try:
        status = os.lstat(path)
    except OSError:
        return path.is_symlink()
    return path.is_symlink() or bool(
        getattr(status, "st_file_attributes", 0)
        & getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400)
    )


def publish_promoted_operational_launch_assembly_spec_file(
    path: Path, spec: PromotedOperationalAssemblySpec
) -> None:
    """Publish ``spec`` to ``path`` using a bounded, module-local
    create-once writer.

    Never truncates, overwrites, replaces, or deletes. An absent target
    is created exclusively (``O_CREAT | O_EXCL``), fully written, fsynced,
    and closed, then cold-read back and required byte-identical -- never
    a temporary-file rename, which could silently replace an existing
    file. An existing exact regular file is accepted only if it is
    byte-identical to the freshly encoded payload; any other existing
    content (different, empty, oversized, a symlink/reparse point, or
    otherwise unsafe) fails closed. The parent directory must already
    exist as a real, non-link directory -- this function never creates or
    scans a directory.
    """

    if type(spec) is not PromotedOperationalAssemblySpec:
        raise PromotedOperationalLaunchError(_ERR_LAUNCH)
    spec_verify_failed = False
    try:
        spec.verify_content_identity()
    except Exception:
        spec_verify_failed = True
    if spec_verify_failed:
        raise PromotedOperationalLaunchError(_ERR_LAUNCH)

    if type(path) is not _CONCRETE_PATH_TYPE:
        raise PromotedOperationalLaunchError(_ERR_LAUNCH)
    if not path.is_absolute() or ".." in path.parts:
        raise PromotedOperationalLaunchError(_ERR_LAUNCH)

    parent_check_failed = False
    parent_ok = False
    try:
        parent = path.parent
        parent_ok = parent.is_dir() and not _is_link_like(parent)
    except Exception:
        parent_check_failed = True
    if parent_check_failed or not parent_ok:
        raise PromotedOperationalLaunchError(_ERR_LAUNCH)

    encode_failed = False
    payload = b""
    try:
        payload = encode_promoted_operational_assembly_spec(spec)
    except Exception:
        encode_failed = True
    if encode_failed:
        raise PromotedOperationalLaunchError(_ERR_LAUNCH)

    exists_check_failed = False
    target_present = False
    try:
        target_present = path.exists() or path.is_symlink()
    except Exception:
        exists_check_failed = True
    if exists_check_failed:
        raise PromotedOperationalLaunchError(_ERR_LAUNCH)

    if target_present:
        replay_failed = False
        existing = b""
        existing_decoded_id = None
        try:
            existing = read_stable_regular_file(path, maximum_bytes=MAXIMUM_ASSEMBLY_SPEC_BYTES)
            if existing == payload:
                existing_decoded_id = decode_promoted_operational_assembly_spec(
                    existing
                ).assembly_spec_id
        except Exception:
            replay_failed = True
        if (
            replay_failed
            or existing != payload
            or existing_decoded_id != spec.assembly_spec_id
        ):
            raise PromotedOperationalLaunchError(_ERR_LAUNCH)
        return

    write_failed = False
    try:
        flags = (
            os.O_CREAT
            | os.O_EXCL
            | os.O_WRONLY
            | getattr(os, "O_BINARY", 0)
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_NOINHERIT", 0)
            | getattr(os, "O_NOFOLLOW", 0)
        )
        descriptor = os.open(path, flags, 0o600)
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
    except Exception:
        write_failed = True
    if write_failed:
        raise PromotedOperationalLaunchError(_ERR_LAUNCH)

    cold_read_failed = False
    cold_bytes = b""
    cold_decoded_id = None
    try:
        cold_bytes = read_stable_regular_file(path, maximum_bytes=MAXIMUM_ASSEMBLY_SPEC_BYTES)
        if cold_bytes == payload:
            cold_decoded_id = decode_promoted_operational_assembly_spec(cold_bytes).assembly_spec_id
    except Exception:
        cold_read_failed = True
    if (
        cold_read_failed
        or cold_bytes != payload
        or cold_decoded_id != spec.assembly_spec_id
    ):
        raise PromotedOperationalLaunchError(_ERR_LAUNCH)
