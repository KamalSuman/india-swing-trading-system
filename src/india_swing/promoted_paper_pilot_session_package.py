"""One deterministic, offline first-session packager.

Starts from one exact, already-published promoted research run plus one
explicitly reconciled empty paper-portfolio genesis, and publishes the
exact operational preparation, portfolio artifact, and promoted
operational assembly spec required by the already-accepted cloud-
control/input-publication/deployment path.

This module composes -- and never duplicates -- three already-accepted
boundaries in strict sequence:

1. ``prepare_and_publish`` (``promoted_operational_preparation``): resolves
   the exact research run and durably publishes its paper-only operational
   preparation.
2. ``seal_promoted_paper_portfolio_genesis``
   (``promoted_paper_portfolio_genesis``): archives the four exact
   evidence payloads and durably seals the initial, empty, manually
   reconciled paper portfolio artifact.
3. ``prepare_promoted_operational_launch`` +
   ``publish_promoted_operational_launch_assembly_spec_file``
   (``promoted_operational_launch``): constructs the existing
   ``PromotedOperationalLaunchRequest`` from this package's explicit
   controls plus the two freshly resolved IDs, dry-assembles it (which
   independently re-enforces portfolio freshness and the open-listing-
   key/open-position count invariant), and publishes the resulting
   assembly spec create-once.

This module introduces no new trading, ranking, risk, eligibility, cloud,
broker, notification, or deployment logic, and has no clock, environment,
network, GCP, Kite, Telegram, LLM, or subprocess capability of its own.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Mapping

from india_swing.operations.portfolio_store import (
    LocalSwingPortfolioArtifactStore,
    SwingPortfolioEvidenceKind,
    SwingPortfolioSnapshotArtifact,
)
from india_swing.promoted_operational_assembly import (
    PromotedOperationalAssemblySpec,
    PromotedOperationalRuntimeAssembly,
)
from india_swing.promoted_operational_launch import (
    PROMOTED_OPERATIONAL_LAUNCH_REQUEST_SCHEMA_VERSION,
    LaunchAllocationPolicyRequest,
    LaunchQuoteGatePolicyRequest,
    LaunchSizingPolicyRequest,
    PromotedOperationalLaunchRequest,
    canonical_utc_z_timestamp,
    prepare_promoted_operational_launch,
    publish_promoted_operational_launch_assembly_spec_file,
)
from india_swing.promoted_operational_preparation import (
    LocalPromotedOperationalPreparationStore,
    VerifiedPromotedOperationalPreparation,
    prepare_and_publish,
)
from india_swing.promoted_paper_portfolio_genesis import (
    PromotedPaperPortfolioGenesisRequest,
    PromotedPortfolioEvidenceArchive,
    seal_promoted_paper_portfolio_genesis,
)


class PromotedPaperPilotSessionPackageError(ValueError):
    pass


_ERR = "promoted paper pilot session package call is invalid"

_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_BUCKET = re.compile(r"[a-z0-9][a-z0-9._-]{1,61}[a-z0-9]\Z")
_LISTING_KEY = re.compile(r"NSE:[A-Z0-9&.\-]{1,32}\Z")

PROMOTED_PAPER_PILOT_SESSION_PACKAGE_REQUEST_SCHEMA_VERSION = (
    "promoted-paper-pilot-session-package-request/v1"
)
MAXIMUM_SESSION_PACKAGE_REQUEST_BYTES = 64 * 1024


def _require_literal_utc(value: object) -> datetime:
    """Require an exact ``datetime`` with a literal zero UTC offset, never
    normalizing a non-UTC-but-aware representation -- this module defines
    its own copy (rather than importing another module's private helper
    of the same shape) so a malformed datetime here always raises this
    module's own static ``PromotedPaperPilotSessionPackageError``."""

    if type(value) is not datetime:
        raise PromotedPaperPilotSessionPackageError(_ERR)
    offset_failed = False
    offset = None
    try:
        offset = value.utcoffset()
    except Exception:
        offset_failed = True
    if offset_failed:
        raise PromotedPaperPilotSessionPackageError(_ERR)
    if value.tzinfo is None or offset != timedelta(0):
        raise PromotedPaperPilotSessionPackageError(_ERR)
    return value


def _parse_canonical_z_timestamp(value: object) -> datetime:
    if type(value) is not str or not value.endswith("Z"):
        raise ValueError("not a canonical timestamp")
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None or parsed.utcoffset() != timedelta(0):
        raise ValueError("not a canonical timestamp")
    if canonical_utc_z_timestamp(parsed) != value:
        raise ValueError("not a canonical timestamp")
    return parsed


def _canonical_date(value: object) -> date:
    if type(value) is not str:
        raise ValueError("not a canonical date")
    result = date.fromisoformat(value)
    if result.isoformat() != value:
        raise ValueError("not a canonical date")
    return result


def _parse_strict_int(value: object) -> int:
    if type(value) is not int:
        raise ValueError("not a canonical integer")
    return value


def _parse_canonical_decimal(value: object) -> Decimal:
    if type(value) is not str:
        raise ValueError("not a canonical decimal string")
    try:
        result = Decimal(value)
    except InvalidOperation:
        raise ValueError("not a canonical decimal string") from None
    if not result.is_finite() or str(result) != value:
        raise ValueError("not a canonical decimal string")
    return result


def _canonical_listing_keys(value: object) -> tuple[str, ...]:
    if type(value) is not tuple or any(type(item) is not str for item in value):
        raise PromotedPaperPilotSessionPackageError(_ERR)
    if any(_LISTING_KEY.fullmatch(item) is None for item in value):
        raise PromotedPaperPilotSessionPackageError(_ERR)
    if len(set(value)) != len(value):
        raise PromotedPaperPilotSessionPackageError(_ERR)
    if tuple(sorted(value)) != value:
        raise PromotedPaperPilotSessionPackageError(_ERR)
    return value


@dataclass(frozen=True, slots=True)
class PromotedPaperPilotSessionPackageRequest:
    """One strict, human-authored request: every quote-gate, sizing,
    portfolio-age, decision-window, chunk, and bucket value is an
    explicit control -- never inferred. This request never carries a
    caller-supplied preparation, portfolio, policy, assembly, engine,
    graph, or operational-run ID; every such identity is derived
    exclusively from the exact ``research_run_id`` plus the accepted
    domain constructors this module composes."""

    schema_version: str
    research_run_id: str
    target_session: date
    expected_quote_source_id: str
    open_listing_keys: tuple[str, ...]
    decision_not_before: datetime
    decision_deadline: datetime
    quote_gate_policy: LaunchQuoteGatePolicyRequest
    allocation_policy: LaunchAllocationPolicyRequest
    maximum_quote_chunk_size: int
    binding_bucket: str

    def __post_init__(self) -> None:
        if (
            type(self.schema_version) is not str
            or self.schema_version != PROMOTED_PAPER_PILOT_SESSION_PACKAGE_REQUEST_SCHEMA_VERSION
        ):
            raise PromotedPaperPilotSessionPackageError(_ERR)
        if type(self.research_run_id) is not str or _SHA256.fullmatch(self.research_run_id) is None:
            raise PromotedPaperPilotSessionPackageError(_ERR)
        if type(self.target_session) is not date:
            raise PromotedPaperPilotSessionPackageError(_ERR)
        if (
            type(self.expected_quote_source_id) is not str
            or _SHA256.fullmatch(self.expected_quote_source_id) is None
        ):
            raise PromotedPaperPilotSessionPackageError(_ERR)
        object.__setattr__(self, "open_listing_keys", _canonical_listing_keys(self.open_listing_keys))

        not_before = _require_literal_utc(self.decision_not_before)
        deadline = _require_literal_utc(self.decision_deadline)
        if not_before >= deadline:
            raise PromotedPaperPilotSessionPackageError(_ERR)

        if type(self.quote_gate_policy) is not LaunchQuoteGatePolicyRequest:
            raise PromotedPaperPilotSessionPackageError(_ERR)
        if type(self.allocation_policy) is not LaunchAllocationPolicyRequest:
            raise PromotedPaperPilotSessionPackageError(_ERR)
        if (
            type(self.maximum_quote_chunk_size) is not int
            or self.maximum_quote_chunk_size <= 0
        ):
            raise PromotedPaperPilotSessionPackageError(_ERR)
        if type(self.binding_bucket) is not str or _BUCKET.fullmatch(self.binding_bucket) is None:
            raise PromotedPaperPilotSessionPackageError(_ERR)

    def replay(self) -> None:
        """Replay both nested policy requests first, then reconstruct a
        fresh exact instance from every retained top-level field and
        require equality -- catches a post-construction
        ``object.__setattr__`` tamper of any single field anywhere in the
        request graph."""

        if type(self) is not PromotedPaperPilotSessionPackageRequest:
            raise PromotedPaperPilotSessionPackageError(_ERR)
        nested_failed = False
        try:
            self.quote_gate_policy.replay()
            self.allocation_policy.replay()
        except Exception:
            nested_failed = True
        if nested_failed:
            raise PromotedPaperPilotSessionPackageError(_ERR)
        reconstruct_failed = False
        reraise: PromotedPaperPilotSessionPackageError | None = None
        fresh: PromotedPaperPilotSessionPackageRequest | None = None
        try:
            fresh = PromotedPaperPilotSessionPackageRequest(
                schema_version=self.schema_version,
                research_run_id=self.research_run_id,
                target_session=self.target_session,
                expected_quote_source_id=self.expected_quote_source_id,
                open_listing_keys=self.open_listing_keys,
                decision_not_before=self.decision_not_before,
                decision_deadline=self.decision_deadline,
                quote_gate_policy=self.quote_gate_policy,
                allocation_policy=self.allocation_policy,
                maximum_quote_chunk_size=self.maximum_quote_chunk_size,
                binding_bucket=self.binding_bucket,
            )
        except PromotedPaperPilotSessionPackageError as error:
            reraise = error
        except Exception:
            reconstruct_failed = True
        if reraise is not None:
            raise reraise
        if reconstruct_failed or fresh is None or fresh != self:
            raise PromotedPaperPilotSessionPackageError(_ERR)


_TOP_KEYS = frozenset(
    {
        "schema_version",
        "research_run_id",
        "target_session",
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


def _quote_gate_policy_body(value: LaunchQuoteGatePolicyRequest) -> dict[str, object]:
    return {
        "maximum_batch_collection_seconds": value.maximum_batch_collection_seconds,
        "maximum_quote_age_seconds": value.maximum_quote_age_seconds,
        "maximum_last_trade_age_seconds": value.maximum_last_trade_age_seconds,
        "maximum_spread_bps": str(value.maximum_spread_bps),
    }


def _sizing_policy_body(value: LaunchSizingPolicyRequest) -> dict[str, object]:
    return {
        "per_trade_risk_fraction": str(value.per_trade_risk_fraction),
        "maximum_total_open_risk_fraction": str(value.maximum_total_open_risk_fraction),
        "maximum_position_notional_fraction": str(value.maximum_position_notional_fraction),
        "maximum_gross_exposure_fraction": str(value.maximum_gross_exposure_fraction),
        "maximum_daily_turnover_participation": str(value.maximum_daily_turnover_participation),
        "maximum_top_ask_participation": str(value.maximum_top_ask_participation),
        "maximum_daily_loss_fraction": str(value.maximum_daily_loss_fraction),
        "maximum_pilot_drawdown_fraction": str(value.maximum_pilot_drawdown_fraction),
        "minimum_net_reward_risk": str(value.minimum_net_reward_risk),
        "maximum_open_positions": value.maximum_open_positions,
        "maximum_new_positions_per_run": value.maximum_new_positions_per_run,
    }


def _allocation_policy_body(value: LaunchAllocationPolicyRequest) -> dict[str, object]:
    return {
        "maximum_portfolio_age_seconds": value.maximum_portfolio_age_seconds,
        "sizing_policy": _sizing_policy_body(value.sizing_policy),
    }


def encode_promoted_paper_pilot_session_package_request(
    value: PromotedPaperPilotSessionPackageRequest,
) -> bytes:
    if type(value) is not PromotedPaperPilotSessionPackageRequest:
        raise PromotedPaperPilotSessionPackageError(_ERR)
    value.replay()
    body = {
        "schema_version": value.schema_version,
        "research_run_id": value.research_run_id,
        "target_session": value.target_session.isoformat(),
        "expected_quote_source_id": value.expected_quote_source_id,
        "open_listing_keys": list(value.open_listing_keys),
        "decision_not_before": canonical_utc_z_timestamp(value.decision_not_before),
        "decision_deadline": canonical_utc_z_timestamp(value.decision_deadline),
        "quote_gate_policy": _quote_gate_policy_body(value.quote_gate_policy),
        "allocation_policy": _allocation_policy_body(value.allocation_policy),
        "maximum_quote_chunk_size": value.maximum_quote_chunk_size,
        "binding_bucket": value.binding_bucket,
    }
    payload = (
        json.dumps(body, allow_nan=False, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
        + "\n"
    ).encode("utf-8")
    if not payload or len(payload) > MAXIMUM_SESSION_PACKAGE_REQUEST_BYTES:
        raise PromotedPaperPilotSessionPackageError(_ERR)
    return payload


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise PromotedPaperPilotSessionPackageError(_ERR)
        result[key] = value
    return result


def _reject_number(_token: str) -> object:
    raise PromotedPaperPilotSessionPackageError(_ERR)


def decode_promoted_paper_pilot_session_package_request(
    payload: bytes,
) -> PromotedPaperPilotSessionPackageRequest:
    if (
        type(payload) is not bytes
        or not payload
        or len(payload) > MAXIMUM_SESSION_PACKAGE_REQUEST_BYTES
    ):
        raise PromotedPaperPilotSessionPackageError(_ERR)

    decode_failed = False
    reraise: PromotedPaperPilotSessionPackageError | None = None
    request: PromotedPaperPilotSessionPackageRequest | None = None
    try:
        text = payload.decode("utf-8", errors="strict")
        root = json.loads(
            text, object_pairs_hook=_unique_object, parse_float=_reject_number, parse_constant=_reject_number,
        )
        if type(root) is not dict or set(root) != _TOP_KEYS:
            raise PromotedPaperPilotSessionPackageError(_ERR)
        quote_gate_raw = root["quote_gate_policy"]
        if type(quote_gate_raw) is not dict or set(quote_gate_raw) != _QUOTE_GATE_KEYS:
            raise PromotedPaperPilotSessionPackageError(_ERR)
        allocation_raw = root["allocation_policy"]
        if type(allocation_raw) is not dict or set(allocation_raw) != _ALLOCATION_KEYS:
            raise PromotedPaperPilotSessionPackageError(_ERR)
        sizing_raw = allocation_raw["sizing_policy"]
        if type(sizing_raw) is not dict or set(sizing_raw) != _SIZING_KEYS:
            raise PromotedPaperPilotSessionPackageError(_ERR)
        open_listing_keys_raw = root["open_listing_keys"]
        if type(open_listing_keys_raw) is not list:
            raise PromotedPaperPilotSessionPackageError(_ERR)

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
        request = PromotedPaperPilotSessionPackageRequest(
            schema_version=root["schema_version"],
            research_run_id=root["research_run_id"],
            target_session=_canonical_date(root["target_session"]),
            expected_quote_source_id=root["expected_quote_source_id"],
            open_listing_keys=tuple(open_listing_keys_raw),
            decision_not_before=_parse_canonical_z_timestamp(root["decision_not_before"]),
            decision_deadline=_parse_canonical_z_timestamp(root["decision_deadline"]),
            quote_gate_policy=quote_gate_policy,
            allocation_policy=allocation_policy,
            maximum_quote_chunk_size=root["maximum_quote_chunk_size"],
            binding_bucket=root["binding_bucket"],
        )
    except PromotedPaperPilotSessionPackageError as error:
        reraise = error
    except Exception:
        decode_failed = True
    if reraise is not None:
        raise reraise
    if decode_failed or request is None:
        raise PromotedPaperPilotSessionPackageError(_ERR)
    if encode_promoted_paper_pilot_session_package_request(request) != payload:
        raise PromotedPaperPilotSessionPackageError(_ERR)
    return request


@dataclass(frozen=True, slots=True)
class PromotedPaperPilotSessionPackageResult:
    """The exact sanitized coordinates of one completed first-session
    package -- never a path, a capital amount, a holding, an evidence
    hash, a policy threshold, a bucket, or a raw candidate."""

    target_session: date
    research_run_id: str
    preparation_id: str
    portfolio_artifact_id: str
    portfolio_snapshot_id: str
    assembly_spec_id: str
    candidate_count: int
    open_position_count: int

    def __post_init__(self) -> None:
        if type(self.target_session) is not date:
            raise PromotedPaperPilotSessionPackageError(_ERR)
        for value in (
            self.research_run_id,
            self.preparation_id,
            self.portfolio_artifact_id,
            self.portfolio_snapshot_id,
            self.assembly_spec_id,
        ):
            if type(value) is not str or _SHA256.fullmatch(value) is None:
                raise PromotedPaperPilotSessionPackageError(_ERR)
        if (
            type(self.candidate_count) is not int
            or self.candidate_count < 0
            or type(self.open_position_count) is not int
            or self.open_position_count < 0
        ):
            raise PromotedPaperPilotSessionPackageError(_ERR)


def prepare_promoted_paper_pilot_first_session_package(
    *,
    package_request: PromotedPaperPilotSessionPackageRequest,
    genesis_request: PromotedPaperPortfolioGenesisRequest,
    genesis_evidence_payloads: Mapping[SwingPortfolioEvidenceKind, bytes],
    research_stores,
    preparations: LocalPromotedOperationalPreparationStore,
    evidence_archive: PromotedPortfolioEvidenceArchive,
    portfolio_store: LocalSwingPortfolioArtifactStore,
    output_assembly_spec_file: Path,
) -> PromotedPaperPilotSessionPackageResult:
    """Resolve+publish the operational preparation from the package's own
    ``research_run_id``, seal the initial empty portfolio genesis, then
    construct and dry-assemble the existing
    ``PromotedOperationalLaunchRequest`` and publish its assembly spec --
    in that exact order. Every stage composes only the already-accepted
    boundary named in this module's own docstring; none of their
    financial/identity logic is reproduced here."""

    if type(package_request) is not PromotedPaperPilotSessionPackageRequest:
        raise PromotedPaperPilotSessionPackageError(_ERR)
    if type(preparations) is not LocalPromotedOperationalPreparationStore:
        raise PromotedPaperPilotSessionPackageError(_ERR)
    if type(portfolio_store) is not LocalSwingPortfolioArtifactStore:
        raise PromotedPaperPilotSessionPackageError(_ERR)

    replay_failed = False
    try:
        package_request.replay()
    except Exception:
        replay_failed = True
    if replay_failed:
        raise PromotedPaperPilotSessionPackageError(_ERR)

    # Stage 1: resolve+publish the operational preparation. Every property
    # this function later depends on is captured here, inside the same
    # sanitized boundary as the dependency call and its own verification --
    # a wrong return type, a malicious property, or a foreign exception
    # anywhere in this stage leaves only the one static
    # PromotedPaperPilotSessionPackageError, never a foreign exception type,
    # message, __cause__, or __context__.
    preparation_failed = False
    preparation_id = ""
    try:
        preparation = prepare_and_publish(
            package_request.research_run_id, research_stores, preparations
        )
        if type(preparation) is not VerifiedPromotedOperationalPreparation:
            raise PromotedPaperPilotSessionPackageError(_ERR)
        preparation.verify_content_identity()
        preparation_target_session = preparation.manifest.target_session
        preparation_id = preparation.manifest.preparation_id
        if preparation_target_session != package_request.target_session:
            raise PromotedPaperPilotSessionPackageError(_ERR)
    except Exception:
        preparation_failed = True
    if preparation_failed:
        raise PromotedPaperPilotSessionPackageError(_ERR)

    # Stage 2: seal the initial empty genesis portfolio -- same discipline.
    genesis_failed = False
    portfolio_artifact_id = ""
    portfolio_snapshot_id = ""
    try:
        artifact = seal_promoted_paper_portfolio_genesis(
            request=genesis_request,
            evidence_payloads=genesis_evidence_payloads,
            evidence_archive=evidence_archive,
            portfolio_store=portfolio_store,
        )
        if type(artifact) is not SwingPortfolioSnapshotArtifact:
            raise PromotedPaperPilotSessionPackageError(_ERR)
        artifact.verify_content_identity()
        portfolio_artifact_id = artifact.artifact_id
        portfolio_snapshot_id = artifact.portfolio_snapshot_id
    except Exception:
        genesis_failed = True
    if genesis_failed:
        raise PromotedPaperPilotSessionPackageError(_ERR)

    # Stage 3: construct and dry-assemble the existing launch request --
    # same discipline for the assembly object, its own assembly_spec
    # property, and every field read from either.
    launch_failed = False
    assembly_spec_id = ""
    spec_target_session = None
    open_position_count = 0
    candidate_count = 0
    spec: PromotedOperationalAssemblySpec | None = None
    try:
        launch_request = PromotedOperationalLaunchRequest(
            schema_version=PROMOTED_OPERATIONAL_LAUNCH_REQUEST_SCHEMA_VERSION,
            preparation_id=preparation_id,
            portfolio_artifact_id=portfolio_artifact_id,
            expected_quote_source_id=package_request.expected_quote_source_id,
            open_listing_keys=package_request.open_listing_keys,
            decision_not_before=package_request.decision_not_before,
            decision_deadline=package_request.decision_deadline,
            quote_gate_policy=package_request.quote_gate_policy,
            allocation_policy=package_request.allocation_policy,
            maximum_quote_chunk_size=package_request.maximum_quote_chunk_size,
            binding_bucket=package_request.binding_bucket,
        )
        assembly = prepare_promoted_operational_launch(
            request=launch_request,
            preparation_resolver=preparations,
            portfolio_artifact_resolver=portfolio_store,
        )
        if type(assembly) is not PromotedOperationalRuntimeAssembly:
            raise PromotedPaperPilotSessionPackageError(_ERR)
        candidate_spec = assembly.assembly_spec
        if type(candidate_spec) is not PromotedOperationalAssemblySpec:
            raise PromotedPaperPilotSessionPackageError(_ERR)
        candidate_spec.verify_content_identity()
        if (
            candidate_spec.preparation_id != preparation_id
            or candidate_spec.portfolio_artifact_id != portfolio_artifact_id
            or candidate_spec.target_session != package_request.target_session
        ):
            raise PromotedPaperPilotSessionPackageError(_ERR)
        assembly_preparation = assembly.preparation
        if type(assembly_preparation) is not VerifiedPromotedOperationalPreparation:
            raise PromotedPaperPilotSessionPackageError(_ERR)
        spec = candidate_spec
        assembly_spec_id = spec.assembly_spec_id
        spec_target_session = spec.target_session
        open_position_count = len(spec.open_listing_keys)
        candidate_count = len(assembly_preparation.candidates)
    except Exception:
        launch_failed = True
    if launch_failed or spec is None:
        raise PromotedPaperPilotSessionPackageError(_ERR)

    # Stage 4: publish the assembly spec create-once.
    publish_failed = False
    try:
        publish_promoted_operational_launch_assembly_spec_file(output_assembly_spec_file, spec)
    except Exception:
        publish_failed = True
    if publish_failed:
        raise PromotedPaperPilotSessionPackageError(_ERR)

    # Stage 5: construct the sanitized result -- every field read above was
    # already captured as a plain, already-validated built-in value, so
    # this construction cannot itself dereference a foreign/malicious
    # dependency property; it is still wrapped for the same uniform
    # discipline.
    result_failed = False
    result: PromotedPaperPilotSessionPackageResult | None = None
    try:
        result = PromotedPaperPilotSessionPackageResult(
            target_session=spec_target_session,
            research_run_id=package_request.research_run_id,
            preparation_id=preparation_id,
            portfolio_artifact_id=portfolio_artifact_id,
            portfolio_snapshot_id=portfolio_snapshot_id,
            assembly_spec_id=assembly_spec_id,
            candidate_count=candidate_count,
            open_position_count=open_position_count,
        )
    except Exception:
        result_failed = True
    if result_failed or result is None:
        raise PromotedPaperPilotSessionPackageError(_ERR)
    return result
