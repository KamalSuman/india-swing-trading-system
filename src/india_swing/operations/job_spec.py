from __future__ import annotations

import json
import re
from dataclasses import dataclass, field, fields
from datetime import date, datetime, timezone
from pathlib import Path

from india_swing._filesystem import FileSafetyError, read_stable_regular_file
from india_swing.identity import content_id
from india_swing.market_data.models import MAXIMUM_QUOTE_KEYS
from india_swing.risk.swing_portfolio import SwingPortfolioSizingPolicy
from india_swing.signals.opportunity_ranking import SwingOpportunityRankingPolicy
from india_swing.signals.quote_gate import SwingQuoteGatePolicy

from .models import SwingOperationalRunSpec


JOB_SPEC_SCHEMA_VERSION = "swing-operational-job-spec/v1"
JOB_SPEC_CODEC_VERSION = "swing-operational-job-spec-json/v1"
MAXIMUM_JOB_SPEC_BYTES = 64 * 1024

_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_CONCRETE_PATH_TYPE: type = type(Path())


class SwingOperationalJobSpecError(ValueError):
    pass


def _sha(value: object, name: str) -> str:
    if type(value) is not str or _SHA256.fullmatch(value) is None:
        raise SwingOperationalJobSpecError(f"{name} must be a lowercase SHA-256")
    return value


def _utc(value: object, name: str) -> datetime:
    if type(value) is not datetime or value.tzinfo is None:
        raise SwingOperationalJobSpecError(f"{name} must be timezone-aware")
    try:
        offset = value.utcoffset()
    except Exception:
        raise SwingOperationalJobSpecError(f"{name} has invalid timezone behavior") from None
    if offset is None:
        raise SwingOperationalJobSpecError(f"{name} must be timezone-aware")
    return value.astimezone(timezone.utc)


@dataclass(frozen=True, slots=True)
class SwingOperationalJobSpec:
    proposal_batch_id: str
    portfolio_artifact_id: str
    portfolio_snapshot_id: str
    target_session: date
    decision_not_before: datetime
    decision_deadline: datetime
    quote_policy_id: str
    ranking_policy_id: str
    sizing_policy_id: str
    expected_operational_spec_id: str
    quote_chunk_size: int = MAXIMUM_QUOTE_KEYS
    maximum_portfolio_age_seconds: int = 300
    mode: str = "PAPER_ONLY"
    schema_version: str = JOB_SPEC_SCHEMA_VERSION
    job_spec_id: str = field(init=False)

    def __post_init__(self) -> None:
        for value, name in (
            (self.proposal_batch_id, "proposal_batch_id"),
            (self.portfolio_artifact_id, "portfolio_artifact_id"),
            (self.portfolio_snapshot_id, "portfolio_snapshot_id"),
            (self.quote_policy_id, "quote_policy_id"),
            (self.ranking_policy_id, "ranking_policy_id"),
            (self.sizing_policy_id, "sizing_policy_id"),
            (self.expected_operational_spec_id, "expected_operational_spec_id"),
        ):
            _sha(value, name)
        if type(self.target_session) is not date:
            raise SwingOperationalJobSpecError("target_session must be an exact date")
        object.__setattr__(
            self,
            "decision_not_before",
            _utc(self.decision_not_before, "decision_not_before"),
        )
        object.__setattr__(
            self,
            "decision_deadline",
            _utc(self.decision_deadline, "decision_deadline"),
        )
        if self.decision_not_before >= self.decision_deadline:
            raise SwingOperationalJobSpecError("job decision window is invalid")
        if (
            type(self.quote_chunk_size) is not int
            or not 1 <= self.quote_chunk_size <= MAXIMUM_QUOTE_KEYS
        ):
            raise SwingOperationalJobSpecError("job quote chunk size is invalid")
        if (
            type(self.maximum_portfolio_age_seconds) is not int
            or not 1 <= self.maximum_portfolio_age_seconds <= 86_400
        ):
            raise SwingOperationalJobSpecError("portfolio maximum age is invalid")
        if self.mode != "PAPER_ONLY" or self.schema_version != JOB_SPEC_SCHEMA_VERSION:
            raise SwingOperationalJobSpecError("job authority boundary is invalid")
        object.__setattr__(self, "job_spec_id", self._calculated_id())

    def _calculated_id(self) -> str:
        return content_id(
            {
                item.name: getattr(self, item.name)
                for item in fields(self)
                if item.name != "job_spec_id"
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        try:
            fresh = SwingOperationalJobSpec(
                **{
                    item.name: getattr(self, item.name)
                    for item in fields(self)
                    if item.name != "job_spec_id"
                }
            )
        except Exception:
            raise SwingOperationalJobSpecError("job spec content identity failed") from None
        if self.job_spec_id != fresh.job_spec_id:
            raise SwingOperationalJobSpecError("job spec content identity failed")


def job_spec_from_operational_spec(
    *,
    operational_spec: SwingOperationalRunSpec,
    portfolio_artifact_id: str,
    portfolio_snapshot_id: str,
    maximum_portfolio_age_seconds: int = 300,
) -> SwingOperationalJobSpec:
    if type(operational_spec) is not SwingOperationalRunSpec:
        raise SwingOperationalJobSpecError("operational spec must be exact")
    operational_spec.verify_content_identity()
    return SwingOperationalJobSpec(
        proposal_batch_id=operational_spec.proposal_batch.batch_id,
        portfolio_artifact_id=portfolio_artifact_id,
        portfolio_snapshot_id=portfolio_snapshot_id,
        target_session=operational_spec.target_session,
        decision_not_before=operational_spec.decision_not_before,
        decision_deadline=operational_spec.decision_deadline,
        quote_policy_id=operational_spec.quote_policy.policy_id,
        ranking_policy_id=operational_spec.ranking_policy.policy_id,
        sizing_policy_id=operational_spec.sizing_policy.policy_id,
        expected_operational_spec_id=operational_spec.spec_id,
        quote_chunk_size=operational_spec.quote_chunk_size,
        maximum_portfolio_age_seconds=maximum_portfolio_age_seconds,
    )


def _spec_data(value: SwingOperationalJobSpec) -> dict[str, object]:
    value.verify_content_identity()
    return {
        "decision_deadline": value.decision_deadline.isoformat(),
        "decision_not_before": value.decision_not_before.isoformat(),
        "expected_operational_spec_id": value.expected_operational_spec_id,
        "job_spec_id": value.job_spec_id,
        "maximum_portfolio_age_seconds": value.maximum_portfolio_age_seconds,
        "mode": value.mode,
        "portfolio_artifact_id": value.portfolio_artifact_id,
        "portfolio_snapshot_id": value.portfolio_snapshot_id,
        "proposal_batch_id": value.proposal_batch_id,
        "quote_chunk_size": value.quote_chunk_size,
        "quote_policy_id": value.quote_policy_id,
        "ranking_policy_id": value.ranking_policy_id,
        "schema_version": value.schema_version,
        "sizing_policy_id": value.sizing_policy_id,
        "target_session": value.target_session.isoformat(),
    }


def encode_swing_operational_job_spec(value: SwingOperationalJobSpec) -> bytes:
    if type(value) is not SwingOperationalJobSpec:
        raise SwingOperationalJobSpecError("job spec must be exact")
    payload = (
        json.dumps(
            {
                "codec_schema_version": JOB_SPEC_CODEC_VERSION,
                "spec": _spec_data(value),
            },
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )
        + "\n"
    ).encode("utf-8")
    if len(payload) > MAXIMUM_JOB_SPEC_BYTES:
        raise SwingOperationalJobSpecError("job spec exceeds its size limit")
    return payload


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise SwingOperationalJobSpecError("job spec contains duplicate keys")
        result[key] = value
    return result


_SPEC_FIELDS = {
    "decision_deadline",
    "decision_not_before",
    "expected_operational_spec_id",
    "job_spec_id",
    "maximum_portfolio_age_seconds",
    "mode",
    "portfolio_artifact_id",
    "portfolio_snapshot_id",
    "proposal_batch_id",
    "quote_chunk_size",
    "quote_policy_id",
    "ranking_policy_id",
    "schema_version",
    "sizing_policy_id",
    "target_session",
}


def _strict_date(value: object) -> date:
    if type(value) is not str:
        raise ValueError
    result = date.fromisoformat(value)
    if result.isoformat() != value:
        raise ValueError
    return result


def _strict_datetime(value: object) -> datetime:
    if type(value) is not str:
        raise ValueError
    result = datetime.fromisoformat(value)
    if (
        result.tzinfo is None
        or result.isoformat() != value
        or result.astimezone(timezone.utc).isoformat() != value
    ):
        raise ValueError
    return result


def parse_swing_operational_job_spec(payload: bytes) -> SwingOperationalJobSpec:
    if type(payload) is not bytes or not payload or len(payload) > MAXIMUM_JOB_SPEC_BYTES:
        raise SwingOperationalJobSpecError("stored job spec is invalid")
    try:
        raw = json.loads(
            payload.decode("utf-8"),
            object_pairs_hook=_unique_object,
            parse_float=lambda _: (_ for _ in ()).throw(ValueError()),
            parse_constant=lambda _: (_ for _ in ()).throw(ValueError()),
        )
        if type(raw) is not dict or set(raw) != {"codec_schema_version", "spec"}:
            raise ValueError
        if raw["codec_schema_version"] != JOB_SPEC_CODEC_VERSION:
            raise ValueError
        value = raw["spec"]
        if type(value) is not dict or set(value) != _SPEC_FIELDS:
            raise ValueError
        stored_id = _sha(value["job_spec_id"], "job_spec_id")
        spec = SwingOperationalJobSpec(
            proposal_batch_id=value["proposal_batch_id"],
            portfolio_artifact_id=value["portfolio_artifact_id"],
            portfolio_snapshot_id=value["portfolio_snapshot_id"],
            target_session=_strict_date(value["target_session"]),
            decision_not_before=_strict_datetime(value["decision_not_before"]),
            decision_deadline=_strict_datetime(value["decision_deadline"]),
            quote_policy_id=value["quote_policy_id"],
            ranking_policy_id=value["ranking_policy_id"],
            sizing_policy_id=value["sizing_policy_id"],
            expected_operational_spec_id=value["expected_operational_spec_id"],
            quote_chunk_size=value["quote_chunk_size"],
            maximum_portfolio_age_seconds=value["maximum_portfolio_age_seconds"],
            mode=value["mode"],
            schema_version=value["schema_version"],
        )
        if spec.job_spec_id != stored_id:
            raise SwingOperationalJobSpecError("stored job spec identity differs")
        if encode_swing_operational_job_spec(spec) != payload:
            raise SwingOperationalJobSpecError("stored job spec is not canonical")
        return spec
    except SwingOperationalJobSpecError:
        raise
    except (KeyError, TypeError, UnicodeDecodeError, ValueError, json.JSONDecodeError):
        raise SwingOperationalJobSpecError("stored job spec is invalid") from None


def load_swing_operational_job_spec_file(path: Path) -> SwingOperationalJobSpec:
    if (
        type(path) is not _CONCRETE_PATH_TYPE
        or not path.is_absolute()
        or ".." in path.parts
        or path.parent == path
    ):
        raise SwingOperationalJobSpecError("job spec file could not be loaded")
    try:
        payload = read_stable_regular_file(path, maximum_bytes=MAXIMUM_JOB_SPEC_BYTES)
        return parse_swing_operational_job_spec(payload)
    except (FileSafetyError, SwingOperationalJobSpecError):
        raise SwingOperationalJobSpecError("job spec file could not be loaded") from None


def default_job_policies() -> tuple[
    SwingQuoteGatePolicy,
    SwingOpportunityRankingPolicy,
    SwingPortfolioSizingPolicy,
]:
    return (
        SwingQuoteGatePolicy(),
        SwingOpportunityRankingPolicy(),
        SwingPortfolioSizingPolicy(),
    )
