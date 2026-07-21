from __future__ import annotations

import json
import os
import re
import stat
import tempfile
from dataclasses import dataclass, field, fields
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Protocol

from india_swing._filesystem import (
    FileLockUnavailable,
    FileSafetyError,
    advisory_file_lock,
    read_stable_regular_file,
)
from india_swing.calendar_data.materialization_store import (
    LocalCalendarMaterializationStore,
)
from india_swing.historical_prices import LocalHistoricalPriceArtifactStore
from india_swing.identity import content_id
from india_swing.outcomes import Preventability, ReviewClassification, ReviewConfidence
from india_swing.paper_trades import LocalPaperTradeLedger, PaperTradeSummary
from india_swing.tick_sizes import LocalTickSizeSnapshotStore

from .models import (
    PaperInstrumentBinding,
    PaperOutcomeExitReason,
    PaperOutcomeObservation,
    PaperOutcomePolicy,
    PaperOutcomeReplay,
    PaperOutcomeStatus,
    bind_paper_instrument,
    observe_paper_session,
)
from .reconciliation import ReconciliationResult, reconcile_paper_outcome
from .resolver import replay_paper_outcome


_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_ISIN = re.compile(r"[A-Z]{2}[A-Z0-9]{9}[0-9]\Z")
_MAXIMUM_RECORD_BYTES = 4 * 1024 * 1024
_MAXIMUM_SPEC_BYTES = 1024 * 1024
_SPEC_SCHEMA = "paper-outcome-job-spec/v1"
_REVIEW_SCHEMA = "paper-outcome-review/v1"
_RECORD_SCHEMA = "paper-outcome-run-record/v1"
_SPEC_CODEC = "paper-outcome-job-spec-json/v1"
_RECORD_CODEC = "paper-outcome-run-record-json/v1"


class PaperOutcomeOperationalError(RuntimeError):
    pass


class PaperOutcomeOperationalConflict(PaperOutcomeOperationalError):
    pass


class PaperOutcomeOperationalNotFound(PaperOutcomeOperationalError):
    pass


def _sha(value: object, name: str) -> str:
    if type(value) is not str or _SHA256.fullmatch(value) is None:
        raise PaperOutcomeOperationalError(f"{name} must be a lowercase SHA-256")
    return value


def _utc(value: object, name: str) -> datetime:
    if type(value) is not datetime or value.tzinfo is None or value.utcoffset() is None:
        raise PaperOutcomeOperationalError(f"{name} must be timezone-aware")
    return value.astimezone(timezone.utc)


def _decimal_text(value: Decimal) -> str:
    if type(value) is not Decimal or not value.is_finite():
        raise PaperOutcomeOperationalError("paper outcome decimal is invalid")
    return str(value)


def _identity(value: object, omitted: set[str]) -> str:
    return content_id(
        {
            item.name: getattr(value, item.name)
            for item in fields(value)
            if item.name not in omitted
        },
        length=64,
    )


@dataclass(frozen=True, slots=True)
class PaperOutcomeJobSpec:
    registration_id: str
    calendar_materialization_id: str
    tick_snapshot_id: str
    historical_artifact_ids: tuple[str, ...]
    series: str
    validated_isin: str
    as_of: datetime
    policy: PaperOutcomePolicy
    expected_replay_id: str
    schema_version: str = _SPEC_SCHEMA
    job_spec_id: str = field(init=False)

    def __post_init__(self) -> None:
        for value, name in (
            (self.registration_id, "registration_id"),
            (self.calendar_materialization_id, "calendar_materialization_id"),
            (self.tick_snapshot_id, "tick_snapshot_id"),
            (self.expected_replay_id, "expected_replay_id"),
        ):
            _sha(value, name)
        if (
            type(self.historical_artifact_ids) is not tuple
            or not self.historical_artifact_ids
            or len(set(self.historical_artifact_ids)) != len(self.historical_artifact_ids)
        ):
            raise PaperOutcomeOperationalError(
                "historical_artifact_ids must be a non-empty unique tuple"
            )
        for value in self.historical_artifact_ids:
            _sha(value, "historical_artifact_id")
        if (
            type(self.series) is not str
            or not self.series
            or self.series != self.series.strip().upper()
        ):
            raise PaperOutcomeOperationalError("series must be normalized")
        if type(self.validated_isin) is not str or _ISIN.fullmatch(self.validated_isin) is None:
            raise PaperOutcomeOperationalError("validated_isin is invalid")
        object.__setattr__(self, "as_of", _utc(self.as_of, "as_of"))
        if type(self.policy) is not PaperOutcomePolicy:
            raise PaperOutcomeOperationalError("policy must be exact")
        self.policy.verify_content_identity()
        object.__setattr__(
            self,
            "policy",
            PaperOutcomePolicy(
                slippage_bps=self.policy.slippage_bps,
                maximum_participation=self.policy.maximum_participation,
                policy_version=self.policy.policy_version,
            ),
        )
        if self.schema_version != _SPEC_SCHEMA:
            raise PaperOutcomeOperationalError("unsupported paper outcome job spec")
        object.__setattr__(self, "job_spec_id", _identity(self, {"job_spec_id"}))

    def verify_content_identity(self) -> None:
        try:
            fresh = PaperOutcomeJobSpec(
                **{
                    item.name: getattr(self, item.name)
                    for item in fields(self)
                    if item.name != "job_spec_id"
                }
            )
        except Exception:
            raise PaperOutcomeOperationalError("job spec identity verification failed") from None
        if fresh.job_spec_id != self.job_spec_id:
            raise PaperOutcomeOperationalError("job spec identity verification failed")


def _spec_body(value: PaperOutcomeJobSpec) -> dict[str, object]:
    return {
        "as_of": value.as_of.isoformat(),
        "calendar_materialization_id": value.calendar_materialization_id,
        "expected_replay_id": value.expected_replay_id,
        "historical_artifact_ids": list(value.historical_artifact_ids),
        "job_spec_id": value.job_spec_id,
        "policy": {
            "maximum_participation": str(value.policy.maximum_participation),
            "policy_id": value.policy.policy_id,
            "policy_version": value.policy.policy_version,
            "slippage_bps": str(value.policy.slippage_bps),
        },
        "registration_id": value.registration_id,
        "schema_version": value.schema_version,
        "series": value.series,
        "tick_snapshot_id": value.tick_snapshot_id,
        "validated_isin": value.validated_isin,
    }


def encode_paper_outcome_job_spec(value: PaperOutcomeJobSpec) -> bytes:
    if type(value) is not PaperOutcomeJobSpec:
        raise PaperOutcomeOperationalError("job spec must be exact")
    value.verify_content_identity()
    return (
        json.dumps(
            {"codec_schema_version": _SPEC_CODEC, "spec": _spec_body(value)},
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )
        + "\n"
    ).encode("utf-8")


def decode_paper_outcome_job_spec(payload: bytes) -> PaperOutcomeJobSpec:
    if type(payload) is not bytes or not payload or len(payload) > _MAXIMUM_SPEC_BYTES:
        raise PaperOutcomeOperationalError("paper outcome job spec bytes are invalid")
    try:
        root = json.loads(
            payload.decode("utf-8"),
            object_pairs_hook=_unique_object,
            parse_float=lambda _: (_ for _ in ()).throw(ValueError()),
            parse_constant=lambda _: (_ for _ in ()).throw(ValueError()),
        )
        if type(root) is not dict or set(root) != {"codec_schema_version", "spec"}:
            raise ValueError
        if root["codec_schema_version"] != _SPEC_CODEC:
            raise ValueError
        raw = root["spec"]
        expected = {
            "as_of", "calendar_materialization_id", "expected_replay_id",
            "historical_artifact_ids", "job_spec_id", "policy", "registration_id",
            "schema_version", "series", "tick_snapshot_id", "validated_isin",
        }
        if type(raw) is not dict or set(raw) != expected:
            raise ValueError
        policy_raw = raw["policy"]
        if type(policy_raw) is not dict or set(policy_raw) != {
            "maximum_participation", "policy_id", "policy_version", "slippage_bps"
        }:
            raise ValueError
        policy = PaperOutcomePolicy(
            slippage_bps=Decimal(policy_raw["slippage_bps"]),
            maximum_participation=Decimal(policy_raw["maximum_participation"]),
            policy_version=policy_raw["policy_version"],
        )
        if policy.policy_id != policy_raw["policy_id"]:
            raise ValueError
        stored_id = raw["job_spec_id"]
        value = PaperOutcomeJobSpec(
            registration_id=raw["registration_id"],
            calendar_materialization_id=raw["calendar_materialization_id"],
            tick_snapshot_id=raw["tick_snapshot_id"],
            historical_artifact_ids=tuple(raw["historical_artifact_ids"]),
            series=raw["series"],
            validated_isin=raw["validated_isin"],
            as_of=datetime.fromisoformat(raw["as_of"]),
            policy=policy,
            expected_replay_id=raw["expected_replay_id"],
            schema_version=raw["schema_version"],
        )
        if value.job_spec_id != stored_id or encode_paper_outcome_job_spec(value) != payload:
            raise ValueError
        return value
    except Exception:
        raise PaperOutcomeOperationalError("paper outcome job spec is invalid") from None


def load_paper_outcome_job_spec_file(path: Path) -> PaperOutcomeJobSpec:
    if type(path) is not type(Path()):
        raise PaperOutcomeOperationalError("paper outcome job spec path is invalid")
    try:
        payload = read_stable_regular_file(path, maximum_bytes=_MAXIMUM_SPEC_BYTES)
    except Exception:
        raise PaperOutcomeOperationalError("paper outcome job spec file is unavailable") from None
    return decode_paper_outcome_job_spec(payload)


@dataclass(frozen=True, slots=True)
class PaperOutcomeEvidence:
    registration: object
    binding: PaperInstrumentBinding
    calendar: object
    observations: tuple[PaperOutcomeObservation, ...]

    def __post_init__(self) -> None:
        from india_swing.paper_trades import PaperTradeRegistration
        from india_swing.reference.calendar import CalendarSnapshot

        if type(self.registration) is not PaperTradeRegistration:
            raise PaperOutcomeOperationalError("paper registration evidence must be exact")
        if type(self.binding) is not PaperInstrumentBinding:
            raise PaperOutcomeOperationalError("instrument binding evidence must be exact")
        if type(self.calendar) is not CalendarSnapshot:
            raise PaperOutcomeOperationalError("calendar evidence must be exact")
        if (
            type(self.observations) is not tuple
            or any(type(value) is not PaperOutcomeObservation for value in self.observations)
        ):
            raise PaperOutcomeOperationalError("paper observations must be an exact tuple")
        self.registration.verify_content_identity()
        self.binding.verify_content_identity()
        self.calendar.verify_content_identity()
        for value in self.observations:
            value.verify_content_identity()


class PaperOutcomeEvidenceSource(Protocol):
    def load(self, spec: PaperOutcomeJobSpec) -> PaperOutcomeEvidence: ...


@dataclass(frozen=True, slots=True)
class LocalPaperOutcomeEvidenceSource:
    paper_ledger: LocalPaperTradeLedger
    calendar_store: LocalCalendarMaterializationStore
    tick_store: LocalTickSizeSnapshotStore
    historical_store: LocalHistoricalPriceArtifactStore

    def __post_init__(self) -> None:
        if (
            type(self.paper_ledger) is not LocalPaperTradeLedger
            or type(self.calendar_store) is not LocalCalendarMaterializationStore
            or type(self.tick_store) is not LocalTickSizeSnapshotStore
            or type(self.historical_store) is not LocalHistoricalPriceArtifactStore
        ):
            raise PaperOutcomeOperationalError(
                "local paper outcome evidence stores must be exact"
            )
        if (
            self.calendar_store.daily_reports_root
            != self.historical_store.daily_reports_root
        ):
            raise PaperOutcomeOperationalError(
                "paper outcome daily-report evidence roots differ"
            )

    def load(self, spec: PaperOutcomeJobSpec) -> PaperOutcomeEvidence:
        if type(spec) is not PaperOutcomeJobSpec:
            raise PaperOutcomeOperationalError("job spec must be exact")
        spec.verify_content_identity()
        try:
            registration = self.paper_ledger.get_registration(spec.registration_id)
            stored_calendar = self.calendar_store.get(spec.calendar_materialization_id)
            calendar = stored_calendar.materialization.calendar_snapshot
            tick_snapshot = self.tick_store.get(spec.tick_snapshot_id)
            binding = bind_paper_instrument(
                registration,
                tick_snapshot,
                series=spec.series,
                validated_isin=spec.validated_isin,
            )
            artifacts = tuple(
                self.historical_store.get(value).artifact
                for value in spec.historical_artifact_ids
            )
            if artifacts != tuple(sorted(artifacts, key=lambda value: value.market_session)):
                raise ValueError
            observations = tuple(
                observe_paper_session(value, calendar, binding) for value in artifacts
            )
            return PaperOutcomeEvidence(
                registration=registration,
                binding=binding,
                calendar=calendar,
                observations=observations,
            )
        except Exception:
            raise PaperOutcomeOperationalError(
                "paper outcome evidence could not be loaded safely"
            ) from None


def prepare_paper_outcome_job_spec(
    *,
    registration_id: str,
    calendar_materialization_id: str,
    tick_snapshot_id: str,
    historical_artifact_ids: tuple[str, ...],
    series: str,
    validated_isin: str,
    as_of: datetime,
    policy: PaperOutcomePolicy,
    evidence_source: PaperOutcomeEvidenceSource,
) -> PaperOutcomeJobSpec:
    """Build a sealed spec by replaying the exact immutable evidence once.

    The temporary all-zero expected replay ID is only a local construction
    placeholder.  It is never returned or persisted.  The production evidence
    source selects every object solely from the other exact IDs, and the final
    spec pins the verified replay ID produced from those objects.
    """

    try:
        draft = PaperOutcomeJobSpec(
            registration_id=registration_id,
            calendar_materialization_id=calendar_materialization_id,
            tick_snapshot_id=tick_snapshot_id,
            historical_artifact_ids=historical_artifact_ids,
            series=series,
            validated_isin=validated_isin,
            as_of=as_of,
            policy=policy,
            expected_replay_id="0" * 64,
        )
        evidence = evidence_source.load(draft)
        if type(evidence) is not PaperOutcomeEvidence:
            raise ValueError
        if (
            evidence.registration.registration_id != draft.registration_id
            or evidence.binding.registration_id != draft.registration_id
            or evidence.binding.tick_snapshot_id != draft.tick_snapshot_id
        ):
            raise ValueError
        replay = replay_paper_outcome(
            registration=evidence.registration,
            binding=evidence.binding,
            calendar=evidence.calendar,
            observations=evidence.observations,
            as_of=draft.as_of,
            policy=draft.policy,
        )
        return PaperOutcomeJobSpec(
            registration_id=draft.registration_id,
            calendar_materialization_id=draft.calendar_materialization_id,
            tick_snapshot_id=draft.tick_snapshot_id,
            historical_artifact_ids=draft.historical_artifact_ids,
            series=draft.series,
            validated_isin=draft.validated_isin,
            as_of=draft.as_of,
            policy=draft.policy,
            expected_replay_id=replay.replay_id,
        )
    except PaperOutcomeOperationalError:
        raise
    except Exception:
        raise PaperOutcomeOperationalError(
            "paper outcome job spec preparation failed safely"
        ) from None


@dataclass(frozen=True, slots=True)
class PaperOutcomeReview:
    registration_id: str
    replay_id: str
    net_pnl: Decimal
    planned_risk: Decimal
    realized_r: Decimal
    classification: ReviewClassification
    confidence: ReviewConfidence
    preventability: Preventability
    known_facts: tuple[str, ...]
    uncertainties: tuple[str, ...]
    likely_explanation: str
    action: str
    schema_version: str = _REVIEW_SCHEMA
    review_id: str = field(init=False)

    def __post_init__(self) -> None:
        _sha(self.registration_id, "registration_id")
        _sha(self.replay_id, "replay_id")
        _decimal_text(self.net_pnl)
        if (
            type(self.planned_risk) is not Decimal
            or not self.planned_risk.is_finite()
            or self.planned_risk <= 0
        ):
            raise PaperOutcomeOperationalError("review planned risk is invalid")
        _decimal_text(self.realized_r)
        if self.realized_r != self.net_pnl / self.planned_risk:
            raise PaperOutcomeOperationalError("review realized R differs from P&L")
        if type(self.classification) is not ReviewClassification:
            raise PaperOutcomeOperationalError("review classification must be exact")
        if type(self.confidence) is not ReviewConfidence:
            raise PaperOutcomeOperationalError("review confidence must be exact")
        if type(self.preventability) is not Preventability:
            raise PaperOutcomeOperationalError("review preventability must be exact")
        allowed_review_states = {
            ReviewClassification.PROFITABLE_OUTCOME: (
                ReviewConfidence.HIGH,
                Preventability.NO,
            ),
            ReviewClassification.TAIL_OR_GAP_LOSS: (
                ReviewConfidence.MEDIUM,
                Preventability.PARTIAL,
            ),
            ReviewClassification.UNRESOLVED_FORECAST_MISS: (
                ReviewConfidence.LOW,
                Preventability.UNRESOLVED,
            ),
        }
        if (
            self.classification not in allowed_review_states
            or (self.confidence, self.preventability)
            != allowed_review_states[self.classification]
            or (
                self.classification is ReviewClassification.PROFITABLE_OUTCOME
                and self.net_pnl < 0
            )
            or (
                self.classification is not ReviewClassification.PROFITABLE_OUTCOME
                and self.net_pnl >= 0
            )
        ):
            raise PaperOutcomeOperationalError(
                "paper outcome review evidence classification is inconsistent"
            )
        if (
            type(self.known_facts) is not tuple
            or type(self.uncertainties) is not tuple
            or not self.known_facts
            or any(type(value) is not str or not value for value in self.known_facts)
            or any(type(value) is not str or not value for value in self.uncertainties)
        ):
            raise PaperOutcomeOperationalError("review evidence statements are invalid")
        if (
            type(self.likely_explanation) is not str
            or not self.likely_explanation
            or type(self.action) is not str
            or not self.action
        ):
            raise PaperOutcomeOperationalError("review narrative is invalid")
        if self.schema_version != _REVIEW_SCHEMA:
            raise PaperOutcomeOperationalError("unsupported paper outcome review")
        object.__setattr__(self, "review_id", _identity(self, {"review_id"}))

    def verify_content_identity(self) -> None:
        try:
            fresh = PaperOutcomeReview(
                **{
                    item.name: getattr(self, item.name)
                    for item in fields(self)
                    if item.name != "review_id"
                }
            )
        except Exception:
            raise PaperOutcomeOperationalError("review identity verification failed") from None
        if fresh.review_id != self.review_id:
            raise PaperOutcomeOperationalError("review identity verification failed")


def build_paper_outcome_review(
    replay: PaperOutcomeReplay,
    summary: PaperTradeSummary,
    *,
    planned_risk: Decimal,
    stop_price: Decimal,
) -> PaperOutcomeReview | None:
    if replay.status is not PaperOutcomeStatus.CLOSED:
        return None
    if summary.gross_pnl is None or summary.estimated_net_pnl is None:
        raise PaperOutcomeOperationalError("closed paper outcome has no P&L")
    if type(planned_risk) is not Decimal or not planned_risk.is_finite() or planned_risk <= 0:
        raise PaperOutcomeOperationalError("planned paper risk is invalid")
    if type(stop_price) is not Decimal or not stop_price.is_finite() or stop_price <= 0:
        raise PaperOutcomeOperationalError("paper stop price is invalid")
    net = summary.estimated_net_pnl
    realized_r = net / planned_risk
    exit_reason = replay.exit.reason
    known = (
        f"estimated net P&L: {net}",
        f"realized R: {realized_r}",
        f"exit reason: {exit_reason.value}",
        f"replay evidence count: {len(replay.source_observation_ids)}",
    )
    uncertainties = (
        "No point-in-time market, sector, or news attribution evidence was supplied.",
        "Paper fills and costs are estimates, not broker contract-note evidence.",
    )
    if net >= 0:
        classification = ReviewClassification.PROFITABLE_OUTCOME
        confidence = ReviewConfidence.HIGH
        preventability = Preventability.NO
        explanation = "The conservative paper replay completed with non-negative estimated net P&L."
        action = "Record it with the same evidence standard as losses; do not generalize from one trade."
    elif (
        exit_reason is PaperOutcomeExitReason.STOP
        and replay.exit.price < stop_price
    ):
        # Price-only evidence cannot prove a news cause.  A materially adverse
        # stop fill is classified as tail/gap behavior, not a forecast cause.
        classification = ReviewClassification.TAIL_OR_GAP_LOSS
        confidence = ReviewConfidence.MEDIUM
        preventability = Preventability.PARTIAL
        explanation = "The conservative stop fill was materially adverse to the planned risk path."
        action = "Review liquidity, gap reserves, and size across a predeclared batch."
    else:
        classification = ReviewClassification.UNRESOLVED_FORECAST_MISS
        confidence = ReviewConfidence.LOW
        preventability = Preventability.UNRESOLVED
        explanation = "Observed price evidence establishes the loss but not a unique causal explanation."
        action = "Keep the cause unresolved and evaluate calibration only across a predeclared batch."
    return PaperOutcomeReview(
        registration_id=replay.registration_id,
        replay_id=replay.replay_id,
        net_pnl=net,
        planned_risk=planned_risk,
        realized_r=realized_r,
        classification=classification,
        confidence=confidence,
        preventability=preventability,
        known_facts=known,
        uncertainties=uncertainties,
        likely_explanation=explanation,
        action=action,
    )


@dataclass(frozen=True, slots=True)
class PaperOutcomeRunRecord:
    job_spec_id: str
    registration_id: str
    symbol: str
    replay_id: str
    binding_id: str
    policy_id: str
    calendar_snapshot_id: str
    as_of: datetime
    outcome_status: PaperOutcomeStatus
    reason_code: str
    source_observation_ids: tuple[str, ...]
    event_ids: tuple[str, ...]
    appended_event_ids: tuple[str, ...]
    gross_pnl: Decimal | None
    estimated_net_pnl: Decimal | None
    review: PaperOutcomeReview | None
    message: str
    schema_version: str = _RECORD_SCHEMA
    record_id: str = field(init=False)

    def __post_init__(self) -> None:
        for value, name in (
            (self.job_spec_id, "job_spec_id"),
            (self.registration_id, "registration_id"),
            (self.replay_id, "replay_id"),
            (self.binding_id, "binding_id"),
            (self.policy_id, "policy_id"),
            (self.calendar_snapshot_id, "calendar_snapshot_id"),
        ):
            _sha(value, name)
        if (
            type(self.symbol) is not str
            or not self.symbol
            or self.symbol != self.symbol.strip().upper()
        ):
            raise PaperOutcomeOperationalError("record symbol must be normalized")
        object.__setattr__(self, "as_of", _utc(self.as_of, "as_of"))
        if type(self.outcome_status) is not PaperOutcomeStatus:
            raise PaperOutcomeOperationalError("outcome status must be exact")
        for values, name in (
            (self.source_observation_ids, "source observation IDs"),
            (self.event_ids, "event IDs"),
            (self.appended_event_ids, "appended event IDs"),
        ):
            if type(values) is not tuple or len(set(values)) != len(values):
                raise PaperOutcomeOperationalError(f"{name} must be a unique tuple")
            for value in values:
                _sha(value, name)
        if not set(self.appended_event_ids).issubset(self.event_ids):
            raise PaperOutcomeOperationalError("appended events are not in the event chain")
        if (self.gross_pnl is None) != (self.estimated_net_pnl is None):
            raise PaperOutcomeOperationalError("P&L fields must be present together")
        if self.gross_pnl is not None:
            _decimal_text(self.gross_pnl)
            _decimal_text(self.estimated_net_pnl)
        if self.outcome_status is PaperOutcomeStatus.CLOSED:
            if self.review is None or self.estimated_net_pnl is None:
                raise PaperOutcomeOperationalError("closed outcome requires P&L and review")
        elif self.review is not None:
            raise PaperOutcomeOperationalError("non-closed outcome cannot carry a review")
        if self.review is not None:
            if type(self.review) is not PaperOutcomeReview:
                raise PaperOutcomeOperationalError("review must be exact")
            self.review.verify_content_identity()
            if (
                self.review.registration_id != self.registration_id
                or self.review.replay_id != self.replay_id
                or self.review.net_pnl != self.estimated_net_pnl
            ):
                raise PaperOutcomeOperationalError("review lineage differs")
        if type(self.reason_code) is not str or not self.reason_code:
            raise PaperOutcomeOperationalError("reason code is invalid")
        if type(self.message) is not str or not self.message or len(self.message) > 4096:
            raise PaperOutcomeOperationalError("outcome message is invalid")
        if self.schema_version != _RECORD_SCHEMA:
            raise PaperOutcomeOperationalError("unsupported paper outcome record")
        object.__setattr__(self, "record_id", _identity(self, {"record_id"}))

    def verify_content_identity(self) -> None:
        try:
            fresh = PaperOutcomeRunRecord(
                **{
                    item.name: getattr(self, item.name)
                    for item in fields(self)
                    if item.name != "record_id"
                }
            )
        except Exception:
            raise PaperOutcomeOperationalError("record identity verification failed") from None
        if fresh.record_id != self.record_id:
            raise PaperOutcomeOperationalError("record identity verification failed")


def _review_body(value: PaperOutcomeReview | None) -> object:
    if value is None:
        return None
    return {
        "action": value.action,
        "classification": value.classification.value,
        "confidence": value.confidence.value,
        "known_facts": list(value.known_facts),
        "likely_explanation": value.likely_explanation,
        "net_pnl": str(value.net_pnl),
        "planned_risk": str(value.planned_risk),
        "preventability": value.preventability.value,
        "realized_r": str(value.realized_r),
        "registration_id": value.registration_id,
        "replay_id": value.replay_id,
        "review_id": value.review_id,
        "schema_version": value.schema_version,
        "uncertainties": list(value.uncertainties),
    }


def _record_body(value: PaperOutcomeRunRecord) -> dict[str, object]:
    return {
        "appended_event_ids": list(value.appended_event_ids),
        "as_of": value.as_of.isoformat(),
        "binding_id": value.binding_id,
        "calendar_snapshot_id": value.calendar_snapshot_id,
        "estimated_net_pnl": None if value.estimated_net_pnl is None else str(value.estimated_net_pnl),
        "event_ids": list(value.event_ids),
        "gross_pnl": None if value.gross_pnl is None else str(value.gross_pnl),
        "job_spec_id": value.job_spec_id,
        "message": value.message,
        "outcome_status": value.outcome_status.value,
        "policy_id": value.policy_id,
        "reason_code": value.reason_code,
        "record_id": value.record_id,
        "registration_id": value.registration_id,
        "replay_id": value.replay_id,
        "review": _review_body(value.review),
        "schema_version": value.schema_version,
        "source_observation_ids": list(value.source_observation_ids),
        "symbol": value.symbol,
    }


def encode_paper_outcome_record(value: PaperOutcomeRunRecord) -> bytes:
    if type(value) is not PaperOutcomeRunRecord:
        raise PaperOutcomeOperationalError("record must be exact")
    value.verify_content_identity()
    return (
        json.dumps(
            {"codec_schema_version": _RECORD_CODEC, "record": _record_body(value)},
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )
        + "\n"
    ).encode("utf-8")


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError
        result[key] = value
    return result


def decode_paper_outcome_record(payload: bytes) -> PaperOutcomeRunRecord:
    try:
        root = json.loads(
            payload.decode("utf-8"),
            object_pairs_hook=_unique_object,
            parse_float=lambda _: (_ for _ in ()).throw(ValueError()),
            parse_constant=lambda _: (_ for _ in ()).throw(ValueError()),
        )
        if type(root) is not dict or set(root) != {"codec_schema_version", "record"}:
            raise ValueError
        if root["codec_schema_version"] != _RECORD_CODEC:
            raise ValueError
        raw = root["record"]
        if type(raw) is not dict or set(raw) != set(_record_body_placeholder()):
            raise ValueError
        review = _review_from_raw(raw["review"])
        stored_id = raw["record_id"]
        value = PaperOutcomeRunRecord(
            job_spec_id=raw["job_spec_id"],
            registration_id=raw["registration_id"],
            symbol=raw["symbol"],
            replay_id=raw["replay_id"],
            binding_id=raw["binding_id"],
            policy_id=raw["policy_id"],
            calendar_snapshot_id=raw["calendar_snapshot_id"],
            as_of=datetime.fromisoformat(raw["as_of"]),
            outcome_status=PaperOutcomeStatus(raw["outcome_status"]),
            reason_code=raw["reason_code"],
            source_observation_ids=tuple(raw["source_observation_ids"]),
            event_ids=tuple(raw["event_ids"]),
            appended_event_ids=tuple(raw["appended_event_ids"]),
            gross_pnl=None if raw["gross_pnl"] is None else Decimal(raw["gross_pnl"]),
            estimated_net_pnl=(
                None if raw["estimated_net_pnl"] is None else Decimal(raw["estimated_net_pnl"])
            ),
            review=review,
            message=raw["message"],
            schema_version=raw["schema_version"],
        )
        if value.record_id != stored_id or encode_paper_outcome_record(value) != payload:
            raise ValueError
        return value
    except Exception:
        raise PaperOutcomeOperationalError("stored paper outcome record is invalid") from None


def _record_body_placeholder() -> dict[str, object]:
    return {
        key: None
        for key in (
            "appended_event_ids", "as_of", "binding_id", "calendar_snapshot_id",
            "estimated_net_pnl", "event_ids", "gross_pnl", "job_spec_id", "message",
            "outcome_status", "policy_id", "reason_code", "record_id", "registration_id",
            "replay_id", "review", "schema_version", "source_observation_ids",
            "symbol",
        )
    }


def _review_from_raw(raw: object) -> PaperOutcomeReview | None:
    if raw is None:
        return None
    expected = {
        "action", "classification", "confidence", "known_facts", "likely_explanation",
        "net_pnl", "planned_risk", "preventability", "realized_r", "registration_id", "replay_id",
        "review_id", "schema_version", "uncertainties",
    }
    if type(raw) is not dict or set(raw) != expected:
        raise ValueError
    stored_id = raw["review_id"]
    value = PaperOutcomeReview(
        registration_id=raw["registration_id"],
        replay_id=raw["replay_id"],
        net_pnl=Decimal(raw["net_pnl"]),
        planned_risk=Decimal(raw["planned_risk"]),
        realized_r=Decimal(raw["realized_r"]),
        classification=ReviewClassification(raw["classification"]),
        confidence=ReviewConfidence(raw["confidence"]),
        preventability=Preventability(raw["preventability"]),
        known_facts=tuple(raw["known_facts"]),
        uncertainties=tuple(raw["uncertainties"]),
        likely_explanation=raw["likely_explanation"],
        action=raw["action"],
        schema_version=raw["schema_version"],
    )
    if value.review_id != stored_id:
        raise ValueError
    return value


def _is_link_like(path: Path) -> bool:
    try:
        status = os.lstat(path)
    except OSError:
        return path.is_symlink()
    return path.is_symlink() or bool(
        getattr(status, "st_file_attributes", 0)
        & getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400)
    )


class LocalPaperOutcomeRunStore:
    def __init__(self, root: Path) -> None:
        self.root = Path(root)

    @property
    def records_root(self) -> Path:
        return self.root / "records"

    def path_for(self, job_spec_id: str) -> Path:
        _sha(job_spec_id, "job_spec_id")
        return self.records_root / f"{job_spec_id}.json"

    def get(self, job_spec_id: str) -> PaperOutcomeRunRecord:
        path = self.path_for(job_spec_id)
        if not path.exists():
            raise PaperOutcomeOperationalNotFound("paper outcome record was not found")
        if not path.is_file() or _is_link_like(path):
            raise PaperOutcomeOperationalError("paper outcome record path is unsafe")
        try:
            value = decode_paper_outcome_record(
                read_stable_regular_file(path, maximum_bytes=_MAXIMUM_RECORD_BYTES)
            )
        except PaperOutcomeOperationalError:
            raise
        except Exception:
            raise PaperOutcomeOperationalError("paper outcome record could not be read") from None
        if value.job_spec_id != job_spec_id:
            raise PaperOutcomeOperationalError("paper outcome record differs from its path")
        return value

    def put(self, value: PaperOutcomeRunRecord) -> PaperOutcomeRunRecord:
        if type(value) is not PaperOutcomeRunRecord:
            raise PaperOutcomeOperationalError("paper outcome record must be exact")
        payload = encode_paper_outcome_record(value)
        try:
            self.records_root.mkdir(parents=True, exist_ok=True)
            if _is_link_like(self.records_root):
                raise PaperOutcomeOperationalError("paper outcome store root is unsafe")
            target = self.path_for(value.job_spec_id)
            with advisory_file_lock(self.records_root / ".paper-outcomes.lock"):
                if target.exists():
                    stored = self.get(value.job_spec_id)
                    if stored != value:
                        raise PaperOutcomeOperationalConflict(
                            "paper outcome job already has another terminal record"
                        )
                    return stored
                descriptor, name = tempfile.mkstemp(
                    prefix=".paper-outcome-", suffix=".tmp", dir=self.records_root
                )
                temporary = Path(name)
                try:
                    with os.fdopen(descriptor, "wb") as handle:
                        handle.write(payload)
                        handle.flush()
                        os.fsync(handle.fileno())
                    os.link(temporary, target)
                finally:
                    temporary.unlink(missing_ok=True)
        except (FileLockUnavailable, FileSafetyError, OSError):
            raise PaperOutcomeOperationalConflict("paper outcome store is unavailable") from None
        return self.get(value.job_spec_id)


def _message(
    replay: PaperOutcomeReplay,
    summary: PaperTradeSummary,
    *,
    symbol: str,
) -> str:
    lines = [
        "PAPER-ONLY SWING OUTCOME — NOT A BROKER EXECUTION",
        f"Symbol: {symbol}",
        f"Status: {replay.status.value}",
        f"Reason: {replay.reason_code}",
    ]
    if summary.estimated_net_pnl is not None:
        lines.append(f"Estimated net P&L: Rs {summary.estimated_net_pnl}")
    lines.extend(
        (
            f"Registration ID: {replay.registration_id}",
            f"Replay ID: {replay.replay_id}",
            "Collection-only evidence; no real order was placed.",
        )
    )
    return "\n".join(lines)


def run_paper_outcome_job(
    *,
    spec: PaperOutcomeJobSpec,
    evidence_source: PaperOutcomeEvidenceSource,
    ledger: LocalPaperTradeLedger,
    record_store: LocalPaperOutcomeRunStore,
) -> PaperOutcomeRunRecord:
    if type(spec) is not PaperOutcomeJobSpec:
        raise PaperOutcomeOperationalError("job spec must be exact")
    if type(ledger) is not LocalPaperTradeLedger:
        raise PaperOutcomeOperationalError("paper ledger must be exact")
    if type(record_store) is not LocalPaperOutcomeRunStore:
        raise PaperOutcomeOperationalError("record store must be exact")
    spec.verify_content_identity()

    try:
        existing = record_store.get(spec.job_spec_id)
    except PaperOutcomeOperationalNotFound:
        existing = None
    if existing is not None:
        events = ledger.list_events(existing.registration_id)
        observed_ids = tuple(value.event_id for value in events)
        if observed_ids[: len(existing.event_ids)] != existing.event_ids:
            raise PaperOutcomeOperationalError("terminal record differs from paper ledger")
        summary = ledger.summary(existing.registration_id)
        if (
            summary.gross_pnl != existing.gross_pnl
            or summary.estimated_net_pnl != existing.estimated_net_pnl
        ):
            # A later OPEN -> CLOSED evolution legitimately changes the current
            # ledger summary.  Historical non-closed records remain valid as
            # long as their event prefix is intact; closed records are terminal
            # and must still equal the ledger's P&L exactly.
            if existing.outcome_status is PaperOutcomeStatus.CLOSED:
                raise PaperOutcomeOperationalError(
                    "terminal record P&L differs from paper ledger"
                )
        return existing

    try:
        evidence = evidence_source.load(spec)
        if type(evidence) is not PaperOutcomeEvidence:
            raise ValueError
        if (
            evidence.registration.registration_id != spec.registration_id
            or evidence.binding.registration_id != spec.registration_id
        ):
            raise ValueError
        replay = replay_paper_outcome(
            registration=evidence.registration,
            binding=evidence.binding,
            calendar=evidence.calendar,
            observations=evidence.observations,
            as_of=spec.as_of,
            policy=spec.policy,
        )
        if replay.replay_id != spec.expected_replay_id:
            raise ValueError
        reconciliation: ReconciliationResult = reconcile_paper_outcome(
            ledger=ledger,
            replay=replay,
        )
        summary = ledger.summary(spec.registration_id)
        planned_risk = (
            (evidence.registration.entry_high - evidence.registration.stop)
            * evidence.registration.quantity
        )
        review = build_paper_outcome_review(
            replay,
            summary,
            planned_risk=planned_risk,
            stop_price=evidence.registration.stop,
        )
        record = PaperOutcomeRunRecord(
            job_spec_id=spec.job_spec_id,
            registration_id=spec.registration_id,
            symbol=evidence.registration.symbol,
            replay_id=replay.replay_id,
            binding_id=evidence.binding.binding_id,
            policy_id=spec.policy.policy_id,
            calendar_snapshot_id=evidence.calendar.snapshot_id,
            as_of=spec.as_of,
            outcome_status=replay.status,
            reason_code=replay.reason_code,
            source_observation_ids=replay.source_observation_ids,
            event_ids=tuple(value.event_id for value in reconciliation.events),
            appended_event_ids=reconciliation.appended_event_ids,
            gross_pnl=summary.gross_pnl,
            estimated_net_pnl=summary.estimated_net_pnl,
            review=review,
            message=_message(replay, summary, symbol=evidence.registration.symbol),
        )
        return record_store.put(record)
    except PaperOutcomeOperationalError:
        raise
    except Exception:
        raise PaperOutcomeOperationalError("paper outcome job failed safely") from None
