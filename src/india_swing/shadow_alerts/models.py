from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass, field, fields
from enum import Enum

from india_swing.domain.models import (
    DecisionAction,
    ResearchAssessment,
    RunStatus,
    TradeDecision,
)
from india_swing.identity import content_id
from india_swing.pipeline import PipelineResult


_HEX_ID = re.compile(r"[0-9a-f]{20,64}\Z")
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_SENSITIVE_TEXT = re.compile(
    r"api[_ -]?key|access[_ -]?token|refresh[_ -]?token|authorization|"
    r"password|private[_ -]?key|client[_ -]?secret",
    re.IGNORECASE,
)


class ShadowAlertError(ValueError):
    pass


class ShadowAlertKind(str, Enum):
    CANDIDATE = "CANDIDATE"
    NO_TRADE = "NO_TRADE"


def _require_public_text(value: str, name: str, *, allow_empty: bool = False) -> None:
    if type(value) is not str or (not allow_empty and not value.strip()):
        raise ShadowAlertError(f"{name} must be public text")
    if "\x00" in value or _SENSITIVE_TEXT.search(value):
        raise ShadowAlertError(f"{name} contains forbidden content")


def _require_public_text_tuple(value: tuple[str, ...], name: str) -> None:
    if type(value) is not tuple:
        raise ShadowAlertError(f"{name} must be an immutable text tuple")
    for item in value:
        _require_public_text(item, name)


@dataclass(frozen=True, slots=True)
class ShadowAlert:
    """A non-executable projection of one complete pipeline result."""

    source_run_id: str
    source_pipeline_integrity_hash: str
    source_snapshot_id: str
    source_snapshot_fingerprint: str
    trial_id: str
    model_bundle_id: str
    data_content_hash: str
    source_revision: str
    execution_policy_version: str
    cost_schedule_version: str
    reference_readiness: str
    decision: TradeDecision
    evidence_ids: tuple[str, ...]
    research_model_version: str
    kind: ShadowAlertKind
    mode: str = "RESEARCH_ONLY"
    schema_version: str = "shadow-alert/v1"
    alert_id: str = field(init=False)

    def __post_init__(self) -> None:
        if type(self.decision) is not TradeDecision:
            raise ShadowAlertError("decision must be an exact TradeDecision")
        try:
            self.decision.verify_integrity()
        except Exception:
            raise ShadowAlertError("decision integrity verification failed") from None
        if self.decision.execution_eligible:
            raise ShadowAlertError("shadow alerts must never be execution eligible")
        if type(self.kind) is not ShadowAlertKind:
            raise ShadowAlertError("kind must be an exact ShadowAlertKind")
        if self.mode != "RESEARCH_ONLY" or self.schema_version != "shadow-alert/v1":
            raise ShadowAlertError("shadow alert authority boundary is invalid")
        if _HEX_ID.fullmatch(self.source_run_id) is None:
            raise ShadowAlertError("source_run_id must be a lowercase content ID")
        for name in (
            "source_pipeline_integrity_hash",
            "source_snapshot_fingerprint",
        ):
            if _SHA256.fullmatch(getattr(self, name)) is None:
                raise ShadowAlertError(f"{name} must be a lowercase SHA-256")
        for name in (
            "source_snapshot_id",
            "trial_id",
            "model_bundle_id",
            "data_content_hash",
            "source_revision",
            "execution_policy_version",
            "cost_schedule_version",
            "reference_readiness",
        ):
            _require_public_text(getattr(self, name), name)
        _require_public_text_tuple(self.evidence_ids, "evidence_ids")
        if len(self.evidence_ids) != len(set(self.evidence_ids)):
            raise ShadowAlertError("evidence_ids must be unique")

        if self.kind is ShadowAlertKind.CANDIDATE:
            if self.decision.action is not DecisionAction.BUY:
                raise ShadowAlertError("candidate alerts require a BUY research decision")
            if not self.evidence_ids:
                raise ShadowAlertError("candidate alerts require curated evidence")
            _require_public_text(self.research_model_version, "research_model_version")
            _require_public_text(self.decision.thesis, "decision thesis")
            _require_public_text(self.decision.bear_case, "decision bear_case")
        else:
            if self.decision.action is not DecisionAction.NO_TRADE:
                raise ShadowAlertError("NO_TRADE alerts require a NO_TRADE decision")
            if self.evidence_ids or self.research_model_version:
                raise ShadowAlertError("NO_TRADE alerts cannot imply selected research")

        object.__setattr__(self, "alert_id", self._calculated_alert_id())

    def _calculated_alert_id(self) -> str:
        return content_id(
            {
                item.name: getattr(self, item.name)
                for item in fields(self)
                if item.name != "alert_id"
            },
            length=64,
        )

    def verify_integrity(self) -> None:
        try:
            fresh = ShadowAlert(
                source_run_id=self.source_run_id,
                source_pipeline_integrity_hash=self.source_pipeline_integrity_hash,
                source_snapshot_id=self.source_snapshot_id,
                source_snapshot_fingerprint=self.source_snapshot_fingerprint,
                trial_id=self.trial_id,
                model_bundle_id=self.model_bundle_id,
                data_content_hash=self.data_content_hash,
                source_revision=self.source_revision,
                execution_policy_version=self.execution_policy_version,
                cost_schedule_version=self.cost_schedule_version,
                reference_readiness=self.reference_readiness,
                decision=self.decision,
                evidence_ids=self.evidence_ids,
                research_model_version=self.research_model_version,
                kind=self.kind,
                mode=self.mode,
                schema_version=self.schema_version,
            )
        except Exception:
            raise ShadowAlertError("shadow alert integrity verification failed") from None
        if type(self.alert_id) is not str or self.alert_id != fresh.alert_id:
            raise ShadowAlertError("shadow alert integrity verification failed")


@dataclass(frozen=True, slots=True)
class ShadowNotification:
    alert_id: str
    source_run_id: str
    source_pipeline_integrity_hash: str
    source_decision_integrity_hash: str
    signal_id: str
    kind: ShadowAlertKind
    reference_readiness: str
    message: str
    message_sha256: str
    mode: str = "RESEARCH_ONLY"
    schema_version: str = "shadow-notification/v1"

    def __post_init__(self) -> None:
        for value, name, pattern in (
            (self.alert_id, "alert_id", _SHA256),
            (self.source_run_id, "source_run_id", _HEX_ID),
            (
                self.source_pipeline_integrity_hash,
                "source_pipeline_integrity_hash",
                _SHA256,
            ),
            (
                self.source_decision_integrity_hash,
                "source_decision_integrity_hash",
                _SHA256,
            ),
            (self.message_sha256, "message_sha256", _SHA256),
        ):
            if type(value) is not str or pattern.fullmatch(value) is None:
                raise ShadowAlertError(f"{name} has invalid content identity")
        _require_public_text(self.signal_id, "signal_id")
        _require_public_text(self.reference_readiness, "reference_readiness")
        if type(self.kind) is not ShadowAlertKind:
            raise ShadowAlertError("notification kind is invalid")
        if self.mode != "RESEARCH_ONLY" or self.schema_version != "shadow-notification/v1":
            raise ShadowAlertError("notification authority boundary is invalid")
        _require_public_text(self.message, "message")
        if hashlib.sha256(self.message.encode("utf-8")).hexdigest() != self.message_sha256:
            raise ShadowAlertError("notification message hash differs")
        if not self.message.startswith(
            "RESEARCH-ONLY SHADOW ALERT — DO NOT EXECUTE\n"
        ):
            raise ShadowAlertError("notification warning is missing")


def build_shadow_alert(result: PipelineResult) -> ShadowAlert:
    if type(result) is not PipelineResult:
        raise ShadowAlertError("pipeline result must be exact")
    try:
        result.verify_integrity()
    except Exception:
        raise ShadowAlertError("pipeline result integrity verification failed") from None
    if result.status is not RunStatus.COMPLETE:
        raise ShadowAlertError("failed pipeline runs cannot create shadow alerts")
    decision = result.decision
    if decision.execution_eligible:
        raise ShadowAlertError("executable decisions cannot enter the shadow outbox")

    evidence_ids: tuple[str, ...] = ()
    research_model_version = ""
    if decision.action is DecisionAction.BUY:
        matching = tuple(
            assessment
            for assessment in result.research
            if assessment.symbol == decision.symbol
        )
        if len(matching) != 1 or type(matching[0]) is not ResearchAssessment:
            raise ShadowAlertError("selected candidate research is not unique")
        assessment = matching[0]
        if (
            assessment.thesis != decision.thesis
            or assessment.bear_case != decision.bear_case
        ):
            raise ShadowAlertError("selected decision differs from its research")
        evidence_ids = tuple(assessment.evidence_ids)
        research_model_version = assessment.model_version
        kind = ShadowAlertKind.CANDIDATE
    elif decision.action is DecisionAction.NO_TRADE:
        kind = ShadowAlertKind.NO_TRADE
    else:
        raise ShadowAlertError("unsupported pipeline decision action")

    return ShadowAlert(
        source_run_id=result.run_id,
        source_pipeline_integrity_hash=result.integrity_hash,
        source_snapshot_id=result.snapshot_id,
        source_snapshot_fingerprint=result.snapshot_fingerprint,
        trial_id=result.trial_id,
        model_bundle_id=result.model_bundle_id,
        data_content_hash=result.data_content_hash,
        source_revision=result.source_revision,
        execution_policy_version=result.execution_policy_version,
        cost_schedule_version=result.cost_schedule_version,
        reference_readiness=result.reference_readiness,
        decision=decision,
        evidence_ids=evidence_ids,
        research_model_version=research_model_version,
        kind=kind,
    )


def _line(value: object) -> str:
    return str(value).replace("\r", " ").replace("\n", " ")


def render_shadow_alert(alert: ShadowAlert) -> str:
    if type(alert) is not ShadowAlert:
        raise ShadowAlertError("shadow alert must be exact")
    alert.verify_integrity()
    decision = alert.decision
    lines = [
        "RESEARCH-ONLY SHADOW ALERT — DO NOT EXECUTE",
        f"Mode: {alert.mode}",
        f"Result: {alert.kind.value}",
        f"Decision time: {_line(decision.decision_time.isoformat())}",
        f"Reference readiness: {_line(alert.reference_readiness)}",
    ]
    if alert.kind is ShadowAlertKind.CANDIDATE:
        lines.extend(
            (
                f"Symbol: {_line(decision.symbol)}",
                f"Simulated quantity: {decision.quantity}",
                f"Entry window: {_line(decision.earliest_entry_at.isoformat())} to {_line(decision.entry_expires_at.isoformat())}",
                f"Entry range: {_line(decision.entry_low)} – {_line(decision.entry_high)}",
                f"Stop: {_line(decision.stop)}",
                f"Target: {_line(decision.target)}",
                f"Maximum holding sessions: {decision.max_holding_sessions}",
                f"Planned maximum loss: INR {_line(decision.planned_max_loss)}",
                f"Estimated round-trip cost: INR {_line(decision.estimated_cost)}",
                f"Net reward/risk: {_line(decision.net_reward_risk)}",
                f"Expected R: {_line(decision.expected_r)}",
                f"Target probability: {_line(decision.target_probability)}",
                f"Stop probability: {_line(decision.stop_probability)}",
                f"Probability status: {decision.probability_status.value} (sample {decision.calibration_sample_size})",
                f"Order type (simulation only): {_line(decision.order_type)}",
                "Why this candidate:",
                *[f"- {_line(reason)}" for reason in decision.reasons],
                f"Thesis: {_line(decision.thesis)}",
                f"Bear case: {_line(decision.bear_case)}",
                "Cancel if:",
                *[f"- {_line(item)}" for item in decision.cancel_conditions],
                "Evidence:",
                *[f"- {_line(item)}" for item in alert.evidence_ids],
                f"Research model: {_line(alert.research_model_version)}",
            )
        )
    else:
        lines.extend(("No candidate passed every gate.", "Reasons:"))
        lines.extend(f"- {_line(reason)}" for reason in decision.reasons)
    lines.extend(
        (
            f"Pipeline run: {alert.source_run_id}",
            f"Signal: {_line(decision.signal_id)}",
            f"Alert: {alert.alert_id}",
            "This artifact records a paper observation only. It is not investment advice or broker authority.",
        )
    )
    message = "\n".join(lines) + "\n"
    _require_public_text(message, "rendered message")
    return message


def notification_from_alert(alert: ShadowAlert) -> ShadowNotification:
    message = render_shadow_alert(alert)
    return ShadowNotification(
        alert_id=alert.alert_id,
        source_run_id=alert.source_run_id,
        source_pipeline_integrity_hash=alert.source_pipeline_integrity_hash,
        source_decision_integrity_hash=alert.decision.integrity_hash,
        signal_id=alert.decision.signal_id,
        kind=alert.kind,
        reference_readiness=alert.reference_readiness,
        message=message,
        message_sha256=hashlib.sha256(message.encode("utf-8")).hexdigest(),
    )
