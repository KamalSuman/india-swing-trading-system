from __future__ import annotations

import re
from dataclasses import dataclass, field
from enum import Enum

from india_swing.identity import content_id

from .canonical_response import PILOT_PROTOCOL_SHA256
from .control_plane_store import (
    PublishedQualityPilotControlArtifact,
    QualityPilotControlArtifactKind,
)
from .campaign_ledger import QualityPilotCampaignPlan


QUALITY_PILOT_DEPLOYMENT_PREFLIGHT_SCHEMA_VERSION = (
    "quality_pilot_deployment_preflight_v1"
)
QUALITY_PILOT_FOUNDER_AUTHORIZATION_ID = "HYP-002-QP-001"

_SHA256_PATTERN = re.compile(r"[0-9a-f]{64}\Z")
_PROJECT_PATTERN = re.compile(r"[a-z][a-z0-9\-]{4,28}[a-z0-9]\Z")
_REGION_PATTERN = re.compile(r"[a-z]+-[a-z]+[0-9]\Z")
_JOB_PATTERN = re.compile(r"[a-z][a-z0-9\-]{0,61}[a-z0-9]\Z")
_SERVICE_ACCOUNT_PATTERN = re.compile(
    r"[a-z][a-z0-9\-]{0,61}[a-z0-9]@[a-z][a-z0-9\-]{4,28}[a-z0-9]\.iam\.gserviceaccount\.com\Z"
)
_IMAGE_DIGEST_PATTERN = re.compile(r"[^\s@]+@sha256:[0-9a-f]{64}\Z")
_BUCKET_PATTERN = re.compile(r"[a-z0-9][a-z0-9\-_.]{1,61}[a-z0-9]\Z")


class QualityPilotDeploymentPreflightError(ValueError):
    """The offline deployment-preflight input or report is malformed."""


def _fail(message: str) -> None:
    raise QualityPilotDeploymentPreflightError(message)


def _is_sha256(value: object) -> bool:
    return type(value) is str and _SHA256_PATTERN.fullmatch(value) is not None


class QualityPilotDeploymentPreflightStatus(Enum):
    BLOCKED = "BLOCKED"
    READY_FOR_HUMAN_DEPLOYMENT_REVIEW = "READY_FOR_HUMAN_DEPLOYMENT_REVIEW"


@dataclass(frozen=True, slots=True)
class QualityPilotDeploymentCheck:
    code: str
    passed: bool

    def __post_init__(self) -> None:
        if type(self.code) is not str or not self.code or self.code != self.code.upper():
            _fail("deployment preflight check code is invalid")
        if type(self.passed) is not bool:
            _fail("deployment preflight check result is invalid")


@dataclass(frozen=True, slots=True)
class QualityPilotCloudRunPreflightInput:
    project_id: object
    region: object
    job_name: object
    service_account_email: object
    image_reference: object
    bucket: object
    campaign_plan: object
    published_plan: object
    code_sha256: object
    environment_sha256: object
    protocol_sha256: object
    founder_authorization_id: object
    maximum_instances: object
    task_count: object
    parallelism: object
    timeout_seconds: object
    kite_daily_login_ready: object
    calendar_evidence_workflow_ready: object


@dataclass(frozen=True, slots=True)
class QualityPilotDeploymentPreflightReport:
    checks: tuple[QualityPilotDeploymentCheck, ...]
    status: QualityPilotDeploymentPreflightStatus = field(init=False)
    report_id: str = field(init=False)

    def __post_init__(self) -> None:
        if type(self.checks) is not tuple or not self.checks:
            _fail("deployment preflight checks are invalid")
        if any(type(item) is not QualityPilotDeploymentCheck for item in self.checks):
            _fail("deployment preflight check type is invalid")
        codes = tuple(item.code for item in self.checks)
        if len(codes) != len(set(codes)):
            _fail("deployment preflight checks contain duplicate codes")
        status = (
            QualityPilotDeploymentPreflightStatus.READY_FOR_HUMAN_DEPLOYMENT_REVIEW
            if all(item.passed for item in self.checks)
            else QualityPilotDeploymentPreflightStatus.BLOCKED
        )
        object.__setattr__(self, "status", status)
        object.__setattr__(
            self,
            "report_id",
            content_id(
                {
                    "schema": QUALITY_PILOT_DEPLOYMENT_PREFLIGHT_SCHEMA_VERSION,
                    "checks": tuple((item.code, item.passed) for item in self.checks),
                    "status": status.value,
                    "quality_only": True,
                    "launch_performed": False,
                },
                length=64,
            ),
        )

    @property
    def blocker_codes(self) -> tuple[str, ...]:
        return tuple(item.code for item in self.checks if not item.passed)

    @property
    def quality_only(self) -> bool:
        return True

    @property
    def launch_performed(self) -> bool:
        return False

    @property
    def paper_trade_eligible(self) -> bool:
        return False

    @property
    def execution_eligible(self) -> bool:
        return False

    @property
    def capital_eligible(self) -> bool:
        return False

    def verify_content_identity(self) -> None:
        reconstructed = QualityPilotDeploymentPreflightReport(self.checks)
        if self.status is not reconstructed.status or self.report_id != reconstructed.report_id:
            _fail("deployment preflight report identity failed")


def evaluate_quality_pilot_cloud_run_preflight(
    value: QualityPilotCloudRunPreflightInput,
) -> QualityPilotDeploymentPreflightReport:
    """Evaluate deployment prerequisites without reading env, secrets, GCP, or clocks.

    A passing result is deliberately limited to human deployment review. It
    never starts a Cloud Run Job, logs into Kite, captures market data, sends
    a notification, generates a signal, or authorizes paper/real capital.
    """

    if type(value) is not QualityPilotCloudRunPreflightInput:
        _fail("deployment preflight input type is invalid")
    plan = value.published_plan
    campaign_plan = value.campaign_plan
    campaign_plan_ok = False
    if type(campaign_plan) is QualityPilotCampaignPlan:
        verification_failed = False
        try:
            campaign_plan.verify_content_identity()
        except Exception:
            verification_failed = True
        campaign_plan_ok = not verification_failed
    plan_pin_ok = False
    if campaign_plan_ok and type(plan) is PublishedQualityPilotControlArtifact:
        try:
            plan_pin_ok = (
                plan.kind is QualityPilotControlArtifactKind.CAMPAIGN_PLAN
                and plan.bucket == value.bucket
                and plan.generation > 0
                and plan.protocol_sha256 == PILOT_PROTOCOL_SHA256
                and plan.artifact_id == campaign_plan.plan_id
                and plan.pilot_run_id == campaign_plan.campaign.pilot_run_id
            )
        except Exception:
            plan_pin_ok = False
    checks = (
        QualityPilotDeploymentCheck(
            "PROJECT_ID", type(value.project_id) is str and _PROJECT_PATTERN.fullmatch(value.project_id) is not None
        ),
        QualityPilotDeploymentCheck(
            "REGION", type(value.region) is str and _REGION_PATTERN.fullmatch(value.region) is not None
        ),
        QualityPilotDeploymentCheck(
            "JOB_NAME", type(value.job_name) is str and _JOB_PATTERN.fullmatch(value.job_name) is not None
        ),
        QualityPilotDeploymentCheck(
            "SERVICE_ACCOUNT", type(value.service_account_email) is str and _SERVICE_ACCOUNT_PATTERN.fullmatch(value.service_account_email) is not None
        ),
        QualityPilotDeploymentCheck(
            "DIGEST_PINNED_IMAGE", type(value.image_reference) is str and _IMAGE_DIGEST_PATTERN.fullmatch(value.image_reference) is not None
        ),
        QualityPilotDeploymentCheck(
            "BUCKET", type(value.bucket) is str and _BUCKET_PATTERN.fullmatch(value.bucket) is not None
        ),
        QualityPilotDeploymentCheck("CAMPAIGN_PLAN", campaign_plan_ok),
        QualityPilotDeploymentCheck("PUBLISHED_PLAN_PIN", plan_pin_ok),
        QualityPilotDeploymentCheck("CODE_DIGEST", _is_sha256(value.code_sha256)),
        QualityPilotDeploymentCheck("ENVIRONMENT_DIGEST", _is_sha256(value.environment_sha256)),
        QualityPilotDeploymentCheck("PROTOCOL_PIN", value.protocol_sha256 == PILOT_PROTOCOL_SHA256),
        QualityPilotDeploymentCheck(
            "FOUNDER_AUTHORIZATION", value.founder_authorization_id == QUALITY_PILOT_FOUNDER_AUTHORIZATION_ID
        ),
        QualityPilotDeploymentCheck("SINGLE_INSTANCE", type(value.maximum_instances) is int and value.maximum_instances == 1),
        QualityPilotDeploymentCheck("SINGLE_TASK", type(value.task_count) is int and value.task_count == 1),
        QualityPilotDeploymentCheck("SINGLE_PARALLELISM", type(value.parallelism) is int and value.parallelism == 1),
        QualityPilotDeploymentCheck(
            "BOUNDED_TIMEOUT", type(value.timeout_seconds) is int and 60 <= value.timeout_seconds <= 3600
        ),
        QualityPilotDeploymentCheck("KITE_DAILY_LOGIN_READY", value.kite_daily_login_ready is True),
        QualityPilotDeploymentCheck("CALENDAR_EVIDENCE_WORKFLOW_READY", value.calendar_evidence_workflow_ready is True),
    )
    return QualityPilotDeploymentPreflightReport(checks)
