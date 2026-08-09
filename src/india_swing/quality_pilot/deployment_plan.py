"""HYP-002 quality pilot: pure Cloud Run/Scheduler deployment-plan renderer.

Renders one already-verified ``QualityPilotArmingManifest`` into canonical
JSON *data* describing exactly one Cloud Run v2 Job and exactly four Cloud
Scheduler HTTP targets (one per fixed lane). This module never executes a
deployment tool, never calls an API, never opens a socket, never imports
``subprocess``, and never contains a secret value -- every secret is
represented only as a Secret Manager resource name plus its already-pinned
numeric version, exactly as carried by the manifest. Rendering the plan is
not deployment authority: applying it to real infrastructure is a separate,
later, human-authorized step.
"""

from __future__ import annotations

from dataclasses import dataclass

from .arming import (
    QualityPilotArmingManifest,
    QualityPilotArmingScheduleLane,
    QualityPilotArmingSecretKind,
)

QUALITY_PILOT_DEPLOYMENT_PLAN_SCHEMA_VERSION = "quality_pilot_deployment_plan_v1"

_RUN_DUE_WINDOW_COMMAND = ("python", "-m", "india_swing.quality_pilot_job", "run-due-window")
_RUNBOOK_MOUNT_PATH = "/mnt/quality-pilot/runbook.json"
_MANIFEST_MOUNT_PATH = "/mnt/quality-pilot/manifest.json"
_RUNBOOK_VOLUME_NAME = "quality-pilot-runbook"
_ENV_CODE_SHA256 = "INDIA_SWING_QUALITY_PILOT_CODE_SHA256"
_ENV_ENVIRONMENT_SHA256 = "INDIA_SWING_QUALITY_PILOT_ENVIRONMENT_SHA256"
_ENV_KITE_API_KEY = "INDIA_SWING_KITE_API_KEY"
_ENV_KITE_ACCESS_TOKEN = "INDIA_SWING_KITE_ACCESS_TOKEN"

_LANE_SCHEDULER_SUFFIX = {
    QualityPilotArmingScheduleLane.CATALOG_PREOPEN: "catalog-preopen",
    QualityPilotArmingScheduleLane.QUOTE_0920: "quote-0920",
    QualityPilotArmingScheduleLane.QUOTE_CLOSE: "quote-close",
    QualityPilotArmingScheduleLane.OHLCV_CLOSE: "ohlcv-close",
}


class QualityPilotDeploymentPlanError(ValueError):
    """The supplied manifest could not be rendered into a deployment plan."""


def _fail(message: str) -> None:
    raise QualityPilotDeploymentPlanError(message)


def _secret_reference_by_kind(manifest: QualityPilotArmingManifest, kind: QualityPilotArmingSecretKind):
    matches = tuple(item for item in manifest.secret_references if item.kind is kind)
    if len(matches) != 1:
        _fail("deployment plan manifest secret references are malformed")
    return matches[0]


def _cloud_run_job_resource_name(manifest: QualityPilotArmingManifest) -> str:
    return f"projects/{manifest.gcp_project_id}/locations/{manifest.gcp_region}/jobs/{manifest.gcp_job_name}"


def _cloud_run_job_run_uri(manifest: QualityPilotArmingManifest) -> str:
    return (
        f"https://run.googleapis.com/v2/projects/{manifest.gcp_project_id}"
        f"/locations/{manifest.gcp_region}/jobs/{manifest.gcp_job_name}:run"
    )


def _render_cloud_run_job(manifest: QualityPilotArmingManifest) -> dict:
    kite_api_key = _secret_reference_by_kind(manifest, QualityPilotArmingSecretKind.KITE_API_KEY)
    kite_access_token = _secret_reference_by_kind(manifest, QualityPilotArmingSecretKind.KITE_ACCESS_TOKEN)
    runbook_secret = _secret_reference_by_kind(manifest, QualityPilotArmingSecretKind.RUNBOOK)

    container = {
        "image": manifest.image_reference,
        "command": [_RUN_DUE_WINDOW_COMMAND[0]],
        "args": [
            *list(_RUN_DUE_WINDOW_COMMAND[1:]),
            "--runbook-file", _RUNBOOK_MOUNT_PATH,
            "--manifest-file", _MANIFEST_MOUNT_PATH,
        ],
        "env": [
            {"name": _ENV_CODE_SHA256, "value": manifest.code_sha256},
            {"name": _ENV_ENVIRONMENT_SHA256, "value": manifest.environment_sha256},
            {
                "name": _ENV_KITE_API_KEY,
                "value_source": {"secret_key_ref": {"secret": kite_api_key.secret_id, "version": kite_api_key.version}},
            },
            {
                "name": _ENV_KITE_ACCESS_TOKEN,
                "value_source": {
                    "secret_key_ref": {"secret": kite_access_token.secret_id, "version": kite_access_token.version}
                },
            },
        ],
        "volume_mounts": [{"name": _RUNBOOK_VOLUME_NAME, "mount_path": "/mnt/quality-pilot"}],
    }

    return {
        "resource_name": _cloud_run_job_resource_name(manifest),
        "template": {
            "task_count": manifest.tasks,
            "parallelism": manifest.parallelism,
            "template": {
                "max_retries": manifest.max_retries,
                "timeout": f"{manifest.timeout_seconds}s",
                "service_account": manifest.runtime_service_account_email,
                "containers": [container],
                "volumes": [
                    {
                        "name": _RUNBOOK_VOLUME_NAME,
                        "secret": {
                            "secret": runbook_secret.secret_id,
                            "items": [{"version": runbook_secret.version, "path": "runbook.json"}],
                        },
                    }
                ],
            },
        },
    }


def _render_cloud_scheduler_jobs(manifest: QualityPilotArmingManifest) -> tuple[dict, ...]:
    run_uri = _cloud_run_job_run_uri(manifest)
    entries = []
    for schedule in sorted(manifest.schedules, key=lambda item: item.lane.value):
        entries.append(
            {
                "name": (
                    f"projects/{manifest.gcp_project_id}/locations/{manifest.gcp_region}/jobs/"
                    f"{manifest.gcp_job_name}-{_LANE_SCHEDULER_SUFFIX[schedule.lane]}"
                ),
                "schedule": schedule.cron_expression,
                "time_zone": "Asia/Kolkata",
                "retry_config": {"retry_count": 0},
                "http_target": {
                    "uri": run_uri,
                    "http_method": "POST",
                    "oauth_token": {"service_account_email": manifest.scheduler_service_account_email},
                },
            }
        )
    return tuple(entries)


def render_quality_pilot_deployment_plan(manifest: QualityPilotArmingManifest) -> dict:
    """Render one already-verified arming manifest into canonical JSON data
    describing exactly one Cloud Run v2 Job and exactly four Cloud Scheduler
    HTTP targets. Returns pure data -- never a shell script, never an
    executed command, never a secret value."""

    if type(manifest) is not QualityPilotArmingManifest:
        _fail("deployment plan manifest type is invalid")
    manifest_failed = False
    try:
        manifest.verify_content_identity()
    except Exception:
        manifest_failed = True
    if manifest_failed:
        _fail("deployment plan manifest failed independent verification")

    cloud_run_job = _render_cloud_run_job(manifest)
    cloud_scheduler_jobs = _render_cloud_scheduler_jobs(manifest)
    if len(cloud_scheduler_jobs) != len(QualityPilotArmingScheduleLane):
        _fail("deployment plan must render exactly one scheduler target per fixed lane")

    return {
        "schema_version": QUALITY_PILOT_DEPLOYMENT_PLAN_SCHEMA_VERSION,
        "manifest_id": manifest.manifest_id,
        "runbook_id": manifest.runbook_id,
        "pilot_run_id": manifest.pilot_run_id,
        "cloud_run_job": cloud_run_job,
        "cloud_scheduler_jobs": list(cloud_scheduler_jobs),
        "quality_only": True,
        "armed": False,
        "deployment_performed": False,
    }
