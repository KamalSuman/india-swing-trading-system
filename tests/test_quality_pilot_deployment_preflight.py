from __future__ import annotations

import ast
import inspect
import unittest

from india_swing.quality_pilot import deployment_preflight as preflight_module
from india_swing.quality_pilot.canonical_response import PILOT_PROTOCOL_SHA256
from india_swing.quality_pilot.control_plane_store import publish_quality_pilot_control_artifact
from india_swing.quality_pilot.deployment_preflight import (
    QUALITY_PILOT_FOUNDER_AUTHORIZATION_ID,
    QualityPilotCloudRunPreflightInput,
    QualityPilotDeploymentPreflightStatus,
    evaluate_quality_pilot_cloud_run_preflight,
)
from tests.test_quality_pilot_campaign_ledger import BUCKET, _plan
from tests.test_quality_pilot_observation_store import FakeStateObjectWriter


def _input(**changes):
    campaign_plan = _plan()
    plan = publish_quality_pilot_control_artifact(campaign_plan, BUCKET, FakeStateObjectWriter())
    values = dict(
        project_id="india-swing-prod1",
        region="asia-south1",
        job_name="india-swing-quality-pilot",
        service_account_email="quality-pilot@india-swing-prod1.iam.gserviceaccount.com",
        image_reference="asia-south1-docker.pkg.dev/project/repo/image@sha256:" + "1" * 64,
        bucket=BUCKET,
        campaign_plan=campaign_plan,
        published_plan=plan,
        code_sha256="2" * 64,
        environment_sha256="3" * 64,
        protocol_sha256=PILOT_PROTOCOL_SHA256,
        founder_authorization_id=QUALITY_PILOT_FOUNDER_AUTHORIZATION_ID,
        maximum_instances=1,
        task_count=1,
        parallelism=1,
        timeout_seconds=900,
        kite_daily_login_ready=True,
        calendar_evidence_workflow_ready=True,
    )
    values.update(changes)
    return QualityPilotCloudRunPreflightInput(**values)


class DeploymentPreflightTests(unittest.TestCase):
    def test_complete_input_is_only_ready_for_human_review(self) -> None:
        report = evaluate_quality_pilot_cloud_run_preflight(_input())
        self.assertEqual(report.status, QualityPilotDeploymentPreflightStatus.READY_FOR_HUMAN_DEPLOYMENT_REVIEW)
        self.assertEqual(report.blocker_codes, ())
        self.assertFalse(report.launch_performed)
        self.assertFalse(report.paper_trade_eligible)
        self.assertFalse(report.execution_eligible)
        self.assertFalse(report.capital_eligible)
        report.verify_content_identity()

    def test_missing_or_unpinned_inputs_report_exact_blockers(self) -> None:
        report = evaluate_quality_pilot_cloud_run_preflight(
            _input(
                image_reference="image:latest",
                code_sha256="",
                maximum_instances=2,
                kite_daily_login_ready=False,
            )
        )
        self.assertEqual(report.status, QualityPilotDeploymentPreflightStatus.BLOCKED)
        self.assertEqual(
            report.blocker_codes,
            ("DIGEST_PINNED_IMAGE", "CODE_DIGEST", "SINGLE_INSTANCE", "KITE_DAILY_LOGIN_READY"),
        )

    def test_wrong_bucket_breaks_the_plan_binding(self) -> None:
        report = evaluate_quality_pilot_cloud_run_preflight(_input(bucket="another-valid-bucket"))
        self.assertIn("PUBLISHED_PLAN_PIN", report.blocker_codes)

    def test_exact_bool_and_integer_checks_reject_truthy_values(self) -> None:
        report = evaluate_quality_pilot_cloud_run_preflight(
            _input(maximum_instances=True, kite_daily_login_ready=1)
        )
        self.assertIn("SINGLE_INSTANCE", report.blocker_codes)
        self.assertIn("KITE_DAILY_LOGIN_READY", report.blocker_codes)

    def test_module_is_offline_and_has_no_launch_or_secret_capability(self) -> None:
        source = inspect.getsource(preflight_module)
        tree = ast.parse(source)
        forbidden = {"os", "pathlib", "socket", "subprocess", "requests", "urllib", "google", "kiteconnect"}
        imported: set[str] = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imported.update(alias.name.split(".")[0] for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module:
                imported.add(node.module.split(".")[0])
        self.assertEqual(imported & forbidden, set())
        lowered = source.lower()
        for token in ("os.getenv", "os.environ", "secretmanager", "run_job", "create_job", "place_order"):
            self.assertNotIn(token, lowered)


if __name__ == "__main__":
    unittest.main()
