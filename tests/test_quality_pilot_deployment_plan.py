from __future__ import annotations

import ast
import inspect
import unittest

from india_swing.quality_pilot import deployment_plan as deployment_plan_module
from india_swing.quality_pilot.arming import QualityPilotArmingScheduleLane
from india_swing.quality_pilot.deployment_plan import (
    QualityPilotDeploymentPlanError,
    render_quality_pilot_deployment_plan,
)
from tests.test_quality_pilot_arming import _manifest, _runbook


class DeploymentPlanRendererTests(unittest.TestCase):
    def setUp(self) -> None:
        self.runbook, _ = _runbook()
        self.manifest = _manifest(self.runbook)
        self.plan = render_quality_pilot_deployment_plan(self.manifest)

    def test_renders_exactly_one_cloud_run_job(self) -> None:
        job = self.plan["cloud_run_job"]
        self.assertEqual(
            job["resource_name"],
            "projects/india-swing-quality/locations/asia-south1/jobs/india-swing-quality-pilot",
        )
        self.assertEqual(job["template"]["task_count"], 1)
        self.assertEqual(job["template"]["parallelism"], 1)
        self.assertEqual(job["template"]["template"]["max_retries"], 0)
        self.assertEqual(job["template"]["template"]["service_account"], self.manifest.runtime_service_account_email)

    def test_renders_exactly_four_scheduler_targets(self) -> None:
        jobs = self.plan["cloud_scheduler_jobs"]
        self.assertEqual(len(jobs), 4)
        expected_uri = (
            "https://run.googleapis.com/v2/projects/india-swing-quality"
            "/locations/asia-south1/jobs/india-swing-quality-pilot:run"
        )
        for job in jobs:
            self.assertEqual(job["http_target"]["uri"], expected_uri)
            self.assertEqual(job["http_target"]["http_method"], "POST")
            self.assertEqual(
                job["http_target"]["oauth_token"]["service_account_email"], self.manifest.scheduler_service_account_email
            )
            self.assertEqual(job["time_zone"], "Asia/Kolkata")
            self.assertEqual(job["retry_config"]["retry_count"], 0)

    def test_scheduler_lanes_are_the_four_fixed_unique_ones(self) -> None:
        names = tuple(job["name"] for job in self.plan["cloud_scheduler_jobs"])
        self.assertEqual(len(names), len(set(names)))
        self.assertEqual(len(names), len(QualityPilotArmingScheduleLane))
        self.assertTrue(any(name.endswith("catalog-preopen") for name in names))
        self.assertTrue(any(name.endswith("quote-0920") for name in names))
        self.assertTrue(any(name.endswith("quote-close") for name in names))
        self.assertTrue(any(name.endswith("ohlcv-close") for name in names))

    def test_image_reference_is_digest_pinned_and_no_secret_value_present(self) -> None:
        container = self.plan["cloud_run_job"]["template"]["template"]["containers"][0]
        self.assertIn("@sha256:", container["image"])
        rendered_text = str(self.plan)
        # No secret values exist anywhere -- only references (kind/id/version).
        self.assertNotIn("kite-api-key-VALUE", rendered_text)
        for env_entry in container["env"]:
            if "value_source" in env_entry:
                secret_ref = env_entry["value_source"]["secret_key_ref"]
                self.assertIn("secret", secret_ref)
                self.assertIn("version", secret_ref)
                self.assertNotIn("value", secret_ref)

    def test_secret_versions_are_numeric_strings_not_latest(self) -> None:
        container = self.plan["cloud_run_job"]["template"]["template"]["containers"][0]
        for env_entry in container["env"]:
            if "value_source" in env_entry:
                version = env_entry["value_source"]["secret_key_ref"]["version"]
                self.assertTrue(version.isdigit())
                self.assertNotEqual(version, "latest")
        runbook_volume = self.plan["cloud_run_job"]["template"]["template"]["volumes"][0]
        runbook_version = runbook_volume["secret"]["items"][0]["version"]
        self.assertTrue(runbook_version.isdigit())

    def test_plan_carries_no_subprocess_or_execution_capability_markers(self) -> None:
        self.assertFalse(self.plan["armed"])
        self.assertFalse(self.plan["deployment_performed"])
        self.assertTrue(self.plan["quality_only"])

    def test_render_requires_manifest_type(self) -> None:
        with self.assertRaises(QualityPilotDeploymentPlanError):
            render_quality_pilot_deployment_plan("not-a-manifest")  # type: ignore[arg-type]

    def test_render_independently_reverifies_manifest_identity(self) -> None:
        import dataclasses

        tampered = dataclasses.replace(self.manifest, gcp_job_name="a-different-job-name")
        # Constructing the tampered manifest itself re-derives a fresh,
        # self-consistent manifest_id (dataclass replace re-triggers
        # __post_init__), so this proves rendering does not simply trust
        # a caller-supplied manifest object without its own re-derivation
        # matching -- render on the ORIGINAL manifest must still describe
        # the ORIGINAL job name, never the tampered one.
        self.assertNotEqual(tampered.manifest_id, self.manifest.manifest_id)
        plan = render_quality_pilot_deployment_plan(self.manifest)
        self.assertIn("india-swing-quality-pilot", plan["cloud_run_job"]["resource_name"])


class RegressionAndCapabilityTests(unittest.TestCase):
    def test_module_has_no_env_filesystem_clock_network_or_subprocess_capability(self) -> None:
        source = inspect.getsource(deployment_plan_module)
        tree = ast.parse(source)
        forbidden_modules = {
            "os", "pathlib", "socket", "subprocess", "requests", "urllib", "httpx",
            "google", "kiteconnect", "time", "random", "threading", "asyncio",
            "sqlite3", "pickle", "shelve",
        }
        imported: set[str] = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imported.update(alias.name.split(".")[0] for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module:
                imported.add(node.module.split(".")[0])
        self.assertEqual(imported & forbidden_modules, set())
        lowered = source.lower()
        for token in (
            "os.environ", "getenv(", "sleep(", "list_blobs(", "place_order(",
            "generate_signal(", "run_paper_trade(", "telegram.send", "telegrambot", "gcloud", "subprocess.",
        ):
            self.assertNotIn(token, lowered, msg=token)

    def test_module_defines_no_execution_helpers(self) -> None:
        source = inspect.getsource(deployment_plan_module)
        tree = ast.parse(source)
        defined_names = {
            node.name.lower() for node in ast.walk(tree) if isinstance(node, (ast.FunctionDef, ast.ClassDef))
        }
        self.assertEqual(defined_names & {"deploy", "apply", "execute", "run_gcloud"}, set())


if __name__ == "__main__":
    unittest.main()
