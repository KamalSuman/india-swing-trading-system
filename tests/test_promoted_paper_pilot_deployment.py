from __future__ import annotations

import ast
import unittest
from pathlib import Path

import india_swing.promoted_paper_pilot_job as job_module
import india_swing.promoted_paper_pilot_notification as notification_module


_ROOT = Path(__file__).resolve().parents[1]


class PromotedPaperPilotDeploymentTests(unittest.TestCase):
    def test_console_entrypoint_docker_runtime_and_docs_are_wired(self) -> None:
        pyproject = (_ROOT / "pyproject.toml").read_text(encoding="utf-8")
        dockerfile = (_ROOT / "Dockerfile").read_text(encoding="utf-8")
        docs = (_ROOT / "docs" / "PROMOTED_PAPER_PILOT.md").read_text(
            encoding="utf-8"
        )
        self.assertIn("india-swing-promoted-paper-pilot-job", pyproject)
        self.assertIn("/tmp/india-swing", dockerfile)
        self.assertIn("PROMOTED_PAPER_PILOT_JOB_COMPLETE", docs)
        self.assertIn("delivery is marked uncertain", docs)

    def test_deployment_is_digest_and_exact_secret_pinned_with_no_scheduler_creation(self) -> None:
        script = (
            _ROOT / "infra" / "deploy_promoted_paper_pilot.ps1"
        ).read_text(encoding="utf-8")
        self.assertIn("@sha256:[0-9a-f]{64}$", script)
        self.assertIn("--max-retries=0", script)
        self.assertIn("INDIA_SWING_KITE_ACCESS_TOKEN", script)
        self.assertIn("INDIA_SWING_TELEGRAM_BOT_TOKEN", script)
        self.assertIn("INDIA_SWING_TELEGRAM_CHAT_ID", script)
        self.assertIn("INDIA_SWING_PAPER_PILOT_STATE_BUCKET", script)
        self.assertNotIn("secrets:latest", script)
        self.assertNotIn("scheduler jobs create", script)
        self.assertNotIn("scheduler jobs resume", script)

    def test_new_modules_have_no_order_or_bucket_listing_capability(self) -> None:
        for module in (job_module, notification_module):
            source = Path(module.__file__).read_text(encoding="utf-8")
            tree = ast.parse(source)
            names = {
                node.attr
                for node in ast.walk(tree)
                if isinstance(node, ast.Attribute)
            } | {
                node.id
                for node in ast.walk(tree)
                if isinstance(node, ast.Name)
            }
            self.assertFalse(
                names
                & {
                    "place_order",
                    "modify_order",
                    "cancel_order",
                    "list_blobs",
                    "list_objects",
                }
            )


if __name__ == "__main__":
    unittest.main()
