from __future__ import annotations

import io
import json
import unittest
from contextlib import redirect_stderr

from india_swing import (
    paper_portfolio_from_pipeline,
    paper_portfolio_job,
    paper_portfolio_prepare,
    paper_portfolio_restore,
)
from india_swing.paper_outcomes import PaperPortfolioError


class PaperPortfolioCLITests(unittest.TestCase):
    def test_job_argument_parser_requires_each_exact_argument_once(self) -> None:
        with self.assertRaises(PaperPortfolioError):
            paper_portfolio_job._arguments(("--spec-file", "spec.json"))
        with self.assertRaises(PaperPortfolioError):
            paper_portfolio_job._arguments(
                (
                    "--spec-file", "spec.json",
                    "--spec-file", "other.json",
                    "--evidence-root", "evidence",
                    "--state-root", "state",
                )
            )

    def test_restore_argument_parser_requires_exact_envelope(self) -> None:
        with self.assertRaises(PaperPortfolioError):
            paper_portfolio_restore._arguments(
                ("--state-root", "state", "--unexpected", "value")
            )

    def test_cli_failure_is_sanitized(self) -> None:
        stream = io.StringIO()
        secret = "do-not-echo-this"
        with redirect_stderr(stream):
            result = paper_portfolio_job.main(("--spec-file", secret))
        self.assertEqual(result, 2)
        payload = json.loads(stream.getvalue())
        self.assertEqual(
            payload,
            {"error_type": "PaperPortfolioError", "status": "FAILED"},
        )
        self.assertNotIn(secret, stream.getvalue())

    def test_entrypoints_do_not_import_broker_order_capabilities(self) -> None:
        for module in (
            paper_portfolio_job,
            paper_portfolio_from_pipeline,
            paper_portfolio_prepare,
            paper_portfolio_restore,
        ):
            names = set(module.__dict__)
            self.assertNotIn("KiteConnect", names)
            self.assertNotIn("place_order", names)
            self.assertNotIn("modify_order", names)
            self.assertNotIn("cancel_order", names)

    def test_preparation_cli_failure_is_sanitized(self) -> None:
        stream = io.StringIO()
        secret = "private-preparation-path"
        with redirect_stderr(stream):
            result = paper_portfolio_prepare.main(("--spec-file", secret))
        self.assertEqual(result, 2)
        self.assertNotIn(secret, stream.getvalue())
        self.assertEqual(
            json.loads(stream.getvalue()),
            {"error_type": "PaperPortfolioPreparationError", "status": "FAILED"},
        )

    def test_pipeline_bridge_cli_failure_is_sanitized(self) -> None:
        stream = io.StringIO()
        secret = "private-pipeline-id"
        with redirect_stderr(stream):
            result = paper_portfolio_from_pipeline.main(("--run-id", secret))
        self.assertEqual(result, 2)
        self.assertNotIn(secret, stream.getvalue())
        self.assertEqual(
            json.loads(stream.getvalue()),
            {"error_type": "PaperPortfolioPipelineBridgeError", "status": "FAILED"},
        )


if __name__ == "__main__":
    unittest.main()
