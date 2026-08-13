from __future__ import annotations

import io
import json
import unittest
from contextlib import redirect_stderr
from types import SimpleNamespace

import india_swing_forward_paper_bootstrap as bootstrap


class ForwardPaperOperationalBootstrapTests(unittest.TestCase):
    def test_bootstrap_logs_before_import_and_delegates_arguments(self) -> None:
        calls = []
        clock_values = iter((10.0, 10.25, 10.75))

        def application_main(argv):
            calls.append(tuple(argv))
            return 7

        stderr = io.StringIO()
        with redirect_stderr(stderr):
            result = bootstrap.main(
                ("--exact", "value"),
                importer=lambda name: SimpleNamespace(main=application_main),
                clock=lambda: next(clock_values),
                enable_tracebacks=False,
            )

        self.assertEqual(result, 7)
        self.assertEqual(calls, [("--exact", "value")])
        events = [json.loads(line) for line in stderr.getvalue().splitlines()]
        self.assertEqual(
            [(event["stage"], event["status"]) for event in events],
            [
                ("process_start", "completed"),
                ("application_import", "started"),
                ("application_import", "completed"),
            ],
        )
        self.assertTrue(all(event["event"] == "FORWARD_PAPER_BOOTSTRAP" for event in events))


if __name__ == "__main__":
    unittest.main()
