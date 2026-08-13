from __future__ import annotations

import io
import json
import unittest
from contextlib import redirect_stderr
from types import SimpleNamespace
from unittest.mock import patch

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

    def test_bootstrap_strips_preserved_python_module_prefix(self) -> None:
        calls = []
        clock_values = iter((10.0, 10.25, 10.75))
        with redirect_stderr(io.StringIO()):
            result = bootstrap.main(
                (
                    "-m",
                    "india_swing.forward_paper.operational_cloud_job",
                    "--exact",
                    "value",
                ),
                importer=lambda name: SimpleNamespace(
                    main=lambda argv: calls.append(tuple(argv)) or 0
                ),
                clock=lambda: next(clock_values),
                enable_tracebacks=False,
            )

        self.assertEqual(result, 0)
        self.assertEqual(calls, [("--exact", "value")])

    def test_tracebacks_enable_fatal_handler_without_repeating_timer(self) -> None:
        clock_values = iter((10.0, 10.25, 10.75))
        stderr = io.StringIO()
        with (
            patch.object(bootstrap.faulthandler, "enable") as enable,
            patch.object(bootstrap.faulthandler, "dump_traceback_later") as dump_later,
            patch.object(
                bootstrap.faulthandler,
                "cancel_dump_traceback_later",
            ) as cancel_dump,
            redirect_stderr(stderr),
        ):
            result = bootstrap.main(
                (),
                importer=lambda name: SimpleNamespace(main=lambda argv: 0),
                clock=lambda: next(clock_values),
            )

        self.assertEqual(result, 0)
        enable.assert_called_once_with(file=stderr, all_threads=True)
        dump_later.assert_not_called()
        cancel_dump.assert_not_called()


if __name__ == "__main__":
    unittest.main()
