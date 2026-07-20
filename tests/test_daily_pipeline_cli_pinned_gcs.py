from __future__ import annotations

import argparse
import contextlib
import io
import json
import os
import tempfile
import unittest
from datetime import date, datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

from india_swing.calendar_data.materialization_store import LocalCalendarMaterializationStore
from india_swing.daily_pipeline.cli import main, parser
from india_swing.daily_pipeline.pinned_gcs_run_file_boundary import PinnedGCSRunFileBoundaryError
from india_swing.daily_pipeline.pinned_gcs_run_service import PinnedGCSRunServiceError
from india_swing.daily_pipeline.store import LocalDailyPipelineRunStore
from india_swing.daily_reports.artifact_store import LocalDailyBundleArtifactStore
from india_swing.historical_prices.artifact_store import LocalHistoricalPriceArtifactStore
from india_swing.identity_registry.adjudication_store import LocalIdentityAdjudicationQueueStore
from india_swing.identity_registry.artifact_store import LocalIdentityRegistryStore
from india_swing.reference_data.artifact_store import LocalReferenceArtifactStore


_BOUNDARY_TARGET = (
    "india_swing.daily_pipeline.cli.run_daily_pipeline_from_pinned_gcs_run_spec_file"
)
_READER_TARGET = "india_swing.daily_pipeline.cli.GoogleCloudStorageObjectReader"


def _env_for(root: Path) -> dict[str, str]:
    return {
        "INDIA_SWING_DAILY_PIPELINE_ROOT": str(root / "daily_pipeline"),
        "INDIA_SWING_REFERENCE_DATA_ROOT": str(root / "reference_data"),
        "INDIA_SWING_DAILY_REPORTS_ROOT": str(root / "daily_reports"),
        "INDIA_SWING_HISTORICAL_PRICES_ROOT": str(root / "historical_prices"),
        "INDIA_SWING_IDENTITY_REGISTRY_ROOT": str(root / "identity_registry"),
        "INDIA_SWING_CALENDAR_DATA_ROOT": str(root / "calendar_data"),
    }


class _FakeReadiness:
    value = "COLLECTION_ONLY"


class _FakeRun:
    """Duck-typed stand-in for DailyPipelineRun: _summary() only reads
    attributes, so a real, fully cross-validated DailyPipelineRun is not
    required to prove the CLI renders whatever the delegated boundary
    returns."""

    def __init__(self) -> None:
        self.run_id = "f" * 64
        self.market_session = date(2026, 7, 20)
        self.cutoff = datetime(2026, 7, 20, 15, 0, 0, tzinfo=timezone.utc)
        self.previous_run_id = None
        self.current_security_master_artifact_id = "a" * 64
        self.current_daily_bundle_artifact_id = "b" * 64
        self.historical_price_artifact_id = "c" * 64
        self.bar_count = 1
        self.reconciliation_snapshot_id = "d" * 64
        self.unresolved_count = 0
        self.identity_registry_id = "e" * 64
        self.identity_transition_count = 0
        self.adjudication_queue_id = "0" * 64
        self.adjudication_case_count = 0
        self.completeness_issues = ()
        self.readiness = _FakeReadiness()
        self.actionable = False
        self.stable_identity_assigned = False


class _DelegatedSpy:
    def __init__(self, *, result: object = None, raises: BaseException | None = None) -> None:
        self.calls: list[dict[str, object]] = []
        self._result = result
        self._raises = raises

    def __call__(self, spec_path: str, **kwargs: object) -> object:
        self.calls.append({"spec_path": spec_path, "kwargs": kwargs})
        if self._raises is not None:
            raise self._raises
        return self._result


class RunPinnedGcsCliTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.env = _env_for(self.root)

    def tearDown(self) -> None:
        self.temporary.cleanup()


class RunPinnedGcsHappyPathTests(RunPinnedGcsCliTestCase):
    def test_success_calls_delegated_boundary_exactly_once_with_exact_arguments(self) -> None:
        fake_run = _FakeRun()
        spy = _DelegatedSpy(result=fake_run)
        reader_sentinel = object()
        mock_reader_class = MagicMock(return_value=reader_sentinel)
        stdout = io.StringIO()

        with patch.dict(os.environ, self.env, clear=False):
            with patch(_READER_TARGET, mock_reader_class):
                with patch(_BOUNDARY_TARGET, spy):
                    with patch.object(LocalCalendarMaterializationStore, "get") as mock_get:
                        with contextlib.redirect_stdout(stdout):
                            exit_code = main(
                                ["run-pinned-gcs", "--spec-file", "operator-spec.json"]
                            )

        self.assertEqual(exit_code, 0)
        mock_get.assert_not_called()
        mock_reader_class.assert_called_once_with()
        self.assertEqual(len(spy.calls), 1)
        call = spy.calls[0]
        self.assertEqual(call["spec_path"], "operator-spec.json")
        self.assertIsInstance(call["spec_path"], str)

        kwargs = call["kwargs"]
        self.assertEqual(len(kwargs), 8)
        self.assertIs(kwargs["reader"], reader_sentinel)

        calendar_store = kwargs["calendar_store"]
        self.assertIsInstance(calendar_store, LocalCalendarMaterializationStore)
        self.assertEqual(calendar_store.root, self.root / "calendar_data")
        self.assertEqual(calendar_store.daily_reports_root, self.root / "daily_reports")

        reference_store = kwargs["reference_store"]
        self.assertIsInstance(reference_store, LocalReferenceArtifactStore)
        self.assertEqual(reference_store.root, self.root / "reference_data")

        daily_store = kwargs["daily_store"]
        self.assertIsInstance(daily_store, LocalDailyBundleArtifactStore)
        self.assertEqual(daily_store.root, self.root / "daily_reports")

        historical_store = kwargs["historical_store"]
        self.assertIsInstance(historical_store, LocalHistoricalPriceArtifactStore)
        self.assertEqual(historical_store.root, self.root / "historical_prices")
        self.assertEqual(historical_store.daily_reports_root, self.root / "daily_reports")

        identity_store = kwargs["identity_store"]
        self.assertIsInstance(identity_store, LocalIdentityRegistryStore)
        self.assertEqual(identity_store.root, self.root / "identity_registry")
        self.assertEqual(identity_store.reference_data_root, self.root / "reference_data")

        adjudication_store = kwargs["adjudication_store"]
        self.assertIsInstance(adjudication_store, LocalIdentityAdjudicationQueueStore)
        self.assertEqual(adjudication_store.root, self.root / "identity_registry")

        run_store = kwargs["run_store"]
        self.assertIsInstance(run_store, LocalDailyPipelineRunStore)
        self.assertEqual(run_store.root, self.root / "daily_pipeline")

        payload = json.loads(stdout.getvalue())
        self.assertEqual(payload["status"], "COMPLETE")
        self.assertEqual(payload["kind"], "DAILY_PIPELINE_RUN")
        self.assertEqual(payload["run_id"], fake_run.run_id)
        self.assertNotIn("derived_evidence_id", payload)
        self.assertNotIn("tick_size_snapshot_id", payload)

    def test_previous_run_id_from_run_is_rendered_unmodified(self) -> None:
        fake_run = _FakeRun()
        fake_run.previous_run_id = "9" * 64
        spy = _DelegatedSpy(result=fake_run)
        stdout = io.StringIO()

        with patch.dict(os.environ, self.env, clear=False):
            with patch(_READER_TARGET, MagicMock(return_value=object())):
                with patch(_BOUNDARY_TARGET, spy):
                    with contextlib.redirect_stdout(stdout):
                        exit_code = main(["run-pinned-gcs", "--spec-file", "spec.json"])

        self.assertEqual(exit_code, 0)
        payload = json.loads(stdout.getvalue())
        self.assertEqual(payload["previous_run_id"], "9" * 64)


class RunPinnedGcsFailureTests(RunPinnedGcsCliTestCase):
    def test_file_boundary_error_yields_sanitized_failure(self) -> None:
        secret = "SECRET-CLI-FILE-BOUNDARY-FAILURE-DO-NOT-LEAK-4b2e"
        spy = _DelegatedSpy(raises=PinnedGCSRunFileBoundaryError(secret))
        stdout = io.StringIO()
        stderr = io.StringIO()

        with patch.dict(os.environ, self.env, clear=False):
            with patch(_READER_TARGET, MagicMock(return_value=object())):
                with patch(_BOUNDARY_TARGET, spy):
                    with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
                        exit_code = main(["run-pinned-gcs", "--spec-file", "spec.json"])

        self.assertEqual(exit_code, 2)
        self.assertEqual(stdout.getvalue(), "")
        payload = json.loads(stderr.getvalue())
        self.assertEqual(payload, {"status": "FAILED", "error_type": "PinnedGCSRunFileBoundaryError"})
        self.assertNotIn(secret, stderr.getvalue())

    def test_service_error_yields_sanitized_failure(self) -> None:
        secret = "SECRET-CLI-SERVICE-FAILURE-DO-NOT-LEAK-6c3f"
        spy = _DelegatedSpy(raises=PinnedGCSRunServiceError(secret))
        stdout = io.StringIO()
        stderr = io.StringIO()

        with patch.dict(os.environ, self.env, clear=False):
            with patch(_READER_TARGET, MagicMock(return_value=object())):
                with patch(_BOUNDARY_TARGET, spy):
                    with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
                        exit_code = main(["run-pinned-gcs", "--spec-file", "spec.json"])

        self.assertEqual(exit_code, 2)
        self.assertEqual(stdout.getvalue(), "")
        payload = json.loads(stderr.getvalue())
        self.assertEqual(payload, {"status": "FAILED", "error_type": "PinnedGCSRunServiceError"})
        self.assertNotIn(secret, stderr.getvalue())

    def test_missing_spec_file_argument_fails_without_constructing_reader(self) -> None:
        mock_reader_class = MagicMock()
        stdout = io.StringIO()
        stderr = io.StringIO()

        with patch.dict(os.environ, self.env, clear=False):
            with patch(_READER_TARGET, mock_reader_class):
                with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
                    exit_code = main(["run-pinned-gcs"])

        self.assertEqual(exit_code, 2)
        mock_reader_class.assert_not_called()
        self.assertEqual(stdout.getvalue(), "")
        payload = json.loads(stderr.getvalue())
        self.assertEqual(set(payload), {"status", "error_type"})
        self.assertEqual(payload["status"], "FAILED")

    def test_unknown_extra_argument_is_rejected(self) -> None:
        stdout = io.StringIO()
        stderr = io.StringIO()

        with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
            exit_code = main(
                ["run-pinned-gcs", "--spec-file", "spec.json", "--unexpected", "value"]
            )

        self.assertEqual(exit_code, 2)
        self.assertEqual(stdout.getvalue(), "")
        payload = json.loads(stderr.getvalue())
        self.assertEqual(set(payload), {"status", "error_type"})


class RunPinnedGcsArgumentSurfaceTests(unittest.TestCase):
    def test_run_pinned_gcs_parser_exposes_only_spec_file_argument(self) -> None:
        root = parser()
        subparsers_action = next(
            action
            for action in root._actions
            if isinstance(action, argparse._SubParsersAction)
        )
        run_pinned_gcs_parser = subparsers_action.choices["run-pinned-gcs"]
        option_strings = {
            option
            for action in run_pinned_gcs_parser._actions
            for option in action.option_strings
        }
        self.assertEqual(option_strings, {"-h", "--help", "--spec-file"})

    def test_run_pinned_gcs_parses_spec_file_as_raw_string(self) -> None:
        args = parser().parse_args(["run-pinned-gcs", "--spec-file", "some/path.json"])
        self.assertEqual(args.command, "run-pinned-gcs")
        self.assertEqual(args.spec_file, "some/path.json")
        self.assertIsInstance(args.spec_file, str)


class ExistingSubcommandRegressionTests(unittest.TestCase):
    def test_run_subcommand_argument_shape_is_unchanged(self) -> None:
        args = parser().parse_args(
            [
                "run",
                "--session",
                "2026-07-20",
                "--cutoff",
                "2026-07-20T15:00:00+00:00",
                "--calendar-id",
                "a" * 64,
                "--security-master-file",
                "master.csv.gz",
                "--daily-bundle-file",
                "bundle.zip",
            ]
        )
        self.assertEqual(args.command, "run")
        self.assertEqual(args.calendar_id, "a" * 64)
        self.assertIsNone(args.previous_run_id)
        self.assertEqual(args.minimum_history_sessions, 120)

    def test_derive_subcommand_argument_shape_is_unchanged(self) -> None:
        args = parser().parse_args(["derive", "--run-id", "a" * 64])
        self.assertEqual(args.command, "derive")
        self.assertEqual(args.run_id, "a" * 64)
        self.assertEqual(args.minimum_history_sessions, 120)

    def test_show_subcommand_argument_shape_is_unchanged(self) -> None:
        args = parser().parse_args(["show", "--run-id", "a" * 64])
        self.assertEqual(args.command, "show")
        self.assertEqual(args.run_id, "a" * 64)

    def test_list_subcommand_argument_shape_is_unchanged(self) -> None:
        args = parser().parse_args(["list"])
        self.assertEqual(args.command, "list")


if __name__ == "__main__":
    unittest.main()
