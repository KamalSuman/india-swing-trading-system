from __future__ import annotations

import contextlib
import io
import json
import tempfile
import unittest
import zipfile
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from india_swing.market_data.nse_archive_cli import (
    _build_research_dataset_exclusions,
    build_parser,
    main,
)
from india_swing.market_data.nse_archive import import_nse_historical_range
from india_swing.market_data.snapshot_store import LocalMarketSnapshotStore
from tests.test_nse_historical_archive import archive_bytes


UTC = timezone.utc
OBSERVED_AT = datetime(2026, 8, 2, 0, 0, tzinfo=UTC)


def _stage_sessions(root: Path, start: date, end: date) -> None:
    staging = root / "staging"
    archives = root / "source-archives"
    day = start
    while day <= end:
        session_staging = staging / day.isoformat()
        session_archives = archives / day.isoformat()
        session_staging.mkdir(parents=True, exist_ok=True)
        session_archives.mkdir(parents=True, exist_ok=True)
        archive_path = session_archives / f"Reports-Archives-Multiple-{day:%d%m%Y}.zip"
        archive_path.write_bytes(archive_bytes(session=day))
        with zipfile.ZipFile(archive_path) as archive:
            for name in archive.namelist():
                (session_staging / name).write_bytes(archive.read(name))
        day += timedelta(days=1)


def _run(argv: list[str]) -> tuple[int, dict]:
    buffer = io.StringIO()
    with contextlib.redirect_stdout(buffer):
        code = main(argv)
    return code, json.loads(buffer.getvalue())


class _ThreeRoleArchive:
    """Stages and imports one real three-role archive corpus once per test."""

    def __init__(self, root: Path) -> None:
        self.root = root
        self.train_start, self.train_end = date(2024, 1, 1), date(2024, 1, 21)
        self.validation_start, self.validation_end = (
            date(2024, 1, 22),
            date(2024, 2, 11),
        )
        self.test_start, self.test_end = date(2024, 2, 12), date(2024, 3, 3)
        _stage_sessions(root, self.train_start, self.train_end)
        _stage_sessions(root, self.validation_start, self.validation_end)
        _stage_sessions(root, self.test_start, self.test_end)
        self.store_root = root / "canonical"
        self.research_store_root = root / "research"
        store = LocalMarketSnapshotStore(self.store_root)
        _, self.train_index = import_nse_historical_range(
            staging_root=root / "staging",
            archive_root=root / "source-archives",
            store=store,
            start=self.train_start,
            end=self.train_end,
            observed_at=OBSERVED_AT,
        )
        _, self.validation_index = import_nse_historical_range(
            staging_root=root / "staging",
            archive_root=root / "source-archives",
            store=store,
            start=self.validation_start,
            end=self.validation_end,
            observed_at=OBSERVED_AT,
        )
        _, self.test_index = import_nse_historical_range(
            staging_root=root / "staging",
            archive_root=root / "source-archives",
            store=store,
            start=self.test_start,
            end=self.test_end,
            observed_at=OBSERVED_AT,
        )

    def build_argv(self, **overrides: str) -> list[str]:
        argv = [
            "research-dataset-build",
            "--store-root",
            str(self.store_root),
            "--research-store-root",
            str(self.research_store_root),
            "--index-snapshot-id",
            self.train_index.manifest.snapshot_id,
            "--index-snapshot-id",
            self.validation_index.manifest.snapshot_id,
            "--index-snapshot-id",
            self.test_index.manifest.snapshot_id,
            "--train-end",
            self.train_end.isoformat(),
            "--validation-start",
            self.validation_start.isoformat(),
            "--validation-end",
            self.validation_end.isoformat(),
            "--test-start",
            self.test_start.isoformat(),
            "--maximum-forward-label-horizon-sessions",
            "20",
        ]
        for key, value in overrides.items():
            flag = f"--{key.replace('_', '-')}"
            if flag in argv:
                argv[argv.index(flag) + 1] = value
            else:
                argv.extend([flag, value])
        return argv


class ParserTests(unittest.TestCase):
    def test_research_dataset_build_requires_every_input(self) -> None:
        required_flags = (
            "--store-root",
            "--research-store-root",
            "--index-snapshot-id",
            "--train-end",
            "--validation-start",
            "--validation-end",
            "--test-start",
            "--maximum-forward-label-horizon-sessions",
        )
        base = [
            "research-dataset-build",
            "--store-root",
            "s",
            "--research-store-root",
            "r",
            "--index-snapshot-id",
            "0" * 64,
            "--train-end",
            "2024-01-01",
            "--validation-start",
            "2024-01-02",
            "--validation-end",
            "2024-01-02",
            "--test-start",
            "2024-01-03",
            "--maximum-forward-label-horizon-sessions",
            "20",
        ]
        for flag in required_flags:
            with self.subTest(flag):
                argv = list(base)
                index = argv.index(flag)
                del argv[index : index + 2]
                with self.assertRaises(SystemExit):
                    build_parser().parse_args(argv)
        # The base argument set itself must parse cleanly.
        build_parser().parse_args(base)

    def test_research_dataset_show_requires_dataset_id(self) -> None:
        with self.assertRaises(SystemExit):
            build_parser().parse_args(
                ["research-dataset-show", "--research-store-root", "r"]
            )
        build_parser().parse_args(
            [
                "research-dataset-show",
                "--research-store-root",
                "r",
                "--dataset-id",
                "0" * 64,
            ]
        )

    def test_repeated_index_snapshot_id_order_is_preserved(self) -> None:
        ids = ("1" * 64, "2" * 64, "3" * 64)
        argv = [
            "research-dataset-build",
            "--store-root",
            "s",
            "--research-store-root",
            "r",
            "--train-end",
            "2024-01-01",
            "--validation-start",
            "2024-01-02",
            "--validation-end",
            "2024-01-02",
            "--test-start",
            "2024-01-03",
            "--maximum-forward-label-horizon-sessions",
            "20",
        ]
        for value in ids:
            argv.extend(["--index-snapshot-id", value])
        arguments = build_parser().parse_args(argv)
        self.assertEqual(tuple(arguments.index_snapshot_ids), ids)

    def test_exclusions_are_constructed_and_sorted_by_session(self) -> None:
        class _Namespace:
            source_accounting_failed_sessions = [date(2024, 3, 1), date(2024, 1, 1)]
            source_cross_source_join_failed_sessions = [date(2024, 2, 1)]

        exclusions = _build_research_dataset_exclusions(_Namespace())
        self.assertEqual(
            tuple(value.session for value in exclusions),
            (date(2024, 1, 1), date(2024, 2, 1), date(2024, 3, 1)),
        )
        self.assertEqual(exclusions[0].reason.value, "SOURCE_ACCOUNTING_FAILED")
        self.assertEqual(exclusions[1].reason.value, "SOURCE_CROSS_SOURCE_JOIN_FAILED")
        self.assertEqual(exclusions[2].reason.value, "SOURCE_ACCOUNTING_FAILED")


class ResearchDatasetBuildCliTests(unittest.TestCase):
    def test_build_writes_reloads_and_summarizes_without_raw_records(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            archive = _ThreeRoleArchive(Path(temporary))
            code, payload = _run(archive.build_argv())
            self.assertEqual(code, 0)
            self.assertEqual(payload["status"], "NSE_ARCHIVE_RESEARCH_DATASET_READY")
            self.assertEqual(payload["accepted_session_count"], 63)
            self.assertEqual(payload["record_count"], 63)
            self.assertEqual(payload["coverage_start"], "2024-01-01")
            self.assertEqual(payload["coverage_end"], "2024-03-03")
            self.assertEqual(len(payload["partitions"]), 3)
            for partition in payload["partitions"]:
                self.assertEqual(partition["session_count"], 21)
                self.assertEqual(
                    partition["unavailable_label_tail_session_count"], 20
                )
                self.assertEqual(
                    partition["candidate_label_origin_session_count"], 1
                )
            self.assertTrue(payload["collection_only"])
            self.assertFalse(payload["actionable"])
            self.assertFalse(payload["training_eligible"])
            for flag in (
                "feature_eligible",
                "label_eligible",
                "alert_eligible",
                "execution_eligible",
                "identity_resolution_complete",
                "corporate_action_adjustment_complete",
            ):
                self.assertFalse(payload[flag])
            self.assertEqual(payload["exclusions"], [])
            self.assertIn("dataset_id", payload)
            self.assertIn("split_policy_id", payload)

            raw_output = json.dumps(payload)
            for token in ("\"open\"", "\"close\"", "\"volume\"", "\"isin\""):
                self.assertNotIn(token, raw_output)

            code, second = _run(archive.build_argv())
            self.assertEqual(code, 0)
            self.assertEqual(second["dataset_id"], payload["dataset_id"])

    def test_build_rejects_a_date_claimed_under_both_exclusion_reasons(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            archive = _ThreeRoleArchive(Path(temporary))
            argv = archive.build_argv() + [
                "--source-accounting-failed-session",
                "2024-01-22",
                "--source-cross-source-join-failed-session",
                "2024-01-22",
            ]
            code, payload = _run(argv)
            self.assertEqual(code, 1)
            self.assertEqual(payload["status"], "FAILED")
            self.assertIn("error_type", payload)
            self.assertNotIn("2024-01-22", json.dumps(payload))

    def test_build_failure_is_sanitized(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            argv = [
                "research-dataset-build",
                "--store-root",
                str(root / "empty-store"),
                "--research-store-root",
                str(root / "research"),
                "--index-snapshot-id",
                "0" * 64,
                "--train-end",
                "2024-01-01",
                "--validation-start",
                "2024-01-02",
                "--validation-end",
                "2024-01-02",
                "--test-start",
                "2024-01-03",
                "--maximum-forward-label-horizon-sessions",
                "20",
            ]
            code, payload = _run(argv)
            self.assertEqual(code, 1)
            self.assertEqual(set(payload), {"status", "error_type"})
            self.assertEqual(payload["status"], "FAILED")
            self.assertNotIn(str(root), json.dumps(payload))


class ResearchDatasetShowCliTests(unittest.TestCase):
    def test_show_loads_only_the_requested_exact_id(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            archive = _ThreeRoleArchive(Path(temporary))
            code, built = _run(archive.build_argv())
            self.assertEqual(code, 0)

            code, shown = _run(
                [
                    "research-dataset-show",
                    "--research-store-root",
                    str(archive.research_store_root),
                    "--dataset-id",
                    built["dataset_id"],
                ]
            )
            self.assertEqual(code, 0)
            self.assertEqual(shown["status"], "NSE_ARCHIVE_RESEARCH_DATASET_LOADED")
            comparable = dict(shown)
            del comparable["status"]
            built_comparable = dict(built)
            del built_comparable["status"]
            self.assertEqual(comparable, built_comparable)

    def test_show_missing_id_is_sanitized(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            code, payload = _run(
                [
                    "research-dataset-show",
                    "--research-store-root",
                    str(Path(temporary) / "research"),
                    "--dataset-id",
                    "0" * 64,
                ]
            )
            self.assertEqual(code, 1)
            self.assertEqual(set(payload), {"status", "error_type"})

    def test_show_tampered_artifact_is_sanitized(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            archive = _ThreeRoleArchive(Path(temporary))
            code, built = _run(archive.build_argv())
            self.assertEqual(code, 0)

            artifact = (
                archive.research_store_root
                / "nse-archive-research-datasets"
                / f"{built['dataset_id']}.json"
            )
            original = artifact.read_text(encoding="utf-8")
            tampered = original.replace('"record_count":63', '"record_count":64', 1)
            self.assertNotEqual(original, tampered)
            artifact.write_text(tampered, encoding="utf-8")

            code, payload = _run(
                [
                    "research-dataset-show",
                    "--research-store-root",
                    str(archive.research_store_root),
                    "--dataset-id",
                    built["dataset_id"],
                ]
            )
            self.assertEqual(code, 1)
            self.assertEqual(set(payload), {"status", "error_type"})
            self.assertNotIn(str(archive.research_store_root), json.dumps(payload))


class ExistingCommandRegressionTests(unittest.TestCase):
    """import-range/verify-range had no dedicated CLI test file before this task."""

    def test_import_range_and_verify_range_success(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            _stage_sessions(root, date(2024, 4, 1), date(2024, 4, 1))
            argv = [
                "import-range",
                "--staging-root",
                str(root / "staging"),
                "--archive-root",
                str(root / "source-archives"),
                "--store-root",
                str(root / "canonical"),
                "--start",
                "2024-04-01",
                "--end",
                "2024-04-01",
                "--observed-at",
                OBSERVED_AT.isoformat(),
                "--workers",
                "1",
            ]
            code, imported = _run(argv)
            self.assertEqual(code, 0)
            self.assertEqual(imported["status"], "NSE_HISTORICAL_ARCHIVE_IMPORTED")
            self.assertEqual(imported["session_count"], 1)

            code, verified = _run(
                [
                    "verify-range",
                    "--store-root",
                    str(root / "canonical"),
                    "--index-snapshot-id",
                    imported["index_snapshot_id"],
                ]
            )
            self.assertEqual(code, 0)
            self.assertEqual(
                verified["status"], "NSE_HISTORICAL_ARCHIVE_RANGE_VERIFIED"
            )
            self.assertEqual(
                verified["index_snapshot_id"], imported["index_snapshot_id"]
            )

    def test_import_range_and_verify_range_sanitized_failure(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            with self.subTest("import_range_missing_staging"):
                code, payload = _run(
                    [
                        "import-range",
                        "--staging-root",
                        str(root / "missing-staging"),
                        "--archive-root",
                        str(root / "missing-archives"),
                        "--store-root",
                        str(root / "canonical"),
                        "--start",
                        "2024-04-01",
                        "--end",
                        "2024-04-01",
                        "--observed-at",
                        OBSERVED_AT.isoformat(),
                    ]
                )
                self.assertEqual(code, 1)
                self.assertEqual(set(payload), {"status", "error_type"})
                self.assertEqual(payload["status"], "FAILED")

            with self.subTest("verify_range_missing_index"):
                code, payload = _run(
                    [
                        "verify-range",
                        "--store-root",
                        str(root / "canonical"),
                        "--index-snapshot-id",
                        "0" * 64,
                    ]
                )
                self.assertEqual(code, 1)
                self.assertEqual(set(payload), {"status", "error_type"})
                self.assertEqual(payload["status"], "FAILED")


if __name__ == "__main__":
    unittest.main()
