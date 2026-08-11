from __future__ import annotations

import hashlib
import io
import json
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from datetime import date
from pathlib import Path
from unittest import mock

import india_swing.evaluation.nse_archive_research_dataset_cloud_job as module
from india_swing.daily_pipeline.state_publication import PublishedStateObject
from india_swing.evaluation.nse_archive_research_dataset import ResearchArchiveSplitPolicy
from india_swing.evaluation.nse_archive_research_dataset_store import (
    MAXIMUM_MANIFEST_BYTES,
    encode_nse_archive_research_dataset,
)
from tests.test_nse_archive_research_dataset import _baseline_dataset


class _Writer:
    def __init__(self, *, malicious: bool = False) -> None:
        self.calls: list[dict[str, object]] = []
        self.malicious = malicious

    def create_or_verify(self, **kwargs) -> PublishedStateObject:
        self.calls.append(kwargs)
        content = kwargs["content_bytes"]
        object_name = kwargs["object_name"]
        return PublishedStateObject(
            object_name=("foreign/object.json" if self.malicious else object_name),
            generation=7,
            byte_count=len(content),
            sha256=hashlib.sha256(content).hexdigest(),
        )


def _policy() -> ResearchArchiveSplitPolicy:
    return ResearchArchiveSplitPolicy(
        train_end=date(2024, 1, 31),
        validation_start=date(2024, 2, 1),
        validation_end=date(2024, 2, 29),
        test_start=date(2024, 3, 1),
        maximum_forward_label_horizon_sessions=20,
    )


class PublicationBoundaryTests(unittest.TestCase):
    def test_builds_exact_ids_and_publishes_canonical_content_addressed_object(self) -> None:
        dataset = _baseline_dataset()
        writer = _Writer()
        with tempfile.TemporaryDirectory() as directory, mock.patch.object(
            module, "build_nse_archive_research_dataset", return_value=dataset
        ) as build:
            result, published = module.build_and_publish_nse_archive_research_dataset(
                store_root=Path(directory).resolve(),
                bucket="valid-research-bucket",
                index_snapshot_ids=dataset.index_snapshot_ids,
                split_policy=dataset.split_policy,
                exclusions=(),
                writer=writer,
            )

        self.assertIs(result, dataset)
        self.assertEqual(published.generation, 7)
        build.assert_called_once()
        self.assertEqual(build.call_args.kwargs["index_snapshot_ids"], dataset.index_snapshot_ids)
        self.assertEqual(len(writer.calls), 1)
        call = writer.calls[0]
        expected_bytes = encode_nse_archive_research_dataset(dataset)
        self.assertEqual(call["content_bytes"], expected_bytes)
        self.assertEqual(call["bucket"], "valid-research-bucket")
        self.assertEqual(
            call["object_name"],
            f"research/nse-archive-datasets/v1/{dataset.dataset_id}.json",
        )
        self.assertEqual(call["content_type"], "application/json")
        self.assertEqual(call["maximum_bytes"], MAXIMUM_MANIFEST_BYTES)

    def test_relative_source_root_fails_before_build_or_write(self) -> None:
        writer = _Writer()
        with mock.patch.object(module, "build_nse_archive_research_dataset") as build:
            with self.assertRaises(module.NseArchiveResearchDatasetCloudJobError):
                module.build_and_publish_nse_archive_research_dataset(
                    store_root=Path("relative"),
                    bucket="valid-research-bucket",
                    index_snapshot_ids=("a" * 64,),
                    split_policy=_policy(),
                    exclusions=(),
                    writer=writer,
                )
        build.assert_not_called()
        self.assertEqual(writer.calls, [])

    def test_malicious_writer_result_fails_closed(self) -> None:
        dataset = _baseline_dataset()
        with tempfile.TemporaryDirectory() as directory, mock.patch.object(
            module, "build_nse_archive_research_dataset", return_value=dataset
        ):
            with self.assertRaises(module.NseArchiveResearchDatasetCloudJobError):
                module.build_and_publish_nse_archive_research_dataset(
                    store_root=Path(directory).resolve(),
                    bucket="valid-research-bucket",
                    index_snapshot_ids=dataset.index_snapshot_ids,
                    split_policy=dataset.split_policy,
                    exclusions=(),
                    writer=_Writer(malicious=True),
                )


class MainTests(unittest.TestCase):
    def test_invalid_arguments_are_static_and_sanitized(self) -> None:
        marker = "SECRET-ARGUMENT-MARKER"
        stdout, stderr = io.StringIO(), io.StringIO()
        with redirect_stdout(stdout), redirect_stderr(stderr):
            code = module.main(["--store-root", marker])
        self.assertEqual(code, 2)
        self.assertEqual(stdout.getvalue(), "")
        self.assertNotIn(marker, stderr.getvalue())
        self.assertEqual(
            json.loads(stderr.getvalue()),
            {"error_type": "NseArchiveResearchDatasetCloudJobError", "status": "FAILED"},
        )

    def test_success_emits_one_canonical_summary_line(self) -> None:
        dataset = _baseline_dataset()
        writer = _Writer()
        with tempfile.TemporaryDirectory() as directory, mock.patch.object(
            module, "build_nse_archive_research_dataset", return_value=dataset
        ):
            policy = dataset.split_policy
            stdout, stderr = io.StringIO(), io.StringIO()
            with redirect_stdout(stdout), redirect_stderr(stderr):
                code = module.main(
                    [
                        "--store-root", str(Path(directory).resolve()),
                        "--bucket", "valid-research-bucket",
                        "--index-snapshot-id", dataset.index_snapshot_ids[0],
                        "--train-end", policy.train_end.isoformat(),
                        "--validation-start", policy.validation_start.isoformat(),
                        "--validation-end", policy.validation_end.isoformat(),
                        "--test-start", policy.test_start.isoformat(),
                        "--maximum-forward-label-horizon-sessions",
                        str(policy.maximum_forward_label_horizon_sessions),
                    ],
                    writer_factory=lambda: writer,
                )
        self.assertEqual(code, 0)
        self.assertEqual(stderr.getvalue(), "")
        envelope = json.loads(stdout.getvalue())
        self.assertEqual(envelope["status"], "NSE_ARCHIVE_RESEARCH_DATASET_PUBLISHED")
        self.assertEqual(envelope["dataset_id"], dataset.dataset_id)
        self.assertFalse(envelope["actionable"])
        self.assertFalse(envelope["feature_eligible"])
        self.assertTrue(stdout.getvalue().endswith("\n"))

    def test_joined_index_ids_preserve_exact_supplied_order(self) -> None:
        dataset = _baseline_dataset()
        writer = _Writer()
        first, second = "a" * 64, "b" * 64
        with tempfile.TemporaryDirectory() as directory, mock.patch.object(
            module, "build_nse_archive_research_dataset", return_value=dataset
        ) as build:
            with redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
                code = module.main(
                    [
                        "--store-root", str(Path(directory).resolve()),
                        "--bucket", "valid-research-bucket",
                        "--index-snapshot-ids", f"{first};{second}",
                        "--train-end", "2022-12-31",
                        "--validation-start", "2023-01-01",
                        "--validation-end", "2024-12-31",
                        "--test-start", "2025-01-01",
                        "--maximum-forward-label-horizon-sessions", "20",
                    ],
                    writer_factory=lambda: writer,
                )
        self.assertEqual(code, 0)
        self.assertEqual(build.call_args.kwargs["index_snapshot_ids"], (first, second))

    def test_internal_failure_is_static_and_sanitized(self) -> None:
        marker = "SECRET-MARKER-never-echo"
        stdout, stderr = io.StringIO(), io.StringIO()
        with tempfile.TemporaryDirectory() as directory, mock.patch.object(
            module,
            "build_nse_archive_research_dataset",
            side_effect=RuntimeError(marker),
        ):
            with redirect_stdout(stdout), redirect_stderr(stderr):
                code = module.main(
                    [
                        "--store-root", str(Path(directory).resolve()),
                        "--bucket", "valid-research-bucket",
                        "--index-snapshot-id", "a" * 64,
                        "--train-end", "2022-12-31",
                        "--validation-start", "2023-01-01",
                        "--validation-end", "2024-12-31",
                        "--test-start", "2025-01-01",
                        "--maximum-forward-label-horizon-sessions", "20",
                    ],
                    writer_factory=_Writer,
                )
        self.assertEqual(code, 2)
        self.assertEqual(stdout.getvalue(), "")
        self.assertNotIn(marker, stderr.getvalue())
        self.assertEqual(
            json.loads(stderr.getvalue()),
            {"error_type": "NseArchiveResearchDatasetCloudJobError", "status": "FAILED"},
        )


class CapabilityTests(unittest.TestCase):
    def test_module_has_no_listing_latest_order_or_notification_capability(self) -> None:
        source = Path(module.__file__).read_text(encoding="utf-8")
        for forbidden in (
            "list_blobs", "latest", "place_order", "modify_order", "cancel_order",
            "Telegram", "requests.", "urllib", "time.sleep", "while True",
        ):
            self.assertNotIn(forbidden, source)


if __name__ == "__main__":
    unittest.main()
