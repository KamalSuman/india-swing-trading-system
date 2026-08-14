from __future__ import annotations

import hashlib
import io
import json
import unittest
from contextlib import redirect_stderr, redirect_stdout
from datetime import date
from pathlib import Path
from unittest import mock

from india_swing.evaluation import (
    nse_archive_research_identity_checkpoint_cloud_job as module,
)
from india_swing.daily_pipeline.acquisition import GCSObjectPayload
from india_swing.daily_pipeline.state_publication import PublishedStateObject
from india_swing.evaluation import nse_archive_research_identity as identity_module
from india_swing.evaluation.nse_archive_research_dataset_gcs import (
    PinnedNseArchiveResearchDatasetRequest,
    nse_archive_research_dataset_object_name,
)
from india_swing.evaluation.nse_archive_research_dataset_store import (
    MAXIMUM_MANIFEST_BYTES,
    encode_nse_archive_research_dataset,
)
from india_swing.evaluation.nse_archive_research_identity_checkpoint import (
    encode_nse_archive_research_identity_checkpoint,
    nse_archive_research_identity_checkpoint_object_name,
)
from tests.test_nse_archive_research_dataset import _baseline_dataset
from tests.test_nse_archive_research_identity import _record, _session
from tests.test_nse_archive_research_identity_checkpoint import _checkpoint


_BUCKET = "india-swing-research-data"
_ISIN_A = "INE009A01021"
_ISIN_B = "INE467B01029"


class _DatasetReader:
    def __init__(self, content_bytes: bytes, *, generation: int = 41) -> None:
        self.content_bytes = content_bytes
        self.generation = generation
        self.calls: list[dict[str, object]] = []

    def read_generation(self, **kwargs) -> GCSObjectPayload:
        self.calls.append(kwargs)
        return GCSObjectPayload(
            content_bytes=self.content_bytes,
            generation=self.generation,
        )


class _Writer:
    def __init__(self, *, malicious: bool = False) -> None:
        self.malicious = malicious
        self.calls: list[dict[str, object]] = []

    def create_or_verify(self, **kwargs) -> PublishedStateObject:
        self.calls.append(kwargs)
        payload = kwargs["content_bytes"]
        return PublishedStateObject(
            object_name=(
                "research/foreign-checkpoint.json"
                if self.malicious
                else kwargs["object_name"]
            ),
            generation=73,
            byte_count=len(payload),
            sha256=hashlib.sha256(payload).hexdigest(),
        )


class _CountingSessions:
    def __init__(self, values: tuple[object, ...]) -> None:
        self.values = values
        self.calls = 0
        self.yielded = 0

    def __call__(self, _dataset, _reader):
        self.calls += 1
        for value in self.values:
            self.yielded += 1
            yield value


def _dataset_context():
    dataset = _baseline_dataset()
    payload = encode_nse_archive_research_dataset(dataset)
    request = PinnedNseArchiveResearchDatasetRequest(
        bucket=_BUCKET,
        dataset_id=dataset.dataset_id,
        generation=41,
        expected_sha256=hashlib.sha256(payload).hexdigest(),
    )
    return dataset, payload, request


def _arguments(dataset_id: str, sha256: str) -> list[str]:
    return [
        "--market-data-root",
        str(Path.cwd().resolve()),
        "--dataset-bucket",
        _BUCKET,
        "--dataset-id",
        dataset_id,
        "--dataset-generation",
        "41",
        "--dataset-sha256",
        sha256,
        "--checkpoint-session",
        "2024-01-02",
        "--checkpoint-bucket",
        _BUCKET,
    ]


class CheckpointBuilderBoundaryTests(unittest.TestCase):
    def test_exact_dataset_pin_stops_at_session_and_publishes_once(self) -> None:
        dataset, payload, request = _dataset_context()
        first, second, third = dataset.accepted_sessions[:3]
        sessions = _CountingSessions(
            (
                _session(
                    first,
                    (_record(first, symbol="AAA", validated_isin=_ISIN_A),),
                ),
                _session(
                    second,
                    (_record(second, symbol="AAA", validated_isin=_ISIN_B),),
                ),
                _session(
                    third,
                    (_record(third, symbol="BBB", validated_isin=_ISIN_B),),
                ),
            )
        )
        dataset_reader = _DatasetReader(payload)
        writer = _Writer()
        with mock.patch.object(
            identity_module,
            "iter_verified_nse_archive_research_sessions",
            sessions,
        ):
            checkpoint, published = (
                module.build_and_publish_nse_archive_research_identity_checkpoint(
                    dataset_request=request,
                    dataset_reader=dataset_reader,
                    archive_reader=object(),
                    checkpoint_session=second,
                    checkpoint_bucket=_BUCKET,
                    writer=writer,
                )
            )

        self.assertEqual(sessions.calls, 1)
        self.assertEqual(sessions.yielded, 2)
        self.assertEqual(checkpoint.checkpoint_session, second)
        self.assertEqual(checkpoint.latest_by_listing_key[0].source_isin, _ISIN_B)
        self.assertEqual(len(dataset_reader.calls), 1)
        self.assertEqual(
            dataset_reader.calls[0],
            {
                "bucket": _BUCKET,
                "object_name": nse_archive_research_dataset_object_name(
                    dataset.dataset_id
                ),
                "generation": 41,
                "maximum_bytes": MAXIMUM_MANIFEST_BYTES,
            },
        )
        self.assertEqual(len(writer.calls), 1)
        self.assertEqual(
            writer.calls[0]["object_name"],
            nse_archive_research_identity_checkpoint_object_name(
                checkpoint.checkpoint_id
            ),
        )
        self.assertEqual(published.generation, 73)

    def test_malicious_writer_result_is_rejected_with_sanitized_error(self) -> None:
        dataset, payload, request = _dataset_context()
        checkpoint = _checkpoint(dataset, session=dataset.accepted_sessions[0])
        writer = _Writer(malicious=True)
        with mock.patch.object(
            module,
            "build_nse_archive_research_identity_checkpoint",
            return_value=checkpoint,
        ):
            with self.assertRaises(
                module.NseArchiveResearchIdentityCheckpointCloudJobError
            ) as context:
                module.build_and_publish_nse_archive_research_identity_checkpoint(
                    dataset_request=request,
                    dataset_reader=_DatasetReader(payload),
                    archive_reader=object(),
                    checkpoint_session=checkpoint.checkpoint_session,
                    checkpoint_bucket=_BUCKET,
                    writer=writer,
                )
        self.assertEqual(str(context.exception), module._ERROR)
        self.assertIsNone(context.exception.__cause__)
        self.assertIsNone(context.exception.__context__)
        self.assertEqual(len(writer.calls), 1)

    def test_replay_or_dataset_verification_failure_never_publishes(self) -> None:
        dataset, payload, request = _dataset_context()
        boundary = (
            module.build_and_publish_nse_archive_research_identity_checkpoint
        )
        for failure in ("replay", "dataset"):
            with self.subTest(failure=failure):
                writer = _Writer()
                reader = _DatasetReader(payload)
                expected_request = request
                patcher = mock.patch.object(
                    module,
                    "build_nse_archive_research_identity_checkpoint",
                    side_effect=RuntimeError("SECRET-REPLAY-MARKER"),
                )
                if failure == "dataset":
                    malformed = b"{}\n"
                    reader = _DatasetReader(malformed)
                    expected_request = PinnedNseArchiveResearchDatasetRequest(
                        bucket=_BUCKET,
                        dataset_id=dataset.dataset_id,
                        generation=41,
                        expected_sha256=hashlib.sha256(malformed).hexdigest(),
                    )
                    patcher = mock.patch.object(
                        module,
                        "build_nse_archive_research_identity_checkpoint",
                    )
                with patcher as build:
                    with self.assertRaises(
                        module.NseArchiveResearchIdentityCheckpointCloudJobError
                    ) as context:
                        boundary(
                            dataset_request=expected_request,
                            dataset_reader=reader,
                            archive_reader=object(),
                            checkpoint_session=dataset.accepted_sessions[0],
                            checkpoint_bucket=_BUCKET,
                            writer=writer,
                        )
                if failure == "dataset":
                    build.assert_not_called()
                self.assertEqual(writer.calls, [])
                self.assertEqual(str(context.exception), module._ERROR)
                self.assertIsNone(context.exception.__cause__)
                self.assertIsNone(context.exception.__context__)


class CheckpointBuilderMainTests(unittest.TestCase):
    def test_receipt_is_complete_canonical_and_deterministic(self) -> None:
        dataset, payload, _request = _dataset_context()
        checkpoint = _checkpoint(dataset, session=dataset.accepted_sessions[0])
        checkpoint_payload = encode_nse_archive_research_identity_checkpoint(
            checkpoint
        )
        published = PublishedStateObject(
            object_name=nse_archive_research_identity_checkpoint_object_name(
                checkpoint.checkpoint_id
            ),
            generation=73,
            byte_count=len(checkpoint_payload),
            sha256=hashlib.sha256(checkpoint_payload).hexdigest(),
        )
        expected = {
            "dataset_id": checkpoint.dataset_id,
            "checkpoint_session": checkpoint.checkpoint_session.isoformat(),
            "checkpoint_session_snapshot_id": (
                checkpoint.checkpoint_session_snapshot_id
            ),
            "checkpoint_id": checkpoint.checkpoint_id,
            "object_name": published.object_name,
            "generation": published.generation,
            "sha256": published.sha256,
            "listing_state_count": 1,
            "identity_state_count": 1,
            "collection_only": True,
            "actionable": False,
            "training_eligible": False,
            "feature_eligible": False,
            "label_eligible": False,
            "alert_eligible": False,
            "execution_eligible": False,
        }
        outputs = []
        with mock.patch.object(
            module,
            "build_and_publish_nse_archive_research_identity_checkpoint",
            return_value=(checkpoint, published),
        ) as build:
            for _ in range(2):
                stdout, stderr = io.StringIO(), io.StringIO()
                with redirect_stdout(stdout), redirect_stderr(stderr):
                    code = module.main(
                        _arguments(
                            dataset.dataset_id,
                            hashlib.sha256(payload).hexdigest(),
                        ),
                        dataset_reader_factory=object,
                        archive_reader_factory=lambda _root: object(),
                        writer_factory=object,
                    )
                self.assertEqual(code, 0)
                self.assertEqual(stderr.getvalue(), "")
                outputs.append(stdout.getvalue())
        self.assertEqual(build.call_count, 2)
        for call in build.call_args_list:
            request = call.kwargs["dataset_request"]
            self.assertEqual(request.bucket, _BUCKET)
            self.assertEqual(request.dataset_id, dataset.dataset_id)
            self.assertEqual(request.generation, 41)
            self.assertEqual(
                request.expected_sha256,
                hashlib.sha256(payload).hexdigest(),
            )
        self.assertEqual(outputs[0], outputs[1])
        self.assertEqual(json.loads(outputs[0]), expected)
        self.assertEqual(
            outputs[0],
            json.dumps(
                expected,
                allow_nan=False,
                ensure_ascii=False,
                separators=(",", ":"),
                sort_keys=True,
            )
            + "\n",
        )

    def test_partial_and_malformed_arguments_emit_only_static_failure(self) -> None:
        dataset, payload, _request = _dataset_context()
        cases = (
            ["--dataset-id", "SECRET-PARTIAL-MARKER"],
            [
                *_arguments(
                    dataset.dataset_id,
                    hashlib.sha256(payload).hexdigest(),
                )[:-3],
                "2024-1-2",
                "--checkpoint-bucket",
                _BUCKET,
            ],
        )
        for arguments in cases:
            with self.subTest(arguments=arguments):
                stdout, stderr = io.StringIO(), io.StringIO()
                with redirect_stdout(stdout), redirect_stderr(stderr):
                    code = module.main(arguments)
                self.assertEqual(code, 2)
                self.assertEqual(stdout.getvalue(), "")
                self.assertNotIn("SECRET", stderr.getvalue())
                self.assertEqual(
                    json.loads(stderr.getvalue()),
                    {
                        "error_type": (
                            "NseArchiveResearchIdentityCheckpointCloudJobError"
                        ),
                        "status": "FAILED",
                    },
                )

    def test_internal_failure_emits_only_static_failure(self) -> None:
        dataset, payload, _request = _dataset_context()
        stdout, stderr = io.StringIO(), io.StringIO()
        with mock.patch.object(
            module,
            "build_and_publish_nse_archive_research_identity_checkpoint",
            side_effect=RuntimeError("SECRET-INTERNAL-MARKER"),
        ):
            with redirect_stdout(stdout), redirect_stderr(stderr):
                code = module.main(
                    _arguments(
                        dataset.dataset_id,
                        hashlib.sha256(payload).hexdigest(),
                    ),
                    dataset_reader_factory=object,
                    archive_reader_factory=lambda _root: object(),
                    writer_factory=object,
                )
        self.assertEqual(code, 2)
        self.assertEqual(stdout.getvalue(), "")
        self.assertNotIn("SECRET", stderr.getvalue())
        self.assertEqual(
            json.loads(stderr.getvalue()),
            {
                "error_type": "NseArchiveResearchIdentityCheckpointCloudJobError",
                "status": "FAILED",
            },
        )


class CheckpointBuilderCapabilityTests(unittest.TestCase):
    def test_boundary_has_no_discovery_trading_or_messaging_capability(self) -> None:
        source = Path(module.__file__).read_text(encoding="utf-8")
        for forbidden in (
            "list_blobs(",
            "list_objects(",
            "get_latest",
            "select_latest",
            "latest_object",
            "place_order",
            "modify_order",
            "cancel_order",
            "Telegram",
            "getUpdates",
            "requests.",
            "urllib",
            "scheduler",
        ):
            self.assertNotIn(forbidden, source)

        manifest = Path("infra/forward-paper-identity-checkpoint-job.yaml").read_text(
            encoding="utf-8"
        )
        self.assertIn("kind: Job", manifest)
        self.assertIn("maxRetries: 0", manifest)
        self.assertNotIn("kind: JobSchedule", manifest)
        self.assertNotIn("schedule:", manifest)
        self.assertIn("--dataset-generation", manifest)
        self.assertIn("--dataset-sha256", manifest)
        self.assertIn("--checkpoint-session", manifest)

        project = Path("pyproject.toml").read_text(encoding="utf-8")
        self.assertIn(
            "india-swing-nse-archive-identity-checkpoint-cloud-job = "
            '"india_swing.evaluation.'
            'nse_archive_research_identity_checkpoint_cloud_job:main"',
            project,
        )


if __name__ == "__main__":
    unittest.main()
