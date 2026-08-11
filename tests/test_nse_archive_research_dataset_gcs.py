from __future__ import annotations

import hashlib
import unittest

from india_swing.daily_pipeline.acquisition import GCSObjectPayload
from india_swing.evaluation.nse_archive_research_dataset_gcs import (
    ExactNseArchiveResearchDatasetResolver,
    NseArchiveResearchDatasetGCSError,
    PinnedNseArchiveResearchDatasetRequest,
    nse_archive_research_dataset_object_name,
    read_pinned_nse_archive_research_dataset,
)
from india_swing.evaluation.nse_archive_research_dataset_store import (
    MAXIMUM_MANIFEST_BYTES,
    encode_nse_archive_research_dataset,
)

from tests.test_nse_archive_research_dataset import _baseline_dataset


class FakeReader:
    def __init__(self, content: bytes, *, generation: int = 19) -> None:
        self.content = content
        self.generation = generation
        self.calls: list[dict[str, object]] = []

    def read_generation(self, **kwargs: object) -> GCSObjectPayload:
        self.calls.append(dict(kwargs))
        return GCSObjectPayload(
            content_bytes=self.content,
            generation=self.generation,
        )


class PinnedNseArchiveResearchDatasetGCSTests(unittest.TestCase):
    def setUp(self) -> None:
        self.dataset = _baseline_dataset()
        self.content = encode_nse_archive_research_dataset(self.dataset)
        self.request = PinnedNseArchiveResearchDatasetRequest(
            bucket="india-swing-research-data",
            dataset_id=self.dataset.dataset_id,
            generation=19,
            expected_sha256=hashlib.sha256(self.content).hexdigest(),
        )

    def test_reads_only_the_canonical_exact_generation(self) -> None:
        reader = FakeReader(self.content)
        result = read_pinned_nse_archive_research_dataset(
            request=self.request,
            reader=reader,
        )
        self.assertEqual(result, self.dataset)
        self.assertEqual(
            reader.calls,
            [
                {
                    "bucket": self.request.bucket,
                    "object_name": nse_archive_research_dataset_object_name(
                        self.dataset.dataset_id
                    ),
                    "generation": 19,
                    "maximum_bytes": MAXIMUM_MANIFEST_BYTES,
                }
            ],
        )

    def test_rejects_wrong_generation_hash_or_dataset_content(self) -> None:
        cases = (
            FakeReader(self.content, generation=20),
            FakeReader(self.content + b"\n"),
            FakeReader(
                encode_nse_archive_research_dataset(
                    _baseline_dataset(shift_days=2)
                )
            ),
        )
        for reader in cases:
            with self.subTest(reader=reader), self.assertRaises(
                NseArchiveResearchDatasetGCSError
            ):
                read_pinned_nse_archive_research_dataset(
                    request=self.request,
                    reader=reader,
                )

    def test_bool_generation_and_noncanonical_values_are_rejected(self) -> None:
        for generation in (True, 0, -1):
            with self.subTest(generation=generation), self.assertRaises(
                NseArchiveResearchDatasetGCSError
            ):
                PinnedNseArchiveResearchDatasetRequest(
                    bucket="india-swing-research-data",
                    dataset_id=self.dataset.dataset_id,
                    generation=generation,
                    expected_sha256="a" * 64,
                )

    def test_exact_resolver_cannot_resolve_a_foreign_identity(self) -> None:
        resolver = ExactNseArchiveResearchDatasetResolver(self.dataset)
        self.assertIs(resolver.get(self.dataset.dataset_id), self.dataset)
        with self.assertRaises(NseArchiveResearchDatasetGCSError):
            resolver.get("0" * 64)

    def test_errors_are_sanitized_without_nested_context(self) -> None:
        try:
            read_pinned_nse_archive_research_dataset(
                request=self.request,
                reader=FakeReader(b"secret-invalid-json"),
            )
        except NseArchiveResearchDatasetGCSError as error:
            self.assertEqual(str(error), "pinned NSE archive research dataset read failed")
            self.assertIsNone(error.__cause__)
            self.assertIsNone(error.__context__)
            self.assertNotIn("secret", str(error))
        else:
            self.fail("expected sanitized failure")


if __name__ == "__main__":
    unittest.main()
