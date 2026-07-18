from __future__ import annotations

import gzip
import hashlib
import unittest
from datetime import date

from india_swing.daily_pipeline.acquisition import (
    AcquiredFile,
    AcquisitionError,
    AcquisitionFileType,
    GCSLandingObjectReader,
    GCSObjectPayload,
    GCSObjectReader,
    GoogleCloudStorageObjectReader,
    LandingObjectRequest,
)

_SECURITY_MASTER_MAXIMUM_BYTES = 32 * 1024 * 1024
_DAILY_BUNDLE_MAXIMUM_BYTES = 128 * 1024 * 1024


class FakeGCSObjectReader:
    """Fake GCSObjectReader. Never contacts GCP; records every call made."""

    def __init__(self, *, generation: int, content_bytes: bytes) -> None:
        self.generation = generation
        self.content_bytes = content_bytes
        self.calls: list[dict[str, object]] = []

    def read_generation(
        self, *, bucket: str, object_name: str, generation: int, maximum_bytes: int
    ) -> GCSObjectPayload:
        self.calls.append(
            {
                "bucket": bucket,
                "object_name": object_name,
                "generation": generation,
                "maximum_bytes": maximum_bytes,
            }
        )
        return GCSObjectPayload(content_bytes=self.content_bytes, generation=self.generation)


class FakeBlob:
    """Stand-in for google.cloud.storage.Blob. Never lists; records download args."""

    def __init__(
        self,
        object_name: str,
        requested_generation: object,
        *,
        observed_generation: object,
        content_bytes: bytes,
    ) -> None:
        self.name = object_name
        self.requested_generation = requested_generation
        self.generation = None
        self._observed_generation = observed_generation
        self._content_bytes = content_bytes
        self.download_calls: list[dict[str, object]] = []

    def download_as_bytes(
        self, *, end=None, raw_download: bool = False, if_generation_match=None
    ) -> bytes:
        self.download_calls.append(
            {"end": end, "raw_download": raw_download, "if_generation_match": if_generation_match}
        )
        self.generation = self._observed_generation
        if end is None:
            return self._content_bytes
        return self._content_bytes[: end + 1]


class ContentEncodingAwareFakeBlob:
    """Simulates GCS transcoding: Content-Encoding: gzip objects are served
    decompressed unless raw_download=True, which returns the exact stored
    (compressed) bytes. Used only by the content-encoding regression test.
    """

    def __init__(
        self,
        object_name: str,
        requested_generation: object,
        *,
        observed_generation: object,
        stored_bytes: bytes,
        expanded_bytes: bytes,
    ) -> None:
        self.name = object_name
        self.requested_generation = requested_generation
        self.generation = None
        self._observed_generation = observed_generation
        self._stored_bytes = stored_bytes
        self._expanded_bytes = expanded_bytes
        self.download_calls: list[dict[str, object]] = []

    def download_as_bytes(
        self, *, end=None, raw_download: bool = False, if_generation_match=None
    ) -> bytes:
        self.download_calls.append(
            {"end": end, "raw_download": raw_download, "if_generation_match": if_generation_match}
        )
        self.generation = self._observed_generation
        content = self._stored_bytes if raw_download else self._expanded_bytes
        if end is None:
            return content
        return content[: end + 1]


class FakeBucket:
    """Stand-in for google.cloud.storage.Bucket. Has no listing method."""

    def __init__(self, name: str, *, blob_factory) -> None:
        self.name = name
        self._blob_factory = blob_factory
        self.blob_calls: list[tuple[str, object]] = []
        self.blobs: list[object] = []

    def blob(self, object_name: str, generation: object = None) -> object:
        self.blob_calls.append((object_name, generation))
        blob = self._blob_factory(object_name, generation)
        self.blobs.append(blob)
        return blob


class FakeStorageClient:
    """Stand-in for google.cloud.storage.Client. Has no listing method.

    Defaults to producing plain FakeBlob instances from observed_generation/
    content_bytes; pass blob_factory=... to inject a different fake blob
    (e.g. ContentEncodingAwareFakeBlob) for a specific test.
    """

    def __init__(
        self,
        *,
        observed_generation: object = None,
        content_bytes: bytes = b"",
        blob_factory=None,
    ) -> None:
        if blob_factory is not None:
            self._blob_factory = blob_factory
        else:
            self._blob_factory = lambda object_name, generation: FakeBlob(
                object_name,
                generation,
                observed_generation=observed_generation,
                content_bytes=content_bytes,
            )
        self.bucket_calls: list[str] = []
        self.buckets: list[FakeBucket] = []

    def bucket(self, bucket_name: str) -> FakeBucket:
        self.bucket_calls.append(bucket_name)
        bucket = FakeBucket(bucket_name, blob_factory=self._blob_factory)
        self.buckets.append(bucket)
        return bucket


_BUCKET = "swing-data-indian-swing-trading-bot"


def _security_master_request(
    *,
    target_session: date,
    generation: int = 100,
    content_bytes: bytes = b"mock-master-content",
) -> tuple[LandingObjectRequest, FakeGCSObjectReader]:
    filename = f"NSE_CM_security_{target_session.strftime('%d%m%Y')}.csv.gz"
    request = LandingObjectRequest(
        bucket=_BUCKET,
        object_name=f"landing/{target_session.isoformat()}/{filename}",
        generation=generation,
        expected_sha256=hashlib.sha256(content_bytes).hexdigest(),
        target_session=target_session,
        file_type=AcquisitionFileType.SECURITY_MASTER,
    )
    reader = FakeGCSObjectReader(generation=generation, content_bytes=content_bytes)
    return request, reader


def _daily_bundle_request(
    *,
    target_session: date,
    generation: int = 200,
    content_bytes: bytes = b"mock-bundle-content",
) -> tuple[LandingObjectRequest, FakeGCSObjectReader]:
    request = LandingObjectRequest(
        bucket=_BUCKET,
        object_name=f"landing/{target_session.isoformat()}/Reports-Daily-Multiple.zip",
        generation=generation,
        expected_sha256=hashlib.sha256(content_bytes).hexdigest(),
        target_session=target_session,
        file_type=AcquisitionFileType.DAILY_BUNDLE,
    )
    reader = FakeGCSObjectReader(generation=generation, content_bytes=content_bytes)
    return request, reader


class LandingObjectRequestTests(unittest.TestCase):
    def test_accepts_valid_security_master_request(self) -> None:
        request, _ = _security_master_request(target_session=date(2026, 7, 15))
        self.assertEqual(request.file_type, AcquisitionFileType.SECURITY_MASTER)
        self.assertEqual(
            request.object_name, "landing/2026-07-15/NSE_CM_security_15072026.csv.gz"
        )

    def test_accepts_valid_daily_bundle_request(self) -> None:
        request, _ = _daily_bundle_request(target_session=date(2026, 7, 15))
        self.assertEqual(request.file_type, AcquisitionFileType.DAILY_BUNDLE)
        self.assertEqual(
            request.object_name, "landing/2026-07-15/Reports-Daily-Multiple.zip"
        )

    def test_rejects_invalid_bucket_name(self) -> None:
        request, _ = _security_master_request(target_session=date(2026, 7, 15))
        with self.assertRaises(AcquisitionError):
            LandingObjectRequest(
                bucket="Bad_Bucket!",
                object_name=request.object_name,
                generation=request.generation,
                expected_sha256=request.expected_sha256,
                target_session=request.target_session,
                file_type=request.file_type,
            )

    def test_rejects_non_positive_generation(self) -> None:
        request, _ = _security_master_request(target_session=date(2026, 7, 15))
        for bad_generation in (0, -1, -100):
            with self.assertRaises(AcquisitionError):
                LandingObjectRequest(
                    bucket=request.bucket,
                    object_name=request.object_name,
                    generation=bad_generation,
                    expected_sha256=request.expected_sha256,
                    target_session=request.target_session,
                    file_type=request.file_type,
                )

    def test_rejects_non_integer_generation(self) -> None:
        request, _ = _security_master_request(target_session=date(2026, 7, 15))
        for bad_generation in ("100", 100.0, True, None):
            with self.assertRaises(AcquisitionError):
                LandingObjectRequest(
                    bucket=request.bucket,
                    object_name=request.object_name,
                    generation=bad_generation,
                    expected_sha256=request.expected_sha256,
                    target_session=request.target_session,
                    file_type=request.file_type,
                )

    def test_accepts_generation_at_int64_max(self) -> None:
        request, _ = _security_master_request(
            target_session=date(2026, 7, 15), generation=9223372036854775807
        )
        self.assertEqual(request.generation, 9223372036854775807)

    def test_rejects_generation_above_int64_max(self) -> None:
        request, _ = _security_master_request(target_session=date(2026, 7, 15))
        for bad_generation in (9223372036854775808, 2**100):
            with self.assertRaises(AcquisitionError):
                LandingObjectRequest(
                    bucket=request.bucket,
                    object_name=request.object_name,
                    generation=bad_generation,
                    expected_sha256=request.expected_sha256,
                    target_session=request.target_session,
                    file_type=request.file_type,
                )

    def test_rejects_malformed_sha256(self) -> None:
        request, _ = _security_master_request(target_session=date(2026, 7, 15))
        for bad_sha in ("not-a-hash", "ABCDEF" * 10 + "abcd", "", "0" * 63, "0" * 65):
            with self.assertRaises(AcquisitionError):
                LandingObjectRequest(
                    bucket=request.bucket,
                    object_name=request.object_name,
                    generation=request.generation,
                    expected_sha256=bad_sha,
                    target_session=request.target_session,
                    file_type=request.file_type,
                )

    def test_rejects_non_date_target_session(self) -> None:
        request, _ = _security_master_request(target_session=date(2026, 7, 15))
        with self.assertRaises(AcquisitionError):
            LandingObjectRequest(
                bucket=request.bucket,
                object_name=request.object_name,
                generation=request.generation,
                expected_sha256=request.expected_sha256,
                target_session="2026-07-15",
                file_type=request.file_type,
            )

    def test_rejects_invalid_file_type(self) -> None:
        request, _ = _security_master_request(target_session=date(2026, 7, 15))
        with self.assertRaises(AcquisitionError):
            LandingObjectRequest(
                bucket=request.bucket,
                object_name=request.object_name,
                generation=request.generation,
                expected_sha256=request.expected_sha256,
                target_session=request.target_session,
                file_type="SECURITY_MASTER",
            )

    def test_rejects_security_master_filename_with_wrong_date(self) -> None:
        target_session = date(2026, 7, 15)
        content_bytes = b"mock-master-content"
        with self.assertRaises(AcquisitionError):
            LandingObjectRequest(
                bucket=_BUCKET,
                object_name="landing/2026-07-15/NSE_CM_security_16072026.csv.gz",
                generation=100,
                expected_sha256=hashlib.sha256(content_bytes).hexdigest(),
                target_session=target_session,
                file_type=AcquisitionFileType.SECURITY_MASTER,
            )

    def test_rejects_object_name_with_mismatched_session_path(self) -> None:
        target_session = date(2026, 7, 15)
        content_bytes = b"mock-master-content"
        with self.assertRaises(AcquisitionError):
            LandingObjectRequest(
                bucket=_BUCKET,
                object_name="landing/2026-07-16/NSE_CM_security_15072026.csv.gz",
                generation=100,
                expected_sha256=hashlib.sha256(content_bytes).hexdigest(),
                target_session=target_session,
                file_type=AcquisitionFileType.SECURITY_MASTER,
            )

    def test_rejects_path_traversal_object_name(self) -> None:
        target_session = date(2026, 7, 15)
        content_bytes = b"mock-master-content"
        for bad_object_name in (
            "landing/../secrets/NSE_CM_security_15072026.csv.gz",
            "/etc/landing/2026-07-15/NSE_CM_security_15072026.csv.gz",
            "landing/2026-07-15/../../NSE_CM_security_15072026.csv.gz",
            "landing/2026-07-15/NSE_CM_security_15072026.csv.gz/../../etc/passwd",
        ):
            with self.assertRaises(AcquisitionError):
                LandingObjectRequest(
                    bucket=_BUCKET,
                    object_name=bad_object_name,
                    generation=100,
                    expected_sha256=hashlib.sha256(content_bytes).hexdigest(),
                    target_session=target_session,
                    file_type=AcquisitionFileType.SECURITY_MASTER,
                )

    def test_rejects_daily_bundle_object_name_missing_session_prefix(self) -> None:
        target_session = date(2026, 7, 15)
        content_bytes = b"mock-bundle-content"
        with self.assertRaises(AcquisitionError):
            LandingObjectRequest(
                bucket=_BUCKET,
                object_name="landing/Reports-Daily-Multiple.zip",
                generation=100,
                expected_sha256=hashlib.sha256(content_bytes).hexdigest(),
                target_session=target_session,
                file_type=AcquisitionFileType.DAILY_BUNDLE,
            )

    def test_rejects_daily_bundle_wrong_filename(self) -> None:
        target_session = date(2026, 7, 15)
        content_bytes = b"mock-bundle-content"
        with self.assertRaises(AcquisitionError):
            LandingObjectRequest(
                bucket=_BUCKET,
                object_name="landing/2026-07-15/Reports-Daily-Multiple-Renamed.zip",
                generation=100,
                expected_sha256=hashlib.sha256(content_bytes).hexdigest(),
                target_session=target_session,
                file_type=AcquisitionFileType.DAILY_BUNDLE,
            )

    def test_rejects_malformed_object_name_type(self) -> None:
        request, _ = _security_master_request(target_session=date(2026, 7, 15))
        with self.assertRaises(AcquisitionError):
            LandingObjectRequest(
                bucket=request.bucket,
                object_name=None,
                generation=request.generation,
                expected_sha256=request.expected_sha256,
                target_session=request.target_session,
                file_type=request.file_type,
            )


class GCSLandingObjectReaderTests(unittest.TestCase):
    def test_reads_and_verifies_security_master(self) -> None:
        target_session = date(2026, 7, 15)
        request, fake = _security_master_request(target_session=target_session)
        reader = GCSLandingObjectReader(fake)

        acquired = reader.read(request)

        self.assertIsInstance(acquired, AcquiredFile)
        self.assertEqual(acquired.bucket, _BUCKET)
        self.assertEqual(acquired.object_name, request.object_name)
        self.assertEqual(acquired.generation, request.generation)
        self.assertEqual(acquired.target_session, target_session)
        self.assertEqual(acquired.file_type, AcquisitionFileType.SECURITY_MASTER)
        self.assertEqual(acquired.content_bytes, b"mock-master-content")
        self.assertEqual(acquired.sha256_hash, request.expected_sha256)

    def test_reads_and_verifies_daily_bundle(self) -> None:
        target_session = date(2026, 7, 16)
        request, fake = _daily_bundle_request(target_session=target_session)
        reader = GCSLandingObjectReader(fake)

        acquired = reader.read(request)

        self.assertEqual(acquired.file_type, AcquisitionFileType.DAILY_BUNDLE)
        self.assertEqual(acquired.object_name, request.object_name)
        self.assertEqual(acquired.content_bytes, b"mock-bundle-content")

    def test_makes_exactly_one_explicit_generation_call(self) -> None:
        request, fake = _security_master_request(target_session=date(2026, 7, 15))
        reader = GCSLandingObjectReader(fake)

        reader.read(request)

        self.assertEqual(
            fake.calls,
            [
                {
                    "bucket": request.bucket,
                    "object_name": request.object_name,
                    "generation": request.generation,
                    "maximum_bytes": _SECURITY_MASTER_MAXIMUM_BYTES,
                }
            ],
        )
        self.assertFalse(hasattr(fake, "list_blobs"))
        self.assertFalse(hasattr(fake, "list_blobs_generic"))

    def test_passes_security_master_maximum_bytes(self) -> None:
        request, fake = _security_master_request(target_session=date(2026, 7, 15))
        reader = GCSLandingObjectReader(fake)
        reader.read(request)
        self.assertEqual(fake.calls[0]["maximum_bytes"], _SECURITY_MASTER_MAXIMUM_BYTES)

    def test_passes_daily_bundle_maximum_bytes(self) -> None:
        request, fake = _daily_bundle_request(target_session=date(2026, 7, 15))
        reader = GCSLandingObjectReader(fake)
        reader.read(request)
        self.assertEqual(fake.calls[0]["maximum_bytes"], _DAILY_BUNDLE_MAXIMUM_BYTES)

    def test_rejects_non_exact_request_type(self) -> None:
        reader = GCSLandingObjectReader(
            FakeGCSObjectReader(generation=1, content_bytes=b"x")
        )
        with self.assertRaises(AcquisitionError):
            reader.read("not-a-request")  # type: ignore[arg-type]

    def test_rejects_wrong_generation_returned_by_client(self) -> None:
        request, fake = _security_master_request(
            target_session=date(2026, 7, 15), generation=100
        )
        fake.generation = 999
        reader = GCSLandingObjectReader(fake)
        with self.assertRaises(AcquisitionError):
            reader.read(request)

    def test_rejects_sha256_mismatch(self) -> None:
        request, fake = _security_master_request(target_session=date(2026, 7, 15))
        fake.content_bytes = b"tampered-content"
        reader = GCSLandingObjectReader(fake)
        with self.assertRaises(AcquisitionError):
            reader.read(request)

    def test_rejects_empty_content(self) -> None:
        target_session = date(2026, 7, 15)
        request, fake = _security_master_request(
            target_session=target_session, content_bytes=b""
        )
        reader = GCSLandingObjectReader(fake)
        with self.assertRaises(AcquisitionError):
            reader.read(request)

    def test_rejects_oversized_security_master_content(self) -> None:
        target_session = date(2026, 7, 15)
        oversized = b"a" * (_SECURITY_MASTER_MAXIMUM_BYTES + 1)
        request, fake = _security_master_request(
            target_session=target_session, content_bytes=oversized
        )
        reader = GCSLandingObjectReader(fake)
        with self.assertRaises(AcquisitionError):
            reader.read(request)

    def test_accepts_content_exactly_at_security_master_limit(self) -> None:
        target_session = date(2026, 7, 15)
        exactly_at_limit = b"a" * _SECURITY_MASTER_MAXIMUM_BYTES
        request, fake = _security_master_request(
            target_session=target_session, content_bytes=exactly_at_limit
        )
        reader = GCSLandingObjectReader(fake)
        acquired = reader.read(request)
        self.assertEqual(len(acquired.content_bytes), _SECURITY_MASTER_MAXIMUM_BYTES)

    def test_rejects_oversized_daily_bundle_content(self) -> None:
        target_session = date(2026, 7, 15)
        oversized = b"a" * (_DAILY_BUNDLE_MAXIMUM_BYTES + 1)
        request, fake = _daily_bundle_request(
            target_session=target_session, content_bytes=oversized
        )
        reader = GCSLandingObjectReader(fake)
        with self.assertRaises(AcquisitionError):
            reader.read(request)

    def test_rejects_non_bytes_payload_content(self) -> None:
        request, fake = _security_master_request(target_session=date(2026, 7, 15))
        fake.content_bytes = "not-bytes"  # type: ignore[assignment]
        reader = GCSLandingObjectReader(fake)
        with self.assertRaises(AcquisitionError):
            reader.read(request)

    def test_rejects_non_integer_payload_generation(self) -> None:
        request, fake = _security_master_request(target_session=date(2026, 7, 15))
        fake.generation = "100"  # type: ignore[assignment]
        reader = GCSLandingObjectReader(fake)
        with self.assertRaises(AcquisitionError):
            reader.read(request)

    def test_rejects_bool_payload_generation(self) -> None:
        target_session = date(2026, 7, 15)
        request, fake = _security_master_request(target_session=target_session, generation=1)
        fake.generation = True  # numerically equal to 1, must still be rejected
        reader = GCSLandingObjectReader(fake)
        with self.assertRaises(AcquisitionError):
            reader.read(request)


class GoogleCloudStorageObjectReaderTests(unittest.TestCase):
    """Exercises the production wrapper against a fake GCS SDK. No network, no GCP, no mocks."""

    def test_invalid_requested_generation_never_reaches_the_sdk(self) -> None:
        invalid_requested_generations = (
            True,
            False,
            0,
            -1,
            9223372036854775808,
            2**100,
            "a string",
        )
        for bad_generation in invalid_requested_generations:
            client = FakeStorageClient(observed_generation=100, content_bytes=b"abc")
            reader = GoogleCloudStorageObjectReader(client=client)
            with self.assertRaises(AcquisitionError):
                reader.read_generation(
                    bucket="b", object_name="o", generation=bad_generation, maximum_bytes=10
                )
            self.assertEqual(client.bucket_calls, [])
            self.assertEqual(client.buckets, [])

    def test_calls_bucket_and_blob_with_exact_values(self) -> None:
        client = FakeStorageClient(observed_generation=100, content_bytes=b"abc")
        reader = GoogleCloudStorageObjectReader(client=client)

        reader.read_generation(
            bucket="my-bucket", object_name="landing/2026-07-15/x", generation=100,
            maximum_bytes=10,
        )

        self.assertEqual(client.bucket_calls, ["my-bucket"])
        self.assertEqual(
            client.buckets[0].blob_calls, [("landing/2026-07-15/x", 100)]
        )

    def test_generation_is_passed_to_blob(self) -> None:
        client = FakeStorageClient(observed_generation=42, content_bytes=b"abc")
        reader = GoogleCloudStorageObjectReader(client=client)

        reader.read_generation(bucket="b", object_name="o", generation=42, maximum_bytes=10)

        self.assertEqual(client.buckets[0].blob_calls[0][1], 42)

    def test_download_as_bytes_receives_exact_end_raw_download_and_if_generation_match(
        self,
    ) -> None:
        client = FakeStorageClient(observed_generation=100, content_bytes=b"abc")
        reader = GoogleCloudStorageObjectReader(client=client)

        reader.read_generation(
            bucket="b", object_name="o", generation=100,
            maximum_bytes=_SECURITY_MASTER_MAXIMUM_BYTES,
        )

        self.assertEqual(
            client.buckets[0].blobs[0].download_calls,
            [
                {
                    "end": _SECURITY_MASTER_MAXIMUM_BYTES,
                    "raw_download": True,
                    "if_generation_match": 100,
                }
            ],
        )

    def test_raw_download_true_prevents_content_encoding_expansion(self) -> None:
        target_session = date(2026, 7, 15)
        csv_content = b"SYMBOL,SERIES,ISIN\nRELIANCE,EQ,INE002A01018\n" * 200
        stored_gzip_bytes = gzip.compress(csv_content)
        expanded_bytes = csv_content
        self.assertNotEqual(stored_gzip_bytes, expanded_bytes)

        def blob_factory(object_name: str, generation: object) -> ContentEncodingAwareFakeBlob:
            return ContentEncodingAwareFakeBlob(
                object_name,
                generation,
                observed_generation=100,
                stored_bytes=stored_gzip_bytes,
                expanded_bytes=expanded_bytes,
            )

        client = FakeStorageClient(blob_factory=blob_factory)
        gcs_reader = GoogleCloudStorageObjectReader(client=client)
        request = LandingObjectRequest(
            bucket=_BUCKET,
            object_name=f"landing/{target_session.isoformat()}/NSE_CM_security_15072026.csv.gz",
            generation=100,
            expected_sha256=hashlib.sha256(stored_gzip_bytes).hexdigest(),
            target_session=target_session,
            file_type=AcquisitionFileType.SECURITY_MASTER,
        )
        reader = GCSLandingObjectReader(gcs_reader)

        acquired = reader.read(request)

        self.assertEqual(acquired.content_bytes, stored_gzip_bytes)
        self.assertEqual(acquired.sha256_hash, hashlib.sha256(stored_gzip_bytes).hexdigest())
        self.assertNotEqual(acquired.content_bytes, expanded_bytes)
        blob = client.buckets[0].blobs[0]
        self.assertTrue(blob.download_calls[0]["raw_download"])

    def test_rejects_missing_observed_generation(self) -> None:
        client = FakeStorageClient(observed_generation=None, content_bytes=b"abc")
        reader = GoogleCloudStorageObjectReader(client=client)
        with self.assertRaises(AcquisitionError):
            reader.read_generation(bucket="b", object_name="o", generation=100, maximum_bytes=10)

    def test_never_substitutes_requested_generation_when_missing(self) -> None:
        # A buggy implementation could fall back to the requested generation
        # when the observed one is missing, which would make this silently
        # "succeed" with a spoofed generation. It must not.
        client = FakeStorageClient(observed_generation=None, content_bytes=b"abc")
        reader = GoogleCloudStorageObjectReader(client=client)
        with self.assertRaises(AcquisitionError) as ctx:
            reader.read_generation(bucket="b", object_name="o", generation=555, maximum_bytes=10)
        self.assertIn("missing", str(ctx.exception))

    def test_rejects_bool_observed_generation(self) -> None:
        client = FakeStorageClient(observed_generation=True, content_bytes=b"abc")
        reader = GoogleCloudStorageObjectReader(client=client)
        with self.assertRaises(AcquisitionError):
            # generation=1 so a naive `!=` equality check would NOT catch this;
            # only an explicit type check does (True == 1 in Python).
            reader.read_generation(bucket="b", object_name="o", generation=1, maximum_bytes=10)

    def test_rejects_string_observed_generation(self) -> None:
        client = FakeStorageClient(observed_generation="100", content_bytes=b"abc")
        reader = GoogleCloudStorageObjectReader(client=client)
        with self.assertRaises(AcquisitionError):
            reader.read_generation(bucket="b", object_name="o", generation=100, maximum_bytes=10)

    def test_rejects_mismatched_observed_generation(self) -> None:
        client = FakeStorageClient(observed_generation=999, content_bytes=b"abc")
        reader = GoogleCloudStorageObjectReader(client=client)
        with self.assertRaises(AcquisitionError):
            reader.read_generation(bucket="b", object_name="o", generation=100, maximum_bytes=10)

    def test_rejects_non_positive_observed_generation(self) -> None:
        # Requested generation must itself be valid so this isolates the
        # independent observed-generation check, not the requested-generation gate.
        client = FakeStorageClient(observed_generation=0, content_bytes=b"abc")
        reader = GoogleCloudStorageObjectReader(client=client)
        with self.assertRaises(AcquisitionError):
            reader.read_generation(bucket="b", object_name="o", generation=100, maximum_bytes=10)

    def test_accepts_observed_generation_at_int64_max(self) -> None:
        client = FakeStorageClient(observed_generation=9223372036854775807, content_bytes=b"abc")
        reader = GoogleCloudStorageObjectReader(client=client)
        payload = reader.read_generation(
            bucket="b", object_name="o", generation=9223372036854775807, maximum_bytes=10
        )
        self.assertEqual(payload.generation, 9223372036854775807)

    def test_rejects_observed_generation_above_int64_max(self) -> None:
        # Requested generation must itself be valid so this isolates the
        # independent observed-generation check, not the requested-generation gate.
        client = FakeStorageClient(observed_generation=9223372036854775808, content_bytes=b"abc")
        reader = GoogleCloudStorageObjectReader(client=client)
        with self.assertRaises(AcquisitionError):
            reader.read_generation(bucket="b", object_name="o", generation=100, maximum_bytes=10)

    def test_accepts_matching_positive_observed_generation(self) -> None:
        client = FakeStorageClient(observed_generation=100, content_bytes=b"abc")
        reader = GoogleCloudStorageObjectReader(client=client)
        payload = reader.read_generation(
            bucket="b", object_name="o", generation=100, maximum_bytes=10
        )
        self.assertEqual(payload.generation, 100)
        self.assertEqual(payload.content_bytes, b"abc")

    def test_has_no_listing_capability(self) -> None:
        client = FakeStorageClient(observed_generation=100, content_bytes=b"abc")
        bucket = client.bucket("b")
        blob = bucket.blob("o", 100)
        for candidate in (client, bucket, blob):
            self.assertFalse(hasattr(candidate, "list_blobs"))
            self.assertFalse(hasattr(candidate, "list"))


class EndToEndFileTypeLimitTests(unittest.TestCase):
    """GCSLandingObjectReader + GoogleCloudStorageObjectReader wired together."""

    def test_security_master_uses_32_mib_limit_and_ranged_read(self) -> None:
        target_session = date(2026, 7, 15)
        content_bytes = b"mock-master-content"
        client = FakeStorageClient(observed_generation=100, content_bytes=content_bytes)
        gcs_reader = GoogleCloudStorageObjectReader(client=client)
        request = LandingObjectRequest(
            bucket=_BUCKET,
            object_name=f"landing/{target_session.isoformat()}/NSE_CM_security_15072026.csv.gz",
            generation=100,
            expected_sha256=hashlib.sha256(content_bytes).hexdigest(),
            target_session=target_session,
            file_type=AcquisitionFileType.SECURITY_MASTER,
        )
        reader = GCSLandingObjectReader(gcs_reader)

        acquired = reader.read(request)

        self.assertEqual(acquired.content_bytes, content_bytes)
        blob = client.buckets[0].blobs[0]
        self.assertEqual(blob.download_calls[0]["end"], _SECURITY_MASTER_MAXIMUM_BYTES)
        self.assertEqual(blob.download_calls[0]["if_generation_match"], 100)

    def test_daily_bundle_uses_128_mib_limit_and_ranged_read(self) -> None:
        target_session = date(2026, 7, 15)
        content_bytes = b"mock-bundle-content"
        client = FakeStorageClient(observed_generation=200, content_bytes=content_bytes)
        gcs_reader = GoogleCloudStorageObjectReader(client=client)
        request = LandingObjectRequest(
            bucket=_BUCKET,
            object_name=f"landing/{target_session.isoformat()}/Reports-Daily-Multiple.zip",
            generation=200,
            expected_sha256=hashlib.sha256(content_bytes).hexdigest(),
            target_session=target_session,
            file_type=AcquisitionFileType.DAILY_BUNDLE,
        )
        reader = GCSLandingObjectReader(gcs_reader)

        acquired = reader.read(request)

        self.assertEqual(acquired.content_bytes, content_bytes)
        blob = client.buckets[0].blobs[0]
        self.assertEqual(blob.download_calls[0]["end"], _DAILY_BUNDLE_MAXIMUM_BYTES)
        self.assertEqual(blob.download_calls[0]["if_generation_match"], 200)

    def test_oversized_object_is_rejected_via_ranged_read_not_full_download(self) -> None:
        target_session = date(2026, 7, 15)
        real_size = _SECURITY_MASTER_MAXIMUM_BYTES + (8 * 1024 * 1024)
        content_bytes = b"a" * real_size
        client = FakeStorageClient(observed_generation=100, content_bytes=content_bytes)
        gcs_reader = GoogleCloudStorageObjectReader(client=client)
        request = LandingObjectRequest(
            bucket=_BUCKET,
            object_name=f"landing/{target_session.isoformat()}/NSE_CM_security_15072026.csv.gz",
            generation=100,
            expected_sha256=hashlib.sha256(content_bytes).hexdigest(),
            target_session=target_session,
            file_type=AcquisitionFileType.SECURITY_MASTER,
        )
        reader = GCSLandingObjectReader(gcs_reader)

        with self.assertRaises(AcquisitionError):
            reader.read(request)

        blob = client.buckets[0].blobs[0]
        self.assertEqual(len(blob.download_calls), 1)
        self.assertEqual(blob.download_calls[0]["end"], _SECURITY_MASTER_MAXIMUM_BYTES)

    def test_no_listing_operation_exists_anywhere_in_the_stack(self) -> None:
        for candidate in (
            GCSObjectReader,
            GoogleCloudStorageObjectReader,
            GCSLandingObjectReader,
        ):
            members = [name for name in dir(candidate) if not name.startswith("__")]
            self.assertFalse(
                any("list" in name.lower() for name in members),
                f"{candidate!r} unexpectedly exposes a listing-shaped member",
            )


if __name__ == "__main__":
    unittest.main()
