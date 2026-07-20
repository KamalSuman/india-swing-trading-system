from __future__ import annotations

import ast
import hashlib
import json
import tempfile
import unittest
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

from india_swing.calendar_data.artifact_store import LocalCalendarSourceArtifactStore
from india_swing.calendar_data.materialization import (
    CollectionCalendarMaterialization,
    materialize_collection_calendar,
)
from india_swing.calendar_data.materialization_codec import (
    MAXIMUM_CALENDAR_MATERIALIZATION_BYTES,
    encode_calendar_materialization,
)
from india_swing.calendar_data.models import CALENDAR_DECLARATION_SCHEMA_VERSION
from india_swing.daily_pipeline.acquisition import GCSObjectPayload, GCSObjectReader
from india_swing.daily_pipeline.calendar_materialization_acquisition import (
    AcquiredCalendarMaterialization,
    CalendarMaterializationAcquisitionError,
    CalendarMaterializationObjectRequest,
    acquire_calendar_materialization,
)
from india_swing.reference.models import ReferenceReadiness


UTC = timezone.utc
_BUCKET = "trusted-calendar-materialization-bucket"

# Reproduced deterministic fixture patterns (per architecture_contract's own
# "reuse existing deterministic calendar fixture patterns"), matching
# tests/test_calendar_materialization_codec.py's event-declaration builders.


def _window(phase: str, opens: str, closes: str) -> dict[str, str]:
    return {"phase": phase, "opens": opens, "closes": closes}


def _locator(record: str) -> dict[str, object]:
    return {"page": 1, "section": "CM schedule", "record": record}


def _base_event(
    *,
    effective_from: str = "2026-01-01",
    effective_to_exclusive: str = "2027-01-01",
) -> dict[str, object]:
    return {
        "event_type": "BASE_WEEKLY_SCHEDULE",
        "effective_from": effective_from,
        "effective_to_exclusive": effective_to_exclusive,
        "weekdays": ["MON", "TUE", "WED", "THU", "FRI"],
        "windows": [_window("LIVE_CONTINUOUS", "09:15:00", "15:30:00")],
        "supersedes_event_ids": [],
        "source_locator": _locator("regular"),
        "reason": "Regular capital-market schedule",
    }


class FakeGCSObjectReader:
    """Fake GCSObjectReader. Never contacts GCP; records every call made."""

    def __init__(self, *, generation: object, content_bytes: object) -> None:
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


class _RaisingReader:
    def __init__(self, error: BaseException) -> None:
        self._error = error

    def read_generation(self, **_kwargs: object) -> GCSObjectPayload:
        raise self._error


class _FixtureTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.sequence = 0

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def import_source(
        self,
        document_id: str,
        events: list[dict[str, object]],
        *,
        validated_at: datetime,
    ):
        self.sequence += 1
        source_name = f"{document_id}-{self.sequence}.pdf"
        declaration_name = f"{document_id}-{self.sequence}.events.json"
        source_bytes = f"%PDF-1.7\n{document_id}-{self.sequence}\n%%EOF\n".encode("ascii")
        input_root = self.root / "inputs"
        input_root.mkdir(exist_ok=True)
        source_path = input_root / source_name
        declaration_path = input_root / declaration_name
        source_path.write_bytes(source_bytes)
        declaration = {
            "schema_version": CALENDAR_DECLARATION_SCHEMA_VERSION,
            "exchange": "NSE",
            "segment": "CM",
            "claimed_authority": "NSE",
            "claimed_document_id": document_id,
            "claimed_issue_date": "2026-01-01",
            "claimed_source_url": f"https://example.invalid/{source_name}",
            "source_filename": source_name,
            "source_media_type": "application/pdf",
            "source_byte_count": len(source_bytes),
            "source_sha256": hashlib.sha256(source_bytes).hexdigest(),
            "events": events,
        }
        declaration_path.write_text(
            json.dumps(declaration, separators=(",", ":")), encoding="utf-8"
        )
        values = iter((validated_at - timedelta(seconds=1), validated_at))
        return LocalCalendarSourceArtifactStore(
            self.root / "archive",
            clock=lambda: next(values),
        ).import_source(source_path, declaration_path)

    def schedule_only_materialization(self) -> CollectionCalendarMaterialization:
        base = self.import_source(
            "CMTR-ACQ-BASE",
            [_base_event()],
            validated_at=datetime(2026, 7, 13, 12, 0, tzinfo=UTC),
        )
        return materialize_collection_calendar(
            sources=(base,),
            coverage_start=date(2026, 7, 13),
            coverage_end=date(2026, 7, 14),
            cutoff=datetime(2026, 7, 14, 16, 0, tzinfo=UTC),
        )

    def other_schedule_only_materialization(self) -> CollectionCalendarMaterialization:
        base = self.import_source(
            "CMTR-ACQ-OTHER",
            [_base_event()],
            validated_at=datetime(2026, 7, 15, 12, 0, tzinfo=UTC),
        )
        return materialize_collection_calendar(
            sources=(base,),
            coverage_start=date(2026, 7, 15),
            coverage_end=date(2026, 7, 16),
            cutoff=datetime(2026, 7, 16, 16, 0, tzinfo=UTC),
        )

    def valid_request_and_payload(
        self, *, generation: int = 777, bucket: str = _BUCKET
    ) -> tuple[CalendarMaterializationObjectRequest, bytes, CollectionCalendarMaterialization]:
        materialization = self.schedule_only_materialization()
        payload = encode_calendar_materialization(materialization)
        request = CalendarMaterializationObjectRequest(
            bucket=bucket,
            object_name=(
                f"calendar-materializations/{materialization.materialization_id}"
                "/materialization.json"
            ),
            generation=generation,
            expected_sha256=hashlib.sha256(payload).hexdigest(),
            materialization_id=materialization.materialization_id,
        )
        return request, payload, materialization


class RequestConstructionTests(unittest.TestCase):
    def _valid_kwargs(self, **overrides: object) -> dict[str, object]:
        materialization_id = "a" * 64
        base: dict[str, object] = dict(
            bucket=_BUCKET,
            object_name=f"calendar-materializations/{materialization_id}/materialization.json",
            generation=777,
            expected_sha256="b" * 64,
            materialization_id=materialization_id,
        )
        base.update(overrides)
        return base

    def test_accepts_valid_request(self) -> None:
        request = CalendarMaterializationObjectRequest(**self._valid_kwargs())
        self.assertEqual(request.generation, 777)

    def test_rejects_invalid_bucket_name(self) -> None:
        with self.assertRaises(CalendarMaterializationAcquisitionError):
            CalendarMaterializationObjectRequest(**self._valid_kwargs(bucket="A"))

    def test_rejects_bool_generation(self) -> None:
        with self.assertRaises(CalendarMaterializationAcquisitionError):
            CalendarMaterializationObjectRequest(**self._valid_kwargs(generation=True))

    def test_rejects_non_positive_generation(self) -> None:
        with self.assertRaises(CalendarMaterializationAcquisitionError):
            CalendarMaterializationObjectRequest(**self._valid_kwargs(generation=0))

    def test_rejects_generation_above_int64_max(self) -> None:
        with self.assertRaises(CalendarMaterializationAcquisitionError):
            CalendarMaterializationObjectRequest(
                **self._valid_kwargs(generation=9223372036854775808)
            )

    def test_accepts_generation_at_int64_max(self) -> None:
        request = CalendarMaterializationObjectRequest(
            **self._valid_kwargs(generation=9223372036854775807)
        )
        self.assertEqual(request.generation, 9223372036854775807)

    def test_rejects_malformed_expected_sha256(self) -> None:
        with self.assertRaises(CalendarMaterializationAcquisitionError):
            CalendarMaterializationObjectRequest(
                **self._valid_kwargs(expected_sha256="B" * 64)
            )

    def test_rejects_malformed_materialization_id(self) -> None:
        with self.assertRaises(CalendarMaterializationAcquisitionError):
            CalendarMaterializationObjectRequest(
                **self._valid_kwargs(materialization_id="not-hex")
            )

    def test_rejects_object_name_with_wrong_prefix(self) -> None:
        materialization_id = "a" * 64
        with self.assertRaises(CalendarMaterializationAcquisitionError):
            CalendarMaterializationObjectRequest(
                **self._valid_kwargs(
                    materialization_id=materialization_id,
                    object_name=f"other-prefix/{materialization_id}/materialization.json",
                )
            )

    def test_rejects_path_traversal_object_name(self) -> None:
        materialization_id = "a" * 64
        with self.assertRaises(CalendarMaterializationAcquisitionError):
            CalendarMaterializationObjectRequest(
                **self._valid_kwargs(
                    materialization_id=materialization_id,
                    object_name=(
                        f"calendar-materializations/{materialization_id}/"
                        "../materialization.json"
                    ),
                )
            )

    def test_rejects_object_name_bound_to_a_different_materialization_id(self) -> None:
        with self.assertRaises(CalendarMaterializationAcquisitionError):
            CalendarMaterializationObjectRequest(
                **self._valid_kwargs(
                    materialization_id="a" * 64,
                    object_name=f"calendar-materializations/{'c' * 64}/materialization.json",
                )
            )

    def test_rejects_non_string_object_name(self) -> None:
        with self.assertRaises(CalendarMaterializationAcquisitionError):
            CalendarMaterializationObjectRequest(**self._valid_kwargs(object_name=12345))


class AcquisitionAcceptanceTests(_FixtureTestCase):
    def test_successful_read_returns_verified_lineage(self) -> None:
        request, payload, materialization = self.valid_request_and_payload()
        fake = FakeGCSObjectReader(generation=request.generation, content_bytes=payload)

        acquired = acquire_calendar_materialization(request, reader=fake)

        self.assertIsInstance(acquired, AcquiredCalendarMaterialization)
        self.assertEqual(
            fake.calls,
            [
                {
                    "bucket": request.bucket,
                    "object_name": request.object_name,
                    "generation": request.generation,
                    "maximum_bytes": MAXIMUM_CALENDAR_MATERIALIZATION_BYTES,
                }
            ],
        )
        self.assertEqual(acquired.request, request)
        self.assertEqual(acquired.observed_generation, request.generation)
        self.assertEqual(acquired.observed_sha256, request.expected_sha256)
        self.assertEqual(acquired.materialization, materialization)
        self.assertEqual(acquired.materialization.materialization_id, request.materialization_id)

    def test_makes_exactly_one_call_and_reader_exposes_no_listing(self) -> None:
        request, payload, _ = self.valid_request_and_payload()
        fake = FakeGCSObjectReader(generation=request.generation, content_bytes=payload)

        acquire_calendar_materialization(request, reader=fake)

        self.assertEqual(len(fake.calls), 1)
        self.assertFalse(hasattr(fake, "list_blobs"))
        self.assertFalse(hasattr(fake, "list_blobs_generic"))

    def test_gcs_object_reader_protocol_exposes_only_read_generation(self) -> None:
        members = [name for name in dir(GCSObjectReader) if not name.startswith("_")]
        self.assertEqual(members, ["read_generation"])


class PreReadRejectionTests(_FixtureTestCase):
    def test_rejects_wrong_request_type(self) -> None:
        fake = FakeGCSObjectReader(generation=1, content_bytes=b"x")
        with self.assertRaises(CalendarMaterializationAcquisitionError):
            acquire_calendar_materialization("not-a-request", reader=fake)  # type: ignore[arg-type]
        self.assertEqual(fake.calls, [])

    def test_rejects_request_subclass(self) -> None:
        request, payload, _ = self.valid_request_and_payload()

        class _RequestSubclass(CalendarMaterializationObjectRequest):
            pass

        subclass_instance = _RequestSubclass(
            bucket=request.bucket,
            object_name=request.object_name,
            generation=request.generation,
            expected_sha256=request.expected_sha256,
            materialization_id=request.materialization_id,
        )
        fake = FakeGCSObjectReader(generation=request.generation, content_bytes=payload)
        with self.assertRaises(CalendarMaterializationAcquisitionError):
            acquire_calendar_materialization(subclass_instance, reader=fake)
        self.assertEqual(fake.calls, [])

    def test_rejects_post_construction_mutated_request(self) -> None:
        request, payload, _ = self.valid_request_and_payload()
        object.__setattr__(request, "materialization_id", "f" * 64)
        fake = FakeGCSObjectReader(generation=request.generation, content_bytes=payload)
        with self.assertRaises(CalendarMaterializationAcquisitionError):
            acquire_calendar_materialization(request, reader=fake)
        self.assertEqual(fake.calls, [])


class PostReadRejectionTests(_FixtureTestCase):
    def test_rejects_wrong_payload_type(self) -> None:
        request, _payload, _ = self.valid_request_and_payload()

        class _BadReader:
            def read_generation(self, **_kwargs: object) -> str:
                return "not-a-payload"

        with self.assertRaises(CalendarMaterializationAcquisitionError):
            acquire_calendar_materialization(request, reader=_BadReader())

    def test_rejects_non_integer_payload_generation(self) -> None:
        request, payload, _ = self.valid_request_and_payload()
        fake = FakeGCSObjectReader(generation="777", content_bytes=payload)
        with self.assertRaises(CalendarMaterializationAcquisitionError):
            acquire_calendar_materialization(request, reader=fake)

    def test_rejects_bool_payload_generation(self) -> None:
        request, payload, _ = self.valid_request_and_payload(generation=1)
        fake = FakeGCSObjectReader(generation=True, content_bytes=payload)
        with self.assertRaises(CalendarMaterializationAcquisitionError):
            acquire_calendar_materialization(request, reader=fake)

    def test_rejects_mismatched_generation(self) -> None:
        request, payload, _ = self.valid_request_and_payload()
        fake = FakeGCSObjectReader(generation=request.generation + 1, content_bytes=payload)
        with self.assertRaises(CalendarMaterializationAcquisitionError):
            acquire_calendar_materialization(request, reader=fake)

    def test_rejects_non_bytes_content(self) -> None:
        request, _payload, _ = self.valid_request_and_payload()
        fake = FakeGCSObjectReader(generation=request.generation, content_bytes="not-bytes")
        with self.assertRaises(CalendarMaterializationAcquisitionError):
            acquire_calendar_materialization(request, reader=fake)

    def test_rejects_empty_content(self) -> None:
        request, _payload, _ = self.valid_request_and_payload()
        fake = FakeGCSObjectReader(generation=request.generation, content_bytes=b"")
        with self.assertRaises(CalendarMaterializationAcquisitionError):
            acquire_calendar_materialization(request, reader=fake)

    def test_rejects_over_limit_content_via_patched_ceiling(self) -> None:
        request, payload, _ = self.valid_request_and_payload()
        fake = FakeGCSObjectReader(generation=request.generation, content_bytes=payload)
        with patch(
            "india_swing.daily_pipeline.calendar_materialization_acquisition."
            "MAXIMUM_CALENDAR_MATERIALIZATION_BYTES",
            len(payload) - 1,
        ):
            with self.assertRaises(CalendarMaterializationAcquisitionError):
                acquire_calendar_materialization(request, reader=fake)

    def test_accepts_content_exactly_at_the_patched_ceiling(self) -> None:
        request, payload, _ = self.valid_request_and_payload()
        fake = FakeGCSObjectReader(generation=request.generation, content_bytes=payload)
        with patch(
            "india_swing.daily_pipeline.calendar_materialization_acquisition."
            "MAXIMUM_CALENDAR_MATERIALIZATION_BYTES",
            len(payload),
        ):
            acquire_calendar_materialization(request, reader=fake)

    def test_rejects_stored_byte_hash_mismatch(self) -> None:
        request, payload, _ = self.valid_request_and_payload()
        fake = FakeGCSObjectReader(
            generation=request.generation, content_bytes=payload + b"tampered"
        )
        with self.assertRaises(CalendarMaterializationAcquisitionError):
            acquire_calendar_materialization(request, reader=fake)

    def test_rejects_malformed_decoder_bytes(self) -> None:
        materialization = self.schedule_only_materialization()
        malformed = b"not-canonical-json-bytes"
        request = CalendarMaterializationObjectRequest(
            bucket=_BUCKET,
            object_name=(
                f"calendar-materializations/{materialization.materialization_id}"
                "/materialization.json"
            ),
            generation=777,
            expected_sha256=hashlib.sha256(malformed).hexdigest(),
            materialization_id=materialization.materialization_id,
        )
        fake = FakeGCSObjectReader(generation=777, content_bytes=malformed)
        with self.assertRaises(CalendarMaterializationAcquisitionError):
            acquire_calendar_materialization(request, reader=fake)

    def test_rejects_decoded_materialization_id_mismatch(self) -> None:
        served = self.other_schedule_only_materialization()
        served_payload = encode_calendar_materialization(served)
        claimed = self.schedule_only_materialization()
        request = CalendarMaterializationObjectRequest(
            bucket=_BUCKET,
            object_name=(
                f"calendar-materializations/{claimed.materialization_id}/materialization.json"
            ),
            generation=777,
            expected_sha256=hashlib.sha256(served_payload).hexdigest(),
            materialization_id=claimed.materialization_id,
        )
        fake = FakeGCSObjectReader(generation=777, content_bytes=served_payload)
        with self.assertRaises(CalendarMaterializationAcquisitionError):
            acquire_calendar_materialization(request, reader=fake)
        self.assertEqual(len(fake.calls), 1)


class AcquiredValueDirectConstructionTests(_FixtureTestCase):
    def test_valid_construction_succeeds(self) -> None:
        request, _payload, materialization = self.valid_request_and_payload()
        acquired = AcquiredCalendarMaterialization(
            request=request,
            observed_generation=request.generation,
            observed_sha256=request.expected_sha256,
            materialization=materialization,
        )
        self.assertEqual(acquired.materialization, materialization)

    def test_rejects_mismatched_observed_generation(self) -> None:
        request, _payload, materialization = self.valid_request_and_payload()
        with self.assertRaises(CalendarMaterializationAcquisitionError):
            AcquiredCalendarMaterialization(
                request=request,
                observed_generation=request.generation + 1,
                observed_sha256=request.expected_sha256,
                materialization=materialization,
            )

    def test_rejects_mismatched_observed_sha256(self) -> None:
        request, _payload, materialization = self.valid_request_and_payload()
        with self.assertRaises(CalendarMaterializationAcquisitionError):
            AcquiredCalendarMaterialization(
                request=request,
                observed_generation=request.generation,
                observed_sha256="f" * 64,
                materialization=materialization,
            )

    def test_rejects_fabricated_hash_pair_matching_each_other_but_not_the_materialization(
        self,
    ) -> None:
        # Regression for Codex's INVALID_DIRECT_CONSTRUCTION_ACCEPTED: a
        # valid materialization paired with a fabricated all-zero hash that
        # request.expected_sha256 and observed_sha256 both happen to agree
        # on, but which is not SHA-256(encode_calendar_materialization(
        # materialization)). Agreement between the two caller-supplied
        # values alone must not be sufficient.
        request, _payload, materialization = self.valid_request_and_payload()
        fabricated_hash = "0" * 64
        self.assertNotEqual(fabricated_hash, request.expected_sha256)
        fabricated_request = CalendarMaterializationObjectRequest(
            bucket=request.bucket,
            object_name=request.object_name,
            generation=request.generation,
            expected_sha256=fabricated_hash,
            materialization_id=request.materialization_id,
        )
        with self.assertRaises(CalendarMaterializationAcquisitionError):
            AcquiredCalendarMaterialization(
                request=fabricated_request,
                observed_generation=fabricated_request.generation,
                observed_sha256=fabricated_hash,
                materialization=materialization,
            )

    def test_rejects_mutated_materialization_readiness(self) -> None:
        request, _payload, materialization = self.valid_request_and_payload()
        object.__setattr__(materialization, "readiness", ReferenceReadiness.SYNTHETIC_TEST)
        with self.assertRaises(CalendarMaterializationAcquisitionError):
            AcquiredCalendarMaterialization(
                request=request,
                observed_generation=request.generation,
                observed_sha256=request.expected_sha256,
                materialization=materialization,
            )

    def test_rejects_readiness_mutated_to_raw_string_even_with_recomputed_identity(
        self,
    ) -> None:
        # Mutating readiness to the raw string "COLLECTION_ONLY" (not the
        # real enum member) and then recomputing/overwriting
        # materialization_id -- and the request's own materialization_id/
        # object_name to match -- makes both verify_content_identity() and
        # the request/materialization ID cross-check pass, isolating the
        # explicit `is ReferenceReadiness.COLLECTION_ONLY` identity check
        # (not equality, and not reliance on content identity alone) as
        # what actually rejects this.
        request, _payload, materialization = self.valid_request_and_payload()
        object.__setattr__(materialization, "readiness", "COLLECTION_ONLY")
        recomputed_id = materialization._calculated_materialization_id()
        object.__setattr__(materialization, "materialization_id", recomputed_id)
        materialization.verify_content_identity()  # passes despite the mutation

        matching_request = CalendarMaterializationObjectRequest(
            bucket=request.bucket,
            object_name=f"calendar-materializations/{recomputed_id}/materialization.json",
            generation=request.generation,
            expected_sha256=request.expected_sha256,
            materialization_id=recomputed_id,
        )
        with self.assertRaises(CalendarMaterializationAcquisitionError):
            AcquiredCalendarMaterialization(
                request=matching_request,
                observed_generation=matching_request.generation,
                observed_sha256=matching_request.expected_sha256,
                materialization=materialization,
            )

    def test_rejects_mutated_materialization_actionable(self) -> None:
        request, _payload, materialization = self.valid_request_and_payload()
        object.__setattr__(materialization, "actionable", True)
        with self.assertRaises(CalendarMaterializationAcquisitionError):
            AcquiredCalendarMaterialization(
                request=request,
                observed_generation=request.generation,
                observed_sha256=request.expected_sha256,
                materialization=materialization,
            )

    def test_rejects_wrong_materialization_type(self) -> None:
        request, _payload, _materialization = self.valid_request_and_payload()
        with self.assertRaises(CalendarMaterializationAcquisitionError):
            AcquiredCalendarMaterialization(
                request=request,
                observed_generation=request.generation,
                observed_sha256=request.expected_sha256,
                materialization="not-a-materialization",  # type: ignore[arg-type]
            )


class OrderingAndSanitizationTests(_FixtureTestCase):
    def test_hash_verification_precedes_decoding(self) -> None:
        request, payload, _ = self.valid_request_and_payload()
        fake = FakeGCSObjectReader(
            generation=request.generation, content_bytes=payload + b"x"
        )
        with patch(
            "india_swing.daily_pipeline.calendar_materialization_acquisition."
            "decode_calendar_materialization"
        ) as decode_spy:
            with self.assertRaises(CalendarMaterializationAcquisitionError):
                acquire_calendar_materialization(request, reader=fake)
        decode_spy.assert_not_called()

    def test_failure_never_triggers_a_second_read(self) -> None:
        request, _payload, _ = self.valid_request_and_payload()
        fake = FakeGCSObjectReader(generation=request.generation, content_bytes=b"")
        with self.assertRaises(CalendarMaterializationAcquisitionError):
            acquire_calendar_materialization(request, reader=fake)
        self.assertEqual(len(fake.calls), 1)

    def test_base_exception_is_not_intercepted(self) -> None:
        request, _payload, _ = self.valid_request_and_payload()
        with self.assertRaises(KeyboardInterrupt):
            acquire_calendar_materialization(request, reader=_RaisingReader(KeyboardInterrupt()))

    def test_secret_bearing_reader_exception_never_leaks(self) -> None:
        request, _payload, _ = self.valid_request_and_payload()
        secret = "SECRET-READER-FAILURE-DO-NOT-LEAK-9f3c"

        class _DistinctiveReaderFailure(RuntimeError):
            pass

        error = _DistinctiveReaderFailure(f"failure fetching {request.object_name}: {secret}")
        with self.assertRaises(CalendarMaterializationAcquisitionError) as ctx:
            acquire_calendar_materialization(request, reader=_RaisingReader(error))
        self.assertNotIn(secret, str(ctx.exception))
        self.assertNotIn(secret, repr(ctx.exception))
        self.assertNotIn("_DistinctiveReaderFailure", str(ctx.exception))
        self.assertNotIn("_DistinctiveReaderFailure", repr(ctx.exception))
        self.assertNotIn("RuntimeError", str(ctx.exception))
        self.assertIsNone(ctx.exception.__cause__)
        self.assertIsNone(ctx.exception.__context__)
        self.assertIsNot(ctx.exception, error)

    def test_secret_bearing_decoder_exception_never_leaks(self) -> None:
        request, payload, _ = self.valid_request_and_payload()
        secret = "SECRET-DECODE-FAILURE-DO-NOT-LEAK-7c3e"

        class _DistinctiveDecoderFailure(ValueError):
            pass

        fake = FakeGCSObjectReader(generation=request.generation, content_bytes=payload)

        def _raise(_bytes: bytes) -> None:
            raise _DistinctiveDecoderFailure(f"malformed near {secret}")

        with patch(
            "india_swing.daily_pipeline.calendar_materialization_acquisition."
            "decode_calendar_materialization",
            side_effect=_raise,
        ):
            with self.assertRaises(CalendarMaterializationAcquisitionError) as ctx:
                acquire_calendar_materialization(request, reader=fake)
        self.assertNotIn(secret, str(ctx.exception))
        self.assertNotIn(secret, repr(ctx.exception))
        self.assertNotIn("_DistinctiveDecoderFailure", str(ctx.exception))
        self.assertNotIn("_DistinctiveDecoderFailure", repr(ctx.exception))
        self.assertIsNone(ctx.exception.__cause__)
        self.assertIsNone(ctx.exception.__context__)

    def test_request_fields_never_leak_in_a_rejection(self) -> None:
        distinctive_bucket = "distinctive-bucket-do-not-leak-9a1b"
        request, _payload, _ = self.valid_request_and_payload(bucket=distinctive_bucket)
        fake = FakeGCSObjectReader(generation=request.generation, content_bytes=b"")
        with self.assertRaises(CalendarMaterializationAcquisitionError) as ctx:
            acquire_calendar_materialization(request, reader=fake)
        self.assertNotIn(distinctive_bucket, str(ctx.exception))
        self.assertNotIn(request.materialization_id, str(ctx.exception))
        self.assertNotIn(request.expected_sha256, str(ctx.exception))

    def test_reader_injected_same_class_exception_is_not_bare_reraised(self) -> None:
        # Regression for Codex's PUBLIC-ERROR-INJECTION-LEAK: an untrusted
        # reader raising CalendarMaterializationAcquisitionError itself
        # (not merely some other ordinary Exception) must not have that
        # exact object or its attacker-controlled message escape unchanged
        # -- there is no same-class bare-reraise privilege.
        request, _payload, _ = self.valid_request_and_payload()
        secret = "SECRET-INJECTED-READER-ERROR-DO-NOT-LEAK-2b6f"
        injected = CalendarMaterializationAcquisitionError(secret)
        with self.assertRaises(CalendarMaterializationAcquisitionError) as ctx:
            acquire_calendar_materialization(request, reader=_RaisingReader(injected))
        self.assertIsNot(ctx.exception, injected)
        self.assertNotIn(secret, str(ctx.exception))
        self.assertNotIn(secret, repr(ctx.exception))
        self.assertEqual(
            str(ctx.exception),
            "calendar materialization acquisition failed generation-pinned "
            "read or verification",
        )
        self.assertIsNone(ctx.exception.__cause__)
        self.assertIsNone(ctx.exception.__context__)

    def test_decoder_injected_same_class_exception_is_not_bare_reraised(self) -> None:
        # Same regression via the decoder call site, proving the same
        # unified except-and-discard control path is not bypassed there
        # either.
        request, payload, _ = self.valid_request_and_payload()
        secret = "SECRET-INJECTED-DECODER-ERROR-DO-NOT-LEAK-9d4a"
        fake = FakeGCSObjectReader(generation=request.generation, content_bytes=payload)

        def _raise(_bytes: bytes) -> None:
            raise CalendarMaterializationAcquisitionError(secret)

        with patch(
            "india_swing.daily_pipeline.calendar_materialization_acquisition."
            "decode_calendar_materialization",
            side_effect=_raise,
        ):
            with self.assertRaises(CalendarMaterializationAcquisitionError) as ctx:
                acquire_calendar_materialization(request, reader=fake)
        self.assertNotIn(secret, str(ctx.exception))
        self.assertNotIn(secret, repr(ctx.exception))
        self.assertEqual(
            str(ctx.exception),
            "calendar materialization acquisition failed generation-pinned "
            "read or verification",
        )
        self.assertIsNone(ctx.exception.__cause__)
        self.assertIsNone(ctx.exception.__context__)


_EXACT_ALLOWED_ACQUISITION_IMPORTS = frozenset(
    (
        # (level, module, imported name, asname). Closed set matching the
        # architecture_contract's own "may import only hashlib, dataclass,
        # the committed calendar materialization/codec types including
        # encode_calendar_materialization, ReferenceReadiness from
        # india_swing.reference.models, and the existing injected
        # GCSObjectPayload/GCSObjectReader protocol."
        (0, "__future__", "annotations", None),
        (0, "hashlib", None, None),
        (0, "dataclasses", "dataclass", None),
        (0, "india_swing.calendar_data.materialization", "CollectionCalendarMaterialization", None),
        (
            0,
            "india_swing.calendar_data.materialization_codec",
            "MAXIMUM_CALENDAR_MATERIALIZATION_BYTES",
            None,
        ),
        (
            0,
            "india_swing.calendar_data.materialization_codec",
            "decode_calendar_materialization",
            None,
        ),
        (
            0,
            "india_swing.calendar_data.materialization_codec",
            "encode_calendar_materialization",
            None,
        ),
        (0, "india_swing.reference.models", "ReferenceReadiness", None),
        (1, "acquisition", "GCSObjectPayload", None),
        (1, "acquisition", "GCSObjectReader", None),
    )
)

_EXACT_ALLOWED_ACQUISITION_CALL_TARGETS = frozenset(
    (
        "dataclass",
        "frozenset",
        "type",
        "len",
        "all",
        "_is_valid_bucket_name",
        "_is_lowercase_hex64",
        "_expected_object_name",
        "CalendarMaterializationAcquisitionError",
        "CalendarMaterializationObjectRequest",
        "AcquiredCalendarMaterialization",
        "verify_content_identity",
        "__setattr__",
        "read_generation",
        "sha256",
        "hexdigest",
        "decode_calendar_materialization",
        "encode_calendar_materialization",
    )
)

_FORBIDDEN_ACQUISITION_NAME_TOKENS = (
    "path",
    "open",
    "filesystem",
    "environ",
    "getenv",
    "now",
    "utcnow",
    "today",
    "google",
    "storage",
    "client",
    "list",
    "latest",
    "retry",
    "fallback",
    "subprocess",
    "notify",
    "notification",
    "broker",
    "order",
    "strategy",
    "model",
    "scheduler",
    "deploy",
    "logger",
    "logging",
    "print",
    "input",
    "socket",
    "requests",
    "urllib",
    "http",
)


class CapabilityLockTests(unittest.TestCase):
    """Proves calendar_materialization_acquisition.py introduces no
    filesystem, environment, current-clock, network/storage client,
    listing/latest, retry/fallback, subprocess, logger, CLI, broker,
    notification, strategy, model, or LLM capability. Imports and the
    callable surface are both locked to exact closed sets.
    """

    def _module_ast(self) -> ast.Module:
        source = (
            Path(__file__).resolve().parent.parent
            / "src"
            / "india_swing"
            / "daily_pipeline"
            / "calendar_materialization_acquisition.py"
        ).read_text(encoding="utf-8")
        return ast.parse(source)

    def test_imports_match_an_exact_allowlist(self) -> None:
        tree = self._module_ast()
        actual: set[tuple[int, str, str | None, str | None]] = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    actual.add((0, alias.name, None, alias.asname))
            elif isinstance(node, ast.ImportFrom):
                module = node.module or ""
                level = node.level or 0
                for alias in node.names:
                    actual.add((level, module, alias.name, alias.asname))
        self.assertEqual(actual, _EXACT_ALLOWED_ACQUISITION_IMPORTS)

    def test_callable_surface_is_locked_to_an_exact_allowlist(self) -> None:
        tree = self._module_ast()
        offenders = []
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            func = node.func
            if isinstance(func, ast.Name):
                target = func.id
            elif isinstance(func, ast.Attribute):
                target = func.attr
            else:
                offenders.append(ast.dump(func))
                continue
            if target not in _EXACT_ALLOWED_ACQUISITION_CALL_TARGETS:
                offenders.append(target)
        self.assertEqual(offenders, [])

    def test_identifiers_carry_no_disallowed_capability_token(self) -> None:
        tree = self._module_ast()
        offenders = []
        for node in ast.walk(tree):
            if isinstance(node, ast.Name):
                candidate = node.id.lower()
            elif isinstance(node, ast.Attribute):
                candidate = node.attr.lower()
            else:
                continue
            for token in _FORBIDDEN_ACQUISITION_NAME_TOKENS:
                if token in candidate:
                    offenders.append(candidate)
        self.assertEqual(offenders, [])


if __name__ == "__main__":
    unittest.main()
