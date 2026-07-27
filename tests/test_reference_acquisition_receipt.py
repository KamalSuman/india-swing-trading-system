from __future__ import annotations

import dataclasses
import hashlib
import json
import unittest
from datetime import date, datetime, timedelta, timezone

from india_swing.daily_pipeline.acquisition import AcquisitionFileType, LandingObjectRequest
from india_swing.reference_data.acquisition_receipt import (
    ReferenceAcquisitionReceiptError,
    ReferenceAcquisitionReceiptVerifier,
    TrustedReferenceAcquisitionBinding,
    VerifiedReferenceAcquisitionReceipt,
)

_REPORT_DATE = date(2026, 7, 16)
_BUCKET = "trusted-bucket"
_ACQUIRER_ID = "a" * 64
_RAW_SHA256 = hashlib.sha256(b"raw-security-master-content").hexdigest()
_NOT_BEFORE = datetime(2026, 7, 16, 0, 0, 0, tzinfo=timezone.utc)
_CUTOFF = datetime(2026, 7, 16, 23, 59, 59, tzinfo=timezone.utc)
_ACQUIRED_AT = "2026-07-16T13:30:00Z"


def _object_name(report_date: date = _REPORT_DATE) -> str:
    return f"landing/{report_date.isoformat()}/NSE_CM_security_{report_date.strftime('%d%m%Y')}.csv.gz"


def _requested_url(report_date: date = _REPORT_DATE) -> str:
    return f"https://nsearchives.nseindia.com/content/cm/NSE_CM_security_{report_date.strftime('%d%m%Y')}.csv.gz"


def _valid_landing_object(
    *,
    report_date: date = _REPORT_DATE,
    bucket: str = _BUCKET,
    generation: object = 123,
    sha256: str = _RAW_SHA256,
    object_name: str | None = None,
    file_type: str = "SECURITY_MASTER",
) -> dict[str, object]:
    return {
        "file_type": file_type,
        "bucket": bucket,
        "object_name": object_name if object_name is not None else _object_name(report_date),
        "generation": generation,
        "sha256": sha256,
    }


def _valid_receipt_dict(
    *,
    report_date: date = _REPORT_DATE,
    bucket: str = _BUCKET,
    acquirer_id: str = _ACQUIRER_ID,
    acquired_at: object = _ACQUIRED_AT,
    requested_url: str | None = None,
    response_status: object = 200,
    response_media_type: object = "application/gzip",
    raw_byte_count: object = 54321,
    raw_sha256: str = _RAW_SHA256,
    landing_object: dict[str, object] | None = None,
    schema_version: object = 1,
    dataset: object = "nse-cm-mii-security",
    authority: object = "NSE",
    report_date_str: str | None = None,
) -> dict[str, object]:
    return {
        "schema_version": schema_version,
        "dataset": dataset,
        "authority": authority,
        "acquirer_id": acquirer_id,
        "acquired_at": acquired_at,
        "report_date": report_date_str if report_date_str is not None else report_date.isoformat(),
        "requested_url": (
            requested_url if requested_url is not None else _requested_url(report_date)
        ),
        "response_status": response_status,
        "response_media_type": response_media_type,
        "raw_byte_count": raw_byte_count,
        "raw_sha256": raw_sha256,
        "landing_object": (
            landing_object
            if landing_object is not None
            else _valid_landing_object(report_date=report_date, bucket=bucket)
        ),
    }


def _encode(receipt: dict[str, object]) -> bytes:
    return json.dumps(receipt, separators=(",", ":")).encode("utf-8")


def _binding_for(
    receipt_bytes: bytes,
    *,
    bucket: str = _BUCKET,
    report_date: date = _REPORT_DATE,
    not_before: datetime = _NOT_BEFORE,
    cutoff: datetime = _CUTOFF,
    raw_sha256: str = _RAW_SHA256,
    acquirer_id: str = _ACQUIRER_ID,
) -> TrustedReferenceAcquisitionBinding:
    return TrustedReferenceAcquisitionBinding(
        expected_receipt_sha256=hashlib.sha256(receipt_bytes).hexdigest(),
        expected_raw_sha256=raw_sha256,
        allowed_bucket=bucket,
        target_report_date=report_date,
        not_before=not_before,
        cutoff=cutoff,
        trusted_acquirer_id=acquirer_id,
    )


def _valid_pair() -> tuple[bytes, TrustedReferenceAcquisitionBinding]:
    receipt_bytes = _encode(_valid_receipt_dict())
    return receipt_bytes, _binding_for(receipt_bytes)


def _kwargs_from(result: VerifiedReferenceAcquisitionReceipt) -> dict[str, object]:
    return {
        "schema_version": result.schema_version,
        "receipt_bytes": result.receipt_bytes,
        "receipt_sha256": result.receipt_sha256,
        "dataset": result.dataset,
        "authority": result.authority,
        "acquirer_id": result.acquirer_id,
        "acquired_at": result.acquired_at,
        "report_date": result.report_date,
        "requested_url": result.requested_url,
        "response_status": result.response_status,
        "response_media_type": result.response_media_type,
        "raw_byte_count": result.raw_byte_count,
        "raw_sha256": result.raw_sha256,
        "landing_object": result.landing_object,
        "binding": result.binding,
    }


def _handcrafted_valid_text() -> str:
    return (
        "{"
        '"schema_version":1,'
        '"dataset":"nse-cm-mii-security",'
        '"authority":"NSE",'
        f'"acquirer_id":"{_ACQUIRER_ID}",'
        f'"acquired_at":"{_ACQUIRED_AT}",'
        f'"report_date":"{_REPORT_DATE.isoformat()}",'
        f'"requested_url":"{_requested_url()}",'
        '"response_status":200,'
        '"response_media_type":"application/gzip",'
        '"raw_byte_count":54321,'
        f'"raw_sha256":"{_RAW_SHA256}",'
        '"landing_object":{'
        '"file_type":"SECURITY_MASTER",'
        f'"bucket":"{_BUCKET}",'
        f'"object_name":"{_object_name()}",'
        '"generation":123,'
        f'"sha256":"{_RAW_SHA256}"'
        "}}"
    )


class TrustedReferenceAcquisitionBindingTests(unittest.TestCase):
    def _binding(self, **overrides: object) -> TrustedReferenceAcquisitionBinding:
        values = dict(
            expected_receipt_sha256="0" * 64,
            expected_raw_sha256=_RAW_SHA256,
            allowed_bucket=_BUCKET,
            target_report_date=_REPORT_DATE,
            not_before=_NOT_BEFORE,
            cutoff=_CUTOFF,
            trusted_acquirer_id=_ACQUIRER_ID,
        )
        values.update(overrides)
        return TrustedReferenceAcquisitionBinding(**values)

    def test_accepts_valid_binding(self) -> None:
        binding = self._binding()
        self.assertEqual(binding.allowed_bucket, _BUCKET)

    def test_rejects_malformed_expected_receipt_hash(self) -> None:
        for bad in ("NOT-A-HASH", "0" * 63, "0" * 65, "A" * 64):
            with self.assertRaises(ReferenceAcquisitionReceiptError):
                self._binding(expected_receipt_sha256=bad)

    def test_rejects_malformed_expected_raw_hash(self) -> None:
        for bad in ("NOT-A-HASH", "0" * 63, "0" * 65, "A" * 64):
            with self.assertRaises(ReferenceAcquisitionReceiptError):
                self._binding(expected_raw_sha256=bad)

    def test_rejects_malformed_acquirer_id(self) -> None:
        for bad in ("NOT-A-HASH", "0" * 63, "0" * 65, "A" * 64):
            with self.assertRaises(ReferenceAcquisitionReceiptError):
                self._binding(trusted_acquirer_id=bad)

    def test_rejects_invalid_bucket(self) -> None:
        with self.assertRaises(ReferenceAcquisitionReceiptError):
            self._binding(allowed_bucket="Bad_Bucket!")

    def test_rejects_non_date_report_date(self) -> None:
        with self.assertRaises(ReferenceAcquisitionReceiptError):
            self._binding(target_report_date="2026-07-16")

    def test_rejects_datetime_subclass_as_report_date(self) -> None:
        with self.assertRaises(ReferenceAcquisitionReceiptError):
            self._binding(target_report_date=datetime(2026, 7, 16, tzinfo=timezone.utc))

    def test_rejects_naive_not_before(self) -> None:
        with self.assertRaises(ReferenceAcquisitionReceiptError):
            self._binding(not_before=datetime(2026, 7, 16, 0, 0, 0))

    def test_rejects_naive_cutoff(self) -> None:
        with self.assertRaises(ReferenceAcquisitionReceiptError):
            self._binding(cutoff=datetime(2026, 7, 16, 23, 59, 59))

    def test_rejects_non_utc_offset_not_before(self) -> None:
        ist = timezone(timedelta(hours=5, minutes=30))
        with self.assertRaises(ReferenceAcquisitionReceiptError):
            self._binding(not_before=datetime(2026, 7, 16, 5, 30, tzinfo=ist))

    def test_rejects_non_utc_offset_cutoff(self) -> None:
        ist = timezone(timedelta(hours=5, minutes=30))
        with self.assertRaises(ReferenceAcquisitionReceiptError):
            self._binding(cutoff=datetime(2026, 7, 17, 5, 29, 59, tzinfo=ist))

    def test_rejects_not_before_after_cutoff(self) -> None:
        with self.assertRaises(ReferenceAcquisitionReceiptError):
            self._binding(
                not_before=datetime(2027, 1, 1, tzinfo=timezone.utc), cutoff=_CUTOFF
            )

    def test_accepts_not_before_equal_to_cutoff(self) -> None:
        binding = self._binding(not_before=_CUTOFF, cutoff=_CUTOFF)
        self.assertEqual(binding.not_before, _CUTOFF)

    def test_rejects_wrong_type_and_subclass(self) -> None:
        class NotABinding:
            pass

        with self.assertRaises(ReferenceAcquisitionReceiptError):
            ReferenceAcquisitionReceiptVerifier().verify(b"{}", NotABinding())  # type: ignore[arg-type]

    def test_rejects_str_subclass_expected_receipt_sha256(self) -> None:
        class _StrSubclass(str):
            pass

        with self.assertRaises(ReferenceAcquisitionReceiptError):
            self._binding(expected_receipt_sha256=_StrSubclass("0" * 64))

    def test_rejects_str_subclass_expected_raw_sha256(self) -> None:
        class _StrSubclass(str):
            pass

        with self.assertRaises(ReferenceAcquisitionReceiptError):
            self._binding(expected_raw_sha256=_StrSubclass(_RAW_SHA256))

    def test_rejects_str_subclass_allowed_bucket(self) -> None:
        class _StrSubclass(str):
            pass

        with self.assertRaises(ReferenceAcquisitionReceiptError):
            self._binding(allowed_bucket=_StrSubclass(_BUCKET))

    def test_rejects_str_subclass_trusted_acquirer_id(self) -> None:
        class _StrSubclass(str):
            pass

        with self.assertRaises(ReferenceAcquisitionReceiptError):
            self._binding(trusted_acquirer_id=_StrSubclass(_ACQUIRER_ID))


class ReferenceAcquisitionReceiptVerifierAcceptanceTests(unittest.TestCase):
    def test_accepts_valid_receipt_and_returns_complete_lineage(self) -> None:
        receipt_bytes, binding = _valid_pair()
        result = ReferenceAcquisitionReceiptVerifier().verify(receipt_bytes, binding)

        self.assertIsInstance(result, VerifiedReferenceAcquisitionReceipt)
        self.assertEqual(result.schema_version, 1)
        self.assertEqual(result.receipt_bytes, receipt_bytes)
        self.assertEqual(result.receipt_sha256, binding.expected_receipt_sha256)
        self.assertEqual(result.dataset, "nse-cm-mii-security")
        self.assertEqual(result.authority, "NSE")
        self.assertEqual(result.acquirer_id, _ACQUIRER_ID)
        self.assertEqual(
            result.acquired_at, datetime(2026, 7, 16, 13, 30, 0, tzinfo=timezone.utc)
        )
        self.assertEqual(result.report_date, _REPORT_DATE)
        self.assertEqual(result.requested_url, _requested_url())
        self.assertEqual(result.response_status, 200)
        self.assertEqual(result.response_media_type, "application/gzip")
        self.assertEqual(result.raw_byte_count, 54321)
        self.assertEqual(result.raw_sha256, _RAW_SHA256)
        self.assertIsInstance(result.landing_object, LandingObjectRequest)
        self.assertEqual(result.landing_object.file_type, AcquisitionFileType.SECURITY_MASTER)
        self.assertEqual(result.landing_object.bucket, _BUCKET)
        self.assertEqual(result.landing_object.object_name, _object_name())
        self.assertEqual(result.landing_object.generation, 123)
        self.assertEqual(result.landing_object.expected_sha256, _RAW_SHA256)
        self.assertIs(result.binding, binding)
        result_recomputed_id = hashlib.sha256(receipt_bytes).hexdigest()
        self.assertEqual(result.receipt_sha256, result_recomputed_id)

    def test_no_readiness_actionable_or_promotion_field_exists(self) -> None:
        receipt_bytes, binding = _valid_pair()
        result = ReferenceAcquisitionReceiptVerifier().verify(receipt_bytes, binding)
        field_names = {field.name for field in dataclasses.fields(result)}
        for banned in (
            "readiness",
            "actionable",
            "verified_report_date",
            "acquisition_mode",
            "promoted",
            "promotion",
            "training_eligible",
        ):
            self.assertNotIn(banned, field_names)

    def test_returned_dataclasses_are_frozen_and_bytes_are_exact(self) -> None:
        receipt_bytes, binding = _valid_pair()
        result = ReferenceAcquisitionReceiptVerifier().verify(receipt_bytes, binding)

        with self.assertRaises(dataclasses.FrozenInstanceError):
            result.schema_version = 2  # type: ignore[misc]
        with self.assertRaises(dataclasses.FrozenInstanceError):
            binding.allowed_bucket = "other"  # type: ignore[misc]

        self.assertIs(type(result.receipt_bytes), bytes)
        self.assertEqual(result.receipt_bytes, receipt_bytes)


class ReferenceAcquisitionReceiptVerifierHashBoundaryTests(unittest.TestCase):
    def test_one_byte_tampering_fails_the_external_receipt_hash(self) -> None:
        receipt_bytes, binding = _valid_pair()
        tampered = receipt_bytes[:-1] + bytes([receipt_bytes[-1] ^ 0x01])

        with self.assertRaises(ReferenceAcquisitionReceiptError):
            ReferenceAcquisitionReceiptVerifier().verify(tampered, binding)

    def test_recomputing_a_self_declared_hash_cannot_bypass_the_external_binding(self) -> None:
        original_bytes, binding = _valid_pair()
        tampered_bytes = _encode(_valid_receipt_dict(response_status=201))
        self.assertNotEqual(original_bytes, tampered_bytes)

        with self.assertRaises(ReferenceAcquisitionReceiptError):
            ReferenceAcquisitionReceiptVerifier().verify(tampered_bytes, binding)

    def test_wrong_expected_raw_hash_fails(self) -> None:
        receipt_bytes = _encode(_valid_receipt_dict())
        binding = _binding_for(receipt_bytes, raw_sha256="b" * 64)

        with self.assertRaises(ReferenceAcquisitionReceiptError):
            ReferenceAcquisitionReceiptVerifier().verify(receipt_bytes, binding)

    def test_receipt_raw_hash_disagreeing_with_binding_fails(self) -> None:
        other_raw_sha256 = hashlib.sha256(b"different-content").hexdigest()
        receipt_bytes = _encode(
            _valid_receipt_dict(
                raw_sha256=other_raw_sha256,
                landing_object=_valid_landing_object(sha256=other_raw_sha256),
            )
        )
        binding = _binding_for(receipt_bytes, raw_sha256=_RAW_SHA256)

        with self.assertRaises(ReferenceAcquisitionReceiptError):
            ReferenceAcquisitionReceiptVerifier().verify(receipt_bytes, binding)

    def test_landing_object_hash_mismatched_with_raw_hash_fails(self) -> None:
        other_sha256 = hashlib.sha256(b"landing-object-content").hexdigest()
        receipt_bytes = _encode(
            _valid_receipt_dict(landing_object=_valid_landing_object(sha256=other_sha256))
        )
        binding = _binding_for(receipt_bytes)

        with self.assertRaises(ReferenceAcquisitionReceiptError):
            ReferenceAcquisitionReceiptVerifier().verify(receipt_bytes, binding)

    def test_mutated_retained_receipt_bytes_are_rejected_on_direct_construction(self) -> None:
        receipt_bytes, binding = _valid_pair()
        result = ReferenceAcquisitionReceiptVerifier().verify(receipt_bytes, binding)

        with self.assertRaises(ReferenceAcquisitionReceiptError):
            VerifiedReferenceAcquisitionReceipt(
                schema_version=result.schema_version,
                receipt_bytes=receipt_bytes + b"x",
                receipt_sha256=result.receipt_sha256,
                dataset=result.dataset,
                authority=result.authority,
                acquirer_id=result.acquirer_id,
                acquired_at=result.acquired_at,
                report_date=result.report_date,
                requested_url=result.requested_url,
                response_status=result.response_status,
                response_media_type=result.response_media_type,
                raw_byte_count=result.raw_byte_count,
                raw_sha256=result.raw_sha256,
                landing_object=result.landing_object,
                binding=binding,
            )


class ReferenceAcquisitionReceiptVerifierSourceTests(unittest.TestCase):
    def test_wrong_dataset_fails(self) -> None:
        receipt_bytes = _encode(_valid_receipt_dict(dataset="other-dataset"))
        binding = _binding_for(receipt_bytes)
        with self.assertRaises(ReferenceAcquisitionReceiptError):
            ReferenceAcquisitionReceiptVerifier().verify(receipt_bytes, binding)

    def test_wrong_authority_fails(self) -> None:
        receipt_bytes = _encode(_valid_receipt_dict(authority="BSE"))
        binding = _binding_for(receipt_bytes)
        with self.assertRaises(ReferenceAcquisitionReceiptError):
            ReferenceAcquisitionReceiptVerifier().verify(receipt_bytes, binding)

    def test_wrong_acquirer_fails(self) -> None:
        other_acquirer = "c" * 64
        receipt_bytes = _encode(_valid_receipt_dict(acquirer_id=other_acquirer))
        binding = _binding_for(receipt_bytes, acquirer_id=_ACQUIRER_ID)
        with self.assertRaises(ReferenceAcquisitionReceiptError):
            ReferenceAcquisitionReceiptVerifier().verify(receipt_bytes, binding)

    def test_malformed_acquirer_id_fails(self) -> None:
        receipt_bytes = _encode(_valid_receipt_dict(acquirer_id="not-a-hash"))
        binding = _binding_for(receipt_bytes)
        with self.assertRaises(ReferenceAcquisitionReceiptError):
            ReferenceAcquisitionReceiptVerifier().verify(receipt_bytes, binding)

    def test_wrong_report_date_fails(self) -> None:
        receipt_bytes, binding = _valid_pair()
        wrong_binding = _binding_for(receipt_bytes, report_date=date(2026, 7, 17))
        with self.assertRaises(ReferenceAcquisitionReceiptError):
            ReferenceAcquisitionReceiptVerifier().verify(receipt_bytes, wrong_binding)

    def test_noncanonical_report_date_fails(self) -> None:
        for bad in ("2026-7-16", "16-07-2026", "2026/07/16", "2026-07-16T00:00:00"):
            receipt_bytes = _encode(_valid_receipt_dict(report_date_str=bad))
            binding = _binding_for(receipt_bytes)
            with self.subTest(bad=bad):
                with self.assertRaises(ReferenceAcquisitionReceiptError):
                    ReferenceAcquisitionReceiptVerifier().verify(receipt_bytes, binding)

    def test_naive_acquired_at_fails(self) -> None:
        receipt_bytes = _encode(_valid_receipt_dict(acquired_at="2026-07-16T13:30:00"))
        binding = _binding_for(receipt_bytes)
        with self.assertRaises(ReferenceAcquisitionReceiptError):
            ReferenceAcquisitionReceiptVerifier().verify(receipt_bytes, binding)

    def test_non_utc_acquired_at_fails(self) -> None:
        receipt_bytes = _encode(_valid_receipt_dict(acquired_at="2026-07-16T19:00:00+05:30"))
        binding = _binding_for(receipt_bytes)
        with self.assertRaises(ReferenceAcquisitionReceiptError):
            ReferenceAcquisitionReceiptVerifier().verify(receipt_bytes, binding)

    def test_acquired_at_before_not_before_fails(self) -> None:
        not_before = datetime(2026, 7, 16, 14, 0, 0, tzinfo=timezone.utc)
        receipt_bytes = _encode(_valid_receipt_dict(acquired_at="2026-07-16T13:30:00Z"))
        binding = _binding_for(receipt_bytes, not_before=not_before, cutoff=_CUTOFF)
        with self.assertRaises(ReferenceAcquisitionReceiptError):
            ReferenceAcquisitionReceiptVerifier().verify(receipt_bytes, binding)

    def test_acquired_at_after_cutoff_fails(self) -> None:
        cutoff = datetime(2026, 7, 16, 10, 0, 0, tzinfo=timezone.utc)
        receipt_bytes = _encode(_valid_receipt_dict(acquired_at="2026-07-16T13:30:00Z"))
        binding = _binding_for(receipt_bytes, cutoff=cutoff)
        with self.assertRaises(ReferenceAcquisitionReceiptError):
            ReferenceAcquisitionReceiptVerifier().verify(receipt_bytes, binding)

    def test_acquired_at_equal_to_cutoff_succeeds(self) -> None:
        cutoff = datetime(2026, 7, 16, 13, 30, 0, tzinfo=timezone.utc)
        receipt_bytes = _encode(_valid_receipt_dict(acquired_at="2026-07-16T13:30:00Z"))
        binding = _binding_for(receipt_bytes, cutoff=cutoff)
        result = ReferenceAcquisitionReceiptVerifier().verify(receipt_bytes, binding)
        self.assertEqual(result.acquired_at, cutoff)

    def test_future_report_date_relative_to_acquisition_in_ist_fails(self) -> None:
        # acquired_at is 2026-07-16T18:35:00Z == 2026-07-17T00:05 IST, so a
        # report_date of 2026-07-18 claims knowledge of a report date that is
        # still a full day beyond what could exist yet in IST.
        acquired_at = "2026-07-16T18:35:00Z"
        report_date = date(2026, 7, 18)
        receipt_bytes = _encode(
            _valid_receipt_dict(
                report_date=report_date,
                acquired_at=acquired_at,
                requested_url=_requested_url(report_date),
            )
        )
        cutoff = datetime(2026, 7, 16, 23, 59, 59, tzinfo=timezone.utc)
        binding = _binding_for(
            receipt_bytes, report_date=report_date, not_before=_NOT_BEFORE, cutoff=cutoff
        )
        with self.assertRaises(ReferenceAcquisitionReceiptError):
            ReferenceAcquisitionReceiptVerifier().verify(receipt_bytes, binding)

    def test_report_date_equal_to_acquired_at_ist_date_succeeds(self) -> None:
        # 2026-07-16T13:30:00Z is 2026-07-16T19:00 IST -- same calendar date.
        receipt_bytes, binding = _valid_pair()
        result = ReferenceAcquisitionReceiptVerifier().verify(receipt_bytes, binding)
        self.assertEqual(result.report_date, _REPORT_DATE)

    def test_query_string_in_url_fails(self) -> None:
        bad_url = _requested_url() + "?download=1"
        receipt_bytes = _encode(_valid_receipt_dict(requested_url=bad_url))
        binding = _binding_for(receipt_bytes)
        with self.assertRaises(ReferenceAcquisitionReceiptError):
            ReferenceAcquisitionReceiptVerifier().verify(receipt_bytes, binding)

    def test_fragment_in_url_fails(self) -> None:
        bad_url = _requested_url() + "#frag"
        receipt_bytes = _encode(_valid_receipt_dict(requested_url=bad_url))
        binding = _binding_for(receipt_bytes)
        with self.assertRaises(ReferenceAcquisitionReceiptError):
            ReferenceAcquisitionReceiptVerifier().verify(receipt_bytes, binding)

    def test_alternate_host_in_url_fails(self) -> None:
        bad_url = _requested_url().replace(
            "nsearchives.nseindia.com", "nsearchives.nseindia.com.evil.example"
        )
        receipt_bytes = _encode(_valid_receipt_dict(requested_url=bad_url))
        binding = _binding_for(receipt_bytes)
        with self.assertRaises(ReferenceAcquisitionReceiptError):
            ReferenceAcquisitionReceiptVerifier().verify(receipt_bytes, binding)

    def test_alternate_scheme_in_url_fails(self) -> None:
        bad_url = "http://" + _requested_url().removeprefix("https://")
        receipt_bytes = _encode(_valid_receipt_dict(requested_url=bad_url))
        binding = _binding_for(receipt_bytes)
        with self.assertRaises(ReferenceAcquisitionReceiptError):
            ReferenceAcquisitionReceiptVerifier().verify(receipt_bytes, binding)

    def test_alternate_port_in_url_fails(self) -> None:
        bad_url = _requested_url().replace(
            "nsearchives.nseindia.com", "nsearchives.nseindia.com:8443"
        )
        receipt_bytes = _encode(_valid_receipt_dict(requested_url=bad_url))
        binding = _binding_for(receipt_bytes)
        with self.assertRaises(ReferenceAcquisitionReceiptError):
            ReferenceAcquisitionReceiptVerifier().verify(receipt_bytes, binding)

    def test_unicode_confusable_host_in_url_fails(self) -> None:
        bad_url = _requested_url().replace("nsearchives", "nseаrchives")  # Cyrillic 'a'
        receipt_bytes = _encode(_valid_receipt_dict(requested_url=bad_url))
        binding = _binding_for(receipt_bytes)
        with self.assertRaises(ReferenceAcquisitionReceiptError):
            ReferenceAcquisitionReceiptVerifier().verify(receipt_bytes, binding)

    def test_path_traversal_in_url_fails(self) -> None:
        bad_url = "https://nsearchives.nseindia.com/content/cm/../../etc/passwd"
        receipt_bytes = _encode(_valid_receipt_dict(requested_url=bad_url))
        binding = _binding_for(receipt_bytes)
        with self.assertRaises(ReferenceAcquisitionReceiptError):
            ReferenceAcquisitionReceiptVerifier().verify(receipt_bytes, binding)

    def test_wrong_date_in_url_filename_fails(self) -> None:
        bad_url = _requested_url(date(2026, 7, 17))
        receipt_bytes = _encode(_valid_receipt_dict(requested_url=bad_url))
        binding = _binding_for(receipt_bytes)
        with self.assertRaises(ReferenceAcquisitionReceiptError):
            ReferenceAcquisitionReceiptVerifier().verify(receipt_bytes, binding)

    def test_bse_exclusive_interoperability_filename_fails(self) -> None:
        bad_url = (
            "https://nsearchives.nseindia.com/content/cm/"
            f"NSE_CM_security_and_BSE_exclusive_{_REPORT_DATE.strftime('%d%m%Y')}.csv.gz"
        )
        receipt_bytes = _encode(_valid_receipt_dict(requested_url=bad_url))
        binding = _binding_for(receipt_bytes)
        with self.assertRaises(ReferenceAcquisitionReceiptError):
            ReferenceAcquisitionReceiptVerifier().verify(receipt_bytes, binding)


class ReferenceAcquisitionReceiptVerifierTransportObjectTests(unittest.TestCase):
    def test_non_200_status_fails(self) -> None:
        for bad in (200.0, "200", 201, 404, True):
            receipt_bytes = _encode(_valid_receipt_dict(response_status=bad))
            binding = _binding_for(receipt_bytes)
            with self.subTest(bad=bad):
                with self.assertRaises(ReferenceAcquisitionReceiptError):
                    ReferenceAcquisitionReceiptVerifier().verify(receipt_bytes, binding)

    def test_wrong_media_type_fails(self) -> None:
        for bad in ("text/csv", "application/x-gzip", "application/gzip; charset=utf-8"):
            receipt_bytes = _encode(_valid_receipt_dict(response_media_type=bad))
            binding = _binding_for(receipt_bytes)
            with self.subTest(bad=bad):
                with self.assertRaises(ReferenceAcquisitionReceiptError):
                    ReferenceAcquisitionReceiptVerifier().verify(receipt_bytes, binding)

    def test_zero_raw_byte_count_fails(self) -> None:
        receipt_bytes = _encode(_valid_receipt_dict(raw_byte_count=0))
        binding = _binding_for(receipt_bytes)
        with self.assertRaises(ReferenceAcquisitionReceiptError):
            ReferenceAcquisitionReceiptVerifier().verify(receipt_bytes, binding)

    def test_negative_raw_byte_count_fails(self) -> None:
        receipt_bytes = _encode(_valid_receipt_dict(raw_byte_count=-1))
        binding = _binding_for(receipt_bytes)
        with self.assertRaises(ReferenceAcquisitionReceiptError):
            ReferenceAcquisitionReceiptVerifier().verify(receipt_bytes, binding)

    def test_over_32_mib_raw_byte_count_fails(self) -> None:
        receipt_bytes = _encode(
            _valid_receipt_dict(raw_byte_count=32 * 1024 * 1024 + 1)
        )
        binding = _binding_for(receipt_bytes)
        with self.assertRaises(ReferenceAcquisitionReceiptError):
            ReferenceAcquisitionReceiptVerifier().verify(receipt_bytes, binding)

    def test_exactly_32_mib_raw_byte_count_succeeds(self) -> None:
        receipt_bytes = _encode(_valid_receipt_dict(raw_byte_count=32 * 1024 * 1024))
        binding = _binding_for(receipt_bytes)
        result = ReferenceAcquisitionReceiptVerifier().verify(receipt_bytes, binding)
        self.assertEqual(result.raw_byte_count, 32 * 1024 * 1024)

    def test_bool_raw_byte_count_fails(self) -> None:
        receipt_bytes = _encode(_valid_receipt_dict(raw_byte_count=True))
        binding = _binding_for(receipt_bytes)
        with self.assertRaises(ReferenceAcquisitionReceiptError):
            ReferenceAcquisitionReceiptVerifier().verify(receipt_bytes, binding)

    def test_wrong_file_type_fails(self) -> None:
        receipt_bytes = _encode(
            _valid_receipt_dict(landing_object=_valid_landing_object(file_type="DAILY_BUNDLE"))
        )
        binding = _binding_for(receipt_bytes)
        with self.assertRaises(ReferenceAcquisitionReceiptError):
            ReferenceAcquisitionReceiptVerifier().verify(receipt_bytes, binding)

    def test_invalid_file_type_fails(self) -> None:
        receipt_bytes = _encode(
            _valid_receipt_dict(landing_object=_valid_landing_object(file_type="NOT_A_TYPE"))
        )
        binding = _binding_for(receipt_bytes)
        with self.assertRaises(ReferenceAcquisitionReceiptError):
            ReferenceAcquisitionReceiptVerifier().verify(receipt_bytes, binding)

    def test_syntactically_valid_non_allowed_bucket_fails(self) -> None:
        receipt_bytes = _encode(
            _valid_receipt_dict(
                landing_object=_valid_landing_object(bucket="another-syntactically-valid-bucket")
            )
        )
        binding = _binding_for(receipt_bytes, bucket=_BUCKET)
        with self.assertRaises(ReferenceAcquisitionReceiptError):
            ReferenceAcquisitionReceiptVerifier().verify(receipt_bytes, binding)

    def test_wrong_date_in_object_path_fails(self) -> None:
        receipt_bytes = _encode(
            _valid_receipt_dict(
                landing_object=_valid_landing_object(
                    object_name="landing/2026-07-16/NSE_CM_security_17072026.csv.gz"
                )
            )
        )
        binding = _binding_for(receipt_bytes)
        with self.assertRaises(ReferenceAcquisitionReceiptError):
            ReferenceAcquisitionReceiptVerifier().verify(receipt_bytes, binding)

    def test_wrong_session_prefix_in_object_path_fails(self) -> None:
        receipt_bytes = _encode(
            _valid_receipt_dict(
                landing_object=_valid_landing_object(
                    object_name="landing/2026-07-17/NSE_CM_security_16072026.csv.gz"
                )
            )
        )
        binding = _binding_for(receipt_bytes)
        with self.assertRaises(ReferenceAcquisitionReceiptError):
            ReferenceAcquisitionReceiptVerifier().verify(receipt_bytes, binding)

    def test_path_traversal_in_object_name_fails(self) -> None:
        receipt_bytes = _encode(
            _valid_receipt_dict(
                landing_object=_valid_landing_object(
                    object_name="landing/../secrets/NSE_CM_security_16072026.csv.gz"
                )
            )
        )
        binding = _binding_for(receipt_bytes)
        with self.assertRaises(ReferenceAcquisitionReceiptError):
            ReferenceAcquisitionReceiptVerifier().verify(receipt_bytes, binding)

    def test_bool_generation_fails(self) -> None:
        receipt_bytes = _encode(
            _valid_receipt_dict(landing_object=_valid_landing_object(generation=True))
        )
        binding = _binding_for(receipt_bytes)
        with self.assertRaises(ReferenceAcquisitionReceiptError):
            ReferenceAcquisitionReceiptVerifier().verify(receipt_bytes, binding)

    def test_zero_generation_fails(self) -> None:
        receipt_bytes = _encode(
            _valid_receipt_dict(landing_object=_valid_landing_object(generation=0))
        )
        binding = _binding_for(receipt_bytes)
        with self.assertRaises(ReferenceAcquisitionReceiptError):
            ReferenceAcquisitionReceiptVerifier().verify(receipt_bytes, binding)

    def test_negative_generation_fails(self) -> None:
        receipt_bytes = _encode(
            _valid_receipt_dict(landing_object=_valid_landing_object(generation=-5))
        )
        binding = _binding_for(receipt_bytes)
        with self.assertRaises(ReferenceAcquisitionReceiptError):
            ReferenceAcquisitionReceiptVerifier().verify(receipt_bytes, binding)

    def test_string_generation_fails(self) -> None:
        receipt_bytes = _encode(
            _valid_receipt_dict(landing_object=_valid_landing_object(generation="123"))
        )
        binding = _binding_for(receipt_bytes)
        with self.assertRaises(ReferenceAcquisitionReceiptError):
            ReferenceAcquisitionReceiptVerifier().verify(receipt_bytes, binding)

    def test_uppercase_sha256_fails(self) -> None:
        receipt_bytes = _encode(
            _valid_receipt_dict(landing_object=_valid_landing_object(sha256=_RAW_SHA256.upper()))
        )
        binding = _binding_for(receipt_bytes)
        with self.assertRaises(ReferenceAcquisitionReceiptError):
            ReferenceAcquisitionReceiptVerifier().verify(receipt_bytes, binding)


class ReferenceAcquisitionReceiptVerifierStrictJsonTests(unittest.TestCase):
    def test_empty_bytes_fail(self) -> None:
        _, binding = _valid_pair()
        with self.assertRaises(ReferenceAcquisitionReceiptError):
            ReferenceAcquisitionReceiptVerifier().verify(b"", binding)

    def test_oversized_receipt_fails(self) -> None:
        _, binding = _valid_pair()
        oversized = b"a" * (64 * 1024 + 1)
        with self.assertRaises(ReferenceAcquisitionReceiptError):
            ReferenceAcquisitionReceiptVerifier().verify(oversized, binding)

    def test_non_bytes_input_fails(self) -> None:
        _, binding = _valid_pair()
        with self.assertRaises(ReferenceAcquisitionReceiptError):
            ReferenceAcquisitionReceiptVerifier().verify("not-bytes", binding)  # type: ignore[arg-type]
        with self.assertRaises(ReferenceAcquisitionReceiptError):
            ReferenceAcquisitionReceiptVerifier().verify(bytearray(b"{}"), binding)  # type: ignore[arg-type]

    def test_invalid_utf8_fails(self) -> None:
        bad_bytes = b"\xff\xfe\x00\x01not-valid-utf8"
        binding = _binding_for(bad_bytes)
        with self.assertRaises(ReferenceAcquisitionReceiptError):
            ReferenceAcquisitionReceiptVerifier().verify(bad_bytes, binding)

    def test_malformed_json_fails(self) -> None:
        bad_bytes = b'{"schema_version": 1, "dataset": ['
        binding = _binding_for(bad_bytes)
        with self.assertRaises(ReferenceAcquisitionReceiptError):
            ReferenceAcquisitionReceiptVerifier().verify(bad_bytes, binding)

    def test_duplicate_top_level_json_key_fails(self) -> None:
        text = _handcrafted_valid_text()
        tampered = text.replace('"schema_version":1,', '"schema_version":1,"schema_version":1,', 1)
        receipt_bytes = tampered.encode("utf-8")
        binding = _binding_for(receipt_bytes)
        with self.assertRaises(ReferenceAcquisitionReceiptError):
            ReferenceAcquisitionReceiptVerifier().verify(receipt_bytes, binding)

    def test_duplicate_nested_landing_object_json_key_fails(self) -> None:
        text = _handcrafted_valid_text()
        needle = f'"bucket":"{_BUCKET}","object_name"'
        replacement = f'"bucket":"{_BUCKET}","bucket":"{_BUCKET}","object_name"'
        tampered = text.replace(needle, replacement, 1)
        self.assertNotEqual(text, tampered)
        receipt_bytes = tampered.encode("utf-8")
        binding = _binding_for(receipt_bytes)
        with self.assertRaises(ReferenceAcquisitionReceiptError):
            ReferenceAcquisitionReceiptVerifier().verify(receipt_bytes, binding)

    def test_floats_and_exponents_are_rejected(self) -> None:
        for token in ("54321.0", "5.4321e4"):
            bad_bytes = f'{{"schema_version": 1, "raw_byte_count": {token}}}'.encode("utf-8")
            binding = _binding_for(bad_bytes)
            with self.subTest(token=token):
                with self.assertRaises(ReferenceAcquisitionReceiptError):
                    ReferenceAcquisitionReceiptVerifier().verify(bad_bytes, binding)

    def test_nan_and_infinity_are_rejected(self) -> None:
        for token in ("NaN", "Infinity", "-Infinity"):
            bad_bytes = f'{{"schema_version": 1, "extra": {token}}}'.encode("utf-8")
            binding = _binding_for(bad_bytes)
            with self.subTest(token=token):
                with self.assertRaises(ReferenceAcquisitionReceiptError):
                    ReferenceAcquisitionReceiptVerifier().verify(bad_bytes, binding)

    def test_bool_in_integer_field_fails(self) -> None:
        receipt_bytes = _encode(_valid_receipt_dict(schema_version=True))
        binding = _binding_for(receipt_bytes)
        with self.assertRaises(ReferenceAcquisitionReceiptError):
            ReferenceAcquisitionReceiptVerifier().verify(receipt_bytes, binding)

    def test_huge_integer_token_raises_sanitized_error_not_raw_value_error(self) -> None:
        huge_digits = "9" * 5000
        bad_bytes = f'{{"schema_version": 1, "extra": {huge_digits}}}'.encode("utf-8")
        binding = _binding_for(bad_bytes)
        with self.assertRaises(ReferenceAcquisitionReceiptError) as ctx:
            ReferenceAcquisitionReceiptVerifier().verify(bad_bytes, binding)
        message = str(ctx.exception)
        self.assertNotIn(huge_digits, message)
        self.assertNotIn("5000", message)
        self.assertNotIn("int_max_str_digits", message)
        self.assertNotIn("ValueError", message)

    def test_missing_top_level_key_fails(self) -> None:
        receipt = _valid_receipt_dict()
        del receipt["acquired_at"]
        receipt_bytes = _encode(receipt)
        binding = _binding_for(receipt_bytes)
        with self.assertRaises(ReferenceAcquisitionReceiptError):
            ReferenceAcquisitionReceiptVerifier().verify(receipt_bytes, binding)

    def test_extra_top_level_key_fails(self) -> None:
        receipt = _valid_receipt_dict()
        receipt["unexpected"] = "value"
        receipt_bytes = _encode(receipt)
        binding = _binding_for(receipt_bytes)
        with self.assertRaises(ReferenceAcquisitionReceiptError):
            ReferenceAcquisitionReceiptVerifier().verify(receipt_bytes, binding)

    def test_missing_landing_object_key_fails(self) -> None:
        landing_object = _valid_landing_object()
        del landing_object["generation"]
        receipt_bytes = _encode(_valid_receipt_dict(landing_object=landing_object))
        binding = _binding_for(receipt_bytes)
        with self.assertRaises(ReferenceAcquisitionReceiptError):
            ReferenceAcquisitionReceiptVerifier().verify(receipt_bytes, binding)

    def test_extra_landing_object_key_fails(self) -> None:
        landing_object = _valid_landing_object()
        landing_object["extra"] = "value"
        receipt_bytes = _encode(_valid_receipt_dict(landing_object=landing_object))
        binding = _binding_for(receipt_bytes)
        with self.assertRaises(ReferenceAcquisitionReceiptError):
            ReferenceAcquisitionReceiptVerifier().verify(receipt_bytes, binding)

    def test_schema_version_must_be_exact_integer_one(self) -> None:
        for bad_version in (2, 0, "1", 1.0, True):
            receipt_bytes = _encode(_valid_receipt_dict(schema_version=bad_version))
            binding = _binding_for(receipt_bytes)
            with self.subTest(bad_version=bad_version):
                with self.assertRaises(ReferenceAcquisitionReceiptError):
                    ReferenceAcquisitionReceiptVerifier().verify(receipt_bytes, binding)


class ReferenceAcquisitionReceiptVerifierSanitizationTests(unittest.TestCase):
    def test_injected_secret_in_bucket_never_appears_in_error(self) -> None:
        secret = "SECRET-TOKEN-do-not-leak-9f3a"
        receipt_bytes = _encode(
            _valid_receipt_dict(landing_object=_valid_landing_object(bucket=secret))
        )
        binding = _binding_for(receipt_bytes, bucket=_BUCKET)

        with self.assertRaises(ReferenceAcquisitionReceiptError) as ctx:
            ReferenceAcquisitionReceiptVerifier().verify(receipt_bytes, binding)
        self.assertNotIn(secret, str(ctx.exception))

    def test_injected_secret_in_object_name_never_appears_in_error(self) -> None:
        secret = "SECRET-PATH-do-not-leak-71cd"
        receipt_bytes = _encode(
            _valid_receipt_dict(
                landing_object=_valid_landing_object(object_name=f"landing/{secret}/traversal")
            )
        )
        binding = _binding_for(receipt_bytes)

        with self.assertRaises(ReferenceAcquisitionReceiptError) as ctx:
            ReferenceAcquisitionReceiptVerifier().verify(receipt_bytes, binding)
        self.assertNotIn(secret, str(ctx.exception))

    def test_injected_secret_in_url_never_appears_in_error(self) -> None:
        secret = "SECRET-URL-do-not-leak-1a2b"
        bad_url = _requested_url() + f"?{secret}"
        receipt_bytes = _encode(_valid_receipt_dict(requested_url=bad_url))
        binding = _binding_for(receipt_bytes)

        with self.assertRaises(ReferenceAcquisitionReceiptError) as ctx:
            ReferenceAcquisitionReceiptVerifier().verify(receipt_bytes, binding)
        self.assertNotIn(secret, str(ctx.exception))

    def test_injected_secret_in_malformed_json_never_appears_in_error(self) -> None:
        secret = "SECRET-JSON-do-not-leak-44bb"
        bad_bytes = f'{{"schema_version": 1, "leak": "{secret}"'.encode("utf-8")
        binding = _binding_for(bad_bytes)

        with self.assertRaises(ReferenceAcquisitionReceiptError) as ctx:
            ReferenceAcquisitionReceiptVerifier().verify(bad_bytes, binding)
        self.assertNotIn(secret, str(ctx.exception))

    def test_hash_mismatch_error_never_contains_either_hash(self) -> None:
        receipt_bytes, binding = _valid_pair()
        tampered = receipt_bytes + b"x"

        with self.assertRaises(ReferenceAcquisitionReceiptError) as ctx:
            ReferenceAcquisitionReceiptVerifier().verify(tampered, binding)
        self.assertNotIn(binding.expected_receipt_sha256, str(ctx.exception))
        self.assertNotIn(hashlib.sha256(tampered).hexdigest(), str(ctx.exception))

    def test_injected_secret_in_acquirer_id_never_appears_in_error(self) -> None:
        # Not a valid SHA-256, so this fails the format check, but must still
        # never echo the caller-supplied text.
        secret = "SECRET-ACQUIRER-do-not-leak-77cc"
        receipt_bytes = _encode(_valid_receipt_dict(acquirer_id=secret))
        binding = _binding_for(receipt_bytes)

        with self.assertRaises(ReferenceAcquisitionReceiptError) as ctx:
            ReferenceAcquisitionReceiptVerifier().verify(receipt_bytes, binding)
        self.assertNotIn(secret, str(ctx.exception))

    def test_nested_exception_type_and_text_never_appear_in_error(self) -> None:
        bad_bytes = b"\xff\xfe\x00\x01not-valid-utf8"
        binding = _binding_for(bad_bytes)

        with self.assertRaises(ReferenceAcquisitionReceiptError) as ctx:
            ReferenceAcquisitionReceiptVerifier().verify(bad_bytes, binding)
        message = str(ctx.exception)
        self.assertNotIn("UnicodeDecodeError", message)
        self.assertNotIn("codec", message)


class VerifiedReferenceAcquisitionReceiptDefensiveConstructionTests(unittest.TestCase):
    def _result(self):
        receipt_bytes, binding = _valid_pair()
        return receipt_bytes, binding, ReferenceAcquisitionReceiptVerifier().verify(
            receipt_bytes, binding
        )

    def test_direct_construction_without_binding_is_not_possible(self) -> None:
        _receipt_bytes, _binding, result = self._result()
        with self.assertRaises(TypeError):
            VerifiedReferenceAcquisitionReceipt(
                schema_version=result.schema_version,
                receipt_bytes=result.receipt_bytes,
                receipt_sha256=result.receipt_sha256,
                dataset=result.dataset,
                authority=result.authority,
                acquirer_id=result.acquirer_id,
                acquired_at=result.acquired_at,
                report_date=result.report_date,
                requested_url=result.requested_url,
                response_status=result.response_status,
                response_media_type=result.response_media_type,
                raw_byte_count=result.raw_byte_count,
                raw_sha256=result.raw_sha256,
                landing_object=result.landing_object,
            )  # type: ignore[call-arg]

    def test_direct_construction_with_hash_mismatched_binding_fails(self) -> None:
        receipt_bytes, binding, result = self._result()
        other_bytes = _encode(_valid_receipt_dict(response_status=201))
        other_binding = _binding_for(other_bytes)

        with self.assertRaises(ReferenceAcquisitionReceiptError):
            VerifiedReferenceAcquisitionReceipt(
                schema_version=result.schema_version,
                receipt_bytes=result.receipt_bytes,
                receipt_sha256=result.receipt_sha256,
                dataset=result.dataset,
                authority=result.authority,
                acquirer_id=result.acquirer_id,
                acquired_at=result.acquired_at,
                report_date=result.report_date,
                requested_url=result.requested_url,
                response_status=result.response_status,
                response_media_type=result.response_media_type,
                raw_byte_count=result.raw_byte_count,
                raw_sha256=result.raw_sha256,
                landing_object=result.landing_object,
                binding=other_binding,
            )

    def test_direct_construction_with_report_date_mismatched_binding_fails(self) -> None:
        receipt_bytes, binding, result = self._result()
        mismatched_binding = _binding_for(receipt_bytes, report_date=date(2026, 7, 17))

        with self.assertRaises(ReferenceAcquisitionReceiptError):
            VerifiedReferenceAcquisitionReceipt(
                schema_version=result.schema_version,
                receipt_bytes=result.receipt_bytes,
                receipt_sha256=result.receipt_sha256,
                dataset=result.dataset,
                authority=result.authority,
                acquirer_id=result.acquirer_id,
                acquired_at=result.acquired_at,
                report_date=result.report_date,
                requested_url=result.requested_url,
                response_status=result.response_status,
                response_media_type=result.response_media_type,
                raw_byte_count=result.raw_byte_count,
                raw_sha256=result.raw_sha256,
                landing_object=result.landing_object,
                binding=mismatched_binding,
            )

    def test_direct_construction_with_bucket_mismatched_binding_fails(self) -> None:
        receipt_bytes, binding, result = self._result()
        mismatched_binding = _binding_for(
            receipt_bytes, bucket="another-syntactically-valid-bucket"
        )

        with self.assertRaises(ReferenceAcquisitionReceiptError):
            VerifiedReferenceAcquisitionReceipt(
                schema_version=result.schema_version,
                receipt_bytes=result.receipt_bytes,
                receipt_sha256=result.receipt_sha256,
                dataset=result.dataset,
                authority=result.authority,
                acquirer_id=result.acquirer_id,
                acquired_at=result.acquired_at,
                report_date=result.report_date,
                requested_url=result.requested_url,
                response_status=result.response_status,
                response_media_type=result.response_media_type,
                raw_byte_count=result.raw_byte_count,
                raw_sha256=result.raw_sha256,
                landing_object=result.landing_object,
                binding=mismatched_binding,
            )

    def test_direct_construction_with_acquired_at_outside_binding_bounds_fails(self) -> None:
        receipt_bytes, binding, result = self._result()
        narrow_binding = _binding_for(
            receipt_bytes,
            not_before=datetime(2026, 7, 16, 20, 0, 0, tzinfo=timezone.utc),
            cutoff=_CUTOFF,
        )

        with self.assertRaises(ReferenceAcquisitionReceiptError):
            VerifiedReferenceAcquisitionReceipt(
                schema_version=result.schema_version,
                receipt_bytes=result.receipt_bytes,
                receipt_sha256=result.receipt_sha256,
                dataset=result.dataset,
                authority=result.authority,
                acquirer_id=result.acquirer_id,
                acquired_at=result.acquired_at,  # 13:30, before narrow_binding.not_before=20:00
                report_date=result.report_date,
                requested_url=result.requested_url,
                response_status=result.response_status,
                response_media_type=result.response_media_type,
                raw_byte_count=result.raw_byte_count,
                raw_sha256=result.raw_sha256,
                landing_object=result.landing_object,
                binding=narrow_binding,
            )

    def test_direct_construction_with_tampered_dataset_fails(self) -> None:
        receipt_bytes, binding, result = self._result()

        with self.assertRaises(ReferenceAcquisitionReceiptError):
            VerifiedReferenceAcquisitionReceipt(
                schema_version=result.schema_version,
                receipt_bytes=result.receipt_bytes,
                receipt_sha256=result.receipt_sha256,
                dataset="tampered-dataset",
                authority=result.authority,
                acquirer_id=result.acquirer_id,
                acquired_at=result.acquired_at,
                report_date=result.report_date,
                requested_url=result.requested_url,
                response_status=result.response_status,
                response_media_type=result.response_media_type,
                raw_byte_count=result.raw_byte_count,
                raw_sha256=result.raw_sha256,
                landing_object=result.landing_object,
                binding=binding,
            )


class ReferenceAcquisitionReceiptCapabilityTests(unittest.TestCase):
    def test_no_listing_latest_or_io_shaped_capability_exists(self) -> None:
        banned_substrings = (
            "list",
            "latest",
            "find",
            "select",
            "download",
            "fetch",
            "network",
            "filesystem",
            "environ",
            "clock",
        )
        for candidate in (
            ReferenceAcquisitionReceiptVerifier,
            TrustedReferenceAcquisitionBinding,
            VerifiedReferenceAcquisitionReceipt,
        ):
            members = [name for name in dir(candidate) if not name.startswith("__")]
            for name in members:
                lowered = name.lower()
                self.assertFalse(
                    any(bad in lowered for bad in banned_substrings),
                    f"{candidate!r} unexpectedly exposes {name!r}",
                )

    def test_importing_module_causes_no_io(self) -> None:
        # The module must not import any I/O-capable facility at module
        # scope (network, filesystem beyond what its own dataclasses need,
        # GCS SDK, environment reads, or a wall clock). Reloading the module
        # in-process would rebind its classes to new objects and poison
        # identity checks for every other test in this process, so this
        # instead inspects the already-imported module's own namespace.
        import india_swing.reference_data.acquisition_receipt as module

        banned_module_names = {"os", "socket", "urllib", "requests", "storage"}
        top_level_names = {
            name
            for name in vars(module)
            if not name.startswith("_") and not name[0].isupper()
        }
        self.assertFalse(top_level_names & banned_module_names)
        self.assertFalse(hasattr(module, "storage"))


class ReferenceAcquisitionReceiptCanonicalAcquiredAtTests(unittest.TestCase):
    def test_only_canonical_spelling_is_accepted(self) -> None:
        receipt_bytes = _encode(_valid_receipt_dict(acquired_at="2026-07-16T13:30:00Z"))
        binding = _binding_for(receipt_bytes)
        result = ReferenceAcquisitionReceiptVerifier().verify(receipt_bytes, binding)
        self.assertEqual(
            result.acquired_at, datetime(2026, 7, 16, 13, 30, 0, tzinfo=timezone.utc)
        )

    def test_noncanonical_acquired_at_spellings_are_rejected(self) -> None:
        for bad in (
            "2026-07-16T13:30:00+00:00",
            "2026-07-16 13:30:00Z",
            "2026-07-16t13:30:00z",
            "2026-07-16T13:30Z",
            "2026-07-16T13:30:00.000Z",
            "2026-07-16T18:30:00+05:00",
            "2026-07-16T13:30:00",
        ):
            receipt_bytes = _encode(_valid_receipt_dict(acquired_at=bad))
            binding = _binding_for(receipt_bytes)
            with self.subTest(bad=bad):
                with self.assertRaises(ReferenceAcquisitionReceiptError):
                    ReferenceAcquisitionReceiptVerifier().verify(receipt_bytes, binding)


class ReferenceAcquisitionReceiptDownloadRootAuthorityTests(unittest.TestCase):
    def test_module_exposes_no_duplicated_download_root_constant(self) -> None:
        import india_swing.reference_data.acquisition_receipt as module

        self.assertFalse(hasattr(module, "NSE_CM_CLAIMED_DOWNLOAD_ROOT"))
        self.assertNotIn("NSE_CM_CLAIMED_DOWNLOAD_ROOT", dir(module))

    def test_requested_url_derives_from_artifact_store_authority(self) -> None:
        from india_swing.reference_data import artifact_store

        receipt_bytes, binding = _valid_pair()
        result = ReferenceAcquisitionReceiptVerifier().verify(receipt_bytes, binding)
        self.assertTrue(result.requested_url.startswith(artifact_store.NSE_CM_CLAIMED_DOWNLOAD_ROOT))
        self.assertEqual(
            result.requested_url,
            artifact_store.NSE_CM_CLAIMED_DOWNLOAD_ROOT
            + f"NSE_CM_security_{_REPORT_DATE.strftime('%d%m%Y')}.csv.gz",
        )


class VerifiedReferenceAcquisitionReceiptContentIdentityTests(unittest.TestCase):
    def test_codex_reproduction_status_201_bytes_vs_200_direct_construction_fails(self) -> None:
        # Externally hash-bound receipt bytes claim response_status=201 (an
        # invalid value), while direct construction supplies response_status=200
        # and every other field independently valid. This must never succeed:
        # __post_init__ independently re-parses receipt_bytes and disagrees.
        report_date = _REPORT_DATE
        receipt_bytes = _encode(_valid_receipt_dict(response_status=201))
        binding = _binding_for(receipt_bytes)
        landing_object = LandingObjectRequest(
            bucket=_BUCKET,
            object_name=_object_name(report_date),
            generation=123,
            expected_sha256=_RAW_SHA256,
            target_session=report_date,
            file_type=AcquisitionFileType.SECURITY_MASTER,
        )
        with self.assertRaises(ReferenceAcquisitionReceiptError):
            VerifiedReferenceAcquisitionReceipt(
                schema_version=1,
                receipt_bytes=receipt_bytes,
                receipt_sha256=binding.expected_receipt_sha256,
                dataset="nse-cm-mii-security",
                authority="NSE",
                acquirer_id=_ACQUIRER_ID,
                acquired_at=datetime(2026, 7, 16, 13, 30, 0, tzinfo=timezone.utc),
                report_date=report_date,
                requested_url=_requested_url(report_date),
                response_status=200,
                response_media_type="application/gzip",
                raw_byte_count=54321,
                raw_sha256=_RAW_SHA256,
                landing_object=landing_object,
                binding=binding,
            )

    def test_happy_path_verify_content_identity_succeeds(self) -> None:
        receipt_bytes, binding = _valid_pair()
        result = ReferenceAcquisitionReceiptVerifier().verify(receipt_bytes, binding)
        result.verify_content_identity()

    def test_direct_construction_mismatches_fail_even_when_independently_valid(self) -> None:
        receipt_bytes, binding = _valid_pair()
        result = ReferenceAcquisitionReceiptVerifier().verify(receipt_bytes, binding)
        base = _kwargs_from(result)
        other_landing_object = LandingObjectRequest(
            bucket=_BUCKET,
            object_name=_object_name(date(2026, 7, 17)),
            generation=123,
            expected_sha256=_RAW_SHA256,
            target_session=date(2026, 7, 17),
            file_type=AcquisitionFileType.SECURITY_MASTER,
        )
        overrides = {
            "dataset": "other-dataset",
            "authority": "BSE",
            "acquirer_id": "c" * 64,
            "acquired_at": datetime(2026, 7, 16, 14, 0, 0, tzinfo=timezone.utc),
            "report_date": date(2026, 7, 17),
            "requested_url": _requested_url(date(2026, 7, 17)),
            "response_status": 201,
            "response_media_type": "application/x-gzip",
            "raw_byte_count": 99999,
            "raw_sha256": "d" * 64,
            "landing_object": other_landing_object,
            "receipt_sha256": hashlib.sha256(b"different").hexdigest(),
            "receipt_bytes": _encode(_valid_receipt_dict(raw_byte_count=11111)),
        }
        for field, bad_value in overrides.items():
            kwargs = dict(base)
            kwargs[field] = bad_value
            with self.subTest(field=field):
                with self.assertRaises(ReferenceAcquisitionReceiptError):
                    VerifiedReferenceAcquisitionReceipt(**kwargs)


class VerifiedReferenceAcquisitionReceiptMutationTests(unittest.TestCase):
    def _fresh_result(self) -> VerifiedReferenceAcquisitionReceipt:
        receipt_bytes, binding = _valid_pair()
        return ReferenceAcquisitionReceiptVerifier().verify(receipt_bytes, binding)

    def test_mutating_top_level_field_fails_closed(self) -> None:
        mutations = {
            "schema_version": 2,
            "receipt_sha256": "e" * 64,
            "dataset": "tampered",
            "authority": "BSE",
            "acquirer_id": "f" * 64,
            "acquired_at": datetime(2026, 7, 16, 15, 0, 0, tzinfo=timezone.utc),
            "report_date": date(2026, 7, 20),
            "requested_url": _requested_url(date(2026, 7, 20)),
            "response_status": 500,
            "response_media_type": "text/plain",
            "raw_byte_count": 1,
            "raw_sha256": "1" * 64,
        }
        for field, bad_value in mutations.items():
            result = self._fresh_result()
            object.__setattr__(result, field, bad_value)
            with self.subTest(field=field):
                with self.assertRaises(ReferenceAcquisitionReceiptError):
                    result.verify_content_identity()

    def test_mutating_receipt_bytes_fails_closed(self) -> None:
        result = self._fresh_result()
        object.__setattr__(result, "receipt_bytes", result.receipt_bytes + b"x")
        with self.assertRaises(ReferenceAcquisitionReceiptError):
            result.verify_content_identity()

    def test_mutating_landing_object_field_fails_closed(self) -> None:
        mutations = {
            "bucket": "another-syntactically-valid-bucket",
            "object_name": "landing/2026-07-16/NSE_CM_security_17072026.csv.gz",
            "generation": 999,
            "expected_sha256": "2" * 64,
            "target_session": date(2026, 7, 20),
            "file_type": AcquisitionFileType.DAILY_BUNDLE,
        }
        for field, bad_value in mutations.items():
            result = self._fresh_result()
            object.__setattr__(result.landing_object, field, bad_value)
            with self.subTest(field=field):
                with self.assertRaises(ReferenceAcquisitionReceiptError):
                    result.verify_content_identity()

    def test_mutating_binding_field_fails_closed(self) -> None:
        mutations = {
            "expected_receipt_sha256": "3" * 64,
            "expected_raw_sha256": "4" * 64,
            "allowed_bucket": "another-syntactically-valid-bucket",
            "target_report_date": date(2026, 7, 20),
            "not_before": datetime(2026, 7, 16, 20, 0, 0, tzinfo=timezone.utc),
            "cutoff": datetime(2026, 7, 16, 10, 0, 0, tzinfo=timezone.utc),
            "trusted_acquirer_id": "5" * 64,
        }
        for field, bad_value in mutations.items():
            result = self._fresh_result()
            object.__setattr__(result.binding, field, bad_value)
            with self.subTest(field=field):
                with self.assertRaises(ReferenceAcquisitionReceiptError):
                    result.verify_content_identity()

    def test_mutating_not_before_earlier_while_still_valid_fails_closed(self) -> None:
        # not_before moves an hour earlier but remains an aware-UTC bound with
        # not_before <= cutoff and acquired_at still inside [not_before, cutoff]:
        # individually valid, but a silent widening of the trust boundary.
        result = self._fresh_result()
        object.__setattr__(
            result.binding, "not_before", result.binding.not_before - timedelta(hours=1)
        )
        with self.assertRaises(ReferenceAcquisitionReceiptError):
            result.verify_content_identity()

    def test_mutating_cutoff_later_while_still_valid_fails_closed(self) -> None:
        # cutoff moves an hour later but remains an aware-UTC bound with
        # not_before <= cutoff and acquired_at still inside [not_before, cutoff]:
        # individually valid, but a silent widening of the trust boundary.
        result = self._fresh_result()
        object.__setattr__(
            result.binding, "cutoff", result.binding.cutoff + timedelta(hours=1)
        )
        with self.assertRaises(ReferenceAcquisitionReceiptError):
            result.verify_content_identity()

    def test_mutating_binding_reference_fails_closed(self) -> None:
        result = self._fresh_result()
        other_binding = _binding_for(result.receipt_bytes, report_date=date(2026, 7, 20))
        object.__setattr__(result, "binding", other_binding)
        with self.assertRaises(ReferenceAcquisitionReceiptError):
            result.verify_content_identity()

    def test_mutating_landing_object_reference_fails_closed(self) -> None:
        result = self._fresh_result()
        other_landing_object = LandingObjectRequest(
            bucket=_BUCKET,
            object_name=_object_name(date(2026, 7, 17)),
            generation=123,
            expected_sha256=_RAW_SHA256,
            target_session=date(2026, 7, 17),
            file_type=AcquisitionFileType.SECURITY_MASTER,
        )
        object.__setattr__(result, "landing_object", other_landing_object)
        with self.assertRaises(ReferenceAcquisitionReceiptError):
            result.verify_content_identity()


class ReferenceAcquisitionReceiptSubclassImpostorTests(unittest.TestCase):
    def test_trusted_binding_subclass_rejected_by_verifier(self) -> None:
        class _BindingSubclass(TrustedReferenceAcquisitionBinding):
            pass

        receipt_bytes, binding = _valid_pair()
        subclass_binding = _BindingSubclass(
            expected_receipt_sha256=binding.expected_receipt_sha256,
            expected_raw_sha256=binding.expected_raw_sha256,
            allowed_bucket=binding.allowed_bucket,
            target_report_date=binding.target_report_date,
            not_before=binding.not_before,
            cutoff=binding.cutoff,
            trusted_acquirer_id=binding.trusted_acquirer_id,
        )
        with self.assertRaises(ReferenceAcquisitionReceiptError):
            ReferenceAcquisitionReceiptVerifier().verify(receipt_bytes, subclass_binding)

    def test_trusted_binding_subclass_rejected_by_direct_construction(self) -> None:
        class _BindingSubclass(TrustedReferenceAcquisitionBinding):
            pass

        receipt_bytes, binding = _valid_pair()
        result = ReferenceAcquisitionReceiptVerifier().verify(receipt_bytes, binding)
        subclass_binding = _BindingSubclass(
            expected_receipt_sha256=binding.expected_receipt_sha256,
            expected_raw_sha256=binding.expected_raw_sha256,
            allowed_bucket=binding.allowed_bucket,
            target_report_date=binding.target_report_date,
            not_before=binding.not_before,
            cutoff=binding.cutoff,
            trusted_acquirer_id=binding.trusted_acquirer_id,
        )
        kwargs = _kwargs_from(result)
        kwargs["binding"] = subclass_binding
        with self.assertRaises(ReferenceAcquisitionReceiptError):
            VerifiedReferenceAcquisitionReceipt(**kwargs)

    def test_str_subclass_field_rejected(self) -> None:
        class _StrSubclass(str):
            pass

        receipt_bytes, binding = _valid_pair()
        result = ReferenceAcquisitionReceiptVerifier().verify(receipt_bytes, binding)
        kwargs = _kwargs_from(result)
        kwargs["dataset"] = _StrSubclass(result.dataset)
        with self.assertRaises(ReferenceAcquisitionReceiptError):
            VerifiedReferenceAcquisitionReceipt(**kwargs)

    def test_bytes_subclass_receipt_input_rejected_by_verifier(self) -> None:
        class _BytesSubclass(bytes):
            pass

        receipt_bytes, binding = _valid_pair()
        with self.assertRaises(ReferenceAcquisitionReceiptError):
            ReferenceAcquisitionReceiptVerifier().verify(_BytesSubclass(receipt_bytes), binding)

    def test_bytes_subclass_receipt_bytes_rejected_by_direct_construction(self) -> None:
        class _BytesSubclass(bytes):
            pass

        receipt_bytes, binding = _valid_pair()
        result = ReferenceAcquisitionReceiptVerifier().verify(receipt_bytes, binding)
        kwargs = _kwargs_from(result)
        kwargs["receipt_bytes"] = _BytesSubclass(receipt_bytes)
        with self.assertRaises(ReferenceAcquisitionReceiptError):
            VerifiedReferenceAcquisitionReceipt(**kwargs)

    def test_int_subclass_field_rejected(self) -> None:
        class _IntSubclass(int):
            pass

        receipt_bytes, binding = _valid_pair()
        result = ReferenceAcquisitionReceiptVerifier().verify(receipt_bytes, binding)
        kwargs = _kwargs_from(result)
        kwargs["response_status"] = _IntSubclass(result.response_status)
        with self.assertRaises(ReferenceAcquisitionReceiptError):
            VerifiedReferenceAcquisitionReceipt(**kwargs)

    def test_datetime_subclass_field_rejected(self) -> None:
        class _DatetimeSubclass(datetime):
            pass

        receipt_bytes, binding = _valid_pair()
        result = ReferenceAcquisitionReceiptVerifier().verify(receipt_bytes, binding)
        kwargs = _kwargs_from(result)
        kwargs["acquired_at"] = _DatetimeSubclass(
            result.acquired_at.year,
            result.acquired_at.month,
            result.acquired_at.day,
            result.acquired_at.hour,
            result.acquired_at.minute,
            result.acquired_at.second,
            tzinfo=timezone.utc,
        )
        with self.assertRaises(ReferenceAcquisitionReceiptError):
            VerifiedReferenceAcquisitionReceipt(**kwargs)

    def test_date_datetime_confusion_on_report_date_rejected(self) -> None:
        receipt_bytes, binding = _valid_pair()
        result = ReferenceAcquisitionReceiptVerifier().verify(receipt_bytes, binding)
        kwargs = _kwargs_from(result)
        kwargs["report_date"] = datetime(2026, 7, 16, tzinfo=timezone.utc)
        with self.assertRaises(ReferenceAcquisitionReceiptError):
            VerifiedReferenceAcquisitionReceipt(**kwargs)

    def test_landing_object_subclass_rejected(self) -> None:
        class _LandingObjectSubclass(LandingObjectRequest):
            pass

        receipt_bytes, binding = _valid_pair()
        result = ReferenceAcquisitionReceiptVerifier().verify(receipt_bytes, binding)
        subclass_landing_object = _LandingObjectSubclass(
            bucket=result.landing_object.bucket,
            object_name=result.landing_object.object_name,
            generation=result.landing_object.generation,
            expected_sha256=result.landing_object.expected_sha256,
            target_session=result.landing_object.target_session,
            file_type=result.landing_object.file_type,
        )
        kwargs = _kwargs_from(result)
        kwargs["landing_object"] = subclass_landing_object
        with self.assertRaises(ReferenceAcquisitionReceiptError):
            VerifiedReferenceAcquisitionReceipt(**kwargs)


if __name__ == "__main__":
    unittest.main()
