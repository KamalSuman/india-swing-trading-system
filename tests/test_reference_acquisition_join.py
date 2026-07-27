from __future__ import annotations

import csv
import dataclasses
import gzip
import hashlib
import io
import json
import unittest
from datetime import date, datetime, timedelta, timezone

from india_swing.daily_pipeline.acquisition import (
    AcquiredFile,
    AcquisitionFileType,
    GCSLandingObjectReader,
    GCSObjectPayload,
)
from india_swing.reference_data.acquisition_join import (
    REFERENCE_ACQUISITION_JOIN_SCHEMA_VERSION,
    ReferenceAcquisitionJoinError,
    ReferenceAcquisitionJoinService,
    VerifiedReferenceAcquisitionJoin,
    _build_join_facts,
)
from india_swing.reference_data.acquisition_receipt import (
    ReferenceAcquisitionReceiptVerifier,
    TrustedReferenceAcquisitionBinding,
    VerifiedReferenceAcquisitionReceipt,
)
from india_swing.reference_data.security_master import NSE_CM_MII_SECURITY_HEADER

_REPORT_DATE = date(2026, 7, 16)
_BUCKET = "trusted-bucket"
_ACQUIRER_ID = "a" * 64
_NOT_BEFORE = datetime(2026, 7, 16, 0, 0, 0, tzinfo=timezone.utc)
_CUTOFF = datetime(2026, 7, 16, 23, 59, 59, tzinfo=timezone.utc)
_ACQUIRED_AT = "2026-07-16T13:30:00Z"
_GENERATION = 123
_MAXIMUM_RAW_BYTES = 32 * 1024 * 1024


def _object_name(report_date: date = _REPORT_DATE) -> str:
    return f"landing/{report_date.isoformat()}/NSE_CM_security_{report_date.strftime('%d%m%Y')}.csv.gz"


def _requested_url(report_date: date = _REPORT_DATE) -> str:
    return (
        "https://nsearchives.nseindia.com/content/cm/"
        f"NSE_CM_security_{report_date.strftime('%d%m%Y')}.csv.gz"
    )


def _security_master_row(**overrides: str) -> dict[str, str]:
    values: dict[str, str] = {name: "" for name in NSE_CM_MII_SECURITY_HEADER}
    values.update(
        {
            "FinInstrmId": "12345",
            "TckrSymb": "RELIANCE",
            "SctySrs": "EQ",
            "FinInstrmNm": "RELIANCE INDUSTRIES",
            "ISIN": "INE002A01018",
            "NewBrdLotQty": "1",
            "SctyTpFlg": "0",
            "BidIntrvl": "5",
            "CallAuctnInd": "0",
            "PrtdToTrad": "1",
            "SctyStsNrmlMkt": "1",
            "ElgbltyNrmlMkt": "1",
            "SctyStsOddLotMkt": "1",
            "ElgbltyOddLotMkt": "1",
            "SctyStsRETDBTMkt": "1",
            "ElgbltyRETDBTMkt": "1",
            "SctyStsAuctnMkt": "1",
            "ElgbltyAuctnMkt": "1",
            "SctyStsAddtlMkt1": "1",
            "ElgbltyAddtlMkt1": "1",
            "SctyStsAddtlMkt2": "1",
            "ElgbltyAddtlMkt2": "1",
            "ListgDt": "1000",
            "RmvlDt": "0",
            "RadmssnDt": "0",
            "DelFlg": "N",
        }
    )
    values.update(overrides)
    return values


def _csv_bytes(rows: list[dict[str, str]], *, header: tuple[str, ...] = NSE_CM_MII_SECURITY_HEADER) -> bytes:
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(header)
    for row in rows:
        writer.writerow([row[name] for name in NSE_CM_MII_SECURITY_HEADER])
    return buf.getvalue().encode("utf-8")


def _security_master_gzip(*, rows: list[dict[str, str]] | None = None) -> bytes:
    if rows is None:
        rows = [_security_master_row()]
    return gzip.compress(_csv_bytes(rows))


def _valid_receipt_dict(
    *,
    report_date: date = _REPORT_DATE,
    bucket: str = _BUCKET,
    acquirer_id: str = _ACQUIRER_ID,
    acquired_at: str = _ACQUIRED_AT,
    raw_sha256: str,
    raw_byte_count: int,
    generation: int = _GENERATION,
) -> dict[str, object]:
    return {
        "schema_version": 1,
        "dataset": "nse-cm-mii-security",
        "authority": "NSE",
        "acquirer_id": acquirer_id,
        "acquired_at": acquired_at,
        "report_date": report_date.isoformat(),
        "requested_url": _requested_url(report_date),
        "response_status": 200,
        "response_media_type": "application/gzip",
        "raw_byte_count": raw_byte_count,
        "raw_sha256": raw_sha256,
        "landing_object": {
            "file_type": "SECURITY_MASTER",
            "bucket": bucket,
            "object_name": _object_name(report_date),
            "generation": generation,
            "sha256": raw_sha256,
        },
    }


def _encode(receipt: dict[str, object]) -> bytes:
    return json.dumps(receipt, separators=(",", ":")).encode("utf-8")


def _binding_for(
    receipt_bytes: bytes,
    *,
    raw_sha256: str,
    bucket: str = _BUCKET,
    report_date: date = _REPORT_DATE,
    acquirer_id: str = _ACQUIRER_ID,
    not_before: datetime = _NOT_BEFORE,
    cutoff: datetime = _CUTOFF,
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


def _valid_receipt(
    *,
    report_date: date = _REPORT_DATE,
    gz_bytes: bytes | None = None,
    bucket: str = _BUCKET,
    generation: int = _GENERATION,
    acquirer_id: str = _ACQUIRER_ID,
) -> tuple[VerifiedReferenceAcquisitionReceipt, bytes]:
    if gz_bytes is None:
        gz_bytes = _security_master_gzip()
    raw_sha256 = hashlib.sha256(gz_bytes).hexdigest()
    receipt_dict = _valid_receipt_dict(
        report_date=report_date,
        bucket=bucket,
        acquirer_id=acquirer_id,
        raw_sha256=raw_sha256,
        raw_byte_count=len(gz_bytes),
        generation=generation,
    )
    receipt_bytes = _encode(receipt_dict)
    binding = _binding_for(
        receipt_bytes,
        raw_sha256=raw_sha256,
        bucket=bucket,
        report_date=report_date,
        acquirer_id=acquirer_id,
    )
    receipt = ReferenceAcquisitionReceiptVerifier().verify(receipt_bytes, binding)
    return receipt, gz_bytes


def _receipt_for_raw_bytes(
    raw_bytes: bytes, *, report_date: date = _REPORT_DATE
) -> VerifiedReferenceAcquisitionReceipt:
    raw_sha256 = hashlib.sha256(raw_bytes).hexdigest()
    receipt_dict = _valid_receipt_dict(
        report_date=report_date, raw_sha256=raw_sha256, raw_byte_count=len(raw_bytes)
    )
    receipt_bytes = _encode(receipt_dict)
    binding = _binding_for(receipt_bytes, raw_sha256=raw_sha256, report_date=report_date)
    return ReferenceAcquisitionReceiptVerifier().verify(receipt_bytes, binding)


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


class RaisingFakeGCSObjectReader:
    """Fake GCSObjectReader whose read_generation always raises a caller-supplied
    ordinary exception, used to prove nested exception text/type never leaks.
    """

    def __init__(self, *, error: BaseException) -> None:
        self._error = error
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
        raise self._error


def _service_for(
    receipt: VerifiedReferenceAcquisitionReceipt, gz_bytes: bytes
) -> tuple[ReferenceAcquisitionJoinService, FakeGCSObjectReader]:
    fake = FakeGCSObjectReader(
        generation=receipt.landing_object.generation, content_bytes=gz_bytes
    )
    reader = GCSLandingObjectReader(fake)
    return ReferenceAcquisitionJoinService(reader), fake


def _valid_join() -> VerifiedReferenceAcquisitionJoin:
    receipt, gz_bytes = _valid_receipt()
    service, _ = _service_for(receipt, gz_bytes)
    return service.join(receipt)


def _kwargs_from(join: VerifiedReferenceAcquisitionJoin) -> dict[str, object]:
    return {
        "schema_version": join.schema_version,
        "receipt": join.receipt,
        "acquired_file": join.acquired_file,
        "parsed": join.parsed,
        "join_id": join.join_id,
    }


def _acquired_file_with(join: VerifiedReferenceAcquisitionJoin, **overrides: object) -> AcquiredFile:
    values: dict[str, object] = {
        "bucket": join.acquired_file.bucket,
        "object_name": join.acquired_file.object_name,
        "generation": join.acquired_file.generation,
        "target_session": join.acquired_file.target_session,
        "file_type": join.acquired_file.file_type,
        "content_bytes": join.acquired_file.content_bytes,
        "sha256_hash": join.acquired_file.sha256_hash,
    }
    values.update(overrides)
    return AcquiredFile(**values)


class ReferenceAcquisitionJoinAcceptanceTests(unittest.TestCase):
    def test_happy_path_reads_exactly_once_and_matches_all_facts(self) -> None:
        receipt, gz_bytes = _valid_receipt()
        service, fake = _service_for(receipt, gz_bytes)

        join = service.join(receipt)

        self.assertEqual(len(fake.calls), 1)
        self.assertEqual(
            fake.calls[0],
            {
                "bucket": _BUCKET,
                "object_name": _object_name(),
                "generation": _GENERATION,
                "maximum_bytes": _MAXIMUM_RAW_BYTES,
            },
        )
        self.assertEqual(join.schema_version, REFERENCE_ACQUISITION_JOIN_SCHEMA_VERSION)
        self.assertIs(join.receipt, receipt)
        self.assertEqual(join.acquired_file.bucket, _BUCKET)
        self.assertEqual(join.acquired_file.object_name, _object_name())
        self.assertEqual(join.acquired_file.generation, _GENERATION)
        self.assertEqual(join.acquired_file.content_bytes, gz_bytes)
        self.assertEqual(join.acquired_file.sha256_hash, receipt.raw_sha256)
        self.assertEqual(join.parsed.raw_sha256, receipt.raw_sha256)
        self.assertEqual(join.parsed.claimed_report_date, _REPORT_DATE)
        self.assertEqual(len(join.parsed.records), 1)
        self.assertEqual(join.parsed.excluded_alternative_venue_count, 0)
        self.assertEqual(len(join.join_id), 64)
        join.verify_content_identity()

    def test_no_readiness_actionable_or_trading_field_exists(self) -> None:
        join = _valid_join()
        field_names = {field.name for field in dataclasses.fields(join)}
        for banned in (
            "readiness",
            "actionable",
            "verified_report_date",
            "acquisition_mode",
            "promotion",
            "signal",
            "recommendation",
            "notification",
            "order",
            "broker",
            "capital",
        ):
            self.assertNotIn(banned, field_names)

    def test_returned_dataclass_is_frozen(self) -> None:
        join = _valid_join()
        with self.assertRaises(dataclasses.FrozenInstanceError):
            join.join_id = "x"  # type: ignore[misc]


class ReferenceAcquisitionJoinReceiptGateTests(unittest.TestCase):
    def test_invalid_receipt_type_causes_zero_reads(self) -> None:
        receipt, gz_bytes = _valid_receipt()
        service, fake = _service_for(receipt, gz_bytes)
        with self.assertRaises(ReferenceAcquisitionJoinError):
            service.join("not-a-receipt")  # type: ignore[arg-type]
        self.assertEqual(fake.calls, [])

    def test_mutated_receipt_causes_zero_reads(self) -> None:
        receipt, gz_bytes = _valid_receipt()
        service, fake = _service_for(receipt, gz_bytes)
        object.__setattr__(receipt, "response_status", 201)
        with self.assertRaises(ReferenceAcquisitionJoinError):
            service.join(receipt)
        self.assertEqual(fake.calls, [])

    def test_mutated_receipt_binding_causes_zero_reads(self) -> None:
        receipt, gz_bytes = _valid_receipt()
        service, fake = _service_for(receipt, gz_bytes)
        object.__setattr__(
            receipt.binding, "not_before", receipt.binding.not_before - timedelta(hours=1)
        )
        with self.assertRaises(ReferenceAcquisitionJoinError):
            service.join(receipt)
        self.assertEqual(fake.calls, [])


class ReferenceAcquisitionJoinServiceRejectionTests(unittest.TestCase):
    def test_wrong_generation_returned_by_reader_fails(self) -> None:
        receipt, gz_bytes = _valid_receipt()
        service, fake = _service_for(receipt, gz_bytes)
        fake.generation = 999
        with self.assertRaises(ReferenceAcquisitionJoinError):
            service.join(receipt)

    def test_tampered_raw_bytes_returned_by_reader_fails(self) -> None:
        receipt, gz_bytes = _valid_receipt()
        service, fake = _service_for(receipt, gz_bytes)
        fake.content_bytes = gz_bytes + b"tampered"
        with self.assertRaises(ReferenceAcquisitionJoinError):
            service.join(receipt)

    def test_empty_bytes_returned_by_reader_fails(self) -> None:
        receipt, gz_bytes = _valid_receipt()
        service, fake = _service_for(receipt, gz_bytes)
        fake.content_bytes = b""
        with self.assertRaises(ReferenceAcquisitionJoinError):
            service.join(receipt)

    def test_non_bytes_payload_from_reader_fails(self) -> None:
        receipt, gz_bytes = _valid_receipt()
        service, fake = _service_for(receipt, gz_bytes)
        fake.content_bytes = "not-bytes"  # type: ignore[assignment]
        with self.assertRaises(ReferenceAcquisitionJoinError):
            service.join(receipt)

    def test_reader_exception_is_translated_to_sanitized_error(self) -> None:
        receipt, _ = _valid_receipt()
        fake = RaisingFakeGCSObjectReader(error=RuntimeError("boom"))
        service = ReferenceAcquisitionJoinService(GCSLandingObjectReader(fake))
        with self.assertRaises(ReferenceAcquisitionJoinError):
            service.join(receipt)
        self.assertEqual(len(fake.calls), 1)


class ReferenceAcquisitionJoinParseRejectionTests(unittest.TestCase):
    def test_malformed_gzip_fails(self) -> None:
        raw_bytes = b"not-a-gzip-stream-at-all"
        receipt = _receipt_for_raw_bytes(raw_bytes)
        service, _ = _service_for(receipt, raw_bytes)
        with self.assertRaises(ReferenceAcquisitionJoinError):
            service.join(receipt)

    def test_truncated_gzip_fails(self) -> None:
        full = _security_master_gzip()
        truncated = full[:-4]
        receipt = _receipt_for_raw_bytes(truncated)
        service, _ = _service_for(receipt, truncated)
        with self.assertRaises(ReferenceAcquisitionJoinError):
            service.join(receipt)

    def test_wrong_header_fails(self) -> None:
        bad_header = ("WRONG",) + NSE_CM_MII_SECURITY_HEADER[1:]
        bad_gz = gzip.compress(_csv_bytes([_security_master_row()], header=bad_header))
        receipt = _receipt_for_raw_bytes(bad_gz)
        service, _ = _service_for(receipt, bad_gz)
        with self.assertRaises(ReferenceAcquisitionJoinError):
            service.join(receipt)

    def test_alternative_venue_content_is_rejected(self) -> None:
        gz_bytes = _security_master_gzip(rows=[_security_master_row(PrtdToTrad="2")])
        receipt, _ = _valid_receipt(gz_bytes=gz_bytes)
        service, _ = _service_for(receipt, gz_bytes)
        with self.assertRaises(ReferenceAcquisitionJoinError):
            service.join(receipt)


class VerifiedReferenceAcquisitionJoinLineageRejectionTests(unittest.TestCase):
    def test_wrong_bucket_in_acquired_file_is_rejected(self) -> None:
        join = _valid_join()
        bad_file = _acquired_file_with(join, bucket="another-syntactically-valid-bucket")
        kwargs = _kwargs_from(join)
        kwargs["acquired_file"] = bad_file
        with self.assertRaises(ReferenceAcquisitionJoinError):
            VerifiedReferenceAcquisitionJoin(**kwargs)

    def test_wrong_object_path_session_in_acquired_file_is_rejected(self) -> None:
        join = _valid_join()
        bad_file = _acquired_file_with(
            join, object_name="landing/2026-07-17/NSE_CM_security_17072026.csv.gz"
        )
        kwargs = _kwargs_from(join)
        kwargs["acquired_file"] = bad_file
        with self.assertRaises(ReferenceAcquisitionJoinError):
            VerifiedReferenceAcquisitionJoin(**kwargs)

    def test_wrong_generation_in_acquired_file_is_rejected(self) -> None:
        join = _valid_join()
        bad_file = _acquired_file_with(join, generation=999)
        kwargs = _kwargs_from(join)
        kwargs["acquired_file"] = bad_file
        with self.assertRaises(ReferenceAcquisitionJoinError):
            VerifiedReferenceAcquisitionJoin(**kwargs)

    def test_wrong_file_type_in_acquired_file_is_rejected(self) -> None:
        join = _valid_join()
        bad_file = _acquired_file_with(join, file_type=AcquisitionFileType.DAILY_BUNDLE)
        kwargs = _kwargs_from(join)
        kwargs["acquired_file"] = bad_file
        with self.assertRaises(ReferenceAcquisitionJoinError):
            VerifiedReferenceAcquisitionJoin(**kwargs)

    def test_receipt_raw_byte_count_disagreement_is_rejected(self) -> None:
        join = _valid_join()
        other_bytes = _security_master_gzip(
            rows=[
                _security_master_row(FinInstrmId="99999", TckrSymb="TCS"),
                _security_master_row(FinInstrmId="88888", TckrSymb="INFY"),
            ]
        )
        self.assertNotEqual(len(other_bytes), len(join.acquired_file.content_bytes))
        bad_file = _acquired_file_with(
            join,
            content_bytes=other_bytes,
            sha256_hash=hashlib.sha256(other_bytes).hexdigest(),
        )
        kwargs = _kwargs_from(join)
        kwargs["acquired_file"] = bad_file
        with self.assertRaises(ReferenceAcquisitionJoinError):
            VerifiedReferenceAcquisitionJoin(**kwargs)

    def test_receipt_raw_hash_disagreement_is_rejected(self) -> None:
        join = _valid_join()
        tampered = bytearray(join.acquired_file.content_bytes)
        tampered[-1] ^= 0x01
        tampered_bytes = bytes(tampered)
        self.assertEqual(len(tampered_bytes), len(join.acquired_file.content_bytes))
        bad_file = _acquired_file_with(
            join,
            content_bytes=tampered_bytes,
            sha256_hash=hashlib.sha256(tampered_bytes).hexdigest(),
        )
        kwargs = _kwargs_from(join)
        kwargs["acquired_file"] = bad_file
        with self.assertRaises(ReferenceAcquisitionJoinError):
            VerifiedReferenceAcquisitionJoin(**kwargs)


class BuildJoinFactsFilenameTests(unittest.TestCase):
    def test_filename_disagreement_with_report_date_is_rejected(self) -> None:
        # LandingObjectRequest's own construction-time validation always ties
        # object_name to the receipt's report_date, so this state cannot arise
        # through the public API; it is exercised here as a direct, isolated
        # test of _build_join_facts's own independent defense-in-depth check.
        receipt, gz_bytes = _valid_receipt()
        wrong_object_name = _object_name(date(2026, 7, 17))
        object.__setattr__(receipt.landing_object, "object_name", wrong_object_name)
        acquired_file = AcquiredFile(
            bucket=_BUCKET,
            object_name=wrong_object_name,
            generation=_GENERATION,
            target_session=_REPORT_DATE,
            file_type=AcquisitionFileType.SECURITY_MASTER,
            content_bytes=gz_bytes,
            sha256_hash=hashlib.sha256(gz_bytes).hexdigest(),
        )
        with self.assertRaises(ReferenceAcquisitionJoinError):
            _build_join_facts(receipt, acquired_file)


class VerifiedReferenceAcquisitionJoinDirectConstructionMismatchTests(unittest.TestCase):
    def test_replacing_receipt_with_a_different_valid_receipt_fails(self) -> None:
        join = _valid_join()
        other_receipt, _ = _valid_receipt(generation=999)
        kwargs = _kwargs_from(join)
        kwargs["receipt"] = other_receipt
        with self.assertRaises(ReferenceAcquisitionJoinError):
            VerifiedReferenceAcquisitionJoin(**kwargs)

    def test_replacing_acquired_file_with_a_different_valid_one_fails(self) -> None:
        join = _valid_join()
        other_receipt, other_gz = _valid_receipt(generation=888)
        other_service, _ = _service_for(other_receipt, other_gz)
        other_join = other_service.join(other_receipt)
        kwargs = _kwargs_from(join)
        kwargs["acquired_file"] = other_join.acquired_file
        with self.assertRaises(ReferenceAcquisitionJoinError):
            VerifiedReferenceAcquisitionJoin(**kwargs)

    def test_replacing_parsed_with_a_different_valid_one_fails(self) -> None:
        join = _valid_join()
        other_gz = _security_master_gzip(
            rows=[_security_master_row(FinInstrmId="55555", TckrSymb="INFY")]
        )
        other_receipt, _ = _valid_receipt(gz_bytes=other_gz, generation=777)
        other_service, _ = _service_for(other_receipt, other_gz)
        other_join = other_service.join(other_receipt)
        kwargs = _kwargs_from(join)
        kwargs["parsed"] = other_join.parsed
        with self.assertRaises(ReferenceAcquisitionJoinError):
            VerifiedReferenceAcquisitionJoin(**kwargs)

    def test_replacing_schema_marker_fails(self) -> None:
        join = _valid_join()
        kwargs = _kwargs_from(join)
        kwargs["schema_version"] = "reference-acquisition-join/v2"
        with self.assertRaises(ReferenceAcquisitionJoinError):
            VerifiedReferenceAcquisitionJoin(**kwargs)

    def test_replacing_join_id_fails(self) -> None:
        join = _valid_join()
        kwargs = _kwargs_from(join)
        kwargs["join_id"] = hashlib.sha256(b"different").hexdigest()
        with self.assertRaises(ReferenceAcquisitionJoinError):
            VerifiedReferenceAcquisitionJoin(**kwargs)


class VerifiedReferenceAcquisitionJoinMutationTests(unittest.TestCase):
    def test_mutating_top_level_schema_version_fails_closed(self) -> None:
        join = _valid_join()
        object.__setattr__(join, "schema_version", "other")
        with self.assertRaises(ReferenceAcquisitionJoinError):
            join.verify_content_identity()

    def test_mutating_top_level_join_id_fails_closed(self) -> None:
        join = _valid_join()
        object.__setattr__(join, "join_id", "0" * 64)
        with self.assertRaises(ReferenceAcquisitionJoinError):
            join.verify_content_identity()

    def test_mutating_receipt_field_fails_closed(self) -> None:
        join = _valid_join()
        object.__setattr__(join.receipt, "response_status", 201)
        with self.assertRaises(ReferenceAcquisitionJoinError):
            join.verify_content_identity()

    def test_mutating_receipt_binding_field_fails_closed(self) -> None:
        join = _valid_join()
        object.__setattr__(
            join.receipt.binding, "allowed_bucket", "another-syntactically-valid-bucket"
        )
        with self.assertRaises(ReferenceAcquisitionJoinError):
            join.verify_content_identity()

    def test_mutating_receipt_landing_object_field_fails_closed(self) -> None:
        join = _valid_join()
        object.__setattr__(join.receipt.landing_object, "generation", 999)
        with self.assertRaises(ReferenceAcquisitionJoinError):
            join.verify_content_identity()

    def test_mutating_acquired_file_field_fails_closed(self) -> None:
        join = _valid_join()
        object.__setattr__(join.acquired_file, "bucket", "another-syntactically-valid-bucket")
        with self.assertRaises(ReferenceAcquisitionJoinError):
            join.verify_content_identity()

    def test_mutating_parsed_field_fails_closed(self) -> None:
        join = _valid_join()
        object.__setattr__(join.parsed, "excluded_alternative_venue_count", 1)
        with self.assertRaises(ReferenceAcquisitionJoinError):
            join.verify_content_identity()

    def test_mutating_nested_parsed_record_field_fails_closed(self) -> None:
        join = _valid_join()
        record = join.parsed.records[0]
        object.__setattr__(record, "ticker_symbol", "TAMPERED")
        with self.assertRaises(ReferenceAcquisitionJoinError):
            join.verify_content_identity()


class ReferenceAcquisitionJoinSubclassImpostorTests(unittest.TestCase):
    def test_verified_receipt_subclass_rejected_by_service(self) -> None:
        class _ReceiptSubclass(VerifiedReferenceAcquisitionReceipt):
            pass

        receipt, gz_bytes = _valid_receipt()
        subclass_receipt = _ReceiptSubclass(
            schema_version=receipt.schema_version,
            receipt_bytes=receipt.receipt_bytes,
            receipt_sha256=receipt.receipt_sha256,
            dataset=receipt.dataset,
            authority=receipt.authority,
            acquirer_id=receipt.acquirer_id,
            acquired_at=receipt.acquired_at,
            report_date=receipt.report_date,
            requested_url=receipt.requested_url,
            response_status=receipt.response_status,
            response_media_type=receipt.response_media_type,
            raw_byte_count=receipt.raw_byte_count,
            raw_sha256=receipt.raw_sha256,
            landing_object=receipt.landing_object,
            binding=receipt.binding,
        )
        service, fake = _service_for(receipt, gz_bytes)
        with self.assertRaises(ReferenceAcquisitionJoinError):
            service.join(subclass_receipt)
        self.assertEqual(fake.calls, [])

    def test_acquired_file_subclass_rejected_by_direct_construction(self) -> None:
        class _AcquiredFileSubclass(AcquiredFile):
            pass

        join = _valid_join()
        subclass_file = _AcquiredFileSubclass(
            bucket=join.acquired_file.bucket,
            object_name=join.acquired_file.object_name,
            generation=join.acquired_file.generation,
            target_session=join.acquired_file.target_session,
            file_type=join.acquired_file.file_type,
            content_bytes=join.acquired_file.content_bytes,
            sha256_hash=join.acquired_file.sha256_hash,
        )
        kwargs = _kwargs_from(join)
        kwargs["acquired_file"] = subclass_file
        with self.assertRaises(ReferenceAcquisitionJoinError):
            VerifiedReferenceAcquisitionJoin(**kwargs)

    def test_parsed_subclass_rejected_by_direct_construction(self) -> None:
        from india_swing.reference_data.models import ParsedNseCmSecurityMaster

        class _ParsedSubclass(ParsedNseCmSecurityMaster):
            pass

        join = _valid_join()
        subclass_parsed = _ParsedSubclass(
            **{field.name: getattr(join.parsed, field.name) for field in dataclasses.fields(join.parsed)}
        )
        kwargs = _kwargs_from(join)
        kwargs["parsed"] = subclass_parsed
        with self.assertRaises(ReferenceAcquisitionJoinError):
            VerifiedReferenceAcquisitionJoin(**kwargs)

    def test_gcs_landing_object_reader_subclass_rejected_by_service_init(self) -> None:
        class _ReaderSubclass(GCSLandingObjectReader):
            pass

        fake = FakeGCSObjectReader(generation=_GENERATION, content_bytes=b"x")
        subclass_reader = _ReaderSubclass(fake)
        with self.assertRaises(ReferenceAcquisitionJoinError):
            ReferenceAcquisitionJoinService(subclass_reader)

    def test_gcs_landing_object_reader_impostor_rejected_by_service_init(self) -> None:
        class _NotAReader:
            def read(self, request: object) -> None:
                raise AssertionError("should never be called")

        with self.assertRaises(ReferenceAcquisitionJoinError):
            ReferenceAcquisitionJoinService(_NotAReader())  # type: ignore[arg-type]

    def test_bytes_subclass_content_fails_via_direct_construction(self) -> None:
        class _BytesSubclass(bytes):
            pass

        join = _valid_join()
        bad_file = _acquired_file_with(
            join, content_bytes=_BytesSubclass(join.acquired_file.content_bytes)
        )
        kwargs = _kwargs_from(join)
        kwargs["acquired_file"] = bad_file
        with self.assertRaises(ReferenceAcquisitionJoinError):
            VerifiedReferenceAcquisitionJoin(**kwargs)

    def test_str_subclass_join_id_fails(self) -> None:
        class _StrSubclass(str):
            pass

        join = _valid_join()
        kwargs = _kwargs_from(join)
        kwargs["join_id"] = _StrSubclass(join.join_id)
        with self.assertRaises(ReferenceAcquisitionJoinError):
            VerifiedReferenceAcquisitionJoin(**kwargs)

    def test_int_subclass_generation_fails(self) -> None:
        class _IntSubclass(int):
            pass

        join = _valid_join()
        bad_file = _acquired_file_with(join, generation=_IntSubclass(join.acquired_file.generation))
        kwargs = _kwargs_from(join)
        kwargs["acquired_file"] = bad_file
        with self.assertRaises(ReferenceAcquisitionJoinError):
            VerifiedReferenceAcquisitionJoin(**kwargs)

    def test_datetime_target_session_confusion_fails(self) -> None:
        join = _valid_join()
        bad_file = _acquired_file_with(
            join, target_session=datetime(2026, 7, 16, tzinfo=timezone.utc)
        )
        kwargs = _kwargs_from(join)
        kwargs["acquired_file"] = bad_file
        with self.assertRaises(ReferenceAcquisitionJoinError):
            VerifiedReferenceAcquisitionJoin(**kwargs)


class ReferenceAcquisitionJoinContentIdCompletenessTests(unittest.TestCase):
    def test_equal_semantic_input_produces_same_join_id(self) -> None:
        gz_bytes = _security_master_gzip()
        receipt_a, _ = _valid_receipt(gz_bytes=gz_bytes)
        service_a, _ = _service_for(receipt_a, gz_bytes)
        join_a = service_a.join(receipt_a)

        receipt_b, _ = _valid_receipt(gz_bytes=gz_bytes)
        service_b, _ = _service_for(receipt_b, gz_bytes)
        join_b = service_b.join(receipt_b)

        self.assertEqual(join_a.join_id, join_b.join_id)

    def test_different_generation_changes_join_id(self) -> None:
        gz_bytes = _security_master_gzip()
        receipt_a, _ = _valid_receipt(generation=100, gz_bytes=gz_bytes)
        service_a, _ = _service_for(receipt_a, gz_bytes)
        join_a = service_a.join(receipt_a)

        receipt_b, _ = _valid_receipt(generation=200, gz_bytes=gz_bytes)
        service_b, _ = _service_for(receipt_b, gz_bytes)
        join_b = service_b.join(receipt_b)

        self.assertNotEqual(join_a.join_id, join_b.join_id)

    def test_different_bucket_changes_join_id(self) -> None:
        other_bucket = "another-syntactically-valid-bucket"
        gz_bytes = _security_master_gzip()
        receipt_a, _ = _valid_receipt(gz_bytes=gz_bytes)
        service_a, _ = _service_for(receipt_a, gz_bytes)
        join_a = service_a.join(receipt_a)

        receipt_b, _ = _valid_receipt(bucket=other_bucket, gz_bytes=gz_bytes)
        service_b, _ = _service_for(receipt_b, gz_bytes)
        join_b = service_b.join(receipt_b)

        self.assertNotEqual(join_a.join_id, join_b.join_id)

    def test_different_content_changes_join_id(self) -> None:
        gz_a = _security_master_gzip()
        receipt_a, _ = _valid_receipt(gz_bytes=gz_a)
        service_a, _ = _service_for(receipt_a, gz_a)
        join_a = service_a.join(receipt_a)

        gz_b = _security_master_gzip(
            rows=[_security_master_row(FinInstrmId="99999", TckrSymb="TCS")]
        )
        receipt_b, _ = _valid_receipt(gz_bytes=gz_b)
        service_b, _ = _service_for(receipt_b, gz_b)
        join_b = service_b.join(receipt_b)

        self.assertNotEqual(join_a.join_id, join_b.join_id)

    def test_different_acquirer_changes_join_id(self) -> None:
        other_acquirer = "b" * 64
        gz_bytes = _security_master_gzip()
        receipt_a, _ = _valid_receipt(gz_bytes=gz_bytes)
        service_a, _ = _service_for(receipt_a, gz_bytes)
        join_a = service_a.join(receipt_a)

        receipt_b, _ = _valid_receipt(gz_bytes=gz_bytes, acquirer_id=other_acquirer)
        service_b, _ = _service_for(receipt_b, gz_bytes)
        join_b = service_b.join(receipt_b)

        self.assertNotEqual(join_a.join_id, join_b.join_id)

    def test_different_report_date_changes_join_id(self) -> None:
        gz_bytes = _security_master_gzip()
        receipt_a, _ = _valid_receipt(gz_bytes=gz_bytes)
        service_a, _ = _service_for(receipt_a, gz_bytes)
        join_a = service_a.join(receipt_a)

        earlier_date = date(2026, 7, 15)
        gz_b = _security_master_gzip()
        raw_sha256 = hashlib.sha256(gz_b).hexdigest()
        receipt_dict = _valid_receipt_dict(
            report_date=earlier_date,
            raw_sha256=raw_sha256,
            raw_byte_count=len(gz_b),
            acquired_at="2026-07-15T13:30:00Z",
        )
        receipt_bytes = _encode(receipt_dict)
        binding = _binding_for(
            receipt_bytes,
            raw_sha256=raw_sha256,
            report_date=earlier_date,
            not_before=datetime(2026, 7, 15, 0, 0, 0, tzinfo=timezone.utc),
            cutoff=datetime(2026, 7, 15, 23, 59, 59, tzinfo=timezone.utc),
        )
        receipt_b = ReferenceAcquisitionReceiptVerifier().verify(receipt_bytes, binding)
        service_b, _ = _service_for(receipt_b, gz_b)
        join_b = service_b.join(receipt_b)

        self.assertNotEqual(join_a.join_id, join_b.join_id)


class ReferenceAcquisitionJoinSanitizationTests(unittest.TestCase):
    def test_secret_in_raising_reader_exception_never_appears_in_error(self) -> None:
        receipt, _ = _valid_receipt()
        secret = "SECRET-READER-do-not-leak-9f3a"
        fake = RaisingFakeGCSObjectReader(error=RuntimeError(f"boom {secret}"))
        service = ReferenceAcquisitionJoinService(GCSLandingObjectReader(fake))
        with self.assertRaises(ReferenceAcquisitionJoinError) as ctx:
            service.join(receipt)
        message = str(ctx.exception)
        self.assertNotIn(secret, message)
        self.assertNotIn("RuntimeError", message)
        self.assertNotIn("boom", message)

    def test_secret_in_malformed_content_never_appears_in_error(self) -> None:
        secret_bytes = b"SECRET-GZIP-do-not-leak-71cd"
        receipt = _receipt_for_raw_bytes(secret_bytes)
        service, _ = _service_for(receipt, secret_bytes)
        with self.assertRaises(ReferenceAcquisitionJoinError) as ctx:
            service.join(receipt)
        message = str(ctx.exception)
        self.assertNotIn(secret_bytes.decode(), message)

    def test_secret_in_wrong_bucket_never_appears_in_error(self) -> None:
        secret_bucket = "secret-do-not-leak-bucket-value"
        join = _valid_join()
        bad_file = _acquired_file_with(join, bucket=secret_bucket)
        kwargs = _kwargs_from(join)
        kwargs["acquired_file"] = bad_file
        with self.assertRaises(ReferenceAcquisitionJoinError) as ctx:
            VerifiedReferenceAcquisitionJoin(**kwargs)
        self.assertNotIn(secret_bucket, str(ctx.exception))

    def test_secret_in_wrong_object_name_never_appears_in_error(self) -> None:
        secret_path = "SECRET-PATH-do-not-leak-44bb"
        join = _valid_join()
        bad_file = _acquired_file_with(join, object_name=f"landing/{secret_path}/traversal")
        kwargs = _kwargs_from(join)
        kwargs["acquired_file"] = bad_file
        with self.assertRaises(ReferenceAcquisitionJoinError) as ctx:
            VerifiedReferenceAcquisitionJoin(**kwargs)
        self.assertNotIn(secret_path, str(ctx.exception))

    def test_hash_mismatch_error_never_contains_either_hash(self) -> None:
        join = _valid_join()
        tampered = bytearray(join.acquired_file.content_bytes)
        tampered[-1] ^= 0x01
        tampered_bytes = bytes(tampered)
        tampered_hash = hashlib.sha256(tampered_bytes).hexdigest()
        bad_file = _acquired_file_with(
            join, content_bytes=tampered_bytes, sha256_hash=tampered_hash
        )
        kwargs = _kwargs_from(join)
        kwargs["acquired_file"] = bad_file
        with self.assertRaises(ReferenceAcquisitionJoinError) as ctx:
            VerifiedReferenceAcquisitionJoin(**kwargs)
        message = str(ctx.exception)
        self.assertNotIn(tampered_hash, message)
        self.assertNotIn(join.receipt.raw_sha256, message)


class ReferenceAcquisitionJoinCapabilityTests(unittest.TestCase):
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
        for candidate in (ReferenceAcquisitionJoinService, VerifiedReferenceAcquisitionJoin):
            members = [name for name in dir(candidate) if not name.startswith("__")]
            for name in members:
                lowered = name.lower()
                self.assertFalse(
                    any(bad in lowered for bad in banned_substrings),
                    f"{candidate!r} unexpectedly exposes {name!r}",
                )

    def test_no_readiness_or_trading_authority_field_exists_on_class(self) -> None:
        field_names = {field.name for field in dataclasses.fields(VerifiedReferenceAcquisitionJoin)}
        for banned in (
            "readiness",
            "actionable",
            "verified_report_date",
            "acquisition_mode",
            "promotion",
            "signal",
            "recommendation",
            "notification",
            "order",
            "broker",
            "capital",
        ):
            self.assertNotIn(banned, field_names)

    def test_importing_module_causes_no_io(self) -> None:
        import india_swing.reference_data.acquisition_join as module

        banned_module_names = {"os", "socket", "urllib", "requests", "storage"}
        top_level_names = {
            name
            for name in vars(module)
            if not name.startswith("_") and not name[0].isupper()
        }
        self.assertFalse(top_level_names & banned_module_names)


if __name__ == "__main__":
    unittest.main()
