from __future__ import annotations

import csv
import dataclasses
import gzip
import hashlib
import io
import json
import tempfile
import unittest
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from india_swing.daily_pipeline.acquisition import (
    GCSLandingObjectReader,
    GCSObjectPayload,
)
from india_swing.reference.models import ReferenceReadiness
from india_swing.reference_data.acquisition_join import (
    ReferenceAcquisitionJoinService,
    VerifiedReferenceAcquisitionJoin,
)
from india_swing.reference_data.acquisition_promotion import (
    REFERENCE_ARTIFACT_PROMOTION_SCHEMA_VERSION,
    ReferenceArtifactPromotionError,
    ReferenceArtifactPromotionService,
    VerifiedReferenceArtifactPromotion,
    _build_promotion_facts,
    _promotion_identity,
    _recomputed_normalized_sha256,
    _require_lineage_agreement,
    _require_source_state,
)
from india_swing.reference_data.acquisition_receipt import (
    ReferenceAcquisitionReceiptVerifier,
    TrustedReferenceAcquisitionBinding,
    VerifiedReferenceAcquisitionReceipt,
)
from india_swing.reference_data.artifact_store import LocalReferenceArtifactStore
from india_swing.reference_data.models import (
    AcquisitionMode,
    ParsedNseCmSecurityMaster,
    StoredReferenceArtifact,
)
from india_swing.reference_data.security_master import NSE_CM_MII_SECURITY_HEADER


UTC = timezone.utc
_REPORT_DATE = date(2026, 7, 16)
_BUCKET = "trusted-bucket"
_ACQUIRER_ID = "a" * 64
_NOT_BEFORE = datetime(2026, 7, 16, 0, 0, 0, tzinfo=UTC)
_CUTOFF = datetime(2026, 7, 16, 23, 59, 59, tzinfo=UTC)
_ACQUIRED_AT = "2026-07-16T13:30:00Z"
_GENERATION = 123
_FIRST_SEEN = datetime(2026, 7, 16, 12, 0, tzinfo=UTC)
_VALIDATED = datetime(2026, 7, 16, 12, 0, 2, tzinfo=UTC)


def _filename(report_date: date = _REPORT_DATE) -> str:
    return f"NSE_CM_security_{report_date.strftime('%d%m%Y')}.csv.gz"


def _object_name(report_date: date = _REPORT_DATE) -> str:
    return f"landing/{report_date.isoformat()}/{_filename(report_date)}"


def _requested_url(report_date: date = _REPORT_DATE) -> str:
    return f"https://nsearchives.nseindia.com/content/cm/{_filename(report_date)}"


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
    return gzip.compress(_csv_bytes(rows), mtime=0)


class FakeGCSObjectReader:
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


def _valid_receipt_dict(
    *,
    report_date: date,
    bucket: str,
    acquirer_id: str,
    acquired_at: str,
    raw_sha256: str,
    raw_byte_count: int,
    generation: int,
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


def _valid_receipt(
    *,
    gz_bytes: bytes,
    report_date: date = _REPORT_DATE,
    bucket: str = _BUCKET,
    generation: int = _GENERATION,
    acquirer_id: str = _ACQUIRER_ID,
    acquired_at: str = _ACQUIRED_AT,
    not_before: datetime = _NOT_BEFORE,
    cutoff: datetime = _CUTOFF,
) -> VerifiedReferenceAcquisitionReceipt:
    raw_sha256 = hashlib.sha256(gz_bytes).hexdigest()
    receipt_dict = _valid_receipt_dict(
        report_date=report_date,
        bucket=bucket,
        acquirer_id=acquirer_id,
        acquired_at=acquired_at,
        raw_sha256=raw_sha256,
        raw_byte_count=len(gz_bytes),
        generation=generation,
    )
    receipt_bytes = _encode(receipt_dict)
    binding = TrustedReferenceAcquisitionBinding(
        expected_receipt_sha256=hashlib.sha256(receipt_bytes).hexdigest(),
        expected_raw_sha256=raw_sha256,
        allowed_bucket=bucket,
        target_report_date=report_date,
        not_before=not_before,
        cutoff=cutoff,
        trusted_acquirer_id=acquirer_id,
    )
    return ReferenceAcquisitionReceiptVerifier().verify(receipt_bytes, binding)


def _join_for(
    gz_bytes: bytes,
    *,
    report_date: date = _REPORT_DATE,
    bucket: str = _BUCKET,
    generation: int = _GENERATION,
    acquirer_id: str = _ACQUIRER_ID,
    acquired_at: str = _ACQUIRED_AT,
    not_before: datetime | None = None,
    cutoff: datetime | None = None,
) -> VerifiedReferenceAcquisitionJoin:
    if not_before is None:
        not_before = datetime.combine(report_date, datetime.min.time(), tzinfo=UTC)
    if cutoff is None:
        cutoff = datetime.combine(report_date, datetime.max.time(), tzinfo=UTC).replace(
            microsecond=0
        )
    receipt = _valid_receipt(
        gz_bytes=gz_bytes,
        report_date=report_date,
        bucket=bucket,
        generation=generation,
        acquirer_id=acquirer_id,
        acquired_at=acquired_at,
        not_before=not_before,
        cutoff=cutoff,
    )
    fake = FakeGCSObjectReader(generation=generation, content_bytes=gz_bytes)
    reader = GCSLandingObjectReader(fake)
    service = ReferenceAcquisitionJoinService(reader)
    return service.join(receipt)


def _import_artifact(
    root: Path,
    gz_bytes: bytes,
    *,
    report_date: date = _REPORT_DATE,
    first_seen: datetime = _FIRST_SEEN,
    validated: datetime = _VALIDATED,
) -> StoredReferenceArtifact:
    source_dir = root / "source"
    source_dir.mkdir(parents=True, exist_ok=True)
    source_file = source_dir / _filename(report_date)
    source_file.write_bytes(gz_bytes)
    calls = iter((first_seen, validated))
    store = LocalReferenceArtifactStore(root / "archive", clock=lambda: next(calls))
    return store.import_security_master(source_file)


def _build_pair(
    root: Path,
    *,
    rows: list[dict[str, str]] | None = None,
    report_date: date = _REPORT_DATE,
    bucket: str = _BUCKET,
    generation: int = _GENERATION,
    acquirer_id: str = _ACQUIRER_ID,
    acquired_at: str = _ACQUIRED_AT,
    first_seen: datetime = _FIRST_SEEN,
    validated: datetime = _VALIDATED,
) -> tuple[VerifiedReferenceAcquisitionJoin, StoredReferenceArtifact]:
    gz_bytes = _security_master_gzip(rows=rows)
    not_before = None
    cutoff = None
    if report_date != _REPORT_DATE:
        not_before = datetime.combine(report_date, datetime.min.time(), tzinfo=UTC)
        cutoff = datetime.combine(report_date, datetime.max.time(), tzinfo=UTC).replace(
            microsecond=0
        )
    join = _join_for(
        gz_bytes,
        report_date=report_date,
        bucket=bucket,
        generation=generation,
        acquirer_id=acquirer_id,
        acquired_at=acquired_at,
        not_before=not_before,
        cutoff=cutoff,
    )
    artifact = _import_artifact(
        root, gz_bytes, report_date=report_date, first_seen=first_seen, validated=validated
    )
    return join, artifact


def _kwargs_from(promotion: VerifiedReferenceArtifactPromotion) -> dict[str, object]:
    return {field.name: getattr(promotion, field.name) for field in dataclasses.fields(promotion)}


def _archive_snapshot(archive_root: Path) -> dict[str, bytes]:
    if not archive_root.exists():
        return {}
    return {
        str(path.relative_to(archive_root)): path.read_bytes()
        for path in sorted(archive_root.rglob("*"))
        if path.is_file()
    }


class ReferenceArtifactPromotionAcceptanceTests(unittest.TestCase):
    def test_happy_path_promotes_and_verifies(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            join, artifact = _build_pair(root)

            promotion = ReferenceArtifactPromotionService().promote(join, artifact)

            self.assertEqual(promotion.schema_version, REFERENCE_ARTIFACT_PROMOTION_SCHEMA_VERSION)
            self.assertIs(promotion.join, join)
            self.assertIs(promotion.artifact, artifact)
            self.assertEqual(
                promotion.promoted_acquisition_mode, AcquisitionMode.TRUSTED_PINNED_GCS_RECEIPT
            )
            self.assertEqual(promotion.promoted_readiness, ReferenceReadiness.POINT_IN_TIME_VERIFIED)
            self.assertFalse(promotion.actionable)
            self.assertEqual(promotion.verified_report_date, join.receipt.report_date)
            self.assertEqual(promotion.knowledge_time, join.receipt.acquired_at)
            self.assertEqual(len(promotion.promotion_id), 64)
            promotion.verify_content_identity()

    def test_equal_semantic_inputs_produce_same_promotion_id(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_a, tempfile.TemporaryDirectory() as tmp_b:
            join_a, artifact_a = _build_pair(Path(tmp_a))
            join_b, artifact_b = _build_pair(Path(tmp_b))

            promotion_a = ReferenceArtifactPromotionService().promote(join_a, artifact_a)
            promotion_b = ReferenceArtifactPromotionService().promote(join_b, artifact_b)

            self.assertEqual(promotion_a.promotion_id, promotion_b.promotion_id)

    def test_promotion_type_returned_is_frozen(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            join, artifact = _build_pair(Path(tmp))
            promotion = ReferenceArtifactPromotionService().promote(join, artifact)
            with self.assertRaises(dataclasses.FrozenInstanceError):
                promotion.promotion_id = "x"  # type: ignore[misc]

    def test_no_calendar_universe_signal_or_trading_field_exists(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            join, artifact = _build_pair(Path(tmp))
            promotion = ReferenceArtifactPromotionService().promote(join, artifact)
            field_names = {field.name for field in dataclasses.fields(promotion)}
            for banned in (
                "calendar",
                "universe",
                "price",
                "liquidity",
                "surveillance",
                "corporate_action",
                "model",
                "signal",
                "ranking",
                "recommendation",
                "notification",
                "order",
                "broker",
                "capital",
            ):
                self.assertFalse(any(banned in name for name in field_names))


class ReferenceArtifactPromotionArchiveIsolationTests(unittest.TestCase):
    def test_promotion_never_mutates_the_sealed_archive(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            join, artifact = _build_pair(root)
            archive_root = root / "archive"
            before = _archive_snapshot(archive_root)
            self.assertTrue(before)

            ReferenceArtifactPromotionService().promote(join, artifact)

            after = _archive_snapshot(archive_root)
            self.assertEqual(before, after)
            artifact_directories = [
                path
                for path in archive_root.glob("*/*/*")
                if path.is_dir() and not path.name.startswith(".")
            ]
            self.assertEqual(len(artifact_directories), 1)


class ReferenceArtifactPromotionSourceStateRejectionTests(unittest.TestCase):
    def test_wrong_acquisition_mode_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            _, artifact = _build_pair(Path(tmp))
            object.__setattr__(
                artifact.manifest, "acquisition_mode", AcquisitionMode.TRUSTED_PINNED_GCS_RECEIPT
            )
            with self.assertRaises(ReferenceArtifactPromotionError):
                _require_source_state(artifact.manifest)

    def test_wrong_readiness_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            _, artifact = _build_pair(Path(tmp))
            object.__setattr__(
                artifact.manifest, "readiness", ReferenceReadiness.POINT_IN_TIME_VERIFIED
            )
            with self.assertRaises(ReferenceArtifactPromotionError):
                _require_source_state(artifact.manifest)

    def test_wrong_actionable_flag_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            _, artifact = _build_pair(Path(tmp))
            object.__setattr__(artifact.manifest, "actionable", True)
            with self.assertRaises(ReferenceArtifactPromotionError):
                _require_source_state(artifact.manifest)

    def test_non_null_verified_report_date_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            _, artifact = _build_pair(Path(tmp))
            object.__setattr__(artifact.manifest, "verified_report_date", _REPORT_DATE)
            with self.assertRaises(ReferenceArtifactPromotionError):
                _require_source_state(artifact.manifest)

    def test_wrong_publication_time_status_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            _, artifact = _build_pair(Path(tmp))
            mutated_manifest = dataclasses.replace(
                artifact.manifest, publication_time_status="TRUSTED_PINNED_GCS_RECEIPT"
            )
            with self.assertRaises(ReferenceArtifactPromotionError):
                _require_source_state(mutated_manifest)


class ReferenceArtifactPromotionLineageRejectionTests(unittest.TestCase):
    def _pair(self, tmp: str):
        return _build_pair(Path(tmp))

    def test_raw_byte_mismatch_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            join, artifact = self._pair(tmp)
            bad_artifact = dataclasses.replace(artifact, raw_bytes=b"different-bytes")
            with self.assertRaises(ReferenceArtifactPromotionError):
                _require_lineage_agreement(join, bad_artifact)

    def test_raw_hash_mismatch_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            join, artifact = self._pair(tmp)
            bad_manifest = dataclasses.replace(artifact.manifest, raw_sha256="b" * 64)
            bad_artifact = dataclasses.replace(artifact, manifest=bad_manifest)
            with self.assertRaises(ReferenceArtifactPromotionError):
                _require_lineage_agreement(join, bad_artifact)

    def test_raw_byte_count_mismatch_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            join, artifact = self._pair(tmp)
            bad_manifest = dataclasses.replace(
                artifact.manifest, compressed_byte_count=artifact.manifest.compressed_byte_count + 1
            )
            bad_artifact = dataclasses.replace(artifact, manifest=bad_manifest)
            with self.assertRaises(ReferenceArtifactPromotionError):
                _require_lineage_agreement(join, bad_artifact)

    def test_report_date_mismatch_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            join, artifact = self._pair(tmp)
            bad_manifest = dataclasses.replace(artifact.manifest, claimed_report_date=date(2026, 7, 15))
            bad_artifact = dataclasses.replace(artifact, manifest=bad_manifest)
            with self.assertRaises(ReferenceArtifactPromotionError):
                _require_lineage_agreement(join, bad_artifact)

    def test_filename_mismatch_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            join, artifact = self._pair(tmp)
            bad_manifest = dataclasses.replace(
                artifact.manifest, original_filename="NSE_CM_security_01012026.csv.gz"
            )
            bad_artifact = dataclasses.replace(artifact, manifest=bad_manifest)
            with self.assertRaises(ReferenceArtifactPromotionError):
                _require_lineage_agreement(join, bad_artifact)

    def test_source_url_mismatch_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            join, artifact = self._pair(tmp)
            bad_manifest = dataclasses.replace(
                artifact.manifest,
                claimed_download_url="https://nsearchives.nseindia.com/content/cm/OTHER.csv.gz",
            )
            bad_artifact = dataclasses.replace(artifact, manifest=bad_manifest)
            with self.assertRaises(ReferenceArtifactPromotionError):
                _require_lineage_agreement(join, bad_artifact)

    def test_media_type_mismatch_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            join, artifact = self._pair(tmp)
            bad_manifest = dataclasses.replace(artifact.manifest, source_media_type="text/csv")
            bad_artifact = dataclasses.replace(artifact, manifest=bad_manifest)
            with self.assertRaises(ReferenceArtifactPromotionError):
                _require_lineage_agreement(join, bad_artifact)

    def test_parser_version_mismatch_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            join, artifact = self._pair(tmp)
            bad_manifest = dataclasses.replace(artifact.manifest, parser_version="nse-cm-mii-security-parser/v99")
            bad_artifact = dataclasses.replace(artifact, manifest=bad_manifest)
            with self.assertRaises(ReferenceArtifactPromotionError):
                _require_lineage_agreement(join, bad_artifact)

    def test_source_schema_version_mismatch_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            join, artifact = self._pair(tmp)
            bad_manifest = dataclasses.replace(
                artifact.manifest, source_schema_version="nse-cm-mii-security/iso-tags-120/v99"
            )
            bad_artifact = dataclasses.replace(artifact, manifest=bad_manifest)
            with self.assertRaises(ReferenceArtifactPromotionError):
                _require_lineage_agreement(join, bad_artifact)

    def test_scope_policy_version_mismatch_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            join, artifact = self._pair(tmp)
            bad_manifest = dataclasses.replace(
                artifact.manifest, scope_policy_version="nse-cm-equity-scope/collection-only-v99"
            )
            bad_artifact = dataclasses.replace(artifact, manifest=bad_manifest)
            with self.assertRaises(ReferenceArtifactPromotionError):
                _require_lineage_agreement(join, bad_artifact)

    def test_normalized_codec_version_mismatch_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            join, artifact = self._pair(tmp)
            bad_manifest = dataclasses.replace(
                artifact.manifest, normalized_codec_version="nse-cm-mii-security-json/v99"
            )
            bad_artifact = dataclasses.replace(artifact, manifest=bad_manifest)
            with self.assertRaises(ReferenceArtifactPromotionError):
                _require_lineage_agreement(join, bad_artifact)

    def test_uncompressed_hash_mismatch_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            join, artifact = self._pair(tmp)
            bad_manifest = dataclasses.replace(artifact.manifest, uncompressed_sha256="c" * 64)
            bad_artifact = dataclasses.replace(artifact, manifest=bad_manifest)
            with self.assertRaises(ReferenceArtifactPromotionError):
                _require_lineage_agreement(join, bad_artifact)

    def test_header_hash_mismatch_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            join, artifact = self._pair(tmp)
            bad_manifest = dataclasses.replace(artifact.manifest, header_sha256="d" * 64)
            bad_artifact = dataclasses.replace(artifact, manifest=bad_manifest)
            with self.assertRaises(ReferenceArtifactPromotionError):
                _require_lineage_agreement(join, bad_artifact)

    def test_ordered_row_digest_mismatch_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            join, artifact = self._pair(tmp)
            bad_manifest = dataclasses.replace(artifact.manifest, ordered_row_digest="e" * 64)
            bad_artifact = dataclasses.replace(artifact, manifest=bad_manifest)
            with self.assertRaises(ReferenceArtifactPromotionError):
                _require_lineage_agreement(join, bad_artifact)

    def test_row_count_mismatch_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            join, artifact = self._pair(tmp)
            bad_manifest = dataclasses.replace(
                artifact.manifest,
                raw_row_count=artifact.manifest.raw_row_count + 1,
                parsed_row_count=artifact.manifest.parsed_row_count + 1,
                retained_unverified_equity_count=(
                    artifact.manifest.retained_unverified_equity_count + 1
                ),
            )
            bad_artifact = dataclasses.replace(artifact, manifest=bad_manifest)
            with self.assertRaises(ReferenceArtifactPromotionError):
                _require_lineage_agreement(join, bad_artifact)

    def test_disposition_count_mismatch_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            join, artifact = self._pair(tmp)
            bad_manifest = dataclasses.replace(
                artifact.manifest,
                retained_unverified_equity_count=0,
                excluded_non_equity_count=(
                    artifact.manifest.excluded_non_equity_count + 1
                ),
            )
            bad_artifact = dataclasses.replace(artifact, manifest=bad_manifest)
            with self.assertRaises(ReferenceArtifactPromotionError):
                _require_lineage_agreement(join, bad_artifact)

    def test_parsed_type_or_value_mismatch_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            join, artifact = self._pair(tmp)
            other_join = _join_for(
                _security_master_gzip(rows=[_security_master_row(FinInstrmId="99999", TckrSymb="TCS")]),
                generation=999,
            )
            bad_artifact = dataclasses.replace(artifact, parsed=other_join.parsed)
            with self.assertRaises(ReferenceArtifactPromotionError):
                _require_lineage_agreement(join, bad_artifact)


class ReferenceArtifactPromotionNormalizedRejectionTests(unittest.TestCase):
    def test_normalized_byte_mismatch_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            join, artifact = _build_pair(Path(tmp))
            bad_artifact = dataclasses.replace(artifact, normalized_bytes=b"{}")
            with self.assertRaises(ReferenceArtifactPromotionError):
                _recomputed_normalized_sha256(join, bad_artifact)

    def test_normalized_hash_mismatch_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            join, artifact = _build_pair(Path(tmp))
            bad_manifest = dataclasses.replace(artifact.manifest, normalized_sha256="f" * 64)
            bad_artifact = dataclasses.replace(artifact, manifest=bad_manifest)
            with self.assertRaises(ReferenceArtifactPromotionError):
                _recomputed_normalized_sha256(join, bad_artifact)


class ReferenceArtifactPromotionProvenanceRejectionTests(unittest.TestCase):
    def test_missing_path_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            join, artifact = _build_pair(root)
            missing_artifact = dataclasses.replace(artifact, path=root / "archive" / "does-not-exist")
            with self.assertRaises(ReferenceArtifactPromotionError):
                ReferenceArtifactPromotionService().promote(join, missing_artifact)

    def test_extra_archive_entry_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            join, artifact = _build_pair(root)
            (artifact.path / "extra.txt").write_text("extra", encoding="utf-8")
            with self.assertRaises(ReferenceArtifactPromotionError):
                ReferenceArtifactPromotionService().promote(join, artifact)

    def test_missing_archive_entry_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            join, artifact = _build_pair(root)
            (artifact.path / "normalized.json").unlink()
            with self.assertRaises(ReferenceArtifactPromotionError):
                ReferenceArtifactPromotionService().promote(join, artifact)

    def test_changed_manifest_bytes_are_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            join, artifact = _build_pair(root)
            manifest_path = artifact.path / "manifest.json"
            value = json.loads(manifest_path.read_text(encoding="utf-8"))
            value["validated_at"] = "2020-01-01T00:00:00+00:00"
            manifest_path.write_text(json.dumps(value), encoding="utf-8")
            with self.assertRaises(ReferenceArtifactPromotionError):
                ReferenceArtifactPromotionService().promote(join, artifact)

    def test_changed_raw_bytes_on_disk_are_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            join, artifact = _build_pair(root)
            (artifact.path / "source.csv.gz").write_bytes(b"tampered")
            with self.assertRaises(ReferenceArtifactPromotionError):
                ReferenceArtifactPromotionService().promote(join, artifact)

    def test_changed_normalized_bytes_on_disk_are_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            join, artifact = _build_pair(root)
            (artifact.path / "normalized.json").write_text("{}", encoding="utf-8")
            with self.assertRaises(ReferenceArtifactPromotionError):
                ReferenceArtifactPromotionService().promote(join, artifact)

    def test_symbolic_link_archive_entry_is_rejected_when_supported(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            join, artifact = _build_pair(root)
            link_target = root / "outside.txt"
            link_target.write_text("outside", encoding="utf-8")
            raw_path = artifact.path / "source.csv.gz"
            raw_bytes = raw_path.read_bytes()
            raw_path.unlink()
            try:
                raw_path.symlink_to(link_target)
            except OSError:
                self.skipTest("symbolic links are unavailable in this environment")
            with self.assertRaises(ReferenceArtifactPromotionError):
                ReferenceArtifactPromotionService().promote(join, artifact)


class ReferenceArtifactPromotionMalformedGraphRejectionTests(unittest.TestCase):
    """Regression coverage for Codex's revision-1 finding: a malformed nested
    StoredReferenceArtifact field must never leak a raw nested exception
    (TypeError/AttributeError/...) from verify_stored_reference_provenance --
    every failure must surface as one static sanitized
    ReferenceArtifactPromotionError, whether reached through
    ReferenceArtifactPromotionService.promote() or through consumption-time
    verify_content_identity() replay on an already-valid promotion.
    """

    def test_codex_reported_path_int_mutation_leaks_no_nested_exception_text(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            join, artifact = _build_pair(Path(tmp))
            promotion = ReferenceArtifactPromotionService().promote(join, artifact)
            object.__setattr__(promotion.artifact, "path", 123)
            with self.assertRaises(ReferenceArtifactPromotionError) as ctx:
                promotion.verify_content_identity()
            message = str(ctx.exception)
            self.assertNotIn("TypeError", message)
            self.assertNotIn("123", message)

    def test_malformed_path_values_are_rejected_by_promotion_and_replay(self) -> None:
        for bad_path in (123, None, object()):
            with self.subTest(bad_path=bad_path):
                with tempfile.TemporaryDirectory() as tmp:
                    join, artifact = _build_pair(Path(tmp))
                    bad_artifact = dataclasses.replace(artifact, path=bad_path)
                    with self.assertRaises(ReferenceArtifactPromotionError):
                        ReferenceArtifactPromotionService().promote(join, bad_artifact)

                with tempfile.TemporaryDirectory() as tmp:
                    join, artifact = _build_pair(Path(tmp))
                    promotion = ReferenceArtifactPromotionService().promote(join, artifact)
                    object.__setattr__(promotion.artifact, "path", bad_path)
                    with self.assertRaises(ReferenceArtifactPromotionError):
                        promotion.verify_content_identity()

    def test_malformed_manifest_reference_is_rejected_by_promotion_and_replay(self) -> None:
        for bad_manifest in (None, object()):
            with self.subTest(bad_manifest=bad_manifest):
                with tempfile.TemporaryDirectory() as tmp:
                    join, artifact = _build_pair(Path(tmp))
                    bad_artifact = dataclasses.replace(artifact, manifest=bad_manifest)
                    with self.assertRaises(ReferenceArtifactPromotionError):
                        ReferenceArtifactPromotionService().promote(join, bad_artifact)

                with tempfile.TemporaryDirectory() as tmp:
                    join, artifact = _build_pair(Path(tmp))
                    promotion = ReferenceArtifactPromotionService().promote(join, artifact)
                    object.__setattr__(promotion.artifact, "manifest", bad_manifest)
                    with self.assertRaises(ReferenceArtifactPromotionError):
                        promotion.verify_content_identity()

    def test_malformed_parsed_reference_is_rejected_by_promotion_and_replay(self) -> None:
        for bad_parsed in (None, object()):
            with self.subTest(bad_parsed=bad_parsed):
                with tempfile.TemporaryDirectory() as tmp:
                    join, artifact = _build_pair(Path(tmp))
                    bad_artifact = dataclasses.replace(artifact, parsed=bad_parsed)
                    with self.assertRaises(ReferenceArtifactPromotionError):
                        ReferenceArtifactPromotionService().promote(join, bad_artifact)

                with tempfile.TemporaryDirectory() as tmp:
                    join, artifact = _build_pair(Path(tmp))
                    promotion = ReferenceArtifactPromotionService().promote(join, artifact)
                    object.__setattr__(promotion.artifact, "parsed", bad_parsed)
                    with self.assertRaises(ReferenceArtifactPromotionError):
                        promotion.verify_content_identity()

    def test_malformed_raw_bytes_type_is_rejected_by_promotion_and_replay(self) -> None:
        for bad_raw_bytes in (123, "not-bytes", None):
            with self.subTest(bad_raw_bytes=bad_raw_bytes):
                with tempfile.TemporaryDirectory() as tmp:
                    join, artifact = _build_pair(Path(tmp))
                    bad_artifact = dataclasses.replace(artifact, raw_bytes=bad_raw_bytes)
                    with self.assertRaises(ReferenceArtifactPromotionError):
                        ReferenceArtifactPromotionService().promote(join, bad_artifact)

                with tempfile.TemporaryDirectory() as tmp:
                    join, artifact = _build_pair(Path(tmp))
                    promotion = ReferenceArtifactPromotionService().promote(join, artifact)
                    object.__setattr__(promotion.artifact, "raw_bytes", bad_raw_bytes)
                    with self.assertRaises(ReferenceArtifactPromotionError):
                        promotion.verify_content_identity()

    def test_malformed_normalized_bytes_type_is_rejected_by_promotion_and_replay(self) -> None:
        for bad_normalized_bytes in (123, "not-bytes", None):
            with self.subTest(bad_normalized_bytes=bad_normalized_bytes):
                with tempfile.TemporaryDirectory() as tmp:
                    join, artifact = _build_pair(Path(tmp))
                    bad_artifact = dataclasses.replace(
                        artifact, normalized_bytes=bad_normalized_bytes
                    )
                    with self.assertRaises(ReferenceArtifactPromotionError):
                        ReferenceArtifactPromotionService().promote(join, bad_artifact)

                with tempfile.TemporaryDirectory() as tmp:
                    join, artifact = _build_pair(Path(tmp))
                    promotion = ReferenceArtifactPromotionService().promote(join, artifact)
                    object.__setattr__(
                        promotion.artifact, "normalized_bytes", bad_normalized_bytes
                    )
                    with self.assertRaises(ReferenceArtifactPromotionError):
                        promotion.verify_content_identity()


class ReferenceArtifactPromotionDirectConstructionMismatchTests(unittest.TestCase):
    def test_replacing_join_with_a_different_valid_one_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            join, artifact = _build_pair(Path(tmp))
            promotion = ReferenceArtifactPromotionService().promote(join, artifact)
            other_join = _join_for(_security_master_gzip(), generation=777)
            kwargs = _kwargs_from(promotion)
            kwargs["join"] = other_join
            with self.assertRaises(ReferenceArtifactPromotionError):
                VerifiedReferenceArtifactPromotion(**kwargs)

    def test_replacing_artifact_with_a_different_valid_one_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_a, tempfile.TemporaryDirectory() as tmp_b:
            join, artifact = _build_pair(Path(tmp_a))
            promotion = ReferenceArtifactPromotionService().promote(join, artifact)
            _, other_artifact = _build_pair(
                Path(tmp_b),
                rows=[_security_master_row(FinInstrmId="55555", TckrSymb="INFY")],
            )
            kwargs = _kwargs_from(promotion)
            kwargs["artifact"] = other_artifact
            with self.assertRaises(ReferenceArtifactPromotionError):
                VerifiedReferenceArtifactPromotion(**kwargs)

    def test_replacing_schema_marker_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            join, artifact = _build_pair(Path(tmp))
            promotion = ReferenceArtifactPromotionService().promote(join, artifact)
            kwargs = _kwargs_from(promotion)
            kwargs["schema_version"] = "reference-artifact-promotion/v3"
            with self.assertRaises(ReferenceArtifactPromotionError):
                VerifiedReferenceArtifactPromotion(**kwargs)

    def test_replacing_acquisition_mode_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            join, artifact = _build_pair(Path(tmp))
            promotion = ReferenceArtifactPromotionService().promote(join, artifact)
            kwargs = _kwargs_from(promotion)
            kwargs["promoted_acquisition_mode"] = AcquisitionMode.UNVERIFIED_MANUAL_FILE
            with self.assertRaises(ReferenceArtifactPromotionError):
                VerifiedReferenceArtifactPromotion(**kwargs)

    def test_replacing_readiness_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            join, artifact = _build_pair(Path(tmp))
            promotion = ReferenceArtifactPromotionService().promote(join, artifact)
            kwargs = _kwargs_from(promotion)
            kwargs["promoted_readiness"] = ReferenceReadiness.COLLECTION_ONLY
            with self.assertRaises(ReferenceArtifactPromotionError):
                VerifiedReferenceArtifactPromotion(**kwargs)

    def test_replacing_report_date_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            join, artifact = _build_pair(Path(tmp))
            promotion = ReferenceArtifactPromotionService().promote(join, artifact)
            kwargs = _kwargs_from(promotion)
            kwargs["verified_report_date"] = date(2020, 1, 1)
            with self.assertRaises(ReferenceArtifactPromotionError):
                VerifiedReferenceArtifactPromotion(**kwargs)

    def test_replacing_knowledge_time_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            join, artifact = _build_pair(Path(tmp))
            promotion = ReferenceArtifactPromotionService().promote(join, artifact)
            kwargs = _kwargs_from(promotion)
            kwargs["knowledge_time"] = datetime(2020, 1, 1, tzinfo=UTC)
            with self.assertRaises(ReferenceArtifactPromotionError):
                VerifiedReferenceArtifactPromotion(**kwargs)

    def test_replacing_actionable_flag_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            join, artifact = _build_pair(Path(tmp))
            promotion = ReferenceArtifactPromotionService().promote(join, artifact)
            kwargs = _kwargs_from(promotion)
            kwargs["actionable"] = True
            with self.assertRaises(ReferenceArtifactPromotionError):
                VerifiedReferenceArtifactPromotion(**kwargs)

    def test_replacing_promotion_id_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            join, artifact = _build_pair(Path(tmp))
            promotion = ReferenceArtifactPromotionService().promote(join, artifact)
            kwargs = _kwargs_from(promotion)
            kwargs["promotion_id"] = hashlib.sha256(b"different").hexdigest()
            with self.assertRaises(ReferenceArtifactPromotionError):
                VerifiedReferenceArtifactPromotion(**kwargs)


class ReferenceArtifactPromotionMutationTests(unittest.TestCase):
    def _promotion(self, tmp: str) -> VerifiedReferenceArtifactPromotion:
        join, artifact = _build_pair(Path(tmp))
        return ReferenceArtifactPromotionService().promote(join, artifact)

    def test_mutating_top_level_schema_version_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            promotion = self._promotion(tmp)
            object.__setattr__(promotion, "schema_version", "other")
            with self.assertRaises(ReferenceArtifactPromotionError):
                promotion.verify_content_identity()

    def test_mutating_top_level_promotion_id_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            promotion = self._promotion(tmp)
            object.__setattr__(promotion, "promotion_id", "0" * 64)
            with self.assertRaises(ReferenceArtifactPromotionError):
                promotion.verify_content_identity()

    def test_mutating_promoted_acquisition_mode_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            promotion = self._promotion(tmp)
            object.__setattr__(
                promotion, "promoted_acquisition_mode", AcquisitionMode.UNVERIFIED_MANUAL_FILE
            )
            with self.assertRaises(ReferenceArtifactPromotionError):
                promotion.verify_content_identity()

    def test_mutating_promoted_readiness_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            promotion = self._promotion(tmp)
            object.__setattr__(promotion, "promoted_readiness", ReferenceReadiness.COLLECTION_ONLY)
            with self.assertRaises(ReferenceArtifactPromotionError):
                promotion.verify_content_identity()

    def test_mutating_verified_report_date_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            promotion = self._promotion(tmp)
            object.__setattr__(promotion, "verified_report_date", date(2020, 1, 1))
            with self.assertRaises(ReferenceArtifactPromotionError):
                promotion.verify_content_identity()

    def test_mutating_knowledge_time_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            promotion = self._promotion(tmp)
            object.__setattr__(promotion, "knowledge_time", datetime(2020, 1, 1, tzinfo=UTC))
            with self.assertRaises(ReferenceArtifactPromotionError):
                promotion.verify_content_identity()

    def test_mutating_actionable_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            promotion = self._promotion(tmp)
            object.__setattr__(promotion, "actionable", True)
            with self.assertRaises(ReferenceArtifactPromotionError):
                promotion.verify_content_identity()

    def test_mutating_nested_join_join_id_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            promotion = self._promotion(tmp)
            object.__setattr__(promotion.join, "join_id", "0" * 64)
            with self.assertRaises(ReferenceArtifactPromotionError):
                promotion.verify_content_identity()

    def test_mutating_nested_receipt_field_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            promotion = self._promotion(tmp)
            object.__setattr__(promotion.join.receipt, "response_status", 201)
            with self.assertRaises(ReferenceArtifactPromotionError):
                promotion.verify_content_identity()

    def test_mutating_nested_binding_field_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            promotion = self._promotion(tmp)
            object.__setattr__(
                promotion.join.receipt.binding,
                "allowed_bucket",
                "another-syntactically-valid-bucket",
            )
            with self.assertRaises(ReferenceArtifactPromotionError):
                promotion.verify_content_identity()

    def test_mutating_nested_binding_cutoff_field_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            promotion = self._promotion(tmp)
            object.__setattr__(
                promotion.join.receipt.binding,
                "cutoff",
                promotion.join.receipt.binding.cutoff + timedelta(days=1),
            )
            with self.assertRaises(ReferenceArtifactPromotionError):
                promotion.verify_content_identity()

    def test_mutating_nested_landing_object_field_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            promotion = self._promotion(tmp)
            object.__setattr__(promotion.join.receipt.landing_object, "generation", 999)
            with self.assertRaises(ReferenceArtifactPromotionError):
                promotion.verify_content_identity()

    def test_mutating_nested_acquired_file_field_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            promotion = self._promotion(tmp)
            object.__setattr__(
                promotion.join.acquired_file, "bucket", "another-syntactically-valid-bucket"
            )
            with self.assertRaises(ReferenceArtifactPromotionError):
                promotion.verify_content_identity()

    def test_mutating_nested_parsed_record_field_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            promotion = self._promotion(tmp)
            object.__setattr__(promotion.join.parsed.records[0], "ticker_symbol", "TAMPERED")
            with self.assertRaises(ReferenceArtifactPromotionError):
                promotion.verify_content_identity()

    def test_mutating_stored_manifest_field_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            promotion = self._promotion(tmp)
            object.__setattr__(promotion.artifact.manifest, "raw_sha256", "a" * 64)
            with self.assertRaises(ReferenceArtifactPromotionError):
                promotion.verify_content_identity()

    def test_mutating_stored_raw_bytes_field_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            promotion = self._promotion(tmp)
            object.__setattr__(promotion.artifact, "raw_bytes", b"different")
            with self.assertRaises(ReferenceArtifactPromotionError):
                promotion.verify_content_identity()

    def test_mutating_stored_normalized_bytes_field_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            promotion = self._promotion(tmp)
            object.__setattr__(promotion.artifact, "normalized_bytes", b"{}")
            with self.assertRaises(ReferenceArtifactPromotionError):
                promotion.verify_content_identity()

    def test_mutating_stored_path_field_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            promotion = self._promotion(tmp)
            object.__setattr__(promotion.artifact, "path", promotion.artifact.path.parent / "missing")
            with self.assertRaises(ReferenceArtifactPromotionError):
                promotion.verify_content_identity()


class ReferenceArtifactPromotionSubclassImpostorTests(unittest.TestCase):
    def test_join_subclass_rejected_by_service(self) -> None:
        class _JoinSubclass(VerifiedReferenceAcquisitionJoin):
            pass

        with tempfile.TemporaryDirectory() as tmp:
            join, artifact = _build_pair(Path(tmp))
            subclass_join = _JoinSubclass(
                **{field.name: getattr(join, field.name) for field in dataclasses.fields(join)}
            )
            with self.assertRaises(ReferenceArtifactPromotionError):
                ReferenceArtifactPromotionService().promote(subclass_join, artifact)

    def test_artifact_subclass_rejected_by_service(self) -> None:
        class _ArtifactSubclass(StoredReferenceArtifact):
            pass

        with tempfile.TemporaryDirectory() as tmp:
            join, artifact = _build_pair(Path(tmp))
            subclass_artifact = _ArtifactSubclass(
                path=artifact.path,
                manifest=artifact.manifest,
                parsed=artifact.parsed,
                raw_bytes=artifact.raw_bytes,
                normalized_bytes=artifact.normalized_bytes,
            )
            with self.assertRaises(ReferenceArtifactPromotionError):
                ReferenceArtifactPromotionService().promote(join, subclass_artifact)

    def test_parsed_subclass_on_artifact_is_rejected(self) -> None:
        class _ParsedSubclass(ParsedNseCmSecurityMaster):
            pass

        with tempfile.TemporaryDirectory() as tmp:
            join, artifact = _build_pair(Path(tmp))
            subclass_parsed = _ParsedSubclass(
                **{
                    field.name: getattr(artifact.parsed, field.name)
                    for field in dataclasses.fields(artifact.parsed)
                }
            )
            bad_artifact = dataclasses.replace(artifact, parsed=subclass_parsed)
            with self.assertRaises(ReferenceArtifactPromotionError):
                _require_lineage_agreement(join, bad_artifact)

    def test_bytes_subclass_raw_bytes_is_rejected(self) -> None:
        class _BytesSubclass(bytes):
            pass

        with tempfile.TemporaryDirectory() as tmp:
            join, artifact = _build_pair(Path(tmp))
            bad_artifact = dataclasses.replace(
                artifact, raw_bytes=_BytesSubclass(artifact.raw_bytes)
            )
            with self.assertRaises(ReferenceArtifactPromotionError):
                _require_lineage_agreement(join, bad_artifact)

    def test_str_subclass_promotion_id_is_rejected(self) -> None:
        class _StrSubclass(str):
            pass

        with tempfile.TemporaryDirectory() as tmp:
            join, artifact = _build_pair(Path(tmp))
            promotion = ReferenceArtifactPromotionService().promote(join, artifact)
            kwargs = _kwargs_from(promotion)
            kwargs["promotion_id"] = _StrSubclass(promotion.promotion_id)
            with self.assertRaises(ReferenceArtifactPromotionError):
                VerifiedReferenceArtifactPromotion(**kwargs)

    def test_datetime_confused_with_date_on_verified_report_date_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            join, artifact = _build_pair(Path(tmp))
            promotion = ReferenceArtifactPromotionService().promote(join, artifact)
            kwargs = _kwargs_from(promotion)
            kwargs["verified_report_date"] = datetime(2026, 7, 16, tzinfo=UTC)
            with self.assertRaises(ReferenceArtifactPromotionError):
                VerifiedReferenceArtifactPromotion(**kwargs)

    def test_date_confused_with_datetime_on_knowledge_time_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            join, artifact = _build_pair(Path(tmp))
            promotion = ReferenceArtifactPromotionService().promote(join, artifact)
            kwargs = _kwargs_from(promotion)
            kwargs["knowledge_time"] = date(2026, 7, 16)
            with self.assertRaises(ReferenceArtifactPromotionError):
                VerifiedReferenceArtifactPromotion(**kwargs)

    def test_acquisition_mode_and_readiness_enums_cannot_be_subclassed(self) -> None:
        with self.assertRaises(TypeError):

            class _AcquisitionModeSubclass(AcquisitionMode):
                pass

        with self.assertRaises(TypeError):

            class _ReadinessSubclass(ReferenceReadiness):
                pass

    def test_fully_valid_subclass_construction_is_still_rejected(self) -> None:
        # A bare subclass constructed with every field copied unmodified from
        # a genuine promotion (no field substitution at all) must still be
        # rejected: content-identity agreement alone is not sufficient, the
        # concrete type itself must be exactly VerifiedReferenceArtifactPromotion.
        class _PromotionSubclass(VerifiedReferenceArtifactPromotion):
            pass

        with tempfile.TemporaryDirectory() as tmp:
            join, artifact = _build_pair(Path(tmp))
            promotion = ReferenceArtifactPromotionService().promote(join, artifact)
            kwargs = _kwargs_from(promotion)
            with self.assertRaises(ReferenceArtifactPromotionError):
                _PromotionSubclass(**kwargs)

    def test_reassigning_class_on_a_valid_instance_is_not_a_substitute_for_the_subclass_test(
        self,
    ) -> None:
        # Mutating __class__ on an already-verified exact instance is not an
        # equivalent reproduction of "construct a subclass with valid
        # fields" -- __slots__ makes the assignment itself fail, so this
        # documents that the dedicated construction-time test above is the
        # only valid way to exercise the subclass rejection.
        class _PromotionSubclass(VerifiedReferenceArtifactPromotion):
            pass

        with tempfile.TemporaryDirectory() as tmp:
            join, artifact = _build_pair(Path(tmp))
            promotion = ReferenceArtifactPromotionService().promote(join, artifact)
            with self.assertRaises(TypeError):
                promotion.__class__ = _PromotionSubclass


class ReferenceArtifactPromotionContentIdCompletenessTests(unittest.TestCase):
    def test_identity_key_set_is_exact(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            join, artifact = _build_pair(Path(tmp))
            normalized_sha256 = _recomputed_normalized_sha256(join, artifact)
            material = _promotion_identity(
                join, artifact, normalized_sha256, join.receipt.report_date, join.receipt.acquired_at
            )
            self.assertEqual(
                set(material),
                {
                    "schema_version",
                    "join_id",
                    "receipt_sha256",
                    "source_artifact_id",
                    "source_manifest_id",
                    "raw_sha256",
                    "normalized_sha256",
                    "uncompressed_sha256",
                    "header_sha256",
                    "ordered_row_digest",
                    "report_date",
                    "knowledge_time",
                    "trusted_binding_expected_receipt_sha256",
                    "trusted_binding_expected_raw_sha256",
                    "trusted_binding_allowed_bucket",
                    "trusted_binding_target_report_date",
                    "trusted_binding_not_before",
                    "trusted_binding_cutoff",
                    "trusted_binding_trusted_acquirer_id",
                    "promoted_acquisition_mode",
                    "promoted_readiness",
                    "actionable",
                },
            )

    def test_different_generation_changes_promotion_id(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_a, tempfile.TemporaryDirectory() as tmp_b:
            join_a, artifact_a = _build_pair(Path(tmp_a), generation=100)
            join_b, artifact_b = _build_pair(Path(tmp_b), generation=200)
            promotion_a = ReferenceArtifactPromotionService().promote(join_a, artifact_a)
            promotion_b = ReferenceArtifactPromotionService().promote(join_b, artifact_b)
            self.assertNotEqual(promotion_a.promotion_id, promotion_b.promotion_id)

    def test_different_bucket_changes_promotion_id(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_a, tempfile.TemporaryDirectory() as tmp_b:
            join_a, artifact_a = _build_pair(Path(tmp_a))
            join_b, artifact_b = _build_pair(
                Path(tmp_b), bucket="another-syntactically-valid-bucket"
            )
            promotion_a = ReferenceArtifactPromotionService().promote(join_a, artifact_a)
            promotion_b = ReferenceArtifactPromotionService().promote(join_b, artifact_b)
            self.assertNotEqual(promotion_a.promotion_id, promotion_b.promotion_id)

    def test_different_content_changes_promotion_id(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_a, tempfile.TemporaryDirectory() as tmp_b:
            join_a, artifact_a = _build_pair(Path(tmp_a))
            join_b, artifact_b = _build_pair(
                Path(tmp_b),
                rows=[_security_master_row(FinInstrmId="99999", TckrSymb="TCS")],
            )
            promotion_a = ReferenceArtifactPromotionService().promote(join_a, artifact_a)
            promotion_b = ReferenceArtifactPromotionService().promote(join_b, artifact_b)
            self.assertNotEqual(promotion_a.promotion_id, promotion_b.promotion_id)

    def test_different_report_date_changes_promotion_id(self) -> None:
        earlier_date = date(2026, 7, 15)
        with tempfile.TemporaryDirectory() as tmp_a, tempfile.TemporaryDirectory() as tmp_b:
            join_a, artifact_a = _build_pair(Path(tmp_a))
            join_b, artifact_b = _build_pair(
                Path(tmp_b),
                report_date=earlier_date,
                acquired_at="2026-07-15T13:30:00Z",
                first_seen=datetime(2026, 7, 15, 12, 0, tzinfo=UTC),
                validated=datetime(2026, 7, 15, 12, 0, 2, tzinfo=UTC),
            )
            promotion_a = ReferenceArtifactPromotionService().promote(join_a, artifact_a)
            promotion_b = ReferenceArtifactPromotionService().promote(join_b, artifact_b)
            self.assertNotEqual(promotion_a.promotion_id, promotion_b.promotion_id)

    def test_different_knowledge_time_changes_promotion_id(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_a, tempfile.TemporaryDirectory() as tmp_b:
            join_a, artifact_a = _build_pair(Path(tmp_a))
            join_b, artifact_b = _build_pair(
                Path(tmp_b), acquired_at="2026-07-16T14:00:00Z"
            )
            promotion_a = ReferenceArtifactPromotionService().promote(join_a, artifact_a)
            promotion_b = ReferenceArtifactPromotionService().promote(join_b, artifact_b)
            self.assertNotEqual(promotion_a.knowledge_time, promotion_b.knowledge_time)
            self.assertNotEqual(promotion_a.promotion_id, promotion_b.promotion_id)

    def test_different_artifact_or_manifest_identity_changes_promotion_id(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_a, tempfile.TemporaryDirectory() as tmp_b:
            gz_bytes = _security_master_gzip()
            join_a = _join_for(gz_bytes)
            artifact_a = _import_artifact(Path(tmp_a), gz_bytes)
            join_b = _join_for(gz_bytes)
            artifact_b = _import_artifact(
                Path(tmp_b),
                gz_bytes,
                first_seen=_FIRST_SEEN,
                validated=_VALIDATED + timedelta(seconds=5),
            )
            self.assertEqual(artifact_a.manifest.artifact_id, artifact_b.manifest.artifact_id)
            self.assertNotEqual(artifact_a.manifest.manifest_id, artifact_b.manifest.manifest_id)

            promotion_a = ReferenceArtifactPromotionService().promote(join_a, artifact_a)
            promotion_b = ReferenceArtifactPromotionService().promote(join_b, artifact_b)
            self.assertNotEqual(promotion_a.promotion_id, promotion_b.promotion_id)

    def test_different_binding_cutoff_changes_promotion_id(self) -> None:
        # Blocker 2 regression: two otherwise-identical valid receipt/join/
        # artifact graphs whose TrustedReferenceAcquisitionBinding differs
        # only in a still-individually-valid cutoff must not collide under
        # the same promotion_id. The v2 upstream identity content-binds
        # every trusted binding field, so widening cutoff alone is enough
        # to change the promotion_id.
        with tempfile.TemporaryDirectory() as tmp_a, tempfile.TemporaryDirectory() as tmp_b:
            gz_bytes = _security_master_gzip()
            join_a = _join_for(gz_bytes, cutoff=_CUTOFF)
            artifact_a = _import_artifact(Path(tmp_a), gz_bytes)
            join_b = _join_for(gz_bytes, cutoff=_CUTOFF + timedelta(days=1))
            artifact_b = _import_artifact(Path(tmp_b), gz_bytes)
            self.assertNotEqual(join_a.receipt.binding.cutoff, join_b.receipt.binding.cutoff)

            promotion_a = ReferenceArtifactPromotionService().promote(join_a, artifact_a)
            promotion_b = ReferenceArtifactPromotionService().promote(join_b, artifact_b)
            self.assertNotEqual(promotion_a.promotion_id, promotion_b.promotion_id)

    def test_equal_content_under_different_filesystem_path_retains_same_promotion_id(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_a, tempfile.TemporaryDirectory() as tmp_b:
            gz_bytes = _security_master_gzip()
            join_a = _join_for(gz_bytes)
            artifact_a = _import_artifact(Path(tmp_a), gz_bytes)
            join_b = _join_for(gz_bytes)
            artifact_b = _import_artifact(Path(tmp_b), gz_bytes)

            self.assertNotEqual(artifact_a.path, artifact_b.path)
            self.assertEqual(artifact_a.manifest.artifact_id, artifact_b.manifest.artifact_id)
            self.assertEqual(artifact_a.manifest.manifest_id, artifact_b.manifest.manifest_id)

            promotion_a = ReferenceArtifactPromotionService().promote(join_a, artifact_a)
            promotion_b = ReferenceArtifactPromotionService().promote(join_b, artifact_b)
            self.assertEqual(promotion_a.promotion_id, promotion_b.promotion_id)


class ReferenceArtifactPromotionSanitizationTests(unittest.TestCase):
    def test_secret_in_tampered_manifest_never_appears_in_error(self) -> None:
        secret = "SECRET-MANIFEST-do-not-leak-8f2a"
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            join, artifact = _build_pair(root)
            manifest_path = artifact.path / "manifest.json"
            value = json.loads(manifest_path.read_text(encoding="utf-8"))
            value["original_filename"] = secret
            manifest_path.write_text(json.dumps(value), encoding="utf-8")
            with self.assertRaises(ReferenceArtifactPromotionError) as ctx:
                ReferenceArtifactPromotionService().promote(join, artifact)
            self.assertNotIn(secret, str(ctx.exception))

    def test_secret_in_tampered_bucket_never_appears_in_error(self) -> None:
        secret_bucket = "secret-do-not-leak-bucket-value"
        with tempfile.TemporaryDirectory() as tmp:
            join, artifact = _build_pair(Path(tmp))
            bad_manifest = dataclasses.replace(
                artifact.manifest, claimed_download_url=f"https://{secret_bucket}/x.csv.gz"
            )
            bad_artifact = dataclasses.replace(artifact, manifest=bad_manifest)
            with self.assertRaises(ReferenceArtifactPromotionError) as ctx:
                _require_lineage_agreement(join, bad_artifact)
            self.assertNotIn(secret_bucket, str(ctx.exception))

    def test_hash_mismatch_error_never_contains_either_hash(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            join, artifact = _build_pair(Path(tmp))
            tampered_hash = "b" * 64
            bad_manifest = dataclasses.replace(artifact.manifest, raw_sha256=tampered_hash)
            bad_artifact = dataclasses.replace(artifact, manifest=bad_manifest)
            with self.assertRaises(ReferenceArtifactPromotionError) as ctx:
                _require_lineage_agreement(join, bad_artifact)
            message = str(ctx.exception)
            self.assertNotIn(tampered_hash, message)
            self.assertNotIn(join.parsed.raw_sha256, message)


class ReferenceArtifactPromotionCapabilityTests(unittest.TestCase):
    def test_no_io_shaped_capability_exists(self) -> None:
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
            "write",
            "rename",
            "chmod",
            "touch",
            "delete",
        )
        for candidate in (
            ReferenceArtifactPromotionService,
            VerifiedReferenceArtifactPromotion,
        ):
            members = [name for name in dir(candidate) if not name.startswith("__")]
            for name in members:
                lowered = name.lower()
                self.assertFalse(
                    any(bad in lowered for bad in banned_substrings),
                    f"{candidate!r} unexpectedly exposes {name!r}",
                )

    def test_no_forbidden_capability_field_exists_on_class(self) -> None:
        field_names = {
            field.name for field in dataclasses.fields(VerifiedReferenceArtifactPromotion)
        }
        for banned in (
            "calendar",
            "universe",
            "price",
            "liquidity",
            "surveillance",
            "corporate_action",
            "model",
            "signal",
            "ranking",
            "recommendation",
            "notification",
            "order",
            "broker",
            "capital",
        ):
            self.assertFalse(any(banned in name for name in field_names))

    def test_promotion_record_cannot_be_used_as_promotion_evidence(self) -> None:
        from india_swing.promotion.models import PromotionEvidence

        with tempfile.TemporaryDirectory() as tmp:
            join, artifact = _build_pair(Path(tmp))
            promotion = ReferenceArtifactPromotionService().promote(join, artifact)
            self.assertNotIsInstance(promotion, PromotionEvidence)
            self.assertFalse(issubclass(VerifiedReferenceArtifactPromotion, PromotionEvidence))

    def test_importing_module_causes_no_io(self) -> None:
        import india_swing.reference_data.acquisition_promotion as module

        banned_module_names = {"os", "socket", "urllib", "requests", "storage"}
        top_level_names = {
            name
            for name in vars(module)
            if not name.startswith("_") and not name[0].isupper()
        }
        self.assertFalse(top_level_names & banned_module_names)


if __name__ == "__main__":
    unittest.main()
