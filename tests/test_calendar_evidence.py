from __future__ import annotations

import json
import tempfile
import unittest
from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from india_swing.calendar_evidence import (
    POSITIVE_TRADE_DATES_ONLY,
    CalendarEvidenceIntegrityError,
    ObservedMarketDateArtifact,
    build_observed_market_date_artifact,
    encode_observed_market_date_artifact,
)
from india_swing.daily_reports.artifact_store import (
    LocalDailyBundleArtifactStore,
    _manifest_identity as daily_manifest_identity,
)
from india_swing.daily_reports.models import (
    DailyReportFamily,
    StoredDailyBundleArtifact,
)
from india_swing.identity import content_id
from india_swing.reference.models import ReferenceReadiness
from tests.test_reconciliation import _bundle_bytes


UTC = timezone.utc
FIRST_SEEN = datetime(2026, 7, 15, 14, 37, tzinfo=UTC)
VALIDATED = FIRST_SEEN + timedelta(seconds=7)
CUTOFF = VALIDATED + timedelta(seconds=1)


def stored_bundle(
    dates: tuple[date, ...],
    *,
    storage_root: Path,
    first_seen: datetime = FIRST_SEEN,
    validated: datetime = VALIDATED,
) -> StoredDailyBundleArtifact:
    source = storage_root / "source" / "Reports-Daily-Multiple.zip"
    source.parent.mkdir(parents=True)
    source.write_bytes(_bundle_bytes(trade_dates=dates))
    clock_values = iter((first_seen, validated))
    return LocalDailyBundleArtifactStore(
        storage_root / "archive",
        clock=lambda: next(clock_values),
    ).import_bundle(source)


class CalendarEvidenceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.bundle_number = 0

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def stored_bundle(self, dates: tuple[date, ...], **kwargs: object):
        self.bundle_number += 1
        return stored_bundle(
            dates,
            storage_root=self.root / f"bundle-{self.bundle_number}",
            **kwargs,
        )

    def test_records_only_positive_dates_without_inferring_the_gap(self) -> None:
        source = self.stored_bundle((date(2026, 7, 13), date(2026, 7, 15)))

        artifact = build_observed_market_date_artifact(source, cutoff=CUTOFF)

        self.assertEqual(
            artifact.observed_dates,
            (date(2026, 7, 13), date(2026, 7, 15)),
        )
        self.assertNotIn(date(2026, 7, 14), artifact.observed_dates)
        self.assertEqual(artifact.inference_scope, POSITIVE_TRADE_DATES_ONLY)
        self.assertIs(artifact.readiness, ReferenceReadiness.COLLECTION_ONLY)
        self.assertFalse(artifact.actionable)
        self.assertEqual(artifact.knowledge_time, VALIDATED)
        self.assertEqual(
            tuple(ref.family for ref in artifact.observations[0].report_refs),
            (
                DailyReportFamily.UDIFF_BHAVCOPY,
                DailyReportFamily.FULL_BHAVCOPY_DELIVERY,
            ),
        )
        self.assertFalse(hasattr(artifact, "session_windows"))
        self.assertFalse(hasattr(artifact, "next_session"))

    def test_validation_time_is_the_cutoff_not_trade_or_filename_date(self) -> None:
        source = self.stored_bundle((date(2026, 7, 14),))

        with self.assertRaisesRegex(
            CalendarEvidenceIntegrityError,
            "not validated",
        ):
            build_observed_market_date_artifact(
                source,
                cutoff=VALIDATED - timedelta(microseconds=1),
            )

        artifact = build_observed_market_date_artifact(source, cutoff=VALIDATED)
        self.assertEqual(artifact.knowledge_time, VALIDATED)
        self.assertEqual(
            artifact.observations[0].report_refs[0].knowledge_time,
            VALIDATED,
        )

    def test_future_or_intraday_final_reports_cannot_become_date_evidence(self) -> None:
        future = self.stored_bundle((date(2026, 7, 16),))
        early_validation = datetime(2026, 7, 15, 14, 29, tzinfo=UTC)
        intraday = self.stored_bundle(
            (date(2026, 7, 15),),
            first_seen=early_validation - timedelta(seconds=1),
            validated=early_validation,
        )

        for source, cutoff in (
            (future, CUTOFF),
            (intraday, datetime(2026, 7, 15, 14, 30, tzinfo=UTC)),
        ):
            with self.subTest(), self.assertRaisesRegex(
                CalendarEvidenceIntegrityError,
                "event boundary",
            ):
                build_observed_market_date_artifact(source, cutoff=cutoff)

    def test_strict_store_supplies_one_udiff_full_pair_per_observed_date(self) -> None:
        source = self.stored_bundle(
            (date(2026, 7, 14), date(2026, 7, 15)),
        )

        artifact = build_observed_market_date_artifact(source, cutoff=CUTOFF)

        self.assertEqual(len(artifact.observations), 2)
        self.assertTrue(
            all(
                tuple(reference.family for reference in observation.report_refs)
                == (
                    DailyReportFamily.UDIFF_BHAVCOPY,
                    DailyReportFamily.FULL_BHAVCOPY_DELIVERY,
                )
                for observation in artifact.observations
            )
        )

    def test_source_bytes_normalized_payload_and_ids_are_reverified(self) -> None:
        source = self.stored_bundle((date(2026, 7, 14),))
        raw_tampered = replace(source, raw_bytes=b"different")
        normalized_tampered = replace(source, normalized_bytes=b"{}")
        id_tampered = self.stored_bundle((date(2026, 7, 14),))
        object.__setattr__(id_tampered.manifest, "artifact_id", "f" * 64)
        changed_time = replace(
            source.manifest,
            validated_at=source.manifest.validated_at + timedelta(microseconds=1),
        )
        forged_manifest = replace(
            changed_time,
            manifest_id=content_id(daily_manifest_identity(changed_time), length=64),
        )
        availability_forged = replace(source, manifest=forged_manifest)

        with self.assertRaisesRegex(CalendarEvidenceIntegrityError, "raw hash"):
            build_observed_market_date_artifact(raw_tampered, cutoff=CUTOFF)
        with self.assertRaisesRegex(CalendarEvidenceIntegrityError, "not deterministic"):
            build_observed_market_date_artifact(normalized_tampered, cutoff=CUTOFF)
        with self.assertRaisesRegex(CalendarEvidenceIntegrityError, "artifact ID"):
            build_observed_market_date_artifact(id_tampered, cutoff=CUTOFF)
        with self.assertRaisesRegex(CalendarEvidenceIntegrityError, "sealed provenance"):
            build_observed_market_date_artifact(
                availability_forged,
                cutoff=CUTOFF,
            )

    def test_identity_and_codec_are_deterministic_and_lineage_complete(self) -> None:
        source = self.stored_bundle((date(2026, 7, 14), date(2026, 7, 15)))
        first = build_observed_market_date_artifact(source, cutoff=CUTOFF)
        second = build_observed_market_date_artifact(source, cutoff=CUTOFF)

        self.assertEqual(first.artifact_id, second.artifact_id)
        encoded = encode_observed_market_date_artifact(first)
        self.assertEqual(encoded, encode_observed_market_date_artifact(second))
        value = json.loads(encoded)
        self.assertEqual(value["artifact_id"], first.artifact_id)
        self.assertEqual(
            value["source"]["bundle_artifact_id"],
            source.manifest.artifact_id,
        )
        self.assertEqual(value["source"]["validated_at"], VALIDATED.isoformat())
        self.assertNotIn("session_windows", value)
        self.assertNotIn("calendar_day_kind", value)

        offset_cutoff = CUTOFF.astimezone(timezone(timedelta(hours=5, minutes=30)))
        offset_artifact = build_observed_market_date_artifact(
            source,
            cutoff=offset_cutoff,
        )
        self.assertEqual(offset_artifact.artifact_id, first.artifact_id)
        self.assertEqual(
            encode_observed_market_date_artifact(offset_artifact),
            encoded,
        )

    def test_manual_evidence_cannot_be_upgraded_or_made_actionable(self) -> None:
        artifact = build_observed_market_date_artifact(
            self.stored_bundle((date(2026, 7, 14),)),
            cutoff=CUTOFF,
        )

        with self.assertRaisesRegex(ValueError, "remain collection-only"):
            replace(
                artifact,
                readiness=ReferenceReadiness.POINT_IN_TIME_VERIFIED,
            )
        with self.assertRaisesRegex(ValueError, "remain collection-only"):
            replace(artifact, actionable=True)

    def test_nested_mutation_is_detected_before_encoding(self) -> None:
        artifact = build_observed_market_date_artifact(
            self.stored_bundle((date(2026, 7, 14),)),
            cutoff=CUTOFF,
        )
        object.__setattr__(artifact.observations[0], "market_date", date(2026, 7, 15))

        with self.assertRaisesRegex(
            CalendarEvidenceIntegrityError,
            "content identity",
        ):
            encode_observed_market_date_artifact(artifact)

        reference = artifact.observations[0].report_refs[0]
        with self.assertRaisesRegex(
            CalendarEvidenceIntegrityError,
            "final-report boundary",
        ):
            replace(reference, trade_date=date(2030, 1, 2))

    def test_builder_rejects_subclassed_source_artifact(self) -> None:
        class ForgedStoredArtifact(StoredDailyBundleArtifact):
            pass

        source = self.stored_bundle((date(2026, 7, 14),))
        forged = ForgedStoredArtifact(
            path=source.path,
            manifest=source.manifest,
            parsed=source.parsed,
            raw_bytes=source.raw_bytes,
            normalized_bytes=source.normalized_bytes,
        )

        with self.assertRaisesRegex(TypeError, "exact StoredDailyBundleArtifact"):
            build_observed_market_date_artifact(forged, cutoff=CUTOFF)


if __name__ == "__main__":
    unittest.main()
