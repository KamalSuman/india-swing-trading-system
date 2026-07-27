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

import india_swing.universe.promoted_identity as promoted_identity_module
from india_swing.calendar_data import (
    CollectionCalendarMaterialization,
    LocalCalendarSourceArtifactStore,
    materialize_collection_calendar,
)
from india_swing.calendar_data.models import CALENDAR_DECLARATION_SCHEMA_VERSION
from india_swing.daily_pipeline.acquisition import GCSLandingObjectReader, GCSObjectPayload
from india_swing.identity_decisions import (
    IDENTITY_REVIEW_DECLARATION_SCHEMA_VERSION,
    LocalIdentityReviewBundleStore,
    PromotedIdentityAdjudicationService,
    VerifiedPromotedIdentityAdjudication,
)
from india_swing.identity_evidence import (
    IDENTITY_EVIDENCE_DECLARATION_SCHEMA_VERSION,
    LocalIdentityEvidenceArtifactStore,
)
from india_swing.identity_registry.promoted_intake import PromotedIdentityIntakeService
from india_swing.reference.models import ReferenceReadiness
from india_swing.reference.universe import UniverseEntry, UniverseSnapshot
from india_swing.reference_data.acquisition_join import ReferenceAcquisitionJoinService
from india_swing.reference_data.acquisition_promotion import (
    ReferenceArtifactPromotionService,
    VerifiedReferenceArtifactPromotion,
)
from india_swing.reference_data.acquisition_receipt import (
    ReferenceAcquisitionReceiptVerifier,
    TrustedReferenceAcquisitionBinding,
)
from india_swing.reference_data.artifact_store import LocalReferenceArtifactStore
from india_swing.reference_data.security_master import NSE_CM_MII_SECURITY_HEADER
from india_swing.promotion.models import PromotionDecision, PromotionEvidence
from india_swing.universe import (
    PromotedIdentitySessionDisposition,
    PromotedIdentitySessionEntry,
    PromotedIdentitySessionUniverseError,
    PromotedIdentitySessionUniverseService,
    VerifiedPromotedIdentitySessionUniverse,
)
from india_swing.universe.promoted_identity import _build_entries, _build_session_universe_facts


UTC = timezone.utc
BUCKET = "trusted-bucket"
ACQUIRER_ID = "a" * 64
D1 = date(2026, 7, 15)
D2 = date(2026, 7, 16)
D0 = date(2026, 7, 14)
PDF_BYTES = b"%PDF-1.7\n1 0 obj\n<< /Type /Catalog >>\nendobj\n%%EOF\n"
INTAKE_CUTOFF = datetime(2026, 7, 16, 14, 0, tzinfo=UTC)
EVIDENCE_AT = datetime(2026, 7, 16, 15, 0, tzinfo=UTC)
REVIEWED_AT = datetime(2026, 7, 16, 15, 5, tzinfo=UTC)
ADJUDICATION_CUTOFF = datetime(2026, 7, 16, 16, 0, tzinfo=UTC)
CALENDAR_CUTOFF = datetime(2026, 7, 16, 16, 0, tzinfo=UTC)
SESSION_CUTOFF = datetime(2026, 7, 16, 17, 0, tzinfo=UTC)


def _filename(report_date: date) -> str:
    return f"NSE_CM_security_{report_date.strftime('%d%m%Y')}.csv.gz"


def _object_name(report_date: date) -> str:
    return f"landing/{report_date.isoformat()}/{_filename(report_date)}"


def _requested_url(report_date: date) -> str:
    return f"https://nsearchives.nseindia.com/content/cm/{_filename(report_date)}"


def security_row(**overrides: str) -> dict[str, str]:
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


def _csv_bytes(rows: list[dict[str, str]]) -> bytes:
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(NSE_CM_MII_SECURITY_HEADER)
    for row in rows:
        writer.writerow([row[name] for name in NSE_CM_MII_SECURITY_HEADER])
    return buf.getvalue().encode("utf-8")


def _security_master_gzip(rows: list[dict[str, str]]) -> bytes:
    return gzip.compress(_csv_bytes(rows), mtime=0)


class FakeGCSObjectReader:
    def __init__(self, *, generation: int, content_bytes: bytes) -> None:
        self.generation = generation
        self.content_bytes = content_bytes

    def read_generation(
        self, *, bucket: str, object_name: str, generation: int, maximum_bytes: int
    ) -> GCSObjectPayload:
        return GCSObjectPayload(content_bytes=self.content_bytes, generation=self.generation)


def build_promotion(
    root: Path,
    *,
    report_date: date,
    generation: int,
    rows: list[dict[str, str]],
    first_seen: datetime,
    validated: datetime,
) -> VerifiedReferenceArtifactPromotion:
    gz_bytes = _security_master_gzip(rows)
    fn = _filename(report_date)
    raw_sha256 = hashlib.sha256(gz_bytes).hexdigest()
    receipt_dict = {
        "schema_version": 1,
        "dataset": "nse-cm-mii-security",
        "authority": "NSE",
        "acquirer_id": ACQUIRER_ID,
        "acquired_at": f"{report_date.isoformat()}T13:30:00Z",
        "report_date": report_date.isoformat(),
        "requested_url": _requested_url(report_date),
        "response_status": 200,
        "response_media_type": "application/gzip",
        "raw_byte_count": len(gz_bytes),
        "raw_sha256": raw_sha256,
        "landing_object": {
            "file_type": "SECURITY_MASTER",
            "bucket": BUCKET,
            "object_name": _object_name(report_date),
            "generation": generation,
            "sha256": raw_sha256,
        },
    }
    receipt_bytes = json.dumps(receipt_dict, separators=(",", ":")).encode("utf-8")
    not_before = datetime.combine(report_date, datetime.min.time(), tzinfo=UTC)
    cutoff_bound = datetime.combine(report_date, datetime.max.time(), tzinfo=UTC).replace(
        microsecond=0
    )
    binding = TrustedReferenceAcquisitionBinding(
        expected_receipt_sha256=hashlib.sha256(receipt_bytes).hexdigest(),
        expected_raw_sha256=raw_sha256,
        allowed_bucket=BUCKET,
        target_report_date=report_date,
        not_before=not_before,
        cutoff=cutoff_bound,
        trusted_acquirer_id=ACQUIRER_ID,
    )
    receipt = ReferenceAcquisitionReceiptVerifier().verify(receipt_bytes, binding)
    fake = FakeGCSObjectReader(generation=generation, content_bytes=gz_bytes)
    reader = GCSLandingObjectReader(fake)
    join = ReferenceAcquisitionJoinService(reader).join(receipt)

    source_dir = root / f"source-{report_date.isoformat()}-{generation}"
    source_dir.mkdir(parents=True, exist_ok=True)
    source_file = source_dir / fn
    source_file.write_bytes(gz_bytes)
    calls = iter((first_seen, validated))
    store = LocalReferenceArtifactStore(
        root / f"archive-{report_date.isoformat()}-{generation}", clock=lambda: next(calls)
    )
    artifact = store.import_security_master(source_file)
    return ReferenceArtifactPromotionService().promote(join, artifact)


def build_evidence(
    root: Path,
    *,
    candidate_id: str,
    requirements: tuple,
    symbol: str,
    series: str,
    isin: str,
    suffix: str,
    first_seen: datetime = EVIDENCE_AT,
    validated: datetime = EVIDENCE_AT,
):
    source = root / f"CML-{suffix}.pdf"
    declaration = root / f"CML-{suffix}.evidence.json"
    source.write_bytes(PDF_BYTES)
    claims = []
    for requirement in requirements:
        claims.append(
            {
                "candidate_id": candidate_id,
                "requirement": requirement.value,
                "effective_date": (
                    "2026-07-16"
                    if requirement.value
                    in {"OFFICIAL_LISTING_LIFECYCLE", "OFFICIAL_LISTING_STATUS"}
                    else None
                ),
                "symbol": symbol,
                "series": series,
                "isin": isin,
                "locator": {"page": 1, "row": None, "section": requirement.value},
                "claim_text": f"Synthetic official-source claim for {requirement.value}.",
            }
        )
    value = {
        "schema_version": IDENTITY_EVIDENCE_DECLARATION_SCHEMA_VERSION,
        "exchange": "NSE",
        "segment": "CM",
        "claimed_authority": "NSE",
        "source_kind": "LISTING_CIRCULAR_PDF",
        "claimed_document_id": f"NSE/LIST/C/2026/TEST{suffix.upper()}",
        "claimed_issue_date": "2026-07-16",
        "claimed_publication_at": None,
        "claimed_source_url": f"https://nsearchives.nseindia.com/content/circulars/{source.name}",
        "source_filename": source.name,
        "source_media_type": "application/pdf",
        "source_byte_count": len(PDF_BYTES),
        "source_sha256": hashlib.sha256(PDF_BYTES).hexdigest(),
        "claims": claims,
    }
    declaration.write_text(json.dumps(value), encoding="utf-8")
    calls = iter((first_seen, validated))
    return LocalIdentityEvidenceArtifactStore(
        root / "evidence", clock=lambda: next(calls)
    ).import_source(source, declaration)


def build_review(
    root: Path,
    *,
    queue_id: str,
    source_registry_id: str,
    candidate_id: str,
    requirements: tuple,
    evidence,
    reviewer_id: str = "owner:kamal",
    reviewed_at: datetime = REVIEWED_AT,
    first_seen: datetime = REVIEWED_AT,
    validated: datetime = REVIEWED_AT,
    suffix: str = "review",
):
    claims_by_requirement = {value.requirement.value: value for value in evidence.parsed.claims}
    decisions = []
    for requirement in requirements:
        evidence_claim = claims_by_requirement[requirement.value]
        decisions.append(
            {
                "candidate_id": candidate_id,
                "requirement": requirement.value,
                "outcome": "ACCEPTED",
                "evidence_artifact_id": evidence.manifest.artifact_id,
                "evidence_claim_id": evidence_claim.claim_id,
                "rationale": f"Reviewed exact evidence for {requirement.value}.",
            }
        )
    value = {
        "schema_version": IDENTITY_REVIEW_DECLARATION_SCHEMA_VERSION,
        "queue_id": queue_id,
        "source_registry_id": source_registry_id,
        "reviewer_id": reviewer_id,
        "reviewed_at": reviewed_at.isoformat(),
        "decisions": decisions,
    }
    path = root / f"review-{suffix}.json"
    path.write_text(json.dumps(value), encoding="utf-8")
    calls = iter((first_seen, validated))
    return LocalIdentityReviewBundleStore(
        root / "evidence", clock=lambda: next(calls)
    ).import_declaration(path)


def _base_event(exchange_weekdays=("MON", "TUE", "WED", "THU", "FRI")) -> dict[str, object]:
    return {
        "event_type": "BASE_WEEKLY_SCHEDULE",
        "effective_from": "2026-01-01",
        "effective_to_exclusive": "2027-01-01",
        "weekdays": list(exchange_weekdays),
        "windows": [{"phase": "LIVE_CONTINUOUS", "opens": "09:15:00", "closes": "15:30:00"}],
        "supersedes_event_ids": [],
        "source_locator": {"page": 1, "section": "CM schedule", "record": "regular"},
        "reason": "Regular capital-market schedule",
    }


def _closed(day: date, predecessor: str, kind: str = "HOLIDAY") -> dict[str, object]:
    return {
        "event_type": "DATE_CLOSED",
        "date": day.isoformat(),
        "day_kind": kind,
        "supersedes_event_ids": [predecessor],
        "source_locator": {"page": 1, "section": "CM schedule", "record": f"closed-{day}"},
        "reason": "Explicit dated closure",
    }


def import_calendar_source(
    root: Path,
    document_id: str,
    events: list[dict[str, object]],
    *,
    validated_at: datetime,
    exchange: str = "NSE",
    segment: str = "CM",
):
    source_bytes = f"%PDF-1.7\n{document_id}\n%%EOF\n".encode("ascii")
    input_root = root / "cal_inputs"
    input_root.mkdir(parents=True, exist_ok=True)
    source_path = input_root / f"{document_id}.pdf"
    declaration_path = input_root / f"{document_id}.events.json"
    source_path.write_bytes(source_bytes)
    declaration = {
        "schema_version": CALENDAR_DECLARATION_SCHEMA_VERSION,
        "exchange": exchange,
        "segment": segment,
        "claimed_authority": "NSE",
        "claimed_document_id": document_id,
        "claimed_issue_date": "2026-01-01",
        "claimed_source_url": f"https://example.invalid/{document_id}.pdf",
        "source_filename": source_path.name,
        "source_media_type": "application/pdf",
        "source_byte_count": len(source_bytes),
        "source_sha256": hashlib.sha256(source_bytes).hexdigest(),
        "events": events,
    }
    declaration_path.write_text(json.dumps(declaration, separators=(",", ":")), encoding="utf-8")
    calls = iter((validated_at - timedelta(seconds=1), validated_at))
    return LocalCalendarSourceArtifactStore(
        root / f"cal_archive_{document_id}", clock=lambda: next(calls)
    ).import_source(source_path, declaration_path)


def build_calendar(
    root: Path,
    *,
    coverage_start: date = D0,
    coverage_end: date = D2,
    cutoff: datetime = CALENDAR_CUTOFF,
    holiday_on: date | None = None,
    exchange: str = "NSE",
    segment: str = "CM",
) -> CollectionCalendarMaterialization:
    base = import_calendar_source(
        root, "CMTR-BASE", [_base_event()], validated_at=D0, exchange=exchange, segment=segment
    ) if False else import_calendar_source(
        root,
        "CMTR-BASE",
        [_base_event()],
        validated_at=datetime(2026, 7, 1, 12, 0, tzinfo=UTC),
        exchange=exchange,
        segment=segment,
    )
    sources = (base,)
    if holiday_on is not None:
        base_id = base.parsed.events[0].event_id
        holiday = import_calendar_source(
            root,
            "CMTR-HOLIDAY",
            [_closed(holiday_on, base_id)],
            validated_at=datetime(2026, 7, 1, 12, 0, 1, tzinfo=UTC),
            exchange=exchange,
            segment=segment,
        )
        sources = (base, holiday)
    return materialize_collection_calendar(
        sources=sources, coverage_start=coverage_start, coverage_end=coverage_end, cutoff=cutoff
    )


def build_intake_and_adjudication(root: Path, *, include_reliance_review: bool = True):
    p1 = build_promotion(
        root,
        report_date=D1,
        generation=100,
        rows=[
            security_row(),
            security_row(FinInstrmId="22222", TckrSymb="SMALL1", ISIN="INE001A01036"),
        ],
        first_seen=datetime(2026, 7, 15, 12, 0, tzinfo=UTC),
        validated=datetime(2026, 7, 15, 12, 0, 2, tzinfo=UTC),
    )
    p2 = build_promotion(
        root,
        report_date=D2,
        generation=200,
        rows=[
            security_row(),
            security_row(FinInstrmId="22222", TckrSymb="SMALL1", ISIN="INE001A01036"),
            security_row(FinInstrmId="30000", TckrSymb="NONEQ", SctyTpFlg="1"),
            security_row(FinInstrmId="40000", TckrSymb="NSETEST"),
            security_row(
                FinInstrmId="50000",
                TckrSymb="DELISTD",
                ISIN="INE000052A02",
                DelFlg="Y",
                RmvlDt="20260601",
            ),
        ],
        first_seen=datetime(2026, 7, 16, 12, 0, tzinfo=UTC),
        validated=datetime(2026, 7, 16, 12, 0, 2, tzinfo=UTC),
    )
    intake = PromotedIdentityIntakeService().materialize(
        promotions=(p1, p2), expected_report_dates=(D1, D2), cutoff=INTAKE_CUTOFF
    )
    observation_by_id = {value.observation_id: value for value in intake.observations}
    reliance_case = next(
        c
        for c in intake.queue.cases
        if any(
            observation_by_id[oid].ticker_symbol == "RELIANCE"
            for oid in next(
                cand.observation_ids
                for cand in intake.candidates
                if cand.candidate_id == c.candidate_id
            )
        )
    )
    status = next(
        s for s in intake.requirement_statuses if s.candidate_id == reliance_case.candidate_id
    )
    evidence_artifacts: tuple = ()
    review_bundles: tuple = ()
    if include_reliance_review:
        evidence = build_evidence(
            root,
            candidate_id=reliance_case.candidate_id,
            requirements=status.unresolved_requirements,
            symbol="RELIANCE",
            series="EQ",
            isin="INE002A01018",
            suffix="reliance",
        )
        review = build_review(
            root,
            queue_id=intake.queue.queue_id,
            source_registry_id=intake.source_graph_id,
            candidate_id=reliance_case.candidate_id,
            requirements=status.unresolved_requirements,
            evidence=evidence,
            suffix="reliance",
        )
        evidence_artifacts = (evidence,)
        review_bundles = (review,)
    adjudication = PromotedIdentityAdjudicationService().materialize(
        intake=intake,
        evidence_artifacts=evidence_artifacts,
        review_bundles=review_bundles,
        cutoff=ADJUDICATION_CUTOFF,
    )
    return p1, p2, intake, reliance_case, status, adjudication


def build_alternate_intake_and_adjudication(root: Path):
    """A genuinely different fixture (different generations/tickers/ISIN
    from build_intake_and_adjudication), used to prove substitution of an
    otherwise-valid-but-different adjudication is detected rather than
    silently accepted because two independently built copies of the same
    deterministic fixture happen to be content-identical."""

    p1 = build_promotion(
        root,
        report_date=D1,
        generation=900,
        rows=[security_row(FinInstrmId="70001", TckrSymb="ALTONE", ISIN="INE001A01036")],
        first_seen=datetime(2026, 7, 15, 12, 0, tzinfo=UTC),
        validated=datetime(2026, 7, 15, 12, 0, 2, tzinfo=UTC),
    )
    p2 = build_promotion(
        root,
        report_date=D2,
        generation=901,
        rows=[security_row(FinInstrmId="70001", TckrSymb="ALTONE", ISIN="INE001A01036")],
        first_seen=datetime(2026, 7, 16, 12, 0, tzinfo=UTC),
        validated=datetime(2026, 7, 16, 12, 0, 2, tzinfo=UTC),
    )
    intake = PromotedIdentityIntakeService().materialize(
        promotions=(p1, p2), expected_report_dates=(D1, D2), cutoff=INTAKE_CUTOFF
    )
    adjudication = PromotedIdentityAdjudicationService().materialize(
        intake=intake, evidence_artifacts=(), review_bundles=(), cutoff=ADJUDICATION_CUTOFF
    )
    return p1, p2, intake, adjudication


def happy_path_fixture(root: Path):
    p1, p2, intake, reliance_case, status, adjudication = build_intake_and_adjudication(root)
    calendar = build_calendar(root)
    universe = PromotedIdentitySessionUniverseService().materialize(
        adjudication=adjudication,
        calendar=calendar,
        market_session=D2,
        cutoff=SESSION_CUTOFF,
    )
    return p1, p2, adjudication, calendar, universe


def _kwargs_from(universe: VerifiedPromotedIdentitySessionUniverse) -> dict[str, object]:
    return {field.name: getattr(universe, field.name) for field in dataclasses.fields(universe)}


class PromotedIdentitySessionUniverseAcceptanceTests(unittest.TestCase):
    def test_resolved_and_unresolved_equities_both_stay_in_scope(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _, _, adjudication, calendar, universe = happy_path_fixture(root)

            by_symbol = {entry.symbol: entry for entry in universe.entries}
            self.assertEqual(
                by_symbol["RELIANCE"].disposition,
                PromotedIdentitySessionDisposition.IDENTITY_RESOLVED_COLLECTION_ONLY,
            )
            self.assertIsNotNone(by_symbol["RELIANCE"].stable_instrument_id)
            self.assertEqual(
                by_symbol["SMALL1"].disposition,
                PromotedIdentitySessionDisposition.IDENTITY_UNRESOLVED,
            )
            self.assertIsNone(by_symbol["SMALL1"].stable_instrument_id)
            self.assertEqual(len(universe.entries), 5)
            self.assertIs(universe.readiness, ReferenceReadiness.COLLECTION_ONLY)
            self.assertFalse(universe.actionable)
            self.assertFalse(universe.execution_eligible)
            universe.verify_content_identity()

    def test_deleted_security_is_retained_verbatim_not_dropped_or_converted(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _, _, _, _, universe = happy_path_fixture(root)
            by_symbol = {entry.symbol: entry for entry in universe.entries}
            deleted = by_symbol["DELISTD"]
            self.assertEqual(deleted.delete_flag, "Y")
            self.assertNotEqual(deleted.removal_timestamp, 0)
            self.assertEqual(
                deleted.disposition,
                PromotedIdentitySessionDisposition.IDENTITY_UNRESOLVED,
            )
            field_names = {field.name for field in dataclasses.fields(PromotedIdentitySessionEntry)}
            self.assertNotIn("effective_interval", field_names)
            self.assertNotIn("delisted_at", field_names)
            self.assertNotIn("lifecycle_interval", field_names)

    def test_every_row_gets_exactly_one_disposition_covering_all_five_rows(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _, p2, _, _, universe = happy_path_fixture(root)
            self.assertEqual(len(universe.entries), len(p2.artifact.parsed.records))
            source_by_id = {
                record.source_record_id: record
                for record in p2.artifact.parsed.records
            }
            self.assertEqual(
                {entry.source_record_id for entry in universe.entries},
                set(source_by_id),
            )
            for entry in universe.entries:
                self.assertIs(
                    entry.source_disposition,
                    source_by_id[entry.source_record_id].disposition,
                )
            dispositions = {entry.disposition for entry in universe.entries}
            self.assertEqual(
                dispositions,
                {
                    PromotedIdentitySessionDisposition.IDENTITY_RESOLVED_COLLECTION_ONLY,
                    PromotedIdentitySessionDisposition.IDENTITY_UNRESOLVED,
                    PromotedIdentitySessionDisposition.EXCLUDED_NON_EQUITY,
                    PromotedIdentitySessionDisposition.EXCLUDED_TEST_SECURITY,
                },
            )

    def test_exact_reason_code_sets_per_category(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _, _, _, _, universe = happy_path_fixture(root)
            by_symbol = {entry.symbol: entry for entry in universe.entries}
            resolved_reasons = set(by_symbol["RELIANCE"].reason_codes)
            self.assertEqual(
                resolved_reasons,
                {
                    "COLLECTION_ONLY_IDENTITY_EVIDENCE",
                    "POINT_IN_TIME_LISTING_INTERVAL_UNVERIFIED",
                    "BOARD_CLASSIFICATION_UNVERIFIED",
                    "SURVEILLANCE_STATE_UNAVAILABLE",
                    "LIQUIDITY_UNAVAILABLE",
                },
            )
            unresolved_reasons = set(by_symbol["SMALL1"].reason_codes)
            self.assertTrue(resolved_reasons.issubset(unresolved_reasons))
            self.assertIn("STABLE_IDENTITY_UNRESOLVED", unresolved_reasons)
            self.assertTrue(
                any(code.startswith("UNRESOLVED_REQUIREMENT_") for code in unresolved_reasons)
            )
            self.assertEqual(
                set(by_symbol["NONEQ"].reason_codes),
                {"SOURCE_EXCLUSION_EXCLUDED_NON_EQUITY"},
            )
            self.assertEqual(
                set(by_symbol["NSETEST"].reason_codes),
                {"SOURCE_EXCLUSION_EXCLUDED_TEST_SECURITY"},
            )

    def test_entries_are_canonically_sorted_by_source_record_id(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _, _, _, _, universe = happy_path_fixture(root)
            self.assertEqual(
                universe.entries,
                tuple(sorted(universe.entries, key=lambda value: value.source_record_id)),
            )

    def test_no_market_cap_or_similar_filter_capability_exists(self) -> None:
        banned_substrings = (
            "market_cap",
            "marketcap",
            "index",
            "price",
            "volume",
            "liquidity",
            "model_score",
            "score",
            "rank",
        )
        for candidate in (
            PromotedIdentitySessionUniverseService,
            VerifiedPromotedIdentitySessionUniverse,
            PromotedIdentitySessionEntry,
        ):
            members = [name for name in dir(candidate) if not name.startswith("__")]
            for name in members:
                lowered = name.lower()
                self.assertFalse(
                    any(bad in lowered for bad in banned_substrings),
                    f"{candidate!r} unexpectedly exposes {name!r}",
                )


class PromotedIdentitySessionUniverseSelectionRejectionTests(unittest.TestCase):
    def test_no_promotion_matches_market_session_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _, _, _, _, _, adjudication = build_intake_and_adjudication(root)
            calendar = build_calendar(root, coverage_start=D0, coverage_end=D2)
            with self.assertRaises(PromotedIdentitySessionUniverseError):
                PromotedIdentitySessionUniverseService().materialize(
                    adjudication=adjudication,
                    calendar=calendar,
                    market_session=D0,
                    cutoff=SESSION_CUTOFF,
                )

    def test_holiday_market_session_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _, _, _, _, _, adjudication = build_intake_and_adjudication(root)
            calendar = build_calendar(root, holiday_on=D2)
            with self.assertRaises(PromotedIdentitySessionUniverseError):
                PromotedIdentitySessionUniverseService().materialize(
                    adjudication=adjudication,
                    calendar=calendar,
                    market_session=D2,
                    cutoff=SESSION_CUTOFF,
                )

    def test_weekend_market_session_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            weekend = date(2026, 7, 18)  # Saturday
            p1, p2, intake, reliance_case, status, adjudication = build_intake_and_adjudication(
                root
            )
            calendar = build_calendar(root, coverage_start=D1, coverage_end=weekend)
            with self.assertRaises(PromotedIdentitySessionUniverseError):
                PromotedIdentitySessionUniverseService().materialize(
                    adjudication=adjudication,
                    calendar=calendar,
                    market_session=weekend,
                    cutoff=SESSION_CUTOFF,
                )

    def test_coverage_outside_market_session_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _, _, _, _, _, adjudication = build_intake_and_adjudication(root)
            calendar = build_calendar(root, coverage_start=D1, coverage_end=D1)
            with self.assertRaises(PromotedIdentitySessionUniverseError):
                PromotedIdentitySessionUniverseService().materialize(
                    adjudication=adjudication,
                    calendar=calendar,
                    market_session=D2,
                    cutoff=SESSION_CUTOFF,
                )

    def test_wrong_exchange_calendar_is_rejected(self) -> None:
        # The calendar_data source parser itself already forbids anything but
        # NSE/CM declarations, so a "wrong exchange" CollectionCalendarMateri
        # alization can never be legitimately constructed -- this exercises
        # this module's own defense-in-depth check via direct mutation of an
        # otherwise-valid calendar (object.__setattr__, bypassing the source
        # parser entirely), calling the service on the mutated value.
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _, _, adjudication, calendar, _ = happy_path_fixture(root)
            object.__setattr__(calendar, "exchange", "BSE")
            with self.assertRaises(PromotedIdentitySessionUniverseError):
                PromotedIdentitySessionUniverseService().materialize(
                    adjudication=adjudication,
                    calendar=calendar,
                    market_session=D2,
                    cutoff=SESSION_CUTOFF,
                )

    def test_wrong_segment_calendar_is_rejected(self) -> None:
        # Same rationale as test_wrong_exchange_calendar_is_rejected above:
        # the source parser already forbids a non-CM segment, so this proves
        # the defense-in-depth check via direct mutation instead.
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _, _, adjudication, calendar, _ = happy_path_fixture(root)
            object.__setattr__(calendar, "segment", "FO")
            with self.assertRaises(PromotedIdentitySessionUniverseError):
                PromotedIdentitySessionUniverseService().materialize(
                    adjudication=adjudication,
                    calendar=calendar,
                    market_session=D2,
                    cutoff=SESSION_CUTOFF,
                )

    def test_cutoff_before_adjudication_knowledge_time_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _, _, adjudication, calendar, _ = happy_path_fixture(root)
            with self.assertRaises(PromotedIdentitySessionUniverseError):
                PromotedIdentitySessionUniverseService().materialize(
                    adjudication=adjudication,
                    calendar=calendar,
                    market_session=D2,
                    cutoff=adjudication.snapshot.knowledge_time - timedelta(days=1),
                )

    def test_cutoff_before_calendar_cutoff_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _, _, adjudication, calendar, _ = happy_path_fixture(root)
            with self.assertRaises(PromotedIdentitySessionUniverseError):
                PromotedIdentitySessionUniverseService().materialize(
                    adjudication=adjudication,
                    calendar=calendar,
                    market_session=D2,
                    cutoff=calendar.cutoff - timedelta(days=1),
                )

    def test_wrong_type_adjudication_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _, _, _, calendar, _ = happy_path_fixture(root)
            with self.assertRaises(PromotedIdentitySessionUniverseError):
                PromotedIdentitySessionUniverseService().materialize(
                    adjudication="not-an-adjudication",  # type: ignore[arg-type]
                    calendar=calendar,
                    market_session=D2,
                    cutoff=SESSION_CUTOFF,
                )

    def test_wrong_type_calendar_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _, _, adjudication, _, _ = happy_path_fixture(root)
            with self.assertRaises(PromotedIdentitySessionUniverseError):
                PromotedIdentitySessionUniverseService().materialize(
                    adjudication=adjudication,
                    calendar="not-a-calendar",  # type: ignore[arg-type]
                    market_session=D2,
                    cutoff=SESSION_CUTOFF,
                )

    def test_naive_cutoff_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _, _, adjudication, calendar, _ = happy_path_fixture(root)
            with self.assertRaises(PromotedIdentitySessionUniverseError):
                PromotedIdentitySessionUniverseService().materialize(
                    adjudication=adjudication,
                    calendar=calendar,
                    market_session=D2,
                    cutoff=datetime(2026, 7, 16, 17, 0),
                )

    def test_earlier_vintage_selection_resolves_with_its_own_effective_listing(self) -> None:
        # The shared adjudication core assigns one EffectiveStableListingObs
        # ervation per underlying observation (each carrying its own
        # effective_on equal to that vintage's claimed_report_date), not a
        # single one pinned only to the latest vintage. Selecting D1 must
        # therefore still resolve RELIANCE correctly against D1's own
        # listing -- proving the per-row observation-to-listing join checks
        # real identity agreement, not merely "some listing exists somewhere".
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _, _, _, _, _, adjudication = build_intake_and_adjudication(root)
            calendar = build_calendar(root, coverage_start=D1, coverage_end=D2)
            universe = PromotedIdentitySessionUniverseService().materialize(
                adjudication=adjudication,
                calendar=calendar,
                market_session=D1,
                cutoff=SESSION_CUTOFF,
            )
            reliance = next(e for e in universe.entries if e.symbol == "RELIANCE")
            self.assertEqual(
                reliance.disposition,
                PromotedIdentitySessionDisposition.IDENTITY_RESOLVED_COLLECTION_ONLY,
            )
            self.assertIsNotNone(reliance.stable_instrument_id)
            universe.verify_content_identity()


class PromotedIdentitySessionUniverseWhiteBoxJoinTests(unittest.TestCase):
    """Some join failures required by the architecture (missing observation/
    candidate/resolution for a retained row) are structurally unreachable
    through the public API: VerifiedPromotedIdentityIntake and
    VerifiedPromotedIdentityAdjudication already guarantee, at their own
    construction time, that every retained row has exactly one observation,
    candidate, and resolution -- and adjudication.verify_content_identity()
    (which _build_session_universe_facts always calls first) would catch any
    post-construction tampering before this module's own join checks ever
    ran. These tests call the private _build_entries directly against a
    hand-mutated (object.__setattr__) fixture, bypassing that outer replay,
    to prove each check is still genuinely load-bearing defense-in-depth."""

    def test_missing_observation_for_retained_row_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _, p2, _, _, _, adjudication = build_intake_and_adjudication(root)
            filtered = tuple(
                value
                for value in adjudication.intake.observations
                if value.ticker_symbol != "RELIANCE"
            )
            object.__setattr__(adjudication.intake, "observations", filtered)
            with self.assertRaises(PromotedIdentitySessionUniverseError):
                _build_entries(adjudication, p2, D2)

    def test_missing_candidate_for_observation_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _, p2, _, _, _, adjudication = build_intake_and_adjudication(root)
            reliance_observation_ids = {
                value.observation_id
                for value in adjudication.intake.observations
                if value.ticker_symbol == "RELIANCE"
            }
            filtered = tuple(
                value
                for value in adjudication.intake.candidates
                if not (set(value.observation_ids) & reliance_observation_ids)
            )
            object.__setattr__(adjudication.intake, "candidates", filtered)
            with self.assertRaises(PromotedIdentitySessionUniverseError):
                _build_entries(adjudication, p2, D2)

    def test_missing_resolution_for_candidate_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _, p2, _, reliance_case, _, adjudication = build_intake_and_adjudication(root)
            filtered = tuple(
                value
                for value in adjudication.snapshot.resolutions
                if value.candidate_id != reliance_case.candidate_id
            )
            object.__setattr__(adjudication.snapshot, "resolutions", filtered)
            with self.assertRaises(PromotedIdentitySessionUniverseError):
                _build_entries(adjudication, p2, D2)

    def test_unresolved_candidate_with_unexpected_listing_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _, p2, _, _, _, adjudication = build_intake_and_adjudication(
                root, include_reliance_review=False
            )
            # No listing observations exist yet (nothing was adjudicated with
            # a stable ID). Fabricate one bound to SMALL1's observation while
            # its resolution still has no stable_instrument_id, to prove an
            # UNRESOLVED candidate is rejected if a listing exists anyway.
            small1_observation = next(
                value
                for value in adjudication.intake.observations
                if value.ticker_symbol == "SMALL1" and value.claimed_report_date == D2
            )
            from india_swing.identity_decisions.models import EffectiveStableListingObservation

            fabricated = EffectiveStableListingObservation(
                candidate_id="0" * 64,
                source_observation_id=small1_observation.observation_id,
                stable_instrument_id="1" * 64,
                stable_listing_id="2" * 64,
                effective_on=D2,
                symbol="SMALL1",
                series="EQ",
                isin="INE001A01036",
            )
            object.__setattr__(
                adjudication.snapshot, "listing_observations", (fabricated,)
            )
            with self.assertRaises(PromotedIdentitySessionUniverseError):
                _build_entries(adjudication, p2, D2)

    def test_resolved_candidate_with_mismatched_listing_isin_is_rejected(self) -> None:
        # Prove the per-field listing-agreement check (bullet 8: stable
        # instrument ID, stable listing ID, symbol, series, ISIN, candidate
        # ID, effective_on must all agree exactly) actually inspects ISIN,
        # not just symbol/series/candidate_id/effective_on. Replace the
        # genuine RELIANCE listing with an otherwise-identical copy whose
        # isin disagrees with the observation's validated_isin.
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _, p2, _, _, _, adjudication = build_intake_and_adjudication(root)
            genuine = next(
                value
                for value in adjudication.snapshot.listing_observations
                if value.symbol == "RELIANCE" and value.effective_on == D2
            )
            from india_swing.identity_decisions.models import EffectiveStableListingObservation

            tampered = EffectiveStableListingObservation(
                candidate_id=genuine.candidate_id,
                source_observation_id=genuine.source_observation_id,
                stable_instrument_id=genuine.stable_instrument_id,
                stable_listing_id=genuine.stable_listing_id,
                effective_on=genuine.effective_on,
                symbol=genuine.symbol,
                series=genuine.series,
                isin="INE001A01036",
            )
            others = tuple(
                value
                for value in adjudication.snapshot.listing_observations
                if value.record_id != genuine.record_id
            )
            object.__setattr__(
                adjudication.snapshot, "listing_observations", others + (tampered,)
            )
            with self.assertRaises(PromotedIdentitySessionUniverseError):
                _build_entries(adjudication, p2, D2)


class PromotedIdentitySessionUniverseDirectConstructionMismatchTests(unittest.TestCase):
    def _universe(self, tmp: str) -> VerifiedPromotedIdentitySessionUniverse:
        root = Path(tmp)
        _, _, _, _, universe = happy_path_fixture(root)
        return universe

    def test_replacing_schema_version_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            universe = self._universe(tmp)
            kwargs = _kwargs_from(universe)
            kwargs["schema_version"] = "promoted-identity-session-universe/v2"
            with self.assertRaises(PromotedIdentitySessionUniverseError):
                VerifiedPromotedIdentitySessionUniverse(**kwargs)

    def test_replacing_policy_version_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            universe = self._universe(tmp)
            kwargs = _kwargs_from(universe)
            kwargs["policy_version"] = "different-policy/v1"
            with self.assertRaises(PromotedIdentitySessionUniverseError):
                VerifiedPromotedIdentitySessionUniverse(**kwargs)

    def test_replacing_adjudication_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_a, tempfile.TemporaryDirectory() as tmp_b:
            universe = self._universe(tmp_a)
            root_b = Path(tmp_b)
            _, _, _, other_adjudication = build_alternate_intake_and_adjudication(root_b)
            kwargs = _kwargs_from(universe)
            kwargs["adjudication"] = other_adjudication
            with self.assertRaises(PromotedIdentitySessionUniverseError):
                VerifiedPromotedIdentitySessionUniverse(**kwargs)

    def test_replacing_calendar_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_a, tempfile.TemporaryDirectory() as tmp_b:
            universe = self._universe(tmp_a)
            other_calendar = build_calendar(Path(tmp_b), coverage_start=D1, coverage_end=D1)
            kwargs = _kwargs_from(universe)
            kwargs["calendar"] = other_calendar
            with self.assertRaises(PromotedIdentitySessionUniverseError):
                VerifiedPromotedIdentitySessionUniverse(**kwargs)

    def test_replacing_market_session_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            universe = self._universe(tmp)
            kwargs = _kwargs_from(universe)
            kwargs["market_session"] = D1
            with self.assertRaises(PromotedIdentitySessionUniverseError):
                VerifiedPromotedIdentitySessionUniverse(**kwargs)

    def test_replacing_cutoff_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            universe = self._universe(tmp)
            kwargs = _kwargs_from(universe)
            kwargs["cutoff"] = universe.cutoff + timedelta(days=1)
            with self.assertRaises(PromotedIdentitySessionUniverseError):
                VerifiedPromotedIdentitySessionUniverse(**kwargs)

    def test_replacing_entries_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            universe = self._universe(tmp)
            kwargs = _kwargs_from(universe)
            kwargs["entries"] = universe.entries[:-1]
            with self.assertRaises(PromotedIdentitySessionUniverseError):
                VerifiedPromotedIdentitySessionUniverse(**kwargs)

    def test_replacing_disposition_counts_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            universe = self._universe(tmp)
            kwargs = _kwargs_from(universe)
            kwargs["disposition_counts"] = ()
            with self.assertRaises(PromotedIdentitySessionUniverseError):
                VerifiedPromotedIdentitySessionUniverse(**kwargs)

    def test_replacing_reason_counts_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            universe = self._universe(tmp)
            kwargs = _kwargs_from(universe)
            kwargs["reason_counts"] = ()
            with self.assertRaises(PromotedIdentitySessionUniverseError):
                VerifiedPromotedIdentitySessionUniverse(**kwargs)

    def test_replacing_readiness_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            universe = self._universe(tmp)
            kwargs = _kwargs_from(universe)
            kwargs["readiness"] = ReferenceReadiness.POINT_IN_TIME_VERIFIED
            with self.assertRaises(PromotedIdentitySessionUniverseError):
                VerifiedPromotedIdentitySessionUniverse(**kwargs)

    def test_replacing_actionable_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            universe = self._universe(tmp)
            kwargs = _kwargs_from(universe)
            kwargs["actionable"] = True
            with self.assertRaises(PromotedIdentitySessionUniverseError):
                VerifiedPromotedIdentitySessionUniverse(**kwargs)

    def test_replacing_execution_eligible_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            universe = self._universe(tmp)
            kwargs = _kwargs_from(universe)
            kwargs["execution_eligible"] = True
            with self.assertRaises(PromotedIdentitySessionUniverseError):
                VerifiedPromotedIdentitySessionUniverse(**kwargs)

    def test_replacing_universe_id_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            universe = self._universe(tmp)
            kwargs = _kwargs_from(universe)
            kwargs["universe_id"] = hashlib.sha256(b"different").hexdigest()
            with self.assertRaises(PromotedIdentitySessionUniverseError):
                VerifiedPromotedIdentitySessionUniverse(**kwargs)

    def test_replacing_selected_promotion_id_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            universe = self._universe(tmp)
            kwargs = _kwargs_from(universe)
            kwargs["selected_promotion_id"] = "0" * 64
            with self.assertRaises(PromotedIdentitySessionUniverseError):
                VerifiedPromotedIdentitySessionUniverse(**kwargs)


class PromotedIdentitySessionUniverseMutationTests(unittest.TestCase):
    def _universe(self, tmp: str) -> VerifiedPromotedIdentitySessionUniverse:
        root = Path(tmp)
        _, _, _, _, universe = happy_path_fixture(root)
        return universe

    def test_mutating_top_level_universe_id_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            universe = self._universe(tmp)
            object.__setattr__(universe, "universe_id", "0" * 64)
            with self.assertRaises(PromotedIdentitySessionUniverseError):
                universe.verify_content_identity()

    def test_mutating_nested_adjudication_field_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            universe = self._universe(tmp)
            object.__setattr__(universe.adjudication, "adjudication_id", "0" * 64)
            with self.assertRaises(PromotedIdentitySessionUniverseError):
                universe.verify_content_identity()

    def test_mutating_nested_calendar_field_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            universe = self._universe(tmp)
            object.__setattr__(universe.calendar, "materialization_id", "0" * 64)
            with self.assertRaises(PromotedIdentitySessionUniverseError):
                universe.verify_content_identity()

    def test_mutating_nested_source_record_field_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            universe = self._universe(tmp)
            record = universe.adjudication.intake.promotions[0].artifact.parsed.records[0]
            object.__setattr__(record, "ticker_symbol", "TAMPERED")
            with self.assertRaises(PromotedIdentitySessionUniverseError):
                universe.verify_content_identity()

    def test_mutating_nested_calendar_day_field_fails_closed(self) -> None:
        from india_swing.reference.calendar import CalendarDayKind

        with tempfile.TemporaryDirectory() as tmp:
            universe = self._universe(tmp)
            day = universe.calendar.calendar_snapshot.days[0]
            new_kind = (
                CalendarDayKind.HOLIDAY
                if day.kind is not CalendarDayKind.HOLIDAY
                else CalendarDayKind.WEEKEND
            )
            object.__setattr__(day, "kind", new_kind)
            with self.assertRaises(PromotedIdentitySessionUniverseError):
                universe.verify_content_identity()

    def test_mutating_entry_field_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            universe = self._universe(tmp)
            entry = universe.entries[0]
            object.__setattr__(entry, "permitted_to_trade", 2 if entry.permitted_to_trade != 2 else 1)
            with self.assertRaises(PromotedIdentitySessionUniverseError):
                universe.verify_content_identity()

    def test_mutating_nested_resolution_blocker_field_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            universe = self._universe(tmp)
            resolution = next(
                value
                for value in universe.adjudication.snapshot.resolutions
                if value.stable_instrument_id is None
            )
            object.__setattr__(resolution, "blocker_codes", ())
            with self.assertRaises(PromotedIdentitySessionUniverseError):
                universe.verify_content_identity()

    def test_mutating_nested_listing_observation_field_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            universe = self._universe(tmp)
            listing = universe.adjudication.snapshot.listing_observations[0]
            object.__setattr__(listing, "symbol", "TAMPERED")
            with self.assertRaises(PromotedIdentitySessionUniverseError):
                universe.verify_content_identity()


class _EvilEq:
    def __init__(self, secret: str) -> None:
        self._secret = secret

    def __eq__(self, other: object) -> bool:
        raise RuntimeError(f"secret-leak-{self._secret}")

    def __hash__(self) -> int:
        return 0


class _ComparisonBoundaryBaseException(BaseException):
    pass


class _EvilEqBaseException:
    def __eq__(self, other: object) -> bool:
        raise _ComparisonBoundaryBaseException("comparison-boundary-control")

    def __hash__(self) -> int:
        return 0


class PromotedIdentitySessionUniverseComparisonBoundaryTests(unittest.TestCase):
    def _universe(self, tmp: str) -> VerifiedPromotedIdentitySessionUniverse:
        root = Path(tmp)
        _, _, _, _, universe = happy_path_fixture(root)
        return universe

    def _assert_sanitized(self, secret: str, exc: BaseException) -> None:
        self.assertIsInstance(exc, PromotedIdentitySessionUniverseError)
        message = str(exc)
        self.assertNotIn("RuntimeError", message)
        self.assertNotIn(secret, message)

    def test_malicious_entry_field_equality_is_sanitized(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            universe = self._universe(tmp)
            secret = "entry-secret-1a2b"
            entry = universe.entries[0]
            object.__setattr__(entry, "symbol", _EvilEq(secret))
            with self.assertRaises(PromotedIdentitySessionUniverseError) as ctx:
                universe.verify_content_identity()
            self._assert_sanitized(secret, ctx.exception)

    def test_malicious_resolution_equality_is_sanitized(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            universe = self._universe(tmp)
            secret = "resolution-secret-3c4d"
            resolution = universe.adjudication.snapshot.resolutions[0]
            object.__setattr__(resolution, "candidate_id", _EvilEq(secret))
            with self.assertRaises(PromotedIdentitySessionUniverseError) as ctx:
                universe.verify_content_identity()
            self._assert_sanitized(secret, ctx.exception)

    def test_base_exception_from_equality_is_not_swallowed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            universe = self._universe(tmp)
            entry = universe.entries[0]
            object.__setattr__(entry, "symbol", _EvilEqBaseException())
            with self.assertRaises(_ComparisonBoundaryBaseException):
                universe.verify_content_identity()


class PromotedIdentitySessionUniverseSubclassImpostorTests(unittest.TestCase):
    def test_universe_subclass_is_rejected(self) -> None:
        class _UniverseSubclass(VerifiedPromotedIdentitySessionUniverse):
            pass

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _, _, _, _, universe = happy_path_fixture(root)
            with self.assertRaises(PromotedIdentitySessionUniverseError):
                _UniverseSubclass(**_kwargs_from(universe))

    def test_entry_subclass_is_rejected_by_universe_type_check(self) -> None:
        class _EntrySubclass(PromotedIdentitySessionEntry):
            pass

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _, _, _, _, universe = happy_path_fixture(root)
            entry_kwargs = {
                field.name: getattr(universe.entries[0], field.name)
                for field in dataclasses.fields(universe.entries[0])
            }
            impostor = _EntrySubclass(**entry_kwargs)
            kwargs = _kwargs_from(universe)
            kwargs["entries"] = (impostor,) + universe.entries[1:]
            with self.assertRaises(PromotedIdentitySessionUniverseError):
                VerifiedPromotedIdentitySessionUniverse(**kwargs)

    def test_wrong_type_entries_tuple_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _, _, _, _, universe = happy_path_fixture(root)
            kwargs = _kwargs_from(universe)
            kwargs["entries"] = list(universe.entries)  # type: ignore[assignment]
            with self.assertRaises(PromotedIdentitySessionUniverseError):
                VerifiedPromotedIdentitySessionUniverse(**kwargs)


class PromotedIdentitySessionUniverseContentIdCompletenessTests(unittest.TestCase):
    def test_identity_payload_explicitly_binds_intake_graph_and_queue(self) -> None:
        captured: list[dict[str, object]] = []
        real_content_id = promoted_identity_module.content_id

        def recording_content_id(value: object, *args: object, **kwargs: object) -> str:
            if (
                type(value) is dict
                and value.get("schema_version")
                == promoted_identity_module.PROMOTED_IDENTITY_SESSION_UNIVERSE_SCHEMA_VERSION
            ):
                captured.append(dict(value))
            return real_content_id(value, *args, **kwargs)

        promoted_identity_module.content_id = recording_content_id
        try:
            with tempfile.TemporaryDirectory() as tmp:
                _, _, adjudication, _, universe = happy_path_fixture(Path(tmp))
                self.assertTrue(captured)
                payload = captured[-1]
                self.assertEqual(payload["intake_id"], adjudication.intake.intake_id)
                self.assertEqual(
                    payload["source_graph_id"], adjudication.intake.source_graph_id
                )
                self.assertEqual(
                    payload["queue_id"], adjudication.intake.queue.queue_id
                )
        finally:
            promoted_identity_module.content_id = real_content_id

        with tempfile.TemporaryDirectory() as tmp:
            _, _, _, _, verification_universe = happy_path_fixture(Path(tmp))
            verification_universe.verify_content_identity()

    def test_different_market_session_changes_universe_id(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_a, tempfile.TemporaryDirectory() as tmp_b:
            root_a, root_b = Path(tmp_a), Path(tmp_b)
            _, _, adjudication_a, calendar_a, universe_a = happy_path_fixture(root_a)
            _, _, adjudication_b, calendar_b, _ = happy_path_fixture(root_b)
            universe_b = PromotedIdentitySessionUniverseService().materialize(
                adjudication=adjudication_b,
                calendar=calendar_b,
                market_session=D1,
                cutoff=SESSION_CUTOFF,
            )
            self.assertNotEqual(universe_a.universe_id, universe_b.universe_id)

    def test_different_cutoff_changes_universe_id(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_a, tempfile.TemporaryDirectory() as tmp_b:
            root_a, root_b = Path(tmp_a), Path(tmp_b)
            _, _, adjudication_a, calendar_a, universe_a = happy_path_fixture(root_a)
            _, _, adjudication_b, calendar_b, _ = happy_path_fixture(root_b)
            universe_b = PromotedIdentitySessionUniverseService().materialize(
                adjudication=adjudication_b,
                calendar=calendar_b,
                market_session=D2,
                cutoff=SESSION_CUTOFF + timedelta(hours=1),
            )
            self.assertNotEqual(universe_a.universe_id, universe_b.universe_id)

    def test_different_review_outcome_changes_universe_id(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_a, tempfile.TemporaryDirectory() as tmp_b:
            root_a, root_b = Path(tmp_a), Path(tmp_b)
            _, _, _, _, universe_a = happy_path_fixture(root_a)
            _, _, _, calendar_b, _ = happy_path_fixture(root_b)
            _, _, intake_b, _, _, adjudication_b_no_review = build_intake_and_adjudication(
                root_b, include_reliance_review=False
            )
            universe_b = PromotedIdentitySessionUniverseService().materialize(
                adjudication=adjudication_b_no_review,
                calendar=calendar_b,
                market_session=D2,
                cutoff=SESSION_CUTOFF,
            )
            self.assertNotEqual(universe_a.universe_id, universe_b.universe_id)

    def test_filesystem_path_alone_does_not_change_universe_id(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_a, tempfile.TemporaryDirectory() as tmp_b:
            root_a, root_b = Path(tmp_a), Path(tmp_b)
            _, _, _, _, universe_a = happy_path_fixture(root_a)
            _, _, _, _, universe_b = happy_path_fixture(root_b)
            self.assertNotEqual(
                universe_a.adjudication.evidence_artifacts[0].path,
                universe_b.adjudication.evidence_artifacts[0].path,
            )
            self.assertEqual(universe_a.universe_id, universe_b.universe_id)


class PromotedIdentitySessionUniverseCapabilityTests(unittest.TestCase):
    def test_no_actionable_or_watch_only_disposition_exists(self) -> None:
        member_names = {value.name for value in PromotedIdentitySessionDisposition}
        self.assertNotIn("ACTIONABLE", member_names)
        self.assertNotIn("WATCH_ONLY", member_names)

    def test_no_io_shaped_capability_exists(self) -> None:
        # "selected_*" fields (selected_promotion_id, selected_join_id, ...)
        # are legitimate retained-evidence identifiers the architecture
        # requires, not a select/find/list-style I/O capability -- so "select"
        # is checked as a verb-shaped prefix rather than a bare substring to
        # avoid flagging that intentional naming.
        banned_substrings = (
            "list",
            "latest",
            "find",
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
            "notif",
            "broker",
            "order",
            "capital",
        )
        for candidate in (
            PromotedIdentitySessionUniverseService,
            VerifiedPromotedIdentitySessionUniverse,
        ):
            members = [name for name in dir(candidate) if not name.startswith("__")]
            for name in members:
                lowered = name.lower()
                self.assertFalse(
                    any(bad in lowered for bad in banned_substrings),
                    f"{candidate!r} unexpectedly exposes {name!r}",
                )
                self.assertFalse(
                    lowered.startswith("select_") or lowered == "select",
                    f"{candidate!r} unexpectedly exposes {name!r}",
                )

    def test_no_forbidden_capability_field_exists(self) -> None:
        field_names = {
            field.name for field in dataclasses.fields(VerifiedPromotedIdentitySessionUniverse)
        }
        for banned in (
            "price",
            "liquidity",
            "corporate_action",
            "model",
            "signal",
            "ranking",
            "recommendation",
            "notification",
            "broker",
            "order",
            "position_size",
            "capital",
            "market_cap",
        ):
            self.assertFalse(any(banned in name for name in field_names))

    def test_universe_cannot_be_used_as_reference_universe_snapshot(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _, _, _, _, universe = happy_path_fixture(root)
            self.assertNotIsInstance(universe, UniverseSnapshot)
            self.assertFalse(
                issubclass(VerifiedPromotedIdentitySessionUniverse, UniverseSnapshot)
            )

    def test_entry_cannot_be_used_as_reference_universe_entry(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _, _, _, _, universe = happy_path_fixture(root)
            self.assertNotIsInstance(universe.entries[0], UniverseEntry)
            self.assertFalse(issubclass(PromotedIdentitySessionEntry, UniverseEntry))

    def test_universe_cannot_be_used_as_promotion_evidence_or_decision(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _, _, _, _, universe = happy_path_fixture(root)
            self.assertNotIsInstance(universe, PromotionEvidence)
            self.assertNotIsInstance(universe, PromotionDecision)
            self.assertFalse(
                issubclass(VerifiedPromotedIdentitySessionUniverse, PromotionEvidence)
            )

    def test_importing_module_causes_no_io(self) -> None:
        import india_swing.universe.promoted_identity as module

        banned_module_names = {"os", "socket", "urllib", "requests", "storage"}
        top_level_names = {
            name
            for name in vars(module)
            if not name.startswith("_") and not name[0].isupper()
        }
        self.assertFalse(top_level_names & banned_module_names)


class PromotedIdentitySessionUniverseLegacyRegressionTests(unittest.TestCase):
    def test_legacy_universe_module_untouched(self) -> None:
        from india_swing.universe.materialize import materialize_collection_universe

        self.assertTrue(callable(materialize_collection_universe))


if __name__ == "__main__":
    unittest.main()
