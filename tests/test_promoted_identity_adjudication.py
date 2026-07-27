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
from india_swing.identity_decisions import (
    IDENTITY_REVIEW_DECLARATION_SCHEMA_VERSION,
    LocalIdentityReviewBundleStore,
    PromotedIdentityAdjudicationError,
    PromotedIdentityAdjudicationService,
    StoredIdentityReviewBundle,
    VerifiedPromotedIdentityAdjudication,
)
from india_swing.identity_decisions.materialize import (
    _materialize_adjudicated_identity_snapshot_core,
)
from india_swing.identity_evidence import (
    IDENTITY_EVIDENCE_DECLARATION_SCHEMA_VERSION,
    LocalIdentityEvidenceArtifactStore,
    StoredIdentityEvidenceArtifact,
)
from india_swing.identity_registry.promoted_intake import (
    PromotedIdentityIntakeService,
    VerifiedPromotedIdentityIntake,
)
from india_swing.reference.models import ReferenceReadiness
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


UTC = timezone.utc
_BUCKET = "trusted-bucket"
_ACQUIRER_ID = "a" * 64
PDF_BYTES = b"%PDF-1.7\n1 0 obj\n<< /Type /Catalog >>\nendobj\n%%EOF\n"
_D1 = date(2026, 7, 15)
_D2 = date(2026, 7, 16)
_INTAKE_CUTOFF = datetime(2026, 7, 16, 14, 0, tzinfo=UTC)
_EVIDENCE_FIRST = datetime(2026, 7, 16, 15, 0, tzinfo=UTC)
_EVIDENCE_VALIDATED = _EVIDENCE_FIRST + timedelta(seconds=2)
_REVIEWED_AT = datetime(2026, 7, 16, 15, 5, tzinfo=UTC)
_REVIEW_FIRST = _REVIEWED_AT + timedelta(minutes=1)
_REVIEW_VALIDATED = _REVIEW_FIRST + timedelta(seconds=2)
_ADJUDICATION_CUTOFF = datetime(2026, 7, 16, 16, 0, tzinfo=UTC)


def _filename(report_date: date) -> str:
    return f"NSE_CM_security_{report_date.strftime('%d%m%Y')}.csv.gz"


def _object_name(report_date: date) -> str:
    return f"landing/{report_date.isoformat()}/{_filename(report_date)}"


def _requested_url(report_date: date) -> str:
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


def _csv_bytes(rows: list[dict[str, str]]) -> bytes:
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(NSE_CM_MII_SECURITY_HEADER)
    for row in rows:
        writer.writerow([row[name] for name in NSE_CM_MII_SECURITY_HEADER])
    return buf.getvalue().encode("utf-8")


def _security_master_gzip(*, rows: list[dict[str, str]]) -> bytes:
    return gzip.compress(_csv_bytes(rows), mtime=0)


class FakeGCSObjectReader:
    def __init__(self, *, generation: int, content_bytes: bytes) -> None:
        self.generation = generation
        self.content_bytes = content_bytes

    def read_generation(
        self, *, bucket: str, object_name: str, generation: int, maximum_bytes: int
    ) -> GCSObjectPayload:
        return GCSObjectPayload(content_bytes=self.content_bytes, generation=self.generation)


def _build_promotion(
    root: Path,
    *,
    report_date: date,
    generation: int,
    rows: list[dict[str, str]],
    first_seen: datetime,
    validated: datetime,
) -> VerifiedReferenceArtifactPromotion:
    gz_bytes = _security_master_gzip(rows=rows)
    filename = _filename(report_date)
    raw_sha256 = hashlib.sha256(gz_bytes).hexdigest()
    receipt_dict = {
        "schema_version": 1,
        "dataset": "nse-cm-mii-security",
        "authority": "NSE",
        "acquirer_id": _ACQUIRER_ID,
        "acquired_at": f"{report_date.isoformat()}T13:30:00Z",
        "report_date": report_date.isoformat(),
        "requested_url": _requested_url(report_date),
        "response_status": 200,
        "response_media_type": "application/gzip",
        "raw_byte_count": len(gz_bytes),
        "raw_sha256": raw_sha256,
        "landing_object": {
            "file_type": "SECURITY_MASTER",
            "bucket": _BUCKET,
            "object_name": _object_name(report_date),
            "generation": generation,
            "sha256": raw_sha256,
        },
    }
    receipt_bytes = json.dumps(receipt_dict, separators=(",", ":")).encode("utf-8")
    not_before = datetime.combine(report_date, datetime.min.time(), tzinfo=UTC)
    cutoff_bound = datetime.combine(
        report_date, datetime.max.time(), tzinfo=UTC
    ).replace(microsecond=0)
    binding = TrustedReferenceAcquisitionBinding(
        expected_receipt_sha256=hashlib.sha256(receipt_bytes).hexdigest(),
        expected_raw_sha256=raw_sha256,
        allowed_bucket=_BUCKET,
        target_report_date=report_date,
        not_before=not_before,
        cutoff=cutoff_bound,
        trusted_acquirer_id=_ACQUIRER_ID,
    )
    receipt = ReferenceAcquisitionReceiptVerifier().verify(receipt_bytes, binding)
    fake = FakeGCSObjectReader(generation=generation, content_bytes=gz_bytes)
    reader = GCSLandingObjectReader(fake)
    join = ReferenceAcquisitionJoinService(reader).join(receipt)

    source_dir = root / f"source-{report_date.isoformat()}"
    source_dir.mkdir(parents=True, exist_ok=True)
    source_file = source_dir / filename
    source_file.write_bytes(gz_bytes)
    calls = iter((first_seen, validated))
    store = LocalReferenceArtifactStore(
        root / f"archive-{report_date.isoformat()}", clock=lambda: next(calls)
    )
    artifact = store.import_security_master(source_file)
    return ReferenceArtifactPromotionService().promote(join, artifact)


def _build_intake(root: Path) -> VerifiedPromotedIdentityIntake:
    p1 = _build_promotion(
        root,
        report_date=_D1,
        generation=100,
        rows=[_security_master_row()],
        first_seen=datetime(2026, 7, 15, 12, 0, tzinfo=UTC),
        validated=datetime(2026, 7, 15, 12, 0, 2, tzinfo=UTC),
    )
    p2 = _build_promotion(
        root,
        report_date=_D2,
        generation=200,
        rows=[_security_master_row(TckrSymb="RELNEW", FinInstrmId="99999")],
        first_seen=datetime(2026, 7, 16, 12, 0, tzinfo=UTC),
        validated=datetime(2026, 7, 16, 12, 0, 2, tzinfo=UTC),
    )
    return PromotedIdentityIntakeService().materialize(
        promotions=(p1, p2),
        expected_report_dates=(_D1, _D2),
        cutoff=_INTAKE_CUTOFF,
    )


def _build_alternate_intake(root: Path) -> VerifiedPromotedIdentityIntake:
    """A genuinely different intake (different rows/tickers/generations from
    _build_intake), used to prove substitution of an otherwise-valid-but-
    different intake/snapshot is detected rather than silently accepted
    because two independently built copies of the same deterministic
    fixture happen to be content-identical."""

    p1 = _build_promotion(
        root,
        report_date=_D1,
        generation=500,
        rows=[_security_master_row(FinInstrmId="70001", TckrSymb="ALTONE")],
        first_seen=datetime(2026, 7, 15, 12, 0, tzinfo=UTC),
        validated=datetime(2026, 7, 15, 12, 0, 2, tzinfo=UTC),
    )
    p2 = _build_promotion(
        root,
        report_date=_D2,
        generation=600,
        rows=[_security_master_row(FinInstrmId="70001", TckrSymb="ALTONENEW")],
        first_seen=datetime(2026, 7, 16, 12, 0, tzinfo=UTC),
        validated=datetime(2026, 7, 16, 12, 0, 2, tzinfo=UTC),
    )
    return PromotedIdentityIntakeService().materialize(
        promotions=(p1, p2),
        expected_report_dates=(_D1, _D2),
        cutoff=_INTAKE_CUTOFF,
    )


def _build_evidence(
    root: Path,
    *,
    candidate_id: str,
    requirements: tuple,
    symbol: str = "RELNEW",
    series: str = "EQ",
    isin: str | None = "INE002A01018",
    first_seen: datetime = _EVIDENCE_FIRST,
    validated: datetime = _EVIDENCE_VALIDATED,
    filename_suffix: str = "",
    listing_overrides: dict | None = None,
) -> StoredIdentityEvidenceArtifact:
    source = root / f"CML-IDENTITY{filename_suffix}.pdf"
    declaration = root / f"CML-IDENTITY{filename_suffix}.evidence.json"
    source.write_bytes(PDF_BYTES)
    claims = []
    for requirement in requirements:
        req_symbol, req_series = (listing_overrides or {}).get(
            requirement.value, (symbol, series)
        )
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
                "symbol": req_symbol,
                "series": req_series,
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
        "claimed_document_id": f"NSE/LIST/C/2026/TEST{(filename_suffix or '0').upper()}",
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


def _build_review(
    root: Path,
    *,
    queue_id: str,
    source_registry_id: str,
    candidate_id: str,
    requirements: tuple,
    evidence: StoredIdentityEvidenceArtifact,
    outcomes: dict | None = None,
    reviewer_id: str = "owner:kamal",
    reviewed_at: datetime = _REVIEWED_AT,
    first_seen: datetime = _REVIEW_FIRST,
    validated: datetime = _REVIEW_VALIDATED,
    claim_override: dict | None = None,
    filename_suffix: str = "",
) -> StoredIdentityReviewBundle:
    claims_by_requirement = {value.requirement.value: value for value in evidence.parsed.claims}
    decisions = []
    for requirement in requirements:
        evidence_claim = claims_by_requirement[requirement.value]
        decisions.append(
            {
                "candidate_id": candidate_id,
                "requirement": requirement.value,
                "outcome": (outcomes or {}).get(requirement.value, "ACCEPTED"),
                "evidence_artifact_id": evidence.manifest.artifact_id,
                "evidence_claim_id": (claim_override or {}).get(
                    requirement.value, evidence_claim.claim_id
                ),
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
    path = root / f"review-{reviewer_id.replace(':', '-')}{filename_suffix}.json"
    path.write_text(json.dumps(value), encoding="utf-8")
    calls = iter((first_seen, validated))
    return LocalIdentityReviewBundleStore(
        root / "evidence", clock=lambda: next(calls)
    ).import_declaration(path)


def _happy_path_fixture(root: Path, *, intake_builder=_build_intake):
    is_alternate = intake_builder is _build_alternate_intake
    intake = intake_builder(root)
    case = intake.queue.cases[0]
    status = next(
        value for value in intake.requirement_statuses if value.candidate_id == case.candidate_id
    )
    evidence = _build_evidence(
        root,
        candidate_id=case.candidate_id,
        requirements=status.unresolved_requirements,
        symbol="ALTONENEW" if is_alternate else "RELNEW",
        isin="INE002A01018",
    )
    review = _build_review(
        root,
        queue_id=intake.queue.queue_id,
        source_registry_id=intake.source_graph_id,
        candidate_id=case.candidate_id,
        requirements=status.unresolved_requirements,
        evidence=evidence,
    )
    return intake, case, status, evidence, review


def _alternate_happy_path_fixture(root: Path):
    return _happy_path_fixture(root, intake_builder=_build_alternate_intake)


def _kwargs_from(adjudication: VerifiedPromotedIdentityAdjudication) -> dict[str, object]:
    return {field.name: getattr(adjudication, field.name) for field in dataclasses.fields(adjudication)}


class PromotedIdentityAdjudicationAcceptanceTests(unittest.TestCase):
    def test_happy_path_discharges_pre_satisfied_and_assigns_stable_ids(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            intake, case, status, evidence, review = _happy_path_fixture(root)

            adjudication = PromotedIdentityAdjudicationService().materialize(
                intake=intake,
                evidence_artifacts=(evidence,),
                review_bundles=(review,),
                cutoff=_ADJUDICATION_CUTOFF,
            )

            resolution = next(
                value
                for value in adjudication.snapshot.resolutions
                if value.candidate_id == case.candidate_id
            )
            self.assertEqual(resolution.blocker_codes, ())
            self.assertIsNotNone(resolution.stable_instrument_id)
            self.assertTrue(
                set(status.satisfied_requirements).isdisjoint(
                    set(resolution.missing_requirements)
                )
            )
            self.assertTrue(adjudication.stable_identity_assigned)
            self.assertEqual(adjudication.readiness, ReferenceReadiness.COLLECTION_ONLY)
            self.assertFalse(adjudication.actionable)
            adjudication.verify_content_identity()

    def test_pre_satisfied_requirements_need_no_evidence_or_review(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            intake, case, status, evidence, review = _happy_path_fixture(root)
            adjudication = PromotedIdentityAdjudicationService().materialize(
                intake=intake,
                evidence_artifacts=(evidence,),
                review_bundles=(review,),
                cutoff=_ADJUDICATION_CUTOFF,
            )
            resolution = adjudication.snapshot.resolutions[0]
            decision_requirements = set()
            for decision_id in resolution.accepted_decision_ids + resolution.rejected_decision_ids:
                for bundle in adjudication.review_bundles:
                    for decision in bundle.parsed.decisions:
                        if decision.decision_id == decision_id:
                            decision_requirements.add(decision.requirement)
            self.assertTrue(decision_requirements.isdisjoint(status.satisfied_requirements))


class PromotedIdentityAdjudicationPreSatisfiedRejectionTests(unittest.TestCase):
    def test_decision_targeting_pre_satisfied_requirement_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            intake = _build_intake(root)
            case = intake.queue.cases[0]
            status = next(
                value
                for value in intake.requirement_statuses
                if value.candidate_id == case.candidate_id
            )
            all_requirements = tuple(case.requirements)
            evidence = _build_evidence(
                root, candidate_id=case.candidate_id, requirements=all_requirements
            )
            review = _build_review(
                root,
                queue_id=intake.queue.queue_id,
                source_registry_id=intake.source_graph_id,
                candidate_id=case.candidate_id,
                requirements=all_requirements,
                evidence=evidence,
            )
            with self.assertRaises(PromotedIdentityAdjudicationError):
                PromotedIdentityAdjudicationService().materialize(
                    intake=intake,
                    evidence_artifacts=(evidence,),
                    review_bundles=(review,),
                    cutoff=_ADJUDICATION_CUTOFF,
                )


class PromotedIdentityAdjudicationEmptyInputsTests(unittest.TestCase):
    def test_empty_evidence_and_review_report_only_unresolved_as_missing(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            intake = _build_intake(root)
            case = intake.queue.cases[0]
            status = next(
                value
                for value in intake.requirement_statuses
                if value.candidate_id == case.candidate_id
            )
            adjudication = PromotedIdentityAdjudicationService().materialize(
                intake=intake,
                evidence_artifacts=(),
                review_bundles=(),
                cutoff=_ADJUDICATION_CUTOFF,
            )
            resolution = adjudication.snapshot.resolutions[0]
            self.assertFalse(adjudication.stable_identity_assigned)
            self.assertEqual(set(resolution.missing_requirements), set(status.unresolved_requirements))
            self.assertTrue(set(status.satisfied_requirements).isdisjoint(resolution.missing_requirements))


class PromotedIdentityAdjudicationRejectionTests(unittest.TestCase):
    def test_rejected_decision_blocks_assignment(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            intake, case, status, evidence, _ = _happy_path_fixture(root)
            rejected = _build_review(
                root,
                queue_id=intake.queue.queue_id,
                source_registry_id=intake.source_graph_id,
                candidate_id=case.candidate_id,
                requirements=status.unresolved_requirements,
                evidence=evidence,
                outcomes={status.unresolved_requirements[0].value: "REJECTED"},
                reviewer_id="owner:reject",
            )
            adjudication = PromotedIdentityAdjudicationService().materialize(
                intake=intake,
                evidence_artifacts=(evidence,),
                review_bundles=(rejected,),
                cutoff=_ADJUDICATION_CUTOFF,
            )
            self.assertFalse(adjudication.stable_identity_assigned)

    def test_missing_decision_blocks_assignment(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            intake, case, status, evidence, _ = _happy_path_fixture(root)
            partial_requirements = status.unresolved_requirements[:-1]
            if not partial_requirements:
                self.skipTest("only one unresolved requirement in this fixture")
            partial_review = _build_review(
                root,
                queue_id=intake.queue.queue_id,
                source_registry_id=intake.source_graph_id,
                candidate_id=case.candidate_id,
                requirements=partial_requirements,
                evidence=evidence,
                reviewer_id="owner:partial",
            )
            adjudication = PromotedIdentityAdjudicationService().materialize(
                intake=intake,
                evidence_artifacts=(evidence,),
                review_bundles=(partial_review,),
                cutoff=_ADJUDICATION_CUTOFF,
            )
            self.assertFalse(adjudication.stable_identity_assigned)

    def test_duplicate_decision_pair_across_bundles_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            intake, case, status, evidence, review = _happy_path_fixture(root)
            second = _build_review(
                root,
                queue_id=intake.queue.queue_id,
                source_registry_id=intake.source_graph_id,
                candidate_id=case.candidate_id,
                requirements=status.unresolved_requirements,
                evidence=evidence,
                reviewer_id="owner:second",
            )
            with self.assertRaises(PromotedIdentityAdjudicationError):
                PromotedIdentityAdjudicationService().materialize(
                    intake=intake,
                    evidence_artifacts=(evidence,),
                    review_bundles=(review, second),
                    cutoff=_ADJUDICATION_CUTOFF,
                )

    def test_unselected_evidence_reference_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            intake, case, status, evidence, review = _happy_path_fixture(root)
            other_evidence = _build_evidence(
                root,
                candidate_id=case.candidate_id,
                requirements=status.unresolved_requirements,
                filename_suffix="-other",
                first_seen=_EVIDENCE_FIRST + timedelta(hours=1),
                validated=_EVIDENCE_VALIDATED + timedelta(hours=1),
            )
            with self.assertRaises(PromotedIdentityAdjudicationError):
                PromotedIdentityAdjudicationService().materialize(
                    intake=intake,
                    evidence_artifacts=(other_evidence,),
                    review_bundles=(review,),
                    cutoff=_ADJUDICATION_CUTOFF,
                )

    def test_mismatched_candidate_subject_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            intake = _build_intake(root)
            case = intake.queue.cases[0]
            status = next(
                value
                for value in intake.requirement_statuses
                if value.candidate_id == case.candidate_id
            )
            requirements = status.unresolved_requirements
            if len(requirements) < 2:
                self.skipTest("need at least two unresolved requirements")
            evidence = _build_evidence(root, candidate_id=case.candidate_id, requirements=requirements)
            claims_by_requirement = {value.requirement: value.claim_id for value in evidence.parsed.claims}
            mismatched = _build_review(
                root,
                queue_id=intake.queue.queue_id,
                source_registry_id=intake.source_graph_id,
                candidate_id=case.candidate_id,
                requirements=requirements,
                evidence=evidence,
                claim_override={
                    requirements[0].value: claims_by_requirement[requirements[1]]
                },
                reviewer_id="owner:mismatch",
            )
            with self.assertRaises(PromotedIdentityAdjudicationError):
                PromotedIdentityAdjudicationService().materialize(
                    intake=intake,
                    evidence_artifacts=(evidence,),
                    review_bundles=(mismatched,),
                    cutoff=_ADJUDICATION_CUTOFF,
                )

    def test_conflicting_accepted_isin_claims_fail(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            intake = _build_intake(root)
            case = intake.queue.cases[0]
            status = next(
                value
                for value in intake.requirement_statuses
                if value.candidate_id == case.candidate_id
            )
            requirements = status.unresolved_requirements
            source = root / "CML-CONFLICT.pdf"
            declaration = root / "CML-CONFLICT.evidence.json"
            source.write_bytes(PDF_BYTES)
            claims = []
            for index, requirement in enumerate(requirements):
                claims.append(
                    {
                        "candidate_id": case.candidate_id,
                        "requirement": requirement.value,
                        "effective_date": (
                            "2026-07-16"
                            if requirement.value
                            in {"OFFICIAL_LISTING_LIFECYCLE", "OFFICIAL_LISTING_STATUS"}
                            else None
                        ),
                        "symbol": "RELNEW",
                        "series": "EQ",
                        "isin": "INE002A01018" if index == 0 else "INE001A01036",
                        "locator": {"page": 1, "row": None, "section": requirement.value},
                        "claim_text": f"Conflicting claim for {requirement.value}.",
                    }
                )
            value = {
                "schema_version": IDENTITY_EVIDENCE_DECLARATION_SCHEMA_VERSION,
                "exchange": "NSE",
                "segment": "CM",
                "claimed_authority": "NSE",
                "source_kind": "LISTING_CIRCULAR_PDF",
                "claimed_document_id": "NSE/LIST/C/2026/CONFLICT",
                "claimed_issue_date": "2026-07-16",
                "claimed_publication_at": None,
                "claimed_source_url": "https://nsearchives.nseindia.com/content/circulars/CML-CONFLICT.pdf",
                "source_filename": source.name,
                "source_media_type": "application/pdf",
                "source_byte_count": len(PDF_BYTES),
                "source_sha256": hashlib.sha256(PDF_BYTES).hexdigest(),
                "claims": claims,
            }
            declaration.write_text(json.dumps(value), encoding="utf-8")
            calls = iter((_EVIDENCE_FIRST, _EVIDENCE_VALIDATED))
            evidence = LocalIdentityEvidenceArtifactStore(
                root / "evidence", clock=lambda: next(calls)
            ).import_source(source, declaration)
            review = _build_review(
                root,
                queue_id=intake.queue.queue_id,
                source_registry_id=intake.source_graph_id,
                candidate_id=case.candidate_id,
                requirements=requirements,
                evidence=evidence,
            )
            # The underlying IdentityDecisionIntegrityError("... conflicting
            # ISIN ...") raised by the shared core is deliberately sanitized
            # into one static PromotedIdentityAdjudicationError by this
            # module's own broad-exception boundary, so only the sanitized
            # type is asserted here.
            with self.assertRaises(PromotedIdentityAdjudicationError):
                PromotedIdentityAdjudicationService().materialize(
                    intake=intake,
                    evidence_artifacts=(evidence,),
                    review_bundles=(review,),
                    cutoff=_ADJUDICATION_CUTOFF,
                )

    def test_review_predating_evidence_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            intake = _build_intake(root)
            case = intake.queue.cases[0]
            status = next(
                value
                for value in intake.requirement_statuses
                if value.candidate_id == case.candidate_id
            )
            evidence = _build_evidence(
                root, candidate_id=case.candidate_id, requirements=status.unresolved_requirements
            )
            early_review = _build_review(
                root,
                queue_id=intake.queue.queue_id,
                source_registry_id=intake.source_graph_id,
                candidate_id=case.candidate_id,
                requirements=status.unresolved_requirements,
                evidence=evidence,
                reviewed_at=_EVIDENCE_FIRST - timedelta(minutes=1),
                first_seen=_EVIDENCE_FIRST - timedelta(seconds=30),
                validated=_EVIDENCE_FIRST - timedelta(seconds=28),
                reviewer_id="owner:early",
            )
            with self.assertRaises(PromotedIdentityAdjudicationError):
                PromotedIdentityAdjudicationService().materialize(
                    intake=intake,
                    evidence_artifacts=(evidence,),
                    review_bundles=(early_review,),
                    cutoff=_ADJUDICATION_CUTOFF,
                )

    def test_evidence_after_cutoff_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            intake, case, status, evidence, review = _happy_path_fixture(root)
            with self.assertRaises(PromotedIdentityAdjudicationError):
                PromotedIdentityAdjudicationService().materialize(
                    intake=intake,
                    evidence_artifacts=(evidence,),
                    review_bundles=(review,),
                    cutoff=_EVIDENCE_FIRST - timedelta(days=1),
                )

    def test_review_after_cutoff_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            intake, case, status, evidence, review = _happy_path_fixture(root)
            with self.assertRaises(PromotedIdentityAdjudicationError):
                PromotedIdentityAdjudicationService().materialize(
                    intake=intake,
                    evidence_artifacts=(evidence,),
                    review_bundles=(review,),
                    cutoff=_REVIEWED_AT,
                )

    def test_cutoff_before_intake_knowledge_time_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            intake, case, status, evidence, review = _happy_path_fixture(root)
            with self.assertRaises(PromotedIdentityAdjudicationError):
                PromotedIdentityAdjudicationService().materialize(
                    intake=intake,
                    evidence_artifacts=(),
                    review_bundles=(),
                    cutoff=intake.knowledge_time - timedelta(days=1),
                )


class PromotedIdentityAdjudicationSubstitutionTests(unittest.TestCase):
    def test_review_targeting_another_queue_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            intake, case, status, evidence, _ = _happy_path_fixture(root)
            wrong_queue_review = _build_review(
                root,
                queue_id="0" * 64,
                source_registry_id=intake.source_graph_id,
                candidate_id=case.candidate_id,
                requirements=status.unresolved_requirements,
                evidence=evidence,
                reviewer_id="owner:wrongqueue",
            )
            with self.assertRaises(PromotedIdentityAdjudicationError):
                PromotedIdentityAdjudicationService().materialize(
                    intake=intake,
                    evidence_artifacts=(evidence,),
                    review_bundles=(wrong_queue_review,),
                    cutoff=_ADJUDICATION_CUTOFF,
                )

    def test_review_targeting_another_source_graph_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            intake, case, status, evidence, _ = _happy_path_fixture(root)
            wrong_source_review = _build_review(
                root,
                queue_id=intake.queue.queue_id,
                source_registry_id="0" * 64,
                candidate_id=case.candidate_id,
                requirements=status.unresolved_requirements,
                evidence=evidence,
                reviewer_id="owner:wrongsource",
            )
            with self.assertRaises(PromotedIdentityAdjudicationError):
                PromotedIdentityAdjudicationService().materialize(
                    intake=intake,
                    evidence_artifacts=(evidence,),
                    review_bundles=(wrong_source_review,),
                    cutoff=_ADJUDICATION_CUTOFF,
                )

    def test_duplicate_evidence_artifact_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            intake, case, status, evidence, review = _happy_path_fixture(root)
            with self.assertRaises(PromotedIdentityAdjudicationError):
                PromotedIdentityAdjudicationService().materialize(
                    intake=intake,
                    evidence_artifacts=(evidence, evidence),
                    review_bundles=(review,),
                    cutoff=_ADJUDICATION_CUTOFF,
                )

    def test_duplicate_review_bundle_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            intake, case, status, evidence, review = _happy_path_fixture(root)
            with self.assertRaises(PromotedIdentityAdjudicationError):
                PromotedIdentityAdjudicationService().materialize(
                    intake=intake,
                    evidence_artifacts=(evidence,),
                    review_bundles=(review, review),
                    cutoff=_ADJUDICATION_CUTOFF,
                )

    def test_intake_subclass_is_rejected_before_it_can_reach_the_adjudication(self) -> None:
        # VerifiedPromotedIdentityIntake's own exact-type guard (added to fix
        # a prior Codex-reported subclass-acceptance defect) already rejects
        # a bare subclass at construction time, even with every field copied
        # unmodified from a genuine intake. That means such an impostor can
        # never reach PromotedIdentityAdjudicationService in the first
        # place -- this test documents that fact rather than re-deriving it,
        # since constructing the subclass itself is what fails, not this
        # module's own intake type check.
        from india_swing.identity_registry.promoted_intake import (
            PromotedIdentityIntakeError,
        )

        class _IntakeSubclass(VerifiedPromotedIdentityIntake):
            pass

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            intake, case, status, evidence, review = _happy_path_fixture(root)
            with self.assertRaises(PromotedIdentityIntakeError):
                _IntakeSubclass(
                    **{
                        field.name: getattr(intake, field.name)
                        for field in dataclasses.fields(intake)
                    }
                )

    def test_wrong_evidence_tuple_type_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            intake, case, status, evidence, review = _happy_path_fixture(root)
            with self.assertRaises(PromotedIdentityAdjudicationError):
                PromotedIdentityAdjudicationService().materialize(
                    intake=intake,
                    evidence_artifacts=[evidence],  # type: ignore[arg-type]
                    review_bundles=(review,),
                    cutoff=_ADJUDICATION_CUTOFF,
                )

    def test_wrong_review_tuple_type_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            intake, case, status, evidence, review = _happy_path_fixture(root)
            with self.assertRaises(PromotedIdentityAdjudicationError):
                PromotedIdentityAdjudicationService().materialize(
                    intake=intake,
                    evidence_artifacts=(evidence,),
                    review_bundles=[review],  # type: ignore[arg-type]
                    cutoff=_ADJUDICATION_CUTOFF,
                )


class PromotedIdentityAdjudicationKnowledgeTimeTests(unittest.TestCase):
    def test_snapshot_knowledge_time_uses_max_of_intake_evidence_review(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            intake, case, status, evidence, review = _happy_path_fixture(root)
            adjudication = PromotedIdentityAdjudicationService().materialize(
                intake=intake,
                evidence_artifacts=(evidence,),
                review_bundles=(review,),
                cutoff=_ADJUDICATION_CUTOFF,
            )
            expected = max(
                intake.knowledge_time,
                evidence.manifest.validated_at,
                review.manifest.validated_at,
            )
            self.assertEqual(adjudication.snapshot.knowledge_time, expected)
            self.assertGreater(adjudication.snapshot.knowledge_time, intake.knowledge_time)


class PromotedIdentityAdjudicationLegacyRegressionTests(unittest.TestCase):
    def test_shared_core_signature_and_legacy_suite_are_unaffected(self) -> None:
        import inspect

        signature = inspect.signature(_materialize_adjudicated_identity_snapshot_core)
        self.assertIn("pre_satisfied_requirements", signature.parameters)
        self.assertIn("observations", signature.parameters)
        self.assertIn("candidates", signature.parameters)

    def test_legacy_snapshot_id_is_exactly_preserved(self) -> None:
        from tests.test_identity_decisions import IdentityDecisionTests

        legacy = IdentityDecisionTests()
        legacy.setUp()
        try:
            self.assertEqual(
                legacy.materialize().snapshot_id,
                "196ee9422cdf4311048b92c9f4a4311f8e2c76e4de4ad7601ea800deff6a8d2a",
            )
        finally:
            legacy.tearDown()


class PromotedIdentityAdjudicationDirectConstructionMismatchTests(unittest.TestCase):
    def _adjudication(self, tmp: str) -> VerifiedPromotedIdentityAdjudication:
        root = Path(tmp)
        intake, case, status, evidence, review = _happy_path_fixture(root)
        return PromotedIdentityAdjudicationService().materialize(
            intake=intake,
            evidence_artifacts=(evidence,),
            review_bundles=(review,),
            cutoff=_ADJUDICATION_CUTOFF,
        )

    def test_replacing_schema_marker_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            adjudication = self._adjudication(tmp)
            kwargs = _kwargs_from(adjudication)
            kwargs["schema_version"] = "promoted-identity-adjudication/v2"
            with self.assertRaises(PromotedIdentityAdjudicationError):
                VerifiedPromotedIdentityAdjudication(**kwargs)

    def test_replacing_cutoff_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            adjudication = self._adjudication(tmp)
            kwargs = _kwargs_from(adjudication)
            kwargs["cutoff"] = adjudication.cutoff + timedelta(days=1)
            with self.assertRaises(PromotedIdentityAdjudicationError):
                VerifiedPromotedIdentityAdjudication(**kwargs)

    def test_replacing_intake_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_a, tempfile.TemporaryDirectory() as tmp_b:
            adjudication = self._adjudication(tmp_a)
            other_intake = _build_alternate_intake(Path(tmp_b))
            kwargs = _kwargs_from(adjudication)
            kwargs["intake"] = other_intake
            with self.assertRaises(PromotedIdentityAdjudicationError):
                VerifiedPromotedIdentityAdjudication(**kwargs)

    def test_replacing_evidence_artifacts_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            adjudication = self._adjudication(tmp)
            kwargs = _kwargs_from(adjudication)
            kwargs["evidence_artifacts"] = ()
            with self.assertRaises(PromotedIdentityAdjudicationError):
                VerifiedPromotedIdentityAdjudication(**kwargs)

    def test_replacing_review_bundles_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            adjudication = self._adjudication(tmp)
            kwargs = _kwargs_from(adjudication)
            kwargs["review_bundles"] = ()
            with self.assertRaises(PromotedIdentityAdjudicationError):
                VerifiedPromotedIdentityAdjudication(**kwargs)

    def test_replacing_snapshot_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_a, tempfile.TemporaryDirectory() as tmp_b:
            adjudication_a = self._adjudication(tmp_a)
            root_b = Path(tmp_b)
            intake_b, case_b, status_b, evidence_b, review_b = _alternate_happy_path_fixture(root_b)
            adjudication_b = PromotedIdentityAdjudicationService().materialize(
                intake=intake_b,
                evidence_artifacts=(evidence_b,),
                review_bundles=(review_b,),
                cutoff=_ADJUDICATION_CUTOFF,
            )
            kwargs = _kwargs_from(adjudication_a)
            kwargs["snapshot"] = adjudication_b.snapshot
            with self.assertRaises(PromotedIdentityAdjudicationError):
                VerifiedPromotedIdentityAdjudication(**kwargs)

    def test_replacing_readiness_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            adjudication = self._adjudication(tmp)
            kwargs = _kwargs_from(adjudication)
            kwargs["readiness"] = ReferenceReadiness.POINT_IN_TIME_VERIFIED
            with self.assertRaises(PromotedIdentityAdjudicationError):
                VerifiedPromotedIdentityAdjudication(**kwargs)

    def test_replacing_actionable_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            adjudication = self._adjudication(tmp)
            kwargs = _kwargs_from(adjudication)
            kwargs["actionable"] = True
            with self.assertRaises(PromotedIdentityAdjudicationError):
                VerifiedPromotedIdentityAdjudication(**kwargs)

    def test_replacing_stable_identity_assigned_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            adjudication = self._adjudication(tmp)
            kwargs = _kwargs_from(adjudication)
            kwargs["stable_identity_assigned"] = not adjudication.stable_identity_assigned
            with self.assertRaises(PromotedIdentityAdjudicationError):
                VerifiedPromotedIdentityAdjudication(**kwargs)

    def test_replacing_adjudication_id_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            adjudication = self._adjudication(tmp)
            kwargs = _kwargs_from(adjudication)
            kwargs["adjudication_id"] = hashlib.sha256(b"different").hexdigest()
            with self.assertRaises(PromotedIdentityAdjudicationError):
                VerifiedPromotedIdentityAdjudication(**kwargs)


class PromotedIdentityAdjudicationMutationTests(unittest.TestCase):
    def _adjudication(self, tmp: str) -> VerifiedPromotedIdentityAdjudication:
        root = Path(tmp)
        intake, case, status, evidence, review = _happy_path_fixture(root)
        return PromotedIdentityAdjudicationService().materialize(
            intake=intake,
            evidence_artifacts=(evidence,),
            review_bundles=(review,),
            cutoff=_ADJUDICATION_CUTOFF,
        )

    def test_mutating_top_level_adjudication_id_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            adjudication = self._adjudication(tmp)
            object.__setattr__(adjudication, "adjudication_id", "0" * 64)
            with self.assertRaises(PromotedIdentityAdjudicationError):
                adjudication.verify_content_identity()

    def test_mutating_nested_intake_field_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            adjudication = self._adjudication(tmp)
            object.__setattr__(adjudication.intake, "intake_id", "0" * 64)
            with self.assertRaises(PromotedIdentityAdjudicationError):
                adjudication.verify_content_identity()

    def test_mutating_nested_evidence_manifest_field_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            adjudication = self._adjudication(tmp)
            object.__setattr__(
                adjudication.evidence_artifacts[0].manifest, "source_sha256", "a" * 64
            )
            with self.assertRaises(PromotedIdentityAdjudicationError):
                adjudication.verify_content_identity()

    def test_mutating_nested_evidence_claim_field_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            adjudication = self._adjudication(tmp)
            claim = adjudication.evidence_artifacts[0].parsed.claims[0]
            object.__setattr__(claim, "symbol", "TAMPERED")
            with self.assertRaises(PromotedIdentityAdjudicationError):
                adjudication.verify_content_identity()

    def test_mutating_nested_review_manifest_field_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            adjudication = self._adjudication(tmp)
            object.__setattr__(
                adjudication.review_bundles[0].manifest, "reviewer_id", "owner:tampered"
            )
            with self.assertRaises(PromotedIdentityAdjudicationError):
                adjudication.verify_content_identity()

    def test_mutating_nested_review_decision_field_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            adjudication = self._adjudication(tmp)
            decision = adjudication.review_bundles[0].parsed.decisions[0]
            object.__setattr__(decision, "rationale", "tampered rationale")
            with self.assertRaises(PromotedIdentityAdjudicationError):
                adjudication.verify_content_identity()

    def test_mutating_snapshot_resolution_field_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            adjudication = self._adjudication(tmp)
            resolution = adjudication.snapshot.resolutions[0]
            self.assertTrue(resolution.accepted_decision_ids)
            object.__setattr__(resolution, "accepted_decision_ids", ())
            with self.assertRaises(PromotedIdentityAdjudicationError):
                adjudication.verify_content_identity()

    def test_mutating_listing_observation_field_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            adjudication = self._adjudication(tmp)
            self.assertTrue(adjudication.snapshot.listing_observations)
            observation = adjudication.snapshot.listing_observations[0]
            object.__setattr__(observation, "symbol", "TAMPERED")
            with self.assertRaises(PromotedIdentityAdjudicationError):
                adjudication.verify_content_identity()


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


class PromotedIdentityAdjudicationComparisonBoundaryTests(unittest.TestCase):
    def _adjudication(self, tmp: str) -> VerifiedPromotedIdentityAdjudication:
        root = Path(tmp)
        intake, case, status, evidence, review = _happy_path_fixture(root)
        return PromotedIdentityAdjudicationService().materialize(
            intake=intake,
            evidence_artifacts=(evidence,),
            review_bundles=(review,),
            cutoff=_ADJUDICATION_CUTOFF,
        )

    def _assert_sanitized(self, secret: str, exc: BaseException) -> None:
        self.assertIsInstance(exc, PromotedIdentityAdjudicationError)
        message = str(exc)
        self.assertNotIn("RuntimeError", message)
        self.assertNotIn(secret, message)

    def test_malicious_evidence_claim_equality_is_sanitized(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            adjudication = self._adjudication(tmp)
            secret = "evidence-secret-1a2b"
            claim = adjudication.evidence_artifacts[0].parsed.claims[0]
            object.__setattr__(claim, "symbol", _EvilEq(secret))
            with self.assertRaises(PromotedIdentityAdjudicationError) as ctx:
                adjudication.verify_content_identity()
            self._assert_sanitized(secret, ctx.exception)

    def test_malicious_review_decision_equality_is_sanitized(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            adjudication = self._adjudication(tmp)
            secret = "review-secret-3c4d"
            decision = adjudication.review_bundles[0].parsed.decisions[0]
            object.__setattr__(decision, "rationale", _EvilEq(secret))
            with self.assertRaises(PromotedIdentityAdjudicationError) as ctx:
                adjudication.verify_content_identity()
            self._assert_sanitized(secret, ctx.exception)

    def test_malicious_snapshot_resolution_equality_is_sanitized(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            adjudication = self._adjudication(tmp)
            secret = "snapshot-secret-5e6f"
            resolution = adjudication.snapshot.resolutions[0]
            object.__setattr__(resolution, "candidate_id", _EvilEq(secret))
            with self.assertRaises(PromotedIdentityAdjudicationError) as ctx:
                adjudication.verify_content_identity()
            self._assert_sanitized(secret, ctx.exception)

    def test_base_exception_from_equality_is_not_swallowed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            adjudication = self._adjudication(tmp)
            claim = adjudication.evidence_artifacts[0].parsed.claims[0]
            object.__setattr__(claim, "symbol", _EvilEqBaseException())
            with self.assertRaises(_ComparisonBoundaryBaseException):
                adjudication.verify_content_identity()


class PromotedIdentityAdjudicationContentIdCompletenessTests(unittest.TestCase):
    def test_different_cutoff_changes_adjudication_id(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_a, tempfile.TemporaryDirectory() as tmp_b:
            root_a, root_b = Path(tmp_a), Path(tmp_b)
            intake_a, case_a, status_a, evidence_a, review_a = _happy_path_fixture(root_a)
            intake_b, case_b, status_b, evidence_b, review_b = _happy_path_fixture(root_b)
            adjudication_a = PromotedIdentityAdjudicationService().materialize(
                intake=intake_a,
                evidence_artifacts=(evidence_a,),
                review_bundles=(review_a,),
                cutoff=_ADJUDICATION_CUTOFF,
            )
            adjudication_b = PromotedIdentityAdjudicationService().materialize(
                intake=intake_b,
                evidence_artifacts=(evidence_b,),
                review_bundles=(review_b,),
                cutoff=_ADJUDICATION_CUTOFF + timedelta(hours=1),
            )
            self.assertNotEqual(adjudication_a.adjudication_id, adjudication_b.adjudication_id)

    def test_filesystem_path_alone_does_not_change_adjudication_id(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_a, tempfile.TemporaryDirectory() as tmp_b:
            root_a, root_b = Path(tmp_a), Path(tmp_b)
            intake_a, case_a, status_a, evidence_a, review_a = _happy_path_fixture(root_a)
            intake_b, case_b, status_b, evidence_b, review_b = _happy_path_fixture(root_b)
            self.assertNotEqual(evidence_a.path, evidence_b.path)
            adjudication_a = PromotedIdentityAdjudicationService().materialize(
                intake=intake_a,
                evidence_artifacts=(evidence_a,),
                review_bundles=(review_a,),
                cutoff=_ADJUDICATION_CUTOFF,
            )
            adjudication_b = PromotedIdentityAdjudicationService().materialize(
                intake=intake_b,
                evidence_artifacts=(evidence_b,),
                review_bundles=(review_b,),
                cutoff=_ADJUDICATION_CUTOFF,
            )
            self.assertEqual(adjudication_a.adjudication_id, adjudication_b.adjudication_id)


class PromotedIdentityAdjudicationCapabilityTests(unittest.TestCase):
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
            "notif",
            "broker",
            "order",
            "capital",
        )
        for candidate in (
            PromotedIdentityAdjudicationService,
            VerifiedPromotedIdentityAdjudication,
        ):
            members = [name for name in dir(candidate) if not name.startswith("__")]
            for name in members:
                lowered = name.lower()
                self.assertFalse(
                    any(bad in lowered for bad in banned_substrings),
                    f"{candidate!r} unexpectedly exposes {name!r}",
                )

    def test_no_forbidden_capability_field_exists(self) -> None:
        field_names = {
            field.name for field in dataclasses.fields(VerifiedPromotedIdentityAdjudication)
        }
        for banned in (
            "universe",
            "calendar",
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
        ):
            self.assertFalse(any(banned in name for name in field_names))

    def test_adjudication_cannot_be_used_as_promotion_evidence(self) -> None:
        from india_swing.promotion.models import PromotionDecision, PromotionEvidence

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            intake, case, status, evidence, review = _happy_path_fixture(root)
            adjudication = PromotedIdentityAdjudicationService().materialize(
                intake=intake,
                evidence_artifacts=(evidence,),
                review_bundles=(review,),
                cutoff=_ADJUDICATION_CUTOFF,
            )
            self.assertNotIsInstance(adjudication, PromotionEvidence)
            self.assertNotIsInstance(adjudication, PromotionDecision)
            self.assertFalse(
                issubclass(VerifiedPromotedIdentityAdjudication, PromotionEvidence)
            )

    def test_importing_module_causes_no_io(self) -> None:
        import india_swing.identity_decisions.promoted_materialize as module

        banned_module_names = {"os", "socket", "urllib", "requests", "storage"}
        top_level_names = {
            name
            for name in vars(module)
            if not name.startswith("_") and not name[0].isupper()
        }
        self.assertFalse(top_level_names & banned_module_names)


if __name__ == "__main__":
    unittest.main()
