from __future__ import annotations

import hashlib
import io
import json
import tempfile
import unittest
from contextlib import redirect_stdout
from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

from india_swing.identity_evidence import (
    IDENTITY_EVIDENCE_DECLARATION_SCHEMA_VERSION,
    IdentityEvidenceConfig,
    IdentityEvidenceDeclarationParser,
    IdentityEvidenceIntegrityError,
    LocalIdentityEvidenceArtifactStore,
    build_identity_evidence_coverage,
    encode_identity_evidence_declaration,
)
from india_swing.identity_registry import (
    IdentityAdjudicationCase,
    IdentityAdjudicationQueue,
    IdentityAdjudicationRequirement,
    IdentityCandidateBasis,
    IdentityCandidateStatus,
)
from india_swing.identity_evidence.cli import main as identity_evidence_main


UTC = timezone.utc
FIRST_SEEN = datetime(2026, 7, 16, 12, 0, tzinfo=UTC)
VALIDATED = FIRST_SEEN + timedelta(seconds=2)
PDF_NAME = "CML73417.pdf"
PDF_BYTES = b"%PDF-1.7\n1 0 obj\n<< /Type /Catalog >>\nendobj\n%%EOF\n"
CANDIDATE_ID = "a" * 64


def clock_sequence(*values: datetime):
    iterator = iter(values)
    return lambda: next(iterator)


def claim(*, candidate_id: str = CANDIDATE_ID, requirement: str = "OFFICIAL_LISTING_LIFECYCLE", effective_date: str | None = "2026-03-24") -> dict[str, object]:
    return {
        "candidate_id": candidate_id,
        "requirement": requirement,
        "effective_date": effective_date,
        "symbol": "EXAMPLE",
        "series": "EQ",
        "isin": "INE009A01021",
        "locator": {"page": 1, "row": None, "section": "Removal table"},
        "claim_text": "Removal from dissemination board effective on the declared date.",
    }


def declaration_value(*, source_bytes: bytes = PDF_BYTES, claims: list[dict[str, object]] | None = None, source_url: str = "https://nsearchives.nseindia.com/content/circulars/CML73417.pdf") -> dict[str, object]:
    return {
        "schema_version": IDENTITY_EVIDENCE_DECLARATION_SCHEMA_VERSION,
        "exchange": "NSE",
        "segment": "CM",
        "claimed_authority": "NSE",
        "source_kind": "LISTING_CIRCULAR_PDF",
        "claimed_document_id": "NSE/LIST/C/2026/0489",
        "claimed_issue_date": "2026-03-23",
        "claimed_publication_at": None,
        "claimed_source_url": source_url,
        "source_filename": PDF_NAME,
        "source_media_type": "application/pdf",
        "source_byte_count": len(source_bytes),
        "source_sha256": hashlib.sha256(source_bytes).hexdigest(),
        "claims": claims or [claim()],
    }


def declaration_bytes(**kwargs: object) -> bytes:
    return json.dumps(declaration_value(**kwargs), separators=(",", ":"), sort_keys=True).encode()


def queue() -> IdentityAdjudicationQueue:
    requirements = tuple(sorted(
        {
            IdentityAdjudicationRequirement.AUTHORIZED_SOURCE_PROVENANCE,
            IdentityAdjudicationRequirement.REPORT_DATE_VERIFICATION,
            IdentityAdjudicationRequirement.OFFICIAL_LISTING_LIFECYCLE,
        },
        key=lambda value: value.value,
    ))
    case = IdentityAdjudicationCase(
        candidate_id=CANDIDATE_ID,
        basis=IdentityCandidateBasis.VALIDATED_ISIN,
        candidate_status=IdentityCandidateStatus.CANDIDATE_CONTINUITY,
        observation_claims=(("b" * 64, date(2026, 7, 16)),),
        transition_ids=("c" * 64,), conflict_ids=(), requirements=requirements,
    )
    return IdentityAdjudicationQueue(
        source_registry_id="d" * 64,
        source_cutoff=datetime(2026, 7, 16, 12, 5, tzinfo=UTC),
        source_knowledge_time=datetime(2026, 7, 16, 12, 0, tzinfo=UTC),
        source_artifact_ids=("e" * 64,), source_manifest_ids=("f" * 64,),
        cases=(case,),
    )


class IdentityEvidenceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.source = self.root / PDF_NAME
        self.declaration = self.root / "CML73417.identity.json"
        self.source.write_bytes(PDF_BYTES)
        self.declaration.write_bytes(declaration_bytes())

    def tearDown(self) -> None:
        self.temp.cleanup()

    def parser(self):
        return IdentityEvidenceDeclarationParser().parse_bytes(
            self.declaration.read_bytes(), source_bytes=PDF_BYTES,
            source_filename=PDF_NAME, declaration_filename=self.declaration.name,
        )

    def store(self) -> LocalIdentityEvidenceArtifactStore:
        return LocalIdentityEvidenceArtifactStore(
            self.root / "archive", clock=clock_sequence(FIRST_SEEN, VALIDATED)
        )

    def test_declaration_is_exact_source_bound_and_deterministic(self) -> None:
        first, second = self.parser(), self.parser()
        self.assertEqual(first, second)
        self.assertEqual(first.claims[0].candidate_id, CANDIDATE_ID)
        self.assertIs(first.claims[0].requirement, IdentityAdjudicationRequirement.OFFICIAL_LISTING_LIFECYCLE)
        self.assertEqual(encode_identity_evidence_declaration(first), encode_identity_evidence_declaration(second))
        with self.assertRaisesRegex(IdentityEvidenceIntegrityError, "exact source bytes"):
            IdentityEvidenceDeclarationParser().parse_bytes(
                self.declaration.read_bytes(), source_bytes=PDF_BYTES + b"x",
                source_filename=PDF_NAME, declaration_filename=self.declaration.name,
            )

    def test_only_official_nse_urls_and_strict_schema_are_accepted(self) -> None:
        self.declaration.write_bytes(declaration_bytes(source_url="https://example.com/CML73417.pdf"))
        with self.assertRaisesRegex(IdentityEvidenceIntegrityError, "pinned contract"):
            self.parser()
        value = declaration_value()
        value["knowledge_time"] = "2020-01-01T00:00:00Z"
        self.declaration.write_text(json.dumps(value), encoding="utf-8")
        with self.assertRaisesRegex(IdentityEvidenceIntegrityError, "schema mismatch"):
            self.parser()

    def test_listing_lifecycle_claim_requires_effective_date(self) -> None:
        self.declaration.write_bytes(declaration_bytes(claims=[claim(effective_date=None)]))
        with self.assertRaisesRegex(IdentityEvidenceIntegrityError, "invalid identity evidence claim"):
            self.parser()

    def test_archive_is_create_once_idempotent_and_detects_tampering(self) -> None:
        store = self.store()
        first = store.import_source(self.source, self.declaration)
        second = LocalIdentityEvidenceArtifactStore(
            store.root, clock=clock_sequence(FIRST_SEEN + timedelta(days=1), VALIDATED + timedelta(days=1))
        ).import_source(self.source, self.declaration)
        self.assertEqual(first.manifest, second.manifest)
        self.assertEqual(first.source_bytes, PDF_BYTES)
        self.assertFalse(first.manifest.actionable)
        self.assertFalse(first.manifest.stable_identity_assigned)
        (first.path / "normalized.json").write_bytes(b"{}")
        with self.assertRaisesRegex(IdentityEvidenceIntegrityError, "payload digest"):
            store.get(first.manifest.artifact_id)

    def test_coverage_reports_presence_without_satisfying_requirements(self) -> None:
        stored = self.store().import_source(self.source, self.declaration)
        report = build_identity_evidence_coverage(queue(), (stored,))
        self.assertEqual(report.required_pair_count, 3)
        self.assertEqual(report.evidence_collected_pair_count, 1)
        self.assertEqual(report.missing_pair_count, 2)
        self.assertFalse(report.requirements_satisfied)
        self.assertFalse(report.actionable)
        self.assertFalse(report.stable_identity_assigned)

    def test_claim_for_unknown_queue_candidate_fails_closed(self) -> None:
        self.declaration.write_bytes(declaration_bytes(claims=[claim(candidate_id="9" * 64)]))
        stored = self.store().import_source(self.source, self.declaration)
        with self.assertRaisesRegex(IdentityEvidenceIntegrityError, "exact requirement"):
            build_identity_evidence_coverage(queue(), (stored,))

    def test_corporate_action_csv_requires_official_columns_and_row_locator(self) -> None:
        csv_bytes = (
            b"SYMBOL,COMPANY NAME,SERIES,PURPOSE,FACE VALUE,EX-DATE,RECORD DATE,"
            b"BOOK CLOSURE START DATE,BOOK CLOSURE END DATE\n"
            b"EXAMPLE,Example Limited,EQ,Face Value Split,10,24-Mar-2026,25-Mar-2026,,\n"
        )
        name = "corporate_actions.csv"
        value = declaration_value(source_bytes=csv_bytes)
        value.update({
            "source_kind": "CORPORATE_ACTION_CSV", "source_filename": name,
            "source_media_type": "text/csv",
            "claimed_source_url": "https://www.nseindia.com/companies-listing/corporate-filings-actions",
        })
        csv_claim = claim(requirement="AUTHORIZED_SOURCE_PROVENANCE", effective_date=None)
        csv_claim["locator"] = {"page": None, "row": 2, "section": "Corporate action row"}
        value["claims"] = [csv_claim]
        parsed = IdentityEvidenceDeclarationParser().parse_bytes(
            json.dumps(value).encode(), source_bytes=csv_bytes, source_filename=name,
            declaration_filename="corporate_actions.identity.json",
        )
        self.assertEqual(parsed.claims[0].locator.row, 2)

    def test_config_default_and_override(self) -> None:
        self.assertEqual(IdentityEvidenceConfig.from_env({}).data_root, Path("var/identity_evidence"))
        self.assertEqual(
            IdentityEvidenceConfig.from_env({"INDIA_SWING_IDENTITY_EVIDENCE_ROOT": str(self.root)}).data_root,
            self.root,
        )

    def test_nested_claim_mutation_is_detected(self) -> None:
        parsed = self.parser()
        object.__setattr__(parsed.claims[0], "claim_text", "mutated")
        with self.assertRaisesRegex(IdentityEvidenceIntegrityError, "claim identity"):
            encode_identity_evidence_declaration(parsed)

    def test_cli_imports_shows_and_lists_collection_only_artifact(self) -> None:
        archive = self.root / "cli-archive"
        with patch.dict(
            "os.environ",
            {"INDIA_SWING_IDENTITY_EVIDENCE_ROOT": str(archive)},
            clear=False,
        ):
            output = io.StringIO()
            with redirect_stdout(output):
                self.assertEqual(identity_evidence_main([
                    "import", "--source", str(self.source),
                    "--declaration", str(self.declaration),
                ]), 0)
            imported = json.loads(output.getvalue())
            self.assertFalse(imported["actionable"])
            evidence_id = imported["evidence_id"]

            for arguments in (["show", "--evidence-id", evidence_id], ["list"]):
                output = io.StringIO()
                with redirect_stdout(output):
                    self.assertEqual(identity_evidence_main(arguments), 0)
                self.assertEqual(json.loads(output.getvalue())["status"], "COMPLETE")


if __name__ == "__main__":
    unittest.main()
