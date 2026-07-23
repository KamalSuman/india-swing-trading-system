from __future__ import annotations

import csv
import gzip
import hashlib
import io
import json
import tempfile
import unittest
from contextlib import redirect_stdout
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

from india_swing.identity_decisions import (
    ADJUDICATED_IDENTITY_POLICY_VERSION,
    IDENTITY_REVIEW_DECLARATION_SCHEMA_VERSION,
    IdentityDecisionConflict,
    IdentityDecisionIntegrityError,
    IdentityResolutionBlocker,
    LocalAdjudicatedIdentitySnapshotStore,
    LocalIdentityReviewBundleStore,
    decode_adjudicated_identity_snapshot,
    encode_adjudicated_identity_snapshot,
    materialize_adjudicated_identity_snapshot,
)
from india_swing.identity_decisions.cli import main as identity_decision_main
from india_swing.identity_evidence import (
    IDENTITY_EVIDENCE_DECLARATION_SCHEMA_VERSION,
    LocalIdentityEvidenceArtifactStore,
)
from india_swing.identity_registry import (
    IdentityCandidateBasis,
    LocalIdentityAdjudicationQueueStore,
    LocalIdentityRegistryStore,
    build_identity_adjudication_queue,
    materialize_cross_vintage_identity_registry,
)
from india_swing.reference_data.artifact_store import LocalReferenceArtifactStore
from india_swing.reference_data.security_master import NSE_CM_MII_SECURITY_HEADER


UTC = timezone.utc
DAY_ONE_FIRST = datetime(2026, 7, 15, 10, 0, tzinfo=UTC)
DAY_ONE_VALIDATED = DAY_ONE_FIRST + timedelta(seconds=2)
DAY_TWO_FIRST = datetime(2026, 7, 16, 10, 0, tzinfo=UTC)
DAY_TWO_VALIDATED = DAY_TWO_FIRST + timedelta(seconds=2)
REGISTRY_CUTOFF = datetime(2026, 7, 16, 10, 5, tzinfo=UTC)
EVIDENCE_FIRST = datetime(2026, 7, 16, 11, 0, tzinfo=UTC)
EVIDENCE_VALIDATED = EVIDENCE_FIRST + timedelta(seconds=2)
REVIEWED_AT = datetime(2026, 7, 16, 11, 5, tzinfo=UTC)
REVIEW_FIRST = REVIEWED_AT + timedelta(minutes=1)
REVIEW_VALIDATED = REVIEW_FIRST + timedelta(seconds=2)
SNAPSHOT_CUTOFF = datetime(2026, 7, 16, 12, 0, tzinfo=UTC)
PDF_BYTES = b"%PDF-1.7\n1 0 obj\n<< /Type /Catalog >>\nendobj\n%%EOF\n"


def clock_sequence(*values: datetime):
    iterator = iter(values)
    return lambda: next(iterator)


def security_row(**overrides: str) -> list[str]:
    values = {name: "" for name in NSE_CM_MII_SECURITY_HEADER}
    values.update({
        "FinInstrmId": "1594", "TckrSymb": "INFY", "SctySrs": "EQ",
        "FinInstrmNm": "INFOSYS LIMITED", "ISIN": "INE009A01021",
        "NewBrdLotQty": "1", "ParVal": "500", "SctyTpFlg": "0",
        "BidIntrvl": "5", "TrckgInd": "0", "CallAuctnInd": "1",
        "PrtdToTrad": "0", "PricRg": "0.00-99999.00",
        "SctyStsNrmlMkt": "6", "ElgbltyNrmlMkt": "1",
        "SctyStsOddLotMkt": "2", "ElgbltyOddLotMkt": "1",
        "SctyStsRETDBTMkt": "2", "ElgbltyRETDBTMkt": "0",
        "SctyStsAuctnMkt": "2", "ElgbltyAuctnMkt": "1",
        "SctyStsAddtlMkt1": "1", "ElgbltyAddtlMkt1": "0",
        "SctyStsAddtlMkt2": "1", "ElgbltyAddtlMkt2": "0",
        "ListgDt": "476668800", "RmvlDt": "0", "RadmssnDt": "0",
        "DelFlg": "N",
    })
    values.update(overrides)
    return [values[name] for name in NSE_CM_MII_SECURITY_HEADER]


def master_bytes(rows: list[list[str]]) -> bytes:
    stream = io.StringIO(newline="")
    writer = csv.writer(stream, lineterminator="\n")
    writer.writerow(NSE_CM_MII_SECURITY_HEADER)
    writer.writerows(rows)
    return gzip.compress(stream.getvalue().encode(), mtime=0)


class IdentityDecisionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.reference_root = self.root / "reference"
        self.identity_root = self.root / "identity"
        self.evidence_root = self.root / "evidence"
        first = self.root / "NSE_CM_security_15072026.csv.gz"
        second = self.root / "NSE_CM_security_16072026.csv.gz"
        first.write_bytes(master_bytes([security_row()]))
        second.write_bytes(master_bytes([security_row(TckrSymb="INFYNEW", FinInstrmId="2000")]))
        reference_store = LocalReferenceArtifactStore(
            self.reference_root,
            clock=clock_sequence(DAY_ONE_FIRST, DAY_ONE_VALIDATED, DAY_TWO_FIRST, DAY_TWO_VALIDATED),
        )
        sources = (
            reference_store.import_security_master(first),
            reference_store.import_security_master(second),
        )
        self.registry = materialize_cross_vintage_identity_registry(
            sources=sources, cutoff=REGISTRY_CUTOFF
        )
        registry_store = LocalIdentityRegistryStore(self.identity_root, self.reference_root)
        registry_store.put(self.registry)
        self.queue = LocalIdentityAdjudicationQueueStore(
            self.identity_root, registry_store
        ).publish(build_identity_adjudication_queue(self.registry), registry_id=self.registry.registry_id)
        self.case = self.queue.cases[0]
        self.evidence = self._import_evidence()
        self.review = self._import_review()

    def tearDown(self) -> None:
        self.temp.cleanup()

    def _import_evidence(self):
        source = self.root / "CML-IDENTITY.pdf"
        declaration = self.root / "CML-IDENTITY.evidence.json"
        source.write_bytes(PDF_BYTES)
        claims = []
        for requirement in self.case.requirements:
            claims.append({
                "candidate_id": self.case.candidate_id,
                "requirement": requirement.value,
                "effective_date": "2026-07-16" if requirement.value in {
                    "OFFICIAL_LISTING_LIFECYCLE", "OFFICIAL_LISTING_STATUS"
                } else None,
                "symbol": "INFYNEW", "series": "EQ", "isin": "INE009A01021",
                "locator": {"page": 1, "row": None, "section": requirement.value},
                "claim_text": f"Synthetic official-source claim for {requirement.value}.",
            })
        value = {
            "schema_version": IDENTITY_EVIDENCE_DECLARATION_SCHEMA_VERSION,
            "exchange": "NSE", "segment": "CM", "claimed_authority": "NSE",
            "source_kind": "LISTING_CIRCULAR_PDF",
            "claimed_document_id": "NSE/LIST/C/2026/TEST",
            "claimed_issue_date": "2026-07-16", "claimed_publication_at": None,
            "claimed_source_url": "https://nsearchives.nseindia.com/content/circulars/CML-IDENTITY.pdf",
            "source_filename": source.name, "source_media_type": "application/pdf",
            "source_byte_count": len(PDF_BYTES),
            "source_sha256": hashlib.sha256(PDF_BYTES).hexdigest(),
            "claims": claims,
        }
        declaration.write_text(json.dumps(value), encoding="utf-8")
        return LocalIdentityEvidenceArtifactStore(
            self.evidence_root,
            clock=clock_sequence(EVIDENCE_FIRST, EVIDENCE_VALIDATED),
        ).import_source(source, declaration)

    def _review_value(
        self,
        *,
        outcomes: dict[str, str] | None = None,
        reviewer_id: str = "owner:kamal",
        reviewed_at: datetime = REVIEWED_AT,
        claim_override: dict[str, str] | None = None,
    ) -> dict[str, object]:
        claims = {value.requirement.value: value for value in self.evidence.parsed.claims}
        decisions = []
        for requirement in self.case.requirements:
            evidence_claim = claims[requirement.value]
            decisions.append({
                "candidate_id": self.case.candidate_id,
                "requirement": requirement.value,
                "outcome": (outcomes or {}).get(requirement.value, "ACCEPTED"),
                "evidence_artifact_id": self.evidence.manifest.artifact_id,
                "evidence_claim_id": (claim_override or {}).get(requirement.value, evidence_claim.claim_id),
                "rationale": f"Reviewed exact evidence for {requirement.value}.",
            })
        return {
            "schema_version": IDENTITY_REVIEW_DECLARATION_SCHEMA_VERSION,
            "queue_id": self.queue.queue_id,
            "source_registry_id": self.registry.registry_id,
            "reviewer_id": reviewer_id,
            "reviewed_at": reviewed_at.isoformat(),
            "decisions": decisions,
        }

    def _import_review(self, **kwargs: object):
        reviewer = str(kwargs.get("reviewer_id", "owner:kamal")).replace(":", "-")
        path = self.root / f"review-{reviewer}-{len(tuple(self.root.glob('review-*.json')))}.json"
        path.write_text(json.dumps(self._review_value(**kwargs)), encoding="utf-8")
        return LocalIdentityReviewBundleStore(
            self.evidence_root,
            clock=clock_sequence(REVIEW_FIRST, REVIEW_VALIDATED),
        ).import_declaration(path)

    def materialize(self, reviews=None, evidence=None):
        return materialize_adjudicated_identity_snapshot(
            registry=self.registry, queue=self.queue,
            evidence_artifacts=(self.evidence,) if evidence is None else evidence,
            review_bundles=(self.review,) if reviews is None else reviews,
            cutoff=SNAPSHOT_CUTOFF,
        )

    def test_complete_acceptance_assigns_stable_ids_without_actionability(self) -> None:
        snapshot = self.materialize()
        self.assertTrue(snapshot.stable_identity_assigned)
        self.assertFalse(snapshot.actionable)
        self.assertEqual(len(snapshot.listing_observations), 2)
        self.assertEqual(
            len({value.stable_instrument_id for value in snapshot.listing_observations}), 1
        )
        self.assertEqual(
            len({value.stable_listing_id for value in snapshot.listing_observations}), 1
        )
        self.assertEqual(
            {value.symbol for value in snapshot.listing_observations}, {"INFY", "INFYNEW"}
        )

    def test_empty_explicit_review_set_assigns_nothing(self) -> None:
        snapshot = self.materialize(reviews=(), evidence=())
        resolution = snapshot.resolutions[0]
        self.assertFalse(snapshot.stable_identity_assigned)
        self.assertIn(IdentityResolutionBlocker.MISSING_REVIEW_DECISION, resolution.blocker_codes)
        self.assertEqual(set(resolution.missing_requirements), set(self.case.requirements))

    def test_rejected_decision_blocks_assignment(self) -> None:
        rejected = self._import_review(outcomes={self.case.requirements[0].value: "REJECTED"}, reviewer_id="owner:reject")
        snapshot = self.materialize(reviews=(rejected,))
        self.assertFalse(snapshot.stable_identity_assigned)
        self.assertIn(
            IdentityResolutionBlocker.REJECTED_REVIEW_DECISION,
            snapshot.resolutions[0].blocker_codes,
        )

    def test_mismatched_claim_subject_fails_closed(self) -> None:
        first, second = self.case.requirements[:2]
        claims = {value.requirement: value.claim_id for value in self.evidence.parsed.claims}
        mismatched = self._import_review(
            reviewer_id="owner:mismatch",
            claim_override={first.value: claims[second]},
        )
        with self.assertRaisesRegex(IdentityDecisionIntegrityError, "subjects differ"):
            self.materialize(reviews=(mismatched,))

    def test_duplicate_explicit_decisions_fail_instead_of_choosing_latest(self) -> None:
        second = self._import_review(reviewer_id="owner:second")
        with self.assertRaisesRegex(IdentityDecisionConflict, "duplicate decisions"):
            self.materialize(reviews=(self.review, second))

    def test_review_cannot_predate_evidence(self) -> None:
        early = self._import_review(
            reviewer_id="owner:early",
            reviewed_at=EVIDENCE_FIRST - timedelta(minutes=1),
        )
        with self.assertRaisesRegex(IdentityDecisionIntegrityError, "predates"):
            self.materialize(reviews=(early,))

    def test_review_store_is_idempotent_and_detects_tampering(self) -> None:
        loaded = LocalIdentityReviewBundleStore(self.evidence_root).get(self.review.manifest.bundle_id)
        self.assertEqual(loaded.manifest, self.review.manifest)
        (loaded.path / "normalized.json").write_bytes(b"{}")
        with self.assertRaisesRegex(IdentityDecisionIntegrityError, "payload digest"):
            LocalIdentityReviewBundleStore(self.evidence_root).get(self.review.manifest.bundle_id)

    def test_snapshot_codec_store_round_trip_and_tamper_detection(self) -> None:
        snapshot = self.materialize()
        self.assertEqual(decode_adjudicated_identity_snapshot(
            encode_adjudicated_identity_snapshot(snapshot)
        ), snapshot)
        store = LocalAdjudicatedIdentitySnapshotStore(self.evidence_root)
        self.assertEqual(store.put(snapshot), snapshot)
        path = store.path_for(snapshot.snapshot_id)
        path.write_bytes(b"{}")
        with self.assertRaises(IdentityDecisionIntegrityError):
            store.get(snapshot.snapshot_id)

    def test_cli_materializes_and_shows_explicit_snapshot(self) -> None:
        environment = {
            "INDIA_SWING_REFERENCE_DATA_ROOT": str(self.reference_root),
            "INDIA_SWING_IDENTITY_REGISTRY_ROOT": str(self.identity_root),
            "INDIA_SWING_IDENTITY_EVIDENCE_ROOT": str(self.evidence_root),
        }
        with patch.dict("os.environ", environment, clear=False):
            output = io.StringIO()
            with redirect_stdout(output):
                self.assertEqual(identity_decision_main([
                    "materialize", "--registry-id", self.registry.registry_id,
                    "--evidence-id", self.evidence.manifest.artifact_id,
                    "--review-bundle-id", self.review.manifest.bundle_id,
                    "--cutoff", SNAPSHOT_CUTOFF.isoformat(),
                ]), 0)
            materialized = json.loads(output.getvalue())
            self.assertEqual(materialized["assigned_candidate_count"], 1)
            self.assertFalse(materialized["actionable"])

            output = io.StringIO()
            with redirect_stdout(output):
                self.assertEqual(identity_decision_main([
                    "snapshot-show", "--snapshot-id", materialized["snapshot_id"],
                ]), 0)
            self.assertEqual(json.loads(output.getvalue())["snapshot_id"], materialized["snapshot_id"])


class ReviewedIdentityCorrectionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)

    def tearDown(self) -> None:
        self.temp.cleanup()

    def _registry_and_queue(
        self,
        rows: list[list[str]],
        rows_second: list[list[str]] | None = None,
    ):
        source = self.root / "NSE_CM_security_15072026.csv.gz"
        source.write_bytes(master_bytes(rows))
        clock_values = [DAY_ONE_FIRST, DAY_ONE_VALIDATED]
        if rows_second is not None:
            clock_values.extend([DAY_TWO_FIRST, DAY_TWO_VALIDATED])
        store = LocalReferenceArtifactStore(
            self.root / "reference",
            clock=clock_sequence(*clock_values),
        )
        sources = [store.import_security_master(source)]
        if rows_second is not None:
            second = self.root / "NSE_CM_security_16072026.csv.gz"
            second.write_bytes(master_bytes(rows_second))
            sources.append(store.import_security_master(second))
        registry = materialize_cross_vintage_identity_registry(
            sources=tuple(sources),
            cutoff=REGISTRY_CUTOFF,
        )
        return registry, build_identity_adjudication_queue(registry)

    def _evidence_and_review(
        self,
        *,
        registry,
        queue,
        case,
        default_isin: str | None,
        claim_isins: dict[str, str | None] | None = None,
        claim_listings: dict[str, tuple[str, str]] | None = None,
    ):
        source = self.root / f"CML-{case.candidate_id[:8]}.pdf"
        declaration = self.root / f"CML-{case.candidate_id[:8]}.evidence.json"
        source.write_bytes(PDF_BYTES)
        observations = {
            value.observation_id: value for value in registry.observations
        }
        first_observation = observations[case.observation_ids[0]]
        claims = []
        for requirement in case.requirements:
            symbol, series = (claim_listings or {}).get(
                requirement.value,
                (
                    first_observation.ticker_symbol,
                    first_observation.security_series,
                ),
            )
            claims.append(
                {
                    "candidate_id": case.candidate_id,
                    "requirement": requirement.value,
                    "effective_date": (
                        "2026-07-15"
                        if requirement.value
                        in {
                            "OFFICIAL_LISTING_LIFECYCLE",
                            "OFFICIAL_LISTING_STATUS",
                        }
                        else None
                    ),
                    "symbol": symbol,
                    "series": series,
                    "isin": (claim_isins or {}).get(
                        requirement.value,
                        default_isin,
                    ),
                    "locator": {
                        "page": 1,
                        "row": None,
                        "section": requirement.value,
                    },
                    "claim_text": (
                        f"Reviewed source statement for {requirement.value}."
                    ),
                }
            )
        declaration.write_text(
            json.dumps(
                {
                    "schema_version": (
                        IDENTITY_EVIDENCE_DECLARATION_SCHEMA_VERSION
                    ),
                    "exchange": "NSE",
                    "segment": "CM",
                    "claimed_authority": "NSE",
                    "source_kind": "LISTING_CIRCULAR_PDF",
                    "claimed_document_id": (
                        f"NSE/LIST/C/2026/{case.candidate_id[:8].upper()}"
                    ),
                    "claimed_issue_date": "2026-07-15",
                    "claimed_publication_at": None,
                    "claimed_source_url": (
                        "https://nsearchives.nseindia.com/content/circulars/"
                        f"{source.name}"
                    ),
                    "source_filename": source.name,
                    "source_media_type": "application/pdf",
                    "source_byte_count": len(PDF_BYTES),
                    "source_sha256": hashlib.sha256(PDF_BYTES).hexdigest(),
                    "claims": claims,
                }
            ),
            encoding="utf-8",
        )
        evidence = LocalIdentityEvidenceArtifactStore(
            self.root / "evidence",
            clock=clock_sequence(EVIDENCE_FIRST, EVIDENCE_VALIDATED),
        ).import_source(source, declaration)
        claims_by_requirement = {
            value.requirement: value for value in evidence.parsed.claims
        }
        review_path = self.root / f"review-{case.candidate_id[:8]}.json"
        review_path.write_text(
            json.dumps(
                {
                    "schema_version": IDENTITY_REVIEW_DECLARATION_SCHEMA_VERSION,
                    "queue_id": queue.queue_id,
                    "source_registry_id": registry.registry_id,
                    "reviewer_id": "owner:kamal",
                    "reviewed_at": REVIEWED_AT.isoformat(),
                    "decisions": [
                        {
                            "candidate_id": case.candidate_id,
                            "requirement": requirement.value,
                            "outcome": "ACCEPTED",
                            "evidence_artifact_id": (
                                evidence.manifest.artifact_id
                            ),
                            "evidence_claim_id": (
                                claims_by_requirement[requirement].claim_id
                            ),
                            "rationale": (
                                f"Reviewed exact evidence for {requirement.value}."
                            ),
                        }
                        for requirement in case.requirements
                    ],
                }
            ),
            encoding="utf-8",
        )
        review = LocalIdentityReviewBundleStore(
            self.root / "evidence",
            clock=clock_sequence(REVIEW_FIRST, REVIEW_VALIDATED),
        ).import_declaration(review_path)
        return evidence, review

    def _materialize(self, registry, queue, evidence, review):
        return materialize_adjudicated_identity_snapshot(
            registry=registry,
            queue=queue,
            evidence_artifacts=(evidence,),
            review_bundles=(review,),
            cutoff=SNAPSHOT_CUTOFF,
        )

    def test_reviewed_unvalidated_identifier_assigns_claimed_isin(self) -> None:
        registry, queue = self._registry_and_queue(
            [security_row(ISIN="DUMMY1594")]
        )
        case = queue.cases[0]
        evidence, review = self._evidence_and_review(
            registry=registry,
            queue=queue,
            case=case,
            default_isin="INE009A01021",
        )
        snapshot = self._materialize(registry, queue, evidence, review)

        self.assertEqual(
            snapshot.policy_version,
            ADJUDICATED_IDENTITY_POLICY_VERSION,
        )
        self.assertTrue(snapshot.stable_identity_assigned)
        self.assertEqual(
            {value.isin for value in snapshot.listing_observations},
            {"INE009A01021"},
        )
        self.assertEqual(snapshot.resolutions[0].blocker_codes, ())

    def test_accepted_identifier_claim_requires_an_isin(self) -> None:
        registry, queue = self._registry_and_queue(
            [security_row(ISIN="DUMMY1594")]
        )
        case = queue.cases[0]
        evidence, review = self._evidence_and_review(
            registry=registry,
            queue=queue,
            case=case,
            default_isin=None,
        )
        with self.assertRaisesRegex(
            IdentityDecisionIntegrityError,
            "does not contain an ISIN",
        ):
            self._materialize(registry, queue, evidence, review)

    def test_identifier_claim_must_target_the_source_listing(self) -> None:
        registry, queue = self._registry_and_queue(
            [security_row(ISIN="DUMMY1594")]
        )
        case = queue.cases[0]
        evidence, review = self._evidence_and_review(
            registry=registry,
            queue=queue,
            case=case,
            default_isin="INE009A01021",
            claim_listings={"VALIDATED_IDENTIFIER": ("TCS", "EQ")},
        )
        with self.assertRaisesRegex(
            IdentityDecisionIntegrityError,
            "another listing",
        ):
            self._materialize(registry, queue, evidence, review)

    def test_all_accepted_non_null_isin_claims_must_agree(self) -> None:
        registry, queue = self._registry_and_queue(
            [security_row(ISIN="DUMMY1594")]
        )
        case = queue.cases[0]
        evidence, review = self._evidence_and_review(
            registry=registry,
            queue=queue,
            case=case,
            default_isin="INE009A01021",
            claim_isins={
                "AUTHORIZED_SOURCE_PROVENANCE": "INE009A01039",
            },
        )
        with self.assertRaisesRegex(
            IdentityDecisionIntegrityError,
            "conflicting ISIN",
        ):
            self._materialize(registry, queue, evidence, review)

    def test_reviewed_validated_conflict_can_assign_stable_identity(self) -> None:
        registry, queue = self._registry_and_queue(
            [
                security_row(),
                security_row(FinInstrmId="2000", TckrSymb="INFYALT"),
            ]
        )
        case = queue.cases[0]
        evidence, review = self._evidence_and_review(
            registry=registry,
            queue=queue,
            case=case,
            default_isin="INE009A01021",
        )
        snapshot = self._materialize(registry, queue, evidence, review)

        self.assertTrue(snapshot.stable_identity_assigned)
        self.assertEqual(len(snapshot.listing_observations), 2)
        self.assertEqual(snapshot.resolutions[0].blocker_codes, ())

    def test_unvalidated_conflict_stays_unsupported_without_identifier_review(
        self,
    ) -> None:
        registry, queue = self._registry_and_queue(
            [security_row(ISIN="DUMMY1594")],
            [
                security_row(
                    TckrSymb="INFYALT",
                    ISIN="INE009A01021",
                )
            ],
        )
        candidate = next(
            value
            for value in registry.candidates
            if value.basis
            is IdentityCandidateBasis.UNVALIDATED_SOURCE_IDENTIFIER
        )
        case = next(
            value for value in queue.cases if value.candidate_id == candidate.candidate_id
        )
        evidence, review = self._evidence_and_review(
            registry=registry,
            queue=queue,
            case=case,
            default_isin="INE009A01021",
        )
        snapshot = self._materialize(registry, queue, evidence, review)
        resolution = next(
            value
            for value in snapshot.resolutions
            if value.candidate_id == candidate.candidate_id
        )

        self.assertIn(
            IdentityResolutionBlocker.UNSUPPORTED_CANDIDATE_SHAPE,
            resolution.blocker_codes,
        )
        self.assertIsNone(resolution.stable_instrument_id)


if __name__ == "__main__":
    unittest.main()
