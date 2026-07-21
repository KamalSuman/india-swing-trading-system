from __future__ import annotations

import tempfile
import unittest
from datetime import date, datetime, timezone
from pathlib import Path

from india_swing.paper_outcomes import (
    LocalPaperPortfolioBatchStore,
    LocalPaperPortfolioStateStore,
    PaperPortfolioPreparationError,
    PaperPortfolioPreparationSpec,
    PaperRegistrationListing,
    decode_paper_portfolio_preparation_spec,
    encode_paper_portfolio_preparation_spec,
    prepare_paper_portfolio_batch,
)
from india_swing.paper_trades import LocalPaperTradeLedger, PaperTradeIntegrityError
from tests.test_paper_outcome_operational import _EvidenceSource, _evidence, _spec
from tests.test_paper_outcomes import _calendar, _observation


UTC = timezone.utc


class PaperPortfolioPreparationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.ledger = LocalPaperTradeLedger(self.root / "paper")
        self.portfolio_store = LocalPaperPortfolioStateStore(
            self.root / "paper_portfolio"
        )
        calendar = _calendar()
        self.evidence = _evidence((_observation(calendar, date(2026, 1, 2)),))
        self.ledger.register_value(self.evidence.registration)
        self.as_of = datetime(2026, 1, 3, tzinfo=UTC)
        self.template_job = _spec(self.evidence, as_of=self.as_of)
        self.listing = PaperRegistrationListing(
            registration_id=self.evidence.registration.registration_id,
            tick_snapshot_id=self.template_job.tick_snapshot_id,
            series=self.template_job.series,
            validated_isin=self.template_job.validated_isin,
        )

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def _preparation(self, listings=None):
        return PaperPortfolioPreparationSpec(
            as_of=self.as_of,
            calendar_materialization_id=self.template_job.calendar_materialization_id,
            historical_artifact_ids=self.template_job.historical_artifact_ids,
            listings=(self.listing,) if listings is None else listings,
            policy=self.template_job.policy,
        )

    def test_prepares_and_stores_exact_batch_for_complete_active_set(self) -> None:
        preparation = self._preparation()
        self.assertEqual(
            decode_paper_portfolio_preparation_spec(
                encode_paper_portfolio_preparation_spec(preparation)
            ),
            preparation,
        )
        source = _EvidenceSource(self.evidence)

        batch = prepare_paper_portfolio_batch(
            spec=preparation,
            ledger=self.ledger,
            evidence_source=source,
            portfolio_store=self.portfolio_store,
        )

        self.assertEqual(len(batch.outcome_jobs), 1)
        self.assertEqual(
            batch.outcome_jobs[0].registration_id,
            self.evidence.registration.registration_id,
        )
        self.assertEqual(batch.outcome_jobs[0].expected_replay_id, self.template_job.expected_replay_id)
        self.assertEqual(len(source.calls), 1)
        store = LocalPaperPortfolioBatchStore(self.root / "prepared")
        self.assertEqual(store.put(batch), batch)
        self.assertEqual(store.put(batch), batch)
        self.assertEqual(store.get(batch.batch_id), batch)

    def test_missing_active_listing_fails_before_evidence_read(self) -> None:
        preparation = self._preparation(listings=())
        source = _EvidenceSource(self.evidence)

        with self.assertRaisesRegex(PaperPortfolioPreparationError, "coverage"):
            prepare_paper_portfolio_batch(
                spec=preparation,
                ledger=self.ledger,
                evidence_source=source,
                portfolio_store=self.portfolio_store,
            )
        self.assertEqual(source.calls, [])

    def test_registration_enumeration_rejects_unknown_directory_entries(self) -> None:
        self.assertEqual(
            self.ledger.list_registrations(),
            (self.evidence.registration,),
        )
        (self.ledger.registrations_root / "untrusted.tmp").write_bytes(b"x")

        with self.assertRaises(PaperTradeIntegrityError):
            self.ledger.list_registrations()

    def test_noncanonical_preparation_is_rejected(self) -> None:
        payload = encode_paper_portfolio_preparation_spec(self._preparation())
        tampered = payload.replace(b'"series":"EQ"', b'"series":"BE"')
        with self.assertRaises(PaperPortfolioPreparationError):
            decode_paper_portfolio_preparation_spec(tampered)


if __name__ == "__main__":
    unittest.main()
