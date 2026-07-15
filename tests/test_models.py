from __future__ import annotations

import unittest

from india_swing.domain.models import ResearchAssessment, ResearchVerdict


class ResearchAssessmentTests(unittest.TestCase):
    def test_approve_requires_nonempty_auditable_research(self) -> None:
        invalid_values = (
            {"thesis": ""},
            {"bear_case": ""},
            {"model_version": ""},
            {"evidence_ids": ()},
        )
        base = {
            "symbol": "TEST",
            "verdict": ResearchVerdict.APPROVE,
            "thesis": "Evidence supports the setup.",
            "bear_case": "The setup may fail if participation fades.",
            "risks": ("gap risk",),
            "evidence_ids": ("evidence-1",),
            "model_version": "research-v1",
            "instrument_id": "instrument-test",
            "listing_id": "listing-test",
            "universe_snapshot_id": "universe-1",
            "data_snapshot_id": "snapshot-1",
            "data_snapshot_fingerprint": "snapshot-fingerprint-1",
            "instrument_fingerprint": "instrument-fingerprint-1",
        }

        for overrides in invalid_values:
            with self.subTest(overrides=overrides):
                values = base | overrides
                with self.assertRaises(ValueError):
                    ResearchAssessment(**values)

    def test_duplicate_research_evidence_is_rejected(self) -> None:
        with self.assertRaisesRegex(ValueError, "must be unique"):
            ResearchAssessment(
                symbol="TEST",
                verdict=ResearchVerdict.APPROVE,
                thesis="Evidence supports the setup.",
                bear_case="The setup may fail.",
                risks=("gap risk",),
                evidence_ids=("evidence-1", "evidence-1"),
                model_version="research-v1",
                instrument_id="instrument-test",
                listing_id="listing-test",
                universe_snapshot_id="universe-1",
                data_snapshot_id="snapshot-1",
                data_snapshot_fingerprint="snapshot-fingerprint-1",
                instrument_fingerprint="instrument-fingerprint-1",
            )


if __name__ == "__main__":
    unittest.main()
