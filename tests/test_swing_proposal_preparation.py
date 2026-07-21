from __future__ import annotations

import io
import json
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from dataclasses import replace
from pathlib import Path

from india_swing import proposal_prepare
from india_swing.reference.models import ReferenceReadiness
from india_swing.signals import (
    LocalSwingProposalBatchStore,
    LocalSwingProposalParentStore,
    LocalSwingProposalPreparationStore,
    SwingProposalPreparationError,
    build_swing_proposal_preparation_spec,
    decode_swing_proposal_preparation_spec,
    encode_swing_proposal_preparation_spec,
    prepare_stored_swing_proposal_graph,
)
from tests import test_swing_opportunity_ranking as ranking_fixtures


class SwingProposalPreparationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        fixture = ranking_fixtures.SwingOpportunityRankingBatchTests(
            methodName="test_ranks_every_pass_with_explainable_bounded_components"
        )
        fixture.setUp()
        cls.batch = fixture.fixture.proposal_batch

    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.parent_store = LocalSwingProposalParentStore(self.root)
        self.proposal_store = LocalSwingProposalBatchStore(self.root)
        self.preparation_store = LocalSwingProposalPreparationStore(self.root)
        self.parent_store.put_universe_batch(self.batch.universe_batch)
        self.parent_store.put_calendar_snapshot(self.batch.calendar)
        self.parent_store.put_signal_config(self.batch.config)
        self.spec = build_swing_proposal_preparation_spec(
            universe_batch=self.batch.universe_batch,
            calendar=self.batch.calendar,
            signal_config=self.batch.config,
        )

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def test_spec_binds_every_promoted_subject_and_round_trips_canonically(self) -> None:
        self.assertEqual(self.spec.expected_proposal_batch_id, self.batch.batch_id)
        self.assertEqual(
            self.spec.proposal_subject_count,
            self.batch.proposal_subject_count,
        )
        self.assertEqual(self.spec.veto_subject_count, self.batch.veto_subject_count)
        self.assertEqual(
            tuple(value.assembly_id for value in self.spec.subject_bindings),
            tuple(value.assembly_id for value in self.batch.universe_batch.assemblies),
        )
        self.assertEqual(
            tuple(value.promotion_decision_id for value in self.spec.subject_bindings),
            tuple(
                value.promotion_decision_id
                for value in self.batch.universe_batch.assemblies
            ),
        )
        payload = encode_swing_proposal_preparation_spec(self.spec)
        self.assertEqual(decode_swing_proposal_preparation_spec(payload), self.spec)
        with self.assertRaises(SwingProposalPreparationError):
            decode_swing_proposal_preparation_spec(b" " + payload)
        duplicate = payload.replace(
            b'{"codec_schema_version":',
            b'{"codec_schema_version":"duplicate","codec_schema_version":',
            1,
        )
        with self.assertRaises(SwingProposalPreparationError):
            decode_swing_proposal_preparation_spec(duplicate)

    def test_exact_stored_inputs_publish_terminal_manifest_idempotently(self) -> None:
        first = prepare_stored_swing_proposal_graph(
            spec=self.spec,
            parent_store=self.parent_store,
            proposal_store=self.proposal_store,
            preparation_store=self.preparation_store,
        )
        second = prepare_stored_swing_proposal_graph(
            spec=self.spec,
            parent_store=self.parent_store,
            proposal_store=self.proposal_store,
            preparation_store=self.preparation_store,
        )

        self.assertEqual(second, first)
        self.assertEqual(first.proposal_batch_id, self.batch.batch_id)
        self.assertEqual(
            self.proposal_store.load(first.proposal_batch_id, self.parent_store),
            self.batch,
        )
        self.assertEqual(
            self.preparation_store.get(self.spec.preparation_id), self.spec
        )

    def test_wrong_expected_batch_fails_before_preparation_or_manifest(self) -> None:
        invalid = replace(self.spec, expected_proposal_batch_id="0" * 64)
        with self.assertRaisesRegex(SwingProposalPreparationError, "differ"):
            prepare_stored_swing_proposal_graph(
                spec=invalid,
                parent_store=self.parent_store,
                proposal_store=self.proposal_store,
                preparation_store=self.preparation_store,
            )
        self.assertFalse(
            self.preparation_store.path_for(invalid.preparation_id).exists()
        )
        self.assertFalse(self.proposal_store.path_for(self.batch.batch_id).exists())

    def test_missing_parent_fails_closed_without_terminal_writes(self) -> None:
        empty = self.root / "empty"
        empty.mkdir()
        preparations = LocalSwingProposalPreparationStore(empty)
        proposals = LocalSwingProposalBatchStore(empty)
        with self.assertRaisesRegex(SwingProposalPreparationError, "loaded safely"):
            prepare_stored_swing_proposal_graph(
                spec=self.spec,
                parent_store=LocalSwingProposalParentStore(empty),
                proposal_store=proposals,
                preparation_store=preparations,
            )
        self.assertFalse(preparations.specifications_root.exists())
        self.assertFalse(proposals.path_for(self.batch.batch_id).exists())

    def test_collection_only_and_tampered_subject_binding_are_rejected(self) -> None:
        with self.assertRaisesRegex(SwingProposalPreparationError, "collection-only"):
            replace(self.spec, readiness=ReferenceReadiness.COLLECTION_ONLY)
        original = self.spec.subject_bindings[0].assembly_id
        object.__setattr__(self.spec.subject_bindings[0], "assembly_id", "0" * 64)
        try:
            with self.assertRaisesRegex(SwingProposalPreparationError, "identity"):
                self.spec.verify_content_identity()
        finally:
            object.__setattr__(self.spec.subject_bindings[0], "assembly_id", original)

    def test_cli_prepares_exact_graph_and_sanitizes_failures(self) -> None:
        spec_file = self.root / "proposal-preparation.json"
        spec_file.write_bytes(encode_swing_proposal_preparation_spec(self.spec))
        output = io.StringIO()
        with redirect_stdout(output):
            code = proposal_prepare.main(
                ("--spec-file", str(spec_file), "--graph-root", str(self.root))
            )
        result = json.loads(output.getvalue())
        self.assertEqual(code, 0)
        self.assertEqual(result["proposal_batch_id"], self.batch.batch_id)
        self.assertEqual(result["proposal_subject_count"], self.batch.proposal_subject_count)
        self.assertTrue(result["research_only"])
        exact_output = io.StringIO()
        with redirect_stdout(exact_output):
            code = proposal_prepare.main(
                (
                    "--graph-root", str(self.root),
                    "--universe-batch-id", self.batch.universe_batch.batch_id,
                    "--calendar-snapshot-id", self.batch.calendar.snapshot_id,
                    "--signal-config-id", self.batch.config.config_id,
                )
            )
        self.assertEqual(code, 0)
        self.assertEqual(
            json.loads(exact_output.getvalue())["preparation_id"],
            self.spec.preparation_id,
        )

        failure = io.StringIO()
        secret = "private-proposal-spec"
        with redirect_stderr(failure):
            code = proposal_prepare.main(("--spec-file", secret))
        self.assertEqual(code, 2)
        self.assertNotIn(secret, failure.getvalue())
        self.assertEqual(
            json.loads(failure.getvalue()),
            {"error_type": "SwingProposalPreparationError", "status": "FAILED"},
        )

    def test_preparation_store_has_no_listing_or_latest_selection(self) -> None:
        names = {value.casefold() for value in dir(self.preparation_store)}
        self.assertFalse(any("latest" in value for value in names))
        self.assertFalse(any(value.startswith("list") for value in names))
        module_names = set(proposal_prepare.__dict__)
        for capability in ("KiteConnect", "place_order", "modify_order", "cancel_order"):
            self.assertNotIn(capability, module_names)


if __name__ == "__main__":
    unittest.main()
