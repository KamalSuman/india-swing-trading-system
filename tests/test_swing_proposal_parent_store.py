from __future__ import annotations

import io
import json
import tempfile
import unittest
from contextlib import redirect_stdout
from dataclasses import replace
from pathlib import Path

from india_swing.operations import build_stored_swing_operational_run_spec
from india_swing.operations.cli import main as operational_cli
from india_swing.signals.proposal_artifacts import LocalSwingProposalBatchStore
from india_swing.signals.proposal_parent_store import (
    LocalSwingProposalParentStore,
    SwingProposalParentKind,
    SwingProposalParentNotFound,
    SwingProposalParentStoreError,
    decode_swing_proposal_parent,
    encode_swing_proposal_parent,
    publish_swing_proposal_with_parents,
)

from tests import test_swing_opportunity_ranking as ranking_fixtures


class SwingProposalParentStoreTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        fixture = ranking_fixtures.SwingOpportunityRankingBatchTests(
            methodName="test_ranks_every_pass_with_explainable_bounded_components"
        )
        fixture.setUp()
        cls.batch = fixture.fixture.proposal_batch

    def test_each_approved_root_round_trips_canonically(self) -> None:
        roots = (
            (
                SwingProposalParentKind.UNIVERSE_BATCH,
                self.batch.universe_batch,
            ),
            (
                SwingProposalParentKind.CALENDAR_SNAPSHOT,
                self.batch.calendar,
            ),
            (
                SwingProposalParentKind.SIGNAL_CONFIG,
                self.batch.config,
            ),
        )
        for kind, value in roots:
            with self.subTest(kind=kind):
                payload = encode_swing_proposal_parent(value, kind)
                decoded = decode_swing_proposal_parent(payload, kind)
                self.assertEqual(type(decoded), type(value))
                self.assertEqual(decoded, value)
                self.assertEqual(encode_swing_proposal_parent(decoded, kind), payload)
                decoded.verify_content_identity()

    def test_fresh_store_instances_rebuild_the_exact_proposal_and_spec(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            proposal_store = LocalSwingProposalBatchStore(root / "proposals")
            parent_store = LocalSwingProposalParentStore(root / "parents")
            manifest = publish_swing_proposal_with_parents(
                batch=self.batch,
                proposal_store=proposal_store,
                parent_store=parent_store,
            )

            fresh_proposals = LocalSwingProposalBatchStore(root / "proposals")
            fresh_parents = LocalSwingProposalParentStore(root / "parents")
            rebuilt = fresh_proposals.load(self.batch.batch_id, fresh_parents)
            spec = build_stored_swing_operational_run_spec(
                proposal_batch_id=self.batch.batch_id,
                proposal_store=fresh_proposals,
                proposal_resolver=fresh_parents,
                quote_chunk_size=1,
            )

            self.assertEqual(manifest.proposal_batch_id, self.batch.batch_id)
            self.assertEqual(rebuilt, self.batch)
            self.assertEqual(spec.proposal_batch, self.batch)
            self.assertEqual(
                fresh_parents.get_universe_batch(self.batch.universe_batch.batch_id),
                self.batch.universe_batch,
            )
            self.assertEqual(
                fresh_parents.get_calendar_snapshot(self.batch.calendar.snapshot_id),
                self.batch.calendar,
            )
            self.assertEqual(
                fresh_parents.get_signal_config(self.batch.config.config_id),
                self.batch.config,
            )

    def test_publication_is_idempotent_and_manifest_is_written_after_parents(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            proposal_store = LocalSwingProposalBatchStore(root / "proposals")
            parent_store = LocalSwingProposalParentStore(root / "parents")
            first = publish_swing_proposal_with_parents(
                batch=self.batch,
                proposal_store=proposal_store,
                parent_store=parent_store,
            )
            second = publish_swing_proposal_with_parents(
                batch=self.batch,
                proposal_store=proposal_store,
                parent_store=parent_store,
            )

            self.assertEqual(first, second)
            for kind, root_id in (
                (
                    SwingProposalParentKind.UNIVERSE_BATCH,
                    first.universe_batch_id,
                ),
                (
                    SwingProposalParentKind.CALENDAR_SNAPSHOT,
                    first.calendar_snapshot_id,
                ),
                (
                    SwingProposalParentKind.SIGNAL_CONFIG,
                    first.signal_config_id,
                ),
            ):
                self.assertTrue(parent_store.path_for(kind, root_id).is_file())
            self.assertTrue(proposal_store.path_for(first.proposal_batch_id).is_file())

    def test_cli_verify_replays_fresh_stores_without_market_capabilities(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            proposal_root = root / "proposals"
            parent_root = root / "parents"
            publish_swing_proposal_with_parents(
                batch=self.batch,
                proposal_store=LocalSwingProposalBatchStore(proposal_root),
                parent_store=LocalSwingProposalParentStore(parent_root),
            )
            output = io.StringIO()
            with redirect_stdout(output):
                code = operational_cli(
                    [
                        "--proposal-root",
                        str(proposal_root),
                        "--parent-root",
                        str(parent_root),
                        "proposal-verify",
                        "--proposal-batch-id",
                        self.batch.batch_id,
                    ]
                )
            value = json.loads(output.getvalue())
            self.assertEqual(code, 0)
            self.assertEqual(value["proposal_batch_id"], self.batch.batch_id)
            self.assertEqual(
                value["universe_batch_id"],
                self.batch.universe_batch.batch_id,
            )
            self.assertNotIn("bars", value)
            self.assertNotIn("api_key", value)

    def test_decoder_rejects_unknown_type_extra_field_float_and_computed_id_tamper(self) -> None:
        kind = SwingProposalParentKind.UNIVERSE_BATCH
        payload = encode_swing_proposal_parent(self.batch.universe_batch, kind)
        raw = json.loads(payload)
        invalid: list[bytes] = []

        unknown_type = json.loads(payload)
        unknown_type["value"]["$dataclass"] = "builtins.dict"
        invalid.append(json.dumps(unknown_type, separators=(",", ":"), sort_keys=True).encode())

        extra = json.loads(payload)
        extra["value"]["fields"]["unexpected"] = True
        invalid.append(json.dumps(extra, separators=(",", ":"), sort_keys=True).encode())

        floating = json.loads(payload)
        floating["value"]["fields"]["scoped_subject_count"] = 1.0
        invalid.append(json.dumps(floating, separators=(",", ":"), sort_keys=True).encode())

        wrong_computed_id = json.loads(payload)
        wrong_computed_id["value"]["fields"]["batch_id"] = "0" * 64
        invalid.append(
            json.dumps(wrong_computed_id, separators=(",", ":"), sort_keys=True).encode()
        )

        duplicate = payload.replace(
            b'{"codec_schema_version":',
            b'{"kind":"universe-batches","codec_schema_version":',
            1,
        )
        invalid.append(duplicate)
        invalid.append(b" " + payload)

        self.assertEqual(raw["root_id"], self.batch.universe_batch.batch_id)
        for value in invalid:
            with self.subTest(value=value[:80]):
                with self.assertRaises(SwingProposalParentStoreError):
                    decode_swing_proposal_parent(value, kind)

    def test_decoder_rejects_cross_kind_payload_and_unapproved_root(self) -> None:
        payload = encode_swing_proposal_parent(
            self.batch.calendar,
            SwingProposalParentKind.CALENDAR_SNAPSHOT,
        )
        with self.assertRaises(SwingProposalParentStoreError):
            decode_swing_proposal_parent(
                payload,
                SwingProposalParentKind.SIGNAL_CONFIG,
            )
        with self.assertRaises(SwingProposalParentStoreError):
            encode_swing_proposal_parent(
                self.batch,
                SwingProposalParentKind.UNIVERSE_BATCH,
            )

    def test_nested_content_mutation_is_rejected_before_publication(self) -> None:
        mutated = replace(
            self.batch.universe_batch,
            scoped_subject_count=self.batch.universe_batch.scoped_subject_count,
        )
        original = mutated.current_universe.snapshot_id
        object.__setattr__(mutated.current_universe, "snapshot_id", "0" * 64)
        try:
            with tempfile.TemporaryDirectory() as directory:
                store = LocalSwingProposalParentStore(Path(directory))
                with self.assertRaises(SwingProposalParentStoreError):
                    store.put_universe_batch(mutated)
        finally:
            object.__setattr__(mutated.current_universe, "snapshot_id", original)

    def test_store_rejects_missing_unsafe_ids_tamper_and_no_latest_capability(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            store = LocalSwingProposalParentStore(Path(directory))
            with self.assertRaises(SwingProposalParentNotFound):
                store.get_signal_config("0" * 64)
            for value in ("../escape", "A" * 64, "0" * 63, object()):
                with self.subTest(value=value):
                    with self.assertRaises(SwingProposalParentStoreError):
                        store.path_for(SwingProposalParentKind.SIGNAL_CONFIG, value)
            stored = store.put_signal_config(self.batch.config)
            path = store.path_for(SwingProposalParentKind.SIGNAL_CONFIG, stored.config_id)
            path.write_bytes(path.read_bytes().replace(stored.config_id.encode(), b"0" * 64))
            with self.assertRaises(SwingProposalParentStoreError):
                store.get_signal_config(stored.config_id)

            self.assertFalse(any("latest" in name.casefold() for name in dir(store)))
            self.assertFalse(any(name.startswith("list") for name in dir(store)))


if __name__ == "__main__":
    unittest.main()
