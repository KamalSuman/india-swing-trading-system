from __future__ import annotations

import io
import json
import tempfile
import unittest
from dataclasses import replace
from datetime import datetime, time, timezone
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch

from india_swing.evaluation.cli import main as evaluation_cli_main
from india_swing.evaluation.dataset_assembly import (
    AssembledEvaluationDataset,
    EffectiveTickSize,
    EvaluationSessionEvidence,
)
from india_swing.evaluation.promoted_experiment_evidence_store import (
    LocalPromotedExperimentReadinessEvidenceStore,
    PromotedExperimentEvidenceConflict,
    PromotedExperimentEvidenceStoreError,
    PromotedHistoricalReplayProjection,
    decode_promoted_experiment_readiness_evidence,
    encode_promoted_experiment_readiness_evidence,
)
from india_swing.evaluation.promoted_experiment_assembly import (
    PromotedExperimentReadinessIssueCode,
)
from india_swing.identity import content_id
from india_swing.reference.models import ReferenceReadiness
from tests.test_promoted_experiment_assembly import (
    _evidence,
    _relaxed_config,
)


def _id(label: str) -> str:
    return content_id(
        {
            "schema": "promoted-readiness-store-test/v1",
            "label": label,
        },
        length=64,
    )


def _assembled(root: Path):
    panel, plan, dataset, instruments, replay, resolver = _evidence(root)
    universe_by_session = {
        session: _id(f"universe:{session.isoformat()}")
        for session in dataset.sessions
    }
    dataset = replace(
        dataset,
        universe_snapshot_ids=tuple(
            sorted(universe_by_session.values())
        ),
    )
    instrument = replace(
        instruments[0],
        universe_snapshot_id=universe_by_session[
            dataset.sessions[0]
        ],
        stable_instrument_id="stable-reliance",
        eligibility_bindings=tuple(
            (
                session,
                universe_by_session[session],
            )
            for session in dataset.sessions
        ),
    )
    tick = EffectiveTickSize(
        instrument_id="stable-reliance",
        listing_id="listing-reliance",
        effective_from_session=dataset.sessions[0],
        effective_to_exclusive=None,
        tick_size=Decimal("0.05"),
        knowledge_time=datetime.combine(
            dataset.sessions[0],
            time(0),
            tzinfo=timezone.utc,
        ),
        source_snapshot_id=_id("tick-source"),
        readiness=ReferenceReadiness.SYNTHETIC_TEST,
    )
    session_evidence = tuple(
        EvaluationSessionEvidence(
            market_session=session,
            calendar_snapshot_id=_id(
                f"calendar:{session.isoformat()}"
            ),
            universe_snapshot_id=universe_by_session[session],
            price_snapshot_id=_id(
                f"price:{session.isoformat()}"
            ),
            price_source_artifact_id=_id(
                f"artifact:{session.isoformat()}"
            ),
            price_source_snapshot_ids=(
                _id(f"price-source:{session.isoformat()}"),
            ),
            cutoff=datetime.combine(
                session,
                time(18),
                tzinfo=timezone.utc,
            ),
            actionable_listing_ids=("listing-reliance",),
            explicit_nontrading_listing_ids=(),
            tick_size_specification_ids=(tick.specification_id,),
        )
        for session in dataset.sessions
    )
    assembled = AssembledEvaluationDataset(
        dataset=dataset,
        instruments=(instrument,),
        session_evidence=session_evidence,
        tick_sizes=(tick,),
    )
    return panel, plan, assembled, replay, resolver


class PromotedHistoricalReplayProjectionTests(unittest.TestCase):
    def test_projection_recomputes_the_original_run_identity(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            _, _, _, replay, resolver = _assembled(Path(tmp))
            projection = PromotedHistoricalReplayProjection.from_run(
                replay
            )
        projection.verify_content_identity()
        self.assertEqual(projection.run_id, replay.run_id)
        self.assertEqual(
            projection.cross_section_panel_ids,
            tuple(
                value.cross_section_panel_id
                for value in replay.results
            ),
        )
        reconstructed = projection.reconstruct(resolver)
        self.assertEqual(reconstructed, replay)

    def test_projection_rejects_a_self_authored_run_id(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            _, _, _, replay, _ = _assembled(Path(tmp))
            projection = PromotedHistoricalReplayProjection.from_run(
                replay
            )
        with self.assertRaises(PromotedExperimentEvidenceStoreError):
            replace(projection, run_id="f" * 64)


class PromotedExperimentEvidenceCodecTests(unittest.TestCase):
    def _published(self, root: Path):
        _, plan, assembled, replay, resolver = _assembled(
            root / "inputs"
        )
        return LocalPromotedExperimentReadinessEvidenceStore(
            root / "store"
        ).publish(
            config=_relaxed_config(),
            split_plan=plan,
            assembled_dataset=assembled,
            replay_runs=(replay,),
            cross_section_resolver=resolver,
        )

    def test_canonical_round_trip_retains_complete_plan_and_report(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            evidence = self._published(Path(tmp))
            payload = encode_promoted_experiment_readiness_evidence(
                evidence
            )
            restored = (
                decode_promoted_experiment_readiness_evidence(payload)
            )
        self.assertTrue(payload.endswith(b"\n"))
        self.assertEqual(restored, evidence)
        self.assertEqual(
            restored.split_plan.ordered_sessions,
            evidence.split_plan.ordered_sessions,
        )
        self.assertEqual(restored.report, evidence.report)

    def test_decoder_rejects_duplicate_keys(self) -> None:
        payload = (
            b'{"store_schema_version":"x",'
            b'"store_schema_version":"y","evidence":{}}\n'
        )
        with self.assertRaises(PromotedExperimentEvidenceConflict):
            decode_promoted_experiment_readiness_evidence(payload)

    def test_decoder_rejects_float_tokens(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            evidence = self._published(Path(tmp))
            raw = json.loads(
                encode_promoted_experiment_readiness_evidence(evidence)
            )
            raw["evidence"]["report"][
                "instrument_count"
            ] = 1.5
            payload = json.dumps(raw).encode()
        with self.assertRaises(PromotedExperimentEvidenceConflict):
            decode_promoted_experiment_readiness_evidence(payload)

    def test_decoder_rejects_authority_upgrade(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            evidence = self._published(Path(tmp))
            raw = json.loads(
                encode_promoted_experiment_readiness_evidence(evidence)
            )
            raw["evidence"]["report"]["actionable"] = True
            payload = (
                json.dumps(
                    raw,
                    separators=(",", ":"),
                    sort_keys=True,
                )
                + "\n"
            ).encode()
        with self.assertRaises(PromotedExperimentEvidenceConflict):
            decode_promoted_experiment_readiness_evidence(payload)


class PromotedExperimentEvidenceStoreTests(unittest.TestCase):
    def test_publish_reruns_audit_and_is_idempotent(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _, plan, assembled, replay, resolver = _assembled(
                root / "inputs"
            )
            store = LocalPromotedExperimentReadinessEvidenceStore(
                root / "store"
            )
            first = store.publish(
                config=_relaxed_config(),
                split_plan=plan,
                assembled_dataset=assembled,
                replay_runs=(replay,),
                cross_section_resolver=resolver,
            )
            second = store.publish(
                config=_relaxed_config(),
                split_plan=plan,
                assembled_dataset=assembled,
                replay_runs=(replay,),
                cross_section_resolver=resolver,
            )
            restored = store.get(first.evidence_id)
            reaudited = store.reaudit(
                evidence_id=first.evidence_id,
                assembled_dataset=assembled,
                cross_section_resolver=resolver,
            )
        self.assertEqual(first, second)
        self.assertEqual(restored, first)
        self.assertEqual(reaudited, first)
        self.assertEqual(first.dataset_id, assembled.dataset.dataset_id)
        self.assertEqual(
            first.dataset_assembly_id,
            assembled.assembly_id,
        )
        self.assertFalse(first.report.actionable)
        self.assertFalse(first.report.execution_eligible)
        self.assertIn(
            PromotedExperimentReadinessIssueCode
            .CROSS_SECTION_BINDING_INVALID,
            {value.code for value in first.report.issues},
        )

    def test_tampered_file_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _, plan, assembled, replay, resolver = _assembled(
                root / "inputs"
            )
            store = LocalPromotedExperimentReadinessEvidenceStore(
                root / "store"
            )
            evidence = store.publish(
                config=_relaxed_config(),
                split_plan=plan,
                assembled_dataset=assembled,
                replay_runs=(replay,),
                cross_section_resolver=resolver,
            )
            path = store.path_for(evidence.evidence_id)
            raw = json.loads(path.read_bytes())
            raw["evidence"]["dataset_assembly_id"] = "f" * 64
            path.write_bytes(
                (
                    json.dumps(
                        raw,
                        separators=(",", ":"),
                        sort_keys=True,
                    )
                    + "\n"
                ).encode()
            )
            with self.assertRaises(
                PromotedExperimentEvidenceConflict
            ):
                store.get(evidence.evidence_id)

    def test_invalid_id_cannot_select_a_path(self) -> None:
        store = LocalPromotedExperimentReadinessEvidenceStore(
            Path("unused")
        )
        for value in ("latest", "../latest", "A" * 64, "0" * 63):
            with self.assertRaises(
                PromotedExperimentEvidenceStoreError
            ):
                store.path_for(value)

    def test_store_has_no_discovery_or_latest_operation(self) -> None:
        public = {
            value
            for value in dir(
                LocalPromotedExperimentReadinessEvidenceStore
            )
            if not value.startswith("_")
        }
        self.assertEqual(
            public,
            {
                "evidence_root",
                "get",
                "path_for",
                "publish",
                "reaudit",
            },
        )

    def test_cli_shows_only_the_exact_requested_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _, plan, assembled, replay, resolver = _assembled(
                root / "inputs"
            )
            evidence_root = root / "evaluation"
            store = LocalPromotedExperimentReadinessEvidenceStore(
                evidence_root
            )
            evidence = store.publish(
                config=_relaxed_config(),
                split_plan=plan,
                assembled_dataset=assembled,
                replay_runs=(replay,),
                cross_section_resolver=resolver,
            )
            output = io.StringIO()
            with patch.dict(
                "os.environ",
                {"INDIA_SWING_EVALUATION_ROOT": str(evidence_root)},
                clear=False,
            ), patch("sys.stdout", output):
                code = evaluation_cli_main(
                    [
                        "promoted-readiness",
                        "show",
                        "--evidence-id",
                        evidence.evidence_id,
                    ]
                )
        self.assertEqual(code, 0)
        self.assertIn(evidence.evidence_id, output.getvalue())
        self.assertIn(evidence.report.report_id, output.getvalue())
        self.assertIn("Offline evaluation ready", output.getvalue())


if __name__ == "__main__":
    unittest.main()
