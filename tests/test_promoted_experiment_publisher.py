from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from india_swing.evaluation.dataset_assembly_store import (
    LocalEvaluationDatasetStore,
)
from india_swing.evaluation.promoted_experiment_evidence_store import (
    LocalPromotedExperimentReadinessEvidenceStore,
)
from india_swing.evaluation.promoted_experiment_publisher import (
    PromotedExperimentPublicationError,
    PromotedExperimentReadinessPublisher,
)
from india_swing.features.store import (
    LocalPromotedCrossSectionStore,
    LocalPromotedFeatureInputStore,
    LocalPromotedTechnicalFeatureStore,
)
from tests.test_promoted_experiment_assembly import _relaxed_config
from tests.test_promoted_experiment_evidence_store import _assembled


class _Resolver:
    def __init__(self, values, identity_name: str) -> None:
        self.values = {
            getattr(value, identity_name): value for value in values
        }

    def get(self, value_id: str):
        return self.values[value_id]


def _stores(root: Path):
    panel, plan, assembled, _, _ = _assembled(root / "inputs")
    source = panel.source_panel.source_panel
    feature_root = root / "features"
    input_store = LocalPromotedFeatureInputStore(
        feature_root,
        _Resolver((source.adjustment_panel,), "bridge_id"),
        _Resolver((source.tick_panel,), "panel_id"),
    )
    input_store.put(source)
    technical_store = LocalPromotedTechnicalFeatureStore(
        feature_root,
        input_store,
    )
    technical_store.put(panel.source_panel)
    cross_store = LocalPromotedCrossSectionStore(
        feature_root,
        technical_store,
    )
    cross_store.put(panel)
    dataset_store = LocalEvaluationDatasetStore(
        root / "evaluation"
    )
    dataset_store.put(assembled)
    evidence_store = (
        LocalPromotedExperimentReadinessEvidenceStore(
            root / "evaluation"
        )
    )
    return (
        panel,
        plan,
        assembled,
        dataset_store,
        cross_store,
        evidence_store,
    )


class PromotedExperimentReadinessPublisherTests(unittest.TestCase):
    def test_publishes_from_only_exact_persisted_artifact_ids(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            (
                panel,
                plan,
                assembled,
                dataset_store,
                cross_store,
                evidence_store,
            ) = _stores(Path(tmp))
            evidence = PromotedExperimentReadinessPublisher().publish(
                config=_relaxed_config(),
                split_plan=plan,
                dataset_assembly_id=assembled.assembly_id,
                cross_section_panel_ids=(panel.panel_id,),
                dataset_resolver=dataset_store,
                cross_section_resolver=cross_store,
                evidence_store=evidence_store,
            )
            restored = evidence_store.get(evidence.evidence_id)
            reaudited = evidence_store.reaudit(
                evidence_id=evidence.evidence_id,
                assembled_dataset=assembled,
                cross_section_resolver=cross_store,
            )
        self.assertEqual(restored, evidence)
        self.assertEqual(reaudited, evidence)
        self.assertEqual(
            evidence.dataset_assembly_id,
            assembled.assembly_id,
        )
        self.assertEqual(
            evidence.replay_projections[0].cross_section_panel_ids,
            (panel.panel_id,),
        )

    def test_missing_exact_cross_section_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            (
                _,
                plan,
                assembled,
                dataset_store,
                cross_store,
                evidence_store,
            ) = _stores(Path(tmp))
            with self.assertRaises(
                PromotedExperimentPublicationError
            ):
                PromotedExperimentReadinessPublisher().publish(
                    config=_relaxed_config(),
                    split_plan=plan,
                    dataset_assembly_id=assembled.assembly_id,
                    cross_section_panel_ids=("f" * 64,),
                    dataset_resolver=dataset_store,
                    cross_section_resolver=cross_store,
                    evidence_store=evidence_store,
                )

    def test_missing_exact_dataset_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            (
                panel,
                plan,
                _,
                dataset_store,
                cross_store,
                evidence_store,
            ) = _stores(Path(tmp))
            with self.assertRaises(
                PromotedExperimentPublicationError
            ):
                PromotedExperimentReadinessPublisher().publish(
                    config=_relaxed_config(),
                    split_plan=plan,
                    dataset_assembly_id="f" * 64,
                    cross_section_panel_ids=(panel.panel_id,),
                    dataset_resolver=dataset_store,
                    cross_section_resolver=cross_store,
                    evidence_store=evidence_store,
                )

    def test_duplicate_or_unsorted_panel_ids_are_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            (
                panel,
                plan,
                assembled,
                dataset_store,
                cross_store,
                evidence_store,
            ) = _stores(Path(tmp))
            for panel_ids in (
                (panel.panel_id, panel.panel_id),
                ("f" * 64, "e" * 64),
                (panel.panel_id, 1),
            ):
                with self.assertRaises(
                    PromotedExperimentPublicationError
                ):
                    PromotedExperimentReadinessPublisher().publish(
                        config=_relaxed_config(),
                        split_plan=plan,
                        dataset_assembly_id=assembled.assembly_id,
                        cross_section_panel_ids=panel_ids,
                        dataset_resolver=dataset_store,
                        cross_section_resolver=cross_store,
                        evidence_store=evidence_store,
                    )

    def test_publisher_exposes_only_publish(self) -> None:
        public = {
            value
            for value in dir(PromotedExperimentReadinessPublisher)
            if not value.startswith("_")
        }
        self.assertEqual(public, {"publish"})


if __name__ == "__main__":
    unittest.main()
