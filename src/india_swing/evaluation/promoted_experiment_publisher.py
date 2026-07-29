"""Exact-artifact publisher for promoted experiment readiness evidence."""

from __future__ import annotations

import re
from typing import Protocol

from india_swing.evaluation.dataset_assembly import (
    AssembledEvaluationDataset,
)
from india_swing.evaluation.models import PurgedWalkForwardPlan
from india_swing.evaluation.promoted_experiment_assembly import (
    ExactCrossSectionResolver,
    PromotedExperimentReadinessConfig,
)
from india_swing.evaluation.promoted_experiment_evidence_store import (
    LocalPromotedExperimentReadinessEvidenceStore,
    PromotedExperimentReadinessEvidence,
)
from india_swing.features.historical_replay import (
    reconstruct_promoted_historical_replay_run,
)
from india_swing.features.promoted_cross_section import (
    VerifiedPromotedCrossSectionPanel,
)


_SHA256 = re.compile(r"[0-9a-f]{64}\Z")


class PromotedExperimentPublicationError(ValueError):
    pass


class ExactEvaluationDatasetResolver(Protocol):
    def get(
        self,
        assembly_id: str,
    ) -> AssembledEvaluationDataset: ...


class PromotedExperimentReadinessPublisher:
    """Publishes one audit from explicitly selected durable artifacts."""

    def publish(
        self,
        *,
        config: PromotedExperimentReadinessConfig,
        split_plan: PurgedWalkForwardPlan,
        dataset_assembly_id: str,
        cross_section_panel_ids: tuple[str, ...],
        dataset_resolver: ExactEvaluationDatasetResolver,
        cross_section_resolver: ExactCrossSectionResolver,
        evidence_store: LocalPromotedExperimentReadinessEvidenceStore,
    ) -> PromotedExperimentReadinessEvidence:
        if (
            type(config) is not PromotedExperimentReadinessConfig
            or type(split_plan) is not PurgedWalkForwardPlan
            or type(dataset_assembly_id) is not str
            or _SHA256.fullmatch(dataset_assembly_id) is None
            or type(cross_section_panel_ids) is not tuple
            or not cross_section_panel_ids
            or any(
                type(value) is not str
                or _SHA256.fullmatch(value) is None
                for value in cross_section_panel_ids
            )
            or cross_section_panel_ids
            != tuple(sorted(set(cross_section_panel_ids)))
            or not callable(getattr(dataset_resolver, "get", None))
            or not callable(
                getattr(cross_section_resolver, "get", None)
            )
            or type(evidence_store)
            is not LocalPromotedExperimentReadinessEvidenceStore
        ):
            raise PromotedExperimentPublicationError(
                "promoted experiment publication input is invalid"
            )
        try:
            assembled = dataset_resolver.get(dataset_assembly_id)
            if (
                type(assembled) is not AssembledEvaluationDataset
                or assembled.assembly_id != dataset_assembly_id
            ):
                raise PromotedExperimentPublicationError(
                    "promoted experiment dataset resolution differs"
                )
            assembled.verify_content_identity()
            panels = tuple(
                cross_section_resolver.get(panel_id)
                for panel_id in cross_section_panel_ids
            )
            if any(
                type(panel) is not VerifiedPromotedCrossSectionPanel
                or panel.panel_id != panel_id
                for panel, panel_id in zip(
                    panels,
                    cross_section_panel_ids,
                )
            ):
                raise PromotedExperimentPublicationError(
                    "promoted experiment cross-section resolution differs"
                )
            replay = reconstruct_promoted_historical_replay_run(
                panels
            )
            expected_panel_ids = tuple(
                panel.panel_id
                for panel in sorted(
                    panels,
                    key=lambda value: (
                        value.source_panel.source_panel.adjustment_panel
                        .signal_session
                    ),
                )
            )
            evidence = evidence_store.publish(
                config=config,
                split_plan=split_plan,
                assembled_dataset=assembled,
                replay_runs=(replay,),
                cross_section_resolver=cross_section_resolver,
            )
        except PromotedExperimentPublicationError:
            raise
        except Exception:
            raise PromotedExperimentPublicationError(
                "promoted experiment publication failed"
            ) from None
        if (
            type(evidence) is not PromotedExperimentReadinessEvidence
            or evidence.dataset_assembly_id != dataset_assembly_id
            or evidence.split_plan.plan_id != split_plan.plan_id
            or evidence.config.config_id != config.config_id
            or evidence.replay_projections[0].cross_section_panel_ids
            != expected_panel_ids
        ):
            raise PromotedExperimentPublicationError(
                "promoted experiment publication output differs"
            )
        return evidence
