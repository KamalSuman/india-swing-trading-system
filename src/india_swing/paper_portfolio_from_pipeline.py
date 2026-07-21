from __future__ import annotations

import json
import sys
from collections.abc import Sequence
from pathlib import Path

from india_swing.calendar_data.materialization_store import LocalCalendarMaterializationStore
from india_swing.daily_pipeline.derived_evidence_store import LocalDailyDerivedEvidenceStore
from india_swing.daily_pipeline.store import LocalDailyPipelineRunStore
from india_swing.historical_prices import LocalHistoricalPriceArtifactStore
from india_swing.operations.job import validate_swing_operational_state_root
from india_swing.paper_outcomes import (
    LocalPaperPortfolioBatchStore,
    LocalPaperPortfolioPreparationStore,
    LocalPaperPortfolioStateStore,
    PaperPortfolioPipelineBridgeError,
    prepare_paper_portfolio_from_daily_pipeline,
)
from india_swing.paper_trades import LocalPaperTradeLedger
from india_swing.reference_data.artifact_store import LocalReferenceArtifactStore
from india_swing.tick_sizes import LocalTickSizeSnapshotStore


def _arguments(argv: Sequence[str]) -> dict[str, str]:
    allowed = {"--run-id", "--derived-evidence-id", "--evidence-root", "--state-root"}
    values: dict[str, str] = {}
    index = 0
    while index < len(argv):
        token = argv[index]
        if token not in allowed or token in values or index + 1 >= len(argv):
            raise PaperPortfolioPipelineBridgeError("invalid pipeline bridge arguments")
        value = argv[index + 1]
        if type(value) is not str or not value:
            raise PaperPortfolioPipelineBridgeError("invalid pipeline bridge arguments")
        values[token] = value
        index += 2
    if set(values) != allowed:
        raise PaperPortfolioPipelineBridgeError("pipeline bridge arguments are incomplete")
    return values


def main(argv: Sequence[str] | None = None) -> int:
    try:
        values = _arguments(list(argv) if argv is not None else sys.argv[1:])
        evidence_root = validate_swing_operational_state_root(
            Path(values["--evidence-root"])
        )
        state_root = validate_swing_operational_state_root(Path(values["--state-root"]))
        pipeline_root = evidence_root / "daily_pipeline"
        batch_store = LocalPaperPortfolioBatchStore(
            state_root / "paper_portfolio_batches"
        )
        preparation_store = LocalPaperPortfolioPreparationStore(
            state_root / "paper_portfolio_preparations"
        )
        result = prepare_paper_portfolio_from_daily_pipeline(
            run_id=values["--run-id"],
            derived_evidence_id=values["--derived-evidence-id"],
            ledger=LocalPaperTradeLedger(state_root / "paper"),
            run_store=LocalDailyPipelineRunStore(pipeline_root),
            derived_store=LocalDailyDerivedEvidenceStore(pipeline_root),
            calendar_store=LocalCalendarMaterializationStore(
                evidence_root / "calendar_data", evidence_root / "daily_reports"
            ),
            tick_store=LocalTickSizeSnapshotStore(
                evidence_root / "tick_sizes", evidence_root / "reference_data"
            ),
            historical_store=LocalHistoricalPriceArtifactStore(
                evidence_root / "historical_prices", evidence_root / "daily_reports"
            ),
            reference_store=LocalReferenceArtifactStore(
                evidence_root / "reference_data"
            ),
            portfolio_store=LocalPaperPortfolioStateStore(
                state_root / "paper_portfolio"
            ),
            preparation_store=preparation_store,
            batch_store=batch_store,
        )
        print(
            json.dumps(
                {
                    "batch_id": result.batch.batch_id,
                    "batch_spec_file": str(
                        batch_store.path_for(result.batch.batch_id).resolve()
                    ),
                    "derived_evidence_id": result.derived_evidence_id,
                    "outcome_job_count": len(result.batch.outcome_jobs),
                    "preparation_id": result.preparation.preparation_id,
                    "preparation_spec_file": str(
                        preparation_store.path_for(
                            result.preparation.preparation_id
                        ).resolve()
                    ),
                    "run_id": result.run_id,
                    "status": "PREPARED",
                },
                separators=(",", ":"),
                sort_keys=True,
            )
        )
        return 0
    except Exception:
        print(
            json.dumps(
                {
                    "error_type": PaperPortfolioPipelineBridgeError.__name__,
                    "status": "FAILED",
                },
                separators=(",", ":"),
                sort_keys=True,
            ),
            file=sys.stderr,
        )
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
