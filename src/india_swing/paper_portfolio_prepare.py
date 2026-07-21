from __future__ import annotations

import json
import sys
from collections.abc import Sequence
from pathlib import Path

from india_swing.calendar_data.materialization_store import LocalCalendarMaterializationStore
from india_swing.historical_prices import LocalHistoricalPriceArtifactStore
from india_swing.operations.job import validate_swing_operational_state_root
from india_swing.paper_outcomes import (
    LocalPaperOutcomeEvidenceSource,
    LocalPaperPortfolioBatchStore,
    LocalPaperPortfolioStateStore,
    PaperPortfolioPreparationError,
    load_paper_portfolio_preparation_spec_file,
    prepare_paper_portfolio_batch,
)
from india_swing.paper_trades import LocalPaperTradeLedger
from india_swing.tick_sizes import LocalTickSizeSnapshotStore


def _arguments(argv: Sequence[str]) -> tuple[Path, Path, Path]:
    allowed = {"--spec-file", "--evidence-root", "--state-root"}
    values: dict[str, str] = {}
    index = 0
    while index < len(argv):
        token = argv[index]
        if token not in allowed or token in values or index + 1 >= len(argv):
            raise PaperPortfolioPreparationError("invalid preparation arguments")
        value = argv[index + 1]
        if type(value) is not str or not value:
            raise PaperPortfolioPreparationError("invalid preparation arguments")
        values[token] = value
        index += 2
    if set(values) != allowed:
        raise PaperPortfolioPreparationError("preparation arguments are incomplete")
    return (
        Path(values["--spec-file"]),
        Path(values["--evidence-root"]),
        Path(values["--state-root"]),
    )


def main(argv: Sequence[str] | None = None) -> int:
    try:
        spec_path, evidence_root, state_root = _arguments(
            list(argv) if argv is not None else sys.argv[1:]
        )
        evidence_root = validate_swing_operational_state_root(evidence_root)
        state_root = validate_swing_operational_state_root(state_root)
        spec = load_paper_portfolio_preparation_spec_file(spec_path)
        ledger = LocalPaperTradeLedger(state_root / "paper")
        source = LocalPaperOutcomeEvidenceSource(
            paper_ledger=ledger,
            calendar_store=LocalCalendarMaterializationStore(
                evidence_root / "calendar_data", evidence_root / "daily_reports"
            ),
            tick_store=LocalTickSizeSnapshotStore(
                evidence_root / "tick_sizes", evidence_root / "reference_data"
            ),
            historical_store=LocalHistoricalPriceArtifactStore(
                evidence_root / "historical_prices", evidence_root / "daily_reports"
            ),
        )
        batch = prepare_paper_portfolio_batch(
            spec=spec,
            ledger=ledger,
            evidence_source=source,
            portfolio_store=LocalPaperPortfolioStateStore(
                state_root / "paper_portfolio"
            ),
        )
        store = LocalPaperPortfolioBatchStore(state_root / "paper_portfolio_batches")
        stored = store.put(batch)
        print(
            json.dumps(
                {
                    "batch_id": stored.batch_id,
                    "batch_spec_file": str(store.path_for(stored.batch_id).resolve()),
                    "outcome_job_count": len(stored.outcome_jobs),
                    "preparation_id": spec.preparation_id,
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
                    "error_type": PaperPortfolioPreparationError.__name__,
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
