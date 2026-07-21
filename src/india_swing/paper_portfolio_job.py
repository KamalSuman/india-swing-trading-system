from __future__ import annotations

import json
import os
import sys
from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
from pathlib import Path

from india_swing.daily_pipeline.state_publication import GoogleCloudStorageStateObjectWriter
from india_swing.notifications import (
    TelegramBotConfig,
    UrllibTelegramHTTPTransport,
)
from india_swing.operations.job import validate_swing_operational_state_root
from india_swing.paper_outcomes import (
    PaperPortfolioError,
    load_paper_portfolio_batch_spec_file,
    run_paper_portfolio_operational_service,
    validate_paper_outcome_state_bucket,
)


def _arguments(argv: Sequence[str]) -> tuple[Path, Path, Path]:
    allowed = {"--spec-file", "--evidence-root", "--state-root"}
    values: dict[str, str] = {}
    index = 0
    while index < len(argv):
        token = argv[index]
        if token not in allowed or token in values or index + 1 >= len(argv):
            raise PaperPortfolioError("invalid paper portfolio job arguments")
        values[token] = argv[index + 1]
        index += 2
    if set(values) != allowed or any(not value for value in values.values()):
        raise PaperPortfolioError("paper portfolio job arguments are incomplete")
    return Path(values["--spec-file"]), Path(values["--evidence-root"]), Path(values["--state-root"])


def main(argv: Sequence[str] | None = None, *, environ: Mapping[str, str] | None = None) -> int:
    try:
        spec_path, evidence_root, state_root = _arguments(
            list(argv) if argv is not None else sys.argv[1:]
        )
        evidence_root = validate_swing_operational_state_root(evidence_root)
        state_root = validate_swing_operational_state_root(state_root)
        runtime = os.environ if environ is None else environ
        bucket = validate_paper_outcome_state_bucket(
            runtime.get("INDIA_SWING_PAPER_OUTCOME_STATE_BUCKET")
        )
        telegram_config = TelegramBotConfig.from_env(runtime)
        spec = load_paper_portfolio_batch_spec_file(spec_path)
        result = run_paper_portfolio_operational_service(
            spec=spec,
            evidence_root=evidence_root,
            state_root=state_root,
            bucket=bucket,
            writer=GoogleCloudStorageStateObjectWriter(),
            telegram_config=telegram_config,
            telegram_transport=UrllibTelegramHTTPTransport(),
            clock=lambda: datetime.now(timezone.utc),
        )
        state = result.state
        outcome_publications = [
            {
                "job_spec_id": publication.manifest.job_spec_id,
                "manifest_generation": publication.manifest_object.generation,
                "manifest_object_name": publication.manifest_object.object_name,
                "manifest_sha256": publication.manifest_object.sha256,
            }
            for publication in result.outcome_publications
        ]
        portfolio_publication = result.portfolio_publication
        receipt = result.telegram_receipt
        print(json.dumps({
            "batch_id": state.batch_id,
            "cumulative_realized_pnl": str(state.cumulative_realized_pnl),
            "daily_realized_pnl": str(state.daily_realized_pnl),
            "outcome_manifests": outcome_publications,
            "portfolio_manifest_generation": portfolio_publication.manifest_object.generation,
            "portfolio_manifest_object_name": portfolio_publication.manifest_object.object_name,
            "portfolio_manifest_sha256": portfolio_publication.manifest_object.sha256,
            "risk_halt_reasons": list(state.risk_halt_reasons),
            "state_id": state.state_id,
            "status": "COMPLETE",
            "telegram_receipt_id": receipt.receipt_id,
        }, separators=(",", ":"), sort_keys=True))
        return 0
    except Exception:
        print(json.dumps(
            {"error_type": PaperPortfolioError.__name__, "status": "FAILED"},
            separators=(",", ":"), sort_keys=True,
        ), file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
