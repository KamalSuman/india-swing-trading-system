from __future__ import annotations

import json
import os
import sys
from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

from india_swing.daily_pipeline.state_publication import GoogleCloudStorageStateObjectWriter
from india_swing.daily_workflow import (
    DailyPaperWorkflowError,
    DailyPaperWorkflowSpec,
    LocalDailyPaperWorkflowStore,
    LocalDailyPaperWorkflowWorker,
    run_daily_paper_workflow,
)
from india_swing.notifications import TelegramBotConfig, UrllibTelegramHTTPTransport


def _arguments(argv: Sequence[str]) -> dict[str, str]:
    required = {"--run-id", "--derived-evidence-id", "--evidence-root", "--state-root"}
    optional = {"--daily-loss-limit", "--cumulative-loss-limit", "--maximum-attempts"}
    allowed = required | optional
    values: dict[str, str] = {}
    index = 0
    while index < len(argv):
        token = argv[index]
        if token not in allowed or token in values or index + 1 >= len(argv):
            raise DailyPaperWorkflowError("invalid daily workflow arguments")
        value = argv[index + 1]
        if type(value) is not str or not value:
            raise DailyPaperWorkflowError("invalid daily workflow arguments")
        values[token] = value
        index += 2
    if not required.issubset(values):
        raise DailyPaperWorkflowError("daily workflow arguments are incomplete")
    return values


def main(
    argv: Sequence[str] | None = None,
    *,
    environ: Mapping[str, str] | None = None,
) -> int:
    try:
        values = _arguments(list(argv) if argv is not None else sys.argv[1:])
        runtime = os.environ if environ is None else environ
        spec = DailyPaperWorkflowSpec(
            run_id=values["--run-id"],
            derived_evidence_id=values["--derived-evidence-id"],
            state_bucket=runtime["INDIA_SWING_PAPER_OUTCOME_STATE_BUCKET"],
            daily_loss_limit=Decimal(values.get("--daily-loss-limit", "1000")),
            cumulative_loss_limit=Decimal(
                values.get("--cumulative-loss-limit", "2000")
            ),
            maximum_attempts=int(values.get("--maximum-attempts", "3")),
        )
        evidence_root = Path(values["--evidence-root"])
        state_root = Path(values["--state-root"])
        clock = lambda: datetime.now(timezone.utc)
        terminal = run_daily_paper_workflow(
            spec=spec,
            worker=LocalDailyPaperWorkflowWorker(
                evidence_root=evidence_root,
                state_root=state_root,
                writer=GoogleCloudStorageStateObjectWriter(),
                telegram_config=TelegramBotConfig.from_env(runtime),
                telegram_transport=UrllibTelegramHTTPTransport(),
                clock=clock,
            ),
            store=LocalDailyPaperWorkflowStore(state_root / "daily_workflow"),
            clock=clock,
        )
        output = terminal.output
        print(
            json.dumps(
                {
                    "batch_id": output.batch_id,
                    "derived_evidence_id": spec.derived_evidence_id,
                    "output_id": output.output_id,
                    "run_id": spec.run_id,
                    "state_id": output.state_id,
                    "status": output.status.value,
                    "telegram_receipt_id": output.telegram_receipt_id,
                    "terminal_id": terminal.terminal_id,
                    "workflow_id": spec.workflow_id,
                },
                separators=(",", ":"),
                sort_keys=True,
            )
        )
        return 0
    except Exception:
        print(
            json.dumps(
                {"error_type": DailyPaperWorkflowError.__name__, "status": "FAILED"},
                separators=(",", ":"),
                sort_keys=True,
            ),
            file=sys.stderr,
        )
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
