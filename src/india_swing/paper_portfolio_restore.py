from __future__ import annotations

import json
import os
import sys
from collections.abc import Mapping, Sequence
from pathlib import Path

from india_swing.daily_pipeline.acquisition import GoogleCloudStorageObjectReader
from india_swing.operations.job import validate_swing_operational_state_root
from india_swing.paper_outcomes import (
    LocalPaperPortfolioStateStore,
    PaperPortfolioError,
    restore_paper_portfolio_state,
    validate_paper_outcome_state_bucket,
)


def _arguments(argv: Sequence[str]) -> dict[str, str]:
    allowed = {
        "--state-root",
        "--expected-batch-id",
        "--manifest-object",
        "--manifest-generation",
        "--manifest-sha256",
    }
    values: dict[str, str] = {}
    index = 0
    while index < len(argv):
        token = argv[index]
        if token not in allowed or token in values or index + 1 >= len(argv):
            raise PaperPortfolioError("invalid paper portfolio restore arguments")
        value = argv[index + 1]
        if type(value) is not str or not value:
            raise PaperPortfolioError("invalid paper portfolio restore arguments")
        values[token] = value
        index += 2
    if set(values) != allowed:
        raise PaperPortfolioError("paper portfolio restore arguments are incomplete")
    return values


def main(
    argv: Sequence[str] | None = None,
    *,
    environ: Mapping[str, str] | None = None,
) -> int:
    try:
        values = _arguments(list(argv) if argv is not None else sys.argv[1:])
        state_root = validate_swing_operational_state_root(Path(values["--state-root"]))
        runtime = os.environ if environ is None else environ
        bucket = validate_paper_outcome_state_bucket(
            runtime.get("INDIA_SWING_PAPER_OUTCOME_STATE_BUCKET")
        )
        try:
            generation = int(values["--manifest-generation"])
        except (TypeError, ValueError):
            raise PaperPortfolioError(
                "paper portfolio manifest generation is invalid"
            ) from None
        state = restore_paper_portfolio_state(
            expected_batch_id=values["--expected-batch-id"],
            bucket=bucket,
            manifest_object_name=values["--manifest-object"],
            manifest_generation=generation,
            manifest_sha256=values["--manifest-sha256"],
            reader=GoogleCloudStorageObjectReader(),
            store=LocalPaperPortfolioStateStore(state_root / "paper_portfolio"),
        )
        print(
            json.dumps(
                {
                    "batch_id": state.batch_id,
                    "state_id": state.state_id,
                    "status": "RESTORED",
                },
                separators=(",", ":"),
                sort_keys=True,
            )
        )
        return 0
    except Exception:
        print(
            json.dumps(
                {"error_type": PaperPortfolioError.__name__, "status": "FAILED"},
                separators=(",", ":"),
                sort_keys=True,
            ),
            file=sys.stderr,
        )
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
