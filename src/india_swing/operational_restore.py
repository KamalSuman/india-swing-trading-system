from __future__ import annotations

import json
import os
import sys
from collections.abc import Mapping, Sequence
from pathlib import Path

from india_swing.daily_pipeline.acquisition import GoogleCloudStorageObjectReader
from india_swing.operations import (
    LocalSwingOperationalRunStore,
    SwingOperationalStateError,
    SwingOperationalStateRestoreRequest,
    restore_swing_operational_state_from_gcs,
    validate_operational_state_bucket,
    validate_swing_operational_state_root,
)
from india_swing.paper_trades import LocalPaperTradeLedger
from india_swing.recommendations import LocalSwingDecisionOutbox


_ARGUMENTS = {
    "--expected-spec-id",
    "--manifest-generation",
    "--manifest-object",
    "--manifest-sha256",
    "--state-root",
}


def _arguments(argv: Sequence[str]) -> dict[str, str]:
    values: dict[str, str] = {}
    index = 0
    while index < len(argv):
        token = argv[index]
        if token not in _ARGUMENTS or token in values or index + 1 >= len(argv):
            raise SwingOperationalStateError("invalid operational restore argument")
        value = argv[index + 1]
        if type(value) is not str or not value:
            raise SwingOperationalStateError("invalid operational restore argument")
        values[token] = value
        index += 2
    if set(values) != _ARGUMENTS:
        raise SwingOperationalStateError("required operational restore argument is absent")
    return values


def _generation(value: str) -> int:
    if not value.isascii() or not value.isdigit() or value.startswith("0"):
        raise SwingOperationalStateError("manifest generation argument is invalid")
    try:
        return int(value)
    except Exception:
        raise SwingOperationalStateError("manifest generation argument is invalid") from None


def main(
    argv: Sequence[str] | None = None,
    *,
    environ: Mapping[str, str] | None = None,
) -> int:
    args = list(argv) if argv is not None else sys.argv[1:]
    try:
        values = _arguments(args)
        runtime = os.environ if environ is None else environ
        bucket = validate_operational_state_bucket(
            runtime.get("INDIA_SWING_OPERATIONAL_STATE_BUCKET")
        )
        state_root = validate_swing_operational_state_root(
            Path(values["--state-root"])
        )
        request = SwingOperationalStateRestoreRequest(
            bucket=bucket,
            manifest_object_name=values["--manifest-object"],
            generation=_generation(values["--manifest-generation"]),
            expected_sha256=values["--manifest-sha256"],
            expected_spec_id=values["--expected-spec-id"],
        )
        restored = restore_swing_operational_state_from_gcs(
            request=request,
            reader=GoogleCloudStorageObjectReader(),
            run_store=LocalSwingOperationalRunStore(state_root / "operational"),
            decision_outbox=LocalSwingDecisionOutbox(
                state_root / "decision_outbox"
            ),
            paper_ledger=LocalPaperTradeLedger(state_root / "paper"),
        )
        print(
            json.dumps(
                {
                    "action": restored.record.action.value,
                    "publication_id": restored.manifest.publication_id,
                    "record_id": restored.record.record_id,
                    "spec_id": restored.record.spec_id,
                    "status": restored.record.status.value,
                    "target_session": restored.record.target_session.isoformat(),
                },
                allow_nan=False,
                ensure_ascii=False,
                separators=(",", ":"),
                sort_keys=True,
            )
        )
        return 0
    except Exception:
        print(
            json.dumps(
                {
                    "error_type": SwingOperationalStateError.__name__,
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
