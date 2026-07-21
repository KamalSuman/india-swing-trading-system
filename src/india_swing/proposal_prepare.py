from __future__ import annotations

import json
import sys
from collections.abc import Sequence
from pathlib import Path

from india_swing.operations.job import validate_swing_operational_state_root
from india_swing.signals.proposal_artifacts import LocalSwingProposalBatchStore
from india_swing.signals.proposal_parent_store import LocalSwingProposalParentStore
from india_swing.signals.proposal_preparation import (
    LocalSwingProposalPreparationStore,
    SwingProposalPreparationError,
    build_swing_proposal_preparation_spec,
    load_swing_proposal_preparation_spec_file,
    prepare_stored_swing_proposal_graph,
)


def _arguments(argv: Sequence[str]) -> dict[str, str]:
    allowed = {
        "--spec-file",
        "--graph-root",
        "--universe-batch-id",
        "--calendar-snapshot-id",
        "--signal-config-id",
    }
    values: dict[str, str] = {}
    index = 0
    while index < len(argv):
        token = argv[index]
        if token not in allowed or token in values or index + 1 >= len(argv):
            raise SwingProposalPreparationError("invalid proposal preparation arguments")
        value = argv[index + 1]
        if type(value) is not str or not value:
            raise SwingProposalPreparationError("invalid proposal preparation arguments")
        values[token] = value
        index += 2
    exact_ids = {
        "--universe-batch-id",
        "--calendar-snapshot-id",
        "--signal-config-id",
    }
    if "--graph-root" not in values or not (
        set(values) == {"--graph-root", "--spec-file"}
        or set(values) == {"--graph-root"} | exact_ids
    ):
        raise SwingProposalPreparationError("proposal preparation arguments are incomplete")
    return values


def main(argv: Sequence[str] | None = None) -> int:
    try:
        values = _arguments(
            list(argv) if argv is not None else sys.argv[1:]
        )
        graph_root = validate_swing_operational_state_root(
            Path(values["--graph-root"])
        )
        parent_store = LocalSwingProposalParentStore(graph_root)
        if "--spec-file" in values:
            spec = load_swing_proposal_preparation_spec_file(
                Path(values["--spec-file"])
            )
        else:
            spec = build_swing_proposal_preparation_spec(
                universe_batch=parent_store.get_universe_batch(
                    values["--universe-batch-id"]
                ),
                calendar=parent_store.get_calendar_snapshot(
                    values["--calendar-snapshot-id"]
                ),
                signal_config=parent_store.get_signal_config(
                    values["--signal-config-id"]
                ),
            )
        preparation_store = LocalSwingProposalPreparationStore(graph_root)
        manifest = prepare_stored_swing_proposal_graph(
            spec=spec,
            parent_store=parent_store,
            proposal_store=LocalSwingProposalBatchStore(graph_root),
            preparation_store=preparation_store,
        )
        print(
            json.dumps(
                {
                    "calendar_snapshot_id": manifest.calendar_snapshot_id,
                    "preparation_id": spec.preparation_id,
                    "preparation_spec_file": str(
                        preparation_store.path_for(spec.preparation_id).resolve()
                    ),
                    "proposal_batch_id": manifest.proposal_batch_id,
                    "proposal_subject_count": spec.proposal_subject_count,
                    "research_only": spec.research_only,
                    "signal_config_id": manifest.signal_config_id,
                    "status": "PREPARED",
                    "universe_batch_id": manifest.universe_batch_id,
                    "veto_subject_count": spec.veto_subject_count,
                },
                separators=(",", ":"),
                sort_keys=True,
            )
        )
        return 0
    except Exception:
        print(
            json.dumps(
                {"error_type": SwingProposalPreparationError.__name__, "status": "FAILED"},
                separators=(",", ":"),
                sort_keys=True,
            ),
            file=sys.stderr,
        )
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
