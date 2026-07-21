from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

from india_swing.signals.proposal_artifacts import (
    LocalSwingProposalBatchStore,
    SwingProposalArtifactError,
    SwingProposalBatchManifest,
)
from india_swing.signals.proposal_parent_store import (
    LocalSwingProposalParentStore,
)
from india_swing.signals.proposal_batch import SwingProposalBatch

from .models import SwingOperationalRunRecord
from .store import LocalSwingOperationalRunStore, SwingOperationalStoreError


def _record_data(value: SwingOperationalRunRecord) -> dict[str, object]:
    value.verify_content_identity()
    return {
        "action": value.action.value,
        "completed_at": value.completed_at.isoformat(),
        "decision_id": value.decision_id,
        "evaluated_at": (
            None if value.evaluated_at is None else value.evaluated_at.isoformat()
        ),
        "failure_codes": [item.value for item in value.failure_codes],
        "message": value.message,
        "notification_id": value.notification_id,
        "package_id": value.package_id,
        "paper_registration_id": value.paper_registration_id,
        "portfolio_snapshot_id": value.portfolio_snapshot_id,
        "proposal_batch_id": value.proposal_batch_id,
        "quote_batch_id": value.quote_batch_id,
        "record_id": value.record_id,
        "run_id": value.run_id,
        "spec_id": value.spec_id,
        "started_at": value.started_at.isoformat(),
        "status": value.status.value,
        "target_session": value.target_session.isoformat(),
    }


def _proposal_manifest_data(value: SwingProposalBatchManifest) -> dict[str, object]:
    value.verify_content_identity()
    return {
        "calendar_snapshot_id": value.calendar_snapshot_id,
        "cutoff": value.cutoff.isoformat(),
        "manifest_id": value.manifest_id,
        "proposal_batch_id": value.proposal_batch_id,
        "proposal_subject_count": value.proposal_subject_count,
        "scoped_subject_count": value.scoped_subject_count,
        "signal_config_id": value.signal_config_id,
        "signal_session": value.signal_session.isoformat(),
        "universe_batch_id": value.universe_batch_id,
        "universe_snapshot_id": value.universe_snapshot_id,
        "veto_subject_count": value.veto_subject_count,
    }


def _verified_proposal_data(value: SwingProposalBatch) -> dict[str, object]:
    value.verify_content_identity()
    return {
        "calendar_snapshot_id": value.calendar.snapshot_id,
        "proposal_batch_id": value.batch_id,
        "proposal_subject_count": value.proposal_subject_count,
        "readiness": value.readiness.value,
        "research_only": value.research_only,
        "scoped_subject_count": value.scoped_subject_count,
        "signal_config_id": value.config.config_id,
        "signal_session": value.universe_batch.signal_session.isoformat(),
        "universe_batch_id": value.universe_batch.batch_id,
        "veto_subject_count": value.veto_subject_count,
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="india-swing-operational")
    parser.add_argument(
        "--root",
        type=Path,
        default=Path(os.environ.get("INDIA_SWING_OPERATIONAL_ROOT", "var/operational")),
    )
    parser.add_argument(
        "--proposal-root",
        type=Path,
        default=Path(os.environ.get("INDIA_SWING_PROPOSAL_ROOT", "var/proposals")),
    )
    parser.add_argument(
        "--parent-root",
        type=Path,
        default=Path(os.environ.get("INDIA_SWING_PROPOSAL_PARENT_ROOT", "var/proposals")),
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    show = subparsers.add_parser("show")
    show.add_argument("--spec-id", required=True)
    subparsers.add_parser("list")
    proposal_show = subparsers.add_parser("proposal-show")
    proposal_show.add_argument("--proposal-batch-id", required=True)
    subparsers.add_parser("proposal-list")
    proposal_verify = subparsers.add_parser("proposal-verify")
    proposal_verify.add_argument("--proposal-batch-id", required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "show":
            store = LocalSwingOperationalRunStore(args.root)
            payload: object = _record_data(store.get(args.spec_id))
        elif args.command == "list":
            store = LocalSwingOperationalRunStore(args.root)
            payload = [_record_data(value) for value in store.list_records()]
        else:
            proposal_store = LocalSwingProposalBatchStore(args.proposal_root)
            if args.command == "proposal-show":
                payload = _proposal_manifest_data(
                    proposal_store.get_manifest(args.proposal_batch_id)
                )
            elif args.command == "proposal-list":
                payload = [
                    _proposal_manifest_data(value)
                    for value in proposal_store.list_manifests()
                ]
            else:
                payload = _verified_proposal_data(
                    proposal_store.load(
                        args.proposal_batch_id,
                        LocalSwingProposalParentStore(args.parent_root),
                    )
                )
        print(
            json.dumps(
                payload,
                allow_nan=False,
                ensure_ascii=False,
                separators=(",", ":"),
                sort_keys=True,
            )
        )
        return 0
    except (SwingOperationalStoreError, SwingProposalArtifactError) as exc:
        print(json.dumps({"error": type(exc).__name__}))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
