from __future__ import annotations

import argparse
import json
from datetime import date, datetime
from pathlib import Path

from india_swing.evaluation.nse_archive_research_dataset import (
    NseArchiveResearchDataset,
    ResearchArchiveExclusion,
    ResearchArchiveExclusionReason,
    ResearchArchiveSplitPolicy,
    build_nse_archive_research_dataset,
)
from india_swing.evaluation.nse_archive_research_dataset_store import (
    LocalNseArchiveResearchDatasetStore,
)

from .nse_archive import import_nse_historical_range
from .nse_archive_range import load_verified_nse_historical_archive_range
from .snapshot_store import LocalMarketSnapshotStore


def _date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError("expected YYYY-MM-DD") from error


def _aware_datetime(value: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError("expected ISO-8601 datetime") from error
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise argparse.ArgumentTypeError("datetime must include a UTC offset")
    return parsed


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m india_swing.market_data.nse_archive_cli"
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    import_range = subparsers.add_parser("import-range")
    import_range.add_argument("--staging-root", type=Path, required=True)
    import_range.add_argument("--archive-root", type=Path, required=True)
    import_range.add_argument("--store-root", type=Path, required=True)
    import_range.add_argument("--start", type=_date, required=True)
    import_range.add_argument("--end", type=_date, required=True)
    import_range.add_argument("--observed-at", type=_aware_datetime, required=True)
    import_range.add_argument("--workers", type=int, default=4)
    verify_range = subparsers.add_parser("verify-range")
    verify_range.add_argument("--store-root", type=Path, required=True)
    verify_range.add_argument("--index-snapshot-id", required=True)

    research_dataset_build = subparsers.add_parser("research-dataset-build")
    research_dataset_build.add_argument("--store-root", type=Path, required=True)
    research_dataset_build.add_argument(
        "--research-store-root", type=Path, required=True
    )
    research_dataset_build.add_argument(
        "--index-snapshot-id",
        dest="index_snapshot_ids",
        action="append",
        required=True,
    )
    research_dataset_build.add_argument("--train-end", type=_date, required=True)
    research_dataset_build.add_argument(
        "--validation-start", type=_date, required=True
    )
    research_dataset_build.add_argument("--validation-end", type=_date, required=True)
    research_dataset_build.add_argument("--test-start", type=_date, required=True)
    research_dataset_build.add_argument(
        "--maximum-forward-label-horizon-sessions", type=int, required=True
    )
    research_dataset_build.add_argument(
        "--source-accounting-failed-session",
        dest="source_accounting_failed_sessions",
        type=_date,
        action="append",
        default=[],
    )
    research_dataset_build.add_argument(
        "--source-cross-source-join-failed-session",
        dest="source_cross_source_join_failed_sessions",
        type=_date,
        action="append",
        default=[],
    )

    research_dataset_show = subparsers.add_parser("research-dataset-show")
    research_dataset_show.add_argument(
        "--research-store-root", type=Path, required=True
    )
    research_dataset_show.add_argument("--dataset-id", required=True)
    return parser


def _research_dataset_summary(dataset: NseArchiveResearchDataset) -> dict:
    return {
        "dataset_id": dataset.dataset_id,
        "index_snapshot_ids": list(dataset.index_snapshot_ids),
        "coverage_start": dataset.range_bindings[0].range_start.isoformat(),
        "coverage_end": dataset.range_bindings[-1].range_end.isoformat(),
        "accepted_session_count": len(dataset.accepted_sessions),
        "record_count": dataset.record_count,
        "identity_issue_count": dataset.identity_issue_count,
        "identity_quarantined_session_count": (
            dataset.identity_quarantined_session_count
        ),
        "incomplete_evidence_session_count": (
            dataset.incomplete_evidence_session_count
        ),
        "evidence_profile_counts": dict(dataset.evidence_profile_counts),
        "exclusions": [
            {"session": value.session.isoformat(), "reason": value.reason.value}
            for value in dataset.exclusions
        ],
        "partitions": [
            {
                "role": partition.role.value,
                "session_count": len(partition.sessions),
                "candidate_label_origin_session_count": len(
                    partition.candidate_label_origin_sessions
                ),
                "unavailable_label_tail_session_count": len(
                    partition.unavailable_label_tail_sessions
                ),
            }
            for partition in dataset.partitions
        ],
        "split_policy_id": dataset.split_policy_id,
        "collection_only": dataset.collection_only,
        "actionable": dataset.actionable,
        "training_eligible": dataset.training_eligible,
        "feature_eligible": dataset.feature_eligible,
        "label_eligible": dataset.label_eligible,
        "alert_eligible": dataset.alert_eligible,
        "execution_eligible": dataset.execution_eligible,
        "identity_resolution_complete": dataset.identity_resolution_complete,
        "corporate_action_adjustment_complete": (
            dataset.corporate_action_adjustment_complete
        ),
        "coverage_complete": dataset.coverage_complete,
    }


def _build_research_dataset_exclusions(
    arguments: argparse.Namespace,
) -> tuple[ResearchArchiveExclusion, ...]:
    exclusions = tuple(
        ResearchArchiveExclusion(
            session=value,
            reason=ResearchArchiveExclusionReason.SOURCE_ACCOUNTING_FAILED,
        )
        for value in arguments.source_accounting_failed_sessions
    ) + tuple(
        ResearchArchiveExclusion(
            session=value,
            reason=ResearchArchiveExclusionReason.SOURCE_CROSS_SOURCE_JOIN_FAILED,
        )
        for value in arguments.source_cross_source_join_failed_sessions
    )
    return tuple(sorted(exclusions, key=lambda value: value.session))


def main(argv: list[str] | None = None) -> int:
    arguments = build_parser().parse_args(argv)
    try:
        if arguments.command == "verify-range":
            verified = load_verified_nse_historical_archive_range(
                LocalMarketSnapshotStore(arguments.store_root),
                index_snapshot_id=arguments.index_snapshot_id,
            )
        elif arguments.command == "import-range":
            sessions, index = import_nse_historical_range(
                staging_root=arguments.staging_root,
                archive_root=arguments.archive_root,
                store=LocalMarketSnapshotStore(arguments.store_root),
                start=arguments.start,
                end=arguments.end,
                observed_at=arguments.observed_at,
                workers=arguments.workers,
            )
        elif arguments.command == "research-dataset-build":
            split_policy = ResearchArchiveSplitPolicy(
                train_end=arguments.train_end,
                validation_start=arguments.validation_start,
                validation_end=arguments.validation_end,
                test_start=arguments.test_start,
                maximum_forward_label_horizon_sessions=(
                    arguments.maximum_forward_label_horizon_sessions
                ),
            )
            exclusions = _build_research_dataset_exclusions(arguments)
            dataset = build_nse_archive_research_dataset(
                LocalMarketSnapshotStore(arguments.store_root),
                index_snapshot_ids=tuple(arguments.index_snapshot_ids),
                split_policy=split_policy,
                exclusions=exclusions,
            )
            research_store = LocalNseArchiveResearchDatasetStore(
                arguments.research_store_root
            )
            research_store.put(dataset)
            reloaded = research_store.get(dataset.dataset_id)
            reloaded.verify_content_identity()
            if reloaded != dataset:
                raise RuntimeError(
                    "reloaded research dataset differs from the built dataset"
                )
            dataset = reloaded
        else:
            research_store = LocalNseArchiveResearchDatasetStore(
                arguments.research_store_root
            )
            dataset = research_store.get(arguments.dataset_id)
            dataset.verify_content_identity()
    except Exception as error:
        print(
            json.dumps(
                {
                    "status": "FAILED",
                    "error_type": type(error).__name__,
                },
                sort_keys=True,
            )
        )
        return 1
    if arguments.command == "verify-range":
        print(
            json.dumps(
                {
                    "status": "NSE_HISTORICAL_ARCHIVE_RANGE_VERIFIED",
                    "collection_only": True,
                    "actionable": False,
                    "training_eligible": False,
                    "index_snapshot_id": verified.index_snapshot_id,
                    "coverage_start": verified.range_start.isoformat(),
                    "coverage_end": verified.range_end.isoformat(),
                    "session_count": len(verified.sessions),
                    "record_count": verified.record_count,
                    "identity_issue_count": verified.identity_issue_count,
                    "identity_quarantined_session_count": (
                        verified.identity_quarantined_session_count
                    ),
                    "incomplete_evidence_session_count": (
                        verified.incomplete_evidence_session_count
                    ),
                    "evidence_profile_counts": dict(
                        verified.evidence_profile_counts
                    ),
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 0
    if arguments.command == "import-range":
        print(
            json.dumps(
                {
                    "status": "NSE_HISTORICAL_ARCHIVE_IMPORTED",
                    "collection_only": True,
                    "actionable": False,
                    "training_eligible": False,
                    "session_count": len(sessions),
                    "record_count": sum(value.record_count for value in sessions),
                    "identity_issue_count": sum(
                        value.identity_issue_count for value in sessions
                    ),
                    "identity_quarantined_session_count": sum(
                        value.identity_issue_count > 0 for value in sessions
                    ),
                    "incomplete_evidence_session_count": index.normalized_payload[
                        "incomplete_evidence_session_count"
                    ],
                    "evidence_profile_counts": index.normalized_payload[
                        "evidence_profile_counts"
                    ],
                    "coverage_start": sessions[0].session.isoformat(),
                    "coverage_end": sessions[-1].session.isoformat(),
                    "index_snapshot_id": index.manifest.snapshot_id,
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 0
    if arguments.command == "research-dataset-build":
        print(
            json.dumps(
                {
                    "status": "NSE_ARCHIVE_RESEARCH_DATASET_READY",
                    **_research_dataset_summary(dataset),
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 0
    print(
        json.dumps(
            {
                "status": "NSE_ARCHIVE_RESEARCH_DATASET_LOADED",
                **_research_dataset_summary(dataset),
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
