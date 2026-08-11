"""One-shot Cloud Run boundary for the verified NSE archive research dataset.

The expensive archive replay reads only exact, caller-supplied range snapshot
IDs from a mounted corpus.  The resulting canonical dataset manifest is
published create-once to an exact content-addressed GCS object.  This module
never lists a bucket, discovers an object, mutates source data, or makes a
research dataset actionable.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from collections.abc import Callable, Sequence
from datetime import date
from pathlib import Path

from india_swing.daily_pipeline.state_publication import (
    GoogleCloudStorageStateObjectWriter,
    PublishedStateObject,
    StateObjectWriter,
)
from india_swing.market_data.snapshot_store import LocalMarketSnapshotStore

from .nse_archive_research_dataset import (
    NseArchiveResearchDataset,
    ResearchArchiveExclusion,
    ResearchArchiveExclusionReason,
    ResearchArchiveSplitPolicy,
    build_nse_archive_research_dataset,
)
from .nse_archive_research_dataset_store import (
    MAXIMUM_MANIFEST_BYTES,
    encode_nse_archive_research_dataset,
)


class NseArchiveResearchDatasetCloudJobError(ValueError):
    """The cloud build or publication boundary failed closed."""


_ERROR = "NSE archive research dataset cloud job failed"
_OBJECT_PREFIX = "research/nse-archive-datasets/v1"


class _StaticArgumentParser(argparse.ArgumentParser):
    def error(self, _message: str) -> None:
        raise NseArchiveResearchDatasetCloudJobError(_ERROR)


def _iso_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except (TypeError, ValueError) as error:
        raise argparse.ArgumentTypeError("expected YYYY-MM-DD") from error


def _parser() -> argparse.ArgumentParser:
    parser = _StaticArgumentParser(
        prog="india-swing-nse-archive-research-cloud-job"
    )
    parser.add_argument("--store-root", type=Path, required=True)
    parser.add_argument("--bucket", required=True)
    parser.add_argument(
        "--index-snapshot-id", dest="index_snapshot_ids", action="append", required=True
    )
    parser.add_argument("--train-end", type=_iso_date, required=True)
    parser.add_argument("--validation-start", type=_iso_date, required=True)
    parser.add_argument("--validation-end", type=_iso_date, required=True)
    parser.add_argument("--test-start", type=_iso_date, required=True)
    parser.add_argument(
        "--maximum-forward-label-horizon-sessions", type=int, required=True
    )
    parser.add_argument(
        "--source-accounting-failed-session",
        dest="source_accounting_failed_sessions",
        type=_iso_date,
        action="append",
        default=[],
    )
    parser.add_argument(
        "--source-cross-source-join-failed-session",
        dest="source_cross_source_join_failed_sessions",
        type=_iso_date,
        action="append",
        default=[],
    )
    return parser


def _exclusions(arguments: argparse.Namespace) -> tuple[ResearchArchiveExclusion, ...]:
    values = tuple(
        ResearchArchiveExclusion(
            session=session,
            reason=ResearchArchiveExclusionReason.SOURCE_ACCOUNTING_FAILED,
        )
        for session in arguments.source_accounting_failed_sessions
    ) + tuple(
        ResearchArchiveExclusion(
            session=session,
            reason=ResearchArchiveExclusionReason.SOURCE_CROSS_SOURCE_JOIN_FAILED,
        )
        for session in arguments.source_cross_source_join_failed_sessions
    )
    return tuple(sorted(values, key=lambda value: value.session))


def _default_writer_factory() -> StateObjectWriter:
    return GoogleCloudStorageStateObjectWriter()


def build_and_publish_nse_archive_research_dataset(
    *,
    store_root: Path,
    bucket: str,
    index_snapshot_ids: tuple[str, ...],
    split_policy: ResearchArchiveSplitPolicy,
    exclusions: tuple[ResearchArchiveExclusion, ...],
    writer: StateObjectWriter,
) -> tuple[NseArchiveResearchDataset, PublishedStateObject]:
    """Replay exact source ranges and publish one canonical manifest."""

    if not isinstance(store_root, Path) or not store_root.is_absolute():
        raise NseArchiveResearchDatasetCloudJobError(_ERROR)
    if not callable(getattr(writer, "create_or_verify", None)):
        raise NseArchiveResearchDatasetCloudJobError(_ERROR)

    dataset = build_nse_archive_research_dataset(
        LocalMarketSnapshotStore(store_root),
        index_snapshot_ids=index_snapshot_ids,
        split_policy=split_policy,
        exclusions=exclusions,
    )
    dataset.verify_content_identity()
    content_bytes = encode_nse_archive_research_dataset(dataset)
    object_name = f"{_OBJECT_PREFIX}/{dataset.dataset_id}.json"
    published = writer.create_or_verify(
        bucket=bucket,
        object_name=object_name,
        content_bytes=content_bytes,
        content_type="application/json",
        maximum_bytes=MAXIMUM_MANIFEST_BYTES,
    )
    expected_sha256 = hashlib.sha256(content_bytes).hexdigest()
    if (
        type(published) is not PublishedStateObject
        or published.object_name != object_name
        or type(published.generation) is not int
        or published.generation <= 0
        or published.byte_count != len(content_bytes)
        or published.sha256 != expected_sha256
    ):
        raise NseArchiveResearchDatasetCloudJobError(_ERROR)
    return dataset, published


def main(
    argv: Sequence[str] | None = None,
    *,
    writer_factory: Callable[[], StateObjectWriter] | None = None,
) -> int:
    try:
        arguments = _parser().parse_args(list(argv) if argv is not None else None)
        split_policy = ResearchArchiveSplitPolicy(
            train_end=arguments.train_end,
            validation_start=arguments.validation_start,
            validation_end=arguments.validation_end,
            test_start=arguments.test_start,
            maximum_forward_label_horizon_sessions=(
                arguments.maximum_forward_label_horizon_sessions
            ),
        )
        active_factory = writer_factory or _default_writer_factory
        if not callable(active_factory):
            raise NseArchiveResearchDatasetCloudJobError(_ERROR)
        writer = active_factory()
        dataset, published = build_and_publish_nse_archive_research_dataset(
            store_root=arguments.store_root,
            bucket=arguments.bucket,
            index_snapshot_ids=tuple(arguments.index_snapshot_ids),
            split_policy=split_policy,
            exclusions=_exclusions(arguments),
            writer=writer,
        )
        result = {
            "status": "NSE_ARCHIVE_RESEARCH_DATASET_PUBLISHED",
            "dataset_id": dataset.dataset_id,
            "record_count": dataset.record_count,
            "accepted_session_count": len(dataset.accepted_sessions),
            "collection_only": dataset.collection_only,
            "actionable": dataset.actionable,
            "training_eligible": dataset.training_eligible,
            "feature_eligible": dataset.feature_eligible,
            "object_name": published.object_name,
            "generation": published.generation,
            "sha256": published.sha256,
            "byte_count": published.byte_count,
        }
        print(
            json.dumps(
                result,
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
                    "error_type": NseArchiveResearchDatasetCloudJobError.__name__,
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
