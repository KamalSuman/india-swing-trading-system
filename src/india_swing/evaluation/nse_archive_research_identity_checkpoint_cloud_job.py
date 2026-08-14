"""One-shot builder for one exact NSE archive identity checkpoint.

The command reads one generation- and SHA-pinned research dataset manifest,
authenticates its mounted historical prefix through an explicit session, and
publishes one content-addressed checkpoint with create-or-verify semantics.
It grants no research or trading authority and creates no recurring work.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from collections.abc import Callable, Sequence
from datetime import date
from pathlib import Path

from india_swing.daily_pipeline.acquisition import (
    GCSObjectReader,
    GoogleCloudStorageObjectReader,
)
from india_swing.daily_pipeline.state_publication import (
    GoogleCloudStorageStateObjectWriter,
    PublishedStateObject,
    StateObjectWriter,
)
from india_swing.market_data.nse_archive_range import (
    NseHistoricalArchiveSnapshotReader,
)
from india_swing.market_data.snapshot_store import LocalMarketSnapshotStore

from .nse_archive_research_dataset_gcs import (
    PinnedNseArchiveResearchDatasetRequest,
    read_pinned_nse_archive_research_dataset,
)
from .nse_archive_research_identity_checkpoint import (
    NseArchiveResearchIdentityCheckpoint,
    encode_nse_archive_research_identity_checkpoint,
    nse_archive_research_identity_checkpoint_object_name,
    publish_nse_archive_research_identity_checkpoint,
)
from .nse_archive_research_identity_checkpoint_runtime import (
    build_nse_archive_research_identity_checkpoint,
)


class NseArchiveResearchIdentityCheckpointCloudJobError(ValueError):
    """The one-shot checkpoint boundary failed with a static error."""


_ERROR = "NSE archive research identity checkpoint cloud job failed"
_MAXIMUM_GENERATION = 9_223_372_036_854_775_807
_BUCKET = re.compile(r"[a-z0-9][a-z0-9._-]{1,61}[a-z0-9]\Z")


class _StaticArgumentParser(argparse.ArgumentParser):
    def error(self, _message: str) -> None:
        raise NseArchiveResearchIdentityCheckpointCloudJobError(_ERROR)


def _absolute_path(value: str) -> Path:
    path = Path(value)
    if not path.is_absolute():
        raise argparse.ArgumentTypeError("expected absolute path")
    return path


def _canonical_date(value: str) -> date:
    try:
        result = date.fromisoformat(value)
    except Exception:
        raise argparse.ArgumentTypeError("expected canonical date") from None
    if result.isoformat() != value:
        raise argparse.ArgumentTypeError("expected canonical date")
    return result


def _positive_generation(value: str) -> int:
    try:
        result = int(value)
    except Exception:
        raise argparse.ArgumentTypeError("expected positive generation") from None
    if result <= 0 or result > _MAXIMUM_GENERATION:
        raise argparse.ArgumentTypeError("expected positive generation")
    return result


def _bucket(value: str) -> str:
    if _BUCKET.fullmatch(value) is None:
        raise argparse.ArgumentTypeError("expected bucket")
    return value


def _parser() -> argparse.ArgumentParser:
    parser = _StaticArgumentParser(
        prog="india-swing-nse-archive-identity-checkpoint-cloud-job"
    )
    parser.add_argument("--market-data-root", type=_absolute_path, required=True)
    parser.add_argument("--dataset-bucket", type=_bucket, required=True)
    parser.add_argument("--dataset-id", required=True)
    parser.add_argument(
        "--dataset-generation",
        type=_positive_generation,
        required=True,
    )
    parser.add_argument("--dataset-sha256", required=True)
    parser.add_argument(
        "--checkpoint-session",
        type=_canonical_date,
        required=True,
    )
    parser.add_argument("--checkpoint-bucket", type=_bucket, required=True)
    return parser


def _fail() -> None:
    raise NseArchiveResearchIdentityCheckpointCloudJobError(_ERROR)


def build_and_publish_nse_archive_research_identity_checkpoint(
    *,
    dataset_request: PinnedNseArchiveResearchDatasetRequest,
    dataset_reader: GCSObjectReader,
    archive_reader: NseHistoricalArchiveSnapshotReader,
    checkpoint_session: date,
    checkpoint_bucket: str,
    writer: StateObjectWriter,
) -> tuple[NseArchiveResearchIdentityCheckpoint, PublishedStateObject]:
    """Authenticate one exact prefix and publish its sealed identity state."""

    failed = False
    checkpoint = None
    published = None
    try:
        if (
            type(dataset_request) is not PinnedNseArchiveResearchDatasetRequest
            or dataset_reader is None
            or archive_reader is None
            or type(checkpoint_session) is not date
            or type(checkpoint_bucket) is not str
            or _BUCKET.fullmatch(checkpoint_bucket) is None
            or not callable(getattr(writer, "create_or_verify", None))
        ):
            raise ValueError
        dataset = read_pinned_nse_archive_research_dataset(
            request=dataset_request,
            reader=dataset_reader,
        )
        checkpoint = build_nse_archive_research_identity_checkpoint(
            dataset,
            archive_reader,
            checkpoint_session=checkpoint_session,
        )
        if (
            checkpoint.dataset_id != dataset_request.dataset_id
            or checkpoint.checkpoint_session != checkpoint_session
            or checkpoint.collection_only is not True
            or checkpoint.actionable is not False
            or checkpoint.training_eligible is not False
            or checkpoint.feature_eligible is not False
            or checkpoint.label_eligible is not False
            or checkpoint.alert_eligible is not False
            or checkpoint.execution_eligible is not False
        ):
            raise ValueError
        position = dataset.accepted_sessions.index(checkpoint_session)
        if (
            checkpoint.checkpoint_session_snapshot_id
            != dataset.session_snapshot_ids[position]
        ):
            raise ValueError

        published = publish_nse_archive_research_identity_checkpoint(
            checkpoint=checkpoint,
            bucket=checkpoint_bucket,
            writer=writer,
        )
        payload = encode_nse_archive_research_identity_checkpoint(checkpoint)
        expected_object_name = nse_archive_research_identity_checkpoint_object_name(
            checkpoint.checkpoint_id
        )
        if (
            type(published) is not PublishedStateObject
            or published.object_name != expected_object_name
            or type(published.generation) is not int
            or type(published.generation) is bool
            or not 0 < published.generation <= _MAXIMUM_GENERATION
            or published.byte_count != len(payload)
            or published.sha256 != hashlib.sha256(payload).hexdigest()
        ):
            raise ValueError
        published = PublishedStateObject(
            object_name=published.object_name,
            generation=published.generation,
            byte_count=published.byte_count,
            sha256=published.sha256,
        )
    except Exception:
        failed = True
    if failed or checkpoint is None or published is None:
        _fail()
    return checkpoint, published


def _default_archive_reader_factory(
    root: Path,
) -> NseHistoricalArchiveSnapshotReader:
    return LocalMarketSnapshotStore(root)


def main(
    argv: Sequence[str] | None = None,
    *,
    dataset_reader_factory: Callable[[], GCSObjectReader] | None = None,
    archive_reader_factory: (
        Callable[[Path], NseHistoricalArchiveSnapshotReader] | None
    ) = None,
    writer_factory: Callable[[], StateObjectWriter] | None = None,
) -> int:
    try:
        arguments = _parser().parse_args(list(argv) if argv is not None else None)
        active_dataset_reader_factory = (
            dataset_reader_factory or GoogleCloudStorageObjectReader
        )
        active_archive_reader_factory = (
            archive_reader_factory or _default_archive_reader_factory
        )
        active_writer_factory = writer_factory or GoogleCloudStorageStateObjectWriter
        if (
            not callable(active_dataset_reader_factory)
            or not callable(active_archive_reader_factory)
            or not callable(active_writer_factory)
        ):
            raise ValueError

        checkpoint, published = (
            build_and_publish_nse_archive_research_identity_checkpoint(
                dataset_request=PinnedNseArchiveResearchDatasetRequest(
                    bucket=arguments.dataset_bucket,
                    dataset_id=arguments.dataset_id,
                    generation=arguments.dataset_generation,
                    expected_sha256=arguments.dataset_sha256,
                ),
                dataset_reader=active_dataset_reader_factory(),
                archive_reader=active_archive_reader_factory(
                    arguments.market_data_root
                ),
                checkpoint_session=arguments.checkpoint_session,
                checkpoint_bucket=arguments.checkpoint_bucket,
                writer=active_writer_factory(),
            )
        )
        receipt = {
            "dataset_id": checkpoint.dataset_id,
            "checkpoint_session": checkpoint.checkpoint_session.isoformat(),
            "checkpoint_session_snapshot_id": (
                checkpoint.checkpoint_session_snapshot_id
            ),
            "checkpoint_id": checkpoint.checkpoint_id,
            "object_name": published.object_name,
            "generation": published.generation,
            "sha256": published.sha256,
            "listing_state_count": len(checkpoint.latest_by_listing_key),
            "identity_state_count": len(checkpoint.latest_by_identity),
            "collection_only": checkpoint.collection_only,
            "actionable": checkpoint.actionable,
            "training_eligible": checkpoint.training_eligible,
            "feature_eligible": checkpoint.feature_eligible,
            "label_eligible": checkpoint.label_eligible,
            "alert_eligible": checkpoint.alert_eligible,
            "execution_eligible": checkpoint.execution_eligible,
        }
        print(
            json.dumps(
                receipt,
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
                    "error_type": (
                        NseArchiveResearchIdentityCheckpointCloudJobError.__name__
                    ),
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
