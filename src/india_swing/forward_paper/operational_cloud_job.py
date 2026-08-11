"""Cloud Run entry point for one exact forward-paper operational graph build."""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Callable, Sequence

from india_swing.daily_pipeline.acquisition import (
    GCSObjectReader,
    GoogleCloudStorageObjectReader,
)
from india_swing.corporate_actions.snapshot_store import (
    LocalCorporateActionSnapshotStore,
)
from india_swing.daily_pipeline.state_publication import (
    GoogleCloudStorageStateObjectWriter,
    StateObjectWriter,
)
from india_swing.evaluation.nse_archive_research_dataset_gcs import (
    ExactNseArchiveResearchDatasetResolver,
    PinnedNseArchiveResearchDatasetRequest,
    read_pinned_nse_archive_research_dataset,
)
from india_swing.forward_paper.operational_job import (
    ForwardPaperOperationalJobRequest,
    NseArchiveForwardPaperHistoryBuilder,
    run_forward_paper_operational_job,
)
from india_swing.market_data.snapshot_store import LocalMarketSnapshotStore
from india_swing.promoted_engine import build_promoted_engine_stores


class ForwardPaperOperationalCloudJobError(ValueError):
    """Static sanitized failure for the runnable cloud boundary."""


_ERROR = "forward paper operational cloud job failed"


class _StaticArgumentParser(argparse.ArgumentParser):
    def error(self, _message: str) -> None:
        raise ForwardPaperOperationalCloudJobError(_ERROR)


@dataclass(frozen=True, slots=True)
class ForwardPaperOperationalCloudRuntime:
    history_builder: NseArchiveForwardPaperHistoryBuilder
    corporate_actions: object
    tick_panels: object


RuntimeFactory = Callable[[argparse.Namespace], ForwardPaperOperationalCloudRuntime]
WriterFactory = Callable[[], StateObjectWriter]
ReaderFactory = Callable[[], GCSObjectReader]


def _absolute_path(value: str) -> Path:
    path = Path(value)
    if not path.is_absolute():
        raise argparse.ArgumentTypeError("expected absolute path")
    return path


def _date(value: str) -> date:
    try:
        parsed = date.fromisoformat(value)
    except Exception:
        raise argparse.ArgumentTypeError("expected date") from None
    if parsed.isoformat() != value:
        raise argparse.ArgumentTypeError("expected canonical date")
    return parsed


def _cutoff(value: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value)
    except Exception:
        raise argparse.ArgumentTypeError("expected datetime") from None
    if (
        parsed.tzinfo is None
        or parsed.utcoffset() is None
        or parsed.utcoffset() != timedelta(0)
        or parsed.isoformat() != value
    ):
        raise argparse.ArgumentTypeError("expected canonical UTC datetime")
    return parsed


def _sessions(value: str) -> tuple[date, ...]:
    try:
        values = tuple(_date(item) for item in value.split(";"))
    except Exception:
        raise argparse.ArgumentTypeError("expected exact session tuple") from None
    if len(values) != 60 or values != tuple(sorted(set(values))):
        raise argparse.ArgumentTypeError("expected exact session tuple")
    return values


def _positive_generation(value: str) -> int:
    try:
        result = int(value)
    except Exception:
        raise argparse.ArgumentTypeError("expected positive generation") from None
    if result <= 0 or result > 9_223_372_036_854_775_807:
        raise argparse.ArgumentTypeError("expected positive generation")
    return result


def _parser() -> argparse.ArgumentParser:
    parser = _StaticArgumentParser(prog="india-swing-forward-paper-operational-job")
    for name in (
        "market-data-root",
        "reference-root",
        "identity-evidence-root",
        "calendar-root",
        "daily-reports-root",
        "historical-corpus-root",
        "promoted-root",
        "engine-run-root",
    ):
        parser.add_argument(f"--{name}", type=_absolute_path, required=True)
    parser.add_argument("--dataset-id", required=True)
    parser.add_argument("--dataset-bucket", required=True)
    parser.add_argument(
        "--dataset-generation", type=_positive_generation, required=True
    )
    parser.add_argument("--dataset-sha256", required=True)
    parser.add_argument("--signal-session", type=_date, required=True)
    parser.add_argument("--decision-cutoff", type=_cutoff, required=True)
    parser.add_argument("--expected-market-sessions", type=_sessions, required=True)
    parser.add_argument("--corporate-action-snapshot-id", required=True)
    parser.add_argument("--tick-panel-id", required=True)
    parser.add_argument("--bucket", required=True)
    return parser


def _default_runtime_factory(
    arguments: argparse.Namespace,
    *,
    dataset_reader: GCSObjectReader | None = None,
) -> ForwardPaperOperationalCloudRuntime:
    market_data_root = arguments.market_data_root
    promoted_root = arguments.promoted_root
    reader = dataset_reader or GoogleCloudStorageObjectReader()
    dataset = read_pinned_nse_archive_research_dataset(
        request=PinnedNseArchiveResearchDatasetRequest(
            bucket=arguments.dataset_bucket,
            dataset_id=arguments.dataset_id,
            generation=arguments.dataset_generation,
            expected_sha256=arguments.dataset_sha256,
        ),
        reader=reader,
    )
    engine_stores = build_promoted_engine_stores(
        reference_root=arguments.reference_root,
        identity_evidence_root=arguments.identity_evidence_root,
        calendar_root=arguments.calendar_root,
        daily_reports_root=arguments.daily_reports_root,
        historical_corpus_root=arguments.historical_corpus_root,
        promoted_root=promoted_root,
        engine_run_root=arguments.engine_run_root,
    )
    return ForwardPaperOperationalCloudRuntime(
        history_builder=NseArchiveForwardPaperHistoryBuilder(
            datasets=ExactNseArchiveResearchDatasetResolver(dataset),
            reader=LocalMarketSnapshotStore(market_data_root),
        ),
        corporate_actions=LocalCorporateActionSnapshotStore(promoted_root),
        tick_panels=engine_stores.effective_session_ticks,
    )


def _default_writer_factory() -> StateObjectWriter:
    return GoogleCloudStorageStateObjectWriter()


def main(
    argv: Sequence[str] | None = None,
    *,
    runtime_factory: RuntimeFactory | None = None,
    writer_factory: WriterFactory | None = None,
    reader_factory: ReaderFactory | None = None,
) -> int:
    try:
        arguments = _parser().parse_args(list(argv) if argv is not None else None)
        if runtime_factory is None:
            active_reader_factory = reader_factory or GoogleCloudStorageObjectReader
            if not callable(active_reader_factory):
                raise ForwardPaperOperationalCloudJobError(_ERROR)
            active_runtime_factory = lambda values: _default_runtime_factory(
                values, dataset_reader=active_reader_factory()
            )
        else:
            active_runtime_factory = runtime_factory
        active_writer_factory = writer_factory or _default_writer_factory
        if not callable(active_runtime_factory) or not callable(active_writer_factory):
            raise ForwardPaperOperationalCloudJobError(_ERROR)
        runtime = active_runtime_factory(arguments)
        if type(runtime) is not ForwardPaperOperationalCloudRuntime:
            raise ForwardPaperOperationalCloudJobError(_ERROR)
        writer = active_writer_factory()
        request = ForwardPaperOperationalJobRequest(
            dataset_id=arguments.dataset_id,
            signal_session=arguments.signal_session,
            decision_cutoff=arguments.decision_cutoff,
            expected_market_sessions=arguments.expected_market_sessions,
            corporate_action_snapshot_id=arguments.corporate_action_snapshot_id,
            tick_panel_id=arguments.tick_panel_id,
            bucket=arguments.bucket,
        )
        receipt = run_forward_paper_operational_job(
            request=request,
            history_builder=runtime.history_builder,
            corporate_actions=runtime.corporate_actions,
            tick_panels=runtime.tick_panels,
            writer=writer,
        )
        manifest_object = receipt.publication.manifest_object
        output = {
            "blocked_feature_count": (
                receipt.graph.technical_feature_window.blocked_feature_count
            ),
            "collection_only": receipt.collection_only,
            "computed_feature_count": (
                receipt.graph.technical_feature_window.computed_feature_count
            ),
            "execution_eligible": receipt.execution_eligible,
            "graph_id": receipt.graph.graph_id,
            "manifest_generation": manifest_object.generation,
            "manifest_object_name": manifest_object.object_name,
            "manifest_sha256": manifest_object.sha256,
            "notification_eligible": receipt.notification_eligible,
            "paper_trade_eligible": receipt.paper_trade_eligible,
            "receipt_id": receipt.receipt_id,
            "request_id": receipt.request.request_id,
            "signal_session": receipt.request.signal_session.isoformat(),
            "status": "FORWARD_PAPER_OPERATIONAL_GRAPH_PUBLISHED",
        }
        print(
            json.dumps(
                output,
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
                    "error_type": ForwardPaperOperationalCloudJobError.__name__,
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
