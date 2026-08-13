"""Cloud Run entry point for one exact-pinned baseline/challenger run."""

from __future__ import annotations

import argparse
import json
import sys
import time
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Callable, Mapping, Sequence

from india_swing.daily_pipeline.acquisition import (
    GCSObjectReader,
    GoogleCloudStorageObjectReader,
)
from india_swing.daily_pipeline.state_publication import (
    GoogleCloudStorageStateObjectWriter,
    StateObjectWriter,
)
from india_swing.features.promoted_cross_section import PromotedCrossSectionConfig

from .operational_cloud_job import (
    ForwardPaperOperationalCloudRuntime,
    build_forward_paper_operational_cloud_runtime,
)
from .research_gcs import ForwardPaperOperationalManifestPin
from .research_job import (
    ForwardPaperResearchJobRequest,
    run_forward_paper_research_job,
)


FORWARD_PAPER_RESEARCH_DESIGN = "DEFAULT_V1_VS_HIGH_VOL_040_V1"


class ForwardPaperResearchCloudJobError(ValueError):
    """Static, sanitized failure for the runnable research cloud boundary."""


_ERROR = "forward paper research cloud job failed"


class _StaticArgumentParser(argparse.ArgumentParser):
    def error(self, _message: str) -> None:
        raise ForwardPaperResearchCloudJobError(_ERROR)


@dataclass(frozen=True, slots=True)
class ForwardPaperResearchCloudRuntime:
    operational: ForwardPaperOperationalCloudRuntime


RuntimeFactory = Callable[[argparse.Namespace], ForwardPaperResearchCloudRuntime]
WriterFactory = Callable[[], StateObjectWriter]
ReaderFactory = Callable[[], GCSObjectReader]


class _StructuredProgressLogger:
    """Bounded timing events on stderr; stdout remains one canonical receipt."""

    def __init__(self) -> None:
        self._started_at = time.perf_counter()
        self._stage_started_at: dict[str, float] = {}

    def __call__(
        self,
        stage: str,
        status: str,
        details: Mapping[str, int],
    ) -> None:
        now = time.perf_counter()
        payload: dict[str, object] = {
            "elapsed_seconds": round(now - self._started_at, 3),
            "event": "FORWARD_PAPER_RESEARCH_STAGE",
            "stage": stage,
            "status": status,
        }
        if status == "started":
            self._stage_started_at[stage] = now
        elif status in {"completed", "progress"}:
            started = self._stage_started_at.get(stage)
            if started is not None:
                payload["stage_elapsed_seconds"] = round(now - started, 3)
        for key, value in details.items():
            if type(key) is str and type(value) is int and value >= 0:
                payload[key] = value
        print(
            json.dumps(payload, separators=(",", ":"), sort_keys=True),
            file=sys.stderr,
            flush=True,
        )


def _absolute_path(value: str) -> Path:
    path = Path(value)
    if not path.is_absolute() or ".." in path.parts:
        raise argparse.ArgumentTypeError("expected absolute path")
    return path


def _positive_integer(value: str) -> int:
    try:
        result = int(value)
    except Exception:
        raise argparse.ArgumentTypeError("expected positive integer") from None
    if result <= 0 or result > 9_223_372_036_854_775_807:
        raise argparse.ArgumentTypeError("expected positive integer")
    return result


def _parser() -> argparse.ArgumentParser:
    parser = _StaticArgumentParser(prog="india-swing-forward-paper-research-job")
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
    parser.add_argument("--dataset-generation", type=_positive_integer, required=True)
    parser.add_argument("--dataset-sha256", required=True)
    parser.add_argument("--source-bucket", required=True)
    parser.add_argument("--source-graph-id", required=True)
    parser.add_argument("--source-manifest-object-name", required=True)
    parser.add_argument(
        "--source-manifest-generation", type=_positive_integer, required=True
    )
    parser.add_argument("--source-manifest-sha256", required=True)
    parser.add_argument("--output-bucket", required=True)
    parser.add_argument(
        "--research-design",
        choices=(FORWARD_PAPER_RESEARCH_DESIGN,),
        required=True,
    )
    parser.add_argument("--baseline-config-id", required=True)
    parser.add_argument("--challenger-config-id", required=True)
    parser.add_argument(
        "--comparison-top-tiers", type=_positive_integer, default=10
    )
    return parser


def forward_paper_research_design_configs(
    design: str,
) -> tuple[PromotedCrossSectionConfig, PromotedCrossSectionConfig]:
    if design != FORWARD_PAPER_RESEARCH_DESIGN:
        raise ForwardPaperResearchCloudJobError(_ERROR)
    baseline = PromotedCrossSectionConfig()
    challenger = PromotedCrossSectionConfig(
        high_volatility_threshold=Decimal("0.40")
    )
    return baseline, challenger


def build_forward_paper_research_cloud_runtime(
    arguments: argparse.Namespace,
    *,
    dataset_reader: GCSObjectReader,
    progress: _StructuredProgressLogger | None = None,
) -> ForwardPaperResearchCloudRuntime:
    operational = build_forward_paper_operational_cloud_runtime(
        arguments,
        dataset_reader=dataset_reader,
        progress=progress,
    )
    return ForwardPaperResearchCloudRuntime(operational=operational)


def _default_writer_factory() -> StateObjectWriter:
    return GoogleCloudStorageStateObjectWriter()


def _default_reader_factory() -> GCSObjectReader:
    return GoogleCloudStorageObjectReader()


def main(
    argv: Sequence[str] | None = None,
    *,
    runtime_factory: RuntimeFactory | None = None,
    writer_factory: WriterFactory | None = None,
    reader_factory: ReaderFactory | None = None,
) -> int:
    try:
        arguments = _parser().parse_args(list(argv) if argv is not None else None)
        baseline, challenger = forward_paper_research_design_configs(
            arguments.research_design
        )
        if (
            baseline.config_id != arguments.baseline_config_id
            or challenger.config_id != arguments.challenger_config_id
        ):
            raise ForwardPaperResearchCloudJobError(_ERROR)
        active_reader_factory = reader_factory or _default_reader_factory
        active_writer_factory = writer_factory or _default_writer_factory
        if not callable(active_reader_factory) or not callable(active_writer_factory):
            raise ForwardPaperResearchCloudJobError(_ERROR)
        reader = active_reader_factory()
        progress = _StructuredProgressLogger() if runtime_factory is None else None
        if runtime_factory is None:
            runtime = build_forward_paper_research_cloud_runtime(
                arguments,
                dataset_reader=reader,
                progress=progress,
            )
        else:
            runtime = runtime_factory(arguments)
        if type(runtime) is not ForwardPaperResearchCloudRuntime:
            raise ForwardPaperResearchCloudJobError(_ERROR)
        operational = runtime.operational
        if type(operational) is not ForwardPaperOperationalCloudRuntime:
            raise ForwardPaperResearchCloudJobError(_ERROR)
        request = ForwardPaperResearchJobRequest(
            source_pin=ForwardPaperOperationalManifestPin(
                bucket=arguments.source_bucket,
                expected_graph_id=arguments.source_graph_id,
                object_name=arguments.source_manifest_object_name,
                generation=arguments.source_manifest_generation,
                sha256=arguments.source_manifest_sha256,
            ),
            baseline_config=baseline,
            challenger_config=challenger,
            comparison_top_tiers=arguments.comparison_top_tiers,
            output_bucket=arguments.output_bucket,
        )
        receipt = run_forward_paper_research_job(
            request=request,
            reader=reader,
            history_windows=operational.history_builder,
            corporate_actions=operational.corporate_actions,
            tick_panels=operational.tick_panels,
            writer=active_writer_factory(),
            stage_observer=progress,
        )
        published = receipt.publication.manifest_object
        output = {
            "baseline_arm_id": receipt.run.baseline.arm_id,
            "baseline_config_id": receipt.run.baseline.config.config_id,
            "baseline_top_count": receipt.run.baseline_top_count,
            "challenger_arm_id": receipt.run.challenger.arm_id,
            "challenger_config_id": receipt.run.challenger.config.config_id,
            "challenger_top_count": receipt.run.challenger_top_count,
            "collection_only": receipt.collection_only,
            "execution_eligible": receipt.execution_eligible,
            "manifest_generation": published.generation,
            "manifest_object_name": published.object_name,
            "manifest_sha256": published.sha256,
            "notification_eligible": receipt.notification_eligible,
            "overlap_count": receipt.run.overlap_count,
            "paper_trade_eligible": receipt.paper_trade_eligible,
            "promotion_eligible": receipt.promotion_eligible,
            "receipt_id": receipt.receipt_id,
            "request_id": receipt.request.request_id,
            "research_design": arguments.research_design,
            "run_id": receipt.run.run_id,
            "source_graph_id": receipt.run.source_graph.graph_id,
            "status": "FORWARD_PAPER_RESEARCH_RUN_PUBLISHED",
        }
        print(json.dumps(output, separators=(",", ":"), sort_keys=True))
        return 0
    except Exception:
        print(
            json.dumps(
                {
                    "error_type": ForwardPaperResearchCloudJobError.__name__,
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
