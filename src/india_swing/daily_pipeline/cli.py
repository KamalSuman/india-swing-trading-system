from __future__ import annotations

import argparse
import json
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Sequence

from india_swing.calendar_data.config import CalendarDataConfig
from india_swing.calendar_data.materialization_store import LocalCalendarMaterializationStore
from india_swing.daily_reports.artifact_store import LocalDailyBundleArtifactStore
from india_swing.daily_reports.config import DailyReportsConfig
from india_swing.historical_prices.artifact_store import LocalHistoricalPriceArtifactStore
from india_swing.historical_prices.config import HistoricalPricesConfig
from india_swing.identity_registry.adjudication_store import LocalIdentityAdjudicationQueueStore
from india_swing.identity_registry.artifact_store import LocalIdentityRegistryStore
from india_swing.identity_registry.config import IdentityRegistryConfig
from india_swing.reference_data.artifact_store import LocalReferenceArtifactStore
from india_swing.reference_data.config import ReferenceDataConfig

from .config import DailyPipelineConfig
from .models import DailyPipelineRun
from .runner import run_daily_pipeline
from .store import LocalDailyPipelineRunStore


class DailyPipelineArgumentError(ValueError):
    pass


class SanitizedArgumentParser(argparse.ArgumentParser):
    def error(self, message: str) -> None:
        raise DailyPipelineArgumentError("invalid daily-pipeline arguments")


def _date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("session must be YYYY-MM-DD") from exc


def _aware_datetime(value: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("cutoff must be ISO-8601") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise argparse.ArgumentTypeError("cutoff must include a timezone offset")
    return parsed


def parser() -> argparse.ArgumentParser:
    root = SanitizedArgumentParser(
        description="Run the explicit, collection-only NSE CM daily pipeline"
    )
    commands = root.add_subparsers(dest="command", required=True)
    run = commands.add_parser("run", help="import and derive one explicit market session")
    run.add_argument("--session", type=_date, required=True)
    run.add_argument("--cutoff", type=_aware_datetime, required=True)
    run.add_argument("--calendar-id", required=True)
    run.add_argument("--security-master-file", type=Path, required=True)
    run.add_argument("--daily-bundle-file", type=Path, required=True)
    run.add_argument("--previous-run-id")
    show = commands.add_parser("show", help="show one persisted daily run")
    show.add_argument("--run-id", required=True)
    commands.add_parser("list", help="list persisted daily runs")
    return root


def _summary(run: DailyPipelineRun) -> dict[str, object]:
    return {
        "run_id": run.run_id,
        "market_session": run.market_session.isoformat(),
        "cutoff": run.cutoff.isoformat(),
        "previous_run_id": run.previous_run_id,
        "security_master_artifact_id": run.current_security_master_artifact_id,
        "daily_bundle_artifact_id": run.current_daily_bundle_artifact_id,
        "historical_price_artifact_id": run.historical_price_artifact_id,
        "bar_count": run.bar_count,
        "reconciliation_snapshot_id": run.reconciliation_snapshot_id,
        "unresolved_count": run.unresolved_count,
        "identity_registry_id": run.identity_registry_id,
        "identity_transition_count": run.identity_transition_count,
        "adjudication_queue_id": run.adjudication_queue_id,
        "adjudication_case_count": run.adjudication_case_count,
        "completeness_issues": list(run.completeness_issues),
        "readiness": run.readiness.value,
        "actionable": run.actionable,
        "stable_identity_assigned": run.stable_identity_assigned,
    }


def main(argv: Sequence[str] | None = None) -> int:
    try:
        args = parser().parse_args(argv)
        run_store = LocalDailyPipelineRunStore(DailyPipelineConfig.from_env().data_root)
        if args.command == "run":
            reference_config = ReferenceDataConfig.from_env()
            daily_config = DailyReportsConfig.from_env()
            historical_config = HistoricalPricesConfig.from_env()
            identity_config = IdentityRegistryConfig.from_env()
            reference_store = LocalReferenceArtifactStore(reference_config.data_root)
            daily_store = LocalDailyBundleArtifactStore(daily_config.data_root)
            historical_store = LocalHistoricalPriceArtifactStore(
                historical_config.data_root,
                historical_config.daily_reports_root,
            )
            identity_store = LocalIdentityRegistryStore(
                identity_config.data_root,
                reference_config.data_root,
            )
            adjudication_store = LocalIdentityAdjudicationQueueStore(
                identity_config.data_root,
                identity_store,
            )
            calendar_stored = LocalCalendarMaterializationStore(
                CalendarDataConfig.from_env().data_root,
                daily_config.data_root,
            ).get(args.calendar_id)
            value = run_daily_pipeline(
                market_session=args.session,
                cutoff=args.cutoff,
                calendar_materialization_id=args.calendar_id,
                calendar=calendar_stored.materialization.calendar_snapshot,
                security_master_file=args.security_master_file,
                daily_bundle_file=args.daily_bundle_file,
                previous_run_id=args.previous_run_id,
                reference_store=reference_store,
                daily_store=daily_store,
                historical_store=historical_store,
                identity_store=identity_store,
                adjudication_store=adjudication_store,
                run_store=run_store,
            )
            response = {"status": "COMPLETE", "kind": "DAILY_PIPELINE_RUN", **_summary(value)}
        elif args.command == "show":
            response = {
                "status": "COMPLETE",
                "kind": "DAILY_PIPELINE_RUN",
                **_summary(run_store.get(args.run_id)),
            }
        else:
            response = {
                "status": "COMPLETE",
                "kind": "DAILY_PIPELINE_RUN_LIST",
                "runs": [_summary(value) for value in run_store.list_runs()],
            }
    except Exception as exc:
        print(
            json.dumps({"status": "FAILED", "error_type": type(exc).__name__}, sort_keys=True),
            file=sys.stderr,
        )
        return 2
    print(json.dumps(response, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
