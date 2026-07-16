from __future__ import annotations

import argparse
import json
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Sequence

from india_swing.calendar_evidence import build_observed_market_date_artifact
from india_swing.daily_reports.artifact_store import LocalDailyBundleArtifactStore
from india_swing.daily_reports.config import DailyReportsConfig

from .artifact_store import LocalCalendarSourceArtifactStore
from .config import CalendarDataConfig
from .materialization import materialize_collection_calendar
from .materialization_store import LocalCalendarMaterializationStore


class CalendarDataArgumentError(ValueError):
    pass


class SanitizedArgumentParser(argparse.ArgumentParser):
    def error(self, message: str) -> None:
        raise CalendarDataArgumentError("invalid calendar-data arguments")


def _date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("date must be YYYY-MM-DD") from exc


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
        description="Archive and materialize collection-only NSE CM calendars"
    )
    commands = root.add_subparsers(dest="command", required=True)

    source = commands.add_parser(
        "source-import",
        help="archive one official PDF and its strict event declaration",
    )
    source.add_argument("--source-pdf", type=Path, required=True)
    source.add_argument("--declaration", type=Path, required=True)

    materialize = commands.add_parser(
        "materialize",
        help="resolve an explicit collection-only calendar event graph",
    )
    materialize.add_argument(
        "--source-id",
        action="append",
        required=True,
        dest="source_ids",
    )
    materialize.add_argument(
        "--observed-daily-bundle-id",
        action="append",
        default=[],
        dest="observed_bundle_ids",
    )
    materialize.add_argument("--coverage-start", type=_date, required=True)
    materialize.add_argument("--coverage-end", type=_date, required=True)
    materialize.add_argument("--cutoff", type=_aware_datetime, required=True)
    return root


def main(argv: Sequence[str] | None = None) -> int:
    try:
        args = parser().parse_args(argv)
        calendar_config = CalendarDataConfig.from_env()
        daily_config = DailyReportsConfig.from_env()
        source_store = LocalCalendarSourceArtifactStore(calendar_config.data_root)
        if args.command == "source-import":
            stored = source_store.import_source(
                args.source_pdf,
                args.declaration,
            )
            response = {
                "status": "COMPLETE",
                "kind": "CALENDAR_SOURCE",
                "artifact_id": stored.manifest.artifact_id,
                "manifest_id": stored.manifest.manifest_id,
                "claimed_document_id": stored.manifest.claimed_document_id,
                "knowledge_time": stored.knowledge_time.isoformat(),
                "event_ids": list(stored.manifest.event_ids),
                "readiness": stored.manifest.readiness.value,
                "actionable": stored.manifest.actionable,
            }
        else:
            sources = tuple(source_store.get(value) for value in args.source_ids)
            daily_store = LocalDailyBundleArtifactStore(daily_config.data_root)
            evidence = tuple(
                build_observed_market_date_artifact(
                    daily_store.get(value),
                    cutoff=args.cutoff,
                )
                for value in args.observed_bundle_ids
            )
            materialization = materialize_collection_calendar(
                sources=sources,
                coverage_start=args.coverage_start,
                coverage_end=args.coverage_end,
                cutoff=args.cutoff,
                observed_date_artifacts=evidence,
            )
            stored = LocalCalendarMaterializationStore(
                calendar_config.data_root,
                daily_config.data_root,
            ).put(materialization)
            response = {
                "status": "COMPLETE",
                "kind": "CALENDAR_MATERIALIZATION",
                "materialization_id": stored.manifest.artifact_id,
                "manifest_id": stored.manifest.manifest_id,
                "calendar_snapshot_id": stored.manifest.calendar_snapshot_id,
                "coverage_start": stored.manifest.coverage_start.isoformat(),
                "coverage_end": stored.manifest.coverage_end.isoformat(),
                "day_count": stored.manifest.day_count,
                "session_count": stored.manifest.session_count,
                "source_count": stored.manifest.source_count,
                "observed_evidence_count": (
                    stored.manifest.observed_evidence_count
                ),
                "readiness": stored.manifest.readiness.value,
                "actionable": stored.manifest.actionable,
            }
    except Exception as exc:
        print(
            json.dumps(
                {"status": "FAILED", "error_type": type(exc).__name__},
                sort_keys=True,
            ),
            file=sys.stderr,
        )
        return 2

    print(json.dumps(response, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
