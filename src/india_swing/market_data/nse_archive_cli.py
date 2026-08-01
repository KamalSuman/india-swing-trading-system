from __future__ import annotations

import argparse
import json
from datetime import date, datetime
from pathlib import Path

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
    return parser


def main(argv: list[str] | None = None) -> int:
    arguments = build_parser().parse_args(argv)
    try:
        if arguments.command == "verify-range":
            verified = load_verified_nse_historical_archive_range(
                LocalMarketSnapshotStore(arguments.store_root),
                index_snapshot_id=arguments.index_snapshot_id,
            )
        else:
            sessions, index = import_nse_historical_range(
                staging_root=arguments.staging_root,
                archive_root=arguments.archive_root,
                store=LocalMarketSnapshotStore(arguments.store_root),
                start=arguments.start,
                end=arguments.end,
                observed_at=arguments.observed_at,
                workers=arguments.workers,
            )
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
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 0
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
                "coverage_start": sessions[0].session.isoformat(),
                "coverage_end": sessions[-1].session.isoformat(),
                "index_snapshot_id": index.manifest.snapshot_id,
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
