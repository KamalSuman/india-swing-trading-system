from __future__ import annotations

import argparse
import json
import sys
from datetime import date
from typing import Sequence

from .collection import MarketDataCollector
from .config import KiteCredentials, MarketDataConfig
from .kite import KiteMarketDataAdapter
from .snapshot_store import LocalMarketSnapshotStore


def _date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("expected YYYY-MM-DD") from exc


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser(description="Collect immutable read-only Kite snapshots")
    commands = root.add_subparsers(dest="command", required=True)

    instruments = commands.add_parser("instruments", help="archive the daily instrument dump")
    instruments.add_argument("--exchange", default="NSE")

    daily = commands.add_parser("daily", help="archive one finalized daily candle")
    daily.add_argument("--instrument-master-snapshot-id", required=True)
    daily.add_argument("--instrument-token", type=int, required=True)
    daily.add_argument("--session", type=_date, required=True)
    daily.add_argument("--exchange", default="NSE")
    return root


def main(argv: Sequence[str] | None = None) -> int:
    args = parser().parse_args(argv)
    try:
        config = MarketDataConfig.from_env()
        credentials = KiteCredentials.from_env()
        adapter = KiteMarketDataAdapter.from_official_sdk(
            credentials,
            required_version=config.kite_sdk_version,
        )
        collector = MarketDataCollector(
            adapter,
            LocalMarketSnapshotStore(config.data_root),
        )
        if args.command == "instruments":
            stored = collector.collect_instruments(args.exchange)
        else:
            stored = collector.collect_daily_candle(
                instrument_master_snapshot_id=args.instrument_master_snapshot_id,
                instrument_token=args.instrument_token,
                session=args.session,
                exchange=args.exchange,
            )
    except Exception as exc:
        print(
            json.dumps(
                {
                    "status": "FAILED",
                    "error_type": type(exc).__name__,
                },
                sort_keys=True,
            ),
            file=sys.stderr,
        )
        return 2

    print(
        json.dumps(
            {
                "status": "COMPLETE",
                "dataset": stored.manifest.dataset,
                "snapshot_id": stored.manifest.snapshot_id,
                "observed_at": stored.manifest.observed_at.isoformat(),
                "record_count": stored.manifest.record_count,
                "path": str(stored.path.resolve()),
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
