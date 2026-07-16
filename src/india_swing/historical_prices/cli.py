from __future__ import annotations

import argparse
import json
import sys
from datetime import date, datetime
from typing import Sequence

from india_swing.daily_reports.artifact_store import LocalDailyBundleArtifactStore

from .artifact_store import LocalHistoricalPriceArtifactStore
from .config import HistoricalPricesConfig
from .materialize import materialize_nse_eod_session


class HistoricalPricesArgumentError(ValueError):
    pass


class SanitizedArgumentParser(argparse.ArgumentParser):
    def error(self, message: str) -> None:
        raise HistoricalPricesArgumentError("invalid historical-price arguments")


def _date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("market session must be YYYY-MM-DD") from exc


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
        description="Materialize sealed collection-only NSE historical prices"
    )
    commands = root.add_subparsers(dest="command", required=True)
    materialize = commands.add_parser(
        "materialize",
        help="derive one raw NSE CM EOD session from an archived daily bundle",
    )
    materialize.add_argument("--daily-bundle-id", required=True)
    materialize.add_argument("--market-session", type=_date, required=True)
    materialize.add_argument("--cutoff", type=_aware_datetime, required=True)
    return root


def main(argv: Sequence[str] | None = None) -> int:
    try:
        args = parser().parse_args(argv)
        config = HistoricalPricesConfig.from_env()
        daily_store = LocalDailyBundleArtifactStore(config.daily_reports_root)
        source = daily_store.get(args.daily_bundle_id)
        artifact = materialize_nse_eod_session(
            source,
            market_session=args.market_session,
            cutoff=args.cutoff,
        )
        stored = LocalHistoricalPriceArtifactStore(
            config.data_root,
            config.daily_reports_root,
        ).put(artifact)
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
                "artifact_id": stored.manifest.artifact_id,
                "manifest_id": stored.manifest.manifest_id,
                "market_session": stored.manifest.market_session.isoformat(),
                "cutoff": stored.manifest.cutoff.isoformat(),
                "knowledge_time": stored.manifest.knowledge_time.isoformat(),
                "bar_count": stored.manifest.bar_count,
                "udiff_row_count": stored.manifest.udiff_row_count,
                "full_delivery_row_count": (
                    stored.manifest.full_delivery_row_count
                ),
                "price_basis": stored.manifest.price_basis,
                "coverage_scope": stored.manifest.coverage_scope,
                "readiness": stored.manifest.readiness.value,
                "actionable": stored.manifest.actionable,
                "path": str(stored.path.resolve()),
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
