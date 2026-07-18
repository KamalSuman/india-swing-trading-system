from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from typing import Sequence

from india_swing.historical_prices import LocalHistoricalPriceArtifactStore
from india_swing.historical_prices.config import HistoricalPricesConfig

from .config import LiquidityConfig
from .materialize import materialize_collection_liquidity
from .models import CollectionLiquiditySnapshot
from .store import LocalLiquiditySnapshotStore


class LiquidityArgumentError(ValueError):
    pass


class SanitizedArgumentParser(argparse.ArgumentParser):
    def error(self, message: str) -> None:
        raise LiquidityArgumentError("invalid liquidity arguments")


def _aware_datetime(value: str) -> datetime:
    try:
        result = datetime.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("cutoff must be ISO-8601") from exc
    if result.tzinfo is None or result.utcoffset() is None:
        raise argparse.ArgumentTypeError("cutoff must include a timezone offset")
    return result


def parser() -> argparse.ArgumentParser:
    root = SanitizedArgumentParser(
        description="Materialize trailing collection-only liquidity evidence"
    )
    commands = root.add_subparsers(dest="command", required=True)
    materialize = commands.add_parser("materialize")
    materialize.add_argument(
        "--historical-price-id",
        action="append",
        required=True,
        dest="historical_price_ids",
    )
    materialize.add_argument("--cutoff", required=True, type=_aware_datetime)
    materialize.add_argument("--minimum-history-sessions", type=int, default=120)
    show = commands.add_parser("show")
    show.add_argument("--snapshot-id", required=True)
    commands.add_parser("list")
    return root


def _summary(value: CollectionLiquiditySnapshot) -> dict[str, object]:
    if type(value) is not CollectionLiquiditySnapshot:
        raise TypeError("liquidity summary requires an exact snapshot")
    value.verify_content_identity()
    return {
        "snapshot_id": value.snapshot_id,
        "decision_cutoff": value.decision_cutoff.isoformat(),
        "coverage_start": value.coverage_start.isoformat(),
        "coverage_end": value.coverage_end.isoformat(),
        "source_session_count": len(value.source_sessions),
        "candidate_count": len(value.observations),
        "minimum_history_sessions": value.minimum_history_sessions,
        "candidates_meeting_minimum_history": sum(
            item.meets_minimum_history for item in value.observations
        ),
        "reason_codes": list(value.reason_codes),
        "readiness": value.readiness.value,
        "actionable": value.actionable,
    }


def main(argv: Sequence[str] | None = None) -> int:
    try:
        args = parser().parse_args(argv)
        historical_config = HistoricalPricesConfig.from_env()
        historical_root = historical_config.data_root
        daily_root = historical_config.daily_reports_root
        store = LocalLiquiditySnapshotStore(
            LiquidityConfig.from_env().data_root,
            historical_root,
            daily_root,
        )
        if args.command == "materialize":
            price_store = LocalHistoricalPriceArtifactStore(
                historical_root,
                daily_root,
            )
            sources = tuple(
                price_store.get(value).artifact
                for value in args.historical_price_ids
            )
            result = store.put(
                materialize_collection_liquidity(
                    sources,
                    decision_cutoff=args.cutoff,
                    minimum_history_sessions=args.minimum_history_sessions,
                )
            )
            response = {
                "status": "COMPLETE",
                "kind": "LIQUIDITY_SNAPSHOT",
                **_summary(result),
            }
        elif args.command == "show":
            response = {
                "status": "COMPLETE",
                "kind": "LIQUIDITY_SNAPSHOT",
                **_summary(store.get(args.snapshot_id)),
            }
        else:
            response = {
                "status": "COMPLETE",
                "kind": "LIQUIDITY_SNAPSHOT_LIST",
                "snapshots": [_summary(value) for value in store.list_snapshots()],
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
