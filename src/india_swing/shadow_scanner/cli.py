from __future__ import annotations

import argparse
import json
import sys
from decimal import Decimal, InvalidOperation

from india_swing.daily_pipeline.config import DailyPipelineConfig
from india_swing.daily_pipeline.derived_evidence_store import (
    LocalDailyDerivedEvidenceStore,
)
from india_swing.historical_prices import LocalHistoricalPriceArtifactStore
from india_swing.historical_prices.config import HistoricalPricesConfig
from india_swing.liquidity import LocalLiquiditySnapshotStore
from india_swing.liquidity.config import LiquidityConfig
from india_swing.reference_data.config import ReferenceDataConfig
from india_swing.tick_sizes import LocalTickSizeSnapshotStore
from india_swing.tick_sizes.config import TickSizeConfig
from india_swing.universe import LocalCollectionUniverseSnapshotStore
from india_swing.universe.config import CollectionUniverseConfig

from .models import CollectionShadowScannerConfig
from .scanner import scan_collection_artifacts
from .config import ShadowScanStoreConfig
from .store import LocalCollectionShadowScanStore


class ShadowScannerArgumentError(ValueError):
    pass


class SanitizedArgumentParser(argparse.ArgumentParser):
    def error(self, message: str) -> None:
        raise ShadowScannerArgumentError("invalid command arguments")


def _decimal(value: str) -> Decimal:
    try:
        parsed = Decimal(value)
    except (InvalidOperation, TypeError, ValueError):
        raise ShadowScannerArgumentError("invalid decimal argument") from None
    if not parsed.is_finite():
        raise ShadowScannerArgumentError("invalid decimal argument")
    return parsed


def _parser() -> argparse.ArgumentParser:
    parser = SanitizedArgumentParser(
        prog="india-swing-shadow-scan",
        description="Run one exact research-only collection scan",
    )
    parser.add_argument("--derived-evidence-id", required=True)
    parser.add_argument("--momentum-lookback-sessions", type=int, default=20)
    parser.add_argument(
        "--minimum-median-traded-value",
        type=_decimal,
        default=Decimal("10000000"),
    )
    parser.add_argument(
        "--minimum-delivery-percent",
        type=_decimal,
        default=Decimal("20"),
    )
    parser.add_argument("--top-n", type=int, default=20)
    parser.add_argument(
        "--publish",
        action="store_true",
        help="publish the exact result to the create-once local scan store",
    )
    return parser


def _candidate(value) -> dict[str, object]:
    return {
        "candidate_id": value.candidate_id,
        "symbol": value.symbol,
        "series": value.series,
        "validated_isin": value.validated_isin,
        "market_session": value.market_session.isoformat(),
        "current_close": str(value.current_close),
        "tick_size_rupees": str(value.tick_size_rupees),
        "lookback_return_pct": str(value.lookback_return_pct),
        "positive_session_fraction": str(value.positive_session_fraction),
        "median_daily_traded_value": str(value.median_daily_traded_value),
        "median_daily_volume": str(value.median_daily_volume),
        "median_delivery_percent": str(value.median_delivery_percent),
        "warnings": list(value.warnings),
    }


def main(argv: list[str] | None = None) -> int:
    try:
        args = _parser().parse_args(argv)
        daily_config = DailyPipelineConfig.from_env()
        history_config = HistoricalPricesConfig.from_env()
        liquidity_config = LiquidityConfig.from_env()
        universe_config = CollectionUniverseConfig.from_env()
        tick_config = TickSizeConfig.from_env()
        reference_config = ReferenceDataConfig.from_env()
        scan_store_config = ShadowScanStoreConfig.from_env()

        derived = LocalDailyDerivedEvidenceStore(daily_config.data_root).get(
            args.derived_evidence_id
        )
        history_store = LocalHistoricalPriceArtifactStore(
            history_config.data_root,
            history_config.daily_reports_root,
        )
        history = tuple(
            history_store.get(value).artifact
            for value in derived.historical_price_artifact_ids
        )
        universe = LocalCollectionUniverseSnapshotStore(
            universe_config.data_root,
            reference_config.data_root,
        ).get(derived.universe_snapshot_id)
        liquidity = LocalLiquiditySnapshotStore(
            liquidity_config.data_root,
            history_config.data_root,
            history_config.daily_reports_root,
        ).get(derived.liquidity_snapshot_id)
        ticks = LocalTickSizeSnapshotStore(
            tick_config.data_root,
            reference_config.data_root,
        ).get(derived.tick_size_snapshot_id)
        config = CollectionShadowScannerConfig(
            minimum_history_sessions=derived.minimum_history_sessions,
            momentum_lookback_sessions=args.momentum_lookback_sessions,
            minimum_median_traded_value=args.minimum_median_traded_value,
            minimum_delivery_percent=args.minimum_delivery_percent,
            top_n=args.top_n,
        )
        result = scan_collection_artifacts(
            derived=derived,
            history=history,
            universe=universe,
            liquidity=liquidity,
            ticks=ticks,
            config=config,
        )
        published_path = None
        if args.publish:
            scan_store = LocalCollectionShadowScanStore(
                scan_store_config.data_root
            )
            scan_store.put(result)
            published_path = str(scan_store.path_for(result.result_id).resolve())
        print(
            json.dumps(
                {
                    "schema_version": result.schema_version,
                    "mode": result.mode,
                    "actionable": result.actionable,
                    "result_id": result.result_id,
                    "status": result.status.value,
                    "market_session": result.market_session.isoformat(),
                    "cutoff": result.cutoff.isoformat(),
                    "derived_evidence_id": result.derived_evidence_id,
                    "config_id": result.config_id,
                    "history_session_count": len(
                        result.historical_price_artifact_ids
                    ),
                    "candidate_count": len(result.candidates),
                    "candidates": [_candidate(value) for value in result.candidates],
                    "exclusion_counts": dict(result.exclusion_counts),
                    "blockers": list(result.blockers),
                    "published_path": published_path,
                },
                ensure_ascii=False,
                indent=2,
                sort_keys=True,
            )
        )
        return 0
    except Exception as exc:
        print(
            json.dumps({"error": type(exc).__name__}, sort_keys=True),
            file=sys.stderr,
        )
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
