from __future__ import annotations

import argparse
import json
import sys
from datetime import date
from typing import Sequence

from india_swing.daily_pipeline.config import DailyPipelineConfig
from india_swing.daily_pipeline.store import LocalDailyPipelineRunStore
from india_swing.reference_data.config import ReferenceDataConfig
from india_swing.tick_sizes import (
    LocalTickSizeSnapshotStore,
    TickSizeConfig,
    tick_size_promotion_evidence,
)

from .adapters import promotion_evidence_from_daily_run
from .config import PromotionConfig
from .gate import evaluate_promotion
from .models import PromotionDecision
from .store import LocalPromotionDecisionStore


class PromotionArgumentError(ValueError):
    pass


class SanitizedArgumentParser(argparse.ArgumentParser):
    def error(self, message: str) -> None:
        raise PromotionArgumentError("invalid promotion arguments")


def _date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("date must be ISO-8601") from exc


def parser() -> argparse.ArgumentParser:
    root = SanitizedArgumentParser(
        description="Evaluate and inspect fail-closed promotion decisions"
    )
    commands = root.add_subparsers(dest="command", required=True)
    evaluate = commands.add_parser(
        "evaluate-daily-run",
        help="evaluate one sealed collection-only daily run",
    )
    evaluate.add_argument("--run-id", required=True)
    evaluate.add_argument("--history-start", type=_date, required=True)
    evaluate.add_argument("--tick-size-snapshot-id")
    show = commands.add_parser("show", help="show one promotion decision")
    show.add_argument("--decision-id", required=True)
    commands.add_parser("list", help="list promotion decisions")
    return root


def _summary(value: PromotionDecision) -> dict[str, object]:
    if type(value) is not PromotionDecision:
        raise TypeError("promotion summary requires an exact decision")
    value.verify_content_identity()
    return {
        "decision_id": value.decision_id,
        "market_session": value.market_session.isoformat(),
        "history_start": value.history_start.isoformat(),
        "decision_cutoff": value.decision_cutoff.isoformat(),
        "achieved_stage": value.achieved_stage.value,
        "research_eligible": value.research_eligible,
        "backtest_eligible": value.backtest_eligible,
        "alert_eligible": value.alert_eligible,
        "evidence_count": len(value.evidence),
        "evidence_capabilities": [
            item.capability.value for item in value.evidence
        ],
        "research_blockers": list(value.research_blockers),
        "backtest_blockers": list(value.backtest_blockers),
        "alert_blockers": list(value.alert_blockers),
        "policy_version": value.policy_version,
    }


def main(argv: Sequence[str] | None = None) -> int:
    try:
        args = parser().parse_args(argv)
        store = LocalPromotionDecisionStore(PromotionConfig.from_env().data_root)
        if args.command == "evaluate-daily-run":
            run = LocalDailyPipelineRunStore(
                DailyPipelineConfig.from_env().data_root
            ).get(args.run_id)
            evidence = list(promotion_evidence_from_daily_run(run))
            if args.tick_size_snapshot_id is not None:
                tick_snapshot = LocalTickSizeSnapshotStore(
                    TickSizeConfig.from_env().data_root,
                    ReferenceDataConfig.from_env().data_root,
                ).get(args.tick_size_snapshot_id)
                evidence.append(tick_size_promotion_evidence(tick_snapshot))
            decision = evaluate_promotion(
                market_session=run.market_session,
                history_start=args.history_start,
                decision_cutoff=run.cutoff,
                evidence=tuple(evidence),
            )
            response = {
                "status": "COMPLETE",
                "kind": "PROMOTION_DECISION",
                **_summary(store.put(decision)),
            }
        elif args.command == "show":
            response = {
                "status": "COMPLETE",
                "kind": "PROMOTION_DECISION",
                **_summary(store.get(args.decision_id)),
            }
        else:
            response = {
                "status": "COMPLETE",
                "kind": "PROMOTION_DECISION_LIST",
                "decisions": [
                    _summary(value) for value in store.list_decisions()
                ],
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
