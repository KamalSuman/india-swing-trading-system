from __future__ import annotations

import argparse
import json
import sys
from typing import Sequence

from .baseline_store import (
    LocalDeterministicComparisonRunStore,
    LocalGeneratedIntentBatchStore,
)
from .comparison_store import LocalTrialEvaluationComparisonStore
from .config import EvaluationEvidenceConfig, TrialRegistryConfig
from .family_aggregate_store import LocalTrialFamilyAggregateStore
from .family_report import build_trial_family_evaluation_report
from .family_report_store import LocalTrialFamilyReportStore
from .result_store import LocalTrialEvaluationResultStore
from .trial_store import LocalTrialRegistry


class EvaluationCliArgumentError(ValueError):
    pass


class SanitizedArgumentParser(argparse.ArgumentParser):
    def error(self, message: str) -> None:
        raise EvaluationCliArgumentError("invalid evaluation-report arguments")


def parser() -> argparse.ArgumentParser:
    root = SanitizedArgumentParser(
        description="Publish and inspect immutable trial-family evaluation reports"
    )
    commands = root.add_subparsers(dest="resource", required=True)
    report = commands.add_parser("report", help="manage family evaluation reports")
    report_commands = report.add_subparsers(dest="command", required=True)
    publish = report_commands.add_parser(
        "publish", help="build a report from the current persisted family snapshot"
    )
    publish.add_argument("--strategy-family-id", required=True)
    show = report_commands.add_parser(
        "show", help="write one persisted Markdown report to standard output"
    )
    show.add_argument("--aggregate-id", required=True)
    report_commands.add_parser("list", help="list persisted family reports")
    return root


def _stores():
    registry = LocalTrialRegistry(TrialRegistryConfig.from_env().data_root)
    evidence_root = EvaluationEvidenceConfig.from_env().data_root
    result_store = LocalTrialEvaluationResultStore(evidence_root, registry)
    comparison_store = LocalTrialEvaluationComparisonStore(
        evidence_root, registry, result_store
    )
    batch_store = LocalGeneratedIntentBatchStore(evidence_root, registry)
    run_store = LocalDeterministicComparisonRunStore(batch_store, comparison_store)
    aggregate_store = LocalTrialFamilyAggregateStore(
        evidence_root, registry, run_store
    )
    report_store = LocalTrialFamilyReportStore(evidence_root, aggregate_store)
    return registry, run_store, aggregate_store, report_store


def main(argv: Sequence[str] | None = None) -> int:
    try:
        args = parser().parse_args(argv)
        registry, run_store, aggregate_store, report_store = _stores()
        if args.command == "publish":
            registrations = registry.registrations_for_family(
                args.strategy_family_id
            )
            if not registrations:
                raise ValueError("strategy family is not registered")
            trial_ids = tuple(sorted(value.trial_id for value in registrations))
            runs = tuple(run_store.get(value) for value in trial_ids)
            aggregate = aggregate_store.get(args.strategy_family_id, trial_ids)
            report = build_trial_family_evaluation_report(
                aggregate=aggregate,
                runs=runs,
            )
            stored = report_store.publish(
                report,
                aggregate=aggregate,
                runs=runs,
            )
            response = {
                "status": "COMPLETE",
                "kind": "TRIAL_FAMILY_EVALUATION_REPORT",
                "strategy_family_id": args.strategy_family_id,
                "aggregate_id": stored.aggregate_id,
                "report_id": stored.report_id,
                "path": str(report_store.path_for(stored.aggregate_id).resolve()),
            }
        elif args.command == "show":
            report = report_store.get(args.aggregate_id)
            sys.stdout.write(report.markdown)
            return 0
        else:
            reports = report_store.list_reports()
            response = {
                "status": "COMPLETE",
                "kind": "TRIAL_FAMILY_EVALUATION_REPORT_LIST",
                "count": len(reports),
                "reports": [
                    {
                        "aggregate_id": value.aggregate_id,
                        "report_id": value.report_id,
                    }
                    for value in reports
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
