from __future__ import annotations

import argparse
import json
import sys
from datetime import date, datetime
from typing import Sequence

from india_swing.calendar_evidence import build_observed_market_date_artifact
from india_swing.daily_reports.artifact_store import LocalDailyBundleArtifactStore
from india_swing.daily_reports.config import DailyReportsConfig
from india_swing.reference_data.artifact_store import LocalReferenceArtifactStore
from india_swing.reference_data.config import ReferenceDataConfig

from .reconciler import reconcile_collection_only


class EvidenceArgumentError(ValueError):
    pass


class SanitizedArgumentParser(argparse.ArgumentParser):
    def error(self, message: str) -> None:
        raise EvidenceArgumentError("invalid evidence arguments")


def _aware_datetime(value: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("cutoff must be an ISO-8601 datetime") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise argparse.ArgumentTypeError("cutoff must include a timezone offset")
    return parsed


def _date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("market session must be YYYY-MM-DD") from exc


def parser() -> argparse.ArgumentParser:
    root = SanitizedArgumentParser(
        description="Build collection-only NSE calendar and listing evidence"
    )
    commands = root.add_subparsers(dest="command", required=True)

    observed = commands.add_parser(
        "observed-dates",
        help="derive positive traded-date evidence from one archived daily bundle",
    )
    observed.add_argument("--daily-bundle-id", required=True)
    observed.add_argument("--cutoff", type=_aware_datetime, required=True)

    reconcile = commands.add_parser(
        "reconcile",
        help="reconcile every retained security-master row without creating a universe",
    )
    reconcile.add_argument("--security-master-id", required=True)
    reconcile.add_argument(
        "--daily-bundle-id",
        action="append",
        required=True,
        dest="daily_bundle_ids",
    )
    reconcile.add_argument("--market-session", type=_date, required=True)
    reconcile.add_argument("--cutoff", type=_aware_datetime, required=True)
    return root


def main(argv: Sequence[str] | None = None) -> int:
    try:
        args = parser().parse_args(argv)
        daily_store = LocalDailyBundleArtifactStore(
            DailyReportsConfig.from_env().data_root
        )
        if args.command == "observed-dates":
            source = daily_store.get(args.daily_bundle_id)
            artifact = build_observed_market_date_artifact(
                source,
                cutoff=args.cutoff,
            )
            response = {
                "status": "COMPLETE",
                "kind": "OBSERVED_MARKET_DATES",
                "artifact_id": artifact.artifact_id,
                "source_daily_bundle_id": artifact.source_bundle_artifact_id,
                "observed_dates": [value.isoformat() for value in artifact.observed_dates],
                "knowledge_time": artifact.knowledge_time.isoformat(),
                "inference_scope": artifact.inference_scope,
                "readiness": artifact.readiness.value,
                "actionable": artifact.actionable,
            }
        else:
            reference_store = LocalReferenceArtifactStore(
                ReferenceDataConfig.from_env().data_root
            )
            master = reference_store.get(args.security_master_id)
            bundles = tuple(
                daily_store.get(artifact_id)
                for artifact_id in args.daily_bundle_ids
            )
            snapshot = reconcile_collection_only(
                security_master=master,
                daily_bundles=bundles,
                market_session=args.market_session,
                cutoff=args.cutoff,
            )
            response = {
                "status": "COMPLETE",
                "kind": "COLLECTION_RECONCILIATION",
                "snapshot_id": snapshot.snapshot_id,
                "market_session": snapshot.market_session.isoformat(),
                "cutoff": snapshot.cutoff.isoformat(),
                "retained_row_count": snapshot.retained_row_count,
                "main_eq_scope_count": snapshot.main_scope_count,
                "sme_sm_scope_count": snapshot.sme_scope_count,
                "unsupported_series_count": snapshot.unsupported_series_count,
                "unresolved_count": snapshot.unresolved_count,
                "traded_row_count": snapshot.traded_row_count,
                "orphan_report_key_count": len(snapshot.orphan_report_keys),
                "report_binding_count": len(snapshot.report_bindings),
                "global_reason_codes": list(snapshot.global_reason_codes),
                "readiness": snapshot.readiness.value,
                "actionable": snapshot.actionable,
            }
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

    print(json.dumps(response, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
