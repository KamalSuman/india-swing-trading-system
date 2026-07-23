from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Sequence

from india_swing.calendar_data.config import CalendarDataConfig
from india_swing.calendar_data.materialization_store import (
    LocalCalendarMaterializationStore,
)
from india_swing.daily_reports.config import DailyReportsConfig
from india_swing.historical_prices import (
    HistoricalPricesConfig,
    LocalHistoricalPriceArtifactStore,
)
from india_swing.identity_decisions import (
    LocalAdjudicatedIdentitySnapshotStore,
)
from india_swing.identity_evidence import IdentityEvidenceConfig
from india_swing.identity_registry.artifact_store import LocalIdentityRegistryStore
from india_swing.identity_registry.adjudication_store import (
    LocalIdentityAdjudicationQueueStore,
)
from india_swing.identity_registry.config import IdentityRegistryConfig
from india_swing.identity_registry.models import CrossVintageIdentityRegistry
from india_swing.reference_data.config import ReferenceDataConfig
from india_swing.reference_data.artifact_store import (
    LocalReferenceArtifactStore,
)

from .backfill import (
    HistoricalBackfillPlan,
    HistoricalBackfillRunner,
    LocalHistoricalBackfillProgressStore,
    UpstoxCatalogInstrumentResolver,
    build_historical_backfill_plan,
)
from .backfill_blockers import (
    HISTORICAL_BACKFILL_BLOCKER_POLICY_VERSION,
    LocalHistoricalBackfillBlockerReportStore,
    build_historical_backfill_blocker_report,
)
from .backfill_pilot import (
    MAXIMUM_PILOT_TOTAL_REQUESTS,
    HistoricalBackfillPilotService,
)
from .backfill_evidence_worklist import (
    HISTORICAL_BACKFILL_EVIDENCE_WORK_PACKAGE_POLICY_VERSION,
    LocalHistoricalBackfillEvidenceWorkPackageStore,
    build_historical_backfill_evidence_work_package,
)
from .collection import (
    HistoricalReconciliationCollector,
    MarketDataCollector,
    historical_dataset_name,
)
from .config import (
    KiteCredentials,
    KiteLoginCredentials,
    MarketDataConfig,
    UpstoxCredentials,
)
from .kite import KiteMarketDataAdapter
from .kite_auth import KiteInteractiveAuthenticator, LoopbackKiteCallbackReceiver
from .kite_instruments import (
    KITE_INSTRUMENTS_DATASET,
    KITE_PROVIDER,
    KiteInstrumentSnapshotResolver,
)
from .models import HistoricalDailyCandleBatch
from .reconciliation import reconcile_historical_batch
from .snapshot_store import LocalMarketSnapshotStore
from .upstox import UPSTOX_PROVIDER, UpstoxHistoricalDataAdapter
from .upstox_instruments import (
    LocalUpstoxInstrumentCatalogStore,
    fetch_upstox_nse_instrument_catalog,
    import_upstox_nse_instrument_catalog,
)


def _date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("expected YYYY-MM-DD") from exc


def _aware_datetime(value: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("expected an ISO-8601 datetime") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise argparse.ArgumentTypeError("datetime must include an explicit offset")
    return parsed


def _add_plan_arguments(command: argparse.ArgumentParser) -> None:
    command.add_argument("--identity-registry-id", required=True)
    command.add_argument(
        "--identity-snapshot-id",
        help=(
            "optional exact reviewed adjudicated-identity snapshot; "
            "never inferred as latest"
        ),
    )
    command.add_argument("--calendar-materialization-id", required=True)
    command.add_argument(
        "--provider",
        choices=(UPSTOX_PROVIDER, KITE_PROVIDER),
        default=UPSTOX_PROVIDER,
        help="historical provider; defaults to UPSTOX for backward compatibility",
    )
    command.add_argument(
        "--upstox-catalog-id",
        help="required exactly when --provider is UPSTOX",
    )
    command.add_argument(
        "--kite-instrument-snapshot-id",
        help="required exactly when --provider is ZERODHA_KITE",
    )
    command.add_argument("--coverage-start", type=_date, required=True)
    command.add_argument("--coverage-end", type=_date, required=True)
    command.add_argument("--requested-at", type=_aware_datetime, required=True)


def _add_kite_interactive_login_argument(command: argparse.ArgumentParser) -> None:
    command.add_argument(
        "--kite-interactive-login",
        action="store_true",
        help=(
            "obtain Kite credentials through one local interactive SDK "
            "login instead of the environment; rejected for provider UPSTOX"
        ),
    )


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser(
        description="Plan, run, and reconcile read-only Upstox historical backfills"
    )
    commands = root.add_subparsers(dest="command", required=True)

    plan = commands.add_parser(
        "plan",
        help="build a credential-free deterministic backfill plan",
    )
    _add_plan_arguments(plan)

    run = commands.add_parser(
        "run",
        help="execute or resume a pinned backfill plan",
    )
    _add_plan_arguments(run)
    run.add_argument("--maximum-requests", type=int)
    run.add_argument(
        "--allow-collection-with-issues",
        action="store_true",
        help="collect safe requests despite explicitly reported coverage issues",
    )
    _add_kite_interactive_login_argument(run)

    pilot = commands.add_parser(
        "pilot",
        help=(
            "collect and reconcile a capped deterministic plan prefix "
            "(research-only, never actionable)"
        ),
    )
    _add_plan_arguments(pilot)
    pilot.add_argument(
        "--maximum-total-requests",
        type=int,
        required=True,
        help=f"positive integer at or below {MAXIMUM_PILOT_TOTAL_REQUESTS}",
    )
    pilot.add_argument(
        "--nse-artifact-id",
        action="append",
        required=True,
        dest="nse_artifact_ids",
    )
    pilot.add_argument(
        "--reconciled-at",
        type=_aware_datetime,
        required=True,
    )
    _add_kite_interactive_login_argument(pilot)

    blockers = commands.add_parser(
        "blockers",
        help="seal a plan-bound work list for genuine backfill blockers",
    )
    _add_plan_arguments(blockers)

    evidence_worklist = commands.add_parser(
        "evidence-worklist",
        help=(
            "seal a CSV/JSON evidence-procurement work package for one "
            "exact blocker report"
        ),
    )
    evidence_worklist.add_argument("--blocker-report-id", required=True)

    reconcile = commands.add_parser(
        "reconcile",
        help="compare one provider batch with exact NSE EOD artifacts",
    )
    reconcile.add_argument("--provider", required=True)
    reconcile.add_argument("--provider-snapshot-id", required=True)
    reconcile.add_argument(
        "--nse-artifact-id",
        action="append",
        required=True,
        dest="nse_artifact_ids",
    )
    reconcile.add_argument(
        "--reconciled-at",
        type=_aware_datetime,
        required=True,
    )

    catalog_fetch = commands.add_parser(
        "catalog-fetch",
        help="download and seal the public Upstox NSE BOD instrument file",
    )
    catalog_fetch.set_defaults(command="catalog-fetch")

    catalog_import = commands.add_parser(
        "catalog-import",
        help="seal a manually downloaded Upstox NSE BOD instrument file",
    )
    catalog_import.add_argument("--source-file", required=True)
    catalog_import.add_argument(
        "--observed-at",
        type=_aware_datetime,
        required=True,
    )

    kite_instruments_fetch = commands.add_parser(
        "kite-instruments-fetch",
        help="collect and seal one exact Kite NSE instrument snapshot",
    )
    _add_kite_interactive_login_argument(kite_instruments_fetch)
    return root


def _require_provider_evidence(args: argparse.Namespace) -> None:
    if args.provider == UPSTOX_PROVIDER:
        if args.upstox_catalog_id is None:
            raise ValueError(
                "--upstox-catalog-id is required when --provider is UPSTOX"
            )
        if args.kite_instrument_snapshot_id is not None:
            raise ValueError(
                "--kite-instrument-snapshot-id cannot be used with --provider UPSTOX"
            )
    elif args.provider == KITE_PROVIDER:
        if args.kite_instrument_snapshot_id is None:
            raise ValueError(
                "--kite-instrument-snapshot-id is required when --provider "
                "is ZERODHA_KITE"
            )
        if args.upstox_catalog_id is not None:
            raise ValueError(
                "--upstox-catalog-id cannot be used with --provider ZERODHA_KITE"
            )
    else:
        raise ValueError("unsupported historical provider")


def _resolver_for_provider(
    args: argparse.Namespace,
    market_config: MarketDataConfig,
    identity_registry: CrossVintageIdentityRegistry | None = None,
):
    if args.provider == UPSTOX_PROVIDER:
        catalog = LocalUpstoxInstrumentCatalogStore(
            market_config.data_root
        ).get(args.upstox_catalog_id)
        return UpstoxCatalogInstrumentResolver(catalog)
    if args.provider == KITE_PROVIDER:
        snapshot = LocalMarketSnapshotStore(market_config.data_root).get(
            KITE_INSTRUMENTS_DATASET,
            args.kite_instrument_snapshot_id,
        )
        return KiteInstrumentSnapshotResolver(snapshot, identity_registry)
    raise ValueError("unsupported historical provider")


def _configured_plan_context(args: argparse.Namespace):
    _require_provider_evidence(args)
    reference_config = ReferenceDataConfig.from_env()
    identity_config = IdentityRegistryConfig.from_env()
    calendar_config = CalendarDataConfig.from_env()
    daily_config = DailyReportsConfig.from_env()
    identity_store = LocalIdentityRegistryStore(
        identity_config.data_root,
        reference_config.data_root,
    )
    registry = identity_store.get(args.identity_registry_id).registry
    reference_store = LocalReferenceArtifactStore(
        reference_config.data_root
    )
    security_master_sources = tuple(
        reference_store.get(value)
        for value in registry.source_artifact_ids
    )
    materialization = LocalCalendarMaterializationStore(
        calendar_config.data_root,
        daily_config.data_root,
    ).get(args.calendar_materialization_id).materialization
    market_config = MarketDataConfig.from_env()
    resolver = _resolver_for_provider(args, market_config, registry)
    identity_snapshot = (
        LocalAdjudicatedIdentitySnapshotStore(
            IdentityEvidenceConfig.from_env().data_root
        ).get(args.identity_snapshot_id)
        if args.identity_snapshot_id is not None
        else None
    )
    plan = build_historical_backfill_plan(
        registry=registry,
        security_master_sources=security_master_sources,
        calendar=materialization.calendar_snapshot,
        resolver=resolver,
        coverage_start=args.coverage_start,
        coverage_end=args.coverage_end,
        requested_at=args.requested_at,
        identity_snapshot=identity_snapshot,
    )
    return plan, registry, identity_store


def _configured_plan(args: argparse.Namespace) -> HistoricalBackfillPlan:
    return _configured_plan_context(args)[0]


def _plan_value(plan: HistoricalBackfillPlan) -> dict[str, object]:
    issue_counts = Counter(value.code.value for value in plan.issues)
    return {
        "plan_id": plan.plan_id,
        "provider": plan.provider,
        "identity_registry_id": plan.identity_registry_id,
        "identity_snapshot_id": plan.identity_snapshot_id,
        "calendar_snapshot_id": plan.calendar_snapshot_id,
        "coverage_start": plan.coverage_start.isoformat(),
        "coverage_end": plan.coverage_end.isoformat(),
        "requested_at": plan.requested_at.isoformat(),
        "safe_request_count": plan.safe_request_count,
        "safe_session_count": plan.safe_session_count,
        "coverage_issue_count": len(plan.issues),
        "blocking_issue_count": plan.blocking_issue_count,
        "exclusion_issue_count": plan.exclusion_issue_count,
        "warning_issue_count": plan.warning_issue_count,
        "coverage_issues_by_code": dict(sorted(issue_counts.items())),
        "collection_only": plan.collection_only,
        "coverage_complete": not plan.has_blocking_issues,
    }


def _kite_credentials(args: argparse.Namespace) -> KiteCredentials:
    if getattr(args, "kite_interactive_login", False):
        login_credentials = KiteLoginCredentials.from_env()
        receiver = LoopbackKiteCallbackReceiver()
        authenticator = KiteInteractiveAuthenticator.from_official_sdk(
            login_credentials,
            receiver,
        )
        return authenticator.login()
    return KiteCredentials.from_env()


def _connector_for_plan(plan: HistoricalBackfillPlan, args: argparse.Namespace):
    if plan.provider == UPSTOX_PROVIDER:
        if getattr(args, "kite_interactive_login", False):
            raise ValueError(
                "--kite-interactive-login is only valid for the ZERODHA_KITE "
                "provider"
            )
        return UpstoxHistoricalDataAdapter(UpstoxCredentials.from_env())
    if plan.provider == KITE_PROVIDER:
        return KiteMarketDataAdapter.from_official_sdk(_kite_credentials(args))
    raise ValueError("unsupported historical provider")


def _run_plan(
    plan: HistoricalBackfillPlan,
    args: argparse.Namespace,
) -> tuple[int, dict[str, object]]:
    summary = _plan_value(plan)
    if plan.has_blocking_issues and not args.allow_collection_with_issues:
        return 3, {
            "status": "BLOCKED_COVERAGE",
            **summary,
            "message": (
                "rerun with --allow-collection-with-issues only for "
                "explicitly partial collection"
            ),
        }

    config = MarketDataConfig.from_env()
    connector = _connector_for_plan(plan, args)
    runner = HistoricalBackfillRunner(
        connector,
        LocalMarketSnapshotStore(config.data_root),
        LocalHistoricalBackfillProgressStore(config.data_root),
    )
    progress = runner.run(
        plan,
        maximum_requests=args.maximum_requests,
    )
    safe_complete = runner.is_complete(plan, progress)
    return 0, {
        "status": (
            "SAFE_REQUESTS_COMPLETE"
            if safe_complete
            else "SAFE_REQUESTS_PARTIAL"
        ),
        **summary,
        "connector_version": progress.connector_version,
        "completed_request_count": len(progress.completions),
        "remaining_request_count": (
            len(plan.requests) - len(progress.completions)
        ),
        "safe_requests_complete": safe_complete,
        "progress_id": progress.progress_id,
        "updated_at": progress.updated_at.isoformat(),
    }


def _reconcile(args: argparse.Namespace) -> tuple[int, dict[str, object]]:
    market_config = MarketDataConfig.from_env()
    historical_config = HistoricalPricesConfig.from_env()
    market_store = LocalMarketSnapshotStore(market_config.data_root)
    stored_batch = market_store.get(
        historical_dataset_name(args.provider),
        args.provider_snapshot_id,
    )
    batch = stored_batch.normalized_payload
    if type(batch) is not HistoricalDailyCandleBatch:
        raise TypeError("provider snapshot is not a historical candle batch")
    historical_store = LocalHistoricalPriceArtifactStore(
        historical_config.data_root,
        historical_config.daily_reports_root,
    )
    artifacts = tuple(
        historical_store.get(value).artifact
        for value in args.nse_artifact_ids
    )
    report = reconcile_historical_batch(
        batch,
        artifacts,
        reconciled_at=args.reconciled_at,
    )
    stored_report = HistoricalReconciliationCollector(market_store).collect(report)
    status_counts = Counter(value.status.value for value in report.rows)
    return (0 if report.passed else 4), {
        "status": (
            "RECONCILIATION_PASSED"
            if report.passed
            else "RECONCILIATION_FAILED"
        ),
        "report_id": report.report_id,
        "report_snapshot_id": stored_report.manifest.snapshot_id,
        "historical_batch_id": report.historical_batch_id,
        "provider": report.provider,
        "listing_key": report.listing_key,
        "security_series": report.security_series,
        "isin": report.isin,
        "session_count": len(report.rows),
        "row_status_counts": dict(sorted(status_counts.items())),
        "passed": report.passed,
        "actionable": report.actionable,
    }


def _pilot(
    plan: HistoricalBackfillPlan,
    args: argparse.Namespace,
) -> tuple[int, dict[str, object]]:
    market_config = MarketDataConfig.from_env()
    historical_config = HistoricalPricesConfig.from_env()
    snapshot_store = LocalMarketSnapshotStore(market_config.data_root)
    connector = _connector_for_plan(plan, args)
    runner = HistoricalBackfillRunner(
        connector,
        snapshot_store,
        LocalHistoricalBackfillProgressStore(market_config.data_root),
    )
    service = HistoricalBackfillPilotService(
        runner,
        HistoricalReconciliationCollector(snapshot_store),
    )
    historical_store = LocalHistoricalPriceArtifactStore(
        historical_config.data_root,
        historical_config.daily_reports_root,
    )
    nse_artifacts = tuple(
        historical_store.get(value).artifact
        for value in args.nse_artifact_ids
    )
    result = service.run(
        plan,
        nse_artifacts,
        args.maximum_total_requests,
        reconciled_at=args.reconciled_at,
    )
    return (0 if result.passed else 4), {
        "status": (
            "PILOT_PASSED" if result.passed else "PILOT_RECONCILIATION_FAILED"
        ),
        "result_id": result.result_id,
        "plan_id": result.plan_id,
        "progress_id": result.progress_id,
        "provider": result.provider,
        "connector_version": result.connector_version,
        "maximum_total_requests": result.maximum_total_requests,
        "selected_request_count": len(result.selected_request_ids),
        "completed_request_count": len(result.completions),
        "reconciliation_report_count": len(result.reconciliations),
        "passed_reconciliation_count": sum(
            1 for value in result.reconciliations if value.passed
        ),
        "reconciled_at": result.reconciled_at.isoformat(),
        "passed": result.passed,
        "collection_only": result.collection_only,
        "actionable": result.actionable,
    }


def _kite_instruments_fetch(
    args: argparse.Namespace,
) -> tuple[int, dict[str, object]]:
    market_config = MarketDataConfig.from_env()
    credentials = _kite_credentials(args)
    adapter = KiteMarketDataAdapter.from_official_sdk(credentials)
    store = LocalMarketSnapshotStore(market_config.data_root)
    stored = MarketDataCollector(adapter, store).collect_instruments("NSE")
    payload = stored.normalized_payload
    return 0, {
        "status": "KITE_INSTRUMENTS_READY",
        "snapshot_id": stored.manifest.snapshot_id,
        "observed_at": stored.manifest.observed_at.isoformat(),
        "provider_version": stored.manifest.provider_version,
        "exchange": payload.exchange,
        "instrument_count": len(payload.instruments),
    }


def _catalog_value(catalog) -> dict[str, object]:
    return {
        "status": "UPSTOX_CATALOG_READY",
        "catalog_id": catalog.catalog_id,
        "observed_at": catalog.observed_at.isoformat(),
        "source_url": catalog.source_url,
        "raw_sha256": catalog.raw_sha256,
        "compressed_byte_count": catalog.compressed_byte_count,
        "uncompressed_sha256": catalog.uncompressed_sha256,
        "uncompressed_byte_count": catalog.uncompressed_byte_count,
        "source_row_count": catalog.source_row_count,
        "nse_equity_instrument_count": len(catalog.instruments),
        "actionable": catalog.actionable,
    }


def _catalog_fetch() -> tuple[int, dict[str, object]]:
    config = MarketDataConfig.from_env()
    catalog = fetch_upstox_nse_instrument_catalog(
        store=LocalUpstoxInstrumentCatalogStore(config.data_root),
    )
    return 0, _catalog_value(catalog)


def _catalog_import(args: argparse.Namespace) -> tuple[int, dict[str, object]]:
    config = MarketDataConfig.from_env()
    catalog = import_upstox_nse_instrument_catalog(
        Path(args.source_file),
        observed_at=args.observed_at,
        store=LocalUpstoxInstrumentCatalogStore(config.data_root),
    )
    return 0, _catalog_value(catalog)


def _blockers(args: argparse.Namespace) -> tuple[int, dict[str, object]]:
    plan, registry, identity_store = _configured_plan_context(args)
    queue = LocalIdentityAdjudicationQueueStore(
        identity_store.root,
        identity_store,
    ).get(registry.registry_id)
    report = build_historical_backfill_blocker_report(
        plan=plan,
        registry=registry,
        adjudication_queue=queue,
        generated_at=datetime.now(timezone.utc),
    )
    config = MarketDataConfig.from_env()
    stored = LocalHistoricalBackfillBlockerReportStore(
        config.data_root
    ).put(report)
    issue_counts = Counter(
        value.issue_code.value for value in report.entries
    )
    action_counts = Counter(
        action.value
        for value in report.entries
        for action in value.actions
    )
    requirement_counts = Counter(
        requirement.value
        for value in report.entries
        for requirement in value.requirements
    )
    return 0, {
        "status": (
            "BACKFILL_BLOCKERS_REPORTED"
            if report.entries
            else "BACKFILL_HAS_NO_BLOCKERS"
        ),
        "report_id": stored.report_id,
        "plan_id": report.plan_id,
        "identity_registry_id": report.identity_registry_id,
        "adjudication_queue_id": report.adjudication_queue_id,
        "generated_at": report.generated_at.isoformat(),
        "blocker_count": report.record_count,
        "candidate_count": report.candidate_count,
        "adjudication_case_count": report.adjudication_case_count,
        "blockers_by_code": dict(sorted(issue_counts.items())),
        "actions": dict(sorted(action_counts.items())),
        "requirements": dict(sorted(requirement_counts.items())),
        "policy_version": HISTORICAL_BACKFILL_BLOCKER_POLICY_VERSION,
        "actionable": report.actionable,
        "evidence_satisfied": report.evidence_satisfied,
    }


def _evidence_worklist(
    args: argparse.Namespace,
) -> tuple[int, dict[str, object]]:
    market_config = MarketDataConfig.from_env()
    blocker_report = LocalHistoricalBackfillBlockerReportStore(
        market_config.data_root
    ).get(args.blocker_report_id)
    reference_config = ReferenceDataConfig.from_env()
    identity_config = IdentityRegistryConfig.from_env()
    identity_store = LocalIdentityRegistryStore(
        identity_config.data_root,
        reference_config.data_root,
    )
    registry = identity_store.get(
        blocker_report.identity_registry_id
    ).registry
    queue = LocalIdentityAdjudicationQueueStore(
        identity_store.root,
        identity_store,
    ).get(registry.registry_id)
    package = build_historical_backfill_evidence_work_package(
        blocker_report=blocker_report,
        registry=registry,
        adjudication_queue=queue,
        generated_at=datetime.now(timezone.utc),
    )
    store = LocalHistoricalBackfillEvidenceWorkPackageStore(
        market_config.data_root
    )
    stored = store.put(package)
    requirement_counts = Counter(
        requirement.value
        for request in stored.case_requests
        for requirement in request.requirements
    )
    document_need_counts = Counter(
        need.value
        for request in stored.case_requests
        for need in request.document_needs
    )
    document_need_counts.update(
        need.value
        for request in stored.operational_requests
        for need in request.document_needs
    )
    return 0, {
        "status": "BACKFILL_EVIDENCE_WORKLIST_READY",
        "package_id": stored.package_id,
        "blocker_report_id": stored.blocker_report_id,
        "plan_id": stored.plan_id,
        "identity_registry_id": stored.identity_registry_id,
        "adjudication_queue_id": stored.adjudication_queue_id,
        "generated_at": stored.generated_at.isoformat(),
        "candidate_count": stored.candidate_count,
        "observation_count": stored.observation_count,
        "requirement_pair_count": stored.requirement_pair_count,
        "operational_request_count": len(stored.operational_requests),
        "requirements": dict(sorted(requirement_counts.items())),
        "recommended_document_needs": dict(
            sorted(document_need_counts.items())
        ),
        "csv_path": str(store.worklist_path(stored.package_id).resolve()),
        "policy_version": (
            HISTORICAL_BACKFILL_EVIDENCE_WORK_PACKAGE_POLICY_VERSION
        ),
        "actionable": stored.actionable,
        "evidence_satisfied": stored.evidence_satisfied,
    }


def main(argv: Sequence[str] | None = None) -> int:
    args = parser().parse_args(argv)
    try:
        if args.command == "plan":
            plan = _configured_plan(args)
            exit_code = 0
            value = {
                "status": (
                    "PLAN_READY"
                    if not plan.has_coverage_issues
                    else "PLAN_HAS_COVERAGE_ISSUES"
                ),
                **_plan_value(plan),
            }
        elif args.command == "run":
            exit_code, value = _run_plan(_configured_plan(args), args)
        elif args.command == "pilot":
            exit_code, value = _pilot(_configured_plan(args), args)
        elif args.command == "reconcile":
            exit_code, value = _reconcile(args)
        elif args.command == "blockers":
            exit_code, value = _blockers(args)
        elif args.command == "evidence-worklist":
            exit_code, value = _evidence_worklist(args)
        elif args.command == "catalog-fetch":
            exit_code, value = _catalog_fetch()
        elif args.command == "catalog-import":
            exit_code, value = _catalog_import(args)
        else:
            exit_code, value = _kite_instruments_fetch(args)
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
    print(json.dumps(value, indent=2, sort_keys=True))
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
