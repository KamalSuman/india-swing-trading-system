from __future__ import annotations

import os
import re
import tempfile
from contextlib import contextmanager
from datetime import date, datetime
from pathlib import Path
from typing import Iterator

from india_swing._filesystem import FileSafetyError, read_stable_regular_file
from india_swing.calendar_evidence import build_observed_market_date_artifact
from india_swing.daily_reports.artifact_store import LocalDailyBundleArtifactStore
from india_swing.daily_reports.parser import NSE_DAILY_BUNDLE_FILENAME
from india_swing.historical_prices.artifact_store import LocalHistoricalPriceArtifactStore
from india_swing.historical_prices.materialize import materialize_nse_eod_session
from india_swing.identity_registry.adjudication import build_identity_adjudication_queue
from india_swing.identity_registry.adjudication_store import LocalIdentityAdjudicationQueueStore
from india_swing.identity_registry.artifact_store import LocalIdentityRegistryStore
from india_swing.identity_registry.materialize import materialize_cross_vintage_identity_registry
from india_swing.reconciliation.reconciler import reconcile_collection_only
from india_swing.reference.calendar import CalendarSnapshot
from india_swing.reference_data.artifact_store import LocalReferenceArtifactStore

from .models import (
    VERIFIED_LANDING_LINEAGE_UNAVAILABLE,
    DailyPipelineIntegrityError,
    DailyPipelineRun,
)
from .store import LocalDailyPipelineRunStore


_BROWSER_RENAMED_BUNDLE = re.compile(r"Reports-Daily-Multiple \([1-9][0-9]*\)\.zip\Z")
_MAX_BUNDLE_BYTES = 128 * 1024 * 1024


@contextmanager
def _official_bundle_source(source: Path) -> Iterator[Path]:
    source = Path(source)
    if source.name == NSE_DAILY_BUNDLE_FILENAME:
        yield source
        return
    if _BROWSER_RENAMED_BUNDLE.fullmatch(source.name) is None:
        raise DailyPipelineIntegrityError(
            "daily bundle must use the official name or a recognized browser duplicate suffix"
        )
    try:
        payload = read_stable_regular_file(source, maximum_bytes=_MAX_BUNDLE_BYTES)
    except FileSafetyError as exc:
        raise DailyPipelineIntegrityError("daily bundle source is unsafe") from exc
    with tempfile.TemporaryDirectory(prefix="india-swing-daily-bundle-") as temporary:
        canonical = Path(temporary) / NSE_DAILY_BUNDLE_FILENAME
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_BINARY", 0)
        descriptor = os.open(canonical, flags, 0o600)
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        yield canonical


def run_daily_pipeline(
    *,
    market_session: date,
    cutoff: datetime,
    calendar_materialization_id: str,
    calendar: CalendarSnapshot,
    security_master_file: Path,
    daily_bundle_file: Path,
    previous_run_id: str | None,
    reference_store: LocalReferenceArtifactStore,
    daily_store: LocalDailyBundleArtifactStore,
    historical_store: LocalHistoricalPriceArtifactStore,
    identity_store: LocalIdentityRegistryStore,
    adjudication_store: LocalIdentityAdjudicationQueueStore,
    run_store: LocalDailyPipelineRunStore,
) -> DailyPipelineRun:
    if type(market_session) is not date:
        raise TypeError("market_session must be a date")
    if not isinstance(cutoff, datetime) or cutoff.tzinfo is None or cutoff.utcoffset() is None:
        raise DailyPipelineIntegrityError("cutoff must be timezone-aware")
    if type(calendar) is not CalendarSnapshot:
        raise TypeError("calendar must be exact")
    calendar.verify_content_identity()
    calendar.require_session(market_session)
    if calendar.cutoff > cutoff:
        raise DailyPipelineIntegrityError("calendar vintage follows the run cutoff")

    previous = None
    if previous_run_id is not None:
        previous = run_store.get(previous_run_id)
        expected_previous = calendar.previous_session(market_session).day
        if previous.market_session != expected_previous:
            raise DailyPipelineIntegrityError(
                "previous run is not the calendar-declared preceding session"
            )
        if previous.cutoff >= cutoff:
            raise DailyPipelineIntegrityError("previous run cutoff must precede current cutoff")

    current_master = reference_store.import_security_master(Path(security_master_file))
    if current_master.parsed.claimed_report_date != market_session:
        raise DailyPipelineIntegrityError(
            "security-master claimed date differs from the market session"
        )
    with _official_bundle_source(Path(daily_bundle_file)) as bundle_source:
        current_bundle = daily_store.import_bundle(bundle_source)

    previous_master_ids = () if previous is None else previous.security_master_artifact_ids
    previous_bundle_ids = () if previous is None else previous.daily_bundle_artifact_ids
    if current_master.manifest.artifact_id in previous_master_ids:
        raise DailyPipelineIntegrityError("current security master duplicates run history")
    if current_bundle.manifest.artifact_id in previous_bundle_ids:
        raise DailyPipelineIntegrityError("current daily bundle duplicates run history")
    master_ids = previous_master_ids + (current_master.manifest.artifact_id,)
    bundle_ids = previous_bundle_ids + (current_bundle.manifest.artifact_id,)

    observed = build_observed_market_date_artifact(current_bundle, cutoff=cutoff)
    if market_session not in observed.observed_dates:
        raise DailyPipelineIntegrityError(
            "current daily bundle does not confirm the requested market session"
        )
    historical = historical_store.put(
        materialize_nse_eod_session(
            current_bundle,
            market_session=market_session,
            cutoff=cutoff,
        )
    )
    bundles = tuple(daily_store.get(value) for value in bundle_ids)
    reconciliation = reconcile_collection_only(
        security_master=current_master,
        daily_bundles=bundles,
        market_session=market_session,
        cutoff=cutoff,
        calendar=calendar,
    )

    masters = tuple(reference_store.get(value) for value in master_ids)
    registry = identity_store.put(
        materialize_cross_vintage_identity_registry(
            sources=masters,
            cutoff=cutoff,
        )
    )
    queue = adjudication_store.publish(
        build_identity_adjudication_queue(registry.registry),
        registry_id=registry.registry.registry_id,
    )

    issues = set(reconciliation.global_reason_codes)
    issues.update(
        {
            "IDENTITY_ADJUDICATION_REQUIRED",
            "STABLE_IDENTITY_UNAVAILABLE",
            VERIFIED_LANDING_LINEAGE_UNAVAILABLE,
        }
    )
    if previous is None:
        issues.add("NO_PREVIOUS_DAILY_RUN")

    report = DailyPipelineRun(
        market_session=market_session,
        cutoff=cutoff,
        calendar_materialization_id=calendar_materialization_id,
        calendar_snapshot_id=calendar.snapshot_id,
        previous_run_id=previous_run_id,
        security_master_artifact_ids=master_ids,
        daily_bundle_artifact_ids=bundle_ids,
        current_security_master_artifact_id=current_master.manifest.artifact_id,
        current_daily_bundle_artifact_id=current_bundle.manifest.artifact_id,
        observed_date_artifact_id=observed.artifact_id,
        observed_dates=observed.observed_dates,
        historical_price_artifact_id=historical.manifest.artifact_id,
        historical_price_manifest_id=historical.manifest.manifest_id,
        bar_count=historical.manifest.bar_count,
        reconciliation_snapshot_id=reconciliation.snapshot_id,
        reconciliation_global_reason_codes=reconciliation.global_reason_codes,
        retained_row_count=reconciliation.retained_row_count,
        main_scope_count=reconciliation.main_scope_count,
        sme_scope_count=reconciliation.sme_scope_count,
        unsupported_series_count=reconciliation.unsupported_series_count,
        unresolved_count=reconciliation.unresolved_count,
        traded_row_count=reconciliation.traded_row_count,
        orphan_report_key_count=len(reconciliation.orphan_report_keys),
        identity_registry_id=registry.manifest.registry_id,
        identity_registry_manifest_id=registry.manifest.manifest_id,
        identity_observation_count=registry.manifest.observation_count,
        identity_candidate_count=registry.manifest.candidate_count,
        identity_transition_count=registry.manifest.transition_count,
        identity_conflict_count=registry.manifest.conflict_count,
        adjudication_queue_id=queue.queue_id,
        adjudication_case_count=len(queue.cases),
        adjudication_requirement_counts=queue.requirement_counts,
        completeness_issues=tuple(sorted(issues)),
        landing_input_lineage=None,
    )
    return run_store.publish(report)
