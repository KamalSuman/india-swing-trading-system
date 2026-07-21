from __future__ import annotations

import os
import stat
from collections.abc import Callable
from datetime import datetime
from pathlib import Path

from india_swing.market_data.kite import KiteMarketDataAdapter
from india_swing.paper_trades import LocalPaperTradeLedger
from india_swing.recommendations import LocalSwingDecisionOutbox
from india_swing.signals.proposal_artifacts import LocalSwingProposalBatchStore
from india_swing.signals.proposal_parent_store import LocalSwingProposalParentStore

from .job_spec import SwingOperationalJobSpec, default_job_policies
from .models import SwingOperationalRunRecord, SwingOperationalStatus
from .portfolio_store import (
    LocalSwingPortfolioArtifactStore,
    StoredSwingPortfolioSource,
)
from .runner import KiteSwingQuoteSource
from .service import build_stored_swing_operational_run_spec
from .store import (
    LocalSwingOperationalRunStore,
    SwingOperationalRunNotFound,
    run_and_publish_swing_operation,
)


_CONCRETE_PATH_TYPE: type = type(Path())


class SwingOperationalJobError(RuntimeError):
    pass


def _is_link_like(path: Path) -> bool:
    try:
        status = os.lstat(path)
    except OSError:
        return path.is_symlink()
    return path.is_symlink() or bool(
        getattr(status, "st_file_attributes", 0)
        & getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400)
    )


def _validated_state_root(value: object) -> Path:
    if (
        type(value) is not _CONCRETE_PATH_TYPE
        or not value.is_absolute()
        or ".." in value.parts
        or value.parent == value
    ):
        raise SwingOperationalJobError("operational state root is invalid")
    try:
        if not value.exists() or not value.is_dir() or _is_link_like(value):
            raise SwingOperationalJobError("operational state root is invalid")
        resolved = value.resolve()
    except SwingOperationalJobError:
        raise
    except OSError:
        raise SwingOperationalJobError("operational state root is unavailable") from None
    if resolved != value:
        raise SwingOperationalJobError("operational state root must be canonical")
    return value


def validate_swing_operational_state_root(value: object) -> Path:
    return _validated_state_root(value)


def run_swing_operational_job(
    *,
    job_spec: SwingOperationalJobSpec,
    state_root: Path,
    kite_adapter: KiteMarketDataAdapter,
    clock: Callable[[], datetime],
) -> SwingOperationalRunRecord:
    """One restart-safe, idempotent, paper-only scheduled job invocation."""

    if type(job_spec) is not SwingOperationalJobSpec:
        raise SwingOperationalJobError("job spec must be exact")
    if type(kite_adapter) is not KiteMarketDataAdapter:
        raise SwingOperationalJobError("Kite adapter must be exact and read-only")
    if not callable(clock):
        raise SwingOperationalJobError("operational clock is required")
    job_spec.verify_content_identity()
    state_root = _validated_state_root(state_root)

    graph_root = state_root / "proposal_graph"
    proposal_store = LocalSwingProposalBatchStore(graph_root)
    parent_store = LocalSwingProposalParentStore(graph_root)
    quote_policy, ranking_policy, sizing_policy = default_job_policies()
    try:
        operational_spec = build_stored_swing_operational_run_spec(
            proposal_batch_id=job_spec.proposal_batch_id,
            proposal_store=proposal_store,
            proposal_resolver=parent_store,
            quote_policy=quote_policy,
            ranking_policy=ranking_policy,
            sizing_policy=sizing_policy,
            quote_chunk_size=job_spec.quote_chunk_size,
        )
    except Exception:
        raise SwingOperationalJobError("proposal graph could not be loaded safely") from None

    if (
        operational_spec.spec_id != job_spec.expected_operational_spec_id
        or operational_spec.target_session != job_spec.target_session
        or operational_spec.decision_not_before != job_spec.decision_not_before
        or operational_spec.decision_deadline != job_spec.decision_deadline
        or operational_spec.quote_policy.policy_id != job_spec.quote_policy_id
        or operational_spec.ranking_policy.policy_id != job_spec.ranking_policy_id
        or operational_spec.sizing_policy.policy_id != job_spec.sizing_policy_id
    ):
        raise SwingOperationalJobError("job spec differs from the reconstructed run spec")

    portfolio_source = StoredSwingPortfolioSource(
        store=LocalSwingPortfolioArtifactStore(state_root / "portfolio"),
        artifact_id=job_spec.portfolio_artifact_id,
        expected_portfolio_snapshot_id=job_spec.portfolio_snapshot_id,
        decision_not_before=job_spec.decision_not_before,
        decision_deadline=job_spec.decision_deadline,
        maximum_age_seconds=job_spec.maximum_portfolio_age_seconds,
    )
    try:
        portfolio_source.read_portfolio()
    except Exception:
        raise SwingOperationalJobError("portfolio artifact could not be loaded safely") from None

    run_store = LocalSwingOperationalRunStore(state_root / "operational")
    decision_outbox = LocalSwingDecisionOutbox(state_root / "decision_outbox")
    paper_ledger = LocalPaperTradeLedger(state_root / "paper")
    try:
        existing = run_store.get(operational_spec.spec_id)
    except SwingOperationalRunNotFound:
        existing = None
    except Exception:
        raise SwingOperationalJobError("terminal operational state is invalid") from None
    if existing is not None:
        if (
            existing.proposal_batch_id != job_spec.proposal_batch_id
            or (
                existing.portfolio_snapshot_id is not None
                and existing.portfolio_snapshot_id != job_spec.portfolio_snapshot_id
            )
        ):
            raise SwingOperationalJobError("existing terminal run differs from the job spec")
        if existing.status is SwingOperationalStatus.COMPLETE:
            try:
                if existing.decision_id is None:
                    raise ValueError
                notification = decision_outbox.get(existing.decision_id)
                if (
                    notification.notification_id != existing.notification_id
                    or notification.message != existing.message
                    or notification.message_sha256 != existing.message_sha256
                ):
                    raise ValueError
                if existing.paper_registration_id is not None:
                    registration = paper_ledger.get_registration(
                        existing.paper_registration_id
                    )
                    if (
                        registration.alert_id != existing.notification_id
                        or registration.source_run_id != existing.spec_id
                        or registration.source_pipeline_integrity_hash
                        != existing.package_id
                        or registration.source_decision_integrity_hash
                        != existing.decision_id
                    ):
                        raise ValueError
            except Exception:
                raise SwingOperationalJobError(
                    "existing terminal side effects are invalid"
                ) from None
        return existing

    try:
        _, record = run_and_publish_swing_operation(
            spec=operational_spec,
            quote_source=KiteSwingQuoteSource(kite_adapter),
            portfolio_source=portfolio_source,
            clock=clock,
            run_store=run_store,
            decision_outbox=decision_outbox,
            paper_ledger=paper_ledger,
        )
    except Exception:
        raise SwingOperationalJobError("operational job execution failed safely") from None
    if (
        record.spec_id != job_spec.expected_operational_spec_id
        or record.proposal_batch_id != job_spec.proposal_batch_id
        or (
            record.portfolio_snapshot_id is not None
            and record.portfolio_snapshot_id != job_spec.portfolio_snapshot_id
        )
    ):
        raise SwingOperationalJobError("terminal operational record differs from the job spec")
    return record
