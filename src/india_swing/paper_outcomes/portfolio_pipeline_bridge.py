from __future__ import annotations

import re
from dataclasses import dataclass
from decimal import Decimal

from india_swing.calendar_data.materialization_store import LocalCalendarMaterializationStore
from india_swing.daily_pipeline.derived_evidence import (
    DailyDerivedEvidence,
    validate_daily_derived_evidence,
)
from india_swing.daily_pipeline.derived_evidence_store import LocalDailyDerivedEvidenceStore
from india_swing.daily_pipeline.store import LocalDailyPipelineRunStore
from india_swing.historical_prices import LocalHistoricalPriceArtifactStore
from india_swing.paper_trades import LocalPaperTradeLedger, PaperTradeStatus
from india_swing.reference_data.artifact_store import LocalReferenceArtifactStore
from india_swing.tick_sizes import LocalTickSizeSnapshotStore, materialize_collection_tick_sizes

from .models import PaperOutcomePolicy
from .operational import LocalPaperOutcomeEvidenceSource
from .portfolio import LocalPaperPortfolioStateStore, PaperPortfolioBatchSpec
from .portfolio_preparation import (
    LocalPaperPortfolioBatchStore,
    LocalPaperPortfolioPreparationStore,
    PaperPortfolioPreparationError,
    PaperPortfolioPreparationSpec,
    PaperRegistrationListing,
    prepare_paper_portfolio_batch,
)


_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_ACTIVE = frozenset({PaperTradeStatus.ALERTED, PaperTradeStatus.OPEN})
_ALLOWED_SERIES = "EQ"


class PaperPortfolioPipelineBridgeError(PaperPortfolioPreparationError):
    pass


def _sha(value: object, name: str) -> str:
    if type(value) is not str or _SHA256.fullmatch(value) is None:
        raise PaperPortfolioPipelineBridgeError(f"{name} must be a lowercase SHA-256")
    return value


@dataclass(frozen=True, slots=True)
class PreparedPaperPortfolioFromDailyPipeline:
    run_id: str
    derived_evidence_id: str
    preparation: PaperPortfolioPreparationSpec
    batch: PaperPortfolioBatchSpec

    def __post_init__(self) -> None:
        _sha(self.run_id, "run_id")
        _sha(self.derived_evidence_id, "derived_evidence_id")
        if type(self.preparation) is not PaperPortfolioPreparationSpec:
            raise PaperPortfolioPipelineBridgeError("bridge preparation must be exact")
        if type(self.batch) is not PaperPortfolioBatchSpec:
            raise PaperPortfolioPipelineBridgeError("bridge batch must be exact")
        self.preparation.verify_content_identity()
        self.batch.verify_content_identity()
        if (
            self.batch.as_of != self.preparation.as_of
            or self.batch.previous_batch_id != self.preparation.previous_batch_id
            or self.batch.expected_previous_state_id
            != self.preparation.expected_previous_state_id
            or self.batch.daily_loss_limit != self.preparation.daily_loss_limit
            or self.batch.cumulative_loss_limit
            != self.preparation.cumulative_loss_limit
        ):
            raise PaperPortfolioPipelineBridgeError("bridge result lineage differs")


def prepare_paper_portfolio_from_daily_pipeline(
    *,
    run_id: str,
    derived_evidence_id: str,
    ledger: LocalPaperTradeLedger,
    run_store: LocalDailyPipelineRunStore,
    derived_store: LocalDailyDerivedEvidenceStore,
    calendar_store: LocalCalendarMaterializationStore,
    tick_store: LocalTickSizeSnapshotStore,
    historical_store: LocalHistoricalPriceArtifactStore,
    reference_store: LocalReferenceArtifactStore,
    portfolio_store: LocalPaperPortfolioStateStore,
    preparation_store: LocalPaperPortfolioPreparationStore,
    batch_store: LocalPaperPortfolioBatchStore,
    policy: PaperOutcomePolicy | None = None,
    daily_loss_limit: Decimal = Decimal("1000"),
    cumulative_loss_limit: Decimal = Decimal("2000"),
) -> PreparedPaperPortfolioFromDailyPipeline:
    """Seal a portfolio batch from one exact daily run and derived bundle.

    This function performs no listing-based selection. The run and derived
    evidence IDs are caller-pinned; predecessor selection is allowed only when
    the complete local portfolio state graph has exactly one leaf.
    """

    _sha(run_id, "run_id")
    _sha(derived_evidence_id, "derived_evidence_id")
    expected_types = (
        (ledger, LocalPaperTradeLedger),
        (run_store, LocalDailyPipelineRunStore),
        (derived_store, LocalDailyDerivedEvidenceStore),
        (calendar_store, LocalCalendarMaterializationStore),
        (tick_store, LocalTickSizeSnapshotStore),
        (historical_store, LocalHistoricalPriceArtifactStore),
        (reference_store, LocalReferenceArtifactStore),
        (portfolio_store, LocalPaperPortfolioStateStore),
        (preparation_store, LocalPaperPortfolioPreparationStore),
        (batch_store, LocalPaperPortfolioBatchStore),
    )
    if any(type(value) is not expected for value, expected in expected_types):
        raise PaperPortfolioPipelineBridgeError("bridge stores must be exact")
    if policy is None:
        policy = PaperOutcomePolicy()
    if type(policy) is not PaperOutcomePolicy:
        raise PaperPortfolioPipelineBridgeError("bridge policy must be exact")
    policy.verify_content_identity()
    try:
        run = run_store.get(run_id)
        derived = derived_store.get(derived_evidence_id)
        if type(derived) is not DailyDerivedEvidence:
            raise ValueError
        runs = validate_daily_derived_evidence(derived, run=run, run_store=run_store)
        if derived.cutoff != run.cutoff:
            raise ValueError

        stored_calendar = calendar_store.get(run.calendar_materialization_id)
        calendar = stored_calendar.materialization
        calendar.verify_content_identity()
        if (
            calendar.materialization_id != run.calendar_materialization_id
            or calendar.calendar_snapshot.snapshot_id != run.calendar_snapshot_id
            or calendar.calendar_snapshot.snapshot_id != derived.calendar_snapshot_id
        ):
            raise ValueError

        decision_ticks = tuple(
            tick_store.put(
                materialize_collection_tick_sizes(
                    reference_store.get(item.current_security_master_artifact_id),
                    cutoff=item.cutoff,
                )
            )
            for item in runs
        )
        tick = decision_ticks[-1]
        tick.verify_content_identity()
        if (
            tick.snapshot_id != derived.tick_size_snapshot_id
            or tick.market_session_claim != run.market_session
            or tick.cutoff != run.cutoff
            or tick.knowledge_time > run.cutoff
        ):
            raise ValueError

        artifacts = tuple(
            historical_store.get(value).artifact
            for value in derived.historical_price_artifact_ids
        )
        for artifact in artifacts:
            artifact.verify_content_identity()
            if artifact.knowledge_time > run.cutoff:
                raise ValueError
        if (
            tuple(value.artifact_id for value in artifacts)
            != derived.historical_price_artifact_ids
            or tuple(value.market_session for value in artifacts)
            != tuple(sorted({value.market_session for value in artifacts}))
            or artifacts[-1].market_session != run.market_session
        ):
            raise ValueError

        registrations = ledger.list_registrations()
        active = tuple(
            value
            for value in registrations
            if ledger.summary(value.registration_id).status in _ACTIVE
        )
        if not active:
            raise PaperPortfolioPipelineBridgeError("no active paper registrations exist")

        states = portfolio_store.list_states()
        previous_batch_id = None
        previous_state_id = None
        prior_jobs = {}
        if states:
            predecessor_ids = {
                value.previous_state_id
                for value in states
                if value.previous_state_id is not None
            }
            leaves = tuple(value for value in states if value.state_id not in predecessor_ids)
            if len(leaves) != 1:
                raise PaperPortfolioPipelineBridgeError(
                    "portfolio state graph does not have one leaf"
                )
            leaf = leaves[0]
            prior_batch = batch_store.get(leaf.batch_id)
            if tuple(sorted(value.job_spec_id for value in prior_batch.outcome_jobs)) != leaf.outcome_job_spec_ids:
                raise ValueError
            previous_batch_id = leaf.batch_id
            previous_state_id = leaf.state_id
            prior_jobs = {
                value.registration_id: value for value in prior_batch.outcome_jobs
            }

        listings = []
        for registration in active:
            prior = prior_jobs.get(registration.registration_id)
            if prior is not None:
                decision_tick = tick_store.get(prior.tick_snapshot_id)
                decision_tick.verify_content_identity()
                if decision_tick.knowledge_time > registration.decision_time:
                    raise ValueError
            else:
                eligible_ticks = tuple(
                    value
                    for value in decision_ticks
                    if value.knowledge_time <= registration.decision_time
                )
                if not eligible_ticks:
                    raise PaperPortfolioPipelineBridgeError(
                        "active paper registration lacks decision-time tick evidence"
                    )
                decision_tick = eligible_ticks[-1]
            matches = tuple(
                value
                for value in decision_tick.observations
                if value.symbol == registration.symbol
                and value.series == _ALLOWED_SERIES
                and value.validated_isin is not None
            )
            if len(matches) != 1:
                raise PaperPortfolioPipelineBridgeError(
                    "active paper registration has ambiguous listing evidence"
                )
            observation = matches[0]
            if prior is not None and (
                prior.tick_snapshot_id != decision_tick.snapshot_id
                or prior.series != observation.series
                or prior.validated_isin != observation.validated_isin
            ):
                raise PaperPortfolioPipelineBridgeError(
                    "active paper registration listing identity changed"
                )
            listings.append(
                PaperRegistrationListing(
                    registration_id=registration.registration_id,
                    tick_snapshot_id=decision_tick.snapshot_id,
                    series=observation.series,
                    validated_isin=observation.validated_isin,
                )
            )

        preparation = PaperPortfolioPreparationSpec(
            as_of=run.cutoff,
            calendar_materialization_id=run.calendar_materialization_id,
            historical_artifact_ids=derived.historical_price_artifact_ids,
            listings=tuple(sorted(listings, key=lambda value: value.registration_id)),
            policy=policy,
            previous_batch_id=previous_batch_id,
            expected_previous_state_id=previous_state_id,
            daily_loss_limit=daily_loss_limit,
            cumulative_loss_limit=cumulative_loss_limit,
        )
        preparation = preparation_store.put(preparation)
        source = LocalPaperOutcomeEvidenceSource(
            paper_ledger=ledger,
            calendar_store=calendar_store,
            tick_store=tick_store,
            historical_store=historical_store,
        )
        batch = prepare_paper_portfolio_batch(
            spec=preparation,
            ledger=ledger,
            evidence_source=source,
            portfolio_store=portfolio_store,
        )
        batch = batch_store.put(batch)
        return PreparedPaperPortfolioFromDailyPipeline(
            run_id=run.run_id,
            derived_evidence_id=derived.evidence_id,
            preparation=preparation,
            batch=batch,
        )
    except PaperPortfolioPipelineBridgeError:
        raise
    except Exception:
        raise PaperPortfolioPipelineBridgeError(
            "daily pipeline portfolio preparation failed safely"
        ) from None
