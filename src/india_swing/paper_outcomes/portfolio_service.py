from __future__ import annotations

import hashlib
import re
from collections.abc import Callable
from dataclasses import dataclass, field, fields
from datetime import datetime, timezone
from pathlib import Path

from india_swing.calendar_data.materialization_store import (
    LocalCalendarMaterializationStore,
)
from india_swing.daily_pipeline.state_publication import StateObjectWriter
from india_swing.historical_prices import LocalHistoricalPriceArtifactStore
from india_swing.notifications import (
    LocalTelegramDeliveryReceiptStore,
    TelegramBotConfig,
    TelegramDeliveryReceipt,
    TelegramDeliveryRequest,
    TelegramHTTPTransport,
    deliver_telegram_notification,
)
from india_swing.operations.job import validate_swing_operational_state_root
from india_swing.operations.portfolio_store import LocalSwingPortfolioArtifactStore
from india_swing.identity import content_id
from india_swing.paper_trades import LocalPaperTradeLedger
from india_swing.tick_sizes import LocalTickSizeSnapshotStore

from .gcs_state import (
    CompletedPaperOutcomeStatePublication,
    publish_paper_outcome_state_to_gcs,
    validate_paper_outcome_state_bucket,
)
from .models import PaperOutcomeStatus
from .operational import (
    LocalPaperOutcomeEvidenceSource,
    LocalPaperOutcomeRunStore,
    PaperOutcomeEvidenceSource,
)
from .portfolio import (
    LocalPaperPortfolioStateStore,
    PaperPortfolioBatchSpec,
    PaperPortfolioError,
    PaperPortfolioState,
    run_paper_portfolio_batch,
)
from .portfolio_gcs import (
    CompletedPaperPortfolioPublication,
    publish_paper_portfolio_state,
)
from .portfolio_rollover import (
    LocalPaperPortfolioRolloverStore,
    PaperPortfolioMark,
    PaperPortfolioRollover,
    build_paper_portfolio_mark,
    roll_paper_portfolio,
)
from .portfolio_rollover_gcs import (
    CompletedPaperPortfolioRolloverPublication,
    publish_paper_portfolio_rollover,
)


_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_ROLLOVER_REQUEST_SCHEMA = "paper-portfolio-rollover-request/v1"
_ROLLOVER_LINEAGE_SCHEMA = "paper-portfolio-rollover-lineage/v1"


class PaperPortfolioServiceError(PaperPortfolioError):
    pass


@dataclass(frozen=True, slots=True)
class PaperPortfolioRolloverLineage:
    """The only local identities from which a rollover request may be prepared."""

    genesis_artifact_id: str
    previous_rollover_id: str | None
    schema_version: str = _ROLLOVER_LINEAGE_SCHEMA
    lineage_id: str = field(init=False)

    def __post_init__(self) -> None:
        if (
            type(self.genesis_artifact_id) is not str
            or _SHA256.fullmatch(self.genesis_artifact_id) is None
        ):
            raise PaperPortfolioServiceError("rollover genesis ID is invalid")
        if self.previous_rollover_id is not None and (
            type(self.previous_rollover_id) is not str
            or _SHA256.fullmatch(self.previous_rollover_id) is None
        ):
            raise PaperPortfolioServiceError("rollover predecessor ID is invalid")
        if self.schema_version != _ROLLOVER_LINEAGE_SCHEMA:
            raise PaperPortfolioServiceError("rollover lineage schema is unsupported")
        object.__setattr__(
            self,
            "lineage_id",
            content_id(
                {
                    "genesis_artifact_id": self.genesis_artifact_id,
                    "previous_rollover_id": self.previous_rollover_id,
                    "schema_version": self.schema_version,
                },
                length=64,
            ),
        )

    def verify_content_identity(self) -> None:
        try:
            fresh = PaperPortfolioRolloverLineage(
                genesis_artifact_id=self.genesis_artifact_id,
                previous_rollover_id=self.previous_rollover_id,
                schema_version=self.schema_version,
            )
        except Exception:
            raise PaperPortfolioServiceError(
                "rollover lineage identity failed"
            ) from None
        if fresh.lineage_id != self.lineage_id:
            raise PaperPortfolioServiceError("rollover lineage identity failed")


@dataclass(frozen=True, slots=True)
class PaperPortfolioRolloverRequest:
    """Exact, caller-sealed authority to close one portfolio batch.

    Marks remain explicit inputs.  The service never discovers a latest price,
    genesis, or predecessor on its own.
    """

    genesis_artifact_id: str
    previous_rollover_id: str | None
    marks: tuple[PaperPortfolioMark, ...]
    as_of: datetime
    schema_version: str = _ROLLOVER_REQUEST_SCHEMA
    request_id: str = field(init=False)

    def __post_init__(self) -> None:
        if (
            type(self.genesis_artifact_id) is not str
            or _SHA256.fullmatch(self.genesis_artifact_id) is None
        ):
            raise PaperPortfolioServiceError("rollover genesis ID is invalid")
        if self.previous_rollover_id is not None and (
            type(self.previous_rollover_id) is not str
            or _SHA256.fullmatch(self.previous_rollover_id) is None
        ):
            raise PaperPortfolioServiceError("rollover predecessor ID is invalid")
        if (
            type(self.marks) is not tuple
            or any(type(value) is not PaperPortfolioMark for value in self.marks)
        ):
            raise PaperPortfolioServiceError("rollover marks must be an exact tuple")
        for value in self.marks:
            value.verify_content_identity()
        if tuple(value.registration_id for value in self.marks) != tuple(
            sorted({value.registration_id for value in self.marks})
        ):
            raise PaperPortfolioServiceError("rollover marks are not canonical")
        if (
            type(self.as_of) is not datetime
            or self.as_of.tzinfo is None
            or self.as_of.utcoffset() is None
        ):
            raise PaperPortfolioServiceError("rollover cutoff must be timezone-aware")
        object.__setattr__(self, "as_of", self.as_of.astimezone(timezone.utc))
        if self.schema_version != _ROLLOVER_REQUEST_SCHEMA:
            raise PaperPortfolioServiceError("rollover request schema is unsupported")
        object.__setattr__(self, "request_id", self._calculated_id())

    def _calculated_id(self) -> str:
        return content_id(
            {
                item.name: getattr(self, item.name)
                for item in fields(self)
                if item.name != "request_id"
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        try:
            for value in self.marks:
                value.verify_content_identity()
            fresh = PaperPortfolioRolloverRequest(
                genesis_artifact_id=self.genesis_artifact_id,
                previous_rollover_id=self.previous_rollover_id,
                marks=self.marks,
                as_of=self.as_of,
                schema_version=self.schema_version,
            )
        except Exception:
            raise PaperPortfolioServiceError(
                "rollover request identity failed"
            ) from None
        if fresh.request_id != self.request_id:
            raise PaperPortfolioServiceError("rollover request identity failed")


def prepare_paper_portfolio_rollover_request(
    *,
    state: PaperPortfolioState,
    spec: PaperPortfolioBatchSpec,
    lineage: PaperPortfolioRolloverLineage,
    evidence_source: PaperOutcomeEvidenceSource,
    outcome_store: LocalPaperOutcomeRunStore,
) -> PaperPortfolioRolloverRequest:
    """Seal exact EOD marks from the already pinned outcome evidence.

    The terminal observation in each exact job specification is required to be
    traded and known by the batch cutoff.  A missing terminal bar is never
    replaced by an older price.
    """

    if (
        type(state) is not PaperPortfolioState
        or type(spec) is not PaperPortfolioBatchSpec
        or type(lineage) is not PaperPortfolioRolloverLineage
        or type(outcome_store) is not LocalPaperOutcomeRunStore
    ):
        raise PaperPortfolioServiceError("rollover preparation inputs must be exact")
    if not callable(getattr(evidence_source, "load", None)):
        raise PaperPortfolioServiceError("rollover evidence source is invalid")
    try:
        state.verify_content_identity()
        spec.verify_content_identity()
        lineage.verify_content_identity()
    except Exception:
        raise PaperPortfolioServiceError(
            "rollover preparation identities are invalid"
        ) from None
    if (
        state.batch_id != spec.batch_id
        or state.as_of != spec.as_of
        or state.outcome_job_spec_ids
        != tuple(sorted(value.job_spec_id for value in spec.outcome_jobs))
    ):
        raise PaperPortfolioServiceError("rollover preparation batch lineage differs")
    if any(
        value.outcome_status
        in {PaperOutcomeStatus.WAITING, PaperOutcomeStatus.BLOCKED}
        for value in state.positions
    ):
        raise PaperPortfolioServiceError(
            "rollover preparation has unresolved positions"
        )
    jobs = {value.registration_id: value for value in spec.outcome_jobs}
    marks: list[PaperPortfolioMark] = []
    try:
        for position in state.positions:
            if position.outcome_status is not PaperOutcomeStatus.OPEN:
                continue
            job = jobs.get(position.registration_id)
            if job is None or job.job_spec_id != position.job_spec_id:
                raise ValueError
            record = outcome_store.get(job.job_spec_id)
            if (
                record.record_id != position.record_id
                or record.registration_id != position.registration_id
                or record.outcome_status is not PaperOutcomeStatus.OPEN
            ):
                raise ValueError
            evidence = evidence_source.load(job)
            if not evidence.observations:
                raise ValueError
            if (
                evidence.registration.registration_id != position.registration_id
                or evidence.binding.registration_id != position.registration_id
                or evidence.binding.symbol != position.symbol
                or evidence.binding.series != job.series
                or evidence.binding.validated_isin != job.validated_isin
                or evidence.observations
                != tuple(
                    sorted(
                        evidence.observations,
                        key=lambda value: value.market_session,
                    )
                )
                or len({value.market_session for value in evidence.observations})
                != len(evidence.observations)
            ):
                raise ValueError
            observation = evidence.observations[-1]
            if (
                observation.knowledge_time > state.as_of
                or observation.observation_id not in record.source_observation_ids
                or observation.artifact_id not in job.historical_artifact_ids
                or not observation.traded
                or observation.symbol != position.symbol
            ):
                raise ValueError
            marks.append(
                build_paper_portfolio_mark(
                    position=position,
                    listing_key=f"NSE:{position.symbol}",
                    observation=observation,
                )
            )
    except Exception:
        raise PaperPortfolioServiceError(
            "rollover marks could not be prepared safely"
        ) from None
    return PaperPortfolioRolloverRequest(
        genesis_artifact_id=lineage.genesis_artifact_id,
        previous_rollover_id=lineage.previous_rollover_id,
        marks=tuple(sorted(marks, key=lambda value: value.registration_id)),
        as_of=state.as_of,
    )


@dataclass(frozen=True, slots=True)
class CompletedPaperPortfolioRollover:
    request_id: str
    rollover: PaperPortfolioRollover
    publication: CompletedPaperPortfolioRolloverPublication

    def __post_init__(self) -> None:
        if type(self.request_id) is not str or _SHA256.fullmatch(self.request_id) is None:
            raise PaperPortfolioServiceError("completed rollover request ID is invalid")
        if (
            type(self.rollover) is not PaperPortfolioRollover
            or type(self.publication)
            is not CompletedPaperPortfolioRolloverPublication
        ):
            raise PaperPortfolioServiceError("completed rollover result is invalid")
        self.rollover.verify_content_identity()
        if (
            self.publication.manifest.state_id
            != self.rollover.paper_portfolio_state_id
            or self.publication.manifest.rollover_id != self.rollover.rollover_id
            or self.publication.manifest.portfolio_artifact_id
            != self.rollover.portfolio_artifact.artifact_id
        ):
            raise PaperPortfolioServiceError("completed rollover result differs")


def run_paper_portfolio_rollover_service(
    *,
    state: PaperPortfolioState,
    request: PaperPortfolioRolloverRequest,
    state_root: Path,
    bucket: str,
    writer: StateObjectWriter,
) -> CompletedPaperPortfolioRollover:
    """Persist and publish one exact rollover without discovering any input."""

    if type(state) is not PaperPortfolioState:
        raise PaperPortfolioServiceError("paper portfolio state must be exact")
    if type(request) is not PaperPortfolioRolloverRequest:
        raise PaperPortfolioServiceError("rollover request must be exact")
    try:
        state.verify_content_identity()
        request.verify_content_identity()
        state_root = validate_swing_operational_state_root(state_root)
        bucket = validate_paper_outcome_state_bucket(bucket)
    except Exception:
        raise PaperPortfolioServiceError("rollover service inputs are invalid") from None
    if request.as_of != state.as_of:
        raise PaperPortfolioServiceError("rollover cutoff differs from portfolio state")
    if not callable(getattr(writer, "create_or_verify", None)):
        raise PaperPortfolioServiceError("rollover writer is invalid")
    try:
        portfolio_artifact_store = LocalSwingPortfolioArtifactStore(
            state_root / "portfolio"
        )
        rollover_store = LocalPaperPortfolioRolloverStore(
            state_root / "paper_portfolio_rollovers"
        )
        genesis_artifact = portfolio_artifact_store.get(request.genesis_artifact_id)
        previous_rollover = (
            None
            if request.previous_rollover_id is None
            else rollover_store.get(request.previous_rollover_id)
        )
        rollover = roll_paper_portfolio(
            state=state,
            genesis_artifact=genesis_artifact,
            marks=request.marks,
            as_of=request.as_of,
            previous=previous_rollover,
        )
        stored_artifact = portfolio_artifact_store.put(rollover.portfolio_artifact)
        stored_rollover = rollover_store.put(rollover)
        if stored_artifact != rollover.portfolio_artifact or stored_rollover != rollover:
            raise ValueError
        publication = publish_paper_portfolio_rollover(
            rollover=rollover,
            bucket=bucket,
            writer=writer,
        )
        return CompletedPaperPortfolioRollover(
            request_id=request.request_id,
            rollover=rollover,
            publication=publication,
        )
    except PaperPortfolioServiceError:
        raise
    except Exception:
        raise PaperPortfolioServiceError(
            "paper portfolio rollover service failed safely"
        ) from None


@dataclass(frozen=True, slots=True)
class CompletedPaperPortfolioJob:
    state: PaperPortfolioState
    outcome_publications: tuple[CompletedPaperOutcomeStatePublication, ...]
    portfolio_publication: CompletedPaperPortfolioPublication
    telegram_receipt: TelegramDeliveryReceipt
    rollover_request_id: str | None = None
    rollover: PaperPortfolioRollover | None = None
    rollover_publication: CompletedPaperPortfolioRolloverPublication | None = None

    def __post_init__(self) -> None:
        if type(self.state) is not PaperPortfolioState:
            raise PaperPortfolioServiceError("completed portfolio state must be exact")
        self.state.verify_content_identity()
        if (
            type(self.outcome_publications) is not tuple
            or any(
                type(value) is not CompletedPaperOutcomeStatePublication
                for value in self.outcome_publications
            )
        ):
            raise PaperPortfolioServiceError("completed outcome publications are invalid")
        if type(self.portfolio_publication) is not CompletedPaperPortfolioPublication:
            raise PaperPortfolioServiceError("completed portfolio publication is invalid")
        if type(self.telegram_receipt) is not TelegramDeliveryReceipt:
            raise PaperPortfolioServiceError("completed Telegram receipt is invalid")
        self.telegram_receipt.verify_content_identity()
        rollover_values = (
            self.rollover_request_id,
            self.rollover,
            self.rollover_publication,
        )
        if any(value is None for value in rollover_values) and any(
            value is not None for value in rollover_values
        ):
            raise PaperPortfolioServiceError("completed rollover lineage is incomplete")
        if self.rollover is not None:
            if (
                type(self.rollover_request_id) is not str
                or _SHA256.fullmatch(self.rollover_request_id) is None
                or type(self.rollover) is not PaperPortfolioRollover
                or type(self.rollover_publication)
                is not CompletedPaperPortfolioRolloverPublication
            ):
                raise PaperPortfolioServiceError("completed rollover lineage is invalid")
            self.rollover.verify_content_identity()
            if (
                self.rollover.paper_portfolio_state_id != self.state.state_id
                or self.rollover.paper_portfolio_batch_id != self.state.batch_id
                or self.rollover_publication.manifest.state_id != self.state.state_id
                or self.rollover_publication.manifest.rollover_id
                != self.rollover.rollover_id
                or self.rollover_publication.manifest.portfolio_artifact_id
                != self.rollover.portfolio_artifact.artifact_id
            ):
                raise PaperPortfolioServiceError("completed rollover lineage differs")
        if (
            tuple(
                sorted(value.manifest.job_spec_id for value in self.outcome_publications)
            )
            != self.state.outcome_job_spec_ids
            or self.portfolio_publication.manifest.batch_id != self.state.batch_id
            or self.portfolio_publication.manifest.state_id != self.state.state_id
            or self.telegram_receipt.delivery_key != self.state.state_id
        ):
            raise PaperPortfolioServiceError("completed portfolio job lineage differs")


def run_paper_portfolio_operational_service(
    *,
    spec: PaperPortfolioBatchSpec,
    evidence_root: Path,
    state_root: Path,
    bucket: str,
    writer: StateObjectWriter,
    telegram_config: TelegramBotConfig,
    telegram_transport: TelegramHTTPTransport,
    clock: Callable[[], datetime],
    rollover_request: PaperPortfolioRolloverRequest | None = None,
    rollover_lineage: PaperPortfolioRolloverLineage | None = None,
) -> CompletedPaperPortfolioJob:
    """Execute, durably publish, and notify one exact paper portfolio batch.

    Every underlying mutation is create-once or append-only. A retry therefore
    reconstructs the same state, verifies the same GCS bytes, and reuses the
    Telegram receipt instead of creating another logical result.
    """

    if type(spec) is not PaperPortfolioBatchSpec:
        raise PaperPortfolioServiceError("paper portfolio batch spec must be exact")
    try:
        spec.verify_content_identity()
        evidence_root = validate_swing_operational_state_root(evidence_root)
        state_root = validate_swing_operational_state_root(state_root)
        bucket = validate_paper_outcome_state_bucket(bucket)
    except Exception:
        raise PaperPortfolioServiceError(
            "paper portfolio service inputs are invalid"
        ) from None
    if not callable(getattr(writer, "create_or_verify", None)):
        raise PaperPortfolioServiceError("paper portfolio writer is invalid")
    if type(telegram_config) is not TelegramBotConfig:
        raise PaperPortfolioServiceError("Telegram config must be exact")
    if not callable(getattr(telegram_transport, "post_json", None)):
        raise PaperPortfolioServiceError("Telegram transport is invalid")
    if not callable(clock):
        raise PaperPortfolioServiceError("paper portfolio clock is required")
    if rollover_request is not None and rollover_lineage is not None:
        raise PaperPortfolioServiceError("rollover authority is ambiguous")
    if rollover_request is not None:
        if type(rollover_request) is not PaperPortfolioRolloverRequest:
            raise PaperPortfolioServiceError("rollover request must be exact")
        try:
            rollover_request.verify_content_identity()
        except Exception:
            raise PaperPortfolioServiceError("rollover request is invalid") from None
        if rollover_request.as_of != spec.as_of:
            raise PaperPortfolioServiceError("rollover cutoff differs from its batch")
    if rollover_lineage is not None:
        if type(rollover_lineage) is not PaperPortfolioRolloverLineage:
            raise PaperPortfolioServiceError("rollover lineage must be exact")
        try:
            rollover_lineage.verify_content_identity()
        except Exception:
            raise PaperPortfolioServiceError("rollover lineage is invalid") from None

    try:
        ledger = LocalPaperTradeLedger(state_root / "paper")
        outcome_store = LocalPaperOutcomeRunStore(state_root / "paper_outcomes")
        portfolio_store = LocalPaperPortfolioStateStore(
            state_root / "paper_portfolio"
        )
        source = LocalPaperOutcomeEvidenceSource(
            paper_ledger=ledger,
            calendar_store=LocalCalendarMaterializationStore(
                evidence_root / "calendar_data", evidence_root / "daily_reports"
            ),
            tick_store=LocalTickSizeSnapshotStore(
                evidence_root / "tick_sizes", evidence_root / "reference_data"
            ),
            historical_store=LocalHistoricalPriceArtifactStore(
                evidence_root / "historical_prices", evidence_root / "daily_reports"
            ),
        )
        state = run_paper_portfolio_batch(
            spec=spec,
            evidence_source=source,
            ledger=ledger,
            outcome_store=outcome_store,
            portfolio_store=portfolio_store,
        )
        prepared_rollover_request = rollover_request
        if rollover_lineage is not None:
            prepared_rollover_request = prepare_paper_portfolio_rollover_request(
                state=state,
                spec=spec,
                lineage=rollover_lineage,
                evidence_source=source,
                outcome_store=outcome_store,
            )
        publications = tuple(
            publish_paper_outcome_state_to_gcs(
                record=outcome_store.get(job.job_spec_id),
                bucket=bucket,
                writer=writer,
                ledger=ledger,
            )
            for job in spec.outcome_jobs
        )
        portfolio_publication = publish_paper_portfolio_state(
            state=state,
            bucket=bucket,
            writer=writer,
        )
        rollover = None
        rollover_publication = None
        rollover_request_id = None
        if prepared_rollover_request is not None:
            completed_rollover = run_paper_portfolio_rollover_service(
                state=state,
                request=prepared_rollover_request,
                state_root=state_root,
                bucket=bucket,
                writer=writer,
            )
            rollover = completed_rollover.rollover
            rollover_publication = completed_rollover.publication
            rollover_request_id = completed_rollover.request_id
        text = state.report_message + f"\nPortfolio state ID: {state.state_id}\n"
        if rollover is not None:
            text += (
                f"Paper NAV: INR {rollover.nav}\n"
                f"Paper cash: INR {rollover.cash_available}\n"
                f"Paper rollover ID: {rollover.rollover_id}\n"
            )
        receipt = deliver_telegram_notification(
            request=TelegramDeliveryRequest(
                delivery_key=state.state_id,
                text=text,
                message_sha256=hashlib.sha256(text.encode("utf-8")).hexdigest(),
                category="PAPER_OUTCOME_RESULT",
            ),
            config=telegram_config,
            transport=telegram_transport,
            receipt_store=LocalTelegramDeliveryReceiptStore(
                state_root / "notification_delivery" / "telegram"
            ),
            clock=clock,
        )
        return CompletedPaperPortfolioJob(
            state=state,
            outcome_publications=publications,
            portfolio_publication=portfolio_publication,
            telegram_receipt=receipt,
            rollover_request_id=rollover_request_id,
            rollover=rollover,
            rollover_publication=rollover_publication,
        )
    except PaperPortfolioServiceError:
        raise
    except Exception:
        raise PaperPortfolioServiceError(
            "paper portfolio operational service failed safely"
        ) from None
