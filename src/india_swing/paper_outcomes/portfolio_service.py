from __future__ import annotations

import hashlib
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
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
from india_swing.paper_trades import LocalPaperTradeLedger
from india_swing.tick_sizes import LocalTickSizeSnapshotStore

from .gcs_state import (
    CompletedPaperOutcomeStatePublication,
    publish_paper_outcome_state_to_gcs,
    validate_paper_outcome_state_bucket,
)
from .operational import LocalPaperOutcomeEvidenceSource, LocalPaperOutcomeRunStore
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


class PaperPortfolioServiceError(PaperPortfolioError):
    pass


@dataclass(frozen=True, slots=True)
class CompletedPaperPortfolioJob:
    state: PaperPortfolioState
    outcome_publications: tuple[CompletedPaperOutcomeStatePublication, ...]
    portfolio_publication: CompletedPaperPortfolioPublication
    telegram_receipt: TelegramDeliveryReceipt

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
        text = state.report_message + f"\nPortfolio state ID: {state.state_id}\n"
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
        )
    except PaperPortfolioServiceError:
        raise
    except Exception:
        raise PaperPortfolioServiceError(
            "paper portfolio operational service failed safely"
        ) from None
