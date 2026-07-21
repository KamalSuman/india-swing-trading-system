from __future__ import annotations

import hashlib
from collections.abc import Callable
from datetime import datetime
from pathlib import Path

from india_swing.calendar_data.materialization_store import LocalCalendarMaterializationStore
from india_swing.daily_pipeline.derived_evidence_store import LocalDailyDerivedEvidenceStore
from india_swing.daily_pipeline.state_publication import StateObjectWriter
from india_swing.daily_pipeline.store import LocalDailyPipelineRunStore
from india_swing.historical_prices import LocalHistoricalPriceArtifactStore
from india_swing.notifications import (
    LocalTelegramDeliveryReceiptStore,
    TelegramBotConfig,
    TelegramDeliveryRequest,
    TelegramHTTPTransport,
    deliver_telegram_notification,
)
from india_swing.operations.job import validate_swing_operational_state_root
from india_swing.paper_outcomes import (
    LocalPaperPortfolioBatchStore,
    LocalPaperPortfolioPreparationStore,
    LocalPaperPortfolioStateStore,
    PaperPortfolioPipelineBridgeError,
    prepare_paper_portfolio_from_daily_pipeline,
    run_paper_portfolio_operational_service,
)
from india_swing.paper_trades import LocalPaperTradeLedger, PaperTradeStatus
from india_swing.reference_data.artifact_store import LocalReferenceArtifactStore
from india_swing.tick_sizes import LocalTickSizeSnapshotStore

from .models import (
    DailyPaperWorkflowOutput,
    DailyPaperWorkflowOutputStatus,
    DailyPaperWorkflowSpec,
    PublishedManifestPin,
)
from .runner import DailyPaperWorkflowRejected


_ACTIVE = frozenset({PaperTradeStatus.ALERTED, PaperTradeStatus.OPEN})


class LocalDailyPaperWorkflowWorker:
    """Concrete paper-only adapter for one exact restored evidence/state tree."""

    def __init__(
        self,
        *,
        evidence_root: Path,
        state_root: Path,
        writer: StateObjectWriter,
        telegram_config: TelegramBotConfig,
        telegram_transport: TelegramHTTPTransport,
        clock: Callable[[], datetime],
    ) -> None:
        self.evidence_root = validate_swing_operational_state_root(evidence_root)
        self.state_root = validate_swing_operational_state_root(state_root)
        if not callable(getattr(writer, "create_or_verify", None)):
            raise ValueError("workflow state writer is invalid")
        if type(telegram_config) is not TelegramBotConfig:
            raise ValueError("workflow Telegram config must be exact")
        if not callable(getattr(telegram_transport, "post_json", None)):
            raise ValueError("workflow Telegram transport is invalid")
        if not callable(clock):
            raise ValueError("workflow clock is required")
        self.writer = writer
        self.telegram_config = telegram_config
        self.telegram_transport = telegram_transport
        self.clock = clock

    def _no_active_positions(self, spec: DailyPaperWorkflowSpec) -> DailyPaperWorkflowOutput:
        text = (
            "PAPER-ONLY RESEARCH SYSTEM — NOT INVESTMENT ADVICE\n"
            "Daily paper portfolio: no active positions require EOD replay.\n"
            f"Daily run ID: {spec.run_id}\n"
            f"Derived evidence ID: {spec.derived_evidence_id}\n"
            "No broker order was created or authorized.\n"
        )
        receipt = deliver_telegram_notification(
            request=TelegramDeliveryRequest(
                delivery_key=spec.workflow_id,
                text=text,
                message_sha256=hashlib.sha256(text.encode("utf-8")).hexdigest(),
                category="PAPER_OUTCOME_RESULT",
            ),
            config=self.telegram_config,
            transport=self.telegram_transport,
            receipt_store=LocalTelegramDeliveryReceiptStore(
                self.state_root / "notification_delivery" / "telegram"
            ),
            clock=self.clock,
        )
        return DailyPaperWorkflowOutput(
            status=DailyPaperWorkflowOutputStatus.NO_ACTIVE_POSITIONS,
            preparation_id=None,
            batch_id=None,
            state_id=None,
            outcome_manifest_pins=(),
            portfolio_manifest_pin=None,
            telegram_receipt_id=receipt.receipt_id,
        )

    def execute(self, spec: DailyPaperWorkflowSpec) -> DailyPaperWorkflowOutput:
        if type(spec) is not DailyPaperWorkflowSpec:
            raise ValueError("workflow spec must be exact")
        spec.verify_content_identity()
        ledger = LocalPaperTradeLedger(self.state_root / "paper")
        try:
            active = tuple(
                value
                for value in ledger.list_registrations()
                if ledger.summary(value.registration_id).status in _ACTIVE
            )
            if not active:
                return self._no_active_positions(spec)

            pipeline_root = self.evidence_root / "daily_pipeline"
            preparation_store = LocalPaperPortfolioPreparationStore(
                self.state_root / "paper_portfolio_preparations"
            )
            batch_store = LocalPaperPortfolioBatchStore(
                self.state_root / "paper_portfolio_batches"
            )
            prepared = prepare_paper_portfolio_from_daily_pipeline(
                run_id=spec.run_id,
                derived_evidence_id=spec.derived_evidence_id,
                ledger=ledger,
                run_store=LocalDailyPipelineRunStore(pipeline_root),
                derived_store=LocalDailyDerivedEvidenceStore(pipeline_root),
                calendar_store=LocalCalendarMaterializationStore(
                    self.evidence_root / "calendar_data",
                    self.evidence_root / "daily_reports",
                ),
                tick_store=LocalTickSizeSnapshotStore(
                    self.evidence_root / "tick_sizes",
                    self.evidence_root / "reference_data",
                ),
                historical_store=LocalHistoricalPriceArtifactStore(
                    self.evidence_root / "historical_prices",
                    self.evidence_root / "daily_reports",
                ),
                reference_store=LocalReferenceArtifactStore(
                    self.evidence_root / "reference_data"
                ),
                portfolio_store=LocalPaperPortfolioStateStore(
                    self.state_root / "paper_portfolio"
                ),
                preparation_store=preparation_store,
                batch_store=batch_store,
                daily_loss_limit=spec.daily_loss_limit,
                cumulative_loss_limit=spec.cumulative_loss_limit,
            )
        except PaperPortfolioPipelineBridgeError:
            raise DailyPaperWorkflowRejected("EVIDENCE_REJECTED") from None

        completed = run_paper_portfolio_operational_service(
            spec=prepared.batch,
            evidence_root=self.evidence_root,
            state_root=self.state_root,
            bucket=spec.state_bucket,
            writer=self.writer,
            telegram_config=self.telegram_config,
            telegram_transport=self.telegram_transport,
            clock=self.clock,
        )
        outcome_pins = tuple(
            sorted(
                (
                    PublishedManifestPin(
                        object_name=value.manifest_object.object_name,
                        generation=value.manifest_object.generation,
                        sha256=value.manifest_object.sha256,
                    )
                    for value in completed.outcome_publications
                ),
                key=lambda value: value.pin_id,
            )
        )
        portfolio_object = completed.portfolio_publication.manifest_object
        return DailyPaperWorkflowOutput(
            status=DailyPaperWorkflowOutputStatus.COMPLETE,
            preparation_id=prepared.preparation.preparation_id,
            batch_id=prepared.batch.batch_id,
            state_id=completed.state.state_id,
            outcome_manifest_pins=outcome_pins,
            portfolio_manifest_pin=PublishedManifestPin(
                object_name=portfolio_object.object_name,
                generation=portfolio_object.generation,
                sha256=portfolio_object.sha256,
            ),
            telegram_receipt_id=completed.telegram_receipt.receipt_id,
        )
