from __future__ import annotations

import hashlib
import json
import os
import sys
from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
from pathlib import Path

from india_swing.calendar_data.materialization_store import LocalCalendarMaterializationStore
from india_swing.daily_pipeline.state_publication import GoogleCloudStorageStateObjectWriter
from india_swing.historical_prices import LocalHistoricalPriceArtifactStore
from india_swing.notifications import (
    LocalTelegramDeliveryReceiptStore,
    TelegramBotConfig,
    TelegramDeliveryRequest,
    UrllibTelegramHTTPTransport,
    deliver_telegram_notification,
)
from india_swing.operations.job import validate_swing_operational_state_root
from india_swing.paper_outcomes import (
    LocalPaperOutcomeEvidenceSource,
    LocalPaperOutcomeRunStore,
    PaperOutcomeOperationalError,
    load_paper_outcome_job_spec_file,
    publish_paper_outcome_state_to_gcs,
    run_paper_outcome_job,
    validate_paper_outcome_state_bucket,
)
from india_swing.paper_trades import LocalPaperTradeLedger
from india_swing.tick_sizes import LocalTickSizeSnapshotStore


def _arguments(argv: Sequence[str]) -> tuple[Path, Path, Path]:
    values: dict[str, str] = {}
    index = 0
    allowed = {"--spec-file", "--evidence-root", "--state-root"}
    while index < len(argv):
        token = argv[index]
        if token not in allowed or token in values or index + 1 >= len(argv):
            raise PaperOutcomeOperationalError("invalid paper outcome job arguments")
        value = argv[index + 1]
        if type(value) is not str or not value:
            raise PaperOutcomeOperationalError("invalid paper outcome job arguments")
        values[token] = value
        index += 2
    if set(values) != allowed:
        raise PaperOutcomeOperationalError("paper outcome job arguments are incomplete")
    return (
        Path(values["--spec-file"]),
        Path(values["--evidence-root"]),
        Path(values["--state-root"]),
    )


def main(
    argv: Sequence[str] | None = None,
    *,
    environ: Mapping[str, str] | None = None,
) -> int:
    try:
        spec_path, evidence_root, state_root = _arguments(
            list(argv) if argv is not None else sys.argv[1:]
        )
        evidence_root = validate_swing_operational_state_root(evidence_root)
        state_root = validate_swing_operational_state_root(state_root)
        runtime = os.environ if environ is None else environ
        bucket = validate_paper_outcome_state_bucket(
            runtime.get("INDIA_SWING_PAPER_OUTCOME_STATE_BUCKET")
        )
        telegram_config = TelegramBotConfig.from_env(runtime)
        spec = load_paper_outcome_job_spec_file(spec_path)
        ledger = LocalPaperTradeLedger(state_root / "paper")
        record_store = LocalPaperOutcomeRunStore(state_root / "paper_outcomes")
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
        record = run_paper_outcome_job(
            spec=spec,
            evidence_source=source,
            ledger=ledger,
            record_store=record_store,
        )
        publication = publish_paper_outcome_state_to_gcs(
            record=record,
            bucket=bucket,
            writer=GoogleCloudStorageStateObjectWriter(),
            ledger=ledger,
        )
        telegram_text = record.message + f"\nOutcome record ID: {record.record_id}\n"
        receipt = deliver_telegram_notification(
            request=TelegramDeliveryRequest(
                delivery_key=record.record_id,
                text=telegram_text,
                message_sha256=hashlib.sha256(telegram_text.encode("utf-8")).hexdigest(),
                category="PAPER_OUTCOME_RESULT",
            ),
            config=telegram_config,
            transport=UrllibTelegramHTTPTransport(),
            receipt_store=LocalTelegramDeliveryReceiptStore(
                state_root / "notification_delivery" / "telegram"
            ),
            clock=lambda: datetime.now(timezone.utc),
        )
        print(
            json.dumps(
                {
                    "estimated_net_pnl": (
                        None
                        if record.estimated_net_pnl is None
                        else str(record.estimated_net_pnl)
                    ),
                    "job_spec_id": spec.job_spec_id,
                    "manifest_generation": publication.manifest_object.generation,
                    "manifest_object_name": publication.manifest_object.object_name,
                    "manifest_sha256": publication.manifest_object.sha256,
                    "outcome_status": record.outcome_status.value,
                    "publication_id": publication.manifest.publication_id,
                    "record_id": record.record_id,
                    "registration_id": record.registration_id,
                    "replay_id": record.replay_id,
                    "status": "COMPLETE",
                    "telegram_receipt_id": receipt.receipt_id,
                },
                allow_nan=False,
                ensure_ascii=False,
                separators=(",", ":"),
                sort_keys=True,
            )
        )
        return 0
    except Exception:
        print(
            json.dumps(
                {
                    "error_type": PaperOutcomeOperationalError.__name__,
                    "status": "FAILED",
                },
                separators=(",", ":"),
                sort_keys=True,
            ),
            file=sys.stderr,
        )
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
