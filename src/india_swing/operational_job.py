from __future__ import annotations

import hashlib
import json
import os
import sys
from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
from pathlib import Path

from india_swing.market_data.config import KiteCredentials
from india_swing.market_data.kite import KiteMarketDataAdapter
from india_swing.notifications import (
    LocalTelegramDeliveryReceiptStore,
    TelegramBotConfig,
    TelegramDeliveryRequest,
    UrllibTelegramHTTPTransport,
    deliver_telegram_notification,
)
from india_swing.daily_pipeline.state_publication import (
    GoogleCloudStorageStateObjectWriter,
)
from india_swing.paper_trades import LocalPaperTradeLedger
from india_swing.recommendations import LocalSwingDecisionOutbox
from india_swing.operations.gcs_state import (
    publish_swing_operational_state_to_gcs,
    validate_operational_state_bucket,
)
from india_swing.operations.job import SwingOperationalJobError, run_swing_operational_job
from india_swing.operations.job_spec import load_swing_operational_job_spec_file


class SwingOperationalJobConfigurationError(SwingOperationalJobError):
    pass


def _arguments(argv: Sequence[str]) -> tuple[Path, Path]:
    values: dict[str, str] = {}
    index = 0
    while index < len(argv):
        token = argv[index]
        if token not in {"--spec-file", "--state-root"} or token in values:
            raise SwingOperationalJobConfigurationError("invalid operational job argument")
        if index + 1 >= len(argv):
            raise SwingOperationalJobConfigurationError("missing operational job argument")
        value = argv[index + 1]
        if type(value) is not str or not value:
            raise SwingOperationalJobConfigurationError("empty operational job argument")
        values[token] = value
        index += 2
    if set(values) != {"--spec-file", "--state-root"}:
        raise SwingOperationalJobConfigurationError("required operational job argument is absent")
    return Path(values["--spec-file"]), Path(values["--state-root"])


def main(
    argv: Sequence[str] | None = None,
    *,
    environ: Mapping[str, str] | None = None,
) -> int:
    args = list(argv) if argv is not None else sys.argv[1:]
    try:
        spec_path, state_root = _arguments(args)
        spec = load_swing_operational_job_spec_file(spec_path)
        runtime = os.environ if environ is None else environ
        bucket = validate_operational_state_bucket(
            runtime.get("INDIA_SWING_OPERATIONAL_STATE_BUCKET")
        )
        telegram_config = TelegramBotConfig.from_env(runtime)
        clock = lambda: datetime.now(timezone.utc)
        adapter = KiteMarketDataAdapter.from_official_sdk(
            KiteCredentials.from_env(environ),
            clock=clock,
        )
        record = run_swing_operational_job(
            job_spec=spec,
            state_root=state_root,
            kite_adapter=adapter,
            clock=clock,
        )
        writer = GoogleCloudStorageStateObjectWriter()
        publication = publish_swing_operational_state_to_gcs(
            record=record,
            bucket=bucket,
            writer=writer,
            decision_outbox=LocalSwingDecisionOutbox(state_root / "decision_outbox"),
            paper_ledger=LocalPaperTradeLedger(state_root / "paper"),
        )
        telegram_text = (
            record.message
            + f"\nOperational record ID: {record.record_id}\n"
        )
        telegram_receipt = deliver_telegram_notification(
            request=TelegramDeliveryRequest(
                delivery_key=record.record_id,
                text=telegram_text,
                message_sha256=hashlib.sha256(
                    telegram_text.encode("utf-8")
                ).hexdigest(),
            ),
            config=telegram_config,
            transport=UrllibTelegramHTTPTransport(),
            receipt_store=LocalTelegramDeliveryReceiptStore(
                state_root / "notification_delivery" / "telegram"
            ),
            clock=clock,
        )
        print(
            json.dumps(
                {
                    "action": record.action.value,
                    "failure_codes": [value.value for value in record.failure_codes],
                    "job_spec_id": spec.job_spec_id,
                    "manifest_generation": publication.manifest_object.generation,
                    "manifest_object_name": publication.manifest_object.object_name,
                    "manifest_sha256": publication.manifest_object.sha256,
                    "publication_id": publication.manifest.publication_id,
                    "record_id": record.record_id,
                    "run_id": record.run_id,
                    "spec_id": record.spec_id,
                    "status": record.status.value,
                    "target_session": record.target_session.isoformat(),
                    "telegram_receipt_id": telegram_receipt.receipt_id,
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
                    "error_type": SwingOperationalJobError.__name__,
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
