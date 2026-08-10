"""Production-shaped, manually triggered promoted paper-pilot job.

The job composes the accepted hydrated promoted-operational Cloud Run job with
an explicitly authorized, durable Telegram delivery boundary.  The inner job
publishes its terminal state first.  Only a fully verified success envelope and
the matching locally restored advisory/terminal may reach notification.

This process remains paper-only.  It has no order endpoint, no execution
authority, no scheduler, no browser login, and no token refresh capability.
"""

from __future__ import annotations

import io
import json
import os
import re
import stat
import sys
from collections.abc import Callable, Mapping, Sequence
from contextlib import redirect_stderr, redirect_stdout
from datetime import datetime, timezone
from pathlib import Path

from india_swing._filesystem import read_stable_regular_file
from india_swing.notifications import (
    LocalTelegramDeliveryReceiptStore,
    TelegramBotConfig,
    TelegramHTTPTransport,
    UrllibTelegramHTTPTransport,
)
from india_swing.promoted_operational_hydrated_cloud_control import (
    MAXIMUM_HYDRATED_CLOUD_LAUNCH_BYTES,
    PromotedOperationalHydratedCloudLaunch,
    decode_promoted_operational_hydrated_cloud_launch,
)
from india_swing.promoted_operational_persistence import (
    LocalPromotedOperationalAdvisoryOutbox,
    LocalPromotedOperationalTerminalStore,
    PromotedOperationalAdvisoryRecord,
    PromotedOperationalTerminalRecord,
)
from india_swing.promoted_operational_runner import (
    PromotedOperationalRunFailureCode,
)
from india_swing.promoted_paper_pilot_notification import (
    CompletedPromotedPaperPilotNotification,
    GoogleCloudStoragePromotedPaperPilotNotificationStore,
    deliver_promoted_paper_pilot_notification,
)

import india_swing.promoted_operational_hydrated_cloud_job as _hydrated_job


class PromotedPaperPilotJobError(ValueError):
    pass


_ERR = "promoted paper-pilot job call is invalid"
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_STATE_MANIFEST_PATH = re.compile(
    r"promoted-operational-state/v1/(\d{4}-\d{2}-\d{2})/([0-9a-f]{64})/"
    r"manifests/([0-9a-f]{64})\.json\Z"
)
_FIXED_RUNTIME_PARENT = Path("/tmp/india-swing")
_MAXIMUM_INNER_STDOUT_BYTES = 64 * 1024
_FAILURE_CODE_VALUES = frozenset(
    value.value for value in PromotedOperationalRunFailureCode
)

_HYDRATED_SUCCESS_KEYS = frozenset(
    {
        "action",
        "advisory_id",
        "assembly_spec_id",
        "binding_generation",
        "binding_id",
        "cloud_control_id",
        "execution_eligible",
        "failure_codes",
        "inner_status",
        "input_manifest_byte_count",
        "input_manifest_generation",
        "input_manifest_object_name",
        "input_manifest_sha256",
        "input_snapshot_id",
        "launch_id",
        "notification_eligible",
        "operational_run_spec_id",
        "paper_only",
        "preparation_id",
        "reused_existing_terminal",
        "runtime_job_spec_id",
        "state_manifest_byte_count",
        "state_manifest_generation",
        "state_manifest_object_name",
        "state_manifest_sha256",
        "state_publication_id",
        "status",
        "target_session",
        "terminal_id",
        "terminal_status",
    }
)


def _arguments(argv: Sequence[str]) -> Path:
    if len(argv) != 2 or argv[0] != "--launch-file":
        raise PromotedPaperPilotJobError(_ERR)
    raw = argv[1]
    if type(raw) is not str or not raw:
        raise PromotedPaperPilotJobError(_ERR)
    path = Path(raw)
    if not path.is_absolute() or ".." in path.parts:
        raise PromotedPaperPilotJobError(_ERR)
    return path


def _default_clock() -> datetime:
    return datetime.now(timezone.utc)


def _default_gcs_client_factory() -> object:
    from google.cloud import storage

    return storage.Client()


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise PromotedPaperPilotJobError(_ERR)
        result[key] = value
    return result


def _reject_number(_token: str) -> object:
    raise PromotedPaperPilotJobError(_ERR)


def _sha(value: object) -> str:
    if type(value) is not str or _SHA256.fullmatch(value) is None:
        raise PromotedPaperPilotJobError(_ERR)
    return value


def _positive_int(value: object) -> int:
    if type(value) is not int or value <= 0:
        raise PromotedPaperPilotJobError(_ERR)
    return value


def _runtime_parent_identity(path: Path) -> tuple[int, int]:
    if type(path) is not type(Path()) or not path.is_absolute() or ".." in path.parts:
        raise PromotedPaperPilotJobError(_ERR)
    try:
        status = os.lstat(path)
    except Exception:
        raise PromotedPaperPilotJobError(_ERR) from None
    reparse = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400)
    if (
        not stat.S_ISDIR(status.st_mode)
        or stat.S_ISLNK(status.st_mode)
        or bool(getattr(status, "st_file_attributes", 0) & reparse)
    ):
        raise PromotedPaperPilotJobError(_ERR)
    return status.st_dev, status.st_ino


def _parse_hydrated_envelope(
    payload: str,
    *,
    launch: PromotedOperationalHydratedCloudLaunch,
) -> dict[str, object]:
    if (
        type(payload) is not str
        or not payload
        or len(payload.encode("utf-8")) > _MAXIMUM_INNER_STDOUT_BYTES
    ):
        raise PromotedPaperPilotJobError(_ERR)
    lines = payload.split("\n")
    if len(lines) != 2 or lines[1] != "" or not lines[0]:
        raise PromotedPaperPilotJobError(_ERR)
    line = lines[0]
    try:
        value = json.loads(
            line,
            object_pairs_hook=_unique_object,
            parse_float=_reject_number,
            parse_constant=_reject_number,
        )
    except PromotedPaperPilotJobError:
        raise
    except Exception:
        raise PromotedPaperPilotJobError(_ERR) from None
    if type(value) is not dict or set(value) != _HYDRATED_SUCCESS_KEYS:
        raise PromotedPaperPilotJobError(_ERR)
    state_manifest_match = (
        _STATE_MANIFEST_PATH.fullmatch(value["state_manifest_object_name"])
        if type(value["state_manifest_object_name"]) is str
        else None
    )
    if (
        json.dumps(
            value,
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )
        != line
    ):
        raise PromotedPaperPilotJobError(_ERR)

    sha_fields = (
        "advisory_id",
        "assembly_spec_id",
        "binding_id",
        "cloud_control_id",
        "input_manifest_sha256",
        "input_snapshot_id",
        "launch_id",
        "operational_run_spec_id",
        "preparation_id",
        "runtime_job_spec_id",
        "state_manifest_sha256",
        "state_publication_id",
        "terminal_id",
    )
    for key in sha_fields:
        _sha(value[key])
    for key in (
        "binding_generation",
        "input_manifest_byte_count",
        "input_manifest_generation",
        "state_manifest_byte_count",
        "state_manifest_generation",
    ):
        _positive_int(value[key])
    if (
        value["status"]
        != "PROMOTED_OPERATIONAL_HYDRATED_CLOUD_JOB_COMPLETE"
        or value["inner_status"] != "PROMOTED_OPERATIONAL_JOB_COMPLETE"
        or value["assembly_spec_id"] != launch.expected_assembly_spec_id
        or value["operational_run_spec_id"]
        != launch.expected_operational_run_spec_id
        or value["target_session"] != launch.target_session.isoformat()
        or value["launch_id"] != launch.launch_id
        or value["input_snapshot_id"]
        != launch.input_restore.expected_snapshot_id
        or value["input_manifest_object_name"]
        != launch.input_restore.manifest_object_name
        or value["input_manifest_generation"]
        != launch.input_restore.generation
        or value["input_manifest_sha256"]
        != launch.input_restore.expected_sha256
        or value["paper_only"] is not True
        or value["notification_eligible"] is not False
        or value["execution_eligible"] is not False
        or type(value["reused_existing_terminal"]) is not bool
        or type(value["failure_codes"]) is not list
        or any(
            type(item) is not str or item not in _FAILURE_CODE_VALUES
            for item in value["failure_codes"]
        )
        or value["failure_codes"] != sorted(set(value["failure_codes"]))
        or type(value["action"]) is not str
        or value["action"] not in {"PAPER_BUY", "NO_TRADE"}
        or type(value["terminal_status"]) is not str
        or value["terminal_status"] not in {"COMPLETE", "FAILED"}
        or type(value["state_manifest_object_name"]) is not str
        or not value["state_manifest_object_name"]
        or state_manifest_match is None
        or state_manifest_match.group(1) != launch.target_session.isoformat()
        or state_manifest_match.group(2)
        != launch.expected_operational_run_spec_id
        or state_manifest_match.group(3) != value["state_publication_id"]
    ):
        raise PromotedPaperPilotJobError(_ERR)
    if (
        value["terminal_status"] == "COMPLETE" and value["failure_codes"]
    ) or (
        value["terminal_status"] == "FAILED"
        and (
            not value["failure_codes"] or value["action"] != "NO_TRADE"
        )
    ):
        raise PromotedPaperPilotJobError(_ERR)
    return value


def _load_verified_local_result(
    *,
    runtime_parent: Path,
    envelope: Mapping[str, object],
) -> tuple[PromotedOperationalTerminalRecord, PromotedOperationalAdvisoryRecord]:
    terminal = LocalPromotedOperationalTerminalStore(
        runtime_parent / "state" / "terminal"
    ).get(envelope["operational_run_spec_id"])
    advisory = LocalPromotedOperationalAdvisoryOutbox(
        runtime_parent / "state" / "advisory"
    ).get(envelope["advisory_id"])
    if (
        type(terminal) is not PromotedOperationalTerminalRecord
        or type(advisory) is not PromotedOperationalAdvisoryRecord
    ):
        raise PromotedPaperPilotJobError(_ERR)
    terminal.verify_content_identity()
    advisory.verify_content_identity()
    if (
        terminal.spec_id != envelope["operational_run_spec_id"]
        or terminal.terminal_id != envelope["terminal_id"]
        or terminal.advisory_id != envelope["advisory_id"]
        or terminal.preparation_id != envelope["preparation_id"]
        or terminal.target_session.isoformat() != envelope["target_session"]
        or terminal.status.value != envelope["terminal_status"]
        or terminal.action.value != envelope["action"]
        or list(terminal.failure_codes) != envelope["failure_codes"]
        or terminal.paper_only is not True
        or terminal.notification_eligible is not False
        or terminal.execution_eligible is not False
        or advisory.advisory_id != terminal.advisory_id
        or advisory.spec_id != terminal.spec_id
        or advisory.target_session != terminal.target_session
        or advisory.status is not terminal.status
        or advisory.action is not terminal.action
    ):
        raise PromotedPaperPilotJobError(_ERR)
    return terminal, advisory


def main(
    argv: Sequence[str] | None = None,
    *,
    environ: Mapping[str, str] | None = None,
    runtime_parent: Path | None = None,
    clock: Callable[[], datetime] | None = None,
    kite_adapter_factory: Callable[..., object] | None = None,
    gcs_client_factory: Callable[[], object] | None = None,
    hydrated_job_main: Callable[..., int] | None = None,
    telegram_transport: TelegramHTTPTransport | None = None,
    notification_callable: Callable[..., object] | None = None,
) -> int:
    args = list(argv) if argv is not None else sys.argv[1:]
    try:
        launch_file = _arguments(args)
        launch = decode_promoted_operational_hydrated_cloud_launch(
            read_stable_regular_file(
                launch_file,
                maximum_bytes=MAXIMUM_HYDRATED_CLOUD_LAUNCH_BYTES,
            )
        )
        runtime = os.environ if environ is None else environ
        telegram_config = TelegramBotConfig.from_env(runtime)
        expected_state_bucket = runtime.get(
            "INDIA_SWING_PAPER_PILOT_STATE_BUCKET"
        )
        if (
            type(expected_state_bucket) is not str
            or expected_state_bucket != launch.state_bucket
        ):
            raise PromotedPaperPilotJobError(_ERR)

        active_runtime_parent = (
            _FIXED_RUNTIME_PARENT
            if runtime_parent is None
            else Path(runtime_parent)
        )
        parent_identity = _runtime_parent_identity(active_runtime_parent)
        active_clock = _default_clock if clock is None else clock
        active_gcs_factory = (
            _default_gcs_client_factory
            if gcs_client_factory is None
            else gcs_client_factory
        )
        active_hydrated_main = (
            _hydrated_job.main
            if hydrated_job_main is None
            else hydrated_job_main
        )
        active_transport = (
            UrllibTelegramHTTPTransport()
            if telegram_transport is None
            else telegram_transport
        )
        active_notification = (
            deliver_promoted_paper_pilot_notification
            if notification_callable is None
            else notification_callable
        )
        if not (
            callable(active_clock)
            and callable(active_gcs_factory)
            and callable(active_hydrated_main)
            and callable(active_notification)
            and callable(getattr(active_transport, "post_json", None))
        ):
            raise PromotedPaperPilotJobError(_ERR)

        client = active_gcs_factory()
        if client is None:
            raise PromotedPaperPilotJobError(_ERR)

        inner_kwargs: dict[str, object] = {
            "environ": runtime,
            "runtime_parent": active_runtime_parent,
            "gcs_client_factory": lambda: client,
        }
        if clock is not None:
            inner_kwargs["clock"] = active_clock
        if kite_adapter_factory is not None:
            inner_kwargs["kite_adapter_factory"] = kite_adapter_factory

        inner_stdout = io.StringIO()
        inner_stderr = io.StringIO()
        with redirect_stdout(inner_stdout), redirect_stderr(inner_stderr):
            exit_code = active_hydrated_main(
                ["--launch-file", str(launch_file)], **inner_kwargs
            )
        if exit_code != 0 or inner_stderr.getvalue() != "":
            raise PromotedPaperPilotJobError(_ERR)
        envelope = _parse_hydrated_envelope(
            inner_stdout.getvalue(), launch=launch
        )
        if _runtime_parent_identity(active_runtime_parent) != parent_identity:
            raise PromotedPaperPilotJobError(_ERR)
        terminal, advisory = _load_verified_local_result(
            runtime_parent=active_runtime_parent, envelope=envelope
        )

        notification = active_notification(
            bucket=launch.state_bucket,
            terminal=terminal,
            advisory=advisory,
            state_publication_id=envelope["state_publication_id"],
            state_manifest_object_name=envelope[
                "state_manifest_object_name"
            ],
            state_manifest_generation=envelope[
                "state_manifest_generation"
            ],
            state_manifest_sha256=envelope["state_manifest_sha256"],
            config=telegram_config,
            transport=active_transport,
            receipt_store=LocalTelegramDeliveryReceiptStore(
                active_runtime_parent
                / "notification-receipts"
                / telegram_config.chat_binding_id
            ),
            durable_store=GoogleCloudStoragePromotedPaperPilotNotificationStore(
                client
            ),
            clock=active_clock,
        )
        if type(notification) is not CompletedPromotedPaperPilotNotification:
            raise PromotedPaperPilotJobError(_ERR)
        notification.claim.verify_content_identity()
        notification.receipt.verify_content_identity()

        result = dict(envelope)
        result["status"] = "PROMOTED_PAPER_PILOT_JOB_COMPLETE"
        result["inner_status"] = envelope["status"]
        result["notification_claim_id"] = notification.claim.claim_id
        result["notification_receipt_id"] = (
            notification.receipt.notification_receipt_id
        )
        result["telegram_receipt_id"] = (
            notification.receipt.telegram_receipt.receipt_id
        )
        result["notification_replayed"] = notification.replayed
        print(
            json.dumps(
                result,
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
                    "error_type": PromotedPaperPilotJobError.__name__,
                    "status": "FAILED",
                },
                allow_nan=False,
                ensure_ascii=False,
                separators=(",", ":"),
                sort_keys=True,
            ),
            file=sys.stderr,
        )
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
