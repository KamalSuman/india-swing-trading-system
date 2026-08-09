"""Production-shaped, quality-only HYP-002 window-job process entrypoint.

Two commands:

- ``prepare-genesis`` reads one exact absolute local runbook JSON file and
  publishes only the campaign's first confirmed session's catalog action
  binding plus its window entry -- the one bootstrap step that has no
  predecessor for ``window_service.py`` to walk back to.
- ``run-window`` accepts exact bucket, pilot_run_id, target_session, and
  window_kind, constructs one shared injected GCS client, a
  ``KiteMarketDataAdapter`` from non-interactive ``KiteCredentials``
  environment values with ``maximum_attempts == 1``, a
  ``KiteQualityPilotCollector``, and invokes
  ``QualityPilotWindowService.run`` exactly once.

This module never reproduces any capture/completeness/lineage logic
already owned by ``quality_pilot/*.py`` -- it is only the missing process
composition root. It never performs interactive/browser Kite login, token
refresh, a daemon loop, a scheduler, a retry, a sleep, Telegram output, a
strategy computation, a paper trade, or an order.
"""

from __future__ import annotations

import json
import os
import sys
from collections.abc import Callable, Mapping, Sequence
from datetime import date, datetime, timezone
from pathlib import Path

from india_swing._filesystem import read_stable_regular_file
from india_swing.daily_pipeline.acquisition import GoogleCloudStorageObjectReader
from india_swing.daily_pipeline.state_publication import GoogleCloudStorageStateObjectWriter
from india_swing.market_data.config import KiteCredentials
from india_swing.market_data.kite import KiteMarketDataAdapter
from india_swing.market_data.provider import RetryPolicy

from india_swing.quality_pilot.canonical_response import ScheduledWindowKind
from india_swing.quality_pilot.invocation_control_plane import (
    GoogleCloudStorageQualityPilotClaimWriter,
    GoogleCloudStorageQualityPilotCurrentObjectReader,
    MAXIMUM_RUNBOOK_BYTES,
    QualityPilotActionBinding,
    QualityPilotActionKind,
    QualityPilotWindowEntry,
    decode_quality_pilot_invocation_runbook,
    pinned_quality_pilot_action_binding_request,
    publish_quality_pilot_action_binding,
    publish_quality_pilot_window_entry,
)
from india_swing.quality_pilot.kite_collector import KiteQualityPilotCollector
from india_swing.quality_pilot.window_service import (
    MAXIMUM_ACTIONS_PER_INVOCATION_CEILING,
    QualityPilotWindowService,
    QualityPilotWindowServiceResult,
)


class QualityPilotWindowJobError(ValueError):
    pass


_ERR_JOB = "quality pilot window job call is invalid"

_ENV_CODE_SHA256 = "INDIA_SWING_QUALITY_PILOT_CODE_SHA256"
_ENV_ENVIRONMENT_SHA256 = "INDIA_SWING_QUALITY_PILOT_ENVIRONMENT_SHA256"

_PREPARE_GENESIS_OPTIONS = ("--runbook-file", "--bucket")
_RUN_WINDOW_OPTIONS = (
    "--bucket",
    "--pilot-run-id",
    "--target-session",
    "--window-kind",
    "--maximum-actions-per-invocation",
)
_DEFAULT_MAXIMUM_ACTIONS_PER_INVOCATION = MAXIMUM_ACTIONS_PER_INVOCATION_CEILING


def _fail(message: str) -> None:
    raise QualityPilotWindowJobError(message)


def _parse_options(argv: Sequence[str], allowed: tuple[str, ...]) -> dict[str, str]:
    values: dict[str, str] = {}
    index = 0
    while index < len(argv):
        token = argv[index]
        if token not in allowed or token in values:
            _fail(_ERR_JOB)
        if index + 1 >= len(argv):
            _fail(_ERR_JOB)
        value = argv[index + 1]
        if type(value) is not str or not value:
            _fail(_ERR_JOB)
        values[token] = value
        index += 2
    required = set(allowed) - {"--maximum-actions-per-invocation"}
    if not required <= set(values):
        _fail(_ERR_JOB)
    return values


def _absolute_traversal_free_path(raw: str) -> Path:
    path = Path(raw)
    if not path.is_absolute() or ".." in path.parts:
        _fail(_ERR_JOB)
    return path


def _default_clock() -> datetime:
    return datetime.now(timezone.utc)


def _default_gcs_client_factory() -> object:
    from google.cloud import storage

    return storage.Client()


def _default_kite_adapter_factory(
    credentials: KiteCredentials, clock: Callable[[], datetime]
) -> KiteMarketDataAdapter:
    """Construct a KiteMarketDataAdapter configured for exactly one attempt
    per call, as the quality-only pilot's collector requires. Reuses the
    already-validated official-SDK client/version bootstrapping in
    ``KiteMarketDataAdapter.from_official_sdk`` rather than duplicating its
    pinned-version check or SDK construction here."""

    bootstrapped = KiteMarketDataAdapter.from_official_sdk(credentials, clock=clock)
    return KiteMarketDataAdapter(
        bootstrapped._client,
        sdk_version=bootstrapped.sdk_version,
        clock=clock,
        retry_policy=RetryPolicy(max_attempts=1),
    )


def _default_window_service_callable(**kwargs: object) -> QualityPilotWindowServiceResult:
    return QualityPilotWindowService().run(**kwargs)  # type: ignore[arg-type]


def _verified_prepare_genesis_envelope(
    *, runbook: object, published_binding: object, published_entry: object
) -> dict[str, object]:
    check_failed = False
    envelope: dict[str, object] | None = None
    try:
        runbook.verify_content_identity()
        envelope = {
            "status": "QUALITY_PILOT_GENESIS_PREPARED",
            "runbook_id": runbook.runbook_id,
            "pilot_run_id": runbook.campaign.pilot_run_id,
            "target_session": runbook.campaign.confirmed_sessions[0].isoformat(),
            "action_id": published_binding.action_id,
            "action_binding_generation": published_binding.generation,
            "window_entry_generation": published_entry.generation,
            "quality_only": True,
        }
    except Exception:
        check_failed = True
    if check_failed or envelope is None:
        _fail(_ERR_JOB)
    return envelope


def _verified_run_window_envelope(result: object) -> dict[str, object]:
    if type(result) is not QualityPilotWindowServiceResult:
        _fail(_ERR_JOB)
    verify_failed = False
    try:
        result.verify_content_identity()
    except Exception:
        verify_failed = True
    if verify_failed:
        _fail(_ERR_JOB)

    if result.campaign_complete:
        status = "QUALITY_PILOT_CAMPAIGN_COMPLETE"
    elif result.window_complete:
        status = "QUALITY_PILOT_WINDOW_COMPLETE"
    else:
        status = "QUALITY_PILOT_WINDOW_PARTIAL"

    envelope = {
        "status": status,
        "pilot_run_id": result.pilot_run_id,
        "market_session": result.market_session.isoformat(),
        "window_kind": result.window_kind.value,
        "actions_processed": result.actions_processed,
        "actions_reused": result.actions_reused,
        "final_transition_id": result.final_transition_id,
        "campaign_complete": result.campaign_complete,
        "window_complete": result.window_complete,
        "next_window_session": (
            result.next_window_session.isoformat() if result.next_window_session is not None else None
        ),
        "next_window_kind": (result.next_window_kind.value if result.next_window_kind is not None else None),
        "quality_only": True,
    }
    return envelope


def _run_prepare_genesis(
    args: Sequence[str],
    *,
    gcs_client_factory: Callable[[], object],
) -> dict[str, object]:
    options = _parse_options(args, _PREPARE_GENESIS_OPTIONS)
    runbook_path = _absolute_traversal_free_path(options["--runbook-file"])
    bucket = options["--bucket"]

    read_failed = False
    content_bytes = b""
    try:
        content_bytes = read_stable_regular_file(runbook_path, maximum_bytes=MAXIMUM_RUNBOOK_BYTES)
    except Exception:
        read_failed = True
    if read_failed:
        _fail(_ERR_JOB)

    decode_failed = False
    runbook: object = None
    try:
        runbook = decode_quality_pilot_invocation_runbook(content_bytes)
    except Exception:
        decode_failed = True
    if decode_failed:
        _fail(_ERR_JOB)
    if runbook.bucket != bucket:
        _fail(_ERR_JOB)

    client = gcs_client_factory()
    if client is None:
        _fail(_ERR_JOB)
    writer = GoogleCloudStorageStateObjectWriter(client=client)

    genesis_session = runbook.campaign.confirmed_sessions[0]
    build_failed = False
    genesis_binding: object = None
    try:
        genesis_binding = QualityPilotActionBinding(
            runbook=runbook,
            action_kind=QualityPilotActionKind.CATALOG_BOOTSTRAP,
            market_session=genesis_session,
            window_kind=ScheduledWindowKind.CATALOG_PREOPEN,
            prior_plan_pin=None,
            prior_transition_pin=None,
            plan_pin=None,
            predecessor_transition_pin=None,
            target_capture_spec_id=None,
        )
    except Exception:
        build_failed = True
    if build_failed or genesis_binding is None:
        _fail(_ERR_JOB)

    publish_binding_failed = False
    published_binding: object = None
    try:
        published_binding = publish_quality_pilot_action_binding(genesis_binding, writer)
    except Exception:
        publish_binding_failed = True
    if publish_binding_failed:
        _fail(_ERR_JOB)

    binding_pin_failed = False
    binding_pin: object = None
    try:
        binding_pin = pinned_quality_pilot_action_binding_request(published_binding)
    except Exception:
        binding_pin_failed = True
    if binding_pin_failed:
        _fail(_ERR_JOB)

    entry_failed = False
    entry: object = None
    try:
        entry = QualityPilotWindowEntry(
            pilot_run_id=runbook.campaign.pilot_run_id,
            market_session=genesis_session,
            window_kind=ScheduledWindowKind.CATALOG_PREOPEN,
            action_binding_pin=binding_pin,
        )
    except Exception:
        entry_failed = True
    if entry_failed or entry is None:
        _fail(_ERR_JOB)

    publish_entry_failed = False
    published_entry: object = None
    try:
        published_entry = publish_quality_pilot_window_entry(entry, bucket, writer)
    except Exception:
        publish_entry_failed = True
    if publish_entry_failed:
        _fail(_ERR_JOB)

    return _verified_prepare_genesis_envelope(
        runbook=runbook, published_binding=published_binding, published_entry=published_entry
    )


def _run_window(
    args: Sequence[str],
    *,
    environ: Mapping[str, str],
    clock: Callable[[], datetime],
    gcs_client_factory: Callable[[], object],
    kite_adapter_factory: Callable[[KiteCredentials, Callable[[], datetime]], object],
    claim_writer_factory: Callable[[object], object],
    window_service_callable: Callable[..., object],
) -> dict[str, object]:
    options = _parse_options(args, _RUN_WINDOW_OPTIONS)
    bucket = options["--bucket"]
    pilot_run_id = options["--pilot-run-id"]

    session_failed = False
    market_session: date | None = None
    try:
        market_session = date.fromisoformat(options["--target-session"])
    except Exception:
        session_failed = True
    if session_failed or market_session is None:
        _fail(_ERR_JOB)

    kind_failed = False
    window_kind: ScheduledWindowKind | None = None
    try:
        window_kind = ScheduledWindowKind(options["--window-kind"])
    except Exception:
        kind_failed = True
    if kind_failed or window_kind is None:
        _fail(_ERR_JOB)

    maximum_actions = _DEFAULT_MAXIMUM_ACTIONS_PER_INVOCATION
    if "--maximum-actions-per-invocation" in options:
        parse_failed = False
        try:
            maximum_actions = int(options["--maximum-actions-per-invocation"])
        except Exception:
            parse_failed = True
        if parse_failed or not (1 <= maximum_actions <= MAXIMUM_ACTIONS_PER_INVOCATION_CEILING):
            _fail(_ERR_JOB)

    code_sha256 = environ.get(_ENV_CODE_SHA256, "")
    environment_sha256 = environ.get(_ENV_ENVIRONMENT_SHA256, "")
    if type(code_sha256) is not str or type(environment_sha256) is not str:
        _fail(_ERR_JOB)

    credentials = KiteCredentials.from_env(environ)
    adapter = kite_adapter_factory(credentials, clock)
    collector = KiteQualityPilotCollector(adapter, clock=clock)

    client = gcs_client_factory()
    if client is None:
        _fail(_ERR_JOB)
    writer = GoogleCloudStorageStateObjectWriter(client=client)
    pinned_reader = GoogleCloudStorageObjectReader(client=client)
    current_reader = GoogleCloudStorageQualityPilotCurrentObjectReader(client)
    claim_writer = claim_writer_factory(client)

    result = window_service_callable(
        pilot_run_id=pilot_run_id,
        market_session=market_session,
        window_kind=window_kind,
        bucket=bucket,
        maximum_actions_per_invocation=maximum_actions,
        code_sha256=code_sha256,
        environment_sha256=environment_sha256,
        clock=clock,
        current_reader=current_reader,
        pinned_reader=pinned_reader,
        writer=writer,
        claim_writer=claim_writer,
        collector=collector,
    )

    return _verified_run_window_envelope(result)


def main(
    argv: Sequence[str] | None = None,
    *,
    environ: Mapping[str, str] | None = None,
    clock: Callable[[], datetime] | None = None,
    gcs_client_factory: Callable[[], object] | None = None,
    kite_adapter_factory: (
        Callable[[KiteCredentials, Callable[[], datetime]], object] | None
    ) = None,
    claim_writer_factory: Callable[[object], object] | None = None,
    window_service_callable: Callable[..., object] | None = None,
) -> int:
    args = list(argv) if argv is not None else sys.argv[1:]
    try:
        if not args:
            _fail(_ERR_JOB)
        command = args[0]
        remaining = args[1:]
        if command not in ("prepare-genesis", "run-window"):
            _fail(_ERR_JOB)

        active_clock = clock if clock is not None else _default_clock
        active_gcs_client_factory = (
            gcs_client_factory if gcs_client_factory is not None else _default_gcs_client_factory
        )
        active_kite_adapter_factory = (
            kite_adapter_factory if kite_adapter_factory is not None else _default_kite_adapter_factory
        )
        active_claim_writer_factory = (
            claim_writer_factory
            if claim_writer_factory is not None
            else GoogleCloudStorageQualityPilotClaimWriter
        )
        active_window_service_callable = (
            window_service_callable if window_service_callable is not None else _default_window_service_callable
        )
        if not (
            callable(active_clock)
            and callable(active_gcs_client_factory)
            and callable(active_kite_adapter_factory)
            and callable(active_claim_writer_factory)
            and callable(active_window_service_callable)
        ):
            _fail(_ERR_JOB)

        if command == "prepare-genesis":
            envelope = _run_prepare_genesis(remaining, gcs_client_factory=active_gcs_client_factory)
        else:
            runtime_environ = os.environ if environ is None else environ
            envelope = _run_window(
                remaining,
                environ=runtime_environ,
                clock=active_clock,
                gcs_client_factory=active_gcs_client_factory,
                kite_adapter_factory=active_kite_adapter_factory,
                claim_writer_factory=active_claim_writer_factory,
                window_service_callable=active_window_service_callable,
            )

        print(
            json.dumps(
                envelope, allow_nan=False, ensure_ascii=False, separators=(",", ":"), sort_keys=True,
            )
        )
        return 0
    except Exception:
        print(
            json.dumps(
                {"error_type": QualityPilotWindowJobError.__name__, "status": "FAILED"},
                allow_nan=False, ensure_ascii=False, separators=(",", ":"), sort_keys=True,
            ),
            file=sys.stderr,
        )
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
