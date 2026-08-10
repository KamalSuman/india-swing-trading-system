"""Hydrated Cloud Run-shaped entrypoint.

Reads one portable, path-free ``PromotedOperationalHydratedCloudLaunch``
launch-control file, acquires the complete externally pinned promoted-
operational input snapshot it names, hydrates it into a fresh ephemeral
runtime directory, canonically writes the derived local
``PromotedOperationalCloudRunControl`` bootstrap file, and invokes the
already-accepted ``promoted_operational_cloud_job.main`` exactly once with
the same shared injected GCS client -- never a second client.

This module never reproduces any input-snapshot, GCS-state, cloud-control,
assembly, strategy, quote, allocation, risk, sizing, terminal-binding,
advisory, registration, or promoted-engine codec/algorithm; it only
composes the already-accepted layers. It performs no deployment, no
scheduling, no Telegram delivery, no interactive/browser login, no token
refresh, no bucket listing, no "latest" object selection, and grants no
real-capital authority. It is one bounded invocation: no loop, retry,
sleep, polling, cleanup, or deletion exists here. A partial local
hydration, or a completed immutable GCS upload followed by a later local
failure, is an auditable failed attempt -- it is never reported as
success and never automatically repaired.
"""

from __future__ import annotations

import hashlib
import io
import json
import os
import re
import stat
import sys
from collections.abc import Callable, Mapping, Sequence
from contextlib import redirect_stderr, redirect_stdout
from datetime import datetime
from pathlib import Path

from india_swing._filesystem import read_stable_regular_file
from india_swing.daily_pipeline.acquisition import GoogleCloudStorageObjectReader
from india_swing.promoted_operational_cloud_control import (
    MAXIMUM_CLOUD_CONTROL_BYTES,
    PromotedOperationalCloudRunControl,
    encode_promoted_operational_cloud_control,
)
from india_swing.promoted_operational_decision import PromotedOperationalDecisionAction
from india_swing.promoted_operational_hydrated_cloud_control import (
    MAXIMUM_HYDRATED_CLOUD_LAUNCH_BYTES,
    PromotedOperationalHydratedCloudLaunch,
    decode_promoted_operational_hydrated_cloud_launch,
)
from india_swing.promoted_operational_input_gcs import (
    AcquiredPromotedOperationalInputSnapshot,
    CompletedPromotedOperationalInputRestore,
    acquire_promoted_operational_input_snapshot,
    encode_promoted_operational_input_snapshot_manifest,
    hydrate_promoted_operational_input_snapshot,
)
from india_swing.promoted_operational_input_snapshot import ROOT_INPUT_NAMES
from india_swing.promoted_operational_runner import (
    PromotedOperationalRunFailureCode,
    PromotedOperationalRunStatus,
)

import india_swing.promoted_operational_cloud_job as _cloud_job


class PromotedOperationalHydratedCloudJobError(ValueError):
    pass


_ERR_JOB = "promoted operational hydrated cloud job call is invalid"
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_CONCRETE_PATH_TYPE = type(Path())
_FIXED_RUNTIME_PARENT = Path("/tmp/india-swing")

# Mirrors the exact accepted promoted-operational-state manifest path
# shape defined privately in promoted_operational_gcs_state.py -- defined
# locally here (rather than importing a private module attribute) so a
# malformed/foreign state_manifest_object_name can never be echoed to
# this wrapper's own stdout.
_STATE_MANIFEST_PATH = re.compile(
    r"promoted-operational-state/v1/(\d{4}-\d{2}-\d{2})/([0-9a-f]{64})/"
    r"manifests/([0-9a-f]{64})\.json\Z"
)
_RUN_STATUS_VALUES = frozenset(value.value for value in PromotedOperationalRunStatus)
_DECISION_ACTION_VALUES = frozenset(value.value for value in PromotedOperationalDecisionAction)
_FAILURE_CODE_VALUES = frozenset(value.value for value in PromotedOperationalRunFailureCode)

# Conservative ceiling on the captured inner stdout text, checked before
# any JSON parsing is attempted -- the accepted envelope is always a
# small single-line JSON object; this bounds how much untrusted text this
# wrapper will ever hold/parse from the inner call.
_MAXIMUM_INNER_STDOUT_BYTES = 64 * 1024

_INNER_CLOUD_JOB_SUCCESS_KEYS = frozenset(
    {
        "status",
        "assembly_spec_id",
        "runtime_job_spec_id",
        "operational_run_spec_id",
        "preparation_id",
        "target_session",
        "terminal_id",
        "terminal_status",
        "action",
        "failure_codes",
        "advisory_id",
        "binding_id",
        "binding_generation",
        "reused_existing_terminal",
        "paper_only",
        "notification_eligible",
        "execution_eligible",
        "cloud_control_id",
        "state_publication_id",
        "state_manifest_object_name",
        "state_manifest_generation",
        "state_manifest_sha256",
        "state_manifest_byte_count",
    }
)
_RESULT_ADDED_KEYS = frozenset(
    {
        "status",
        "inner_status",
        "launch_id",
        "input_snapshot_id",
        "input_manifest_object_name",
        "input_manifest_generation",
        "input_manifest_sha256",
        "input_manifest_byte_count",
    }
)
_RESULT_KEYS = _INNER_CLOUD_JOB_SUCCESS_KEYS | _RESULT_ADDED_KEYS


def _launch_file_path(argv: Sequence[str]) -> Path:
    if len(argv) != 2 or argv[0] != "--launch-file":
        raise PromotedOperationalHydratedCloudJobError(_ERR_JOB)
    value = argv[1]
    if type(value) is not str or not value:
        raise PromotedOperationalHydratedCloudJobError(_ERR_JOB)
    path = Path(value)
    if not path.is_absolute() or ".." in path.parts:
        raise PromotedOperationalHydratedCloudJobError(_ERR_JOB)
    return path


def _is_link_like(status: os.stat_result) -> bool:
    if stat.S_ISLNK(status.st_mode):
        return True
    attribute = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400)
    return bool(getattr(status, "st_file_attributes", 0) & attribute)


def _lstat(path: Path) -> os.stat_result:
    failed = False
    status: os.stat_result | None = None
    try:
        status = os.lstat(path)
    except OSError:
        failed = True
    if failed or status is None:
        raise PromotedOperationalHydratedCloudJobError(_ERR_JOB)
    return status


def _validated_runtime_parent(path: object) -> tuple[Path, tuple[int, int]]:
    """The fixed (or, on Windows, injected-for-tests) runtime parent must
    be an existing, exact, empty, safe, non-link real directory. Its
    filesystem identity is captured here and rechecked before every later
    write inside this module."""

    if type(path) is not _CONCRETE_PATH_TYPE:
        raise PromotedOperationalHydratedCloudJobError(_ERR_JOB)
    if not path.is_absolute() or ".." in path.parts:
        raise PromotedOperationalHydratedCloudJobError(_ERR_JOB)
    status = _lstat(path)
    if not stat.S_ISDIR(status.st_mode) or _is_link_like(status):
        raise PromotedOperationalHydratedCloudJobError(_ERR_JOB)
    listing_failed = False
    names: list[str] = []
    try:
        with os.scandir(path) as iterator:
            names = [entry.name for entry in iterator]
    except OSError:
        listing_failed = True
    if listing_failed or names:
        raise PromotedOperationalHydratedCloudJobError(_ERR_JOB)
    return path, (status.st_dev, status.st_ino)


def _recheck_runtime_parent(path: Path, expected_identity: tuple[int, int]) -> None:
    status = _lstat(path)
    if (
        not stat.S_ISDIR(status.st_mode)
        or _is_link_like(status)
        or (status.st_dev, status.st_ino) != expected_identity
    ):
        raise PromotedOperationalHydratedCloudJobError(_ERR_JOB)


def _default_gcs_client_factory() -> object:
    from google.cloud import storage

    return storage.Client()


def _write_runtime_control_file(path: Path, payload: bytes) -> None:
    """Exclusive, single-attempt local write for the derived
    ``runtime-control.json`` bootstrap file inside the already-verified-
    empty runtime parent. Never overwrites, truncates, or repairs -- a
    pre-existing destination fails closed."""

    write_failed = False
    try:
        flags = (
            os.O_CREAT
            | os.O_EXCL
            | os.O_WRONLY
            | getattr(os, "O_BINARY", 0)
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_NOINHERIT", 0)
            | getattr(os, "O_NOFOLLOW", 0)
        )
        descriptor = os.open(path, flags, 0o600)
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
    except Exception:
        write_failed = True
    if write_failed:
        raise PromotedOperationalHydratedCloudJobError(_ERR_JOB)

    cold_read_failed = False
    cold_bytes = b""
    try:
        cold_bytes = read_stable_regular_file(path, maximum_bytes=MAXIMUM_CLOUD_CONTROL_BYTES)
    except Exception:
        cold_read_failed = True
    if cold_read_failed or cold_bytes != payload:
        raise PromotedOperationalHydratedCloudJobError(_ERR_JOB)


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise PromotedOperationalHydratedCloudJobError(_ERR_JOB)
        result[key] = value
    return result


def _reject_number(_token: str) -> object:
    raise PromotedOperationalHydratedCloudJobError(_ERR_JOB)


def _validate_inner_envelope(
    envelope: dict[str, object],
    *,
    launch: PromotedOperationalHydratedCloudLaunch,
    hydration_control: PromotedOperationalCloudRunControl,
) -> None:
    """The inner ``promoted_operational_cloud_job`` envelope is untrusted
    even once it is canonical JSON with the exact accepted key set --
    every field is independently type/format checked, and every lineage
    field is cross-checked against this job's own launch/control, before
    any of it is forwarded into this wrapper's own final stdout."""

    if envelope["status"] != "PROMOTED_OPERATIONAL_JOB_COMPLETE":
        raise PromotedOperationalHydratedCloudJobError(_ERR_JOB)
    if (
        type(envelope["assembly_spec_id"]) is not str
        or envelope["assembly_spec_id"] != launch.expected_assembly_spec_id
    ):
        raise PromotedOperationalHydratedCloudJobError(_ERR_JOB)
    if type(envelope["runtime_job_spec_id"]) is not str or _SHA256.fullmatch(envelope["runtime_job_spec_id"]) is None:
        raise PromotedOperationalHydratedCloudJobError(_ERR_JOB)
    if (
        type(envelope["operational_run_spec_id"]) is not str
        or envelope["operational_run_spec_id"] != launch.expected_operational_run_spec_id
    ):
        raise PromotedOperationalHydratedCloudJobError(_ERR_JOB)
    if type(envelope["preparation_id"]) is not str or _SHA256.fullmatch(envelope["preparation_id"]) is None:
        raise PromotedOperationalHydratedCloudJobError(_ERR_JOB)
    if (
        type(envelope["target_session"]) is not str
        or envelope["target_session"] != launch.target_session.isoformat()
    ):
        raise PromotedOperationalHydratedCloudJobError(_ERR_JOB)
    if type(envelope["terminal_id"]) is not str or _SHA256.fullmatch(envelope["terminal_id"]) is None:
        raise PromotedOperationalHydratedCloudJobError(_ERR_JOB)
    terminal_status = envelope["terminal_status"]
    if type(terminal_status) is not str or terminal_status not in _RUN_STATUS_VALUES:
        raise PromotedOperationalHydratedCloudJobError(_ERR_JOB)
    action = envelope["action"]
    if type(action) is not str or action not in _DECISION_ACTION_VALUES:
        raise PromotedOperationalHydratedCloudJobError(_ERR_JOB)
    failure_codes = envelope["failure_codes"]
    if type(failure_codes) is not list or len(failure_codes) > len(_FAILURE_CODE_VALUES):
        raise PromotedOperationalHydratedCloudJobError(_ERR_JOB)
    for code in failure_codes:
        if type(code) is not str or code not in _FAILURE_CODE_VALUES:
            raise PromotedOperationalHydratedCloudJobError(_ERR_JOB)
    if len(set(failure_codes)) != len(failure_codes) or failure_codes != sorted(failure_codes):
        raise PromotedOperationalHydratedCloudJobError(_ERR_JOB)
    # Terminal consistency, mirroring the accepted terminal-record model:
    # COMPLETE never carries a failure code; FAILED always carries at
    # least one and is always paired with the NO_TRADE action.
    if terminal_status == PromotedOperationalRunStatus.COMPLETE.value:
        if failure_codes:
            raise PromotedOperationalHydratedCloudJobError(_ERR_JOB)
    else:
        if not failure_codes or action != PromotedOperationalDecisionAction.NO_TRADE.value:
            raise PromotedOperationalHydratedCloudJobError(_ERR_JOB)
    if type(envelope["advisory_id"]) is not str or _SHA256.fullmatch(envelope["advisory_id"]) is None:
        raise PromotedOperationalHydratedCloudJobError(_ERR_JOB)
    if type(envelope["binding_id"]) is not str or _SHA256.fullmatch(envelope["binding_id"]) is None:
        raise PromotedOperationalHydratedCloudJobError(_ERR_JOB)
    if type(envelope["binding_generation"]) is not int or envelope["binding_generation"] <= 0:
        raise PromotedOperationalHydratedCloudJobError(_ERR_JOB)
    if type(envelope["reused_existing_terminal"]) is not bool:
        raise PromotedOperationalHydratedCloudJobError(_ERR_JOB)
    if (
        envelope["paper_only"] is not True
        or envelope["notification_eligible"] is not False
        or envelope["execution_eligible"] is not False
    ):
        raise PromotedOperationalHydratedCloudJobError(_ERR_JOB)
    if (
        type(envelope["cloud_control_id"]) is not str
        or _SHA256.fullmatch(envelope["cloud_control_id"]) is None
        or envelope["cloud_control_id"] != hydration_control.control_id
    ):
        raise PromotedOperationalHydratedCloudJobError(_ERR_JOB)
    if (
        type(envelope["state_publication_id"]) is not str
        or _SHA256.fullmatch(envelope["state_publication_id"]) is None
    ):
        raise PromotedOperationalHydratedCloudJobError(_ERR_JOB)
    state_manifest_object_name = envelope["state_manifest_object_name"]
    if type(state_manifest_object_name) is not str:
        raise PromotedOperationalHydratedCloudJobError(_ERR_JOB)
    state_manifest_match = _STATE_MANIFEST_PATH.fullmatch(state_manifest_object_name)
    if (
        state_manifest_match is None
        or state_manifest_match.group(1) != launch.target_session.isoformat()
        or state_manifest_match.group(2) != launch.expected_operational_run_spec_id
        or state_manifest_match.group(3) != envelope["state_publication_id"]
    ):
        raise PromotedOperationalHydratedCloudJobError(_ERR_JOB)
    if type(envelope["state_manifest_generation"]) is not int or envelope["state_manifest_generation"] <= 0:
        raise PromotedOperationalHydratedCloudJobError(_ERR_JOB)
    if (
        type(envelope["state_manifest_sha256"]) is not str
        or _SHA256.fullmatch(envelope["state_manifest_sha256"]) is None
    ):
        raise PromotedOperationalHydratedCloudJobError(_ERR_JOB)
    if type(envelope["state_manifest_byte_count"]) is not int or envelope["state_manifest_byte_count"] <= 0:
        raise PromotedOperationalHydratedCloudJobError(_ERR_JOB)


def main(
    argv: Sequence[str] | None = None,
    *,
    runtime_parent: Path | None = None,
    environ: Mapping[str, str] | None = None,
    clock: Callable[[], datetime] | None = None,
    kite_adapter_factory: Callable[..., object] | None = None,
    gcs_client_factory: Callable[[], object] | None = None,
    runtime_callable: Callable[..., object] | None = None,
    cloud_job_main: Callable[..., int] | None = None,
) -> int:
    args = list(argv) if argv is not None else sys.argv[1:]
    try:
        launch_file = _launch_file_path(args)

        launch_payload = read_stable_regular_file(
            launch_file, maximum_bytes=MAXIMUM_HYDRATED_CLOUD_LAUNCH_BYTES
        )
        launch = decode_promoted_operational_hydrated_cloud_launch(launch_payload)

        active_runtime_parent = runtime_parent if runtime_parent is not None else _FIXED_RUNTIME_PARENT
        parent, parent_identity = _validated_runtime_parent(active_runtime_parent)

        active_gcs_client_factory = (
            gcs_client_factory if gcs_client_factory is not None else _default_gcs_client_factory
        )
        active_cloud_job_main = cloud_job_main if cloud_job_main is not None else _cloud_job.main
        if not (
            (clock is None or callable(clock))
            and (kite_adapter_factory is None or callable(kite_adapter_factory))
            and callable(active_gcs_client_factory)
            and (runtime_callable is None or callable(runtime_callable))
            and callable(active_cloud_job_main)
        ):
            raise PromotedOperationalHydratedCloudJobError(_ERR_JOB)

        # Exactly one GCS client for the entire invocation: used both for
        # input-snapshot acquisition below and, via gcs_client_factory=
        # lambda: client, for the inner cloud-job call further down.
        client = active_gcs_client_factory()
        if client is None:
            raise PromotedOperationalHydratedCloudJobError(_ERR_JOB)
        reader = GoogleCloudStorageObjectReader(client=client)

        acquired = acquire_promoted_operational_input_snapshot(
            request=launch.input_restore, reader=reader,
        )
        if type(acquired) is not AcquiredPromotedOperationalInputSnapshot:
            raise PromotedOperationalHydratedCloudJobError(_ERR_JOB)
        acquired = AcquiredPromotedOperationalInputSnapshot(
            request=acquired.request, manifest=acquired.manifest, blobs=acquired.blobs,
        )
        if (
            acquired.manifest.bucket != launch.state_bucket
            or acquired.manifest.inventory.expected_assembly_spec_id != launch.expected_assembly_spec_id
            or acquired.manifest.inventory.target_session != launch.target_session
            or acquired.manifest.snapshot_id != launch.input_restore.expected_snapshot_id
        ):
            raise PromotedOperationalHydratedCloudJobError(_ERR_JOB)

        _recheck_runtime_parent(parent, parent_identity)

        root_paths = {name: parent / name for name in ROOT_INPUT_NAMES}
        hydration_control = PromotedOperationalCloudRunControl(
            expected_assembly_spec_id=launch.expected_assembly_spec_id,
            expected_operational_run_spec_id=launch.expected_operational_run_spec_id,
            target_session=launch.target_session,
            state_bucket=launch.state_bucket,
            assembly_spec_file=parent / "assembly-spec.json",
            state_root=parent / "state",
            prior_state_restore=launch.prior_state_restore,
            **root_paths,
        )

        restore = hydrate_promoted_operational_input_snapshot(
            control=hydration_control, acquired=acquired,
        )
        if type(restore) is not CompletedPromotedOperationalInputRestore:
            raise PromotedOperationalHydratedCloudJobError(_ERR_JOB)
        restore = CompletedPromotedOperationalInputRestore(request=restore.request, manifest=restore.manifest)
        if (
            restore.manifest.snapshot_id != launch.input_restore.expected_snapshot_id
            or restore.manifest.inventory.expected_assembly_spec_id != launch.expected_assembly_spec_id
            or restore.manifest.inventory.target_session != launch.target_session
            or restore.manifest.bucket != launch.state_bucket
        ):
            raise PromotedOperationalHydratedCloudJobError(_ERR_JOB)

        _recheck_runtime_parent(parent, parent_identity)

        control_payload = encode_promoted_operational_cloud_control(hydration_control)
        runtime_control_file = parent / "runtime-control.json"
        _write_runtime_control_file(runtime_control_file, control_payload)

        runtime_environ = os.environ if environ is None else environ
        inner_kwargs: dict[str, object] = {
            "environ": runtime_environ,
            "gcs_client_factory": lambda: client,
        }
        if clock is not None:
            inner_kwargs["clock"] = clock
        if kite_adapter_factory is not None:
            inner_kwargs["kite_adapter_factory"] = kite_adapter_factory
        if runtime_callable is not None:
            inner_kwargs["runtime_callable"] = runtime_callable

        inner_argv = ["--control-file", str(runtime_control_file)]

        inner_stdout = io.StringIO()
        inner_stderr = io.StringIO()
        with redirect_stdout(inner_stdout), redirect_stderr(inner_stderr):
            inner_exit_code = active_cloud_job_main(inner_argv, **inner_kwargs)

        inner_stdout_text = inner_stdout.getvalue()
        inner_stderr_text = inner_stderr.getvalue()
        if inner_exit_code != 0 or inner_stderr_text != "":
            raise PromotedOperationalHydratedCloudJobError(_ERR_JOB)
        if len(inner_stdout_text.encode("utf-8")) > _MAXIMUM_INNER_STDOUT_BYTES:
            raise PromotedOperationalHydratedCloudJobError(_ERR_JOB)

        stdout_lines = inner_stdout_text.split("\n")
        if len(stdout_lines) != 2 or stdout_lines[1] != "" or not stdout_lines[0]:
            raise PromotedOperationalHydratedCloudJobError(_ERR_JOB)
        inner_line = stdout_lines[0]

        inner_envelope = json.loads(
            inner_line,
            object_pairs_hook=_unique_object,
            parse_float=_reject_number,
            parse_constant=_reject_number,
        )
        if type(inner_envelope) is not dict or set(inner_envelope) != _INNER_CLOUD_JOB_SUCCESS_KEYS:
            raise PromotedOperationalHydratedCloudJobError(_ERR_JOB)
        if (
            json.dumps(
                inner_envelope, allow_nan=False, ensure_ascii=False, separators=(",", ":"), sort_keys=True,
            )
            != inner_line
        ):
            raise PromotedOperationalHydratedCloudJobError(_ERR_JOB)

        _validate_inner_envelope(inner_envelope, launch=launch, hydration_control=hydration_control)

        input_manifest_payload = encode_promoted_operational_input_snapshot_manifest(acquired.manifest)
        if hashlib.sha256(input_manifest_payload).hexdigest() != launch.input_restore.expected_sha256:
            raise PromotedOperationalHydratedCloudJobError(_ERR_JOB)

        result: dict[str, object] = dict(inner_envelope)
        result["status"] = "PROMOTED_OPERATIONAL_HYDRATED_CLOUD_JOB_COMPLETE"
        result["inner_status"] = inner_envelope["status"]
        result["launch_id"] = launch.launch_id
        result["input_snapshot_id"] = acquired.manifest.snapshot_id
        result["input_manifest_object_name"] = launch.input_restore.manifest_object_name
        result["input_manifest_generation"] = launch.input_restore.generation
        result["input_manifest_sha256"] = launch.input_restore.expected_sha256
        result["input_manifest_byte_count"] = len(input_manifest_payload)

        if set(result) != _RESULT_KEYS:
            raise PromotedOperationalHydratedCloudJobError(_ERR_JOB)

        print(
            json.dumps(
                result, allow_nan=False, ensure_ascii=False, separators=(",", ":"), sort_keys=True,
            )
        )
        return 0
    except Exception:
        print(
            json.dumps(
                {"error_type": PromotedOperationalHydratedCloudJobError.__name__, "status": "FAILED"},
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
