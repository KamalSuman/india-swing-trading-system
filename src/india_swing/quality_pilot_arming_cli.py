"""Offline-only HYP-002 quality pilot arming CLI.

Two commands, both entirely local and offline:

- ``compile-runbook`` reads one exact absolute traversal-free draft JSON
  file and atomically creates one new output runbook file -- it never
  overwrites an existing path.
- ``inspect-plan`` reads one exact runbook file plus one exact arming
  manifest file, cross-verifies their full identities and agreement, and
  emits a compact sanitized deployment-plan envelope on stdout.

This module never accesses credentials, GCP, Kite, or the network, and never
spawns a child process. It is the only module in this task authorized to
read or write an exact local file path.
"""

from __future__ import annotations

import json
import os
import sys
from collections.abc import Sequence
from pathlib import Path

from india_swing._filesystem import read_stable_regular_file
from india_swing.quality_pilot.arming import (
    MAXIMUM_DRAFT_BYTES,
    MAXIMUM_MANIFEST_BYTES,
    decode_quality_pilot_arming_manifest,
    decode_quality_pilot_runbook_draft,
    compile_quality_pilot_invocation_runbook,
)
from india_swing.quality_pilot.deployment_plan import render_quality_pilot_deployment_plan
from india_swing.quality_pilot.invocation_control_plane import (
    MAXIMUM_RUNBOOK_BYTES,
    decode_quality_pilot_invocation_runbook,
)


class QualityPilotArmingCliError(ValueError):
    pass


_ERR_CLI = "quality pilot arming CLI call is invalid"

_COMPILE_RUNBOOK_OPTIONS = ("--draft-file", "--output-file")
_INSPECT_PLAN_OPTIONS = ("--runbook-file", "--manifest-file")


def _fail(message: str) -> None:
    raise QualityPilotArmingCliError(message)


def _parse_options(argv: Sequence[str], allowed: tuple[str, ...]) -> dict[str, str]:
    values: dict[str, str] = {}
    index = 0
    while index < len(argv):
        token = argv[index]
        if token not in allowed or token in values:
            _fail(_ERR_CLI)
        if index + 1 >= len(argv):
            _fail(_ERR_CLI)
        value = argv[index + 1]
        if type(value) is not str or not value:
            _fail(_ERR_CLI)
        values[token] = value
        index += 2
    if set(allowed) != set(values):
        _fail(_ERR_CLI)
    return values


def _absolute_traversal_free_path(raw: str) -> Path:
    path = Path(raw)
    if not path.is_absolute() or ".." in path.parts:
        _fail(_ERR_CLI)
    return path


def _create_new_file_without_overwrite(path: Path, content_bytes: bytes) -> None:
    """Atomically create one new local file. Never overwrites, never
    follows a symlink, never truncates an existing regular file."""

    flags = (
        os.O_WRONLY
        | os.O_CREAT
        | os.O_EXCL
        | getattr(os, "O_BINARY", 0)
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NOINHERIT", 0)
        | getattr(os, "O_NOFOLLOW", 0)
    )
    create_failed = False
    try:
        descriptor = os.open(path, flags, 0o600)
    except OSError:
        create_failed = True
    if create_failed:
        _fail(_ERR_CLI)
    write_failed = False
    try:
        with os.fdopen(descriptor, "wb", closefd=True) as handle:
            handle.write(content_bytes)
            handle.flush()
            os.fsync(handle.fileno())
    except Exception:
        write_failed = True
    if write_failed:
        _fail(_ERR_CLI)


def _run_compile_runbook(args: Sequence[str]) -> dict[str, object]:
    options = _parse_options(args, _COMPILE_RUNBOOK_OPTIONS)
    draft_path = _absolute_traversal_free_path(options["--draft-file"])
    output_path = _absolute_traversal_free_path(options["--output-file"])
    if draft_path == output_path:
        _fail(_ERR_CLI)

    read_failed = False
    draft_bytes = b""
    try:
        draft_bytes = read_stable_regular_file(draft_path, maximum_bytes=MAXIMUM_DRAFT_BYTES)
    except Exception:
        read_failed = True
    if read_failed:
        _fail(_ERR_CLI)

    decode_failed = False
    draft: object = None
    try:
        draft = decode_quality_pilot_runbook_draft(draft_bytes)
    except Exception:
        decode_failed = True
    if decode_failed or draft is None:
        _fail(_ERR_CLI)

    compile_failed = False
    runbook: object = None
    encoded: bytes = b""
    try:
        runbook, encoded = compile_quality_pilot_invocation_runbook(draft)
    except Exception:
        compile_failed = True
    if compile_failed or runbook is None or not encoded:
        _fail(_ERR_CLI)

    _create_new_file_without_overwrite(output_path, encoded)

    return {
        "status": "QUALITY_PILOT_RUNBOOK_COMPILED",
        "runbook_id": runbook.runbook_id,
        "pilot_run_id": runbook.campaign.pilot_run_id,
        "bucket": runbook.bucket,
        "window_count": len(runbook.windows),
        "quality_only": True,
    }


def _run_inspect_plan(args: Sequence[str]) -> dict[str, object]:
    options = _parse_options(args, _INSPECT_PLAN_OPTIONS)
    runbook_path = _absolute_traversal_free_path(options["--runbook-file"])
    manifest_path = _absolute_traversal_free_path(options["--manifest-file"])

    runbook_read_failed = False
    runbook_bytes = b""
    try:
        runbook_bytes = read_stable_regular_file(runbook_path, maximum_bytes=MAXIMUM_RUNBOOK_BYTES)
    except Exception:
        runbook_read_failed = True
    if runbook_read_failed:
        _fail(_ERR_CLI)
    runbook_decode_failed = False
    runbook: object = None
    try:
        runbook = decode_quality_pilot_invocation_runbook(runbook_bytes)
    except Exception:
        runbook_decode_failed = True
    if runbook_decode_failed or runbook is None:
        _fail(_ERR_CLI)

    manifest_read_failed = False
    manifest_bytes = b""
    try:
        manifest_bytes = read_stable_regular_file(manifest_path, maximum_bytes=MAXIMUM_MANIFEST_BYTES)
    except Exception:
        manifest_read_failed = True
    if manifest_read_failed:
        _fail(_ERR_CLI)
    manifest_decode_failed = False
    manifest: object = None
    try:
        manifest = decode_quality_pilot_arming_manifest(manifest_bytes, runbook=runbook)
    except Exception:
        manifest_decode_failed = True
    if manifest_decode_failed or manifest is None:
        _fail(_ERR_CLI)

    plan_failed = False
    plan: dict[str, object] | None = None
    try:
        plan = render_quality_pilot_deployment_plan(manifest)
    except Exception:
        plan_failed = True
    if plan_failed or plan is None:
        _fail(_ERR_CLI)

    return {
        "status": "QUALITY_PILOT_PLAN_INSPECTED",
        "manifest_id": manifest.manifest_id,
        "runbook_id": manifest.runbook_id,
        "pilot_run_id": manifest.pilot_run_id,
        "bucket": manifest.bucket,
        "gcp_project_id": manifest.gcp_project_id,
        "gcp_region": manifest.gcp_region,
        "gcp_job_name": manifest.gcp_job_name,
        "scheduler_job_names": sorted(item["name"] for item in plan["cloud_scheduler_jobs"]),
        "cloud_run_resource_name": plan["cloud_run_job"]["resource_name"],
        "armed": manifest.armed,
        "quality_only": True,
    }


def main(argv: Sequence[str] | None = None) -> int:
    args = list(argv) if argv is not None else sys.argv[1:]
    try:
        if not args:
            _fail(_ERR_CLI)
        command = args[0]
        remaining = args[1:]
        if command not in ("compile-runbook", "inspect-plan"):
            _fail(_ERR_CLI)

        if command == "compile-runbook":
            envelope = _run_compile_runbook(remaining)
        else:
            envelope = _run_inspect_plan(remaining)

        print(
            json.dumps(envelope, allow_nan=False, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
        )
        return 0
    except Exception:
        print(
            json.dumps(
                {"error_type": QualityPilotArmingCliError.__name__, "status": "FAILED"},
                allow_nan=False, ensure_ascii=False, separators=(",", ":"), sort_keys=True,
            ),
            file=sys.stderr,
        )
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
