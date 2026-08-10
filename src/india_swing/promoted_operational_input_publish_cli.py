"""Offline local publisher CLI.

Reads one existing operator-authored ``PromotedOperationalCloudRunControl``,
publishes its complete local promoted-operational input snapshot to GCS
exactly once, derives the exact externally pinned
``PromotedOperationalInputRestoreRequest`` from the resulting publication,
and writes one portable, path-free ``PromotedOperationalHydratedCloudLaunch``
launch-control file -- the single artifact a later, separately reviewed
hydrated Cloud Run entrypoint needs to restore the exact pinned input
snapshot into a fresh ephemeral container filesystem.

This CLI performs no deployment, no scheduling, no notification, no
broker/order call, and never runs the promoted engine itself. It
constructs exactly one GCS client through an injectable factory and calls
the accepted ``publish_promoted_operational_input_snapshot`` boundary
exactly once; it never lists a bucket, never selects a "latest" object,
and never overwrites, deletes, or repairs its own output file.
"""

from __future__ import annotations

import json
import os
import stat
import sys
from collections.abc import Callable, Sequence
from pathlib import Path

from india_swing._filesystem import read_stable_regular_file
from india_swing.daily_pipeline.state_publication import GoogleCloudStorageStateObjectWriter
from india_swing.promoted_operational_assembly import load_promoted_operational_assembly_spec_file
from india_swing.promoted_operational_cloud_control import (
    MAXIMUM_CLOUD_CONTROL_BYTES,
    decode_promoted_operational_cloud_control,
)
from india_swing.promoted_operational_hydrated_cloud_control import (
    MAXIMUM_HYDRATED_CLOUD_LAUNCH_BYTES,
    PromotedOperationalHydratedCloudLaunch,
    encode_promoted_operational_hydrated_cloud_launch,
)
from india_swing.promoted_operational_input_gcs import (
    CompletedPromotedOperationalInputPublication,
    PromotedOperationalInputRestoreRequest,
    publish_promoted_operational_input_snapshot,
)


class PromotedOperationalInputPublishError(ValueError):
    pass


_ERR = "promoted operational input publish call is invalid"

_CONCRETE_PATH_TYPE = type(Path())

_RESULT_KEYS = frozenset(
    {
        "status",
        "launch_id",
        "input_snapshot_id",
        "expected_assembly_spec_id",
        "expected_operational_run_spec_id",
        "target_session",
        "input_manifest_object_name",
        "input_manifest_generation",
        "input_manifest_sha256",
        "input_manifest_byte_count",
    }
)


def _path_argument(value: object) -> Path:
    if type(value) is not str or not value:
        raise PromotedOperationalInputPublishError(_ERR)
    path = Path(value)
    if not path.is_absolute() or ".." in path.parts:
        raise PromotedOperationalInputPublishError(_ERR)
    return path


def _parse_args(argv: Sequence[str]) -> tuple[Path, Path]:
    if len(argv) != 4:
        raise PromotedOperationalInputPublishError(_ERR)
    options: dict[str, str] = {}
    iterator = iter(argv)
    for flag, value in zip(iterator, iterator):
        if type(flag) is not str or flag in options:
            raise PromotedOperationalInputPublishError(_ERR)
        options[flag] = value
    if set(options) != {"--source-control-file", "--output-launch-file"}:
        raise PromotedOperationalInputPublishError(_ERR)
    source = _path_argument(options["--source-control-file"])
    output = _path_argument(options["--output-launch-file"])
    return source, output


def _default_gcs_client_factory() -> object:
    from google.cloud import storage

    return storage.Client()


def _is_link_like(status: os.stat_result) -> bool:
    if stat.S_ISLNK(status.st_mode):
        return True
    attribute = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400)
    return bool(getattr(status, "st_file_attributes", 0) & attribute)


def _parent_identity(path: Path) -> tuple[Path, tuple[int, int]]:
    """Capture the output file's parent directory identity with exactly
    one ``os.lstat`` call -- never a split ``Path.is_dir()`` /
    ``Path.is_symlink()`` pair, which is two separate syscalls and cannot
    itself detect a replacement between them."""

    parent = path.parent
    status_failed = False
    status: os.stat_result | None = None
    try:
        status = os.lstat(parent)
    except OSError:
        status_failed = True
    if status_failed or status is None:
        raise PromotedOperationalInputPublishError(_ERR)
    if not stat.S_ISDIR(status.st_mode) or _is_link_like(status):
        raise PromotedOperationalInputPublishError(_ERR)
    return parent, (status.st_dev, status.st_ino)


def _verify_parent_identity(parent: Path, expected_identity: tuple[int, int]) -> None:
    """Recheck the captured parent identity. Bounded discipline only: this
    detects an *observable* replacement (the originally validated parent
    renamed away and a different directory created at the same path) --
    it makes no claim to eliminate an unobservable kernel-level race
    between this check and the very next syscall."""

    status_failed = False
    status: os.stat_result | None = None
    try:
        status = os.lstat(parent)
    except OSError:
        status_failed = True
    if status_failed or status is None:
        raise PromotedOperationalInputPublishError(_ERR)
    if (
        not stat.S_ISDIR(status.st_mode)
        or _is_link_like(status)
        or (status.st_dev, status.st_ino) != expected_identity
    ):
        raise PromotedOperationalInputPublishError(_ERR)


def _publish_output_launch_file(path: Path, payload: bytes) -> None:
    """Create-once local writer for the launch-control bytes, adapted from
    the accepted create-once local-file-safety discipline used elsewhere in
    this codebase: an absent destination is created exclusively
    (``O_CREAT | O_EXCL``), fully written, fsynced, and cold-read back
    byte-identical -- never a temporary-file rename, which could silently
    replace an existing file. An existing exact regular file is accepted
    only if it is byte-identical to the freshly encoded payload; any other
    existing content (different, a symlink/reparse point, or otherwise
    unsafe) fails closed. The parent directory must already exist as a
    real, non-link directory; its identity is captured once and rechecked
    before an existing-file replay read, immediately before exclusive
    creation, and after the cold read -- this function never creates or
    scans a directory, and it never overwrites, truncates, deletes, or
    repairs."""

    if type(path) is not _CONCRETE_PATH_TYPE:
        raise PromotedOperationalInputPublishError(_ERR)

    parent, parent_identity = _parent_identity(path)

    exists_check_failed = False
    target_present = False
    try:
        target_present = path.exists() or path.is_symlink()
    except Exception:
        exists_check_failed = True
    if exists_check_failed:
        raise PromotedOperationalInputPublishError(_ERR)

    if target_present:
        _verify_parent_identity(parent, parent_identity)
        replay_failed = False
        existing = b""
        try:
            existing = read_stable_regular_file(path, maximum_bytes=MAXIMUM_HYDRATED_CLOUD_LAUNCH_BYTES)
        except Exception:
            replay_failed = True
        if replay_failed or existing != payload:
            raise PromotedOperationalInputPublishError(_ERR)
        return

    _verify_parent_identity(parent, parent_identity)

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
        raise PromotedOperationalInputPublishError(_ERR)

    cold_read_failed = False
    cold_bytes = b""
    try:
        cold_bytes = read_stable_regular_file(path, maximum_bytes=MAXIMUM_HYDRATED_CLOUD_LAUNCH_BYTES)
    except Exception:
        cold_read_failed = True
    if cold_read_failed or cold_bytes != payload:
        raise PromotedOperationalInputPublishError(_ERR)

    _verify_parent_identity(parent, parent_identity)


def main(
    argv: Sequence[str] | None = None,
    *,
    gcs_client_factory: Callable[[], object] | None = None,
) -> int:
    args = list(argv) if argv is not None else sys.argv[1:]
    try:
        source_control_file, output_launch_file = _parse_args(args)

        control_payload = read_stable_regular_file(
            source_control_file, maximum_bytes=MAXIMUM_CLOUD_CONTROL_BYTES
        )
        control = decode_promoted_operational_cloud_control(control_payload)

        assembly_spec = load_promoted_operational_assembly_spec_file(control.assembly_spec_file)
        if (
            assembly_spec.assembly_spec_id != control.expected_assembly_spec_id
            or assembly_spec.target_session != control.target_session
            or assembly_spec.binding_bucket != control.state_bucket
        ):
            raise PromotedOperationalInputPublishError(_ERR)

        active_gcs_client_factory = (
            gcs_client_factory if gcs_client_factory is not None else _default_gcs_client_factory
        )
        if not callable(active_gcs_client_factory):
            raise PromotedOperationalInputPublishError(_ERR)

        client = active_gcs_client_factory()
        if client is None:
            raise PromotedOperationalInputPublishError(_ERR)
        writer = GoogleCloudStorageStateObjectWriter(client=client)

        publication = publish_promoted_operational_input_snapshot(
            control=control, bucket=control.state_bucket, writer=writer,
        )
        if type(publication) is not CompletedPromotedOperationalInputPublication:
            raise PromotedOperationalInputPublishError(_ERR)
        publication = CompletedPromotedOperationalInputPublication(
            manifest=publication.manifest, manifest_object=publication.manifest_object,
        )
        if (
            publication.manifest.bucket != control.state_bucket
            or publication.manifest.inventory.expected_assembly_spec_id != control.expected_assembly_spec_id
            or publication.manifest.inventory.target_session != control.target_session
        ):
            raise PromotedOperationalInputPublishError(_ERR)

        input_restore = PromotedOperationalInputRestoreRequest(
            bucket=control.state_bucket,
            manifest_object_name=publication.manifest_object.object_name,
            generation=publication.manifest_object.generation,
            expected_sha256=publication.manifest_object.sha256,
            expected_snapshot_id=publication.manifest.snapshot_id,
            expected_assembly_spec_id=control.expected_assembly_spec_id,
            target_session=control.target_session,
        )

        launch = PromotedOperationalHydratedCloudLaunch(
            expected_assembly_spec_id=control.expected_assembly_spec_id,
            expected_operational_run_spec_id=control.expected_operational_run_spec_id,
            target_session=control.target_session,
            state_bucket=control.state_bucket,
            input_restore=input_restore,
            prior_state_restore=control.prior_state_restore,
        )

        launch_payload = encode_promoted_operational_hydrated_cloud_launch(launch)
        _publish_output_launch_file(output_launch_file, launch_payload)

        result: dict[str, object] = {
            "status": "PROMOTED_OPERATIONAL_INPUT_LAUNCH_READY",
            "launch_id": launch.launch_id,
            "input_snapshot_id": publication.manifest.snapshot_id,
            "expected_assembly_spec_id": launch.expected_assembly_spec_id,
            "expected_operational_run_spec_id": launch.expected_operational_run_spec_id,
            "target_session": launch.target_session.isoformat(),
            "input_manifest_object_name": publication.manifest_object.object_name,
            "input_manifest_generation": publication.manifest_object.generation,
            "input_manifest_sha256": publication.manifest_object.sha256,
            "input_manifest_byte_count": publication.manifest_object.byte_count,
        }
        if (
            type(result) is not dict
            or set(result) != _RESULT_KEYS
            or type(result["status"]) is not str
            or type(result["launch_id"]) is not str
            or type(result["input_snapshot_id"]) is not str
            or type(result["expected_assembly_spec_id"]) is not str
            or type(result["expected_operational_run_spec_id"]) is not str
            or type(result["target_session"]) is not str
            or type(result["input_manifest_object_name"]) is not str
            or type(result["input_manifest_generation"]) is not int
            or type(result["input_manifest_sha256"]) is not str
            or type(result["input_manifest_byte_count"]) is not int
        ):
            raise PromotedOperationalInputPublishError(_ERR)

        print(
            json.dumps(
                result, allow_nan=False, ensure_ascii=False, separators=(",", ":"), sort_keys=True,
            )
        )
        return 0
    except Exception:
        print(
            json.dumps(
                {"error_type": PromotedOperationalInputPublishError.__name__, "status": "FAILED"},
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
