"""Offline operator CLI for preparing one promoted-operational cloud control.

The command loads one exact assembly spec, resolves its exact preparation and
portfolio artifact from explicitly supplied local roots, derives the accepted
operational/runtime specs through ``assemble_promoted_operational_runtime_inputs``,
and publishes one canonical ``PromotedOperationalCloudRunControl`` file.

It performs no market-data request, GCS call, deployment, scheduling,
notification, order placement, environment read, or clock read.  A same-run
restart is optional and must be supplied as the three exact pinned state
manifest coordinates emitted by a previous hydrated-cloud invocation.
"""

from __future__ import annotations

import json
import os
import re
import stat
import sys
from collections.abc import Sequence
from pathlib import Path

from india_swing._filesystem import read_stable_regular_file
from india_swing.operations.portfolio_store import LocalSwingPortfolioArtifactStore
from india_swing.promoted_operational_assembly import (
    assemble_promoted_operational_runtime_inputs,
    load_promoted_operational_assembly_spec_file,
)
from india_swing.promoted_operational_cloud_control import (
    MAXIMUM_CLOUD_CONTROL_BYTES,
    PromotedOperationalCloudRunControl,
    decode_promoted_operational_cloud_control,
    encode_promoted_operational_cloud_control,
)
from india_swing.promoted_operational_gcs_state import PromotedOperationalGCSRestoreRequest
from india_swing.promoted_operational_preparation import (
    build_promoted_operational_preparation_store,
)


class PromotedOperationalCloudControlPrepareError(ValueError):
    pass


_ERR = "promoted operational cloud control prepare call is invalid"
_CONCRETE_PATH_TYPE = type(Path())
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_MAXIMUM_GENERATION = 9_223_372_036_854_775_807

_ROOT_OPTIONS = (
    "--reference-root",
    "--identity-evidence-root",
    "--calendar-root",
    "--daily-reports-root",
    "--historical-corpus-root",
    "--promoted-root",
    "--graph-publication-root",
    "--engine-run-root",
    "--research-run-root",
    "--operational-preparation-root",
    "--portfolio-artifact-root",
)
_REQUIRED_OPTIONS = (
    "--assembly-spec-file",
    *_ROOT_OPTIONS,
    "--state-root",
    "--output-control-file",
)
_PRIOR_OPTIONS = (
    "--prior-state-manifest-object-name",
    "--prior-state-manifest-generation",
    "--prior-state-manifest-sha256",
)
_ALL_OPTIONS = frozenset((*_REQUIRED_OPTIONS, *_PRIOR_OPTIONS))

_RESULT_KEYS = frozenset(
    {
        "status",
        "control_id",
        "assembly_spec_id",
        "operational_run_spec_id",
        "runtime_job_spec_id",
        "preparation_id",
        "portfolio_artifact_id",
        "target_session",
        "candidate_count",
        "open_position_count",
        "prior_state_restore_present",
        "paper_only",
        "notification_eligible",
        "execution_eligible",
    }
)


def _parse_path(value: object) -> Path:
    if type(value) is not str or not value:
        raise PromotedOperationalCloudControlPrepareError(_ERR)
    path = Path(value)
    if not path.is_absolute() or ".." in path.parts:
        raise PromotedOperationalCloudControlPrepareError(_ERR)
    return path


def _parse_arguments(argv: Sequence[str]) -> tuple[dict[str, Path], dict[str, str]]:
    if type(argv) not in (list, tuple) or len(argv) % 2 != 0:
        raise PromotedOperationalCloudControlPrepareError(_ERR)
    values: dict[str, str] = {}
    index = 0
    while index < len(argv):
        option = argv[index]
        value = argv[index + 1]
        if (
            type(option) is not str
            or type(value) is not str
            or option not in _ALL_OPTIONS
            or option in values
            or not value
        ):
            raise PromotedOperationalCloudControlPrepareError(_ERR)
        values[option] = value
        index += 2

    if not set(_REQUIRED_OPTIONS).issubset(values):
        raise PromotedOperationalCloudControlPrepareError(_ERR)
    prior_present = set(values).intersection(_PRIOR_OPTIONS)
    if prior_present and prior_present != set(_PRIOR_OPTIONS):
        raise PromotedOperationalCloudControlPrepareError(_ERR)

    paths = {option: _parse_path(values[option]) for option in _REQUIRED_OPTIONS}
    prior = {option: values[option] for option in _PRIOR_OPTIONS if option in values}
    return paths, prior


def _is_link_like(status: os.stat_result) -> bool:
    if stat.S_ISLNK(status.st_mode):
        return True
    attribute = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400)
    return bool(getattr(status, "st_file_attributes", 0) & attribute)


def _validate_read_only_roots(paths: dict[str, Path]) -> None:
    """Validate only the eleven snapshot input roots.

    ``state_root`` is intentionally excluded: it is neither an input snapshot
    root nor a source this offline preparer may inspect or create.
    """

    for option in _ROOT_OPTIONS:
        failed = False
        status: os.stat_result | None = None
        try:
            status = os.lstat(paths[option])
        except OSError:
            failed = True
        if (
            failed
            or status is None
            or not stat.S_ISDIR(status.st_mode)
            or _is_link_like(status)
        ):
            raise PromotedOperationalCloudControlPrepareError(_ERR)


def _paths_overlap(first: Path, second: Path) -> bool:
    first_parts = first.parts
    second_parts = second.parts
    if len(first_parts) <= len(second_parts):
        shorter, longer = first_parts, second_parts
    else:
        shorter, longer = second_parts, first_parts
    return longer[: len(shorter)] == shorter


def _reject_output_overlap(paths: dict[str, Path]) -> None:
    output = paths["--output-control-file"]
    protected = (paths["--assembly-spec-file"], paths["--state-root"])
    protected += tuple(paths[option] for option in _ROOT_OPTIONS)
    if any(_paths_overlap(output, path) for path in protected):
        raise PromotedOperationalCloudControlPrepareError(_ERR)


def _parse_generation(value: object) -> int:
    if type(value) is not str or not re.fullmatch(r"[1-9][0-9]*", value):
        raise PromotedOperationalCloudControlPrepareError(_ERR)
    generation = int(value)
    if generation > _MAXIMUM_GENERATION:
        raise PromotedOperationalCloudControlPrepareError(_ERR)
    return generation


def _prior_restore(
    prior: dict[str, str],
    *,
    bucket: str,
    operational_run_spec_id: str,
) -> PromotedOperationalGCSRestoreRequest | None:
    if not prior:
        return None
    expected_sha256 = prior["--prior-state-manifest-sha256"]
    if type(expected_sha256) is not str or _SHA256.fullmatch(expected_sha256) is None:
        raise PromotedOperationalCloudControlPrepareError(_ERR)
    failed = False
    restore: PromotedOperationalGCSRestoreRequest | None = None
    try:
        restore = PromotedOperationalGCSRestoreRequest(
            bucket=bucket,
            manifest_object_name=prior["--prior-state-manifest-object-name"],
            generation=_parse_generation(prior["--prior-state-manifest-generation"]),
            expected_sha256=expected_sha256,
            expected_spec_id=operational_run_spec_id,
        )
    except Exception:
        failed = True
    if failed or restore is None:
        raise PromotedOperationalCloudControlPrepareError(_ERR)
    return restore


def _parent_identity(path: Path) -> tuple[Path, tuple[int, int]]:
    parent = path.parent
    failed = False
    status: os.stat_result | None = None
    try:
        status = os.lstat(parent)
    except OSError:
        failed = True
    if (
        failed
        or status is None
        or not stat.S_ISDIR(status.st_mode)
        or _is_link_like(status)
    ):
        raise PromotedOperationalCloudControlPrepareError(_ERR)
    return parent, (status.st_dev, status.st_ino)


def _verify_parent_identity(parent: Path, expected: tuple[int, int]) -> None:
    failed = False
    status: os.stat_result | None = None
    try:
        status = os.lstat(parent)
    except OSError:
        failed = True
    if (
        failed
        or status is None
        or not stat.S_ISDIR(status.st_mode)
        or _is_link_like(status)
        or (status.st_dev, status.st_ino) != expected
    ):
        raise PromotedOperationalCloudControlPrepareError(_ERR)


def _target_present(path: Path) -> bool:
    try:
        os.lstat(path)
    except FileNotFoundError:
        return False
    except OSError:
        raise PromotedOperationalCloudControlPrepareError(_ERR) from None
    return True


def _publish_control_file(path: Path, payload: bytes) -> None:
    """Create once, or accept one byte-identical stable existing file."""

    if (
        type(path) is not _CONCRETE_PATH_TYPE
        or type(payload) is not bytes
        or not (0 < len(payload) <= MAXIMUM_CLOUD_CONTROL_BYTES)
    ):
        raise PromotedOperationalCloudControlPrepareError(_ERR)
    parent, parent_identity = _parent_identity(path)

    if _target_present(path):
        _verify_parent_identity(parent, parent_identity)
        failed = False
        existing = b""
        try:
            existing = read_stable_regular_file(path, maximum_bytes=MAXIMUM_CLOUD_CONTROL_BYTES)
        except Exception:
            failed = True
        _verify_parent_identity(parent, parent_identity)
        if failed or existing != payload:
            raise PromotedOperationalCloudControlPrepareError(_ERR)
        return

    _verify_parent_identity(parent, parent_identity)
    failed = False
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
            written = handle.write(payload)
            if type(written) is not int or written != len(payload):
                raise OSError("short write")
            handle.flush()
            os.fsync(handle.fileno())
            opened = os.fstat(handle.fileno())
            if (
                not stat.S_ISREG(opened.st_mode)
                or _is_link_like(opened)
                or opened.st_nlink != 1
                or opened.st_size != len(payload)
            ):
                raise OSError("unsafe output")
    except Exception:
        failed = True
    if failed:
        raise PromotedOperationalCloudControlPrepareError(_ERR)

    read_failed = False
    stored = b""
    try:
        stored = read_stable_regular_file(path, maximum_bytes=MAXIMUM_CLOUD_CONTROL_BYTES)
    except Exception:
        read_failed = True
    _verify_parent_identity(parent, parent_identity)
    if read_failed or stored != payload:
        raise PromotedOperationalCloudControlPrepareError(_ERR)


def _root_kwargs(paths: dict[str, Path]) -> dict[str, Path]:
    return {option[2:].replace("-", "_"): paths[option] for option in _ROOT_OPTIONS}


def main(argv: Sequence[str] | None = None) -> int:
    args = list(argv) if argv is not None else sys.argv[1:]
    try:
        paths, prior_values = _parse_arguments(args)
        _reject_output_overlap(paths)
        _validate_read_only_roots(paths)

        assembly_spec = load_promoted_operational_assembly_spec_file(
            paths["--assembly-spec-file"]
        )
        root_kwargs = _root_kwargs(paths)
        preparation_kwargs = {
            key: value
            for key, value in root_kwargs.items()
            if key != "portfolio_artifact_root"
        }
        _, preparations = build_promoted_operational_preparation_store(
            **preparation_kwargs
        )
        portfolio_artifacts = LocalSwingPortfolioArtifactStore(
            root_kwargs["portfolio_artifact_root"]
        )
        assembly = assemble_promoted_operational_runtime_inputs(
            spec=assembly_spec,
            preparation_resolver=preparations,
            portfolio_artifact_resolver=portfolio_artifacts,
        )

        # Re-read the source assembly after resolver work so a mutation during
        # preflight cannot produce a READY control bound to stale bytes.
        final_spec = load_promoted_operational_assembly_spec_file(
            paths["--assembly-spec-file"]
        )
        if final_spec.assembly_spec_id != assembly_spec.assembly_spec_id:
            raise PromotedOperationalCloudControlPrepareError(_ERR)

        restore = _prior_restore(
            prior_values,
            bucket=assembly_spec.binding_bucket,
            operational_run_spec_id=assembly.run_spec.spec_id,
        )
        control = PromotedOperationalCloudRunControl(
            expected_assembly_spec_id=assembly_spec.assembly_spec_id,
            expected_operational_run_spec_id=assembly.run_spec.spec_id,
            target_session=assembly_spec.target_session,
            state_bucket=assembly_spec.binding_bucket,
            assembly_spec_file=paths["--assembly-spec-file"],
            state_root=paths["--state-root"],
            prior_state_restore=restore,
            **root_kwargs,
        )
        control.verify_content_identity()
        payload = encode_promoted_operational_cloud_control(control)
        _publish_control_file(paths["--output-control-file"], payload)

        # Independent cold decode of the exact published bytes.
        published = decode_promoted_operational_cloud_control(
            read_stable_regular_file(
                paths["--output-control-file"],
                maximum_bytes=MAXIMUM_CLOUD_CONTROL_BYTES,
            )
        )
        if published.control_id != control.control_id:
            raise PromotedOperationalCloudControlPrepareError(_ERR)

        result: dict[str, object] = {
            "status": "PROMOTED_OPERATIONAL_CLOUD_CONTROL_READY",
            "control_id": published.control_id,
            "assembly_spec_id": assembly_spec.assembly_spec_id,
            "operational_run_spec_id": assembly.run_spec.spec_id,
            "runtime_job_spec_id": assembly.runtime_job_spec.job_spec_id,
            "preparation_id": assembly_spec.preparation_id,
            "portfolio_artifact_id": assembly_spec.portfolio_artifact_id,
            "target_session": assembly_spec.target_session.isoformat(),
            "candidate_count": len(assembly.preparation.candidates),
            "open_position_count": len(assembly_spec.open_listing_keys),
            "prior_state_restore_present": restore is not None,
            "paper_only": True,
            "notification_eligible": False,
            "execution_eligible": False,
        }
        if set(result) != _RESULT_KEYS:
            raise PromotedOperationalCloudControlPrepareError(_ERR)
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
                    "error_type": PromotedOperationalCloudControlPrepareError.__name__,
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
