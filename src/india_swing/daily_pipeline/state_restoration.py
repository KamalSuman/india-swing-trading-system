from __future__ import annotations

import hashlib
import os
import shutil
import stat
import tempfile
from dataclasses import dataclass
from pathlib import Path

from india_swing._filesystem import (
    FileSafetyError,
    advisory_file_lock,
    read_stable_regular_file,
)

from .state_hydration import (
    HydratedPipelineStateEntry,
    VerifiedHydratedPipelineState,
)
from .state_inventory import MAXIMUM_FILE_BYTES, ROOT_NAMES, PipelineStateRoots


_ERROR_STATE = "pipeline state restoration input verification failed"
_ERROR_DESTINATION = "pipeline state restoration destination verification failed"
_ERROR_STAGE = "pipeline state restoration staging failed"
_ERROR_VERIFY = "pipeline state restoration tree verification failed"
_ERROR_PUBLISH = "pipeline state restoration publication failed"
_ERROR_CLEANUP = "pipeline state restoration cleanup failed"
_ERROR_RESULT = "pipeline state restoration result verification failed"

_CONCRETE_PATH_TYPE: type = type(Path())
_LOCK_FILE_NAME = ".pipeline-state-restore.lock"
_STAGING_INFIX = ".restore-"
_SHA256_CHARS = frozenset("0123456789abcdef")


class PipelineStateRestorationError(Exception):
    pass


def _is_link_like(status: os.stat_result) -> bool:
    if stat.S_ISLNK(status.st_mode):
        return True
    attribute = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400)
    return bool(getattr(status, "st_file_attributes", 0) & attribute)


def _reconstructed_state(value: object) -> VerifiedHydratedPipelineState:
    if type(value) is not VerifiedHydratedPipelineState:
        raise PipelineStateRestorationError(_ERROR_STATE)

    failed = False
    reconstructed: VerifiedHydratedPipelineState | None = None
    try:
        reconstructed = VerifiedHydratedPipelineState(
            acquired_blobs=value.acquired_blobs,
            entries=value.entries,
        )
    except Exception:
        failed = True
    if failed or reconstructed is None:
        raise PipelineStateRestorationError(_ERROR_STATE)
    return reconstructed


def _reconstructed_roots(value: object) -> PipelineStateRoots:
    if type(value) is not PipelineStateRoots:
        raise PipelineStateRestorationError(_ERROR_RESULT)
    failed = False
    reconstructed: PipelineStateRoots | None = None
    try:
        reconstructed = PipelineStateRoots(
            calendar_data=value.calendar_data,
            identity_registry=value.identity_registry,
            historical_prices=value.historical_prices,
            daily_reports=value.daily_reports,
            reference_data=value.reference_data,
            daily_pipeline=value.daily_pipeline,
        )
    except Exception:
        failed = True
    if failed or reconstructed is None:
        raise PipelineStateRestorationError(_ERROR_RESULT)
    return reconstructed


@dataclass(frozen=True, slots=True)
class CompletedPipelineStateRestore:
    snapshot_root: Path
    roots: PipelineStateRoots
    run_id: str
    inventory_id: str

    def __post_init__(self) -> None:
        if (
            type(self.snapshot_root) is not _CONCRETE_PATH_TYPE
            or not self.snapshot_root.is_absolute()
            or type(self.run_id) is not str
            or type(self.inventory_id) is not str
            or len(self.run_id) != 64
            or len(self.inventory_id) != 64
            or not _SHA256_CHARS.issuperset(self.run_id)
            or not _SHA256_CHARS.issuperset(self.inventory_id)
            or ".." in self.snapshot_root.parts
            or self.snapshot_root.name != self.run_id
        ):
            raise PipelineStateRestorationError(_ERROR_RESULT)
        roots = _reconstructed_roots(self.roots)
        if any(
            getattr(roots, root_name) != self.snapshot_root / root_name
            for root_name in ROOT_NAMES
        ):
            raise PipelineStateRestorationError(_ERROR_RESULT)
        object.__setattr__(self, "roots", roots)


def _roots_for(snapshot_root: Path) -> PipelineStateRoots:
    return PipelineStateRoots(
        **{root_name: snapshot_root / root_name for root_name in ROOT_NAMES}
    )


def _lstat(path: Path, error_message: str) -> os.stat_result:
    failed = False
    status: os.stat_result | None = None
    try:
        status = os.lstat(path)
    except OSError:
        failed = True
    if failed or status is None:
        raise PipelineStateRestorationError(error_message)
    return status


def _validate_destination(
    destination: object,
    state: VerifiedHydratedPipelineState,
) -> tuple[Path, Path, tuple[int, int]]:
    if type(destination) is not _CONCRETE_PATH_TYPE:
        raise PipelineStateRestorationError(_ERROR_DESTINATION)
    if (
        not destination.is_absolute()
        or ".." in destination.parts
        or destination.name != state.acquired_blobs.control.inventory.run_id
    ):
        raise PipelineStateRestorationError(_ERROR_DESTINATION)

    parent = destination.parent
    if parent == destination:
        raise PipelineStateRestorationError(_ERROR_DESTINATION)
    parent_status = _lstat(parent, _ERROR_DESTINATION)
    if not stat.S_ISDIR(parent_status.st_mode) or _is_link_like(parent_status):
        raise PipelineStateRestorationError(_ERROR_DESTINATION)
    return destination, parent, (parent_status.st_dev, parent_status.st_ino)


def _path_exists(path: Path, error_message: str) -> bool:
    try:
        os.lstat(path)
    except FileNotFoundError:
        return False
    except OSError as exc:
        raise PipelineStateRestorationError(error_message) from exc
    return True


def _directory_identity(status: os.stat_result) -> tuple[int, int, int, int]:
    return (status.st_dev, status.st_ino, status.st_size, status.st_mtime_ns)


def _write_verified_file(path: Path, content_bytes: bytes) -> None:
    flags = (
        os.O_WRONLY
        | os.O_CREAT
        | os.O_EXCL
        | getattr(os, "O_BINARY", 0)
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NOINHERIT", 0)
        | getattr(os, "O_NOFOLLOW", 0)
    )
    descriptor: int | None = None
    failed = False
    try:
        descriptor = os.open(path, flags, 0o600)
        with os.fdopen(descriptor, "wb", closefd=True) as handle:
            descriptor = None
            opened = os.fstat(handle.fileno())
            if (
                not stat.S_ISREG(opened.st_mode)
                or _is_link_like(opened)
                or opened.st_nlink != 1
            ):
                raise OSError()
            written = handle.write(content_bytes)
            if written != len(content_bytes):
                raise OSError()
            handle.flush()
            os.fsync(handle.fileno())
            after_write = os.fstat(handle.fileno())
            current = os.lstat(path)
            if (
                _directory_identity(opened)[:2] != _directory_identity(after_write)[:2]
                or _directory_identity(opened)[:2] != _directory_identity(current)[:2]
                or after_write.st_size != len(content_bytes)
                or not stat.S_ISREG(current.st_mode)
                or _is_link_like(current)
                or after_write.st_nlink != 1
                or current.st_nlink != 1
            ):
                raise OSError()
    except OSError:
        failed = True
    finally:
        if descriptor is not None:
            try:
                os.close(descriptor)
            except OSError:
                failed = True
    if failed:
        raise PipelineStateRestorationError(_ERROR_STAGE)


def _fsync_directory(path: Path) -> None:
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    descriptor: int | None = None
    try:
        descriptor = os.open(path, flags)
        os.fsync(descriptor)
    except OSError:
        pass
    finally:
        if descriptor is not None:
            try:
                os.close(descriptor)
            except OSError:
                pass


def _expected_directories(
    state: VerifiedHydratedPipelineState,
) -> set[tuple[str, ...]]:
    expected = {(root_name,) for root_name in ROOT_NAMES}
    for hydrated_entry in state.entries:
        entry = hydrated_entry.inventory_entry
        segments = tuple(entry.relative_path.split("/"))
        for length in range(1, len(segments)):
            expected.add((entry.root_name,) + segments[:length])
    return expected


def _create_staged_tree(
    staging: Path,
    state: VerifiedHydratedPipelineState,
) -> None:
    expected_directories = _expected_directories(state)
    for segments in sorted(expected_directories, key=lambda item: (len(item), item)):
        directory = staging.joinpath(*segments)
        try:
            directory.mkdir()
        except OSError as exc:
            raise PipelineStateRestorationError(_ERROR_STAGE) from exc
        status = _lstat(directory, _ERROR_STAGE)
        if not stat.S_ISDIR(status.st_mode) or _is_link_like(status):
            raise PipelineStateRestorationError(_ERROR_STAGE)

    for hydrated_entry in state.entries:
        entry = hydrated_entry.inventory_entry
        path = staging / entry.root_name
        path = path.joinpath(*entry.relative_path.split("/"))
        _write_verified_file(path, hydrated_entry.content_bytes)

    for segments in sorted(expected_directories, key=len, reverse=True):
        _fsync_directory(staging.joinpath(*segments))
    _fsync_directory(staging)


def _scan_tree(
    snapshot_root: Path,
) -> tuple[
    set[tuple[str, ...]],
    dict[tuple[str, ...], bytes],
    list[tuple[Path, tuple[int, int, int, int], tuple[str, ...]]],
]:
    root_status = _lstat(snapshot_root, _ERROR_VERIFY)
    if not stat.S_ISDIR(root_status.st_mode) or _is_link_like(root_status):
        raise PipelineStateRestorationError(_ERROR_VERIFY)

    observed_directories: set[tuple[str, ...]] = set()
    observed_files: dict[tuple[str, ...], bytes] = {}
    fingerprints: list[
        tuple[Path, tuple[int, int, int, int], tuple[str, ...]]
    ] = []
    stack: list[tuple[Path, tuple[str, ...]]] = [(snapshot_root, ())]

    while stack:
        directory, relative = stack.pop()
        before = _lstat(directory, _ERROR_VERIFY)
        if not stat.S_ISDIR(before.st_mode) or _is_link_like(before):
            raise PipelineStateRestorationError(_ERROR_VERIFY)
        failed = False
        names: tuple[str, ...] = ()
        try:
            with os.scandir(directory) as iterator:
                names = tuple(sorted(item.name for item in iterator))
        except OSError:
            failed = True
        if failed:
            raise PipelineStateRestorationError(_ERROR_VERIFY)
        after = _lstat(directory, _ERROR_VERIFY)
        if _directory_identity(before) != _directory_identity(after):
            raise PipelineStateRestorationError(_ERROR_VERIFY)
        fingerprints.append((directory, _directory_identity(after), names))

        for name in reversed(names):
            child = directory / name
            child_relative = relative + (name,)
            status = _lstat(child, _ERROR_VERIFY)
            if _is_link_like(status):
                raise PipelineStateRestorationError(_ERROR_VERIFY)
            if stat.S_ISDIR(status.st_mode):
                observed_directories.add(child_relative)
                stack.append((child, child_relative))
            elif stat.S_ISREG(status.st_mode):
                if status.st_nlink != 1:
                    raise PipelineStateRestorationError(_ERROR_VERIFY)
                try:
                    observed_files[child_relative] = read_stable_regular_file(
                        child,
                        maximum_bytes=MAXIMUM_FILE_BYTES,
                    )
                except FileSafetyError as exc:
                    raise PipelineStateRestorationError(_ERROR_VERIFY) from exc
            else:
                raise PipelineStateRestorationError(_ERROR_VERIFY)

    return observed_directories, observed_files, fingerprints


def _verify_exact_tree(
    snapshot_root: Path,
    state: VerifiedHydratedPipelineState,
) -> None:
    observed_directories, observed_files, fingerprints = _scan_tree(snapshot_root)
    if observed_directories != _expected_directories(state):
        raise PipelineStateRestorationError(_ERROR_VERIFY)

    expected_paths: dict[tuple[str, ...], HydratedPipelineStateEntry] = {}
    for item in state.entries:
        entry = item.inventory_entry
        key = (entry.root_name,) + tuple(entry.relative_path.split("/"))
        expected_paths[key] = item
    if set(observed_files) != set(expected_paths):
        raise PipelineStateRestorationError(_ERROR_VERIFY)
    for key, expected in expected_paths.items():
        payload = observed_files[key]
        if (
            len(payload) != expected.inventory_entry.byte_count
            or hashlib.sha256(payload).hexdigest() != expected.inventory_entry.sha256
            or payload != expected.content_bytes
        ):
            raise PipelineStateRestorationError(_ERROR_VERIFY)

    for directory, expected_identity, expected_names in fingerprints:
        before = _lstat(directory, _ERROR_VERIFY)
        if (
            not stat.S_ISDIR(before.st_mode)
            or _is_link_like(before)
            or _directory_identity(before) != expected_identity
        ):
            raise PipelineStateRestorationError(_ERROR_VERIFY)
        failed = False
        observed_names: tuple[str, ...] = ()
        try:
            with os.scandir(directory) as iterator:
                observed_names = tuple(sorted(item.name for item in iterator))
        except OSError:
            failed = True
        after = _lstat(directory, _ERROR_VERIFY)
        if (
            failed
            or observed_names != expected_names
            or _directory_identity(after) != expected_identity
        ):
            raise PipelineStateRestorationError(_ERROR_VERIFY)


def _verify_cleanup_tree_is_safe(staging: Path) -> None:
    stack = [staging]
    while stack:
        directory = stack.pop()
        status = _lstat(directory, _ERROR_CLEANUP)
        if not stat.S_ISDIR(status.st_mode) or _is_link_like(status):
            raise PipelineStateRestorationError(_ERROR_CLEANUP)
        failed = False
        children: tuple[Path, ...] = ()
        try:
            with os.scandir(directory) as iterator:
                children = tuple(directory / item.name for item in iterator)
        except OSError:
            failed = True
        if failed:
            raise PipelineStateRestorationError(_ERROR_CLEANUP)
        for child in children:
            child_status = _lstat(child, _ERROR_CLEANUP)
            if _is_link_like(child_status):
                raise PipelineStateRestorationError(_ERROR_CLEANUP)
            if stat.S_ISDIR(child_status.st_mode):
                stack.append(child)
            elif not stat.S_ISREG(child_status.st_mode):
                raise PipelineStateRestorationError(_ERROR_CLEANUP)


def _safe_cleanup(staging: Path, parent: Path, run_id: str) -> None:
    if not _path_exists(staging, _ERROR_CLEANUP):
        return
    expected_prefix = f".{run_id}{_STAGING_INFIX}"
    if (
        staging.parent != parent
        or not staging.name.startswith(expected_prefix)
        or staging == parent
    ):
        raise PipelineStateRestorationError(_ERROR_CLEANUP)
    status = _lstat(staging, _ERROR_CLEANUP)
    if not stat.S_ISDIR(status.st_mode) or _is_link_like(status):
        raise PipelineStateRestorationError(_ERROR_CLEANUP)
    _verify_cleanup_tree_is_safe(staging)
    try:
        shutil.rmtree(staging)
    except OSError as exc:
        raise PipelineStateRestorationError(_ERROR_CLEANUP) from exc


def _completed_result(
    destination: Path,
    state: VerifiedHydratedPipelineState,
) -> CompletedPipelineStateRestore:
    failed = False
    result: CompletedPipelineStateRestore | None = None
    try:
        inventory = state.acquired_blobs.control.inventory
        result = CompletedPipelineStateRestore(
            snapshot_root=destination,
            roots=_roots_for(destination),
            run_id=inventory.run_id,
            inventory_id=inventory.inventory_id,
        )
    except Exception:
        failed = True
    if failed or result is None:
        raise PipelineStateRestorationError(_ERROR_RESULT)
    return result


def restore_verified_pipeline_state(
    state: VerifiedHydratedPipelineState,
    *,
    destination: Path,
) -> CompletedPipelineStateRestore:
    """Publishes one complete immutable local snapshot without overwriting.

    All six roots are created below one randomized same-parent staging
    directory. Files are created exclusively, fsynced, and the exact tree is
    verified before one directory rename makes the run-id-named snapshot
    visible. Existing exact snapshots are accepted idempotently; inconsistent
    destinations are rejected and never replaced. Ordinary failures are
    sanitized, staging is removed when safe, and no partial result is returned.
    """

    state = _reconstructed_state(state)
    destination, parent, parent_identity = _validate_destination(destination, state)
    run_id = state.acquired_blobs.control.inventory.run_id

    try:
        lock = advisory_file_lock(parent / _LOCK_FILE_NAME)
        with lock:
            current_parent = _lstat(parent, _ERROR_DESTINATION)
            if (
                not stat.S_ISDIR(current_parent.st_mode)
                or _is_link_like(current_parent)
                or (current_parent.st_dev, current_parent.st_ino) != parent_identity
            ):
                raise PipelineStateRestorationError(_ERROR_DESTINATION)
            if _path_exists(destination, _ERROR_DESTINATION):
                _verify_exact_tree(destination, state)
                return _completed_result(destination, state)

            staging: Path | None = None
            try:
                staging = Path(
                    tempfile.mkdtemp(
                        dir=parent,
                        prefix=f".{run_id}{_STAGING_INFIX}",
                    )
                )
                staging_status = _lstat(staging, _ERROR_STAGE)
                if (
                    staging.parent != parent
                    or not stat.S_ISDIR(staging_status.st_mode)
                    or _is_link_like(staging_status)
                ):
                    raise PipelineStateRestorationError(_ERROR_STAGE)

                _create_staged_tree(staging, state)
                _verify_exact_tree(staging, state)
                if _path_exists(destination, _ERROR_PUBLISH):
                    raise PipelineStateRestorationError(_ERROR_PUBLISH)
                try:
                    os.rename(staging, destination)
                except OSError as exc:
                    raise PipelineStateRestorationError(_ERROR_PUBLISH) from exc
                staging = None
                _fsync_directory(parent)
                _verify_exact_tree(destination, state)
                return _completed_result(destination, state)
            finally:
                if staging is not None:
                    _safe_cleanup(staging, parent, run_id)
    except PipelineStateRestorationError:
        raise
    except Exception as exc:
        raise PipelineStateRestorationError(_ERROR_PUBLISH) from exc
