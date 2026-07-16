from __future__ import annotations

import os
import stat
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator


class FileSafetyError(Exception):
    """A local path could not be used without weakening integrity guarantees."""


class FileLockUnavailable(FileSafetyError):
    """Another process currently owns a non-blocking advisory lock."""


def _is_reparse_point(status: os.stat_result) -> bool:
    attribute = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400)
    return bool(getattr(status, "st_file_attributes", 0) & attribute)


def _same_file_identity(
    first: os.stat_result,
    second: os.stat_result,
) -> bool:
    return (first.st_dev, first.st_ino) == (second.st_dev, second.st_ino)


def _same_file_version(
    first: os.stat_result,
    second: os.stat_result,
) -> bool:
    return (
        _same_file_identity(first, second)
        and first.st_size == second.st_size
        and first.st_mtime_ns == second.st_mtime_ns
    )


def read_stable_regular_file(path: Path, *, maximum_bytes: int) -> bytes:
    """Read one file object and reject path swaps or concurrent mutation.

    The descriptor is opened once.  Path and descriptor identities are checked
    before and after the bounded read, so a same-size/same-mtime replacement at
    the pathname cannot be mistaken for the file that was inspected.
    """

    if type(maximum_bytes) is not int or maximum_bytes <= 0:
        raise ValueError("maximum_bytes must be a positive integer")
    source = Path(path)
    try:
        before_path = os.lstat(source)
    except OSError as exc:
        raise FileSafetyError("source is unavailable") from exc
    if not stat.S_ISREG(before_path.st_mode) or _is_reparse_point(before_path):
        raise FileSafetyError("source must be a regular non-link file")
    if before_path.st_size <= 0:
        raise FileSafetyError("source is empty")
    if before_path.st_size > maximum_bytes:
        raise FileSafetyError("source exceeds the size limit")

    flags = (
        os.O_RDONLY
        | getattr(os, "O_BINARY", 0)
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NOINHERIT", 0)
        | getattr(os, "O_NOFOLLOW", 0)
    )
    try:
        descriptor = os.open(source, flags)
    except OSError as exc:
        raise FileSafetyError("source could not be opened safely") from exc

    try:
        with os.fdopen(descriptor, "rb", closefd=True) as handle:
            opened = os.fstat(handle.fileno())
            if (
                not stat.S_ISREG(opened.st_mode)
                or _is_reparse_point(opened)
                or not _same_file_version(before_path, opened)
            ):
                raise FileSafetyError("source changed before it was opened")
            payload = handle.read(maximum_bytes + 1)
            after_read = os.fstat(handle.fileno())
            try:
                after_path = os.lstat(source)
            except OSError as exc:
                raise FileSafetyError("source changed while it was being read") from exc
    except FileSafetyError:
        raise
    except OSError as exc:
        raise FileSafetyError("source could not be read safely") from exc

    if (
        len(payload) != opened.st_size
        or len(payload) > maximum_bytes
        or not _same_file_version(opened, after_read)
        or not _same_file_version(opened, after_path)
    ):
        raise FileSafetyError("source changed while it was being read")
    return payload


@contextmanager
def advisory_file_lock(path: Path) -> Iterator[None]:
    """Acquire a process-released, non-blocking lock on a persistent file."""

    lock_path = Path(path)
    flags = (
        os.O_RDWR
        | os.O_CREAT
        | getattr(os, "O_BINARY", 0)
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NOINHERIT", 0)
        | getattr(os, "O_NOFOLLOW", 0)
    )
    try:
        descriptor = os.open(lock_path, flags, 0o600)
    except OSError as exc:
        raise FileSafetyError("lock file could not be opened safely") from exc

    acquired = False
    try:
        opened = os.fstat(descriptor)
        current = os.lstat(lock_path)
        if (
            not stat.S_ISREG(opened.st_mode)
            or _is_reparse_point(opened)
            or not _same_file_identity(opened, current)
        ):
            raise FileSafetyError("lock path is not a stable regular file")
        if opened.st_size == 0:
            os.write(descriptor, b"\0")
            os.fsync(descriptor)
        os.lseek(descriptor, 0, os.SEEK_SET)

        try:
            if os.name == "nt":
                import msvcrt

                msvcrt.locking(descriptor, msvcrt.LK_NBLCK, 1)
            else:
                import fcntl

                fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except (BlockingIOError, OSError) as exc:
            raise FileLockUnavailable("lock is already owned") from exc
        acquired = True
        yield
    finally:
        if acquired:
            try:
                os.lseek(descriptor, 0, os.SEEK_SET)
                if os.name == "nt":
                    import msvcrt

                    msvcrt.locking(descriptor, msvcrt.LK_UNLCK, 1)
                else:
                    import fcntl

                    fcntl.flock(descriptor, fcntl.LOCK_UN)
            finally:
                os.close(descriptor)
        else:
            os.close(descriptor)
