from __future__ import annotations

import os
import re
import stat
import tempfile
from pathlib import Path

from india_swing._filesystem import (
    FileLockUnavailable,
    FileSafetyError,
    advisory_file_lock,
    read_stable_regular_file,
)

from .codec import decode_shadow_scan_result, encode_shadow_scan_result
from .models import CollectionShadowScanResult, ShadowScanError


_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_MAXIMUM_RESULT_BYTES = 16 * 1024 * 1024


class ShadowScanStoreError(ShadowScanError):
    pass


class ShadowScanNotFound(ShadowScanStoreError):
    pass


def _is_link_like(path: Path) -> bool:
    try:
        status = os.lstat(path)
    except OSError:
        return path.is_symlink()
    return path.is_symlink() or bool(
        getattr(status, "st_file_attributes", 0)
        & getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400)
    )


class LocalCollectionShadowScanStore:
    """Create-once content-addressed storage for observation-only scans."""

    def __init__(self, root: Path) -> None:
        self.root = Path(root)

    @property
    def results_root(self) -> Path:
        return self.root / "results"

    def path_for(self, result_id: str) -> Path:
        if type(result_id) is not str or _SHA256.fullmatch(result_id) is None:
            raise ShadowScanStoreError("result_id must be a lowercase SHA-256")
        return self.results_root / f"{result_id}.json"

    def put(self, value: CollectionShadowScanResult) -> CollectionShadowScanResult:
        if type(value) is not CollectionShadowScanResult:
            raise ShadowScanStoreError("shadow scan result must be exact")
        try:
            value.verify_content_identity()
            payload = encode_shadow_scan_result(value)
        except Exception:
            raise ShadowScanStoreError("shadow scan result is invalid") from None
        target = self.path_for(value.result_id)
        try:
            self.results_root.mkdir(parents=True, exist_ok=True)
            if _is_link_like(self.results_root):
                raise ShadowScanStoreError("shadow scan root cannot be a link")
            with advisory_file_lock(self.results_root / ".shadow-scans.lock"):
                if target.exists():
                    stored = self.get(value.result_id)
                    if stored != value:
                        raise ShadowScanStoreError(
                            "result ID already stores different content"
                        )
                    return stored
                descriptor, temporary_name = tempfile.mkstemp(
                    prefix=".shadow-scan-",
                    suffix=".tmp",
                    dir=self.results_root,
                )
                temporary = Path(temporary_name)
                try:
                    with os.fdopen(descriptor, "wb") as handle:
                        handle.write(payload)
                        handle.flush()
                        os.fsync(handle.fileno())
                    os.link(temporary, target)
                finally:
                    temporary.unlink(missing_ok=True)
        except ShadowScanStoreError:
            raise
        except (FileLockUnavailable, FileSafetyError, OSError):
            raise ShadowScanStoreError("shadow scan store is unavailable") from None
        return self.get(value.result_id)

    def get(self, result_id: str) -> CollectionShadowScanResult:
        path = self.path_for(result_id)
        if not path.exists():
            raise ShadowScanNotFound("shadow scan result was not found")
        if not path.is_file() or _is_link_like(path):
            raise ShadowScanStoreError("shadow scan result must be a regular file")
        try:
            value = decode_shadow_scan_result(
                read_stable_regular_file(
                    path,
                    maximum_bytes=_MAXIMUM_RESULT_BYTES,
                )
            )
        except Exception:
            raise ShadowScanStoreError("shadow scan result could not be read") from None
        if value.result_id != result_id:
            raise ShadowScanStoreError("stored result differs from its path")
        return value
