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
from india_swing.reference_data import LocalReferenceArtifactStore

from .codec import (
    decode_collection_universe_snapshot,
    encode_collection_universe_snapshot,
)
from .materialize import materialize_collection_universe
from .models import (
    CollectionUniverseConflict,
    CollectionUniverseIntegrityError,
    CollectionUniverseNotFound,
    CollectionUniverseSnapshot,
)


_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_MAXIMUM_SNAPSHOT_BYTES = 256 * 1024 * 1024


def _is_link_like(path: Path) -> bool:
    try:
        status = os.lstat(path)
    except OSError:
        return path.is_symlink()
    return path.is_symlink() or bool(
        getattr(status, "st_file_attributes", 0)
        & getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400)
    )


class LocalCollectionUniverseSnapshotStore:
    """Create-once store whose reads replay the exact security master."""

    def __init__(self, root: Path, reference_root: Path) -> None:
        self.root = Path(root)
        self.reference_root = Path(reference_root)

    @property
    def snapshots_root(self) -> Path:
        return self.root / "snapshots"

    def path_for(self, snapshot_id: str) -> Path:
        if not isinstance(snapshot_id, str) or _SHA256.fullmatch(snapshot_id) is None:
            raise CollectionUniverseIntegrityError(
                "snapshot_id must be a full lowercase SHA-256"
            )
        return self.snapshots_root / f"{snapshot_id}.json"

    def put(self, value: CollectionUniverseSnapshot) -> CollectionUniverseSnapshot:
        if type(value) is not CollectionUniverseSnapshot:
            raise TypeError("universe snapshot must be exact")
        value.verify_content_identity()
        if self._replay(value) != value:
            raise CollectionUniverseIntegrityError(
                "universe snapshot does not replay from sealed source"
            )
        payload = encode_collection_universe_snapshot(value)
        self.snapshots_root.mkdir(parents=True, exist_ok=True)
        if _is_link_like(self.snapshots_root):
            raise CollectionUniverseIntegrityError(
                "universe snapshot root cannot be a link"
            )
        target = self.path_for(value.snapshot_id)
        try:
            with advisory_file_lock(self.snapshots_root / ".universe.lock"):
                if target.exists():
                    stored = self.get(value.snapshot_id)
                    if stored != value:
                        raise CollectionUniverseConflict(
                            "snapshot ID already stores different content"
                        )
                    return stored
                descriptor, temporary_name = tempfile.mkstemp(
                    prefix=".universe-",
                    suffix=".tmp",
                    dir=self.snapshots_root,
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
        except (FileLockUnavailable, FileSafetyError) as exc:
            raise CollectionUniverseConflict(
                "universe snapshot store unavailable"
            ) from exc
        return self.get(value.snapshot_id)

    def get(self, snapshot_id: str) -> CollectionUniverseSnapshot:
        path = self.path_for(snapshot_id)
        if not path.exists():
            raise CollectionUniverseNotFound(snapshot_id)
        if not path.is_file() or _is_link_like(path):
            raise CollectionUniverseIntegrityError(
                "universe snapshot must be a regular file"
            )
        try:
            value = decode_collection_universe_snapshot(
                read_stable_regular_file(
                    path,
                    maximum_bytes=_MAXIMUM_SNAPSHOT_BYTES,
                )
            )
        except FileSafetyError as exc:
            raise CollectionUniverseIntegrityError(
                "universe snapshot read was unsafe"
            ) from exc
        if value.snapshot_id != snapshot_id or self._replay(value) != value:
            raise CollectionUniverseIntegrityError(
                "stored universe snapshot differs from its path or source replay"
            )
        return value

    def list_snapshots(self) -> tuple[CollectionUniverseSnapshot, ...]:
        if not self.snapshots_root.exists():
            return ()
        if not self.snapshots_root.is_dir() or _is_link_like(self.snapshots_root):
            raise CollectionUniverseIntegrityError(
                "universe snapshot root is unsafe"
            )
        values = []
        for path in sorted(self.snapshots_root.iterdir(), key=lambda item: item.name):
            if path.name == ".universe.lock":
                continue
            if (
                path.suffix != ".json"
                or _SHA256.fullmatch(path.stem) is None
                or not path.is_file()
                or _is_link_like(path)
            ):
                raise CollectionUniverseIntegrityError(
                    "universe snapshot file set is invalid"
                )
            values.append(self.get(path.stem))
        return tuple(
            sorted(
                values,
                key=lambda value: (value.market_session_claim, value.snapshot_id),
            )
        )

    def _replay(
        self,
        value: CollectionUniverseSnapshot,
    ) -> CollectionUniverseSnapshot:
        source = LocalReferenceArtifactStore(self.reference_root).get(
            value.source_artifact_id
        )
        manifest = source.manifest
        if (
            manifest.manifest_id != value.source_manifest_id
            or manifest.raw_sha256 != value.source_raw_sha256
            or manifest.normalized_sha256 != value.source_normalized_sha256
        ):
            raise CollectionUniverseIntegrityError(
                "universe source lineage differs from sealed reference data"
            )
        return materialize_collection_universe(
            source,
            cutoff=value.cutoff,
            calendar_snapshot_id=value.calendar_snapshot_id,
        )
