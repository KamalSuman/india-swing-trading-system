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
from india_swing.historical_prices import LocalHistoricalPriceArtifactStore

from .codec import decode_liquidity_snapshot, encode_liquidity_snapshot
from .materialize import materialize_collection_liquidity
from .models import (
    CollectionLiquiditySnapshot,
    LiquidityConflict,
    LiquidityIntegrityError,
    LiquidityNotFound,
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


class LocalLiquiditySnapshotStore:
    def __init__(
        self,
        root: Path,
        historical_prices_root: Path,
        daily_reports_root: Path,
    ) -> None:
        self.root = Path(root)
        self.historical_prices_root = Path(historical_prices_root)
        self.daily_reports_root = Path(daily_reports_root)

    @property
    def snapshots_root(self) -> Path:
        return self.root / "snapshots"

    def path_for(self, snapshot_id: str) -> Path:
        if not isinstance(snapshot_id, str) or _SHA256.fullmatch(snapshot_id) is None:
            raise LiquidityIntegrityError(
                "snapshot_id must be a full lowercase SHA-256"
            )
        return self.snapshots_root / f"{snapshot_id}.json"

    def put(self, value: CollectionLiquiditySnapshot) -> CollectionLiquiditySnapshot:
        if type(value) is not CollectionLiquiditySnapshot:
            raise TypeError("liquidity snapshot must be exact")
        value.verify_content_identity()
        if self._replay(value) != value:
            raise LiquidityIntegrityError(
                "liquidity snapshot does not replay from sealed prices"
            )
        payload = encode_liquidity_snapshot(value)
        self.snapshots_root.mkdir(parents=True, exist_ok=True)
        if _is_link_like(self.snapshots_root):
            raise LiquidityIntegrityError("liquidity snapshot root cannot be a link")
        target = self.path_for(value.snapshot_id)
        try:
            with advisory_file_lock(self.snapshots_root / ".liquidity.lock"):
                if target.exists():
                    stored = self.get(value.snapshot_id)
                    if stored != value:
                        raise LiquidityConflict(
                            "snapshot ID already stores different content"
                        )
                    return stored
                descriptor, temporary_name = tempfile.mkstemp(
                    prefix=".liquidity-",
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
            raise LiquidityConflict("liquidity snapshot store unavailable") from exc
        return self.get(value.snapshot_id)

    def get(self, snapshot_id: str) -> CollectionLiquiditySnapshot:
        path = self.path_for(snapshot_id)
        if not path.exists():
            raise LiquidityNotFound(snapshot_id)
        if not path.is_file() or _is_link_like(path):
            raise LiquidityIntegrityError("liquidity snapshot must be a regular file")
        try:
            value = decode_liquidity_snapshot(
                read_stable_regular_file(
                    path,
                    maximum_bytes=_MAXIMUM_SNAPSHOT_BYTES,
                )
            )
        except FileSafetyError as exc:
            raise LiquidityIntegrityError("liquidity snapshot read was unsafe") from exc
        if value.snapshot_id != snapshot_id or self._replay(value) != value:
            raise LiquidityIntegrityError(
                "stored liquidity snapshot differs from its path or source replay"
            )
        return value

    def list_snapshots(self) -> tuple[CollectionLiquiditySnapshot, ...]:
        if not self.snapshots_root.exists():
            return ()
        if not self.snapshots_root.is_dir() or _is_link_like(self.snapshots_root):
            raise LiquidityIntegrityError("liquidity snapshot root is unsafe")
        values = []
        for path in sorted(self.snapshots_root.iterdir(), key=lambda item: item.name):
            if path.name == ".liquidity.lock":
                continue
            if (
                path.suffix != ".json"
                or _SHA256.fullmatch(path.stem) is None
                or not path.is_file()
                or _is_link_like(path)
            ):
                raise LiquidityIntegrityError("liquidity snapshot file set is invalid")
            values.append(self.get(path.stem))
        return tuple(
            sorted(values, key=lambda value: (value.coverage_end, value.snapshot_id))
        )

    def _replay(
        self,
        value: CollectionLiquiditySnapshot,
    ) -> CollectionLiquiditySnapshot:
        store = LocalHistoricalPriceArtifactStore(
            self.historical_prices_root,
            self.daily_reports_root,
        )
        sources = tuple(
            store.get(binding.artifact_id).artifact
            for binding in value.source_sessions
        )
        return materialize_collection_liquidity(
            sources,
            decision_cutoff=value.decision_cutoff,
            minimum_history_sessions=value.minimum_history_sessions,
        )
