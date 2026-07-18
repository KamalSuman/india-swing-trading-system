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

from .codec import decode_promotion_decision, encode_promotion_decision
from .models import PromotionDecision, PromotionIntegrityError


_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_MAXIMUM_DECISION_BYTES = 4 * 1024 * 1024


class PromotionStoreConflict(PromotionIntegrityError):
    pass


class PromotionDecisionNotFound(PromotionIntegrityError):
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


class LocalPromotionDecisionStore:
    """Create-once content-addressed store for promotion decisions."""

    def __init__(self, root: Path) -> None:
        self.root = Path(root)

    @property
    def decisions_root(self) -> Path:
        return self.root / "decisions"

    def path_for(self, decision_id: str) -> Path:
        if not isinstance(decision_id, str) or _SHA256.fullmatch(decision_id) is None:
            raise PromotionIntegrityError(
                "decision_id must be a full lowercase SHA-256"
            )
        return self.decisions_root / f"{decision_id}.json"

    def put(self, value: PromotionDecision) -> PromotionDecision:
        if type(value) is not PromotionDecision:
            raise TypeError("promotion decision must be exact")
        value.verify_content_identity()
        self.decisions_root.mkdir(parents=True, exist_ok=True)
        if _is_link_like(self.decisions_root):
            raise PromotionStoreConflict("promotion decision root cannot be a link")
        target = self.path_for(value.decision_id)
        payload = encode_promotion_decision(value)
        try:
            with advisory_file_lock(self.decisions_root / ".promotion.lock"):
                if target.exists():
                    stored = self.get(value.decision_id)
                    if stored != value:
                        raise PromotionStoreConflict(
                            "decision ID already stores different content"
                        )
                    return stored
                descriptor, temporary_name = tempfile.mkstemp(
                    prefix=".promotion-",
                    suffix=".tmp",
                    dir=self.decisions_root,
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
            raise PromotionStoreConflict("promotion decision store unavailable") from exc
        return self.get(value.decision_id)

    def get(self, decision_id: str) -> PromotionDecision:
        path = self.path_for(decision_id)
        if not path.exists():
            raise PromotionDecisionNotFound(decision_id)
        if not path.is_file() or _is_link_like(path):
            raise PromotionStoreConflict("promotion decision must be a regular file")
        try:
            value = decode_promotion_decision(
                read_stable_regular_file(
                    path,
                    maximum_bytes=_MAXIMUM_DECISION_BYTES,
                )
            )
        except (FileSafetyError, PromotionIntegrityError) as exc:
            raise PromotionStoreConflict(
                "promotion decision could not be read safely"
            ) from exc
        if value.decision_id != decision_id:
            raise PromotionStoreConflict("promotion decision differs from its path")
        return value

    def list_decisions(self) -> tuple[PromotionDecision, ...]:
        if not self.decisions_root.exists():
            return ()
        if not self.decisions_root.is_dir() or _is_link_like(self.decisions_root):
            raise PromotionStoreConflict(
                "promotion decision root must be a real directory"
            )
        values = []
        for path in sorted(self.decisions_root.iterdir(), key=lambda item: item.name):
            if path.name == ".promotion.lock":
                continue
            if (
                path.suffix != ".json"
                or _SHA256.fullmatch(path.stem) is None
                or not path.is_file()
                or _is_link_like(path)
            ):
                raise PromotionStoreConflict("promotion decision file set is invalid")
            values.append(self.get(path.stem))
        return tuple(
            sorted(values, key=lambda value: (value.market_session, value.decision_id))
        )
