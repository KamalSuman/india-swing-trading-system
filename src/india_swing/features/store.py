"""Create-once local stores for replayable promoted feature artifacts."""

from __future__ import annotations

import os
import re
import stat
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Protocol

from india_swing._filesystem import (
    FileLockUnavailable,
    FileSafetyError,
    advisory_file_lock,
    read_stable_regular_file,
)
from india_swing.corporate_actions.promoted_adjustments import (
    VerifiedPromotedCorporateActionAdjustmentPanel,
)
from india_swing.evaluation.promoted_feature_inputs import (
    PromotedFeatureInputService,
    VerifiedPromotedFeatureInputPanel,
)
from india_swing.features.codec import (
    decode_cross_section_record,
    decode_technical_feature_record,
    encode_cross_section_panel,
    encode_technical_feature_panel,
)
from india_swing.features.input_codec import (
    decode_promoted_feature_input_record,
    encode_promoted_feature_input_panel,
)
from india_swing.features.promoted_cross_section import (
    PromotedCrossSectionService,
    VerifiedPromotedCrossSectionPanel,
)
from india_swing.features.promoted_technical import (
    PromotedTechnicalFeatureService,
    VerifiedPromotedTechnicalFeaturePanel,
)
from india_swing.tick_sizes.effective_session import (
    VerifiedPromotedEffectiveSessionTickPanel,
)


class PromotedFeatureStoreError(ValueError):
    pass


class PromotedFeatureStoreConflict(PromotedFeatureStoreError):
    pass


class PromotedFeatureStoreNotFound(PromotedFeatureStoreError):
    pass


class PromotedFeatureInputPanelResolver(Protocol):
    def get(self, panel_id: str) -> VerifiedPromotedFeatureInputPanel: ...


class PromotedAdjustmentPanelResolver(Protocol):
    def get(
        self,
        bridge_id: str,
    ) -> VerifiedPromotedCorporateActionAdjustmentPanel: ...


class PromotedEffectiveTickPanelResolver(Protocol):
    def get(
        self,
        panel_id: str,
    ) -> VerifiedPromotedEffectiveSessionTickPanel: ...


class PromotedTechnicalFeaturePanelResolver(Protocol):
    def get(self, panel_id: str) -> VerifiedPromotedTechnicalFeaturePanel: ...


_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_MAXIMUM_ARTIFACT_BYTES = 64 * 1024 * 1024


def _is_link_like(path: Path) -> bool:
    try:
        status = os.lstat(path)
    except OSError:
        return path.is_symlink()
    return path.is_symlink() or bool(
        getattr(status, "st_file_attributes", 0)
        & getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400)
    )


def _path(root: Path, panel_id: str) -> Path:
    if type(panel_id) is not str or _SHA256.fullmatch(panel_id) is None:
        raise PromotedFeatureStoreError(
            "promoted feature panel ID must be a full lowercase SHA-256"
        )
    return root / f"{panel_id}.json"


def _read(root: Path, panel_id: str) -> bytes:
    path = _path(root, panel_id)
    if not path.exists():
        raise PromotedFeatureStoreNotFound(
            "promoted feature artifact was not found"
        )
    if not path.is_file() or _is_link_like(path):
        raise PromotedFeatureStoreError(
            "promoted feature artifact must be a regular file"
        )
    try:
        return read_stable_regular_file(
            path,
            maximum_bytes=_MAXIMUM_ARTIFACT_BYTES,
        )
    except FileSafetyError:
        raise PromotedFeatureStoreError(
            "promoted feature artifact read was unsafe"
        ) from None


def _put(
    *,
    root: Path,
    lock_name: str,
    panel_id: str,
    payload: bytes,
    read_existing,
) -> bytes:
    root.mkdir(parents=True, exist_ok=True)
    if not root.is_dir() or _is_link_like(root):
        raise PromotedFeatureStoreError(
            "promoted feature artifact root is unsafe"
        )
    target = _path(root, panel_id)
    try:
        with advisory_file_lock(root / lock_name):
            if target.exists():
                existing = read_existing(panel_id)
                if existing != payload:
                    raise PromotedFeatureStoreConflict(
                        "promoted feature panel ID stores different content"
                    )
                return existing
            descriptor, temporary_name = tempfile.mkstemp(
                prefix=".promoted-feature-",
                suffix=".tmp",
                dir=root,
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
    except PromotedFeatureStoreConflict:
        raise
    except (FileLockUnavailable, FileSafetyError, OSError):
        raise PromotedFeatureStoreConflict(
            "promoted feature artifact store is unavailable"
        ) from None
    return _read(root, panel_id)


class ExactPromotedFeatureInputPanelResolver:
    """Exact-ID resolver for caller-supplied, replay-verified source panels."""

    def __init__(
        self,
        values: tuple[VerifiedPromotedFeatureInputPanel, ...],
    ) -> None:
        if (
            type(values) is not tuple
            or not values
            or any(
                type(value) is not VerifiedPromotedFeatureInputPanel
                for value in values
            )
        ):
            raise PromotedFeatureStoreError(
                "promoted feature-input resolver values are invalid"
            )
        try:
            for value in values:
                value.verify_content_identity()
        except Exception:
            raise PromotedFeatureStoreError(
                "promoted feature-input resolver could not verify a panel"
            ) from None
        by_id = {value.panel_id: value for value in values}
        if len(by_id) != len(values):
            raise PromotedFeatureStoreError(
                "promoted feature-input resolver IDs are duplicated"
            )
        self._by_id = by_id

    def get(self, panel_id: str) -> VerifiedPromotedFeatureInputPanel:
        if type(panel_id) is not str or _SHA256.fullmatch(panel_id) is None:
            raise PromotedFeatureStoreError(
                "promoted feature-input resolver ID is invalid"
            )
        try:
            value = self._by_id[panel_id]
        except KeyError:
            raise PromotedFeatureStoreNotFound(
                "promoted feature-input panel was not found"
            ) from None
        value.verify_content_identity()
        return value


class LocalPromotedFeatureInputStore:
    """Replays exact feature inputs from adjustment and tick panels."""

    def __init__(
        self,
        root: Path,
        adjustment_resolver: PromotedAdjustmentPanelResolver,
        tick_resolver: PromotedEffectiveTickPanelResolver,
    ) -> None:
        self.root = Path(root)
        self.adjustment_resolver = adjustment_resolver
        self.tick_resolver = tick_resolver

    @property
    def panels_root(self) -> Path:
        return self.root / "inputs"

    def path_for(self, panel_id: str) -> Path:
        return _path(self.panels_root, panel_id)

    def put(
        self,
        value: VerifiedPromotedFeatureInputPanel,
    ) -> VerifiedPromotedFeatureInputPanel:
        if type(value) is not VerifiedPromotedFeatureInputPanel:
            raise TypeError("promoted feature-input panel must be exact")
        value.verify_content_identity()
        replayed = self._replay(
            adjustment_bridge_id=value.adjustment_panel.bridge_id,
            tick_panel_id=value.tick_panel.panel_id,
            cutoff=value.cutoff,
        )
        if replayed != value:
            raise PromotedFeatureStoreError(
                "promoted feature-input source replay differs"
            )
        payload = encode_promoted_feature_input_panel(value)
        stored = _put(
            root=self.panels_root,
            lock_name=".inputs.lock",
            panel_id=value.panel_id,
            payload=payload,
            read_existing=lambda panel_id: _read(
                self.panels_root,
                panel_id,
            ),
        )
        if stored != payload:
            raise PromotedFeatureStoreConflict(
                "promoted feature-input artifact differs"
            )
        return self.get(value.panel_id)

    def get(
        self,
        panel_id: str,
    ) -> VerifiedPromotedFeatureInputPanel:
        payload = _read(self.panels_root, panel_id)
        try:
            record = decode_promoted_feature_input_record(payload)
            if record.panel_id != panel_id:
                raise PromotedFeatureStoreError(
                    "promoted feature-input path identity differs"
                )
            replayed = self._replay(
                adjustment_bridge_id=record.adjustment_bridge_id,
                tick_panel_id=record.tick_panel_id,
                cutoff=record.cutoff,
            )
            if (
                replayed.panel_id != panel_id
                or encode_promoted_feature_input_panel(replayed)
                != payload
            ):
                raise PromotedFeatureStoreError(
                    "promoted feature-input replay differs"
                )
            return replayed
        except PromotedFeatureStoreError:
            raise
        except Exception:
            raise PromotedFeatureStoreError(
                "stored promoted feature-input artifact is invalid"
            ) from None

    def _replay(
        self,
        *,
        adjustment_bridge_id: str,
        tick_panel_id: str,
        cutoff: datetime,
    ) -> VerifiedPromotedFeatureInputPanel:
        try:
            adjustment = self.adjustment_resolver.get(
                adjustment_bridge_id
            )
            tick = self.tick_resolver.get(tick_panel_id)
            if (
                type(adjustment)
                is not VerifiedPromotedCorporateActionAdjustmentPanel
                or adjustment.bridge_id != adjustment_bridge_id
                or type(tick)
                is not VerifiedPromotedEffectiveSessionTickPanel
                or tick.panel_id != tick_panel_id
            ):
                raise PromotedFeatureStoreError(
                    "promoted feature-input source differs"
                )
            return PromotedFeatureInputService().materialize(
                adjustment_panel=adjustment,
                tick_panel=tick,
                cutoff=cutoff,
            )
        except PromotedFeatureStoreError:
            raise
        except Exception:
            raise PromotedFeatureStoreError(
                "promoted feature-input source replay failed"
            ) from None


class LocalPromotedTechnicalFeatureStore:
    """Stores canonical manifests and replays from exact feature-input panels."""

    def __init__(
        self,
        root: Path,
        source_resolver: PromotedFeatureInputPanelResolver,
    ) -> None:
        self.root = Path(root)
        self.source_resolver = source_resolver

    @property
    def panels_root(self) -> Path:
        return self.root / "technical"

    def path_for(self, panel_id: str) -> Path:
        return _path(self.panels_root, panel_id)

    def put(
        self,
        value: VerifiedPromotedTechnicalFeaturePanel,
    ) -> VerifiedPromotedTechnicalFeaturePanel:
        if type(value) is not VerifiedPromotedTechnicalFeaturePanel:
            raise TypeError("promoted technical feature panel must be exact")
        value.verify_content_identity()
        payload = encode_technical_feature_panel(value)
        stored = _put(
            root=self.panels_root,
            lock_name=".technical.lock",
            panel_id=value.panel_id,
            payload=payload,
            read_existing=lambda panel_id: _read(self.panels_root, panel_id),
        )
        if stored != payload:
            raise PromotedFeatureStoreConflict(
                "promoted technical feature artifact differs"
            )
        return self.get(value.panel_id)

    def get(
        self,
        panel_id: str,
    ) -> VerifiedPromotedTechnicalFeaturePanel:
        payload = _read(self.panels_root, panel_id)
        try:
            record = decode_technical_feature_record(payload)
            if record.panel_id != panel_id:
                raise PromotedFeatureStoreError(
                    "promoted technical feature path identity differs"
                )
            source = self.source_resolver.get(record.source_panel_id)
            if (
                type(source) is not VerifiedPromotedFeatureInputPanel
                or source.panel_id != record.source_panel_id
            ):
                raise PromotedFeatureStoreError(
                    "promoted technical feature source differs"
                )
            replayed = PromotedTechnicalFeatureService().materialize(
                source_panel=source,
                config=record.config,
                cutoff=record.cutoff,
            )
            if (
                replayed.panel_id != panel_id
                or encode_technical_feature_panel(replayed) != payload
            ):
                raise PromotedFeatureStoreError(
                    "promoted technical feature replay differs"
                )
            return replayed
        except PromotedFeatureStoreError:
            raise
        except Exception:
            raise PromotedFeatureStoreError(
                "stored promoted technical feature artifact is invalid"
            ) from None


class LocalPromotedCrossSectionStore:
    """Stores canonical manifests and replays from exact technical panels."""

    def __init__(
        self,
        root: Path,
        source_resolver: PromotedTechnicalFeaturePanelResolver,
    ) -> None:
        self.root = Path(root)
        self.source_resolver = source_resolver

    @property
    def panels_root(self) -> Path:
        return self.root / "cross-sections"

    def path_for(self, panel_id: str) -> Path:
        return _path(self.panels_root, panel_id)

    def put(
        self,
        value: VerifiedPromotedCrossSectionPanel,
    ) -> VerifiedPromotedCrossSectionPanel:
        if type(value) is not VerifiedPromotedCrossSectionPanel:
            raise TypeError("promoted cross-section panel must be exact")
        value.verify_content_identity()
        payload = encode_cross_section_panel(value)
        stored = _put(
            root=self.panels_root,
            lock_name=".cross-sections.lock",
            panel_id=value.panel_id,
            payload=payload,
            read_existing=lambda panel_id: _read(self.panels_root, panel_id),
        )
        if stored != payload:
            raise PromotedFeatureStoreConflict(
                "promoted cross-section artifact differs"
            )
        return self.get(value.panel_id)

    def get(
        self,
        panel_id: str,
    ) -> VerifiedPromotedCrossSectionPanel:
        payload = _read(self.panels_root, panel_id)
        try:
            record = decode_cross_section_record(payload)
            if record.panel_id != panel_id:
                raise PromotedFeatureStoreError(
                    "promoted cross-section path identity differs"
                )
            source = self.source_resolver.get(record.source_panel_id)
            if (
                type(source) is not VerifiedPromotedTechnicalFeaturePanel
                or source.panel_id != record.source_panel_id
            ):
                raise PromotedFeatureStoreError(
                    "promoted cross-section source differs"
                )
            replayed = PromotedCrossSectionService().materialize(
                source_panel=source,
                config=record.config,
                cutoff=record.cutoff,
            )
            if (
                replayed.panel_id != panel_id
                or encode_cross_section_panel(replayed) != payload
            ):
                raise PromotedFeatureStoreError(
                    "promoted cross-section replay differs"
                )
            return replayed
        except PromotedFeatureStoreError:
            raise
        except Exception:
            raise PromotedFeatureStoreError(
                "stored promoted cross-section artifact is invalid"
            ) from None
