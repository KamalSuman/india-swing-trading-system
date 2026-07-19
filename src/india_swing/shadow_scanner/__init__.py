"""Deterministic observation-only scanner over collection artifacts."""

from .models import (
    CollectionShadowCandidate,
    CollectionShadowScanResult,
    CollectionShadowScannerConfig,
    ShadowScanError,
    ShadowScanStatus,
)
from .scanner import scan_collection_artifacts
from .store import (
    LocalCollectionShadowScanStore,
    ShadowScanNotFound,
    ShadowScanStoreError,
)

__all__ = (
    "CollectionShadowCandidate",
    "CollectionShadowScanResult",
    "CollectionShadowScannerConfig",
    "LocalCollectionShadowScanStore",
    "ShadowScanError",
    "ShadowScanNotFound",
    "ShadowScanStoreError",
    "ShadowScanStatus",
    "scan_collection_artifacts",
)
