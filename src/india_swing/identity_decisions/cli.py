from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Sequence

from india_swing.identity_evidence import IdentityEvidenceConfig, LocalIdentityEvidenceArtifactStore
from india_swing.identity_registry import (
    IdentityRegistryConfig,
    LocalIdentityAdjudicationQueueStore,
    LocalIdentityRegistryStore,
)
from india_swing.reference_data.config import ReferenceDataConfig

from .artifact_store import LocalIdentityReviewBundleStore
from .materialize import materialize_adjudicated_identity_snapshot
from .models import AdjudicatedIdentitySnapshot, StoredIdentityReviewBundle
from .snapshot_store import LocalAdjudicatedIdentitySnapshotStore


class IdentityDecisionArgumentError(ValueError):
    pass


class SanitizedArgumentParser(argparse.ArgumentParser):
    def error(self, message: str) -> None:
        raise IdentityDecisionArgumentError("invalid identity-decision arguments")


def _aware_datetime(value: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("cutoff must be ISO-8601") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise argparse.ArgumentTypeError("cutoff must include a timezone offset")
    return parsed


def parser() -> argparse.ArgumentParser:
    root = SanitizedArgumentParser(description="Archive explicit identity reviews and materialize partial stable IDs")
    commands = root.add_subparsers(dest="command", required=True)
    review_import = commands.add_parser("review-import", help="archive one strict manual review declaration")
    review_import.add_argument("--declaration", type=Path, required=True)
    review_show = commands.add_parser("review-show", help="show one review bundle summary")
    review_show.add_argument("--review-bundle-id", required=True)
    commands.add_parser("review-list", help="list review bundle summaries")
    materialize = commands.add_parser("materialize", help="materialize an explicit partial stable-identity snapshot")
    materialize.add_argument("--registry-id", required=True)
    materialize.add_argument("--evidence-id", action="append", default=[], dest="evidence_ids")
    materialize.add_argument("--review-bundle-id", action="append", default=[], dest="review_bundle_ids")
    materialize.add_argument("--cutoff", type=_aware_datetime, required=True)
    snapshot_show = commands.add_parser("snapshot-show", help="show one adjudicated identity snapshot")
    snapshot_show.add_argument("--snapshot-id", required=True)
    commands.add_parser("snapshot-list", help="list adjudicated identity snapshots")
    return root


def _review_summary(value: StoredIdentityReviewBundle) -> dict[str, object]:
    if type(value) is not StoredIdentityReviewBundle:
        raise TypeError("review summary requires an exact stored bundle")
    accepted = sum(item.outcome.value == "ACCEPTED" for item in value.parsed.decisions)
    return {
        "review_bundle_id": value.manifest.bundle_id,
        "manifest_id": value.manifest.manifest_id,
        "queue_id": value.manifest.queue_id,
        "registry_id": value.manifest.source_registry_id,
        "reviewer_id": value.manifest.reviewer_id,
        "reviewed_at": value.manifest.reviewed_at.isoformat(),
        "knowledge_time": value.manifest.validated_at.isoformat(),
        "decision_count": len(value.parsed.decisions),
        "accepted_count": accepted,
        "rejected_count": len(value.parsed.decisions) - accepted,
        "readiness": value.manifest.readiness.value,
        "actionable": value.manifest.actionable,
    }


def _snapshot_summary(value: AdjudicatedIdentitySnapshot) -> dict[str, object]:
    if type(value) is not AdjudicatedIdentitySnapshot:
        raise TypeError("snapshot summary requires an exact adjudicated identity snapshot")
    blocker_counts: dict[str, int] = {}
    for resolution in value.resolutions:
        for blocker in resolution.blocker_codes:
            blocker_counts[blocker.value] = blocker_counts.get(blocker.value, 0) + 1
    return {
        "snapshot_id": value.snapshot_id,
        "registry_id": value.source_registry_id,
        "queue_id": value.source_queue_id,
        "cutoff": value.cutoff.isoformat(),
        "knowledge_time": value.knowledge_time.isoformat(),
        "candidate_count": len(value.resolutions),
        "assigned_candidate_count": sum(item.stable_instrument_id is not None for item in value.resolutions),
        "listing_observation_count": len(value.listing_observations),
        "evidence_artifact_count": len(value.evidence_artifact_ids),
        "review_bundle_count": len(value.review_bundle_ids),
        "blocker_counts": dict(sorted(blocker_counts.items())),
        "readiness": value.readiness.value,
        "actionable": value.actionable,
        "stable_identity_assigned": value.stable_identity_assigned,
    }


def main(argv: Sequence[str] | None = None) -> int:
    try:
        args = parser().parse_args(argv)
        decision_root = IdentityEvidenceConfig.from_env().data_root
        review_store = LocalIdentityReviewBundleStore(decision_root)
        snapshot_store = LocalAdjudicatedIdentitySnapshotStore(decision_root)
        if args.command == "review-import":
            response = {"status": "COMPLETE", "kind": "IDENTITY_REVIEW_BUNDLE", **_review_summary(
                review_store.import_declaration(args.declaration)
            )}
        elif args.command == "review-show":
            response = {"status": "COMPLETE", "kind": "IDENTITY_REVIEW_BUNDLE", **_review_summary(
                review_store.get(args.review_bundle_id)
            )}
        elif args.command == "review-list":
            response = {
                "status": "COMPLETE", "kind": "IDENTITY_REVIEW_BUNDLE_LIST",
                "bundles": [_review_summary(value) for value in review_store.list_bundles()],
            }
        elif args.command == "materialize":
            identity_config = IdentityRegistryConfig.from_env()
            registry_store = LocalIdentityRegistryStore(
                identity_config.data_root, ReferenceDataConfig.from_env().data_root
            )
            queue_store = LocalIdentityAdjudicationQueueStore(identity_config.data_root, registry_store)
            registry = registry_store.get(args.registry_id).registry
            queue = queue_store.get(args.registry_id)
            evidence_store = LocalIdentityEvidenceArtifactStore(decision_root)
            snapshot = materialize_adjudicated_identity_snapshot(
                registry=registry, queue=queue,
                evidence_artifacts=tuple(evidence_store.get(value) for value in args.evidence_ids),
                review_bundles=tuple(review_store.get(value) for value in args.review_bundle_ids),
                cutoff=args.cutoff,
            )
            response = {"status": "COMPLETE", "kind": "ADJUDICATED_IDENTITY_SNAPSHOT", **_snapshot_summary(
                snapshot_store.put(snapshot)
            )}
        elif args.command == "snapshot-show":
            response = {"status": "COMPLETE", "kind": "ADJUDICATED_IDENTITY_SNAPSHOT", **_snapshot_summary(
                snapshot_store.get(args.snapshot_id)
            )}
        else:
            response = {
                "status": "COMPLETE", "kind": "ADJUDICATED_IDENTITY_SNAPSHOT_LIST",
                "snapshots": [_snapshot_summary(value) for value in snapshot_store.list_snapshots()],
            }
    except Exception as exc:
        print(json.dumps({"status": "FAILED", "error_type": type(exc).__name__}, sort_keys=True), file=sys.stderr)
        return 2
    print(json.dumps(response, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
