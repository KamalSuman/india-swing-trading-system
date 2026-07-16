from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from typing import Sequence

from india_swing.reference_data.artifact_store import LocalReferenceArtifactStore
from india_swing.reference_data.config import ReferenceDataConfig

from .adjudication import build_identity_adjudication_queue
from .adjudication_store import LocalIdentityAdjudicationQueueStore
from .artifact_store import LocalIdentityRegistryStore
from .config import IdentityRegistryConfig
from .materialize import materialize_cross_vintage_identity_registry


class IdentityRegistryArgumentError(ValueError):
    pass


class SanitizedArgumentParser(argparse.ArgumentParser):
    def error(self, message: str) -> None:
        raise IdentityRegistryArgumentError("invalid identity-registry arguments")


def _aware_datetime(value: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("cutoff must be ISO-8601") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise argparse.ArgumentTypeError("cutoff must include a timezone offset")
    return parsed


def parser() -> argparse.ArgumentParser:
    root = SanitizedArgumentParser(
        description="Build collection-only NSE CM cross-vintage identity candidates"
    )
    commands = root.add_subparsers(dest="command", required=True)
    materialize = commands.add_parser(
        "materialize",
        help="materialize identity candidates from sealed security-master vintages",
    )
    materialize.add_argument(
        "--security-master-id",
        action="append",
        required=True,
        dest="security_master_ids",
    )
    materialize.add_argument("--cutoff", type=_aware_datetime, required=True)
    adjudication = commands.add_parser(
        "adjudication-materialize",
        help="materialize the complete official-evidence review queue",
    )
    adjudication.add_argument("--registry-id", required=True)
    show = commands.add_parser(
        "adjudication-show",
        help="show one persisted adjudication queue summary",
    )
    show.add_argument("--registry-id", required=True)
    commands.add_parser(
        "adjudication-list",
        help="list persisted adjudication queue summaries",
    )
    return root


def _queue_summary(queue: object) -> dict[str, object]:
    from .adjudication import IdentityAdjudicationQueue

    if type(queue) is not IdentityAdjudicationQueue:
        raise TypeError("queue summary requires an exact adjudication queue")
    return {
        "registry_id": queue.source_registry_id,
        "queue_id": queue.queue_id,
        "case_count": len(queue.cases),
        "requirement_counts": dict(queue.requirement_counts),
        "readiness": queue.readiness.value,
        "actionable": queue.actionable,
        "stable_identity_assigned": queue.stable_identity_assigned,
    }


def main(argv: Sequence[str] | None = None) -> int:
    try:
        args = parser().parse_args(argv)
        reference_config = ReferenceDataConfig.from_env()
        identity_config = IdentityRegistryConfig.from_env()
        registry_store = LocalIdentityRegistryStore(
            identity_config.data_root,
            reference_config.data_root,
        )
        queue_store = LocalIdentityAdjudicationQueueStore(
            identity_config.data_root,
            registry_store,
        )
        if args.command == "materialize":
            source_store = LocalReferenceArtifactStore(reference_config.data_root)
            sources = tuple(
                source_store.get(value) for value in args.security_master_ids
            )
            registry = materialize_cross_vintage_identity_registry(
                sources=sources,
                cutoff=args.cutoff,
            )
            stored = registry_store.put(registry)
            response = {
                "status": "COMPLETE",
                "kind": "CROSS_VINTAGE_IDENTITY_CANDIDATES",
                "registry_id": stored.manifest.registry_id,
                "manifest_id": stored.manifest.manifest_id,
                "source_count": len(stored.manifest.source_artifact_ids),
                "observation_count": stored.manifest.observation_count,
                "candidate_count": stored.manifest.candidate_count,
                "transition_count": stored.manifest.transition_count,
                "conflict_count": stored.manifest.conflict_count,
                "knowledge_time": stored.manifest.knowledge_time.isoformat(),
                "readiness": stored.manifest.readiness.value,
                "actionable": stored.manifest.actionable,
                "stable_identity_assigned": False,
            }
        elif args.command == "adjudication-materialize":
            registry = registry_store.get(args.registry_id).registry
            queue = queue_store.publish(
                build_identity_adjudication_queue(registry),
                registry_id=registry.registry_id,
            )
            response = {
                "status": "COMPLETE",
                "kind": "IDENTITY_ADJUDICATION_QUEUE",
                **_queue_summary(queue),
            }
        elif args.command == "adjudication-show":
            response = {
                "status": "COMPLETE",
                "kind": "IDENTITY_ADJUDICATION_QUEUE",
                **_queue_summary(queue_store.get(args.registry_id)),
            }
        else:
            response = {
                "status": "COMPLETE",
                "kind": "IDENTITY_ADJUDICATION_QUEUE_LIST",
                "queues": [
                    _queue_summary(value) for value in queue_store.list_queues()
                ],
            }
    except Exception as exc:
        print(
            json.dumps(
                {"status": "FAILED", "error_type": type(exc).__name__},
                sort_keys=True,
            ),
            file=sys.stderr,
        )
        return 2
    print(json.dumps(response, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
