from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from typing import Sequence

from india_swing.reference_data import LocalReferenceArtifactStore
from india_swing.reference_data.config import ReferenceDataConfig

from .config import CollectionUniverseConfig
from .materialize import materialize_collection_universe
from .models import CollectionUniverseSnapshot
from .store import LocalCollectionUniverseSnapshotStore


class CollectionUniverseArgumentError(ValueError):
    pass


class SanitizedArgumentParser(argparse.ArgumentParser):
    def error(self, message: str) -> None:
        raise CollectionUniverseArgumentError("invalid universe arguments")


def _aware_datetime(value: str) -> datetime:
    try:
        result = datetime.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("cutoff must be ISO-8601") from exc
    if result.tzinfo is None or result.utcoffset() is None:
        raise argparse.ArgumentTypeError("cutoff must include a timezone offset")
    return result


def parser() -> argparse.ArgumentParser:
    root = SanitizedArgumentParser(
        description="Materialize collection-only broad NSE CM universe evidence"
    )
    commands = root.add_subparsers(dest="command", required=True)
    materialize = commands.add_parser("materialize")
    materialize.add_argument("--security-master-id", required=True)
    materialize.add_argument("--calendar-snapshot-id", required=True)
    materialize.add_argument("--cutoff", required=True, type=_aware_datetime)
    show = commands.add_parser("show")
    show.add_argument("--snapshot-id", required=True)
    commands.add_parser("list")
    return root


def _summary(value: CollectionUniverseSnapshot) -> dict[str, object]:
    if type(value) is not CollectionUniverseSnapshot:
        raise TypeError("universe summary requires an exact snapshot")
    value.verify_content_identity()
    distribution: dict[str, int] = {}
    for observation in value.observations:
        key = observation.disposition.value
        distribution[key] = distribution.get(key, 0) + 1
    return {
        "snapshot_id": value.snapshot_id,
        "market_session_claim": value.market_session_claim.isoformat(),
        "cutoff": value.cutoff.isoformat(),
        "knowledge_time": value.knowledge_time.isoformat(),
        "calendar_snapshot_id": value.calendar_snapshot_id,
        "source_artifact_id": value.source_artifact_id,
        "source_manifest_id": value.source_manifest_id,
        "source_record_count": len(value.observations),
        "broad_equity_scope_count": len(value.in_scope_observations),
        "disposition_distribution": dict(sorted(distribution.items())),
        "market_cap_cutoff": None,
        "reason_codes": list(value.reason_codes),
        "readiness": value.readiness.value,
        "actionable": value.actionable,
    }


def main(argv: Sequence[str] | None = None) -> int:
    try:
        args = parser().parse_args(argv)
        reference_root = ReferenceDataConfig.from_env().data_root
        store = LocalCollectionUniverseSnapshotStore(
            CollectionUniverseConfig.from_env().data_root,
            reference_root,
        )
        if args.command == "materialize":
            source = LocalReferenceArtifactStore(reference_root).get(
                args.security_master_id
            )
            value = store.put(
                materialize_collection_universe(
                    source,
                    cutoff=args.cutoff,
                    calendar_snapshot_id=args.calendar_snapshot_id,
                )
            )
            response = {
                "status": "COMPLETE",
                "kind": "COLLECTION_UNIVERSE_SNAPSHOT",
                **_summary(value),
            }
        elif args.command == "show":
            response = {
                "status": "COMPLETE",
                "kind": "COLLECTION_UNIVERSE_SNAPSHOT",
                **_summary(store.get(args.snapshot_id)),
            }
        else:
            response = {
                "status": "COMPLETE",
                "kind": "COLLECTION_UNIVERSE_SNAPSHOT_LIST",
                "snapshots": [_summary(value) for value in store.list_snapshots()],
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
