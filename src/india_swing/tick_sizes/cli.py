from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from typing import Sequence

from india_swing.reference_data import LocalReferenceArtifactStore
from india_swing.reference_data.config import ReferenceDataConfig

from .config import TickSizeConfig
from .materialize import materialize_collection_tick_sizes
from .models import CollectionTickSizeSnapshot
from .store import LocalTickSizeSnapshotStore


class TickSizeArgumentError(ValueError):
    pass


class SanitizedArgumentParser(argparse.ArgumentParser):
    def error(self, message: str) -> None:
        raise TickSizeArgumentError("invalid tick-size arguments")


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
        description="Materialize collection-only NSE CM tick-size evidence"
    )
    commands = root.add_subparsers(dest="command", required=True)
    materialize = commands.add_parser("materialize")
    materialize.add_argument("--security-master-id", required=True)
    materialize.add_argument("--cutoff", required=True, type=_aware_datetime)
    show = commands.add_parser("show")
    show.add_argument("--snapshot-id", required=True)
    commands.add_parser("list")
    return root


def _summary(value: CollectionTickSizeSnapshot) -> dict[str, object]:
    if type(value) is not CollectionTickSizeSnapshot:
        raise TypeError("tick-size summary requires an exact snapshot")
    value.verify_content_identity()
    distribution: dict[str, int] = {}
    for observation in value.observations:
        key = str(observation.bid_interval_paise)
        distribution[key] = distribution.get(key, 0) + 1
    return {
        "snapshot_id": value.snapshot_id,
        "market_session_claim": value.market_session_claim.isoformat(),
        "cutoff": value.cutoff.isoformat(),
        "knowledge_time": value.knowledge_time.isoformat(),
        "source_artifact_id": value.source_artifact_id,
        "source_manifest_id": value.source_manifest_id,
        "observation_count": len(value.observations),
        "bid_interval_paise_distribution": dict(
            sorted(distribution.items(), key=lambda item: int(item[0]))
        ),
        "reason_codes": list(value.reason_codes),
        "readiness": value.readiness.value,
        "actionable": value.actionable,
    }


def main(argv: Sequence[str] | None = None) -> int:
    try:
        args = parser().parse_args(argv)
        reference_root = ReferenceDataConfig.from_env().data_root
        store = LocalTickSizeSnapshotStore(
            TickSizeConfig.from_env().data_root,
            reference_root,
        )
        if args.command == "materialize":
            source = LocalReferenceArtifactStore(reference_root).get(
                args.security_master_id
            )
            value = store.put(
                materialize_collection_tick_sizes(source, cutoff=args.cutoff)
            )
            response = {
                "status": "COMPLETE",
                "kind": "TICK_SIZE_SNAPSHOT",
                **_summary(value),
            }
        elif args.command == "show":
            response = {
                "status": "COMPLETE",
                "kind": "TICK_SIZE_SNAPSHOT",
                **_summary(store.get(args.snapshot_id)),
            }
        else:
            response = {
                "status": "COMPLETE",
                "kind": "TICK_SIZE_SNAPSHOT_LIST",
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
