from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Sequence

from .artifact_store import LocalReferenceArtifactStore
from .config import ReferenceDataConfig


class ReferenceDataArgumentError(ValueError):
    pass


class SanitizedArgumentParser(argparse.ArgumentParser):
    def error(self, message: str) -> None:
        raise ReferenceDataArgumentError("invalid reference-data arguments")


def parser() -> argparse.ArgumentParser:
    root = SanitizedArgumentParser(
        description="Validate and archive manually downloaded official reference data"
    )
    datasets = root.add_subparsers(dest="dataset", required=True)
    security_master = datasets.add_parser(
        "security-master",
        help="NSE CM MII security-master operations",
    )
    actions = security_master.add_subparsers(dest="action", required=True)
    import_command = actions.add_parser(
        "import",
        help="validate and archive one manually downloaded NSE security master",
    )
    import_command.add_argument("--file", type=Path, required=True)
    return root


def main(argv: Sequence[str] | None = None) -> int:
    try:
        args = parser().parse_args(argv)
        config = ReferenceDataConfig.from_env()
        store = LocalReferenceArtifactStore(config.data_root)
        stored = store.import_security_master(args.file)
    except Exception as exc:
        print(
            json.dumps(
                {
                    "status": "FAILED",
                    "error_type": type(exc).__name__,
                },
                sort_keys=True,
            ),
            file=sys.stderr,
        )
        return 2

    print(
        json.dumps(
            {
                "status": "COMPLETE",
                "dataset": stored.manifest.dataset,
                "artifact_id": stored.manifest.artifact_id,
                "claimed_report_date": (
                    stored.manifest.claimed_report_date.isoformat()
                ),
                "verified_report_date": (
                    stored.manifest.verified_report_date.isoformat()
                    if stored.manifest.verified_report_date is not None
                    else None
                ),
                "acquisition_mode": stored.manifest.acquisition_mode.value,
                "validated_at": stored.manifest.validated_at.isoformat(),
                "record_count": stored.manifest.parsed_row_count,
                "retained_unverified_equity_count": (
                    stored.manifest.retained_unverified_equity_count
                ),
                "readiness": stored.manifest.readiness.value,
                "actionable": stored.manifest.actionable,
                "path": str(stored.path.resolve()),
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
