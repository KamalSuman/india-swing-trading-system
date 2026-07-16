from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Sequence

from .artifact_store import LocalDailyBundleArtifactStore
from .config import DailyReportsConfig
from .parser import report_family_counts


class DailyReportsArgumentError(ValueError):
    pass


class SanitizedArgumentParser(argparse.ArgumentParser):
    def error(self, message: str) -> None:
        raise DailyReportsArgumentError("invalid daily-reports arguments")


def parser() -> argparse.ArgumentParser:
    root = SanitizedArgumentParser(
        description="Validate and archive an NSE multiple-report download ZIP"
    )
    datasets = root.add_subparsers(dest="dataset", required=True)
    bundle = datasets.add_parser("bundle", help="NSE multiple-report bundle operations")
    actions = bundle.add_subparsers(dest="action", required=True)
    import_command = actions.add_parser(
        "import",
        help="validate and archive one manually downloaded report bundle",
    )
    import_command.add_argument("--file", type=Path, required=True)
    return root


def main(argv: Sequence[str] | None = None) -> int:
    try:
        args = parser().parse_args(argv)
        config = DailyReportsConfig.from_env()
        store = LocalDailyBundleArtifactStore(config.data_root)
        stored = store.import_bundle(args.file)
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
                "validated_at": stored.manifest.validated_at.isoformat(),
                "outer_entry_count": stored.manifest.outer_entry_count,
                "selected_report_count": stored.manifest.selected_report_count,
                "quarantined_report_count": (
                    stored.manifest.quarantined_report_count
                ),
                "deferred_report_count": stored.manifest.deferred_report_count,
                "ignored_entry_count": stored.manifest.ignored_entry_count,
                "selected_row_count": stored.manifest.selected_row_count,
                "selected_family_counts": report_family_counts(stored.parsed),
                "acquisition_mode": stored.manifest.acquisition_mode.value,
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
