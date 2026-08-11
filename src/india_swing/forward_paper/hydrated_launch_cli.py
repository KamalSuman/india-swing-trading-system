"""Create one exact local forward-paper hydrated launch file.

This command performs no cloud call, discovery, scheduling, or execution.  It
only combines an already-published promoted-input launch with explicit dataset,
history, corporate-action, tick, and output pins and writes the result once.
"""

from __future__ import annotations

import json
import os
import stat
import sys
from collections.abc import Sequence
from datetime import date, datetime
from pathlib import Path

from india_swing._filesystem import read_stable_regular_file
from india_swing.evaluation.nse_archive_research_dataset_gcs import (
    PinnedNseArchiveResearchDatasetRequest,
)
from india_swing.forward_paper.hydrated_cloud_control import (
    MAXIMUM_FORWARD_PAPER_HYDRATED_LAUNCH_BYTES,
    ForwardPaperHydratedCloudLaunch,
    encode_forward_paper_hydrated_cloud_launch,
)
from india_swing.promoted_operational_hydrated_cloud_control import (
    MAXIMUM_HYDRATED_CLOUD_LAUNCH_BYTES,
    decode_promoted_operational_hydrated_cloud_launch,
)


class ForwardPaperHydratedLaunchCLIError(ValueError):
    """Static, sanitized launch-builder failure."""


_ERROR = "forward paper hydrated launch preparation failed"
_FLAGS = frozenset(
    {
        "--promoted-launch-file",
        "--output-launch-file",
        "--dataset-bucket",
        "--dataset-id",
        "--dataset-generation",
        "--dataset-sha256",
        "--decision-cutoff",
        "--expected-market-sessions",
        "--corporate-action-snapshot-id",
        "--tick-panel-id",
        "--output-bucket",
    }
)


def _options(argv: Sequence[str]) -> dict[str, str]:
    if len(argv) != len(_FLAGS) * 2:
        raise ForwardPaperHydratedLaunchCLIError(_ERROR)
    result: dict[str, str] = {}
    iterator = iter(argv)
    for flag, value in zip(iterator, iterator):
        if type(flag) is not str or flag in result or flag not in _FLAGS:
            raise ForwardPaperHydratedLaunchCLIError(_ERROR)
        if type(value) is not str or not value:
            raise ForwardPaperHydratedLaunchCLIError(_ERROR)
        result[flag] = value
    if set(result) != _FLAGS:
        raise ForwardPaperHydratedLaunchCLIError(_ERROR)
    return result


def _absolute(value: str) -> Path:
    path = Path(value)
    if not path.is_absolute() or ".." in path.parts:
        raise ForwardPaperHydratedLaunchCLIError(_ERROR)
    return path


def _exclusive_write(path: Path, payload: bytes) -> None:
    failed = False
    descriptor: int | None = None
    try:
        parent_status = os.lstat(path.parent)
        if not stat.S_ISDIR(parent_status.st_mode) or stat.S_ISLNK(parent_status.st_mode):
            raise ValueError
        flags = (
            os.O_CREAT
            | os.O_EXCL
            | os.O_WRONLY
            | getattr(os, "O_BINARY", 0)
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_NOINHERIT", 0)
            | getattr(os, "O_NOFOLLOW", 0)
        )
        descriptor = os.open(path, flags, 0o600)
        with os.fdopen(descriptor, "wb") as handle:
            descriptor = None
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        if read_stable_regular_file(
            path, maximum_bytes=MAXIMUM_FORWARD_PAPER_HYDRATED_LAUNCH_BYTES
        ) != payload:
            raise ValueError
    except Exception:
        failed = True
    finally:
        if descriptor is not None:
            try:
                os.close(descriptor)
            except OSError:
                pass
    if failed:
        raise ForwardPaperHydratedLaunchCLIError(_ERROR)


def main(argv: Sequence[str] | None = None) -> int:
    try:
        options = _options(list(argv) if argv is not None else sys.argv[1:])
        source = _absolute(options["--promoted-launch-file"])
        output = _absolute(options["--output-launch-file"])
        promoted = decode_promoted_operational_hydrated_cloud_launch(
            read_stable_regular_file(
                source, maximum_bytes=MAXIMUM_HYDRATED_CLOUD_LAUNCH_BYTES
            )
        )
        sessions = tuple(
            date.fromisoformat(value)
            for value in options["--expected-market-sessions"].split(";")
        )
        launch = ForwardPaperHydratedCloudLaunch(
            promoted_input_launch=promoted,
            dataset_request=PinnedNseArchiveResearchDatasetRequest(
                bucket=options["--dataset-bucket"],
                dataset_id=options["--dataset-id"],
                generation=int(options["--dataset-generation"]),
                expected_sha256=options["--dataset-sha256"],
            ),
            decision_cutoff=datetime.fromisoformat(options["--decision-cutoff"]),
            expected_market_sessions=sessions,
            corporate_action_snapshot_id=options["--corporate-action-snapshot-id"],
            tick_panel_id=options["--tick-panel-id"],
            output_bucket=options["--output-bucket"],
        )
        _exclusive_write(output, encode_forward_paper_hydrated_cloud_launch(launch))
        print(
            json.dumps(
                {
                    "collection_only": True,
                    "launch_id": launch.launch_id,
                    "output_file": str(output),
                    "status": "FORWARD_PAPER_HYDRATED_LAUNCH_PREPARED",
                },
                separators=(",", ":"),
                sort_keys=True,
            )
        )
        return 0
    except Exception:
        print(
            json.dumps(
                {"error_type": ForwardPaperHydratedLaunchCLIError.__name__, "status": "FAILED"},
                separators=(",", ":"),
                sort_keys=True,
            ),
            file=sys.stderr,
        )
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
