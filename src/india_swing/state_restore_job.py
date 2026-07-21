from __future__ import annotations

import json
import sys
from typing import Sequence

from india_swing.daily_pipeline.cli import main as _daily_pipeline_main


class StateRestoreJobConfigurationError(Exception):
    pass


class StateRestoreJobRuntimeError(Exception):
    pass


def _parsed_spec_file(argv: Sequence[str]) -> str:
    spec_file: str | None = None
    index = 0
    while index < len(argv):
        token = argv[index]
        if token != "--spec-file":
            raise StateRestoreJobConfigurationError()
        if spec_file is not None or index + 1 >= len(argv):
            raise StateRestoreJobConfigurationError()
        candidate = argv[index + 1]
        if type(candidate) is not str or not candidate or "\x00" in candidate:
            raise StateRestoreJobConfigurationError()
        spec_file = candidate
        index += 2
    if spec_file is None:
        raise StateRestoreJobConfigurationError()
    return spec_file


def main(argv: Sequence[str] | None = None) -> int:
    """Narrow fail-closed wrapper for an explicitly configured restore job.

    It accepts exactly one ``--spec-file <path>`` pair and delegates once to
    the daily-pipeline restoration command. It reads no file, environment,
    credential, GCS object, clock, broker, or notification configuration and
    has no fallback operation. Deployment must provide a persistent
    POSIX-compatible destination; this module does not provision one.
    """

    args = list(argv) if argv is not None else sys.argv[1:]
    try:
        spec_file = _parsed_spec_file(args)
    except Exception:
        print(
            json.dumps(
                {
                    "status": "FAILED",
                    "error_type": StateRestoreJobConfigurationError.__name__,
                },
                sort_keys=True,
            ),
            file=sys.stderr,
        )
        return 2

    try:
        return _daily_pipeline_main(
            ["restore-pinned-state", "--spec-file", spec_file]
        )
    except Exception:
        print(
            json.dumps(
                {
                    "status": "FAILED",
                    "error_type": StateRestoreJobRuntimeError.__name__,
                },
                sort_keys=True,
            ),
            file=sys.stderr,
        )
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
