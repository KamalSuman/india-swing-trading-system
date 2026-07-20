from __future__ import annotations

import json
import sys
from typing import Sequence

from india_swing.daily_pipeline.cli import main as _daily_pipeline_main


class CloudJobConfigurationError(Exception):
    """Raised for any invalid, missing, duplicate, positional, or unknown
    cloud-job argument. main() always reports this one static, sanitized
    type name -- never argv, paths, or nested exception text."""


class CloudJobRuntimeError(Exception):
    """Raised only when the delegated daily-pipeline CLI call unexpectedly
    raises instead of returning an exit code. main() always reports this
    one static, sanitized type name -- never the nested exception's class,
    message, or any path/secret content."""


def _parsed_spec_file(argv: Sequence[str]) -> str:
    """Strictly accepts exactly one required --spec-file <value> pair.

    No positional arguments, no unknown flags, no duplicate --spec-file,
    and no `=`-joined form -- this is a narrow, single-argument wrapper,
    not a general CLI. The returned value is the exact raw string; no path
    normalization or type conversion is performed here.
    """

    spec_file: str | None = None
    index = 0
    length = len(argv)
    while index < length:
        token = argv[index]
        if token != "--spec-file":
            raise CloudJobConfigurationError("unrecognized cloud job argument")
        if spec_file is not None:
            raise CloudJobConfigurationError("duplicate cloud job argument")
        if index + 1 >= length:
            raise CloudJobConfigurationError("missing cloud job argument value")
        spec_file = argv[index + 1]
        index += 2
    if spec_file is None or type(spec_file) is not str or not spec_file:
        raise CloudJobConfigurationError("missing required cloud job argument")
    return spec_file


def main(argv: Sequence[str] | None = None) -> int:
    """Fail-closed Cloud Run Job entrypoint.

    Accepts exactly one required --spec-file argument and delegates,
    exactly once, to india_swing.daily_pipeline.cli.main(['run-pinned-gcs',
    '--spec-file', <exact value>]). This module reads no spec content, no
    GCP/credential/Secret-Manager/Kite/LLM/notification configuration, and
    imports no demo/forecasting/research/signals/market_data/broker/order
    capability -- the existing daily-pipeline CLI and file boundary remain
    the sole authority for safe file validation, spec parsing, and GCS
    acquisition.

    A malformed argument list never falls back to demo, a discovered path,
    or a partial/successful no-op; it fails closed with one static,
    sanitized CloudJobConfigurationError before any delegation is
    attempted. A delegated call that returns normally (the daily-pipeline
    CLI already sanitizes its own internal failures) passes its exact exit
    code and stdout/stderr through unchanged, with no additional output
    from this wrapper. Only if delegation unexpectedly raises instead of
    returning does this outermost boundary collapse the failure to one
    static, sanitized CloudJobRuntimeError.
    """

    args = list(argv) if argv is not None else sys.argv[1:]
    try:
        spec_file = _parsed_spec_file(args)
    except Exception:
        print(
            json.dumps(
                {"status": "FAILED", "error_type": CloudJobConfigurationError.__name__},
                sort_keys=True,
            ),
            file=sys.stderr,
        )
        return 2

    try:
        return _daily_pipeline_main(["run-pinned-gcs", "--spec-file", spec_file])
    except Exception:
        print(
            json.dumps(
                {"status": "FAILED", "error_type": CloudJobRuntimeError.__name__},
                sort_keys=True,
            ),
            file=sys.stderr,
        )
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
