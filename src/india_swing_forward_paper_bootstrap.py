"""Minimal observable bootstrap for the forward-paper Cloud Run job.

This module intentionally lives outside the ``india_swing`` package so its
first stderr event is emitted before importing the package's broad public
surface.  It delegates all validation and work to the existing cloud-job
entrypoint; no data, identity, or publication rule is changed here.
"""

from __future__ import annotations

import faulthandler
import importlib
import json
import sys
import time
from collections.abc import Callable, Sequence
from types import ModuleType


_LEGACY_MODULE_PREFIX = (
    "-m",
    "india_swing.forward_paper.operational_cloud_job",
)


def _event(stage: str, status: str, elapsed_seconds: float) -> None:
    print(
        json.dumps(
            {
                "elapsed_seconds": round(elapsed_seconds, 3),
                "event": "FORWARD_PAPER_BOOTSTRAP",
                "stage": stage,
                "status": status,
            },
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ),
        file=sys.stderr,
        flush=True,
    )


def main(
    argv: Sequence[str] | None = None,
    *,
    importer: Callable[[str], ModuleType] = importlib.import_module,
    clock: Callable[[], float] = time.perf_counter,
    enable_tracebacks: bool = True,
) -> int:
    started_at = clock()
    delegated_argv = list(argv) if argv is not None else list(sys.argv[1:])
    if tuple(delegated_argv[:2]) == _LEGACY_MODULE_PREFIX:
        delegated_argv = delegated_argv[2:]
    _event("process_start", "completed", 0.0)
    if enable_tracebacks:
        faulthandler.enable(file=sys.stderr, all_threads=True)
        faulthandler.dump_traceback_later(60, repeat=True, file=sys.stderr)
    try:
        _event("application_import", "started", clock() - started_at)
        module = importer("india_swing.forward_paper.operational_cloud_job")
        _event("application_import", "completed", clock() - started_at)
        entrypoint = getattr(module, "main", None)
        if not callable(entrypoint):
            return 2
        return int(entrypoint(delegated_argv))
    finally:
        if enable_tracebacks:
            faulthandler.cancel_dump_traceback_later()


if __name__ == "__main__":
    raise SystemExit(main())
