"""One-shot hydrated Cloud Run wrapper for the forward-paper graph job.

The wrapper composes the existing promoted-input snapshot hydrator with the
exact-generation research-dataset reader.  The large canonical market corpus
remains on a read-only Cloud Storage volume and is never copied or listed.
"""

from __future__ import annotations

import io
import json
import os
import re
import stat
import sys
from collections.abc import Callable, Sequence
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path

from india_swing._filesystem import read_stable_regular_file
from india_swing.daily_pipeline.acquisition import GoogleCloudStorageObjectReader
from india_swing.daily_pipeline.state_publication import (
    GoogleCloudStorageStateObjectWriter,
)
from india_swing.forward_paper.hydrated_cloud_control import (
    MAXIMUM_FORWARD_PAPER_HYDRATED_LAUNCH_BYTES,
    ForwardPaperHydratedCloudLaunch,
    decode_forward_paper_hydrated_cloud_launch,
)
from india_swing.promoted_operational_cloud_control import (
    PromotedOperationalCloudRunControl,
)
from india_swing.promoted_operational_input_gcs import (
    AcquiredPromotedOperationalInputSnapshot,
    CompletedPromotedOperationalInputRestore,
    acquire_promoted_operational_input_snapshot,
    hydrate_promoted_operational_input_snapshot,
)
from india_swing.promoted_operational_input_snapshot import ROOT_INPUT_NAMES

import india_swing.forward_paper.operational_cloud_job as _inner_job


class ForwardPaperHydratedCloudJobError(ValueError):
    """Static, sanitized error for the outer one-shot job boundary."""


_ERROR = "forward paper hydrated cloud job failed"
_FIXED_RUNTIME_PARENT = Path("/tmp/india-swing-forward-paper")
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_MANIFEST_PATH = re.compile(
    r"research/forward-paper-operational/v1/(\d{4}-\d{2}-\d{2})/"
    r"([0-9a-f]{64})/([0-9a-f]{64})\.json\Z"
)
_INNER_KEYS = frozenset(
    {
        "blocked_feature_count",
        "collection_only",
        "computed_feature_count",
        "execution_eligible",
        "graph_id",
        "manifest_generation",
        "manifest_object_name",
        "manifest_sha256",
        "notification_eligible",
        "paper_trade_eligible",
        "receipt_id",
        "request_id",
        "signal_session",
        "status",
    }
)
_MAXIMUM_INNER_STDOUT_BYTES = 64 * 1024


def _paths(argv: Sequence[str]) -> tuple[Path, Path]:
    if len(argv) != 4 or argv[0] != "--launch-file" or argv[2] != "--market-data-root":
        raise ForwardPaperHydratedCloudJobError(_ERROR)
    launch_file = Path(argv[1])
    market_data_root = Path(argv[3])
    for path in (launch_file, market_data_root):
        if not path.is_absolute() or ".." in path.parts:
            raise ForwardPaperHydratedCloudJobError(_ERROR)
    return launch_file, market_data_root


def _link_like(status: os.stat_result) -> bool:
    if stat.S_ISLNK(status.st_mode):
        return True
    return bool(
        getattr(status, "st_file_attributes", 0)
        & getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400)
    )


def _runtime_parent(value: Path) -> tuple[Path, tuple[int, int]]:
    failed = False
    status = None
    names: list[str] = []
    try:
        status = os.lstat(value)
        with os.scandir(value) as iterator:
            names = [entry.name for entry in iterator]
    except Exception:
        failed = True
    if (
        failed
        or status is None
        or not value.is_absolute()
        or ".." in value.parts
        or not stat.S_ISDIR(status.st_mode)
        or _link_like(status)
        or names
    ):
        raise ForwardPaperHydratedCloudJobError(_ERROR)
    return value, (status.st_dev, status.st_ino)


def _recheck_parent(path: Path, identity: tuple[int, int]) -> None:
    failed = False
    status = None
    try:
        status = os.lstat(path)
    except Exception:
        failed = True
    if (
        failed
        or status is None
        or not stat.S_ISDIR(status.st_mode)
        or _link_like(status)
        or (status.st_dev, status.st_ino) != identity
    ):
        raise ForwardPaperHydratedCloudJobError(_ERROR)


def _default_gcs_client_factory() -> object:
    from google.cloud import storage

    return storage.Client()


def _unique(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ForwardPaperHydratedCloudJobError(_ERROR)
        result[key] = value
    return result


def _reject_number(_value: str) -> object:
    raise ForwardPaperHydratedCloudJobError(_ERROR)


def _validate_inner(
    value: object, launch: ForwardPaperHydratedCloudLaunch
) -> dict[str, object]:
    if type(value) is not dict or set(value) != _INNER_KEYS:
        raise ForwardPaperHydratedCloudJobError(_ERROR)
    if value["status"] != "FORWARD_PAPER_OPERATIONAL_GRAPH_PUBLISHED":
        raise ForwardPaperHydratedCloudJobError(_ERROR)
    if (
        value["collection_only"] is not True
        or value["paper_trade_eligible"] is not False
        or value["notification_eligible"] is not False
        or value["execution_eligible"] is not False
        or value["signal_session"] != launch.signal_session.isoformat()
    ):
        raise ForwardPaperHydratedCloudJobError(_ERROR)
    for name in ("graph_id", "manifest_sha256", "receipt_id", "request_id"):
        if type(value[name]) is not str or _SHA256.fullmatch(value[name]) is None:
            raise ForwardPaperHydratedCloudJobError(_ERROR)
    for name in ("blocked_feature_count", "computed_feature_count"):
        if type(value[name]) is not int or value[name] < 0:
            raise ForwardPaperHydratedCloudJobError(_ERROR)
    if type(value["manifest_generation"]) is not int or value["manifest_generation"] <= 0:
        raise ForwardPaperHydratedCloudJobError(_ERROR)
    object_name = value["manifest_object_name"]
    match = _MANIFEST_PATH.fullmatch(object_name) if type(object_name) is str else None
    if (
        match is None
        or match.group(1) != launch.signal_session.isoformat()
        or match.group(2) != value["graph_id"]
    ):
        raise ForwardPaperHydratedCloudJobError(_ERROR)
    return value


def main(
    argv: Sequence[str] | None = None,
    *,
    runtime_parent: Path | None = None,
    gcs_client_factory: Callable[[], object] | None = None,
    inner_job_main: Callable[..., int] | None = None,
) -> int:
    try:
        launch_file, market_data_root = _paths(
            list(argv) if argv is not None else sys.argv[1:]
        )
        launch = decode_forward_paper_hydrated_cloud_launch(
            read_stable_regular_file(
                launch_file,
                maximum_bytes=MAXIMUM_FORWARD_PAPER_HYDRATED_LAUNCH_BYTES,
            )
        )
        parent, parent_identity = _runtime_parent(
            runtime_parent if runtime_parent is not None else _FIXED_RUNTIME_PARENT
        )
        active_client_factory = gcs_client_factory or _default_gcs_client_factory
        active_inner = inner_job_main or _inner_job.main
        if not callable(active_client_factory) or not callable(active_inner):
            raise ForwardPaperHydratedCloudJobError(_ERROR)
        client = active_client_factory()
        if client is None:
            raise ForwardPaperHydratedCloudJobError(_ERROR)
        reader = GoogleCloudStorageObjectReader(client=client)
        acquired = acquire_promoted_operational_input_snapshot(
            request=launch.promoted_input_launch.input_restore,
            reader=reader,
        )
        if type(acquired) is not AcquiredPromotedOperationalInputSnapshot:
            raise ForwardPaperHydratedCloudJobError(_ERROR)
        _recheck_parent(parent, parent_identity)
        roots = {name: parent / name for name in ROOT_INPUT_NAMES}
        promoted = launch.promoted_input_launch
        control = PromotedOperationalCloudRunControl(
            expected_assembly_spec_id=promoted.expected_assembly_spec_id,
            expected_operational_run_spec_id=promoted.expected_operational_run_spec_id,
            target_session=promoted.target_session,
            state_bucket=promoted.state_bucket,
            assembly_spec_file=parent / "assembly-spec.json",
            prior_state_restore=None,
            state_root=parent / "state",
            **roots,
        )
        restored = hydrate_promoted_operational_input_snapshot(
            control=control,
            acquired=acquired,
        )
        if (
            type(restored) is not CompletedPromotedOperationalInputRestore
            or restored.manifest.snapshot_id
            != promoted.input_restore.expected_snapshot_id
        ):
            raise ForwardPaperHydratedCloudJobError(_ERROR)
        _recheck_parent(parent, parent_identity)

        request = launch.dataset_request
        inner_argv: list[str] = []
        path_values = {
            "market-data-root": market_data_root,
            "reference-root": roots["reference_root"],
            "identity-evidence-root": roots["identity_evidence_root"],
            "calendar-root": roots["calendar_root"],
            "daily-reports-root": roots["daily_reports_root"],
            "historical-corpus-root": roots["historical_corpus_root"],
            "promoted-root": roots["promoted_root"],
            "engine-run-root": roots["engine_run_root"],
        }
        for name, path in path_values.items():
            inner_argv.extend((f"--{name}", str(path)))
        inner_argv.extend(
            (
                "--dataset-id", request.dataset_id,
                "--dataset-bucket", request.bucket,
                "--dataset-generation", str(request.generation),
                "--dataset-sha256", request.expected_sha256,
                "--signal-session", launch.signal_session.isoformat(),
                "--decision-cutoff", launch.decision_cutoff.isoformat(),
                "--expected-market-sessions",
                ";".join(item.isoformat() for item in launch.expected_market_sessions),
                "--corporate-action-snapshot-id", launch.corporate_action_snapshot_id,
                "--tick-panel-id", launch.tick_panel_id,
                "--bucket", launch.output_bucket,
            )
        )
        stdout = io.StringIO()
        stderr = io.StringIO()
        with redirect_stdout(stdout), redirect_stderr(stderr):
            code = active_inner(
                inner_argv,
                reader_factory=lambda: reader,
                writer_factory=lambda: GoogleCloudStorageStateObjectWriter(client=client),
            )
        text = stdout.getvalue()
        if code != 0 or stderr.getvalue() or len(text.encode()) > _MAXIMUM_INNER_STDOUT_BYTES:
            raise ForwardPaperHydratedCloudJobError(_ERROR)
        lines = text.splitlines()
        if len(lines) != 1:
            raise ForwardPaperHydratedCloudJobError(_ERROR)
        envelope = json.loads(
            lines[0],
            object_pairs_hook=_unique,
            parse_float=_reject_number,
            parse_constant=_reject_number,
        )
        if json.dumps(envelope, separators=(",", ":"), sort_keys=True) != lines[0]:
            raise ForwardPaperHydratedCloudJobError(_ERROR)
        result = dict(_validate_inner(envelope, launch))
        result.update(
            {
                "dataset_generation": request.generation,
                "dataset_sha256": request.expected_sha256,
                "input_snapshot_id": promoted.input_restore.expected_snapshot_id,
                "inner_status": result["status"],
                "launch_id": launch.launch_id,
                "status": "FORWARD_PAPER_HYDRATED_CLOUD_JOB_COMPLETE",
            }
        )
        print(json.dumps(result, separators=(",", ":"), sort_keys=True))
        return 0
    except Exception:
        print(
            json.dumps(
                {"error_type": ForwardPaperHydratedCloudJobError.__name__, "status": "FAILED"},
                separators=(",", ":"),
                sort_keys=True,
            ),
            file=sys.stderr,
        )
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
