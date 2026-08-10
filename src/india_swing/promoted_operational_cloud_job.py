"""Cloud Run-shaped, paper-only promoted operational wrapper process
entrypoint.

Reads one canonical operator-authored ``PromotedOperationalCloudRunControl``
control file, optionally restores one externally pinned prior
promoted-operational GCS state into the existing local stores, invokes the
accepted ``india_swing.promoted_operational_job.main`` exactly once with one
shared injected GCS client, then independently resolves the verified
resulting terminal from its own local terminal store (never trusting the
inner job's stdout envelope as the state source) and publishes the
advisory/registration/terminal bundle through the accepted
``promoted_operational_gcs_state`` boundary, emitting the exact manifest
coordinates required for the next restart.

This module never reproduces any assembly, strategy, quote, allocation,
risk, sizing, terminal-binding, advisory, registration, or GCS-state codec
-- it only composes the already-accepted layers. It never sends Telegram
output, never places an order, never performs interactive/browser login or
token refresh, never lists a bucket, never selects a "latest" object, and
never deploys anything -- deployment remains a later, separately reviewed
increment. This wrapper is one bounded invocation: no loop, retry, sleep,
or daemon exists here.
"""

from __future__ import annotations

import io
import json
import os
import re
import sys
from collections.abc import Callable, Mapping, Sequence
from contextlib import redirect_stderr, redirect_stdout
from datetime import datetime
from pathlib import Path

from india_swing._filesystem import read_stable_regular_file
from india_swing.daily_pipeline.acquisition import GoogleCloudStorageObjectReader
from india_swing.daily_pipeline.state_publication import GoogleCloudStorageStateObjectWriter
from india_swing.paper_trades.store import LocalPaperTradeLedger
from india_swing.promoted_operational_assembly import load_promoted_operational_assembly_spec_file
from india_swing.promoted_operational_cloud_control import (
    MAXIMUM_CLOUD_CONTROL_BYTES,
    PromotedOperationalCloudRunControl,
    decode_promoted_operational_cloud_control,
)
from india_swing.promoted_operational_gcs_state import (
    CompletedPromotedOperationalGCSPublication,
    publish_promoted_operational_state_to_gcs,
    restore_promoted_operational_state_from_gcs,
)
from india_swing.promoted_operational_persistence import (
    LocalPromotedOperationalAdvisoryOutbox,
    LocalPromotedOperationalTerminalStore,
    PromotedOperationalTerminalRecord,
)

import india_swing.promoted_operational_job as _inner_job


class PromotedOperationalCloudJobError(ValueError):
    pass


_ERR_JOB = "promoted operational cloud job call is invalid"

_SHA256 = re.compile(r"[0-9a-f]{64}\Z")

_INNER_ENVELOPE_KEYS = frozenset(
    {
        "status",
        "assembly_spec_id",
        "runtime_job_spec_id",
        "operational_run_spec_id",
        "preparation_id",
        "target_session",
        "terminal_id",
        "terminal_status",
        "action",
        "failure_codes",
        "advisory_id",
        "binding_id",
        "binding_generation",
        "reused_existing_terminal",
        "paper_only",
        "notification_eligible",
        "execution_eligible",
    }
)

_INNER_JOB_OPTIONS = (
    ("--assembly-spec-file", "assembly_spec_file"),
    ("--reference-root", "reference_root"),
    ("--identity-evidence-root", "identity_evidence_root"),
    ("--calendar-root", "calendar_root"),
    ("--daily-reports-root", "daily_reports_root"),
    ("--historical-corpus-root", "historical_corpus_root"),
    ("--promoted-root", "promoted_root"),
    ("--graph-publication-root", "graph_publication_root"),
    ("--engine-run-root", "engine_run_root"),
    ("--research-run-root", "research_run_root"),
    ("--operational-preparation-root", "operational_preparation_root"),
    ("--portfolio-artifact-root", "portfolio_artifact_root"),
    ("--state-root", "state_root"),
)


def _control_file_path(argv: Sequence[str]):
    if len(argv) != 2 or argv[0] != "--control-file":
        raise PromotedOperationalCloudJobError(_ERR_JOB)
    value = argv[1]
    if type(value) is not str or not value:
        raise PromotedOperationalCloudJobError(_ERR_JOB)
    path = Path(value)
    if not path.is_absolute() or ".." in path.parts:
        raise PromotedOperationalCloudJobError(_ERR_JOB)
    return path


def _inner_job_argv(control: PromotedOperationalCloudRunControl) -> list[str]:
    argv: list[str] = []
    for option, attribute in _INNER_JOB_OPTIONS:
        argv.extend([option, str(getattr(control, attribute))])
    return argv


def _default_gcs_client_factory() -> object:
    from google.cloud import storage

    return storage.Client()


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise PromotedOperationalCloudJobError(_ERR_JOB)
        result[key] = value
    return result


def _reject_number(_token: str) -> object:
    raise PromotedOperationalCloudJobError(_ERR_JOB)


def main(
    argv: Sequence[str] | None = None,
    *,
    environ: Mapping[str, str] | None = None,
    clock: Callable[[], datetime] | None = None,
    kite_adapter_factory: Callable[..., object] | None = None,
    gcs_client_factory: Callable[[], object] | None = None,
    runtime_callable: Callable[..., object] | None = None,
    promoted_operational_job_main: Callable[..., int] | None = None,
) -> int:
    args = list(argv) if argv is not None else sys.argv[1:]
    try:
        control_file = _control_file_path(args)

        control_payload = read_stable_regular_file(
            control_file, maximum_bytes=MAXIMUM_CLOUD_CONTROL_BYTES
        )
        control = decode_promoted_operational_cloud_control(control_payload)

        assembly_spec = load_promoted_operational_assembly_spec_file(control.assembly_spec_file)
        if (
            assembly_spec.assembly_spec_id != control.expected_assembly_spec_id
            or assembly_spec.target_session != control.target_session
            or assembly_spec.binding_bucket != control.state_bucket
        ):
            raise PromotedOperationalCloudJobError(_ERR_JOB)

        active_gcs_client_factory = (
            gcs_client_factory if gcs_client_factory is not None else _default_gcs_client_factory
        )
        active_inner_main = (
            promoted_operational_job_main
            if promoted_operational_job_main is not None
            else _inner_job.main
        )
        if not (
            (clock is None or callable(clock))
            and (kite_adapter_factory is None or callable(kite_adapter_factory))
            and callable(active_gcs_client_factory)
            and (runtime_callable is None or callable(runtime_callable))
            and callable(active_inner_main)
        ):
            raise PromotedOperationalCloudJobError(_ERR_JOB)

        client = active_gcs_client_factory()
        if client is None:
            raise PromotedOperationalCloudJobError(_ERR_JOB)
        reader = GoogleCloudStorageObjectReader(client=client)
        writer = GoogleCloudStorageStateObjectWriter(client=client)

        advisory_outbox = LocalPromotedOperationalAdvisoryOutbox(control.state_root / "advisory")
        terminal_store = LocalPromotedOperationalTerminalStore(control.state_root / "terminal")
        paper_ledger = LocalPaperTradeLedger(control.state_root / "paper")

        if control.prior_state_restore is not None:
            restored = restore_promoted_operational_state_from_gcs(
                request=control.prior_state_restore,
                reader=reader,
                terminal_store=terminal_store,
                advisory_outbox=advisory_outbox,
                paper_ledger=paper_ledger,
            )
            if (
                restored.terminal.spec_id != control.expected_operational_run_spec_id
                or restored.terminal.target_session != control.target_session
                or restored.manifest.bucket != control.state_bucket
            ):
                raise PromotedOperationalCloudJobError(_ERR_JOB)

        runtime_environ = os.environ if environ is None else environ

        inner_argv = _inner_job_argv(control)
        inner_kwargs: dict[str, object] = {
            "environ": runtime_environ,
            "gcs_client_factory": lambda: client,
        }
        if clock is not None:
            inner_kwargs["clock"] = clock
        if kite_adapter_factory is not None:
            inner_kwargs["kite_adapter_factory"] = kite_adapter_factory
        if runtime_callable is not None:
            inner_kwargs["runtime_callable"] = runtime_callable

        inner_stdout = io.StringIO()
        inner_stderr = io.StringIO()
        with redirect_stdout(inner_stdout), redirect_stderr(inner_stderr):
            inner_exit_code = active_inner_main(inner_argv, **inner_kwargs)

        inner_stdout_text = inner_stdout.getvalue()
        inner_stderr_text = inner_stderr.getvalue()
        if inner_exit_code != 0 or inner_stderr_text != "":
            raise PromotedOperationalCloudJobError(_ERR_JOB)

        stdout_lines = inner_stdout_text.split("\n")
        if len(stdout_lines) != 2 or stdout_lines[1] != "" or not stdout_lines[0]:
            raise PromotedOperationalCloudJobError(_ERR_JOB)
        inner_line = stdout_lines[0]

        inner_envelope = json.loads(
            inner_line,
            object_pairs_hook=_unique_object,
            parse_float=_reject_number,
            parse_constant=_reject_number,
        )
        if type(inner_envelope) is not dict or set(inner_envelope) != _INNER_ENVELOPE_KEYS:
            raise PromotedOperationalCloudJobError(_ERR_JOB)
        if (
            json.dumps(
                inner_envelope, allow_nan=False, ensure_ascii=False, separators=(",", ":"), sort_keys=True,
            )
            != inner_line
        ):
            raise PromotedOperationalCloudJobError(_ERR_JOB)
        if (
            inner_envelope["status"] != "PROMOTED_OPERATIONAL_JOB_COMPLETE"
            or inner_envelope["assembly_spec_id"] != control.expected_assembly_spec_id
            or inner_envelope["operational_run_spec_id"] != control.expected_operational_run_spec_id
            or inner_envelope["target_session"] != control.target_session.isoformat()
            or inner_envelope["paper_only"] is not True
            or inner_envelope["notification_eligible"] is not False
            or inner_envelope["execution_eligible"] is not False
        ):
            raise PromotedOperationalCloudJobError(_ERR_JOB)

        # The inner envelope is untrusted even once it is canonical JSON
        # with the exact key set -- every field that can survive into this
        # wrapper's own final stdout must additionally have an exact,
        # non-subclassed, canonical type/format before it is ever echoed.
        runtime_job_spec_id = inner_envelope["runtime_job_spec_id"]
        binding_id = inner_envelope["binding_id"]
        binding_generation = inner_envelope["binding_generation"]
        reused_existing_terminal = inner_envelope["reused_existing_terminal"]
        if (
            type(runtime_job_spec_id) is not str
            or _SHA256.fullmatch(runtime_job_spec_id) is None
            or type(binding_id) is not str
            or _SHA256.fullmatch(binding_id) is None
            or type(binding_generation) is not int
            or binding_generation <= 0
            or type(reused_existing_terminal) is not bool
        ):
            raise PromotedOperationalCloudJobError(_ERR_JOB)

        terminal = terminal_store.get(control.expected_operational_run_spec_id)
        if type(terminal) is not PromotedOperationalTerminalRecord:
            raise PromotedOperationalCloudJobError(_ERR_JOB)
        terminal.verify_content_identity()
        if (
            terminal.spec_id != inner_envelope["operational_run_spec_id"]
            or terminal.terminal_id != inner_envelope["terminal_id"]
            or terminal.status.value != inner_envelope["terminal_status"]
            or terminal.action.value != inner_envelope["action"]
            or terminal.advisory_id != inner_envelope["advisory_id"]
            or terminal.preparation_id != inner_envelope["preparation_id"]
            or list(terminal.failure_codes) != inner_envelope["failure_codes"]
            or terminal.target_session.isoformat() != inner_envelope["target_session"]
            or terminal.paper_only is not True
            or terminal.notification_eligible is not False
            or terminal.execution_eligible is not False
        ):
            raise PromotedOperationalCloudJobError(_ERR_JOB)

        publication = publish_promoted_operational_state_to_gcs(
            terminal=terminal,
            bucket=control.state_bucket,
            writer=writer,
            advisory_outbox=advisory_outbox,
            paper_ledger=paper_ledger,
        )
        if type(publication) is not CompletedPromotedOperationalGCSPublication:
            raise PromotedOperationalCloudJobError(_ERR_JOB)
        publication = CompletedPromotedOperationalGCSPublication(
            manifest=publication.manifest, manifest_object=publication.manifest_object
        )
        if (
            publication.manifest.bucket != control.state_bucket
            or publication.manifest.spec_id != terminal.spec_id
            or publication.manifest.preparation_id != terminal.preparation_id
            or publication.manifest.target_session != terminal.target_session
            or publication.manifest.terminal_id != terminal.terminal_id
            or publication.manifest.status is not terminal.status
            or publication.manifest.action is not terminal.action
            or publication.manifest.advisory_id != terminal.advisory_id
            or publication.manifest.paper_registration_id != terminal.paper_registration_id
        ):
            raise PromotedOperationalCloudJobError(_ERR_JOB)

        envelope = {
            "status": inner_envelope["status"],
            "assembly_spec_id": inner_envelope["assembly_spec_id"],
            "runtime_job_spec_id": inner_envelope["runtime_job_spec_id"],
            "operational_run_spec_id": terminal.spec_id,
            "preparation_id": terminal.preparation_id,
            "target_session": terminal.target_session.isoformat(),
            "terminal_id": terminal.terminal_id,
            "terminal_status": terminal.status.value,
            "action": terminal.action.value,
            "failure_codes": list(terminal.failure_codes),
            "advisory_id": terminal.advisory_id,
            "binding_id": inner_envelope["binding_id"],
            "binding_generation": inner_envelope["binding_generation"],
            "reused_existing_terminal": inner_envelope["reused_existing_terminal"],
            "paper_only": terminal.paper_only,
            "notification_eligible": terminal.notification_eligible,
            "execution_eligible": terminal.execution_eligible,
            "cloud_control_id": control.control_id,
            "state_publication_id": publication.manifest.publication_id,
            "state_manifest_object_name": publication.manifest_object.object_name,
            "state_manifest_generation": publication.manifest_object.generation,
            "state_manifest_sha256": publication.manifest_object.sha256,
            "state_manifest_byte_count": publication.manifest_object.byte_count,
        }
        print(
            json.dumps(
                envelope, allow_nan=False, ensure_ascii=False, separators=(",", ":"), sort_keys=True,
            )
        )
        return 0
    except Exception:
        print(
            json.dumps(
                {"error_type": PromotedOperationalCloudJobError.__name__, "status": "FAILED"},
                allow_nan=False,
                ensure_ascii=False,
                separators=(",", ":"),
                sort_keys=True,
            ),
            file=sys.stderr,
        )
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
