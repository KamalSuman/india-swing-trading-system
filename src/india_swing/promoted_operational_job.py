"""First production-shaped, paper-only process entrypoint for the promoted
operational job.

Accepts one exact local assembly-spec file plus explicit durable roots,
deterministically assembles the already-approved pinned preparation and
portfolio artifacts (via ``promoted_operational_assembly.py``, unchanged),
constructs a Kite quote source and one shared injected GCS client, invokes
the accepted promoted operational runtime (``promoted_operational_runtime.py``,
unchanged) exactly once, and emits one small sanitized audit result. This
module never reproduces any financial calculation, risk rule, sequencing,
or persistence codec already owned by the accepted layers -- it is only
the missing process composition root.

Exposes four small injectable seams (``clock``, ``kite_adapter_factory``,
``gcs_client_factory``, ``runtime_callable``) so tests can exercise the
full composition without monkeypatching SDK modules or touching network/
GCP; production defaults are resolved inside ``main`` at call time, never
at function-definition time. This module never sends Telegram output,
never places an order, never performs interactive/browser login or token
refresh, and never deploys anything -- deployment remains a later,
separately reviewed increment.
"""

from __future__ import annotations

import json
import os
import sys
from collections.abc import Callable, Mapping, Sequence
from datetime import datetime, timezone
from pathlib import Path

from india_swing.daily_pipeline.state_publication import GoogleCloudStorageStateObjectWriter
from india_swing.market_data.config import KiteCredentials
from india_swing.market_data.kite import KiteMarketDataAdapter
from india_swing.operations.portfolio_store import LocalSwingPortfolioArtifactStore
from india_swing.operations.runner import KiteSwingQuoteSource
from india_swing.paper_trades.store import LocalPaperTradeLedger
from india_swing.promoted_operational_assembly import (
    PromotedOperationalRuntimeAssembly,
    assemble_promoted_operational_runtime_inputs,
    load_promoted_operational_assembly_spec_file,
)
from india_swing.promoted_operational_persistence import (
    LocalPromotedOperationalAdvisoryOutbox,
    LocalPromotedOperationalTerminalStore,
    verify_promoted_operational_published_bundle,
)
from india_swing.promoted_operational_preparation import (
    build_promoted_operational_preparation_store,
)
from india_swing.promoted_operational_runtime import (
    PinnedPromotedOperationalPortfolioSource,
    PromotedOperationalRuntimeState,
    run_promoted_operational_runtime_job,
)
from india_swing.promoted_terminal_binding import (
    build_promoted_operational_terminal_binding_record,
    promoted_operational_terminal_binding_object_name,
)
from india_swing.promoted_terminal_binding_control_plane import (
    GoogleCloudStorageTerminalBindingReader,
)


class PromotedOperationalJobError(ValueError):
    pass


_ERR_JOB = "promoted operational job call is invalid"

_OPTIONS = (
    "--assembly-spec-file",
    "--reference-root",
    "--identity-evidence-root",
    "--calendar-root",
    "--daily-reports-root",
    "--historical-corpus-root",
    "--promoted-root",
    "--graph-publication-root",
    "--engine-run-root",
    "--research-run-root",
    "--operational-preparation-root",
    "--portfolio-artifact-root",
    "--state-root",
)


def _arguments(argv: Sequence[str]) -> dict[str, Path]:
    values: dict[str, str] = {}
    index = 0
    while index < len(argv):
        token = argv[index]
        if token not in _OPTIONS or token in values:
            raise PromotedOperationalJobError(_ERR_JOB)
        if index + 1 >= len(argv):
            raise PromotedOperationalJobError(_ERR_JOB)
        value = argv[index + 1]
        if type(value) is not str or not value:
            raise PromotedOperationalJobError(_ERR_JOB)
        values[token] = value
        index += 2
    if set(values) != set(_OPTIONS):
        raise PromotedOperationalJobError(_ERR_JOB)

    paths: dict[str, Path] = {}
    for token, raw in values.items():
        path = Path(raw)
        if not path.is_absolute() or ".." in path.parts:
            raise PromotedOperationalJobError(_ERR_JOB)
        paths[token] = path
    return paths


def _default_clock() -> datetime:
    return datetime.now(timezone.utc)


def _default_kite_adapter_factory(
    credentials: KiteCredentials, clock: Callable[[], datetime]
) -> KiteMarketDataAdapter:
    return KiteMarketDataAdapter.from_official_sdk(credentials, clock=clock)


def _default_gcs_client_factory() -> object:
    from google.cloud import storage

    return storage.Client()


def _verified_success_envelope(
    *, assembly: PromotedOperationalRuntimeAssembly, result: object
) -> dict[str, object]:
    """Independently verify an untrusted runtime result before ever
    serializing it. A malformed/malicious injected ``result`` (wrong
    type, mismatched job-spec identity, a self-consistently rewritten
    published artifact, or internally incoherent binding fields) must
    never leak a foreign exception or produce a false success -- every
    nested access, public verifier/builder call, and envelope
    construction happens inside one sanitized boundary.

    Trusting selected cross-links or individual content identities alone
    is insufficient: independently rewritten terminal and advisory
    records can each be internally self-consistent while disagreeing
    with one another. A stale single-field tamper must also be rejected by
    replaying the affected record's identity. This function therefore
    replays the *complete* published bundle via the accepted
    ``verify_promoted_operational_published_bundle``, and independently
    rebuilds the *expected* binding record from the terminal and the
    assembled run spec via the accepted
    ``build_promoted_operational_terminal_binding_record`` rather than
    trusting the returned binding record's selected fields alone."""

    if type(result) is not PromotedOperationalRuntimeState:
        raise PromotedOperationalJobError(_ERR_JOB)

    check_failed = False
    mismatch = True
    envelope: dict[str, object] | None = None
    try:
        result.job_spec.verify_content_identity()
        published = result.anchored.published
        terminal = published.terminal
        binding_record = result.anchored.binding_record

        verify_promoted_operational_published_bundle(
            terminal, published.advisory, published.paper_registration
        )
        binding_record.verify_content_identity()
        expected_binding_record = build_promoted_operational_terminal_binding_record(
            terminal, assembly.run_spec
        )
        expected_binding_object_name = promoted_operational_terminal_binding_object_name(
            assembly.run_spec
        )

        mismatch = (
            result.job_spec.job_spec_id != assembly.runtime_job_spec.job_spec_id
            or terminal.spec_id != assembly.run_spec.spec_id
            or terminal.preparation_id != assembly.assembly_spec.preparation_id
            or terminal.target_session != assembly.assembly_spec.target_session
            or terminal.paper_only is not True
            or terminal.notification_eligible is not False
            or terminal.execution_eligible is not False
            or binding_record != expected_binding_record
            or result.anchored.binding_object_name != expected_binding_object_name
            or result.anchored.binding_bucket != assembly.runtime_job_spec.binding_bucket
            or published.reused_existing_terminal is not result.anchored.reused_existing_terminal
            or type(result.anchored.binding_generation) is not int
            or result.anchored.binding_generation <= 0
            or type(result.anchored.reused_existing_terminal) is not bool
        )
        if not mismatch:
            envelope = {
                "status": "PROMOTED_OPERATIONAL_JOB_COMPLETE",
                "assembly_spec_id": assembly.assembly_spec.assembly_spec_id,
                "runtime_job_spec_id": result.job_spec.job_spec_id,
                "operational_run_spec_id": terminal.spec_id,
                "preparation_id": terminal.preparation_id,
                "target_session": terminal.target_session.isoformat(),
                "terminal_id": terminal.terminal_id,
                "terminal_status": terminal.status.value,
                "action": terminal.action.value,
                "failure_codes": list(terminal.failure_codes),
                "advisory_id": terminal.advisory_id,
                "binding_id": binding_record.binding_id,
                "binding_generation": result.anchored.binding_generation,
                "reused_existing_terminal": result.anchored.reused_existing_terminal,
                "paper_only": terminal.paper_only,
                "notification_eligible": terminal.notification_eligible,
                "execution_eligible": terminal.execution_eligible,
            }
    except Exception:
        check_failed = True
    if check_failed or mismatch or envelope is None:
        raise PromotedOperationalJobError(_ERR_JOB)
    return envelope


def main(
    argv: Sequence[str] | None = None,
    *,
    environ: Mapping[str, str] | None = None,
    clock: Callable[[], datetime] | None = None,
    kite_adapter_factory: (
        Callable[[KiteCredentials, Callable[[], datetime]], object] | None
    ) = None,
    gcs_client_factory: Callable[[], object] | None = None,
    runtime_callable: Callable[..., object] | None = None,
) -> int:
    args = list(argv) if argv is not None else sys.argv[1:]
    try:
        paths = _arguments(args)

        assembly_spec = load_promoted_operational_assembly_spec_file(
            paths["--assembly-spec-file"]
        )

        _, preparations = build_promoted_operational_preparation_store(
            reference_root=paths["--reference-root"],
            identity_evidence_root=paths["--identity-evidence-root"],
            calendar_root=paths["--calendar-root"],
            daily_reports_root=paths["--daily-reports-root"],
            historical_corpus_root=paths["--historical-corpus-root"],
            promoted_root=paths["--promoted-root"],
            graph_publication_root=paths["--graph-publication-root"],
            engine_run_root=paths["--engine-run-root"],
            research_run_root=paths["--research-run-root"],
            operational_preparation_root=paths["--operational-preparation-root"],
        )
        portfolio_artifacts = LocalSwingPortfolioArtifactStore(
            paths["--portfolio-artifact-root"]
        )

        assembly = assemble_promoted_operational_runtime_inputs(
            spec=assembly_spec,
            preparation_resolver=preparations,
            portfolio_artifact_resolver=portfolio_artifacts,
        )

        # Resolve every production default up front and require each is
        # callable before invoking any of them or constructing any
        # GCS/state-root adapter -- a non-callable injected dependency is
        # a local configuration failure and must never reach the cloud
        # boundary. This checks capability only; it never calls the clock
        # or the runtime.
        active_clock = clock if clock is not None else _default_clock
        active_kite_adapter_factory = (
            kite_adapter_factory
            if kite_adapter_factory is not None
            else _default_kite_adapter_factory
        )
        active_gcs_client_factory = (
            gcs_client_factory if gcs_client_factory is not None else _default_gcs_client_factory
        )
        active_runtime_callable = (
            runtime_callable if runtime_callable is not None else run_promoted_operational_runtime_job
        )
        if not (
            callable(active_clock)
            and callable(active_kite_adapter_factory)
            and callable(active_gcs_client_factory)
            and callable(active_runtime_callable)
        ):
            raise PromotedOperationalJobError(_ERR_JOB)

        runtime_environ = os.environ if environ is None else environ
        credentials = KiteCredentials.from_env(runtime_environ)

        adapter = active_kite_adapter_factory(credentials, active_clock)
        quote_source = KiteSwingQuoteSource(adapter)
        if quote_source.source_id != assembly.runtime_job_spec.expected_quote_source_id:
            raise PromotedOperationalJobError(_ERR_JOB)

        client = active_gcs_client_factory()
        if client is None:
            raise PromotedOperationalJobError(_ERR_JOB)
        writer = GoogleCloudStorageStateObjectWriter(client=client)
        reader = GoogleCloudStorageTerminalBindingReader(client)

        state_root = paths["--state-root"]
        advisory_outbox = LocalPromotedOperationalAdvisoryOutbox(state_root / "advisory")
        terminal_store = LocalPromotedOperationalTerminalStore(state_root / "terminal")
        paper_ledger = LocalPaperTradeLedger(state_root / "paper")
        portfolio_source = PinnedPromotedOperationalPortfolioSource(assembly.portfolio_context)

        result = active_runtime_callable(
            job_spec=assembly.runtime_job_spec,
            run_spec=assembly.run_spec,
            quote_source=quote_source,
            portfolio_source=portfolio_source,
            clock=active_clock,
            advisory_outbox=advisory_outbox,
            terminal_store=terminal_store,
            paper_ledger=paper_ledger,
            binding_writer=writer,
            binding_reader=reader,
            binding_preflight=reader,
        )

        envelope = _verified_success_envelope(assembly=assembly, result=result)
        print(
            json.dumps(
                envelope,
                allow_nan=False,
                ensure_ascii=False,
                separators=(",", ":"),
                sort_keys=True,
            )
        )
        return 0
    except Exception:
        print(
            json.dumps(
                {"error_type": PromotedOperationalJobError.__name__, "status": "FAILED"},
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
