"""Offline CLI for the promoted operational launch-spec preparer.

Reads one explicit local launch-request file plus the ten explicit
promoted-preparation roots and the portfolio-artifact root, resolves the
exact preparation and portfolio artifact, derives and dry-assembles the
canonical ``PromotedOperationalAssemblySpec``, publishes it to one
explicit local output file (create-once), and prints one small sanitized
audit summary. The produced file is exactly what
``india-swing-promoted-operational-job``'s ``--assembly-spec-file``
option consumes.

This CLI never reads an environment variable or the current time, and
never constructs a Kite/GCP/Telegram/network/broker/runtime client --
this preparer is entirely offline.
"""

from __future__ import annotations

import json
import sys
from collections.abc import Sequence
from pathlib import Path

from india_swing._filesystem import read_stable_regular_file
from india_swing.operations.portfolio_store import LocalSwingPortfolioArtifactStore
from india_swing.promoted_operational_launch import (
    MAXIMUM_LAUNCH_REQUEST_BYTES,
    PromotedOperationalLaunchError,
    canonical_utc_z_timestamp,
    decode_promoted_operational_launch_request,
    prepare_promoted_operational_launch,
    publish_promoted_operational_launch_assembly_spec_file,
)
from india_swing.promoted_operational_preparation import (
    build_promoted_operational_preparation_store,
)

_ERR_LAUNCH = "promoted operational launch call is invalid"

_ROOT_OPTIONS = (
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
)
_OPTIONS = (
    ("--launch-request-file",)
    + _ROOT_OPTIONS
    + ("--portfolio-artifact-root", "--output-assembly-spec-file")
)


def _arguments(argv: Sequence[str]) -> dict[str, Path]:
    values: dict[str, str] = {}
    index = 0
    while index < len(argv):
        token = argv[index]
        if token not in _OPTIONS or token in values:
            raise PromotedOperationalLaunchError(_ERR_LAUNCH)
        if index + 1 >= len(argv):
            raise PromotedOperationalLaunchError(_ERR_LAUNCH)
        value = argv[index + 1]
        if type(value) is not str or not value:
            raise PromotedOperationalLaunchError(_ERR_LAUNCH)
        values[token] = value
        index += 2
    if set(values) != set(_OPTIONS):
        raise PromotedOperationalLaunchError(_ERR_LAUNCH)

    paths: dict[str, Path] = {}
    for token, raw in values.items():
        path = Path(raw)
        if not path.is_absolute() or ".." in path.parts:
            raise PromotedOperationalLaunchError(_ERR_LAUNCH)
        paths[token] = path
    return paths


def main(argv: Sequence[str] | None = None) -> int:
    args = list(argv) if argv is not None else sys.argv[1:]
    try:
        paths = _arguments(args)

        payload = read_stable_regular_file(
            paths["--launch-request-file"], maximum_bytes=MAXIMUM_LAUNCH_REQUEST_BYTES
        )
        request = decode_promoted_operational_launch_request(payload)

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

        assembly = prepare_promoted_operational_launch(
            request=request,
            preparation_resolver=preparations,
            portfolio_artifact_resolver=portfolio_artifacts,
        )
        spec = assembly.assembly_spec

        publish_promoted_operational_launch_assembly_spec_file(
            paths["--output-assembly-spec-file"], spec
        )

        envelope = {
            "status": "PROMOTED_OPERATIONAL_LAUNCH_READY",
            "assembly_spec_id": spec.assembly_spec_id,
            "preparation_id": spec.preparation_id,
            "portfolio_artifact_id": spec.portfolio_artifact_id,
            "portfolio_snapshot_id": spec.expected_portfolio_snapshot_id,
            "target_session": spec.target_session.isoformat(),
            "decision_not_before": canonical_utc_z_timestamp(spec.decision_not_before),
            "decision_deadline": canonical_utc_z_timestamp(spec.decision_deadline),
            "quote_gate_policy_id": spec.quote_gate_policy.policy_id,
            "allocation_policy_id": spec.allocation_policy.allocation_policy_id,
            "expected_quote_source_id": spec.expected_quote_source_id,
            "candidate_count": len(assembly.preparation.candidates),
            "open_position_count": len(spec.open_listing_keys),
            "paper_only": spec.paper_only,
            "notification_eligible": spec.notification_eligible,
            "execution_eligible": spec.execution_eligible,
        }
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
                {"error_type": PromotedOperationalLaunchError.__name__, "status": "FAILED"},
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
