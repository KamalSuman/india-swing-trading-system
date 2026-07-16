from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Sequence

from india_swing.identity_registry import (
    IdentityRegistryConfig,
    LocalIdentityAdjudicationQueueStore,
    LocalIdentityRegistryStore,
)
from india_swing.reference_data.config import ReferenceDataConfig

from .artifact_store import LocalIdentityEvidenceArtifactStore
from .config import IdentityEvidenceConfig
from .coverage import build_identity_evidence_coverage


class IdentityEvidenceArgumentError(ValueError):
    pass


class SanitizedArgumentParser(argparse.ArgumentParser):
    def error(self, message: str) -> None:
        raise IdentityEvidenceArgumentError("invalid identity-evidence arguments")


def parser() -> argparse.ArgumentParser:
    root = SanitizedArgumentParser(
        description="Archive collection-only official NSE identity/lifecycle evidence"
    )
    commands = root.add_subparsers(dest="command", required=True)
    source_import = commands.add_parser("import", help="archive one exact NSE source and declaration")
    source_import.add_argument("--source", type=Path, required=True)
    source_import.add_argument("--declaration", type=Path, required=True)
    show = commands.add_parser("show", help="show one evidence artifact summary")
    show.add_argument("--evidence-id", required=True)
    commands.add_parser("list", help="list evidence artifact summaries")
    coverage = commands.add_parser("coverage", help="report collected evidence against one persisted queue")
    coverage.add_argument("--registry-id", required=True)
    coverage.add_argument("--evidence-id", action="append", default=[], dest="evidence_ids")
    return root


def _artifact_summary(value: object) -> dict[str, object]:
    from .models import StoredIdentityEvidenceArtifact

    if type(value) is not StoredIdentityEvidenceArtifact:
        raise TypeError("evidence summary requires an exact stored artifact")
    return {
        "evidence_id": value.manifest.artifact_id,
        "manifest_id": value.manifest.manifest_id,
        "source_kind": value.manifest.source_kind.value,
        "claimed_document_id": value.manifest.claimed_document_id,
        "claimed_issue_date": value.manifest.claimed_issue_date.isoformat(),
        "knowledge_time": value.manifest.validated_at.isoformat(),
        "claim_count": len(value.manifest.claim_ids),
        "readiness": value.manifest.readiness.value,
        "actionable": value.manifest.actionable,
        "stable_identity_assigned": value.manifest.stable_identity_assigned,
    }


def main(argv: Sequence[str] | None = None) -> int:
    try:
        args = parser().parse_args(argv)
        evidence_store = LocalIdentityEvidenceArtifactStore(IdentityEvidenceConfig.from_env().data_root)
        if args.command == "import":
            response = {"status": "COMPLETE", "kind": "IDENTITY_EVIDENCE_ARTIFACT", **_artifact_summary(
                evidence_store.import_source(args.source, args.declaration)
            )}
        elif args.command == "show":
            response = {"status": "COMPLETE", "kind": "IDENTITY_EVIDENCE_ARTIFACT", **_artifact_summary(
                evidence_store.get(args.evidence_id)
            )}
        elif args.command == "list":
            response = {
                "status": "COMPLETE", "kind": "IDENTITY_EVIDENCE_ARTIFACT_LIST",
                "artifacts": [_artifact_summary(value) for value in evidence_store.list_artifacts()],
            }
        else:
            identity_config = IdentityRegistryConfig.from_env()
            registry_store = LocalIdentityRegistryStore(
                identity_config.data_root, ReferenceDataConfig.from_env().data_root
            )
            queue_store = LocalIdentityAdjudicationQueueStore(identity_config.data_root, registry_store)
            queue = queue_store.get(args.registry_id)
            report = build_identity_evidence_coverage(
                queue, tuple(evidence_store.get(value) for value in args.evidence_ids)
            )
            response = {
                "status": "COMPLETE", "kind": "IDENTITY_EVIDENCE_COVERAGE",
                "report_id": report.report_id, "queue_id": report.queue_id,
                "registry_id": report.source_registry_id,
                "evidence_artifact_count": len(report.evidence_artifact_ids),
                "required_pair_count": report.required_pair_count,
                "evidence_collected_pair_count": report.evidence_collected_pair_count,
                "missing_pair_count": report.missing_pair_count,
                "requirement_counts": report.requirement_counts,
                "readiness": report.readiness.value, "actionable": report.actionable,
                "stable_identity_assigned": report.stable_identity_assigned,
                "requirements_satisfied": report.requirements_satisfied,
            }
    except Exception as exc:
        print(json.dumps({"status": "FAILED", "error_type": type(exc).__name__}, sort_keys=True), file=sys.stderr)
        return 2
    print(json.dumps(response, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
