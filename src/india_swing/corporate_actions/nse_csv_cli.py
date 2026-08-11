"""Manual local importer for one exact NSE corporate-action CSV and master."""

from __future__ import annotations

import argparse
import json
from datetime import date, datetime
from pathlib import Path
from typing import Sequence

from india_swing.identity_decisions import (
    stable_instrument_id_for_isin,
    stable_listing_id_for_series,
)
from india_swing.reference_data.security_master import NseCmSecurityMasterParser

from .nse_csv import (
    NseCorporateActionListingBinding,
    import_nse_corporate_action_csv,
)
from .snapshot_store import LocalCorporateActionSnapshotStore


def _datetime(value: str) -> datetime:
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("timestamp must include an offset")
    return parsed


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Import one exact NSE corporate-action CSV without discovery."
    )
    parser.add_argument("--corporate-action-csv", type=Path, required=True)
    parser.add_argument("--security-master", type=Path, required=True)
    parser.add_argument("--observed-at", type=_datetime, required=True)
    parser.add_argument("--security-master-observed-at", type=_datetime, required=True)
    parser.add_argument("--cutoff", type=_datetime, required=True)
    parser.add_argument("--coverage-start", type=date.fromisoformat, required=True)
    parser.add_argument("--coverage-end", type=date.fromisoformat, required=True)
    parser.add_argument("--state-root", type=Path, required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    try:
        arguments = _parser().parse_args(argv)
        csv_path = arguments.corporate_action_csv.resolve(strict=True)
        master_path = arguments.security_master.resolve(strict=True)
        state_root = arguments.state_root.resolve(strict=True)
        source_bytes = csv_path.read_bytes()
        parsed_master = NseCmSecurityMasterParser().parse_bytes(
            master_path.read_bytes(),
            original_filename=master_path.name,
        )
        bindings = []
        for record in parsed_master.records:
            if (
                record.validated_isin is None
                or record.security_series not in {"EQ", "SM"}
            ):
                continue
            stable_instrument_id = stable_instrument_id_for_isin(
                record.validated_isin
            )
            bindings.append(
                NseCorporateActionListingBinding(
                    symbol=record.ticker_symbol,
                    series=record.security_series,
                    stable_instrument_id=stable_instrument_id,
                    stable_listing_id=stable_listing_id_for_series(
                        stable_instrument_id, record.security_series
                    ),
                    source_artifact_id=parsed_master.raw_sha256,
                    knowledge_time=arguments.security_master_observed_at,
                )
            )
        imported = import_nse_corporate_action_csv(
            source_bytes,
            observed_at=arguments.observed_at,
            cutoff=arguments.cutoff,
            coverage_start=arguments.coverage_start,
            coverage_end=arguments.coverage_end,
            listing_bindings=tuple(bindings),
        )
        stored = LocalCorporateActionSnapshotStore(state_root).put(imported.snapshot)
        if stored.snapshot_id != imported.snapshot.snapshot_id:
            raise ValueError("stored corporate-action snapshot differs")
        print(
            json.dumps(
                {
                    "status": "NSE_CORPORATE_ACTION_SNAPSHOT_READY",
                    "snapshot_id": stored.snapshot_id,
                    "source_sha256": imported.source_sha256,
                    "source_row_count": imported.source_row_count,
                    "imported_row_count": imported.imported_row_count,
                    "ignored_non_price_row_count": len(
                        imported.ignored_non_price_row_ids
                    ),
                    "ignored_out_of_scope_row_count": len(
                        imported.ignored_out_of_scope_row_ids
                    ),
                    "listing_binding_count": len(imported.listing_binding_ids),
                    "coverage_start": stored.coverage_start.isoformat(),
                    "coverage_end": stored.coverage_end.isoformat(),
                    "cutoff": stored.cutoff.isoformat(),
                },
                sort_keys=True,
                separators=(",", ":"),
            )
        )
        return 0
    except (KeyboardInterrupt, SystemExit):
        raise
    except Exception:
        print(
            json.dumps(
                {
                    "status": "FAILED",
                    "error_type": "NseCorporateActionCsvImportFailed",
                },
                sort_keys=True,
                separators=(",", ":"),
            )
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
