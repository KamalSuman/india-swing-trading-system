from __future__ import annotations

import csv
import gzip
import io
import json
import tempfile
import unittest
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

from india_swing.identity_registry import (
    POSITIVE_OBSERVATIONS_ONLY,
    CrossVintageIdentityRegistry,
    IdentityCandidateStatus,
    IdentityConflictType,
    IdentityRegistryConfig,
    IdentityRegistryIntegrityError,
    LocalIdentityRegistryStore,
    encode_identity_registry,
    materialize_cross_vintage_identity_registry,
)
from india_swing.identity_registry.cli import main as identity_registry_main
from india_swing.reference_data.artifact_store import LocalReferenceArtifactStore
from india_swing.reference_data.security_master import NSE_CM_MII_SECURITY_HEADER


UTC = timezone.utc
DAY_ONE_FIRST_SEEN = datetime(2026, 7, 15, 12, 0, tzinfo=UTC)
DAY_ONE_VALIDATED = DAY_ONE_FIRST_SEEN + timedelta(seconds=2)
DAY_TWO_FIRST_SEEN = datetime(2026, 7, 16, 10, 0, tzinfo=UTC)
DAY_TWO_VALIDATED = DAY_TWO_FIRST_SEEN + timedelta(seconds=2)
CUTOFF = datetime(2026, 7, 16, 10, 5, tzinfo=UTC)


def security_row(**overrides: str) -> list[str]:
    values = {name: "" for name in NSE_CM_MII_SECURITY_HEADER}
    values.update(
        {
            "FinInstrmId": "1594",
            "TckrSymb": "INFY",
            "SctySrs": "EQ",
            "FinInstrmNm": "INFOSYS LIMITED",
            "ISIN": "INE009A01021",
            "NewBrdLotQty": "1",
            "ParVal": "500",
            "SctyTpFlg": "0",
            "BidIntrvl": "5",
            "TrckgInd": "0",
            "CallAuctnInd": "1",
            "PrtdToTrad": "0",
            "PricRg": "0.00-99999.00",
            "SctyStsNrmlMkt": "6",
            "ElgbltyNrmlMkt": "1",
            "SctyStsOddLotMkt": "2",
            "ElgbltyOddLotMkt": "1",
            "SctyStsRETDBTMkt": "2",
            "ElgbltyRETDBTMkt": "0",
            "SctyStsAuctnMkt": "2",
            "ElgbltyAuctnMkt": "1",
            "SctyStsAddtlMkt1": "1",
            "ElgbltyAddtlMkt1": "0",
            "SctyStsAddtlMkt2": "1",
            "ElgbltyAddtlMkt2": "0",
            "ListgDt": "476668800",
            "RmvlDt": "0",
            "RadmssnDt": "0",
            "DelFlg": "N",
        }
    )
    values.update(overrides)
    return [values[name] for name in NSE_CM_MII_SECURITY_HEADER]


def master_bytes(rows: list[list[str]]) -> bytes:
    stream = io.StringIO(newline="")
    writer = csv.writer(stream, lineterminator="\n")
    writer.writerow(NSE_CM_MII_SECURITY_HEADER)
    writer.writerows(rows)
    return gzip.compress(stream.getvalue().encode("utf-8"), mtime=0)


def clock_sequence(*values: datetime):
    iterator = iter(values)
    return lambda: next(iterator)


def tcs_row(**overrides: str) -> list[str]:
    values = {
        "FinInstrmId": "11536",
        "TckrSymb": "TCS",
        "FinInstrmNm": "TCS LIMITED",
        "ISIN": "INE467B01029",
    }
    values.update(overrides)
    return security_row(**values)


class IdentityRegistryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.reference_root = self.root / "reference"
        self.identity_root = self.root / "identity"

    def tearDown(self) -> None:
        self.temp.cleanup()

    def import_sources(
        self,
        first_rows: list[list[str]],
        second_rows: list[list[str]],
    ):
        first_file = self.root / "NSE_CM_security_15072026.csv.gz"
        second_file = self.root / "NSE_CM_security_16072026.csv.gz"
        first_file.write_bytes(master_bytes(first_rows))
        second_file.write_bytes(master_bytes(second_rows))
        store = LocalReferenceArtifactStore(
            self.reference_root,
            clock=clock_sequence(
                DAY_ONE_FIRST_SEEN,
                DAY_ONE_VALIDATED,
                DAY_TWO_FIRST_SEEN,
                DAY_TWO_VALIDATED,
            ),
        )
        return (
            store.import_security_master(first_file),
            store.import_security_master(second_file),
        )

    def registry(
        self,
        first_rows: list[list[str]],
        second_rows: list[list[str]],
        *,
        cutoff: datetime = CUTOFF,
    ) -> CrossVintageIdentityRegistry:
        return materialize_cross_vintage_identity_registry(
            sources=self.import_sources(first_rows, second_rows),
            cutoff=cutoff,
        )

    def test_same_validated_isin_creates_candidate_not_stable_identity(self) -> None:
        registry = self.registry(
            [security_row()],
            [security_row(TckrSymb="INFYNEW", FinInstrmId="2000")],
        )

        self.assertEqual(len(registry.observations), 2)
        self.assertEqual(len(registry.candidates), 1)
        candidate = registry.candidates[0]
        self.assertIs(candidate.status, IdentityCandidateStatus.CANDIDATE_CONTINUITY)
        self.assertFalse(hasattr(candidate, "instrument_id"))
        self.assertFalse(hasattr(candidate, "listing_id"))
        self.assertEqual(len(registry.transitions), 1)
        transition = registry.transitions[0]
        self.assertTrue(transition.symbol_changed)
        self.assertTrue(transition.financial_instrument_id_changed)
        self.assertFalse(transition.series_changed)
        self.assertFalse(registry.actionable)

    def test_source_padded_instrument_name_is_preserved(self) -> None:
        registry = self.registry(
            [security_row(FinInstrmNm="INFOSYS LIMITED ")],
            [security_row(FinInstrmNm="INFOSYS LIMITED")],
        )

        self.assertEqual(
            {value.instrument_name for value in registry.observations},
            {"INFOSYS LIMITED ", "INFOSYS LIMITED"},
        )
        self.assertTrue(registry.transitions[0].instrument_name_changed)

    def test_concurrent_series_are_distinct_listing_lanes_not_conflicts(self) -> None:
        registry = self.registry(
            [security_row(), security_row(SctySrs="BE", FinInstrmId="1595")],
            [security_row(), security_row(SctySrs="BE", FinInstrmId="1595")],
        )

        self.assertEqual(len(registry.candidates), 1)
        self.assertIs(
            registry.candidates[0].status,
            IdentityCandidateStatus.CANDIDATE_CONTINUITY,
        )
        self.assertEqual(registry.conflicts, ())
        self.assertEqual(len(registry.transitions), 2)
        self.assertEqual(
            {value.series_changed for value in registry.transitions},
            {False},
        )

    def test_duplicate_series_within_isin_vintage_is_a_conflict(self) -> None:
        registry = self.registry(
            [
                security_row(),
                security_row(TckrSymb="INFYALT", FinInstrmId="1595"),
            ],
            [security_row()],
        )

        self.assertEqual(
            {value.conflict_type for value in registry.conflicts},
            {IdentityConflictType.DUPLICATE_SERIES_WITHIN_ISIN_VINTAGE},
        )
        self.assertIs(registry.candidates[0].status, IdentityCandidateStatus.CONFLICT)
        self.assertEqual(registry.transitions, ())

    def test_unvalidated_identifier_never_links_across_vintages(self) -> None:
        registry = self.registry(
            [security_row(ISIN="UNVALIDATED1")],
            [security_row(ISIN="UNVALIDATED1")],
        )

        self.assertEqual(len(registry.candidates), 2)
        self.assertEqual(
            {value.status for value in registry.candidates},
            {IdentityCandidateStatus.UNRESOLVED_IDENTIFIER},
        )
        self.assertEqual(registry.transitions, ())

    def test_reused_ticker_with_new_isin_is_a_conflict_not_a_join(self) -> None:
        registry = self.registry(
            [security_row()],
            [security_row(ISIN="INE001A01036")],
        )

        self.assertEqual(len(registry.candidates), 2)
        self.assertEqual(
            {value.status for value in registry.candidates},
            {IdentityCandidateStatus.CONFLICT},
        )
        self.assertEqual(
            {value.conflict_type for value in registry.conflicts},
            {
                IdentityConflictType.FINANCIAL_ID_REUSED_ACROSS_IDENTIFIERS,
                IdentityConflictType.LISTING_KEY_REUSED_ACROSS_IDENTIFIERS,
            },
        )
        self.assertEqual(registry.transitions, ())

    def test_financial_id_reuse_is_detected_even_when_symbol_changes(self) -> None:
        registry = self.registry(
            [security_row()],
            [security_row(TckrSymb="OTHER", ISIN="INE001A01036")],
        )

        self.assertEqual(
            {value.conflict_type for value in registry.conflicts},
            {IdentityConflictType.FINANCIAL_ID_REUSED_ACROSS_IDENTIFIERS},
        )

    def test_absence_is_not_interpreted_as_delisting(self) -> None:
        registry = self.registry(
            [security_row(), tcs_row()],
            [security_row()],
        )
        tcs_observation = next(
            value for value in registry.observations if value.ticker_symbol == "TCS"
        )
        candidate = registry.candidate_for_observation(tcs_observation.observation_id)

        self.assertIs(candidate.status, IdentityCandidateStatus.SINGLE_VINTAGE)
        self.assertEqual(registry.coverage_scope, POSITIVE_OBSERVATIONS_ONLY)
        self.assertFalse(hasattr(candidate, "delisted"))
        self.assertFalse(hasattr(registry, "delisted_instruments"))

    def test_cutoff_excludes_sources_not_yet_known(self) -> None:
        sources = self.import_sources([security_row()], [security_row()])
        with self.assertRaisesRegex(IdentityRegistryIntegrityError, "known after"):
            materialize_cross_vintage_identity_registry(
                sources=sources,
                cutoff=DAY_TWO_FIRST_SEEN,
            )

    def test_duplicate_source_artifact_is_rejected(self) -> None:
        first, _ = self.import_sources([security_row()], [security_row()])
        with self.assertRaisesRegex(IdentityRegistryIntegrityError, "duplicate"):
            materialize_cross_vintage_identity_registry(
                sources=(first, first),
                cutoff=CUTOFF,
            )

    def test_equivalent_timezone_cutoffs_have_one_identity(self) -> None:
        sources = self.import_sources([security_row()], [security_row()])
        utc_registry = materialize_cross_vintage_identity_registry(
            sources=sources,
            cutoff=CUTOFF,
        )
        ist_registry = materialize_cross_vintage_identity_registry(
            sources=sources,
            cutoff=CUTOFF.astimezone(timezone(timedelta(hours=5, minutes=30))),
        )

        self.assertEqual(utc_registry, ist_registry)
        self.assertEqual(
            encode_identity_registry(utc_registry),
            encode_identity_registry(ist_registry),
        )

    def test_nested_mutation_is_detected(self) -> None:
        registry = self.registry([security_row()], [security_row()])
        object.__setattr__(registry.observations[0], "ticker_symbol", "FORGED")

        with self.assertRaisesRegex(IdentityRegistryIntegrityError, "observation"):
            registry.verify_content_identity()

    def test_store_replays_sources_and_rejects_extra_files(self) -> None:
        registry = self.registry([security_row()], [security_row()])
        store = LocalIdentityRegistryStore(self.identity_root, self.reference_root)

        stored = store.put(registry)
        loaded = store.get(registry.registry_id)
        self.assertEqual(stored.registry, loaded.registry)
        self.assertEqual(stored.payload_bytes, loaded.payload_bytes)
        self.assertEqual(
            {value.name for value in stored.path.iterdir()},
            {"manifest.json", "registry.json"},
        )

        (stored.path / "extra.txt").write_text("unexpected", encoding="utf-8")
        with self.assertRaisesRegex(IdentityRegistryIntegrityError, "file set"):
            store.get(registry.registry_id)

    def test_store_rejects_payload_tampering(self) -> None:
        registry = self.registry([security_row()], [security_row()])
        store = LocalIdentityRegistryStore(self.identity_root, self.reference_root)
        stored = store.put(registry)
        (stored.path / "registry.json").write_bytes(b"{}")

        with self.assertRaisesRegex(IdentityRegistryIntegrityError, "payload"):
            store.get(registry.registry_id)

    def test_collection_only_contract_cannot_be_promoted(self) -> None:
        registry = self.registry([security_row()], [security_row()])
        with self.assertRaisesRegex(ValueError, "collection-only"):
            replace(registry, actionable=True)

    def test_config_uses_explicit_root_and_safe_default(self) -> None:
        self.assertEqual(
            IdentityRegistryConfig.from_env({}).data_root,
            Path("var/identity_registry"),
        )
        self.assertEqual(
            IdentityRegistryConfig.from_env(
                {"INDIA_SWING_IDENTITY_REGISTRY_ROOT": str(self.identity_root)}
            ).data_root,
            self.identity_root,
        )

    def test_cli_materializes_sealed_registry(self) -> None:
        sources = self.import_sources([security_row()], [security_row()])
        with patch.dict(
            "os.environ",
            {
                "INDIA_SWING_REFERENCE_DATA_ROOT": str(self.reference_root),
                "INDIA_SWING_IDENTITY_REGISTRY_ROOT": str(self.identity_root),
            },
            clear=False,
        ):
            exit_code = identity_registry_main(
                [
                    "materialize",
                    "--security-master-id",
                    sources[0].manifest.artifact_id,
                    "--security-master-id",
                    sources[1].manifest.artifact_id,
                    "--cutoff",
                    CUTOFF.isoformat(),
                ]
            )
        self.assertEqual(exit_code, 0)
        manifests = tuple(self.identity_root.rglob("manifest.json"))
        self.assertEqual(len(manifests), 1)
        value = json.loads(manifests[0].read_text(encoding="utf-8"))
        self.assertEqual(value["observation_count"], 2)


if __name__ == "__main__":
    unittest.main()
