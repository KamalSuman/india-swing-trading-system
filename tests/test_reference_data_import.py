from __future__ import annotations

import csv
import gzip
import io
import json
import os
import subprocess
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

from india_swing.reference.models import ReferenceReadiness
from india_swing.reference_data.artifact_store import LocalReferenceArtifactStore
from india_swing.reference_data.cli import main as reference_data_main
from india_swing.reference_data.models import (
    AcquisitionMode,
    NSE_CM_SECURITY_DATASET,
    ReferenceArtifactConflict,
    ReferenceArtifactIntegrityError,
    ReferenceArtifactNotFound,
    ReferenceArtifactStale,
    ReferenceArtifactUnverifiedReportDate,
    SourceRowDisposition,
)
from india_swing.reference_data.security_master import (
    NSE_CM_MII_SECURITY_HEADER,
    NseCmSecurityMasterParser,
)


UTC = timezone.utc
FIRST_SEEN = datetime(2026, 7, 15, 12, 0, tzinfo=UTC)
VALIDATED = FIRST_SEEN + timedelta(seconds=2)
FILENAME = "NSE_CM_security_15072026.csv.gz"


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


def security_master_bytes(
    rows: list[list[str]] | None = None,
    *,
    header: list[str] | None = None,
) -> bytes:
    stream = io.StringIO(newline="")
    writer = csv.writer(stream, lineterminator="\n")
    writer.writerow(list(NSE_CM_MII_SECURITY_HEADER) if header is None else header)
    writer.writerows(rows or [security_row()])
    return gzip.compress(stream.getvalue().encode("utf-8"), mtime=0)


def clock_sequence(*values: datetime):
    iterator = iter(values)
    return lambda: next(iterator)


class NseCmSecurityMasterParserTests(unittest.TestCase):
    def test_current_schema_has_the_expected_120_iso_tag_columns(self) -> None:
        self.assertEqual(len(NSE_CM_MII_SECURITY_HEADER), 120)
        self.assertEqual(NSE_CM_MII_SECURITY_HEADER[:5], (
            "FinInstrmId",
            "TckrSymb",
            "SctySrs",
            "FinInstrmNm",
            "ISIN",
        ))

    def test_parser_preserves_every_row_and_assigns_one_disposition(self) -> None:
        rows = [
            security_row(),
            security_row(
                FinInstrmId="2000",
                TckrSymb="PREFCO",
                SctySrs="P1",
                SctyTpFlg="1",
                ISIN="INE000A01001",
            ),
            security_row(
                FinInstrmId="2001",
                TckrSymb="011NSETEST",
                ISIN="DUMMYSAN005",
            ),
            security_row(
                FinInstrmId="2002",
                TckrSymb="BSEONLY",
                PrtdToTrad="2",
                ISIN="INE000A01002",
            ),
        ]

        parsed = NseCmSecurityMasterParser().parse_bytes(
            security_master_bytes(rows),
            original_filename=FILENAME,
        )

        self.assertEqual(parsed.claimed_report_date.isoformat(), "2026-07-15")
        self.assertEqual(len(parsed.records), 4)
        self.assertEqual(len(parsed.records[0].raw_fields), 120)
        self.assertEqual(parsed.retained_unverified_equity_count, 1)
        self.assertEqual(parsed.excluded_non_equity_count, 1)
        self.assertEqual(parsed.excluded_test_security_count, 1)
        self.assertEqual(parsed.excluded_alternative_venue_count, 1)
        self.assertEqual(
            {record.disposition for record in parsed.records},
            set(SourceRowDisposition),
        )
        self.assertEqual(len({record.source_record_id for record in parsed.records}), 4)
        self.assertEqual(parsed.records[0].raw_source_identifier, "INE009A01021")
        self.assertEqual(parsed.records[0].validated_isin, "INE009A01021")
        self.assertEqual(parsed.records[2].raw_source_identifier, "DUMMYSAN005")
        self.assertIsNone(parsed.records[2].validated_isin)

    def test_currently_blank_scope_fields_fail_closed_when_populated(self) -> None:
        for field_name in ("Xchg", "Sgmt", "SctyTp", "FinInstrmTp", "InstrmTp"):
            with self.subTest(field_name=field_name), self.assertRaisesRegex(
                ReferenceArtifactIntegrityError,
                "currently blank scope field",
            ):
                NseCmSecurityMasterParser().parse_bytes(
                    security_master_bytes([security_row(**{field_name: "NSE"})]),
                    original_filename=FILENAME,
                )

    def test_parser_is_deterministic(self) -> None:
        payload = security_master_bytes()
        parser = NseCmSecurityMasterParser()

        first = parser.parse_bytes(payload, original_filename=FILENAME)
        second = parser.parse_bytes(payload, original_filename=FILENAME)

        self.assertEqual(first, second)

    def test_filename_and_header_are_strict(self) -> None:
        parser = NseCmSecurityMasterParser()
        bad_header = list(NSE_CM_MII_SECURITY_HEADER)
        bad_header[-1] = "UnknownFutureColumn"
        cases = (
            (security_master_bytes(), "security.csv.gz"),
            (security_master_bytes(), "NSE_CM_security_31022026.csv.gz"),
            (security_master_bytes(header=bad_header), FILENAME),
        )
        for payload, filename in cases:
            with self.subTest(filename=filename), self.assertRaises(
                ReferenceArtifactIntegrityError
            ):
                parser.parse_bytes(payload, original_filename=filename)

    def test_malformed_or_duplicate_rows_fail_the_whole_artifact(self) -> None:
        duplicate_id = security_row(FinInstrmId="1594", TckrSymb="TCS")
        duplicate_listing = security_row(FinInstrmId="2000")
        malformed = security_row()[:-1]
        invalid_enum = security_row(SctyStsNrmlMkt="9")
        invalid_symbol = security_row(TckrSymb="infy")
        for rows in (
            [security_row(), duplicate_id],
            [security_row(), duplicate_listing],
            [malformed],
            [invalid_enum],
            [invalid_symbol],
        ):
            with self.subTest(rows=rows), self.assertRaises(
                ReferenceArtifactIntegrityError
            ):
                NseCmSecurityMasterParser().parse_bytes(
                    security_master_bytes(rows),
                    original_filename=FILENAME,
                )

    def test_corrupt_concatenated_trailing_and_oversized_gzip_fail(self) -> None:
        valid = security_master_bytes()
        cases = (
            b"not-gzip",
            valid[:-4],
            valid + b"trailing",
            valid + gzip.compress(b"another-member", mtime=0),
        )
        for payload in cases:
            with self.subTest(size=len(payload)), self.assertRaises(
                ReferenceArtifactIntegrityError
            ):
                NseCmSecurityMasterParser().parse_bytes(
                    payload,
                    original_filename=FILENAME,
                )

        with self.assertRaises(ReferenceArtifactIntegrityError):
            NseCmSecurityMasterParser(
                maximum_uncompressed_bytes=100,
            ).parse_bytes(valid, original_filename=FILENAME)


class LocalReferenceArtifactStoreTests(unittest.TestCase):
    def write_source(self, root: Path, payload: bytes | None = None) -> Path:
        source = root / FILENAME
        source.write_bytes(payload or security_master_bytes())
        return source

    def store(self, root: Path) -> LocalReferenceArtifactStore:
        return LocalReferenceArtifactStore(
            root / "archive",
            clock=clock_sequence(FIRST_SEEN, VALIDATED),
        )

    def test_import_is_byte_exact_collection_only_and_cutoff_safe(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            source = self.write_source(root)
            store = self.store(root)

            stored = store.import_security_master(source)
            loaded = store.get(stored.manifest.artifact_id)

            self.assertEqual(loaded.raw_bytes, source.read_bytes())
            self.assertEqual(loaded.manifest.readiness, ReferenceReadiness.COLLECTION_ONLY)
            self.assertFalse(loaded.manifest.actionable)
            self.assertEqual(
                loaded.manifest.claimed_report_date.isoformat(),
                "2026-07-15",
            )
            self.assertIsNone(loaded.manifest.verified_report_date)
            self.assertEqual(
                loaded.manifest.acquisition_mode,
                AcquisitionMode.UNVERIFIED_MANUAL_FILE,
            )
            self.assertEqual(loaded.manifest.first_seen_at, FIRST_SEEN)
            self.assertEqual(loaded.manifest.validated_at, VALIDATED)
            self.assertEqual(
                loaded.path.parent.name,
                VALIDATED.date().isoformat(),
            )
            self.assertEqual(
                sorted(path.name for path in loaded.path.iterdir()),
                ["manifest.json", "normalized.json", "source.csv.gz"],
            )
            with self.assertRaises(ReferenceArtifactNotFound):
                store.latest_at_or_before(
                    FIRST_SEEN,
                    max_age=timedelta(days=4),
                )
            with self.assertRaises(ReferenceArtifactUnverifiedReportDate):
                store.latest_at_or_before(
                    VALIDATED,
                    max_age=timedelta(days=4),
                )

    def test_reimport_is_idempotent_and_preserves_first_availability(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            source = self.write_source(root)
            store = LocalReferenceArtifactStore(
                root / "archive",
                clock=lambda: FIRST_SEEN,
            )

            first = store.import_security_master(source)
            second = store.import_security_master(source)

            self.assertEqual(first.path, second.path)
            self.assertEqual(first.manifest, second.manifest)
            artifact_directories = [
                path
                for path in (root / "archive").glob("*/*/*")
                if path.is_dir() and not path.name.startswith(".")
            ]
            self.assertEqual(len(artifact_directories), 1)

    def test_same_metadata_path_swap_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            source = self.write_source(root)
            original = os.lstat(source)
            replacement = root / "replacement.csv.gz"
            replacement.write_bytes(source.read_bytes())
            real_open = os.open
            swapped = False

            def swap_before_open(path, flags, mode=0o777, *, dir_fd=None):
                nonlocal swapped
                if Path(path) == source and not swapped:
                    swapped = True
                    os.replace(replacement, source)
                    os.utime(
                        source,
                        ns=(original.st_atime_ns, original.st_mtime_ns),
                    )
                if dir_fd is None:
                    return real_open(path, flags, mode)
                return real_open(path, flags, mode, dir_fd=dir_fd)

            with patch("india_swing._filesystem.os.open", side_effect=swap_before_open):
                with self.assertRaisesRegex(
                    ReferenceArtifactIntegrityError,
                    "changed",
                ):
                    LocalReferenceArtifactStore(root / "archive").import_security_master(
                        source
                    )

    def test_conflicting_bytes_for_one_report_date_fail_closed(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            source = self.write_source(root)
            store = LocalReferenceArtifactStore(
                root / "archive",
                clock=lambda: FIRST_SEEN,
            )
            store.import_security_master(source)
            source.write_bytes(
                security_master_bytes(
                    [security_row(FinInstrmId="2000", TckrSymb="TCS")]
                )
            )

            with self.assertRaises(ReferenceArtifactConflict):
                store.import_security_master(source)

    def test_interoperability_file_cannot_be_archived_as_nse_only(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            source = self.write_source(
                root,
                security_master_bytes(
                    [security_row(TckrSymb="BSEONLY", PrtdToTrad="2")]
                ),
            )

            with self.assertRaisesRegex(
                ReferenceArtifactIntegrityError,
                "interoperability",
            ):
                self.store(root).import_security_master(source)

            self.assertFalse((root / "archive").exists())

    def test_public_channel_floor_future_date_and_unverified_freshness_fail_closed(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            store = LocalReferenceArtifactStore(
                root / "archive",
                clock=lambda: FIRST_SEEN,
            )
            too_old = root / "NSE_CM_security_04022024.csv.gz"
            too_old.write_bytes(security_master_bytes())
            with self.assertRaisesRegex(
                ReferenceArtifactIntegrityError,
                "predates",
            ):
                store.import_security_master(too_old)

            future = root / "NSE_CM_security_31122099.csv.gz"
            future.write_bytes(security_master_bytes())
            with self.assertRaisesRegex(
                ReferenceArtifactIntegrityError,
                "later than",
            ):
                store.import_security_master(future)

        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            stale = root / "NSE_CM_security_10072026.csv.gz"
            stale.write_bytes(security_master_bytes())
            store = LocalReferenceArtifactStore(
                root / "archive",
                clock=lambda: FIRST_SEEN,
            )
            store.import_security_master(stale)
            with self.assertRaises(ReferenceArtifactUnverifiedReportDate):
                store.latest_at_or_before(
                    FIRST_SEEN,
                    max_age=timedelta(days=2),
                )

    def test_future_report_is_not_selected_before_its_india_date(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            tomorrow = root / "NSE_CM_security_16072026.csv.gz"
            tomorrow.write_bytes(security_master_bytes())
            store = LocalReferenceArtifactStore(
                root / "archive",
                clock=lambda: FIRST_SEEN,
            )
            store.import_security_master(tomorrow)

            with self.assertRaises(ReferenceArtifactUnverifiedReportDate):
                store.latest_at_or_before(
                    FIRST_SEEN,
                    max_age=timedelta(days=2),
                )

    def test_concurrent_conflicting_imports_publish_at_most_one_artifact(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            first_dir = root / "first"
            second_dir = root / "second"
            first_dir.mkdir()
            second_dir.mkdir()
            first = first_dir / FILENAME
            second = second_dir / FILENAME
            first.write_bytes(security_master_bytes())
            second.write_bytes(
                security_master_bytes(
                    [security_row(FinInstrmId="2000", TckrSymb="TCS")]
                )
            )
            store = LocalReferenceArtifactStore(
                root / "archive",
                clock=lambda: FIRST_SEEN,
            )

            def attempt(path: Path):
                try:
                    return store.import_security_master(path)
                except ReferenceArtifactConflict as exc:
                    return exc

            with ThreadPoolExecutor(max_workers=2) as pool:
                outcomes = list(pool.map(attempt, (first, second)))

            successes = [
                value for value in outcomes if not isinstance(value, Exception)
            ]
            conflicts = [
                value
                for value in outcomes
                if isinstance(value, ReferenceArtifactConflict)
            ]
            self.assertEqual(len(successes), 1)
            self.assertEqual(len(conflicts), 1)
            artifact_directories = [
                path
                for path in (root / "archive").glob("*/*/*")
                if path.is_dir() and not path.name.startswith(".")
            ]
            self.assertEqual(len(artifact_directories), 1)

    def test_raw_normalized_manifest_and_extra_file_tampering_are_detected(self) -> None:
        mutations = (
            lambda path: (path / "source.csv.gz").write_bytes(b"tampered"),
            lambda path: (path / "normalized.json").write_text(
                "{}", encoding="utf-8"
            ),
            lambda path: self._tamper_manifest(path),
            lambda path: (path / "extra.txt").write_text("extra", encoding="utf-8"),
        )
        for mutation in mutations:
            with self.subTest(mutation=mutation), tempfile.TemporaryDirectory() as temp_dir:
                root = Path(temp_dir)
                source = self.write_source(root)
                store = LocalReferenceArtifactStore(
                    root / "archive",
                    clock=lambda: FIRST_SEEN,
                )
                stored = store.import_security_master(source)
                mutation(stored.path)

                with self.assertRaises(ReferenceArtifactIntegrityError):
                    store.get(stored.manifest.artifact_id)

    @staticmethod
    def _tamper_manifest(path: Path) -> None:
        manifest_path = path / "manifest.json"
        value = json.loads(manifest_path.read_text(encoding="utf-8"))
        value["validated_at"] = "2020-01-01T00:00:00+00:00"
        manifest_path.write_text(json.dumps(value), encoding="utf-8")

    def test_failed_atomic_publish_leaves_no_partial_files(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            source = self.write_source(root)
            store = LocalReferenceArtifactStore(
                root / "archive",
                clock=lambda: FIRST_SEEN,
            )
            with patch(
                "india_swing.reference_data.artifact_store.os.rename",
                side_effect=OSError("publish failed"),
            ):
                with self.assertRaisesRegex(OSError, "publish failed"):
                    store.import_security_master(source)

            self.assertEqual(
                [
                    path
                    for path in (root / "archive").rglob("*")
                    if path.is_file() and ".locks" not in path.parts
                ],
                [],
            )

    def test_symbolic_link_input_is_rejected_when_supported(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            source = self.write_source(root)
            link = root / "linked.csv.gz"
            try:
                link.symlink_to(source)
            except OSError:
                self.skipTest("symbolic links are unavailable in this environment")

            with self.assertRaises(ReferenceArtifactIntegrityError):
                LocalReferenceArtifactStore(root / "archive").import_security_master(link)

    @unittest.skipUnless(
        os.name == "nt" and hasattr(Path, "is_junction"),
        "Windows directory junction test",
    )
    def test_dataset_junction_cannot_escape_the_configured_archive_root(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            source = self.write_source(root)
            outside_root = root / "outside"
            outside_store = LocalReferenceArtifactStore(
                outside_root,
                clock=lambda: FIRST_SEEN,
            )
            stored = outside_store.import_security_master(source)

            configured_root = root / "configured"
            configured_root.mkdir()
            junction = configured_root / NSE_CM_SECURITY_DATASET
            target = outside_root / NSE_CM_SECURITY_DATASET
            result = subprocess.run(
                ["cmd", "/c", "mklink", "/J", str(junction), str(target)],
                capture_output=True,
                check=False,
                text=True,
            )
            if result.returncode != 0:
                self.skipTest("directory junction creation is unavailable")
            try:
                self.assertTrue(junction.is_junction())
                with self.assertRaisesRegex(
                    ReferenceArtifactIntegrityError,
                    "junction",
                ):
                    LocalReferenceArtifactStore(configured_root).get(
                        stored.manifest.artifact_id
                    )
            finally:
                if junction.exists():
                    junction.rmdir()

    @unittest.skipUnless(
        os.name == "nt" and hasattr(Path, "is_junction"),
        "Windows directory junction test",
    )
    def test_import_rejects_dataset_junction_before_any_outside_write(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            source = self.write_source(root)
            outside_dataset = root / "outside-dataset"
            outside_dataset.mkdir()
            configured_root = root / "configured"
            configured_root.mkdir()
            junction = configured_root / NSE_CM_SECURITY_DATASET
            result = subprocess.run(
                [
                    "cmd",
                    "/c",
                    "mklink",
                    "/J",
                    str(junction),
                    str(outside_dataset),
                ],
                capture_output=True,
                check=False,
                text=True,
            )
            if result.returncode != 0:
                self.skipTest("directory junction creation is unavailable")
            before = list(outside_dataset.iterdir())
            try:
                with self.assertRaisesRegex(
                    ReferenceArtifactIntegrityError,
                    "junction",
                ):
                    LocalReferenceArtifactStore(
                        configured_root,
                        clock=lambda: FIRST_SEEN,
                    ).import_security_master(source)
                self.assertEqual(list(outside_dataset.iterdir()), before)
            finally:
                if junction.exists():
                    junction.rmdir()


class ReferenceDataCliTests(unittest.TestCase):
    def test_cli_needs_no_kite_credentials_and_reports_collection_only(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            source = root / FILENAME
            source.write_bytes(security_master_bytes())
            stdout = io.StringIO()
            with patch.dict(
                "os.environ",
                {"INDIA_SWING_REFERENCE_DATA_ROOT": str(root / "archive")},
                clear=True,
            ), patch("sys.stdout", stdout):
                exit_code = reference_data_main(
                    ["security-master", "import", "--file", str(source)]
                )

            response = json.loads(stdout.getvalue())
            self.assertEqual(exit_code, 0)
            self.assertEqual(response["readiness"], "COLLECTION_ONLY")
            self.assertFalse(response["actionable"])
            self.assertEqual(response["record_count"], 1)
            self.assertEqual(response["claimed_report_date"], "2026-07-15")
            self.assertIsNone(response["verified_report_date"])
            self.assertEqual(
                response["acquisition_mode"],
                "UNVERIFIED_MANUAL_FILE",
            )

    def test_cli_failure_is_sanitized(self) -> None:
        stderr = io.StringIO()
        sensitive_path = "access_token=distinct-secret.csv.gz"
        with patch("sys.stderr", stderr):
            exit_code = reference_data_main(
                ["security-master", "import", "--file", sensitive_path]
            )

        self.assertEqual(exit_code, 2)
        self.assertIn("ReferenceArtifactIntegrityError", stderr.getvalue())
        self.assertNotIn("distinct-secret", stderr.getvalue())

    def test_cli_argument_errors_do_not_echo_sensitive_values(self) -> None:
        stderr = io.StringIO()
        with patch("sys.stderr", stderr):
            exit_code = reference_data_main(
                [
                    "security-master",
                    "import",
                    "--file",
                    "missing.csv.gz",
                    "--access_token=distinct-secret",
                ]
            )

        self.assertEqual(exit_code, 2)
        self.assertIn("ReferenceDataArgumentError", stderr.getvalue())
        self.assertNotIn("distinct-secret", stderr.getvalue())


if __name__ == "__main__":
    unittest.main()
