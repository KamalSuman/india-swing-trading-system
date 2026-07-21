from __future__ import annotations

import ast
import hashlib
import io
import json
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

from india_swing.daily_pipeline.acquisition import GCSObjectPayload
from india_swing.daily_pipeline.state_publication import PublishedStateObject
from india_swing.operations import (
    LocalSwingOperationalRunStore,
    SwingOperationalFailureCode,
    SwingOperationalStateError,
    SwingOperationalStateRestoreRequest,
    SwingOperationalStatus,
    decode_operational_state_manifest,
    encode_operational_state_manifest,
    operational_record_from_result,
    publish_swing_operational_run,
    publish_swing_operational_state_to_gcs,
    restore_swing_operational_state_from_gcs,
)
from india_swing.operational_restore import main as operational_restore_main
from india_swing.paper_trades import LocalPaperTradeLedger
from india_swing.recommendations import LocalSwingDecisionOutbox, SwingDecisionAction

from tests import test_swing_operational_run as operational_fixtures


_REPO_ROOT = Path(__file__).resolve().parent.parent


class MemoryGCS:
    def __init__(self, *, fail_on_call: int | None = None) -> None:
        self.objects: dict[tuple[str, str], tuple[bytes, int]] = {}
        self.write_calls: list[dict[str, object]] = []
        self.read_calls: list[dict[str, object]] = []
        self.fail_on_call = fail_on_call
        self._next_generation = 1

    def create_or_verify(self, **values) -> PublishedStateObject:
        self.write_calls.append(values)
        if self.fail_on_call == len(self.write_calls):
            raise RuntimeError("secret publication failure")
        key = (values["bucket"], values["object_name"])
        payload = values["content_bytes"]
        if key in self.objects:
            stored, generation = self.objects[key]
            if stored != payload:
                raise RuntimeError("immutable conflict")
        else:
            generation = self._next_generation
            self._next_generation += 1
            self.objects[key] = (payload, generation)
        return PublishedStateObject(
            object_name=values["object_name"],
            generation=generation,
            byte_count=len(payload),
            sha256=hashlib.sha256(payload).hexdigest(),
        )

    def read_generation(self, **values) -> GCSObjectPayload:
        self.read_calls.append(values)
        payload, generation = self.objects[(values["bucket"], values["object_name"])]
        return GCSObjectPayload(
            content_bytes=payload[: values["maximum_bytes"] + 1],
            generation=generation,
        )


class WrongMetadataWriter(MemoryGCS):
    def create_or_verify(self, **values) -> PublishedStateObject:
        published = super().create_or_verify(**values)
        return PublishedStateObject(
            object_name=published.object_name,
            generation=published.generation,
            byte_count=published.byte_count,
            sha256="0" * 64,
        )


class SwingOperationalGCSStateTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        operational_fixtures.SwingOperationalPublicationTests.setUpClass()
        cls.result = operational_fixtures.SwingOperationalPublicationTests.result

    def _source(self, root: Path, *, result=None):
        outbox = LocalSwingDecisionOutbox(root / "decision_outbox")
        ledger = LocalPaperTradeLedger(root / "paper")
        store = LocalSwingOperationalRunStore(root / "operational")
        record = publish_swing_operational_run(
            result=result or self.result,
            run_store=store,
            decision_outbox=outbox,
            paper_ledger=ledger,
        )
        return record, outbox, ledger

    @staticmethod
    def _request(publication) -> SwingOperationalStateRestoreRequest:
        return SwingOperationalStateRestoreRequest(
            bucket=publication.manifest.bucket,
            manifest_object_name=publication.manifest_object.object_name,
            generation=publication.manifest_object.generation,
            expected_sha256=publication.manifest_object.sha256,
            expected_spec_id=publication.manifest.spec_id,
        )

    def test_complete_state_publishes_terminal_last_and_restores_idempotently(self) -> None:
        with tempfile.TemporaryDirectory() as source_directory, tempfile.TemporaryDirectory() as destination_directory:
            source = Path(source_directory)
            destination = Path(destination_directory)
            record, outbox, ledger = self._source(source)
            gcs = MemoryGCS()

            publication = publish_swing_operational_state_to_gcs(
                record=record,
                bucket="india-swing-private",
                writer=gcs,
                decision_outbox=outbox,
                paper_ledger=ledger,
            )

            self.assertEqual(len(gcs.write_calls), 4)
            self.assertIn("/notification/", gcs.write_calls[0]["object_name"])
            self.assertIn("/paper_registration/", gcs.write_calls[1]["object_name"])
            self.assertIn("/run_record/", gcs.write_calls[2]["object_name"])
            self.assertIn("/manifests/", gcs.write_calls[3]["object_name"])
            self.assertEqual(gcs.write_calls[3]["maximum_bytes"], 256 * 1024)

            run_store = LocalSwingOperationalRunStore(destination / "operational")
            restored_outbox = LocalSwingDecisionOutbox(destination / "decision_outbox")
            restored_ledger = LocalPaperTradeLedger(destination / "paper")
            request = self._request(publication)
            first = restore_swing_operational_state_from_gcs(
                request=request,
                reader=gcs,
                run_store=run_store,
                decision_outbox=restored_outbox,
                paper_ledger=restored_ledger,
            )
            second = restore_swing_operational_state_from_gcs(
                request=request,
                reader=gcs,
                run_store=run_store,
                decision_outbox=restored_outbox,
                paper_ledger=restored_ledger,
            )

            self.assertEqual(first, second)
            self.assertEqual(first.record, record)
            self.assertEqual(run_store.get(record.spec_id), record)
            self.assertEqual(
                restored_outbox.get(record.decision_id).notification_id,
                record.notification_id,
            )
            self.assertEqual(
                restored_ledger.get_registration(record.paper_registration_id).registration_id,
                record.paper_registration_id,
            )
            self.assertEqual(len(gcs.read_calls), 8)

    def test_failed_state_contains_only_record_and_manifest(self) -> None:
        failed_result = replace(
            self.result,
            status=SwingOperationalStatus.FAILED,
            action=SwingDecisionAction.NO_TRADE,
            failure_codes=(SwingOperationalFailureCode.DECISION_ASSEMBLY_FAILED,),
            decision_package=None,
            paper_registration=None,
        )
        with tempfile.TemporaryDirectory() as source_directory, tempfile.TemporaryDirectory() as destination_directory:
            record, outbox, ledger = self._source(Path(source_directory), result=failed_result)
            gcs = MemoryGCS()
            publication = publish_swing_operational_state_to_gcs(
                record=record,
                bucket="india-swing-private",
                writer=gcs,
                decision_outbox=outbox,
                paper_ledger=ledger,
            )
            self.assertEqual(len(gcs.write_calls), 2)
            self.assertIn("/run_record/", gcs.write_calls[0]["object_name"])
            self.assertIn("/manifests/", gcs.write_calls[1]["object_name"])

            destination = Path(destination_directory)
            restored = restore_swing_operational_state_from_gcs(
                request=self._request(publication),
                reader=gcs,
                run_store=LocalSwingOperationalRunStore(destination / "operational"),
                decision_outbox=LocalSwingDecisionOutbox(destination / "decision_outbox"),
                paper_ledger=LocalPaperTradeLedger(destination / "paper"),
            )
            self.assertIsNone(restored.notification)
            self.assertIsNone(restored.paper_registration)
            self.assertEqual(restored.record, record)

    def test_tampered_blob_or_wrong_manifest_generation_fails_before_local_write(self) -> None:
        with tempfile.TemporaryDirectory() as source_directory, tempfile.TemporaryDirectory() as destination_directory:
            record, outbox, ledger = self._source(Path(source_directory))
            gcs = MemoryGCS()
            publication = publish_swing_operational_state_to_gcs(
                record=record,
                bucket="india-swing-private",
                writer=gcs,
                decision_outbox=outbox,
                paper_ledger=ledger,
            )
            artifact = publication.manifest.artifacts[0].published_object
            payload, generation = gcs.objects[(publication.manifest.bucket, artifact.object_name)]
            gcs.objects[(publication.manifest.bucket, artifact.object_name)] = (
                payload + b"tamper",
                generation,
            )
            destination = Path(destination_directory)
            run_store = LocalSwingOperationalRunStore(destination / "operational")
            with self.assertRaises(SwingOperationalStateError):
                restore_swing_operational_state_from_gcs(
                    request=self._request(publication),
                    reader=gcs,
                    run_store=run_store,
                    decision_outbox=LocalSwingDecisionOutbox(destination / "decision_outbox"),
                    paper_ledger=LocalPaperTradeLedger(destination / "paper"),
                )
            self.assertEqual(run_store.list_records(), ())

            wrong_generation = replace(
                self._request(publication),
                generation=publication.manifest_object.generation + 1,
            )
            with self.assertRaises(SwingOperationalStateError):
                restore_swing_operational_state_from_gcs(
                    request=wrong_generation,
                    reader=gcs,
                    run_store=run_store,
                    decision_outbox=LocalSwingDecisionOutbox(destination / "decision_outbox"),
                    paper_ledger=LocalPaperTradeLedger(destination / "paper"),
                )

    def test_manifest_codec_rejects_duplicate_float_extra_and_identity_tamper(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            record, outbox, ledger = self._source(Path(directory))
            publication = publish_swing_operational_state_to_gcs(
                record=record,
                bucket="india-swing-private",
                writer=MemoryGCS(),
                decision_outbox=outbox,
                paper_ledger=ledger,
            )
            payload = encode_operational_state_manifest(publication.manifest)
            self.assertEqual(decode_operational_state_manifest(payload), publication.manifest)
            invalid = [
                payload.replace(b'{"action":', b'{"action":"BUY","action":', 1),
            ]
            for mutation in ("float", "extra", "identity"):
                raw = json.loads(payload)
                if mutation == "float":
                    raw["schema_version"] = 1.0
                elif mutation == "extra":
                    raw["unexpected"] = True
                else:
                    raw["publication_id"] = "0" * 64
                invalid.append(
                    (json.dumps(raw, separators=(",", ":"), sort_keys=True) + "\n").encode()
                )
            for value in invalid:
                with self.subTest(value=value[:80]):
                    with self.assertRaises(SwingOperationalStateError):
                        decode_operational_state_manifest(value)

    def test_publication_rejects_untrusted_writer_metadata_and_does_not_seal_partial(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            record, outbox, ledger = self._source(Path(directory))
            with self.assertRaises(SwingOperationalStateError):
                publish_swing_operational_state_to_gcs(
                    record=record,
                    bucket="india-swing-private",
                    writer=WrongMetadataWriter(),
                    decision_outbox=outbox,
                    paper_ledger=ledger,
                )
            partial = MemoryGCS(fail_on_call=3)
            with self.assertRaises(SwingOperationalStateError):
                publish_swing_operational_state_to_gcs(
                    record=record,
                    bucket="india-swing-private",
                    writer=partial,
                    decision_outbox=outbox,
                    paper_ledger=ledger,
                )
            self.assertFalse(any("/manifests/" in name for _, name in partial.objects))

    def test_request_and_module_expose_no_latest_listing_or_execution_capability(self) -> None:
        for object_name in (
            "../manifest.json",
            "operational-state/latest/" + "0" * 64 + "/manifests/" + "1" * 64 + ".json",
        ):
            with self.subTest(object_name=object_name):
                with self.assertRaises(SwingOperationalStateError):
                    SwingOperationalStateRestoreRequest(
                        bucket="india-swing-private",
                        manifest_object_name=object_name,
                        generation=1,
                        expected_sha256="2" * 64,
                        expected_spec_id="0" * 64,
                    )
        source = (
            _REPO_ROOT / "src/india_swing/operations/gcs_state.py"
        ).read_text(encoding="utf-8")
        lowered = source.casefold()
        for forbidden in (
            "list_blobs",
            "place_order",
            "modify_order",
            "cancel_order",
            "subprocess",
            "pickle",
            "eval(",
            "exec(",
        ):
            self.assertNotIn(forbidden, lowered)
        ast.parse(source)

    def test_restore_cli_uses_exact_pin_and_sanitizes_failures(self) -> None:
        with tempfile.TemporaryDirectory() as source_directory, tempfile.TemporaryDirectory() as destination_directory:
            record, outbox, ledger = self._source(Path(source_directory))
            gcs = MemoryGCS()
            publication = publish_swing_operational_state_to_gcs(
                record=record,
                bucket="india-swing-private",
                writer=gcs,
                decision_outbox=outbox,
                paper_ledger=ledger,
            )
            request = self._request(publication)
            stdout = io.StringIO()
            stderr = io.StringIO()
            with (
                patch(
                    "india_swing.operational_restore.GoogleCloudStorageObjectReader",
                    return_value=gcs,
                ),
                redirect_stdout(stdout),
                redirect_stderr(stderr),
            ):
                code = operational_restore_main(
                    [
                        "--expected-spec-id",
                        request.expected_spec_id,
                        "--manifest-generation",
                        str(request.generation),
                        "--manifest-object",
                        request.manifest_object_name,
                        "--manifest-sha256",
                        request.expected_sha256,
                        "--state-root",
                        str(Path(destination_directory).resolve()),
                    ],
                    environ={
                        "INDIA_SWING_OPERATIONAL_STATE_BUCKET": request.bucket,
                    },
                )
            self.assertEqual(code, 0)
            self.assertEqual(stderr.getvalue(), "")
            self.assertEqual(json.loads(stdout.getvalue())["record_id"], record.record_id)

        secret = "secret-restore-value"
        stderr = io.StringIO()
        with redirect_stderr(stderr):
            code = operational_restore_main(["--unknown", secret], environ={})
        self.assertEqual(code, 2)
        self.assertNotIn(secret, stderr.getvalue())


if __name__ == "__main__":
    unittest.main()
