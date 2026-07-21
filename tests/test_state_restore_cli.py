from __future__ import annotations

import io
import json
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

import india_swing.daily_pipeline.cli as cli_module
from india_swing.daily_pipeline.pinned_gcs_state_restore_file_boundary import (
    PinnedGCSStateRestoreFileBoundaryError,
)
from india_swing.daily_pipeline.pinned_gcs_state_restore_spec import (
    PinnedGCSStateRestoreSpec,
    encode_pinned_gcs_state_restore_spec,
)
from india_swing.daily_pipeline.state_inventory import encode_pipeline_state_inventory
from india_swing.daily_pipeline.state_publication import (
    encode_pipeline_state_publication_manifest,
)

from tests.test_state_blob_acquisition import _build_control
from tests.test_acquisition import FakeBlob, FakeStorageClient


class StateRestoreCLITests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name).resolve()
        control, _ = _build_control()
        self.control = control
        self.inventory = control.inventory
        self.destination = self.root / control.request.expected_run_id
        self.spec_path = self.root / "restore-spec.json"
        self.spec = PinnedGCSStateRestoreSpec(
            schema_version=1,
            publication_request=control.request,
            destination=self.destination,
        )
        self.reader = object()
        self.completed = SimpleNamespace(
            restoration=SimpleNamespace(
                run_id=control.inventory.run_id,
                inventory_id=control.inventory.inventory_id,
                snapshot_root=self.destination,
            ),
            state=SimpleNamespace(
                acquired_blobs=SimpleNamespace(
                    control=SimpleNamespace(inventory=control.inventory)
                )
            ),
        )

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def _run(self) -> tuple[int, str, str]:
        stdout = io.StringIO()
        stderr = io.StringIO()
        with redirect_stdout(stdout), redirect_stderr(stderr):
            result = cli_module.main(
                ["restore-pinned-state", "--spec-file", str(self.spec_path)]
            )
        return result, stdout.getvalue(), stderr.getvalue()

    def test_success_loads_before_client_and_avoids_unrelated_environment(self) -> None:
        events: list[str] = []

        def load(path: Path) -> PinnedGCSStateRestoreSpec:
            events.append("load")
            self.assertEqual(path, self.spec_path)
            return self.spec

        def make_reader() -> object:
            events.append("reader")
            return self.reader

        def restore(request, *, reader, destination):
            events.append("restore")
            self.assertIs(request, self.spec.publication_request)
            self.assertIs(reader, self.reader)
            self.assertEqual(destination, self.destination)
            return self.completed

        with (
            patch.object(
                cli_module,
                "load_pinned_gcs_state_restore_spec_file",
                side_effect=load,
            ),
            patch.object(
                cli_module,
                "GoogleCloudStorageObjectReader",
                side_effect=make_reader,
            ),
            patch.object(
                cli_module,
                "restore_pipeline_state_from_pinned_gcs",
                side_effect=restore,
            ),
            patch.object(
                cli_module.DailyPipelineConfig,
                "from_env",
            ) as daily_config,
        ):
            exit_code, stdout, stderr = self._run()

        self.assertEqual(events, ["load", "reader", "restore"])
        daily_config.assert_not_called()
        self.assertEqual(exit_code, 0)
        self.assertEqual(stderr, "")
        self.assertEqual(
            json.loads(stdout),
            {
                "status": "COMPLETE",
                "kind": "PINNED_GCS_STATE_RESTORE",
                "run_id": self.inventory.run_id,
                "inventory_id": self.inventory.inventory_id,
                "snapshot_root": str(self.destination),
                "entry_count": self.inventory.entry_count,
                "total_bytes": self.inventory.total_bytes,
            },
        )

    def test_real_cli_reader_and_restore_chain_with_fake_gcs_sdk(self) -> None:
        control, contents = _build_control()
        manifest = control.publication.manifest
        objects = {
            (
                control.request.publication_object_name,
                control.request.generation,
            ): encode_pipeline_state_publication_manifest(manifest),
            (
                manifest.inventory_object.object_name,
                manifest.inventory_object.generation,
            ): encode_pipeline_state_inventory(control.inventory),
        }
        objects.update(
            {
                (item.object_name, item.generation): contents[item.sha256]
                for item in manifest.blob_objects
            }
        )

        def blob_factory(object_name: str, generation: object) -> FakeBlob:
            payload = objects[(object_name, generation)]
            return FakeBlob(
                object_name,
                generation,
                observed_generation=generation,
                content_bytes=payload,
            )

        fake_client = FakeStorageClient(blob_factory=blob_factory)
        self.spec_path.write_bytes(encode_pinned_gcs_state_restore_spec(self.spec))
        client_constructor = Mock(return_value=fake_client)
        with patch(
            "india_swing.daily_pipeline.acquisition.storage",
            SimpleNamespace(Client=client_constructor),
        ):
            exit_code, stdout, stderr = self._run()

        self.assertEqual(exit_code, 0)
        self.assertEqual(stderr, "")
        self.assertEqual(json.loads(stdout)["kind"], "PINNED_GCS_STATE_RESTORE")
        self.assertTrue(self.destination.is_dir())
        self.assertEqual(
            len(fake_client.bucket_calls),
            2 + len(manifest.blob_objects),
        )
        client_constructor.assert_called_once_with()

    def test_invalid_spec_fails_before_client_construction(self) -> None:
        with (
            patch.object(
                cli_module,
                "load_pinned_gcs_state_restore_spec_file",
                side_effect=PinnedGCSStateRestoreFileBoundaryError("secret"),
            ),
            patch.object(cli_module, "GoogleCloudStorageObjectReader") as reader,
            patch.object(cli_module, "restore_pipeline_state_from_pinned_gcs") as restore,
            patch.object(cli_module.DailyPipelineConfig, "from_env") as daily_config,
        ):
            exit_code, stdout, stderr = self._run()
        self.assertEqual(exit_code, 2)
        self.assertEqual(stdout, "")
        self.assertEqual(
            json.loads(stderr),
            {
                "status": "FAILED",
                "error_type": "PinnedGCSStateRestoreFileBoundaryError",
            },
        )
        self.assertNotIn("secret", stderr)
        reader.assert_not_called()
        restore.assert_not_called()
        daily_config.assert_not_called()

    def test_reader_or_service_failure_is_sanitized(self) -> None:
        cases = (
            ("GoogleCloudStorageObjectReader", RuntimeError("secret-reader")),
            ("restore_pipeline_state_from_pinned_gcs", RuntimeError("secret-service")),
        )
        for target, error in cases:
            with self.subTest(target=target):
                with (
                    patch.object(
                        cli_module,
                        "load_pinned_gcs_state_restore_spec_file",
                        return_value=self.spec,
                    ),
                    patch.object(
                        cli_module,
                        "GoogleCloudStorageObjectReader",
                        return_value=self.reader,
                    ),
                    patch.object(
                        cli_module,
                        "restore_pipeline_state_from_pinned_gcs",
                        return_value=self.completed,
                    ),
                    patch.object(cli_module, target, side_effect=error),
                ):
                    exit_code, stdout, stderr = self._run()
                self.assertEqual(exit_code, 2)
                self.assertEqual(stdout, "")
                self.assertEqual(json.loads(stderr)["error_type"], "RuntimeError")
                self.assertNotIn("secret", stderr)

    def test_missing_or_unknown_arguments_fail_without_loading(self) -> None:
        cases = (
            ["restore-pinned-state"],
            ["restore-pinned-state", "--unknown", "value"],
        )
        for args in cases:
            with self.subTest(args=args):
                stdout = io.StringIO()
                stderr = io.StringIO()
                with (
                    patch.object(
                        cli_module,
                        "load_pinned_gcs_state_restore_spec_file",
                    ) as load,
                    redirect_stdout(stdout),
                    redirect_stderr(stderr),
                ):
                    exit_code = cli_module.main(args)
                self.assertEqual(exit_code, 2)
                self.assertEqual(stdout.getvalue(), "")
                self.assertEqual(
                    json.loads(stderr.getvalue())["error_type"],
                    "DailyPipelineArgumentError",
                )
                load.assert_not_called()


if __name__ == "__main__":
    unittest.main()
