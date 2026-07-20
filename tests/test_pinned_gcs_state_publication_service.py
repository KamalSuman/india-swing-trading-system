from __future__ import annotations

import ast
import dataclasses
import tempfile
import unittest
from datetime import UTC, date, datetime
from pathlib import Path
from unittest.mock import patch

from india_swing.daily_pipeline import pinned_gcs_state_publication_service
from india_swing.daily_pipeline.acquisition import LandingManifestObjectRequest
from india_swing.daily_pipeline.landing_manifest import TrustedLandingManifestBinding
from india_swing.daily_pipeline.models import DailyPipelineRun
from india_swing.daily_pipeline.pinned_gcs_run_service import PinnedGCSRunServiceError
from india_swing.daily_pipeline.pinned_gcs_run_spec import (
    PINNED_GCS_RUN_SPEC_SCHEMA_VERSION,
    PinnedGCSRunSpec,
)
from india_swing.daily_pipeline.pinned_gcs_state_publication_service import (
    CompletedPinnedGCSStatePublication,
    PinnedGCSStatePublicationServiceError,
    run_daily_pipeline_and_publish_state_from_pinned_gcs_run_spec,
)
from india_swing.daily_pipeline.state_inventory import (
    ROOT_NAMES,
    PipelineStateInventory,
    PipelineStateInventoryError,
    PipelineStateRoots,
    build_pipeline_state_inventory,
)
from india_swing.daily_pipeline.state_publication import (
    CompletedPipelineStatePublication,
    PublishedStateObject,
    StatePublicationError,
    publish_pipeline_state,
)

from tests.test_promotion import daily_run as _promotion_daily_run

_REPO_ROOT = Path(__file__).resolve().parent.parent
_MODULE_PATH = (
    _REPO_ROOT
    / "src"
    / "india_swing"
    / "daily_pipeline"
    / "pinned_gcs_state_publication_service.py"
)

_SESSION = date(2026, 7, 20)
_SPEC_BUCKET = "trusted-run-service-bucket"
_SHA256_HEX = "a" * 64
_NOT_BEFORE = datetime(2026, 7, 20, 0, 0, 0, tzinfo=UTC)
_BINDING_CUTOFF = datetime(2026, 7, 20, 14, 0, 0, tzinfo=UTC)
_RUN_CUTOFF = datetime(2026, 7, 20, 15, 0, 0, tzinfo=UTC)


def _manifest_object_name(session: date) -> str:
    return f"landing/{session.isoformat()}/landing-manifest.json"


def _valid_spec(calendar_materialization_id: str = "b" * 64) -> PinnedGCSRunSpec:
    request = LandingManifestObjectRequest(
        bucket=_SPEC_BUCKET,
        object_name=_manifest_object_name(_SESSION),
        generation=777,
        target_session=_SESSION,
    )
    binding = TrustedLandingManifestBinding(
        expected_manifest_sha256=_SHA256_HEX,
        allowed_bucket=_SPEC_BUCKET,
        target_session=_SESSION,
        not_before=_NOT_BEFORE,
        cutoff=_BINDING_CUTOFF,
    )
    return PinnedGCSRunSpec(
        schema_version=PINNED_GCS_RUN_SPEC_SCHEMA_VERSION,
        manifest_request=request,
        trusted_binding=binding,
        market_session=_SESSION,
        cutoff=_RUN_CUTOFF,
        calendar_materialization_id=calendar_materialization_id,
        previous_run_id=None,
    )


def _make_roots(base: Path) -> PipelineStateRoots:
    kwargs = {}
    for name in ROOT_NAMES:
        root_path = base / name
        root_path.mkdir(parents=True, exist_ok=True)
        kwargs[name] = root_path
    return PipelineStateRoots(**kwargs)


def _write(path: Path, content: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)


class RecordingWriter:
    """Fake StateObjectWriter. Never contacts GCP; deterministic generations."""

    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []
        self._next_generation = 1

    def create_or_verify(
        self,
        *,
        bucket: str,
        object_name: str,
        content_bytes: bytes,
        content_type: str,
        maximum_bytes: int,
    ) -> PublishedStateObject:
        import hashlib

        self.calls.append({"bucket": bucket, "object_name": object_name})
        generation = self._next_generation
        self._next_generation += 1
        return PublishedStateObject(
            object_name=object_name,
            generation=generation,
            byte_count=len(content_bytes),
            sha256=hashlib.sha256(content_bytes).hexdigest(),
        )


class _ServiceTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.base = Path(self.temporary.name)
        self.roots = _make_roots(self.base)
        self.run = _promotion_daily_run()
        _write(self.roots.calendar_data / "a.json", b"content")
        self.inventory = build_pipeline_state_inventory(self.run, self.roots)
        self.bucket = "test-bucket"
        self.publication = publish_pipeline_state(
            self.run, self.inventory, self.roots, self.bucket, RecordingWriter()
        )
        self.spec = _valid_spec()
        self.calendar_materialization = None

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def _call(self, **overrides: object) -> CompletedPinnedGCSStatePublication:
        kwargs: dict[str, object] = dict(
            spec=self.spec,
            calendar_materialization=self.calendar_materialization,
            roots=self.roots,
            bucket=self.bucket,
            reader=object(),
            reference_store=object(),
            daily_store=object(),
            historical_store=object(),
            identity_store=object(),
            adjudication_store=object(),
            run_store=object(),
            writer=object(),
        )
        kwargs.update(overrides)
        return run_daily_pipeline_and_publish_state_from_pinned_gcs_run_spec(**kwargs)

    def _patched(self, *, run_result=None, inventory_result=None, publication_result=None,
                 run_side_effect=None, inventory_side_effect=None, publication_side_effect=None):
        call_order: list[str] = []

        def _run(spec, calendar_materialization, **kwargs):
            call_order.append("run")
            if run_side_effect is not None:
                raise run_side_effect
            return run_result if run_result is not None else self.run

        def _inventory(run, roots):
            call_order.append("inventory")
            if inventory_side_effect is not None:
                raise inventory_side_effect
            return inventory_result if inventory_result is not None else self.inventory

        def _publish(run, inventory, roots, bucket, writer):
            call_order.append("publish")
            if publication_side_effect is not None:
                raise publication_side_effect
            return publication_result if publication_result is not None else self.publication

        run_patch = patch.object(
            pinned_gcs_state_publication_service,
            "run_daily_pipeline_from_pinned_gcs_run_spec",
            side_effect=_run,
        )
        inventory_patch = patch.object(
            pinned_gcs_state_publication_service,
            "build_pipeline_state_inventory",
            side_effect=_inventory,
        )
        publish_patch = patch.object(
            pinned_gcs_state_publication_service, "publish_pipeline_state", side_effect=_publish
        )
        return call_order, run_patch, inventory_patch, publish_patch


class OrderingAndForwardingTests(_ServiceTestCase):
    def test_exact_call_order_count_and_forwarding(self) -> None:
        call_order, run_patch, inventory_patch, publish_patch = self._patched()
        reader_sentinel = object()
        reference_store_sentinel = object()
        daily_store_sentinel = object()
        historical_store_sentinel = object()
        identity_store_sentinel = object()
        adjudication_store_sentinel = object()
        run_store_sentinel = object()
        writer_sentinel = object()

        with run_patch as run_mock, inventory_patch as inventory_mock, publish_patch as publish_mock:
            result = self._call(
                reader=reader_sentinel,
                reference_store=reference_store_sentinel,
                daily_store=daily_store_sentinel,
                historical_store=historical_store_sentinel,
                identity_store=identity_store_sentinel,
                adjudication_store=adjudication_store_sentinel,
                run_store=run_store_sentinel,
                writer=writer_sentinel,
            )

        self.assertEqual(call_order, ["run", "inventory", "publish"])
        self.assertEqual(run_mock.call_count, 1)
        self.assertEqual(inventory_mock.call_count, 1)
        self.assertEqual(publish_mock.call_count, 1)

        run_mock.assert_called_once_with(
            self.spec,
            self.calendar_materialization,
            reader=reader_sentinel,
            reference_store=reference_store_sentinel,
            daily_store=daily_store_sentinel,
            historical_store=historical_store_sentinel,
            identity_store=identity_store_sentinel,
            adjudication_store=adjudication_store_sentinel,
            run_store=run_store_sentinel,
        )
        inventory_mock.assert_called_once_with(self.run, self.roots)
        publish_mock.assert_called_once_with(
            self.run, self.inventory, self.roots, self.bucket, writer_sentinel
        )

        self.assertIs(result.run, self.run)
        self.assertIs(result.inventory, self.inventory)
        self.assertEqual(result.bucket, self.bucket)
        self.assertEqual(
            result.publication.manifest.publication_id,
            self.publication.manifest.publication_id,
        )
        self.assertEqual(
            result.publication.publication_object, self.publication.publication_object
        )

    def test_no_writer_call_reaches_publish_before_run_and_inventory_succeed(self) -> None:
        # Sanity check on ordering semantics: patch publish to record whether
        # it was ever invoked before run/inventory would have completed --
        # already covered by call_order above, this test just re-confirms
        # inventory is never called before run succeeds.
        call_order, run_patch, inventory_patch, publish_patch = self._patched()
        with run_patch, inventory_patch, publish_patch:
            self._call()
        self.assertEqual(call_order.index("run"), 0)
        self.assertEqual(call_order.index("inventory"), 1)
        self.assertEqual(call_order.index("publish"), 2)


class SuccessResultTests(_ServiceTestCase):
    def test_result_is_fresh_exact_aggregate(self) -> None:
        call_order, run_patch, inventory_patch, publish_patch = self._patched()
        with run_patch, inventory_patch, publish_patch:
            result = self._call()
        self.assertIs(type(result), CompletedPinnedGCSStatePublication)
        self.assertEqual(result.bucket, self.bucket)
        self.assertEqual(result.run.run_id, self.run.run_id)
        self.assertEqual(result.inventory.inventory_id, self.inventory.inventory_id)
        self.assertEqual(
            result.publication.manifest.bucket, self.publication.manifest.bucket
        )

    def test_cross_bindings_preserved(self) -> None:
        call_order, run_patch, inventory_patch, publish_patch = self._patched()
        with run_patch, inventory_patch, publish_patch:
            result = self._call()
        self.assertEqual(result.publication.manifest.run_id, result.run.run_id)
        self.assertEqual(
            result.publication.manifest.previous_run_id, result.run.previous_run_id
        )
        self.assertEqual(result.publication.manifest.market_session, result.run.market_session)
        self.assertEqual(result.publication.manifest.cutoff, result.run.cutoff)
        self.assertEqual(result.publication.manifest.inventory_id, result.inventory.inventory_id)
        self.assertEqual(result.publication.manifest.bucket, result.bucket)
        self.assertEqual(result.inventory.run_id, result.run.run_id)


class InvalidInputFailureTests(_ServiceTestCase):
    def test_invalid_spec_type_rejected_before_any_delegated_call(self) -> None:
        call_order, run_patch, inventory_patch, publish_patch = self._patched()
        with run_patch as run_mock, inventory_patch as inventory_mock, publish_patch as publish_mock:
            with self.assertRaises(PinnedGCSStatePublicationServiceError):
                self._call(spec="not-a-spec")
        self.assertEqual(run_mock.call_count, 0)
        self.assertEqual(inventory_mock.call_count, 0)
        self.assertEqual(publish_mock.call_count, 0)

    def test_invalid_roots_type_rejected_before_any_delegated_call(self) -> None:
        call_order, run_patch, inventory_patch, publish_patch = self._patched()
        with run_patch as run_mock, inventory_patch as inventory_mock, publish_patch as publish_mock:
            with self.assertRaises(PinnedGCSStatePublicationServiceError):
                self._call(roots="not-roots")
        self.assertEqual(run_mock.call_count, 0)
        self.assertEqual(inventory_mock.call_count, 0)
        self.assertEqual(publish_mock.call_count, 0)

    def test_invalid_bucket_rejected_before_any_delegated_call(self) -> None:
        call_order, run_patch, inventory_patch, publish_patch = self._patched()
        with run_patch as run_mock, inventory_patch as inventory_mock, publish_patch as publish_mock:
            with self.assertRaises(PinnedGCSStatePublicationServiceError):
                self._call(bucket="INVALID_BUCKET")
        self.assertEqual(run_mock.call_count, 0)
        self.assertEqual(inventory_mock.call_count, 0)
        self.assertEqual(publish_mock.call_count, 0)


class RunStageFailureTests(_ServiceTestCase):
    def test_run_exception_sanitized_no_later_stage_called(self) -> None:
        secret = "SECRET-RUN-FAILURE-DO-NOT-LEAK-7a1c"
        call_order, run_patch, inventory_patch, publish_patch = self._patched(
            run_side_effect=PinnedGCSRunServiceError(secret)
        )
        with run_patch as run_mock, inventory_patch as inventory_mock, publish_patch as publish_mock:
            try:
                self._call()
                self.fail("expected PinnedGCSStatePublicationServiceError")
            except PinnedGCSStatePublicationServiceError as exc:
                self.assertNotIn(secret, str(exc))
                self.assertIsNone(exc.__cause__)
                self.assertIsNone(exc.__context__)
        self.assertEqual(run_mock.call_count, 1)
        self.assertEqual(inventory_mock.call_count, 0)
        self.assertEqual(publish_mock.call_count, 0)

    def test_run_wrong_return_type_rejected(self) -> None:
        call_order, run_patch, inventory_patch, publish_patch = self._patched(
            run_result="not-a-run"
        )
        with run_patch as run_mock, inventory_patch as inventory_mock, publish_patch as publish_mock:
            with self.assertRaises(PinnedGCSStatePublicationServiceError):
                self._call()
        self.assertEqual(run_mock.call_count, 1)
        self.assertEqual(inventory_mock.call_count, 0)
        self.assertEqual(publish_mock.call_count, 0)

    def test_run_subclass_return_rejected(self) -> None:
        class _ShapedRun(DailyPipelineRun):
            pass

        shaped = _ShapedRun(**{f.name: getattr(self.run, f.name) for f in dataclasses.fields(self.run) if f.name != "run_id"})
        call_order, run_patch, inventory_patch, publish_patch = self._patched(run_result=shaped)
        with run_patch as run_mock, inventory_patch as inventory_mock, publish_patch as publish_mock:
            with self.assertRaises(PinnedGCSStatePublicationServiceError):
                self._call()
        self.assertEqual(inventory_mock.call_count, 0)
        self.assertEqual(publish_mock.call_count, 0)

    def test_run_post_construction_mutation_rejected(self) -> None:
        mutated = dataclasses.replace(self.run)
        object.__setattr__(mutated, "bar_count", mutated.bar_count + 1)
        call_order, run_patch, inventory_patch, publish_patch = self._patched(run_result=mutated)
        with run_patch as run_mock, inventory_patch as inventory_mock, publish_patch as publish_mock:
            with self.assertRaises(PinnedGCSStatePublicationServiceError):
                self._call()
        self.assertEqual(inventory_mock.call_count, 0)
        self.assertEqual(publish_mock.call_count, 0)


class InventoryStageFailureTests(_ServiceTestCase):
    def test_inventory_exception_sanitized_no_later_stage_called(self) -> None:
        secret = "SECRET-INVENTORY-FAILURE-DO-NOT-LEAK-3f9d"
        call_order, run_patch, inventory_patch, publish_patch = self._patched(
            inventory_side_effect=PipelineStateInventoryError(secret)
        )
        with run_patch as run_mock, inventory_patch as inventory_mock, publish_patch as publish_mock:
            try:
                self._call()
                self.fail("expected PinnedGCSStatePublicationServiceError")
            except PinnedGCSStatePublicationServiceError as exc:
                self.assertNotIn(secret, str(exc))
                self.assertIsNone(exc.__cause__)
                self.assertIsNone(exc.__context__)
        self.assertEqual(run_mock.call_count, 1)
        self.assertEqual(inventory_mock.call_count, 1)
        self.assertEqual(publish_mock.call_count, 0)

    def test_inventory_wrong_return_type_rejected(self) -> None:
        call_order, run_patch, inventory_patch, publish_patch = self._patched(
            inventory_result="not-an-inventory"
        )
        with run_patch, inventory_patch, publish_patch as publish_mock:
            with self.assertRaises(PinnedGCSStatePublicationServiceError):
                self._call()
        self.assertEqual(publish_mock.call_count, 0)

    def test_inventory_subclass_return_rejected(self) -> None:
        class _ShapedInventory(PipelineStateInventory):
            pass

        shaped = _ShapedInventory(
            schema_version=self.inventory.schema_version,
            run_id=self.inventory.run_id,
            previous_run_id=self.inventory.previous_run_id,
            market_session=self.inventory.market_session,
            cutoff=self.inventory.cutoff,
            entries=self.inventory.entries,
            entry_count=self.inventory.entry_count,
            total_bytes=self.inventory.total_bytes,
        )
        call_order, run_patch, inventory_patch, publish_patch = self._patched(
            inventory_result=shaped
        )
        with run_patch, inventory_patch, publish_patch as publish_mock:
            with self.assertRaises(PinnedGCSStatePublicationServiceError):
                self._call()
        self.assertEqual(publish_mock.call_count, 0)

    def test_inventory_deleted_nested_field_rejected(self) -> None:
        mutated = dataclasses.replace(self.inventory)
        object.__delattr__(mutated.entries[0], "sha256")
        call_order, run_patch, inventory_patch, publish_patch = self._patched(
            inventory_result=mutated
        )
        with run_patch, inventory_patch, publish_patch as publish_mock:
            try:
                self._call()
                self.fail("expected PinnedGCSStatePublicationServiceError")
            except PinnedGCSStatePublicationServiceError as exc:
                self.assertIsNone(exc.__cause__)
                self.assertIsNone(exc.__context__)
        self.assertEqual(publish_mock.call_count, 0)

    def test_inventory_run_mismatch_rejected(self) -> None:
        # A structurally valid inventory (passes its own __post_init__) but
        # bound to a different run_id than self.run -- proves the wrapper's
        # own cross-check catches this, not just PipelineStateInventory's
        # internal validation.
        mismatched_inventory = PipelineStateInventory(
            schema_version=self.inventory.schema_version,
            run_id="f" * 64,
            previous_run_id=self.inventory.previous_run_id,
            market_session=self.inventory.market_session,
            cutoff=self.inventory.cutoff,
            entries=self.inventory.entries,
            entry_count=self.inventory.entry_count,
            total_bytes=self.inventory.total_bytes,
        )
        self.assertNotEqual(mismatched_inventory.run_id, self.run.run_id)

        call_order, run_patch, inventory_patch, publish_patch = self._patched(
            inventory_result=mismatched_inventory
        )
        with run_patch, inventory_patch, publish_patch as publish_mock:
            with self.assertRaises(PinnedGCSStatePublicationServiceError):
                self._call()
        self.assertEqual(publish_mock.call_count, 0)


class PublicationStageFailureTests(_ServiceTestCase):
    def test_publication_exception_sanitized(self) -> None:
        secret = "SECRET-PUBLICATION-FAILURE-DO-NOT-LEAK-8b2e"
        call_order, run_patch, inventory_patch, publish_patch = self._patched(
            publication_side_effect=StatePublicationError(secret)
        )
        with run_patch as run_mock, inventory_patch as inventory_mock, publish_patch as publish_mock:
            try:
                self._call()
                self.fail("expected PinnedGCSStatePublicationServiceError")
            except PinnedGCSStatePublicationServiceError as exc:
                self.assertNotIn(secret, str(exc))
                self.assertIsNone(exc.__cause__)
                self.assertIsNone(exc.__context__)
        self.assertEqual(run_mock.call_count, 1)
        self.assertEqual(inventory_mock.call_count, 1)
        self.assertEqual(publish_mock.call_count, 1)

    def test_publication_wrong_return_type_rejected(self) -> None:
        call_order, run_patch, inventory_patch, publish_patch = self._patched(
            publication_result="not-a-publication"
        )
        with run_patch, inventory_patch, publish_patch:
            with self.assertRaises(PinnedGCSStatePublicationServiceError):
                self._call()

    def test_publication_subclass_return_rejected(self) -> None:
        class _ShapedPublication(CompletedPipelineStatePublication):
            pass

        shaped = _ShapedPublication(
            manifest=self.publication.manifest,
            publication_object=self.publication.publication_object,
        )
        call_order, run_patch, inventory_patch, publish_patch = self._patched(
            publication_result=shaped
        )
        with run_patch, inventory_patch, publish_patch:
            with self.assertRaises(PinnedGCSStatePublicationServiceError):
                self._call()

    def test_publication_post_construction_mutation_rejected(self) -> None:
        mutated = CompletedPipelineStatePublication(
            manifest=self.publication.manifest,
            publication_object=self.publication.publication_object,
        )
        object.__setattr__(mutated.publication_object, "generation", 0)
        call_order, run_patch, inventory_patch, publish_patch = self._patched(
            publication_result=mutated
        )
        with run_patch, inventory_patch, publish_patch:
            try:
                self._call()
                self.fail("expected PinnedGCSStatePublicationServiceError")
            except PinnedGCSStatePublicationServiceError as exc:
                self.assertIsNone(exc.__cause__)
                self.assertIsNone(exc.__context__)

    def test_publication_mismatched_bucket_rejected_at_aggregate_stage(self) -> None:
        other_writer = RecordingWriter()
        mismatched_publication = publish_pipeline_state(
            self.run, self.inventory, self.roots, "a-completely-different-bucket", other_writer
        )
        call_order, run_patch, inventory_patch, publish_patch = self._patched(
            publication_result=mismatched_publication
        )
        with run_patch, inventory_patch, publish_patch:
            with self.assertRaises(PinnedGCSStatePublicationServiceError):
                self._call()


class NoRetryTests(_ServiceTestCase):
    def test_failing_boundary_called_at_most_once_per_invocation(self) -> None:
        call_order, run_patch, inventory_patch, publish_patch = self._patched(
            run_side_effect=PinnedGCSRunServiceError("boom")
        )
        with run_patch as run_mock, inventory_patch, publish_patch:
            with self.assertRaises(PinnedGCSStatePublicationServiceError):
                self._call()
            self.assertEqual(run_mock.call_count, 1)
            with self.assertRaises(PinnedGCSStatePublicationServiceError):
                self._call()
            self.assertEqual(run_mock.call_count, 2)


class CapabilityLockTests(unittest.TestCase):
    _ALLOWED_IMPORTS = frozenset(
        {
            "__future__",
            "dataclasses",
            "india_swing",
        }
    )
    _FORBIDDEN_TOKENS = (
        "list_blobs",
        "list_buckets",
        "get_bucket",
        "latest",
        "delete",
        "rewrite",
        "compose",
        "copy_blob",
        "overwrite",
        "broker",
        "place_order",
        "submit_order",
        "notification",
        "scheduler",
        "socket",
        "subprocess",
        "tempfile",
        "shutil",
        "environ",
        "getenv",
        "requests",
        "urllib",
        "unlink",
        "remove",
        "rename",
        "rmdir",
        "rmtree",
        "mkstemp",
        "mkdtemp",
        "popen",
        "system",
        "eval",
        "storage",
        "client",
        "signal",
        "confidence",
        "alert",
    )
    _EXACT_FORBIDDEN_NAMES = frozenset({"now", "open", "exec"})

    def _module_ast(self) -> ast.Module:
        source = _MODULE_PATH.read_text(encoding="utf-8")
        return ast.parse(source)

    def test_imports_match_an_exact_allowlist(self) -> None:
        tree = self._module_ast()
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    top = alias.name.split(".")[0]
                    self.assertIn(top, self._ALLOWED_IMPORTS, alias.name)
            elif isinstance(node, ast.ImportFrom):
                module = node.module or ""
                if node.level > 0:
                    self.assertIn("india_swing", self._ALLOWED_IMPORTS, module)
                    continue
                top = module.split(".")[0]
                self.assertIn(top, self._ALLOWED_IMPORTS, module)

    def test_identifiers_carry_no_disallowed_capability_token(self) -> None:
        tree = self._module_ast()
        for node in ast.walk(tree):
            name = None
            if isinstance(node, ast.Name):
                name = node.id
            elif isinstance(node, ast.Attribute):
                name = node.attr
            if name is None:
                continue
            lowered = name.lower()
            if lowered in self._EXACT_FORBIDDEN_NAMES:
                self.fail(f"forbidden exact identifier used: {name}")
            for token in self._FORBIDDEN_TOKENS:
                self.assertNotIn(token, lowered, name)

    def test_no_module_scope_call_expression(self) -> None:
        tree = self._module_ast()
        for node in tree.body:
            if isinstance(node, ast.Expr) and isinstance(node.value, ast.Call):
                self.fail("module-level call expression found")

    def test_no_storage_client_construction_at_all(self) -> None:
        source = _MODULE_PATH.read_text(encoding="utf-8")
        self.assertNotIn("google", source)
        self.assertNotIn("Client(", source)


if __name__ == "__main__":
    unittest.main()
