"""Manifest-last GCS publication, exact generation-pinned acquisition, and
fail-closed local hydration for the promoted operational input snapshot.

Composes -- never duplicates -- the pure models and local scan defined in
``promoted_operational_input_snapshot.py``, plus the already-accepted
``StateObjectWriter``/``GCSObjectReader`` ports and
``PromotedOperationalCloudRunControl``/``load_promoted_operational_assembly_spec_file``.
``state_root`` is never used as an input or filesystem/cloud capability in
this module. Its path value is passed opaquely into detached control
reconstructions because ``PromotedOperationalCloudRunControl`` requires the
field, but it is never statted, scanned, published, acquired, or hydrated.

Publication reconstructs one detached canonical control baseline before
any writer capability, builds and verifies the complete local snapshot
from that baseline, precomputes every entry-to-path mapping before the
writer is ever exposed, writes every unique blob, re-confirms the
caller's original control still canonically equals the detached baseline,
and only then seals the manifest last.

Restoration reads the manifest first, then every unique blob at its exact
pinned generation, holding all verified bytes in memory (bounded by the
existing 2 GiB total ceiling) before writing anything locally. Hydration
requires all twelve destinations to be exactly absent and to share one
common parent directory; it stages the complete verified tree under one
private randomized directory beneath that same parent, re-verifies the
staged tree with the accepted inventory builder and assembly loader, and
only then publishes each destination by one same-parent rename -- never
writing directly into a final destination, never overwriting, and never
cleaning up or repairing after a failure (the ephemeral container is
discarded instead).

This module performs no deployment, no live Cloud Run/IAM/Scheduler
configuration, no Telegram delivery, no Kite/broker call, and no bucket
listing or "latest" object selection -- every cloud-shaped capability
arrives only through an injected ``StateObjectWriter``/``GCSObjectReader``.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import stat
import tempfile
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path

from india_swing.daily_pipeline.acquisition import GCSObjectPayload, GCSObjectReader
from india_swing.daily_pipeline.state_publication import PublishedStateObject, StateObjectWriter
from india_swing._filesystem import FileSafetyError, read_stable_regular_file
from india_swing.promoted_operational_assembly import load_promoted_operational_assembly_spec_file
from india_swing.promoted_operational_cloud_control import PromotedOperationalCloudRunControl
from india_swing.promoted_operational_input_snapshot import (
    FILE_INPUT_NAME,
    INPUT_NAMES,
    MAXIMUM_ENCODED_BYTES,
    MAXIMUM_FILE_BYTES,
    ROOT_INPUT_NAMES,
    PromotedOperationalInputEntry,
    PromotedOperationalInputInventory,
    build_promoted_operational_input_inventory,
    decode_promoted_operational_input_inventory,
    encode_promoted_operational_input_inventory,
    input_destination_path,
    verify_hydrated_input_inventory,
)


class PromotedOperationalInputGCSError(ValueError):
    pass


_ERR = "promoted operational input gcs call is invalid"

_BUCKET = re.compile(r"[a-z0-9][a-z0-9._-]{1,61}[a-z0-9]\Z")
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_MANIFEST_PATH = re.compile(
    r"promoted-operational-input/v1/(\d{4}-\d{2}-\d{2})/([0-9a-f]{64})/"
    r"manifests/([0-9a-f]{64})\.json\Z"
)
_MAXIMUM_GENERATION = 9_223_372_036_854_775_807
_SCHEMA_VERSION = 1
_STAGING_PREFIX = ".promoted-operational-input-stage-"


def _validate_bucket(value: object) -> str:
    if type(value) is not str or _BUCKET.fullmatch(value) is None:
        raise PromotedOperationalInputGCSError(_ERR)
    return value


def _validate_sha256(value: object) -> str:
    if type(value) is not str or _SHA256.fullmatch(value) is None:
        raise PromotedOperationalInputGCSError(_ERR)
    return value


def _reconstructed_published_object(value: object) -> PublishedStateObject:
    if type(value) is not PublishedStateObject:
        raise PromotedOperationalInputGCSError(_ERR)
    reconstruction_failed = False
    reconstructed: PublishedStateObject | None = None
    try:
        reconstructed = PublishedStateObject(
            object_name=value.object_name,
            generation=value.generation,
            byte_count=value.byte_count,
            sha256=value.sha256,
        )
    except Exception:
        reconstruction_failed = True
    if reconstruction_failed or reconstructed is None:
        raise PromotedOperationalInputGCSError(_ERR)
    return reconstructed


def _blob_object_name(target_session: date, assembly_spec_id: str, sha256_hash: str) -> str:
    return f"promoted-operational-input/v1/{target_session.isoformat()}/{assembly_spec_id}/blobs/{sha256_hash}"


def _manifest_object_name(target_session: date, assembly_spec_id: str, snapshot_id: str) -> str:
    return (
        f"promoted-operational-input/v1/{target_session.isoformat()}/{assembly_spec_id}/"
        f"manifests/{snapshot_id}.json"
    )


def _reconstructed_inventory(value: object) -> PromotedOperationalInputInventory:
    if type(value) is not PromotedOperationalInputInventory:
        raise PromotedOperationalInputGCSError(_ERR)
    reconstruction_failed = False
    reconstructed: PromotedOperationalInputInventory | None = None
    try:
        reconstructed = PromotedOperationalInputInventory(
            schema_version=value.schema_version,
            expected_assembly_spec_id=value.expected_assembly_spec_id,
            target_session=value.target_session,
            input_names=value.input_names,
            entries=value.entries,
            entry_count=value.entry_count,
            total_bytes=value.total_bytes,
        )
    except Exception:
        reconstruction_failed = True
    if reconstruction_failed or reconstructed is None:
        raise PromotedOperationalInputGCSError(_ERR)
    return reconstructed


def _detached_control(control: object) -> PromotedOperationalCloudRunControl:
    """Reconstruct one fully detached, independently verified
    ``PromotedOperationalCloudRunControl`` from a caller-supplied instance
    so a later ``object.__setattr__``/alias mutation of the caller's own
    object can never redirect an operation already anchored to this
    baseline."""

    if type(control) is not PromotedOperationalCloudRunControl:
        raise PromotedOperationalInputGCSError(_ERR)
    detach_failed = False
    detached: PromotedOperationalCloudRunControl | None = None
    try:
        detached = PromotedOperationalCloudRunControl(
            expected_assembly_spec_id=control.expected_assembly_spec_id,
            expected_operational_run_spec_id=control.expected_operational_run_spec_id,
            target_session=control.target_session,
            state_bucket=control.state_bucket,
            assembly_spec_file=control.assembly_spec_file,
            prior_state_restore=control.prior_state_restore,
            state_root=control.state_root,
            **{name: getattr(control, name) for name in ROOT_INPUT_NAMES},
        )
    except Exception:
        detach_failed = True
    if detach_failed or detached is None:
        raise PromotedOperationalInputGCSError(_ERR)
    return detached


def _validate_manifest_state(
    candidate: "PromotedOperationalInputSnapshotManifest",
) -> tuple[PromotedOperationalInputInventory, tuple[PublishedStateObject, ...]]:
    if type(candidate.schema_version) is not int or candidate.schema_version != _SCHEMA_VERSION:
        raise PromotedOperationalInputGCSError(_ERR)
    _validate_bucket(candidate.bucket)

    inventory = _reconstructed_inventory(candidate.inventory)

    if type(candidate.blob_objects) is not tuple:
        raise PromotedOperationalInputGCSError(_ERR)
    reconstructed_blobs = tuple(
        _reconstructed_published_object(item) for item in candidate.blob_objects
    )

    expected_sizes: dict[str, int] = {}
    for entry in inventory.entries:
        prior = expected_sizes.get(entry.sha256)
        if prior is not None and prior != entry.byte_count:
            raise PromotedOperationalInputGCSError(_ERR)
        expected_sizes[entry.sha256] = entry.byte_count
    unique_hashes = sorted(expected_sizes)

    previous_sha: str | None = None
    seen: set[str] = set()
    for blob in reconstructed_blobs:
        if blob.sha256 in seen:
            raise PromotedOperationalInputGCSError(_ERR)
        seen.add(blob.sha256)
        if previous_sha is not None and not previous_sha < blob.sha256:
            raise PromotedOperationalInputGCSError(_ERR)
        previous_sha = blob.sha256
        if blob.object_name != _blob_object_name(
            inventory.target_session, inventory.expected_assembly_spec_id, blob.sha256
        ):
            raise PromotedOperationalInputGCSError(_ERR)
        if blob.byte_count != expected_sizes.get(blob.sha256):
            raise PromotedOperationalInputGCSError(_ERR)

    if seen != set(unique_hashes):
        raise PromotedOperationalInputGCSError(_ERR)

    return inventory, reconstructed_blobs


def _published_body(value: PublishedStateObject) -> dict[str, object]:
    return {
        "byte_count": value.byte_count,
        "generation": value.generation,
        "object_name": value.object_name,
        "sha256": value.sha256,
    }


def _manifest_body(
    value: "PromotedOperationalInputSnapshotManifest", *, include_snapshot_id: bool
) -> dict[str, object]:
    inventory_bytes = encode_promoted_operational_input_inventory(value.inventory)
    inventory_dict = json.loads(inventory_bytes.decode("utf-8"))
    body: dict[str, object] = {
        "bucket": value.bucket,
        "blob_objects": [_published_body(item) for item in value.blob_objects],
        "inventory": inventory_dict,
        "schema_version": value.schema_version,
    }
    if include_snapshot_id:
        body["snapshot_id"] = value.snapshot_id
    return body


def _canonical_manifest_json_bytes(value: dict[str, object]) -> bytes:
    return (
        json.dumps(value, ensure_ascii=False, allow_nan=False, separators=(",", ":"), sort_keys=True)
        + "\n"
    ).encode("utf-8")


@dataclass(frozen=True, slots=True)
class PromotedOperationalInputSnapshotManifest:
    """Immutable, content-addressed publication manifest binding a bucket,
    the pure local inventory, and the exact deduplicated
    ``PublishedStateObject`` returned for each unique blob."""

    schema_version: int
    bucket: str
    inventory: PromotedOperationalInputInventory
    blob_objects: tuple[PublishedStateObject, ...]
    snapshot_id: str = field(init=False)

    def __post_init__(self) -> None:
        inventory, blob_objects = _validate_manifest_state(self)
        object.__setattr__(self, "inventory", inventory)
        object.__setattr__(self, "blob_objects", blob_objects)
        object.__setattr__(self, "snapshot_id", self._calculated_id())

    def _calculated_id(self) -> str:
        id_failed = False
        digest = ""
        try:
            body_bytes = _canonical_manifest_json_bytes(_manifest_body(self, include_snapshot_id=False))
            digest = hashlib.sha256(body_bytes).hexdigest()
        except Exception:
            id_failed = True
        if id_failed:
            raise PromotedOperationalInputGCSError(_ERR)
        return digest

    def verify_content_identity(self) -> None:
        failed = False
        try:
            if type(self) is not PromotedOperationalInputSnapshotManifest:
                raise PromotedOperationalInputGCSError(_ERR)
            _validate_manifest_state(self)
            if self.snapshot_id != self._calculated_id():
                raise PromotedOperationalInputGCSError(_ERR)
        except Exception:
            failed = True
        if failed:
            raise PromotedOperationalInputGCSError(_ERR)


def _reconstructed_manifest(value: object) -> PromotedOperationalInputSnapshotManifest:
    """Every aggregate that retains a manifest stores a freshly
    reconstructed exact instance -- never merely a ``verify_content_identity``
    call against the caller-owned alias, which a later
    ``object.__setattr__`` on that alias could still silently redirect."""

    if type(value) is not PromotedOperationalInputSnapshotManifest:
        raise PromotedOperationalInputGCSError(_ERR)
    reconstruction_failed = False
    reconstructed: PromotedOperationalInputSnapshotManifest | None = None
    try:
        reconstructed = PromotedOperationalInputSnapshotManifest(
            schema_version=value.schema_version,
            bucket=value.bucket,
            inventory=value.inventory,
            blob_objects=value.blob_objects,
        )
        if reconstructed.snapshot_id != value.snapshot_id:
            raise PromotedOperationalInputGCSError(_ERR)
    except Exception:
        reconstruction_failed = True
    if reconstruction_failed or reconstructed is None:
        raise PromotedOperationalInputGCSError(_ERR)
    return reconstructed


def promoted_operational_input_manifest_object_name(
    manifest: PromotedOperationalInputSnapshotManifest,
) -> str:
    if type(manifest) is not PromotedOperationalInputSnapshotManifest:
        raise PromotedOperationalInputGCSError(_ERR)
    manifest.verify_content_identity()
    return _manifest_object_name(
        manifest.inventory.target_session, manifest.inventory.expected_assembly_spec_id, manifest.snapshot_id
    )


def encode_promoted_operational_input_snapshot_manifest(
    value: PromotedOperationalInputSnapshotManifest,
) -> bytes:
    if type(value) is not PromotedOperationalInputSnapshotManifest:
        raise PromotedOperationalInputGCSError(_ERR)
    value.verify_content_identity()
    encode_failed = False
    payload = b""
    try:
        payload = _canonical_manifest_json_bytes(_manifest_body(value, include_snapshot_id=True))
    except Exception:
        encode_failed = True
    if encode_failed:
        raise PromotedOperationalInputGCSError(_ERR)
    if len(payload) > MAXIMUM_ENCODED_BYTES:
        raise PromotedOperationalInputGCSError(_ERR)
    return payload


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise PromotedOperationalInputGCSError(_ERR)
        result[key] = value
    return result


def _reject_number(_token: str) -> object:
    raise PromotedOperationalInputGCSError(_ERR)


def decode_promoted_operational_input_snapshot_manifest(
    payload: bytes,
) -> PromotedOperationalInputSnapshotManifest:
    if type(payload) is not bytes or not (0 < len(payload) <= MAXIMUM_ENCODED_BYTES):
        raise PromotedOperationalInputGCSError(_ERR)

    decode_failed = False
    manifest: PromotedOperationalInputSnapshotManifest | None = None
    try:
        text = payload.decode("utf-8", errors="strict")
        raw = json.loads(
            text, object_pairs_hook=_unique_object, parse_float=_reject_number, parse_constant=_reject_number,
        )
        if type(raw) is not dict or set(raw) != {
            "bucket", "blob_objects", "inventory", "schema_version", "snapshot_id",
        }:
            raise PromotedOperationalInputGCSError(_ERR)

        raw_blob_objects = raw["blob_objects"]
        if type(raw_blob_objects) is not list:
            raise PromotedOperationalInputGCSError(_ERR)
        blob_objects: list[PublishedStateObject] = []
        for raw_blob in raw_blob_objects:
            if type(raw_blob) is not dict or set(raw_blob) != {
                "byte_count", "generation", "object_name", "sha256",
            }:
                raise PromotedOperationalInputGCSError(_ERR)
            blob_objects.append(PublishedStateObject(**raw_blob))

        inventory_bytes = _canonical_manifest_json_bytes(raw["inventory"])
        inventory = decode_promoted_operational_input_inventory(inventory_bytes)

        manifest = PromotedOperationalInputSnapshotManifest(
            schema_version=raw["schema_version"],
            bucket=raw["bucket"],
            inventory=inventory,
            blob_objects=tuple(blob_objects),
        )
        if manifest.snapshot_id != raw["snapshot_id"]:
            raise PromotedOperationalInputGCSError(_ERR)
        if encode_promoted_operational_input_snapshot_manifest(manifest) != payload:
            raise PromotedOperationalInputGCSError(_ERR)
    except Exception:
        decode_failed = True
    if decode_failed or manifest is None:
        raise PromotedOperationalInputGCSError(_ERR)
    return manifest


@dataclass(frozen=True, slots=True)
class CompletedPromotedOperationalInputPublication:
    manifest: PromotedOperationalInputSnapshotManifest
    manifest_object: PublishedStateObject

    def __post_init__(self) -> None:
        manifest = _reconstructed_manifest(self.manifest)
        published = _reconstructed_published_object(self.manifest_object)
        payload = encode_promoted_operational_input_snapshot_manifest(manifest)
        if (
            published.object_name != promoted_operational_input_manifest_object_name(manifest)
            or published.byte_count != len(payload)
            or published.sha256 != hashlib.sha256(payload).hexdigest()
        ):
            raise PromotedOperationalInputGCSError(_ERR)
        object.__setattr__(self, "manifest", manifest)
        object.__setattr__(self, "manifest_object", published)


def publish_promoted_operational_input_snapshot(
    *, control: PromotedOperationalCloudRunControl, bucket: str, writer: StateObjectWriter,
) -> CompletedPromotedOperationalInputPublication:
    """Reconstruct one detached control baseline before any writer
    capability, build and independently verify the complete local
    snapshot from that baseline, load and re-verify the assembly spec,
    precompute every entry-to-path mapping so no path is ever re-read
    from the caller's original (possibly-mutated) control after the first
    writer call, write every unique blob, re-confirm the caller's control
    still canonically equals the detached baseline, and only then seal
    the manifest last. If any blob write, assembly re-check, or
    end-of-publication control re-check fails, the manifest is never
    attempted."""

    if type(control) is not PromotedOperationalCloudRunControl:
        raise PromotedOperationalInputGCSError(_ERR)
    control_failed = False
    try:
        control.verify_content_identity()
    except Exception:
        control_failed = True
    if control_failed:
        raise PromotedOperationalInputGCSError(_ERR)
    bucket = _validate_bucket(bucket)

    baseline_failed = False
    baseline_control: PromotedOperationalCloudRunControl | None = None
    try:
        baseline_control = _detached_control(control)
    except Exception:
        baseline_failed = True
    if baseline_failed or baseline_control is None:
        raise PromotedOperationalInputGCSError(_ERR)

    inventory_failed = False
    inventory: PromotedOperationalInputInventory | None = None
    try:
        inventory = build_promoted_operational_input_inventory(baseline_control)
    except Exception:
        inventory_failed = True
    if inventory_failed or inventory is None:
        raise PromotedOperationalInputGCSError(_ERR)

    assembly_failed = False
    try:
        assembly_spec = load_promoted_operational_assembly_spec_file(baseline_control.assembly_spec_file)
        if (
            assembly_spec.assembly_spec_id != baseline_control.expected_assembly_spec_id
            or assembly_spec.target_session != baseline_control.target_session
            or assembly_spec.binding_bucket != baseline_control.state_bucket
        ):
            raise PromotedOperationalInputGCSError(_ERR)
        assembly_entry = next(e for e in inventory.entries if e.input_name == FILE_INPUT_NAME)
        reread_payload = read_stable_regular_file(
            baseline_control.assembly_spec_file, maximum_bytes=MAXIMUM_FILE_BYTES
        )
        if (
            len(reread_payload) != assembly_entry.byte_count
            or hashlib.sha256(reread_payload).hexdigest() != assembly_entry.sha256
        ):
            raise PromotedOperationalInputGCSError(_ERR)
    except Exception:
        assembly_failed = True
    if assembly_failed:
        raise PromotedOperationalInputGCSError(_ERR)

    path_failed = False
    paths_by_index: dict[int, Path] = {}
    try:
        for index, entry in enumerate(inventory.entries):
            paths_by_index[index] = input_destination_path(baseline_control, entry)
    except Exception:
        path_failed = True
    if path_failed:
        raise PromotedOperationalInputGCSError(_ERR)

    write_failed = False
    manifest: PromotedOperationalInputSnapshotManifest | None = None
    manifest_object: PublishedStateObject | None = None
    try:
        published_by_hash: dict[str, PublishedStateObject] = {}
        for index, entry in enumerate(inventory.entries):
            if entry.sha256 in published_by_hash:
                continue
            destination = paths_by_index[index]
            read_failed = False
            payload = b""
            try:
                payload = read_stable_regular_file(destination, maximum_bytes=MAXIMUM_FILE_BYTES)
            except FileSafetyError:
                read_failed = True
            if (
                read_failed
                or len(payload) != entry.byte_count
                or hashlib.sha256(payload).hexdigest() != entry.sha256
            ):
                write_failed = True
                break
            object_name = _blob_object_name(
                inventory.target_session, inventory.expected_assembly_spec_id, entry.sha256
            )
            published = writer.create_or_verify(
                bucket=bucket, object_name=object_name, content_bytes=payload,
                content_type="application/octet-stream", maximum_bytes=MAXIMUM_FILE_BYTES,
            )
            if (
                type(published) is not PublishedStateObject
                or published.object_name != object_name
                or published.byte_count != len(payload)
                or published.sha256 != entry.sha256
            ):
                write_failed = True
                break
            published_by_hash[entry.sha256] = published

        if not write_failed:
            unique_hashes = sorted({entry.sha256 for entry in inventory.entries})
            if set(published_by_hash) != set(unique_hashes):
                write_failed = True
            else:
                rebaseline_failed = False
                try:
                    control.verify_content_identity()
                    rebaseline = _detached_control(control)
                    if rebaseline.control_id != baseline_control.control_id:
                        rebaseline_failed = True
                except Exception:
                    rebaseline_failed = True
                if rebaseline_failed:
                    write_failed = True
                else:
                    blob_objects = tuple(published_by_hash[h] for h in unique_hashes)
                    manifest = PromotedOperationalInputSnapshotManifest(
                        schema_version=_SCHEMA_VERSION, bucket=bucket, inventory=inventory, blob_objects=blob_objects,
                    )
                    manifest_payload = encode_promoted_operational_input_snapshot_manifest(manifest)
                    manifest_object = writer.create_or_verify(
                        bucket=bucket, object_name=promoted_operational_input_manifest_object_name(manifest),
                        content_bytes=manifest_payload, content_type="application/json",
                        maximum_bytes=MAXIMUM_ENCODED_BYTES,
                    )
                    if type(manifest_object) is not PublishedStateObject:
                        write_failed = True
    except Exception:
        write_failed = True
    if write_failed or manifest is None or manifest_object is None:
        raise PromotedOperationalInputGCSError(_ERR)
    return CompletedPromotedOperationalInputPublication(manifest=manifest, manifest_object=manifest_object)


@dataclass(frozen=True, slots=True)
class PromotedOperationalInputRestoreRequest:
    bucket: str
    manifest_object_name: str
    generation: int
    expected_sha256: str
    expected_snapshot_id: str
    expected_assembly_spec_id: str
    target_session: date

    def __post_init__(self) -> None:
        _validate_bucket(self.bucket)
        _validate_sha256(self.expected_sha256)
        _validate_sha256(self.expected_snapshot_id)
        _validate_sha256(self.expected_assembly_spec_id)
        if (
            type(self.generation) is not int
            or self.generation <= 0
            or self.generation > _MAXIMUM_GENERATION
        ):
            raise PromotedOperationalInputGCSError(_ERR)
        if type(self.target_session) is not date:
            raise PromotedOperationalInputGCSError(_ERR)
        if type(self.manifest_object_name) is not str:
            raise PromotedOperationalInputGCSError(_ERR)
        match = _MANIFEST_PATH.fullmatch(self.manifest_object_name)
        if (
            match is None
            or match.group(1) != self.target_session.isoformat()
            or match.group(2) != self.expected_assembly_spec_id
            or match.group(3) != self.expected_snapshot_id
        ):
            raise PromotedOperationalInputGCSError(_ERR)


def _reconstructed_request(value: object) -> PromotedOperationalInputRestoreRequest:
    if type(value) is not PromotedOperationalInputRestoreRequest:
        raise PromotedOperationalInputGCSError(_ERR)
    reconstruction_failed = False
    reconstructed: PromotedOperationalInputRestoreRequest | None = None
    try:
        reconstructed = PromotedOperationalInputRestoreRequest(
            bucket=value.bucket,
            manifest_object_name=value.manifest_object_name,
            generation=value.generation,
            expected_sha256=value.expected_sha256,
            expected_snapshot_id=value.expected_snapshot_id,
            expected_assembly_spec_id=value.expected_assembly_spec_id,
            target_session=value.target_session,
        )
    except Exception:
        reconstruction_failed = True
    if reconstruction_failed or reconstructed is None:
        raise PromotedOperationalInputGCSError(_ERR)
    return reconstructed


@dataclass(frozen=True, slots=True)
class AcquiredPromotedOperationalInputBlob:
    published_object: PublishedStateObject
    content_bytes: bytes

    def __post_init__(self) -> None:
        published_object = _reconstructed_published_object(self.published_object)
        if (
            type(self.content_bytes) is not bytes
            or not (0 < len(self.content_bytes) <= MAXIMUM_FILE_BYTES)
            or len(self.content_bytes) != published_object.byte_count
            or hashlib.sha256(self.content_bytes).hexdigest() != published_object.sha256
        ):
            raise PromotedOperationalInputGCSError(_ERR)
        object.__setattr__(self, "published_object", published_object)


def _reconstructed_blob(value: object) -> AcquiredPromotedOperationalInputBlob:
    if type(value) is not AcquiredPromotedOperationalInputBlob:
        raise PromotedOperationalInputGCSError(_ERR)
    reconstruction_failed = False
    reconstructed: AcquiredPromotedOperationalInputBlob | None = None
    try:
        reconstructed = AcquiredPromotedOperationalInputBlob(
            published_object=value.published_object, content_bytes=value.content_bytes,
        )
    except Exception:
        reconstruction_failed = True
    if reconstruction_failed or reconstructed is None:
        raise PromotedOperationalInputGCSError(_ERR)
    return reconstructed


@dataclass(frozen=True, slots=True)
class AcquiredPromotedOperationalInputSnapshot:
    request: PromotedOperationalInputRestoreRequest
    manifest: PromotedOperationalInputSnapshotManifest
    blobs: tuple[AcquiredPromotedOperationalInputBlob, ...]

    def __post_init__(self) -> None:
        request = _reconstructed_request(self.request)
        manifest = _reconstructed_manifest(self.manifest)

        if type(self.blobs) is not tuple:
            raise PromotedOperationalInputGCSError(_ERR)
        reconstruction_failed = False
        reconstructed_blobs: tuple[AcquiredPromotedOperationalInputBlob, ...] = ()
        try:
            reconstructed_blobs = tuple(_reconstructed_blob(item) for item in self.blobs)
        except Exception:
            reconstruction_failed = True
        if reconstruction_failed:
            raise PromotedOperationalInputGCSError(_ERR)

        expected_keys = tuple(
            (item.object_name, item.generation, item.byte_count, item.sha256)
            for item in manifest.blob_objects
        )
        observed_keys = tuple(
            (
                item.published_object.object_name,
                item.published_object.generation,
                item.published_object.byte_count,
                item.published_object.sha256,
            )
            for item in reconstructed_blobs
        )
        if observed_keys != expected_keys:
            raise PromotedOperationalInputGCSError(_ERR)

        if (
            manifest.inventory.expected_assembly_spec_id != request.expected_assembly_spec_id
            or manifest.inventory.target_session != request.target_session
            or manifest.bucket != request.bucket
            or manifest.snapshot_id != request.expected_snapshot_id
            or promoted_operational_input_manifest_object_name(manifest) != request.manifest_object_name
        ):
            raise PromotedOperationalInputGCSError(_ERR)

        object.__setattr__(self, "request", request)
        object.__setattr__(self, "manifest", manifest)
        object.__setattr__(self, "blobs", reconstructed_blobs)


def acquire_promoted_operational_input_snapshot(
    *, request: PromotedOperationalInputRestoreRequest, reader: GCSObjectReader,
) -> AcquiredPromotedOperationalInputSnapshot:
    """Reads the exact pinned manifest generation first, verified against
    the externally pinned SHA-256, then reads exactly the manifest's own
    unique blob set at each pinned generation. No bucket listing or latest
    selection exists on this interface."""

    request = _reconstructed_request(request)

    read_failed = False
    manifest: PromotedOperationalInputSnapshotManifest | None = None
    blobs: list[AcquiredPromotedOperationalInputBlob] = []
    try:
        manifest_payload = reader.read_generation(
            bucket=request.bucket, object_name=request.manifest_object_name,
            generation=request.generation, maximum_bytes=MAXIMUM_ENCODED_BYTES,
        )
        if (
            type(manifest_payload) is not GCSObjectPayload
            or type(manifest_payload.generation) is not int
            or manifest_payload.generation != request.generation
            or type(manifest_payload.content_bytes) is not bytes
            or not (0 < len(manifest_payload.content_bytes) <= MAXIMUM_ENCODED_BYTES)
            or hashlib.sha256(manifest_payload.content_bytes).hexdigest() != request.expected_sha256
        ):
            read_failed = True
        else:
            manifest = decode_promoted_operational_input_snapshot_manifest(manifest_payload.content_bytes)
            if (
                manifest.bucket != request.bucket
                or manifest.inventory.expected_assembly_spec_id != request.expected_assembly_spec_id
                or manifest.inventory.target_session != request.target_session
                or manifest.snapshot_id != request.expected_snapshot_id
                or promoted_operational_input_manifest_object_name(manifest) != request.manifest_object_name
            ):
                read_failed = True
            else:
                for blob_object in manifest.blob_objects:
                    payload = reader.read_generation(
                        bucket=request.bucket, object_name=blob_object.object_name,
                        generation=blob_object.generation, maximum_bytes=MAXIMUM_FILE_BYTES,
                    )
                    if (
                        type(payload) is not GCSObjectPayload
                        or type(payload.generation) is not int
                        or payload.generation != blob_object.generation
                        or type(payload.content_bytes) is not bytes
                        or not (0 < len(payload.content_bytes) <= MAXIMUM_FILE_BYTES)
                        or len(payload.content_bytes) != blob_object.byte_count
                        or hashlib.sha256(payload.content_bytes).hexdigest() != blob_object.sha256
                    ):
                        read_failed = True
                        break
                    blobs.append(
                        AcquiredPromotedOperationalInputBlob(
                            published_object=blob_object, content_bytes=payload.content_bytes,
                        )
                    )
    except Exception:
        read_failed = True
    if read_failed or manifest is None:
        raise PromotedOperationalInputGCSError(_ERR)

    aggregate_failed = False
    result: AcquiredPromotedOperationalInputSnapshot | None = None
    try:
        result = AcquiredPromotedOperationalInputSnapshot(
            request=request, manifest=manifest, blobs=tuple(blobs),
        )
    except Exception:
        aggregate_failed = True
    if aggregate_failed or result is None:
        raise PromotedOperationalInputGCSError(_ERR)
    return result


@dataclass(frozen=True, slots=True)
class CompletedPromotedOperationalInputRestore:
    request: PromotedOperationalInputRestoreRequest
    manifest: PromotedOperationalInputSnapshotManifest

    def __post_init__(self) -> None:
        request = _reconstructed_request(self.request)
        manifest = _reconstructed_manifest(self.manifest)
        if (
            manifest.inventory.expected_assembly_spec_id != request.expected_assembly_spec_id
            or manifest.inventory.target_session != request.target_session
            or manifest.bucket != request.bucket
            or manifest.snapshot_id != request.expected_snapshot_id
        ):
            raise PromotedOperationalInputGCSError(_ERR)
        object.__setattr__(self, "request", request)
        object.__setattr__(self, "manifest", manifest)


def _is_link_like(status: os.stat_result) -> bool:
    if stat.S_ISLNK(status.st_mode):
        return True
    attribute = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400)
    return bool(getattr(status, "st_file_attributes", 0) & attribute)


def _lstat(path: Path) -> os.stat_result:
    failed = False
    status: os.stat_result | None = None
    try:
        status = os.lstat(path)
    except OSError:
        failed = True
    if failed or status is None:
        raise PromotedOperationalInputGCSError(_ERR)
    return status


def _path_exists(path: Path) -> bool:
    try:
        os.lstat(path)
    except FileNotFoundError:
        return False
    except OSError:
        raise PromotedOperationalInputGCSError(_ERR)
    return True


def _directory_identity(status: os.stat_result) -> tuple[int, int, int, int]:
    return (status.st_dev, status.st_ino, status.st_size, status.st_mtime_ns)


def _write_verified_file(path: Path, content_bytes: bytes) -> None:
    """Adapted from the accepted daily-pipeline state_restoration.py
    discipline: exclusive creation, fstat-based single-link/regular-file
    identity verification, an exact write-length check, and an explicit
    flush/fsync before the descriptor is ever trusted."""

    flags = (
        os.O_WRONLY | os.O_CREAT | os.O_EXCL
        | getattr(os, "O_BINARY", 0) | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NOINHERIT", 0) | getattr(os, "O_NOFOLLOW", 0)
    )
    descriptor: int | None = None
    failed = False
    try:
        descriptor = os.open(path, flags, 0o600)
        with os.fdopen(descriptor, "wb", closefd=True) as handle:
            descriptor = None
            opened = os.fstat(handle.fileno())
            if not stat.S_ISREG(opened.st_mode) or _is_link_like(opened) or opened.st_nlink != 1:
                raise OSError()
            written = handle.write(content_bytes)
            if written != len(content_bytes):
                raise OSError()
            handle.flush()
            os.fsync(handle.fileno())
            after_write = os.fstat(handle.fileno())
            current = os.lstat(path)
            if (
                (opened.st_dev, opened.st_ino) != (after_write.st_dev, after_write.st_ino)
                or (opened.st_dev, opened.st_ino) != (current.st_dev, current.st_ino)
                or after_write.st_size != len(content_bytes)
                or not stat.S_ISREG(current.st_mode)
                or _is_link_like(current)
                or after_write.st_nlink != 1
                or current.st_nlink != 1
            ):
                raise OSError()
    except OSError:
        failed = True
    finally:
        if descriptor is not None:
            try:
                os.close(descriptor)
            except OSError:
                failed = True
    if failed:
        raise PromotedOperationalInputGCSError(_ERR)


def _fsync_directory(path: Path) -> None:
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    descriptor: int | None = None
    try:
        descriptor = os.open(path, flags)
        os.fsync(descriptor)
    except OSError:
        pass
    finally:
        if descriptor is not None:
            try:
                os.close(descriptor)
            except OSError:
                pass


def _verify_common_parent_identity(parent: Path, expected_identity: tuple[int, int]) -> None:
    status = _lstat(parent)
    if (
        not stat.S_ISDIR(status.st_mode)
        or _is_link_like(status)
        or (status.st_dev, status.st_ino) != expected_identity
    ):
        raise PromotedOperationalInputGCSError(_ERR)


def _verify_staging_identity(staging: Path, expected_identity: tuple[int, int]) -> None:
    status = _lstat(staging)
    if (
        not stat.S_ISDIR(status.st_mode)
        or _is_link_like(status)
        or (status.st_dev, status.st_ino) != expected_identity
    ):
        raise PromotedOperationalInputGCSError(_ERR)


def _common_parent(control: PromotedOperationalCloudRunControl) -> tuple[Path, tuple[int, int]]:
    """All twelve destinations must share one existing, non-root, non-link
    common parent directory whose exact identity is captured here and
    rechecked immediately before every later rename."""

    destinations = [control.assembly_spec_file] + [
        getattr(control, name) for name in ROOT_INPUT_NAMES
    ]
    parents = {path.parent for path in destinations}
    if len(parents) != 1:
        raise PromotedOperationalInputGCSError(_ERR)
    parent = next(iter(parents))
    if parent == parent.parent:
        raise PromotedOperationalInputGCSError(_ERR)
    basenames = [path.name for path in destinations]
    if len(set(basenames)) != len(basenames):
        raise PromotedOperationalInputGCSError(_ERR)
    status = _lstat(parent)
    if not stat.S_ISDIR(status.st_mode) or _is_link_like(status):
        raise PromotedOperationalInputGCSError(_ERR)
    return parent, (status.st_dev, status.st_ino)


def _staging_control(
    control: PromotedOperationalCloudRunControl, staging: Path
) -> PromotedOperationalCloudRunControl:
    build_failed = False
    result: PromotedOperationalCloudRunControl | None = None
    try:
        result = PromotedOperationalCloudRunControl(
            expected_assembly_spec_id=control.expected_assembly_spec_id,
            expected_operational_run_spec_id=control.expected_operational_run_spec_id,
            target_session=control.target_session,
            state_bucket=control.state_bucket,
            assembly_spec_file=staging / control.assembly_spec_file.name,
            state_root=control.state_root,
            **{name: staging / getattr(control, name).name for name in ROOT_INPUT_NAMES},
        )
    except Exception:
        build_failed = True
    if build_failed or result is None:
        raise PromotedOperationalInputGCSError(_ERR)
    return result


def _create_staged_tree(
    staging_control: PromotedOperationalCloudRunControl,
    manifest: PromotedOperationalInputSnapshotManifest,
    content_by_hash: dict[str, bytes],
) -> None:
    for root_name in ROOT_INPUT_NAMES:
        root_path = getattr(staging_control, root_name)
        try:
            root_path.mkdir()
        except OSError as exc:
            raise PromotedOperationalInputGCSError(_ERR) from None
        status = _lstat(root_path)
        if not stat.S_ISDIR(status.st_mode) or _is_link_like(status):
            raise PromotedOperationalInputGCSError(_ERR)

    for entry in manifest.inventory.entries:
        destination = input_destination_path(staging_control, entry)
        if entry.input_name != FILE_INPUT_NAME:
            try:
                destination.parent.mkdir(parents=True, exist_ok=True)
            except OSError:
                raise PromotedOperationalInputGCSError(_ERR) from None
        _write_verified_file(destination, content_by_hash[entry.sha256])

    for root_name in ROOT_INPUT_NAMES:
        _fsync_directory(getattr(staging_control, root_name))
        for sub in sorted(getattr(staging_control, root_name).rglob("*"), key=lambda p: len(p.parts), reverse=True):
            if sub.is_dir():
                _fsync_directory(sub)
    _fsync_directory(staging_control.assembly_spec_file.parent)


def hydrate_promoted_operational_input_snapshot(
    *, control: PromotedOperationalCloudRunControl, acquired: AcquiredPromotedOperationalInputSnapshot,
) -> CompletedPromotedOperationalInputRestore:
    """Hydrates exactly ``control.assembly_spec_file`` and the eleven
    fixed roots from an already fully acquired-and-verified in-memory
    snapshot, targeting a fresh ephemeral filesystem.

    All twelve destinations must be exactly absent and must share one
    existing common parent directory. The complete verified layout is
    built inside one private randomized staging directory under that same
    parent, re-verified there with the accepted inventory builder and
    assembly loader, and only then published by one same-parent rename per
    destination -- re-checking the common parent's identity and each
    destination's absence immediately before its own rename. Because
    twelve top-level renames cannot be one filesystem transaction, any
    failure during or after staging returns only the static sanitized
    error, never a completed result, and performs no cleanup or repair --
    the caller/container must discard the ephemeral common parent.
    ``state_root`` is never statted, created, staged, renamed, or
    represented anywhere in this function."""

    if type(control) is not PromotedOperationalCloudRunControl:
        raise PromotedOperationalInputGCSError(_ERR)
    baseline_failed = False
    baseline_control: PromotedOperationalCloudRunControl | None = None
    try:
        control.verify_content_identity()
        baseline_control = _detached_control(control)
    except Exception:
        baseline_failed = True
    if baseline_failed or baseline_control is None:
        raise PromotedOperationalInputGCSError(_ERR)

    if type(acquired) is not AcquiredPromotedOperationalInputSnapshot:
        raise PromotedOperationalInputGCSError(_ERR)
    acquired_failed = False
    try:
        acquired = AcquiredPromotedOperationalInputSnapshot(
            request=acquired.request, manifest=acquired.manifest, blobs=acquired.blobs,
        )
    except Exception:
        acquired_failed = True
    if acquired_failed:
        raise PromotedOperationalInputGCSError(_ERR)

    manifest = acquired.manifest
    if (
        manifest.inventory.expected_assembly_spec_id != baseline_control.expected_assembly_spec_id
        or manifest.inventory.target_session != baseline_control.target_session
        or manifest.bucket != baseline_control.state_bucket
    ):
        raise PromotedOperationalInputGCSError(_ERR)

    bind_failed = False
    content_by_hash: dict[str, bytes] = {}
    try:
        for blob in acquired.blobs:
            sha256_hash = blob.published_object.sha256
            if sha256_hash in content_by_hash:
                raise PromotedOperationalInputGCSError(_ERR)
            content_by_hash[sha256_hash] = blob.content_bytes
        for entry in manifest.inventory.entries:
            if entry.sha256 not in content_by_hash:
                raise PromotedOperationalInputGCSError(_ERR)
            if len(content_by_hash[entry.sha256]) != entry.byte_count:
                raise PromotedOperationalInputGCSError(_ERR)
    except Exception:
        bind_failed = True
    if bind_failed:
        raise PromotedOperationalInputGCSError(_ERR)

    common_parent_failed = False
    parent: Path | None = None
    parent_identity: tuple[int, int] | None = None
    try:
        parent, parent_identity = _common_parent(baseline_control)
    except Exception:
        common_parent_failed = True
    if common_parent_failed or parent is None or parent_identity is None:
        raise PromotedOperationalInputGCSError(_ERR)

    preflight_failed = False
    try:
        if _path_exists(baseline_control.assembly_spec_file):
            preflight_failed = True
        else:
            for root_name in ROOT_INPUT_NAMES:
                if _path_exists(getattr(baseline_control, root_name)):
                    preflight_failed = True
                    break
    except Exception:
        preflight_failed = True
    if preflight_failed:
        raise PromotedOperationalInputGCSError(_ERR)

    staging_failed = False
    staging: Path | None = None
    staging_control: PromotedOperationalCloudRunControl | None = None
    staging_identity: tuple[int, int] | None = None
    try:
        # Recheck the common parent's exact identity immediately before
        # creating the private staging directory beneath it.
        _verify_common_parent_identity(parent, parent_identity)

        staging = Path(tempfile.mkdtemp(dir=parent, prefix=_STAGING_PREFIX))
        staging_status = _lstat(staging)
        if (
            staging.parent != parent
            or not stat.S_ISDIR(staging_status.st_mode)
            or _is_link_like(staging_status)
        ):
            raise PromotedOperationalInputGCSError(_ERR)
        staging_identity = (staging_status.st_dev, staging_status.st_ino)

        # Recheck the parent again immediately after staging creation and
        # before any staged root/file content is written.
        _verify_common_parent_identity(parent, parent_identity)
        _verify_staging_identity(staging, staging_identity)

        staging_control = _staging_control(baseline_control, staging)
        _create_staged_tree(staging_control, manifest, content_by_hash)

        # Recheck the staging directory's own identity immediately after
        # staged-tree creation.
        _verify_staging_identity(staging, staging_identity)
    except Exception:
        staging_failed = True
    if staging_failed or staging_control is None or staging is None or staging_identity is None:
        raise PromotedOperationalInputGCSError(_ERR)

    stage_verify_failed = False
    try:
        _verify_staging_identity(staging, staging_identity)
        staged_inventory = build_promoted_operational_input_inventory(staging_control)
        if encode_promoted_operational_input_inventory(
            staged_inventory
        ) != encode_promoted_operational_input_inventory(manifest.inventory):
            raise PromotedOperationalInputGCSError(_ERR)
        staged_assembly_spec = load_promoted_operational_assembly_spec_file(staging_control.assembly_spec_file)
        if (
            staged_assembly_spec.assembly_spec_id != baseline_control.expected_assembly_spec_id
            or staged_assembly_spec.target_session != baseline_control.target_session
            or staged_assembly_spec.binding_bucket != baseline_control.state_bucket
        ):
            raise PromotedOperationalInputGCSError(_ERR)
    except Exception:
        stage_verify_failed = True
    if stage_verify_failed:
        raise PromotedOperationalInputGCSError(_ERR)

    rename_failed = False
    try:
        # Recheck the staging directory's own identity immediately before
        # the first rename.
        _verify_staging_identity(staging, staging_identity)

        rename_pairs = [(staging_control.assembly_spec_file, baseline_control.assembly_spec_file)]
        for root_name in ROOT_INPUT_NAMES:
            rename_pairs.append((getattr(staging_control, root_name), getattr(baseline_control, root_name)))

        for source, destination in rename_pairs:
            current_parent = _lstat(parent)
            if (
                not stat.S_ISDIR(current_parent.st_mode)
                or _is_link_like(current_parent)
                or (current_parent.st_dev, current_parent.st_ino) != parent_identity
            ):
                raise PromotedOperationalInputGCSError(_ERR)
            if _path_exists(destination):
                raise PromotedOperationalInputGCSError(_ERR)
            os.rename(source, destination)
        _fsync_directory(parent)
    except Exception:
        # Multiple top-level renames cannot be one filesystem transaction:
        # a failure here leaves a partially-published, unrepairable layout.
        # No cleanup or repair is attempted -- the ephemeral common parent
        # must be discarded by the caller/container.
        rename_failed = True
    if rename_failed:
        raise PromotedOperationalInputGCSError(_ERR)

    post_verify_failed = False
    try:
        verify_hydrated_input_inventory(baseline_control, manifest.inventory)
        hydrated_assembly_spec = load_promoted_operational_assembly_spec_file(baseline_control.assembly_spec_file)
        if (
            hydrated_assembly_spec.assembly_spec_id != baseline_control.expected_assembly_spec_id
            or hydrated_assembly_spec.target_session != baseline_control.target_session
            or hydrated_assembly_spec.binding_bucket != baseline_control.state_bucket
        ):
            post_verify_failed = True
    except Exception:
        post_verify_failed = True
    if post_verify_failed:
        raise PromotedOperationalInputGCSError(_ERR)

    aggregate_failed = False
    result: CompletedPromotedOperationalInputRestore | None = None
    try:
        result = CompletedPromotedOperationalInputRestore(request=acquired.request, manifest=manifest)
    except Exception:
        aggregate_failed = True
    if aggregate_failed or result is None:
        raise PromotedOperationalInputGCSError(_ERR)
    return result
