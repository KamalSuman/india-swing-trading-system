from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, field

from india_swing.daily_pipeline.acquisition import GCSObjectPayload, GCSObjectReader
from india_swing.daily_pipeline.state_publication import PublishedStateObject, StateObjectWriter
from india_swing.identity import content_id
from india_swing.operations.portfolio_store import (
    LocalSwingPortfolioArtifactStore,
    SwingPortfolioSnapshotArtifact,
    decode_swing_portfolio_artifact,
    encode_swing_portfolio_artifact,
)

from .gcs_state import validate_paper_outcome_state_bucket
from .portfolio_rollover import (
    LocalPaperPortfolioRolloverStore,
    PaperPortfolioRollover,
    PaperPortfolioRolloverError,
    decode_paper_portfolio_rollover,
    encode_paper_portfolio_rollover,
)


_MANIFEST_CODEC = "paper-portfolio-rollover-publication-manifest-json/v1"
_MAXIMUM_BYTES = 16 * 1024 * 1024
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")


class PaperPortfolioRolloverPublicationError(PaperPortfolioRolloverError):
    pass


def _sha(value: object, name: str) -> str:
    if type(value) is not str or _SHA256.fullmatch(value) is None:
        raise PaperPortfolioRolloverPublicationError(
            f"{name} must be a lowercase SHA-256"
        )
    return value


def _published(value: object) -> PublishedStateObject:
    if type(value) is not PublishedStateObject:
        raise PaperPortfolioRolloverPublicationError(
            "paper portfolio rollover object must be exact"
        )
    try:
        return PublishedStateObject(
            object_name=value.object_name,
            generation=value.generation,
            byte_count=value.byte_count,
            sha256=value.sha256,
        )
    except Exception:
        raise PaperPortfolioRolloverPublicationError(
            "paper portfolio rollover object is invalid"
        ) from None


def _root(state_id: str) -> str:
    return f"paper-portfolio-rollovers/{_sha(state_id, 'state_id')}"


def _rollover_name(value: PaperPortfolioRollover) -> str:
    return f"{_root(value.paper_portfolio_state_id)}/rollovers/{value.rollover_id}.json"


def _artifact_name(value: PaperPortfolioRollover) -> str:
    return (
        f"{_root(value.paper_portfolio_state_id)}/portfolio-artifacts/"
        f"{value.portfolio_artifact.artifact_id}.json"
    )


def _manifest_name(state_id: str, publication_id: str) -> str:
    return f"{_root(state_id)}/manifests/{_sha(publication_id, 'publication_id')}.json"


@dataclass(frozen=True, slots=True)
class PaperPortfolioRolloverPublicationManifest:
    bucket: str
    state_id: str
    rollover_id: str
    portfolio_artifact_id: str
    rollover_object: PublishedStateObject
    portfolio_artifact_object: PublishedStateObject
    publication_id: str = field(init=False)

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "bucket", validate_paper_outcome_state_bucket(self.bucket)
        )
        for value, name in (
            (self.state_id, "state_id"),
            (self.rollover_id, "rollover_id"),
            (self.portfolio_artifact_id, "portfolio_artifact_id"),
        ):
            _sha(value, name)
        object.__setattr__(self, "rollover_object", _published(self.rollover_object))
        object.__setattr__(
            self,
            "portfolio_artifact_object",
            _published(self.portfolio_artifact_object),
        )
        expected_root = _root(self.state_id)
        if (
            self.rollover_object.object_name
            != f"{expected_root}/rollovers/{self.rollover_id}.json"
            or self.portfolio_artifact_object.object_name
            != f"{expected_root}/portfolio-artifacts/{self.portfolio_artifact_id}.json"
        ):
            raise PaperPortfolioRolloverPublicationError(
                "paper portfolio rollover object path differs"
            )
        object.__setattr__(
            self,
            "publication_id",
            content_id(
                {
                    "bucket": self.bucket,
                    "state_id": self.state_id,
                    "rollover_id": self.rollover_id,
                    "portfolio_artifact_id": self.portfolio_artifact_id,
                    "rollover_object": self.rollover_object,
                    "portfolio_artifact_object": self.portfolio_artifact_object,
                },
                length=64,
            ),
        )

    def verify_content_identity(self) -> None:
        try:
            fresh = PaperPortfolioRolloverPublicationManifest(
                bucket=self.bucket,
                state_id=self.state_id,
                rollover_id=self.rollover_id,
                portfolio_artifact_id=self.portfolio_artifact_id,
                rollover_object=self.rollover_object,
                portfolio_artifact_object=self.portfolio_artifact_object,
            )
        except Exception:
            raise PaperPortfolioRolloverPublicationError(
                "paper portfolio rollover manifest identity failed"
            ) from None
        if fresh.publication_id != self.publication_id:
            raise PaperPortfolioRolloverPublicationError(
                "paper portfolio rollover manifest identity failed"
            )


def _object_body(value: PublishedStateObject) -> dict[str, object]:
    value = _published(value)
    return {
        "byte_count": value.byte_count,
        "generation": value.generation,
        "object_name": value.object_name,
        "sha256": value.sha256,
    }


def _manifest_bytes(value: PaperPortfolioRolloverPublicationManifest) -> bytes:
    if type(value) is not PaperPortfolioRolloverPublicationManifest:
        raise PaperPortfolioRolloverPublicationError(
            "paper portfolio rollover manifest must be exact"
        )
    value.verify_content_identity()
    return (
        json.dumps(
            {
                "codec_schema_version": _MANIFEST_CODEC,
                "manifest": {
                    "bucket": value.bucket,
                    "portfolio_artifact_id": value.portfolio_artifact_id,
                    "portfolio_artifact_object": _object_body(
                        value.portfolio_artifact_object
                    ),
                    "publication_id": value.publication_id,
                    "rollover_id": value.rollover_id,
                    "rollover_object": _object_body(value.rollover_object),
                    "state_id": value.state_id,
                },
            },
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )
        + "\n"
    ).encode("utf-8")


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError
        result[key] = value
    return result


def _strict_object(value: object, keys: set[str]) -> dict[str, object]:
    if type(value) is not dict or set(value) != keys:
        raise ValueError
    return value


def _decode_published(value: object) -> PublishedStateObject:
    raw = _strict_object(
        value, {"byte_count", "generation", "object_name", "sha256"}
    )
    return PublishedStateObject(
        object_name=raw["object_name"],
        generation=raw["generation"],
        byte_count=raw["byte_count"],
        sha256=raw["sha256"],
    )


def decode_paper_portfolio_rollover_publication_manifest(
    payload: bytes,
) -> PaperPortfolioRolloverPublicationManifest:
    if type(payload) is not bytes or not payload or len(payload) > _MAXIMUM_BYTES:
        raise PaperPortfolioRolloverPublicationError(
            "paper portfolio rollover manifest bytes are invalid"
        )
    try:
        root = json.loads(
            payload.decode("utf-8"),
            object_pairs_hook=_unique_object,
            parse_float=lambda _: (_ for _ in ()).throw(ValueError()),
            parse_constant=lambda _: (_ for _ in ()).throw(ValueError()),
        )
        envelope = _strict_object(root, {"codec_schema_version", "manifest"})
        if envelope["codec_schema_version"] != _MANIFEST_CODEC:
            raise ValueError
        raw = _strict_object(
            envelope["manifest"],
            {
                "bucket",
                "portfolio_artifact_id",
                "portfolio_artifact_object",
                "publication_id",
                "rollover_id",
                "rollover_object",
                "state_id",
            },
        )
        value = PaperPortfolioRolloverPublicationManifest(
            bucket=raw["bucket"],
            state_id=raw["state_id"],
            rollover_id=raw["rollover_id"],
            portfolio_artifact_id=raw["portfolio_artifact_id"],
            rollover_object=_decode_published(raw["rollover_object"]),
            portfolio_artifact_object=_decode_published(
                raw["portfolio_artifact_object"]
            ),
        )
        if (
            value.publication_id != raw["publication_id"]
            or _manifest_bytes(value) != payload
        ):
            raise ValueError
        return value
    except Exception:
        raise PaperPortfolioRolloverPublicationError(
            "paper portfolio rollover manifest is invalid"
        ) from None


@dataclass(frozen=True, slots=True)
class CompletedPaperPortfolioRolloverPublication:
    manifest: PaperPortfolioRolloverPublicationManifest
    manifest_object: PublishedStateObject

    def __post_init__(self) -> None:
        if type(self.manifest) is not PaperPortfolioRolloverPublicationManifest:
            raise PaperPortfolioRolloverPublicationError(
                "completed paper portfolio rollover manifest is invalid"
            )
        object.__setattr__(self, "manifest_object", _published(self.manifest_object))
        payload = _manifest_bytes(self.manifest)
        if (
            self.manifest_object.object_name
            != _manifest_name(self.manifest.state_id, self.manifest.publication_id)
            or self.manifest_object.byte_count != len(payload)
            or self.manifest_object.sha256 != hashlib.sha256(payload).hexdigest()
        ):
            raise PaperPortfolioRolloverPublicationError(
                "completed paper portfolio rollover object differs"
            )


def _check(
    published: PublishedStateObject, object_name: str, payload: bytes
) -> PublishedStateObject:
    published = _published(published)
    if (
        published.object_name != object_name
        or published.byte_count != len(payload)
        or published.sha256 != hashlib.sha256(payload).hexdigest()
    ):
        raise PaperPortfolioRolloverPublicationError(
            "published paper portfolio rollover object differs"
        )
    return published


def publish_paper_portfolio_rollover(
    *, rollover: PaperPortfolioRollover, bucket: str, writer: StateObjectWriter
) -> CompletedPaperPortfolioRolloverPublication:
    try:
        if type(rollover) is not PaperPortfolioRollover:
            raise ValueError
        rollover.verify_content_identity()
        bucket = validate_paper_outcome_state_bucket(bucket)
        artifact_payload = encode_swing_portfolio_artifact(
            rollover.portfolio_artifact
        )
        artifact_name = _artifact_name(rollover)
        artifact_object = _check(
            writer.create_or_verify(
                bucket=bucket,
                object_name=artifact_name,
                content_bytes=artifact_payload,
                content_type="application/json",
                maximum_bytes=_MAXIMUM_BYTES,
            ),
            artifact_name,
            artifact_payload,
        )
        rollover_payload = encode_paper_portfolio_rollover(rollover)
        rollover_name = _rollover_name(rollover)
        rollover_object = _check(
            writer.create_or_verify(
                bucket=bucket,
                object_name=rollover_name,
                content_bytes=rollover_payload,
                content_type="application/json",
                maximum_bytes=_MAXIMUM_BYTES,
            ),
            rollover_name,
            rollover_payload,
        )
        manifest = PaperPortfolioRolloverPublicationManifest(
            bucket=bucket,
            state_id=rollover.paper_portfolio_state_id,
            rollover_id=rollover.rollover_id,
            portfolio_artifact_id=rollover.portfolio_artifact.artifact_id,
            rollover_object=rollover_object,
            portfolio_artifact_object=artifact_object,
        )
        manifest_payload = _manifest_bytes(manifest)
        manifest_name = _manifest_name(manifest.state_id, manifest.publication_id)
        manifest_object = _check(
            writer.create_or_verify(
                bucket=bucket,
                object_name=manifest_name,
                content_bytes=manifest_payload,
                content_type="application/json",
                maximum_bytes=_MAXIMUM_BYTES,
            ),
            manifest_name,
            manifest_payload,
        )
        return CompletedPaperPortfolioRolloverPublication(
            manifest=manifest, manifest_object=manifest_object
        )
    except PaperPortfolioRolloverPublicationError:
        raise
    except Exception:
        raise PaperPortfolioRolloverPublicationError(
            "paper portfolio rollover publication failed safely"
        ) from None


def _read_pinned(
    *,
    reader: GCSObjectReader,
    bucket: str,
    published: PublishedStateObject,
) -> bytes:
    published = _published(published)
    value = reader.read_generation(
        bucket=bucket,
        object_name=published.object_name,
        generation=published.generation,
        maximum_bytes=_MAXIMUM_BYTES,
    )
    if (
        type(value) is not GCSObjectPayload
        or value.generation != published.generation
        or type(value.content_bytes) is not bytes
        or not value.content_bytes
        or len(value.content_bytes) > _MAXIMUM_BYTES
        or len(value.content_bytes) != published.byte_count
        or hashlib.sha256(value.content_bytes).hexdigest() != published.sha256
    ):
        raise PaperPortfolioRolloverPublicationError(
            "paper portfolio rollover object verification failed"
        )
    return value.content_bytes


def restore_paper_portfolio_rollover(
    *,
    expected_state_id: str,
    expected_rollover_id: str,
    bucket: str,
    manifest_object_name: str,
    manifest_generation: int,
    manifest_sha256: str,
    reader: GCSObjectReader,
    rollover_store: LocalPaperPortfolioRolloverStore,
    portfolio_store: LocalSwingPortfolioArtifactStore,
) -> PaperPortfolioRollover:
    _sha(expected_state_id, "expected_state_id")
    _sha(expected_rollover_id, "expected_rollover_id")
    bucket = validate_paper_outcome_state_bucket(bucket)
    _sha(manifest_sha256, "manifest_sha256")
    if (
        type(manifest_generation) is not int
        or type(manifest_generation) is bool
        or manifest_generation <= 0
    ):
        raise PaperPortfolioRolloverPublicationError(
            "paper portfolio rollover manifest generation is invalid"
        )
    expected_prefix = f"{_root(expected_state_id)}/manifests/"
    if (
        type(manifest_object_name) is not str
        or not manifest_object_name.startswith(expected_prefix)
        or not manifest_object_name.endswith(".json")
        or _SHA256.fullmatch(manifest_object_name[len(expected_prefix) : -5])
        is None
    ):
        raise PaperPortfolioRolloverPublicationError(
            "paper portfolio rollover manifest object name is invalid"
        )
    if (
        type(rollover_store) is not LocalPaperPortfolioRolloverStore
        or type(portfolio_store) is not LocalSwingPortfolioArtifactStore
    ):
        raise PaperPortfolioRolloverPublicationError(
            "paper portfolio rollover restore stores must be exact"
        )
    try:
        raw = reader.read_generation(
            bucket=bucket,
            object_name=manifest_object_name,
            generation=manifest_generation,
            maximum_bytes=_MAXIMUM_BYTES,
        )
        if (
            type(raw) is not GCSObjectPayload
            or raw.generation != manifest_generation
            or type(raw.content_bytes) is not bytes
            or not raw.content_bytes
            or len(raw.content_bytes) > _MAXIMUM_BYTES
            or hashlib.sha256(raw.content_bytes).hexdigest() != manifest_sha256
        ):
            raise ValueError
        manifest = decode_paper_portfolio_rollover_publication_manifest(
            raw.content_bytes
        )
        if (
            manifest.bucket != bucket
            or manifest.state_id != expected_state_id
            or manifest.rollover_id != expected_rollover_id
            or _manifest_name(manifest.state_id, manifest.publication_id)
            != manifest_object_name
        ):
            raise ValueError
        artifact = decode_swing_portfolio_artifact(
            _read_pinned(
                reader=reader,
                bucket=bucket,
                published=manifest.portfolio_artifact_object,
            )
        )
        rollover = decode_paper_portfolio_rollover(
            _read_pinned(
                reader=reader,
                bucket=bucket,
                published=manifest.rollover_object,
            )
        )
        if (
            artifact.artifact_id != manifest.portfolio_artifact_id
            or rollover.rollover_id != expected_rollover_id
            or rollover.paper_portfolio_state_id != expected_state_id
            or rollover.portfolio_artifact != artifact
        ):
            raise ValueError
        stored_artifact = portfolio_store.put(artifact)
        stored_rollover = rollover_store.put(rollover)
        if stored_artifact != artifact or stored_rollover != rollover:
            raise ValueError
        return stored_rollover
    except PaperPortfolioRolloverPublicationError:
        raise
    except Exception:
        raise PaperPortfolioRolloverPublicationError(
            "paper portfolio rollover restore failed safely"
        ) from None
