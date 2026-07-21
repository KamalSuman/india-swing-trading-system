from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, field

from india_swing.daily_pipeline.acquisition import GCSObjectPayload, GCSObjectReader
from india_swing.daily_pipeline.state_publication import PublishedStateObject, StateObjectWriter
from india_swing.identity import content_id

from .gcs_state import validate_paper_outcome_state_bucket
from .portfolio import (
    LocalPaperPortfolioStateStore,
    PaperPortfolioError,
    PaperPortfolioState,
    decode_paper_portfolio_state,
    encode_paper_portfolio_state,
)


_MANIFEST_CODEC = "paper-portfolio-publication-manifest-json/v1"
_MAXIMUM_BYTES = 16 * 1024 * 1024
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")


class PaperPortfolioPublicationError(PaperPortfolioError):
    pass


def _sha(value: object, name: str) -> str:
    if type(value) is not str or _SHA256.fullmatch(value) is None:
        raise PaperPortfolioPublicationError(f"{name} must be a lowercase SHA-256")
    return value


def _published(value: object) -> PublishedStateObject:
    if type(value) is not PublishedStateObject:
        raise PaperPortfolioPublicationError("paper portfolio object must be exact")
    try:
        return PublishedStateObject(
            object_name=value.object_name,
            generation=value.generation,
            byte_count=value.byte_count,
            sha256=value.sha256,
        )
    except Exception:
        raise PaperPortfolioPublicationError("paper portfolio object is invalid") from None


def _state_name(value: PaperPortfolioState) -> str:
    return f"paper-portfolios/{value.batch_id}/states/{value.state_id}.json"


def _manifest_name(batch_id: str, publication_id: str) -> str:
    return f"paper-portfolios/{batch_id}/manifests/{publication_id}.json"


@dataclass(frozen=True, slots=True)
class PaperPortfolioPublicationManifest:
    bucket: str
    batch_id: str
    state_id: str
    state_object: PublishedStateObject
    publication_id: str = field(init=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "bucket", validate_paper_outcome_state_bucket(self.bucket))
        _sha(self.batch_id, "batch_id")
        _sha(self.state_id, "state_id")
        object.__setattr__(self, "state_object", _published(self.state_object))
        if self.state_object.object_name != f"paper-portfolios/{self.batch_id}/states/{self.state_id}.json":
            raise PaperPortfolioPublicationError("paper portfolio state object path differs")
        object.__setattr__(
            self,
            "publication_id",
            content_id(
                {
                    "batch_id": self.batch_id,
                    "bucket": self.bucket,
                    "state_id": self.state_id,
                    "state_object": self.state_object,
                },
                length=64,
            ),
        )

    def verify_content_identity(self) -> None:
        try:
            fresh = PaperPortfolioPublicationManifest(
                bucket=self.bucket,
                batch_id=self.batch_id,
                state_id=self.state_id,
                state_object=self.state_object,
            )
        except Exception:
            raise PaperPortfolioPublicationError(
                "paper portfolio manifest identity failed"
            ) from None
        if fresh.publication_id != self.publication_id:
            raise PaperPortfolioPublicationError(
                "paper portfolio manifest identity failed"
            )


def _manifest_bytes(value: PaperPortfolioPublicationManifest) -> bytes:
    if type(value) is not PaperPortfolioPublicationManifest:
        raise PaperPortfolioPublicationError("paper portfolio manifest must be exact")
    value.verify_content_identity()
    return (
        json.dumps(
            {
                "codec_schema_version": _MANIFEST_CODEC,
                "manifest": {
                    "batch_id": value.batch_id,
                    "bucket": value.bucket,
                    "publication_id": value.publication_id,
                    "state_id": value.state_id,
                    "state_object": {
                        "byte_count": value.state_object.byte_count,
                        "generation": value.state_object.generation,
                        "object_name": value.state_object.object_name,
                        "sha256": value.state_object.sha256,
                    },
                },
            },
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


def _decode_manifest(payload: bytes) -> PaperPortfolioPublicationManifest:
    if type(payload) is not bytes or not payload or len(payload) > _MAXIMUM_BYTES:
        raise PaperPortfolioPublicationError("paper portfolio manifest bytes are invalid")
    try:
        root = json.loads(
            payload.decode("utf-8"),
            object_pairs_hook=_unique_object,
            parse_float=lambda _: (_ for _ in ()).throw(ValueError()),
            parse_constant=lambda _: (_ for _ in ()).throw(ValueError()),
        )
        if type(root) is not dict or set(root) != {"codec_schema_version", "manifest"}:
            raise ValueError
        if root["codec_schema_version"] != _MANIFEST_CODEC:
            raise ValueError
        raw = root["manifest"]
        if type(raw) is not dict or set(raw) != {
            "batch_id", "bucket", "publication_id", "state_id", "state_object"
        }:
            raise ValueError
        item = raw["state_object"]
        if type(item) is not dict or set(item) != {
            "byte_count", "generation", "object_name", "sha256"
        }:
            raise ValueError
        value = PaperPortfolioPublicationManifest(
            bucket=raw["bucket"],
            batch_id=raw["batch_id"],
            state_id=raw["state_id"],
            state_object=PublishedStateObject(
                object_name=item["object_name"], generation=item["generation"],
                byte_count=item["byte_count"], sha256=item["sha256"],
            ),
        )
        if (
            value.publication_id != raw["publication_id"]
            or _manifest_bytes(value) != payload
        ):
            raise ValueError
        return value
    except Exception:
        raise PaperPortfolioPublicationError("paper portfolio manifest is invalid") from None


@dataclass(frozen=True, slots=True)
class CompletedPaperPortfolioPublication:
    manifest: PaperPortfolioPublicationManifest
    manifest_object: PublishedStateObject

    def __post_init__(self) -> None:
        if type(self.manifest) is not PaperPortfolioPublicationManifest:
            raise PaperPortfolioPublicationError("completed paper portfolio manifest is invalid")
        object.__setattr__(self, "manifest_object", _published(self.manifest_object))
        expected = _manifest_name(self.manifest.batch_id, self.manifest.publication_id)
        encoded = _manifest_bytes(self.manifest)
        if (
            self.manifest_object.object_name != expected
            or self.manifest_object.byte_count != len(encoded)
            or self.manifest_object.sha256 != hashlib.sha256(encoded).hexdigest()
        ):
            raise PaperPortfolioPublicationError("completed paper portfolio object differs")


def _check(published: PublishedStateObject, name: str, payload: bytes) -> PublishedStateObject:
    published = _published(published)
    if (
        published.object_name != name
        or published.byte_count != len(payload)
        or published.sha256 != hashlib.sha256(payload).hexdigest()
    ):
        raise PaperPortfolioPublicationError("published paper portfolio object differs")
    return published


def publish_paper_portfolio_state(
    *, state: PaperPortfolioState, bucket: str, writer: StateObjectWriter,
) -> CompletedPaperPortfolioPublication:
    try:
        state.verify_content_identity()
        bucket = validate_paper_outcome_state_bucket(bucket)
        payload = encode_paper_portfolio_state(state)
        name = _state_name(state)
        published = _check(
            writer.create_or_verify(
                bucket=bucket, object_name=name, content_bytes=payload,
                content_type="application/json", maximum_bytes=_MAXIMUM_BYTES,
            ), name, payload,
        )
        manifest = PaperPortfolioPublicationManifest(
            bucket=bucket, batch_id=state.batch_id, state_id=state.state_id,
            state_object=published,
        )
        manifest_payload = _manifest_bytes(manifest)
        manifest_name = _manifest_name(state.batch_id, manifest.publication_id)
        manifest_object = _check(
            writer.create_or_verify(
                bucket=bucket, object_name=manifest_name,
                content_bytes=manifest_payload, content_type="application/json",
                maximum_bytes=_MAXIMUM_BYTES,
            ), manifest_name, manifest_payload,
        )
        return CompletedPaperPortfolioPublication(manifest, manifest_object)
    except PaperPortfolioPublicationError:
        raise
    except Exception:
        raise PaperPortfolioPublicationError("paper portfolio publication failed safely") from None


def _read(reader, bucket, published, maximum):
    published = _published(published)
    value = reader.read_generation(
        bucket=bucket, object_name=published.object_name,
        generation=published.generation, maximum_bytes=maximum,
    )
    if (
        type(value) is not GCSObjectPayload or value.generation != published.generation
        or type(value.content_bytes) is not bytes or not value.content_bytes
        or len(value.content_bytes) > maximum
        or len(value.content_bytes) != published.byte_count
        or hashlib.sha256(value.content_bytes).hexdigest() != published.sha256
    ):
        raise PaperPortfolioPublicationError("paper portfolio object verification failed")
    return value.content_bytes


def restore_paper_portfolio_state(
    *, expected_batch_id: str, bucket: str, manifest_object_name: str,
    manifest_generation: int, manifest_sha256: str, reader: GCSObjectReader,
    store: LocalPaperPortfolioStateStore,
) -> PaperPortfolioState:
    _sha(expected_batch_id, "expected_batch_id")
    bucket = validate_paper_outcome_state_bucket(bucket)
    _sha(manifest_sha256, "manifest_sha256")
    if (
        type(manifest_generation) is not int
        or type(manifest_generation) is bool
        or manifest_generation <= 0
    ):
        raise PaperPortfolioPublicationError("paper portfolio manifest generation is invalid")
    prefix = f"paper-portfolios/{expected_batch_id}/manifests/"
    if (
        type(manifest_object_name) is not str
        or not manifest_object_name.startswith(prefix)
        or not manifest_object_name.endswith(".json")
        or _SHA256.fullmatch(manifest_object_name[len(prefix):-5]) is None
    ):
        raise PaperPortfolioPublicationError("paper portfolio manifest object name is invalid")
    if type(store) is not LocalPaperPortfolioStateStore:
        raise PaperPortfolioPublicationError("paper portfolio restore store must be exact")
    try:
        raw = reader.read_generation(
            bucket=bucket, object_name=manifest_object_name,
            generation=manifest_generation, maximum_bytes=_MAXIMUM_BYTES,
        )
        if (
            type(raw) is not GCSObjectPayload or raw.generation != manifest_generation
            or type(raw.content_bytes) is not bytes or not raw.content_bytes
            or len(raw.content_bytes) > _MAXIMUM_BYTES
            or hashlib.sha256(raw.content_bytes).hexdigest() != manifest_sha256
        ):
            raise ValueError
        manifest = _decode_manifest(raw.content_bytes)
        if (
            manifest.bucket != bucket or manifest.batch_id != expected_batch_id
            or _manifest_name(manifest.batch_id, manifest.publication_id) != manifest_object_name
        ):
            raise ValueError
        state = decode_paper_portfolio_state(
            _read(reader, bucket, manifest.state_object, _MAXIMUM_BYTES)
        )
        if state.batch_id != expected_batch_id or state.state_id != manifest.state_id:
            raise ValueError
        return store.put_restored(state)
    except PaperPortfolioPublicationError:
        raise
    except Exception:
        raise PaperPortfolioPublicationError("paper portfolio restoration failed safely") from None
