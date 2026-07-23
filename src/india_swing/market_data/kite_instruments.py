from __future__ import annotations

from datetime import datetime

from india_swing.identity import content_id
from india_swing.identity_registry.models import IdentityObservation

from .models import InstrumentBatch, KiteInstrument
from .snapshot_store import StoredMarketSnapshot


KITE_INSTRUMENT_RESOLVER_POLICY_VERSION = "kite-nse-eq-current-routing/v1"
KITE_INSTRUMENTS_DATASET = "kite-instruments-NSE"
KITE_INSTRUMENTS_SELECTION_KEY = "exchange=NSE"
KITE_PROVIDER = "ZERODHA_KITE"


class KiteInstrumentResolverError(ValueError):
    pass


class KiteInstrumentSnapshotResolver:
    """Route a current NSE EQ tradingsymbol to a Kite instrument_token.

    This is provider-routing evidence only. NSE point-in-time security
    masters remain the sole authority for historical universe membership;
    an absent or ambiguous current Kite symbol becomes an explicit issue in
    the caller's plan, never an inferred replacement or deletion.
    """

    def __init__(self, snapshot: StoredMarketSnapshot) -> None:
        if type(snapshot) is not StoredMarketSnapshot:
            raise TypeError("snapshot must be an exact StoredMarketSnapshot")
        manifest = snapshot.manifest
        if manifest.dataset != KITE_INSTRUMENTS_DATASET:
            raise KiteInstrumentResolverError(
                "Kite instrument snapshot dataset is unsupported"
            )
        if manifest.selection_key != KITE_INSTRUMENTS_SELECTION_KEY:
            raise KiteInstrumentResolverError(
                "Kite instrument snapshot selection key is unsupported"
            )
        if manifest.provider != KITE_PROVIDER:
            raise KiteInstrumentResolverError(
                "Kite instrument snapshot provider is unsupported"
            )
        if (
            type(manifest.provider_version) is not str
            or not manifest.provider_version.strip()
        ):
            raise KiteInstrumentResolverError(
                "Kite instrument snapshot provider_version is unsupported"
            )
        payload = snapshot.normalized_payload
        if type(payload) is not InstrumentBatch:
            raise KiteInstrumentResolverError(
                "Kite instrument snapshot payload has the wrong type"
            )
        if payload.exchange != "NSE":
            raise KiteInstrumentResolverError(
                "Kite instrument snapshot exchange is unsupported"
            )
        if (
            payload.provider_version != manifest.provider_version
            or payload.observed_at != manifest.observed_at
        ):
            raise KiteInstrumentResolverError(
                "Kite instrument snapshot payload lineage disagrees with its manifest"
            )
        if type(payload.instruments) is not tuple or not payload.instruments or any(
            type(value) is not KiteInstrument for value in payload.instruments
        ):
            raise KiteInstrumentResolverError(
                "Kite instrument snapshot rows must be an exact non-empty tuple"
            )

        self._snapshot_id = manifest.snapshot_id
        self._provider_version = manifest.provider_version
        self._observed_at = manifest.observed_at
        by_symbol: dict[str, list[KiteInstrument]] = {}
        for instrument in payload.instruments:
            if instrument.is_nse_eq_record:
                by_symbol.setdefault(instrument.tradingsymbol, []).append(instrument)
        self._by_symbol: dict[str, tuple[KiteInstrument, ...]] = {
            symbol: tuple(values) for symbol, values in by_symbol.items()
        }

    @property
    def provider(self) -> str:
        return KITE_PROVIDER

    @property
    def resolver_version(self) -> str:
        binding = content_id(
            {
                "snapshot_id": self._snapshot_id,
                "provider_version": self._provider_version,
            },
            length=64,
        )
        return f"{KITE_INSTRUMENT_RESOLVER_POLICY_VERSION}@sha256:{binding}"

    @property
    def knowledge_time(self) -> datetime:
        return self._observed_at

    def resolve(self, observation: IdentityObservation) -> str:
        matches = self._matches(observation)
        if len(matches) != 1:
            raise KiteInstrumentResolverError(
                "current NSE tradingsymbol does not resolve to exactly one "
                "Kite EQ instrument"
            )
        return str(matches[0].instrument_token)

    def resolve_isin(self, isin: str) -> str:
        raise KiteInstrumentResolverError(
            "Kite instrument routing cannot resolve an ISIN-only identifier"
        )

    def catalog_contains(self, observation: IdentityObservation) -> bool:
        return len(self._matches(observation)) == 1

    def catalog_contains_isin(self, isin: str) -> bool:
        raise KiteInstrumentResolverError(
            "Kite instrument routing cannot check catalog membership by ISIN alone"
        )

    def _matches(
        self, observation: IdentityObservation
    ) -> tuple[KiteInstrument, ...]:
        if type(observation) is not IdentityObservation:
            raise TypeError("observation must be an exact IdentityObservation")
        try:
            observation.verify_content_identity()
        except Exception as exc:
            raise KiteInstrumentResolverError(
                "identity observation failed identity verification"
            ) from exc
        if observation.security_series != "EQ":
            raise KiteInstrumentResolverError(
                "Kite routing requires an exact EQ security series"
            )
        return self._by_symbol.get(observation.ticker_symbol, ())
