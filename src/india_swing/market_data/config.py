from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path


KITE_API_KEY_ENV = "INDIA_SWING_KITE_API_KEY"
KITE_ACCESS_TOKEN_ENV = "INDIA_SWING_KITE_ACCESS_TOKEN"
UPSTOX_ACCESS_TOKEN_ENV = "INDIA_SWING_UPSTOX_ACCESS_TOKEN"
MARKET_DATA_ROOT_ENV = "INDIA_SWING_MARKET_DATA_ROOT"


class MissingMarketDataConfiguration(ValueError):
    """Raised when a required runtime-only setting is unavailable."""


class KiteCredentials:
    """Runtime credentials that deliberately cannot be dataclass-serialized."""

    __slots__ = ("_access_token", "_api_key")

    def __init__(self, api_key: str, access_token: str) -> None:
        if not api_key.strip():
            raise MissingMarketDataConfiguration("Kite API key is empty")
        if not access_token.strip():
            raise MissingMarketDataConfiguration("Kite access token is empty")
        self._api_key = api_key
        self._access_token = access_token

    @classmethod
    def from_env(cls, environ: Mapping[str, str] | None = None) -> KiteCredentials:
        values = os.environ if environ is None else environ
        missing = [
            name
            for name in (KITE_API_KEY_ENV, KITE_ACCESS_TOKEN_ENV)
            if not values.get(name, "").strip()
        ]
        if missing:
            raise MissingMarketDataConfiguration(
                "missing required environment variables: " + ", ".join(missing)
            )
        return cls(values[KITE_API_KEY_ENV], values[KITE_ACCESS_TOKEN_ENV])

    def api_key(self) -> str:
        return self._api_key

    def access_token(self) -> str:
        return self._access_token

    @property
    def identity_material(self) -> dict[str, str]:
        return {"provider": "ZERODHA_KITE", "auth_scheme": "daily_access_token"}

    def __repr__(self) -> str:
        return "KiteCredentials(api_key=<redacted>, access_token=<redacted>)"


class UpstoxCredentials:
    """Runtime-only bearer/analytics token with no serializable fields."""

    __slots__ = ("_access_token",)

    def __init__(self, access_token: str) -> None:
        if not isinstance(access_token, str) or not access_token.strip():
            raise MissingMarketDataConfiguration("Upstox access token is empty")
        self._access_token = access_token

    @classmethod
    def from_env(cls, environ: Mapping[str, str] | None = None) -> UpstoxCredentials:
        values = os.environ if environ is None else environ
        if not values.get(UPSTOX_ACCESS_TOKEN_ENV, "").strip():
            raise MissingMarketDataConfiguration(
                f"missing required environment variable: {UPSTOX_ACCESS_TOKEN_ENV}"
            )
        return cls(values[UPSTOX_ACCESS_TOKEN_ENV])

    def access_token(self) -> str:
        return self._access_token

    @property
    def identity_material(self) -> dict[str, str]:
        return {"provider": "UPSTOX", "auth_scheme": "bearer_or_analytics_token"}

    def __repr__(self) -> str:
        return "UpstoxCredentials(access_token=<redacted>)"


@dataclass(frozen=True, slots=True)
class MarketDataConfig:
    data_root: Path = Path("var/market_data")
    provider: str = "ZERODHA_KITE"
    kite_sdk_version: str = "5.2.0"

    def __post_init__(self) -> None:
        if not self.provider.strip():
            raise ValueError("provider is required")
        if not self.kite_sdk_version.strip():
            raise ValueError("kite_sdk_version is required")

    @classmethod
    def from_env(cls, environ: Mapping[str, str] | None = None) -> MarketDataConfig:
        values = os.environ if environ is None else environ
        root = values.get(MARKET_DATA_ROOT_ENV, "").strip()
        return cls(data_root=Path(root) if root else Path("var/market_data"))
