from __future__ import annotations

import hashlib
import json
import tempfile
import unittest
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

from india_swing.market_data.codec import (
    MarketPayloadCodecError,
    decode_market_payload,
    encode_market_payload,
)
from india_swing.market_data.collection import (
    HistoricalCollectionError,
    HistoricalMarketDataCollector,
)
from india_swing.market_data.config import (
    MissingMarketDataConfiguration,
    UpstoxCredentials,
)
from india_swing.market_data.models import (
    HistoricalDailyCandleBatch,
    HistoricalDailyRequest,
    HistoricalInstrumentBinding,
)
from india_swing.market_data.provider import RetryPolicy
from india_swing.market_data.snapshot_store import LocalMarketSnapshotStore
from india_swing.market_data.upstox import (
    MAXIMUM_UPSTOX_RESPONSE_BYTES,
    UPSTOX_PROVIDER,
    UpstoxAuthenticationError,
    UpstoxAvailabilityError,
    UpstoxDataIntegrityError,
    UpstoxEndpointRateLimiter,
    UpstoxHistoricalDataAdapter,
    UpstoxHttpResponse,
    UpstoxPermissionError,
    UpstoxRateLimitError,
    UpstoxRequestError,
)


IST = timezone(timedelta(hours=5, minutes=30))
UTC = timezone.utc
ISIN = "INE009A01021"
LISTING_KEY = "NSE:INFY"
TOKEN = "distinct-upstox-secret-token"
SOURCE_SNAPSHOT_ID = "a" * 64
REQUESTED_AT = datetime(2026, 7, 21, 8, 0, tzinfo=IST)
OBSERVED_AT = datetime(2026, 7, 21, 9, 0, tzinfo=IST)
SESSION_ONE = date(2026, 7, 14)
SESSION_TWO = date(2026, 7, 15)


class FakeLimiter:
    def __init__(self) -> None:
        self.operations: list[str] = []

    def wait(self, operation: str) -> None:
        self.operations.append(operation)


class FakeTransport:
    def __init__(self, *outcomes: object) -> None:
        self.outcomes = list(outcomes)
        self.calls: list[dict[str, object]] = []

    def get(
        self,
        url: str,
        *,
        headers,
        timeout_seconds: float,
        maximum_bytes: int,
    ) -> UpstoxHttpResponse:
        self.calls.append(
            {
                "url": url,
                "headers": dict(headers),
                "timeout_seconds": timeout_seconds,
                "maximum_bytes": maximum_bytes,
            }
        )
        if not self.outcomes:
            raise AssertionError("unexpected Upstox transport call")
        outcome = self.outcomes.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome


def binding(
    *,
    provider: str = UPSTOX_PROVIDER,
    provider_instrument_id: str = f"NSE_EQ|{ISIN}",
    valid_from: date = date(2000, 1, 1),
    valid_through: date = date(2026, 7, 20),
) -> HistoricalInstrumentBinding:
    return HistoricalInstrumentBinding(
        provider=provider,
        provider_instrument_id=provider_instrument_id,
        exchange="NSE",
        listing_key=LISTING_KEY,
        security_series="EQ",
        isin=ISIN,
        valid_from=valid_from,
        valid_through=valid_through,
        source_snapshot_ids=(SOURCE_SNAPSHOT_ID,),
    )


def historical_request(
    sessions: tuple[date, ...] = (SESSION_ONE, SESSION_TWO),
    *,
    instrument_binding: HistoricalInstrumentBinding | None = None,
    requested_at: datetime = REQUESTED_AT,
) -> HistoricalDailyRequest:
    return HistoricalDailyRequest(
        binding=instrument_binding or binding(),
        sessions=sessions,
        requested_at=requested_at,
    )


def candle_row(
    session: date,
    *,
    timestamp: str | None = None,
    open_value: object = 100.1,
    high: object = 105.2,
    low: object = 99.5,
    close: object = 103.3,
    volume: object = 123456,
    open_interest: object = 0,
) -> list[object]:
    return [
        timestamp or f"{session.isoformat()}T00:00:00+05:30",
        open_value,
        high,
        low,
        close,
        volume,
        open_interest,
    ]


def success_body(rows: list[list[object]]) -> bytes:
    return json.dumps(
        {"status": "success", "data": {"candles": rows}},
        separators=(",", ":"),
    ).encode("utf-8")


def response(body: bytes, status_code: int = 200) -> UpstoxHttpResponse:
    return UpstoxHttpResponse(status_code=status_code, body=body)


def adapter(
    transport: FakeTransport,
    *,
    limiter: FakeLimiter | None = None,
    sleeper=None,
) -> UpstoxHistoricalDataAdapter:
    return UpstoxHistoricalDataAdapter(
        UpstoxCredentials(TOKEN),
        transport=transport,
        clock=lambda: OBSERVED_AT,
        rate_limiter=limiter or FakeLimiter(),
        retry_policy=RetryPolicy(
            max_attempts=3,
            base_delay_seconds=0,
            maximum_delay_seconds=0,
            jitter_seconds=0,
        ),
        sleeper=sleeper or (lambda _: None),
    )


class UpstoxCredentialsTests(unittest.TestCase):
    def test_credentials_are_runtime_only_and_redacted(self) -> None:
        credentials = UpstoxCredentials(TOKEN)

        rendered = repr(credentials) + json.dumps(credentials.identity_material)

        self.assertNotIn(TOKEN, rendered)
        self.assertIn("redacted", repr(credentials))
        self.assertEqual(credentials.access_token(), TOKEN)

    def test_missing_environment_token_fails_closed(self) -> None:
        with self.assertRaises(MissingMarketDataConfiguration):
            UpstoxCredentials.from_env({})


class UpstoxHistoricalDataAdapterTests(unittest.TestCase):
    def test_success_uses_v3_isin_key_and_returns_canonical_batch(self) -> None:
        body = success_body(
            [candle_row(SESSION_TWO), candle_row(SESSION_ONE)]
        )
        transport = FakeTransport(response(body))
        limiter = FakeLimiter()
        request = historical_request()

        batch = adapter(transport, limiter=limiter).fetch_historical_daily(request)

        self.assertIsInstance(batch, HistoricalDailyCandleBatch)
        self.assertEqual(batch.request, request)
        self.assertEqual(
            tuple(value.session for value in batch.candles),
            request.sessions,
        )
        self.assertEqual(batch.candles[0].open, Decimal("100.1"))
        self.assertEqual(batch.candles[0].close, Decimal("103.3"))
        self.assertEqual(batch.provider, UPSTOX_PROVIDER)
        self.assertEqual(
            batch.response_pages[0].payload_sha256,
            hashlib.sha256(body).hexdigest(),
        )
        self.assertEqual(limiter.operations, ["historical_data"])
        call = transport.calls[0]
        self.assertEqual(
            call["url"],
            "https://api.upstox.com/v3/historical-candle/"
            f"NSE_EQ%7C{ISIN}/days/1/2026-07-15/2026-07-14",
        )
        self.assertEqual(call["headers"]["Authorization"], f"Bearer {TOKEN}")
        self.assertEqual(call["headers"]["Accept"], "application/json")
        self.assertEqual(call["maximum_bytes"], MAXIMUM_UPSTOX_RESPONSE_BYTES)

    def test_widely_separated_history_is_split_into_bounded_pages(self) -> None:
        old_session = date(2010, 1, 4)
        recent_session = SESSION_ONE
        transport = FakeTransport(
            response(success_body([candle_row(old_session)])),
            response(success_body([candle_row(recent_session)])),
        )
        request = historical_request((old_session, recent_session))

        batch = adapter(transport).fetch_historical_daily(request)

        self.assertEqual(len(transport.calls), 2)
        self.assertEqual(len(batch.response_pages), 2)
        self.assertIn(
            "/days/1/2010-01-04/2010-01-04",
            transport.calls[0]["url"],
        )
        self.assertIn(
            "/days/1/2026-07-14/2026-07-14",
            transport.calls[1]["url"],
        )

    def test_malformed_or_inexact_responses_fail_closed(self) -> None:
        exact_rows = [candle_row(SESSION_ONE), candle_row(SESSION_TWO)]
        malformed_cases = {
            "invalid-json": b"<html>failure</html>",
            "duplicate-key": (
                b'{"status":"success","status":"success",'
                b'"data":{"candles":[]}}'
            ),
            "unsuccessful-envelope": json.dumps(
                {"status": "error", "data": {"candles": exact_rows}}
            ).encode(),
            "missing-session": success_body([candle_row(SESSION_ONE)]),
            "extra-session": success_body(
                exact_rows + [candle_row(date(2026, 7, 16))]
            ),
            "duplicate-session": success_body(
                [candle_row(SESSION_ONE), candle_row(SESSION_ONE)]
            ),
            "wrong-offset": success_body(
                [
                    candle_row(
                        SESSION_ONE,
                        timestamp="2026-07-14T00:00:00+00:00",
                    ),
                    candle_row(SESSION_TWO),
                ]
            ),
            "not-session-boundary": success_body(
                [
                    candle_row(
                        SESSION_ONE,
                        timestamp="2026-07-14T09:15:00+05:30",
                    ),
                    candle_row(SESSION_TWO),
                ]
            ),
            "quoted-price": success_body(
                [candle_row(SESSION_ONE, open_value="100.1"), exact_rows[1]]
            ),
            "quoted-volume": success_body(
                [candle_row(SESSION_ONE, volume="123456"), exact_rows[1]]
            ),
            "boolean-volume": success_body(
                [candle_row(SESSION_ONE, volume=True), exact_rows[1]]
            ),
            "invalid-ohlc": success_body(
                [candle_row(SESSION_ONE, high=90), exact_rows[1]]
            ),
            "empty-candles": success_body([]),
        }
        for name, body in malformed_cases.items():
            with self.subTest(name=name):
                with self.assertRaises(UpstoxDataIntegrityError):
                    adapter(FakeTransport(response(body))).fetch_historical_daily(
                        historical_request()
                    )

    def test_oversized_response_is_rejected_before_parsing(self) -> None:
        body = b"x" * (MAXIMUM_UPSTOX_RESPONSE_BYTES + 1)

        with self.assertRaisesRegex(
            UpstoxDataIntegrityError,
            "OversizedResponse",
        ):
            adapter(FakeTransport(response(body))).fetch_historical_daily(
                historical_request()
            )

    def test_provider_or_instrument_identity_mismatch_fails_before_http(self) -> None:
        cases = (
            binding(provider="ZERODHA_KITE", provider_instrument_id="408065"),
            binding(provider_instrument_id="NSE_EQ|INE467B01029"),
            binding(provider_instrument_id=ISIN),
        )
        for instrument_binding in cases:
            with self.subTest(instrument_binding=instrument_binding):
                transport = FakeTransport()
                with self.assertRaises(UpstoxDataIntegrityError):
                    adapter(transport).fetch_historical_daily(
                        historical_request(instrument_binding=instrument_binding)
                    )
                self.assertEqual(transport.calls, [])

    def test_http_errors_are_typed_not_retried_and_sanitized(self) -> None:
        cases = (
            (401, UpstoxAuthenticationError),
            (403, UpstoxPermissionError),
            (400, UpstoxRequestError),
        )
        for status_code, error_type in cases:
            with self.subTest(status_code=status_code):
                secret_body = f"{TOKEN}: upstream details".encode()
                transport = FakeTransport(response(secret_body, status_code))
                with self.assertRaises(error_type) as raised:
                    adapter(transport).fetch_historical_daily(historical_request())
                self.assertEqual(len(transport.calls), 1)
                self.assertNotIn(TOKEN, str(raised.exception))
                self.assertNotIn("upstream details", str(raised.exception))

    def test_rate_limit_and_availability_errors_retry_with_a_hard_bound(self) -> None:
        body = success_body(
            [candle_row(SESSION_ONE), candle_row(SESSION_TWO)]
        )
        transport = FakeTransport(
            response(b"rate limited", 429),
            response(b"unavailable", 503),
            response(body),
        )
        limiter = FakeLimiter()
        sleeps: list[float] = []

        batch = adapter(
            transport,
            limiter=limiter,
            sleeper=sleeps.append,
        ).fetch_historical_daily(historical_request())

        self.assertEqual(batch.record_count, 2)
        self.assertEqual(len(transport.calls), 3)
        self.assertEqual(limiter.operations, ["historical_data"] * 3)
        self.assertEqual(sleeps, [0, 0])

        exhausted = FakeTransport(
            response(b"one", 429),
            response(b"two", 503),
            response(b"three", 503),
        )
        with self.assertRaises(UpstoxAvailabilityError):
            adapter(exhausted).fetch_historical_daily(historical_request())
        self.assertEqual(len(exhausted.calls), 3)

    def test_transport_exception_is_sanitized_and_bounded(self) -> None:
        transport = FakeTransport(
            RuntimeError(TOKEN),
            RuntimeError(TOKEN),
            RuntimeError(TOKEN),
        )

        with self.assertRaises(UpstoxAvailabilityError) as raised:
            adapter(transport).fetch_historical_daily(historical_request())

        self.assertEqual(len(transport.calls), 3)
        self.assertNotIn(TOKEN, str(raised.exception))

    def test_nested_canonical_request_mutation_is_detected_before_http(self) -> None:
        request = historical_request()
        object.__setattr__(
            request.binding,
            "provider_instrument_id",
            f"NSE_EQ|{'B' * 12}",
        )
        transport = FakeTransport()

        with self.assertRaisesRegex(
            UpstoxDataIntegrityError,
            "InvalidCanonicalRequest",
        ):
            adapter(transport).fetch_historical_daily(request)

        self.assertEqual(transport.calls, [])


class ProviderNeutralHistoryTests(unittest.TestCase):
    def test_upstox_batch_round_trips_through_shared_snapshot_store(self) -> None:
        body = success_body(
            [candle_row(SESSION_TWO), candle_row(SESSION_ONE)]
        )
        connector = adapter(FakeTransport(response(body)))
        request = historical_request()

        with tempfile.TemporaryDirectory() as temp_dir:
            store = LocalMarketSnapshotStore(Path(temp_dir))
            stored = HistoricalMarketDataCollector(connector, store).collect(request)
            loaded = store.get(stored.manifest.dataset, stored.manifest.snapshot_id)

            self.assertEqual(
                stored.manifest.dataset,
                "historical-daily-upstox-nse",
            )
            self.assertEqual(stored.manifest.selection_key, request.request_id)
            self.assertEqual(stored.manifest.provider, UPSTOX_PROVIDER)
            self.assertEqual(stored.manifest.record_count, 2)
            self.assertIsInstance(
                loaded.normalized_payload,
                HistoricalDailyCandleBatch,
            )
            loaded.normalized_payload.verify_content_identity()
            for file_path in loaded.path.iterdir():
                self.assertNotIn(TOKEN.encode(), file_path.read_bytes())

    def test_derived_ids_are_verified_during_codec_decode(self) -> None:
        body = success_body(
            [candle_row(SESSION_ONE), candle_row(SESSION_TWO)]
        )
        batch = adapter(FakeTransport(response(body))).fetch_historical_daily(
            historical_request()
        )
        payload = encode_market_payload(batch)
        tampered = payload.replace(
            batch.request.request_id.encode(),
            b"0" * 64,
            1,
        )

        self.assertEqual(decode_market_payload(payload), batch)
        self.assertNotEqual(payload, tampered)
        with self.assertRaises(MarketPayloadCodecError):
            decode_market_payload(tampered)

    def test_collector_rejects_connector_provider_before_fetch(self) -> None:
        class WrongProviderConnector:
            provider = "ZERODHA_KITE"
            provider_version = "wrong/v1"

            def __init__(self) -> None:
                self.calls = 0

            def fetch_historical_daily(self, request):
                self.calls += 1
                raise AssertionError("must not be called")

        connector = WrongProviderConnector()
        with tempfile.TemporaryDirectory() as temp_dir:
            collector = HistoricalMarketDataCollector(
                connector,
                LocalMarketSnapshotStore(Path(temp_dir)),
            )
            with self.assertRaises(HistoricalCollectionError):
                collector.collect(historical_request())
        self.assertEqual(connector.calls, 0)

    def test_kite_and_upstox_emit_the_same_canonical_batch_type(self) -> None:
        from tests.test_market_data import (
            FakeKiteClient,
            adapter as kite_adapter,
            candle_row as kite_candle_row,
        )

        kite_binding = binding(
            provider="ZERODHA_KITE",
            provider_instrument_id="408065",
            valid_from=date(2026, 7, 1),
            valid_through=SESSION_ONE,
        )
        request = historical_request(
            (SESSION_ONE,),
            instrument_binding=kite_binding,
            requested_at=datetime(2026, 7, 15, 8, 0, tzinfo=IST),
        )
        kite = kite_adapter(
            FakeKiteClient(candles=[kite_candle_row(SESSION_ONE)])
        )

        batch = kite.fetch_historical_daily(request)

        self.assertIsInstance(batch, HistoricalDailyCandleBatch)
        self.assertEqual(batch.provider, "ZERODHA_KITE")
        self.assertEqual(batch.request, request)
        self.assertEqual(batch.candles[0].session, SESSION_ONE)


class UpstoxEndpointRateLimiterTests(unittest.TestCase):
    def test_second_request_is_delayed_to_one_second_spacing(self) -> None:
        clock_values = iter((0.0, 0.25, 1.0))
        sleeps: list[float] = []
        limiter = UpstoxEndpointRateLimiter(
            monotonic_clock=lambda: next(clock_values),
            sleeper=sleeps.append,
        )

        limiter.wait("historical_data")
        limiter.wait("historical_data")

        self.assertEqual(sleeps, [0.75])

    def test_unknown_operation_is_rejected(self) -> None:
        limiter = UpstoxEndpointRateLimiter()
        with self.assertRaises(ValueError):
            limiter.wait("orders")


if __name__ == "__main__":
    unittest.main()
