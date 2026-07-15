from __future__ import annotations

import io
import unittest
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from unittest.mock import patch

from india_swing.identity import canonical_identity_json
from india_swing.market_data.cli import main as market_data_main
from india_swing.market_data.config import KiteCredentials, MissingMarketDataConfiguration
from india_swing.market_data.kite import (
    KiteAuthenticationError,
    KiteAvailabilityError,
    KiteDataIntegrityError,
    KiteMarketDataAdapter,
    KitePermissionError,
    EndpointRateLimiter,
    MarketSessionNotFinalError,
    RetryPolicy,
)
from india_swing.market_data.models import (
    NSE_REGULAR_FINALITY_POLICY_VERSION,
    NseSessionFinality,
)


IST = timezone(timedelta(hours=5, minutes=30))
UTC = timezone.utc
OBSERVED_AT = datetime(2026, 7, 15, 17, 0, tzinfo=IST)
FINALITY = NseSessionFinality.regular_collection_guard(date(2026, 7, 15))


def instrument_row(**overrides):
    row = {
        "instrument_token": 408065,
        "exchange_token": "1594",
        "tradingsymbol": "INFY",
        "name": "INFOSYS",
        "last_price": 1500.1,
        "expiry": None,
        "strike": 0,
        "tick_size": 0.05,
        "lot_size": 1,
        "instrument_type": "EQ",
        "segment": "NSE",
        "exchange": "NSE",
    }
    row.update(overrides)
    return row


def candle_row(session: date = date(2026, 7, 15), **overrides):
    row = {
        "date": datetime.combine(session, datetime.min.time(), tzinfo=IST),
        "open": 100.1,
        "high": 104.2,
        "low": 99.5,
        "close": 103.3,
        "volume": 123456,
    }
    row.update(overrides)
    return row


class FakeLimiter:
    def __init__(self) -> None:
        self.operations: list[str] = []

    def wait(self, operation: str) -> None:
        self.operations.append(operation)


class FakeKiteClient:
    def __init__(self, *, instruments=None, candles=None) -> None:
        self.instrument_result = instruments if instruments is not None else [instrument_row()]
        self.candle_result = candles if candles is not None else [candle_row()]
        self.instrument_calls = 0
        self.historical_calls: list[tuple] = []

    def instruments(self, exchange=None):
        self.instrument_calls += 1
        result = self.instrument_result
        if isinstance(result, list) and result and isinstance(result[0], Exception):
            outcome = result.pop(0)
            raise outcome
        if isinstance(result, Exception):
            raise result
        return result

    def historical_data(
        self,
        instrument_token,
        from_date,
        to_date,
        interval,
        continuous=False,
        oi=False,
    ):
        self.historical_calls.append(
            (instrument_token, from_date, to_date, interval, continuous, oi)
        )
        result = self.candle_result
        if isinstance(result, Exception):
            raise result
        return result


def adapter(client: FakeKiteClient, **overrides) -> KiteMarketDataAdapter:
    values = {
        "clock": lambda: OBSERVED_AT,
        "rate_limiter": FakeLimiter(),
        "retry_policy": RetryPolicy(
            max_attempts=3,
            base_delay_seconds=0,
            maximum_delay_seconds=0,
            jitter_seconds=0,
        ),
        "sleeper": lambda _: None,
    }
    values.update(overrides)
    return KiteMarketDataAdapter(client, sdk_version="5.2.0", **values)


class KiteCredentialsTests(unittest.TestCase):
    def test_credentials_are_redacted_from_repr_and_identity(self) -> None:
        credentials = KiteCredentials("distinct-api-key", "distinct-access-token")

        rendered = repr(credentials) + canonical_identity_json(credentials)

        self.assertNotIn("distinct-api-key", rendered)
        self.assertNotIn("distinct-access-token", rendered)
        self.assertIn("redacted", repr(credentials))

    def test_missing_environment_credentials_fail_closed(self) -> None:
        with self.assertRaises(MissingMarketDataConfiguration):
            KiteCredentials.from_env({})

    def test_cli_failure_prints_only_the_error_type(self) -> None:
        stderr = io.StringIO()
        with patch.dict("os.environ", {}, clear=True), patch("sys.stderr", stderr):
            exit_code = market_data_main(["instruments"])

        self.assertEqual(exit_code, 2)
        self.assertIn("MissingMarketDataConfiguration", stderr.getvalue())
        self.assertNotIn("access_token", stderr.getvalue())


class KiteInstrumentAdapterTests(unittest.TestCase):
    def test_instrument_dump_is_normalized_without_using_float_prices(self) -> None:
        client = FakeKiteClient(
            instruments=[
                instrument_row(
                    tradingsymbol="SMALLCO",
                    instrument_token=2,
                    exchange_token="2",
                    last_price="12.30",
                ),
                instrument_row(),
            ]
        )

        batch = adapter(client).fetch_instruments("nse")

        self.assertEqual(batch.exchange, "NSE")
        self.assertEqual(len(batch.instruments), 2)
        self.assertEqual(batch.instruments[0].tradingsymbol, "INFY")
        small = next(item for item in batch.instruments if item.tradingsymbol == "SMALLCO")
        self.assertEqual(small.dump_last_price, Decimal("12.30"))
        self.assertTrue(small.is_nse_eq_record)

    def test_instrument_dump_rejects_duplicates_and_wrong_exchange(self) -> None:
        cases = (
            [instrument_row(), instrument_row()],
            [
                instrument_row(),
                instrument_row(
                    instrument_token=2,
                    exchange_token="2",
                    tradingsymbol="infy",
                ),
            ],
            [instrument_row(), instrument_row(instrument_token=2, tradingsymbol="TCS")],
            [instrument_row(exchange="BSE")],
        )
        for rows in cases:
            with self.subTest(rows=rows):
                with self.assertRaises(KiteDataIntegrityError):
                    adapter(FakeKiteClient(instruments=rows)).fetch_instruments("NSE")

    def test_instrument_integer_and_required_text_fields_are_strict(self) -> None:
        malformed_rows = (
            instrument_row(instrument_token=1.9),
            instrument_row(instrument_token=True),
            instrument_row(lot_size=1.9),
            instrument_row(lot_size=True),
            instrument_row(exchange_token=None),
            instrument_row(tradingsymbol=None),
        )
        for row in malformed_rows:
            with self.subTest(row=row):
                with self.assertRaises(KiteDataIntegrityError):
                    adapter(FakeKiteClient(instruments=[row])).fetch_instruments("NSE")

    def test_malformed_or_empty_dump_is_not_empty_success(self) -> None:
        cases = ([], [{"instrument_token": 1}], "<html>failure</html>")
        for response in cases:
            with self.subTest(response=response):
                with self.assertRaises(KiteDataIntegrityError):
                    adapter(FakeKiteClient(instruments=response)).fetch_instruments("NSE")

    def test_token_error_is_sanitized_and_never_retried(self) -> None:
        class TokenException(Exception):
            code = 403

        client = FakeKiteClient(instruments=TokenException("distinct-access-token"))

        with self.assertRaises(KiteAuthenticationError) as raised:
            adapter(client).fetch_instruments("NSE")

        self.assertEqual(client.instrument_calls, 1)
        self.assertNotIn("distinct-access-token", str(raised.exception))

    def test_transient_error_retries_are_bounded(self) -> None:
        class ServiceUnavailable(Exception):
            code = 503

        client = FakeKiteClient(
            instruments=[ServiceUnavailable("one"), ServiceUnavailable("two"), instrument_row()]
        )
        limiter = FakeLimiter()

        batch = adapter(client, rate_limiter=limiter).fetch_instruments("NSE")

        self.assertEqual(len(batch.instruments), 1)
        self.assertEqual(client.instrument_calls, 3)
        self.assertEqual(limiter.operations, ["instruments"] * 3)

    def test_exhausted_transient_error_is_typed_and_sanitized(self) -> None:
        class ServiceUnavailable(Exception):
            code = 503

        client = FakeKiteClient(
            instruments=[
                ServiceUnavailable("secret-one"),
                ServiceUnavailable("secret-two"),
                ServiceUnavailable("secret-three"),
            ]
        )

        with self.assertRaises(KiteAvailabilityError) as raised:
            adapter(client).fetch_instruments("NSE")

        self.assertEqual(client.instrument_calls, 3)
        self.assertNotIn("secret", str(raised.exception))

    def test_permission_error_is_not_misreported_as_expired_login(self) -> None:
        class PermissionException(Exception):
            code = 403

        client = FakeKiteClient(instruments=PermissionException("paid-plan-required"))

        with self.assertRaises(KitePermissionError):
            adapter(client).fetch_instruments("NSE")

        self.assertEqual(client.instrument_calls, 1)

    def test_sdk_data_exception_is_integrity_failure_and_not_retried(self) -> None:
        class DataException(Exception):
            code = 502

        client = FakeKiteClient(instruments=DataException("malformed-json"))

        with self.assertRaises(KiteDataIntegrityError):
            adapter(client).fetch_instruments("NSE")

        self.assertEqual(client.instrument_calls, 1)


class KiteDailyCandleAdapterTests(unittest.TestCase):
    def test_daily_candles_require_finality_and_preserve_decimal_values(self) -> None:
        client = FakeKiteClient(candles=[candle_row(close="103.30")])

        batch = adapter(client).fetch_daily_candle(
            408065,
            date(2026, 7, 15),
            session_finality=FINALITY,
        )

        self.assertEqual(batch.candles[0].close, Decimal("103.30"))
        self.assertEqual(batch.candles[0].session, date(2026, 7, 15))
        self.assertEqual(client.historical_calls[0][3:], ("day", False, False))

    def test_pre_finality_fetch_is_rejected_without_vendor_call(self) -> None:
        client = FakeKiteClient()
        early = datetime(2026, 7, 15, 15, 0, tzinfo=IST)

        with self.assertRaises(MarketSessionNotFinalError):
            adapter(client, clock=lambda: early).fetch_daily_candle(
                408065,
                date(2026, 7, 15),
                session_finality=FINALITY,
            )

        self.assertEqual(client.historical_calls, [])

    def test_finality_timestamp_must_belong_to_requested_india_session(self) -> None:
        client = FakeKiteClient()
        wrong_session_finality = NseSessionFinality.regular_collection_guard(
            date(2026, 7, 14)
        )

        with self.assertRaisesRegex(ValueError, "requested session"):
            adapter(client).fetch_daily_candle(
                408065,
                date(2026, 7, 15),
                session_finality=wrong_session_finality,
            )

        self.assertEqual(client.historical_calls, [])

    def test_observed_at_is_fetch_completion_time(self) -> None:
        started = datetime(2026, 7, 15, 16, 31, tzinfo=IST)
        completed = datetime(2026, 7, 15, 16, 32, tzinfo=IST)
        times = iter((started, completed))

        batch = adapter(FakeKiteClient(), clock=lambda: next(times)).fetch_daily_candle(
            408065,
            date(2026, 7, 15),
            session_finality=FINALITY,
        )

        self.assertEqual(batch.observed_at, completed)

    def test_missing_end_session_and_out_of_order_candles_fail_closed(self) -> None:
        missing = [candle_row(date(2026, 7, 14))]
        out_of_order = [candle_row(), candle_row(date(2026, 7, 14))]
        for rows in (missing, out_of_order):
            with self.subTest(rows=rows):
                with self.assertRaises(KiteDataIntegrityError):
                    adapter(FakeKiteClient(candles=rows)).fetch_daily_candle(
                        408065,
                        date(2026, 7, 15),
                        session_finality=FINALITY,
                    )

    def test_candle_schema_ohlc_timezone_and_duplicates_are_validated(self) -> None:
        cases = (
            [candle_row(high=99)],
            [candle_row(date=datetime(2026, 7, 15, tzinfo=UTC))],
            [candle_row(), candle_row()],
            [candle_row(volume=-1)],
            [candle_row(volume=1.9)],
            [candle_row(volume=True)],
            [candle_row(close="NaN")],
        )
        for rows in cases:
            with self.subTest(rows=rows):
                with self.assertRaises(KiteDataIntegrityError):
                    adapter(FakeKiteClient(candles=rows)).fetch_daily_candle(
                        408065,
                        date(2026, 7, 15),
                        session_finality=FINALITY,
                    )

    def test_midnight_cannot_be_declared_as_session_finality(self) -> None:
        with self.assertRaisesRegex(ValueError, "fixed 15:30/16:00"):
            NseSessionFinality(
                session=date(2026, 7, 15),
                market_close_at=datetime(2026, 7, 15, 0, 0, tzinfo=IST),
                data_ready_at=datetime(2026, 7, 15, 0, 1, tzinfo=IST),
                policy_version=NSE_REGULAR_FINALITY_POLICY_VERSION,
                actionable=False,
            )

    def test_completion_clock_cannot_move_backwards(self) -> None:
        started = datetime(2026, 7, 15, 16, 32, tzinfo=IST)
        completed = datetime(2026, 7, 15, 16, 31, tzinfo=IST)
        times = iter((started, completed))

        with self.assertRaisesRegex(KiteDataIntegrityError, "NonMonotonic"):
            adapter(FakeKiteClient(), clock=lambda: next(times)).fetch_daily_candle(
                408065,
                date(2026, 7, 15),
                session_finality=FINALITY,
            )


class EndpointRateLimiterTests(unittest.TestCase):
    def test_historical_and_other_endpoints_use_documented_process_pacing(self) -> None:
        now = [0.0]
        delays: list[float] = []

        def sleeper(delay: float) -> None:
            delays.append(delay)
            now[0] += delay

        limiter = EndpointRateLimiter(
            monotonic_clock=lambda: now[0],
            sleeper=sleeper,
        )

        limiter.wait("historical_data")
        limiter.wait("historical_data")
        limiter.wait("instruments")
        limiter.wait("instruments")

        self.assertAlmostEqual(delays[0], 1 / 3)
        self.assertAlmostEqual(delays[1], 0.1)


if __name__ == "__main__":
    unittest.main()
