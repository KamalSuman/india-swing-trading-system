from __future__ import annotations

import ast
import inspect
import unittest
from datetime import datetime, timedelta, timezone
from hashlib import sha256

from india_swing.market_data.kite import (
    KiteAuthenticationError,
    KiteAvailabilityError,
    KiteDataIntegrityError,
    KiteDependencyError,
    KiteMarketDataAdapter,
    KitePermissionError,
    KiteRateLimitError,
    KiteRequestError,
    MarketSessionNotFinalError,
)
from india_swing.market_data.models import NseSessionFinality
from india_swing.market_data.provider import (
    HistoricalEmptyProviderResponseError,
    HistoricalProviderRequestRejectedError,
    RetryPolicy,
)
from india_swing.quality_pilot import kite_collector as collector_module
from india_swing.quality_pilot.canonical_response import (
    EndpointFamily,
    ResponseClassification,
)
from india_swing.quality_pilot.kite_collector import (
    KiteQualityPilotCollector,
    QualityPilotKiteCollectorError,
)
from india_swing.quality_pilot.capture_runner import QualityPilotCaptureRunner
from tests.test_quality_pilot_canonical_response import (
    _catalog_payload,
    _catalog_window,
    _instrument,
    _ohlcv_payload,
    _ohlcv_window,
    _quote,
    _quote_payload,
    _quote_window,
)
from tests.test_quality_pilot_capture_runner import _spec
from tests.test_quality_pilot_observation_store import FakeStateObjectWriter


def _hash(label: str) -> str:
    return sha256(label.encode("utf-8")).hexdigest()


class SequenceClock:
    def __init__(self, *values: object) -> None:
        self.values = list(values)
        self.calls = 0

    def __call__(self) -> datetime:
        value = self.values[self.calls]
        self.calls += 1
        if isinstance(value, Exception):
            raise value
        return value  # type: ignore[return-value]


class FakeKiteAdapter:
    def __init__(self, payload: object) -> None:
        self.provider = "ZERODHA_KITE"
        self.provider_version = "kite-3.0"
        self.maximum_attempts = 1
        self.payload = payload
        self.error: Exception | None = None
        self.calls: list[tuple] = []

    def _return(self):
        if self.error is not None:
            raise self.error
        return self.payload

    def fetch_instruments(self, exchange: str = "NSE"):
        self.calls.append(("instruments", exchange))
        return self._return()

    def fetch_full_quotes(self, listing_keys: tuple[str, ...]):
        self.calls.append(("quote", listing_keys))
        return self._return()

    def fetch_daily_candle(
        self,
        instrument_token: int,
        session,
        *,
        session_finality: NseSessionFinality,
    ):
        self.calls.append(
            ("historical_data", instrument_token, session, session_finality)
        )
        return self._return()


def _payload(spec):
    window = spec.window
    if window.endpoint_family is EndpointFamily.CATALOG:
        return _catalog_payload(window, (_instrument(),))
    if window.endpoint_family is EndpointFamily.FULL_QUOTE:
        quotes = tuple(
            _quote(listing_key=key, token=index + 101, window=window)
            for index, key in enumerate(spec.requested_keys)
        )
        return _quote_payload(window, spec.requested_keys, quotes)
    return _ohlcv_payload(
        window,
        window.market_session,
        token=spec.provider_instrument_token,
    )


def _collect(spec, adapter: FakeKiteAdapter | None = None):
    adapter = adapter or FakeKiteAdapter(_payload(spec))
    clock = SequenceClock(
        spec.window.opens_at,
        spec.window.opens_at + timedelta(seconds=1),
    )
    result = KiteQualityPilotCollector(adapter, clock=clock).collect(spec)
    return result, adapter, clock


class HappyPathTests(unittest.TestCase):
    def test_dispatches_each_endpoint_exactly_once(self) -> None:
        cases = (
            (_spec(_catalog_window()), "instruments"),
            (_spec(_quote_window()), "quote"),
            (_spec(_ohlcv_window(), chunk_index=7, chunk_count=250), "historical_data"),
        )
        for spec, operation in cases:
            with self.subTest(operation=operation):
                result, adapter, clock = _collect(spec)
                self.assertEqual(result.response_classification, ResponseClassification.SUCCESS)
                self.assertIs(result.payload, adapter.payload)
                self.assertEqual(len(adapter.calls), 1)
                self.assertEqual(adapter.calls[0][0], operation)
                self.assertEqual(clock.calls, 2)

        historical = cases[-1][0]
        call = _collect(historical)[1].calls[0]
        self.assertEqual(call[1], historical.provider_instrument_token)
        self.assertEqual(call[2], historical.window.market_session)
        self.assertEqual(
            call[3],
            NseSessionFinality.regular_collection_guard(historical.window.market_session),
        )

    def test_market_adapter_exposes_exact_configured_attempt_limit(self) -> None:
        client = object()
        default = KiteMarketDataAdapter(client, sdk_version="5.2.0")  # type: ignore[arg-type]
        single = KiteMarketDataAdapter(
            client,
            sdk_version="5.2.0",
            retry_policy=RetryPolicy(
                max_attempts=1,
                base_delay_seconds=0,
                maximum_delay_seconds=0,
                jitter_seconds=0,
            ),
        )  # type: ignore[arg-type]
        self.assertEqual(default.maximum_attempts, 3)
        self.assertEqual(single.maximum_attempts, 1)

    def test_collector_flows_through_runner_and_immutable_store_for_all_endpoints(self) -> None:
        for spec in (
            _spec(_catalog_window()),
            _spec(_quote_window()),
            _spec(_ohlcv_window(), chunk_index=7, chunk_count=250),
        ):
            adapter = FakeKiteAdapter(_payload(spec))
            clock = SequenceClock(
                spec.window.opens_at,
                spec.window.opens_at + timedelta(seconds=1),
            )
            writer = FakeStateObjectWriter()
            run = QualityPilotCaptureRunner().run(
                spec,
                KiteQualityPilotCollector(adapter, clock=clock),
                "quality-pilot-test-bucket",
                writer,
            )
            self.assertEqual(run.observation.request.response_classification, ResponseClassification.SUCCESS)
            self.assertEqual(run.published.observation_id, run.observation.observation_id)
            self.assertEqual(len(adapter.calls), 1)
            self.assertEqual(len(writer.calls), 1)
            run.verify_content_identity()


class AdapterBoundaryTests(unittest.TestCase):
    def test_rejects_wrong_identity_or_retry_policy_before_request(self) -> None:
        spec = _spec(_catalog_window())
        for field, value in (
            ("provider", "OTHER"),
            ("provider_version", "other-version"),
            ("maximum_attempts", 2),
            ("maximum_attempts", True),
        ):
            adapter = FakeKiteAdapter(_payload(spec))
            setattr(adapter, field, value)
            with self.subTest(field=field, value=value), self.assertRaises(
                QualityPilotKiteCollectorError
            ):
                _collect(spec, adapter)
            self.assertEqual(adapter.calls, [])

    def test_metadata_and_second_clock_failures_are_sanitized(self) -> None:
        secret = "SECRET-METADATA/C:/credentials"
        spec = _spec(_catalog_window())

        class BadMetadataAdapter(FakeKiteAdapter):
            @property
            def provider(self):
                raise RuntimeError(secret)

            @provider.setter
            def provider(self, value):
                pass

        adapter = BadMetadataAdapter(_payload(spec))
        with self.assertRaises(QualityPilotKiteCollectorError) as context:
            _collect(spec, adapter)
        self.assertNotIn(secret, str(context.exception))
        self.assertIsNone(context.exception.__cause__)
        self.assertIsNone(context.exception.__context__)
        self.assertEqual(adapter.calls, [])

        adapter = FakeKiteAdapter(_payload(spec))
        clock = SequenceClock(spec.window.opens_at, RuntimeError(secret))
        with self.assertRaises(QualityPilotKiteCollectorError) as context:
            KiteQualityPilotCollector(adapter, clock=clock).collect(spec)
        self.assertNotIn(secret, str(context.exception))
        self.assertIsNone(context.exception.__cause__)
        self.assertIsNone(context.exception.__context__)
        self.assertEqual(len(adapter.calls), 1)

    def test_unknown_dependency_and_subclassed_errors_fail_closed_and_sanitized(self) -> None:
        secret = "SECRET/C:/kite-token"
        spec = _spec(_catalog_window())

        class Subclass(KiteAvailabilityError):
            pass

        for error in (
            RuntimeError(secret),
            KiteDependencyError(secret),
            Subclass("instruments", "NetworkException"),
        ):
            adapter = FakeKiteAdapter(_payload(spec))
            adapter.error = error
            with self.subTest(error=type(error)), self.assertRaises(
                QualityPilotKiteCollectorError
            ) as context:
                _collect(spec, adapter)
            self.assertNotIn(secret, str(context.exception))
            self.assertIsNone(context.exception.__cause__)
            self.assertIsNone(context.exception.__context__)
            self.assertEqual(len(adapter.calls), 1)


class ClassificationTests(unittest.TestCase):
    def test_common_kite_failures_have_endpoint_compatible_classifications(self) -> None:
        cases = (
            (_spec(_catalog_window()), KiteAvailabilityError("instruments", "NetworkException"), ResponseClassification.CATALOG_GAP),
            (_spec(_quote_window()), KiteAvailabilityError("quote", "NetworkException"), ResponseClassification.PROVIDER_GAP),
            (_spec(_ohlcv_window()), KiteAvailabilityError("historical_data", "NetworkException"), ResponseClassification.PROVIDER_GAP),
            (_spec(_quote_window()), KiteRateLimitError("quote", "TooManyRequestsException"), ResponseClassification.RATE_LIMITED),
            (_spec(_catalog_window()), KiteAuthenticationError("instruments", "TokenException"), ResponseClassification.REQUEST_REJECTED),
            (_spec(_quote_window()), KitePermissionError("quote", "PermissionException"), ResponseClassification.REQUEST_REJECTED),
            (_spec(_catalog_window()), KiteRequestError("instruments", "InputException"), ResponseClassification.REQUEST_REJECTED),
            (_spec(_catalog_window()), KiteDataIntegrityError("instruments", "EmptyInstrumentDump"), ResponseClassification.CATALOG_GAP),
            (_spec(_quote_window()), KiteDataIntegrityError("quote", "EmptyQuoteResponse"), ResponseClassification.PROVIDER_GAP),
            (_spec(_quote_window()), KiteDataIntegrityError("quote", "IncompleteQuoteCoverage"), ResponseClassification.PARTIAL_KEY_COVERAGE),
            (_spec(_quote_window()), KiteDataIntegrityError("quote", "InvalidRowType"), ResponseClassification.CANONICALIZATION_FAILURE),
            (_spec(_ohlcv_window()), MarketSessionNotFinalError("secret"), ResponseClassification.TIMESTAMP_VIOLATION),
        )
        for spec, error, expected in cases:
            adapter = FakeKiteAdapter(_payload(spec))
            adapter.error = error
            with self.subTest(error=type(error), expected=expected):
                result, _, _ = _collect(spec, adapter)
                self.assertEqual(result.response_classification, expected)
                self.assertIsNone(result.payload)

    def test_wrong_operation_is_canonicalization_failure_not_a_false_gap(self) -> None:
        spec = _spec(_catalog_window())
        adapter = FakeKiteAdapter(_payload(spec))
        adapter.error = KiteAvailabilityError("quote", "NetworkException")
        result, _, _ = _collect(spec, adapter)
        self.assertEqual(
            result.response_classification,
            ResponseClassification.CANONICALIZATION_FAILURE,
        )

        adapter = FakeKiteAdapter(_payload(spec))
        adapter.error = MarketSessionNotFinalError("wrong endpoint")
        self.assertEqual(
            _collect(spec, adapter)[0].response_classification,
            ResponseClassification.CANONICALIZATION_FAILURE,
        )

    def test_historical_empty_and_rejected_are_bound_to_exact_request(self) -> None:
        spec = _spec(_ohlcv_window())
        observed_at = spec.window.opens_at
        common = dict(
            provider=spec.provider,
            provider_version=spec.provider_version,
            provider_instrument_id=str(spec.provider_instrument_token),
            session=spec.window.market_session,
            observed_at=observed_at,
            normalized_response_sha256=_hash("response"),
        )
        cases = (
            (HistoricalEmptyProviderResponseError(**common), ResponseClassification.PROVIDER_GAP),
            (
                HistoricalProviderRequestRejectedError(
                    **common, upstream_error_type="InputException"
                ),
                ResponseClassification.REQUEST_REJECTED,
            ),
        )
        for error, expected in cases:
            adapter = FakeKiteAdapter(_payload(spec))
            adapter.error = error
            with self.subTest(error=type(error)):
                self.assertEqual(_collect(spec, adapter)[0].response_classification, expected)

        mismatched = dict(common)
        mismatched["provider_instrument_id"] = "202"
        adapter = FakeKiteAdapter(_payload(spec))
        adapter.error = HistoricalEmptyProviderResponseError(**mismatched)
        self.assertEqual(
            _collect(spec, adapter)[0].response_classification,
            ResponseClassification.CANONICALIZATION_FAILURE,
        )

        catalog = _spec(_catalog_window())
        adapter = FakeKiteAdapter(_payload(catalog))
        adapter.error = HistoricalEmptyProviderResponseError(**common)
        self.assertEqual(
            _collect(catalog, adapter)[0].response_classification,
            ResponseClassification.CANONICALIZATION_FAILURE,
        )


class TimestampAndPayloadTests(unittest.TestCase):
    def test_outside_or_nonmonotonic_outer_clock_fails_without_fabricating_interval(self) -> None:
        spec = _spec(_catalog_window())
        cases = (
            (spec.window.opens_at - timedelta(microseconds=1), spec.window.opens_at),
            (spec.window.opens_at, spec.window.closes_at + timedelta(microseconds=1)),
            (spec.window.opens_at + timedelta(seconds=2), spec.window.opens_at),
        )
        for start, end in cases:
            adapter = FakeKiteAdapter(_payload(spec))
            clock = SequenceClock(start, end)
            collector = KiteQualityPilotCollector(adapter, clock=clock)
            with self.subTest(start=start, end=end), self.assertRaises(
                QualityPilotKiteCollectorError
            ):
                collector.collect(spec)

    def test_bad_clock_is_sanitized(self) -> None:
        secret = "SECRET-CLOCK/C:/private"
        spec = _spec(_catalog_window())
        clock = SequenceClock(RuntimeError(secret))
        with self.assertRaises(QualityPilotKiteCollectorError) as context:
            KiteQualityPilotCollector(FakeKiteAdapter(_payload(spec)), clock=clock).collect(spec)
        self.assertNotIn(secret, str(context.exception))
        self.assertIsNone(context.exception.__cause__)
        self.assertIsNone(context.exception.__context__)

    def test_wrong_payload_type_is_canonicalization_failure(self) -> None:
        spec = _spec(_catalog_window())
        wrong = _payload(_spec(_quote_window()))
        result, _, _ = _collect(spec, FakeKiteAdapter(wrong))
        self.assertEqual(
            result.response_classification,
            ResponseClassification.CANONICALIZATION_FAILURE,
        )

    def test_payload_timestamp_outside_outer_request_is_timestamp_violation(self) -> None:
        spec = _spec(_catalog_window())
        payload = _catalog_payload(
            spec.window,
            (_instrument(),),
            observed_at=spec.window.opens_at + timedelta(seconds=2),
        )
        result, _, _ = _collect(spec, FakeKiteAdapter(payload))
        self.assertEqual(
            result.response_classification,
            ResponseClassification.TIMESTAMP_VIOLATION,
        )

    def test_valid_payload_for_wrong_ohlcv_token_is_canonicalization_failure(self) -> None:
        spec = _spec(_ohlcv_window(), provider_instrument_token=101)
        payload = _ohlcv_payload(spec.window, spec.window.market_session, token=202)
        result, _, _ = _collect(spec, FakeKiteAdapter(payload))
        self.assertEqual(
            result.response_classification,
            ResponseClassification.CANONICALIZATION_FAILURE,
        )


class CapabilityTests(unittest.TestCase):
    def test_bridge_has_no_ambient_or_mutating_capability(self) -> None:
        source = inspect.getsource(collector_module)
        tree = ast.parse(source)
        forbidden_modules = {
            "os", "pathlib", "socket", "subprocess", "requests", "urllib",
            "httpx", "google", "kiteconnect", "time", "sqlite3", "pickle", "shelve",
        }
        imported: set[str] = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imported.update(alias.name.split(".")[0] for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module:
                imported.add(node.module.split(".")[0])
        self.assertEqual(imported & forbidden_modules, set())
        lowered = source.lower()
        for token in (
            "datetime.now(", "utcnow(", "getenv(", "environ", "sleep(",
            "place_order(", "generate_signal(", "run_paper_trade(", ".delete(",
            "list_blobs(", "from_official_sdk(",
        ):
            self.assertNotIn(token, lowered, msg=token)


if __name__ == "__main__":
    unittest.main()
