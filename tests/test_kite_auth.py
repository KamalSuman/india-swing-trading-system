from __future__ import annotations

import unittest
from importlib import metadata
from unittest.mock import patch

from india_swing.market_data.config import (
    KiteCredentials,
    KiteLoginCredentials,
    MissingMarketDataConfiguration,
)
from india_swing.market_data.kite_auth import (
    LOOPBACK_CALLBACK_PATH,
    KiteInteractiveAuthenticator,
    KiteLoginError,
    LoopbackKiteCallbackReceiver,
    _FAILURE_RESPONSE_BODY,
    _SUCCESS_RESPONSE_BODY,
    _parse_kite_callback,
)


class FakeSessionClient:
    def __init__(
        self,
        *,
        login_url_value: object = "https://kite.zerodha.com/connect/login?v=3&api_key=k",
        session_result: object = None,
        login_url_error: Exception | None = None,
        session_error: Exception | None = None,
    ) -> None:
        self.login_url_value = login_url_value
        self.session_result = (
            session_result
            if session_result is not None
            else {"access_token": "fresh-access-token"}
        )
        self.login_url_error = login_url_error
        self.session_error = session_error
        self.login_url_calls = 0
        self.generate_session_calls: list[tuple[str, str]] = []

    def login_url(self) -> object:
        self.login_url_calls += 1
        if self.login_url_error is not None:
            raise self.login_url_error
        return self.login_url_value

    def generate_session(self, request_token: str, *, api_secret: str) -> object:
        self.generate_session_calls.append((request_token, api_secret))
        if self.session_error is not None:
            raise self.session_error
        return self.session_result


class FakeCallbackReceiver:
    def __init__(
        self,
        *,
        token: object = "req-token",
        error: Exception | None = None,
        close_error: Exception | None = None,
    ) -> None:
        self.token = token
        self.error = error
        self.close_error = close_error
        self.calls = 0
        self.close_calls = 0

    def receive_request_token(self) -> object:
        self.calls += 1
        if self.error is not None:
            raise self.error
        return self.token

    def close(self) -> None:
        self.close_calls += 1
        if self.close_error is not None:
            raise self.close_error


class FakeCallbackServer:
    def __init__(self, *, result: dict[str, object] | None = None) -> None:
        self.result: dict[str, object] = result if result is not None else {}
        self.timeout: float | None = None
        self.handle_request_calls = 0
        self.server_close_calls = 0

    def handle_request(self) -> None:
        self.handle_request_calls += 1

    def server_close(self) -> None:
        self.server_close_calls += 1


class KiteLoginCredentialsTests(unittest.TestCase):
    def test_credentials_are_redacted_from_repr(self) -> None:
        credentials = KiteLoginCredentials("distinct-api-key", "distinct-api-secret")

        rendered = repr(credentials)

        self.assertNotIn("distinct-api-key", rendered)
        self.assertNotIn("distinct-api-secret", rendered)
        self.assertIn("redacted", rendered)
        self.assertEqual(credentials.api_key(), "distinct-api-key")
        self.assertEqual(credentials.api_secret(), "distinct-api-secret")

    def test_missing_blank_or_wrong_type_configuration_fails_closed(self) -> None:
        cases = (
            ("", "secret"),
            ("   ", "secret"),
            ("key", ""),
            ("key", "   "),
            (None, "secret"),
            ("key", None),
            (123, "secret"),
        )
        for api_key, api_secret in cases:
            with self.subTest(api_key=api_key, api_secret=api_secret):
                with self.assertRaises(MissingMarketDataConfiguration):
                    KiteLoginCredentials(api_key, api_secret)

    def test_missing_environment_variables_fail_closed(self) -> None:
        with self.assertRaises(MissingMarketDataConfiguration):
            KiteLoginCredentials.from_env({})

    def test_from_env_uses_injected_environment_only(self) -> None:
        environment = {
            "INDIA_SWING_KITE_API_KEY": "env-key",
            "INDIA_SWING_KITE_API_SECRET": "env-secret",
        }
        credentials = KiteLoginCredentials.from_env(environment)
        self.assertEqual(credentials.api_key(), "env-key")
        self.assertEqual(credentials.api_secret(), "env-secret")


class KiteInteractiveAuthenticatorTests(unittest.TestCase):
    def test_valid_login_flow_uses_only_the_injected_fakes(self) -> None:
        client = FakeSessionClient(session_result={"access_token": "fresh-token"})
        receiver = FakeCallbackReceiver(token="req-token-123")
        opened: list[str] = []
        login_credentials = KiteLoginCredentials("app-key", "app-secret")
        authenticator = KiteInteractiveAuthenticator(
            client,
            login_credentials,
            receiver,
            browser_opener=opened.append,
        )

        credentials = authenticator.login()

        self.assertIsInstance(credentials, KiteCredentials)
        self.assertEqual(credentials.access_token(), "fresh-token")
        self.assertEqual(credentials.api_key(), "app-key")
        self.assertEqual(client.login_url_calls, 1)
        self.assertEqual(opened, [client.login_url_value])
        self.assertEqual(
            client.generate_session_calls, [("req-token-123", "app-secret")]
        )
        self.assertEqual(receiver.calls, 1)
        self.assertEqual(receiver.close_calls, 1)

    def test_wrong_login_credentials_type_is_rejected(self) -> None:
        with self.assertRaises(TypeError):
            KiteInteractiveAuthenticator(
                FakeSessionClient(),
                "not-credentials",
                FakeCallbackReceiver(),
            )

    def test_login_url_failure_is_sanitized_and_skips_callback(self) -> None:
        client = FakeSessionClient(login_url_error=RuntimeError("secret-url-detail"))
        receiver = FakeCallbackReceiver()
        authenticator = KiteInteractiveAuthenticator(
            client,
            KiteLoginCredentials("k", "s"),
            receiver,
            browser_opener=lambda url: None,
        )

        with self.assertRaises(KiteLoginError) as raised:
            authenticator.login()

        self.assertNotIn("secret-url-detail", str(raised.exception))
        self.assertEqual(receiver.calls, 0)
        self.assertEqual(receiver.close_calls, 1)

    def test_invalid_login_url_is_rejected(self) -> None:
        for bad in (None, "", "   ", 123):
            with self.subTest(bad=bad):
                client = FakeSessionClient(login_url_value=bad)
                authenticator = KiteInteractiveAuthenticator(
                    client,
                    KiteLoginCredentials("k", "s"),
                    FakeCallbackReceiver(),
                    browser_opener=lambda url: None,
                )
                with self.assertRaises(KiteLoginError):
                    authenticator.login()

    def test_browser_open_failure_is_sanitized_and_skips_callback(self) -> None:
        def failing_opener(url: str) -> None:
            raise RuntimeError("secret-browser-detail")

        receiver = FakeCallbackReceiver()
        authenticator = KiteInteractiveAuthenticator(
            FakeSessionClient(),
            KiteLoginCredentials("k", "s"),
            receiver,
            browser_opener=failing_opener,
        )

        with self.assertRaises(KiteLoginError) as raised:
            authenticator.login()

        self.assertNotIn("secret-browser-detail", str(raised.exception))
        self.assertEqual(receiver.calls, 0)
        self.assertEqual(receiver.close_calls, 1)

    def test_browser_false_result_is_failure_and_releases_receiver(self) -> None:
        receiver = FakeCallbackReceiver()
        authenticator = KiteInteractiveAuthenticator(
            FakeSessionClient(),
            KiteLoginCredentials("k", "s"),
            receiver,
            browser_opener=lambda url: False,
        )

        with self.assertRaises(KiteLoginError):
            authenticator.login()

        self.assertEqual(receiver.calls, 0)
        self.assertEqual(receiver.close_calls, 1)

    def test_cleanup_failure_after_success_is_sanitized(self) -> None:
        receiver = FakeCallbackReceiver(
            close_error=RuntimeError("secret-cleanup-detail")
        )
        authenticator = KiteInteractiveAuthenticator(
            FakeSessionClient(session_result={"access_token": "fresh-token"}),
            KiteLoginCredentials("k", "s"),
            receiver,
            browser_opener=lambda url: None,
        )

        with self.assertRaises(KiteLoginError) as raised:
            authenticator.login()

        self.assertNotIn("secret-cleanup-detail", str(raised.exception))

    def test_cleanup_failure_does_not_replace_primary_sanitized_failure(self) -> None:
        receiver = FakeCallbackReceiver(
            close_error=RuntimeError("secret-cleanup-detail")
        )
        authenticator = KiteInteractiveAuthenticator(
            FakeSessionClient(login_url_error=RuntimeError("secret-url-detail")),
            KiteLoginCredentials("k", "s"),
            receiver,
            browser_opener=lambda url: None,
        )

        with self.assertRaises(KiteLoginError) as raised:
            authenticator.login()

        self.assertEqual(str(raised.exception), "failed to obtain the Kite login URL")

    def test_foreign_callback_exception_is_sanitized(self) -> None:
        receiver = FakeCallbackReceiver(error=RuntimeError("secret-token-xyz"))
        authenticator = KiteInteractiveAuthenticator(
            FakeSessionClient(),
            KiteLoginCredentials("k", "s"),
            receiver,
            browser_opener=lambda url: None,
        )

        with self.assertRaises(KiteLoginError) as raised:
            authenticator.login()

        self.assertNotIn("secret-token-xyz", str(raised.exception))

    def test_own_login_error_from_receiver_propagates(self) -> None:
        receiver = FakeCallbackReceiver(
            error=KiteLoginError("callback did not complete")
        )
        authenticator = KiteInteractiveAuthenticator(
            FakeSessionClient(),
            KiteLoginCredentials("k", "s"),
            receiver,
            browser_opener=lambda url: None,
        )

        with self.assertRaises(KiteLoginError):
            authenticator.login()

    def test_invalid_request_token_from_receiver_is_rejected(self) -> None:
        for bad in (None, "", "   ", 123):
            with self.subTest(bad=bad):
                receiver = FakeCallbackReceiver(token=bad)
                authenticator = KiteInteractiveAuthenticator(
                    FakeSessionClient(),
                    KiteLoginCredentials("k", "s"),
                    receiver,
                    browser_opener=lambda url: None,
                )
                with self.assertRaises(KiteLoginError):
                    authenticator.login()

    def test_session_exchange_failure_is_sanitized(self) -> None:
        client = FakeSessionClient(
            session_error=RuntimeError("secret-session-detail")
        )
        authenticator = KiteInteractiveAuthenticator(
            client,
            KiteLoginCredentials("k", "s"),
            FakeCallbackReceiver(),
            browser_opener=lambda url: None,
        )

        with self.assertRaises(KiteLoginError) as raised:
            authenticator.login()

        self.assertNotIn("secret-session-detail", str(raised.exception))

    def test_non_mapping_missing_or_blank_access_token_is_rejected(self) -> None:
        cases = (
            "not-a-mapping",
            {},
            {"access_token": None},
            {"access_token": ""},
            {"access_token": "   "},
            {"access_token": 123},
        )
        for session_result in cases:
            with self.subTest(session_result=session_result):
                client = FakeSessionClient(session_result=session_result)
                authenticator = KiteInteractiveAuthenticator(
                    client,
                    KiteLoginCredentials("k", "s"),
                    FakeCallbackReceiver(),
                    browser_opener=lambda url: None,
                )
                with self.assertRaises(KiteLoginError):
                    authenticator.login()

    def test_request_token_never_appears_in_a_raised_error(self) -> None:
        client = FakeSessionClient(session_result={"access_token": ""})
        authenticator = KiteInteractiveAuthenticator(
            client,
            KiteLoginCredentials("k", "s"),
            FakeCallbackReceiver(token="distinct-request-token"),
            browser_opener=lambda url: None,
        )

        with self.assertRaises(KiteLoginError) as raised:
            authenticator.login()

        self.assertNotIn("distinct-request-token", str(raised.exception))


class KiteInteractiveAuthenticatorFromOfficialSdkTests(unittest.TestCase):
    def test_wrong_login_credentials_type_is_rejected_before_any_import(
        self,
    ) -> None:
        with self.assertRaises(TypeError):
            KiteInteractiveAuthenticator.from_official_sdk(
                "not-credentials", FakeCallbackReceiver()
            )

    def test_sdk_version_mismatch_is_rejected_before_any_sdk_import(self) -> None:
        with patch(
            "india_swing.market_data.kite_auth.metadata.version",
            return_value="9.9.9",
        ):
            with self.assertRaises(KiteLoginError):
                KiteInteractiveAuthenticator.from_official_sdk(
                    KiteLoginCredentials("k", "s"),
                    FakeCallbackReceiver(),
                )

    def test_missing_sdk_dependency_is_sanitized(self) -> None:
        with patch(
            "india_swing.market_data.kite_auth.metadata.version",
            side_effect=metadata.PackageNotFoundError("kiteconnect"),
        ):
            with self.assertRaises(KiteLoginError):
                KiteInteractiveAuthenticator.from_official_sdk(
                    KiteLoginCredentials("k", "s"),
                    FakeCallbackReceiver(),
                )


class LoopbackKiteCallbackReceiverTests(unittest.TestCase):
    def test_successful_callback_returns_the_request_token(self) -> None:
        server = FakeCallbackServer(
            result={"status": "success", "request_token": "req-token"}
        )
        receiver = LoopbackKiteCallbackReceiver(server_factory=lambda: server)

        token = receiver.receive_request_token()

        self.assertEqual(token, "req-token")
        self.assertEqual(server.handle_request_calls, 1)
        self.assertEqual(server.server_close_calls, 1)
        self.assertEqual(server.timeout, 120.0)

    def test_second_use_attempt_fails_closed_without_reusing_the_server(
        self,
    ) -> None:
        server = FakeCallbackServer(
            result={"status": "success", "request_token": "req-token"}
        )
        receiver = LoopbackKiteCallbackReceiver(server_factory=lambda: server)
        receiver.receive_request_token()

        with self.assertRaises(KiteLoginError):
            receiver.receive_request_token()
        self.assertEqual(server.handle_request_calls, 1)

    def test_explicit_close_is_idempotent_and_prevents_port_reuse(self) -> None:
        server = FakeCallbackServer(
            result={"status": "success", "request_token": "req-token"}
        )
        receiver = LoopbackKiteCallbackReceiver(server_factory=lambda: server)

        receiver.close()
        receiver.close()

        self.assertEqual(server.server_close_calls, 1)
        with self.assertRaises(KiteLoginError):
            receiver.receive_request_token()
        self.assertEqual(server.handle_request_calls, 0)

    def test_timeout_with_no_callback_fails_closed(self) -> None:
        server = FakeCallbackServer(result={})
        receiver = LoopbackKiteCallbackReceiver(
            server_factory=lambda: server, timeout_seconds=1.0
        )

        with self.assertRaises(KiteLoginError):
            receiver.receive_request_token()
        self.assertEqual(server.server_close_calls, 1)

    def test_unsuccessful_callback_result_fails_closed(self) -> None:
        cases = (
            {"error": "unexpected_path"},
            {"error": "malformed_query"},
            {"error": "unsuccessful_callback"},
        )
        for result in cases:
            with self.subTest(result=result):
                server = FakeCallbackServer(result=dict(result))
                receiver = LoopbackKiteCallbackReceiver(server_factory=lambda: server)
                with self.assertRaises(KiteLoginError):
                    receiver.receive_request_token()

    def test_missing_or_blank_request_token_fails_closed(self) -> None:
        cases = (
            {"status": "success"},
            {"status": "success", "request_token": ""},
            {"status": "success", "request_token": None},
            {"status": "success", "request_token": 123},
        )
        for result in cases:
            with self.subTest(result=result):
                server = FakeCallbackServer(result=dict(result))
                receiver = LoopbackKiteCallbackReceiver(server_factory=lambda: server)
                with self.assertRaises(KiteLoginError):
                    receiver.receive_request_token()

    def test_timeout_seconds_bounds_are_enforced(self) -> None:
        for bad in (0, -1, 301, True, False, "120"):
            with self.subTest(bad=bad):
                with self.assertRaises((TypeError, ValueError)):
                    LoopbackKiteCallbackReceiver(
                        timeout_seconds=bad,
                        server_factory=lambda: FakeCallbackServer(),
                    )

    def test_server_is_constructed_once_before_any_use(self) -> None:
        constructed: list[FakeCallbackServer] = []

        def factory() -> FakeCallbackServer:
            server = FakeCallbackServer(
                result={"status": "success", "request_token": "t"}
            )
            constructed.append(server)
            return server

        receiver = LoopbackKiteCallbackReceiver(server_factory=factory)
        self.assertEqual(len(constructed), 1)
        receiver.receive_request_token()
        self.assertEqual(len(constructed), 1)


class ParseKiteCallbackTests(unittest.TestCase):
    def test_exact_success_is_accepted(self) -> None:
        result = _parse_kite_callback(
            LOOPBACK_CALLBACK_PATH,
            "action=login&status=success&request_token=abc123",
        )
        self.assertEqual(result, {"status": "success", "request_token": "abc123"})

    def test_wrong_path_is_rejected(self) -> None:
        result = _parse_kite_callback(
            "/wrong/path",
            "action=login&status=success&request_token=abc123",
        )
        self.assertEqual(result, {"error": "unexpected_path"})

    def test_wrong_status_or_action_is_rejected(self) -> None:
        cases = (
            "action=login&status=cancel&request_token=abc123",
            "action=refresh&status=success&request_token=abc123",
        )
        for query in cases:
            with self.subTest(query=query):
                result = _parse_kite_callback(LOOPBACK_CALLBACK_PATH, query)
                self.assertEqual(result, {"error": "unsuccessful_callback"})

    def test_blank_request_token_is_an_unsuccessful_callback(self) -> None:
        result = _parse_kite_callback(
            LOOPBACK_CALLBACK_PATH,
            "action=login&status=success&request_token=",
        )
        self.assertEqual(result, {"error": "unsuccessful_callback"})

    def test_missing_or_duplicate_fields_are_malformed(self) -> None:
        cases = (
            "action=login&status=success",
            "status=success&request_token=abc123",
            "action=login&request_token=abc123",
            "action=login&status=success&request_token=a&request_token=b",
            "action=login&status=success&request_token=abc123&extra=1",
        )
        for query in cases:
            with self.subTest(query=query):
                result = _parse_kite_callback(LOOPBACK_CALLBACK_PATH, query)
                self.assertEqual(result, {"error": "malformed_query"})

    def test_unparsable_query_is_malformed(self) -> None:
        result = _parse_kite_callback(LOOPBACK_CALLBACK_PATH, "not-a-valid-query&&")
        self.assertEqual(result, {"error": "malformed_query"})

    def test_empty_query_is_malformed(self) -> None:
        result = _parse_kite_callback(LOOPBACK_CALLBACK_PATH, "")
        self.assertEqual(result, {"error": "malformed_query"})

    def test_response_bodies_never_echo_request_data(self) -> None:
        for body in (_SUCCESS_RESPONSE_BODY, _FAILURE_RESPONSE_BODY):
            lowered = body.lower()
            for forbidden in (b"token", b"?", b"kite/callback", b"api_key"):
                self.assertNotIn(forbidden, lowered)


if __name__ == "__main__":
    unittest.main()
