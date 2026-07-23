from __future__ import annotations

import webbrowser
from collections.abc import Callable, Mapping
from importlib import metadata
from typing import Protocol
from urllib.parse import parse_qs, urlsplit

from .config import KiteCredentials, KiteLoginCredentials
from .kite import PINNED_KITE_SDK_VERSION


LOOPBACK_HOST = "127.0.0.1"
LOOPBACK_PORT = 8765
LOOPBACK_CALLBACK_PATH = "/kite/callback"
MAXIMUM_LOOPBACK_TIMEOUT_SECONDS = 300.0
_SUCCESS_RESPONSE_BODY = b"Kite login complete. You may close this window."
_FAILURE_RESPONSE_BODY = b"Kite login failed. You may close this window."


class KiteLoginError(ValueError):
    pass


class KiteLoginSessionClient(Protocol):
    """The narrow subset of the official SDK client used only to log in."""

    def login_url(self) -> str: ...

    def generate_session(
        self, request_token: str, *, api_secret: str
    ) -> Mapping[str, object]: ...


class KiteLoginCallbackReceiver(Protocol):
    def receive_request_token(self) -> str: ...


class KiteInteractiveAuthenticator:
    """Exchanges one interactive login for in-memory KiteCredentials.

    Never prints, logs, serializes, or persists the API secret, request
    token, or access token; every foreign failure is translated to a static
    sanitized KiteLoginError without embedding upstream exception text.
    """

    def __init__(
        self,
        client: KiteLoginSessionClient,
        login_credentials: KiteLoginCredentials,
        callback_receiver: KiteLoginCallbackReceiver,
        *,
        browser_opener: Callable[[str], object] | None = None,
    ) -> None:
        if type(login_credentials) is not KiteLoginCredentials:
            raise TypeError("login_credentials must be exact KiteLoginCredentials")
        self._client = client
        self._login_credentials = login_credentials
        self._callback_receiver = callback_receiver
        self._browser_opener = browser_opener or webbrowser.open

    @classmethod
    def from_official_sdk(
        cls,
        login_credentials: KiteLoginCredentials,
        callback_receiver: KiteLoginCallbackReceiver,
        *,
        required_version: str = PINNED_KITE_SDK_VERSION,
        browser_opener: Callable[[str], object] | None = None,
    ) -> "KiteInteractiveAuthenticator":
        if type(login_credentials) is not KiteLoginCredentials:
            raise TypeError("login_credentials must be exact KiteLoginCredentials")
        try:
            installed_version = metadata.version("kiteconnect")
        except metadata.PackageNotFoundError:
            raise KiteLoginError(
                "the pinned Kite SDK dependency is unavailable"
            ) from None
        if installed_version != required_version:
            raise KiteLoginError(
                "installed Kite SDK version does not match the pinned version"
            )
        try:
            from kiteconnect import KiteConnect
        except ImportError:
            raise KiteLoginError(
                "the pinned Kite SDK dependency is unavailable"
            ) from None
        client = KiteConnect(api_key=login_credentials.api_key())
        return cls(
            client,
            login_credentials,
            callback_receiver,
            browser_opener=browser_opener,
        )

    def login(self) -> KiteCredentials:
        try:
            credentials = self._login_once()
        except Exception:
            self._close_callback_receiver(best_effort=True)
            raise
        self._close_callback_receiver(best_effort=False)
        return credentials

    def _close_callback_receiver(self, *, best_effort: bool) -> None:
        close = getattr(self._callback_receiver, "close", None)
        if not callable(close):
            return
        try:
            close()
        except Exception:
            if not best_effort:
                raise KiteLoginError("Kite login callback cleanup failed") from None

    def _login_once(self) -> KiteCredentials:
        try:
            login_url = self._client.login_url()
        except Exception:
            raise KiteLoginError("failed to obtain the Kite login URL") from None
        if not isinstance(login_url, str) or not login_url.strip():
            raise KiteLoginError("Kite login URL is invalid")

        try:
            opened = self._browser_opener(login_url)
        except Exception:
            raise KiteLoginError("failed to open the Kite login URL") from None
        if opened is False:
            raise KiteLoginError("failed to open the Kite login URL")

        try:
            request_token = self._callback_receiver.receive_request_token()
        except KiteLoginError:
            raise
        except Exception:
            raise KiteLoginError("Kite login callback failed") from None
        if not isinstance(request_token, str) or not request_token.strip():
            raise KiteLoginError(
                "Kite login callback returned an invalid request token"
            )

        try:
            session = self._client.generate_session(
                request_token,
                api_secret=self._login_credentials.api_secret(),
            )
        except Exception:
            raise KiteLoginError("Kite session exchange failed") from None
        if not isinstance(session, Mapping):
            raise KiteLoginError("Kite session response is invalid")
        access_token = session.get("access_token")
        if not isinstance(access_token, str) or not access_token.strip():
            raise KiteLoginError(
                "Kite session response is missing an access token"
            )
        return KiteCredentials(self._login_credentials.api_key(), access_token)


def _parse_kite_callback(path: str, query: str) -> dict[str, str]:
    """Pure, socket-free parser shared by the real handler and its tests.

    Never returns or embeds the raw path/query in any value beyond the
    validated request_token itself, so a caller can safely exclude it from
    logs, responses, and exceptions.
    """

    if path != LOOPBACK_CALLBACK_PATH:
        return {"error": "unexpected_path"}
    try:
        parsed = parse_qs(
            query,
            strict_parsing=bool(query),
            keep_blank_values=True,
        )
    except ValueError:
        return {"error": "malformed_query"}
    if (
        "request_token" not in parsed
        or len(parsed["request_token"]) != 1
        or any(
            key in parsed and len(parsed[key]) != 1
            for key in ("action", "status")
        )
    ):
        return {"error": "malformed_query"}
    request_token = parsed["request_token"][0]
    action = parsed.get("action", ["login"])[0]
    status = parsed.get("status", ["success"])[0]
    if status != "success" or action != "login" or not request_token.strip():
        return {"error": "unsuccessful_callback"}
    return {"status": "success", "request_token": request_token}


class _CallbackServer(Protocol):
    result: dict[str, object]
    timeout: float | None

    def handle_request(self) -> None: ...

    def server_close(self) -> None: ...


def _default_loopback_server() -> _CallbackServer:
    import http.server

    class _LoopbackKiteCallbackHandler(http.server.BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.0"

        def log_message(self, format_string: str, *args: object) -> None:
            return

        def do_GET(self) -> None:  # noqa: N802 - stdlib handler method name
            parsed = urlsplit(self.path)
            result = _parse_kite_callback(parsed.path, parsed.query)
            self.server.result = result  # type: ignore[attr-defined]
            if result.get("status") == "success":
                self._respond(200, _SUCCESS_RESPONSE_BODY)
            elif result.get("error") == "unexpected_path":
                self._respond(404, _FAILURE_RESPONSE_BODY)
            else:
                self._respond(400, _FAILURE_RESPONSE_BODY)

        def _respond(self, status_code: int, body: bytes) -> None:
            self.close_connection = True
            self.send_response(status_code)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Connection", "close")
            self.end_headers()
            self.wfile.write(body)

    class _LoopbackKiteCallbackServer(http.server.HTTPServer):
        def __init__(self) -> None:
            super().__init__(
                (LOOPBACK_HOST, LOOPBACK_PORT),
                _LoopbackKiteCallbackHandler,
            )
            self.result: dict[str, object] = {}

    return _LoopbackKiteCallbackServer()


class LoopbackKiteCallbackReceiver:
    """Production callback receiver: binds 127.0.0.1:8765 before any browser opens.

    The socket is bound synchronously in the constructor, so a caller that
    constructs this receiver before constructing/using
    KiteInteractiveAuthenticator guarantees the listener exists before the
    login URL is ever opened in a browser.
    """

    def __init__(
        self,
        *,
        timeout_seconds: float = 120.0,
        server_factory: Callable[[], _CallbackServer] | None = None,
    ) -> None:
        if type(timeout_seconds) is bool or type(timeout_seconds) not in (int, float):
            raise TypeError("timeout_seconds must be a positive exact number")
        if not 0 < timeout_seconds <= MAXIMUM_LOOPBACK_TIMEOUT_SECONDS:
            raise ValueError(
                "timeout_seconds must be positive and at most "
                f"{MAXIMUM_LOOPBACK_TIMEOUT_SECONDS}"
            )
        self._used = False
        self._closed = False
        factory = server_factory or _default_loopback_server
        self._server = factory()
        self._server.timeout = timeout_seconds

    def receive_request_token(self) -> str:
        if self._used or self._closed:
            raise KiteLoginError("Kite login callback receiver is single-use")
        self._used = True
        try:
            self._server.handle_request()
        finally:
            self.close()
        result = self._server.result
        if result.get("status") != "success":
            raise KiteLoginError(
                "Kite login callback did not complete successfully"
            )
        request_token = result.get("request_token")
        if not isinstance(request_token, str) or not request_token:
            raise KiteLoginError("Kite login callback is missing a request token")
        return request_token

    def close(self) -> None:
        """Idempotently release the loopback port on every login exit path."""

        if self._closed:
            return
        self._closed = True
        try:
            self._server.server_close()
        except Exception:
            raise KiteLoginError("Kite login callback cleanup failed") from None
