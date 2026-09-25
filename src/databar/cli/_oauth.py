"""Browser OAuth 2.1 (PKCE) login for `databar login`.

Opens the existing Databar consent screen, catches the authorization code on a
local loopback redirect, exchanges it for a short-lived bearer token, then
fetches the workspace API key and writes it to ~/.databar/config.
"""

from __future__ import annotations

import base64
import hashlib
import os
import secrets
import threading
import webbrowser
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Optional
from urllib.parse import parse_qs, urlencode, urlparse

import httpx

# Must match oauth.0002_seed_cli_oauth_application / settings.DATABAR_CLI_*.
OAUTH_CLIENT_ID = "databar-cli"
OAUTH_REDIRECT_URI = "http://127.0.0.1:53682/callback"
OAUTH_CALLBACK_PORT = 53682
OAUTH_SCOPE = "cli:access"
DEFAULT_OAUTH_ISSUER = "https://databar.ai"
_LOGIN_TIMEOUT_S = 300


def _oauth_issuer() -> str:
    return (os.environ.get("DATABAR_OAUTH_ISSUER") or DEFAULT_OAUTH_ISSUER).rstrip("/")


def _pkce_pair() -> tuple[str, str]:
    verifier = secrets.token_urlsafe(64)
    challenge = base64.urlsafe_b64encode(hashlib.sha256(verifier.encode()).digest()).decode().rstrip("=")
    return verifier, challenge


class _CallbackResult:
    def __init__(self) -> None:
        self.code: Optional[str] = None
        self.state: Optional[str] = None
        self.error: Optional[str] = None
        self.error_description: Optional[str] = None
        self.event = threading.Event()


def _make_handler(result: _CallbackResult, expected_state: str):
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, format: str, *args) -> None:  # noqa: A003
            return

        def do_GET(self) -> None:  # noqa: N802
            parsed = urlparse(self.path)
            if parsed.path.rstrip("/") != "/callback":
                self.send_response(404)
                self.end_headers()
                return

            qs = parse_qs(parsed.query)
            result.error = (qs.get("error") or [None])[0]
            result.error_description = (qs.get("error_description") or [None])[0]
            result.code = (qs.get("code") or [None])[0]
            result.state = (qs.get("state") or [None])[0]

            if result.error:
                body = (
                    b"<html><body><h1>Login failed</h1>"
                    b"<p>You can close this window and return to the terminal.</p></body></html>"
                )
                self.send_response(400)
            elif result.state != expected_state:
                result.error = "invalid_state"
                result.error_description = "OAuth state mismatch"
                body = (
                    b"<html><body><h1>Login failed</h1>"
                    b"<p>State mismatch. Close this window and try again.</p></body></html>"
                )
                self.send_response(400)
            else:
                body = (
                    b"<html><body><h1>Logged in</h1>"
                    b"<p>You can close this window and return to the terminal.</p></body></html>"
                )
                self.send_response(200)

            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            result.event.set()

    return Handler


def _exchange_code(issuer: str, code: str, verifier: str) -> str:
    response = httpx.post(
        f"{issuer}/oauth/token/",
        data={
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": OAUTH_REDIRECT_URI,
            "client_id": OAUTH_CLIENT_ID,
            "code_verifier": verifier,
        },
        headers={"Accept": "application/json"},
        timeout=30.0,
    )
    if response.status_code != 200:
        detail = response.text[:200]
        raise RuntimeError(f"Token exchange failed ({response.status_code}): {detail}")
    payload = response.json()
    access_token = payload.get("access_token")
    if not access_token:
        raise RuntimeError("Token exchange response missing access_token")
    return access_token


def _fetch_api_key(issuer: str, access_token: str) -> dict:
    response = httpx.get(
        f"{issuer}/oauth/api/cli-credentials/",
        headers={"Authorization": f"Bearer {access_token}", "Accept": "application/json"},
        timeout=30.0,
    )
    if response.status_code != 200:
        detail = response.text[:200]
        raise RuntimeError(f"Could not fetch API key ({response.status_code}): {detail}")
    payload = response.json()
    if not payload.get("api_key"):
        raise RuntimeError("Credentials response missing api_key")
    return payload


def run_browser_login(*, print_url=None) -> dict:
    """Run the full browser OAuth flow.

    Returns credentials dict with ``api_key`` and ``email``.
    ``print_url`` is called with the authorize URL (so the CLI can show it if
    the browser fails to open).
    """
    issuer = _oauth_issuer()
    verifier, challenge = _pkce_pair()
    state = secrets.token_urlsafe(24)
    result = _CallbackResult()

    try:
        server = HTTPServer(("127.0.0.1", OAUTH_CALLBACK_PORT), _make_handler(result, state))
    except OSError as exc:
        raise RuntimeError(
            f"Port {OAUTH_CALLBACK_PORT} is already in use. "
            f"Close the other process and run `databar login` again."
        ) from exc

    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()

    params = {
        "client_id": OAUTH_CLIENT_ID,
        "redirect_uri": OAUTH_REDIRECT_URI,
        "response_type": "code",
        "scope": OAUTH_SCOPE,
        "code_challenge": challenge,
        "code_challenge_method": "S256",
        "state": state,
    }
    authorize_url = f"{issuer}/oauth/authorize/?{urlencode(params)}"

    opened = webbrowser.open(authorize_url)
    if print_url is not None:
        print_url(authorize_url, opened=opened)

    if not result.event.wait(timeout=_LOGIN_TIMEOUT_S):
        server.shutdown()
        thread.join(timeout=2)
        raise RuntimeError("Timed out waiting for browser login. Run `databar login` again.")

    server.shutdown()
    thread.join(timeout=2)

    if result.error:
        desc = result.error_description or result.error
        raise RuntimeError(f"Login was denied or failed: {desc}")

    if not result.code:
        raise RuntimeError("No authorization code received from the browser callback.")

    access_token = _exchange_code(issuer, result.code, verifier)
    return _fetch_api_key(issuer, access_token)
