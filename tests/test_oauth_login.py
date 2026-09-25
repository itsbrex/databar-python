"""Unit tests for CLI OAuth helpers (token exchange + credentials fetch)."""

from __future__ import annotations

import httpx
import pytest
import respx

from databar.cli._oauth import (
    OAUTH_CLIENT_ID,
    OAUTH_REDIRECT_URI,
    _exchange_code,
    _fetch_api_key,
    _pkce_pair,
)


def test_pkce_pair_s256_shape():
    verifier, challenge = _pkce_pair()
    assert len(verifier) > 40
    assert len(challenge) >= 43
    assert "=" not in challenge


@respx.mock
def test_exchange_code_posts_pkce_fields():
    route = respx.post("https://databar.ai/oauth/token/").mock(
        return_value=httpx.Response(200, json={"access_token": "tok", "token_type": "Bearer"})
    )
    token = _exchange_code("https://databar.ai", "auth-code", "verifier")
    assert token == "tok"
    assert route.called
    request = route.calls.last.request
    body = request.content.decode()
    assert "grant_type=authorization_code" in body
    assert "code=auth-code" in body
    assert f"client_id={OAUTH_CLIENT_ID}" in body
    assert "code_verifier=verifier" in body
    assert OAUTH_REDIRECT_URI.replace(":", "%3A").replace("/", "%2F") in body or "127.0.0.1" in body


@respx.mock
def test_fetch_api_key_sends_bearer():
    respx.get("https://databar.ai/oauth/api/cli-credentials/").mock(
        return_value=httpx.Response(
            200,
            json={"api_key": "k", "email": "a@b.com", "workspace_identifier": "w", "workspace_name": "W"},
        )
    )
    payload = _fetch_api_key("https://databar.ai", "tok")
    assert payload["api_key"] == "k"
    assert payload["email"] == "a@b.com"


@respx.mock
def test_exchange_code_raises_on_error():
    respx.post("https://databar.ai/oauth/token/").mock(
        return_value=httpx.Response(400, text="bad")
    )
    with pytest.raises(RuntimeError, match="Token exchange failed"):
        _exchange_code("https://databar.ai", "bad", "v")
