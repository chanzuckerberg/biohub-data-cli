import json
import time
from pathlib import Path

import click
import pytest

from biohub_data_cli import auth, config

DISCOVERY = {
    "device_authorization_endpoint": "https://issuer.example/v1/device/authorize",
    "token_endpoint": "https://issuer.example/v1/token",
}
DISCOVERY_URL = "https://issuer.example/.well-known/openid-configuration"


class _Resp:
    def __init__(self, status_code: int = 200, payload: dict | None = None) -> None:
        self.status_code = status_code
        self._payload = payload or {}
        self.text = json.dumps(self._payload)

    def json(self) -> dict:
        return self._payload

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")


class _FakeRequests:
    """Routes (method, url) -> a _Resp, or a list of _Resp popped in order."""

    def __init__(self, routes: dict) -> None:
        self.routes = routes

    def _resolve(self, method: str, url: str) -> _Resp:
        r = self.routes[(method, url)]
        return r.pop(0) if isinstance(r, list) else r

    def get(self, url: str, **_: object) -> _Resp:
        return self._resolve("GET", url)

    def post(self, url: str, **_: object) -> _Resp:
        return self._resolve("POST", url)


@pytest.fixture(autouse=True)
def _isolate(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("BIOHUB_DATA_CLI_CONFIG_DIR", str(tmp_path / "cfg"))
    monkeypatch.setenv("BIOHUB_DATA_CLI_OIDC_ISSUER", "https://issuer.example")
    monkeypatch.setenv("BIOHUB_DATA_CLI_OIDC_CLIENT_ID", "cli-client")
    monkeypatch.delenv("ALL_DATA_API_TOKEN", raising=False)
    monkeypatch.setattr(
        auth.time, "sleep", lambda *_: None
    )  # don't actually poll-sleep


def test_login_device_flow_stores_token(monkeypatch: pytest.MonkeyPatch) -> None:
    device = {
        "device_code": "dc",
        "user_code": "ABCD",
        "verification_uri": "https://issuer.example/activate",
        "interval": 0,
        "expires_in": 60,
    }
    routes = {
        ("GET", DISCOVERY_URL): _Resp(200, DISCOVERY),
        ("POST", DISCOVERY["device_authorization_endpoint"]): _Resp(200, device),
        ("POST", DISCOVERY["token_endpoint"]): [
            _Resp(
                400, {"error": "authorization_pending"}
            ),  # first poll: not yet approved
            _Resp(
                200, {"access_token": "at1", "refresh_token": "rt1", "expires_in": 3600}
            ),
        ],
    }
    monkeypatch.setattr(auth, "requests", _FakeRequests(routes))

    auth.login()

    creds = json.loads(auth._credentials_path().read_text())
    assert creds["access_token"] == "at1"
    assert creds["refresh_token"] == "rt1"
    # and the download path picks it up via config.auth_token()
    assert config.auth_token() == "at1"


def test_login_unconfigured_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("BIOHUB_DATA_CLI_OIDC_ISSUER", raising=False)
    monkeypatch.delenv("BIOHUB_DATA_CLI_OIDC_CLIENT_ID", raising=False)
    monkeypatch.setattr(config, "_OIDC_ISSUER", None)
    monkeypatch.setattr(config, "_OIDC_CLIENT_ID", None)
    with pytest.raises(click.ClickException, match="not configured"):
        auth.login()


def test_load_access_token_none_when_not_logged_in() -> None:
    assert auth.load_access_token() is None


def test_valid_token_is_returned_without_network(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    auth._save(
        {
            "issuer": "https://issuer.example",
            "client_id": "cli-client",
            "access_token": "still-good",
            "refresh_token": "rt1",
            "expires_at": time.time() + 3600,
        }
    )
    # empty routes -> any network call KeyErrors; a valid token must not hit the network
    monkeypatch.setattr(auth, "requests", _FakeRequests({}))
    assert auth.load_access_token() == "still-good"


def test_expired_token_is_refreshed(monkeypatch: pytest.MonkeyPatch) -> None:
    auth._save(
        {
            "issuer": "https://issuer.example",
            "client_id": "cli-client",
            "access_token": "old",
            "refresh_token": "rt1",
            "expires_at": time.time() - 10,  # already expired
        }
    )
    routes = {
        ("GET", DISCOVERY_URL): _Resp(200, DISCOVERY),
        # note: no new refresh_token returned -> the old one must be preserved
        ("POST", DISCOVERY["token_endpoint"]): _Resp(
            200, {"access_token": "new", "expires_in": 3600}
        ),
    }
    monkeypatch.setattr(auth, "requests", _FakeRequests(routes))

    assert auth.load_access_token() == "new"
    creds = json.loads(auth._credentials_path().read_text())
    assert creds["access_token"] == "new"
    assert creds["refresh_token"] == "rt1"


def test_expired_token_without_refresh_returns_none(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    auth._save(
        {"access_token": "old", "expires_at": time.time() - 10}
    )  # no refresh_token
    monkeypatch.setattr(auth, "requests", _FakeRequests({}))
    assert auth.load_access_token() is None


def test_auth_token_prefers_env_over_cache(monkeypatch: pytest.MonkeyPatch) -> None:
    auth._save({"access_token": "cached", "expires_at": time.time() + 3600})
    monkeypatch.setenv("ALL_DATA_API_TOKEN", "env-token")
    assert config.auth_token() == "env-token"


def test_logout_removes_credentials() -> None:
    auth._save({"access_token": "x", "expires_at": time.time() + 3600})
    assert auth._credentials_path().exists()
    auth.logout()
    assert not auth._credentials_path().exists()
