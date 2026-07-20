from pathlib import Path

import pytest

import biohub_data_cli.config as config_mod


def test_override_env_wins_over_built_in(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(config_mod, "_ALL_DATA_API_URL", "https://prod.example.com")
    monkeypatch.setenv("ALL_DATA_API_URL_OVERRIDE", "http://localhost:8002/")
    # Trailing slash is stripped so callers can always do `f"{url}/path"`.
    assert config_mod.service_url() == "http://localhost:8002"


def test_falls_back_to_built_in(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(config_mod, "_ALL_DATA_API_URL", "https://prod.example.com")
    monkeypatch.delenv("ALL_DATA_API_URL_OVERRIDE", raising=False)
    assert config_mod.service_url() == "https://prod.example.com"


def test_raises_when_neither_set(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(config_mod, "_ALL_DATA_API_URL", None)
    monkeypatch.delenv("ALL_DATA_API_URL_OVERRIDE", raising=False)
    with pytest.raises(RuntimeError, match="No service URL configured"):
        config_mod.service_url()


def test_auth_token_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ALL_DATA_API_TOKEN", "tok-123")
    assert config_mod.auth_token() == "tok-123"


def test_auth_token_none_when_unset(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.delenv("ALL_DATA_API_TOKEN", raising=False)
    # No env token and no cached login (isolated empty config dir) -> None.
    monkeypatch.setenv("BIOHUB_DATA_CLI_CONFIG_DIR", str(tmp_path))
    assert config_mod.auth_token() is None
