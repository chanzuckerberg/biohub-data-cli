import pytest

import biohub_data_cli.config as config_mod


def test_override_env_wins_over_built_in(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(config_mod, "_OPS_SERVICE_URL", "https://prod.example.com")
    monkeypatch.setenv("OPS_SERVICE_URL_OVERRIDE", "http://localhost:8000/")
    # Trailing slash is stripped so callers can always do `f"{url}/path"`.
    assert config_mod.service_url() == "http://localhost:8000"


def test_falls_back_to_built_in(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(config_mod, "_OPS_SERVICE_URL", "https://prod.example.com")
    monkeypatch.delenv("OPS_SERVICE_URL_OVERRIDE", raising=False)
    assert config_mod.service_url() == "https://prod.example.com"


def test_raises_when_neither_set(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(config_mod, "_OPS_SERVICE_URL", None)
    monkeypatch.delenv("OPS_SERVICE_URL_OVERRIDE", raising=False)
    with pytest.raises(RuntimeError, match="No service URL configured"):
        config_mod.service_url()
