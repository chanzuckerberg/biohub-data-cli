from unittest.mock import MagicMock, patch

import pytest

from biohub_data_cli import analytics


@pytest.fixture(autouse=True)
def reset_analytics_state(tmp_path, monkeypatch):
    """Reset module globals + redirect config dir to a tmp path."""
    analytics._client = None
    analytics._device_id = None
    analytics._cli_version = None
    monkeypatch.setattr(analytics, "user_config_dir", lambda _name: str(tmp_path))
    monkeypatch.delenv("BIOHUB_CLI_ENV", raising=False)
    yield
    analytics._client = None
    analytics._device_id = None
    analytics._cli_version = None


# ── init / device_id ─────────────────────────────────────────────────────────


def test_init_noop_when_keys_empty(monkeypatch):
    monkeypatch.setattr(analytics, "_DEV_KEY", "")
    monkeypatch.setattr(analytics, "_PROD_KEY", "")
    analytics.init()
    assert analytics._client is None


def test_init_constructs_client_and_registers_shutdown(monkeypatch):
    monkeypatch.setattr(analytics, "_PROD_KEY", "fake-key")
    fake_amplitude = MagicMock()
    monkeypatch.setattr(analytics, "Amplitude", fake_amplitude)
    with patch("biohub_data_cli.analytics.atexit.register") as fake_atexit:
        analytics.init()
    fake_amplitude.assert_called_once_with("fake-key")
    fake_atexit.assert_called_once_with(fake_amplitude.return_value.shutdown)
    assert analytics._client is fake_amplitude.return_value
    assert analytics._device_id is not None


def test_init_is_idempotent(monkeypatch):
    monkeypatch.setattr(analytics, "_PROD_KEY", "fake-key")
    fake_amplitude = MagicMock()
    monkeypatch.setattr(analytics, "Amplitude", fake_amplitude)
    analytics.init()
    analytics.init()
    assert fake_amplitude.call_count == 1


@pytest.mark.parametrize("value", ["true", "True", "TRUE"])
def test_init_noop_when_opt_out_env_true(monkeypatch, value):
    monkeypatch.setenv("DISABLE_BIOHUB_DATA_CLI_ANALYTICS", value)
    monkeypatch.setattr(analytics, "_PROD_KEY", "fake-key")
    fake_amplitude = MagicMock()
    monkeypatch.setattr(analytics, "Amplitude", fake_amplitude)
    analytics.init()
    assert analytics._client is None
    fake_amplitude.assert_not_called()


@pytest.mark.parametrize("value", ["1", "yes", "false", "0", ""])
def test_init_proceeds_when_opt_out_env_not_true(monkeypatch, value):
    monkeypatch.setenv("DISABLE_BIOHUB_DATA_CLI_ANALYTICS", value)
    monkeypatch.setattr(analytics, "_PROD_KEY", "fake-key")
    monkeypatch.setattr(analytics, "Amplitude", MagicMock())
    analytics.init()
    assert analytics._client is not None


def test_init_swallows_exceptions(monkeypatch):
    monkeypatch.setattr(analytics, "_PROD_KEY", "fake-key")
    monkeypatch.setattr(
        analytics, "Amplitude", MagicMock(side_effect=RuntimeError("boom"))
    )
    analytics.init()  # must not raise
    assert analytics._client is None


def test_device_id_persists_across_init_calls(monkeypatch, tmp_path):
    monkeypatch.setattr(analytics, "_PROD_KEY", "fake-key")
    monkeypatch.setattr(analytics, "Amplitude", MagicMock())

    analytics.init()
    first_id = analytics._device_id

    analytics._client = None
    analytics._device_id = None
    analytics.init()
    second_id = analytics._device_id

    assert first_id == second_id
    assert (tmp_path / "device_id").exists()


def test_dev_key_selected_when_env_dev(monkeypatch):
    monkeypatch.setenv("BIOHUB_CLI_ENV", "dev")
    monkeypatch.setattr(analytics, "_DEV_KEY", "dev-key")
    monkeypatch.setattr(analytics, "_PROD_KEY", "prod-key")
    assert analytics._resolve_api_key() == "dev-key"


def test_prod_key_selected_by_default(monkeypatch):
    monkeypatch.setattr(analytics, "_DEV_KEY", "dev-key")
    monkeypatch.setattr(analytics, "_PROD_KEY", "prod-key")
    assert analytics._resolve_api_key() == "prod-key"


# ── track ────────────────────────────────────────────────────────────────────


def test_track_noop_when_not_initialized():
    analytics.track("some_event", {"foo": "bar"})  # must not raise


def test_track_sends_device_id_not_user_id(monkeypatch):
    monkeypatch.setattr(analytics, "_PROD_KEY", "fake-key")
    fake_amplitude = MagicMock()
    monkeypatch.setattr(analytics, "Amplitude", fake_amplitude)

    analytics.init()
    analytics.track("download_completed", {"bytes": 123})

    fake_client = fake_amplitude.return_value
    fake_client.track.assert_called_once()
    sent_event = fake_client.track.call_args.args[0]
    assert sent_event.device_id == analytics._device_id
    assert sent_event.user_id is None
    assert sent_event.event_type == "download_completed"
    assert sent_event.event_properties["bytes"] == 123
    assert "cli_version" in sent_event.event_properties


def test_track_swallows_exceptions(monkeypatch):
    monkeypatch.setattr(analytics, "_PROD_KEY", "fake-key")
    fake_amplitude = MagicMock()
    fake_amplitude.return_value.track.side_effect = RuntimeError("network down")
    monkeypatch.setattr(analytics, "Amplitude", fake_amplitude)

    analytics.init()
    analytics.track("anything", {})  # must not raise


def test_track_caller_properties_not_mutated(monkeypatch):
    monkeypatch.setattr(analytics, "_PROD_KEY", "fake-key")
    monkeypatch.setattr(analytics, "Amplitude", MagicMock())

    analytics.init()
    caller_props = {"bytes": 1}
    analytics.track("e", caller_props)
    assert "cli_version" not in caller_props
