from unittest.mock import MagicMock, patch

import pytest

from biohub_data_cli import analytics
from biohub_data_cli.models import Collection, DownloadFailure

pytestmark = pytest.mark.real_analytics


@pytest.fixture(autouse=True)
def reset_analytics_state(tmp_path, monkeypatch):
    """Reset module globals, redirect config dir to a tmp path, and pre-mock the
    Amplitude SDK class."""
    analytics._client = None
    analytics._device_id = None
    analytics._cli_version = None
    analytics._init_done = False
    monkeypatch.setattr(analytics, "user_config_dir", lambda _name: str(tmp_path))
    monkeypatch.delenv("BIOHUB_CLI_ENV", raising=False)
    monkeypatch.delenv("DISABLE_BIOHUB_DATA_CLI_ANALYTICS", raising=False)
    monkeypatch.setattr(analytics, "Amplitude", MagicMock())
    yield
    analytics._client = None
    analytics._device_id = None
    analytics._cli_version = None
    analytics._init_done = False


# ── init / device_id ─────────────────────────────────────────────────────────


def test_init_constructs_client_and_registers_shutdown(monkeypatch):
    monkeypatch.setattr(analytics, "_PROD_KEY", "fake-key")
    fake_amplitude = MagicMock()
    monkeypatch.setattr(analytics, "Amplitude", fake_amplitude)
    with patch("biohub_data_cli.analytics.atexit.register") as fake_atexit:
        analytics._init()
    fake_amplitude.assert_called_once_with("fake-key")
    fake_atexit.assert_called_once_with(fake_amplitude.return_value.shutdown)
    assert analytics._client is fake_amplitude.return_value
    assert analytics._device_id is not None


def test_init_noop_when_client_already_set(monkeypatch):
    monkeypatch.setattr(analytics, "_PROD_KEY", "fake-key")
    fake_amplitude = MagicMock()
    monkeypatch.setattr(analytics, "Amplitude", fake_amplitude)
    analytics._init()
    analytics._init()
    assert fake_amplitude.call_count == 1


def test_init_does_not_retry_after_failure(monkeypatch):
    """Init is one-shot: once it has failed, subsequent track() calls must
    not re-attempt disk I/O or SDK construction for the rest of the process."""
    monkeypatch.setattr(analytics, "_PROD_KEY", "fake-key")
    failing = MagicMock(side_effect=RuntimeError("boom"))
    monkeypatch.setattr(analytics, "Amplitude", failing)
    analytics._init()
    assert analytics._client is None
    assert failing.call_count == 1

    succeeding = MagicMock()
    monkeypatch.setattr(analytics, "Amplitude", succeeding)
    analytics._init()
    assert analytics._client is None
    succeeding.assert_not_called()


@pytest.mark.parametrize("value", ["true", "True", "TRUE", "  true  "])
def test_init_noop_when_opt_out_env_true(monkeypatch, value):
    monkeypatch.setenv("DISABLE_BIOHUB_DATA_CLI_ANALYTICS", value)
    monkeypatch.setattr(analytics, "_PROD_KEY", "fake-key")
    fake_amplitude = MagicMock()
    monkeypatch.setattr(analytics, "Amplitude", fake_amplitude)
    analytics._init()
    assert analytics._client is None
    fake_amplitude.assert_not_called()


@pytest.mark.parametrize("value", ["1", "yes", "false", "0", ""])
def test_init_proceeds_when_opt_out_env_not_true(monkeypatch, value):
    monkeypatch.setenv("DISABLE_BIOHUB_DATA_CLI_ANALYTICS", value)
    monkeypatch.setattr(analytics, "_PROD_KEY", "fake-key")
    monkeypatch.setattr(analytics, "Amplitude", MagicMock())
    analytics._init()
    assert analytics._client is not None


def test_init_does_not_recheck_opt_out_env(monkeypatch):
    """Opt-out is one-shot: once we've seen DISABLE=true and bailed, a later
    track() should not re-read the env var, even if it's been unset in
    between."""
    monkeypatch.setenv("DISABLE_BIOHUB_DATA_CLI_ANALYTICS", "true")
    monkeypatch.setattr(analytics, "_PROD_KEY", "fake-key")
    fake_amplitude = MagicMock()
    monkeypatch.setattr(analytics, "Amplitude", fake_amplitude)

    analytics._init()
    assert analytics._client is None

    monkeypatch.delenv("DISABLE_BIOHUB_DATA_CLI_ANALYTICS")
    analytics._init()
    assert analytics._client is None
    fake_amplitude.assert_not_called()


def test_init_swallows_exceptions(monkeypatch):
    monkeypatch.setattr(analytics, "_PROD_KEY", "fake-key")
    monkeypatch.setattr(
        analytics, "Amplitude", MagicMock(side_effect=RuntimeError("boom"))
    )
    analytics._init()  # must not raise
    assert analytics._client is None


def test_device_id_persists_across_init_calls(monkeypatch, tmp_path):
    monkeypatch.setattr(analytics, "_PROD_KEY", "fake-key")
    monkeypatch.setattr(analytics, "Amplitude", MagicMock())

    analytics._init()
    first_id = analytics._device_id

    # Simulate a second process: reset _init_done so _init() actually runs.
    analytics._client = None
    analytics._device_id = None
    analytics._init_done = False
    analytics._init()
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


def test_track_noop_when_opted_out(monkeypatch):
    monkeypatch.setenv("DISABLE_BIOHUB_DATA_CLI_ANALYTICS", "true")
    monkeypatch.setattr(analytics, "_PROD_KEY", "fake-key")
    fake_amplitude = MagicMock()
    monkeypatch.setattr(analytics, "Amplitude", fake_amplitude)

    analytics.track("some_event", {"foo": "bar"})  # must not raise
    fake_amplitude.assert_not_called()


def test_track_lazily_initializes(monkeypatch):
    monkeypatch.setattr(analytics, "_PROD_KEY", "fake-key")
    fake_amplitude = MagicMock()
    monkeypatch.setattr(analytics, "Amplitude", fake_amplitude)

    assert analytics._client is None
    analytics.track("first_event")
    fake_amplitude.assert_called_once_with("fake-key")
    assert analytics._client is fake_amplitude.return_value


def test_track_sends_device_id_not_user_id(monkeypatch):
    monkeypatch.setattr(analytics, "_PROD_KEY", "fake-key")
    fake_amplitude = MagicMock()
    monkeypatch.setattr(analytics, "Amplitude", fake_amplitude)

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

    analytics.track("anything", {})  # must not raise


def test_track_caller_properties_not_mutated(monkeypatch):
    monkeypatch.setattr(analytics, "_PROD_KEY", "fake-key")
    monkeypatch.setattr(analytics, "Amplitude", MagicMock())

    caller_props = {"bytes": 1}
    analytics.track("e", caller_props)
    assert "cli_version" not in caller_props


# ── track_collection_downloads_initiated ────────────────────────────────────


def test_initiated_emits_one_event_carrying_all_collections():
    coll_a = Collection.model_validate(
        {"id": "c1", "slug": "a", "title": "Alpha", "datasets": []}
    )
    coll_b = Collection.model_validate(
        {"id": "c2", "slug": "b", "title": "Beta", "datasets": []}
    )
    with patch("biohub_data_cli.analytics.track") as mock_track:
        analytics.track_collection_downloads_initiated([coll_a, coll_b])

    mock_track.assert_called_once_with(
        "data_cli_collection_download_initiated",
        {
            "collections": [
                {
                    "collection_id": "c1",
                    "collection_slug": "a",
                    "collection_title": "Alpha",
                },
                {
                    "collection_id": "c2",
                    "collection_slug": "b",
                    "collection_title": "Beta",
                },
            ],
        },
    )


# ── _classify_failure_reason ────────────────────────────────────────────────


@pytest.mark.parametrize(
    "reason,expected",
    [
        ("404 Not Found", "not_found"),
        ("NoSuchKey", "not_found"),
        ("No objects found at s3://bucket/key/", "not_found"),
        ("AccessDenied: anonymous request", "auth"),
        ("403 Forbidden", "auth"),
        ("[Errno 28] ENOSPC: no space left on device", "disk"),
        ("Connection timed out", "network"),
        ("HTTPSConnectionPool: Read timed out", "network"),
        ("Failed to resolve: name resolution failure", "network"),
        ("Unsupported URL scheme", "unsupported_url"),
        ("something weird", "other"),
    ],
)
def test_classify_failure_reason(reason, expected):
    assert analytics._classify_failure_reason(reason) == expected


# ── track_collection_download_outcomes ──────────────────────────────────────


def test_outcomes_emits_completed_when_there_are_no_failures():
    with patch("biohub_data_cli.analytics.track") as mock_track:
        analytics.track_collection_download_outcomes(failures=[], bytes_downloaded=4096)
    mock_track.assert_called_once_with(
        "data_cli_collection_download_completed",
        {"bytes_downloaded": 4096},
    )


def test_outcomes_emits_failed_with_classified_reason_when_failures_present():
    failure = DownloadFailure(
        collection_slug="coll-a",
        dataset_slug="ds-1",
        url="https://example.com/x",
        reason="403 Forbidden",
    )
    with patch("biohub_data_cli.analytics.track") as mock_track:
        analytics.track_collection_download_outcomes(
            failures=[failure], bytes_downloaded=512
        )
    mock_track.assert_called_once_with(
        "data_cli_collection_download_failed",
        {"bytes_downloaded": 512, "failure_reasons": ["auth"]},
    )


def test_outcomes_failure_reasons_deduped_and_sorted():
    """Multiple failures spanning categories collapse to a sorted, deduped
    list — one terminal event per run regardless of how many workers failed."""
    failures = [
        DownloadFailure(
            collection_slug="coll-a",
            dataset_slug="ds-1",
            url="u1",
            reason="Connection timed out",  # → network
        ),
        DownloadFailure(
            collection_slug="coll-a",
            dataset_slug="ds-2",
            url="u2",
            reason="403 Forbidden",  # → auth
        ),
        DownloadFailure(
            collection_slug="coll-b",
            dataset_slug="ds-1",
            url="u3",
            reason="HTTPSConnectionPool: Read timed out",  # → network (dup)
        ),
    ]
    with patch("biohub_data_cli.analytics.track") as mock_track:
        analytics.track_collection_download_outcomes(
            failures=failures, bytes_downloaded=0
        )
    assert mock_track.call_args.args[1]["failure_reasons"] == ["auth", "network"]


def test_outcomes_zero_bytes_still_emits_completed_when_no_failures():
    """An all-empty download (e.g. every dataset had empty URL lists) still
    emits a completed event with 0 bytes."""
    with patch("biohub_data_cli.analytics.track") as mock_track:
        analytics.track_collection_download_outcomes(failures=[], bytes_downloaded=0)
    mock_track.assert_called_once_with(
        "data_cli_collection_download_completed",
        {"bytes_downloaded": 0},
    )
