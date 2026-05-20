from unittest.mock import MagicMock, patch

import pytest

from biohub_data_cli import analytics
from biohub_data_cli.models import Collection, DownloadFailure


@pytest.fixture(autouse=True)
def reset_analytics_state(tmp_path, monkeypatch):
    """Reset module globals, redirect config dir to a tmp path, and pre-mock the
    Amplitude SDK class."""
    analytics._client = None
    analytics._device_id = None
    analytics._cli_version = None
    monkeypatch.setattr(analytics, "user_config_dir", lambda _name: str(tmp_path))
    monkeypatch.delenv("BIOHUB_CLI_ENV", raising=False)
    monkeypatch.setattr(analytics, "Amplitude", MagicMock())
    yield
    analytics._client = None
    analytics._device_id = None
    analytics._cli_version = None


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


def test_init_retries_after_failure(monkeypatch):
    monkeypatch.setattr(analytics, "_PROD_KEY", "fake-key")
    failing = MagicMock(side_effect=RuntimeError("boom"))
    monkeypatch.setattr(analytics, "Amplitude", failing)
    analytics._init()
    assert analytics._client is None

    succeeding = MagicMock()
    monkeypatch.setattr(analytics, "Amplitude", succeeding)
    analytics._init()
    assert analytics._client is succeeding.return_value


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

    analytics._client = None
    analytics._device_id = None
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


def test_initiated_emits_one_event_per_collection():
    coll_a = Collection.model_validate(
        {"id": "c1", "slug": "a", "title": "Alpha", "datasets": []}
    )
    coll_b = Collection.model_validate(
        {"id": "c2", "slug": "b", "title": "Beta", "datasets": []}
    )
    with patch("biohub_data_cli.analytics.track") as mock_track:
        analytics.track_collection_downloads_initiated([coll_a, coll_b])

    assert [c.args for c in mock_track.call_args_list] == [
        (
            "data_cli_collection_download_initiated",
            {"collection_id": "c1", "collection_slug": "a", "collection_name": "Alpha"},
        ),
        (
            "data_cli_collection_download_initiated",
            {"collection_id": "c2", "collection_slug": "b", "collection_name": "Beta"},
        ),
    ]


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


def _make_collection(coll_id: str, slug: str, title: str) -> Collection:
    """Minimal Collection with one placeholder dataset (downloads aren't run here)."""
    return Collection.model_validate(
        {
            "id": coll_id,
            "slug": slug,
            "title": title,
            "datasets": [
                {
                    "id": "ds-1",
                    "slug": "ds-1",
                    "title": "DS",
                    "file_format": "parquet",
                    "urls": ["https://example.com/x.parquet"],
                }
            ],
        }
    )


def test_outcomes_emits_completed_when_collection_has_no_failures():
    coll = _make_collection("c1", "coll-a", "Alpha")
    with patch("biohub_data_cli.analytics.track") as mock_track:
        analytics.track_collection_download_outcomes(
            collections=[coll], failures=[], bytes_by_collection={"coll-a": 4096}
        )
    mock_track.assert_called_once_with(
        "data_cli_collection_download_completed",
        {
            "collection_id": "c1",
            "collection_slug": "coll-a",
            "collection_name": "Alpha",
            "bytes_downloaded": 4096,
        },
    )


def test_outcomes_emits_failed_with_classified_reason_when_failures_present():
    coll = _make_collection("c1", "coll-a", "Alpha")
    failure = DownloadFailure(
        collection_slug="coll-a",
        dataset_slug="ds-1",
        url="https://example.com/x",
        reason="403 Forbidden",
    )
    with patch("biohub_data_cli.analytics.track") as mock_track:
        analytics.track_collection_download_outcomes(
            collections=[coll], failures=[failure], bytes_by_collection={}
        )
    mock_track.assert_called_once_with(
        "data_cli_collection_download_failed",
        {
            "collection_id": "c1",
            "collection_slug": "coll-a",
            "collection_name": "Alpha",
            "failure_reason": "auth",
        },
    )


def test_outcomes_only_first_failure_per_collection_drives_reason():
    """Multiple failures in one collection — only the first one's reason is emitted."""
    coll = _make_collection("c1", "coll-a", "Alpha")
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
            reason="403 Forbidden",  # → auth, ignored
        ),
    ]
    with patch("biohub_data_cli.analytics.track") as mock_track:
        analytics.track_collection_download_outcomes(
            collections=[coll], failures=failures, bytes_by_collection={}
        )
    assert mock_track.call_args.args[1]["failure_reason"] == "network"


def test_outcomes_handles_mixed_success_and_failure_across_collections():
    """One completed event and one failed event when two collections have
    different outcomes — verifies that we don't lump them together."""
    coll_ok = _make_collection("c1", "coll-ok", "OK")
    coll_bad = _make_collection("c2", "coll-bad", "Bad")
    failure = DownloadFailure(
        collection_slug="coll-bad",
        dataset_slug="ds-1",
        url="u",
        reason="NoSuchKey",
    )
    with patch("biohub_data_cli.analytics.track") as mock_track:
        analytics.track_collection_download_outcomes(
            collections=[coll_ok, coll_bad],
            failures=[failure],
            bytes_by_collection={"coll-ok": 100},
        )

    events = [(c.args[0], c.args[1]) for c in mock_track.call_args_list]
    assert events == [
        (
            "data_cli_collection_download_completed",
            {
                "collection_id": "c1",
                "collection_slug": "coll-ok",
                "collection_name": "OK",
                "bytes_downloaded": 100,
            },
        ),
        (
            "data_cli_collection_download_failed",
            {
                "collection_id": "c2",
                "collection_slug": "coll-bad",
                "collection_name": "Bad",
                "failure_reason": "not_found",
            },
        ),
    ]


def test_outcomes_missing_bytes_entry_treated_as_zero():
    """A successful collection that never accumulated any bytes (e.g. all its
    datasets had empty URL lists) still emits a completed event with 0 bytes."""
    coll = _make_collection("c1", "coll-a", "Alpha")
    with patch("biohub_data_cli.analytics.track") as mock_track:
        analytics.track_collection_download_outcomes(
            collections=[coll], failures=[], bytes_by_collection={}
        )
    assert mock_track.call_args.args[1]["bytes_downloaded"] == 0
