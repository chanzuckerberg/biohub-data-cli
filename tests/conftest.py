import pytest


@pytest.fixture(autouse=True)
def _disable_analytics(request, monkeypatch):
    """Replace analytics.track with a no-op for every test so any code path that
    reaches it stays silent — otherwise tests that exercise the real download
    flow would emit events against the prod Amplitude ingest key."""
    if request.node.get_closest_marker("real_analytics"):
        return
    monkeypatch.setattr("biohub_data_cli.analytics.track", lambda *a, **k: None)
