import pytest


@pytest.fixture(autouse=True)
def _disable_analytics(request, monkeypatch):
    """Replace analytics.track with a no-op for every test so any code path that
    reaches it stays silent — otherwise tests that exercise the real download
    flow would emit events against the prod Amplitude ingest key.

    analytics_test.py exercises the analytics module itself end-to-end and
    needs the real track in place, so it's exempted here. Tests elsewhere that
    explicitly patch analytics.track to assert on calls nest cleanly over this
    no-op; their patch is restored on exit, leaving the no-op behind."""
    if request.node.path.name == "analytics_test.py":
        return
    monkeypatch.setattr("biohub_data_cli.analytics.track", lambda *a, **k: None)
