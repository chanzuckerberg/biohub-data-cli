"""Anonymous usage analytics to Amplitude.

Public API: call ``track(event, properties)`` at event points. The first
``track()`` lazily initializes the Amplitude client.

Only an anonymous, randomly generated ``device_id`` is sent. No paths, URLs,
hostnames, or other identifying values should ever be added to event properties.
"""

import atexit
import os
import uuid
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from typing import Any

from amplitude import Amplitude, BaseEvent
from platformdirs import user_config_dir

from biohub_data_cli.models import Collection, DownloadFailure

_APP_NAME = "biohub-data-cli"

# Amplitude write keys are intentionally embedded in the published package, as is
# standard for client-side analytics SDKs. They authorize event ingestion only —
# they cannot read data — so the worst case if leaked is spam events.
_DEV_KEY = "531141822a146f13d16eeaf96b8c91ec"
_PROD_KEY = "507382a5bad17ec853515118a6b8e7c1"

_client: Amplitude | None = None
_device_id: str | None = None
_cli_version: str | None = None


def _resolve_api_key() -> str:
    if os.environ.get("BIOHUB_CLI_ENV", "").strip().lower() == "dev":
        return _DEV_KEY
    return _PROD_KEY


def _load_or_create_device_id() -> str:
    config_dir = Path(user_config_dir(_APP_NAME))
    config_dir.mkdir(parents=True, exist_ok=True)
    path = config_dir / "device_id"
    if path.exists():
        existing = path.read_text().strip()
        if existing:
            return existing
    new_id = str(uuid.uuid4())
    # Non-atomic: two concurrent first-run invocations can each generate an ID and
    # the later writer wins, briefly splitting one user across two device IDs.
    # Accepted — happens at most once per machine and only affects first-run.
    path.write_text(new_id)
    try:
        path.chmod(0o600)
    except OSError:
        pass
    return new_id


def _resolve_cli_version() -> str:
    try:
        return version("biohub-data-cli")
    except PackageNotFoundError:
        return "unknown"


def _init() -> None:
    """Lazily initialize on first track(). No-op once ``_client`` is set; if
    init fails or is opted-out, the next track() call will try again."""
    global _client, _device_id, _cli_version
    if _client is not None:
        return
    if (
        os.environ.get("DISABLE_BIOHUB_DATA_CLI_ANALYTICS", "").strip().lower()
        == "true"
    ):
        return
    try:
        _device_id = _load_or_create_device_id()
        _cli_version = _resolve_cli_version()
        _client = Amplitude(_resolve_api_key())
        atexit.register(_client.shutdown)
    except Exception:
        _client = None
        _device_id = None
        _cli_version = None


def track(event_type: str, properties: dict[str, Any] | None = None) -> None:
    """Emit one event. Never raises; no-op if analytics is disabled."""
    _init()
    if _client is None:
        return
    try:
        props = dict(properties) if properties else {}
        props.setdefault("cli_version", _cli_version)
        _client.track(
            BaseEvent(
                event_type=event_type,
                device_id=_device_id,
                event_properties=props,
            )
        )
    except Exception:
        pass


def _classify_failure_reason(reason: str) -> str:
    """Map a free-form DownloadFailure.reason to a coarse Amplitude category.

    Categories are intentionally small so they remain useful as a chart
    breakdown — anything finer-grained belongs in logs. The matching is
    substring-based against the lowercased reason; producers (http/s3 utils)
    pass through OS / boto / requests error strings, so the patterns target
    the conventional wording those libraries emit.
    """
    r = reason.lower()
    if (
        "404" in r
        or "nosuchkey" in r
        or "no object found" in r
        or "no objects found" in r
    ):
        return "not_found"
    if "403" in r or "401" in r or "accessdenied" in r or "forbidden" in r:
        return "auth"
    if "enospc" in r:
        return "disk"
    if (
        "timeout" in r
        or "timed out" in r
        or "connection" in r
        or "name resolution" in r
        or "dns" in r
    ):
        return "network"
    if "unsupported url scheme" in r:
        return "unsupported_url"
    return "other"


def _collection_event_properties(collection: Collection) -> dict[str, Any]:
    """Base property bag every collection-scoped event ships with."""
    return {
        "collection_id": collection.id,
        "collection_slug": collection.slug,
        "collection_name": collection.title,
    }


def track_collection_downloads_initiated(collections: list[Collection]) -> None:
    """Emit `data_cli_collection_download_initiated` once per collection. Paired
    with `track_collection_download_outcomes` to form the initiated→terminal
    funnel; missing terminal events (e.g. on KeyboardInterrupt) register as
    abandonment in Amplitude funnel reports."""
    for collection in collections:
        track(
            "data_cli_collection_download_initiated",
            _collection_event_properties(collection),
        )


def track_collection_download_outcomes(
    collections: list[Collection],
    failures: list[DownloadFailure],
    bytes_by_collection: dict[str, int],
) -> None:
    """Emit one terminal event per collection: `data_cli_collection_download_completed`
    on success, `data_cli_collection_download_failed` on any recorded failure.
    Both events carry `bytes_downloaded` so partial progress before a failure
    is visible alongside successful totals.
    """
    first_failure_by_collection: dict[str, DownloadFailure] = {}
    for f in failures:
        first_failure_by_collection.setdefault(f.collection_slug, f)
    for collection in collections:
        props = {
            **_collection_event_properties(collection),
            "bytes_downloaded": bytes_by_collection.get(collection.slug, 0),
        }
        failure = first_failure_by_collection.get(collection.slug)
        if failure is not None:
            track(
                "data_cli_collection_download_failed",
                {**props, "failure_reason": _classify_failure_reason(failure.reason)},
            )
        else:
            track("data_cli_collection_download_completed", props)
