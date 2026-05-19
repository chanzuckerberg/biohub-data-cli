"""Anonymous usage analytics to Amplitude.

Public API: ``init()`` once at CLI startup, then ``track(event, properties)``
at event points. Both functions are best-effort and never raise — an analytics
bug must not break a user's command.

Only an anonymous, randomly generated ``device_id`` is sent. No paths, URLs,
hostnames, or other identifying values should ever be added to event properties.
"""

import atexit
import os
import uuid
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from typing import Any, Optional

from amplitude import Amplitude, BaseEvent
from platformdirs import user_config_dir

_APP_NAME = "biohub-data-cli"

# TODO: populate once Amplitude projects are provisioned by ops.
# Until both are non-empty, init() is a no-op even when analytics is enabled.
_DEV_KEY = ""
_PROD_KEY = ""

_client: Optional[Amplitude] = None
_device_id: Optional[str] = None
_cli_version: Optional[str] = None


def _resolve_api_key() -> str:
    if os.environ.get("BIOHUB_CLI_ENV", "").lower() == "dev":
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


def init() -> None:
    """Initialize analytics. Idempotent; a no-op when no API key is configured."""
    global _client, _device_id, _cli_version
    if _client is not None:
        return
    api_key = _resolve_api_key()
    if not api_key:
        return
    try:
        _device_id = _load_or_create_device_id()
        _cli_version = _resolve_cli_version()
        _client = Amplitude(api_key)
        atexit.register(_client.shutdown)
    except Exception:
        _client = None
        _device_id = None
        _cli_version = None


def track(event_type: str, properties: Optional[dict[str, Any]] = None) -> None:
    """Emit one event. Never raises; no-op if analytics is disabled."""
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
