import os

try:
    from biohub_data_cli._build_config import OPS_SERVICE_URL as _OPS_SERVICE_URL
except ImportError:
    _OPS_SERVICE_URL = None

_OPS_SERVICE_URL_OVERRIDE_ENV = "OPS_SERVICE_URL_OVERRIDE"


def service_url() -> str:
    """OPS backend service base URL. Set $OPS_SERVICE_URL_OVERRIDE for local dev."""
    url = os.environ.get(_OPS_SERVICE_URL_OVERRIDE_ENV) or _OPS_SERVICE_URL
    if not url:
        raise RuntimeError(
            "No service URL configured. Set $OPS_SERVICE_URL_OVERRIDE, or install "
            "a released wheel that bundles _build_config.py."
        )
    return url.rstrip("/")
