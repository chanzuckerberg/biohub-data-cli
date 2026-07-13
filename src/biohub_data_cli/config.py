import os

try:
    from biohub_data_cli._build_config import ALL_DATA_API_URL as _ALL_DATA_API_URL
except ImportError:
    _ALL_DATA_API_URL = None

_ALL_DATA_API_URL_OVERRIDE_ENV = "ALL_DATA_API_URL_OVERRIDE"
_ALL_DATA_API_TOKEN_ENV = "ALL_DATA_API_TOKEN"


def service_url() -> str:
    """all-data-api base URL. Set $ALL_DATA_API_URL_OVERRIDE for local dev."""
    url = os.environ.get(_ALL_DATA_API_URL_OVERRIDE_ENV) or _ALL_DATA_API_URL
    if not url:
        raise RuntimeError(
            "No service URL configured. Set $ALL_DATA_API_URL_OVERRIDE, or install "
            "a released wheel that bundles _build_config.py."
        )
    return url.rstrip("/")


def auth_token() -> str | None:
    """Optional bearer token for the Okta-gated internal all-data-api deployment.

    Set $ALL_DATA_API_TOKEN for internal use; unset for a public deployment.
    """
    return os.environ.get(_ALL_DATA_API_TOKEN_ENV) or None
