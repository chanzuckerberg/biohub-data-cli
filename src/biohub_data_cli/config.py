import os

try:
    from biohub_data_cli import _build_config as _bc
except ImportError:
    _bc = None


def _built_in(name: str) -> str | None:
    """A value baked into the wheel at build time (see ci/generate_build_config.py),
    or None when running from a source tree without _build_config.py."""
    return getattr(_bc, name, None) if _bc is not None else None


_ALL_DATA_API_URL = _built_in("ALL_DATA_API_URL")
_OIDC_ISSUER = _built_in("OIDC_ISSUER")
_OIDC_CLIENT_ID = _built_in("OIDC_CLIENT_ID")

_ALL_DATA_API_URL_OVERRIDE_ENV = "ALL_DATA_API_URL_OVERRIDE"
_ALL_DATA_API_TOKEN_ENV = "ALL_DATA_API_TOKEN"
_OIDC_ISSUER_ENV = "BIOHUB_DATA_CLI_OIDC_ISSUER"
_OIDC_CLIENT_ID_ENV = "BIOHUB_DATA_CLI_OIDC_CLIENT_ID"
_OIDC_SCOPES_ENV = "BIOHUB_DATA_CLI_OIDC_SCOPES"


def service_url() -> str:
    """all-data-api base URL. Set $ALL_DATA_API_URL_OVERRIDE for local dev."""
    url = os.environ.get(_ALL_DATA_API_URL_OVERRIDE_ENV) or _ALL_DATA_API_URL
    if not url:
        raise RuntimeError(
            "No service URL configured. Set $ALL_DATA_API_URL_OVERRIDE, or install "
            "a released wheel that bundles _build_config.py."
        )
    return url.rstrip("/")


def oidc_issuer() -> str | None:
    """OIDC issuer for `biohub-data login` (env override wins over the built-in)."""
    return os.environ.get(_OIDC_ISSUER_ENV) or _OIDC_ISSUER


def oidc_client_id() -> str | None:
    """OIDC client id (a public/native Okta app with the device grant) for login."""
    return os.environ.get(_OIDC_CLIENT_ID_ENV) or _OIDC_CLIENT_ID


def oidc_scopes() -> str:
    """Space-separated scopes requested at login. `offline_access` yields a refresh
    token so the CLI can renew without re-prompting."""
    return os.environ.get(_OIDC_SCOPES_ENV) or "openid offline_access"


def auth_token() -> str | None:
    """Bearer token for the Okta-gated internal all-data-api deployment.

    Priority: $ALL_DATA_API_TOKEN (explicit override, e.g. CI) -> a cached token
    from `biohub-data login` (refreshed if expired) -> None. None means anonymous
    access, which is correct for a public deployment.
    """
    explicit = os.environ.get(_ALL_DATA_API_TOKEN_ENV)
    if explicit:
        return explicit
    # Imported lazily: auth imports config, so a top-level import would cycle.
    from biohub_data_cli import auth

    return auth.load_access_token()
