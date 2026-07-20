"""Browser-based login for the Okta-gated internal all-data API.

Uses the OAuth 2.0 **Device Authorization Grant** (RFC 8628): the CLI shows a URL
and a code, the user approves in any browser, and the CLI polls for an access
token. The device flow (rather than a loopback / auth-code flow) is deliberate —
it works on headless machines (Slurm nodes, remote shells) where the CLI can't
open or receive a redirect on a local browser.

Tokens are cached at ``~/.config/biohub-data-cli/credentials.json`` (0600) and
auto-refreshed with the refresh token when expired. ``config.auth_token()`` reads
them, so ``biohub-data download`` picks up the login with no extra flags. A public
deployment needs no login at all — ``load_access_token()`` returns ``None`` and the
CLI calls the API anonymously.

The OIDC issuer + client id are build/config values (see ``config.oidc_issuer`` /
``config.oidc_client_id``); endpoints are resolved from the issuer's OIDC discovery
document so this works with either an Okta org or custom authorization server.
"""

import json
import os
import time
from pathlib import Path

import click
import requests

from biohub_data_cli import config
from biohub_data_cli.utils.cli import console

_DEVICE_GRANT = "urn:ietf:params:oauth:grant-type:device_code"
# Treat a token as expired this many seconds early, so a request started right at
# the boundary doesn't race the expiry.
_EXPIRY_SKEW = 60
_HTTP_TIMEOUT = 15


def _config_dir() -> Path:
    """Directory holding cached credentials. $BIOHUB_DATA_CLI_CONFIG_DIR overrides
    it (used by tests); otherwise $XDG_CONFIG_HOME or ~/.config."""
    override = os.environ.get("BIOHUB_DATA_CLI_CONFIG_DIR")
    if override:
        return Path(override)
    base = os.environ.get("XDG_CONFIG_HOME") or os.path.join(
        os.path.expanduser("~"), ".config"
    )
    return Path(base) / "biohub-data-cli"


def _credentials_path() -> Path:
    return _config_dir() / "credentials.json"


def _load() -> dict | None:
    path = _credentials_path()
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text())
    except (json.JSONDecodeError, OSError):
        return None


def _save(creds: dict) -> None:
    directory = _config_dir()
    directory.mkdir(parents=True, exist_ok=True)
    path = _credentials_path()
    path.write_text(json.dumps(creds, indent=2))
    path.chmod(0o600)


def _discover(issuer: str) -> dict:
    """OIDC discovery: resolve the device-authorization and token endpoints from
    the issuer, so org vs custom authorization servers both work."""
    resp = requests.get(
        f"{issuer}/.well-known/openid-configuration", timeout=_HTTP_TIMEOUT
    )
    resp.raise_for_status()
    return resp.json()


def _store_token(issuer: str, client_id: str, tok: dict) -> None:
    _save(
        {
            "issuer": issuer,
            "client_id": client_id,
            "access_token": tok["access_token"],
            "refresh_token": tok.get("refresh_token"),
            "token_type": tok.get("token_type", "Bearer"),
            "expires_at": time.time() + tok.get("expires_in", 3600) - _EXPIRY_SKEW,
        }
    )


def login() -> None:
    """Run the device-authorization login and cache the resulting token."""
    issuer = config.oidc_issuer()
    client_id = config.oidc_client_id()
    if not issuer or not client_id:
        raise click.ClickException(
            "Login is not configured for this build (no OIDC issuer / client id). "
            "For a public all-data-api deployment no login is needed; for an internal "
            "one, set $BIOHUB_DATA_CLI_OIDC_ISSUER and $BIOHUB_DATA_CLI_OIDC_CLIENT_ID."
        )

    meta = _discover(issuer)
    resp = requests.post(
        meta["device_authorization_endpoint"],
        data={"client_id": client_id, "scope": config.oidc_scopes()},
        timeout=_HTTP_TIMEOUT,
    )
    resp.raise_for_status()
    device = resp.json()

    verification = device.get("verification_uri_complete") or device["verification_uri"]
    console.print("\n[bold]To sign in, open this URL in a browser:[/bold]")
    console.print(f"  [cyan]{verification}[/cyan]")
    if not device.get("verification_uri_complete"):
        console.print(f"and enter the code: [bold]{device['user_code']}[/bold]")
    console.print("\nWaiting for you to approve…")

    interval = device.get("interval", 5)
    deadline = time.time() + device.get("expires_in", 300)
    token_endpoint = meta["token_endpoint"]
    while time.time() < deadline:
        time.sleep(interval)
        poll = requests.post(
            token_endpoint,
            data={
                "grant_type": _DEVICE_GRANT,
                "device_code": device["device_code"],
                "client_id": client_id,
            },
            timeout=_HTTP_TIMEOUT,
        )
        if poll.status_code == 200:
            _store_token(issuer, client_id, poll.json())
            console.print("[green]✅ logged in.[/green]")
            return
        error = poll.json().get("error")
        if error == "authorization_pending":
            continue
        if error == "slow_down":
            interval += 5
            continue
        if error == "access_denied":
            raise click.ClickException("Login was denied.")
        if error == "expired_token":
            break
        raise click.ClickException(f"Login failed: {error or poll.text}")
    raise click.ClickException("Login timed out; run `biohub-data login` again.")


def logout() -> None:
    """Remove cached credentials. No-op if not logged in."""
    path = _credentials_path()
    if path.exists():
        path.unlink()


def _refresh(creds: dict) -> str | None:
    """Exchange the refresh token for a fresh access token, re-storing both.
    Returns the new access token, or None if refresh isn't possible/fails."""
    refresh_token = creds.get("refresh_token")
    issuer = creds.get("issuer")
    client_id = creds.get("client_id")
    if not (refresh_token and issuer and client_id):
        return None
    try:
        meta = _discover(issuer)
        resp = requests.post(
            meta["token_endpoint"],
            data={
                "grant_type": "refresh_token",
                "refresh_token": refresh_token,
                "client_id": client_id,
                "scope": config.oidc_scopes(),
            },
            timeout=_HTTP_TIMEOUT,
        )
    except requests.RequestException:
        return None
    if resp.status_code != 200:
        return None
    tok = resp.json()
    # A rotated refresh token replaces the old one; if the server didn't return a
    # new one, keep the existing token so the next refresh still works.
    tok.setdefault("refresh_token", refresh_token)
    _store_token(issuer, client_id, tok)
    return tok["access_token"]


def load_access_token() -> str | None:
    """Return a valid cached access token, refreshing if expired. None if the user
    isn't logged in (or the token is expired and can't be refreshed) — callers then
    fall back to anonymous access."""
    creds = _load()
    if not creds:
        return None
    if time.time() < creds.get("expires_at", 0):
        return creds["access_token"]
    return _refresh(creds)
