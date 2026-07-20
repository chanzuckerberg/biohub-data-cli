import click

from biohub_data_cli import auth
from biohub_data_cli.download import download_group
from biohub_data_cli.utils.cli import console


@click.group()
def cli() -> None:
    """Biohub data CLI.

    \b
    Anonymous usage analytics are collected to improve the tool; no personal
    data is collected. Set DISABLE_BIOHUB_DATA_CLI_ANALYTICS=true to disable.
    """


cli.add_command(download_group)


@cli.command("login")
def login_command() -> None:
    """Sign in to the internal all-data API via your browser (Okta).

    Opens a device-login flow: you approve in a browser, and the token is cached
    so `biohub-data download` works without further prompts. Not needed for a
    public deployment.
    """
    auth.login()


@cli.command("logout")
def logout_command() -> None:
    """Remove the cached all-data API credentials."""
    auth.logout()
    console.print("Logged out.")


# The tool was originally OPS-specific and shipped as `ops-data`; it now downloads
# any collection, so the primary command is `biohub-data`. `ops-data` is kept as a
# deprecated alias (same behavior, no scope restriction) so existing scripts keep
# working — it just warns and forwards.
_OPS_DATA_DEPRECATION = (
    "warning: `ops-data` is deprecated; use `biohub-data` instead (same arguments)."
)


def ops_data() -> None:
    """Deprecated `ops-data` entry point: warn, then run the `biohub-data` CLI."""
    click.secho(_OPS_DATA_DEPRECATION, fg="yellow", err=True)
    cli()


if __name__ == "__main__":
    cli()
