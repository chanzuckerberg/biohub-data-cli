import click

from biohub_data_cli.download import download_group


@click.group()
def cli() -> None:
    """Biohub data CLI.

    \b
    Anonymous usage analytics are collected to improve the tool; no personal
    data is collected. Set DISABLE_BIOHUB_DATA_CLI_ANALYTICS=true to disable.
    """


cli.add_command(download_group)


# The tool was originally OPS-specific and shipped as `ops-data`; it now downloads
# any collection, so the primary command is `biohub-data`. `ops-data` is kept as a
# deprecated alias (same behavior, no scope restriction) so existing scripts keep
# working — it just warns and forwards.
_OPS_DATA_DEPRECATION = "warning: `ops-data` is deprecated; use `biohub-data` instead (same arguments)."


def ops_data() -> None:
    """Deprecated `ops-data` entry point: warn, then run the `biohub-data` CLI."""
    click.secho(_OPS_DATA_DEPRECATION, fg="yellow", err=True)
    cli()


if __name__ == "__main__":
    cli()
