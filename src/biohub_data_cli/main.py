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

if __name__ == "__main__":
    cli()
