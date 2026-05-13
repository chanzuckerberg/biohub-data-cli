import click

from all_data_cli.download import download_group


@click.group()
def cli():
    """All data platform CLI."""


cli.add_command(download_group)

if __name__ == "__main__":
    cli()
