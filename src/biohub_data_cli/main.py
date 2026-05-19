import click

from biohub_data_cli import analytics
from biohub_data_cli.download import download_group


@click.group()
def cli():
    """Biohub data CLI."""
    analytics.init()


cli.add_command(download_group)

if __name__ == "__main__":
    cli()
