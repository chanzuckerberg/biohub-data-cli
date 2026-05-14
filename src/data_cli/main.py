import click

from data_cli.download import download_group


@click.group()
def cli():
    """biohub data CLI."""


cli.add_command(download_group)

if __name__ == "__main__":
    cli()
