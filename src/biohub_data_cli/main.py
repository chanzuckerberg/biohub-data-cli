import click

from biohub_data_cli.download import download_group


@click.group()
def cli():
    """Biohub data CLI.

    \b
    Analytics: this CLI sends anonymous usage events (commands run, errors,
    timings) to Amplitude to help us improve it. Only a random device ID is
    sent — no paths, URLs, or other identifying values. Set
    DISABLE_BIOHUB_DATA_CLI_ANALYTICS=true to disable.
    """


cli.add_command(download_group)

if __name__ == "__main__":
    cli()
