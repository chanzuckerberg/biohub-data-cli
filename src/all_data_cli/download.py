import functools
import os
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from pathlib import Path

import click
from rich.progress import Progress

from all_data_cli.models import Collection, Dataset, DownloadFailure
from all_data_cli.utils.cli import (
    DownloadDisplay,
    advance_task,
    console,
    grow_task_total,
)
from all_data_cli.utils.http import download_http
from all_data_cli.utils.s3 import download_s3_object, expand_s3_location

_HTTP_MAX_WORKERS = 10
_S3_MAX_WORKERS = 10

_FIXTURES_DIR_ENV = "ALL_DATA_CLI_FIXTURES_DIR"


def fetch_collection(collection_id: str) -> Collection:
    """Fetch a collection by ID.

    Backend endpoint is pending; until it lands, set $ALL_DATA_CLI_FIXTURES_DIR
    to a directory of `<collection_id>.json` files validated against `Collection`.
    Remove this branch once the real endpoint is wired up.
    """
    fixtures_dir = os.environ.get(_FIXTURES_DIR_ENV)
    if fixtures_dir:
        path = Path(fixtures_dir) / f"{collection_id}.json"
        if not path.exists():
            raise click.ClickException(f"No fixture for {collection_id} at {path}")
        return Collection.model_validate_json(path.read_text())
    raise NotImplementedError(
        f"fetch_collection is stubbed; backend endpoint pending (id={collection_id}). "
        f"Set ${_FIXTURES_DIR_ENV} to test with local fixtures."
    )


def submit_dataset_downloads(
    collection_slug: str,
    dataset: Dataset,
    dataset_outdir: Path,
    http_ex: ThreadPoolExecutor,
    s3_ex: ThreadPoolExecutor,
    progress: Progress,
) -> tuple[list[Future], list[DownloadFailure]]:
    """Submit one dataset's downloads to the shared pools.

    Returns (submitted_futures, submission_failures). `submission_failures`
    are failures known synchronously during submission — unsupported URL
    schemes and S3-prefix-listing errors that prevent ever creating a future.
    Anything that fails inside a worker shows up via the returned futures.

    One progress task per dataset is added when there's at least one URL to
    submit; all workers for the dataset share the same on_bytes_downloaded
    callback bound to that task.
    A Zarr that expands to N chunk objects shows one aggregate bar rather than
    N tiny ones.
    """
    submission_failures: list[DownloadFailure] = []
    dataset_outdir.mkdir(parents=True, exist_ok=True)

    http_urls, s3_uris, unknown_urls = [], [], []
    for url in dataset.urls:
        if url.startswith("s3://"):
            s3_uris.append(url)
        elif url.startswith(("http://", "https://")):
            http_urls.append(url)
        else:
            unknown_urls.append(url)

    for url in unknown_urls:
        submission_failures.append(
            DownloadFailure(
                collection_slug=collection_slug,
                dataset_slug=dataset.slug,
                url=url,
                reason="Unsupported URL scheme",
            )
        )

    # Expand S3 prefixes into (uri, size) pairs so we can both submit each
    # object as its own future and seed the progress task with the actual
    # byte total directly from list_objects_v2 / head_object.
    s3_objects: list[tuple[str, int]] = []
    for uri in s3_uris:
        try:
            s3_objects.extend(expand_s3_location(uri))
        except RuntimeError as e:
            submission_failures.append(
                DownloadFailure(
                    collection_slug=collection_slug,
                    dataset_slug=dataset.slug,
                    url=uri,
                    reason=str(e),
                )
            )

    if not s3_objects and not http_urls:
        return [], submission_failures

    # Seed total with what we already know: S3 sizes are exact and free.
    # HTTP totals get added as workers learn Content-Length (see on_size_known).
    initial_total = sum(size for _, size in s3_objects)
    task_id = progress.add_task(
        f"{collection_slug}/{dataset.slug}",
        total=initial_total or None,
    )
    on_bytes_downloaded = functools.partial(advance_task, progress, task_id)
    on_size_known = functools.partial(grow_task_total, progress, task_id)

    futures: list[Future] = [
        # S3 sizes are already accumulated into the task total at `expand_s3_location`
        # time, so no need to call `on_size_known`.
        s3_ex.submit(
            download_s3_object,
            obj_uri,
            dataset_outdir,
            collection_slug,
            dataset.slug,
            on_bytes_downloaded,
        )
        for obj_uri, _ in s3_objects
    ] + [
        http_ex.submit(
            download_http,
            url,
            dataset_outdir,
            collection_slug,
            dataset.slug,
            on_bytes_downloaded,
            on_size_known,
        )
        for url in http_urls
    ]
    return futures, submission_failures


def download_collections(
    collections: list[Collection], outdir: Path
) -> list[DownloadFailure]:
    """Download all datasets across all collections via shared HTTP and S3 pools."""
    all_futures: list[Future] = []

    with (
        DownloadDisplay() as display,
        ThreadPoolExecutor(max_workers=_HTTP_MAX_WORKERS) as http_ex,
        ThreadPoolExecutor(max_workers=_S3_MAX_WORKERS) as s3_ex,
    ):
        for collection in collections:
            for dataset in collection.datasets:
                ds_outdir = outdir / collection.slug / dataset.slug
                futs, submission_failures = submit_dataset_downloads(
                    collection.slug,
                    dataset,
                    ds_outdir,
                    http_ex,
                    s3_ex,
                    display.progress,
                )
                for f in submission_failures:
                    display.record_failure(f)
                all_futures.extend(futs)

        try:
            for future in as_completed(all_futures):
                result = future.result()
                if result is not None:
                    display.record_failure(result)
        except KeyboardInterrupt:
            http_ex.shutdown(wait=False, cancel_futures=True)
            s3_ex.shutdown(wait=False, cancel_futures=True)
            raise

    return display.failures


# ── CLI ───────────────────────────────────────────────────────────────


@click.group("download")
def download_group() -> None:
    """Download data from the all-data platform."""


@download_group.command("collection")
@click.argument("ids", nargs=-1, required=True)
@click.option(
    "-o",
    "--outdir",
    default=".",
    type=click.Path(file_okay=False, dir_okay=True, writable=True, path_type=Path),
    help="Output directory for downloaded files.",
)
@click.option("--yes", "-y", is_flag=True, help="Skip confirmation prompt.")
def download_collection_command(ids: tuple[str, ...], outdir: Path, yes: bool) -> None:
    """Download one or more collections by ID."""
    collections = [fetch_collection(cid) for cid in ids]

    n_datasets = sum(len(c.datasets) for c in collections)
    if n_datasets == 0:
        raise click.ClickException("No datasets to download.")

    # TODO(AIP-284): show statistics on the size of data to be downloaded.
    if not yes:
        click.confirm(
            f"Download {len(collections)} collection(s), {n_datasets} dataset(s)?",
            default=False,
            abort=True,
        )

    failures = download_collections(collections, outdir)

    if failures:
        raise click.ClickException(f"{len(failures)} download(s) failed.")
    console.print(f"\n[green]✅ done — {outdir}[/green]")
