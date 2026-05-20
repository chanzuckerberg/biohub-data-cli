import functools
import os
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from pathlib import Path

import click
from rich.markup import escape

from biohub_data_cli import analytics
from biohub_data_cli.models import Collection, Dataset, DownloadFailure
from biohub_data_cli.utils.cli import DownloadDisplay, console
from biohub_data_cli.utils.http import download_http
from biohub_data_cli.utils.s3 import download_s3_object, resolve_s3_uris
from biohub_data_cli.utils.stats import (
    aggregate_dry_run_stats,
    estimate_size_summary,
    get_collections_stats,
    print_dry_run_summary,
)

_HTTP_MAX_WORKERS = 10
_S3_MAX_WORKERS = 10

_FIXTURES_DIR_ENV = "DATA_CLI_FIXTURES_DIR"


def fetch_collection(collection_id: str) -> Collection:
    """Fetch a collection by ID.

    Backend endpoint is pending; until it lands, set $DATA_CLI_FIXTURES_DIR
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
    http_pool: ThreadPoolExecutor,
    s3_pool: ThreadPoolExecutor,
    display: DownloadDisplay,
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
    s3_objects, listing_failures = resolve_s3_uris(
        collection_slug, dataset.slug, s3_uris
    )
    submission_failures.extend(listing_failures)

    if not s3_objects and not http_urls:
        return [], submission_failures

    # Seed total with what we already know: S3 sizes are exact and free.
    # HTTP totals get added as workers learn Content-Length (see on_size_known).
    # If no initial total, fall back to the curator-provided file_size_bytes.
    initial_total = sum(size for _, size in s3_objects) or dataset.file_size_bytes
    task_id = display.add_dataset_download_task(
        escape(f"{collection_slug}/{dataset.slug}"),
        collection_slug,
        initial_total or None,
    )
    on_bytes_downloaded = functools.partial(display.advance_task, task_id)
    on_size_known = functools.partial(display.grow_task_total, task_id)

    futures: list[Future] = [
        # S3 sizes are already accumulated into the task total at `expand_s3_location`
        # time, so no need to call `on_size_known`.
        s3_pool.submit(
            download_s3_object,
            obj_uri,
            dataset_outdir,
            collection_slug,
            dataset.slug,
            on_bytes_downloaded,
        )
        for obj_uri, _ in s3_objects
    ] + [
        http_pool.submit(
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
    analytics.track_collection_downloads_initiated(collections)

    all_futures: list[Future] = []

    with (
        DownloadDisplay() as display,
        ThreadPoolExecutor(max_workers=_HTTP_MAX_WORKERS) as http_pool,
        ThreadPoolExecutor(max_workers=_S3_MAX_WORKERS) as s3_pool,
    ):
        for collection in collections:
            for dataset in collection.datasets:
                ds_outdir = outdir / collection.slug / dataset.slug
                futs, submission_failures = submit_dataset_downloads(
                    collection.slug,
                    dataset,
                    ds_outdir,
                    http_pool,
                    s3_pool,
                    display,
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
            http_pool.shutdown(wait=False, cancel_futures=True)
            s3_pool.shutdown(wait=False, cancel_futures=True)
            raise

    analytics.track_collection_download_outcomes(
        collections, display.failures, display.get_bytes_by_collection()
    )

    return display.failures


# ── CLI ───────────────────────────────────────────────────────────────


@click.group("download")
def download_group() -> None:
    """Download data from Biohub."""


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
@click.option(
    "--dry-run",
    is_flag=True,
    help="Print per-dataset statistics without downloading.",
)
def download_collection_command(
    ids: tuple[str, ...], outdir: Path, yes: bool, dry_run: bool
) -> None:
    """Download one or more collections by ID."""
    if dry_run and yes:
        raise click.UsageError("--dry-run and --yes are mutually exclusive.")

    collections = [fetch_collection(cid) for cid in ids]

    n_datasets = sum(len(c.datasets) for c in collections)
    if n_datasets == 0:
        raise click.ClickException("No datasets to download.")

    if dry_run:
        stats_by_collection = get_collections_stats(collections)
        aggregate = aggregate_dry_run_stats(stats_by_collection)
        print_dry_run_summary(stats_by_collection, aggregate)
        if aggregate.n_failed_uris:
            raise click.ClickException("Dry run completed with size lookup failures.")
        return

    if not yes:
        estimate = estimate_size_summary(collections)
        click.confirm(
            f"Download {len(collections)} collection(s), {n_datasets} dataset(s) "
            f"({estimate})?",
            default=False,
            abort=True,
        )

    failures = download_collections(collections, outdir)

    if failures:
        raise click.ClickException(f"{len(failures)} download(s) failed.")
    console.print(f"\n[green]✅ done — {outdir}[/green]")
