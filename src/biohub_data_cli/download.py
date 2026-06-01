import functools
import os
import threading
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path

import click
import requests
from pydantic import ValidationError
from rich.filesize import decimal as format_bytes
from rich.markup import escape

from biohub_data_cli.config import service_url
from biohub_data_cli.models import Collection, Dataset, DownloadFailure
from biohub_data_cli.utils.cli import DownloadDisplay, console
from biohub_data_cli.utils.http import download_http
from biohub_data_cli.utils.s3 import (
    S3_MAX_WORKERS,
    download_s3_object,
    mark_phase,
    print_s3_debug_summary_if_enabled,
    resolve_s3_uris,
)
from biohub_data_cli.utils.stats import (
    aggregate_dry_run_stats,
    estimate_size_summary,
    get_collections_stats,
    print_dry_run_summary,
)

_HTTP_MAX_WORKERS = 10

_FIXTURES_DIR_ENV = "DATA_CLI_FIXTURES_DIR"

# Signals a dry run to the backend so it emits a "stats queried" rather than a
# "download initiated" metric. Must match the header alias the backend reads.
_DRY_RUN_HEADER = "X-Biohub-Data-Cli-Dry-Run"

# Analytics are emitted server-side by the backend when the CLI calls it, so
# opting out means asking the backend to skip the analytics event for this
# request. The backend must honor this header (see cellxstate routers/cli.py).
# The env-var name predates the move to backend-emitted analytics (see PR #10).
_DISABLE_ANALYTICS_HEADER = "X-Biohub-Data-Cli-Disable-Analytics"
_DISABLE_ANALYTICS_ENV = "DISABLE_BIOHUB_DATA_CLI_ANALYTICS"


def analytics_disabled() -> bool:
    """Whether the user opted out via $DISABLE_BIOHUB_DATA_CLI_ANALYTICS."""
    return os.environ.get(_DISABLE_ANALYTICS_ENV, "").strip().lower() == "true"


@functools.lru_cache(maxsize=1)
def _user_agent() -> str:
    """`biohub-data-cli/<version>` — the backend parses the version from this prefix."""
    try:
        cli_version = version("biohub-data-cli")
    except PackageNotFoundError:
        # Running from a source tree without installed metadata; the backend
        # records this as cli_version="unknown" rather than failing the request.
        cli_version = "unknown"
    return f"biohub-data-cli/{cli_version}"


def fetch_collection(
    collection_id: str, dry_run: bool = False, analytics: bool = True
) -> Collection:
    """Fetch a collection by id from the OPS backend's `/v1/cli/collections/{id}`.

    The request carries a `User-Agent: biohub-data-cli/<version>` header so the
    backend can attribute usage to a CLI version, and — on dry runs — an
    `X-Biohub-Data-Cli-Dry-Run: true` header so the backend emits a "stats
    queried" metric instead of a "download initiated" one.

    When `analytics` is False, an `X-Biohub-Data-Cli-Disable-Analytics: true`
    header asks the backend to skip the analytics event for this request.

    $DATA_CLI_FIXTURES_DIR, if set, short-circuits the HTTP call and loads
    `<collection_id>.json` from that directory. Used by integration tests to
    exercise the download stack without a live backend.
    """
    fixtures_dir = os.environ.get(_FIXTURES_DIR_ENV)
    if fixtures_dir:
        path = Path(fixtures_dir) / f"{collection_id}.json"
        if not path.exists():
            raise click.ClickException(f"No fixture for {collection_id} at {path}")
        return Collection.model_validate_json(path.read_text())

    headers = {"User-Agent": _user_agent()}
    if dry_run:
        headers[_DRY_RUN_HEADER] = "true"
    if not analytics:
        headers[_DISABLE_ANALYTICS_HEADER] = "true"

    url = f"{service_url()}/v1/cli/collections/{collection_id}"
    try:
        response = requests.get(url, timeout=30, headers=headers)
        response.raise_for_status()
        return Collection.model_validate_json(response.content)
    except requests.RequestException as e:
        status = e.response.status_code if e.response is not None else "n/a"
        raise click.ClickException(
            f"Failed to fetch collection {collection_id} from {url} (status={status}): {e}"
        ) from e
    except ValidationError as e:
        raise click.ClickException(
            f"Unexpected response shape for collection {collection_id} from {url}: {e}"
        ) from e


def submit_dataset_downloads(
    collection_slug: str,
    dataset: Dataset,
    dataset_outdir: Path,
    http_pool: ThreadPoolExecutor,
    s3_pool: ThreadPoolExecutor,
    display: DownloadDisplay,
    cancel: threading.Event,
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
    #
    # Pipe per-page progress to the display so the user sees the LIST walk
    # advancing — without this, huge prefixes (millions of zarr chunks) show
    # an empty Live region for minutes before any download bar appears.
    label = f"{collection_slug}/{dataset.slug}"

    def on_listing_progress(n_objects: int, total_bytes: int) -> None:
        display.set_listing(
            f"listing {label} · {n_objects:,} objects · {format_bytes(total_bytes)}"
        )

    s3_objects, listing_failures = resolve_s3_uris(
        collection_slug,
        dataset.slug,
        s3_uris,
        on_listing_progress=on_listing_progress,
    )
    display.set_listing(None)
    submission_failures.extend(listing_failures)

    if not s3_objects and not http_urls:
        return [], submission_failures

    # Seed total with what we already know: S3 sizes are exact and free.
    # HTTP totals get added as workers learn Content-Length (see on_size_known).
    # Don't fall back to dataset.file_size_bytes — grow_task_total adds, so
    # mixing a BE estimate with per-worker Content-Length growth double-counts.
    # HTTP-only datasets just start indeterminate until the first header lands.
    initial_total = sum(size for _, size in s3_objects)
    task_id = display.progress.add_task(
        escape(f"{collection_slug}/{dataset.slug}"),
        total=initial_total or None,
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
            cancel,
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
            cancel,
        )
        for url in http_urls
    ]
    return futures, submission_failures


def download_collections(
    collections: list[Collection], outdir: Path
) -> list[DownloadFailure]:
    """Download all datasets across all collections via shared HTTP and S3 pools."""
    all_futures: list[Future] = []
    # Shared cancellation signal. Workers check it between chunks and bail out,
    # cleaning up their .part file, so that all workers exit within one chunk
    # instead of hanging.
    cancel = threading.Event()
    kbi: KeyboardInterrupt | None = None

    with (
        DownloadDisplay() as display,
        ThreadPoolExecutor(max_workers=_HTTP_MAX_WORKERS) as http_pool,
        ThreadPoolExecutor(max_workers=S3_MAX_WORKERS) as s3_pool,
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
                    cancel,
                )
                for f in submission_failures:
                    display.record_failure(f)
                all_futures.extend(futs)

        # All listings done; per-object downloads dominate from here. Attribution
        # is fuzzy if datasets interleave (later datasets' listings would land in
        # "download" phase) but accurate for single-dataset runs.
        mark_phase("download")

        try:
            for future in as_completed(all_futures):
                result = future.result()
                if result is not None:
                    display.record_failure(result)
        except KeyboardInterrupt as e:
            cancel.set()
            # Rich Live with transient=False lets console.print insert above
            # the still-active progress bars, so the user gets immediate
            # feedback while in-flight workers drain (which can take several
            # seconds before the with-block can exit).
            console.print(
                "\n[yellow]cancelling — waiting for in-flight downloads to drain…[/yellow]"
            )
            http_pool.shutdown(wait=False, cancel_futures=True)
            s3_pool.shutdown(wait=False, cancel_futures=True)
            kbi = e

    if kbi is not None:
        console.print("\n[yellow]cancelled — partial files cleaned up[/yellow]")
        raise kbi

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

    analytics = not analytics_disabled()
    collections = [
        fetch_collection(cid, dry_run=dry_run, analytics=analytics) for cid in ids
    ]

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

    try:
        failures = download_collections(collections, outdir)
    except KeyboardInterrupt:
        # download_collections already drained workers (its `with` blocks
        # waited for shutdown) and printed the cancellation line. os._exit
        # skips interpreter finalizers — abandoned sockets in S3Transfer's
        # internal thread pool would otherwise produce noisy "Exception
        # ignored while finalizing file <HTTPResponse>" tracebacks. atexit
        # is also skipped, so force the debug summary out beforehand.
        print_s3_debug_summary_if_enabled()
        os._exit(130)

    if failures:
        raise click.ClickException(f"{len(failures)} download(s) failed.")
    console.print(f"\n[green]✅ done — {outdir}[/green]")
