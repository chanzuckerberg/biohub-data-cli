"""Collection/dataset download orchestration.

Resume model & assumptions
--------------------------
A per-collection SQLite DB caches the listing (every concrete s3/http URL plus
its expected size) and a per-file `downloaded` flag. Resume re-submits only the
entries not yet marked `downloaded`. That design has the following assumptions:

- Granularity is per file. A file is either done or not; there is no mid-file
  resume. Downloads stream to a `.part` file and atomically rename on success,
  so an interrupted file is re-fetched from scratch rather than left truncated.
  This works well for collections of many small files (e.g. Zarr chunks) and
  less well for a single very large file.

- Collections/datasets are assumed immutable between runs. The only protection
  against drift is the listing TTL (`LISTING_TTL`): once the cached listing
  expires (or the user passes `--no-resume`) we re-list from scratch; within the
  TTL, changes on the server — added, removed, or modified objects — are ignored
  silently.

- The DB, not the filesystem, is the source of truth for what exists. If a user
  deletes an already-downloaded file by hand, resume trusts the `downloaded`
  flag and skips it silently rather than re-fetching it.

- HTTP entries are stored with `expected_size=None` (Content-Length isn't known
  until GET time), so completion is keyed off the `downloaded` flag, never a
  byte-count comparison.
"""

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

from biohub_data_cli.config import auth_token, service_url
from biohub_data_cli.models import Collection, Dataset, DownloadFailure
from biohub_data_cli.utils.cli import DownloadDisplay, console
from biohub_data_cli.utils.download_state import (
    LISTING_TTL,
    CollectionEntry,
    DownloadStateDB,
)
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
    """Fetch a collection by id from all-data-api's `/v1/collections/{id}/manifest`.

    The manifest returns the collection with each dataset's downloadable S3 `urls`
    already resolved (any S3 asset, since the CLI downloads with the user's own AWS
    creds); non-S3 / external data is reported in the manifest's `skipped` list,
    which the `Collection` model ignores.

    The request carries a `User-Agent: biohub-data-cli/<version>` header, and — when
    `$ALL_DATA_API_TOKEN` is set — an `Authorization: Bearer <token>` header for the
    Okta-gated internal deployment (unset for a public one). The dry-run and
    analytics headers are retained for forward compatibility; server-side analytics
    are not wired on all-data-api yet.

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
    if (token := auth_token()) is not None:
        headers["Authorization"] = f"Bearer {token}"
    if dry_run:
        headers[_DRY_RUN_HEADER] = "true"
    if not analytics:
        headers[_DISABLE_ANALYTICS_HEADER] = "true"

    url = f"{service_url()}/v1/collections/{collection_id}/manifest"
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
    db: DownloadStateDB,
    http_pool: ThreadPoolExecutor,
    s3_pool: ThreadPoolExecutor,
    display: DownloadDisplay,
    cancel: threading.Event,
) -> dict[Future, str]:
    """Submit one dataset's pending downloads to the shared pools.

    Reads the dataset's entries from the DB (caller is responsible for the DB
    already being populated — see `ensure_collection_listed`) and submits a
    future for any entry not yet marked `downloaded=1`. Returns a
    `future_to_url` map so the orchestrator can call `mark_downloaded` after
    each successful future.

    One progress task per dataset; it's seeded with the bytes already on disk
    so a bar visibly picks up where it left off across resume runs.

    This function is intentionally unaware of "is this a fresh run or a
    resume?" — that decision is contained in the listing phase. Here, the DB
    is the single source of truth for what should exist on disk.
    """
    dataset_outdir.mkdir(parents=True, exist_ok=True)
    label = f"{collection_slug}/{dataset.slug}"

    # Query from DB instead of statting file size on disk.
    stats = db.dataset_progress(dataset.slug)
    if stats.total_count == 0:
        return {}

    if stats.pending_count == 0:
        display.progress.add_task(
            escape(label), total=stats.total_bytes or None, completed=stats.done_bytes
        )
        return {}

    task_id = display.progress.add_task(
        escape(label),
        total=stats.total_bytes or None,
        completed=stats.done_bytes,
    )
    on_bytes_downloaded = functools.partial(display.advance_task, task_id)
    on_size_known = functools.partial(display.grow_task_total, task_id)

    future_to_url: dict[Future, str] = {}
    for entry in list(db.iter_entries_for_dataset(dataset.slug, pending_only=True)):
        if entry.url.startswith("s3://"):
            fut = s3_pool.submit(
                download_s3_object,
                entry.url,
                dataset_outdir,
                collection_slug,
                dataset.slug,
                on_bytes_downloaded,
                cancel,
            )
        else:
            fut = http_pool.submit(
                download_http,
                entry.url,
                dataset_outdir,
                collection_slug,
                dataset.slug,
                on_bytes_downloaded,
                on_size_known,
                cancel,
            )
        future_to_url[fut] = entry.url

    return future_to_url


def ensure_collection_listed(
    collection: Collection,
    db: DownloadStateDB,
    display: DownloadDisplay,
) -> None:
    """Ensure `db` holds a fresh listing for every dataset in `collection`.

    Lists only the datasets whose cached listing is absent or expired; datasets
    still within TTL keep their entries and `downloaded` marks so resume works.
    A stale dataset's old rows are dropped before re-listing.
    """
    unexpired_dataset_slugs = db.get_unexpired_dataset_slugs()
    for dataset in collection.datasets:
        if dataset.slug in unexpired_dataset_slugs:
            continue
        db.delete_dataset_entries(dataset.slug)
        listing_failures = _list_and_record(collection.slug, dataset, db, display)
        for f in listing_failures:
            display.record_failure(f)
        if not listing_failures:
            db.mark_dataset_listed(dataset.slug)


def _list_and_record(
    collection_slug: str,
    dataset: Dataset,
    db: DownloadStateDB,
    display: DownloadDisplay,
) -> list[DownloadFailure]:
    """List one dataset's URLs into the DB. Splits `dataset.urls` by scheme,
    expands S3 prefixes, inserts every concrete (url, size) row, and returns
    any listing-side failures (unsupported URL schemes, S3-listing errors).

    HTTP URLs are inserted with `expected_size=None` — Content-Length isn't
    known until the GET response, and HEAD-ing every HTTP URL during listing
    isn't worth the round trip.
    """
    listing_failures: list[DownloadFailure] = []
    http_urls, s3_uris, unknown_urls = [], [], []
    for url in dataset.urls:
        if url.startswith("s3://"):
            s3_uris.append(url)
        elif url.startswith(("http://", "https://")):
            http_urls.append(url)
        else:
            unknown_urls.append(url)

    for url in unknown_urls:
        listing_failures.append(
            DownloadFailure(
                collection_slug=collection_slug,
                dataset_slug=dataset.slug,
                url=url,
                reason="Unsupported URL scheme",
            )
        )

    label = f"{collection_slug}/{dataset.slug}"

    def on_listing_progress(n_objects: int, total_bytes: int) -> None:
        display.set_listing(
            f"listing {label} · {n_objects:,} objects · {format_bytes(total_bytes)}"
        )

    s3_objects, s3_failures = resolve_s3_uris(
        collection_slug,
        dataset.slug,
        s3_uris,
        on_listing_progress=on_listing_progress,
    )
    display.set_listing(None)
    listing_failures.extend(s3_failures)

    entries: list[CollectionEntry] = [
        CollectionEntry(
            dataset_slug=dataset.slug,
            url=obj_uri,
            expected_size=size,
            downloaded=False,
        )
        for obj_uri, size in s3_objects
    ] + [
        CollectionEntry(
            dataset_slug=dataset.slug,
            url=url,
            expected_size=None,
            downloaded=False,
        )
        for url in http_urls
    ]
    db.insert_entries(entries)
    return listing_failures


def download_collections(
    collections: list[Collection],
    outdir: Path,
    resume: bool = True,
    dataset_filtered: bool = False,
) -> list[DownloadFailure]:
    """Download all datasets across all collections via shared HTTP and S3 pools.

    `resume=True` (the default): each dataset's cached listing is reused if it's
    within TTL, so pending downloads pick up where they left off. `resume=False`
    lists from scratch — wiping the whole per-collection DB, or, when
    `dataset_filtered=True` (a `--dataset` subset is in scope), only the in-scope
    datasets' rows so the rest of the collection's resume state is left intact.

    Per-collection state lives at `outdir/{collection.slug}/.biohub-data-cli/state.db`
    and is intentionally kept after success — within TTL, re-running the same
    command re-submits only entries not yet marked `downloaded`, so a completed
    run becomes a fast no-op rather than a full re-list and re-download. Each
    dataset's listing rotates out when its `listed_at` exceeds TTL or the user
    passes `--no-resume`.
    """
    # Shared cancellation signal. Workers check it between chunks and bail out,
    # cleaning up their .part file, so that all workers exit within one chunk
    # instead of hanging.
    cancel = threading.Event()
    kbi: KeyboardInterrupt | None = None

    collection_dbs: dict[str, DownloadStateDB] = {}
    reused_datasets = 0
    for collection in collections:
        db = DownloadStateDB.for_collection(outdir, collection.slug)
        if resume:
            # Reuse the DB, rebuilding it if it's corrupt or a different schema
            # version.
            db.ensure_ready()
            unexpired_dataset_slugs = db.get_unexpired_dataset_slugs()
            reused_datasets += sum(
                1 for d in collection.datasets if d.slug in unexpired_dataset_slugs
            )
        elif dataset_filtered:
            # --no-resume scoped to a --dataset subset: force a fresh listing for
            # only the in-scope datasets, leaving the rest of the collection's
            # state intact.
            db.ensure_ready()
            for d in collection.datasets:
                db.delete_dataset_entries(d.slug)
        else:
            db.init_fresh()
        collection_dbs[collection.slug] = db

    if reused_datasets:
        console.print(
            f"[dim]Resuming {reused_datasets} dataset(s) from cached state.[/dim]"
        )

    all_futures: list[Future] = []
    # future → (collection_slug, dataset_slug, url) — used to mark the entry as
    # downloaded in the right DB once the future completes successfully.
    future_keys: dict[Future, tuple[str, str, str]] = {}

    with (
        DownloadDisplay() as display,
        ThreadPoolExecutor(max_workers=_HTTP_MAX_WORKERS) as http_pool,
        ThreadPoolExecutor(max_workers=S3_MAX_WORKERS) as s3_pool,
    ):
        # Phase 1: ensure every collection has a fresh listing in its DB.
        for collection in collections:
            ensure_collection_listed(
                collection, collection_dbs[collection.slug], display
            )

        # Phase 2: submit pending downloads.
        for collection in collections:
            collection_db = collection_dbs[collection.slug]
            for dataset in collection.datasets:
                ds_outdir = outdir / collection.slug / dataset.slug
                fut_to_url = submit_dataset_downloads(
                    collection.slug,
                    dataset,
                    ds_outdir,
                    collection_db,
                    http_pool,
                    s3_pool,
                    display,
                    cancel,
                )
                for fut, url in fut_to_url.items():
                    future_keys[fut] = (collection.slug, dataset.slug, url)
                    all_futures.append(fut)

        # All listings done; per-object downloads dominate from here. Attribution
        # is fuzzy if datasets interleave (later datasets' listings would land in
        # "download" phase) but accurate for single-dataset runs.
        mark_phase("download")

        try:
            for future in as_completed(all_futures):
                result = future.result()
                coll_slug, ds_slug, url = future_keys[future]
                if result.ok:
                    collection_dbs[coll_slug].mark_downloaded(
                        ds_slug, url, size=result.size
                    )
                else:
                    display.record_failure(result.failure)
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
        console.print(
            "\n[yellow]cancelled — rerun the same command to resume from where "
            "you left off.[/yellow]"
        )
        raise kbi

    return display.failures


# ── CLI ───────────────────────────────────────────────────────────────


@click.group("download")
def download_group() -> None:
    """Download data from Biohub."""


def _filter_datasets(collection: Collection, dataset_slugs: str) -> None:
    """Narrow `collection.datasets` in place to the requested slugs.

    `dataset_slugs` is the raw comma-separated `--dataset` value. Unknown slugs
    are an error listing the available slugs, so a typo surfaces the valid set.
    """
    requested = [s.strip() for s in dataset_slugs.split(",") if s.strip()]
    if not requested:
        raise click.UsageError("--dataset given but no slugs provided.")

    available = {d.slug: d for d in collection.datasets}
    unknown = [s for s in requested if s not in available]
    if unknown:
        raise click.ClickException(
            f"Unknown dataset slug(s) in {collection.slug}: {', '.join(unknown)}. "
            f"Available: {', '.join(sorted(available))}."
        )

    # Preserve the user's requested order, de-duplicating repeats.
    seen: set[str] = set()
    collection.datasets = [
        available[s] for s in requested if not (s in seen or seen.add(s))
    ]


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
    "--dataset",
    "dataset_slugs",
    default=None,
    help="Comma-separated dataset slugs to download a subset of the collection "
    "(e.g. --dataset matrix-a,matrix-b). Only valid with a single collection. "
    "Run --dry-run to see available slugs.",
)
@click.option(
    "--dry-run",
    is_flag=True,
    help="Print per-dataset statistics without downloading.",
)
@click.option(
    "--resume/--no-resume",
    default=True,
    help="Skip files already downloaded in a prior run if cached state is "
    f"available and within TTL ({LISTING_TTL.days} days). --no-resume forces a "
    "fresh listing and re-download.",
)
def download_collection_command(
    ids: tuple[str, ...],
    outdir: Path,
    yes: bool,
    dataset_slugs: str | None,
    dry_run: bool,
    resume: bool,
) -> None:
    """Download one or more collections by ID."""
    if dry_run and yes:
        raise click.UsageError("--dry-run and --yes are mutually exclusive.")

    analytics = not analytics_disabled()
    collections = [
        fetch_collection(cid, dry_run=dry_run, analytics=analytics) for cid in ids
    ]

    if dataset_slugs is not None:
        if len(collections) > 1:
            raise click.UsageError(
                "--dataset can only be used with a single collection."
            )
        _filter_datasets(collections[0], dataset_slugs)

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
        failures = download_collections(
            collections,
            outdir,
            resume=resume,
            dataset_filtered=dataset_slugs is not None,
        )
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
