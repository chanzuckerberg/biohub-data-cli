import functools
import os
import threading
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from pathlib import Path

import click
import requests
from pydantic import ValidationError
from rich.filesize import decimal as format_bytes
from rich.markup import escape

from biohub_data_cli.config import service_url
from biohub_data_cli.models import Collection, Dataset, DownloadFailure
from biohub_data_cli.utils.cli import DownloadDisplay, console
from biohub_data_cli.utils.download_state import CollectionEntry, DownloadStateDB
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


def fetch_collection(collection_id: str) -> Collection:
    """Fetch a collection by id from the OPS backend's `/v1/cli/collections/{id}`.

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

    url = f"{service_url()}/v1/cli/collections/{collection_id}"
    try:
        response = requests.get(url, timeout=30)
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

    # We trust the `downloaded` flag in the DB — if a row says downloaded=1,
    # we assume the file is on disk at the right size and skip it.
    # Alternatively, we can check file size. Not implemented because it involves O(N) stat() syscalls.
    entries = list(db.iter_entries_for_dataset(dataset.slug))
    if not entries:
        return {}

    done_bytes = sum(e.expected_size or 0 for e in entries if e.downloaded)
    total_bytes = sum(e.expected_size or 0 for e in entries)

    if done_bytes == total_bytes and total_bytes > 0:
        # Everything is on disk already — show a completed bar without
        # submitting any work.
        display.progress.add_task(
            escape(label), total=total_bytes, completed=done_bytes
        )
        return {}

    task_id = display.progress.add_task(
        escape(label),
        total=total_bytes or None,
        completed=done_bytes,
    )
    on_bytes_downloaded = functools.partial(display.advance_task, task_id)
    on_size_known = functools.partial(display.grow_task_total, task_id)

    future_to_url: dict[Future, str] = {}
    for entry in entries:
        if entry.downloaded:
            continue
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
        elif entry.url.startswith(("http://", "https://")):
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
        else:
            # Shouldn't reach here — `_list_and_record` filters unknown-scheme
            # URLs at listing time and emits them as DownloadFailures. Skip
            # defensively rather than crash if the DB somehow has one.
            continue
        future_to_url[fut] = entry.url

    return future_to_url


def ensure_collection_listed(
    collection: Collection,
    db: DownloadStateDB,
    display: DownloadDisplay,
) -> None:
    """Populate `db` with a fresh listing for every dataset in `collection`.

    Unconditionally nukes any existing DB and re-lists — the caller is
    responsible for deciding whether listing is needed (cf. `is_listing_fresh`
    and the `--resume` flag in `download_collections`). After this call,
    `db.is_listing_fresh()` returns True until TTL.

    Listing failures are recorded against `display` rather than raised, so
    one bad prefix doesn't prevent other datasets in the same collection
    from being listed and downloaded.
    """
    db.init_fresh()
    for dataset in collection.datasets:
        listing_failures = _list_and_record(collection.slug, dataset, db, display)
        for f in listing_failures:
            display.record_failure(f)
    db.mark_listing_complete()


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
    collections: list[Collection], outdir: Path, resume: bool = True
) -> list[DownloadFailure]:
    """Download all datasets across all collections via shared HTTP and S3 pools.

    `resume=True` (the default): if a non-expired state DB exists for a
    collection, skip listing and pick up pending downloads from it. `resume=False`
    nukes any existing DB for each collection and lists from scratch.

    Per-collection state lives at `outdir/{collection.slug}/.biohub-data-cli/state.db`
    and is intentionally kept after success — within TTL, re-running the same
    command becomes a fast integrity-check no-op rather than a full re-list and
    re-download. The DB rotates out when its `listing_completed_at` exceeds TTL
    or the user passes `--no-resume`.
    """
    # Shared cancellation signal. Workers check it between chunks and bail out,
    # cleaning up their .part file, so that all workers exit within one chunk
    # instead of hanging.
    cancel = threading.Event()
    kbi: KeyboardInterrupt | None = None

    # Pre-decide which collections can reuse their cached listing. Doing this
    # before the Live region takes over lets us show one summary line up top.
    collection_dbs: dict[str, DownloadStateDB] = {}
    cached_collections: set[str] = set()
    for collection in collections:
        db = DownloadStateDB.for_collection(outdir, collection.slug)
        if resume and db.is_listing_fresh():
            cached_collections.add(collection.slug)
        collection_dbs[collection.slug] = db

    if cached_collections:
        console.print(
            f"[dim]Resuming {len(cached_collections)} of {len(collections)} "
            f"collection(s) from cached state.[/dim]"
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
        # Cached collections are skipped — their DB already has authoritative
        # entries within TTL.
        for collection in collections:
            if collection.slug in cached_collections:
                continue
            ensure_collection_listed(
                collection, collection_dbs[collection.slug], display
            )

        # Phase 2: submit pending downloads. By this point every DB is
        # authoritative, so this loop just asks the DB what's still missing.
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
                if result is None:
                    collection_dbs[coll_slug].mark_downloaded(ds_slug, url)
                else:
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
        console.print(
            "\n[yellow]cancelled — rerun the same command to resume from where "
            "you left off.[/yellow]"
        )
        raise kbi

    # Intentionally leave state DBs in place even on full success. Re-running
    # the same command within TTL becomes a fast no-op via the per-file
    # integrity check (file exists + size matches), instead of re-listing and
    # re-downloading from scratch. `--no-resume` is the explicit opt-out.
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
@click.option(
    "--resume/--no-resume",
    default=True,
    help="Skip files already downloaded in a prior run if cached state is "
    "available and within TTL (5 days). --no-resume forces a fresh listing "
    "and re-download.",
)
def download_collection_command(
    ids: tuple[str, ...], outdir: Path, yes: bool, dry_run: bool, resume: bool
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

    try:
        failures = download_collections(collections, outdir, resume=resume)
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
