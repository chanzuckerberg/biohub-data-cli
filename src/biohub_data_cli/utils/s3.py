import atexit
import functools
import os
import sys
import threading
import time
from collections.abc import Callable
from pathlib import Path
from urllib.parse import urlparse

import boto3
from boto3.s3.transfer import S3Transfer, TransferConfig
from botocore import UNSIGNED
from botocore.client import BaseClient
from botocore.config import Config as BotoConfig
from botocore.exceptions import BotoCoreError, ClientError

from biohub_data_cli.models import DownloadFailure
from biohub_data_cli.utils.cli import DownloadCancelled, safe_join

_S3_MULTIPART_SIZE = 16 * 1024 * 1024  # 16 MB
_S3_MAX_CONCURRENCY = 8
S3_MAX_WORKERS = 10

# Opt-in S3 traffic counters. Set DATA_CLI_DEBUG_S3=1 to install botocore event
# hooks that track logical operations, wire attempts (which include retries),
# and SlowDown responses. A summary is printed to stderr at process exit. Used
# to verify adaptive retry is comfortably riding through anonymous-quota
# throttle rather than barely scraping past max_attempts.
_S3_DEBUG = os.environ.get("DATA_CLI_DEBUG_S3", "").lower() in ("1", "true", "yes")


class _PhaseCounters:
    __slots__ = ("ops", "attempts", "slowdowns")

    def __init__(self) -> None:
        self.ops = 0  # logical API calls (one per ListObjectsV2/HeadObject/GetObject)
        self.attempts = 0  # actual HTTP sends; > ops when retries fire
        self.slowdowns = 0  # 503 SlowDown responses observed


class _S3Telemetry:
    """Phase-bucketed counters. Phases (listing → download) are switched
    from the CLI via mark_phase() so the summary can separate retries that
    happened during the upfront LIST burst from retries during per-object
    downloads — these have different fixes.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._phases: dict[str, _PhaseCounters] = {
            "listing": _PhaseCounters(),
            "download": _PhaseCounters(),
        }
        self._current = "listing"
        self._start = time.monotonic()
        self._phase_started_at: dict[str, float] = {"listing": self._start}
        self._phase_ended_at: dict[str, float] = {}
        self._emit_thread: threading.Thread | None = None
        self._stop_emit = threading.Event()

    def start_periodic_emit(self, interval_seconds: float) -> None:
        """Spawn a daemon thread that prints a snapshot every N seconds.

        Set interval ≤ 0 to disable. Snapshots go to stderr; redirect with
        `2> /tmp/log` to avoid visually colliding with the Rich progress bar
        on stdout.
        """
        if self._emit_thread is not None or interval_seconds <= 0:
            return

        def emit_loop() -> None:
            while not self._stop_emit.wait(interval_seconds):
                print(self._snapshot_line(), file=sys.stderr, flush=True)

        t = threading.Thread(target=emit_loop, daemon=True, name="s3-debug-emit")
        t.start()
        self._emit_thread = t

    def _snapshot_line(self) -> str:
        now = time.monotonic()
        parts = [f"[S3 debug t+{now - self._start:.0f}s]"]
        with self._lock:
            for name, p in self._phases.items():
                retries = p.attempts - p.ops
                parts.append(f"{name}(ops={p.ops} ret={retries} slow={p.slowdowns})")
        return " ".join(parts)

    def enter_phase(self, name: str) -> None:
        now = time.monotonic()
        with self._lock:
            self._phase_ended_at[self._current] = now
            self._current = name
            if name not in self._phases:
                self._phases[name] = _PhaseCounters()
            self._phase_started_at.setdefault(name, now)

    def on_param_build(self, **_kwargs: object) -> None:
        with self._lock:
            self._phases[self._current].ops += 1

    def on_before_send(self, **_kwargs: object) -> None:
        with self._lock:
            self._phases[self._current].attempts += 1

    def on_needs_retry(self, response: object = None, **_kwargs: object) -> None:
        if not response:
            return
        http_response = response[0] if isinstance(response, tuple) else None
        if (
            http_response is not None
            and getattr(http_response, "status_code", None) == 503
        ):
            with self._lock:
                self._phases[self._current].slowdowns += 1

    def summary(self) -> str:
        now = time.monotonic()
        lines = ["[S3 debug]"]
        for name, p in self._phases.items():
            start = self._phase_started_at.get(name, self._start)
            end = self._phase_ended_at.get(name, now)
            elapsed = max(0.0, end - start)
            retries = p.attempts - p.ops
            lines.append(
                f"  {name:<8} ops={p.ops} attempts={p.attempts} "
                f"retries={retries} slowdowns={p.slowdowns} elapsed={elapsed:.1f}s"
            )
        lines.append(f"  TOTAL    elapsed={(now - self._start):.1f}s")
        return "\n".join(lines)


_telemetry: _S3Telemetry | None = None


def _install_s3_debug_telemetry(client: object) -> None:
    """Register botocore event hooks on `client` to count S3 traffic.

    No-op unless DATA_CLI_DEBUG_S3 is set. Safe to call multiple times — the
    telemetry instance is process-global and atexit registration only runs once.
    """
    global _telemetry
    if not _S3_DEBUG:
        return
    if _telemetry is None:
        _telemetry = _S3Telemetry()
        atexit.register(lambda: print(_telemetry.summary(), file=sys.stderr))  # type: ignore[union-attr]
        # Default 30s snapshot cadence; set DATA_CLI_DEBUG_S3_INTERVAL=0 to
        # disable periodic emission (end-of-run summary still prints).
        interval = float(os.environ.get("DATA_CLI_DEBUG_S3_INTERVAL", "30"))
        _telemetry.start_periodic_emit(interval)
    events = client.meta.events  # type: ignore[attr-defined]
    events.register("before-parameter-build.s3", _telemetry.on_param_build)
    events.register("before-send.s3", _telemetry.on_before_send)
    events.register("needs-retry.s3", _telemetry.on_needs_retry)


def mark_phase(name: str) -> None:
    """Switch the active telemetry phase. No-op when DATA_CLI_DEBUG_S3 is off.

    Called from the CLI at the listing→download boundary so the summary
    reports retries/SlowDowns separately per phase. Attribution is fuzzy
    in multi-dataset runs (datasets list and download interleaved) — good
    enough for single-dataset debugging.
    """
    if _telemetry is not None:
        _telemetry.enter_phase(name)


def print_s3_debug_summary_if_enabled() -> None:
    """Force-print the telemetry summary now. Useful before os._exit on
    Ctrl+C, where atexit handlers don't run. Safe no-op when debug is off.
    """
    if _telemetry is not None:
        print(_telemetry.summary(), file=sys.stderr)


@functools.cache
def _make_s3_client() -> BaseClient:
    # Cached so all workers share one client (boto3 clients are thread-safe).
    # Constructing a fresh client per call adds non-trivial overhead when
    # downloading many small objects (e.g. Zarr chunks).
    # Unsigned access — only support public buckets. Private buckets will raise ClientError.
    #
    # max_pool_connections sized for the worst case: every dataset-level worker
    # running a multipart download at full concurrency simultaneously. Default
    # is 10. Below this ceiling, urllib3 logs "Connection pool is full,
    # discarding connection" warnings and boto3's internal retry path can
    # swallow progress callbacks (bar stalls below 100% even though the file
    # lands on disk correctly).
    #
    # Adaptive retries: anonymous access shares a bucket-wide quota with all
    # other unsigned consumers, so we hit SlowDown well below our own request
    # ceiling. Adaptive mode does AIMD client-side rate limiting on top of
    # standard retry/backoff — when any worker sees a 503, the shared token
    # bucket throttles all future requests, then ramps back up on success.
    # max_attempts=20 gives enough headroom to ride through sustained
    # contention on a hot bucket; the trade-off is slower-but-completing
    # downloads vs failing fast.
    client = boto3.client(
        "s3",
        config=BotoConfig(
            signature_version=UNSIGNED,
            max_pool_connections=S3_MAX_WORKERS * _S3_MAX_CONCURRENCY,
            retries={"mode": "adaptive", "max_attempts": 20},
        ),
    )
    _install_s3_debug_telemetry(client)
    return client


def s3_url_to_local_path(uri: str, outdir: Path) -> Path:
    """Map an S3 object URI to its local path under outdir, preserving the full S3 key structure.

    s3://bucket/dir1/dir2/file.h5ad -> outdir/dir1/dir2/file.h5ad

    Known collision scenarios (unlikely in practice):
    - Since bucket name is not included in the local path, two buckets with the same key,
      e.g. s3://bucket-a/dir/f1 and s3://bucket-b/dir/f1 both map to outdir/dir/f1.
    """
    key = urlparse(uri).path.lstrip("/")
    return safe_join(outdir, *key.split("/"))


def expand_s3_location(
    uri: str,
    on_listing_progress: Callable[[int, int], None] | None = None,
) -> list[tuple[str, int]]:
    """Expand a URI into a list of (object URI, size in bytes) tuples.

    Visible for testing — only `resolve_s3_uris` in this module should call
    this directly; everywhere else goes through that wrapper.

    Resolution rule — folder wins:
    1. List under `<key>/`. If anything is there, return all of those objects;
       the caller's key is treated as a folder regardless of whether a bare
       object with the same name also exists.
    2. Otherwise, HEAD `<key>` and return [(uri, size)] if it exists.
    3. Otherwise raise RuntimeError.

    Corner cases handling:
    1. If the uri represents an object dir/f1, while dir/f1/sub exists,
       only the object dir/f1/sub will be returned.
    2. If the uri represents an object dir/f1, while dir/f1.bak exists,
       only the object dir/f1 will be returned.
    3. If the uri represents a folder dir/, dir/dir2 exists as on object
       and dir/dir2/f1 exists as another object (i.e. pathological S3 layout),
       both will be returned and the cli will throw when writing to the filesystem.

    `on_listing_progress(n_objects, total_bytes)` fires after every paginated
    LIST page with running cumulative totals — used by the CLI to render a
    live counter during slow walks of huge prefixes (e.g. aconcagua zarr).

    Raises RuntimeError on S3 access errors or when the URI resolves to nothing.
    """
    s3 = _make_s3_client()
    parsed = urlparse(uri)
    bucket, key = parsed.netloc, parsed.path.lstrip("/")

    # List under <key>/ — normalizes trailing slashes and excludes string-prefix
    # false positives like `<key>.bak` at the S3 API level.
    list_prefix = key.rstrip("/") + "/" if key else ""
    raw_keys_and_size: list[tuple[str, int]] = []
    paginator = s3.get_paginator("list_objects_v2")
    n_objects = 0
    total_bytes = 0
    try:
        for page in paginator.paginate(Bucket=bucket, Prefix=list_prefix):
            for obj in page.get("Contents", []):
                if not obj["Key"].endswith("/"):  # skip directory markers
                    raw_keys_and_size.append(
                        (f"s3://{bucket}/{obj['Key']}", obj["Size"])
                    )
                    n_objects += 1
                    total_bytes += obj["Size"]
            if on_listing_progress is not None:
                on_listing_progress(n_objects, total_bytes)
    except (BotoCoreError, ClientError) as e:
        raise RuntimeError(f"Failed to list S3 objects at {uri}: {e}") from e

    if raw_keys_and_size:
        return raw_keys_and_size

    # Nothing under the prefix. If the caller asked for a folder explicitly
    # (trailing slash) or the whole bucket (empty key), there's no fallback.
    if not key or key.endswith("/"):
        raise RuntimeError(f"No objects found at {uri}")

    try:
        head = s3.head_object(Bucket=bucket, Key=key)
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") in ("404", "NoSuchKey"):
            raise RuntimeError(f"No object found at {uri}") from None
        raise RuntimeError(f"Failed to resolve S3 object at {uri}: {e}") from e
    except BotoCoreError as e:
        raise RuntimeError(f"Failed to resolve S3 object at {uri}: {e}") from e
    return [(f"s3://{bucket}/{key}", head["ContentLength"])]


def resolve_s3_uris(
    collection_slug: str,
    dataset_slug: str,
    s3_uris: list[str],
    on_listing_progress: Callable[[int, int], None] | None = None,
) -> tuple[list[tuple[str, int]], list[DownloadFailure]]:
    """Expand s3 uris of a dataset to (object_uri, size). Listing failures get
    attributed to the originating URI and returned alongside the resolved
    objects so callers can continue with the rest.

    `on_listing_progress` is forwarded as-is to each `expand_s3_location`
    call. For datasets with multiple S3 URIs, the counter resets between URIs
    rather than accumulating — OPS datasets have a single S3 path per dataset,
    so this issue won't be surfaced to users in practice. TODO(AIP-297): revisit this.
    """
    s3_objects: list[tuple[str, int]] = []
    failures: list[DownloadFailure] = []
    for uri in s3_uris:
        try:
            s3_objects.extend(
                expand_s3_location(uri, on_listing_progress=on_listing_progress)
            )
        except RuntimeError as e:
            failures.append(
                DownloadFailure(
                    collection_slug=collection_slug,
                    dataset_slug=dataset_slug,
                    url=uri,
                    reason=str(e),
                )
            )
    return s3_objects, failures


def download_s3_object(
    uri: str,
    outdir: Path,
    collection_slug: str,
    dataset_slug: str,
    on_bytes_downloaded: Callable[[int], None],
    cancel: threading.Event,
) -> DownloadFailure | None:
    """Download a single S3 object into outdir, preserving the full S3 key structure.

    No `on_size_known` callback here — S3 sizes are already accumulated into
    the task total at `expand_s3_location` time (from `list_objects_v2` /
    `head_object`), so the worker has nothing to report.
    """
    s3 = _make_s3_client()
    parsed = urlparse(uri)
    bucket, key = parsed.netloc, parsed.path.lstrip("/")
    outpath = s3_url_to_local_path(uri, outdir)
    outpath.parent.mkdir(parents=True, exist_ok=True)
    # Stream to a .part file and atomically rename on success so an interrupted
    # download never leaves a truncated file at outpath that looks complete.
    tmp = outpath.with_name(outpath.name + ".part")

    def callback(n: int) -> None:
        if cancel.is_set():
            raise DownloadCancelled("cancelled")
        on_bytes_downloaded(n)

    try:
        cfg = TransferConfig(
            multipart_threshold=_S3_MULTIPART_SIZE,
            multipart_chunksize=_S3_MULTIPART_SIZE,
            max_concurrency=_S3_MAX_CONCURRENCY,
        )
        S3Transfer(s3, cfg).download_file(bucket, key, str(tmp), callback=callback)
        tmp.replace(outpath)
        return None
    except (BotoCoreError, ClientError, OSError) as e:
        tmp.unlink(missing_ok=True)
        return DownloadFailure(
            collection_slug=collection_slug,
            dataset_slug=dataset_slug,
            url=uri,
            reason=str(e),
        )
