import functools
from collections.abc import Callable
from pathlib import Path
from urllib.parse import urlparse

import boto3
from boto3.s3.transfer import S3Transfer, TransferConfig
from botocore import UNSIGNED
from botocore.config import Config as BotoConfig
from botocore.exceptions import BotoCoreError, ClientError

from data_cli.models import DownloadFailure
from data_cli.utils.cli import safe_join

_S3_MULTIPART_SIZE = 16 * 1024 * 1024  # 16 MB
_S3_MAX_CONCURRENCY = 8


@functools.cache
def _make_s3_client():
    # Cached so all workers share one client (boto3 clients are thread-safe).
    # Constructing a fresh client per call adds non-trivial overhead when
    # downloading many small objects (e.g. Zarr chunks).
    # Unsigned access — only support public buckets. Private buckets will raise ClientError.
    return boto3.client("s3", config=BotoConfig(signature_version=UNSIGNED))


def s3_url_to_local_path(uri: str, outdir: Path) -> Path:
    """Map an S3 object URI to its local path under outdir, preserving the full S3 key structure.

    s3://bucket/dir1/dir2/file.h5ad -> outdir/dir1/dir2/file.h5ad

    Known collision scenarios (unlikely in practice):
    - Since bucket name is not included in the local path, two buckets with the same key,
      e.g. s3://bucket-a/dir/f1 and s3://bucket-b/dir/f1 both map to outdir/dir/f1.
    """
    key = urlparse(uri).path.lstrip("/")
    return safe_join(outdir, *key.split("/"))


def expand_s3_location(uri: str) -> list[tuple[str, int]]:
    """Expand a URI into a list of (object URI, size in bytes) tuples.

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
    try:
        for page in paginator.paginate(Bucket=bucket, Prefix=list_prefix):
            for obj in page.get("Contents", []):
                if not obj["Key"].endswith("/"):  # skip directory markers
                    raw_keys_and_size.append(
                        (f"s3://{bucket}/{obj['Key']}", obj["Size"])
                    )
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


def download_s3_object(
    uri: str,
    outdir: Path,
    collection_slug: str,
    dataset_slug: str,
    on_bytes_downloaded: Callable[[int], None],
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
    try:
        cfg = TransferConfig(
            multipart_threshold=_S3_MULTIPART_SIZE,
            multipart_chunksize=_S3_MULTIPART_SIZE,
            max_concurrency=_S3_MAX_CONCURRENCY,
        )
        S3Transfer(s3, cfg).download_file(
            bucket, key, str(tmp), callback=on_bytes_downloaded
        )
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
