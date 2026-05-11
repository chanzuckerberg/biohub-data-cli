from pathlib import Path
from urllib.parse import urlparse

import boto3
from boto3.s3.transfer import S3Transfer, TransferConfig
from botocore import UNSIGNED
from botocore.config import Config as BotoConfig
from botocore.exceptions import BotoCoreError, ClientError

from utils.cli import _progress_bar_ctx, _safe_join
from models import DownloadFailure

_S3_MULTIPART_SIZE = 16 * 1024 * 1024  # 16 MB
_S3_MAX_CONCURRENCY = 8


def _make_s3_client():
    # Unsigned access — only support public buckets. Private buckets will raise ClientError.
    return boto3.client("s3", config=BotoConfig(signature_version=UNSIGNED))


def _s3_url_to_local_path(uri: str, outdir: str) -> Path:
    """Map an S3 object URI to its local path under outdir, preserving the full S3 key structure.

    s3://bucket/dir1/dir2/file.h5ad -> outdir/dir1/dir2/file.h5ad

    Known collision scenarios (unlikely in practice):
    - Since bucket name is not included in the local path, two buckets with the same key,
      e.g. s3://bucket-a/dir/f1 and s3://bucket-b/dir/f1 both map to outdir/dir/f1.
    """
    key = urlparse(uri).path.lstrip("/")
    return _safe_join(Path(outdir), *key.split("/"))


def expand_s3_location(uri: str) -> list[str]:
    """Expand a URI into a list of individual S3 object URIs.

    For an exact key, returns [uri]. For a prefix, lists all objects under it.
    Raises RuntimeError if listing fails.
    """
    s3 = _make_s3_client()
    parsed = urlparse(uri)
    bucket, key = parsed.netloc, parsed.path.lstrip("/")

    objects = []
    paginator = s3.get_paginator("list_objects_v2")
    try:
        for page in paginator.paginate(Bucket=bucket, Prefix=key):
            for obj in page.get("Contents", []):
                # Skip directory markers (objects ending with /)
                if not obj["Key"].endswith("/"):
                    objects.append(f"s3://{bucket}/{obj['Key']}")
    except (BotoCoreError, ClientError) as e:
        raise RuntimeError(f"Failed to list S3 objects at {uri}: {e}") from e
    return objects


def download_s3_object(
    uri: str, outdir: str, dataset_name: str
) -> DownloadFailure | None:
    """Download a single S3 object into outdir, preserving the full S3 key structure."""
    s3 = _make_s3_client()
    parsed = urlparse(uri)
    bucket, key = parsed.netloc, parsed.path.lstrip("/")
    outpath = _s3_url_to_local_path(uri, outdir)
    outpath.parent.mkdir(parents=True, exist_ok=True)
    try:
        total = s3.head_object(Bucket=bucket, Key=key)["ContentLength"]
        cfg = TransferConfig(
            multipart_threshold=_S3_MULTIPART_SIZE,
            multipart_chunksize=_S3_MULTIPART_SIZE,
            max_concurrency=_S3_MAX_CONCURRENCY,
        )
        with _progress_bar_ctx(total) as pbar:
            S3Transfer(s3, cfg).download_file(
                bucket, key, str(outpath), callback=lambda n: pbar.update(n)
            )
        return None
    except Exception as e:
        if outpath.exists():
            outpath.unlink()
        return DownloadFailure(dataset_name=dataset_name, url=uri, reason=str(e))
