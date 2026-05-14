import urllib.parse
from collections.abc import Callable
from pathlib import Path

import requests
from biohub_data_cli.models import DownloadFailure
from biohub_data_cli.utils.cli import safe_join

_HTTP_CHUNK_SIZE = 1024 * 1024  # 1 MB


def http_url_to_local_path(url: str, outdir: Path) -> Path:
    """Map an HTTP URL to its local path under outdir using the filename from the URL path.

    https://example.com/v2/download/file.h5ad -> outdir/file.h5ad

    Unlike S3 keys, HTTP URL paths can contain arbitrary routing segments unrelated
    to the file being downloaded, so only the filename is used. The filename is
    percent-decoded so `file%20name.h5ad` lands at `outdir/file name.h5ad`.

    Known collision scenarios:
    - Two URLs with different paths but the same filename, e.g.
      https://example.com/human/file.h5ad and https://example.com/mouse/file.h5ad
      both map to outdir/file.h5ad.
    """
    filename = urllib.parse.unquote(Path(urllib.parse.urlparse(url).path).name)
    if not filename:
        raise ValueError(f"cannot determine filename from URL: {url}")
    return safe_join(outdir, filename)


def download_http(
    url: str,
    outdir: Path,
    collection_slug: str,
    dataset_slug: str,
    on_bytes_downloaded: Callable[[int], None],
    on_size_known: Callable[[int], None],
) -> DownloadFailure | None:
    """Download `url` to `outdir/<filename>`.

    `on_size_known(N)` fires once if the server's GET response includes a
    `Content-Length` header, letting the orchestrator grow the task's total
    in lockstep with what we actually learn about the file. Servers that
    omit `Content-Length` (chunked encoding) simply skip that step.
    `on_bytes_downloaded(N)` fires per chunk during streaming.
    """
    try:
        outpath = http_url_to_local_path(url, outdir)
    except ValueError as e:
        return DownloadFailure(
            collection_slug=collection_slug,
            dataset_slug=dataset_slug,
            url=url,
            reason=str(e),
        )
    outpath.parent.mkdir(parents=True, exist_ok=True)
    # Stream to a .part file and atomically rename on success so an interrupted
    # download never leaves a truncated file at outpath that looks complete.
    tmp = outpath.with_name(outpath.name + ".part")

    try:
        with requests.get(url, stream=True, timeout=60) as r:
            r.raise_for_status()
            content_length = r.headers.get("Content-Length")
            if content_length is not None:
                try:
                    on_size_known(int(content_length))
                except ValueError:
                    pass  # malformed header — fall through, just no total update
            with open(tmp, "wb") as f:
                for chunk in r.iter_content(chunk_size=_HTTP_CHUNK_SIZE):
                    if chunk:
                        f.write(chunk)
                        on_bytes_downloaded(len(chunk))
        tmp.replace(outpath)
        return None
    except (requests.RequestException, OSError) as e:
        tmp.unlink(missing_ok=True)
        return DownloadFailure(
            collection_slug=collection_slug,
            dataset_slug=dataset_slug,
            url=url,
            reason=str(e),
        )
