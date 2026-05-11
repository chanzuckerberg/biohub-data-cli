import urllib.parse
from pathlib import Path

import requests
from models import DownloadFailure
from utils.cli import _progress_bar_ctx, _safe_join

_HTTP_CHUNK_SIZE = 8 * 1024  # 8 KB


def _http_url_to_local_path(url: str, outdir: str) -> Path:
    """Map an HTTP URL to its local path under outdir using the filename from the URL path.

    https://example.com/v2/download/file.h5ad -> outdir/file.h5ad

    Unlike S3 keys, HTTP URL paths can contain arbitrary routing segments unrelated
    to the file being downloaded, so only the filename is used.

    Known collision scenarios:
    - Two URLs with different paths but the same filename, e.g.
      https://example.com/human/file.h5ad and https://example.com/mouse/file.h5ad
      both map to outdir/file.h5ad.
    """
    filename = Path(urllib.parse.urlparse(url).path).name
    if not filename:
        raise ValueError(f"cannot determine filename from URL: {url}")
    return _safe_join(Path(outdir), filename)


def download_http(url: str, outdir: str, dataset_name: str) -> DownloadFailure | None:
    outpath = _http_url_to_local_path(url, outdir)
    outpath.parent.mkdir(parents=True, exist_ok=True)

    try:
        with requests.get(url, stream=True, timeout=60) as r:
            r.raise_for_status()
            total = int(r.headers.get("Content-Length", 0))
            with open(outpath, "wb") as f:
                with _progress_bar_ctx(total) as pbar:
                    for chunk in r.iter_content(chunk_size=_HTTP_CHUNK_SIZE):
                        if chunk:
                            f.write(chunk)
                            pbar.update(len(chunk))
        return None
    except Exception as e:
        if outpath.exists():
            outpath.unlink()
        return DownloadFailure(dataset_name=dataset_name, url=url, reason=str(e))
