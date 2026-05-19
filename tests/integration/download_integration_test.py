"""Integration tests against real S3 / HTTP sources.

These tests hit public buckets (`cryoet-data-portal-public`) and HTTP servers.
They are marked `integration` and are not run by the default `pytest` invocation.

Run only the integration suite:

    pytest -m integration tests/integration/

Run everything except the slow large-fixture variant:

    pytest -m "integration and not slow" tests/integration/

See `tests/integration/test_strategy.md` for the three-tier strategy. This file
implements the Tier 1 / "minimum viable assertion set":

    1. set(local files)        == set(expected files from expand_s3_location)
    2. all sizes on disk       == expected sizes
    3. failures                == []
"""

import os
import shutil
import subprocess
from pathlib import Path
from urllib.parse import unquote, urlparse

import pytest

from biohub_data_cli.models import Collection

FIXTURES_DIR = Path(__file__).parent / "fixtures"

# Source-of-truth oracles are external binaries — no Python SDK shared with the
# CLI under test. Skip cleanly on systems missing them rather than crashing.
_REQUIRED_BINARIES = ("aws", "curl")

# Fixtures expected to succeed cleanly (failures == []).
CLEAN_FIXTURES = [
    "medium-mixed-paths-collection.json",
]


def _aws_s3_ls(url: str) -> list[tuple[str, int]]:
    """Run `aws s3 ls --no-sign-request --recursive <url>` and parse (key, size) pairs.

    If the recursive listing is empty the URL probably points at a single object,
    so we fall back to a non-recursive `aws s3 ls` and reconstruct the key from
    the URL path.
    """
    proc = subprocess.run(
        ["aws", "s3", "ls", "--no-sign-request", "--recursive", url],
        capture_output=True,
        text=True,
        check=True,
    )
    lines = proc.stdout.strip().splitlines()
    if lines:
        # Each line: "YYYY-MM-DD HH:MM:SS    SIZE    KEY"
        out = []
        for line in lines:
            _date, _time, size, key = line.split(maxsplit=3)
            out.append((key, int(size)))
        return out

    # Single-object fallback. Non-recursive `aws s3 ls s3://bucket/key` prints
    # one line ending in the basename (no path); we already know the key from URL.
    proc = subprocess.run(
        ["aws", "s3", "ls", "--no-sign-request", url],
        capture_output=True,
        text=True,
        check=True,
    )
    line = proc.stdout.strip()
    assert line, f"no objects found at {url}"
    _date, _time, size, _basename = line.split(maxsplit=3)
    key = urlparse(url).path.lstrip("/")
    return [(key, int(size))]


def _curl_content_length(url: str) -> int:
    """Return Content-Length from `curl -sIL <url>`, following redirects.

    With redirects, curl prints headers from every hop — the final response's
    Content-Length is the last one in the output.
    """
    proc = subprocess.run(
        ["curl", "-sIL", url],
        capture_output=True,
        text=True,
        check=True,
    )
    lengths = [
        int(line.split(":", 1)[1].strip())
        for line in proc.stdout.splitlines()
        if line.lower().startswith("content-length:")
    ]
    assert lengths, f"no Content-Length header in response to {url}"
    return lengths[-1]


def _expected_files(collection: Collection, outdir: Path) -> dict[Path, int]:
    """Build the source-of-truth (path → expected bytes) map using only external CLIs.

    S3: `aws s3 ls --no-sign-request --recursive`. HTTP: `curl -sIL`.
    """
    expected: dict[Path, int] = {}
    for dataset in collection.datasets:
        ds_dir = outdir / collection.slug / dataset.slug
        for url in dataset.urls:
            if url.startswith("s3://"):
                for key, size in _aws_s3_ls(url):
                    expected[ds_dir.joinpath(*key.split("/"))] = size
            elif url.startswith(("http://", "https://")):
                filename = unquote(Path(urlparse(url).path).name)
                expected[ds_dir / filename] = _curl_content_length(url)
            else:
                pytest.fail(f"fixture contains unsupported URL scheme: {url}")
    return expected


def _walk_files(root: Path) -> set[Path]:
    return {p for p in root.rglob("*") if p.is_file()}


@pytest.mark.integration
@pytest.mark.parametrize("fixture_name", CLEAN_FIXTURES)
def test_download_matches_source(fixture_name: str, tmp_path: Path) -> None:
    missing_bins = [b for b in _REQUIRED_BINARIES if shutil.which(b) is None]
    if missing_bins:
        pytest.skip(f"missing required binaries on PATH: {missing_bins}")

    collection = Collection.model_validate_json(
        (FIXTURES_DIR / fixture_name).read_text()
    )

    # Invoke the installed CLI exactly as a user would:
    #   DATA_CLI_FIXTURES_DIR=<fixtures> ops-data download collection <id> -o <tmp> --yes
    cmd = [
        "ops-data",
        "download",
        "collection",
        collection.id,
        "-o",
        str(tmp_path),
        "--yes",
    ]
    env = {**os.environ, "DATA_CLI_FIXTURES_DIR": str(FIXTURES_DIR)}
    result = subprocess.run(cmd, env=env, capture_output=True, text=True)

    # CLI exits non-zero iff there were download failures (or a usage error).
    # This is the subprocess-equivalent of `failures == []`.
    assert result.returncode == 0, (
        f"CLI exited {result.returncode}\n"
        f"cmd: DATA_CLI_FIXTURES_DIR={FIXTURES_DIR} {' '.join(cmd)}\n"
        f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
    )

    expected = _expected_files(collection, tmp_path)

    local_files = _walk_files(tmp_path)
    missing = set(expected) - local_files
    extra = local_files - set(expected)
    assert not missing and not extra, (
        f"file set diverged from source: missing={sorted(missing)} extra={sorted(extra)}"
    )

    size_mismatches = {
        p: (expected[p], p.stat().st_size)
        for p in expected
        if p.stat().st_size != expected[p]
    }
    assert not size_mismatches, f"size mismatches (expected, actual): {size_mismatches}"
