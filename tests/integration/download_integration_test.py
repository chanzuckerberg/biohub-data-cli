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
import sqlite3
import subprocess
from pathlib import Path
from urllib.parse import unquote, urlparse

import pytest

from biohub_data_cli.models import Collection

FIXTURES_DIR = Path(__file__).parent / "fixtures"

# Source-of-truth oracles are external binaries — no Python SDK shared with the
# CLI under test. Skip cleanly on systems missing them rather than crashing.
_REQUIRED_BINARIES = ("aws", "curl")

# Mixed-path fixtures (HTTP + S3) that download cleanly (failures == []). Small
# (1 HTTP + 1 S3) is used by the fast resume-workflow tests; medium (more
# datasets) is the broader oracle check in test_download_matches_source.
SMALL_FIXTURE = "small-mixed-paths-collection.json"
MEDIUM_FIXTURE = "medium-mixed-paths-collection.json"


# ── helpers ───────────────────────────────────────────────────────────────────


def _run_download(
    collection_id: str, outdir: Path, *extra_args: str
) -> subprocess.CompletedProcess:
    """Invoke the installed CLI exactly as a user would, against the fixtures dir.

    DATA_CLI_FIXTURES_DIR=<fixtures> ops-data download collection <id> -o <out> --yes [extra]
    """
    cmd = [
        "ops-data",
        "download",
        "collection",
        collection_id,
        "-o",
        str(outdir),
        "--yes",
        *extra_args,
    ]
    env = {**os.environ, "DATA_CLI_FIXTURES_DIR": str(FIXTURES_DIR)}
    return subprocess.run(cmd, env=env, capture_output=True, text=True)


def _assert_ok(result: subprocess.CompletedProcess, what: str) -> None:
    """Assert the CLI exited 0. Non-zero iff there were download failures (or a
    usage error) — the subprocess-equivalent of `failures == []`."""
    assert result.returncode == 0, (
        f"CLI exited {result.returncode} during {what}\n"
        f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
    )


def _load_collection(fixture_name: str) -> Collection:
    return Collection.model_validate_json((FIXTURES_DIR / fixture_name).read_text())


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


def _data_files(root: Path) -> set[Path]:
    """Downloaded data files only — excludes the .biohub-data-cli/ resume state."""
    return {
        p for p in root.rglob("*") if p.is_file() and ".biohub-data-cli" not in p.parts
    }


def _mtimes(root: Path) -> dict[Path, int]:
    """Map each downloaded data file → its last-modified time (ns).

    Lets a test detect whether a file was rewritten between runs without
    comparing bytes: a re-downloaded file's mtime advances, a skipped file's
    stays put.
    """
    return {p: p.stat().st_mtime_ns for p in _data_files(root)}


def _state_db_path(collection: Collection, outdir: Path) -> Path:
    return outdir / collection.slug / ".biohub-data-cli" / "state.db"


# ── tests ─────────────────────────────────────────────────────────────────────


@pytest.mark.integration
def test_download_matches_source(tmp_path: Path) -> None:
    missing_bins = [b for b in _REQUIRED_BINARIES if shutil.which(b) is None]
    if missing_bins:
        pytest.skip(f"missing required binaries on PATH: {missing_bins}")

    collection = _load_collection(MEDIUM_FIXTURE)

    _assert_ok(_run_download(collection.id, tmp_path), "download")

    expected = _expected_files(collection, tmp_path)

    local_files = _data_files(tmp_path)
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


@pytest.mark.integration
def test_resume_is_a_no_op_when_everything_already_downloaded(tmp_path: Path) -> None:
    """A second run within TTL reuses cached state and re-downloads nothing."""
    collection = _load_collection(SMALL_FIXTURE)

    _assert_ok(_run_download(collection.id, tmp_path), "initial download")
    before = _mtimes(tmp_path)
    assert before, "first run downloaded no files"

    resumed = _run_download(collection.id, tmp_path)
    _assert_ok(resumed, "resume run")

    assert "Resuming" in (resumed.stdout + resumed.stderr)
    assert _mtimes(tmp_path) == before, "resume re-downloaded files (mtimes changed)"


@pytest.mark.integration
def test_resume_redownloads_only_the_missing_dataset(tmp_path: Path) -> None:
    """Simulate an interrupted run by wiping one dataset's files and clearing its
    downloaded flags; resume restores exactly that dataset and leaves the
    already-complete dataset untouched."""
    collection = _load_collection(SMALL_FIXTURE)
    _assert_ok(_run_download(collection.id, tmp_path), "initial download")

    # The S3 dataset is the "interrupted" one; the other is the survivor.
    victim = next(
        ds for ds in collection.datasets if any(u.startswith("s3://") for u in ds.urls)
    )
    survivor = next(ds for ds in collection.datasets if ds.slug != victim.slug)
    coll_dir = tmp_path / collection.slug
    survivor_before = _mtimes(coll_dir / survivor.slug)
    assert survivor_before

    # Roll the victim back to "never downloaded" — the exact state an
    # interrupted run leaves behind: unset flags, files gone.
    con = sqlite3.connect(_state_db_path(collection, tmp_path))
    con.execute(
        "UPDATE collection_entries SET downloaded = 0 WHERE dataset_slug = ?",
        (victim.slug,),
    )
    con.commit()
    con.close()
    shutil.rmtree(coll_dir / victim.slug)

    resumed = _run_download(collection.id, tmp_path)
    _assert_ok(resumed, "resume run")

    assert "Resuming" in (resumed.stdout + resumed.stderr)
    assert _data_files(coll_dir / victim.slug), "interrupted dataset was not restored"
    assert _mtimes(coll_dir / survivor.slug) == survivor_before, (
        "already-complete dataset was needlessly re-downloaded"
    )


@pytest.mark.integration
def test_no_resume_forces_full_redownload(tmp_path: Path) -> None:
    """--no-resume ignores cached state, re-lists, and re-downloads every file."""
    collection = _load_collection(SMALL_FIXTURE)
    _assert_ok(_run_download(collection.id, tmp_path), "initial download")
    before = _mtimes(tmp_path)
    assert before

    redo = _run_download(collection.id, tmp_path, "--no-resume")
    _assert_ok(redo, "--no-resume run")

    assert "Resuming" not in (redo.stdout + redo.stderr)
    after = _mtimes(tmp_path)
    assert set(after) == set(before), "file set changed after --no-resume"
    assert all(after[p] > before[p] for p in before), "files were not re-downloaded"
