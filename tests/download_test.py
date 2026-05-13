from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import patch

import click
import pytest
from click.testing import CliRunner

from all_data_cli.download import (
    download_collections,
    fetch_collection,
    submit_dataset_downloads,
)
from all_data_cli.utils.cli import make_progress
from all_data_cli.main import cli
from all_data_cli.models import Collection, Dataset, DownloadFailure

MOCK_COLLECTION = Collection.model_validate(
    {
        "id": "coll-1",
        "slug": "test-collection",
        "title": "Test Collection",
        "datasets": [
            {
                "id": "ds-1",
                "slug": "matrix-a",
                "title": "Matrix A",
                "file_format": "parquet",
                "file_size_bytes": 1024,
                "urls": ["https://example.com/a.parquet"],
            },
            {
                "id": "ds-2",
                "slug": "matrix-b",
                "title": "Matrix B",
                "file_format": "zarr_v3",
                "file_size_bytes": 512,
                "urls": ["s3://bucket/matrix-b/"],
            },
        ],
    }
)


# ── fetch_collection ────────────────────────────────────────────────────────


def test_fetch_collection_raises_when_no_fixtures_dir(monkeypatch):
    monkeypatch.delenv("ALL_DATA_CLI_FIXTURES_DIR", raising=False)
    with pytest.raises(NotImplementedError, match="ALL_DATA_CLI_FIXTURES_DIR"):
        fetch_collection("coll-1")


def test_fetch_collection_loads_from_fixtures_dir(tmp_path, monkeypatch):
    (tmp_path / "coll-1.json").write_text(MOCK_COLLECTION.model_dump_json())
    monkeypatch.setenv("ALL_DATA_CLI_FIXTURES_DIR", str(tmp_path))

    result = fetch_collection("coll-1")

    assert result.slug == MOCK_COLLECTION.slug
    assert [d.slug for d in result.datasets] == [
        d.slug for d in MOCK_COLLECTION.datasets
    ]


def test_fetch_collection_missing_fixture_raises_click_exception(tmp_path, monkeypatch):
    monkeypatch.setenv("ALL_DATA_CLI_FIXTURES_DIR", str(tmp_path))
    with pytest.raises(click.ClickException, match="No fixture for missing-id"):
        fetch_collection("missing-id")


# ── CLI command ──────────────────────────────────────────────────────────────


def test_download_collection_fetches_and_downloads(tmp_path):
    with (
        patch("all_data_cli.download.fetch_collection") as mock_fetch,
        patch("all_data_cli.download.download_collections") as mock_dl,
    ):
        mock_fetch.return_value = MOCK_COLLECTION
        mock_dl.return_value = []

        result = CliRunner().invoke(
            cli, ["download", "collection", "coll-1", "-o", str(tmp_path), "--yes"]
        )

        assert result.exit_code == 0, result.output
        mock_fetch.assert_called_once_with("coll-1")
        passed_collections, _ = mock_dl.call_args[0]
        assert [c.slug for c in passed_collections] == ["test-collection"]


def test_download_collection_accepts_multiple_ids(tmp_path):
    with (
        patch("all_data_cli.download.fetch_collection") as mock_fetch,
        patch("all_data_cli.download.download_collections") as mock_dl,
    ):
        mock_fetch.return_value = MOCK_COLLECTION
        mock_dl.return_value = []

        result = CliRunner().invoke(
            cli,
            ["download", "collection", "a", "b", "-o", str(tmp_path), "--yes"],
        )

        assert result.exit_code == 0, result.output
        assert mock_fetch.call_count == 2


def test_download_collection_prints_failure_summary_and_exits_nonzero(tmp_path):
    failure = DownloadFailure(
        collection_slug="test-collection",
        dataset_slug="matrix-a",
        url="https://example.com/a.parquet",
        reason="Connection timeout",
    )
    with (
        patch("all_data_cli.download.fetch_collection") as mock_fetch,
        patch("all_data_cli.download.download_collections") as mock_dl,
    ):
        mock_fetch.return_value = MOCK_COLLECTION
        mock_dl.return_value = [failure]

        result = CliRunner().invoke(
            cli, ["download", "collection", "coll-1", "-o", str(tmp_path), "--yes"]
        )

        assert result.exit_code != 0
        assert "1 download(s) failed" in result.output


def test_no_datasets_raises_error(tmp_path):
    empty = MOCK_COLLECTION.model_copy(update={"datasets": []})
    with patch("all_data_cli.download.fetch_collection") as mock_fetch:
        mock_fetch.return_value = empty

        result = CliRunner().invoke(
            cli, ["download", "collection", "coll-1", "-o", str(tmp_path)]
        )

        assert result.exit_code != 0
        assert "no datasets" in result.output.lower()


# ── submit_dataset_downloads ────────────────────────────────────────────────


def test_submit_dataset_downloads_routes_and_collects_submission_failures(tmp_path):
    """Routing: http URLs go to HTTP downloader, s3 URIs are expanded then submitted."""
    dataset = Dataset.model_validate(
        {
            "id": "ds-1",
            "slug": "matrix-a",
            "title": "Matrix A",
            "file_format": "parquet",
            "urls": ["https://example.com/a.csv", "s3://bucket/b.h5ad"],
        }
    )

    with (
        patch("all_data_cli.download.download_http") as mock_http,
        patch("all_data_cli.download.download_s3_object") as mock_s3,
        patch(
            "all_data_cli.download.expand_s3_location",
            return_value=["s3://bucket/b.h5ad"],
        ),
    ):
        mock_http.return_value = None
        mock_s3.return_value = None

        with (
            ThreadPoolExecutor(max_workers=2) as http_ex,
            ThreadPoolExecutor(max_workers=2) as s3_ex,
        ):
            futures, submission_failures = submit_dataset_downloads(
                "coll", dataset, Path(tmp_path), http_ex, s3_ex, make_progress()
            )
            for f in futures:
                f.result()

    assert len(futures) == 2
    assert submission_failures == []
    mock_http.assert_called_once()
    mock_s3.assert_called_once()


def test_submit_dataset_downloads_unknown_scheme(tmp_path):
    dataset = Dataset.model_validate(
        {
            "id": "ds-1",
            "slug": "matrix-a",
            "title": "Matrix A",
            "file_format": "parquet",
            "urls": ["ftp://example.com/file.h5ad"],
        }
    )

    with (
        ThreadPoolExecutor(max_workers=1) as http_ex,
        ThreadPoolExecutor(max_workers=1) as s3_ex,
    ):
        futures, submission_failures = submit_dataset_downloads(
            "coll", dataset, Path(tmp_path), http_ex, s3_ex, make_progress()
        )

    assert futures == []
    assert len(submission_failures) == 1
    assert "Unsupported URL scheme" in submission_failures[0].reason


def test_submit_dataset_downloads_submits_every_expanded_s3_object(tmp_path):
    """A single s3:// prefix that expands to N objects → N S3 download submissions."""
    dataset = Dataset.model_validate(
        {
            "id": "ds-1",
            "slug": "matrix-zarr",
            "title": "Zarr Matrix",
            "file_format": "zarr_v3",
            "urls": ["s3://bucket/zarr-store/"],
        }
    )
    expanded = [
        "s3://bucket/zarr-store/.zarray",
        "s3://bucket/zarr-store/.zattrs",
        "s3://bucket/zarr-store/0/0/0",
        "s3://bucket/zarr-store/0/0/1",
        "s3://bucket/zarr-store/0/0/2",
    ]

    with (
        patch("all_data_cli.download.download_s3_object", return_value=None) as mock_s3,
        patch("all_data_cli.download.expand_s3_location", return_value=expanded),
    ):
        with (
            ThreadPoolExecutor(max_workers=2) as http_ex,
            ThreadPoolExecutor(max_workers=2) as s3_ex,
        ):
            futures, submission_failures = submit_dataset_downloads(
                "coll-x", dataset, Path(tmp_path), http_ex, s3_ex, make_progress()
            )
            for f in futures:
                f.result()

    assert submission_failures == []
    assert len(futures) == len(expanded)
    submitted_uris = {call.args[0] for call in mock_s3.call_args_list}
    assert submitted_uris == set(expanded)


def test_submit_dataset_downloads_records_failure_when_s3_listing_fails(tmp_path):
    """If expand_s3_location raises, that URI becomes an immediate failure with full attribution."""
    dataset = Dataset.model_validate(
        {
            "id": "ds-1",
            "slug": "matrix-zarr",
            "title": "Zarr Matrix",
            "file_format": "zarr_v3",
            "urls": ["s3://bucket/bad-prefix/"],
        }
    )

    with patch(
        "all_data_cli.download.expand_s3_location",
        side_effect=RuntimeError("listing failed: access denied"),
    ):
        with (
            ThreadPoolExecutor(max_workers=1) as http_ex,
            ThreadPoolExecutor(max_workers=1) as s3_ex,
        ):
            futures, submission_failures = submit_dataset_downloads(
                "coll-x", dataset, Path(tmp_path), http_ex, s3_ex, make_progress()
            )

    assert futures == []
    assert len(submission_failures) == 1
    failure = submission_failures[0]
    assert failure.collection_slug == "coll-x"
    assert failure.dataset_slug == "matrix-zarr"
    assert failure.url == "s3://bucket/bad-prefix/"
    assert "listing failed" in failure.reason


# ── download_collections ────────────────────────────────────────────────────


def test_download_collections_writes_to_collection_dataset_subdirs(tmp_path):
    """Verifies the outdir/<collection.slug>/<dataset.slug>/ layout."""
    with (
        patch("all_data_cli.download.download_http", return_value=None) as mock_http,
        patch("all_data_cli.download.expand_s3_location", return_value=[]),
    ):
        download_collections([MOCK_COLLECTION], Path(tmp_path))

    # http URL was submitted with the per-dataset outdir
    called_outdir = mock_http.call_args.args[1]
    assert called_outdir == tmp_path / "test-collection" / "matrix-a"


def test_download_collections_submits_every_dataset_across_collections(tmp_path):
    """Every dataset under every collection should be submitted exactly once."""
    coll_a = Collection.model_validate(
        {
            "id": "a",
            "slug": "coll-a",
            "title": "A",
            "datasets": [
                {
                    "id": "d1",
                    "slug": "ds1",
                    "title": "D1",
                    "file_format": "parquet",
                    "urls": ["https://example.com/a1.parquet"],
                },
                {
                    "id": "d2",
                    "slug": "ds2",
                    "title": "D2",
                    "file_format": "parquet",
                    "urls": ["https://example.com/a2.parquet"],
                },
            ],
        }
    )
    coll_b = Collection.model_validate(
        {
            "id": "b",
            "slug": "coll-b",
            "title": "B",
            "datasets": [
                {
                    "id": "d3",
                    "slug": "ds3",
                    "title": "D3",
                    "file_format": "parquet",
                    "urls": ["https://example.com/b1.parquet"],
                },
            ],
        }
    )

    with (
        patch("all_data_cli.download.download_http", return_value=None) as mock_http,
        patch("all_data_cli.download.expand_s3_location", return_value=[]),
    ):
        failures = download_collections([coll_a, coll_b], Path(tmp_path))

    assert failures == []
    # Three datasets total → three submissions (one URL each).
    assert mock_http.call_count == 3
    # Each (collection, dataset) attribution shows up exactly once.
    attributions = {(call.args[2], call.args[3]) for call in mock_http.call_args_list}
    assert attributions == {
        ("coll-a", "ds1"),
        ("coll-a", "ds2"),
        ("coll-b", "ds3"),
    }


def test_download_collections_collects_worker_failures(tmp_path):
    """A worker returning a DownloadFailure (not None) is appended to failures with its attribution intact."""
    failure = DownloadFailure(
        collection_slug="test-collection",
        dataset_slug="matrix-a",
        url="https://example.com/a.parquet",
        reason="500 Server Error",
    )

    with (
        patch("all_data_cli.download.download_http", return_value=failure),
        patch("all_data_cli.download.expand_s3_location", return_value=[]),
    ):
        failures = download_collections([MOCK_COLLECTION], Path(tmp_path))

    assert failures == [failure]


def test_download_collections_shuts_down_on_keyboard_interrupt(tmp_path):
    """Ctrl-C during the as_completed loop cancels pending futures on both pools and re-raises."""
    shutdown_calls = []

    class SpyExecutor(ThreadPoolExecutor):
        def shutdown(self, wait=True, cancel_futures=False):
            shutdown_calls.append((wait, cancel_futures))
            return super().shutdown(wait=wait, cancel_futures=cancel_futures)

    def raise_kbd(*args, **kwargs):
        raise KeyboardInterrupt

    with (
        patch("all_data_cli.download.ThreadPoolExecutor", SpyExecutor),
        patch("all_data_cli.download.download_http", side_effect=raise_kbd),
        patch("all_data_cli.download.expand_s3_location", return_value=[]),
    ):
        with pytest.raises(KeyboardInterrupt):
            download_collections([MOCK_COLLECTION], Path(tmp_path))

    # Both pools must be shut down with cancel_futures=True before the
    # with-block's default shutdown(wait=True, cancel_futures=False).
    cancel_calls = [c for c in shutdown_calls if c == (False, True)]
    assert len(cancel_calls) == 2


# ── progress wiring ─────────────────────────────────────────────────────────


def test_submit_dataset_downloads_creates_one_progress_task_per_dataset(tmp_path):
    """A Zarr expanding to many objects still shows ONE aggregated progress task."""
    dataset = Dataset.model_validate(
        {
            "id": "ds-1",
            "slug": "matrix-zarr",
            "title": "Z",
            "file_format": "zarr_v3",
            "file_size_bytes": 5000,
            "urls": ["s3://bucket/zarr/"],
        }
    )
    expanded = [f"s3://bucket/zarr/chunk-{i}" for i in range(5)]
    progress = make_progress()

    with (
        patch("all_data_cli.download.download_s3_object", return_value=None) as mock_s3,
        patch("all_data_cli.download.expand_s3_location", return_value=expanded),
    ):
        with (
            ThreadPoolExecutor(max_workers=2) as http_ex,
            ThreadPoolExecutor(max_workers=2) as s3_ex,
        ):
            futures, _ = submit_dataset_downloads(
                "coll", dataset, Path(tmp_path), http_ex, s3_ex, progress
            )
            for f in futures:
                f.result()

    # Five S3 objects, but a single shared progress task with the dataset's total.
    assert len(progress.tasks) == 1
    task = progress.tasks[0]
    assert task.description == "coll/matrix-zarr"
    assert task.total == 5000
    # All five workers got the same on_bytes_downloaded callable (positional arg 4).
    advances = {call.args[4] for call in mock_s3.call_args_list}
    assert len(advances) == 1


def test_submit_dataset_downloads_no_progress_task_when_only_submission_failures(
    tmp_path,
):
    """A dataset whose URLs are all unsupported should not create a progress task."""
    dataset = Dataset.model_validate(
        {
            "id": "ds-1",
            "slug": "matrix",
            "title": "M",
            "file_format": "parquet",
            "urls": ["ftp://example.com/file.h5ad"],
        }
    )
    progress = make_progress()

    with (
        ThreadPoolExecutor(max_workers=1) as http_ex,
        ThreadPoolExecutor(max_workers=1) as s3_ex,
    ):
        futures, submission_failures = submit_dataset_downloads(
            "coll", dataset, Path(tmp_path), http_ex, s3_ex, progress
        )

    assert futures == []
    assert len(submission_failures) == 1
    assert progress.tasks == []
