import inspect
import json
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import click
import pytest
import requests
from click.testing import CliRunner

from biohub_data_cli.download import (
    analytics_disabled,
    download_collections,
    fetch_collection,
    submit_dataset_downloads,
)
from biohub_data_cli.utils.cli import DownloadDisplay
from biohub_data_cli.utils.http import download_http
from biohub_data_cli.main import cli
from biohub_data_cli.models import Collection, Dataset, DownloadFailure

# Never-set event for tests that don't exercise the cancel path.
_NEVER_CANCEL = threading.Event()

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
                "file_size_bytes": 1024,
                "urls": ["https://example.com/a.parquet"],
            },
            {
                "id": "ds-2",
                "slug": "matrix-b",
                "title": "Matrix B",
                "file_size_bytes": 512,
                "urls": ["s3://bucket/matrix-b/"],
            },
        ],
    }
)


# ── fetch_collection ────────────────────────────────────────────────────────


def test_fetch_collection_hits_backend_when_no_fixtures_dir(monkeypatch):
    monkeypatch.delenv("DATA_CLI_FIXTURES_DIR", raising=False)
    monkeypatch.setenv("OPS_SERVICE_URL_OVERRIDE", "https://backend.example.com")

    mock_response = SimpleNamespace(
        content=MOCK_COLLECTION.model_dump_json().encode(),
        raise_for_status=lambda: None,
    )

    with patch(
        "biohub_data_cli.download.requests.get", return_value=mock_response
    ) as mock_get:
        result = fetch_collection("coll-1")

    args, kwargs = mock_get.call_args
    assert args == ("https://backend.example.com/v1/cli/collections/coll-1",)
    assert kwargs["timeout"] == 30
    # User-Agent carries the version the backend parses; no dry-run header on a
    # plain (download) fetch.
    assert kwargs["headers"]["User-Agent"].startswith("biohub-data-cli/")
    assert "X-Biohub-Data-Cli-Dry-Run" not in kwargs["headers"]
    assert result.slug == MOCK_COLLECTION.slug


def test_fetch_collection_sends_dry_run_header(monkeypatch):
    monkeypatch.delenv("DATA_CLI_FIXTURES_DIR", raising=False)
    monkeypatch.setenv("OPS_SERVICE_URL_OVERRIDE", "https://backend.example.com")

    mock_response = SimpleNamespace(
        content=MOCK_COLLECTION.model_dump_json().encode(),
        raise_for_status=lambda: None,
    )

    with patch(
        "biohub_data_cli.download.requests.get", return_value=mock_response
    ) as mock_get:
        fetch_collection("coll-1", dry_run=True)

    headers = mock_get.call_args.kwargs["headers"]
    assert headers["X-Biohub-Data-Cli-Dry-Run"] == "true"
    assert headers["User-Agent"].startswith("biohub-data-cli/")


def test_fetch_collection_omits_disable_analytics_header_by_default(monkeypatch):
    monkeypatch.delenv("DATA_CLI_FIXTURES_DIR", raising=False)
    monkeypatch.setenv("OPS_SERVICE_URL_OVERRIDE", "https://backend.example.com")

    mock_response = SimpleNamespace(
        content=MOCK_COLLECTION.model_dump_json().encode(),
        raise_for_status=lambda: None,
    )

    with patch(
        "biohub_data_cli.download.requests.get", return_value=mock_response
    ) as mock_get:
        fetch_collection("coll-1")

    headers = mock_get.call_args.kwargs["headers"]
    assert "X-Biohub-Data-Cli-Disable-Analytics" not in headers


def test_fetch_collection_sends_disable_analytics_header_when_opted_out(monkeypatch):
    monkeypatch.delenv("DATA_CLI_FIXTURES_DIR", raising=False)
    monkeypatch.setenv("OPS_SERVICE_URL_OVERRIDE", "https://backend.example.com")

    mock_response = SimpleNamespace(
        content=MOCK_COLLECTION.model_dump_json().encode(),
        raise_for_status=lambda: None,
    )

    with patch(
        "biohub_data_cli.download.requests.get", return_value=mock_response
    ) as mock_get:
        fetch_collection("coll-1", analytics=False)

    headers = mock_get.call_args.kwargs["headers"]
    assert headers["X-Biohub-Data-Cli-Disable-Analytics"] == "true"


@pytest.mark.parametrize(
    "env, expected",
    [
        (None, False),
        ("true", True),
        ("TRUE", True),
        ("  true  ", True),
        ("1", False),  # only "true" opts out, matching PR #10
        ("yes", False),
        ("", False),
    ],
)
def test_analytics_disabled(monkeypatch, env, expected):
    if env is None:
        monkeypatch.delenv("DISABLE_BIOHUB_DATA_CLI_ANALYTICS", raising=False)
    else:
        monkeypatch.setenv("DISABLE_BIOHUB_DATA_CLI_ANALYTICS", env)
    assert analytics_disabled() is expected


def test_fetch_collection_wraps_backend_s3_uri_into_urls(monkeypatch):
    # BE returns a singular `s3_uri` per dataset; the validator wraps it into
    # our `urls: list[str]` shape so the download stack sees one field.
    monkeypatch.delenv("DATA_CLI_FIXTURES_DIR", raising=False)
    monkeypatch.setenv("OPS_SERVICE_URL_OVERRIDE", "https://backend.example.com")

    backend_payload = {
        "id": "coll-1",
        "slug": "test-collection",
        "title": "Test Collection",
        "datasets": [
            {
                "id": "ds-1",
                "slug": "matrix-a",
                "title": "Matrix A",
                "file_size_bytes": 1024,
                "s3_uri": "s3://bucket/matrix-a/",
            }
        ],
    }
    mock_response = SimpleNamespace(
        content=json.dumps(backend_payload).encode(),
        raise_for_status=lambda: None,
    )

    with patch("biohub_data_cli.download.requests.get", return_value=mock_response):
        result = fetch_collection("coll-1")

    assert result.datasets[0].urls == ["s3://bucket/matrix-a/"]


def test_fetch_collection_wraps_request_errors_as_click_exception(monkeypatch):
    monkeypatch.delenv("DATA_CLI_FIXTURES_DIR", raising=False)
    monkeypatch.setenv("OPS_SERVICE_URL_OVERRIDE", "https://backend.example.com")

    with patch(
        "biohub_data_cli.download.requests.get",
        side_effect=requests.ConnectionError("boom"),
    ):
        with pytest.raises(
            click.ClickException, match="Failed to fetch collection coll-1"
        ):
            fetch_collection("coll-1")


def test_fetch_collection_loads_from_fixtures_dir(tmp_path, monkeypatch):
    (tmp_path / "coll-1.json").write_text(MOCK_COLLECTION.model_dump_json())
    monkeypatch.setenv("DATA_CLI_FIXTURES_DIR", str(tmp_path))

    result = fetch_collection("coll-1")

    assert result.slug == MOCK_COLLECTION.slug
    assert [d.slug for d in result.datasets] == [
        d.slug for d in MOCK_COLLECTION.datasets
    ]


def test_fetch_collection_missing_fixture_raises_click_exception(tmp_path, monkeypatch):
    monkeypatch.setenv("DATA_CLI_FIXTURES_DIR", str(tmp_path))
    with pytest.raises(click.ClickException, match="No fixture for missing-id"):
        fetch_collection("missing-id")


# ── CLI command ──────────────────────────────────────────────────────────────


def test_download_collection_fetches_and_downloads(tmp_path):
    with (
        patch("biohub_data_cli.download.fetch_collection") as mock_fetch,
        patch("biohub_data_cli.download.download_collections") as mock_dl,
    ):
        mock_fetch.return_value = MOCK_COLLECTION
        mock_dl.return_value = []

        result = CliRunner().invoke(
            cli, ["download", "collection", "coll-1", "-o", str(tmp_path), "--yes"]
        )

        assert result.exit_code == 0, result.output
        mock_fetch.assert_called_once_with("coll-1", dry_run=False, analytics=True)
        passed_collections, _ = mock_dl.call_args[0]
        assert [c.slug for c in passed_collections] == ["test-collection"]


def test_disable_analytics_env_var_disables_analytics(tmp_path, monkeypatch):
    monkeypatch.setenv("DISABLE_BIOHUB_DATA_CLI_ANALYTICS", "true")
    with (
        patch("biohub_data_cli.download.fetch_collection") as mock_fetch,
        patch("biohub_data_cli.download.download_collections") as mock_dl,
    ):
        mock_fetch.return_value = MOCK_COLLECTION
        mock_dl.return_value = []

        result = CliRunner().invoke(
            cli, ["download", "collection", "coll-1", "-o", str(tmp_path), "--yes"]
        )

        assert result.exit_code == 0, result.output
        mock_fetch.assert_called_once_with("coll-1", dry_run=False, analytics=False)


def test_download_collection_accepts_multiple_ids(tmp_path):
    with (
        patch("biohub_data_cli.download.fetch_collection") as mock_fetch,
        patch("biohub_data_cli.download.download_collections") as mock_dl,
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
        patch("biohub_data_cli.download.fetch_collection") as mock_fetch,
        patch("biohub_data_cli.download.download_collections") as mock_dl,
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
    with patch("biohub_data_cli.download.fetch_collection") as mock_fetch:
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
            "urls": ["https://example.com/a.csv", "s3://bucket/b.h5ad"],
        }
    )

    with (
        patch("biohub_data_cli.download.download_http") as mock_http,
        patch("biohub_data_cli.download.download_s3_object") as mock_s3,
        patch(
            "biohub_data_cli.utils.s3.expand_s3_location",
            return_value=[("s3://bucket/b.h5ad", 100)],
        ),
    ):
        mock_http.return_value = None
        mock_s3.return_value = None

        with (
            ThreadPoolExecutor(max_workers=2) as http_ex,
            ThreadPoolExecutor(max_workers=2) as s3_ex,
        ):
            futures, submission_failures = submit_dataset_downloads(
                "coll",
                dataset,
                Path(tmp_path),
                http_ex,
                s3_ex,
                DownloadDisplay(),
                _NEVER_CANCEL,
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
            "urls": ["ftp://example.com/file.h5ad"],
        }
    )

    with (
        ThreadPoolExecutor(max_workers=1) as http_ex,
        ThreadPoolExecutor(max_workers=1) as s3_ex,
    ):
        futures, submission_failures = submit_dataset_downloads(
            "coll",
            dataset,
            Path(tmp_path),
            http_ex,
            s3_ex,
            DownloadDisplay(),
            _NEVER_CANCEL,
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
            "urls": ["s3://bucket/zarr-store/"],
        }
    )
    expanded = [
        ("s3://bucket/zarr-store/.zarray", 100),
        ("s3://bucket/zarr-store/.zattrs", 200),
        ("s3://bucket/zarr-store/0/0/0", 300),
        ("s3://bucket/zarr-store/0/0/1", 400),
        ("s3://bucket/zarr-store/0/0/2", 500),
    ]

    with (
        patch(
            "biohub_data_cli.download.download_s3_object", return_value=None
        ) as mock_s3,
        patch("biohub_data_cli.utils.s3.expand_s3_location", return_value=expanded),
    ):
        with (
            ThreadPoolExecutor(max_workers=2) as http_ex,
            ThreadPoolExecutor(max_workers=2) as s3_ex,
        ):
            futures, submission_failures = submit_dataset_downloads(
                "coll-x",
                dataset,
                Path(tmp_path),
                http_ex,
                s3_ex,
                DownloadDisplay(),
                _NEVER_CANCEL,
            )
            for f in futures:
                f.result()

    assert submission_failures == []
    assert len(futures) == len(expanded)
    submitted_uris = {call.args[0] for call in mock_s3.call_args_list}
    assert submitted_uris == {uri for uri, _ in expanded}


def test_submit_dataset_downloads_records_failure_when_s3_listing_fails(tmp_path):
    """If expand_s3_location raises, that URI becomes an immediate failure with full attribution."""
    dataset = Dataset.model_validate(
        {
            "id": "ds-1",
            "slug": "matrix-zarr",
            "title": "Zarr Matrix",
            "urls": ["s3://bucket/bad-prefix/"],
        }
    )

    with patch(
        "biohub_data_cli.utils.s3.expand_s3_location",
        side_effect=RuntimeError("listing failed: access denied"),
    ):
        with (
            ThreadPoolExecutor(max_workers=1) as http_ex,
            ThreadPoolExecutor(max_workers=1) as s3_ex,
        ):
            futures, submission_failures = submit_dataset_downloads(
                "coll-x",
                dataset,
                Path(tmp_path),
                http_ex,
                s3_ex,
                DownloadDisplay(),
                _NEVER_CANCEL,
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
        patch("biohub_data_cli.download.download_http", return_value=None) as mock_http,
        patch("biohub_data_cli.utils.s3.expand_s3_location", return_value=[]),
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
                    "urls": ["https://example.com/a1.parquet"],
                },
                {
                    "id": "d2",
                    "slug": "ds2",
                    "title": "D2",
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
                    "urls": ["https://example.com/b1.parquet"],
                },
            ],
        }
    )

    with (
        patch("biohub_data_cli.download.download_http", return_value=None) as mock_http,
        patch("biohub_data_cli.utils.s3.expand_s3_location", return_value=[]),
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
        patch("biohub_data_cli.download.download_http", return_value=failure),
        patch("biohub_data_cli.utils.s3.expand_s3_location", return_value=[]),
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
        patch("biohub_data_cli.download.ThreadPoolExecutor", SpyExecutor),
        patch("biohub_data_cli.download.download_http", side_effect=raise_kbd),
        patch("biohub_data_cli.utils.s3.expand_s3_location", return_value=[]),
    ):
        with pytest.raises(KeyboardInterrupt):
            download_collections([MOCK_COLLECTION], Path(tmp_path))

    # Both pools must be shut down with cancel_futures=True before the
    # with-block's default shutdown(wait=True, cancel_futures=False).
    cancel_calls = [c for c in shutdown_calls if c == (False, True)]
    assert len(cancel_calls) == 2


def test_download_collections_passes_cancel_event_to_workers(tmp_path):
    """Workers receive the shared threading.Event so they can bail out mid-chunk."""
    received_cancels = []
    signature = inspect.signature(download_http)

    def capture(*args, **kwargs):
        bound = signature.bind(*args, **kwargs)
        received_cancels.append(bound.arguments["cancel"])
        return None

    with (
        patch("biohub_data_cli.download.download_http", side_effect=capture),
        patch("biohub_data_cli.utils.s3.expand_s3_location", return_value=[]),
    ):
        download_collections([MOCK_COLLECTION], Path(tmp_path))

    assert len(received_cancels) == 1
    assert isinstance(received_cancels[0], threading.Event)
    assert not received_cancels[0].is_set()


# ── progress wiring ─────────────────────────────────────────────────────────


def test_submit_dataset_downloads_creates_one_progress_task_per_dataset(tmp_path):
    """A Zarr expanding to many objects still shows ONE aggregated progress task."""
    dataset = Dataset.model_validate(
        {
            "id": "ds-1",
            "slug": "matrix-zarr",
            "title": "Z",
            "file_size_bytes": 5000,
            "urls": ["s3://bucket/zarr/"],
        }
    )
    # Five chunks of 1000 bytes each → orchestrator seeds total=5000 from listing.
    expanded = [(f"s3://bucket/zarr/chunk-{i}", 1000) for i in range(5)]
    display = DownloadDisplay()

    with (
        patch(
            "biohub_data_cli.download.download_s3_object", return_value=None
        ) as mock_s3,
        patch("biohub_data_cli.utils.s3.expand_s3_location", return_value=expanded),
    ):
        with (
            ThreadPoolExecutor(max_workers=2) as http_ex,
            ThreadPoolExecutor(max_workers=2) as s3_ex,
        ):
            futures, _ = submit_dataset_downloads(
                "coll", dataset, Path(tmp_path), http_ex, s3_ex, display, _NEVER_CANCEL
            )
            for f in futures:
                f.result()

    # Five S3 objects, but a single shared progress task with the dataset's total.
    assert len(display.progress.tasks) == 1
    task = display.progress.tasks[0]
    assert task.description == "coll/matrix-zarr"
    # Total comes from summed S3 sizes (5 × 1000), not from dataset.file_size_bytes.
    assert task.total == 5000
    # All five workers got the same on_bytes_downloaded callable (positional arg 4).
    advances = {call.args[4] for call in mock_s3.call_args_list}
    assert len(advances) == 1


def test_submit_dataset_downloads_http_only_ignores_be_file_size_bytes(tmp_path):
    """HTTP-only dataset with `file_size_bytes` set must NOT seed the bar from it."""
    dataset = Dataset.model_validate(
        {
            "id": "ds-1",
            "slug": "http-only",
            "title": "HTTP only",
            "file_size_bytes": 9999,
            "urls": [
                "https://example.com/a.parquet",
                "https://example.com/b.parquet",
            ],
        }
    )
    display = DownloadDisplay()

    def fake_http(url, outdir, coll, ds, on_bytes_downloaded, on_size_known, cancel):
        on_size_known(100)
        return None

    with patch("biohub_data_cli.download.download_http", side_effect=fake_http):
        with (
            ThreadPoolExecutor(max_workers=2) as http_ex,
            ThreadPoolExecutor(max_workers=2) as s3_ex,
        ):
            futures, _ = submit_dataset_downloads(
                "coll", dataset, Path(tmp_path), http_ex, s3_ex, display, _NEVER_CANCEL
            )
            for f in futures:
                f.result()

    task = display.progress.tasks[0]
    # Sum of the two Content-Length reports, NOT file_size_bytes + reports.
    assert task.total == 200


def test_submit_dataset_downloads_no_progress_task_when_only_submission_failures(
    tmp_path,
):
    """A dataset whose URLs are all unsupported should not create a progress task."""
    dataset = Dataset.model_validate(
        {
            "id": "ds-1",
            "slug": "matrix",
            "title": "M",
            "urls": ["ftp://example.com/file.h5ad"],
        }
    )
    display = DownloadDisplay()

    with (
        ThreadPoolExecutor(max_workers=1) as http_ex,
        ThreadPoolExecutor(max_workers=1) as s3_ex,
    ):
        futures, submission_failures = submit_dataset_downloads(
            "coll", dataset, Path(tmp_path), http_ex, s3_ex, display, _NEVER_CANCEL
        )

    assert futures == []
    assert len(submission_failures) == 1
    assert display.progress.tasks == []


# ── CLI --dry-run ───────────────────────────────────────────────────────────


def test_dry_run_prints_summary_and_does_not_download(tmp_path):
    with (
        patch("biohub_data_cli.download.fetch_collection") as mock_fetch,
        patch("biohub_data_cli.download.download_collections") as mock_dl,
        patch(
            "biohub_data_cli.utils.s3.expand_s3_location",
            return_value=[("s3://bucket/matrix-b/chunk", 512)],
        ),
    ):
        mock_fetch.return_value = MOCK_COLLECTION

        result = CliRunner().invoke(
            cli, ["download", "collection", "coll-1", "-o", str(tmp_path), "--dry-run"]
        )

    assert result.exit_code == 0, result.output
    mock_dl.assert_not_called()
    # Mock collection has one HTTP dataset (matrix-a, silently skipped) and one
    # S3 dataset (matrix-b, sized at 512 from the mocked expansion).
    assert "matrix-a" in result.output
    assert "matrix-b" in result.output
    assert "512 bytes" in result.output


def test_dry_run_exits_nonzero_when_size_lookups_fail(tmp_path):
    # Dataset without `file_size_bytes` so dry-run falls back to S3 listing,
    # which is the only path that can produce a size-lookup failure.
    collection_without_be_size = Collection.model_validate(
        {
            "id": "coll-1",
            "slug": "test-collection",
            "title": "Test Collection",
            "datasets": [
                {
                    "id": "ds-1",
                    "slug": "matrix-b",
                    "title": "Matrix B",
                    "urls": ["s3://bucket/matrix-b/"],
                },
            ],
        }
    )
    with (
        patch("biohub_data_cli.download.fetch_collection") as mock_fetch,
        patch("biohub_data_cli.download.download_collections") as mock_dl,
        patch(
            "biohub_data_cli.utils.s3.expand_s3_location",
            side_effect=RuntimeError("listing failed"),
        ),
    ):
        mock_fetch.return_value = collection_without_be_size

        result = CliRunner().invoke(
            cli, ["download", "collection", "coll-1", "-o", str(tmp_path), "--dry-run"]
        )

    assert result.exit_code != 0
    assert "partial" in result.output.lower() or "failed" in result.output.lower()
    mock_dl.assert_not_called()


def test_dry_run_with_yes_is_mutually_exclusive(tmp_path):
    with patch("biohub_data_cli.download.fetch_collection") as mock_fetch:
        mock_fetch.return_value = MOCK_COLLECTION

        result = CliRunner().invoke(
            cli,
            [
                "download",
                "collection",
                "coll-1",
                "-o",
                str(tmp_path),
                "--dry-run",
                "--yes",
            ],
        )

    assert result.exit_code != 0
    assert "mutually exclusive" in result.output.lower()
