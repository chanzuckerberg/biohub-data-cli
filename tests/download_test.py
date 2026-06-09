import inspect
import json
import threading
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import click
import pytest
import requests
from click.testing import CliRunner

from biohub_data_cli.download import (
    _list_and_record,
    analytics_disabled,
    download_collections,
    ensure_collection_listed,
    fetch_collection,
    submit_dataset_downloads,
)
from biohub_data_cli.utils.cli import DownloadDisplay
from biohub_data_cli.utils.download_state import DownloadStateDB
from biohub_data_cli.utils.http import download_http
from biohub_data_cli.main import cli
from biohub_data_cli.models import Collection, Dataset, DownloadFailure, DownloadResult

# Never-set event for tests that don't exercise the cancel path.
_NEVER_CANCEL = threading.Event()


def _fresh_db(tmp_path: Path, collection_slug: str = "coll") -> DownloadStateDB:
    """A freshly-initialized DownloadStateDB for unit-testing submit_dataset_downloads.

    Tests that exercise submit_dataset_downloads directly need a DB instance
    in the right shape; this is the common setup.
    """
    db = DownloadStateDB.for_collection(Path(tmp_path), collection_slug)
    db.init_fresh()
    return db


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


def test_fetch_collection_hits_backend_when_no_fixtures_dir(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
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


def test_fetch_collection_sends_dry_run_header(monkeypatch: pytest.MonkeyPatch) -> None:
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


def test_fetch_collection_omits_disable_analytics_header_by_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
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


def test_fetch_collection_sends_disable_analytics_header_when_opted_out(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
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
def test_analytics_disabled(
    monkeypatch: pytest.MonkeyPatch, env: str | None, expected: bool
) -> None:
    if env is None:
        monkeypatch.delenv("DISABLE_BIOHUB_DATA_CLI_ANALYTICS", raising=False)
    else:
        monkeypatch.setenv("DISABLE_BIOHUB_DATA_CLI_ANALYTICS", env)
    assert analytics_disabled() is expected


def test_fetch_collection_wraps_backend_s3_uri_into_urls(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
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


def test_fetch_collection_wraps_request_errors_as_click_exception(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
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


def test_fetch_collection_loads_from_fixtures_dir(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    (tmp_path / "coll-1.json").write_text(MOCK_COLLECTION.model_dump_json())
    monkeypatch.setenv("DATA_CLI_FIXTURES_DIR", str(tmp_path))

    result = fetch_collection("coll-1")

    assert result.slug == MOCK_COLLECTION.slug
    assert [d.slug for d in result.datasets] == [
        d.slug for d in MOCK_COLLECTION.datasets
    ]


def test_fetch_collection_missing_fixture_raises_click_exception(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("DATA_CLI_FIXTURES_DIR", str(tmp_path))
    with pytest.raises(click.ClickException, match="No fixture for missing-id"):
        fetch_collection("missing-id")


# ── CLI command ──────────────────────────────────────────────────────────────


def test_download_collection_fetches_and_downloads(tmp_path: Path) -> None:
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


def test_disable_analytics_env_var_disables_analytics(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
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


def test_download_collection_accepts_multiple_ids(tmp_path: Path) -> None:
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


def test_download_collection_dataset_filters_to_subset(tmp_path: Path) -> None:
    with (
        patch("biohub_data_cli.download.fetch_collection") as mock_fetch,
        patch("biohub_data_cli.download.download_collections") as mock_dl,
    ):
        mock_fetch.return_value = MOCK_COLLECTION.model_copy(deep=True)
        mock_dl.return_value = []

        result = CliRunner().invoke(
            cli,
            [
                "download",
                "collection",
                "coll-1",
                "--dataset",
                "matrix-b",
                "-o",
                str(tmp_path),
                "--yes",
            ],
        )

        assert result.exit_code == 0, result.output
        passed_collections, _ = mock_dl.call_args[0]
        assert [d.slug for d in passed_collections[0].datasets] == ["matrix-b"]


def test_download_collection_dataset_unknown_slug_errors(tmp_path: Path) -> None:
    with patch("biohub_data_cli.download.fetch_collection") as mock_fetch:
        mock_fetch.return_value = MOCK_COLLECTION.model_copy(deep=True)

        result = CliRunner().invoke(
            cli,
            ["download", "collection", "coll-1", "--dataset", "nope", "--yes"],
        )

        assert result.exit_code != 0
        assert "Unknown dataset slug(s)" in result.output
        # Lists the available slugs so a typo surfaces the valid set.
        assert "matrix-a" in result.output and "matrix-b" in result.output


def test_download_collection_dataset_rejected_with_multiple_collections(
    tmp_path: Path,
) -> None:
    with patch("biohub_data_cli.download.fetch_collection") as mock_fetch:
        mock_fetch.return_value = MOCK_COLLECTION.model_copy(deep=True)

        result = CliRunner().invoke(
            cli,
            ["download", "collection", "a", "b", "--dataset", "matrix-a", "--yes"],
        )

        assert result.exit_code != 0
        assert "single collection" in result.output


def test_download_collection_prints_failure_summary_and_exits_nonzero(
    tmp_path: Path,
) -> None:
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


def test_no_datasets_raises_error(tmp_path: Path) -> None:
    empty = MOCK_COLLECTION.model_copy(update={"datasets": []})
    with patch("biohub_data_cli.download.fetch_collection") as mock_fetch:
        mock_fetch.return_value = empty

        result = CliRunner().invoke(
            cli, ["download", "collection", "coll-1", "-o", str(tmp_path)]
        )

        assert result.exit_code != 0
        assert "no datasets" in result.output.lower()


# ── submit_dataset_downloads ────────────────────────────────────────────────


def test_submit_dataset_downloads_routes_and_collects_submission_failures(
    tmp_path: Path,
) -> None:
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

        db = _fresh_db(tmp_path)
        display = DownloadDisplay()
        listing_failures = _list_and_record("coll", dataset, db, display)

        with (
            ThreadPoolExecutor(max_workers=2) as http_ex,
            ThreadPoolExecutor(max_workers=2) as s3_ex,
        ):
            futures = submit_dataset_downloads(
                "coll",
                dataset,
                Path(tmp_path),
                db,
                http_ex,
                s3_ex,
                display,
                _NEVER_CANCEL,
            )
            for f in futures:
                f.result()

    assert len(futures) == 2
    assert listing_failures == []
    mock_http.assert_called_once()
    mock_s3.assert_called_once()


def test_submit_dataset_downloads_resume_submits_pending_http_when_s3_done(
    tmp_path: Path,
) -> None:
    """Mixed S3+HTTP dataset: if the S3 objects are already marked downloaded
    but the HTTP file is not, resume must still submit the HTTP download.

    HTTP entries are stored with expected_size=None, so a byte-sum completion
    check would treat this dataset as complete and silently skip the HTTP file.
    Completion must key off the `downloaded` flag, not byte totals.
    """
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

        db = _fresh_db(tmp_path)
        display = DownloadDisplay()
        _list_and_record("coll", dataset, db, display)
        # Simulate a prior run that finished the S3 object but not the HTTP file.
        db.mark_downloaded("matrix-a", "s3://bucket/b.h5ad", size=100)

        with (
            ThreadPoolExecutor(max_workers=2) as http_ex,
            ThreadPoolExecutor(max_workers=2) as s3_ex,
        ):
            futures = submit_dataset_downloads(
                "coll",
                dataset,
                Path(tmp_path),
                db,
                http_ex,
                s3_ex,
                display,
                _NEVER_CANCEL,
            )
            for f in futures:
                f.result()

    assert len(futures) == 1
    mock_http.assert_called_once()
    mock_s3.assert_not_called()


def test_submit_dataset_downloads_unknown_scheme(tmp_path: Path) -> None:
    dataset = Dataset.model_validate(
        {
            "id": "ds-1",
            "slug": "matrix-a",
            "title": "Matrix A",
            "urls": ["ftp://example.com/file.h5ad"],
        }
    )

    db = _fresh_db(tmp_path)
    display = DownloadDisplay()
    listing_failures = _list_and_record("coll", dataset, db, display)

    with (
        ThreadPoolExecutor(max_workers=1) as http_ex,
        ThreadPoolExecutor(max_workers=1) as s3_ex,
    ):
        futures = submit_dataset_downloads(
            "coll",
            dataset,
            Path(tmp_path),
            db,
            http_ex,
            s3_ex,
            display,
            _NEVER_CANCEL,
        )

    assert futures == {}
    assert len(listing_failures) == 1
    assert "Unsupported URL scheme" in listing_failures[0].reason
    # The real invariant the submit loop relies on: _list_and_record never
    # inserts an unknown-scheme URL into the DB, so the loop only ever sees
    # s3/http. This guards the removal of the old defensive `else` branch —
    # if an unknown scheme leaked into the DB it would be submitted as http.
    assert list(db.iter_entries_for_dataset(dataset.slug)) == []


def test_submit_dataset_downloads_submits_every_expanded_s3_object(
    tmp_path: Path,
) -> None:
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
        db = _fresh_db(tmp_path, "coll-x")
        display = DownloadDisplay()
        listing_failures = _list_and_record("coll-x", dataset, db, display)

        with (
            ThreadPoolExecutor(max_workers=2) as http_ex,
            ThreadPoolExecutor(max_workers=2) as s3_ex,
        ):
            futures = submit_dataset_downloads(
                "coll-x",
                dataset,
                Path(tmp_path),
                db,
                http_ex,
                s3_ex,
                display,
                _NEVER_CANCEL,
            )
            for f in futures:
                f.result()

    assert listing_failures == []
    assert len(futures) == len(expanded)
    submitted_uris = {call.args[0] for call in mock_s3.call_args_list}
    assert submitted_uris == {uri for uri, _ in expanded}


def test_submit_dataset_downloads_records_failure_when_s3_listing_fails(
    tmp_path: Path,
) -> None:
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
        db = _fresh_db(tmp_path, "coll-x")
        display = DownloadDisplay()
        listing_failures = _list_and_record("coll-x", dataset, db, display)

        with (
            ThreadPoolExecutor(max_workers=1) as http_ex,
            ThreadPoolExecutor(max_workers=1) as s3_ex,
        ):
            futures = submit_dataset_downloads(
                "coll-x",
                dataset,
                Path(tmp_path),
                db,
                http_ex,
                s3_ex,
                display,
                _NEVER_CANCEL,
            )

    assert futures == {}
    assert len(listing_failures) == 1
    failure = listing_failures[0]
    assert failure.collection_slug == "coll-x"
    assert failure.dataset_slug == "matrix-zarr"
    assert failure.url == "s3://bucket/bad-prefix/"
    assert "listing failed" in failure.reason


def test_ensure_collection_listed_marks_listed_when_dataset_lists_cleanly(
    tmp_path: Path,
) -> None:
    """Happy path: a dataset that lists without error is marked listed, so a
    resume run within TTL can trust its cached entries."""
    collection = Collection.model_validate(
        {
            "id": "coll-1",
            "slug": "coll",
            "title": "Coll",
            "datasets": [
                {
                    "id": "ds-1",
                    "slug": "matrix-a",
                    "title": "A",
                    "urls": ["s3://bucket/a.h5ad"],
                }
            ],
        }
    )
    db = DownloadStateDB.for_collection(Path(tmp_path), "coll")
    db.ensure_ready()
    with patch(
        "biohub_data_cli.utils.s3.expand_s3_location",
        return_value=[("s3://bucket/a.h5ad", 100)],
    ):
        ensure_collection_listed(collection, db, DownloadDisplay())

    assert db.get_unexpired_dataset_slugs() == {"matrix-a"}


def test_ensure_collection_listed_leaves_failed_dataset_unlisted(
    tmp_path: Path,
) -> None:
    """Regression: a dataset that fails to list must NOT be marked listed.

    If one dataset's S3 prefix fails, only the clean dataset is cached; the
    failed one stays unlisted so the next run re-lists just it. Otherwise resume
    would find no entries for it, never resurface the failure, and falsely
    report success — while the clean datasets are unaffected.
    """
    collection = Collection.model_validate(
        {
            "id": "coll-1",
            "slug": "coll",
            "title": "Coll",
            "datasets": [
                {
                    "id": "ds-ok",
                    "slug": "matrix-ok",
                    "title": "OK",
                    "urls": ["s3://bucket/good/a.h5ad"],
                },
                {
                    "id": "ds-bad",
                    "slug": "matrix-bad",
                    "title": "Bad",
                    "urls": ["s3://bucket/bad-prefix/"],
                },
            ],
        }
    )
    db = DownloadStateDB.for_collection(Path(tmp_path), "coll")
    db.ensure_ready()
    display = DownloadDisplay()

    def fake_expand(uri: str, *args: object, **kwargs: object) -> list[tuple[str, int]]:
        if uri == "s3://bucket/bad-prefix/":
            raise RuntimeError("listing failed: access denied")
        return [(uri, 100)]

    with patch(
        "biohub_data_cli.utils.s3.expand_s3_location",
        side_effect=fake_expand,
    ):
        ensure_collection_listed(collection, db, display)

    # The failure is surfaced now...
    assert any(f.dataset_slug == "matrix-bad" for f in display.failures)
    # ...the failed dataset is not trusted for resume, but the clean one is.
    assert db.get_unexpired_dataset_slugs() == {"matrix-ok"}


# ── download_collections ────────────────────────────────────────────────────


def test_download_collections_writes_to_collection_dataset_subdirs(
    tmp_path: Path,
) -> None:
    """Verifies the outdir/<collection.slug>/<dataset.slug>/ layout."""
    with (
        patch(
            "biohub_data_cli.download.download_http",
            return_value=DownloadResult.succeeded(1024),
        ) as mock_http,
        patch("biohub_data_cli.utils.s3.expand_s3_location", return_value=[]),
    ):
        download_collections([MOCK_COLLECTION], Path(tmp_path))

    # http URL was submitted with the per-dataset outdir
    called_outdir = mock_http.call_args.args[1]
    assert called_outdir == tmp_path / "test-collection" / "matrix-a"


def test_download_collections_submits_every_dataset_across_collections(
    tmp_path: Path,
) -> None:
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
        patch(
            "biohub_data_cli.download.download_http",
            return_value=DownloadResult.succeeded(1024),
        ) as mock_http,
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


def test_download_collections_collects_worker_failures(tmp_path: Path) -> None:
    """A worker returning a DownloadFailure (not None) is appended to failures with its attribution intact."""
    failure = DownloadFailure(
        collection_slug="test-collection",
        dataset_slug="matrix-a",
        url="https://example.com/a.parquet",
        reason="500 Server Error",
    )

    with (
        patch(
            "biohub_data_cli.download.download_http",
            return_value=DownloadResult.failed(failure),
        ),
        patch("biohub_data_cli.utils.s3.expand_s3_location", return_value=[]),
    ):
        failures = download_collections([MOCK_COLLECTION], Path(tmp_path))

    assert failures == [failure]


def test_download_collections_persists_downloaded_size(tmp_path: Path) -> None:
    """A successful worker returns a `DownloadResult` carrying the byte count, and
    the orchestrator must persist exactly that size via `mark_downloaded` — the
    success contract is the size, not just "not a failure"."""
    collection = Collection.model_validate(
        {
            "id": "c",
            "slug": "sized-collection",
            "title": "Sized",
            "datasets": [
                {
                    "id": "d1",
                    "slug": "ds1",
                    "title": "D1",
                    "urls": ["https://example.com/a.parquet"],
                }
            ],
        }
    )

    with (
        patch(
            "biohub_data_cli.download.download_http",
            return_value=DownloadResult.succeeded(123),
        ),
        patch("biohub_data_cli.utils.s3.expand_s3_location", return_value=[]),
        patch.object(DownloadStateDB, "mark_downloaded", autospec=True) as mock_mark,
    ):
        failures = download_collections([collection], Path(tmp_path))

    assert failures == []
    # The size from the DownloadResult is threaded through to persistence.
    mock_mark.assert_called_once()
    assert mock_mark.call_args.kwargs == {"size": 123}
    assert mock_mark.call_args.args[1:] == ("ds1", "https://example.com/a.parquet")


def test_download_collections_shuts_down_on_keyboard_interrupt(tmp_path: Path) -> None:
    """Ctrl-C during the as_completed loop cancels pending futures on both pools and re-raises."""
    shutdown_calls: list[tuple[bool, bool]] = []

    class SpyExecutor(ThreadPoolExecutor):
        def shutdown(self, wait: bool = True, cancel_futures: bool = False) -> None:
            shutdown_calls.append((wait, cancel_futures))
            return super().shutdown(wait=wait, cancel_futures=cancel_futures)

    def raise_kbd(*args: object, **kwargs: object) -> None:
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


def test_download_collections_passes_cancel_event_to_workers(tmp_path: Path) -> None:
    """Workers receive the shared threading.Event so they can bail out mid-chunk."""
    received_cancels: list[object] = []
    signature = inspect.signature(download_http)

    def capture(*args: object, **kwargs: object) -> DownloadResult:
        bound = signature.bind(*args, **kwargs)
        received_cancels.append(bound.arguments["cancel"])
        return DownloadResult.succeeded(0)

    with (
        patch("biohub_data_cli.download.download_http", side_effect=capture),
        patch("biohub_data_cli.utils.s3.expand_s3_location", return_value=[]),
    ):
        download_collections([MOCK_COLLECTION], Path(tmp_path))

    assert len(received_cancels) == 1
    assert isinstance(received_cancels[0], threading.Event)
    assert not received_cancels[0].is_set()


# ── progress wiring ─────────────────────────────────────────────────────────


def test_submit_dataset_downloads_creates_one_progress_task_per_dataset(
    tmp_path: Path,
) -> None:
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
        db = _fresh_db(tmp_path)
        _list_and_record("coll", dataset, db, display)

        with (
            ThreadPoolExecutor(max_workers=2) as http_ex,
            ThreadPoolExecutor(max_workers=2) as s3_ex,
        ):
            futures = submit_dataset_downloads(
                "coll",
                dataset,
                Path(tmp_path),
                db,
                http_ex,
                s3_ex,
                display,
                _NEVER_CANCEL,
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


def test_submit_dataset_downloads_http_only_ignores_be_file_size_bytes(
    tmp_path: Path,
) -> None:
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

    def fake_http(
        url: str,
        outdir: Path,
        coll: str,
        ds: str,
        on_bytes_downloaded: Callable[[int], None],
        on_size_known: Callable[[int], None],
        cancel: threading.Event,
    ) -> None:
        on_size_known(100)
        return None

    with patch("biohub_data_cli.download.download_http", side_effect=fake_http):
        db = _fresh_db(tmp_path)
        _list_and_record("coll", dataset, db, display)

        with (
            ThreadPoolExecutor(max_workers=2) as http_ex,
            ThreadPoolExecutor(max_workers=2) as s3_ex,
        ):
            futures = submit_dataset_downloads(
                "coll",
                dataset,
                Path(tmp_path),
                db,
                http_ex,
                s3_ex,
                display,
                _NEVER_CANCEL,
            )
            for f in futures:
                f.result()

    task = display.progress.tasks[0]
    # Sum of the two Content-Length reports, NOT file_size_bytes + reports.
    assert task.total == 200


def test_submit_dataset_downloads_no_progress_task_when_only_submission_failures(
    tmp_path: Path,
) -> None:
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

    db = _fresh_db(tmp_path)
    listing_failures = _list_and_record("coll", dataset, db, display)

    with (
        ThreadPoolExecutor(max_workers=1) as http_ex,
        ThreadPoolExecutor(max_workers=1) as s3_ex,
    ):
        futures = submit_dataset_downloads(
            "coll",
            dataset,
            Path(tmp_path),
            db,
            http_ex,
            s3_ex,
            display,
            _NEVER_CANCEL,
        )

    assert futures == {}
    assert len(listing_failures) == 1
    assert display.progress.tasks == []


# ── CLI --dry-run ───────────────────────────────────────────────────────────


def test_dry_run_prints_summary_and_does_not_download(tmp_path: Path) -> None:
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


def test_dry_run_exits_nonzero_when_size_lookups_fail(tmp_path: Path) -> None:
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


def test_dry_run_with_yes_is_mutually_exclusive(tmp_path: Path) -> None:
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
