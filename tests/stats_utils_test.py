from unittest.mock import patch

from biohub_data_cli.models import Collection
from biohub_data_cli.utils.stats import (
    aggregate_dry_run_stats,
    estimate_size_summary,
    get_collections_stats,
)


def _collection_with_sizes(sizes: list[int | None]) -> Collection:
    """Build a Collection whose datasets carry the given file_size_bytes values."""
    return Collection.model_validate(
        {
            "id": "c",
            "slug": "coll",
            "title": "C",
            "datasets": [
                {
                    "id": f"d{i}",
                    "slug": f"ds{i}",
                    "title": f"D{i}",
                    "file_format": "parquet",
                    "file_size_bytes": size,
                    "urls": [],
                }
                for i, size in enumerate(sizes)
            ],
        }
    )


# ── estimate_size_summary ───────────────────────────────────────────────────


def test_estimate_size_summary_all_sized():
    """1024 + 512 = 1536 B → '1.5 kB' via rich.filesize.decimal."""
    result = estimate_size_summary([_collection_with_sizes([1024, 512])])
    assert "estimated" in result
    assert "1.5" in result


def test_estimate_size_summary_partial_sizing():
    result = estimate_size_summary([_collection_with_sizes([1024, None])])
    assert "estimated" in result
    assert "1 dataset(s) unsized" in result


def test_estimate_size_summary_all_unsized():
    result = estimate_size_summary([_collection_with_sizes([None, None])])
    assert result == "size unknown"


# ── get_collections_stats ───────────────────────────────────────────────────


def test_get_collections_stats_aggregates_per_dataset():
    """One DatasetStats row per dataset, total_bytes summed across S3 URIs."""
    coll = Collection.model_validate(
        {
            "id": "c",
            "slug": "coll",
            "title": "C",
            "datasets": [
                {
                    "id": "d1",
                    "slug": "ds1",
                    "title": "D1",
                    "file_format": "zarr_v3",
                    "urls": ["s3://b/x/", "s3://b/y/"],
                },
                {
                    "id": "d2",
                    "slug": "ds2",
                    "title": "D2",
                    "file_format": "parquet",
                    "urls": ["s3://b/z"],
                },
            ],
        }
    )
    expansions = {
        "s3://b/x/": [("s3://b/x/a", 100), ("s3://b/x/b", 200)],
        "s3://b/y/": [("s3://b/y/c", 300)],
        "s3://b/z": [("s3://b/z", 50)],
    }
    with patch(
        "biohub_data_cli.utils.s3.expand_s3_location",
        side_effect=lambda uri: expansions[uri],
    ):
        stats = get_collections_stats([coll])

    assert len(stats) == 1
    returned_coll, rows = stats[0]
    assert returned_coll is coll
    assert len(rows) == 2
    assert rows[0].collection_slug == "coll"
    assert rows[0].dataset_slug == "ds1"
    assert rows[0].total_bytes == 600
    assert rows[0].n_failed_uris == 0
    assert rows[1].dataset_slug == "ds2"
    assert rows[1].total_bytes == 50


def test_get_collections_stats_silently_skips_http_urls():
    """HTTP URLs are not sized in dry-run; only s3:// URIs contribute."""
    coll = Collection.model_validate(
        {
            "id": "c",
            "slug": "coll",
            "title": "C",
            "datasets": [
                {
                    "id": "d1",
                    "slug": "mixed",
                    "title": "Mixed",
                    "file_format": "parquet",
                    "urls": [
                        "https://example.com/a.parquet",
                        "s3://b/x",
                        "ftp://example.com/file",
                    ],
                }
            ],
        }
    )
    with patch(
        "biohub_data_cli.utils.s3.expand_s3_location",
        return_value=[("s3://b/x", 1024)],
    ) as mock_expand:
        stats = get_collections_stats([coll])

    mock_expand.assert_called_once_with("s3://b/x")
    rows = stats[0][1]
    assert rows[0].total_bytes == 1024
    assert rows[0].n_failed_uris == 0


def test_get_collections_stats_counts_failed_uris_as_partial():
    coll = Collection.model_validate(
        {
            "id": "c",
            "slug": "coll",
            "title": "C",
            "datasets": [
                {
                    "id": "d1",
                    "slug": "ds1",
                    "title": "D1",
                    "file_format": "zarr_v3",
                    "urls": ["s3://b/good", "s3://b/bad"],
                }
            ],
        }
    )

    def expand(uri):
        if uri == "s3://b/bad":
            raise RuntimeError("listing failed")
        return [("s3://b/good/file", 500)]

    with patch("biohub_data_cli.utils.s3.expand_s3_location", side_effect=expand):
        stats = get_collections_stats([coll])

    rows = stats[0][1]
    assert rows[0].total_bytes == 500
    assert rows[0].n_failed_uris == 1


# ── aggregate_dry_run_stats ─────────────────────────────────────────────────


def test_aggregate_counts_http_urls_skipped():
    """Any http:// or https:// URL counts toward n_http_urls_skipped."""
    coll = Collection.model_validate(
        {
            "id": "c",
            "slug": "coll",
            "title": "C",
            "datasets": [
                {
                    "id": "d1",
                    "slug": "ds1",
                    "title": "D1",
                    "file_format": "parquet",
                    "urls": [
                        "https://example.com/a.parquet",
                        "http://example.com/b.parquet",
                        "s3://b/x",
                        "ftp://example.com/c",  # not http, not counted
                    ],
                }
            ],
        }
    )
    with patch(
        "biohub_data_cli.utils.s3.expand_s3_location",
        return_value=[("s3://b/x", 100)],
    ):
        stats = get_collections_stats([coll])

    agg = aggregate_dry_run_stats(stats)
    assert agg.n_http_urls_skipped == 2
    assert agg.total_bytes == 100
    assert agg.n_failed_uris == 0


def test_aggregate_no_http_urls_skipped_when_s3_only():
    coll = Collection.model_validate(
        {
            "id": "c",
            "slug": "coll",
            "title": "C",
            "datasets": [
                {
                    "id": "d1",
                    "slug": "ds1",
                    "title": "D1",
                    "file_format": "parquet",
                    "urls": ["s3://b/x"],
                }
            ],
        }
    )
    with patch(
        "biohub_data_cli.utils.s3.expand_s3_location",
        return_value=[("s3://b/x", 100)],
    ):
        stats = get_collections_stats([coll])

    agg = aggregate_dry_run_stats(stats)
    assert agg.n_http_urls_skipped == 0
