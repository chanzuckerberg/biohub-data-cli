from unittest.mock import ANY, MagicMock, patch

from all_data_cli.utils.s3 import (
    download_s3_object,
    expand_s3_location,
    s3_url_to_local_path,
)


def _make_s3_mock(keys: list[str]) -> MagicMock:
    """S3 client mock whose paginator yields one page with the given object keys."""
    s3 = MagicMock()
    paginator = MagicMock()
    paginator.paginate.return_value = [{"Contents": [{"Key": k} for k in keys]}]
    s3.get_paginator.return_value = paginator
    return s3


def test_expand_s3_location_single_file():
    s3 = _make_s3_mock(["dir/file.h5ad"])
    with patch("all_data_cli.utils.s3._make_s3_client", return_value=s3):
        assert expand_s3_location("s3://bucket/dir/file.h5ad") == [
            "s3://bucket/dir/file.h5ad"
        ]


def test_expand_s3_location_prefix():
    s3 = _make_s3_mock(["prefix/file.h5ad", "prefix/meta.csv"])
    with patch("all_data_cli.utils.s3._make_s3_client", return_value=s3):
        uris = expand_s3_location("s3://bucket/prefix/")
    assert uris == ["s3://bucket/prefix/file.h5ad", "s3://bucket/prefix/meta.csv"]


def test_s3_url_to_local_path_preserves_key_structure(tmp_path):
    result = s3_url_to_local_path("s3://bucket/dir1/dir2/file.h5ad", str(tmp_path))
    assert result == tmp_path / "dir1" / "dir2" / "file.h5ad"


def test_download_s3_object_success(tmp_path):
    s3 = MagicMock()
    s3.head_object.return_value = {"ContentLength": 0}
    with (
        patch("all_data_cli.utils.s3._make_s3_client", return_value=s3),
        patch("all_data_cli.utils.s3.S3Transfer") as mock_transfer,
    ):
        result = download_s3_object("s3://bucket/prefix/file.h5ad", str(tmp_path), "ds")
    assert result is None
    mock_transfer.return_value.download_file.assert_called_once_with(
        "bucket",
        "prefix/file.h5ad",
        str(tmp_path / "prefix" / "file.h5ad"),
        callback=ANY,
    )


def test_download_s3_object_records_failure(tmp_path):
    s3 = MagicMock()
    s3.head_object.side_effect = OSError("Access denied")
    with patch("all_data_cli.utils.s3._make_s3_client", return_value=s3):
        result = download_s3_object(
            "s3://bucket/prefix/file.h5ad", str(tmp_path), "My Dataset"
        )
    assert result is not None
    assert "file.h5ad" in result.url
    assert result.dataset_name == "My Dataset"
    assert "Access denied" in result.reason
