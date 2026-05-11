from unittest.mock import MagicMock, patch

from utils.s3 import _s3_url_to_local_path, download_s3_object, expand_s3_location


def _make_s3_mock(objects: list[str]) -> MagicMock:
    """S3 client mock whose paginator returns the given object URIs as a flat list."""
    s3 = MagicMock()
    page = {"Contents": [{"Key": k.split("/", 3)[-1]} for k in objects]}
    paginator = MagicMock()
    paginator.paginate.return_value = [page]
    s3.get_paginator.return_value = paginator
    return s3


def test_expand_s3_location_single_file():
    s3 = _make_s3_mock(["s3://bucket/dir/file.h5ad"])
    with patch("utils.s3._make_s3_client", return_value=s3):
        assert expand_s3_location("s3://bucket/dir/file.h5ad") == [
            "s3://bucket/dir/file.h5ad"
        ]


def test_expand_s3_location_prefix():
    s3 = _make_s3_mock(["s3://bucket/prefix/file.h5ad", "s3://bucket/prefix/meta.csv"])
    with patch("utils.s3._make_s3_client", return_value=s3):
        uris = expand_s3_location("s3://bucket/prefix/")
    assert len(uris) == 2
    assert "s3://bucket/prefix/file.h5ad" in uris
    assert "s3://bucket/prefix/meta.csv" in uris


def test_s3_url_to_local_path_preserves_key_structure(tmp_path):
    result = _s3_url_to_local_path("s3://bucket/dir1/dir2/file.h5ad", str(tmp_path))
    assert result == tmp_path / "dir1" / "dir2" / "file.h5ad"


def test_download_s3_object_success(tmp_path):
    s3 = MagicMock()
    s3.head_object.return_value = {"ContentLength": 0}
    with (
        patch("utils.s3._make_s3_client", return_value=s3),
        patch("utils.s3.S3Transfer") as mock_transfer,
    ):
        result = download_s3_object("s3://bucket/prefix/file.h5ad", str(tmp_path), "ds")
    assert result is None
    assert mock_transfer.return_value.download_file.call_count == 1


def test_download_s3_object_records_failure(tmp_path):
    s3 = MagicMock()
    s3.head_object.side_effect = RuntimeError("Access denied")
    with patch("utils.s3._make_s3_client", return_value=s3):
        result = download_s3_object(
            "s3://bucket/prefix/file.h5ad", str(tmp_path), "My Dataset"
        )
    assert result is not None
    assert "file.h5ad" in result.url
    assert result.dataset_name == "My Dataset"
    assert "Access denied" in result.reason
