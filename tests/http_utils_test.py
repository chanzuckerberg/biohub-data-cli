from unittest.mock import MagicMock, patch

from biohub_data_cli.utils.http import download_http, http_url_to_local_path


def _ignore_bytes(_: int) -> None: ...


def _ignore_size(_: int) -> None: ...


def test_http_url_to_local_path(tmp_path):
    result = http_url_to_local_path("https://example.com/dir1/dir2/data.h5ad", tmp_path)
    assert result == tmp_path / "data.h5ad"


def test_http_url_to_local_path_url_decodes_filename(tmp_path):
    result = http_url_to_local_path(
        "https://example.com/dir/file%20name.h5ad", tmp_path
    )
    assert result == tmp_path / "file name.h5ad"


def test_download_http_success(tmp_path):
    mock_response = MagicMock()
    mock_response.headers = {"Content-Length": "4"}
    mock_response.iter_content.return_value = [b"data"]
    mock_response.__enter__ = lambda s: s
    mock_response.__exit__ = MagicMock(return_value=False)

    with patch("biohub_data_cli.utils.http.requests.get", return_value=mock_response):
        result = download_http(
            "https://example.com/file.h5ad",
            tmp_path,
            "coll",
            "ds",
            _ignore_bytes,
            _ignore_size,
        )

    assert result is None
    assert (tmp_path / "file.h5ad").read_bytes() == b"data"


def test_download_http_records_failure(tmp_path):
    with patch("biohub_data_cli.utils.http.requests.get", side_effect=OSError("timeout")):
        result = download_http(
            "https://example.com/file.h5ad",
            tmp_path,
            "my-coll",
            "my-ds",
            _ignore_bytes,
            _ignore_size,
        )

    assert result is not None
    assert result.collection_slug == "my-coll"
    assert result.dataset_slug == "my-ds"
    assert "timeout" in result.reason


def test_download_http_calls_on_size_known_from_content_length(tmp_path):
    """Worker reports the file size to the orchestrator via on_size_known."""
    mock_response = MagicMock()
    mock_response.headers = {"Content-Length": "1024"}
    mock_response.iter_content.return_value = [b"data"]
    mock_response.__enter__ = lambda s: s
    mock_response.__exit__ = MagicMock(return_value=False)

    sizes_reported = []
    with patch("biohub_data_cli.utils.http.requests.get", return_value=mock_response):
        download_http(
            "https://example.com/file.h5ad",
            tmp_path,
            "coll",
            "ds",
            _ignore_bytes,
            sizes_reported.append,
        )

    assert sizes_reported == [1024]


def test_download_http_skips_on_size_known_without_content_length(tmp_path):
    """Servers omitting Content-Length (chunked transfer) shouldn't crash us."""
    mock_response = MagicMock()
    mock_response.headers = {}
    mock_response.iter_content.return_value = [b"data"]
    mock_response.__enter__ = lambda s: s
    mock_response.__exit__ = MagicMock(return_value=False)

    sizes_reported = []
    with patch("biohub_data_cli.utils.http.requests.get", return_value=mock_response):
        download_http(
            "https://example.com/file.h5ad",
            tmp_path,
            "coll",
            "ds",
            _ignore_bytes,
            sizes_reported.append,
        )

    assert sizes_reported == []


def test_download_http_records_failure_for_unresolvable_url(tmp_path):
    result = download_http(
        "https://example.com/",
        tmp_path,
        "my-coll",
        "my-ds",
        _ignore_bytes,
        _ignore_size,
    )

    assert result is not None
    assert result.collection_slug == "my-coll"
    assert result.dataset_slug == "my-ds"
    assert result.url == "https://example.com/"
    assert "filename" in result.reason
