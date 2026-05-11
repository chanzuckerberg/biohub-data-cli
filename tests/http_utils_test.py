from unittest.mock import MagicMock, patch

from utils.http import _http_url_to_local_path, download_http


def test_http_url_to_local_path(tmp_path):
    result = _http_url_to_local_path(
        "https://example.com/dir1/dir2/data.h5ad", str(tmp_path)
    )
    assert result == tmp_path / "data.h5ad"


def test_download_http_success(tmp_path):
    mock_response = MagicMock()
    mock_response.headers = {"Content-Length": "4"}
    mock_response.iter_content.return_value = [b"data"]
    mock_response.__enter__ = lambda s: s
    mock_response.__exit__ = MagicMock(return_value=False)

    with patch("utils.http.requests.get", return_value=mock_response):
        result = download_http("https://example.com/file.h5ad", str(tmp_path), "ds")

    assert result is None
    assert (tmp_path / "file.h5ad").read_bytes() == b"data"


def test_download_http_records_failure(tmp_path):
    with patch("utils.http.requests.get", side_effect=OSError("timeout")):
        result = download_http(
            "https://example.com/file.h5ad", str(tmp_path), "My Dataset"
        )

    assert result is not None
    assert result.dataset_name == "My Dataset"
    assert "timeout" in result.reason
