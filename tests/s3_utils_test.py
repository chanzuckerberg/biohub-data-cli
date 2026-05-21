import threading
from pathlib import Path
from typing import NamedTuple
from unittest.mock import ANY, MagicMock, patch

import pytest
from botocore.exceptions import BotoCoreError, ClientError

from biohub_data_cli.utils.s3 import (
    download_s3_object,
    expand_s3_location,
    resolve_s3_uris,
    s3_url_to_local_path,
)


def _ignore_bytes(_: int) -> None: ...


# Every mocked S3 object in this test file reports this fake size; expected
# tuples use it so we can verify expand_s3_location wires Size through.
_MOCK_OBJECT_SIZE = 100


def _make_s3_mock(
    mocked_pages_under_dir: list[list[str]] | None = None,
    head_exists: bool = False,
    paginate_side_effect: Exception | None = None,
    head_side_effect: Exception | None = None,
) -> MagicMock:
    """S3 client mock. `mocked_pages_under_dir` is the paginator output: a list
    of pages, each a list of object keys. Every listed object gets the same
    fake `Size` = `_MOCK_OBJECT_SIZE`. `head_object` succeeds with that same
    size if `head_exists` else raises 404. Either side effect can be supplied
    to override the configured return."""
    s3 = MagicMock()
    paginator = MagicMock()
    if paginate_side_effect is not None:
        paginator.paginate.side_effect = paginate_side_effect
    else:
        paginator.paginate.return_value = [
            {"Contents": [{"Key": k, "Size": _MOCK_OBJECT_SIZE} for k in page]}
            for page in (mocked_pages_under_dir or [[]])
        ]
    s3.get_paginator.return_value = paginator
    if head_side_effect is not None:
        s3.head_object.side_effect = head_side_effect
    elif head_exists:
        s3.head_object.return_value = {"ContentLength": _MOCK_OBJECT_SIZE}
    else:
        s3.head_object.side_effect = ClientError(
            {"Error": {"Code": "404", "Message": "Not Found"}}, "HeadObject"
        )
    return s3


class ReturnCase(NamedTuple):
    id: str
    uri: str
    mocked_pages_under_dir: list[list[str]]
    head_exists: bool
    expected_uris: list[str]  # tuples built from these + _MOCK_OBJECT_SIZE
    list_paginator_called_with: str
    head_called_with: str | None  # None means HEAD must not be called


def test_expand_s3_location_returns():
    cases = [
        ReturnCase(
            id="single_file_head_wins",
            uri="s3://bucket/dir/file.h5ad",
            mocked_pages_under_dir=[[]],
            head_exists=True,
            expected_uris=["s3://bucket/dir/file.h5ad"],
            list_paginator_called_with="dir/file.h5ad/",
            head_called_with="dir/file.h5ad",
        ),
        ReturnCase(
            id="folder_listing",
            uri="s3://bucket/prefix/",
            mocked_pages_under_dir=[["prefix/file.h5ad", "prefix/meta.csv"]],
            head_exists=False,
            expected_uris=[
                "s3://bucket/prefix/file.h5ad",
                "s3://bucket/prefix/meta.csv",
            ],
            list_paginator_called_with="prefix/",
            head_called_with=None,
        ),
        ReturnCase(
            id="folder_wins_over_bare",
            uri="s3://bucket/prefix/file.h5ad",
            mocked_pages_under_dir=[["prefix/file.h5ad/sub", "prefix/file.h5ad/sub2"]],
            head_exists=False,
            expected_uris=[
                "s3://bucket/prefix/file.h5ad/sub",
                "s3://bucket/prefix/file.h5ad/sub2",
            ],
            list_paginator_called_with="prefix/file.h5ad/",
            head_called_with=None,
        ),
        ReturnCase(
            id="skips_directory_markers",
            uri="s3://bucket/prefix/",
            mocked_pages_under_dir=[["prefix/", "prefix/file.h5ad", "prefix/sub/"]],
            head_exists=False,
            expected_uris=["s3://bucket/prefix/file.h5ad"],
            list_paginator_called_with="prefix/",
            head_called_with=None,
        ),
        ReturnCase(
            id="multiple_pages",
            uri="s3://bucket/prefix/",
            mocked_pages_under_dir=[["prefix/a", "prefix/b"], ["prefix/c"]],
            head_exists=False,
            expected_uris=[
                "s3://bucket/prefix/a",
                "s3://bucket/prefix/b",
                "s3://bucket/prefix/c",
            ],
            list_paginator_called_with="prefix/",
            head_called_with=None,
        ),
    ]
    for case in cases:
        s3 = _make_s3_mock(
            mocked_pages_under_dir=case.mocked_pages_under_dir,
            head_exists=case.head_exists,
        )
        with patch("biohub_data_cli.utils.s3._make_s3_client", return_value=s3):
            result = expand_s3_location(case.uri)
        expected = [(uri, _MOCK_OBJECT_SIZE) for uri in case.expected_uris]
        assert result == expected, f"[{case.id}] expected {expected}, got {result}"
        s3.get_paginator.assert_called_once_with("list_objects_v2")
        s3.get_paginator.return_value.paginate.assert_called_once_with(
            Bucket="bucket", Prefix=case.list_paginator_called_with
        )
        if case.head_called_with is None:
            assert not s3.head_object.called, f"[{case.id}] HEAD should not be called"
        else:
            s3.head_object.assert_called_once_with(
                Bucket="bucket", Key=case.head_called_with
            )


class RaisesCase(NamedTuple):
    id: str
    uri: str
    paginate_side_effect: Exception | None
    head_side_effect: Exception | None
    head_exists: bool
    expected_match: str
    head_should_be_called: bool


def test_expand_s3_location_raises():
    cases = [
        RaisesCase(
            id="head_404",
            uri="s3://bucket/key",
            paginate_side_effect=None,
            head_side_effect=ClientError({"Error": {"Code": "404"}}, "HeadObject"),
            head_exists=False,
            expected_match="No object found",
            head_should_be_called=True,
        ),
        RaisesCase(
            id="head_non_404",
            uri="s3://bucket/key",
            paginate_side_effect=None,
            head_side_effect=ClientError(
                {"Error": {"Code": "AccessDenied"}}, "HeadObject"
            ),
            head_exists=False,
            expected_match="Failed to resolve",
            head_should_be_called=True,
        ),
        RaisesCase(
            id="head_botocore_error",
            uri="s3://bucket/key",
            paginate_side_effect=None,
            head_side_effect=BotoCoreError(),
            head_exists=False,
            expected_match="Failed to resolve",
            head_should_be_called=True,
        ),
        RaisesCase(
            id="folder_is_empty",
            uri="s3://bucket/empty/",
            paginate_side_effect=None,
            head_side_effect=None,
            head_exists=True,
            expected_match="No objects found",
            head_should_be_called=False,
        ),
        RaisesCase(
            id="list_fails",
            uri="s3://bucket/prefix/",
            paginate_side_effect=ClientError(
                {"Error": {"Code": "AccessDenied"}}, "ListObjectsV2"
            ),
            head_side_effect=None,
            head_exists=False,
            expected_match="Failed to list S3 objects",
            head_should_be_called=False,
        ),
    ]
    for case in cases:
        s3 = _make_s3_mock(
            mocked_pages_under_dir=[[]],
            head_exists=case.head_exists,
            paginate_side_effect=case.paginate_side_effect,
            head_side_effect=case.head_side_effect,
        )
        with patch("biohub_data_cli.utils.s3._make_s3_client", return_value=s3):
            with pytest.raises(RuntimeError, match=case.expected_match):
                expand_s3_location(case.uri)
        if case.head_should_be_called:
            assert s3.head_object.called, f"[{case.id}] HEAD should be called"
        else:
            assert not s3.head_object.called, f"[{case.id}] HEAD should not be called"


def test_s3_url_to_local_path_preserves_key_structure(tmp_path):
    result = s3_url_to_local_path("s3://bucket/dir1/dir2/file.h5ad", tmp_path)
    assert result == tmp_path / "dir1" / "dir2" / "file.h5ad"


def test_download_s3_object_success(tmp_path):
    s3 = MagicMock()
    s3.head_object.return_value = {"ContentLength": 0}

    def fake_download(bucket, key, dest, callback):
        Path(dest).write_bytes(b"")

    with (
        patch("biohub_data_cli.utils.s3._make_s3_client", return_value=s3),
        patch("biohub_data_cli.utils.s3.S3Transfer") as mock_transfer,
    ):
        mock_transfer.return_value.download_file.side_effect = fake_download
        result = download_s3_object(
            "s3://bucket/prefix/file.h5ad", tmp_path, "coll", "ds", _ignore_bytes
        )
    assert result is None
    assert (tmp_path / "prefix" / "file.h5ad").exists()
    mock_transfer.return_value.download_file.assert_called_once_with(
        "bucket",
        "prefix/file.h5ad",
        str(tmp_path / "prefix" / "file.h5ad.part"),
        callback=ANY,
    )


def test_download_s3_object_records_failure(tmp_path):
    with patch("biohub_data_cli.utils.s3.S3Transfer") as mock_transfer:
        mock_transfer.return_value.download_file.side_effect = OSError("Access denied")
        result = download_s3_object(
            "s3://bucket/prefix/file.h5ad",
            tmp_path,
            "my-coll",
            "my-ds",
            _ignore_bytes,
        )
    assert result is not None
    assert "file.h5ad" in result.url
    assert result.collection_slug == "my-coll"
    assert result.dataset_slug == "my-ds"
    assert "Access denied" in result.reason


def test_download_s3_object_cancels_via_progress_callback_and_cleans_part_file(
    tmp_path,
):
    """When the cancel event is set, the per-chunk callback raises, S3Transfer
    propagates it, the existing handler unlinks the .part file."""
    cancel = threading.Event()
    cancel.set()

    def fake_download(bucket, key, dest, callback):
        Path(dest).write_bytes(b"partial")  # simulate mid-stream write
        callback(7)  # this should raise because cancel is set

    with (
        patch("biohub_data_cli.utils.s3._make_s3_client", return_value=MagicMock()),
        patch("biohub_data_cli.utils.s3.S3Transfer") as mock_transfer,
    ):
        mock_transfer.return_value.download_file.side_effect = fake_download
        result = download_s3_object(
            "s3://bucket/prefix/file.h5ad",
            tmp_path,
            "coll",
            "ds",
            _ignore_bytes,
            cancel,
        )

    assert result is not None
    assert "cancelled" in result.reason
    assert not (tmp_path / "prefix" / "file.h5ad").exists()
    assert not (tmp_path / "prefix" / "file.h5ad.part").exists()


# ── resolve_s3_uris ─────────────────────────────────────────────────────────


def test_resolve_s3_uris_returns_expanded_objects_and_no_failures():
    with patch(
        "biohub_data_cli.utils.s3.expand_s3_location",
        side_effect=lambda uri: [(f"{uri}/a", 100), (f"{uri}/b", 200)],
    ):
        objects, failures = resolve_s3_uris("coll", "ds", ["s3://b/x", "s3://b/y"])

    assert failures == []
    assert objects == [
        ("s3://b/x/a", 100),
        ("s3://b/x/b", 200),
        ("s3://b/y/a", 100),
        ("s3://b/y/b", 200),
    ]


def test_resolve_s3_uris_attributes_listing_failures_and_continues():
    """A failing URI becomes a DownloadFailure; remaining URIs still resolve."""

    def expand(uri):
        if uri == "s3://b/bad":
            raise RuntimeError("listing failed: access denied")
        return [(f"{uri}/file", 50)]

    with patch("biohub_data_cli.utils.s3.expand_s3_location", side_effect=expand):
        objects, failures = resolve_s3_uris(
            "coll-x", "matrix-z", ["s3://b/good", "s3://b/bad"]
        )

    assert objects == [("s3://b/good/file", 50)]
    assert len(failures) == 1
    assert failures[0].collection_slug == "coll-x"
    assert failures[0].dataset_slug == "matrix-z"
    assert failures[0].url == "s3://b/bad"
    assert "listing failed" in failures[0].reason
