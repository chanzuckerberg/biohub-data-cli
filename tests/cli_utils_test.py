from unittest.mock import patch

import pytest
from rich.console import Group

from all_data_cli.models import DownloadFailure
from all_data_cli.utils.cli import DownloadDisplay, safe_join


# ── safe_join ────────────────────────────────────────────────────────────────


def test_safe_join_nested_subdirectories(tmp_path):
    result = safe_join(tmp_path, "a", "b", "c.txt")
    assert result == (tmp_path / "a" / "b" / "c.txt").resolve()


def test_safe_join_single_filename(tmp_path):
    result = safe_join(tmp_path, "file.h5ad")
    assert result == (tmp_path / "file.h5ad").resolve()


def test_safe_join_rejects_parent_traversal(tmp_path):
    with pytest.raises(ValueError, match="escapes"):
        safe_join(tmp_path, "..", "etc", "passwd")


def test_safe_join_rejects_embedded_parent_traversal(tmp_path):
    with pytest.raises(ValueError, match="escapes"):
        safe_join(tmp_path, "a", "..", "..", "etc")


def test_safe_join_rejects_absolute_component(tmp_path):
    with pytest.raises(ValueError, match="escapes"):
        safe_join(tmp_path, "/etc/passwd")


# ── DownloadDisplay ──────────────────────────────────────────────────────────


def _failure(
    coll: str = "coll-a",
    ds: str = "ds-1",
    url: str = "https://example.com/x",
    reason: str = "boom",
) -> DownloadFailure:
    return DownloadFailure(
        collection_slug=coll, dataset_slug=ds, url=url, reason=reason
    )


def test_record_failure_appends_to_failures_list():
    d = DownloadDisplay()
    f1 = _failure(ds="ds-1")
    f2 = _failure(ds="ds-2")
    d.record_failure(f1)
    d.record_failure(f2)
    assert d.failures == [f1, f2]


def test_first_failure_swaps_live_renderable_to_include_tree():
    """Tree is hidden until the first failure; then live.update swaps to Group(progress, tree)."""
    d = DownloadDisplay()
    with patch.object(d._live, "update") as mock_update:
        d.record_failure(_failure())
        mock_update.assert_called_once()
        passed = mock_update.call_args.args[0]
        assert isinstance(passed, Group)
        # The new renderable bundles progress + failures tree.
        assert d.progress in passed.renderables
        assert d._tree in passed.renderables


def test_subsequent_failures_do_not_re_swap_live_renderable():
    """Only the FIRST failure triggers live.update; later failures just mutate the tree."""
    d = DownloadDisplay()
    with patch.object(d._live, "update") as mock_update:
        d.record_failure(_failure(ds="ds-1"))
        d.record_failure(_failure(ds="ds-2"))
        d.record_failure(_failure(ds="ds-3"))
        assert mock_update.call_count == 1


def test_failures_in_same_collection_share_a_branch():
    d = DownloadDisplay()
    d.record_failure(_failure(coll="coll-a", ds="ds-1"))
    d.record_failure(_failure(coll="coll-a", ds="ds-2"))
    # One collection branch, memoized.
    assert list(d._collection_branches) == ["coll-a"]


def test_failures_in_same_dataset_share_a_sub_branch():
    d = DownloadDisplay()
    d.record_failure(_failure(coll="coll-a", ds="ds-1", url="u1"))
    d.record_failure(_failure(coll="coll-a", ds="ds-1", url="u2"))
    # One dataset branch with two leaves (one per failed URL).
    assert list(d._dataset_branches) == [("coll-a", "ds-1")]
    ds_branch = d._dataset_branches[("coll-a", "ds-1")]
    assert len(ds_branch.children) == 2


def test_different_collections_get_separate_branches():
    d = DownloadDisplay()
    d.record_failure(_failure(coll="coll-a"))
    d.record_failure(_failure(coll="coll-b"))
    assert set(d._collection_branches) == {"coll-a", "coll-b"}


def test_leaf_label_contains_url_and_reason():
    d = DownloadDisplay()
    d.record_failure(_failure(url="s3://bucket/key", reason="403 Forbidden"))
    ds_branch = d._dataset_branches[("coll-a", "ds-1")]
    leaf_label = str(ds_branch.children[0].label)
    assert "s3://bucket/key" in leaf_label
    assert "403 Forbidden" in leaf_label
