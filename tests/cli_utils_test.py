import threading
from unittest.mock import patch

import pytest
from rich.console import Console, Group

from data_cli.models import DownloadFailure
from data_cli.utils.cli import (
    DownloadDisplay,
    safe_join,
)


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


def test_failure_fields_with_rich_markup_characters_render_literally():
    """Brackets in error messages (e.g. '[Errno 13]') must not be parsed as rich markup."""
    d = DownloadDisplay()
    d.record_failure(
        _failure(
            coll="coll-[a]",
            ds="ds-[1]",
            url="s3://bucket/key?x=[y]",
            reason="boto error [Errno 13] Permission denied",
        )
    )
    coll_branch = d._collection_branches["coll-[a]"]
    ds_branch = d._dataset_branches[("coll-[a]", "ds-[1]")]
    leaf_label = str(ds_branch.children[0].label)

    # Render each label through a Console with a recorder to assert the bracket
    # content survives as literal text instead of being eaten as markup.
    recorder = Console(record=True, width=200, file=None)
    recorder.print(coll_branch.label)
    recorder.print(ds_branch.label)
    recorder.print(leaf_label)
    rendered = recorder.export_text()
    assert "coll-[a]" in rendered
    assert "ds-[1]" in rendered
    assert "s3://bucket/key?x=[y]" in rendered
    assert "[Errno 13]" in rendered


# ── DownloadDisplay.advance_task / DownloadDisplay.grow_task_total ──────────


def test_advance_task_bumps_completed():
    d = DownloadDisplay()
    task_id = d.progress.add_task("t", total=100)
    d.advance_task(task_id, 30)
    d.advance_task(task_id, 40)
    task = next(t for t in d.progress.tasks if t.id == task_id)
    assert task.completed == 70


def test_grow_task_total_adds_to_existing_total():
    """Used when an HTTP worker learns Content-Length after the task is created."""
    d = DownloadDisplay()
    task_id = d.progress.add_task("t", total=1000)  # seeded from S3 sizes
    d.grow_task_total(task_id, 250)  # HTTP file is 250 bytes
    task = next(t for t in d.progress.tasks if t.id == task_id)
    assert task.total == 1250


def test_grow_task_total_starts_from_none():
    """If the task was created with total=None (no known size), grow from 0."""
    d = DownloadDisplay()
    task_id = d.progress.add_task("t", total=None)
    d.grow_task_total(task_id, 500)
    task = next(t for t in d.progress.tasks if t.id == task_id)
    assert task.total == 500


def test_grow_task_total_is_thread_safe_under_concurrent_workers():
    """Many HTTP workers reporting Content-Length at once must not lose updates."""
    d = DownloadDisplay()
    task_id = d.progress.add_task("t", total=0)
    n_threads = 50
    per_thread = 1000
    start = threading.Event()

    def worker():
        start.wait()
        d.grow_task_total(task_id, per_thread)

    threads = [threading.Thread(target=worker) for _ in range(n_threads)]
    for t in threads:
        t.start()
    start.set()
    for t in threads:
        t.join()

    task = next(t for t in d.progress.tasks if t.id == task_id)
    assert task.total == n_threads * per_thread
