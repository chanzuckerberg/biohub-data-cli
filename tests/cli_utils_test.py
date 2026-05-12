import pytest

from all_data_cli.utils.cli import safe_join


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
