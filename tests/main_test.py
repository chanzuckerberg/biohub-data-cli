import sys

import pytest
from click.testing import CliRunner

from biohub_data_cli import main


def test_biohub_data_is_the_cli_group() -> None:
    result = CliRunner().invoke(main.cli, ["--help"])
    assert result.exit_code == 0
    assert "download" in result.output


def test_ops_data_alias_warns_then_forwards(capsys, monkeypatch: pytest.MonkeyPatch) -> None:
    """`ops-data` is a deprecated alias: it prints a warning to stderr and then
    runs the same CLI group (here exercised via --help, which exits 0)."""
    monkeypatch.setattr(sys, "argv", ["ops-data", "--help"])
    with pytest.raises(SystemExit) as exc:
        main.ops_data()
    assert exc.value.code == 0
    captured = capsys.readouterr()
    assert "deprecated" in captured.err
    assert "biohub-data" in captured.err
    # forwarded to the real CLI: its help lists the download command
    assert "download" in captured.out
