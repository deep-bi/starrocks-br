from pathlib import Path

import pytest
import textwrap
from click.testing import CliRunner

from starrocks_bbr.cli import cli, main
from .utils import write_cfg


@pytest.fixture()
def sample_config(tmp_path: Path) -> Path:
    return write_cfg(tmp_path)


def test_cli_group_help():
    runner = CliRunner()
    result = runner.invoke(cli, ["--help"])  # should show commands
    assert result.exit_code == 0
    assert "init" in result.output and "backup" in result.output and "restore" in result.output and "list" in result.output


def test_init_requires_config():
    runner = CliRunner()
    result = runner.invoke(cli, ["init"])  # missing --config
    assert result.exit_code != 0
    assert "--config" in result.output


def test_init_with_config(sample_config: Path, mocker):
    # Mock Database to avoid real connections
    db_mock_cls = mocker.patch("starrocks_bbr.cli.Database")

    runner = CliRunner()
    result = runner.invoke(cli, ["init", "--config", str(sample_config)])
    assert result.exit_code == 0
    assert "init:" in result.output


def test_main_returns_exit_code_zero(sample_config: Path):
    # ensure main() wrapper returns 0 without picking up pytest args
    from starrocks_bbr.cli import main as entry

    assert entry([]) == 0
