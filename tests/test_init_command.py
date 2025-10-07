from pathlib import Path

from click.testing import CliRunner

from starrocks_br.cli import cli
from .utils import write_cfg


def test_init_creates_metadata(monkeypatch, mocker, tmp_path: Path):
    cfg = write_cfg(tmp_path)

    # Patch Database class used by CLI to avoid real DB
    db_mock_cls = mocker.patch("starrocks_br.cli.Database")
    db_mock = db_mock_cls.return_value

    runner = CliRunner()
    result = runner.invoke(cli, ["init", "--config", str(cfg)])

    assert result.exit_code == 0

    # Ensure we tried to create database/table
    executed_sqls = [call.args[0] for call in db_mock.execute.call_args_list]
    assert any("CREATE DATABASE IF NOT EXISTS ops" in s for s in executed_sqls)
    assert any("CREATE TABLE IF NOT EXISTS ops.backup_history" in s for s in executed_sqls)
