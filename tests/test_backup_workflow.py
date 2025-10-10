from pathlib import Path

import pytest
from click.testing import CliRunner

from starrocks_br.cli import cli
from .utils import write_cfg


@pytest.fixture()
def db_mock(mocker):
    # Patch Database where it is constructed (in CLI module)
    db_cls = mocker.patch("starrocks_br.cli.Database")
    db = db_cls.return_value
    return db


def test_should_run_full_backup_when_no_previous_successful_backup(tmp_path: Path, mocker, db_mock):
    """Runs a full backup when there is no FINISHED backup in history (README §3.b.3)."""
    cfg = write_cfg(tmp_path, ["db1.t1", "db2.t2"])

    # No previous successful backup
    db_mock.query.side_effect = [
        [(None,)],  # get_last_successful_backup()
        [("RUNNING", None)],
        [("FINISHED", "2025-10-06 10:00:00")],
    ]

    runner = CliRunner()
    result = runner.invoke(cli, ["backup", "--config", str(cfg)])

    assert result.exit_code == 0

    executed = [call.args[0] for call in db_mock.execute.call_args_list]
    # Insert RUNNING
    assert any("INSERT INTO ops.backup_history" in s and "RUNNING" in s for s in executed)
    # BACKUP DATABASE with proper syntax
    assert any("BACKUP DATABASE ops ON (db1.t1, db2.t2, ops.backup_history) TO test_repo" in s for s in executed)
    # Update FINISHED with backup_timestamp
    assert any("UPDATE ops.backup_history" in s and "FINISHED" in s for s in executed)


def test_should_run_incremental_when_previous_backup_exists(tmp_path: Path, mocker, db_mock):
    """Runs an incremental backup when there is a FINISHED backup, selecting changed partitions (README §3.b.4)."""
    cfg = write_cfg(tmp_path, ["db1.t1"])

    # Previous backup exists
    db_mock.query.side_effect = [
        [("2025-10-05 12:00:00",)],  # last_success
        [("p1",), ("p2",)],         # changed partitions since last backup
        [("RUNNING", None)],
        [("FINISHED", "2025-10-06 11:00:00")],
    ]

    runner = CliRunner()
    result = runner.invoke(cli, ["backup", "--config", str(cfg)])

    assert result.exit_code == 0

    executed = [call.args[0] for call in db_mock.execute.call_args_list]
    # BACKUP DATABASE with partition hint for incremental
    assert any("BACKUP DATABASE ops ON (db1.t1 PARTITION (p1, p2), ops.backup_history) TO test_repo" in s for s in executed)