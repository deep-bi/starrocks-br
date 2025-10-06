import contextlib
from typing import Iterator

import pytest


@pytest.fixture()
def mocked_mysql(mocker):
    # Simulate mysql.connector.connect and connection/cursor behavior
    mock_conn = mocker.MagicMock()
    mock_cursor = mocker.MagicMock()
    mock_conn.cursor.return_value = mock_cursor

    connect_mock = mocker.patch("starrocks_bbr.db.mysql.connector.connect", autospec=True)
    connect_mock.return_value = mock_conn
    return mock_conn, mock_cursor, connect_mock


def test_db_connect_and_execute(mocked_mysql):
    from starrocks_bbr.db import Database

    mock_conn, mock_cursor, _ = mocked_mysql

    db = Database(host="h", port=1, user="u", password="p", database="d")
    with db.connect() as conn:
        assert conn is mock_conn
        db.execute("CREATE DATABASE IF NOT EXISTS ops")
        mock_cursor.execute.assert_called()


def test_db_execute_many_and_query(mocked_mysql):
    from starrocks_bbr.db import Database

    mock_conn, mock_cursor, _ = mocked_mysql
    mock_cursor.fetchall.return_value = [(1,)]

    db = Database(host="h", port=1, user="u", password="p", database="d")
    db.executemany("INSERT INTO t VALUES (%s)", [(1,), (2,)])
    mock_cursor.executemany.assert_called()

    rows = db.query("SELECT 1")
    assert rows == [(1,)]


def test_db_context_manager_closes(mocked_mysql):
    from starrocks_bbr.db import Database

    mock_conn, mock_cursor, _ = mocked_mysql

    db = Database(host="h", port=1, user="u", password="p", database="d")
    with db.connect():
        pass
    # After context exit, commit and close should be called
    mock_conn.commit.assert_called()
    mock_cursor.close.assert_called()
    mock_conn.close.assert_called()
