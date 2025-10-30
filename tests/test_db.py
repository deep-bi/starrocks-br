import os
import pytest
from starrocks_br import db


@pytest.fixture
def setup_password_env(monkeypatch):
    """Setup STARROCKS_PASSWORD environment variable for testing."""
    monkeypatch.setenv('STARROCKS_PASSWORD', 'test_password')


def test_should_create_database_connection_object(setup_password_env):
    conn = db.StarRocksDB(
        host='127.0.0.1',
        port=9030,
        user='root',
        password=os.getenv('STARROCKS_PASSWORD'),
        database='test_db'
    )
    
    assert conn.host == '127.0.0.1'
    assert conn.port == 9030
    assert conn.user == 'root'
    assert conn.database == 'test_db'


def test_should_execute_sql_statement(mocker, setup_password_env):
    conn = db.StarRocksDB('localhost', 9030, 'root', os.getenv('STARROCKS_PASSWORD'), 'test_db')
    
    mock_connection = mocker.Mock()
    mock_cursor = mocker.Mock()
    mock_connection.cursor.return_value = mock_cursor
    
    mocker.patch('mysql.connector.connect', return_value=mock_connection)
    
    conn.execute("INSERT INTO test_table VALUES (1)")
    
    assert mock_cursor.execute.call_count == 1
    assert mock_connection.commit.call_count == 1


def test_should_query_and_return_results(mocker, setup_password_env):
    conn = db.StarRocksDB('localhost', 9030, 'root', os.getenv('STARROCKS_PASSWORD'), 'test_db')
    
    mock_connection = mocker.Mock()
    mock_cursor = mocker.Mock()
    mock_cursor.fetchall.return_value = [('row1',), ('row2',)]
    mock_connection.cursor.return_value = mock_cursor
    
    mocker.patch('mysql.connector.connect', return_value=mock_connection)
    
    results = conn.query("SELECT * FROM test_table")
    
    assert len(results) == 2
    assert results[0] == ('row1',)
    assert results[1] == ('row2',)


def test_should_support_context_manager(mocker, setup_password_env):
    mock_connection = mocker.Mock()
    mocker.patch('mysql.connector.connect', return_value=mock_connection)
    
    conn = db.StarRocksDB('localhost', 9030, 'root', os.getenv('STARROCKS_PASSWORD'), 'test_db')
    
    with conn as db_conn:
        assert db_conn is conn
    
    assert mock_connection.close.call_count == 1

