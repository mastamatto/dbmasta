"""Verify all new modules and features import cleanly."""

def test_all_imports():
    from dbmasta import DataBase, AsyncDataBase, DataBaseResponse, AsyncDataBaseResponse
    from dbmasta import SchemaQueryError, MissingColumnError
    from dbmasta.pg_client import DataBase as PgDataBase
    from dbmasta.pg_async_client import AsyncDataBase as PgAsyncDataBase
    from dbmasta.errors import is_fatal_connection_error, is_fatal_mysql_connection_error, is_fatal_pg_connection_error
    from dbmasta.retry import sync_retry, async_retry, is_transient
    from dbmasta.transaction import SyncTransaction, AsyncTransaction

    # Context managers
    assert hasattr(DataBase, '__enter__')
    assert hasattr(DataBase, '__exit__')
    assert hasattr(AsyncDataBase, '__aenter__')
    assert hasattr(AsyncDataBase, '__aexit__')
    assert hasattr(PgDataBase, '__enter__')
    assert hasattr(PgAsyncDataBase, '__aenter__')

    # Transaction
    assert hasattr(DataBase, 'transaction')
    assert hasattr(AsyncDataBase, 'transaction')
    assert hasattr(PgDataBase, 'transaction')
    assert hasattr(PgAsyncDataBase, 'transaction')

    # Join
    assert hasattr(DataBase, 'join_select')
    assert hasattr(AsyncDataBase, 'join_select')
    assert hasattr(PgDataBase, 'join_select')
    assert hasattr(PgAsyncDataBase, 'join_select')

    # Engine management
    assert hasattr(DataBase, 'kill_engines')
    assert hasattr(AsyncDataBase, 'kill_engines')
    assert hasattr(PgDataBase, 'kill_engines')
    assert hasattr(PgAsyncDataBase, 'kill_engines')

    # Engine manager
    assert hasattr(DataBase, 'engine_manager') is False  # class level check - it's instance level
    # but the __init__ creates it, so just check it compiles


def test_error_detection():
    from dbmasta.errors import is_fatal_connection_error
    # MySQL errors
    assert is_fatal_connection_error(Exception("Lost connection to MySQL server"), "mysql")
    assert is_fatal_connection_error(Exception("Packet sequence number wrong"), "mysql")
    assert not is_fatal_connection_error(Exception("syntax error"), "mysql")
    # PG errors
    assert is_fatal_connection_error(Exception("connection is closed"), "postgresql")
    assert is_fatal_connection_error(Exception("server closed the connection unexpectedly"), "postgresql")
    assert not is_fatal_connection_error(Exception("syntax error"), "postgresql")


def test_retry_transient():
    from dbmasta.retry import is_transient
    assert is_transient(Exception("Lost connection to server"))
    assert is_transient(Exception("Connection refused"))
    assert is_transient(Exception("Too many connections"))
    assert not is_transient(Exception("syntax error near 'SELECT'"))
