"""
Tests that do not require a live database: exceptions, validation, convert_vals.
"""
import pytest
from dbmasta import MissingColumnError, SchemaQueryError
from dbmasta.async_db_client.base import AsyncDataBase
from dbmasta.db_client.base import DataBase
from dbmasta.authorization import Authorization


@pytest.fixture
def fake_auth():
    return Authorization(
        username="u", password="p", host="h", default_database="d", engine="pymysql"
    )


class TestConvertValsRaisesMissingColumnError:
    """convert_vals must raise MissingColumnError (not KeyError) when column not in coldata."""

    def test_async_db(self, fake_auth):
        db = AsyncDataBase(auth=fake_auth)
        with pytest.raises(MissingColumnError) as exc_info:
            db.convert_vals("tags", "value", coldata={})
        assert "tags" in str(exc_info.value)
        assert "schema" in str(exc_info.value).lower() or "column" in str(exc_info.value).lower()

    def test_sync_db(self, fake_auth):
        db = DataBase(auth=fake_auth)
        with pytest.raises(MissingColumnError) as exc_info:
            db.convert_vals("tags", "value", coldata={})
        assert "tags" in str(exc_info.value)


class TestSchemaQueryError:
    """SchemaQueryError is raised when schema query fails or returns no columns."""

    def test_exception_message(self):
        e = SchemaQueryError("INFORMATION_SCHEMA query failed or returned no columns for 'db'.'t'")
        assert "db" in str(e) and "t" in str(e)


class TestExceptionsExport:
    """Public exceptions are importable from dbmasta."""

    def test_import(self):
        from dbmasta import SchemaQueryError, MissingColumnError
        assert SchemaQueryError is not None
        assert MissingColumnError is not None
