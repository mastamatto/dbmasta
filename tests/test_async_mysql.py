"""
Async MySQL (AsyncDataBase) tests. Require DB env vars.
"""
import pytest
from dbmasta import SchemaQueryError, MissingColumnError
from dbmasta.async_db_client import AsyncDataBase


class TestRun:
    """run() behavior."""

    async def test_run_happy_path(self, async_db_with_table):
        async_db, database = async_db_with_table
        dbr = await async_db.run("SELECT 1 AS one", database)
        assert dbr is not None
        assert getattr(dbr, "successful", True) is not False
        rows = list(dbr)
        assert len(rows) == 1
        assert rows[0]["one"] == 1

    async def test_run_re_raises_on_failure(self, async_db_with_table):
        async_db, database = async_db_with_table
        with pytest.raises(Exception):
            await async_db.run("SELECT * FROM this_table_does_not_exist_xyz_123", database)


class TestGetHeaderInfo:
    """get_header_info() and cache."""

    async def test_returns_columns_for_existing_table(self, async_db_with_table):
        async_db, database = async_db_with_table
        coldata = await async_db.get_header_info(database, "_dbmasta_audit_test")
        assert isinstance(coldata, dict)
        assert len(coldata) >= 4
        assert "id" in coldata
        assert "name" in coldata

    async def test_raises_for_nonexistent_table(self, async_db_with_table):
        async_db, database = async_db_with_table
        with pytest.raises(SchemaQueryError) as exc_info:
            await async_db.get_header_info(database, "nonexistent_table_xyz_123")
        assert "nonexistent_table_xyz_123" in str(exc_info.value)

    async def test_cache_hit_second_call(self, async_db_with_table):
        async_db, database = async_db_with_table
        coldata1 = await async_db.get_header_info(database, "_dbmasta_audit_test")
        coldata2 = await async_db.get_header_info(database, "_dbmasta_audit_test")
        assert coldata1 is coldata2


class TestInsertSelect:
    """insert() and select()."""

    async def test_insert_and_select(self, async_db_with_table):
        async_db, database = async_db_with_table
        table = "_dbmasta_audit_test"
        await async_db.insert(database, table, [{"name": "async_audit_a", "value": 20}])
        dbr = await async_db.select(database, table, params={"name": "async_audit_a"})
        assert len(dbr) >= 1
        assert dbr[0]["name"] == "async_audit_a"
        assert dbr[0]["value"] == 20

    async def test_upsert(self, async_db_with_table):
        async_db, database = async_db_with_table
        table = "_dbmasta_audit_test"
        await async_db.upsert(database, table, [{"name": "async_upsert", "value": 1}])
        await async_db.upsert(database, table, [{"name": "async_upsert", "value": 2}])
        dbr = await async_db.select(database, table, params={"name": "async_upsert"})
        assert len(dbr) >= 1
        assert dbr[0]["value"] == 2


class TestOnQueryErrorAndReRaise:
    """run() fires on_query_error then re-raises."""

    async def test_on_query_error_fires_then_raises(self, async_db_with_table):
        async_db, database = async_db_with_table
        events = []
        async_db.event_handlers["on_query_error"] = lambda exc, tb, dbr: events.append(("error", dbr))
        with pytest.raises(Exception):
            await async_db.run("SELECT * FROM nonexistent_xyz_999", database)
        assert len(events) >= 1
        assert events[0][0] == "error"
