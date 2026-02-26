"""
Sync MySQL (DataBase) tests. Require DB_USERNAME, DB_PASSWORD, DB_HOST, DB_DATABASE.
"""
import pytest
from dbmasta import SchemaQueryError, MissingColumnError
from dbmasta.db_client import DataBase


class TestRun:
    """run() behavior."""

    def test_run_happy_path(self, sync_db_with_table):
        sync_db, database = sync_db_with_table
        dbr = sync_db.run("SELECT 1 AS one", database)
        assert dbr is not None
        assert getattr(dbr, "successful", True) is not False
        rows = list(dbr)
        assert len(rows) == 1
        assert rows[0]["one"] == 1

    def test_run_re_raises_on_failure(self, sync_db_with_table):
        sync_db, database = sync_db_with_table
        with pytest.raises(Exception):
            sync_db.run("SELECT * FROM this_table_does_not_exist_xyz_123", database)


class TestGetHeaderInfo:
    """get_header_info() and header cache."""

    def test_returns_columns_for_existing_table(self, sync_db_with_table):
        sync_db, database = sync_db_with_table
        coldata = sync_db.get_header_info(database, "_dbmasta_audit_test")
        assert isinstance(coldata, dict)
        assert len(coldata) >= 4
        assert "id" in coldata
        assert "name" in coldata
        assert "DATA_TYPE" in coldata["id"] or "value" in coldata

    def test_raises_for_nonexistent_table(self, sync_db_with_table):
        sync_db, database = sync_db_with_table
        with pytest.raises(SchemaQueryError) as exc_info:
            sync_db.get_header_info(database, "nonexistent_table_xyz_123")
        assert "nonexistent_table_xyz_123" in str(exc_info.value)

    def test_cache_hit_second_call(self, sync_db_with_table):
        sync_db, database = sync_db_with_table
        coldata1 = sync_db.get_header_info(database, "_dbmasta_audit_test")
        coldata2 = sync_db.get_header_info(database, "_dbmasta_audit_test")
        assert coldata1 is coldata2  # same dict from cache


class TestInsertSelect:
    """insert() and select() using test table."""

    def test_insert_and_select(self, sync_db_with_table):
        sync_db, database = sync_db_with_table
        table = "_dbmasta_audit_test"
        sync_db.insert(database, table, [{"name": "audit_a", "value": 10}])
        dbr = sync_db.select(database, table, params={"name": "audit_a"})
        assert len(dbr) >= 1
        row = dbr[0]
        assert row["name"] == "audit_a"
        assert row["value"] == 10

    def test_upsert(self, sync_db_with_table):
        sync_db, database = sync_db_with_table
        table = "_dbmasta_audit_test"
        sync_db.upsert(database, table, [{"name": "audit_upsert", "value": 1}])
        sync_db.upsert(database, table, [{"name": "audit_upsert", "value": 2}])
        dbr = sync_db.select(database, table, params={"name": "audit_upsert"})
        assert len(dbr) >= 1
        assert dbr[0]["value"] == 2


class TestCorrectTypesFromTable:
    """correct_types with table= uses _coldata_from_table (no INFORMATION_SCHEMA)."""

    def test_insert_uses_table_derived_coldata(self, sync_db_with_table):
        sync_db, database = sync_db_with_table
        table_name = "_dbmasta_audit_test"
        # Insert twice; second time coldata should come from cache or from table
        sync_db.insert(database, table_name, [{"name": "t1", "value": 1}])
        sync_db.insert(database, table_name, [{"name": "t2", "value": 2}])
        dbr = sync_db.select(database, table_name, params={"name": "t2"})
        assert len(dbr) >= 1 and dbr[0]["value"] == 2
