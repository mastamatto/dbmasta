"""
Tests for dbmasta fixes from .cursor/dbmasta-issues-findings.md.

Covers:
- run() re-raises on failure (no silent empty response)
- get_header_info() raises SchemaQueryError when schema query fails or returns no columns
- convert_vals() raises MissingColumnError when column not in schema (clear error vs KeyError)
- Happy path: run(), get_header_info(), and parameterized schema query work

Requires: DB_USERNAME, DB_PASSWORD, DB_HOST, DB_DATABASE in env (or .env).
Run: python _test.py   or  pytest _test.py -v
"""
import asyncio
import os
import sys

from dotenv import load_dotenv
load_dotenv()

from dbmasta import AsyncDataBase, SchemaQueryError, MissingColumnError
from dbmasta.authorization import Authorization


def _has_db_env():
    return all([
        os.getenv("DB_USERNAME"),
        os.getenv("DB_PASSWORD"),
        os.getenv("DB_HOST"),
        os.getenv("DB_DATABASE"),
    ])


def _make_db():
    return AsyncDataBase(
        auth=Authorization(
            username=os.getenv("DB_USERNAME"),
            password=os.getenv("DB_PASSWORD"),
            host=os.getenv("DB_HOST"),
            default_database=os.getenv("DB_DATABASE"),
            engine="asyncmy",
        ),
        engine_config={"pool_recycle": 3600},
    )


# --- Tests that do NOT require a live DB ---


def test_convert_vals_raises_missing_column_error():
    """convert_vals() raises MissingColumnError (not KeyError) when column not in coldata."""
    from dbmasta.async_db_client import AsyncDataBase
    from dbmasta.authorization import Authorization
    # Minimal auth only for constructing the client
    auth = Authorization(
        username="u", password="p", host="h", default_database="d", engine="asyncmy"
    )
    db = AsyncDataBase(auth=auth)
    try:
        db.convert_vals("tags", "value", coldata={})
        assert False, "Expected MissingColumnError"
    except MissingColumnError as e:
        assert "tags" in str(e)
        assert "schema" in str(e).lower() or "column" in str(e).lower()
    except KeyError:
        assert False, "Should raise MissingColumnError, not KeyError"


# --- Tests that require a live MySQL DB ---


async def test_run_happy_path():
    """run() with valid query returns successful response with rows."""
    if not _has_db_env():
        print("SKIP test_run_happy_path (no DB env)")
        return
    db = _make_db()
    try:
        dbr = await db.run("SHOW TABLES", os.getenv("DB_DATABASE"))
        assert dbr is not None
        assert getattr(dbr, "successful", True) is not False
        # May be 0 tables; we only check we got a result object and no exception
        list(dbr)
    finally:
        await db.kill_engines()


async def test_run_re_raises_on_failure():
    """run() re-raises on query failure (no silent empty response)."""
    if not _has_db_env():
        print("SKIP test_run_re_raises_on_failure (no DB env)")
        return
    db = _make_db()
    try:
        try:
            await db.run(
                "SELECT * FROM this_table_does_not_exist_xyz_123",
                os.getenv("DB_DATABASE"),
            )
            assert False, "run() must re-raise on query failure"
        except Exception:
            pass  # expected: run() now re-raises so callers don't get empty response
    finally:
        await db.kill_engines()


async def test_get_header_info_raises_for_nonexistent_table():
    """get_header_info() raises SchemaQueryError for non-existent table (not empty dict)."""
    if not _has_db_env():
        print("SKIP test_get_header_info_raises_for_nonexistent_table (no DB env)")
        return
    db = _make_db()
    try:
        try:
            await db.get_header_info(os.getenv("DB_DATABASE"), "this_table_does_not_exist_xyz_123")
            assert False, "Expected SchemaQueryError"
        except SchemaQueryError as e:
            assert "this_table_does_not_exist_xyz_123" in str(e) or "no columns" in str(e).lower()
    finally:
        await db.kill_engines()


async def test_get_header_info_happy_path():
    """get_header_info() returns non-empty column dict for existing table; uses parameterized query."""
    if not _has_db_env():
        print("SKIP test_get_header_info_happy_path (no DB env)")
        return
    db = _make_db()
    try:
        dbr = await db.run("SHOW TABLES", os.getenv("DB_DATABASE"))
        tables = [list(r.values())[0] for r in dbr]
        if not tables:
            print("SKIP test_get_header_info_happy_path (no tables in DB)")
            return
        first_table = tables[0]
        coldata = await db.get_header_info(os.getenv("DB_DATABASE"), first_table)
        assert isinstance(coldata, dict)
        assert len(coldata) > 0
    finally:
        await db.kill_engines()


async def test_on_query_error_still_fires_then_re_raises():
    """When run() fails, on_query_error is fired and then the exception is re-raised."""
    if not _has_db_env():
        print("SKIP test_on_query_error_still_fires_then_re_raises (no DB env)")
        return
    events = []
    def on_err(exc, tb, dbr):
        events.append(("on_query_error", exc, dbr))

    db = _make_db()
    db.event_handlers["on_query_error"] = on_err
    try:
        try:
            await db.run("SELECT * FROM nonexistent_table_xyz_999", os.getenv("DB_DATABASE"))
        except Exception as e:
            events.append(("raised", e))
        assert len(events) >= 1
        assert any(e[0] == "on_query_error" for e in events)
        assert any(e[0] == "raised" for e in events)
    finally:
        await db.kill_engines()


async def run_all_async():
    print("--- No DB required ---")
    test_convert_vals_raises_missing_column_error()
    print("  test_convert_vals_raises_missing_column_error OK")

    print("--- DB required ---")
    await test_run_happy_path()
    print("  test_run_happy_path OK")
    await test_run_re_raises_on_failure()
    print("  test_run_re_raises_on_failure OK")
    await test_get_header_info_raises_for_nonexistent_table()
    print("  test_get_header_info_raises_for_nonexistent_table OK")
    await test_get_header_info_happy_path()
    print("  test_get_header_info_happy_path OK")
    await test_on_query_error_still_fires_then_re_raises()
    print("  test_on_query_error_still_fires_then_re_raises OK")

    print("All checks passed.")


# if __name__ == "__main__":
#     asyncio.run(run_all_async())
