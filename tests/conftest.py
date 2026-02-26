"""
Pytest fixtures for dbmasta tests.

Requires MySQL available and env vars set (or .env):
  DB_USERNAME, DB_PASSWORD, DB_HOST, DB_DATABASE

Tests that need a live DB are skipped when these are not set.
A temporary table _dbmasta_audit_test is created for DB tests and dropped in teardown.
"""
import os
import pytest
from dotenv import load_dotenv

load_dotenv()

# Env keys for MySQL (same as _test.py)
DB_USER = os.getenv("DB_USERNAME") or os.getenv("dbmasta_username")
DB_PASS = os.getenv("DB_PASSWORD") or os.getenv("dbmasta_password")
DB_HOST = os.getenv("DB_HOST") or os.getenv("dbmasta_host")
DB_PORT = os.getenv("DB_PORT") or os.getenv("dbmasta_port", "3306")
DB_NAME = os.getenv("DB_DATABASE") or os.getenv("dbmasta_default")

TEST_TABLE = "_dbmasta_audit_test"
# Table for testing TEXT/MEDIUMTEXT/LONGTEXT/JSON truncation limits
TEST_TABLE_TEXT_LIMITS = "_dbmasta_audit_text_limits"


def _has_db_env():
    return all([DB_USER, DB_PASS, DB_HOST, DB_NAME])


@pytest.fixture(scope="session")
def db_env():
    """Session-scoped: (user, password, host, port, database) or None if not configured."""
    if not _has_db_env():
        return None
    return (DB_USER, DB_PASS, DB_HOST, int(DB_PORT), DB_NAME)


@pytest.fixture
def auth(db_env):
    """Authorization instance; skip test if no DB env."""
    if db_env is None:
        pytest.skip("DB_USERNAME, DB_PASSWORD, DB_HOST, DB_DATABASE not set")
    from dbmasta.authorization import Authorization
    user, password, host, port, database = db_env
    return Authorization(
        username=user,
        password=password,
        host=host,
        port=port,
        default_database=database,
        engine="pymysql",
    )


@pytest.fixture
def auth_async(db_env):
    """Authorization for async MySQL (asyncmy)."""
    if db_env is None:
        pytest.skip("DB env not set")
    from dbmasta.authorization import Authorization
    user, password, host, port, database = db_env
    return Authorization(
        username=user,
        password=password,
        host=host,
        port=port,
        default_database=database,
        engine="asyncmy",
    )


@pytest.fixture
def sync_db(auth):
    """Sync MySQL DataBase client."""
    from dbmasta.db_client import DataBase
    return DataBase(auth=auth)


@pytest.fixture
async def async_db(auth_async):
    """Async MySQL AsyncDataBase client. Caller must be in async test."""
    from dbmasta.async_db_client import AsyncDataBase
    db = AsyncDataBase(auth=auth_async, engine_config={"pool_recycle": 3600})
    yield db
    await db.kill_engines()


@pytest.fixture
def sync_db_with_table(sync_db, db_env):
    """Sync client with test table created; table is dropped after test."""
    _, _, _, _, database = db_env
    # Create table
    sync_db.run(
        f"""
        CREATE TABLE IF NOT EXISTS `{TEST_TABLE}` (
            id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(255) NOT NULL UNIQUE,
            value INT DEFAULT 0,
            created_at DATETIME(6) DEFAULT CURRENT_TIMESTAMP(6)
        )
        """,
        database,
    )
    yield sync_db, database
    # Teardown: drop table
    try:
        sync_db.run(f"DROP TABLE IF EXISTS `{TEST_TABLE}`", database)
    except Exception:
        pass


@pytest.fixture
async def async_db_with_table(async_db, db_env):
    """Async client with test table created; table dropped after test."""
    _, _, _, _, database = db_env
    await async_db.run(
        f"""
        CREATE TABLE IF NOT EXISTS `{TEST_TABLE}` (
            id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(255) NOT NULL UNIQUE,
            value INT DEFAULT 0,
            created_at DATETIME(6) DEFAULT CURRENT_TIMESTAMP(6)
        )
        """,
        database,
    )
    yield async_db, database
    try:
        await async_db.run(f"DROP TABLE IF EXISTS `{TEST_TABLE}`", database)
    except Exception:
        pass
    await async_db.kill_engines()


def _create_text_limits_table_sql():
    """SQL to create table with TINYTEXT, TEXT, MEDIUMTEXT, LONGTEXT and a json-like (LONGTEXT) column."""
    return f"""
        CREATE TABLE IF NOT EXISTS `{TEST_TABLE_TEXT_LIMITS}` (
            id INT AUTO_INCREMENT PRIMARY KEY,
            payload_json LONGTEXT COMMENT 'json-like payload',
            txt_tiny TINYTEXT,
            txt_text TEXT,
            txt_medium MEDIUMTEXT,
            txt_long LONGTEXT
        )
    """


@pytest.fixture
def sync_db_with_text_limits_table(sync_db, db_env):
    """Sync client with text-limits table (TINYTEXT, TEXT, MEDIUMTEXT, LONGTEXT, payload_json)."""
    _, _, _, _, database = db_env
    sync_db.run(_create_text_limits_table_sql(), database)
    yield sync_db, database
    try:
        sync_db.run(f"DROP TABLE IF EXISTS `{TEST_TABLE_TEXT_LIMITS}`", database)
    except Exception:
        pass


@pytest.fixture
async def async_db_with_text_limits_table(async_db, db_env):
    """Async client with text-limits table."""
    _, _, _, _, database = db_env
    await async_db.run(_create_text_limits_table_sql(), database)
    yield async_db, database
    try:
        await async_db.run(f"DROP TABLE IF EXISTS `{TEST_TABLE_TEXT_LIMITS}`", database)
    except Exception:
        pass
    await async_db.kill_engines()
