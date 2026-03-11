# dbmasta v0.1.28 — Changes

Release date: 2026-03-11

Post-audit release fixing critical bugs, improving performance, consolidating response classes, and adding new query methods. All existing method signatures and import paths are preserved.

---

## Version Bump

`0.1.27` -> `0.1.28`

Applied in `pyproject.toml`.

---

## Critical Bug Fixes

### URI port number ignored (`authorization.py`)
`uri()` built a `host` variable with port appended but then used `self.host` instead of `host` in the format string. Any non-default port was silently ignored. Fixed.

### `auto_raise_errors` AttributeError (`response/base.py`, `pg_async_client/response.py`)
`DataBaseResponseBase.__init__` accepted `auto_raise_errors` as a parameter but never stored it as an instance attribute. The `pg_async_client` response referenced `self.auto_raise_errors` in `_receive()`, causing `AttributeError` on any query error. Fixed by storing the attribute in the base class.

### VARCHAR.value silently discards JSON (`sql_types/sql_types.py`)
When `_value` was a dict/list, `json.dumps()` assigned to `v`, but then `clearEscapeChars()` re-read `self._value` (the original dict), overwriting the JSON string with Python repr. Fixed by using `else` branch for non-dict/list values.

### INT.value rejects negative numbers (`sql_types/sql_types.py`)
`str.isnumeric()` returns `False` for `"-5"`. Negative integers were silently converted to `0`. Replaced with try/except `int(round(float(...)))`.

### BOOL doesn't pass kwargs to super (`sql_types/sql_types.py`)
`super().__init__(value=value)` dropped `**kwargs`, losing `IS_NULLABLE` and other metadata. Fixed to `super().__init__(value=value, **kwargs)`.

### TIME._type was `datetime` instead of `time` (`sql_types/sql_types.py`)
`TIME._type = dt.datetime` meant passing a `datetime.time` value raised `InvalidDate`. Changed to `dt.time` and updated `.value` to handle both `time` and `datetime` inputs.

---

## Performance

### O(n^2) build_records fixed (`response/base.py`)
`data.pop(0)` on a list is O(n) per call, making `build_records` O(n^2). Replaced with simple iteration (`for x in data`).

### Header info cache TTL (`db_client`, `async_db_client`, `pg_client`, `pg_async_client`)
`_header_info_cache` was an unbounded dict that grew without eviction. Added 30-minute TTL using `time.monotonic()` timestamps.

---

## Response Class Consolidation

### PG response classes now inherit from base
`pg_client/response.py` and `pg_async_client/response.py` were standalone ~100-line classes duplicating `response/base.py`. They now inherit from `DataBaseResponseBase`, overriding only the PostgreSQL dialect for `raw_query` and the `_receive()` method. This gives PG responses all base features: `traceback`, `_handle_error()`, `auto_raise_errors`, `one_or_none` as property.

---

## Code Quality

### Replaced `print()` with `logging`
All library `print()` calls replaced with `logging.getLogger(__name__).warning()` in `response/base.py`, `async_db_client/base.py`, and `pg_async_client/base.py`.

### Table cache TTL consistency
All 4 `tables.py` files had `reset()` using 15-minute TTL vs `__init__` using 30 minutes. Normalized to 30 minutes everywhere.

### Python version requirement
Updated `requires-python` from `">=3.7"` to `">=3.9"` (code uses `list[dict]` type hints, `match` patterns, and other 3.9+ features).

### DB drivers moved to optional dependencies
Database drivers are no longer hard dependencies. Users install only what they need:
```
pip install dbmasta[mysql]        # pymysql
pip install dbmasta[mysql-async]  # aiomysql, asyncmy
pip install dbmasta[pg]           # psycopg-binary
pip install dbmasta[pg-async]     # asyncpg
pip install dbmasta[all]          # all drivers
```

---

## New Features

### `count()` method (all 4 clients)
Returns `int` count of rows matching optional `params` filter.
```python
n = db.count("mydb", "users", {"active": True})
```

### `exists()` method (all 4 clients)
Returns `bool` indicating whether any rows match `params`.
```python
if db.exists("mydb", "users", {"email": "a@b.com"}):
    ...
```

### `bulk_insert()` method (all 4 clients)
Splits large inserts into chunks to avoid packet size limits.
```python
results = db.bulk_insert("mydb", "users", big_list, chunk_size=1000)
```

---

## Files Summary

| File | Changes |
|------|---------|
| `pyproject.toml` | Version 0.1.28, Python >=3.9, optional deps |
| `dbmasta/authorization.py` | Fix port in URI |
| `dbmasta/response/base.py` | Store auto_raise_errors, O(n) build_records, logging |
| `dbmasta/sql_types/sql_types.py` | Fix VARCHAR JSON, INT negatives, BOOL kwargs, TIME._type |
| `dbmasta/pg_client/response.py` | Inherit from DataBaseResponseBase |
| `dbmasta/pg_async_client/response.py` | Inherit from DataBaseResponseBase |
| `dbmasta/db_client/base.py` | TTL cache, count/exists/bulk_insert |
| `dbmasta/async_db_client/base.py` | TTL cache, logging, count/exists/bulk_insert |
| `dbmasta/pg_client/base.py` | TTL cache, count/exists/bulk_insert |
| `dbmasta/pg_async_client/base.py` | TTL cache, logging, count/exists/bulk_insert |
| `dbmasta/db_client/tables.py` | Fix TTL 15min -> 30min |
| `dbmasta/async_db_client/tables.py` | Fix TTL 15min -> 30min |
| `dbmasta/pg_client/tables.py` | Fix TTL 15min -> 30min |
| `dbmasta/pg_async_client/tables.py` | Fix TTL 15min -> 30min |

---
---

# dbmasta v0.1.27 — Changes

Release date: 2026-03-11

This release hardens dbmasta's connection management, adds transaction support, introduces a JOIN API, and fixes several bugs. All existing method signatures and import paths are preserved.

---

## Version Bump

`0.1.26` -> `0.1.27`

Applied in `pyproject.toml`.

---

## Bug Fixes

### `engine.dipose()` typo (pg_client)

`dbmasta/pg_client/base.py` line 91 had `engine.dipose()` instead of `engine.dispose()`. The `preload_tables()` method would crash with an `AttributeError` on PostgreSQL sync clients. Fixed.

### Missing `result.close()` in response handlers

Three of the four response `_receive()` methods did not close the SQLAlchemy result cursor after fetching. Only `async_db_client/response.py` had it. This could leak database cursors under certain drivers.

**Fixed in:**
- `dbmasta/db_client/response.py`
- `dbmasta/pg_client/response.py`
- `dbmasta/pg_async_client/response.py`

Each now has a `finally: result.close()` block matching the async_db_client pattern.

### Missing fatal error detection in pg_async_client

The MySQL async client (`async_db_client`) detected fatal protocol errors and called `connection.invalidate()` to remove broken connections from the pool. The PostgreSQL async client (`pg_async_client`) had no equivalent — a dead connection could be returned to the pool and cause cascading failures.

**Fixed:** `pg_async_client/base.py` `execute()` now checks for fatal PostgreSQL errors (`connection is closed`, `server closed the connection unexpectedly`, `SSL connection has been closed unexpectedly`, `could not connect to server`, `terminating connection due to administrator command`) and invalidates the connection when detected.

### Dead pool config params on pg_client NullPool

`pg_client/base.py` passed `max_overflow`, `pool_size`, `pool_recycle`, and `pool_timeout` kwargs to `create_engine()` when using `NullPool`. `NullPool` ignores all of these. They were removed to avoid confusion (and are no longer relevant since sync clients now use `QueuePool`).

---

## Connection Management Changes (Critical)

### The Problem (Before v0.1.27)

Sync clients (`db_client` and `pg_client`) created a brand-new `NullPool` engine on **every single method call** (`select`, `insert`, `update`, `delete`, `run`). Each call:

1. Called `create_engine()` with `poolclass=NullPool`
2. Opened a TCP connection to the database
3. Executed the query
4. Called `engine.dispose()` in a `finally` block
5. Closed the TCP connection

This meant every query paid the full cost of TCP handshake + authentication + SSL negotiation. For applications making many queries, this was a massive performance bottleneck.

The async clients (`async_db_client` and `pg_async_client`) already had an `EngineManager` with `AsyncAdaptedQueuePool` that maintained persistent, pooled connections. The sync clients had no equivalent.

### The Fix

Sync clients now mirror the async pattern with a `SyncEngineManager`:

**New files:**
- `dbmasta/db_client/engine.py` — `SyncEngine` + `SyncEngineManager` for MySQL
- `dbmasta/pg_client/engine.py` — `SyncEngine` + `SyncEngineManager` for PostgreSQL

**How it works:**

1. On construction, the client creates a `SyncEngineManager`
2. The manager maintains a dict of `SyncEngine` objects keyed by database/schema name
3. Each `SyncEngine` wraps a SQLAlchemy `Engine` with `QueuePool` (connection pooling)
4. `pool_pre_ping=True` is enabled — the pool tests connections before handing them out, automatically discarding stale ones
5. CRUD methods call `self.engine_manager.get_engine(database)` instead of creating a new engine
6. The `engine.dispose()` calls in `finally` blocks of every CRUD method have been **removed** — engines are persistent and managed by the pool
7. `run()` still uses a temporary single-use engine (`get_temporary_engine()`) that is disposed after the query, matching the async client pattern

**Default pool settings (sync MySQL):**

| Setting | Value |
|---|---|
| `pool_size` | 5 |
| `max_overflow` | 3 |
| `pool_recycle` | 3600s |
| `pool_timeout` | 30s |
| `connect_timeout` | 240s |
| `pool_pre_ping` | True |

**Default pool settings (sync PostgreSQL):**

Same as above except `pool_recycle` is 1800s (matching the existing async PG defaults).

### Configuring the Pool

Pass `engine_config` to the constructor or class methods:

```python
db = DataBase.env(engine_config={"pool_size": 10, "max_overflow": 5})
db = DataBase(auth, engine_config={"pool_recycle": 900})
db = DataBase.with_creds(..., engine_config={"connect_timeout": 60})
```

The async clients already supported `engine_config` — no change there.

### Cleanup

Engines must be disposed when you're done. Two options:

```python
# Option 1: Context manager (recommended)
with DataBase.env() as db:
    ...
# kill_engines() called automatically

# Option 2: Manual
db = DataBase.env()
...
db.kill_engines()
```

If you don't call `kill_engines()`, connections remain open until the process exits (which is fine for long-running applications that want to keep the pool alive).

### SELECT No Longer Commits

Previously, sync `execute()` always called `connection.commit()` — even for SELECT queries. This is unnecessary and can cause issues with read replicas or read-only transactions.

**Fixed:** `execute()` now accepts `auto_commit: bool = True`. The `select()` and `join_select()` methods pass `auto_commit=False`. Write methods (`insert`, `update`, `delete`, `clear_table`) pass `auto_commit=True` (default). This matches the pattern already used by both async clients.

### Backward Compatibility

- All existing method signatures are unchanged
- `engine()` method still exists but now returns a `SyncEngine` wrapper instead of a raw SQLAlchemy engine. Code that called `db.engine(database)` to get a raw engine should use `db.engine(database).ctx` instead
- `engine_config` parameter defaults to `None` (backward compatible — creates pool with defaults)
- `env()` and `with_creds()` accept optional `engine_config` kwarg

---

## New Features

### Context Manager Protocol

All four client classes now support `with` / `async with`:

```python
with DataBase.env() as db:
    ...
# db.kill_engines() called automatically

async with AsyncDataBase.env() as db:
    ...
# await db.kill_engines() called automatically
```

**Files changed:**
- `dbmasta/db_client/base.py` — added `__enter__`, `__exit__`
- `dbmasta/pg_client/base.py` — added `__enter__`, `__exit__`
- `dbmasta/async_db_client/base.py` — added `__aenter__`, `__aexit__`
- `dbmasta/pg_async_client/base.py` — added `__aenter__`, `__aexit__`

### Retry Logic

All `execute()` methods are now decorated with automatic retry for transient connection failures.

**New file:** `dbmasta/retry.py`

- `sync_retry(max_retries=2, backoff_base=0.5)` — decorator for sync methods
- `async_retry(max_retries=2, backoff_base=0.5)` — decorator for async methods
- `is_transient(e)` — checks if an exception is a transient connection error

**Retry behavior:**
- Up to 2 retries with exponential backoff (0.5s, 1.0s)
- Only retries on transient errors (lost connection, refused, timeout, too many connections, etc.)
- SQL errors, constraint violations, and other application-level errors propagate immediately — they are never retried

**Applied to:**
- `dbmasta/db_client/base.py` — `execute()` decorated with `@sync_retry`
- `dbmasta/pg_client/base.py` — `execute()` decorated with `@sync_retry`
- `dbmasta/async_db_client/base.py` — `execute()` decorated with `@async_retry`
- `dbmasta/pg_async_client/base.py` — `execute()` decorated with `@async_retry`

### Centralized Error Detection

Fatal connection error detection was previously inline in `async_db_client/base.py` only.

**New file:** `dbmasta/errors.py`

```python
is_fatal_mysql_connection_error(e)    # checks MySQL-specific fatal errors
is_fatal_pg_connection_error(e)       # checks PostgreSQL-specific fatal errors
is_fatal_connection_error(e, dialect) # unified interface
```

The inline `_is_fatal_mysql_protocol_error()` function in `async_db_client/base.py` has been removed and replaced with an import from `dbmasta.errors`.

### Transaction Support

**New file:** `dbmasta/transaction.py`

Two classes:
- `SyncTransaction` — for `db_client` and `pg_client`
- `AsyncTransaction` — for `async_db_client` and `pg_async_client`

Each wraps a single database connection in a transaction with context manager protocol. Commits on clean exit, rolls back on exception.

Transaction methods (`select`, `insert`, `update`, `delete`, `run`, `execute`) mirror the parent client's interface but execute on the shared transaction connection. Type conversion, query building, and condition construction are all delegated to the parent client's existing methods.

The transaction class is dialect-aware (MySQL vs PostgreSQL) and handles the differences in upsert syntax automatically.

**Added `transaction()` method to all 4 client classes:**
- `db_client/base.py`
- `pg_client/base.py`
- `async_db_client/base.py`
- `pg_async_client/base.py`

### JOIN API

Added `join_select()` to all four client classes. Performs SQL JOINs (inner, left, right, full) between two tables.

**Features:**
- `on` dict specifies join conditions: `{"left_col": "right_col"}`
- Auto-labels conflicting column names as `{tablename}_{colname}`
- Dot notation in `columns`, `params`, and `order_by` for table-qualified access
- Supports cross-database (MySQL) and cross-schema (PostgreSQL) joins
- Full support for all existing condition helpers (`in_`, `greater_than`, `_AND_`, `_OR_`, etc.)
- `textual=True` for SQL inspection

**Helper methods added (private, all 4 clients):**
- `_resolve_join_columns()` — auto-select all columns, label conflicts
- `_parse_join_columns()` — parse user-specified column list with dot notation
- `_resolve_join_order_col()` — resolve order_by with dot notation
- `_construct_join_conditions()` — WHERE clause on joined tables
- `_resolve_join_col()` — resolve a single column key across both tables
- `_process_join_condition()` — recursive handler for `_AND_`/`_OR_` on joined tables

---

## Files Summary

| File | Action | Description |
|---|---|---|
| `pyproject.toml` | Modified | Version 0.1.26 -> 0.1.27 |
| `dbmasta/errors.py` | **New** | Centralized fatal error detection |
| `dbmasta/retry.py` | **New** | Retry decorators for transient failures |
| `dbmasta/transaction.py` | **New** | SyncTransaction + AsyncTransaction |
| `dbmasta/db_client/engine.py` | **New** | SyncEngine + SyncEngineManager (MySQL) |
| `dbmasta/pg_client/engine.py` | **New** | SyncEngine + SyncEngineManager (PostgreSQL) |
| `dbmasta/db_client/base.py` | Rewritten | Engine manager, context mgr, retry, transaction, join |
| `dbmasta/pg_client/base.py` | Rewritten | Engine manager, context mgr, retry, transaction, join |
| `dbmasta/async_db_client/base.py` | Modified | Context mgr, retry, centralized errors, transaction, join |
| `dbmasta/pg_async_client/base.py` | Modified | Context mgr, retry, fatal error detection, transaction, join |
| `dbmasta/db_client/response.py` | Modified | Added `result.close()` in finally |
| `dbmasta/pg_client/response.py` | Modified | Added `result.close()` in finally |
| `dbmasta/pg_async_client/response.py` | Modified | Added `result.close()` in finally |
| `tests/test_imports.py` | **New** | Import + feature verification tests |
| `howto.md` | **New** | Full project documentation |
| `changes.md` | **New** | This file |
