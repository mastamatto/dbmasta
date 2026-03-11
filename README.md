# dbmasta

A Python database client built on SQLAlchemy Core (not ORM). Provides a clean, unified interface for MySQL/MariaDB and PostgreSQL with both sync and async support, automatic type conversion, connection pooling, query building, transactions, joins, pagination, and retry logic.

## Table of Contents

- [Installation](#installation)
- [Quick Start](#quick-start)
- [Authorization](#authorization)
- [Client Reference](#client-reference)
- [CRUD Operations](#crud-operations)
  - [select](#select)
  - [insert](#insert)
  - [upsert](#upsert)
  - [update](#update)
  - [delete](#delete)
  - [run (Raw SQL)](#run-raw-sql)
  - [clear_table](#clear_table)
- [Query Filters](#query-filters)
  - [Comparison](#comparison)
  - [String Matching](#string-matching)
  - [Date/Time](#datetime)
  - [NULL](#null)
  - [Negation](#negation)
  - [Nested Logic (_AND_ / _OR_)](#nested-logic-_and_--_or_)
  - [PostgreSQL-Specific (json_like)](#postgresql-specific)
  - [Custom SQL Fragments](#custom-sql-fragments)
- [Pagination](#pagination)
- [Joins](#joins)
- [Transactions](#transactions)
- [Utility Methods](#utility-methods)
  - [count](#count)
  - [exists](#exists)
  - [bulk_insert](#bulk_insert)
- [DataBaseResponse](#databaseresponse)
- [Response Models](#response-models)
- [Connection Pooling & Engine Configuration](#connection-pooling--engine-configuration)
- [Retry Logic](#retry-logic)
- [Type System](#type-system)
- [Exceptions](#exceptions)
- [Environment Variables](#environment-variables)
- [Testing](#testing)
- [Project Structure](#project-structure)

---

## Installation

```bash
pip install dbmasta
```

dbmasta requires **SQLAlchemy >= 2.0.41** and **Python >= 3.9**. Database drivers are optional — install only the ones you need:

```bash
# MySQL/MariaDB sync
pip install dbmasta[mysql]

# MySQL/MariaDB async
pip install dbmasta[mysql-async]

# PostgreSQL sync
pip install dbmasta[pg]

# PostgreSQL async
pip install dbmasta[pg-async]

# Everything
pip install dbmasta[all]
```

| Extra | Drivers installed |
|-------|-------------------|
| `mysql` | pymysql |
| `mysql-async` | aiomysql, asyncmy |
| `pg` | psycopg-binary |
| `pg-async` | asyncpg |
| `all` | All of the above |

---

## Quick Start

### MySQL / MariaDB

```python
from dbmasta import DataBase, AsyncDataBase

# Sync
db = DataBase.env()
users = db.select("my_database", "users", params={"is_active": 1})
for user in users:
    print(user["name"])
db.kill_engines()

# Async
import asyncio
async def main():
    async with AsyncDataBase.env() as db:
        users = await db.select("my_database", "users", params={"is_active": 1})
        for user in users:
            print(user["name"])

asyncio.run(main())
```

### PostgreSQL

```python
from dbmasta.pg_client import DataBase
from dbmasta.pg_async_client import AsyncDataBase

# Sync
with DataBase.env() as db:
    users = db.select("public", "users", params={"is_active": True})

# Async
async with AsyncDataBase.env() as db:
    users = await db.select("public", "users")
```

---

## Authorization

`Authorization` holds credentials and builds SQLAlchemy connection URIs.

### Constructor

```python
from dbmasta.authorization import Authorization

auth = Authorization(
    username="myuser",
    password="mypass",
    host="localhost",
    default_database="mydb",
    port=3306,                              # optional, default port used if omitted
    engine="pymysql",                       # driver to use (see table below)
    extra_connection_params={"charset": "utf8mb4"},  # optional query string params
)
```

### Supported Engines

| Engine | Database | Mode | SQLAlchemy Dialect |
|--------|----------|------|--------------------|
| `pymysql` | MySQL/MariaDB | Sync | `mysql+pymysql` |
| `aiomysql` | MySQL/MariaDB | Async | `mysql+aiomysql` |
| `asyncmy` | MySQL/MariaDB | Async | `mysql+asyncmy` |
| `psycopg2` | PostgreSQL | Sync | `postgresql+psycopg2` |
| `asyncpg` | PostgreSQL | Async | `postgresql+asyncpg` |

### From Environment Variables

```python
auth = Authorization.env(engine="pymysql")
```

Reads: `dbmasta_username`, `dbmasta_password`, `dbmasta_host`, `dbmasta_port`, `dbmasta_default`.

### URI Generation

```python
auth.uri("my_database")
# → "mysql+pymysql://myuser:mypass@localhost:3306/my_database"
```

---

## Client Reference

dbmasta provides 4 client classes with identical APIs (sync vs async, MySQL vs PostgreSQL):

| Import | Database | Mode |
|--------|----------|------|
| `from dbmasta import DataBase` | MySQL/MariaDB | Sync |
| `from dbmasta import AsyncDataBase` | MySQL/MariaDB | Async |
| `from dbmasta.pg_client import DataBase` | PostgreSQL | Sync |
| `from dbmasta.pg_async_client import AsyncDataBase` | PostgreSQL | Async |

### Construction

```python
# From Authorization object
db = DataBase(auth, debug=False, engine_config=None)

# From environment variables
db = DataBase.env(debug=False, engine_config=None)

# From inline credentials
db = DataBase.with_creds(
    host="localhost", port=3306,
    username="user", password="pass",
    database="mydb",
    debug=False, engine_config=None,
)
```

### Context Manager (Recommended)

```python
# Sync
with DataBase.env() as db:
    ...
# kill_engines() called automatically

# Async
async with AsyncDataBase.env() as db:
    ...
# await kill_engines() called automatically
```

### AsyncDataBase Additional Parameters

```python
AsyncDataBase(
    auth,
    debug=False,
    on_new_table=None,            # callback(table_cache) when a new table is reflected
    on_query_error=None,          # callback(exc, tb, dbr) on query errors
    ignore_event_errors=False,    # swallow event handler exceptions
    auto_raise_errors=False,      # auto-raise on query failure
    max_db_concurrency=10,        # semaphore limit for concurrent queries
    db_exec_timeout=30,           # per-query timeout in seconds
    engine_config=None,
)
```

---

## CRUD Operations

All examples below show the sync MySQL client. For async, prefix with `await`. For PostgreSQL, the first parameter is `schema` instead of `database`.

### select

```python
dbr = db.select(
    "my_database",
    "users",
    params={"is_active": 1},             # optional filter conditions
    columns=["id", "name", "email"],     # optional column selection
    distinct=False,                       # SELECT DISTINCT
    order_by="name",                      # column to sort by
    reverse=True,                         # descending order
    limit=100,                            # max rows
    offset=0,                             # skip rows
    textual=False,                        # True → return raw SQL string instead of executing
    response_model=None,                  # callable to map each row
    as_decimals=False,                    # True → keep Decimal types (default: convert to float)
)

for user in dbr:
    print(user["name"])
```

### insert

```python
records = [
    {"name": "Alice", "email": "alice@example.com", "age": 30},
    {"name": "Bob", "email": "bob@example.com", "age": 25},
]
dbr = db.insert("my_database", "users", records)
```

Values are automatically converted to the correct database types using reflected column metadata.

### upsert

Insert or update on conflict (MySQL: `ON DUPLICATE KEY UPDATE`, PostgreSQL: `ON CONFLICT DO UPDATE`).

```python
# MySQL — update_keys controls which columns get updated on conflict
dbr = db.upsert("my_database", "users", records, update_keys=["name", "email"])

# PostgreSQL — update_fields controls which columns get updated on conflict
dbr = db.upsert("public", "users", records, update_fields=["name", "email"])

# Omit update_keys/update_fields to update all columns
dbr = db.upsert("my_database", "users", records)
```

### update

```python
dbr = db.update(
    "my_database", "users",
    update={"is_active": 0, "deactivated_at": datetime.now()},
    where={"email": "alice@example.com"},
)
```

### delete

```python
dbr = db.delete(
    "my_database", "users",
    where={"is_active": 0},
)
```

### run (Raw SQL)

```python
# Simple query
dbr = db.run("SELECT COUNT(*) AS ct FROM users", "my_database")

# Parameterized query (safe from SQL injection)
dbr = db.run(
    "SELECT * FROM users WHERE age > :min_age",
    "my_database",
    params={"min_age": 21},
)
```

`run()` creates a temporary single-use engine that is disposed after execution.

For async clients, `run()` also accepts a `timeout` parameter (seconds):

```python
dbr = await db.run("SELECT SLEEP(5)", "my_database", timeout=10)
```

### clear_table

Delete all rows from a table:

```python
dbr = db.clear_table("my_database", "users")
```

---

## Query Filters

Filter conditions are passed as a `params` dict to `select()`, `update()`, `delete()`, `count()`, `exists()`, and `join_select()`.

Plain values are equality checks. For other operators, use the static methods on the client class:

```python
params = {
    "name": "Alice",                                    # name = 'Alice'
    "age": DataBase.greater_than(18),                   # age > 18
    "status": DataBase.in_(["active", "pending"]),      # status IN ('active','pending')
}
```

### Comparison

```python
DataBase.in_(values, _not=False, include_null=None)
# status IN ('a','b')  or  status NOT IN ('a','b')

DataBase.greater_than(value, or_equal=False, _not=False)
# age > 18  or  age >= 18

DataBase.less_than(value, or_equal=False, _not=False)
# age < 65  or  age <= 65

DataBase.equal_to(value, _not=False, include_null=None)
# status = 'active'  or  status != 'active'

DataBase.between(value1, value2, _not=False)
# age BETWEEN 18 AND 65
```

### String Matching

```python
DataBase.like(value, _not=False)
# name LIKE 'J%hn'

DataBase.starts_with(value, _not=False)
# name LIKE 'John%'

DataBase.ends_with(value, _not=False)
# name LIKE '%son'

DataBase.contains(value, _not=False)
# name LIKE '%oh%'

DataBase.regex(value, _not=False)
# name REGEXP '^[A-Z]'
```

### Date/Time

```python
DataBase.after(date, inclusive=False, _not=False)
# created_at > '2024-01-01'  (alias for greater_than)

DataBase.before(date, inclusive=False, _not=False)
# created_at < '2024-01-01'  (alias for less_than)

DataBase.onDay(date, _not=False)
# created_at = '2024-01-01'
```

### NULL

```python
DataBase.null(_not=False)
# deleted_at IS NULL
# deleted_at IS NOT NULL  (with _not=True)
```

### Negation

Every filter method accepts `_not=True` to negate:

```python
params = {
    "status": DataBase.in_(["deleted", "banned"], _not=True),     # NOT IN
    "name": DataBase.starts_with("test_", _not=True),             # NOT LIKE 'test_%'
    "age": DataBase.between(0, 18, _not=True),                    # NOT BETWEEN
}
```

The `not_()` method wraps any other filter:

```python
DataBase.not_(DataBase.in_, ["a", "b"])
# Equivalent to DataBase.in_(["a","b"], _not=True)
```

### Nested Logic (_AND_ / _OR_)

Combine conditions with boolean logic:

```python
params = {
    # Top-level keys are implicitly AND-ed
    "status": "active",

    # Explicit OR: match ANY of these condition sets
    "_OR_": [
        {"age": DataBase.greater_than(65)},
        {"role": "admin"},
    ],

    # Explicit AND: match ALL of these condition sets
    "_AND_": [
        {"name": DataBase.starts_with("J")},
        {"name": DataBase.not_(DataBase.ends_with, "x")},
    ],
}
```

`_AND_` and `_OR_` can be nested arbitrarily deep:

```python
params = {
    "_OR_": [
        {
            "_AND_": [
                {"category": "electronics"},
                {"price": DataBase.less_than(100)},
            ]
        },
        {
            "_AND_": [
                {"category": "books"},
                {"price": DataBase.less_than(20)},
            ]
        },
    ]
}
```

### PostgreSQL-Specific

```python
# JSONB containment (@> operator)
DataBase.json_like({"role": "admin"}, _not=False)
# WHERE metadata::jsonb @> '{"role": "admin"}'
```

### Custom SQL Fragments

```python
DataBase.custom("> 10 AND col < 100")
```

### Legacy Aliases

CamelCase aliases exist for backward compatibility: `greaterThan`, `lessThan`, `equalTo`, `startsWith`, `endsWith`.

---

## Pagination

### select_pages

Automatically paginate large queries:

```python
# Sync — returns a generator
for page in db.select_pages("my_database", "users", page_size=10000, order_by="id"):
    process_batch(page)  # page is a list of dicts

# Async — returns an async generator
async for page in db.select_pages("my_database", "users", page_size=10000, order_by="id"):
    process_batch(page)
```

`select_pages` accepts the same parameters as `select` (except `limit`/`offset`/`textual`).

### insert_pages / upsert_pages

Split large inserts into chunks:

```python
for dbr in db.insert_pages("my_database", "users", big_list, page_size=5000):
    print(f"Inserted page: {dbr.successful}")

for dbr in db.upsert_pages("my_database", "users", big_list, page_size=5000):
    print(f"Upserted page: {dbr.successful}")
```

---

## Joins

`join_select` performs SQL JOINs between two tables:

```python
dbr = db.join_select(
    "my_database",
    "users",                              # left table
    "orders",                             # right table
    on={"id": "user_id"},                 # left.id = right.user_id
    join_type="inner",                    # "inner", "left", "right", "full"
    params=None,                          # WHERE conditions
    columns=None,                         # column selection (default: all)
    join_table_database=None,             # if right table is in a different database/schema
    distinct=False,
    order_by=None,
    offset=None,
    limit=None,
    reverse=None,
    textual=False,
    response_model=None,
    as_decimals=False,
)
```

### Column Name Conflicts

When both tables have a column with the same name, it's automatically labeled as `{tablename}_{colname}`:

```python
dbr = db.join_select("mydb", "users", "orders", on={"id": "user_id"})
row = dbr[0]
row["users_id"]     # id from users table
row["orders_id"]    # id from orders table
row["email"]        # unique to users, no prefix needed
row["product"]      # unique to orders, no prefix needed
```

### Dot Notation

Use `table.column` syntax in `columns`, `params`, and `order_by` to disambiguate:

```python
dbr = db.join_select(
    "mydb", "users", "orders",
    on={"id": "user_id"},
    columns=["users.name", "orders.product", "orders.price"],
    params={"orders.status": DataBase.in_(["shipped", "delivered"])},
    order_by="orders.price",
    reverse=True,
    limit=50,
)
```

### Cross-Database / Cross-Schema Joins

```python
# MySQL: join tables from different databases
dbr = db.join_select("db1", "users", "orders",
                     on={"id": "user_id"},
                     join_table_database="db2")

# PostgreSQL: join tables from different schemas
dbr = db.join_select("public", "users", "orders",
                     on={"id": "user_id"},
                     join_table_schema="analytics")
```

---

## Transactions

Wrap multiple operations in an atomic transaction. Auto-commits on clean exit, auto-rolls back on exception.

### Sync

```python
with db.transaction("my_database") as txn:
    txn.insert("my_database", "orders", [{"user_id": 1, "product": "widget", "qty": 5}])
    txn.update("my_database", "inventory", update={"stock": 95}, where={"product": "widget"})
    txn.delete("my_database", "cart", where={"user_id": 1})
    result = txn.select("my_database", "orders", params={"user_id": 1})
    # Commits when the `with` block exits cleanly
```

### Async

```python
async with db.transaction("my_database") as txn:
    await txn.insert("my_database", "orders", [{"user_id": 1, "product": "widget"}])
    await txn.update("my_database", "inventory", update={"stock": 95}, where={"product": "widget"})
    # Commits on clean exit, rolls back on exception
```

### Rollback on Error

```python
try:
    with db.transaction("my_database") as txn:
        txn.insert("my_database", "orders", [{"user_id": 1, "product": "widget"}])
        raise ValueError("something went wrong")
        # Transaction is rolled back — the insert is undone
except ValueError:
    pass
```

### Transaction Methods

Transactions support: `select`, `insert` (with `upsert=True`), `update`, `delete`, `run`, `execute`. All use the same signatures as the parent client.

---

## Utility Methods

### count

Returns the number of rows matching optional filters:

```python
total = db.count("my_database", "users")
active = db.count("my_database", "users", params={"is_active": 1})
# async: total = await db.count(...)
```

### exists

Returns `True` if any rows match the filter:

```python
has_admin = db.exists("my_database", "users", params={"role": "admin"})
# async: has_admin = await db.exists(...)
```

### bulk_insert

Splits large inserts into chunks to avoid packet size limits:

```python
results = db.bulk_insert("my_database", "users", big_list, chunk_size=1000)
# results is a list of DataBaseResponse, one per chunk
# async: results = await db.bulk_insert(...)
```

---

## DataBaseResponse

All CRUD methods return a `DataBaseResponse` object:

```python
dbr = db.select("my_database", "users")

# Status
dbr.successful      # True if query succeeded
dbr.error_info      # Error message string (None if no error)
dbr.traceback       # Full traceback string (None if no error)
dbr.raw_query       # Compiled SQL string

# Data access
dbr.records         # List of row dicts
dbr.keys            # Column names
dbr.returns_rows    # True for SELECT queries
dbr.row_count       # Number of rows (same as len(dbr))

# Iteration
for row in dbr:
    print(row)

# Indexing
first = dbr[0]
last = dbr[-1]

# Length
print(len(dbr))

# one_or_none — returns single row or None, raises if multiple rows
user = dbr.one_or_none

# Pop a row
row = dbr.pop()

# Raise if error
dbr.raise_for_error()
```

### Decimal Handling

By default, `Decimal` values from the database are auto-converted to `float`. Pass `as_decimals=True` to `select()` or `join_select()` to keep them as `Decimal`.

---

## Response Models

Map rows into custom objects using `response_model`:

```python
from pydantic import BaseModel

class User(BaseModel):
    id: int
    name: str
    email: str

dbr = db.select("my_database", "users",
                response_model=lambda row: User(**row))

user = dbr[0]       # User instance
user.name            # attribute access
```

Any callable that accepts a dict works — dataclasses, named tuples, plain functions.

---

## Connection Pooling & Engine Configuration

dbmasta uses SQLAlchemy's `QueuePool` (sync) or `AsyncAdaptedQueuePool` (async) for persistent connection pooling with automatic health checks (`pool_pre_ping=True`).

### Default Pool Settings

| Setting | MySQL Sync | MySQL Async | PG Sync | PG Async |
|---------|-----------|-------------|---------|----------|
| `pool_size` | 5 | 10 | 5 | 10 |
| `max_overflow` | 3 | 5 | 3 | 5 |
| `pool_recycle` | 3600s | 3600s | 1800s | 1800s |
| `pool_timeout` | 30s | 30s | 30s | 30s |
| `connect_timeout` | 240s | 30s | 240s | 30s |

### Custom Configuration

```python
db = DataBase.env(engine_config={
    "pool_size": 20,
    "max_overflow": 10,
    "pool_recycle": 900,
    "pool_timeout": 60,
    "connect_timeout": 30,
})
```

### Concurrency Control (Async Only)

```python
db = AsyncDataBase(auth,
    max_db_concurrency=20,    # max concurrent queries (semaphore)
    db_exec_timeout=30,       # per-query timeout in seconds
)
```

### Cleanup

Always dispose engines when done:

```python
# Context manager (recommended)
with DataBase.env() as db:
    ...

# Manual cleanup
db = DataBase.env()
...
db.kill_engines()

# Async
await db.kill_engines()
```

---

## Retry Logic

All `execute()` methods are decorated with automatic retry for transient connection failures:

- Up to **2 retries** with exponential backoff (0.5s, 1.0s)
- Only retries on transient errors: lost connection, refused, timeout, too many connections, packet sequence errors
- SQL errors, constraint violations, and application-level errors are **never retried**

Fatal connection errors (broken protocol, server crash) trigger connection invalidation — the broken connection is removed from the pool.

---

## Type System

dbmasta automatically converts Python values to database-appropriate types before inserts/upserts using reflected column metadata.

### MySQL/MariaDB Types

| SQL Type | Python Type | Notes |
|----------|-------------|-------|
| VARCHAR, CHAR, TEXT, TINYTEXT, MEDIUMTEXT, LONGTEXT | str | Auto-truncates to column length. Dicts/lists auto-serialized to JSON. |
| BLOB, TINYBLOB, MEDIUMBLOB, LONGBLOB | str/bytes | Dicts auto-serialized to JSON |
| ENUM | str | |
| INT, TINYINT, SMALLINT, MEDIUMINT, BIGINT | int | Handles negative numbers, string-to-int conversion |
| BOOL | int (0/1) | |
| FLOAT, DECIMAL, DOUBLE | float | |
| DATE | datetime.date | datetime values auto-converted to date |
| DATETIME, TIMESTAMP | datetime.datetime | Microseconds stripped |
| TIME | datetime.time | Accepts time or datetime (extracts time part) |
| YEAR | int | |

### PostgreSQL Types

All MySQL types above plus:

| SQL Type | Python Type |
|----------|-------------|
| BOOLEAN | bool (native True/False) |
| NUMERIC, MONEY | float |
| TIMESTAMPTZ | datetime.datetime (with timezone) |
| JSON | dict |
| JSONB | dict |
| UUID | str (validated UUID format) |
| BYTEA | bytes |
| POINT | tuple (x, y) |

---

## Exceptions

```python
from dbmasta import SchemaQueryError, MissingColumnError
from dbmasta.exceptions import InvalidDate, NonConfiguredDataType
```

| Exception | When |
|-----------|------|
| `SchemaQueryError` | INFORMATION_SCHEMA query fails or returns no columns (table doesn't exist, permission denied) |
| `MissingColumnError` | A record key doesn't match any column in the table schema |
| `InvalidDate` | A date/time value can't be converted to the expected type |
| `NonConfiguredDataType` | Column type not recognized by the type conversion system |

---

## Environment Variables

For `Authorization.env()` and `DataBase.env()`:

| Variable | Description | Required |
|----------|-------------|----------|
| `dbmasta_username` | Database username | Yes |
| `dbmasta_password` | Database password | Yes |
| `dbmasta_host` | Database host | Yes |
| `dbmasta_port` | Database port | Yes |
| `dbmasta_default` | Default database/schema name | Yes |

Alternative names also supported: `DB_USERNAME`, `DB_PASSWORD`, `DB_HOST`, `DB_PORT`, `DB_DATABASE`.

A `.env` file is loaded automatically in tests via `python-dotenv`.

---

## Testing

```bash
# Install with test dependencies
git clone https://github.com/mastamatto/dbmasta && cd dbmasta
pip install -e ".[test]"

# Run unit tests (no database required)
pytest tests/test_exceptions_and_validation.py tests/test_response_re_raise.py tests/test_imports.py tests/test_text_column_limits.py -v

# Run all tests including DB tests (requires env vars)
pytest tests/ -v

# Run stress tests (async concurrency, transactions, joins, pool exhaustion)
pytest tests/test_stress_async.py -v
```

pytest is configured with `asyncio_mode = "auto"` — async tests run without extra markers.

---

## Project Structure

```
dbmasta/
├── __init__.py                 # Public API: DataBase, AsyncDataBase, DataBaseResponse, exceptions
├── authorization.py            # Credentials and URI building
├── exceptions.py               # InvalidDate, SchemaQueryError, MissingColumnError
├── errors.py                   # Fatal connection error detection
├── retry.py                    # sync_retry / async_retry decorators
├── transaction.py              # SyncTransaction / AsyncTransaction
├── response/
│   └── base.py                 # DataBaseResponseBase (shared by all 4 clients)
├── sql_types/
│   ├── __init__.py             # type_map for MySQL
│   └── sql_types.py            # MySQL type classes (VARCHAR, INT, DATETIME, etc.)
├── sql_types_pg/
│   ├── __init__.py             # type_map for PostgreSQL
│   └── sql_types.py            # PG type classes (+ UUID, JSONB, POINT, BYTEA, etc.)
├── db_client/                  # MySQL/MariaDB sync
│   ├── base.py                 # DataBase class
│   ├── response.py             # DataBaseResponse (inherits base)
│   ├── engine.py               # SyncEngine + SyncEngineManager
│   └── tables.py               # TableCache (30-min TTL)
├── async_db_client/            # MySQL/MariaDB async
│   ├── base.py                 # AsyncDataBase class
│   ├── response.py             # DataBaseResponse (inherits base)
│   ├── engine.py               # Engine + EngineManager
│   └── tables.py               # TableCache (async reflect)
├── pg_client/                  # PostgreSQL sync
│   ├── base.py                 # DataBase class
│   ├── response.py             # DataBaseResponse (PG dialect, inherits base)
│   ├── engine.py               # SyncEngine + SyncEngineManager
│   └── tables.py               # TableCache (schema-aware)
└── pg_async_client/            # PostgreSQL async
    ├── base.py                 # AsyncDataBase class
    ├── response.py             # DataBaseResponse (PG dialect, inherits base)
    ├── engine.py               # Engine + EngineManager
    └── tables.py               # TableCache (schema-aware, async)
```

---

## License

MIT
