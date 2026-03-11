"""
Async stress tests for dbmasta.

Creates real tables, seeds data, then tries to break the library with:
- High-concurrency selects, inserts, upserts, updates, deletes
- Transactions under load (commit and rollback)
- Joins under concurrent access
- count() / exists() / bulk_insert() under load
- Mixed read/write storms
- Rapid engine creation/disposal
- Queries that deliberately error to test error-path concurrency
- Connection pool exhaustion attempts

Requires DB env vars (DB_USERNAME, DB_PASSWORD, DB_HOST, DB_DATABASE).
Tables are created in the DB_DATABASE schema and cleaned up after each test class.
"""
import asyncio
import datetime as dt
import json
import random
import string
import pytest

from dbmasta.authorization import Authorization
from dbmasta.async_db_client import AsyncDataBase

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
TABLE_USERS = "dbmasta_tests_users"
TABLE_ORDERS = "dbmasta_tests_orders"
TABLE_TYPES = "dbmasta_tests_types"
SEED_USER_COUNT = 500
SEED_ORDER_COUNT = 2000


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _rand_str(n=12):
    return "".join(random.choices(string.ascii_lowercase + string.digits, k=n))


def _rand_email():
    return f"{_rand_str(8)}@{_rand_str(5)}.com"


def _seed_users(n=SEED_USER_COUNT):
    """Generate n user dicts."""
    records = []
    for i in range(n):
        records.append({
            "username": f"user_{i:05d}_{_rand_str(4)}",
            "email": _rand_email(),
            "age": random.randint(18, 80),
            "score": round(random.uniform(-100.0, 100.0), 2),
            "is_active": random.choice([0, 1]),
            "bio": _rand_str(200),
            "metadata_json": json.dumps({"level": random.randint(1, 50), "tags": [_rand_str(4) for _ in range(3)]}),
            "created_at": dt.datetime(2024, random.randint(1, 12), random.randint(1, 28),
                                       random.randint(0, 23), random.randint(0, 59), random.randint(0, 59)),
        })
    return records


def _seed_orders(user_ids, n=SEED_ORDER_COUNT):
    """Generate n order dicts referencing user IDs."""
    records = []
    for i in range(n):
        records.append({
            "user_id": random.choice(user_ids),
            "product": random.choice(["widget", "gadget", "doohickey", "thingamajig", "whatchamacallit"]),
            "quantity": random.randint(1, 100),
            "price": round(random.uniform(0.99, 999.99), 2),
            "status": random.choice(["pending", "shipped", "delivered", "cancelled"]),
            "ordered_at": dt.datetime(2025, random.randint(1, 12), random.randint(1, 28),
                                       random.randint(0, 23), random.randint(0, 59), random.randint(0, 59)),
        })
    return records


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------
@pytest.fixture(scope="session")
def db_env():
    import os
    from dotenv import load_dotenv
    load_dotenv()
    user = os.getenv("DB_USERNAME") or os.getenv("dbmasta_username")
    pw = os.getenv("DB_PASSWORD") or os.getenv("dbmasta_password")
    host = os.getenv("DB_HOST") or os.getenv("dbmasta_host")
    port = os.getenv("DB_PORT") or os.getenv("dbmasta_port", "3306")
    db = os.getenv("DB_DATABASE") or os.getenv("dbmasta_default")
    if not all([user, pw, host, db]):
        pytest.skip("DB env vars not set")
    return user, pw, host, int(port), db


@pytest.fixture
async def adb(db_env):
    """Async MySQL client with small pool to stress connection management."""
    user, pw, host, port, database = db_env
    auth = Authorization(
        username=user, password=pw, host=host, port=port,
        default_database=database, engine="asyncmy",
    )
    db = AsyncDataBase(
        auth=auth,
        max_db_concurrency=20,
        db_exec_timeout=60,
        engine_config={
            "pool_size": 5,
            "max_overflow": 5,
            "pool_recycle": 300,
            "pool_timeout": 30,
        },
    )
    yield db
    await db.kill_engines()


@pytest.fixture
async def adb_tiny_pool(db_env):
    """Async client with intentionally tiny pool (2 connections) to force contention."""
    user, pw, host, port, database = db_env
    auth = Authorization(
        username=user, password=pw, host=host, port=port,
        default_database=database, engine="asyncmy",
    )
    db = AsyncDataBase(
        auth=auth,
        max_db_concurrency=50,
        db_exec_timeout=60,
        engine_config={
            "pool_size": 2,
            "max_overflow": 1,
            "pool_recycle": 60,
            "pool_timeout": 30,
        },
    )
    yield db
    await db.kill_engines()


@pytest.fixture
async def seeded(adb, db_env):
    """Create tables, seed data, yield (db, database, user_ids), then drop tables."""
    _, _, _, _, database = db_env

    # Drop if leftover from a failed run
    for tbl in [TABLE_ORDERS, TABLE_TYPES, TABLE_USERS]:
        try:
            await adb.run(f"DROP TABLE IF EXISTS `{tbl}`", database)
        except Exception:
            pass

    # Create users table
    await adb.run(f"""
        CREATE TABLE `{TABLE_USERS}` (
            id INT AUTO_INCREMENT PRIMARY KEY,
            username VARCHAR(255) NOT NULL UNIQUE,
            email VARCHAR(255) NOT NULL,
            age INT DEFAULT 0,
            score FLOAT DEFAULT 0.0,
            is_active TINYINT DEFAULT 1,
            bio TEXT,
            metadata_json LONGTEXT,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        ) ENGINE=InnoDB
    """, database)

    # Create orders table (FK to users)
    await adb.run(f"""
        CREATE TABLE `{TABLE_ORDERS}` (
            id INT AUTO_INCREMENT PRIMARY KEY,
            user_id INT NOT NULL,
            product VARCHAR(100) NOT NULL,
            quantity INT DEFAULT 1,
            price FLOAT DEFAULT 0.0,
            status VARCHAR(50) DEFAULT 'pending',
            ordered_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (user_id) REFERENCES `{TABLE_USERS}`(id) ON DELETE CASCADE
        ) ENGINE=InnoDB
    """, database)

    # Create types table (for type-edge-case testing)
    await adb.run(f"""
        CREATE TABLE `{TABLE_TYPES}` (
            id INT AUTO_INCREMENT PRIMARY KEY,
            val_int INT,
            val_neg INT,
            val_float FLOAT,
            val_bool TINYINT,
            val_varchar VARCHAR(500),
            val_text TEXT,
            val_date DATE,
            val_datetime DATETIME,
            val_time TIME,
            val_json LONGTEXT,
            val_decimal DECIMAL(10,4),
            val_bigint BIGINT,
            nullable_col VARCHAR(100) NULL DEFAULT NULL
        ) ENGINE=InnoDB
    """, database)

    # Seed users
    user_records = _seed_users(SEED_USER_COUNT)
    # Insert in pages to avoid packet limits
    page = 100
    for i in range(0, len(user_records), page):
        await adb.insert(database, TABLE_USERS, user_records[i:i + page])

    # Get user IDs
    all_users = await adb.select(database, TABLE_USERS, columns=["id"])
    user_ids = [r["id"] for r in all_users]
    assert len(user_ids) == SEED_USER_COUNT

    # Seed orders
    order_records = _seed_orders(user_ids, SEED_ORDER_COUNT)
    for i in range(0, len(order_records), page):
        await adb.insert(database, TABLE_ORDERS, order_records[i:i + page])

    yield adb, database, user_ids

    # Cleanup
    for tbl in [TABLE_ORDERS, TABLE_TYPES, TABLE_USERS]:
        try:
            await adb.run(f"DROP TABLE IF EXISTS `{tbl}`", database)
        except Exception:
            pass


# ===================================================================
# TEST CLASS: Concurrent Select Storm
# ===================================================================
class TestConcurrentSelects:
    """Fire many selects at once and validate every result."""

    async def test_100_concurrent_selects(self, seeded):
        db, database, user_ids = seeded

        async def do_select(uid):
            dbr = await db.select(database, TABLE_USERS, params={"id": uid})
            assert dbr.successful is True or dbr.successful is None
            assert len(dbr) == 1
            assert dbr[0]["id"] == uid
            return dbr[0]

        sample = random.sample(user_ids, min(100, len(user_ids)))
        results = await asyncio.gather(*[do_select(uid) for uid in sample])
        assert len(results) == len(sample)

    async def test_concurrent_selects_with_filters(self, seeded):
        db, database, _ = seeded

        async def select_active():
            return await db.select(database, TABLE_USERS,
                                   params={"is_active": 1}, limit=50)

        async def select_age_range():
            return await db.select(database, TABLE_USERS,
                                   params={"age": db.greater_than(30)}, limit=50)

        async def select_score_negative():
            return await db.select(database, TABLE_USERS,
                                   params={"score": db.less_than(0)}, limit=50)

        async def select_email_like():
            return await db.select(database, TABLE_USERS,
                                   params={"email": db.contains("@")}, limit=50)

        async def select_ordered():
            return await db.select(database, TABLE_USERS,
                                   order_by="age", limit=20, reverse=True)

        tasks = []
        for _ in range(20):
            tasks.extend([
                select_active(), select_age_range(),
                select_score_negative(), select_email_like(),
                select_ordered(),
            ])

        results = await asyncio.gather(*tasks)
        assert len(results) == 100
        for r in results:
            assert r.successful is True or r.successful is None

    async def test_concurrent_selects_with_complex_conditions(self, seeded):
        db, database, _ = seeded

        async def complex_query():
            return await db.select(database, TABLE_USERS, params={
                "_OR_": [
                    {"age": db.between(20, 30), "is_active": 1},
                    {"score": db.greater_than(50, or_equal=True)},
                ]
            }, limit=100)

        results = await asyncio.gather(*[complex_query() for _ in range(50)])
        for r in results:
            assert r.successful is True or r.successful is None

    async def test_select_pages_concurrent(self, seeded):
        db, database, _ = seeded
        all_records = []
        async for page in db.select_pages(database, TABLE_USERS,
                                          order_by="id", page_size=100):
            all_records.extend(page)
        assert len(all_records) == SEED_USER_COUNT

    async def test_distinct_and_columns(self, seeded):
        db, database, _ = seeded
        dbr = await db.select(database, TABLE_USERS,
                              columns=["is_active"], distinct=True)
        vals = {r["is_active"] for r in dbr}
        assert vals <= {0, 1}


# ===================================================================
# TEST CLASS: Concurrent Writes
# ===================================================================
class TestConcurrentWrites:
    """Hammer inserts, upserts, updates, and deletes concurrently."""

    async def test_concurrent_inserts(self, seeded):
        db, database, _ = seeded
        records_per_task = 5
        num_tasks = 40

        async def insert_batch(batch_id):
            recs = [{"username": f"stress_ins_{batch_id}_{i}_{_rand_str(4)}",
                      "email": _rand_email(), "age": 25, "score": 1.0,
                      "is_active": 1, "bio": "stress"}
                     for i in range(records_per_task)]
            dbr = await db.insert(database, TABLE_USERS, recs)
            return dbr

        results = await asyncio.gather(*[insert_batch(b) for b in range(num_tasks)])
        for r in results:
            assert r.successful is True

        # Verify total
        ct = await db.count(database, TABLE_USERS)
        assert ct >= SEED_USER_COUNT + (num_tasks * records_per_task)

    async def test_concurrent_upserts(self, seeded):
        db, database, _ = seeded

        # Insert base records
        base = [{"username": f"upsert_stress_{i}", "email": _rand_email(),
                 "age": 20, "score": 0.0, "is_active": 1, "bio": "upsert_base"}
                for i in range(50)]
        await db.insert(database, TABLE_USERS, base)

        async def upsert_one(i):
            rec = {"username": f"upsert_stress_{i}", "email": _rand_email(),
                   "age": 99, "score": 99.9, "is_active": 0, "bio": "upsert_updated"}
            return await db.upsert(database, TABLE_USERS, [rec])

        results = await asyncio.gather(*[upsert_one(i) for i in range(50)])
        for r in results:
            assert r.successful is True

        # Verify age was updated
        dbr = await db.select(database, TABLE_USERS,
                              params={"username": "upsert_stress_0"})
        assert dbr[0]["age"] == 99

    async def test_concurrent_updates(self, seeded):
        db, database, user_ids = seeded
        sample = random.sample(user_ids, min(50, len(user_ids)))

        async def update_one(uid):
            new_score = round(random.uniform(-999, 999), 2)
            return await db.update(database, TABLE_USERS,
                                   update={"score": new_score},
                                   where={"id": uid})

        results = await asyncio.gather(*[update_one(uid) for uid in sample])
        for r in results:
            assert r.successful is True

    async def test_concurrent_deletes(self, seeded):
        db, database, _ = seeded
        # Insert ephemeral rows then delete them concurrently
        ephemeral = [{"username": f"del_stress_{i}_{_rand_str(6)}",
                      "email": _rand_email(), "age": 1, "score": 0,
                      "is_active": 0, "bio": "delete_me"}
                     for i in range(100)]
        await db.insert(database, TABLE_USERS, ephemeral)

        async def delete_one(uname):
            return await db.delete(database, TABLE_USERS,
                                   where={"username": uname})

        results = await asyncio.gather(
            *[delete_one(r["username"]) for r in ephemeral])
        for r in results:
            assert r.successful is True

        # Verify they're gone
        for rec in ephemeral[:5]:
            exists = await db.exists(database, TABLE_USERS,
                                     params={"username": rec["username"]})
            assert exists is False


# ===================================================================
# TEST CLASS: Mixed Read/Write Storm
# ===================================================================
class TestMixedReadWriteStorm:
    """Concurrent reads and writes hitting the same table."""

    async def test_mixed_operations_storm(self, seeded):
        db, database, user_ids = seeded
        errors = []

        async def reader():
            try:
                uid = random.choice(user_ids)
                dbr = await db.select(database, TABLE_USERS, params={"id": uid})
                assert len(dbr) <= 1
            except Exception as e:
                errors.append(("read", e))

        async def writer():
            try:
                rec = {"username": f"storm_{_rand_str(10)}",
                       "email": _rand_email(), "age": 30, "score": 5.0,
                       "is_active": 1, "bio": "storm_test"}
                await db.insert(database, TABLE_USERS, [rec])
            except Exception as e:
                errors.append(("write", e))

        async def updater():
            try:
                uid = random.choice(user_ids)
                await db.update(database, TABLE_USERS,
                                update={"bio": f"updated_{_rand_str(6)}"},
                                where={"id": uid})
            except Exception as e:
                errors.append(("update", e))

        async def counter():
            try:
                ct = await db.count(database, TABLE_USERS)
                assert ct > 0
            except Exception as e:
                errors.append(("count", e))

        tasks = []
        for _ in range(30):
            tasks.append(reader())
            tasks.append(writer())
            tasks.append(updater())
            tasks.append(counter())

        await asyncio.gather(*tasks)
        assert len(errors) == 0, f"Storm errors: {errors}"


# ===================================================================
# TEST CLASS: Transaction Stress
# ===================================================================
class TestTransactionStress:
    """Transactions under concurrency: commit, rollback, isolation."""

    async def test_transaction_commit(self, seeded):
        db, database, _ = seeded

        async with db.transaction(database) as txn:
            await txn.insert(database, TABLE_USERS,
                             [{"username": "txn_commit_test", "email": "txn@test.com",
                               "age": 42, "score": 100.0, "is_active": 1, "bio": "txn"}])
            dbr = await txn.select(database, TABLE_USERS,
                                   params={"username": "txn_commit_test"})
            assert len(dbr) == 1

        # Should be visible outside the transaction
        dbr = await db.select(database, TABLE_USERS,
                              params={"username": "txn_commit_test"})
        assert len(dbr) == 1
        assert dbr[0]["age"] == 42

    async def test_transaction_rollback_on_error(self, seeded):
        db, database, _ = seeded
        uname = f"txn_rollback_{_rand_str(6)}"

        with pytest.raises(ValueError):
            async with db.transaction(database) as txn:
                await txn.insert(database, TABLE_USERS,
                                 [{"username": uname, "email": "roll@back.com",
                                   "age": 1, "score": 0, "is_active": 0, "bio": "nope"}])
                raise ValueError("intentional rollback")

        # Row should NOT exist
        dbr = await db.select(database, TABLE_USERS, params={"username": uname})
        assert len(dbr) == 0

    async def test_concurrent_transactions(self, seeded):
        db, database, _ = seeded

        async def txn_insert_and_read(batch_id):
            uname = f"ctxn_{batch_id}_{_rand_str(4)}"
            async with db.transaction(database) as txn:
                await txn.insert(database, TABLE_USERS,
                                 [{"username": uname, "email": _rand_email(),
                                   "age": batch_id, "score": float(batch_id),
                                   "is_active": 1, "bio": "concurrent_txn"}])
                dbr = await txn.select(database, TABLE_USERS,
                                       params={"username": uname})
                assert len(dbr) == 1
                assert dbr[0]["age"] == batch_id
            return uname

        names = await asyncio.gather(*[txn_insert_and_read(i) for i in range(30)])
        assert len(names) == 30

        # All should be committed
        for name in names:
            dbr = await db.select(database, TABLE_USERS, params={"username": name})
            assert len(dbr) == 1

    async def test_transaction_update_and_delete(self, seeded):
        db, database, _ = seeded
        uname = f"txn_upd_del_{_rand_str(6)}"
        await db.insert(database, TABLE_USERS,
                        [{"username": uname, "email": "ud@test.com",
                          "age": 10, "score": 0, "is_active": 1, "bio": "to_update"}])

        async with db.transaction(database) as txn:
            await txn.update(database, TABLE_USERS,
                             update={"age": 99}, where={"username": uname})
            dbr = await txn.select(database, TABLE_USERS,
                                   params={"username": uname})
            assert dbr[0]["age"] == 99

            await txn.delete(database, TABLE_USERS, where={"username": uname})
            dbr = await txn.select(database, TABLE_USERS,
                                   params={"username": uname})
            assert len(dbr) == 0

        # Verify deleted outside txn
        dbr = await db.select(database, TABLE_USERS, params={"username": uname})
        assert len(dbr) == 0

    async def test_transaction_upsert(self, seeded):
        db, database, _ = seeded
        uname = f"txn_upsert_{_rand_str(6)}"

        async with db.transaction(database) as txn:
            await txn.insert(database, TABLE_USERS,
                             [{"username": uname, "email": "upsert@txn.com",
                               "age": 1, "score": 1.0, "is_active": 1, "bio": "v1"}])
            await txn.insert(database, TABLE_USERS,
                             [{"username": uname, "email": "upsert@txn.com",
                               "age": 2, "score": 2.0, "is_active": 1, "bio": "v2"}],
                             upsert=True)
            dbr = await txn.select(database, TABLE_USERS,
                                   params={"username": uname})
            assert len(dbr) == 1
            assert dbr[0]["age"] == 2

    async def test_multiple_rollbacks_dont_leak_connections(self, seeded):
        db, database, _ = seeded

        async def fail_txn(i):
            with pytest.raises(RuntimeError):
                async with db.transaction(database) as txn:
                    await txn.insert(database, TABLE_USERS,
                                     [{"username": f"leak_test_{i}_{_rand_str(4)}",
                                       "email": "l@l.com", "age": 0, "score": 0,
                                       "is_active": 0, "bio": "leak"}])
                    raise RuntimeError("intentional")

        # Do many failing transactions concurrently
        await asyncio.gather(*[fail_txn(i) for i in range(30)])

        # DB should still work fine
        dbr = await db.run("SELECT 1 AS ok", database)
        assert list(dbr)[0]["ok"] == 1


# ===================================================================
# TEST CLASS: JOIN Stress
# ===================================================================
class TestJoinStress:
    """Concurrent join_select operations."""

    async def test_join_select_basic(self, seeded):
        db, database, _ = seeded
        dbr = await db.join_select(
            database, TABLE_USERS, TABLE_ORDERS,
            on={"id": "user_id"},
            join_type="inner",
            limit=100,
        )
        assert dbr.successful is True or dbr.successful is None
        assert len(dbr) > 0
        # Should have columns from both tables
        keys = set(dbr[0].keys())
        assert "product" in keys or "quantity" in keys

    async def test_join_select_left(self, seeded):
        db, database, _ = seeded
        dbr = await db.join_select(
            database, TABLE_USERS, TABLE_ORDERS,
            on={"id": "user_id"},
            join_type="left",
            limit=200,
        )
        assert len(dbr) > 0

    async def test_join_select_with_params(self, seeded):
        db, database, _ = seeded
        dbr = await db.join_select(
            database, TABLE_USERS, TABLE_ORDERS,
            on={"id": "user_id"},
            join_type="inner",
            params={f"{TABLE_ORDERS}.status": "shipped"},
            limit=50,
        )
        for r in dbr:
            assert r.get("status") or r.get(f"{TABLE_ORDERS}_status") == "shipped"

    async def test_join_select_with_columns(self, seeded):
        db, database, _ = seeded
        dbr = await db.join_select(
            database, TABLE_USERS, TABLE_ORDERS,
            on={"id": "user_id"},
            columns=[f"{TABLE_USERS}.username", f"{TABLE_ORDERS}.product",
                     f"{TABLE_ORDERS}.price"],
            limit=50,
        )
        assert len(dbr) > 0
        row = dbr[0]
        assert "username" in row
        assert "product" in row
        assert "price" in row

    async def test_join_select_with_order_and_offset(self, seeded):
        db, database, _ = seeded
        dbr1 = await db.join_select(
            database, TABLE_USERS, TABLE_ORDERS,
            on={"id": "user_id"},
            order_by=f"{TABLE_ORDERS}.price",
            limit=10,
        )
        dbr2 = await db.join_select(
            database, TABLE_USERS, TABLE_ORDERS,
            on={"id": "user_id"},
            order_by=f"{TABLE_ORDERS}.price",
            limit=10, offset=10,
        )
        assert len(dbr1) > 0
        assert len(dbr2) > 0
        # Pages shouldn't overlap (unless duplicate prices)
        ids1 = {r.get(f"{TABLE_ORDERS}_id", r.get("id")) for r in dbr1}
        ids2 = {r.get(f"{TABLE_ORDERS}_id", r.get("id")) for r in dbr2}
        # They could overlap on id column names, but at least results should differ
        assert dbr1.records != dbr2.records

    async def test_concurrent_joins(self, seeded):
        db, database, _ = seeded

        async def join_query(limit):
            return await db.join_select(
                database, TABLE_USERS, TABLE_ORDERS,
                on={"id": "user_id"},
                join_type="inner",
                limit=limit,
            )

        results = await asyncio.gather(
            *[join_query(random.randint(10, 100)) for _ in range(50)]
        )
        for r in results:
            assert r.successful is True or r.successful is None
            assert len(r) > 0

    async def test_join_with_complex_filter(self, seeded):
        db, database, _ = seeded
        dbr = await db.join_select(
            database, TABLE_USERS, TABLE_ORDERS,
            on={"id": "user_id"},
            params={
                "_AND_": [
                    {f"{TABLE_USERS}.is_active": 1},
                    {f"{TABLE_ORDERS}.status": db.in_(["shipped", "delivered"])},
                ]
            },
            limit=50,
        )
        assert dbr.successful is True or dbr.successful is None


# ===================================================================
# TEST CLASS: count / exists / bulk_insert
# ===================================================================
class TestCountExistsBulkInsert:

    async def test_count_all(self, seeded):
        db, database, _ = seeded
        ct = await db.count(database, TABLE_USERS)
        assert ct >= SEED_USER_COUNT

    async def test_count_with_filter(self, seeded):
        db, database, _ = seeded
        ct_active = await db.count(database, TABLE_USERS,
                                   params={"is_active": 1})
        ct_inactive = await db.count(database, TABLE_USERS,
                                     params={"is_active": 0})
        ct_total = await db.count(database, TABLE_USERS)
        # active + inactive should be <= total (other stress tests may add rows)
        assert ct_active + ct_inactive <= ct_total
        assert ct_active > 0
        assert ct_inactive > 0

    async def test_exists_true(self, seeded):
        db, database, user_ids = seeded
        result = await db.exists(database, TABLE_USERS,
                                 params={"id": user_ids[0]})
        assert result is True

    async def test_exists_false(self, seeded):
        db, database, _ = seeded
        result = await db.exists(database, TABLE_USERS,
                                 params={"username": "absolutely_does_not_exist_xyz_999"})
        assert result is False

    async def test_concurrent_count_exists(self, seeded):
        db, database, user_ids = seeded

        async def do_count():
            return await db.count(database, TABLE_USERS)

        async def do_exists(uid):
            return await db.exists(database, TABLE_USERS, params={"id": uid})

        tasks = [do_count() for _ in range(25)]
        tasks += [do_exists(random.choice(user_ids)) for _ in range(25)]
        results = await asyncio.gather(*tasks)
        assert len(results) == 50
        for r in results[:25]:
            assert isinstance(r, int)
            assert r > 0
        for r in results[25:]:
            assert r is True

    async def test_bulk_insert(self, seeded):
        db, database, _ = seeded
        records = [{"username": f"bulk_{i}_{_rand_str(6)}",
                    "email": _rand_email(), "age": 25, "score": 0,
                    "is_active": 1, "bio": "bulk_test"}
                   for i in range(250)]
        results = await db.bulk_insert(database, TABLE_USERS, records,
                                       chunk_size=50)
        assert len(results) == 5  # 250 / 50
        for r in results:
            assert r.successful is True

    async def test_bulk_insert_single_chunk(self, seeded):
        db, database, _ = seeded
        records = [{"username": f"bulks_{i}_{_rand_str(6)}",
                    "email": _rand_email(), "age": 30, "score": 0,
                    "is_active": 1, "bio": "single_chunk"}
                   for i in range(10)]
        results = await db.bulk_insert(database, TABLE_USERS, records,
                                       chunk_size=100)
        assert len(results) == 1
        assert results[0].successful is True


# ===================================================================
# TEST CLASS: Type Edge Cases Under Load
# ===================================================================
class TestTypeEdgeCases:
    """Insert edge-case values into the types table concurrently."""

    async def test_insert_type_edge_cases(self, seeded):
        db, database, _ = seeded
        records = [
            {"val_int": 0, "val_neg": -2147483648, "val_float": -0.0,
             "val_bool": 0, "val_varchar": "", "val_text": "",
             "val_date": dt.date(2000, 1, 1),
             "val_datetime": dt.datetime(2000, 1, 1, 0, 0, 0),
             "val_time": dt.datetime(2000, 1, 1, 0, 0, 0),
             "val_json": json.dumps({}), "val_decimal": 0.0,
             "val_bigint": 0, "nullable_col": None},
            {"val_int": 2147483647, "val_neg": -1, "val_float": 3.14159,
             "val_bool": 1, "val_varchar": "x" * 500,
             "val_text": "y" * 10000,
             "val_date": dt.date(2099, 12, 31),
             "val_datetime": dt.datetime(2099, 12, 31, 23, 59, 59),
             "val_time": dt.datetime(2000, 1, 1, 23, 59, 59),
             "val_json": json.dumps({"nested": {"deep": [1, 2, 3]}}),
             "val_decimal": 999999.9999,
             "val_bigint": 9223372036854775807,
             "nullable_col": "not_null"},
            {"val_int": -999, "val_neg": -42, "val_float": -273.15,
             "val_bool": 1,
             "val_varchar": json.dumps({"key": "value"}),
             "val_text": "special chars: <>&\"'\\n\\t",
             "val_date": dt.date(1970, 1, 1),
             "val_datetime": dt.datetime(1970, 1, 1, 0, 0, 1),
             "val_time": dt.datetime(2000, 1, 1, 12, 30, 45),
             "val_json": json.dumps([1, "two", None, True]),
             "val_decimal": -0.0001,
             "val_bigint": -9223372036854775808,
             "nullable_col": None},
        ]
        dbr = await db.insert(database, TABLE_TYPES, records)
        assert dbr.successful is True

        # Read back and validate
        all_rows = await db.select(database, TABLE_TYPES, order_by="id")
        assert len(all_rows) >= 3

        row0 = all_rows[0]
        assert row0["val_neg"] == -2147483648
        assert row0["val_bigint"] == 0
        assert row0["nullable_col"] is None

        row1 = all_rows[1]
        assert row1["val_int"] == 2147483647
        assert row1["val_bigint"] == 9223372036854775807
        assert len(row1["val_varchar"]) == 500

        row2 = all_rows[2]
        assert row2["val_int"] == -999
        assert row2["val_neg"] == -42

    async def test_concurrent_type_inserts(self, seeded):
        db, database, _ = seeded

        async def insert_typed(i):
            rec = {
                "val_int": i * (-1 if i % 2 else 1),
                "val_neg": -i,
                "val_float": i * 0.1,
                "val_bool": i % 2,
                "val_varchar": f"concurrent_{i}",
                "val_text": f"text_{i}" * 100,
                "val_date": dt.date(2024, (i % 12) + 1, (i % 28) + 1),
                "val_datetime": dt.datetime(2024, 1, 1, i % 24, i % 60, 0),
                "val_time": dt.datetime(2000, 1, 1, i % 24, i % 60, 0),
                "val_json": json.dumps({"i": i}),
                "val_decimal": round(i * 1.1111, 4),
                "val_bigint": i * 1000000,
                "nullable_col": None if i % 3 == 0 else f"val_{i}",
            }
            return await db.insert(database, TABLE_TYPES, [rec])

        results = await asyncio.gather(*[insert_typed(i) for i in range(50)])
        for r in results:
            assert r.successful is True


# ===================================================================
# TEST CLASS: Error Path Concurrency
# ===================================================================
class TestErrorPathConcurrency:
    """Queries that error should not corrupt state or leak connections."""

    async def test_concurrent_errors_dont_break_pool(self, seeded):
        db, database, _ = seeded
        error_count = 0

        async def bad_query():
            nonlocal error_count
            try:
                await db.run("SELECT * FROM this_table_does_not_exist_abc_xyz", database)
            except Exception:
                error_count += 1

        async def good_query():
            dbr = await db.select(database, TABLE_USERS, limit=1)
            assert len(dbr) == 1

        # Interleave bad and good queries
        tasks = []
        for i in range(40):
            if i % 2 == 0:
                tasks.append(bad_query())
            else:
                tasks.append(good_query())

        await asyncio.gather(*tasks)
        assert error_count == 20

        # DB should still be fully functional
        ct = await db.count(database, TABLE_USERS)
        assert ct > 0

    async def test_duplicate_key_errors_concurrent(self, seeded):
        db, database, _ = seeded
        uname = f"dup_test_{_rand_str(6)}"
        await db.insert(database, TABLE_USERS,
                        [{"username": uname, "email": "dup@test.com",
                          "age": 1, "score": 0, "is_active": 1, "bio": "dup"}])
        error_count = 0

        async def try_dup():
            nonlocal error_count
            try:
                await db.insert(database, TABLE_USERS,
                                [{"username": uname, "email": "dup2@test.com",
                                  "age": 2, "score": 0, "is_active": 1, "bio": "dup2"}])
            except Exception:
                error_count += 1

        await asyncio.gather(*[try_dup() for _ in range(20)])
        assert error_count == 20

        # Still works
        dbr = await db.select(database, TABLE_USERS, params={"username": uname})
        assert len(dbr) == 1


# ===================================================================
# TEST CLASS: Pool Exhaustion
# ===================================================================
class TestPoolExhaustion:
    """Try to exhaust the connection pool with a tiny pool."""

    async def test_tiny_pool_under_heavy_load(self, adb_tiny_pool, seeded):
        """50 concurrent queries on a pool_size=2, max_overflow=1 pool."""
        db = adb_tiny_pool
        _, database, _ = seeded

        async def query(i):
            dbr = await db.select(database, TABLE_USERS, limit=5)
            return len(dbr)

        results = await asyncio.gather(*[query(i) for i in range(50)])
        for r in results:
            assert r == 5

    async def test_rapid_run_queries_single_use_engines(self, seeded):
        """run() creates single-use engines. Rapid fire should not leak."""
        db, database, _ = seeded

        async def run_query(i):
            dbr = await db.run(f"SELECT {i} AS val", database)
            return list(dbr)[0]["val"]

        results = await asyncio.gather(*[run_query(i) for i in range(100)])
        assert sorted(results) == list(range(100))


# ===================================================================
# TEST CLASS: Data Integrity
# ===================================================================
class TestDataIntegrity:
    """Verify data consistency after all the chaos."""

    async def test_seed_data_intact(self, seeded):
        """The original seed data should still be queryable."""
        db, database, user_ids = seeded
        # Check a random sample of original users still exist
        sample = random.sample(user_ids, min(20, len(user_ids)))
        for uid in sample:
            dbr = await db.select(database, TABLE_USERS, params={"id": uid})
            assert len(dbr) == 1, f"User {uid} missing"

    async def test_order_foreign_keys_intact(self, seeded):
        """All orders should reference valid users."""
        db, database, _ = seeded
        orphan_check = await db.run(f"""
            SELECT o.id FROM `{TABLE_ORDERS}` o
            LEFT JOIN `{TABLE_USERS}` u ON o.user_id = u.id
            WHERE u.id IS NULL
            LIMIT 1
        """, database)
        assert len(list(orphan_check)) == 0, "Found orphaned orders"

    async def test_select_all_orders_consistent(self, seeded):
        db, database, _ = seeded
        ct = await db.count(database, TABLE_ORDERS)
        assert ct >= SEED_ORDER_COUNT

        all_records = []
        async for page in db.select_pages(database, TABLE_ORDERS,
                                          order_by="id", page_size=500):
            all_records.extend(page)
        assert len(all_records) == ct

    async def test_one_or_none_property(self, seeded):
        db, database, user_ids = seeded
        dbr = await db.select(database, TABLE_USERS,
                              params={"id": user_ids[0]}, limit=1)
        result = dbr.one_or_none
        assert result is not None
        assert result["id"] == user_ids[0]

        dbr_empty = await db.select(database, TABLE_USERS,
                                    params={"username": "nonexistent_zzzzzz"})
        assert dbr_empty.one_or_none is None

    async def test_response_iteration(self, seeded):
        db, database, _ = seeded
        dbr = await db.select(database, TABLE_USERS, limit=10, order_by="id")
        # Test __iter__
        ids_iter = [r["id"] for r in dbr]
        # Test __getitem__
        ids_index = [dbr[i]["id"] for i in range(len(dbr))]
        assert ids_iter == ids_index
        # Test __len__
        assert len(dbr) == 10
        # Test row_count
        assert dbr.row_count == 10

    async def test_clear_table_and_recount(self, seeded):
        """Clear the types table and verify count goes to zero."""
        db, database, _ = seeded
        # Insert something first
        await db.insert(database, TABLE_TYPES,
                        [{"val_int": 999, "val_neg": -1, "val_float": 0,
                          "val_bool": 0, "val_varchar": "clear_test",
                          "val_text": "", "val_date": dt.date(2024, 1, 1),
                          "val_datetime": dt.datetime(2024, 1, 1),
                          "val_time": dt.datetime(2000, 1, 1, 12, 0, 0),
                          "val_json": "{}", "val_decimal": 0,
                          "val_bigint": 0, "nullable_col": None}])
        ct_before = await db.count(database, TABLE_TYPES)
        assert ct_before > 0

        await db.clear_table(database, TABLE_TYPES)
        ct_after = await db.count(database, TABLE_TYPES)
        assert ct_after == 0
