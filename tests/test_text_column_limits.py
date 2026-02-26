"""
Test that TINYTEXT, TEXT, MEDIUMTEXT, LONGTEXT (and json-like column) truncate at the correct limits.

Ensures we don't incorrectly limit everything to 255 chars (VARCHAR default); each type
must cap at its intended length: TINYTEXT=255, TEXT=65535, MEDIUMTEXT=16777215, LONGTEXT=~4GB.
"""
import pytest

TEST_TABLE_TEXT_LIMITS = "_dbmasta_audit_text_limits"

# Lengths we use in tests (chars)
TINYTEXT_MAX = 255
TEXT_MIN_BEYOND_TINY = 700   # well above 255, below 65535
MEDIUM_LONG_LEN = 1000       # for medium/long/json columns


def _make_str(n: int, prefix: str = "x") -> str:
    return (prefix * (n // len(prefix) + 1))[:n]


class TestTextColumnLimitsSync:
    """Sync client: verify each text type truncates at the right place."""

    def test_tinytext_truncates_at_255(self, sync_db_with_text_limits_table):
        sync_db, database = sync_db_with_text_limits_table
        table = TEST_TABLE_TEXT_LIMITS
        # Insert 300 chars into TINYTEXT -> must be truncated to 255
        long_300 = _make_str(300)
        sync_db.insert(database, table, [{
            "txt_tiny": long_300,
            "txt_text": _make_str(100, "a"),
            "txt_medium": _make_str(100, "b"),
            "txt_long": _make_str(100, "c"),
            "payload_json": '{"k": "v"}',
        }])
        dbr = sync_db.select(database, table, columns=["txt_tiny", "txt_text", "txt_medium", "txt_long", "payload_json"], limit=1)
        assert len(dbr) >= 1
        row = dbr[0]
        assert len(row["txt_tiny"]) == TINYTEXT_MAX, "TINYTEXT must be truncated to 255, not 300"
        assert row["txt_tiny"] == long_300[:TINYTEXT_MAX]

    def test_text_accepts_beyond_255(self, sync_db_with_text_limits_table):
        sync_db, database = sync_db_with_text_limits_table
        table = TEST_TABLE_TEXT_LIMITS
        text_700 = _make_str(TEXT_MIN_BEYOND_TINY, "t")
        sync_db.insert(database, table, [{
            "txt_tiny": "short",
            "txt_text": text_700,
            "txt_medium": _make_str(100, "b"),
            "txt_long": _make_str(100, "c"),
            "payload_json": "{}",
        }])
        dbr = sync_db.select(database, table, columns=["txt_text"], limit=1, order_by="id", reverse=True)
        assert len(dbr) >= 1
        assert len(dbr[0]["txt_text"]) == TEXT_MIN_BEYOND_TINY, (
            "TEXT must not truncate to 255; expected 700 chars"
        )
        assert dbr[0]["txt_text"] == text_700

    def test_mediumtext_accepts_large_string(self, sync_db_with_text_limits_table):
        sync_db, database = sync_db_with_text_limits_table
        table = TEST_TABLE_TEXT_LIMITS
        medium_1000 = _make_str(MEDIUM_LONG_LEN, "m")
        sync_db.insert(database, table, [{
            "txt_tiny": "short",
            "txt_text": "short",
            "txt_medium": medium_1000,
            "txt_long": _make_str(100, "c"),
            "payload_json": "{}",
        }])
        dbr = sync_db.select(database, table, columns=["txt_medium"], limit=1, order_by="id", reverse=True)
        assert len(dbr) >= 1
        assert len(dbr[0]["txt_medium"]) == MEDIUM_LONG_LEN
        assert dbr[0]["txt_medium"] == medium_1000

    def test_longtext_accepts_large_string(self, sync_db_with_text_limits_table):
        sync_db, database = sync_db_with_text_limits_table
        table = TEST_TABLE_TEXT_LIMITS
        long_1000 = _make_str(MEDIUM_LONG_LEN, "L")
        sync_db.insert(database, table, [{
            "txt_tiny": "short",
            "txt_text": "short",
            "txt_medium": "short",
            "txt_long": long_1000,
            "payload_json": "{}",
        }])
        dbr = sync_db.select(database, table, columns=["txt_long"], limit=1, order_by="id", reverse=True)
        assert len(dbr) >= 1
        assert len(dbr[0]["txt_long"]) == MEDIUM_LONG_LEN
        assert dbr[0]["txt_long"] == long_1000

    def test_json_like_column_accepts_large_payload(self, sync_db_with_text_limits_table):
        """payload_json is LONGTEXT; must not truncate to 255 (e.g. JSON payloads)."""
        sync_db, database = sync_db_with_text_limits_table
        table = TEST_TABLE_TEXT_LIMITS
        json_like = '{"data": "' + _make_str(MEDIUM_LONG_LEN - 20, "j") + '"}'
        sync_db.insert(database, table, [{
            "txt_tiny": "short",
            "txt_text": "short",
            "txt_medium": "short",
            "txt_long": "short",
            "payload_json": json_like,
        }])
        dbr = sync_db.select(database, table, columns=["payload_json"], limit=1, order_by="id", reverse=True)
        assert len(dbr) >= 1
        got = dbr[0]["payload_json"]
        assert len(got) == len(json_like), (
            f"JSON-like (LONGTEXT) column must not truncate to 255; got len={len(got)}, expected {len(json_like)}"
        )
        assert got == json_like


class TestTextColumnLimitsAsync:
    """Async client: same checks for TINYTEXT, TEXT, MEDIUMTEXT, LONGTEXT, json-like."""

    async def test_tinytext_truncates_at_255(self, async_db_with_text_limits_table):
        async_db, database = async_db_with_text_limits_table
        table = TEST_TABLE_TEXT_LIMITS
        long_300 = _make_str(300)
        await async_db.insert(database, table, [{
            "txt_tiny": long_300,
            "txt_text": _make_str(100, "a"),
            "txt_medium": _make_str(100, "b"),
            "txt_long": _make_str(100, "c"),
            "payload_json": '{"k": "v"}',
        }])
        dbr = await async_db.select(database, table, columns=["txt_tiny"], limit=1, order_by="id", reverse=True)
        assert len(dbr) >= 1
        assert len(dbr[0]["txt_tiny"]) == TINYTEXT_MAX
        assert dbr[0]["txt_tiny"] == long_300[:TINYTEXT_MAX]

    async def test_text_accepts_beyond_255(self, async_db_with_text_limits_table):
        async_db, database = async_db_with_text_limits_table
        table = TEST_TABLE_TEXT_LIMITS
        text_700 = _make_str(TEXT_MIN_BEYOND_TINY, "t")
        await async_db.insert(database, table, [{
            "txt_tiny": "short",
            "txt_text": text_700,
            "txt_medium": "short",
            "txt_long": "short",
            "payload_json": "{}",
        }])
        dbr = await async_db.select(database, table, columns=["txt_text"], limit=1, order_by="id", reverse=True)
        assert len(dbr) >= 1
        assert len(dbr[0]["txt_text"]) == TEXT_MIN_BEYOND_TINY
        assert dbr[0]["txt_text"] == text_700

    async def test_mediumtext_accepts_large_string(self, async_db_with_text_limits_table):
        async_db, database = async_db_with_text_limits_table
        table = TEST_TABLE_TEXT_LIMITS
        medium_1000 = _make_str(MEDIUM_LONG_LEN, "m")
        await async_db.insert(database, table, [{
            "txt_tiny": "short",
            "txt_text": "short",
            "txt_medium": medium_1000,
            "txt_long": "short",
            "payload_json": "{}",
        }])
        dbr = await async_db.select(database, table, columns=["txt_medium"], limit=1, order_by="id", reverse=True)
        assert len(dbr) >= 1
        assert len(dbr[0]["txt_medium"]) == MEDIUM_LONG_LEN
        assert dbr[0]["txt_medium"] == medium_1000

    async def test_longtext_accepts_large_string(self, async_db_with_text_limits_table):
        async_db, database = async_db_with_text_limits_table
        table = TEST_TABLE_TEXT_LIMITS
        long_1000 = _make_str(MEDIUM_LONG_LEN, "L")
        await async_db.insert(database, table, [{
            "txt_tiny": "short",
            "txt_text": "short",
            "txt_medium": "short",
            "txt_long": long_1000,
            "payload_json": "{}",
        }])
        dbr = await async_db.select(database, table, columns=["txt_long"], limit=1, order_by="id", reverse=True)
        assert len(dbr) >= 1
        assert len(dbr[0]["txt_long"]) == MEDIUM_LONG_LEN
        assert dbr[0]["txt_long"] == long_1000

    async def test_json_like_column_accepts_large_payload(self, async_db_with_text_limits_table):
        json_like = '{"data": "' + _make_str(MEDIUM_LONG_LEN - 20, "j") + '"}'
        async_db, database = async_db_with_text_limits_table
        table = TEST_TABLE_TEXT_LIMITS
        await async_db.insert(database, table, [{
            "txt_tiny": "short",
            "txt_text": "short",
            "txt_medium": "short",
            "txt_long": "short",
            "payload_json": json_like,
        }])
        dbr = await async_db.select(database, table, columns=["payload_json"], limit=1, order_by="id", reverse=True)
        assert len(dbr) >= 1
        got = dbr[0]["payload_json"]
        assert len(got) == len(json_like), (
            f"JSON-like (LONGTEXT) must not truncate to 255; got len={len(got)}, expected {len(json_like)}"
        )
        assert got == json_like
