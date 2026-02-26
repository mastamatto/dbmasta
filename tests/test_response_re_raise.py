"""
Test that sync response _receive() re-raises so callers see fetch/result errors (audit fix).
"""
import pytest
from unittest.mock import MagicMock

from dbmasta.db_client.response import DataBaseResponse


class TestSyncResponseReRaise:
    """DataBaseResponse._receive must re-raise on exception."""

    def test_receive_re_raises_on_failure(self):
        """When _receive() gets a result that raises on fetchall(), exception propagates."""
        dbr = DataBaseResponse(None)
        result = MagicMock()
        result.returns_rows = True
        result.keys = MagicMock(return_value=["a"])
        result.fetchall = MagicMock(side_effect=RuntimeError("fetch failed"))
        with pytest.raises(RuntimeError, match="fetch failed"):
            dbr._receive(result)
        assert dbr.successful is False
        assert dbr.error_info is not None
