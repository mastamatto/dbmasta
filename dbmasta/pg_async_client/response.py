# -*- coding: utf-8 -*-
from dbmasta.response import DataBaseResponseBase
from sqlalchemy.dialects import postgresql


class DataBaseResponse(DataBaseResponseBase):
    def __init__(self, query, schema: str = None, **kwargs):
        super().__init__(query, **kwargs)
        self.schema = schema
        # Override raw_query to use PostgreSQL dialect
        if query is not None:
            self.raw_query = str(query.compile(
                dialect=postgresql.dialect(),
                compile_kwargs={"literal_binds": False})
            )

    @classmethod
    def default(cls, schema: str):
        dbr = cls(None, schema=schema)
        return dbr

    async def _receive(self, result):
        try:
            self.successful = True
            self.returns_rows = result.returns_rows
            if result.returns_rows:
                self.keys = list(result.keys())
                data = result.fetchall()
                self.records = list(self.build_records(data))
        except Exception as e:
            self._handle_error(e)
            if self.auto_raise_errors:
                self.raise_for_error()
        finally:
            try:
                result.close()
            except Exception:
                pass
