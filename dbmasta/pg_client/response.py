# -*- coding: utf-8 -*-
from dbmasta.response import DataBaseResponseBase
from sqlalchemy.dialects import postgresql


class DataBaseResponse(DataBaseResponseBase):
    def __init__(self, query, **kwargs):
        super().__init__(query, **kwargs)
        # Override raw_query to use PostgreSQL dialect
        if query is not None:
            self.raw_query = str(query.compile(
                dialect=postgresql.dialect(),
                compile_kwargs={"literal_binds": False})
            )

    @classmethod
    def default(cls, database: str):
        dbr = cls(None)
        dbr.database = database
        return dbr

    def _receive(self, result):
        try:
            self.successful = True
            self.returns_rows = result.returns_rows
            if result.returns_rows:
                self.keys = list(result.keys())
                data = result.fetchall()
                self.records = list(self.build_records(data))
        except Exception as e:
            self._handle_error(e)
            raise
        finally:
            try:
                result.close()
            except Exception:
                pass
