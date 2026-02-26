# -*- coding: utf-8 -*-
from dbmasta.response import DataBaseResponseBase

class DataBaseResponse(DataBaseResponseBase):

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
            raise
        finally:
            try:
                result.close()
            except Exception:
                pass