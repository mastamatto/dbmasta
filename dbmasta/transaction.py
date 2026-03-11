"""Transaction support for sync and async database clients."""
from sqlalchemy import text as sql_text
from sqlalchemy.sql import select, update as sql_update, delete
from sqlalchemy.dialects.mysql import insert as mysql_insert
from sqlalchemy.dialects.postgresql import insert as pg_insert


class SyncTransaction:
    """Context manager wrapping a single connection + transaction.

    Usage::

        with db.transaction("mydb") as txn:
            txn.insert("mydb", "orders", [{"item": "widget", "qty": 5}])
            txn.update("mydb", "inventory", update={"qty": 10}, where={"item": "widget"})
            # auto-commits on clean exit, auto-rollback on exception
    """

    def __init__(self, db, database: str):
        self._db = db
        self._database = database
        self._engine = None
        self._connection = None
        self._trans = None

    def __enter__(self):
        self._engine = self._db.engine_manager.get_engine(self._database)
        self._connection = self._engine.ctx.connect()
        self._trans = self._connection.begin()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            if exc_type is None:
                self._trans.commit()
            else:
                self._trans.rollback()
        finally:
            self._connection.close()
            self._connection = None
            self._trans = None
        return False

    def _execute_on_connection(self, query, params=None):
        return self._connection.execute(query, parameters=params or {})

    def execute(self, query, **dbr_args):
        ResponseClass = self._get_response_class()
        dbr = ResponseClass(query, **dbr_args)
        result = self._execute_on_connection(query)
        dbr._receive(result)
        return dbr

    def run(self, query, database=None, *, params=None, **dbr_args):
        if isinstance(query, str):
            query = sql_text(query)
        ResponseClass = self._get_response_class()
        dbr = ResponseClass(query, **dbr_args)
        result = self._execute_on_connection(query, params)
        dbr._receive(result)
        return dbr

    def select(self, database: str, table_name: str, params=None, columns=None,
               distinct=False, order_by=None, offset=None, limit=None,
               reverse=None, textual=False, response_model=None,
               as_decimals=False):
        from sqlalchemy import asc, desc
        table = self._db.get_table(database, table_name)
        if columns:
            query = select(*[table.c[col] for col in columns])
        else:
            query = select(table)
        if distinct:
            query = query.distinct()
        if params:
            query = self._db._construct_conditions(query, table, params)
        if order_by:
            reverse = False if reverse is None else reverse
            dir_f = asc if not reverse else desc
            query = query.order_by(dir_f(table.c[order_by]))
        if limit:
            offset = 0 if offset is None else offset
            query = query.limit(limit).offset(offset)
        if textual:
            return self._db.textualize(query)
        ResponseClass = self._get_response_class()
        dbr = ResponseClass(query, response_model=response_model, as_decimals=as_decimals)
        result = self._execute_on_connection(query)
        dbr._receive(result)
        return dbr

    def insert(self, database: str, table_name: str, records: list,
               upsert=False, textual=False, **kwargs):
        table = self._db.get_table(database, table_name)
        insert_fn = self._get_insert_fn()
        # Type conversion
        if hasattr(self._db, 'correct_types'):
            if self._is_mysql():
                records = self._db.correct_types(database, table_name, records, table=table)
            else:
                records = self._db.correct_types(database, table_name, records)
        if upsert:
            query = self._build_upsert(table, records, insert_fn, **kwargs)
        else:
            query = insert_fn(table).values(records)
        if textual:
            return self._db.textualize(query)
        ResponseClass = self._get_response_class()
        dbr = ResponseClass(query)
        result = self._execute_on_connection(query)
        dbr._receive(result)
        return dbr

    def update(self, database: str, table_name: str,
               update: dict = None, where: dict = None, textual=False):
        update = update or {}
        where = where or {}
        table = self._db.get_table(database, table_name)
        query = sql_update(table)
        query = self._db._construct_conditions(query, table, where)
        query = query.values(**update)
        if textual:
            return self._db.textualize(query)
        ResponseClass = self._get_response_class()
        dbr = ResponseClass(query)
        result = self._execute_on_connection(query)
        dbr._receive(result)
        return dbr

    def delete(self, database: str, table_name: str,
               where: dict = None, textual=False):
        where = where or {}
        table = self._db.get_table(database, table_name)
        query = delete(table)
        query = self._db._construct_conditions(query, table, where)
        if textual:
            return self._db.textualize(query)
        ResponseClass = self._get_response_class()
        dbr = ResponseClass(query)
        result = self._execute_on_connection(query)
        dbr._receive(result)
        return dbr

    def _is_mysql(self):
        return self._db.auth.dialect == "mysql"

    def _get_insert_fn(self):
        return mysql_insert if self._is_mysql() else pg_insert

    def _build_upsert(self, table, records, insert_fn, **kwargs):
        if self._is_mysql():
            update_keys = kwargs.get('update_keys', None)
            if update_keys is None:
                update_keys = [c.key for c in table.c]
            stmt = insert_fn(table).values(records)
            update_dict = {k: stmt.inserted[k] for k in update_keys}
            return stmt.on_duplicate_key_update(**update_dict)
        else:
            update_fields = kwargs.get('update_fields', None)
            if update_fields is None:
                update_fields = [c.key for c in table.c]
            stmt = insert_fn(table).values(records)
            pk_keys = [k.key for k in table.primary_key.columns]
            update_dict = {k: stmt.excluded[k] for k in update_fields}
            return stmt.on_conflict_do_update(index_elements=pk_keys, set_=update_dict)

    def _get_response_class(self):
        if self._is_mysql():
            from dbmasta.db_client.response import DataBaseResponse
        else:
            from dbmasta.pg_client.response import DataBaseResponse
        return DataBaseResponse


class AsyncTransaction:
    """Async context manager wrapping a single connection + transaction.

    Usage::

        async with db.transaction("mydb") as txn:
            await txn.insert("mydb", "orders", [{"item": "widget", "qty": 5}])
            await txn.update("mydb", "inventory", update={"qty": 10}, where={"item": "widget"})
    """

    def __init__(self, db, database: str):
        self._db = db
        self._database = database
        self._engine = None
        self._connection = None
        self._trans = None

    async def __aenter__(self):
        self._engine = self._db.engine_manager.get_engine(self._database)
        self._connection = await self._engine.ctx.connect()
        self._trans = await self._connection.begin()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        try:
            if exc_type is None:
                await self._trans.commit()
            else:
                await self._trans.rollback()
        finally:
            await self._connection.close()
            self._connection = None
            self._trans = None
        return False

    async def _execute_on_connection(self, query, params=None):
        return await self._connection.execute(query, parameters=params or {})

    async def execute(self, query, **dbr_args):
        ResponseClass = self._get_response_class()
        dbr = ResponseClass(query, **dbr_args)
        result = await self._execute_on_connection(query)
        await dbr._receive(result)
        return dbr

    async def run(self, query, database=None, *, params=None, **dbr_args):
        if isinstance(query, str):
            query = sql_text(query)
        ResponseClass = self._get_response_class()
        dbr = ResponseClass(query, **dbr_args)
        result = await self._execute_on_connection(query, params)
        await dbr._receive(result)
        return dbr

    async def select(self, database: str, table_name: str, params=None, columns=None,
                     distinct=False, order_by=None, offset=None, limit=None,
                     reverse=None, textual=False, response_model=None,
                     as_decimals=False):
        from sqlalchemy import asc, desc
        table = await self._db.get_table(database, table_name)
        if columns:
            query = select(*[table.c[col] for col in columns])
        else:
            query = select(table)
        if distinct:
            query = query.distinct()
        if params:
            query = self._db._construct_conditions(query, table, params)
        if order_by:
            reverse = False if reverse is None else reverse
            dir_f = asc if not reverse else desc
            query = query.order_by(dir_f(table.c[order_by]))
        if limit:
            offset = 0 if offset is None else offset
            query = query.limit(limit).offset(offset)
        if textual:
            return self._db.textualize(query)
        ResponseClass = self._get_response_class()
        dbr = ResponseClass(query, response_model=response_model, as_decimals=as_decimals)
        result = await self._execute_on_connection(query)
        await dbr._receive(result)
        return dbr

    async def insert(self, database: str, table_name: str, records: list,
                     upsert=False, textual=False, **kwargs):
        table = await self._db.get_table(database, table_name)
        insert_fn = self._get_insert_fn()
        if hasattr(self._db, 'correct_types'):
            if self._is_mysql():
                records = await self._db.correct_types(database, table_name, records, table=table)
            else:
                records = await self._db.correct_types(database, table_name, records)
        if upsert:
            query = self._build_upsert(table, records, insert_fn, **kwargs)
        else:
            query = insert_fn(table).values(records)
        if textual:
            return self._db.textualize(query)
        ResponseClass = self._get_response_class()
        dbr = ResponseClass(query)
        result = await self._execute_on_connection(query)
        await dbr._receive(result)
        return dbr

    async def update(self, database: str, table_name: str,
                     update: dict = None, where: dict = None, textual=False):
        update = update or {}
        where = where or {}
        table = await self._db.get_table(database, table_name)
        query = sql_update(table)
        query = self._db._construct_conditions(query, table, where)
        query = query.values(**update)
        if textual:
            return self._db.textualize(query)
        ResponseClass = self._get_response_class()
        dbr = ResponseClass(query)
        result = await self._execute_on_connection(query)
        await dbr._receive(result)
        return dbr

    async def delete(self, database: str, table_name: str,
                     where: dict = None, textual=False):
        where = where or {}
        table = await self._db.get_table(database, table_name)
        query = delete(table)
        query = self._db._construct_conditions(query, table, where)
        if textual:
            return self._db.textualize(query)
        ResponseClass = self._get_response_class()
        dbr = ResponseClass(query)
        result = await self._execute_on_connection(query)
        await dbr._receive(result)
        return dbr

    def _is_mysql(self):
        return self._db.auth.dialect == "mysql"

    def _get_insert_fn(self):
        return mysql_insert if self._is_mysql() else pg_insert

    def _get_response_class(self):
        if self._is_mysql():
            from dbmasta.async_db_client.response import DataBaseResponse
        else:
            from dbmasta.pg_async_client.response import DataBaseResponse
        return DataBaseResponse

    def _build_upsert(self, table, records, insert_fn, **kwargs):
        if self._is_mysql():
            update_keys = kwargs.get('update_keys', None)
            if update_keys is None:
                update_keys = [c.key for c in table.c]
            stmt = insert_fn(table).values(records)
            update_dict = {k: stmt.inserted[k] for k in update_keys}
            return stmt.on_duplicate_key_update(**update_dict)
        else:
            update_fields = kwargs.get('update_fields', None)
            if update_fields is None:
                update_fields = [c.key for c in table.c]
            stmt = insert_fn(table).values(records)
            pk_keys = [k.key for k in table.primary_key.columns]
            update_dict = {k: stmt.excluded[k] for k in update_fields}
            return stmt.on_conflict_do_update(index_elements=pk_keys, set_=update_dict)
