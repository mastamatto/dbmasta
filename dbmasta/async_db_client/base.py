from sqlalchemy import (text as sql_text, asc, desc, null as sql_null, func, label)
from sqlalchemy.sql import (and_, or_, 
                            not_ as sql_not, 
                            select, 
                            #insert, 
                            update as sql_update, 
                            delete,
                            join as sql_join
                            )
from sqlalchemy.dialects.mysql import insert
import datetime as dt
from typing import AsyncGenerator, Any, Optional
from dbmasta.authorization import Authorization, ENGINE
from dbmasta.exceptions import SchemaQueryError, MissingColumnError
from .response import DataBaseResponse
from dbmasta.sql_types import type_map
from .tables import TableCache
from .engine import Engine, EngineManager
from dbmasta.errors import is_fatal_mysql_connection_error
from dbmasta.retry import async_retry
import asyncio, logging, time, traceback

logger = logging.getLogger(__name__)

DB_CONCURRENCY_DEFAULT = 10
DB_EXECUTE_TIMEOUT_DEFAULT = 30
_HEADER_CACHE_TTL = 1800  # 30 minutes

# Map reflected SQLAlchemy column type class names to type_map keys (MySQL / generic)
_SA_TYPE_NAME_TO_MAP_KEY = {
    "Integer": "INT", "INTEGER": "INT",
    "SmallInteger": "SMALLINT", "SMALLINT": "SMALLINT",
    "BigInteger": "BIGINT", "BIGINT": "BIGINT",
    "TINYINT": "TINYINT", "MEDIUMINT": "MEDIUMINT",
    "String": "VARCHAR", "VARCHAR": "VARCHAR", "CHAR": "CHAR", "NVARCHAR": "VARCHAR",
    "Text": "TEXT", "TEXT": "TEXT", "LONGTEXT": "LONGTEXT", "MEDIUMTEXT": "MEDIUMTEXT", "TINYTEXT": "TINYTEXT",
    "Boolean": "BOOL", "BOOLEAN": "BOOL",
    "DateTime": "DATETIME", "DATETIME": "DATETIME",
    "Date": "DATE", "DATE": "DATE",
    "Time": "TIME", "TIME": "TIME",
    "Timestamp": "TIMESTAMP", "TIMESTAMP": "TIMESTAMP",
    "Numeric": "DECIMAL", "DECIMAL": "DECIMAL", "Float": "FLOAT", "FLOAT": "FLOAT", "DOUBLE": "DOUBLE",
    "LargeBinary": "BLOB", "BLOB": "BLOB", "TINYBLOB": "TINYBLOB", "MEDIUMBLOB": "MEDIUMBLOB", "LONGBLOB": "LONGBLOB",
    "ENUM": "ENUM", "JSON": "LONGTEXT",  # JSON: use LONGTEXT so large payloads (e.g. json) aren't truncated
}

class AsyncDataBase():
    Authorization = Authorization
    
    BLDRS = {
        'select': select,
        'insert': insert,
        'update': sql_update,
        'delete': delete,
        'text': sql_text,
        'and': and_,
        'or': or_,
        'not': sql_not,
        'asc': asc,
        'desc': desc,
        'null': sql_null,
        'join': sql_join
    }
    
    
    def __init__(self,
                 auth:Authorization,
                 debug:bool=False,
                 on_new_table=None,
                 on_query_error=None,
                 ignore_event_errors:bool=False,
                 auto_raise_errors:bool=False,
                 max_db_concurrency:int=DB_CONCURRENCY_DEFAULT,
                 db_exec_timeout:int=DB_EXECUTE_TIMEOUT_DEFAULT,
                 engine_config:dict|None=None
                 ):
        self.auth = auth
        self.database     = auth.default_database
        self.debug        = debug
        self.table_cache  = {}
        self._header_info_cache = {}  # (database, table_name) -> coldata dict for correct_types
        self.ignore_event_errors = ignore_event_errors
        self.event_handlers = {
            "on_new_table": on_new_table,
            "on_query_error": on_query_error
        }
        engine_config = engine_config or dict()
        self.engine_manager = EngineManager(db=self, **engine_config)
        self.auto_raise_errors = auto_raise_errors
        self._max_db_concurrency = max_db_concurrency
        self._db_exec_timeout:int = db_exec_timeout
        self._db_semaphor = asyncio.Semaphore(self.max_db_concurrency)
        
    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        await self.kill_engines()
        return False

    @property
    def max_db_concurrency(self) -> int:
        return self._max_db_concurrency
    
    @max_db_concurrency.setter
    def max_db_concurrency(self, value:int):
        self._max_db_concurrency = value or DB_CONCURRENCY_DEFAULT
        self._db_semaphor = asyncio.Semaphore(self._max_db_concurrency)
        
    ### INITIALIZERS
    @classmethod
    def env(cls, engine:ENGINE="pymysql", debug:bool=False):
        auth = Authorization.env(engine)
        return cls(auth, debug=debug)


    def engine(self, database:str, single_use:bool=False) -> Engine:
        if single_use:
            engine = self.engine_manager.get_temporary_engine(database)
        else:
            engine = self.engine_manager.get_engine(database)
        return engine


    async def kill_engines(self):
        await self.engine_manager.dispose_all()


    async def preload_tables(self, *db_tbls:list[tuple[str,str]])->None:
        for db,tbl in db_tbls:
            _ = await self.get_table(db, tbl)


    async def raise_event(self, event:str, *a, **kw):
        hnd = self.event_handlers.get(event, None)
        if hnd:
            try:
                if asyncio.iscoroutinefunction(hnd):
                    await hnd(*a, **kw)
                else:
                    hnd(*a, **kw)
            except Exception as err:
                if not self.ignore_event_errors:
                    raise err # re-raise


    async def get_table(self, database:str, table_name:str):
        engine = self.engine_manager.get_engine(database)
        table_cache = self.table_cache.get((database, table_name), None)
        if table_cache is None:
            table_cache = await TableCache.new(database, table_name, engine.ctx)
            await self.raise_event("on_new_table", table_cache)
        elif table_cache.expired:
            await table_cache.reset(engine.ctx)
        self.table_cache[(database,table_name)] = table_cache
        return table_cache.table


    async def run(self, query, database=None, *, params:dict=None, timeout:Optional[int]=None, **dbr_args):
        database = self.database if database is None else database
        engine = self.engine_manager.get_engine(database)
        if isinstance(query, str):
            query = sql_text(query)
        dbr = DataBaseResponse(query, auto_raise_errors=self.auto_raise_errors, **dbr_args) # can be overwritten
        try:
            dbr = await self.execute(engine.ctx, query, auto_commit=not query.is_select, params=params, timeout=timeout, **dbr_args)
        except Exception as e:
            logger.warning("An error occurred running custom query")
            dbr.error_info = e.__repr__()
            tb = traceback.format_exc()
            await self.raise_event("on_query_error", exc=e, tb=tb, dbr=dbr)
            raise
        finally:
            if engine.single_use:
                await engine.kill()
        return dbr

    async def _execute_and_commit(self, connection, query, auto_commit, params:dict=None):
        result = await connection.execute(query, parameters=params or {})
        if auto_commit:
            await connection.commit()
        return result

    @async_retry(max_retries=2, backoff_base=0.5)
    async def execute(self, engine, query, *, auto_commit:bool=True, params:dict=None, timeout:Optional[int]=None, **dbr_args) -> DataBaseResponse:
        dbr = DataBaseResponse(query, auto_raise_errors=self.auto_raise_errors, **dbr_args)
        effective_timeout = timeout if timeout is not None else self._db_exec_timeout
        async with self._db_semaphor:
            async with engine.connect() as connection:
                try:
                    if effective_timeout is not None:
                        result = await asyncio.wait_for(
                            self._execute_and_commit(connection, query, auto_commit, params),
                            effective_timeout
                        )
                    else:
                        result = await self._execute_and_commit(connection, query, auto_commit, params)
                    await dbr._receive(result) # handles closure
                except asyncio.TimeoutError as e:
                    dbr.error_info = str(e)
                    dbr.successful = False
                    raise
                except Exception as e:
                    if is_fatal_mysql_connection_error(e):
                        connection.invalidate()
                    dbr.error_info = str(e)
                    dbr.successful = False
                    raise
        return dbr


    def convert_vals(self, key:str, value:object, 
                     coldata:dict, **kwargs):
        if key not in coldata:
            raise MissingColumnError(
                f"Table schema empty or missing column {key!r}; "
                "schema query may have failed or table does not exist."
            )
        col = coldata[key]
        kwargs.update(col)
        # used for datatypes on 'write' queries
        val = col['DATA_TYPE'](value, **kwargs)
        return val.value


    def convert_header_info(self, mapp, value):
        if mapp == 'IS_NULLABLE':
            return value == 'YES'
        elif mapp == 'DATA_TYPE':
            return type_map[str(value).upper()]
        elif mapp == 'COLUMN_TYPE':
            return value.upper()
        else:
            return value

    def _coldata_from_table(self, table):
        """Build coldata dict from a reflected SQLAlchemy Table (same shape as get_header_info).
        Used so insert/upsert can derive column metadata from get_table() and skip INFORMATION_SCHEMA.
        """
        res = {}
        for col in table.c:
            type_name = type(col.type).__name__
            type_key = _SA_TYPE_NAME_TO_MAP_KEY.get(type_name, "STR")
            data_type = type_map.get(type_key, type_map["STR"])
            entry = {
                "DATA_TYPE": data_type,
                "IS_NULLABLE": bool(col.nullable),
                "COLUMN_TYPE": type_key,
            }
            length = getattr(col.type, "length", None)
            if length is not None:
                entry["CHARACTER_MAXIMUM_LENGTH"] = length
            res[col.key] = entry
        return res
    
    
    async def get_header_info(self, database, table_name):
        if not database:
            database = self.database
        key = (database, table_name)
        cached = self._header_info_cache.get(key)
        if cached is not None:
            entry, ts = cached
            if time.monotonic() - ts < _HEADER_CACHE_TTL:
                return entry
            del self._header_info_cache[key]
        hdrQry = sql_text(
            "SELECT * FROM INFORMATION_SCHEMA.COLUMNS "
            "WHERE TABLE_NAME = :table_name AND TABLE_SCHEMA = :database"
        )
        # Use longer timeout for schema query so it doesn't fail under load (default 30s can be tight)
        schema_timeout = max(60, (self._db_exec_timeout or 30) * 2)
        hdrRsp = await self.run(hdrQry, database, params={"table_name": table_name, "database": database}, timeout=schema_timeout)
        if not getattr(hdrRsp, "successful", True) or len(hdrRsp) == 0:
            raise SchemaQueryError(
                f"INFORMATION_SCHEMA query failed or returned no columns for {database!r}.{table_name!r}"
            )
        res = {x['COLUMN_NAME']: {k : self.convert_header_info(k, v)
                                  for k,v in x.items()} for x in hdrRsp}
        self._header_info_cache[key] = (res, time.monotonic())
        return res


    async def correct_types(self, database:str, table_name:str, records:list, table=None):
        """Convert record values to DB types. If table is provided (from get_table), coldata
        is derived from it and no INFORMATION_SCHEMA query is run."""
        if table is not None:
            coldata = self._coldata_from_table(table)
        else:
            coldata = await self.get_header_info(database, table_name)
        for r in records:
            for k in r:
                r[k] = self.convert_vals(k, r[k], coldata)
        return records


    def textualize(self, query):
        txt = str(query.compile(compile_kwargs={"literal_binds": True}))
        return txt


    ### SELECT, INSERT, UPDATE, DELETE, UPSERT
    async def select(self, database:str, table_name:str, params:dict=None, columns:list=None, 
               distinct:bool=False, order_by:str=None, offset:int=None, limit:int=None, 
               reverse:bool=None, textual:bool=False, response_model:object=None
               ) -> DataBaseResponse | str:
        engine = self.engine(database)
        dbr = DataBaseResponse.default(database)
        try:
            table = await self.get_table(database, table_name)
            if columns:
                query = select(*[table.c[col] for col in columns])
            else:
                query = select(table)
            if distinct:
                query = query.distinct()
            # Constructing complex conditions
            if params:
                query = self._construct_conditions(query, table, params)
            if order_by:
                reverse = False if reverse is None else reverse
                dir_f = asc if not reverse else desc
                query = query.order_by(dir_f(table.c[order_by]))
            if limit:
                offset = 0 if offset is None else offset
                query = query.limit(limit).offset(offset)
            if textual:
                txt = self.textualize(query)
                return txt
            dbr = await self.execute(engine.ctx, query, response_model=response_model, auto_commit=False)
        except Exception as e:
            dbr.successful = False
            dbr.error_info = e.__repr__()
            tb = traceback.format_exc()
            await self.raise_event("on_query_error", exc=e, tb=tb, dbr=dbr)
            raise e
        finally:
            if engine.single_use:
                await engine.kill()
        return dbr


    async def select_pages(self, database:str, table_name:str, params:dict=None, columns:list=None, 
                distinct:bool=False, order_by:str=None, page_size:int=25_000, 
                reverse:bool=None, response_model:object=None) -> AsyncGenerator[None, list[Any]]:
        """Automatically paginate larger queries
        into smaller chunks. Returns a generator
        """
        limit = page_size
        offset = 0
        has_more = True
        while has_more:
            dbr = await self.select(database, table_name, params, columns=columns, distinct=distinct,
                              order_by=order_by, limit=limit+1, offset=offset,
                              reverse=reverse, response_model=response_model)
            has_more = len(dbr) > limit
            offset += limit
            data = dbr.records[:min(len(dbr),limit)]
            yield data


    async def insert(self, database:str, table_name:str,
               records:list, upsert:bool=False, 
               update_keys:list=None, textual:bool=False) -> DataBaseResponse | str:
        engine = self.engine(database)
        dbr = DataBaseResponse.default(database)
        try:
            table = await self.get_table(database, table_name)
            records = await self.correct_types(database, table_name, records, table=table)
            if upsert:
                if update_keys is None:
                    update_keys = [
                        c.key for c in table.c
                    ]
                stmt = insert(table).values(records)
                update_dict = {k: stmt.inserted[k] for k in update_keys}
                query = stmt.on_duplicate_key_update(**update_dict)
            else:
                query = insert(table).values(records)
            if textual:
                txt = self.textualize(query)
                return txt
            dbr = await self.execute(engine.ctx, query)
        except Exception as e:
            dbr.successful = False
            dbr.error_info = e.__repr__()
            raise e
        finally:
            if engine.single_use:
                await engine.kill()
        return dbr


    async def insert_pages(self, database:str, table_name:str, records:list[dict], 
                     upsert:bool=False, update_keys:list=None, page_size:int=10_000):
        max_ix = len(records)
        start_ix = 0
        while start_ix < max_ix:
            end_ix = min(page_size + start_ix, max_ix)
            ctx = records[start_ix:end_ix]
            dbr = await self.insert(database, table_name, ctx, 
                              upsert=upsert,
                              update_keys=update_keys)
            yield dbr
            start_ix = end_ix


    async def upsert(self, database:str, table_name:str,
               records:list, update_keys:list=None, 
               textual:bool=False):
        return await self.insert(database, table_name,
                           records, upsert=True, 
                           update_keys=update_keys,
                           textual=textual)


    async def upsert_pages(self, database:str, table_name:str, records:list[dict], 
                    update_keys:list=None, page_size:int=10_000):
        max_ix = len(records)
        start_ix = 0
        while start_ix < max_ix:
            end_ix = min(page_size + start_ix, max_ix)
            ctx = records[start_ix:end_ix]
            dbr = await self.upsert(database, table_name, ctx, 
                            update_keys=update_keys)
            yield dbr
            start_ix = end_ix


    async def update(self, database:str, table_name:str, 
               update:dict={}, where:dict={}, textual:bool=False):
        engine = self.engine(database)
        dbr = DataBaseResponse.default(database)
        try:
            table = await self.get_table(database, table_name)
            query = sql_update(table)
            query = self._construct_conditions(query, table, where)
            query = query.values(**update)
            if textual:
                txt = self.textualize(query)
                return txt
            dbr = await self.execute(engine.ctx, query)
        except Exception as e:
            dbr.successful = False
            dbr.error_info = e.__repr__()
            raise e
        finally:
            if engine.single_use:
                await engine.kill()
        return dbr
        
        
    async def delete(self, database:str, table_name:str,
               where:dict, textual:bool=False):
        engine = self.engine(database)
        dbr = DataBaseResponse.default(database)
        try:
            table = await self.get_table(database, table_name)
            query = delete(table)
            query = self._construct_conditions(query, table, where)
            if textual:
                txt = self.textualize(query)
                return txt
            dbr = await self.execute(engine.ctx, query)
        except Exception as e:
            dbr.successful = False
            dbr.error_info = e.__repr__()
            raise e
        finally:
            if engine.single_use:
                await engine.kill()
        return dbr
    
    
    async def clear_table(self, database:str, table_name:str, textual:bool=False):
        engine = self.engine(database)
        dbr = DataBaseResponse.default(database)
        try:
            table = await self.get_table(database, table_name)
            query = delete(table)
            if textual:
                txt = self.textualize(query)
                return txt
            dbr = await self.execute(engine.ctx, query)
        except Exception as e:
            dbr.successful = False
            dbr.error_info = e.__repr__()
            raise e
        finally:
            if engine.single_use:
                await engine.kill()
        return dbr

    def transaction(self, database=None):
        from dbmasta.transaction import AsyncTransaction
        return AsyncTransaction(db=self, database=database or self.database)

    async def join_select(self, database:str, table_name:str, join_table:str,
                    on:dict, join_type:str="inner", params:dict=None,
                    columns:list=None, join_table_database:str=None,
                    distinct:bool=False, order_by:str=None, offset:int=None,
                    limit:int=None, reverse:bool=None, textual:bool=False,
                    response_model:object=None, as_decimals:bool=False
                    ) -> DataBaseResponse | str:
        engine = self.engine(database)
        dbr = DataBaseResponse.default(database)
        try:
            table1 = await self.get_table(database, table_name)
            jdb = join_table_database or database
            table2 = await self.get_table(jdb, join_table)
            on_clause = and_(*[table1.c[lk] == table2.c[rk] for lk, rk in on.items()])
            jt = join_type.lower()
            is_outer = jt in ("left", "right", "full")
            is_full = jt == "full"
            j = sql_join(table1, table2, on_clause, isouter=is_outer, full=is_full)
            if columns:
                col_objs = self._parse_join_columns(columns, table1, table2)
            else:
                col_objs = self._resolve_join_columns(table1, table2)
            query = select(*col_objs).select_from(j)
            if distinct:
                query = query.distinct()
            if params:
                query = self._construct_join_conditions(query, table1, table2, params)
            if order_by:
                reverse = False if reverse is None else reverse
                dir_f = asc if not reverse else desc
                order_col = self._resolve_join_order_col(order_by, table1, table2)
                query = query.order_by(dir_f(order_col))
            if limit:
                offset = 0 if offset is None else offset
                query = query.limit(limit).offset(offset)
            if textual:
                return self.textualize(query)
            dbr = await self.execute(engine.ctx, query, auto_commit=False, response_model=response_model, as_decimals=as_decimals)
        except Exception as e:
            dbr.successful = False
            dbr.error_info = e.__repr__()
            raise e
        finally:
            if engine.single_use:
                await engine.kill()
        return dbr

    def _resolve_join_columns(self, table1, table2):
        cols = []
        t1_names = {c.key for c in table1.c}
        t2_names = {c.key for c in table2.c}
        shared = t1_names & t2_names
        for c in table1.c:
            if c.key in shared:
                cols.append(label(f"{table1.name}_{c.key}", c))
            else:
                cols.append(c)
        for c in table2.c:
            if c.key in shared:
                cols.append(label(f"{table2.name}_{c.key}", c))
            else:
                cols.append(c)
        return cols

    def _parse_join_columns(self, columns, table1, table2):
        cols = []
        for col_spec in columns:
            if '.' in col_spec:
                tname, cname = col_spec.split('.', 1)
                if tname == table1.name:
                    cols.append(table1.c[cname])
                elif tname == table2.name:
                    cols.append(table2.c[cname])
                else:
                    raise ValueError(f"Unknown table {tname!r} in column spec {col_spec!r}")
            else:
                if col_spec in table1.c:
                    cols.append(table1.c[col_spec])
                elif col_spec in table2.c:
                    cols.append(table2.c[col_spec])
                else:
                    raise ValueError(f"Column {col_spec!r} not found in either table")
        return cols

    def _resolve_join_order_col(self, order_by, table1, table2):
        if '.' in order_by:
            tname, cname = order_by.split('.', 1)
            if tname == table1.name:
                return table1.c[cname]
            elif tname == table2.name:
                return table2.c[cname]
            else:
                raise ValueError(f"Unknown table {tname!r} in order_by")
        if order_by in table1.c:
            return table1.c[order_by]
        if order_by in table2.c:
            return table2.c[order_by]
        raise ValueError(f"Column {order_by!r} not found in either table")

    def _construct_join_conditions(self, query, table1, table2, params):
        for key, condition in params.items():
            if key in ('_AND_', '_OR_'):
                nested = self._process_join_condition(table1, table2, {key: condition})
                query = query.where(nested)
            else:
                col = self._resolve_join_col(key, table1, table2)
                if callable(condition):
                    query = query.where(condition(col))
                else:
                    query = query.where(col == condition)
        return query

    def _resolve_join_col(self, key, table1, table2):
        if '.' in key:
            tname, cname = key.split('.', 1)
            if tname == table1.name:
                return table1.c[cname]
            elif tname == table2.name:
                return table2.c[cname]
            else:
                raise ValueError(f"Unknown table {tname!r} in param key {key!r}")
        if key in table1.c:
            return table1.c[key]
        if key in table2.c:
            return table2.c[key]
        raise ValueError(f"Column {key!r} not found in either table")

    @staticmethod
    def _process_join_condition(table1, table2, condition):
        if isinstance(condition, dict):
            conditions = []
            for key, value in condition.items():
                if key == '_AND_':
                    conditions.append(and_(*[AsyncDataBase._process_join_condition(table1, table2, v) for v in value]))
                elif key == '_OR_':
                    conditions.append(or_(*[AsyncDataBase._process_join_condition(table1, table2, v) for v in value]))
                else:
                    if '.' in key:
                        tname, cname = key.split('.', 1)
                        col = table1.c[cname] if tname == table1.name else table2.c[cname]
                    elif key in table1.c:
                        col = table1.c[key]
                    else:
                        col = table2.c[key]
                    if callable(value):
                        conditions.append(value(col))
                    else:
                        conditions.append(col == value)
            return conditions[0] if len(conditions) == 1 else and_(*conditions)
        else:
            raise ValueError("Invalid condition format: Expected a dict.")

    def get_custom_builder(self, request:list[str]):
        output = [
            self.BLDRS.get(bldr, None) for bldr in request
        ]
        return output

    ### QUERY CONSTRUCTORS
    
    @staticmethod
    def and_(conditions):
        # implemented for legacy versions
        return conditions

    
    @staticmethod
    def or_(conditions):
        # implemented for legacy versions
        return conditions
    
    
    @staticmethod
    def not_(func, *args):
        """Returns a negated condition."""
        return lambda col: sql_not(func(*args)(col))
    
    
    @staticmethod
    def in_(values, _not=False, include_null:bool=None):
        """Returns a callable for 'in' condition."""
        include_null = _not and include_null is None
        if include_null:
            def condition(col):
                in_clause = col.in_(values)
                null_clause = col.is_(None)
                if _not:
                    return sql_not(in_clause) | null_clause
                else:
                    return in_clause | null_clause
            return condition
        else:
            return lambda col: col.in_(values) if not _not else ~col.in_(values)

    ### QUERY FRAGMENTS
    
    @staticmethod
    def greater_than(value, or_equal:bool=False, _not=False):
        """Returns a callable for greater than condition."""
        def f(col):
            if or_equal:
                return col >= value if not _not else col < value
            else:
                return col > value if not _not else col <= value
        return f
    
    
    @staticmethod
    def greaterThan(value, orEqual, _not):
        return AsyncDataBase.greater_than(value, orEqual, _not)
    
    
    @staticmethod
    def less_than(value, or_equal:bool=False, _not=False):
        """Returns a callable for greater than condition."""
        def f(col):
            if or_equal:
                return col <= value if not _not else col > value
            else:
                return col < value if not _not else col >= value
        return f
    
    
    @staticmethod
    def lessThan(value, orEqual, _not):
        return AsyncDataBase.less_than(value, orEqual, _not)
    
    
    @staticmethod
    def equal_to(value, _not=False, include_null:bool=None):
        include_null = _not and include_null is None
        if include_null:
            return lambda col: func.ifnull(col, '') == value if not _not else func.ifnull(col, '') != value
        return lambda col: col == value if not _not else col != value
    
    
    @staticmethod
    def equalTo(value, _not=False, include_null:bool=None):
        return AsyncDataBase.equal_to(value, _not=_not, include_null=include_null)
    
    
    @staticmethod
    def between(value1, value2, _not=False): # not inclusive
        def f(col):
            v1 = min([value1, value2])
            v2 = max([value1, value2])
            return col.between(v1, v2) if not _not else sql_not(col.between(v1, v2))
        return f
    
    
    @staticmethod
    def after(date, inclusive = False, _not=False):
        return AsyncDataBase.greater_than(date, inclusive, _not)
    
    
    @staticmethod
    def before(date, inclusive = False, _not=False):
        return AsyncDataBase.less_than(date, inclusive, _not)
    
    
    @staticmethod
    def onDay(date, _not = False):
        if isinstance(date, dt.datetime):
            date = date.date()
        return AsyncDataBase.equal_to(date, _not)
    
    
    @staticmethod
    def null(_not=False):
        return lambda col: col.is_(None) if not _not else col.isnot(None)
    
    
    @staticmethod
    def like(value, _not=False):
        return lambda col: col.like(value) if not _not else col.not_like(value)
    
    
    @staticmethod
    def starts_with(value, _not=False):
        """Returns a callable for starts with condition."""
        return AsyncDataBase.like(f"{value}%", _not)
    
    
    @staticmethod
    def startsWith(value, _not=False):
        return AsyncDataBase.starts_with(value, _not)
    
    
    @staticmethod
    def ends_with(value, _not=False):
        return AsyncDataBase.like(f"%{value}", _not)
    
    
    @staticmethod
    def endsWith(value, _not=False):
        return AsyncDataBase.ends_with(value, _not)
    
    
    @staticmethod
    def regex(value, _not=False):
        return lambda col: col.regexp_match(value) if not _not else ~col.regexp_match(value)
    
    
    @staticmethod
    def contains(value, _not=False):
        return AsyncDataBase.like(f"%{value}%", _not)
    
    
    @staticmethod
    def custom(value:str):
        return lambda col: sql_text(f"`{col.table.name}`.`{col.key}` {value}")


    @staticmethod
    def _process_condition(table, condition):
        if isinstance(condition, dict):
            conditions = []
            for key, value in condition.items():
                if key == '_AND_':
                    conditions.append(and_(*[AsyncDataBase._process_condition(table, v) for v in value]))
                elif key == '_OR_':
                    conditions.append(or_(*[AsyncDataBase._process_condition(table, v) for v in value]))
                elif callable(value):
                    conditions.append(value(table.c[key]))
                else:
                    conditions.append(table.c[key] == value)
            return conditions[0] if len(conditions) == 1 else and_(*conditions)
        else:
            raise ValueError("Invalid condition format: Expected a dict or appropriate condition type.")


    def _construct_conditions(self, query, table, params):
        """Constructs complex conditions for the query."""
        for key, condition in params.items():
            if key in ['_AND_', '_OR_']:
                nested_condition = AsyncDataBase._process_condition(table, {key: condition})
                query = query.where(nested_condition)
            elif callable(condition):
                query = query.where(condition(table.c[key]))
            else:
                query = query.where(table.c[key] == condition)
        return query
    
    
    async def count(self, database: str, table_name: str, params: dict = None) -> int:
        engine = self.engine(database)
        table = await self.get_table(database, table_name)
        query = select(func.count()).select_from(table)
        if params:
            query = self._construct_conditions(query, table, params)
        dbr = await self.execute(engine.ctx, query, auto_commit=False)
        return dbr.records[0][list(dbr.records[0].keys())[0]] if dbr.records else 0

    async def exists(self, database: str, table_name: str, params: dict) -> bool:
        engine = self.engine(database)
        table = await self.get_table(database, table_name)
        sub = select(sql_text("1")).select_from(table)
        sub = self._construct_conditions(sub, table, params)
        sub = sub.limit(1)
        query = select(func.count()).select_from(sub.subquery())
        dbr = await self.execute(engine.ctx, query, auto_commit=False)
        return dbr.records[0][list(dbr.records[0].keys())[0]] > 0 if dbr.records else False

    async def bulk_insert(self, database: str, table_name: str, records: list,
                          chunk_size: int = 1000) -> list:
        results = []
        for i in range(0, len(records), chunk_size):
            chunk = records[i:i + chunk_size]
            dbr = await self.insert(database, table_name, chunk)
            results.append(dbr)
        return results

    def __repr__(self):
        return f"<DbMasta Async MariaDB Client ({self.auth.username})>"