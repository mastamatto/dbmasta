from sqlalchemy import (text as sql_text, asc, desc, null as sql_null, func)
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
from typing import AsyncGenerator, Any
from dbmasta.authorization import Authorization, ENGINE
from .response import DataBaseResponse
from dbmasta.sql_types import type_map
from .tables import TableCache
from .engine import Engine, EngineManager
import asyncio, traceback

DB_CONCURRENCY_DEFAULT = 10
DB_EXECUTE_TIMEOUT_DEFAULT = 30

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
                 ):
        self.auth = auth
        self.database     = auth.default_database
        self.debug        = debug
        self.table_cache  = {}
        self.ignore_event_errors = ignore_event_errors
        self.event_handlers = {
            "on_new_table": on_new_table,
            "on_query_error": on_query_error
        }
        self.engine_manager = EngineManager(db=self)
        self.auto_raise_errors = auto_raise_errors
        self._max_db_concurrency = max_db_concurrency
        self._db_exec_timeout:int = db_exec_timeout
        self._db_semaphor = asyncio.Semaphore(self.max_db_concurrency)
        
    @property
    def max_db_concurrency(self) -> int:
        return self._max_db_concurrency
    
    @max_db_concurrency.setter
    def max_db_concurrency(self, value:int):
        self._max_db_concurrency = value or DB_CONCURRENCY_DEFAULT
        self.db_semaphor = asyncio.Semaphore(self.max_db_concurrency)
        
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


    async def run(self, query, database=None, **dbr_args):
        database = self.database if database is None else database
        engine = self.engine_manager.get_engine(database)
        if isinstance(query, str):
            query = sql_text(query)
        dbr = DataBaseResponse(query, auto_raise_errors=self.auto_raise_errors, **dbr_args) # can be overwritten
        try:
            dbr = await self.execute(engine.ctx, query, auto_commit= not query.is_select, **dbr_args)
        except Exception as e:
            print("An error occurred running custom query")
            dbr.error_info = e.__repr__()
            tb = traceback.format_exc()
            await self.raise_event("on_query_error", exc=e, tb=tb, dbr=dbr)
        finally:
            if engine.single_use:
                await engine.kill()
        return dbr

    async def _execute_and_commit(self, connection, query, auto_commit):
        result = await connection.execute(query)
        if auto_commit:
            await connection.commit()
        return result

    async def execute(self, engine, query, auto_commit:bool=True, **dbr_args) -> DataBaseResponse:
        dbr = DataBaseResponse(query, auto_raise_errors=self.auto_raise_errors, **dbr_args)
        async with self._db_semaphor:
            async with engine.connect() as connection:
                try:
                    result = asyncio.wait_for(
                        self._execute_and_commit(connection, query, auto_commit),
                        self._db_exec_timeout
                    )
                    await dbr._receive(result)
                except asyncio.TimeoutError as e:
                    dbr.error_info = str(e)
                    dbr.successful = False
                    raise
                except Exception as e:
                    dbr.error_info = str(e)
                    dbr.successful = False
                    raise
        return dbr


    def convert_vals(self, key:str, value:object, 
                     coldata:dict, **kwargs):
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
    
    
    async def get_header_info(self, database, table_name):
        if not database:
            database = self.database
        hdrQry = f"SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = N'{table_name}' AND TABLE_SCHEMA = N'{database}'"
        hdrRsp = await self.run(hdrQry, database)
        res = {x['COLUMN_NAME']: {k : self.convert_header_info(k, v) 
                                  for k,v in x.items()} for x in hdrRsp}
        return res


    async def correct_types(self, database:str, table_name:str, records:list):
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
            records = await self.correct_types(database, table_name, records)
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
    
    
    def __repr__(self):
        return f"<DbMasta Async MariaDB Client ({self.auth.username})>"