from sqlalchemy import (text as sql_text, asc, desc, null as sql_null, func)
from sqlalchemy.sql import (and_, or_, 
                            not_ as sql_not, 
                            select, 
                            #insert, 
                            update as sql_update, 
                            delete,
                            join as sql_join
                            )
from sqlalchemy.dialects.postgresql import insert
import datetime as dt
from dbmasta.authorization import Authorization
from .response import DataBaseResponse
from dbmasta.sql_types_pg import type_map
from .tables import TableCache
from .engine import Engine, EngineManager
import asyncio, traceback
from collections import defaultdict
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy import cast

TIMEOUT_SECONDS = 240

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
                 ):
        self.auth = auth
        self.database     = auth.default_database
        self.debug        = debug
        self.table_cache  = {}
        self.enums        = {}
        self.ignore_event_errors = ignore_event_errors
        self.event_handlers = {
            "on_new_table": on_new_table,
            "on_query_error": on_query_error
        }
        self.engine_manager = EngineManager(db=self)
        
        
        
    ### INITIALIZERS
    @classmethod
    def env(cls, debug:bool=False):
        auth = Authorization.env(as_async=True)
        return cls(auth, debug=debug)


    def engine(self, schema:str, single_use:bool=False) -> Engine:
        if single_use:
            engine = self.engine_manager.get_temporary_engine(schema)
        else:
            engine = self.engine_manager.get_engine(schema)
        return engine


    async def kill_engines(self):
        await self.engine_manager.dispose_all()


    async def preload_tables(self, *db_tbls:list[tuple[str,str]])->None:
        for db,tbl in db_tbls:
            _ = await self.get_table(db, tbl)


    async def preload_enums(self)->None:
        qry = """
        SELECT
        n.nspname      AS schema,
        t.typname      AS enum_name,
        e.enumlabel    AS enum_value,
        e.enumsortorder
        FROM pg_type t
        JOIN pg_enum e         ON t.oid = e.enumtypid
        JOIN pg_namespace n    ON n.oid = t.typnamespace
        ORDER BY n.nspname, t.typname, e.enumsortorder;
        """
        res = await self.run(qry, "core")
        enums = defaultdict(list) # (schema,enum_name) -> list[enum_value]
        for r in res:
            sc = r['schema']
            en = r['enum_name']
            ev = r['enum_value']
            enums[(sc,en)].append(ev)
        self.enums = dict(enums)
        
        

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


    async def get_table(self, schema:str, table_name:str):
        engine = self.engine_manager.get_engine(schema)
        table_cache = self.table_cache.get((schema, table_name), None)
        if table_cache is None:
            table_cache = await TableCache.new(schema, table_name, engine.ctx)
            await self.raise_event("on_new_table", table_cache)
        elif table_cache.expired:
            await table_cache.reset(engine.ctx)
        self.table_cache[(schema,table_name)] = table_cache
        return table_cache.table


    async def run(self, query, schema, **dbr_args):
        engine = self.engine_manager.get_engine(schema)
        if isinstance(query, str):
            query = sql_text(query)
        dbr = DataBaseResponse(query, schema=schema, **dbr_args)
        try:
            dbr = await self.execute(engine.ctx, query, **dbr_args)
        except Exception as e:
            print("An error occurred running custom query")
            dbr.error_info = e.__repr__()
            tb = traceback.format_exc()
            await self.raise_event("on_query_error", exc=e, tb=tb, dbr=dbr)
        finally:
            if engine.single_use:
                await engine.kill()
        return dbr


    async def execute(self, engine, query, **dbr_args) -> DataBaseResponse:
        dbr = DataBaseResponse(query, **dbr_args)
        async with engine.connect() as connection:
            compiled = query.compile(dialect=engine.dialect)
            result = await connection.execute(compiled)
            await connection.commit()
            await dbr._receive(result)
        return dbr


    def convert_vals(self, key:str, value:object, 
                     coldata:dict, **kwargs):
        col = coldata[key]
        kwargs.update(col)
        # used for datatypes on 'write' queries
        val = col['data_type'](value, **kwargs)
        return val.value


    # def convert_header_info(self, mapp, value):
    #     if mapp == 'is_nullable':
    #         return value == 'YES'
    #     elif mapp == 'data_type':
    #         if str(value).lower() == "user-defined":
    #             return type_map['text'] # we will just treat these as text for now
    #         return type_map[value]
    #     elif mapp == 'column_type':
    #         return value.upper()
    #     else:
    #         return value
    
    
    def convert_header(self, value:dict) -> dict:
        nullable = str(value['is_nullable']).lower() == 'yes'
        data_type = value['data_type']
        updatable = str(value['is_updatable']).lower() == 'yes'
        default = value['column_default']
        enum_values = []
        if str(data_type).lower() == 'user-defined':
            key = (value['udt_schema'], value['udt_name'])
            data_type = type_map['text']
            if key in self.enums:
                enum_values = self.enums[key]
        else:
            data_type = type_map[data_type]
        return dict(
            is_nullable = nullable,
            data_type = data_type,
            is_updatable = updatable,
            column_default = default,
            enum_values = enum_values
        )

    
    async def get_header_info(self, schema, table_name):
        query = f"""
            SELECT * FROM information_schema.columns 
            WHERE table_name = '{table_name}' AND table_schema = '{schema}'
        """
        response = await self.run(query, schema)
        assert len(response) > 0, f"Table Does Not Exist: {table_name}"
        res = {x['column_name']: self.convert_header(x)
               for x in response}
        # res = {x['column_name']: {k : self.convert_header_info(k, v) 
        #                           for k,v in x.items()} for x in response}
        return res


    async def correct_types(self, schema:str, table_name:str, records:list):
        coldata = await self.get_header_info(schema, table_name)
        for r in records:
            for k in r:
                r[k] = self.convert_vals(k, r[k], coldata)
        return records


    def textualize(self, query):
        txt = str(query.compile(compile_kwargs={"literal_binds": True}))
        return txt


    ### SELECT, INSERT, UPDATE, DELETE, UPSERT
    async def select(self, schema:str, table_name:str, params:dict=None, columns:list=None, 
               distinct:bool=False, order_by:str=None, offset:int=None, limit:int=None, 
               reverse:bool=None, textual:bool=False, response_model:object=None
               ) -> DataBaseResponse | str:
        engine = self.engine(schema)
        dbr = DataBaseResponse.default(schema)
        try:
            table = await self.get_table(schema, table_name)
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
            dbr = await self.execute(engine.ctx, query, response_model=response_model)
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


    async def select_pages(self, schema:str, table_name:str, params:dict=None, columns:list=None, 
                distinct:bool=False, order_by:str=None, page_size:int=25_000, 
                reverse:bool=None, response_model:object=None):
        """Automatically paginate larger queries
        into smaller chunks. Returns a generator
        """
        limit = page_size
        offset = 0
        has_more = True
        while has_more:
            dbr = await self.select(schema, table_name, params, columns=columns, distinct=distinct,
                              order_by=order_by, limit=limit+1, offset=offset,
                              reverse=reverse, response_model=response_model)
            has_more = len(dbr) > limit
            offset += limit
            data = dbr.records[:min(len(dbr),limit)]
            yield data


    async def insert(self, schema:str, table_name:str,
               records:list, upsert:bool=False, 
               update_fields:list=None, textual:bool=False) -> DataBaseResponse | str:
        engine = self.engine(schema)
        dbr = DataBaseResponse.default(schema)
        try:
            table = await self.get_table(schema, table_name)
            records = await self.correct_types(schema, table_name, records)
            if upsert:
                update_fieldsnone = update_fields is None
                if update_fieldsnone:
                    update_fields = [
                        c.key for c in table.c
                    ]
                stmt = insert(table).values(records)
                update_keys = [k.key for k in table.primary_key.columns]
                update_dict = {k: stmt.excluded[k] for k in update_fields}
                query = stmt.on_conflict_do_update(index_elements=update_keys, set_=update_dict)
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


    async def insert_pages(self, schema:str, table_name:str, records:list[dict], 
                     upsert:bool=False, update_fields:list=None, page_size:int=10_000):
        max_ix = len(records)
        start_ix = 0
        while start_ix < max_ix:
            end_ix = min(page_size + start_ix, max_ix)
            ctx = records[start_ix:end_ix]
            dbr = await self.insert(schema, table_name, ctx, 
                              upsert=upsert,
                              update_fields=update_fields)
            yield dbr
            start_ix = end_ix


    async def upsert(self, schema:str, table_name:str,
               records:list, update_fields:list=None, 
               textual:bool=False):
        return await self.insert(schema, table_name,
                           records, upsert=True, 
                           update_fields=update_fields,
                           textual=textual)


    async def upsert_pages(self, schema:str, table_name:str, records:list[dict], 
                    update_fields:list=None, page_size:int=10_000):
        max_ix = len(records)
        start_ix = 0
        while start_ix < max_ix:
            end_ix = min(page_size + start_ix, max_ix)
            ctx = records[start_ix:end_ix]
            dbr = await self.upsert(schema, table_name, ctx, 
                            update_fields=update_fields)
            yield dbr
            start_ix = end_ix


    async def update(self, schema:str, table_name:str, 
               update:dict={}, where:dict={}, textual:bool=False):
        engine = self.engine(schema)
        dbr = DataBaseResponse.default(schema)
        try:
            table = await self.get_table(schema, table_name)
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
        
        
    async def delete(self, schema:str, table_name:str,
               where:dict, textual:bool=False):
        engine = self.engine(schema)
        dbr = DataBaseResponse.default(schema)
        try:
            table = await self.get_table(schema, table_name)
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
    
    
    async def clear_table(self, schema:str, table_name:str, textual:bool=False):
        engine = self.engine(schema)
        dbr = DataBaseResponse.default(schema)
        try:
            table = await self.get_table(schema, table_name)
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
    def json_like(value: dict, _not: bool = False):
        def condition(col):
            expr = cast(col, JSONB).contains(value)
            return ~expr if _not else expr
        return condition
    
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
        return f"<DB ({self.auth.username})>"