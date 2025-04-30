"""
John 3:16
For God so loved the world, that he gave his only begotten Son, that whosoever believeth
in Him should not perish, but have everlasting life. 
"""
from sqlalchemy import (create_engine, 
                        text as sql_text, asc, desc, null as sql_null, func)
from sqlalchemy.sql import (and_, or_, 
                            not_ as sql_not, 
                            select, 
                            #insert, 
                            update as sql_update, 
                            delete,
                            join as sql_join
                            )
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.pool import QueuePool
import datetime as dt
from dbmasta.authorization import Authorization
from .response import DataBaseResponse
from dbmasta.sql_types_pg import type_map
from .tables import TableCache

DEBUG = False
NULLPOOL_LIMIT  = 10_000
TIMEOUT_SECONDS = 240
POOL_RECYCLE    = 900


class DataBase:
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
    def __init__(self, auth: Authorization, debug:bool=False):
        self.auth = auth
        self.database = auth.default_database
        self.debug = debug
        self.table_cache = {
            # name: TableCache
        }

    @classmethod
    def env(cls, debug:bool=False):
        auth = Authorization.env()
        return cls(auth, debug)
    

    @classmethod
    def with_creds(cls, host:str, port:int, username:str, password:str, database:str, debug:bool=False):
        auth = Authorization(username, password, host, port, database)
        return cls(auth, debug)


    def engine(self):
        # nullpool always on for now
        return create_engine(
            url = self.auth.uri(),
            echo = self.debug,
            poolclass = QueuePool,  # Use a QueuePool for pooling connections
            max_overflow = 0,       # No extra connections beyond the pool size
            pool_size = 1,          # Pool size of 1, mimicking single connection behavior
            pool_recycle = -1,      # Disables connection recycling
            pool_timeout = 30,      # Timeout for getting a connection from the pool
            connect_args = {'connect_timeout': TIMEOUT_SECONDS})


    def preload_tables(self, db_tbls:list[tuple[str,str]])->None:
        if len(db_tbls) > 0:
            engine = self.engine()
            for db,tbl in db_tbls:
                _=self.get_table(db,tbl,engine)
            engine.dipose()


    def get_table(self, schema:str, table_name:str, engine=None):
        encapped = False
        if engine is None:
            encapped = True
            engine = self.engine()
        table_cache = self.table_cache.get((schema, table_name), None)
        if table_cache is None:
            table_cache = TableCache.new(schema, table_name, engine)
        elif table_cache.expired:
            table_cache.reset(engine)
        self.table_cache[(schema,table_name)] = table_cache
        if encapped:
            engine.dispose()
        return table_cache.table


    def run(self, query, schema, **dbr_args):
        engine = self.engine()
        dbr = DataBaseResponse.default(schema)
        if isinstance(query, str):
            query = sql_text(query)
        try:
            dbr = self.execute(engine, query, **dbr_args)
        except Exception as e:
            dbr.error_info = str(e.__repr__())
            dbr.successful = False
        finally:
            engine.dispose()
        return dbr


    def execute(self, engine, query, **dbr_args) -> DataBaseResponse:
        dbr = DataBaseResponse(query, **dbr_args)
        with engine.connect() as connection:
            result = connection.execute(query)
            connection.commit()
            dbr._receive(result)
        return dbr


    def convert_vals(self, key:str, value:object, 
                     coldata:dict, **kwargs):
        col = coldata[key]
        kwargs.update(col)
        # used for datatypes on 'write' queries
        val = col['DATA_TYPE'](value, **kwargs)
        return val.value


    def convert_header_info(self, mapp, value):
        if mapp == 'is_nullable':
            return value == 'YES'
        elif mapp == 'data_type':
            return type_map[value]
        elif mapp == 'column_type':
            return value.upper()
        else:
            return value
    
    
    def get_header_info(self, schema, table_name) -> dict:
        query = f"""
            SELECT * FROM information_schema.columns 
            WHERE table_name = '{table_name}' AND table_schema = '{schema}'
        """
        response = self.run(query, schema)
        assert len(response) > 0, f"Table Does Not Exist: {table_name}"
        res = {x['column_name']: {k : self.convert_header_info(k, v) 
                                  for k,v in x.items()} for x in response}
        return res


    def correct_types(self, schema:str, table_name:str, records:list):
        coldata = self.get_header_info(schema, table_name)
        for r in records:
            for k in r:
                r[k] = self.convert_vals(k, r[k], coldata)
        return records


    def textualize(self, query):
        txt = str(query.compile(compile_kwargs={"literal_binds": True}))
        return txt


    ### SELECT, INSERT, UPDATE, DELETE, UPSERT
    def select(self, schema:str, table_name:str, params:dict=None, columns:list=None, 
               distinct:bool=False, order_by:str=None, offset:int=None, limit:int=None, 
               reverse:bool=None, textual:bool=False, response_model:object=None, 
               as_decimals:bool=False) -> DataBaseResponse | str:
        engine = self.engine()
        dbr = DataBaseResponse.default(schema)
        try:
            table = self.get_table(schema, table_name, engine)
            if columns:
                query = select(*[table.c[col] for col in columns])
            else:
                query = select(table)
            if distinct:
                query = query.distinct()
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
                dbr = self.textualize(query)
            else:
                dbr = self.execute(engine, query, response_model=response_model, as_decimals=as_decimals)
        except Exception as e:
            dbr.error_info = str(e.__repr__())
            dbr.successful = False
            raise e
        finally:
            engine.dispose()
        return dbr


    def select_pages(self, schema:str, table_name:str, params:dict=None, columns:list=None, 
                distinct:bool=False, order_by:str=None, page_size:int=25_000, 
                reverse:bool=None, response_model:object=None):
        """Automatically paginate larger queries
        into smaller chunks. Returns a generator
        """
        limit = page_size
        offset = 0
        has_more = True
        while has_more:
            dbr = self.select(schema, table_name, params, columns=columns, distinct=distinct,
                              order_by=order_by, limit=limit+1, offset=offset,
                              reverse=reverse, response_model=response_model)
            has_more = len(dbr) > limit
            offset += limit
            data = dbr.records[:min(len(dbr),limit)]
            yield data


    def insert(self, schema:str, table_name:str,
               records:list, upsert:bool=False, 
               update_keys:list=None, textual:bool=False) -> DataBaseResponse | str:
        engine = self.engine()
        dbr = DataBaseResponse.default(schema)
        try:
            table = self.get_table(schema, table_name, engine)
            # clean dtypes
            records = self.correct_types(schema, table_name, records)
            updateskeysnone = update_keys is None
            if upsert:
                if updateskeysnone:
                    update_keys = [
                        c.key for c in table.c
                    ]
                stmt = insert(table).values(records)
                update_dict = {k: stmt.inserted[k] for k in update_keys}
                query = stmt.on_duplicate_key_update(**update_dict)
            else:
                query = insert(table).values(records)
            if textual:
                dbr = self.textualize(query)
            else:
                dbr = self.execute(engine, query)
        except Exception as e:
            dbr.error_info = str(e.__repr__())
            dbr.successful = False
            raise e
        finally:
            engine.dispose()
        return dbr


    def insert_pages(self, schema:str, table_name:str, records:list[dict], 
                     upsert:bool=False, update_keys:list=None, page_size:int=10_000):
        max_ix = len(records)
        start_ix = 0
        while start_ix < max_ix:
            end_ix = min(page_size + start_ix, max_ix)
            ctx = records[start_ix:end_ix]
            dbr = self.insert(schema, table_name, ctx, 
                              upsert=upsert,
                              update_keys=update_keys)
            yield dbr
            start_ix = end_ix
    

    def upsert(self, schema:str, table_name:str,
               records:list, update_keys:list=None, 
               textual:bool=False) -> DataBaseResponse | str:
        return self.insert(schema, table_name,
                           records, upsert=True, 
                           update_keys=update_keys,
                           textual=textual)


    def upsert_pages(self, schema:str, table_name:str, records:list[dict], 
                    update_keys:list=None, page_size:int=10_000):
        max_ix = len(records)
        start_ix = 0
        while start_ix < max_ix:
            end_ix = min(page_size + start_ix, max_ix)
            ctx = records[start_ix:end_ix]
            dbr = self.upsert(schema, table_name, ctx, 
                            update_keys=update_keys)
            yield dbr
            start_ix = end_ix


    def update(self, schema:str, table_name:str, 
               update:dict={}, where:dict={}, textual:bool=False) -> DataBaseResponse | str:
        engine = self.engine()
        dbr = DataBaseResponse.default(schema)
        try:
            table = self.get_table(schema, table_name, engine)
            query = sql_update(table)
            query = self._construct_conditions(query, table, where)
            query = query.values(**update)
            if textual:
                dbr = self.textualize(query)
            else:
                dbr = self.execute(engine, query)
        except Exception as e:
            dbr.error_info = str(e.__repr__())
            dbr.successful = False
            raise e
        finally:
            engine.dispose()
        return dbr
        
        
    def delete(self, schema:str, table_name:str,
               where:dict, textual:bool=False) -> DataBaseResponse | str:
        engine = self.engine()
        dbr = DataBaseResponse.default(schema)
        try:
            table = self.get_table(schema, table_name, engine)
            query = delete(table)
            query = self._construct_conditions(query, table, where)
            if textual:
                dbr = self.textualize(query)
            else:
                dbr = self.execute(engine, query)
        except Exception as e:
            dbr.error_info = str(e.__repr__())
            dbr.successful = False
            raise e
        finally:
            engine.dispose()
        return dbr
    
    
    def clear_table(self, schema:str, table_name:str, textual:bool=False):
        engine = self.engine()
        dbr = DataBaseResponse.default(schema)
        try:
            table = self.get_table(schema, table_name, engine)
            query = delete(table)
            if textual:
                dbr = self.textualize(query)
            else:
                dbr = self.execute(engine, query)
        except Exception as e:
            dbr.error_info = str(e.__repr__())
            dbr.successful = False
            raise e
        finally:
            engine.dispose()
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
        return DataBase.greater_than(value, orEqual, _not)
    
    
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
        return DataBase.less_than(value, orEqual, _not)
    
    
    @staticmethod
    def equal_to(value, _not=False, include_null:bool=None):
        include_null = _not and include_null is None
        if include_null:
            return lambda col: func.ifnull(col, '') == value if not _not else func.ifnull(col, '') != value
        return lambda col: col == value if not _not else col != value
    
    
    @staticmethod
    def equalTo(value, _not=False, include_null:bool=None):
        return DataBase.equal_to(value, _not=_not, include_null=include_null)
    
    
    @staticmethod
    def between(value1, value2, _not=False): # not inclusive
        def f(col):
            v1 = min([value1, value2])
            v2 = max([value1, value2])
            return col.between(v1, v2) if not _not else sql_not(col.between(v1, v2))
        return f
    
    
    @staticmethod
    def after(date, inclusive = False, _not = False):
        return DataBase.greater_than(date, inclusive, _not)
    
    
    @staticmethod
    def before(date, inclusive = False, _not=False):
        return DataBase.less_than(date, inclusive, _not)
    
    
    @staticmethod
    def onDay(date, _not = False):
        if isinstance(date, dt.datetime):
            date = date.date()
        return DataBase.equal_to(date, _not)
    
    
    @staticmethod
    def null(_not = False):
        return lambda col: col.is_(None) if not _not else col.isnot(None)
    
    
    @staticmethod
    def like(value, _not=False):
        return lambda col: col.like(value) if not _not else col.not_like(value)
    
    
    @staticmethod
    def starts_with(value, _not=False):
        """Returns a callable for starts with condition."""
        return DataBase.like(f"{value}%", _not)
    
    
    @staticmethod
    def startsWith(value, _not=False):
        return DataBase.starts_with(value, _not)
    
    
    @staticmethod
    def ends_with(value, _not=False):
        return DataBase.like(f"%{value}", _not)
    
    
    @staticmethod
    def endsWith(value, _not=False):
        return DataBase.ends_with(value, _not)
    
    
    @staticmethod
    def regex(value, _not=False):
        return lambda col: col.regexp_match(value) if not _not else ~col.regexp_match(value)
    
    
    @staticmethod
    def contains(value, _not=False):
        return DataBase.like(f"%{value}%", _not)
    
    
    @staticmethod
    def custom(value:str):
        return lambda col: sql_text(f"`{col.table.name}`.`{col.key}` {value}")


    @staticmethod
    def _process_condition(table, condition):
        if isinstance(condition, dict):
            conditions = []
            for key, value in condition.items():
                if key == '_AND_':
                    conditions.append(and_(*[DataBase._process_condition(table, v) for v in value]))
                elif key == '_OR_':
                    conditions.append(or_(*[DataBase._process_condition(table, v) for v in value]))
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
                nested_condition = DataBase._process_condition(table, {key: condition})
                query = query.where(nested_condition)
            elif callable(condition):
                query = query.where(condition(table.c[key]))
            else:
                query = query.where(table.c[key] == condition)
        return query
    
    
    def __repr__(self):
        return f"<DB ({self.auth.username})>"