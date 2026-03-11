"""
John 3:16
For God so loved the world, that he gave his only begotten Son, that whosoever believeth
in Him should not perish, but have everlasting life.
"""
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
from dbmasta.authorization import Authorization
from dbmasta.exceptions import SchemaQueryError, MissingColumnError
from .response import DataBaseResponse
from dbmasta.sql_types import type_map
from .tables import TableCache
from .engine import SyncEngine, SyncEngineManager
from dbmasta.retry import sync_retry
import time

DEBUG = False
TIMEOUT_SECONDS = 240
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
    "ENUM": "ENUM", "JSON": "LONGTEXT",  # JSON: use LONGTEXT so large payloads aren't truncated
}


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
    def __init__(self, auth: Authorization, debug:bool=False, engine_config:dict=None):
        self.auth = auth
        self.database = auth.default_database
        self.debug = debug
        self.table_cache = {
            # name: TableCache
        }
        self._header_info_cache = {}  # (database, table_name) -> coldata dict for correct_types
        self.engine_manager = SyncEngineManager(db=self, **(engine_config or {}))

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.kill_engines()
        return False

    @classmethod
    def env(cls, debug:bool=False, engine_config:dict=None):
        auth = Authorization.env()
        return cls(auth, debug, engine_config=engine_config)

    @classmethod
    def with_creds(cls, host:str, port:int, username:str, password:str, database:str, debug:bool=False, engine_config:dict=None):
        auth = Authorization(username, password, host, port, database)
        return cls(auth, debug, engine_config=engine_config)

    def engine(self, database:str, single_use:bool=False) -> SyncEngine:
        if not database:
            database = self.database
        if single_use:
            return self.engine_manager.get_temporary_engine(database)
        return self.engine_manager.get_engine(database)

    def kill_engines(self):
        self.engine_manager.dispose_all()

    def preload_tables(self, db_tbls:list[tuple[str,str]])->None:
        if len(db_tbls) > 0:
            for db,tbl in db_tbls:
                _=self.get_table(db,tbl)

    def get_table(self, database:str, table_name:str, engine=None):
        eng = self.engine_manager.get_engine(database)
        raw_engine = engine if engine is not None else eng.ctx
        table_cache = self.table_cache.get((database, table_name), None)
        if table_cache is None:
            table_cache = TableCache.new(database, table_name, raw_engine)
        elif table_cache.expired:
            table_cache.reset(raw_engine)
        self.table_cache[(database,table_name)] = table_cache
        return table_cache.table

    def run(self, query, database=None, *, params:dict=None, **dbr_args):
        database = self.database if database is None else database
        eng = self.engine(database, single_use=True)
        dbr = DataBaseResponse.default(database)
        if isinstance(query, str):
            query = sql_text(query)
        try:
            dbr = self.execute(eng.ctx, query, params=params, **dbr_args)
        except Exception as e:
            dbr.error_info = str(e.__repr__())
            dbr.successful = False
            raise
        finally:
            if eng.single_use:
                eng.kill()
        return dbr

    @sync_retry(max_retries=2, backoff_base=0.5)
    def execute(self, engine, query, *, auto_commit:bool=True, params:dict=None, **dbr_args) -> DataBaseResponse:
        dbr = DataBaseResponse(query, **dbr_args)
        with engine.connect() as connection:
            result = connection.execute(query, parameters=params or {})
            if auto_commit:
                connection.commit()
            dbr._receive(result)
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
        """Build coldata dict from a reflected SQLAlchemy Table (same shape as get_header_info)."""
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


    def get_header_info(self, database, table_name) -> dict:
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
        hdrRsp = self.run(hdrQry, database, params={"table_name": table_name, "database": database})
        if not getattr(hdrRsp, "successful", True) or len(hdrRsp) == 0:
            raise SchemaQueryError(
                f"INFORMATION_SCHEMA query failed or returned no columns for {database!r}.{table_name!r}"
            )
        res = {x['COLUMN_NAME']: {k : self.convert_header_info(k, v)
                                  for k,v in x.items()} for x in hdrRsp}
        self._header_info_cache[key] = (res, time.monotonic())
        return res


    def correct_types(self, database:str, table_name:str, records:list, table=None):
        """Convert record values to DB types. If table is provided (from get_table), coldata is derived from it."""
        if table is not None:
            coldata = self._coldata_from_table(table)
        else:
            coldata = self.get_header_info(database, table_name)
        for r in records:
            for k in r:
                r[k] = self.convert_vals(k, r[k], coldata)
        return records


    def textualize(self, query):
        txt = str(query.compile(compile_kwargs={"literal_binds": True}))
        return txt


    ### SELECT, INSERT, UPDATE, DELETE, UPSERT
    def select(self, database:str, table_name:str, params:dict=None, columns:list=None,
               distinct:bool=False, order_by:str=None, offset:int=None, limit:int=None,
               reverse:bool=None, textual:bool=False, response_model:object=None,
               as_decimals:bool=False) -> DataBaseResponse | str:
        eng = self.engine(database)
        dbr = DataBaseResponse.default(database)
        try:
            table = self.get_table(database, table_name)
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
                dbr = self.execute(eng.ctx, query, auto_commit=False, response_model=response_model, as_decimals=as_decimals)
        except Exception as e:
            dbr.error_info = str(e.__repr__())
            dbr.successful = False
            raise e
        return dbr


    def select_pages(self, database:str, table_name:str, params:dict=None, columns:list=None,
                distinct:bool=False, order_by:str=None, page_size:int=25_000,
                reverse:bool=None, response_model:object=None):
        """Automatically paginate larger queries
        into smaller chunks. Returns a generator
        """
        limit = page_size
        offset = 0
        has_more = True
        while has_more:
            dbr = self.select(database, table_name, params, columns=columns, distinct=distinct,
                              order_by=order_by, limit=limit+1, offset=offset,
                              reverse=reverse, response_model=response_model)
            has_more = len(dbr) > limit
            offset += limit
            data = dbr.records[:min(len(dbr),limit)]
            yield data


    def insert(self, database:str, table_name:str,
               records:list, upsert:bool=False,
               update_keys:list=None, textual:bool=False) -> DataBaseResponse | str:
        eng = self.engine(database)
        dbr = DataBaseResponse.default(database)
        try:
            table = self.get_table(database, table_name)
            # clean dtypes
            records = self.correct_types(database, table_name, records, table=table)
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
                dbr = self.textualize(query)
            else:
                dbr = self.execute(eng.ctx, query)
        except Exception as e:
            dbr.error_info = str(e.__repr__())
            dbr.successful = False
            raise e
        return dbr


    def insert_pages(self, database:str, table_name:str, records:list[dict],
                     upsert:bool=False, update_keys:list=None, page_size:int=10_000):
        max_ix = len(records)
        start_ix = 0
        while start_ix < max_ix:
            end_ix = min(page_size + start_ix, max_ix)
            ctx = records[start_ix:end_ix]
            dbr = self.insert(database, table_name, ctx,
                              upsert=upsert,
                              update_keys=update_keys)
            yield dbr
            start_ix = end_ix


    def upsert(self, database:str, table_name:str,
               records:list, update_keys:list=None,
               textual:bool=False) -> DataBaseResponse | str:
        return self.insert(database, table_name,
                           records, upsert=True,
                           update_keys=update_keys,
                           textual=textual)


    def upsert_pages(self, database:str, table_name:str, records:list[dict],
                    update_keys:list=None, page_size:int=10_000):
        max_ix = len(records)
        start_ix = 0
        while start_ix < max_ix:
            end_ix = min(page_size + start_ix, max_ix)
            ctx = records[start_ix:end_ix]
            dbr = self.upsert(database, table_name, ctx,
                            update_keys=update_keys)
            yield dbr
            start_ix = end_ix


    def update(self, database:str, table_name:str,
               update:dict={}, where:dict={}, textual:bool=False) -> DataBaseResponse | str:
        eng = self.engine(database)
        dbr = DataBaseResponse.default(database)
        try:
            table = self.get_table(database, table_name)
            query = sql_update(table)
            query = self._construct_conditions(query, table, where)
            query = query.values(**update)
            if textual:
                dbr = self.textualize(query)
            else:
                dbr = self.execute(eng.ctx, query)
        except Exception as e:
            dbr.error_info = str(e.__repr__())
            dbr.successful = False
            raise e
        return dbr


    def delete(self, database:str, table_name:str,
               where:dict, textual:bool=False) -> DataBaseResponse | str:
        eng = self.engine(database)
        dbr = DataBaseResponse.default(database)
        try:
            table = self.get_table(database, table_name)
            query = delete(table)
            query = self._construct_conditions(query, table, where)
            if textual:
                dbr = self.textualize(query)
            else:
                dbr = self.execute(eng.ctx, query)
        except Exception as e:
            dbr.error_info = str(e.__repr__())
            dbr.successful = False
            raise e
        return dbr


    def clear_table(self, database:str, table_name:str, textual:bool=False):
        eng = self.engine(database)
        dbr = DataBaseResponse.default(database)
        try:
            table = self.get_table(database, table_name)
            query = delete(table)
            if textual:
                dbr = self.textualize(query)
            else:
                dbr = self.execute(eng.ctx, query)
        except Exception as e:
            dbr.error_info = str(e.__repr__())
            dbr.successful = False
            raise e
        return dbr


    def transaction(self, database=None):
        from dbmasta.transaction import SyncTransaction
        return SyncTransaction(db=self, database=database or self.database)


    def join_select(self, database:str, table_name:str, join_table:str,
                    on:dict, join_type:str="inner", params:dict=None,
                    columns:list=None, join_table_database:str=None,
                    distinct:bool=False, order_by:str=None, offset:int=None,
                    limit:int=None, reverse:bool=None, textual:bool=False,
                    response_model:object=None, as_decimals:bool=False
                    ) -> DataBaseResponse | str:
        eng = self.engine(database)
        dbr = DataBaseResponse.default(database)
        try:
            table1 = self.get_table(database, table_name)
            jdb = join_table_database or database
            table2 = self.get_table(jdb, join_table)
            # Build ON clause
            on_clause = and_(*[table1.c[lk] == table2.c[rk] for lk, rk in on.items()])
            # Build join
            jt = join_type.lower()
            is_outer = jt in ("left", "right", "full")
            is_full = jt == "full"
            j = sql_join(table1, table2, on_clause, isouter=is_outer, full=is_full)
            # Column selection
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
                dbr = self.textualize(query)
            else:
                dbr = self.execute(eng.ctx, query, auto_commit=False, response_model=response_model, as_decimals=as_decimals)
        except Exception as e:
            dbr.error_info = str(e.__repr__())
            dbr.successful = False
            raise e
        return dbr


    def _resolve_join_columns(self, table1, table2):
        """Auto-select all columns, labeling conflicts as tablename_colname."""
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
        """Parse user-specified column list supporting 'table.col' notation."""
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
        """Resolve order_by with optional 'table.col' notation."""
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
        """WHERE on joined tables, supporting 'table.col' dot notation."""
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
        """Resolve a column key that may be 'table.col' or just 'col' (left-table-first)."""
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
                    conditions.append(and_(*[DataBase._process_join_condition(table1, table2, v) for v in value]))
                elif key == '_OR_':
                    conditions.append(or_(*[DataBase._process_join_condition(table1, table2, v) for v in value]))
                else:
                    # resolve col
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


    def count(self, database: str, table_name: str, params: dict = None) -> int:
        eng = self.engine(database)
        table = self.get_table(database, table_name)
        query = select(func.count()).select_from(table)
        if params:
            query = self._construct_conditions(query, table, params)
        dbr = self.execute(eng.ctx, query, auto_commit=False)
        return dbr.records[0][list(dbr.records[0].keys())[0]] if dbr.records else 0

    def exists(self, database: str, table_name: str, params: dict) -> bool:
        eng = self.engine(database)
        table = self.get_table(database, table_name)
        sub = select(sql_text("1")).select_from(table)
        sub = self._construct_conditions(sub, table, params)
        sub = sub.limit(1)
        query = select(func.count()).select_from(sub.subquery())
        dbr = self.execute(eng.ctx, query, auto_commit=False)
        return dbr.records[0][list(dbr.records[0].keys())[0]] > 0 if dbr.records else False

    def bulk_insert(self, database: str, table_name: str, records: list,
                    chunk_size: int = 1000) -> list:
        results = []
        for i in range(0, len(records), chunk_size):
            chunk = records[i:i + chunk_size]
            dbr = self.insert(database, table_name, chunk)
            results.append(dbr)
        return results

    def __repr__(self):
        return f"<DbMasta MariaDB ({self.auth.username})>"
