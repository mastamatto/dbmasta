from .sql_types import *

type_map = {
    'character': CHAR,
    'char': CHAR,
    'character varying': VARCHAR,
    'varchar': VARCHAR,
    'text': TEXT,
    'boolean': BOOL,
    'bool': BOOL,
    'integer': INT,
    'int': INT,
    'smallint': SMALLINT,
    'bigint': BIGINT,
    'decimal': DECIMAL,
    'numeric': NUMERIC,
    'real': FLOAT,
    'double precision': DOUBLE,
    'money': MONEY,
    'date': DATE,
    'timestamp without time zone': TIMESTAMP,
    'timestamp with time zone': TIMESTAMPTZ,
    'time without time zone': TIME,
    'uuid': UUID,
    'json': JSONTYPE,
    'jsonb': JSONB,
    'bytea': BYTEA,
    'point': POINT
}
