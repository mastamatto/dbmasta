from .sql_types import *

type_map = {
    'character varying': VARCHAR,
    'varchar': VARCHAR,
    'character': CHAR,
    'char': CHAR,
    'text': TEXT,
    'boolean': BOOL,
    'bool': BOOL,
    'integer': INT,
    'int': INT,
    'smallint': SMALLINT,
    'bigint': BIGINT,
    'decimal': DECIMAL,
    'numeric': DECIMAL,
    'real': FLOAT,
    'double precision': DOUBLE,
    'date': DATE,
    'timestamp': TIMESTAMP,
    'timestamp without time zone': TIMESTAMP,
    'timestamp with time zone': TIMESTAMPTZ,  # assuming you added this in `sql_types`
    'time': TIME,
    'time without time zone': TIME,
    'json': JSONTYPE,
    'jsonb': JSONB
}
