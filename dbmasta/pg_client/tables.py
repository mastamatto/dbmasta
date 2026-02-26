import datetime as dt
from sqlalchemy import Table, MetaData


class TableCache:
    def __init__(self, database:str, table_name:str, table):
        self.database   = database
        self.table_name = table_name
        self.table      = table
        self.expires_at = self.now+dt.timedelta(minutes=30)
        
    @classmethod
    def new(cls, schema:str, table_name:str, engine):
        metadata = MetaData(schema=schema)
        table = Table(
            table_name,
            metadata,
            schema=schema,
            autoload_with=engine
            )
        return cls(schema, table_name, table)
        
    def reset(self, engine):
        metadata = MetaData(schema=self.database)
        self.table = Table(
            self.table_name,
            metadata,
            schema=self.database,
            autoload_with=engine,
        )
        self.expires_at = self.now+dt.timedelta(minutes=15)
        
    @property
    def now(self):
        return dt.datetime.now()
    
    @property
    def expired(self):
        n=self.now;e=self.expires_at
        return n >= e
            
    def __repr__(self):
        return f"<TableCache (`{self.database}`.`{self.table_name}`)>"