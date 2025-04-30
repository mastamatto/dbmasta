import datetime as dt
from sqlalchemy import MetaData


class TableCache:
    def __init__(self, database:str, table_name:str, table):
        self.database   = database
        self.table_name = table_name
        self.table      = table
        self.expires_at = self.now+dt.timedelta(minutes=30)
        
    @classmethod
    async def new(cls, database:str, table_name:str, engine):
        metadata = MetaData()
        async with engine.begin() as conn:
            # Reflect the table
            await conn.run_sync(metadata.reflect, only=[table_name])
        # Access the reflected Table object
        table = metadata.tables[table_name]
        return cls(database, table_name, table)
        
    async def reset(self, engine):
        metadata = MetaData()
        async with engine.begin() as conn:
            await conn.run_sync(metadata.reflect, only=[self.table_name])
        table = metadata.tables[self.table_name]
        self.table = table
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