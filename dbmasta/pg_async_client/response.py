# -*- coding: utf-8 -*-
from decimal import Decimal
from sqlalchemy.dialects import postgresql

class DataBaseResponse():
    def __init__(self,
                 query,
                 schema:str=None,
                 as_decimals:bool=False,
                 response_model:object=None,
                 **dbr_args
                 ):
        # defaults
        self.query    = query
        if query is None:
            self.raw_query = None
        else:
            self.raw_query = str(query.compile(
                dialect=postgresql.dialect(),
                compile_kwargs={"literal_binds": False})
                )
        self.as_decimals = as_decimals
        self.response_model = response_model # callable or class that takes one positional argument 'row'
        # some default values
        self.keys         = []
        self.returns_rows = False
        self.records      = []
        self.successful   = None
        self.error_info   = None
        
        # check if any dbr args were passed that haven't been configured yet
        if len(dbr_args) > 0:
            keys = '\n'.join(f" - {key}" for key in dbr_args)
            print(f"WARNING!\nTHE FOLLOWING DataBaseResponse CONFIGURATION ARGUMENTS HAVEN'T BEEN CONFIGURED YET: \n{keys}")
        
    @classmethod
    def default(cls, schema:str):
        dbr = cls(None)
        dbr.schema = schema
        return dbr
        
    async def _receive(self, result):
        try:
            self.successful = True
            self.returns_rows = result.returns_rows
            if result.returns_rows:
                self.keys = list(result.keys())
                data = result.fetchall()
                self.records = list(self.build_records(data))
        except Exception as e:
            self.successful = False
            self.error_info = str(e.__repr__())
            
    def build_records(self, data):
        while len(data) > 0:
            x = data.pop(0)
            yield self.build_record(x)
            
    def build_record(self, x):
        row = dict(zip(self.keys, x))
        if not self.as_decimals:
            for k in row:
                if isinstance(row[k], Decimal):
                    row[k] = float(row[k])
        # see if we need to return models or just dictionaries
        if self.response_model is not None:
            maker = self.response_model
        else:
            maker = lambda row: row
        row = maker(row)
        return row
    
    @property
    def row_count(self):
        return len(self)
    
    def __getitem__(self, k:int):
        return self.records[k]
    
    def __iter__(self):
        for x in self.records:
            yield x
            
    def __len__(self):
        return len(self.records)
    
    def __in__(self, value):
        return value in self.records
    
    def pop(self, index:int=-1):
        itm = self.records.pop(index)
        return itm
    
    def __repr__(self):
        return f"<DataBase Response ({len(self)} Records)>"