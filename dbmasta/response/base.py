# -*- coding: utf-8 -*-
from decimal import Decimal
from sqlalchemy.dialects import mysql
import traceback

class DataBaseResponseBase():
    def __init__(self,
                 query,
                 as_decimals:bool=False,
                 response_model:object=None,
                 auto_raise_errors:bool=False,
                 **dbr_args
                 ):
        # defaults
        self.database = None
        self.query    = query
        if query is None:
            self.raw_query = None
        else:
            self.raw_query = str(query.compile(
                dialect=mysql.dialect(),
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
        self.traceback    = None
        self.auto_raise_errors = auto_raise_errors
        
        # check if any dbr args were passed that haven't been configured yet
        if len(dbr_args) > 0:
            keys = '\n'.join(f" - {key}" for key in dbr_args)
            print(f"WARNING!\nTHE FOLLOWING DataBaseResponse CONFIGURATION ARGUMENTS HAVEN'T BEEN CONFIGURED YET: \n{keys}")
        
    @classmethod
    def default(cls, database:str):
        dbr = cls(None)
        dbr.database = database
        return dbr
    
    def _handle_error(self, exc:Exception):
        self.successful = False
        self.error_info = str(exc.__repr__())
        self.traceback = traceback.format_exc()
        if self.auto_raise_errors:
            self.raise_for_error()
    
    def raise_for_error(self):
        if not self.successful:
            raise Exception(self.error_info)
            
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
    
    @property
    def one_or_none(self):
        """
        Returns a single object if one exists.
        If more than one exists, it'll raise an exception saying limit=1 should be used in the query
        """
        if self.row_count == 0:
            return None
        if self.row_count > 1:
            raise Exception("One or none requires limit=1 to be used in the 'select' query. Please add that and try again.")
        return self.records[0]
    
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
        return f"<DbMasta Response ({len(self)} Records)>"