# -*- coding: utf-8 -*-
import os

class Authorization:
    def __init__(self,
                 username:str,
                 password:str,
                 host:str,
                 port:int,
                 default_database:str,
                 as_async:bool=False
                 ):
        """Authentication management
        
        Example usage:
        ---
        ```
        from dbConnect import DataBase, AsyncDataBase
        
        config = {
            "username": "someuser",
            "password": "somepass",
            "host": "localhost:25252",
            "port": 3306,
            "default_database": "default_database"
        }
        
        # Instantiate the database controller
        db = DataBase(**config)
        
        # Instantiate the async version of the database controller
        adb = AsyncDataBase(**config) 
        ```
        """
        self.username        = username
        self.password        = password
        self.host            = host
        self.port            = int(port)
        self.default_database= default_database
        self.engine_name     = "aiomysql" if as_async else "pymysql"
    
    
    @classmethod
    def env(cls, as_async:bool=False):
        """Create Authorization from environment variables
        Required variables:
            - db_username    (username)
            - db_password    (password)
            - db_host        (host, ie localhost)
            - db_port        (port, ie 3306)
            - db_default     (default database name, ie my_db)
        """
        username = os.getenv('db_username')
        password = os.getenv('db_password')
        host = os.getenv('db_host')
        port = os.getenv('db_port')
        default_database = os.getenv('db_default')
        
        assert username is not None, "Missing Env Var: db_username"
        assert password is not None, "Missing Env Var: db_password"
        assert host is not None, "Missing Env Var: db_host"
        assert port is not None, "Missing Env Var: db_port"
        assert default_database is not None, "Missing Env Var: db_default"
        
        auth = cls(
            username = username,
            password = password,
            host     = host,
            port     = port,
            default_database = default_database,
            as_async = as_async
        )
        return auth
        
        
    def uri(self, database:str=None):
        database = database if database is not None else self.default_database
        uri = "mysql+{engine}://{user}:{password}@{host}:{port}/{database}".format(
            engine=self.engine_name,
            user= self.username, password=self.password, host=self.host, 
            port=self.port, database=database
        )
        return uri
    
    
    def __repr__(self):
        return f"<Database Authorization ({self.username})>"