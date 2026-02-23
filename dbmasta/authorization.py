# -*- coding: utf-8 -*-
import os
from typing import Literal


ENGINE = Literal["aiomysql", "asyncmy", "pymysql", "asyncpg", "psycopg2","postgresql"]
ENGINE_MAP = {
    "aiomysql": "mysql+aiomysql",
    "asyncmy": "mysql+asyncmy",
    "pymysql": "mysql+pymysql",
    "asyncpg": "postgresql+asyncpg",
    "psycopg2": "postgresql+psycopg2",
    # "postgresql": "postgresql"
}


class Authorization:
    def __init__(self,
                 username:str,
                 password:str,
                 host:str,
                 default_database:str,
                 port:int=None,
                 engine:ENGINE="aiomysql",
                 extra_connection_params:dict=None
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
        self.port            = int(port) if port else None
        self.default_database= default_database
        self.engine_name     = engine
        self.engine          = ENGINE_MAP[engine]
        self.dialect         = "postgresql" if self.engine_name in ["asyncpg", "psycopg2", "postgresql"] else "mysql"
        self.extra_connection_params = extra_connection_params or {}
    
    
    @classmethod
    def env(cls, engine: ENGINE="mysql"):
        """Create Authorization from environment variables
        Required variables:
            - dbmasta_username    (username)
            - dbmasta_password    (password)
            - dbmasta_host        (host, ie localhost)
            - dbmasta_port        (port, ie 3306)
            - dbmasta_default     (default database name, ie my_db)
        """
        username = os.getenv('dbmasta_username')
        password = os.getenv('dbmasta_password')
        host = os.getenv('dbmasta_host')
        port = os.getenv('dbmasta_port')
        default_database = os.getenv('dbmasta_default')
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
            engine= engine
        )
        return auth
        
    
        
        
    def uri(self, database:str=None):
        if self.dialect == "postgresql":
            database = self.default_database
        database = database if database is not None else self.default_database
        host = self.host
        if self.port:
            host += f":{self.port}"
        uri = "{engine}://{user}:{password}@{host}/{database}".format(
            engine=self.engine,
            user= self.username, 
            password=self.password, 
            host=self.host, 
            database=database
        )
        if self.extra_connection_params:
            params = []
            for key,value in self.extra_connection_params.items():
                ctx = f"{key}={value}"
                params.append(ctx)
            if params:
                param = "&".join(params)
                uri = f"{uri}?{param}"
        return uri
    
    
    def __repr__(self):
        return f"<DbMasta Authorization ({self.username})>"