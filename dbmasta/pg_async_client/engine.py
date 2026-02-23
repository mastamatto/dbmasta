from sqlalchemy.ext.asyncio import create_async_engine, AsyncEngine
from sqlalchemy.pool import AsyncAdaptedQueuePool

class Engine:
    def __init__(self, schema:str, engine:AsyncEngine, manager, single_use:bool=False):
        self.schema = schema
        self.ctx = engine
        self.manager = manager
        self.single_use = single_use
        
    @classmethod
    def new(cls, schema:str, manager):
        temp_engine_kwargs = {
            "url": manager.auth.uri(),
            "echo": manager.db.debug,
            "poolclass": AsyncAdaptedQueuePool,
            "max_overflow": manager.max_overflow,
            "pool_size": manager.pool_size,
            "pool_recycle": manager.pool_recycle,
            "pool_timeout": manager.pool_timeout,
            "connect_args": {'timeout': manager.connect_timeout},
            "pool_pre_ping": True
        }
        engine = create_async_engine(**temp_engine_kwargs)
        return cls(schema, engine, manager)
        
    @classmethod
    def temporary(cls, schema:str, manager):
        temp_engine_kwargs = {
            "url": manager.auth.uri(),
            "echo": manager.db.debug,
            "poolclass": AsyncAdaptedQueuePool,
            "max_overflow": 0,
            "pool_size": 1,
            "pool_recycle": -1,
            "pool_timeout": 30,
            "connect_args": {'timeout': manager.connect_timeout},
        }
        engine = create_async_engine(**temp_engine_kwargs)
        return cls(schema, engine, manager, single_use=True)
        
    def __repr__(self): return f"<Engine ({'single use only' if self.single_use else 'stays alive'})>"
    
    async def kill(self):
        try:
            await self.ctx.dispose(close=True)
        except Exception as err:
            print("dbConnect Engine Kill Error:\n", err)
            ...
        

class EngineManager:
    def __init__(self, db, 
                 pool_size:int=10, 
                 pool_recycle:int=1800,
                 pool_timeout:int=30,
                 max_overflow:int=5,
                 connect_timeout:int=30
                 ):
        self.engines    = {} # schema:str || database_engine:AsyncEngine
        self.db         = db
        self.auth       = db.auth
        self.pool_size  = pool_size
        self.pool_recycle = pool_recycle
        self.pool_timeout = pool_timeout
        self.max_overflow = max_overflow
        self.connect_timeout = connect_timeout
        
    def create(self, schema:str):
        engine = Engine.new(schema, manager=self)
        self.engines[schema] = engine
        return engine
        
    def get_temporary_engine(self, schema:str) -> Engine:
        engine = Engine.temporary(schema, manager=self)
        return engine
        
    def get_engine(self, schema:str) -> Engine:
        if schema not in self.engines:
            engine = self.create(schema)
            return engine
        else:
            return self.engines[schema]
        
    async def kill(self, schema:str):
        engine = self.engines.get(schema, None)
        if engine is not None:
            await engine.kill()
            del self.engines[schema]
        
    async def dispose_all(self):
        for schema, engine in self.engines.items():
            await engine.kill()
        self.engines.clear()