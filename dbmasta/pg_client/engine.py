from sqlalchemy import create_engine, Engine as SAEngine
from sqlalchemy.pool import QueuePool


class SyncEngine:
    def __init__(self, schema: str, engine: SAEngine, manager, single_use: bool = False):
        self.schema = schema
        self.ctx = engine
        self.manager = manager
        self.single_use = single_use

    @classmethod
    def new(cls, schema: str, manager: "SyncEngineManager"):
        engine = create_engine(
            url=manager.auth.uri(),
            echo=manager.db.debug,
            poolclass=QueuePool,
            max_overflow=manager.max_overflow,
            pool_size=manager.pool_size,
            pool_recycle=manager.pool_recycle,
            pool_timeout=manager.pool_timeout,
            connect_args={"connect_timeout": manager.connect_timeout},
            pool_pre_ping=True,
        )
        return cls(schema, engine, manager)

    @classmethod
    def temporary(cls, schema: str, manager: "SyncEngineManager"):
        engine = create_engine(
            url=manager.auth.uri(),
            echo=manager.db.debug,
            poolclass=QueuePool,
            max_overflow=0,
            pool_size=1,
            pool_recycle=-1,
            pool_timeout=30,
            connect_args={"connect_timeout": manager.connect_timeout},
        )
        return cls(schema, engine, manager, single_use=True)

    def __repr__(self):
        return f"<SyncEngine PG ({'single use only' if self.single_use else 'stays alive'})>"

    def kill(self):
        try:
            self.ctx.dispose(close=True)
        except Exception as err:
            print("dbConnect SyncEngine Kill Error:\n", err)


class SyncEngineManager:
    def __init__(self, db,
                 pool_size: int = 5,
                 pool_recycle: int = 1800,
                 pool_timeout: int = 30,
                 max_overflow: int = 3,
                 connect_timeout: int = 240):
        self.engines = {}
        self.db = db
        self.auth = db.auth
        self.pool_size = pool_size
        self.pool_recycle = pool_recycle
        self.pool_timeout = pool_timeout
        self.max_overflow = max_overflow
        self.connect_timeout = connect_timeout

    def create(self, schema: str):
        engine = SyncEngine.new(schema, manager=self)
        self.engines[schema] = engine
        return engine

    def get_temporary_engine(self, schema: str) -> SyncEngine:
        return SyncEngine.temporary(schema, manager=self)

    def get_engine(self, schema: str) -> SyncEngine:
        if schema not in self.engines:
            return self.create(schema)
        return self.engines[schema]

    def kill(self, schema: str):
        engine = self.engines.get(schema, None)
        if engine is not None:
            engine.kill()
            del self.engines[schema]

    def dispose_all(self):
        for _, engine in self.engines.items():
            engine.kill()
        self.engines.clear()
