from sqlalchemy import create_engine, Engine as SAEngine
from sqlalchemy.pool import QueuePool


class SyncEngine:
    def __init__(self, database: str, engine: SAEngine, manager, single_use: bool = False):
        self.database = database
        self.ctx = engine
        self.manager = manager
        self.single_use = single_use

    @classmethod
    def new(cls, database: str, manager: "SyncEngineManager"):
        engine = create_engine(
            url=manager.auth.uri(database),
            echo=manager.db.debug,
            poolclass=QueuePool,
            max_overflow=manager.max_overflow,
            pool_size=manager.pool_size,
            pool_recycle=manager.pool_recycle,
            pool_timeout=manager.pool_timeout,
            connect_args={"connect_timeout": manager.connect_timeout},
            pool_pre_ping=True,
        )
        return cls(database, engine, manager)

    @classmethod
    def temporary(cls, database: str, manager: "SyncEngineManager"):
        engine = create_engine(
            url=manager.auth.uri(database),
            echo=manager.db.debug,
            poolclass=QueuePool,
            max_overflow=0,
            pool_size=1,
            pool_recycle=-1,
            pool_timeout=30,
            connect_args={"connect_timeout": manager.connect_timeout},
        )
        return cls(database, engine, manager, single_use=True)

    def __repr__(self):
        return f"<SyncEngine ({'single use only' if self.single_use else 'stays alive'})>"

    def kill(self):
        try:
            self.ctx.dispose(close=True)
        except Exception as err:
            print("dbConnect SyncEngine Kill Error:\n", err)


class SyncEngineManager:
    def __init__(self, db,
                 pool_size: int = 5,
                 pool_recycle: int = 3600,
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

    def create(self, database: str):
        engine = SyncEngine.new(database, manager=self)
        self.engines[database] = engine
        return engine

    def get_temporary_engine(self, database: str) -> SyncEngine:
        return SyncEngine.temporary(database, manager=self)

    def get_engine(self, database: str) -> SyncEngine:
        if database not in self.engines:
            return self.create(database)
        return self.engines[database]

    def kill(self, database: str):
        engine = self.engines.get(database, None)
        if engine is not None:
            engine.kill()
            del self.engines[database]

    def dispose_all(self):
        for _, engine in self.engines.items():
            engine.kill()
        self.engines.clear()
