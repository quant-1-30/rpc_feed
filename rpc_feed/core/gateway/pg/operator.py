# !/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import pandas as pd
from sqlalchemy import Select
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker
from contextlib import asynccontextmanager, AbstractAsyncContextManager
from typing import Union, List, Iterable, Any, AsyncGenerator

from .schema import Base
from utils.wrapper import singleton


class AsyncResultStream(AbstractAsyncContextManager):
    def __init__(self, session_ctx, query):
        self._session_ctx = session_ctx
        self._query = query
        self._stream = None
        self._scalars = None

    async def __aenter__(self):
        _session = await self._session_ctx.__aenter__()
        self._stream = await _session.stream(self._query)
        self._scalars = self._stream.scalars()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        try:
            if self._stream is not None:
                await self._stream.close()
        finally:
            await self._session_ctx.__aexit__(exc_type, exc_val, exc_tb)

    def __aiter__(self) -> AsyncGenerator[Any, None]:
        return self._scalars.__aiter__()


@singleton
class AsyncOps:
    """Local provider class
    It is a set of interface that allow users to access data.
    Because PITD is not exposed publicly to users, so it is not included in the interface.

    To keep compatible with old qlib provider.
    select and insert in seprate mode / begin used in insert / select just session is ok
    """

    def __init__(self):
        self._initialized = False
        self.engine = None
        self._session_factory = None # singleton session cause bug when async

    
    async def initialize(self):
        """Async initialization method"""
        if self._initialized:
            return
        await self._build_engine()
        self._initialized = True
    
    async def _ensure_initialized(self):
        """Helper method to ensure initialization"""
        if not self._initialized:
            await self.initialize()

    # @classmethod
    async def _build_engine(self):
        """
            a. create all tables
            b. reflect tables
            c. bug --- every restart service result scan model to recreate (rollback)
        """
        # asyncpg / psycopg2 / pycopy2cffi
        url = f'postgresql+{os.getenv("PGENGINE")}://{os.getenv("PGUSER")}:{os.getenv("PGPWD")}@{os.getenv("PGHOST")}:{os.getenv("PGPORT")}/{os.getenv("PGDB")}'
        engine = create_async_engine(url, 
                               pool_size=int(os.getenv("PGPOOLSIZE")),
                               pool_timeout=int(os.getenv("PGTIMEOUT")),
                               max_overflow=int(os.getenv("PGMAXOVERFLOW")),
                               pool_recycle=int(os.getenv("PGPOOLRECYCLE")), 
                               pool_pre_ping=bool(int(os.getenv("PGPREPING"))),
                               # isolation_level="AUTOCOMMIT"
                               # stream_results = True/ False
                               # autocommit = True/ False
                               # compiled_cache = True/ False
                               # async_creator=lambda: on_connect(url),
                               echo=bool(int(os.getenv("PGECHO")))).execution_options(compiled_cache={}) 
        
        # Create tables and reflect schema asynchronously
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
            await conn.run_sync(Base.metadata.reflect)
            MapBase = automap_base(metadata=Base.metadata)
            await conn.run_sync(MapBase.prepare)
        
        self.engine = engine
        self._orm_map = MapBase.classes

        self._session_factory = sessionmaker(
                bind=self.engine,
                class_=AsyncSession,
                expire_on_commit=False
            )
        
    @asynccontextmanager
    async def get_db(self):
        """Get database session with proper lifecycle management"""
        await self._ensure_initialized()        
                
        # Create a new session for each context
        session = self._session_factory()
        try:
            yield session
        except Exception as e:
            await session.rollback() 
            raise e
        finally:
            await session.close() # force close return to conn

    async def __aenter__(self):
        await self._ensure_initialized()
        return self

    # async def on_query(self, query):
    #     session_ctx = self.get_db()
    #     # session = await session_ctx.__aenter__()  
    #     return AsyncResultStream(session_ctx, query)  

    async def on_query(self, query):
        async with self.get_db() as session:
                # stream.scalars() one field / stream.scalars().all() all scalars values into list
                # in asynchronous mode, the synchronous yield_per isn't directly applicable. 
                # Instead stream() method, which allows streaming query results asynchronously.
                # result = await session.stream(
                #     query.execution_options(yield_per=1000)
                # )
                # async for row in result:
                    # yield row
                stream = await session.stream(query) # GeneratorExit when break
                async for row in stream: 
                    yield row

    async def on_insert(self, table_name: str, data: Union[pd.DataFrame, List[dict], dict]):
        async with self.get_db() as session:
            async with session.begin():
                if isinstance(data, pd.DataFrame):
                    inserts = list(data.T.to_dict().values())
                    # Iterable 可迭代对象 __iter__ 使用for / Iterator 迭代器 __iter__ , __next__ yield
                elif isinstance(data, Iterable):
                    inserts = data
                else:
                    inserts = [data]
                base_obj = self._orm_map[table_name]
                inserts = [base_obj(**self.filter_valid_keys(base_obj, insert)) for insert in inserts]
                session.add_all(inserts)

    @staticmethod
    def filter_valid_keys(base_obj, insert):
        valid_keys = [column.name for column in base_obj.__table__.columns]
        return {key: value for key, value in insert.items() if key in valid_keys}
    
    async def on_query_obj(self, query: Select, params=None):
        # commit rollback / AsyncSession.execute()  BEGIN session and select not need
        # without commit() auto ROLLBACK to release connection to pool / just SQLAlchemy clean session for read 
        # ROLLBACK no withdraw writer just tell session is over and recycle conn
        async with self.get_db() as session:
            result = await session.execute(query, params=params)
            return result.all() # scalars() single field / scalars().all() all scalars values into list
    
    async def on_insert_obj(self, objs: Union[List[Base], Base], return_obj):
        async with self.get_db() as session:
            async with session.begin():
                objs = [objs] if not isinstance(objs, Iterable) else objs
                session.add_all(objs)
                # flush() send sql to sync and refresh to reload 
                if return_obj:
                    await session.flush() # refresh
                    return objs
        return None
            
    async def on_delete_obj(self, query: Select):
        async with self.get_db() as session:
            async with session.begin(): 
                await session.execute(query)

    async def __aexit__(self, exc_type, exc_value, traceback): # bool
            if exc_type is not None:
                print(f"AsyncOps Error: {exc_type}, {exc_value}, {traceback}")
            # __aexit__  True mean suppress exception
            return False
    
    async def cleanup(self):
        if self.engine is not None:
            await self.engine.dispose()
            self.engine = None
        self._initialized = False
        print("cleanup")

async_ops = AsyncOps()
