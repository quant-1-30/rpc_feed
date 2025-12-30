# !/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import pandas as pd
import asyncpg
from sqlalchemy import Select, Update, TypeDecorator, Text 
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker
from typing import Union, Iterable, Any, List, Mapping, Any
from contextlib import asynccontextmanager
from functools import lru_cache

from .schema import Base


__all__ = ["async_ops"]


class AsyncOps(object):
    """Local provider class
    It is a set of interface that allow users to access data.
    Because PITD is not exposed publicly to users, so it is not included in the interface.

    To keep compatible with old qlib provider.
    """
    params = ()

    def __init__(self):
        self._initialized = False
        # self.session = None
        self.engine = None

    async def __aenter__(self):
        await self._ensure_initialized()
        return self
    
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

            # Reflect ORM objects
            MapBase = automap_base(metadata=Base.metadata)
            await conn.run_sync(MapBase.prepare)
        
        self.engine = engine
        self._orm_map = MapBase.classes

    @asynccontextmanager
    async def get_db(self): # avoid reuse connection
        await self._ensure_initialized()
        AsyncSessionLocal = sessionmaker(
            bind=self.engine,
            class_=AsyncSession,
            expire_on_commit=False
        )
        session = AsyncSessionLocal()

        # conn = await session.connection()
        # raw_conn = await conn.get_raw_connection()
        # await raw_conn.driver_connection.set_type_codec(
        #     'text', schema='pg_catalog', encoder=str, decoder=lambda x: x, format='binary'
        # )
        try:
            yield session
        except Exception:
            await session.rollback() 
            raise
        finally:
            await session.close()  

    @staticmethod
    def filter_valid_keys(base_obj, insert):
        valid_keys = [column.name for column in base_obj.__table__.columns]
        return {key: value for key, value in insert.items() if key in valid_keys}
    
    async def on_query(self, query):
        async with self.get_db() as session:
                # in asynchronous mode, the synchronous yield_per isn't directly applicable. 
                # Instead stream() method, which allows streaming query results asynchronously.
                # result = await session.stream(
                #     query.execution_options(yield_per=1000)
                # )
                # async for row in result:
                    # yield row
                stream = await session.stream(query)
                async for row in stream:
                    yield row
                # stream.scalars() return one field
    
    async def on_insert(self, table_name: str, data):
        print(f"insert {len(data)} into {table_name}")
        async with self.get_db() as session:
            async with session.begin(): # # await session.commit() 
                if isinstance(data, pd.DataFrame):
                    inserts = list(data.T.to_dict().values())
                # Iterable 可迭代对象 __iter__ 使用for / Iterator 迭代器 __iter__ , __next__ yield
                elif isinstance(data, Iterable):
                    inserts = data
                else:
                    inserts = [data]
                # too slow to MetaClass
                # base_obj = self._orm_map[table_name]
                # inserts = [base_obj(**self.filter_valid_keys(base_obj, insert)) for insert in inserts]
                # session.add_all(inserts)
                await session.execute(insert(base_obj), inserts)
                await session.commit()
    
    async def on_query_obj(self, query: Select, params=None):
        async with self.get_db() as session:
            result = await session.execute(query, params=params)
            # result.all() returns a list of Row objects, each representing a row in the result set.
            # scalars().all() single field [item] / all() multiple fields tuple[(item,)]
            return result.scalars().all()
    
    async def on_insert_obj(self, objs: Union[List[Base], Base], return_obj=False):
        async with self.get_db() as session:
            async with session.begin():
                objs = [objs] if not isinstance(objs, Iterable) else objs
                session.add_all(objs)
            await session.commit()
            if return_obj:
                for obj in objs:
                    await session.refresh(obj)
                return objs

    async def on_execute(self, query: str, params: dict={}):  
        async with self.get_db() as session:
            async with session.begin():
                await session.execute(query, params)
                await session.commit()
            
    async def on_delete_obj(self, query: Select):
        async with self.get_db() as session:
            async with session.begin():
                await session.execute(query)
                await session.commit()

    async def __aexit__(self, exc_type, exc_value, traceback):
            if exc_type is not None:
                print(f"Error: {exc_type}, {exc_value}, {traceback}")
            # True mean suppress exception
            return True
    
    async def cleanup(self):
        if self.engine:
            await self.engine.dispose()
            self.engine = None
        self._initialized = False


async_ops = AsyncOps()
