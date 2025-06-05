# !/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import pandas as pd
from dotenv import  load_dotenv
from sqlalchemy import Select
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker
from typing import Union, Dict, Iterable, Any, List
from contextlib import asynccontextmanager
from functools import lru_cache

from rpc_feed.meta import with_metaclass, MetaSingleton
from rpc_feed.core.schema import Base

__all__ = ["async_ops"]

load_dotenv()


class AsyncOps(with_metaclass(MetaSingleton, object)):
    """Local provider class
    It is a set of interface that allow users to access data.
    Because PITD is not exposed publicly to users, so it is not included in the interface.

    To keep compatible with old qlib provider.
    """
    # psycopp / asyncpg
    params = (
        # ("host", "localhost"),
        # ("port", "5432"),
        # ("user", "postgres"),
        # ("pwd", "20210718"),
        # ("db", "bt_feed"),
        # ("engine", "asyncpg"),
        # ("pool_size", 20),
        # ("max_overflow", 10),
        # ("pool_recycle", 3600),
        # ("pool_pre_ping", True),
        # ("echo", False)
    )

    def __init__(self):
        self._initialized = False
        self.session = None
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

    # @classmethod
    async def _build_engine(self):
        """
            a. create all tables
            b. reflect tables
            c. bug --- every restart service result scan model to recreate (rollback)
        """
        # postgresql+psycopg2cffi://user:password@host:port/dbname[?key=value&key=value...]
        # postgresql+psycopg2://me@localhost/mydb
        # postgresql+asyncpg://me@localhost/mydb
        print("builder ", self)
        url = f'postgresql+{os.getenv("PGENGINE")}://{os.getenv("PGUSER")}:{os.getenv("PGPWD")}@{os.getenv("PGHOST")}:{os.getenv("PGPORT")}/{os.getenv("PGDB")}'
        # isolation_level="AUTOCOMMIT"
        engine = create_async_engine(url, 
                               pool_size=int(os.getenv("PGPOOLSIZE")),
                               pool_timeout=int(os.getenv("PGTIMEOUT")),
                               max_overflow=int(os.getenv("PGMAXOVERFLOW")),
                               # 每小时回收连接
                               pool_recycle=int(os.getenv("PGPOOLRECYCLE")), 
                               # 使用 ping 检查连接有效性 
                               pool_pre_ping=bool(int(os.getenv("PGPOOLPREPING"))),
                               # stream_results = True/ False
                               # autocommit = True/ False
                               # compiled_cache = True/ False
                               echo=bool(int(os.getenv("PGECHO")))).execution_options(compiled_cache={})
        
        # Create tables and reflect schema asynchronously
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
            await conn.run_sync(Base.metadata.reflect)
            # Reflect ORM objects
            MapBase = automap_base(metadata=Base.metadata)
            await conn.run_sync(MapBase.prepare)
        
        self.engine = engine
        # # Table object
        # setattr(self, "_tables", Base.metadata.tables)
        # tables to orm classes
        self._orm_map = MapBase.classes

    @asynccontextmanager
    async def get_db(self):
        if self.session is None:
            await self._ensure_initialized()                
            AsyncSessionLocal = sessionmaker(
                bind=self.engine,
                class_=AsyncSession,
                expire_on_commit=False
            )
            self.session = AsyncSessionLocal()
        try:
                yield self.session
        finally:
                await self.session.close()
    
    async def on_query(self, query):
        # await self._ensure_initialized()
        # AsyncSession not support query 
        async with self.get_db() as session:
                # in asynchronous mode, the synchronous yield_per isn't directly applicable. 
                # Instead, you can use the stream() method, which allows streaming query results asynchronously.
                stream = await session.stream(query)
                # stream.scalars() return one field
                async for row in stream:
                    yield row

    async def on_insert(self, table_name: str, data: Union[List[dict], dict]):
        print(f"insert {len(data)} into {table_name}")
        # await self._ensure_initialized()
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
                # 只设置模型中定义的字段
                inserts = [base_obj(**self.filter_valid_keys(base_obj, insert)) for insert in inserts]
                session.add_all(inserts)
    
    @staticmethod
    def filter_valid_keys(base_obj, insert):
        valid_keys = [column.name for column in base_obj.__table__.columns]
        # 只设置模型中定义的字段
        return {key: value for key, value in insert.items() if key in valid_keys}
    
    async def on_query_obj(self, query: Select, params=None):
        # await self._ensure_initialized()
        async with self.get_db() as session:
            result = await session.execute(query, params=params)
            # result.all() returns a list of Row objects, each representing a row in the result set.
            # scalars().all() single field / all() multiple fields tuple
            return result.all()
    
    async def on_insert_obj(self, objs: Union[List[Base], Base], return_obj=False):
        # await self._ensure_initialized()
        async with self.get_db() as session:
            async with session.begin():
                objs = [objs] if not isinstance(objs, Iterable) else objs
                session.add_all(objs)
                # session.bulk_save_objects(objs)
            await session.commit()
            if return_obj:
                for obj in objs:
                    await session.refresh(obj)
                return objs
            
    async def on_execute(self, query: str):
        async with self.get_db() as session:
            async with session.begin():
                await session.execute(query)
                await session.commit()
            
    async def on_delete_obj(self, query: Select):
        # await self._ensure_initialized()
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
