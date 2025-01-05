# !/usr/bin/env python3
# -*- coding: utf-8 -*-

import pandas as pd
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker
from typing import Union, List, Iterable
from contextlib import asynccontextmanager
from functools import lru_cache
from meta import with_metaclass, MetaBase
from .schema import Base
from utils.wrapper import singleton


@singleton
class AsyncOps(with_metaclass(MetaBase, object)):
    """Local provider class
    It is a set of interface that allow users to access data.
    Because PITD is not exposed publicly to users, so it is not included in the interface.

    To keep compatible with old qlib provider.
    """
    params = (
        ("host", "localhost"),
        ("port", "5432"),
        ("user", "postgres"),
        ("pwd", "20210718"),
        ("db", "backtest"),
        ("engine", "psycopg"),
        ("pool_size", 20),
        ("max_overflow", 10),
        ("pool_pre_ping", True),
        ("echo", True)
    )

    def __init__(self):
        self._initialized = False
    
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

    # @staticmethod
    @classmethod
    async def _build_engine(cls):
        """
            a. create all tables
            b. reflect tables
            c. bug --- every restart service result scan model to recreate (rollback)
        """
        # postgresql+psycopg2cffi://user:password@host:port/dbname[?key=value&key=value...]
        # postgresql+psycopg2://me@localhost/mydb
        # postgresql+asyncpg://me@localhost/mydb
        print("builder ", cls)
        url = f"postgresql+{cls.p.engine}://{cls.p.user}:{cls.p.pwd}@{cls.p.host}:{cls.p.port}/{cls.p.db}"
        # isolation_level="AUTOCOMMIT"
        engine = create_async_engine(url, 
                               pool_size=cls.p.pool_size, 
                               max_overflow=cls.p.max_overflow,
                               # 每小时回收连接
                               pool_recycle=3600, 
                               # 使用 ping 检查连接有效性 
                               pool_pre_ping=cls.p.pool_pre_ping,
                               echo=cls.p.echo).execution_options(compiled_cache={})
        
        # Create tables and reflect schema asynchronously
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
            await conn.run_sync(Base.metadata.reflect)
            # Reflect ORM objects
            MapBase = automap_base(metadata=Base.metadata)
            await conn.run_sync(MapBase.prepare)
        
        setattr(cls, "engine", engine)
        setattr(cls, "_tables", Base.metadata.tables)
        setattr(cls, "_orm_map", MapBase.classes)

    async def get_tables(self):
        await self._ensure_initialized()
        return self._tables

    @classmethod
    @asynccontextmanager
    async def get_db(cls):
        if isinstance(cls, type):
            # If called on class, ensure instance is initialized
            instance = cls()
            await instance._ensure_initialized()
        else:
            # If called on instance
            await cls._ensure_initialized()        
             
        AsyncSessionLocal = sessionmaker(
            bind=cls.engine,
            class_=AsyncSession,
            expire_on_commit=False
        )
        session = AsyncSessionLocal()
        print("session", session)
        try:
                yield session
        finally:
                await session.close()
    
    async def on_query(self, query):
        await self._ensure_initialized()
        async with self.get_db() as session:
            async with session.begin():
                    # stmt = select(cal).execution_options(**self.options)
                    # AsyncSession not support query 
                    # result = await session.execute(query)
                    # yield result.scalars().all()
                    # in asynchronous mode, the synchronous yield_per isn't directly applicable. 
                    # Instead, you can use the stream() method, which allows streaming query results asynchronously.
                    stream = await session.stream(query)
                    # stream.scalars() return one field
                    # async for row in stream.scalars():
                    async for row in stream:
                        # Use `scalars()` for ORM-mapped rows
                        yield row

    async def on_insert(self, table_name: str, data: Union[pd.DataFrame, List[dict], dict]):
        await self._ensure_initialized()
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


async_ops = AsyncOps()

__all__ = ["async_ops"]
