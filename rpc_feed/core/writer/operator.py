# !/usr/bin/env python3
# -*- coding: utf-8 -*-

import pandas as pd
from sqlalchemy import Select
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker
from typing import Union, Dict, Iterable, Any, List
from contextlib import asynccontextmanager
from functools import lru_cache
from meta import with_metaclass, MetaParams, ParamsBase
from .schema import Base
from utils.wrapper import singleton


class MetaAsyncOps(MetaParams):
    '''
    Once the object is created (effectively pre-init) the "owner" of this
    class is sought
    '''
    def doinit(cls, *args, **kwargs):
        print("doinit", cls, args, kwargs)
        _obj, args, kwargs = super(MetaAsyncOps, cls).doinit(*args, **kwargs)
        
        # # Find the owner and store it
        # # startlevel = 4 ... to skip intermediate call stacks
        # ownerskip = kwargs.pop('_ownerskip', None)
        # # _obj._owner = metabase.findowner(_obj,
        # #                                  _obj._OwnerCls or LineMultiple,
        # #                                  skip=ownerskip)
        # # Parameter values have now been set before __init__
        _obj._initialized = False
        _obj.async_engine = cls._build_async_engine(_obj.p)
        return _obj, args, kwargs

    @classmethod
    def _build_async_engine(cls, p):
        """
           drivers: psycopg2cffi, psycopg2, asyncpg
           postgresql+psycopg2cffi://user:password@host:port/dbname[?key=value&key=value...]
           postgresql+psycopg2://me@localhost/mydb
           postgresql+asyncpg://me@localhost/mydb
        """
        # print("builder ", cls)
        url = f"postgresql+{p.driver}://{p.user}:{p.pwd}@{p.host}:{p.port}/{p.db}"
        # isolation_level="AUTOCOMMIT"
        engine = create_async_engine(url, 
                               pool_size=p.pool_size, 
                               max_overflow=p.max_overflow,
                               # 每小时回收连接
                               pool_recycle=3600, 
                               # 使用 ping 检查连接有效性 
                               pool_pre_ping=p.pool_pre_ping,
                               echo=p.echo).execution_options(compiled_cache={})
        return engine


class AsyncOps(with_metaclass(MetaAsyncOps, object)):
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
        ("db", "bt_broker"),
        ("driver", "psycopg"),
        ("pool_size", 20),
        ("max_overflow", 10),
        ("pool_pre_ping", True),
        ("echo", True)
    )

    def __init__(self, args=(), kwargs={}):
        print("init method")

    async def _ensure_initialized(self):
        """Helper method to ensure initialization"""
        if not self._initialized:
            await self._async_initialize()
    
    async def _async_initialize(self):
        # engine 对象 只有begin之后 才能获取到连接与conn同等对象
        # Create tables and reflect schema asynchronously
        async with self.async_engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
            await conn.run_sync(Base.metadata.reflect)
            # Reflect ORM objects
            MapBase = automap_base(metadata=Base.metadata)
            await conn.run_sync(MapBase.prepare)

        self._tables = Base.metadata.tables
        self._orm_map = MapBase.classes
        self._initialized = True

    async def __aenter__(self):
        print("__aenter__", self)
        await self._ensure_initialized()
        return self
    
    @asynccontextmanager
    async def get_db(self):
        AsyncSessionLocal = sessionmaker(
            bind=self.async_engine,
            class_=AsyncSession,
            expire_on_commit=False
        )
        session = AsyncSessionLocal()
        print("session", session)
        try:
                yield session
        finally:
                await session.close()

    @staticmethod
    def filter_valid_keys(base_obj, insert):
        valid_keys = [column.name for column in base_obj.__table__.columns]
        # 只设置模型中定义的字段
        return {key: value for key, value in insert.items() if key in valid_keys}
    
    async def on_iter_query(self, query):
        # await self._ensure_initialized()
        async with self.get_db() as session:
            async with session.begin():
                    # stmt = select(cal).execution_options(**self.options)
                    # AsyncSession not support query 
                    # in asynchronous mode, the synchronous yield_per isn't directly applicable. 
                    # Instead, you can use the stream() method, which allows streaming query results asynchronously.
                    # result = await session.execute(query)
                    stream = await session.stream(query)
                    # stream.scalars() return one field
                    # yield result.scalars().all()
                    # async for row in stream.scalars():
                    async for row in stream:
                        # Use `scalars()` for ORM-mapped rows
                        yield row

    async def on_query(self, query: Select, params=None):
        # await self._ensure_initialized()
        async with self.get_db() as session:
            async with session.begin():
                result = await session.execute(query, params=params)
                # all() 返回一个列表 / scalars().all() single column or field
            return result.scalars().all()

    async def on_val_insert(self, table_name: str, data: Union[pd.DataFrame, Dict[str, Any], Iterable]):
        # await self._ensure_initialized()
        async with self.get_db() as session:
            async with session.begin():
                base_obj = self._orm_map[table_name]
                if isinstance(data, pd.DataFrame):
                    inserts = list(data.T.to_dict().values())
                elif isinstance(data, Iterable):
                    # Iterable 可迭代对象 __iter__ 使用for / Iterator 迭代器 __iter__ , __next__ yield
                    inserts = data
                elif isinstance(data, Dict):
                    inserts = [data]
                else:
                    raise ValueError(f"Invalid data type: {type(data)}")
                # 只设置模型中定义的字段
                inserts = [base_obj(**self.filter_valid_keys(base_obj, insert)) for insert in inserts]
                session.add_all(inserts)
            await session.commit()
     
    async def on_obj_insert(self, objs: Union[List[Base], Base]):
        # await self._ensure_initialized()
        async with self.get_db() as session:
            async with session.begin():
                print("on_insert_obj", objs)
                objs = [objs] if not isinstance(objs, Iterable) else objs
                session.add_all(objs)
            await session.commit()
            # if refresh:
            #    for obj in objs:
            #        await session.refresh(obj)
            # print("refresh objs", objs)
            return objs

    async def __aexit__(self, exc_type, exc_value, traceback):
        
        if exc_type is not None:
            print(f"Error: {exc_type}, {exc_value}, {traceback}")
        # True mean suppress exception
        return True

    async def cleanup(self):
        # 释放所有连接，断开数据库 / 清理的
        self.engine.dispose()
        print("cleanup")


# kwargs = {"pool_size":100}
# async_ops = AsyncOps((), **kwargs)
async_ops = AsyncOps()

__all__ = ["async_ops"]

