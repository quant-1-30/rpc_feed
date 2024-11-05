# !/usr/bin/env python3
# -*- coding: utf-8 -*-

import pandas as pd
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker
from typing import Union, List, Iterable
from meta import with_metaclass, MetaBase
from .schema import Base
from contextlib import asynccontextmanager



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
        # pdb.set_trace()
        # isolation_level="AUTOCOMMIT"
        # engine = create_engine(url, 
        engine = create_async_engine(url, 
                               pool_size=cls.p.pool_size, 
                               max_overflow=cls.p.max_overflow,
                               # 每小时回收连接
                               pool_recycle=3600, 
                               # 使用 ping 检查连接有效性 
                               pool_pre_ping=cls.p.pool_pre_ping,
                               echo=cls.p.echo)
        
        # Create tables and reflect schema asynchronously
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
            await conn.run_sync(Base.metadata.reflect)
        
        # Reflect ORM objects
        MapBase = automap_base(metadata=Base.metadata)
        async with engine.begin() as conn:
            await conn.run_sync(MapBase.prepare)
        
        setattr(cls, "engine", engine)
        # setattr(cls, "tables", Base.metadata.tables)
        setattr(cls, "orm_map", MapBase.classes)

    @classmethod
    @asynccontextmanager
    async def get_db(cls):
            print("doinit")
            await cls._build_engine()
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
        with self.get_db() as session:
            async with session.begin():
                    result = await session.execute(query)
                    return result.scalars().all()

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
                base_obj = self.orm_map[table_name]
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
