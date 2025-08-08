#! /usr/bin/env python3
# -*- coding: utf-8 -*-

# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import datetime
import toolz
from sqlalchemy import select, and_, or_ # SQLAlchemy 2.0.39 正确的导入方式

from .base import Provider
from .model import *
from rpc_feed.core.middleware.schema import *
from rpc_feed.core.middleware.operator import async_ops, duck_mgr

__all__ = ["_providers"]


class TradingCalendar(Provider):
    """Calendar provider base class

    Provide calendar data.
    """

    async def __call__(self, req: Request):
        """Get calendar of certain market in given time range.

        Parameters
        ----------
        start_time : str
            start of the time range.
        end_time : str
            end of the time range.
        freq : str
            time frequency, available: year/quarter/month/week/day.
        future : bool
            whether including future trading day.

        Returns
        ----------
        list
            calendar list
        """

        start_time = int(req.start_date.strftime("%Y%m%d")) if isinstance(req.start_date, datetime.datetime) else req.start_date
        end_time = int(req.end_date.strftime("%Y%m%d")) if isinstance(req.end_date, datetime.datetime) else req.end_date
        
        async with async_ops as ctx:
            stmt = select(Calendar.trading_date).where(
                and_(
                    Calendar.trading_date.between(start_time, end_time)
                )
            ).order_by(Calendar.trading_date)

            async for trading_dt in ctx.on_query(stmt):
                yield CalendarModel(trading_date=trading_dt[0]).model_dump()


class Instrument(Provider):
    """Instrument provider base class

    Provide instrument data.
    """
   
    async def __call__(self, req: Request):
        """Get the existiongconfig dictionary for a base market adding several dynamic filters.
        """
        async with async_ops as ctx:
            stmt = select(Asset)
            if req.sid:
                stmt = stmt.where(Asset.sid.in_(req.sid))
            else:
                stmt = stmt.where(
                    and_(
                        Asset.first_trading.between(req.start_date, req.end_date)
                    )
                )

            async for item in ctx.on_query(stmt):
                row = item[0].serialize()
                yield AssetModel(**row).model_dump()


# class Tick(Provider):
#     """Dataset provider class via duckdb
#     """
#     async def __call__(self, req: Request):
#         """Get dataset data.

#         Parameters
#         ----------
#         request:  DataRequest

#         Returns
#         ----------
#         """
#         async with async_ops as ctx:
#             stmt = select(Line).where(
#                 and_(
#                     Line.tick >= req.start_date,
#                     Line.tick <= req.end_date
#                 )
#             ).order_by(Line.tick)
#             if req.sid:
#                 stmt = stmt.where(Line.sid.in_(req.sid))

#             async for item in ctx.on_query(stmt):
#                 row = item[0].serialize()
#                 yield LineModel(**row).model_dump()


class Tick(Provider):
    """Dataset provider class via duckdb
    """

    async def __call__(self, req: Request):
        """Get dataset data.

        Parameters
        ----------
        request:  DataRequest

        Returns
        ----------
        # SELECT * FROM stock
        # WHERE datetime >= TIMESTAMP '2024-06-01 09:30:00'
        # DuckDB 会自动把 '2024-06-01 09:30:00' 解析为内部 TIMESTAMP 类型，与 Parquet 里的 datetime 字段对齐
        
        # 读取并查询（可用 glob 模式、支持 partition pushdown)
        # hive_partitioning  --- automate path to key=value in partition cols 
        """
        # calculate qfq / hfq

        async with duck_mgr as ctx:
            async for row in ctx.query(req.model_dump()):
                line = tuple_to_model(row, LineModel) # 增加coef 
                yield line.model_dump()


class Adjust(Provider):
    """
        Calendar provider base class
        Provide calendar data.
    """

    async def __call__(self, req: Request):
        """Get dvidends of certain asset in given time range.

        Parameters
        ----------
        request : Request
            start of the time range.

        Returns
        ----------
        """
        async with async_ops as ctx:
            stmt = select(Adjustment).where( # and_ / or_
                Adjustment.ex_date.between(req.start_date, req.end_date)
                ).order_by(Adjustment.ex_date)
            if req.sid:
                stmt = stmt.where(Adjustment.sid.in_(req.sid))
            async for item in ctx.on_query(stmt):
                row = item[0].serialize()
                row = toolz.valmap(lambda x: 0 if x is None else x, row)
                yield AdjustmentModel(**row).model_dump()


class Right(Provider):
    """
        Calendar provider base class
        Provide calendar data.
    """

    async def __call__(self, req: Request):
        """Get dvidends of certain asset in given time range.

        Parameters
        ----------
        request : Request
            start of the time range.

        Returns
        ----------
        """
        async with async_ops as ctx:
            stmt = select(Rightment).where(
                     Rightment.ex_date.between(req.start_date, req.end_date)
            ).order_by(Rightment.ex_date)
            if req.sid:
                stmt = stmt.where(Rightment.sid.in_(req.sid))
            
            async for item in ctx.on_query(stmt):
                row = item[0].serialize()
                yield RightmentModel(**row).model_dump()


class Update(Provider):
    """
        Update asset data.
    """
    async def __call__(self, meta: dict):
        """Update asset data."""
        async with async_ops as ctx:
            for item in meta:
                stmt = select(Asset).where(Asset.sid == item["sid"])
                asset = await ctx.on_query_obj(stmt)
                if asset:
                    asset[0].delist = item["delist"] # 如果记录存在，更新 delist 列
                else:
                    asset = Asset(**item) # 如果记录不存在，插入新记录
                await ctx.on_insert_obj(asset)


_providers = dict(
    (("calendar", TradingCalendar()),
    ("asset", Instrument()),
    ("line", Tick()),
    ("adjust", Adjust()),
    ("right", Right()),
    ("update", Update()),
    ))



