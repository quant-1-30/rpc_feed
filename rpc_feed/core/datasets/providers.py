#! /usr/bin/env python3
# -*- coding: utf-8 -*-

# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import datetime
import pandas as pd
from sqlalchemy import select
from sqlalchemy import and_
from utils.dt_utilty import locate_index

from .base import Provider
from .object import *
from core.writer.schema import *
from core.writer.operator import async_ops


class TradingCalendar(Provider):
    """Calendar provider base class

    Provide calendar data.
    """

    async def load(self, req: Request):
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
        calendar = []
        async with async_ops as ctx:
            stmt = select(Calendar.trading_date).order_by(Calendar.trading_date)
            async for trading_dt in ctx.on_iter_query(stmt):
                calendar.append(Calendar(trading_dt[0]))

            calendar.sort(key=lambda x: x.trading_date)
            start_time, end_time = req.range()
            if isinstance(start_time, (pd.Timestamp, datetime.datetime)):
                start_time = int(start_time.strftime("%Y%m%d"))
        
            if isinstance(end_time, (pd.Timestamp, datetime.datetime)):
                end_time = int(end_time.strftime("%Y%m%d"))

            if start_time > int(calendar[-1].trading_date):
                    yield np.array([])

            if end_time < calendar[0].trading_date:
                    yield np.array([])

            si, ei = locate_index(calendar, start_time, end_time) 
            for item in calendar[si:ei]:
                yield item


class Instrument(Provider):
    """Instrument provider base class

    Provide instrument data.
    """
   
    async def load(self, req: Request):
        """Get the existiongconfig dictionary for a base market adding several dynamic filters.
        """
        assets = []
        async with async_ops as ctx:
            stmt = select(Asset).where(
                and_(
                    Asset.first_trading.between(req.start_date, req.end_date)
                )
            )
            if req.sid:
                stmt = stmt.where(Asset.sid.in_(req.sid))
            stmt = stmt.execution_options(**self.execution_options)

            async for item in ctx.on_iter_query(stmt):
                assets.append(Asset(*item[1:]))

            for item in assets:
                yield item.serialize()
    
    
class Tick(Provider):
    """Dataset provider class
    Provide Dataset data.
    """

    async def load(self, req: Request):
        """Get dataset data.

        Parameters
        ----------
        request:  DataRequest

        Returns
        ----------
        pd.DataFrame
            a pandas dataframe with <instrument, datetime> index.
        """
        async with async_ops as ctx:
            stmt = select(Line).where(
                and_(
                    Line.tick >= req.start_date,
                    Line.tick <= req.end_date
                )
            ).order_by(Line.tick)
            if req.sid:
                stmt = stmt.where(Line.sid.in_(req.sid))

            stmt = stmt.execution_options(**self.execution_options)

            async for item in ctx.on_iter_query(stmt):
                yield Line(*item[1:]).serialize()


class Adjustment(Provider):
    """
        Calendar provider base class
        Provide calendar data.
    """

    async def load(self, req: Request):
        """Get dvidends of certain asset in given time range.

        Parameters
        ----------
        request : Request
            start of the time range.

        Returns
        ----------
        """
        async with async_ops as ctx:
            stmt = select(Adjustment).where(
                and_(
                    Adjustment.ex_date.between(req.start_date, req.end_date),
                        # or_()
                    )   
                ).order_by(Adjustment.ex_date)
            if req.sid:
                stmt = stmt.where(Adjustment.sid.in_(req.sid))
            stmt = stmt.execution_options(**self.execution_options)
            async for item in ctx.on_iter_query(stmt):
                yield Dividend(*item[1:]).serialize()


class Right(Provider):
    """
        Calendar provider base class
        Provide calendar data.
    """

    async def load(self, req: Request):
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
                and_(
                    Rightment.ex_date.between(req.start_date, req.end_date),
                    # or_()
                )   
            ).order_by(Rightment.ex_date)
            if req.sid:
                stmt = stmt.where(Rightment.sid.in_(req.sid))
            stmt = stmt.execution_options(**self.execution_options)
            async for item in ctx.on_iter_query(stmt):
                yield Rgt(*item[1:]).serialize()


_providers = dict(
    (("calendar", TradingCalendar()),
    ("asset", Instrument()),
    ("line", Tick()),
    ("adjustment", Adjustment()),
    ("right", Right()),
    ))


__all__ = ["_providers"]

