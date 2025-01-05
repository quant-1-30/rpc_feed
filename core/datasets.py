# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import datetime
import pandas as pd
from sqlalchemy import select, text
from sqlalchemy import and_, or_
from meta import with_metaclass, MetaBase
from utils.cache import lazyproperty
from core.model import *
from core.ops.operator import async_ops
from utils.wrapper import async_method_warning
from utils.dt_utilty import locate_index

class BaseProvider(with_metaclass(MetaBase, object)):
    """Client Provider

    Requesting data from server as a client. Can propose requests:

        - Calendar : Directly respond a list of calendars
        - Instruments (without filter): Directly respond a list/dict of instruments
        - Instruments (with filters):  Respond a list/dict of instruments
        - Features : Respond a cache uri

    The general workflow is described as follows:
    When the user use client provider to propose a request, the client provider will connect the server and send the request. The client will start to wait for the response. The response will be made instantly indicating whether the cache is available. The waiting procedure will terminate only when the client get the response saying `feature_available` is true.
    `BUG` : Everytime we make request for certain data we need to connect to the server, wait for the response and disconnect from it. We can't make a sequence of requests within one connection. You can refer to https://python-socketio.readthedocs.io/en/latest/client.html for documentation of python-socketIO client.
    
        Local provider class
    It is a set of interface that allow users to access data.
    Because PITD is not exposed publicly to users, so it is not included in the interface.

    To keep compatible with old qlib provider.
    """
    execution_options = {"timeout": 60, 
                         "stream_results": True, 
                         "stream_chunk_size": 1000}
    
    def get_data(self, req):
        raise NotImplementedError("implement get_data method")

    @async_method_warning
    def __len__(self):
        raise NotImplementedError("length")
    
    @async_method_warning
    def __getitem__(self, index):
        raise NotImplementedError("getitem")

    async def alen(self):
        """Async version of __len__"""
        raise NotImplementedError("implement alen method")

    async def aget(self, index):
        """Async version of __getitem__"""
        raise NotImplementedError("implement aget method")
  

class TradingCalendar(BaseProvider):
    """Calendar provider base class

    Provide calendar data.
    """
    params = (
        ("table", "calendar"),
    )

    async def alen(self):
        calendar = await self.get_data(Request())
        return len(calendar)
    
    async def aget(self, index):
        calendar = await self.get_data(Request())
        return calendar[index]

    async def get_data(self, req: Request):
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
        objs = await async_ops.get_tables()

        # stmt = text("select trading_date from calendar")
        stmt = select(objs[self.p.table].c.trading_date)
        async for trading_dt in async_ops.on_query(stmt):
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


class Instrument(BaseProvider):
    """Instrument provider base class

    Provide instrument data.
    """
    params = (
        ("table", "asset"),
    )

    def __len__(self):
        return len(self.instruments)
    
    def __getitem__(self, sid):
        item = [sid for sid in self.instruments if sid.sid == sid]
        return item[0]
    
    async def get_data(self, req: Request):
        """Get the existiongconfig dictionary for a base market adding several dynamic filters.
        """
        assets = []
        objs = await async_ops.get_tables()

        stmt = select(objs[self.p.table]).execution_options(**self.execution_options)
        async for item in async_ops.on_query(stmt):
            assets.append(Asset(*item[1:]))

        # filter by time range
        assets = [asset for asset in assets if asset.first_trading >= req.start_date
                  and asset.first_trading <= req.end_date]
        # filter by sids
        if req.sids:
            assets = [asset for asset in assets if asset.sid in req.sids]
        print("assets ", assets)

        for item in assets:
            yield item.serialize()
    
    
class Tick(BaseProvider):
    """Dataset provider class
    Provide Dataset data.
    """
    params = (
        ("table", "minute"),
    )

    async def get_data(self, req: Request):
        """Get dataset data.

        Parameters
        ----------
        request:  DataRequest

        Returns
        ----------
        pd.DataFrame
            a pandas dataframe with <instrument, datetime> index.
        """
        objs = await async_ops.get_tables()
        model = objs[self.p.table]
        # relationship
        # from sqlalchemy.orm import selectinload
        # stmt = select(table).options(selectinload(table.c.sid))
        stmt = select(model).where(
            and_(
                # model.c.sid.in_(req.sids),
                model.c.tick > req.start_date,
                model.c.tick < req.end_date
            )
        ).execution_options(**self.execution_options)

        async for item in async_ops.on_query(stmt):
            yield Line(*item[1:]).serialize()


class Adjustment(BaseProvider):
    """
        Calendar provider base class
        Provide calendar data.
    """
    params = (
        ("table", "adjustment"),
    )

    async def get_data(self, req: Request):
        """Get dvidends of certain asset in given time range.

        Parameters
        ----------
        request : Request
            start of the time range.

        Returns
        ----------
        """
        objs = await async_ops.get_tables()
        stmt = select(objs[self.p.table]).where(
            and_(
                objs[self.p.table].c.trading_date.between(req.start_date, req.end_date),
                objs[self.p.table].c.sid.in_(req.sids),
                # or_()
            )   
        )
        async for item in async_ops.on_query(stmt):
            yield Adjustment(*item[1:]).serialize()


class Right(BaseProvider):
    """
        Calendar provider base class
        Provide calendar data.
    """
    params = (
        ("table", "rightment"),
    )

    async def get_data(self, req: Request):
        """Get dvidends of certain asset in given time range.

        Parameters
        ----------
        request : Request
            start of the time range.

        Returns
        ----------
        """
        objs = await async_ops.get_tables()
        stmt = select(objs[self.p.table]).where(
            and_(
                objs[self.p.table].c.ex_date.between(req.start_date, req.end_date),
                objs[self.p.table].c.sid.in_(req.sids),
                # or_()
            )   
        )
        async for item in async_ops.on_query(stmt):
            yield Right(*item[1:]).serialize()


Providers = dict(
    (("calendar", TradingCalendar()),
    ("asset", Instrument()),
    ("line", Tick()),
    ("adjustment", Adjustment()),
    ("right", Right()),
    ))