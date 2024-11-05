# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import numpy as np
import pandas as pd
from sqlalchemy import select
from sqlalchemy.orm import Session
from typing import List, Union, Optional, Dict
from meta import with_metaclass, MetaBase
from utils.cache import lazyproperty
from core.model import *


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
    
    def get_data(self, req):
        raise NotImplementedError("implement get_data method")

    def __len__(self):
        raise NotImplementedError("length")
    
    def __getitem__(self, index):
        raise NotImplementedError("getitem")
    

class Calendar(BaseProvider):
    """Calendar provider base class

    Provide calendar data.
    """
    params = (
        ("table", "calendar"),
    )

    @lazyproperty
    def calendar(self):
        """
            Get calendar of certain market in given time range. 
        """
        calendar = []
        cal = self.tables[self.p.table]
        with Session(bind=self.engine) as session:
            # stmt = select(cal).execution_options(**self.options)
            stmt = select(cal.c.trading_date)
            for trading_dt in session.scalars(stmt).yield_per(10):
            # for trading_dt in session.execute(stmt).yield_per(10):
            # for trading_dt in session.query(cal).all():
                calendar.append(Calendar(trading_dt))
        return calendar
    
    def __len__(self):
        return len(self.calendar)
    
    def __getitem__(self, index):
        return self.calendar[index]
    
    def get_data(self, request: Request):
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
        start_time, end_time = request.range.serialize()
        if start_time == "None":
            start_time = None
        if end_time == "None":
            end_time = None
        # strip
        if start_time:
            start_time = pd.Timestamp(start_time)
            if start_time > self.calendar[-1].trading_date:
                return np.array([])
        else:
            start_time = self.calendar[0]
        if end_time:
            end_time = pd.Timestamp(end_time)
            if end_time < self.calendar[0].trading_date:
                return np.array([])
        else:
            end_time = self.calendar[-1]
        _, _, si, ei = self.locate_index(start_time, end_time)
        return self.calendar[si : ei + 1]


class Instrument(BaseProvider):
    """Instrument provider base class

    Provide instrument data.
    """
    params = (
        ("table", "instrument"),
    )

    @lazyproperty
    def instruments(self):
        """List the overall instruments.

        Returns
        -------
        dict or list
            instruments list or dictionary with time spans
        """
        assets = list()
        inst = self.tables[self.p.table]
        with Session(self.engine) as session:
            # stmt = select(inst).execution_options(**self.options)
            stmt = select(inst)
            # for sid in self.session.scalars(stmt).yield_per(10):
            for sid in session.execute(stmt).yield_per(10):
                assets.append(Asset(*sid[1:]).serialize())
        return assets

    def __len__(self):
        return len(self.instruments)
    
    def __getitem__(self, sid):
        item = [sid for sid in self.instruments if sid.sid == sid]
        return item[0]

    def get_data(self, message: Dict[str, Union[str, int]]):
        """Get the existiongconfig dictionary for a base market adding several dynamic filters.
        """
        req = self.trans2Req(message)
        assets = [sid for sid in self.instruments if sid.first_trading_date >= req.start 
                        and sid.first_trading_date <= req.end]
        assets = [sid for sid in assets if sid.delist > req.end]
        return assets
    
    
class Line(BaseProvider):
    """Dataset provider class
    Provide Dataset data.
    """
    params = (
        ("table", "minute"),
    )

    def get_data(self, req: Request):
        """Get dataset data.

        Parameters
        ----------
        request:  DataRequest

        Returns
        ----------
        pd.DataFrame
            a pandas dataframe with <instrument, datetime> index.
        """
        dataset = self.tables[self.p.table]
        with Session(self.engine) as session:
            # stmt = select(dataset).where(dataset.utc.between(*request.range)).execution_options(**self.options)
            stmt = select(dataset).where(dataset.c.tick.between(req.start, req.end))
            # session.scalars(stmt).yield_per(10):
            for line in session.execute(stmt).yield_per(10):
                meta = Line(*line[1:]).serialize()
                yield meta
    

class Adjustment(BaseProvider):
    """
        Calendar provider base class
        Provide calendar data.
    """
    params = (
        ("table", "adjustment"),
    )

    def get_data(self, req: Request):
        """Get dvidends of certain asset in given time range.

        Parameters
        ----------
        request : Request
            start of the time range.

        Returns
        ----------
        """
        adj = self.tables[self.p.table]
        with Session(self.engine) as session:
            stmt = select(adj).where(adj.c.ex_date.between(req.start, req.end))
            for data in session.execute(stmt).yield_per(10):
                meta = Dividend(*data[1:]).serialize()
                yield meta 


class Right(BaseProvider):
    """
        Calendar provider base class
        Provide calendar data.
    """
    params = (
        ("table", "rightment"),
    )

    def get_data(self, req: Request):
        """Get dvidends of certain asset in given time range.

        Parameters
        ----------
        request : Request
            start of the time range.

        Returns
        ----------
        """
        rgt = self.tables[self.p.table]
        with Session(self.engine) as session:
            stmt = select(rgt).where(rgt.c.ex_date.between(req.start, req.end))
            for data in session.execute(stmt).yield_per(10):
                meta = Rightment(*data[1:]).serialize()
                yield meta 


Providers = dict(
    (("calendar", Calendar()),
    ("instrument", Instrument()),
    ("line", Line()),
    ("adjustment", Adjustment()),
    ("right", Right()),
    ))