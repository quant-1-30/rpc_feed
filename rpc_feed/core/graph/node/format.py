#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import datetime
import pytz
import pandas as pd
import numpy as np
from typing import Any, Iterator, Union
from toolz import valmap
from utils.registry import registry
from utils.utility import no_hup
from core.graph.base import Node


@registry
class Decode(Node):

    params = (
        ("lines", ["dates", "sub_dates"]),
        )

    def prenext(self, df: pd.DataFrame):
        df = df.to_dict()
        df = valmap(lambda x: int(x), df)
        year = df['dates'] // 2048 + 2004
        month = (df['dates'] % 2048) // 100
        day = (df['dates'] % 2048) % 100
        hour = df['sub_dates'] // 60
        minute = df['sub_dates'] % 60
        tick = datetime.datetime(year, month, day, hour, minute)
        return tick
    
    def next(self, df: pd.DataFrame):
        if df:
            assert "dates" in df.columns, "missing dates column"
            df["tick"] = df.loc[:, self.p.lines].apply(lambda _slice: self.prenext(_slice), axis=1)
        return df

    def __repr__(self):
        format_string = "format: %s" % self.__class__.__name__
        return format_string


@registry
class UTC(Node):

    params = (
        ("lines", "tick"),
        ("tz", "Asia/Shanghai"),
        ("fmt", "%Y-%m-%d %H:%M"),
        )

    def prenext(self, dt: Any):
        # if not dt.tzinfo:
        #     dt = dt.replace(tzinfo=pytz.timezone(tz))
        # return dt.replace(tzinfo=pytz.utc) 
        tz = pytz.timezone("UTC")
        if isinstance(dt, datetime.datetime):
            localized = dt.tz_localize(tz=self.p.tz)
        elif isinstance(dt, datetime.datetime.timestamp):
            localized = datetime.datetime.fromtimestamp(dt, tz=self.p.tz)
        else:
            localized = datetime.datetime.strptime(dt, self.p.fmt)
        utc_timestamp = localized.astimezone(tz=tz).timestamp() 
        return utc_timestamp
    
    def next(self, df: pd.DataFrame) -> Any:
        """
            Converts a UTC tz-naive timestamp to a tz-aware timestamp.
            Normalize a time. If the time is tz-naive, assume it is UTC.
            Drop the nanoseconds field. warn=False suppresses the warning
            that we are losing the nanoseconds; however, this is intended.
            return pd.Timestamp(ts.to_pydatetime(warn=False), tz='UTC')
        """
        if df:
            df["tick"] = df["tick"].apply(lambda x: self.prenext(x))
        return df

    def __repr__(self):
        format_string = "format: %s" % self.__class__.__name__
        return format_string


@registry
class Multiply(Node):

    params = (
        ("multiply", 1000),
        ("lines", ["amount"])
        )
    
    def prenext(self, df: pd.DataFrame):
        for col in self.p.lines:
            df[col] = df[col].map(lambda x: np.float32(x) * self.p.multiply)
        return df

    def next(self, df: pd.DataFrame):
        if df:
            df = self.prenext(df)
        return df
