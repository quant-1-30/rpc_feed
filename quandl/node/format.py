#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import datetime
import pytz
import pandas as pd
from typing import Any, Iterator, Union
from utils.registry import registry
from meta import ParamBase


@registry
class UTC(ParamBase):

    params = (
        # ("alias", "utc"),
        ("local_tz", "Asia/Shanghai"),
        )

    def canonical_time(self, ts: int):
        if isinstance(ts, (int, float)):
            pass
        elif isinstance(ts, str):
            pass
        else:
            raise TypeError("")
        
    def _on_transform(self, dt: Union[datetime.datetime, str, datetime.datetime.timestamp]):
        # if not dt.tzinfo:
        #     dt = dt.replace(tzinfo=pytz.timezone(tz))
        # return dt.replace(tzinfo=pytz.utc) 
        tz = pytz.timezone("UTC")
        # pdb.set_trace()
        if isinstance(dt, datetime.datetime):
            local_dt = dt.tz_localize(tz=self.p.local_tz)
        elif isinstance(dt, datetime.datetime.timestamp):
            local_dt = datetime.datetime.fromtimestamp(dt, tz=self.p.local_tz)
        else:
            local_dt = datetime.datetime.strptime(dt, self.p.format)
        # utc_timestamp = utc_dt.astimezone(tz=tz).timestamp() 
        utc_dt = local_dt.astimezone(tz=tz)
        # pdb.set_trace()
        return utc_dt.timestamp()
    
    def on_handle(self, frame: pd.DataFrame) -> Any:
        """
            Converts a UTC tz-naive timestamp to a tz-aware timestamp.
            Normalize a time. If the time is tz-naive, assume it is UTC.
            Drop the nanoseconds field. warn=False suppresses the warning
            that we are losing the nanoseconds; however, this is intended.
            return pd.Timestamp(ts.to_pydatetime(warn=False), tz='UTC')
        """
        if len(frame):
            frame.loc[:, "tick"] = frame["timestamp"].apply(lambda x: self._on_transform(x))
        return frame

    def __repr__(self):
        format_string = "format: %s" % self.__class__.__name__
        return format_string


@registry
class Date2Int(ParamBase):

    params = (
        ("fields", ("trading_date", "first_trading", "delist", "register_date", "ex_date", "effective_date")),
        ("sep", ['/', '-', '*', '.', '~', '"', '^', '#'])
    )
    
    def _trans_dt(self, dt):
        sep = [s for s in self.p.sep if s in str(dt)]
        assert len(sep) <= 1, f"{sep} has at least one sep"
        if len(sep):
            dt = int(str(dt).replace(sep[0], ""))
        return dt

    def on_handle(self, frame: pd.DataFrame):
        if len(frame):
            cols = list(set(frame.columns) & set(self.p.fields))
            if cols:
                frame.loc[:, cols] = frame.loc[:, cols].map(lambda x: self._trans_dt(x))
        #         frame.loc[:, cols].replace(self.p.re, "", regex=True, inplace=True)
        return frame
