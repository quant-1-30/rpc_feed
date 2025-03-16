# !/usr/bin/env python3
# -*- coding: utf-8 -*-

import datetime
import numpy as np
import pandas as pd
from toolz import valmap
from meta import ParamBase
from utils.registry import registry


@registry
class ProcessNa(ParamBase):

    """Process Nan"""

    params = (
        ("na", 0),
        ("fields", ["open", "high", "low", "close", "volume", "amount"])
        )

    def _replace_na(self, df, cols=None):
        # df.fillna(self.fill_value, inplace=True)
        # this implementation is extremely slow
        # df.fillna({col: self.fill_value for col in cols}, inplace=True)
        # So we use numpy to accelerate filling values
        if cols:
            nan_select = np.isnan(df.values)
            nan_select[:, ~df.columns.isin(cols)] = False
            df.values[nan_select] = self.p.na  
        else:
            df.fillna(self.p.na, inplace=True) 
        return df

    def on_handle(self, df: pd.DataFrame):

        # # validate columns
        # if columns:
        #     assert set(columns) in set(df.columns), ValueError("illegal f{columns}")
        
        # inf and na
        if len(df):
            df = self._replace_na(df)  
        return df


@registry
class ProcessInf(ParamBase):

    """Process infinity"""

    params = (
        ("inf", "mean"),
        ("fields", ["open", "high", "low", "close", "volume", "amount"])
        )

    def _inf_func(self):
        f = getattr(np, self.p.inf, "")
        if not f:
            raise NotImplementedError("{self.p.inf} need to implemented")
        return f

    def _replace_inf(self, df):
        proc = self._inf_func()
        for col in self.p.fields:
            # FIXME: Such behavior is very weird
            # df[col] = df[col].replace([np.inf, -np.inf], df[col][~np.isinf(df[col])].mean())
            df[col] = df[col].replace([np.inf, -np.inf], proc(df[col][~np.isinf(df[col])]))
        df.sort_index(inplace=True)
        return df

    def on_handle(self, df: pd.DataFrame):
        
        # # validate columns
        if len(df):
            df = self._replace_inf(df)
        return df
    

@registry
class Multiply(ParamBase):

    params = (
        # ("alias", "multiply"),
        ("multiply", 1000),
        ("fields", ["amount"])
        )
    
    def __init__(self, fields=None):
        self.fields = fields

    def on_handle(self, frame: pd.DataFrame):
        trans_fields = self.fields if self.fields else self.p.fields
        if len(frame):
            for col in trans_fields:
                frame[col] = frame[col].map(lambda x: np.float32(x) * self.p.multiply)
                # pdb.set_trace()
        return frame


@registry
class Duplicate(ParamBase):
    params = (
        ("sort", "first_trading"),
        ("subset", "sid")
    )

    def on_handle(self, frame: pd.DataFrame):
        if len(frame):
            # 不可能存在多次退市, 中间退市的股票只会显示上市列表
            frame.sort_values(by=self.p.sort, ascending=True, inplace=True)
            frame.drop_duplicates(subset=[self.p.subset], keep="last", inplace=True)
        return frame
    

@registry
class ParseDate(ParamBase):

    params = (
        # ("alias", "normalize"),
        ("fields", ["dates", "sub_dates"]),
        )

    def _parse_date(self, frame):
        frame = frame.to_dict()
        frame = valmap(lambda x: int(x), frame)
        year = frame['dates'] // 2048 + 2004
        month = (frame['dates'] % 2048) // 100
        day = (frame['dates'] % 2048) % 100
        hour = frame['sub_dates'] // 60
        minutes = frame['sub_dates'] % 60
        tick = datetime.datetime(year, month, day, hour, minutes)
        return tick
    
    def on_handle(self, frame: pd.DataFrame):
        if len(frame):
            assert "dates" in frame.columns, "missing dates column"
            trans_dates = frame.loc[:, self.p.fields].apply(lambda x: self._parse_date(x), axis=1)
            frame.loc[:, "timestamp"] = trans_dates
        return frame

    def __repr__(self):
        format_string = "format: %s" % self.__class__.__name__
        return format_string
