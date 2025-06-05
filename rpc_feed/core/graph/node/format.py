#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import datetime
import pytz
import pandas as pd
import numpy as np
from typing import Any, Iterator, Union

from rpc_feed.utils.registry import registry
from rpc_feed.core.graph.base import Node


@registry
class DateParser(Node):

    params = (
        ("lines", ["dates", "sub_dates"]),
        ("tz", "Asia/Shanghai"),
        ("precision", "ms"),
        )

    def prenext(self, ele: pd.Series):
        dates = ele['dates']
        sub_dates = ele['sub_dates']
        # 
        year = dates // 2048 + 2004
        month = (dates % 2048) // 100
        day = (dates % 2048) % 100
        hour = sub_dates // 60
        minute = sub_dates % 60
        dt = datetime.datetime(year, month, day, hour, minute)
        return pd.to_datetime(dt)
    
    def next(self, meta: pd.DataFrame, params: dict={}):
        if len(meta):
            assert "dates" in meta.columns, "missing dates column"
            meta["datetime"] = meta.loc[:, self.p.lines].apply(lambda ele: self.prenext(ele), axis=1)
            meta["tick"] = (meta["datetime"].astype("int64") // 10**9).astype("int64")
            # meta["date_str"] = meta["datetime"].dt.strftime("%Y%m")
            # remove
            meta.drop(columns=["dates", "sub_dates"], inplace=True)
        return meta

    def __repr__(self):
        format_string = "format: %s" % self.__class__.__name__
        return format_string


@registry
class Multiply(Node):

    params = (
        ("multiply", 1000),
        ("exclude", ["sid", "datetime", "tick"]),
    )

    def prenext(self, ele: pd.DataFrame):
        # 获取非排除列 & 数值列
        cols_to_scale = ele.columns.difference(self.p.exclude)
        numeric_cols = ele[cols_to_scale].select_dtypes(include=[np.number]).columns

        # 放大并压缩精度
        ele[numeric_cols] = (ele[numeric_cols] * self.p.multiply).astype(np.int32)

        return ele

    def next(self, meta: pd.DataFrame, params: dict = {}):
        if not meta.empty:
            meta = self.prenext(meta)
        return meta


@registry
class Dtypes(Node):

    params = (
        ("is_parquet", False),
        ("dtypes", {
            "open": "int64",
            "high": "int64",
            "low": "int64",
            "close": "int64",
            "amount": "int64",
            "volume": "int64",}
    ),)

    # pyarrow.lib.ArrowNotImplementedError: Unsupported numpy type 17 --- 时区的 datetime64
    def dtype_for_parquet(meta: pd.DataFrame) -> pd.DataFrame:
        for col in meta.columns:
            if pd.api.types.is_datetime64tz_dtype(meta[col]):
                meta[col] = meta[col].dt.tz_localize(None)
            elif pd.api.types.is_categorical_dtype(meta[col]):
                meta[col] = meta[col].astype(str)
            elif pd.api.types.is_object_dtype(meta[col]):
                meta[col] = meta[col].astype(str)
        return meta
    
    def next(self, meta: pd.DataFrame, params: dict={}):
            # 设置数据类型
        for col, dtype in self.p.dtypes.items():
            if col in meta.columns:
                meta[col] = meta[col].astype(dtype)
        
        is_parquet = params.get("is_parquet", self.p.is_parquet)
        if is_parquet:
            meta = self.dtype_for_parquet(meta)
        return meta

