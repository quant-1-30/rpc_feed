#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import datetime
import pandas as pd
import numpy as np

from rpc_feed.utils.wrapper import registry
from .node import Node


@registry
class StructDateParser(Node):
    params = (
        ("lines", ["dates", "sub_dates"]),
        ("tz", "Asia/Shanghai"),
    )

    def next(self, meta: pd.DataFrame):
        if not meta.empty:
            # =======================================================
            # 1. vectorvize replace apply 
            # =======================================================
            dates_col = meta["dates"]
            sub_dates_col = meta["sub_dates"]

            years = dates_col // 2048 + 2004
            months = (dates_col % 2048) // 100
            days = (dates_col % 2048) % 100
            hours = sub_dates_col // 60
            minutes = sub_dates_col % 60

            # pd.to_datetime fast than apply and Naive Datetime
            meta["datetime"] = pd.to_datetime(pd.DataFrame({
                'year': years,
                'month': months,
                'day': days,
                'hour': hours,
                'minute': minutes
            }))

            # =======================================================
            # 2. timezone align
            # =======================================================
            # tz_localize("Asia/Shanghai") 
            localized_dt = meta["datetime"].dt.tz_localize(self.p.tz)
            # Pandas auto calculate the offset to UTC Epoch, then get int64 tick
            meta["tick"] = (localized_dt.view("int64") // 10**9).astype("int64")
            
            # meta["datetime"] = localized_dt.dt.tz_convert("UTC") # tz_convert(None)
            meta.drop(columns=["dates", "sub_dates"], inplace=True)
        return meta

    def __repr__(self):
        return f"format: {self.__class__.__name__} (TZ: {self.p.tz})"


@registry
class UniverseDateParser(Node):
    params = (
        ("parser_col", "datetime"),
        ("format", "%Y%m%d %H:%M:%S"),
    )

    def next(self, meta: pd.DataFrame): 
        col = self.p.parser_col
        if not meta.empty and col in meta.columns:
            # abandon apply + strptime
            # pd.to_datetime C vectorize
            meta[col] = pd.to_datetime(meta[col], format=self.p.format)
            
            meta["tick"] = meta[col].view("int64") // 10**9 # view zero-copy
        return meta


@registry
class Multiply(Node):
    params = (
        ("multiply", 1000), # int / map
        ("exclude", ["sid", "datetime", "tick"]),
    )

    def next(self, meta: pd.DataFrame):
        if meta.empty:
            return meta

        cols_to_scale = meta.columns.difference(self.p.exclude)
        numeric_cols = meta[cols_to_scale].select_dtypes(include=[np.number]).columns

        if len(numeric_cols) == 0:
            return meta

        if isinstance(self.p.multiply, (int, float)):
            # DataFrame .mul() is vectorized
            meta[numeric_cols] = meta[numeric_cols].mul(self.p.multiply).astype(np.int64)
        else:
            # multiply is dict and only apply to specified columns 
            # pd.Series broadcasting: align index with numeric_cols and multiply, then astype once
            mult_series = pd.Series({k: v for k, v in self.p.multiply.items() if k in numeric_cols})
            if not mult_series.empty:
                meta[mult_series.index] = meta[mult_series.index].mul(mult_series).astype(np.int64)
        return meta


@registry
class Dtypes(Node):
    params = (
        ("pd_api_types", False),
        ("dtypes", {
            "open": "int64",
            "high": "int64",
            "low": "int64",
            "close": "int64",
            "amount": "int64",
            "volume": "int64",
        }),
    )
    
    def next(self, meta: pd.DataFrame):
        if meta.empty:
            return meta

        # avoid for 
        valid_dtypes = {col: dtp for col, dtp in self.p.dtypes.items() if col in meta.columns}
        if valid_dtypes:
            meta = meta.astype(valid_dtypes)
        
        if self.p.pd_api_types:
            # Pandas abandon api.types  
            # is_datetime64tz_dtype
            tz_cols = meta.select_dtypes(include=["datetimetz"]).columns
            for col in tz_cols:
                meta[col] = meta[col].dt.tz_localize(None)
                
            # is_categorical_dtype 
            cat_cols = meta.select_dtypes(include=["category"]).columns
            if not cat_cols.empty:
                meta[cat_cols] = meta[cat_cols].astype(str)
                
            # is_object_dtype
            obj_cols = meta.select_dtypes(include=["object"]).columns
            if not obj_cols.empty:
                meta[obj_cols] = meta[obj_cols].astype(str)
        return meta
