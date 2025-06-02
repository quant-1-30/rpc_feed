# !/usr/bin/env python3
# -*- coding: utf-8 -*-

import numpy as np
import pandas as pd
from toolz import valmap

from rpc_feed.core.graph.base import Node
from rpc_feed.utils.registry import registry


@registry
class ProcessNa(Node):

    """Process Nan"""

    params = (
        ("na", 0),
        ("exclude", "tick"), 
        )

    def prenext(self, df):
        # df.fillna(self.fill_value, inplace=True)
        # this implementation is extremely slow
        # df.fillna({col: self.fill_value for col in cols}, inplace=True)
        # So we use numpy to accelerate filling values
        nan_select = np.isnan(df.values)
        nan_select[:, ~df.columns.isin(self.p.exclude)] = False
        df.values[nan_select] = self.p.na  
        return df

    def next(self, meta: pd.DataFrame, params: dict={}):
        # inf and na
        if len(meta):
            meta = self.prenext(meta)  
        return meta


@registry
class ProcessInf(Node):

    """Process infinity"""

    params = (
        ("inf", "mean"),
        ("lines", ["open", "high", "low", "close", "volume", "amount"])
        )

    def __init__(self):
        self.proc = getattr(np, self.p.inf, "")

    def prenext(self, df):
        for col in self.p.lines:
            # FIXME: Such behavior is very weird
            # df[col] = df[col].replace([np.inf, -np.inf], df[col][~np.isinf(df[col])].mean())
            df[col] = df[col].replace([np.inf, -np.inf], self.proc(df[col][~np.isinf(df[col])]))
        df.sort_index(inplace=True)
        return df

    def next(self, meta: pd.DataFrame, params: dict={}):
        # # validate columns
        if len(meta):
            meta = self.prenext(meta)
        return meta
    