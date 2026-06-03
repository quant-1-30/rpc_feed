# !/usr/bin/env python3
# -*- coding: utf-8 -*-

import numpy as np
import pandas as pd

from .node import Node
from rpc_feed.utils.wrapper import registry


@registry
class ProcessNa(Node):
    """Process Nan"""
    params = (
        ("na", 0),
        ("exclude", ["tick"]), 
    )

    def next(self, meta: pd.DataFrame):
        if meta.empty:
            return meta
            
        # abandon Numpy boolean mask 
        cols_to_fill = meta.columns.difference(self.p.exclude)
        numeric_cols = meta[cols_to_fill].select_dtypes(include=[np.number]).columns
        
        # Pandas fillna
        if len(numeric_cols) > 0:
            meta[numeric_cols] = meta[numeric_cols].fillna(self.p.na)
        return meta


@registry
class ProcessInf(Node):
    """Process infinity"""
    params = (
        ("inf", "mean"),
        ("lines", ["open", "high", "low", "close", "volume", "amount"])
    )

    def next(self, meta: pd.DataFrame):
        if meta.empty:
            return meta
            
        valid_cols = [c for c in self.p.lines if c in meta.columns]
        if not valid_cols:
            return meta

        # inf -> nan optimized C 
        meta[valid_cols] = meta[valid_cols].replace([np.inf, -np.inf], np.nan)
        
        # mean() median() ...  auto ignore Nan ---> ~np.isinf() avoid slice 
        if self.p.inf in ['mean', 'median', 'min', 'max']:
            replacement_vals = getattr(meta[valid_cols], self.p.inf)()
        else:
            # fallback
            try:
                proc = getattr(np, self.p.inf)
                replacement_vals = {col: proc(meta[col].dropna()) for col in valid_cols}
            except AttributeError:
                replacement_vals = {col: 0 for col in valid_cols}

        # bulk fill with fillna 
        meta[valid_cols] = meta[valid_cols].fillna(replacement_vals)
        meta.sort_index(inplace=True)
        return meta
