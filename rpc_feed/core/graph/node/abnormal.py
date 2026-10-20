# !/usr/bin/env python3
# -*- coding: utf-8 -*-

import polars as pl

from .node import Node
from rpc_feed.utils.wrapper import registry
from rpc_feed.utils.node_util import ensure_polars


@registry
class ProcessNa(Node):
    """Process Nan / Null"""
    params = (
        ("na", 0),
        ("exclude", ["tick"]),
    )

    def next(self, meta: pl.DataFrame) -> pl.DataFrame:
        meta = ensure_polars(meta)
        exclude = set(self.p.exclude or ())
        numeric_cols = [
            c for c, dt in meta.schema.items()
            if c not in exclude and dt.is_numeric()
        ]
        if meta.is_empty() or not numeric_cols:
            return meta
        return meta.with_columns(
            pl.col(numeric_cols).fill_nan(self.p.na).fill_null(self.p.na)
        )


@registry
class ProcessInf(Node):
    """Process infinity"""
    params = (
        ("inf", "mean"),
        ("lines", ["open", "high", "low", "close", "volume", "amount"]),
    )

    def next(self, meta: pl.DataFrame) -> pl.DataFrame:
        meta = ensure_polars(meta)
        valid_cols = [
            c for c in self.p.lines
            if c in meta.columns and meta.schema[c].is_float()
        ]
        if meta.is_empty() or not valid_cols:
            return meta

        cols = pl.col(valid_cols)
        finite = pl.when(cols.is_finite()).then(cols)
        stat = getattr(finite, self.p.inf)() if hasattr(finite, str(self.p.inf)) else 0.0
        return meta.with_columns(
            pl.when(cols.is_infinite()).then(stat).otherwise(cols)
        )
