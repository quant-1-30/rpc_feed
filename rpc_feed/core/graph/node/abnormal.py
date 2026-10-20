# !/usr/bin/env python3
# -*- coding: utf-8 -*-

import polars as pl

from .node import Node
from rpc_feed.utils.wrapper import registry
from rpc_feed.utils.node_util import ensure_polars


_STRATEGY_MAP = {
    "forward": "forward",
    "ffill": "forward",
    "backward": "backward", # loopahead bias
    "bfill": "backward", # loopahead bias
    "zero": "zero",
}


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
    """Process infinity without lookahead bias"""

    params = (
        ("inf", "forward"),
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
        clean = pl.when(~cols.is_infinite()).then(cols)
        strategy = _STRATEGY_MAP.get(str(self.p.inf).lower())
        expr = clean.fill_null(strategy=strategy) if strategy else clean.fill_null(0.0)
        if "sid" in meta.columns:
            expr = expr.over("sid")
        return meta.with_columns(expr.fill_null(0.0))
