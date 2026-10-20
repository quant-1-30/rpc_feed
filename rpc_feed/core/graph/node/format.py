#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import polars as pl

from .node import Node
from rpc_feed.utils.wrapper import registry
from rpc_feed.utils.node_util import ensure_polars, to_polars_dtype


@registry
class StructDateParser(Node):
    _TDX_YEAR_MOD = 2048
    _TDX_YEAR_BASE = 2004
    _TDX_MONTH_DIV = 100
    _MINUTES_PER_HOUR = 60

    params = (
        ("lines", ["dates", "sub_dates"]),
        ("tz", "Asia/Shanghai"),
    )

    def next(self, meta: pl.DataFrame) -> pl.DataFrame:
        meta = ensure_polars(meta)
        if meta.is_empty():
            return meta

        d_name, s_name = self.p.lines[0], self.p.lines[1]
        dates_col = pl.col(d_name).cast(pl.Int64)
        sub_dates_col = pl.col(s_name).cast(pl.Int64)

        dt_expr = pl.datetime(
            year=dates_col // self._TDX_YEAR_MOD + self._TDX_YEAR_BASE,
            month=(dates_col % self._TDX_YEAR_MOD) // self._TDX_MONTH_DIV,
            day=(dates_col % self._TDX_YEAR_MOD) % self._TDX_MONTH_DIV,
            hour=sub_dates_col // self._MINUTES_PER_HOUR,
            minute=sub_dates_col % self._MINUTES_PER_HOUR,
            time_unit="ns",
        )
        tick_expr = dt_expr.dt.replace_time_zone(self.p.tz).dt.epoch("s")
        drop_cols = [c for c in self.p.lines if c in meta.columns]
        return meta.with_columns(datetime=dt_expr, tick=tick_expr).drop(drop_cols)

    def __repr__(self):
        return f"format: {self.__class__.__name__} (TZ: {self.p.tz})"


@registry
class UniverseDateParser(Node):
    params = (
        ("parser_col", "datetime"),
        ("format", "%Y%m%d %H:%M:%S"),
        ("tz", "Asia/Shanghai"),
    )

    def next(self, meta: pl.DataFrame) -> pl.DataFrame:
        meta = ensure_polars(meta)
        col = self.p.parser_col
        if meta.is_empty() or col not in meta.columns:
            return meta

        dt_expr = pl.col(col).str.to_datetime(format=self.p.format, time_unit="ns")
        tick_expr = dt_expr.dt.replace_time_zone(self.p.tz).dt.epoch("s")
        return meta.with_columns(dt_expr.alias(col), tick=tick_expr)


@registry
class Multiply(Node):
    params = (
        ("multiply", 1000),  # int / float / dict
        ("exclude", ["sid", "datetime", "tick"]),
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

        if isinstance(self.p.multiply, (int, float)):
            return meta.with_columns(
                (pl.col(numeric_cols).cast(pl.Float64) * self.p.multiply)
                .round(0, mode="half_to_even") # round to nearest even
                .cast(pl.Int64)
            )

        exprs = [
            (pl.col(c).cast(pl.Float64) * v).round(0, mode="half_to_even").cast(pl.Int64)
            for c, v in self.p.multiply.items()
            if c in numeric_cols
        ]
        return meta.with_columns(exprs) if exprs else meta


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

    def next(self, meta: pl.DataFrame) -> pl.DataFrame:
        meta = ensure_polars(meta)
        if meta.is_empty():
            return meta

        exprs = [
            pl.col(col).cast(to_polars_dtype(dtp))
            for col, dtp in self.p.dtypes.items()
            if col in meta.columns
        ]
        if exprs:
            meta = meta.with_columns(exprs)

        if self.p.pd_api_types:
            extra_exprs = []
            for col, dt in meta.schema.items():
                if isinstance(dt, pl.Datetime) and dt.time_zone is not None:
                    extra_exprs.append(pl.col(col).dt.replace_time_zone(None))
                elif isinstance(dt, (pl.Categorical, pl.Enum)):
                    extra_exprs.append(pl.col(col).cast(pl.String))
                elif isinstance(dt, pl.Object):
                    # Object can not be directly cast, must call str element-wise
                    extra_exprs.append(pl.col(col).map_elements(str, return_dtype=pl.String))

            if extra_exprs:
                meta = meta.with_columns(extra_exprs)
        return meta
