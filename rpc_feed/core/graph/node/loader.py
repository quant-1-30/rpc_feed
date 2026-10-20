# !/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import struct
import numpy as np
import polars as pl
from avro.datafile import DataFileReader
from avro.io import DatumReader

from .node import Node
from rpc_feed.utils.wrapper import registry
from rpc_feed.utils.node_util import build_struct_np_dtype, to_polars_dtype


@registry
class StructUnpacker(Node):
    """bytes ---> Polars DataFrame, big end (human) and small end"""

    params = (
        ("pack", "HhIIIIfii"),
        ("buflen", 32),
        ("sep", "."),
        ("lines", ["dates", "sub_dates", "open", "high", "low", "close", "amount", "volume", "appendix"]),
        ("duplicate", False),
        ("subset", None),
        ("path", ""),
    )

    def next(self, input_path):
        if not input_path:
            return pl.DataFrame()
        sid = os.path.basename(input_path).split(self.p.sep)[0][2:]
        with open(input_path, "rb") as f:
            buf = f.read()

        size = len(buf) // self.p.buflen
        if size == 0:
            return pl.DataFrame()
        raw_slice = memoryview(buf)[:size * self.p.buflen]

        np_dt = build_struct_np_dtype(self.p.pack, self.p.lines, self.p.buflen)
        if np_dt is not None:
            frame = pl.from_numpy(np.frombuffer(raw_slice, dtype=np_dt))
        else:
            data = list(struct.iter_unpack(self.p.pack, raw_slice))
            frame = pl.DataFrame(data, schema=list(self.p.lines), orient="row")

        if "appendix" in frame.columns:
            frame = frame.drop("appendix")
        subset = list(self.p.subset) if self.p.subset else None
        frame = frame.unique(subset=subset, keep="first", maintain_order=True)
        return frame.with_columns(pl.lit(sid).alias("sid"))


@registry
class AvroUnpacker(Node):

    params = (
        ("duplicate", False),
        ("subset", None),
    )

    def next(self, input_path):
        if not input_path:
            return pl.DataFrame()

        with open(input_path, "rb") as f:
            reader = DataFileReader(f, DatumReader())
            arrays = list(reader)
            reader.close()

        frame = pl.DataFrame(arrays)
        if not frame.is_empty():
            subset = list(self.p.subset) if self.p.subset else None
            frame = frame.unique(subset=subset, keep="first", maintain_order=True)
        return frame


@registry
class TextLoader(Node):

    params = (
        ("csvsep", ","),
        ("csv_filternan", True),
        ("csv_counter", True),
        ("indent", 2),
        ("separators", ["=", "-", "+", "*", ".", "~", '"', "^", "#"]),
        ("seplen", 79),
        ("dtype", {"sid": "str"}),
        ("alias", ""),
        ("subset", []),
        ("rename", {}),
        ("columns", []),
    )

    def prenext(self, input_path):
        with open(input_path, "r") as f:
            return f.readlines()

    def next(self, input_path):
        if not input_path:
            return pl.DataFrame()

        if input_path.endswith(".csv"):
            inv_rename = {v: k for k, v in (self.p.rename or {}).items()}
            schema_overrides = {
                inv_rename.get(col, col): to_polars_dtype(dt)
                for col, dt in (self.p.dtype or {}).items()
            }
            frame = pl.read_csv(
                input_path,
                separator=self.p.csvsep,
                schema_overrides=schema_overrides,
                infer_schema_length=None,
            )
            if self.p.rename:
                rename_map = {k: v for k, v in self.p.rename.items() if k in frame.columns}
                if rename_map:
                    frame = frame.rename(rename_map)
            if not frame.is_empty():
                subset = list(self.p.subset) if self.p.subset else None
                frame = frame.unique(subset=subset, keep="first", maintain_order=True)
        else:
            lines = self.prenext(input_path)
            cols = list(self.p.columns) if self.p.columns else ["line"]
            frame = pl.DataFrame({cols[0]: lines}) if len(cols) == 1 else pl.DataFrame(lines, schema=cols, orient="row")
        return frame
