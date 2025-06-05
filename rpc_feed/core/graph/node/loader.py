# !/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import struct
import pandas as pd
from avro.datafile import DataFileReader
from avro.io import DatumReader

from rpc_feed.utils.registry import registry
from rpc_feed.core.graph.base import Node


@registry
class StructUnpacker(Node):
 
    """bytes ---> int () int.from_bytes() | struct.unpack(), 大端(human)与小端数据"""

    params=(
        ("pack", "HhIIIIfii"),
        ("buflen", 32), 
        ("sep", "."),
        ("lines", ["dates", "sub_dates", "open", "high", "low", "close", "amount", "volume", "appendix"]),
        ("duplicate", False),
        ("subset", None),
    )

    def next(self, path, params: dict={}):
        if not path:
            return pd.DataFrame()
        sid = os.path.basename(path).split(self.p.sep)[0][2:]
        frame = pd.DataFrame()
        with open(path, 'rb') as f:
            buf = f.read()
            size = int(len(buf) / self.p.buflen)
            data = []
            for num in range(size):
                idx = 32 * num
                line = struct.unpack(self.p.pack, buf[idx:idx + self.p.buflen])
                data.append(line)
            frame = pd.DataFrame(data, columns=self.p.lines)
        # postprocess
        frame.drop(columns="appendix", inplace=True)
        frame.drop_duplicates(subset=self.p.subset, inplace=True) if self.p.subset else frame.drop_duplicates(inplace=True)
        frame.attrs["sid"] = sid
        return frame


@registry
class AvroUnpacker(Node):

    params=(
        ("duplicate", False),
        ("subset", None),
        )

    def next(self, path, params: dict={}):
        if not path:
            return pd.DataFrame()
        frame = pd.DataFrame()
        # tick.avro
        arrays = []
        reader = DataFileReader(open(path, "rb"), DatumReader())
        for ele in reader:
            arrays.append(ele)
        reader.close()
        frame = pd.DataFrame(arrays)
        frame.drop_duplicates(subset=self.p.subset, inplace=True) if self.p.subset else frame.drop_duplicates(inplace=True)
        return frame


@registry
class TextLoader(Node):

    params = (
        ('csvsep', ','),
        ('csv_filternan', True),
        ('csv_counter', True),
        ('indent', 2),
        ('separators', ['=', '-', '+', '*', '.', '~', '"', '^', '#']),
        ('seplen', 79),
        ("dtype", {"sid": "str"}),
        ("alias", "avro"),
        ("subset", None),
    )

    def prenext(self, path):
        lines = []
        with open(path, "r") as f:
            for line in f.readlines:
                lines.append(line)
            return lines

    def next(self, path, params: dict={}):
        if not path:
            return []

        if path.endswith(".csv"):
            frame = pd.read_csv(path, dtype=self.p.dtype, sep=self.p.csvsep)
            frame.drop_duplicates(subset=self.p.subset, inplace=True) if self.p.subset else frame.drop_duplicates(inplace=True)
            values = list(frame.T.to_dict().values())
        else:
            values = self.prenext(path)
        return values
