# !/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import struct
import pandas as pd
from avro.datafile import DataFileReader
from avro.io import DatumReader
from utils.registry import registry
from core.graph.base import Node


@registry
class StructUnpacker(Node):

    params=(
        ("pack", "HhIIIIfii"),
        ("buflen", 32), 
        ("sep", "."),
        ("lines", ["dates", "sub_dates", "open", "high", "low", "close", "amount", "volume", "appendix"]),
        ("duplicate", False),
        ("subset", None),
    )

    def __init__(self, kwargs):
        # update params from kwargs
        pass

    def next(self, path):
        frame=pd.DataFrame()
        if path:
            sid = os.path.basename(path).split(self.p.sep)[0][2:]
            with open(path, 'rb') as f:
                buf = f.read()
                size = int(len(buf) / self.p.buflen)
                data = []
                for num in range(size):
                    idx = 32 * num
                    line = struct.unpack(self.p.pack, buf[idx:idx + self.p.buflen])
                    data.append(line)
                frame = pd.DataFrame(data, columns=self.p.lines)
                # frame.index = [sid] * len(frame)
            frame.drop_duplicates(subset=self.p.subset, inplace=True) if self.p.subset else frame.drop_duplicates(inplace=True)
            frame.loc[:, "sid"] = sid
        return frame


@registry
class AvroUnpacker(Node):

    params=(
        ("duplicate", False),
        ("subset", None),
        )

    def __init__(self, kwargs):
        # update params from kwargs
        pass

    def next(self, path):
        frame = pd.DataFrame()
        if path:
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
    )

    def __init__(self, kwargs):
        # update params from kwargs
        pass

    def prenext(self, path):
        lines = []
        with open(path, "r") as f:
            for line in f.readlines:
                lines.append(line)
            return lines

    def next(self, path):
        if path.endswith(".csv"):
            frame = pd.read_csv(path, dtype=self.p.dtype, sep=self.p.csvsep)
        else:
            frame = self.prenext(path)
        frame.drop_duplicates(subset=self.p.subset, inplace=True) if self.p.subset else frame.drop_duplicates(inplace=True)
        return frame
