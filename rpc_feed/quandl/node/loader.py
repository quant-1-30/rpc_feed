# !/usr/bin/env python3
# -*- coding: utf-8 -*-
import pdb
import os
import io
import struct
import pandas as pd
from avro.datafile import DataFileReader
from avro.io import DatumReader
from meta import ParamBase
from utils.registry import registry


@registry
class StructLoader(ParamBase):

    params=(
        # ("alias", "struct"),
        ("pack", "HhIIIIfii"),
        ("buflen", 32), 
        ("sep", "."),
        ("schema", ["dates", "sub_dates", "open", "high", "low", "close", "amount", "volume", "appendix"])
    )

    def on_handle(self, path):
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
                frame = pd.DataFrame(data, columns=self.p.schema)
                # frame.index = [sid] * len(frame)
            frame.loc[:, "sid"] = sid
        return frame


@registry
class AvroLoader(ParamBase):

    # params=(
    #     ("alias", "avro"),
    #     )

    def on_handle(self, path):
        frame = pd.DataFrame()
        if path:
            # tick.avro
            arrays = []
            reader = DataFileReader(open(path, "rb"), DatumReader())
            for ele in reader:
                arrays.append(ele)
            reader.close()
            frame = pd.DataFrame(arrays)
        return frame


@registry
class TextLoader(ParamBase):

    params = (
        ('csvsep', ','),
        ('csv_filternan', True),
        ('csv_counter', True),
        ('indent', 2),
        ('separators', ['=', '-', '+', '*', '.', '~', '"', '^', '#']),
        ('seplen', 79),
        ("dtype", {"sid": "str"})
    )

    def _readlines(self, path):
        lines = []
        with open(path, "r") as f:
            for line in f.readlines:
                lines.append(line)
            return lines

    def on_handle(self, path):
        if path.endswith(".csv"):
            frame = pd.read_csv(path, dtype=self.p.dtype, sep=self.p.csvsep)
        else:
            frame = self._readlines(path)
        return frame


# from io import StringIO

# output = StringIO()
# output.write('First line.\n')
# contents = output.getvalue()
# output.close()
# fd = StringIO()
# fd.tell()
# fd.seek(0)
# fd.close()
# fd = StringIO()
# if isinstance(data, str):
#     fd.write(data)
# else:
#     for chunk in data:
#         fd.write(chunk)
# self.fetch_size = fd.tell()
# fd.seek(0)
