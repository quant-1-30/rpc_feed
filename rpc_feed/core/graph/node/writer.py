#!/usr/bin/env python3
# -*- coding: utf-8; py-indent-offset:4 -*-

import io
import sys
import avro.schema
import pandas as pd
from typing import Any, Union, List
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter
from avro.datafile import DataFileWriter

from core.graph.base import Node
from utils.registry import registry
from core.writer.operator import async_ops


@registry
class AvroWriter(Node):

    params = (("is_async", False),)

    def next(self, schema_path: str, data_path: str, frame: pd.DataFrame) -> Any:
        # ticker.avsc
        schema = avro.schema.parse(open(schema_path, "rb").read())
        # users.avro 
        writer = DataFileWriter((open(data_path), "wb"), DatumWriter(), schema)
        dicts = frame.to_dict()
        for element in dicts.values:
            # element --- {"name": "Ben", "favorite_number": 7, "favorite_color": "red"}
            writer.append(element)
        writer.close()
        # reader = DataFileReader(open("users.avro", "rb"), DatumReader())
        # for user in reader:
        #     print (user)
        # reader.close()


@registry
class PgWriter(Node):
    """
        Postgresql Writer
    """
    params = (
        ("table", ""),
        ("is_async", True),
    )
    
    def __init__(self, kwargs):
        if "table" not in kwargs:
            raise TypeError
    
    async def next(self, data: Union[pd.DataFrame, List[dict], dict]):
        async with async_ops as ctx:
            try:
                await ctx.on_insert_val(self.p.table, data)
                status = {"status": 0, "error": ""}
            except Exception as e:
                print(e)
            status = {"status": 1, "error": str(e)}
        return status


@registry
class CsvWriter(Node):

    '''The system wide writer class.
    It can be parametrized with:

      - ``out`` (default: ``sys.stdout``): output stream to write to

        If a string is passed a filename with the content of the parameter will
        be used.

        If you wish to run with ``sys.stdout`` while doing multiprocess optimization, leave it as ``None``, which will
        automatically initiate ``sys.stdout`` on the child processes.

      - ``csv`` (default: ``False``)

        If a csv stream of the data feeds, strategies, observers and indicators
        has to be written to the stream during execution

        Which objects actually go into the csv stream can be controlled with
        the ``csv`` attribute of each object (defaults to ``True`` for ``data
        feeds`` and ``observers`` / False for ``indicators``)

      - ``csv_filternan`` (default: ``True``) whether ``nan`` values have to be
        purged out of the csv stream (replaced by an empty field)

      - ``csv_counter`` (default: ``True``) if the writer shall keep and print
        out a counter of the lines actually output

      - ``indent`` (default: ``2``) indentation spaces for each level

      - ``separators`` (default: ``['=', '-', '+', '*', '.', '~', '"', '^',
        '#']``)

        Characters used for line separators across section/sub(sub)sections

      - ``seplen`` (default: ``79``)

        total length of a line separator including indentation

    '''
    params = (
        ('out', None),
        ('indent', 2),
        ("headers", ""),
        ("separator", ","),
        ("is_async", False),
    )

    def _start_output(self):
        # open file if needed
        if self.p.out is None:
            out = sys.stdout
        elif isinstance(self.p.out, str):
            out = open(self.p.out, 'w')
        else:
            out = self.p.out
        return out
    
    def __init__(self):
        self.out = self._start_output

    def stop(self):
        self.out.close()

    async def _writeline(self, line):
        await self.out.write(line + '\n')

    async def writelineseparator(self):
        if self.p.headers:
            headers = self.p.headers.split(self.p.separator)
            sep = ' ' * self.p.indent + self.p.separator
            csv_header = sep.join(headers)
            await self._writeline(csv_header)

    async def next(self, lines):
        if lines:
            await self.writelineseparator()
            for l in lines:
                await self._writeline(l + '\n')
            self.stop() 

        
@registry
class WriterStringIO(Node):

    params = (
        ('fd', io.StringIO),
        ("is_async", False),
        )
    # output.write('First line.\n')
    # contents = output.getvalue()
    # output.close()
    # sys.stdout = output / print redirect to getvalue

    def __init__(self):
        super(WriterStringIO, self).__init__()

    def reset(self):
        # super(WriterStringIO, self).stop()
        # Leave the file positioned at the beginning
        self.out.seek(0)

    def next(self, data):
        fd = self.p.fd()
        if isinstance(data, str):
            fd.write(data)
        else:
            for chunk in data:
                fd.write(str(chunk))
        # self.fetch_size = fd.tell()
        fd.close()
