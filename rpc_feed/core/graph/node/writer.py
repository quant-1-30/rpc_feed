#!/usr/bin/env python3
# -*- coding: utf-8; py-indent-offset:4 -*-

import io
import sys
import avro.schema
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.dataset as ds

from typing import Any, Union, List, Optional
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter
from avro.datafile import DataFileWriter

from rpc_feed.core.graph.base import Node
from rpc_feed.utils.registry import registry
from rpc_feed.core.middleware.ops.operator import async_ops
from rpc_feed.utils.io import expand_path


@registry
class AvroWriter(Node):

    params = (("is_async", True),)

    def next(self, meta: pd.DataFrame, params: dict={}) -> Any:
        schema_path, data_path = params.values()
        # ticker.avsc
        schema = avro.schema.parse(open(schema_path, "rb").read())
        # users.avro 
        # reader = DataFileReader(open("users.avro", "rb"), DatumReader())
        # for user in reader:
        #     print (user)
        # reader.close()
        writer = DataFileWriter((open(data_path), "wb"), DatumWriter(), schema)
        dicts = meta.to_dict()
        for element in dicts.values:
            # element --- {"name": "Ben", "favorite_number": 7, "favorite_color": "red"}
            writer.append(element)
        writer.close()


@registry
class PgWriter(Node):
    """
        Postgresql Writer
    """
    params = (
        ("is_async", True),
    )
    
    async def next(self, meta: Union[pd.DataFrame, List[dict], dict], params: dict={}):
        async with async_ops as ctx:
            try:
                await ctx.on_insert(params["table"], meta)
                status = {"status": 0, "error": ""}
            except Exception as e:
                print("PgWriter Error", e)
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
        ("is_async", True),
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

    async def next(self, meta, params: dict={}):
        if meta:
            await self.writelineseparator()
            for l in meta:
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

    def next(self, meta, params: dict={}):
        fd = self.p.fd()
        if isinstance(meta, str):
            fd.write(meta)
        else:
            for chunk in meta:
                fd.write(str(chunk))
        # self.fetch_size = fd.tell()
        fd.close()

@registry
class ParquetWriter(Node):

    params = (
        ("is_async", True),
        # parquet 参数
        ("max_partitions", 10000),  # 最大分区数，增加以适应更多股票
        ("max_open_files", 1000),  # 最大打开文件数，增加以提高并发
        ("max_rows_per_file", 1000000),  # 每个文件最大行数，减小以优化读取性能
        ("max_rows_per_group", 100000),  # 每个行组 基础存储单位 列块
        ("max_file_size", "128MB"),  # 最大文件大小
        ("compression", "snappy"),  # 压缩方式
        ("row_group_size", 100000),  # 行组大小，影响读取性能
        ("use_dictionary", True),  # 使用字典编码
        ("write_statistics", True),  # 写入统计信息
        ("coerce_timestamps", "ms"),  # 时间戳精度
        ("allow_truncated_timestamps", False),  # 是否允许截断时间戳
        )
    
    def _make_partition(self, meta: pd.DataFrame) -> pd.DataFrame:
        meta["year"] = meta["datetime"].dt.year
        meta["quarter"] = meta['datetime'].apply(lambda x: f'Q{((x.month - 1) // 3) + 1}')
        meta["date"] = meta["datetime"].dt.strftime("%Y%m")
        partition_cols = ["year", "quarter", "sid", "date"]
        return meta, partition_cols

    def _make_schema(self, df: pd.DataFrame) -> pa.Schema:
        fields = []
        # for col in df.columns.difference(self.p.partition):
        for col in df.columns:
            if col == "datetime":
                fields.append(pa.field(col, pa.timestamp("ms")))
            elif col in ["quarter", "sid", "date"]:
                fields.append(pa.field(col, pa.string()))
            else:
                fields.append(pa.field(col, pa.from_numpy_dtype(df[col].dtype)))
        return pa.schema(fields)

    def _write_parquet(
        self,
        meta: pd.DataFrame,
        params: dict={}
    ):
        root_path = expand_path(params["root_path"])
        # set parquet partition and schema
        meta, partition_cols = self._make_partition(meta)
        schema = self._make_schema(meta)
        table = pa.Table.from_pandas(meta, schema=schema, preserve_index=False) # preserve_index 不作为单独一列 
        # # pq.write_table(table, out_file, compression='snappy')
        ds.write_dataset(
            data=table,
            base_dir=str(root_path),
            format="parquet",
            partitioning=partition_cols,  # 必须指定列名
            existing_data_behavior="overwrite_or_ignore",  # 避免重复写入报错
            max_rows_per_file=self.p.max_rows_per_file,  # 其他参数可参考写入优化
            max_rows_per_group=self.p.max_rows_per_group
        )

    async def next(self, meta: pd.DataFrame, params: dict = {}):
        """
        Async entry point for writing parquet using `to_thread` to avoid blocking.
        """
        self._write_parquet(meta, params)
