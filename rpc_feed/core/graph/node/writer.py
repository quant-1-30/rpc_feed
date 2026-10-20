#!/usr/bin/env python3
# -*- coding: utf-8; py-indent-offset:4 -*-

import os
import io
import re
import asyncio
import datetime
import sys
import avro.schema
import pandas as pd
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.dataset as ds

from pathlib import Path
from typing import Any, Union, List, Optional
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter

from .node import Node
from rpc_feed.core.gateway import async_ops
from rpc_feed.utils.wrapper import registry
from rpc_feed.utils.io import expand_path, upsert_daily_from_arrow_table
from rpc_feed.utils.node_util import ensure_polars


@registry
class AvroWriter(Node):

    params = (
        ("is_writer", True),
        ("is_async", True),
        ("schema_path", ""),
        ("data_path", ""),
    )

    async def next(self, meta: Union[pl.DataFrame, pd.DataFrame]) -> Any:
        meta = ensure_polars(meta)
        with open(self.p.schema_path, "rb") as sf:
            schema = avro.schema.parse(sf.read())
        with DataFileWriter(open(self.p.data_path, "wb"), DatumWriter(), schema) as writer:
            for element in meta.to_dicts():
                writer.append(element)


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
        ("out", None),
        ("indent", 2),
        ("headers", ""),
        ("separator", ","),
        ("is_writer", True),
        ("is_async", True),
    )

    def _start_output(self):
        # open file if needed
        if self.p.out is None:
            out = sys.stdout
        elif isinstance(self.p.out, str):
            out = open(self.p.out, "w")
        else:
            out = self.p.out
        return out
    
    def __init__(self):
        self.out = self._start_output()

    def stop(self):
        self.out.close()

    def _writeline(self, line):
        self.out.write(line + "\n")

    async def writelineseparator(self):
        if self.p.headers:
            headers = self.p.headers.split(self.p.separator)
            sep = " " * self.p.indent + self.p.separator
            csv_header = sep.join(headers)
            self._writeline(csv_header)

    async def next(self, meta):
        if meta:
            await self.writelineseparator()
            for l in meta:
                self._writeline(l + "\n")
            self.stop()

        
@registry
class WriterStringIO(Node):

    params = (
        ("fd", io.StringIO),
        ("is_writer", True),
        # next() is sync: the engine would `await` it and swallow the TypeError,
        # so run it through the executor path instead
        ("is_async", False),
    )

    def next(self, meta):
        fd = self.p.fd()
        if isinstance(meta, str):
            fd.write(meta)
        else:
            for chunk in meta:
                fd.write(str(chunk))
        fd.close()


@registry
class PgWriter(Node):
    """
        Postgresql Writer
    """
    params = (
        ("table", ""),
        ("mode", "insert"),
        ("is_writer", True),
        ("is_async", True),
    )

    def __init__(self):
        # fail fast instead of hitting AttributeError on AsyncOps.on_update /
        # on_delete (they don't exist) and silently dropping the write
        if self.p.mode != "insert":
            raise ValueError(
                f"PgWriter: mode={self.p.mode!r} is not implemented "
                f"(AsyncOps has no on_update/on_delete); only 'insert' works"
            )

    async def next(self, meta: Union[pl.DataFrame, pd.DataFrame, List[dict], dict]):
        async with async_ops as ctx:
            try:
                await ctx.on_insert(self.p.table, meta)
                status = {"status": 0, "error": ""}
            except Exception as e:
                print("PgWriter Error", e)
                status = {"status": 1, "error": str(e)}
        return status


@registry
class ParquetWriter(Node):

    params = (
        ("root_path", ""),
        # parquet 参数
        ("incr", False),
        ("partition_cols", ["year", "quarter", "sid", "date"]),
        ("max_partitions", 100000),  # 增加最大分区数到 100000
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
        ("sync_daily", True),  # 分钟线写入/更新时同步更新 daily parquet
        ("is_writer", True),
        ("is_async", True),
    )

    def __init__(self):
        self.existing_data_behavior = "overwrite_or_ignore" if self.p.incr else "delete_matching"
        self.basename_template = "part-{i}.parquet"
    
    def _make_schema(self, df: Union[pl.DataFrame, pd.DataFrame]) -> tuple[pa.Schema, pa.Schema]:
        df = ensure_polars(df)
        arrow_schema = df.head(0).to_arrow().schema
        data_fields = []
        # 注意: sorted(...) 产出字典序(amount, close, datetime, ...),
        # 与数据湖存量 105 万个 parquet 文件的物理列序完全一致, 勿改顺序。
        for col in sorted(set(df.columns) - set(self.p.partition_cols)):
            if col == "datetime":
                data_fields.append(pa.field(col, pa.timestamp("ms", tz="UTC")))
            else:
                ftype = arrow_schema.field(col).type
                if pa.types.is_large_string(ftype):
                    ftype = pa.string()
                elif pa.types.is_large_binary(ftype):
                    ftype = pa.binary()
                data_fields.append(pa.field(col, ftype))
        
        partition_fields = [pa.field(col, pa.string()) for col in self.p.partition_cols]
        return pa.schema(data_fields + partition_fields), pa.schema(partition_fields)
    
    def _make_partition(self, meta: Union[pl.DataFrame, pd.DataFrame]) -> pl.DataFrame:
        meta = ensure_polars(meta)
        dt = pl.col("datetime")
        return meta.with_columns(
            year=dt.dt.year().cast(pl.String),
            quarter=pl.lit("Q") + dt.dt.quarter().cast(pl.String),
            sid=pl.col("sid").cast(pl.String).str.replace_all(r"^[a-zA-Z]+\.|\.[a-zA-Z]+$", ""),
            date=dt.dt.strftime("%Y%m"),
            datetime=(
                dt.dt.replace_time_zone("Asia/Shanghai")
                .dt.convert_time_zone("UTC")
                .dt.replace_time_zone(None)
            ),
        )

    def _write_parquet(
        self,
        meta: Union[pl.DataFrame, pd.DataFrame],
    ):
        meta = ensure_polars(meta)
        if meta.is_empty():
            return
        meta = self._make_partition(meta)
        schema, partition_schema = self._make_schema(meta)
        table = meta.to_arrow().select(schema.names).cast(schema)

        basename_template = (
            f"day_{int(meta['tick'].min())}_{int(meta['tick'].max())}_{{i}}.parquet"
            if self.p.incr
            else self.basename_template
        )

        base_dir = expand_path(self.p.root_path)
        ds.write_dataset(
            data=table,
            base_dir=base_dir,
            format="parquet",
            partitioning=ds.partitioning(partition_schema, flavor="hive"),
            basename_template=basename_template,
            existing_data_behavior=self.existing_data_behavior,
            max_partitions=self.p.max_partitions,
            max_rows_per_file=self.p.max_rows_per_file,
            max_rows_per_group=self.p.max_rows_per_group,
        )

        if self.p.sync_daily:
            upsert_daily_from_arrow_table(Path(base_dir), table)

    async def next(self, meta: Union[pl.DataFrame, pd.DataFrame]):
        """
        Async entry point for writing parquet using `to_thread` to avoid blocking.
        """
        await asyncio.to_thread(self._write_parquet, meta)
