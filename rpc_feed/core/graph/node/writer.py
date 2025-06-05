#!/usr/bin/env python3
# -*- coding: utf-8; py-indent-offset:4 -*-

import io
import sys
import avro.schema
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.dataset as ds
import asyncio

from typing import Any, Union, List, Optional
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter
from avro.datafile import DataFileWriter

from rpc_feed.core.graph.base import Node
from rpc_feed.utils.registry import registry
from rpc_feed.core.middleware.operator import async_ops
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
        )

    def _make_schema(self, df: pd.DataFrame) -> pa.Schema:
        fields = []
        for col in df.columns:
            if col == "datetime":
                fields.append(pa.field(col, pa.timestamp("ms"))) 
            elif col in self.p.partition_cols:
                fields.append(pa.field(col, pa.string()))
            else:
                fields.append(pa.field(col, pa.from_numpy_dtype(df[col].dtype)))
        return pa.schema(fields)
    
    def _make_schema(self, df: pd.DataFrame) -> pa.Schema:
        data_fields = []
        for col in df.columns.difference(self.p.partition_cols):
            if col == "datetime":
                data_fields.append(pa.field(col, pa.timestamp("ms"))) 
            else:
                data_fields.append(pa.field(col, pa.from_numpy_dtype(df[col].dtype)))
        
        partition_fields = []
        for col in self.p.partition_cols:
            partition_fields.append(pa.field(col, pa.string()))
        return pa.schema(data_fields+partition_fields), pa.schema(partition_fields)
    
    def _make_partition(self, meta: pd.DataFrame) -> pd.DataFrame:
        meta["year"] = meta["datetime"].apply(lambda x: str(x.year))
        meta["quarter"] = meta['datetime'].apply(lambda x: f'Q{((x.month - 1) // 3) + 1}')
        meta["sid"] = meta.attrs["sid"]
        meta["date"] = meta["datetime"].dt.strftime("%Y%m")
        return meta

    def _write_parquet(
        self,
        meta: pd.DataFrame,
        params: dict={}
    ):
        try:
            meta = self._make_partition(meta)
            schema, partition_schema = self._make_schema(meta)
            table = pa.Table.from_pandas(meta, schema=schema, preserve_index=False)
            root_path = expand_path(params["root_path"])
            
            ds.write_dataset(
                data=table,
                base_dir=str(root_path),
                format="parquet",
                partitioning=ds.partitioning(partition_schema, flavor="hive"),
                existing_data_behavior="overwrite_or_ignore",
                max_partitions=self.p.max_partitions,
                max_rows_per_file=self.p.max_rows_per_file,
                max_rows_per_group=self.p.max_rows_per_group,
            )
        except AttributeError as e:
            print(f"属性错误: {e}")
            raise
        except TypeError as e:
            print(f"类型错误: {e}")
            raise
        except ValueError as e:
            print(f"值错误: {e}")
            raise
        except Exception as e:
            print(f"其他错误: {e}")
            raise

    async def next(self, meta: pd.DataFrame, params: dict = {}):
        """
        Async entry point for writing parquet using `to_thread` to avoid blocking.
        """
        self._write_parquet(meta, params)

    async def query(self, sql: str, batch_size: int = 1000):
        loop = asyncio.get_running_loop()
        queue = asyncio.Queue(maxsize=self.max_queue_size)

        def producer():
            try:
                for row in self._query_stream(sql, batch_size):
                    print("put to queue", row)
                    # 使用 call_soon_threadsafe 来安全地在线程中调用异步函数
                    future = asyncio.run_coroutine_threadsafe(queue.put(row), loop)
                    # 等待操作完成
                    future.result()
            finally:
                # 发送结束标记
                future = asyncio.run_coroutine_threadsafe(queue.put(None), loop)
                future.result()

        # 创建后台任务
        task = asyncio.create_task(asyncio.to_thread(producer))
        self._tasks.add(task)

        # 清理已完成的任务
        def _done(_):
            self._tasks.discard(task)
        task.add_done_callback(_done)

        try:
            while True:
                row = await queue.get()
                print("get from queue", row)
                if row is None:
                    break
                yield row
        finally:
            # 确保任务被清理
            if not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
