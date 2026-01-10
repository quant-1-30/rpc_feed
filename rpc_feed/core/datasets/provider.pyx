#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False 
# cython: cdivision=True

import pyarrow as pa
import numpy as np
from sqlalchemy import select, and_, or_, func, cast, Integer, BigInteger
from sqlalchemy.orm import load_only  # load_only 在 orm 模块中

from core.gateway import *
from core.rpc.serialize.pb import service_pb2

from libc.math cimport round 
from libc.stdint cimport uint8_t, int64_t
from libcpp.string cimport string as cpp_string
from rpc_feed.core.gateway.duckdb.utils cimport Request


cdef long CHUNK_SIZE = 1024 
cdef long MULT = 1000 
cdef cpp_string tz_info = b"Asia/Shanghai"


cdef class TradingCalendar:
    """Calendar provider base class

    Provide calendar data.
    """

    def __init__(self): # __cinit__ used for memory allocate in C
        self._buf_date = np.empty(CHUNK_SIZE, dtype=np.int32)

    async def __call__(self, int start_date, int end_date, list sids=[]):
        """Get calendar of certain market in given time range.

        Parameters
        ----------
        start_time : str
            start of the time range.
        end_time : str
            end of the time range.
        freq : str
            time frequency, available: year/quarter/month/week/day.
        future : bool
            whether including future trading day.

        Returns: List[int]
        """ 
        cdef int i    
        cdef object row, stream, result

        async with async_ops as ctx:
            stmt = select(Benchmark.date).distinct().where(
                    Benchmark.date.between(start_date, end_date)
            ).order_by(Benchmark.date)

            stream_wrap = await ctx.on_query(stmt) # stream wrap

            async with stream_wrap as stream_proxy:
                async for row in stream_proxy.scalars():
                    r_sid = row

                    if i >= CHUNK_SIZE:
                        yield self._flush(i)
                        i = 0

                    self._buf_date[i] = row
                    i += 1

                if i > 0:
                    yield self._flush(i)

    cdef object _flush(self, int count): # protobuf extend slice copy avoid append 
        cdef object resp = service_pb2.Calendar()

        resp.tz_info = tz_info
        resp.date.extend(self._buf_date[:count])
        return resp


cdef class Instrument:
    """
        内存复用  预分配和NumPy 数组
        减少对象创建 load_only
        零拷贝思想 Protobuf 高效 extend 接口
    """

    def __init__(self):
        self._buf_first_trading = np.empty(CHUNK_SIZE, dtype=np.int32)
        self._buf_delist = np.empty(CHUNK_SIZE, dtype=np.int32)
        # str / bytes not allowed preallocate memory / prefill
        self._buf_sid = [b''] * CHUNK_SIZE
        self._buf_name = [b''] * CHUNK_SIZE

    async def __call__(self, int start_date, int end_date, list sids=None):
        cdef:
            int i = 0
            object row, stream

        async with async_ops as ctx:
            stmt = select(Asset).options(
                load_only(Asset.sid, Asset.name, Asset.first_trading, Asset.delist) # only load necessary columns
            )
            
            if sids:
                stmt = stmt.where(Asset.sid.in_(sids))
            else:
                stmt = stmt.where(Asset.first_trading.between(start_date, end_date))

            stream_wrap = await ctx.on_query(stmt)

            async with stream_wrap as stream_proxy: 
                async for row in stream_proxy.scalars():
                    if i >= CHUNK_SIZE:
                        yield self._flush(i)
                        i = 0

                    self._buf_sid[i] = row.sid # avoid row.sid so rather than row.sid
                    self._buf_name[i] = row.name
                    self._buf_first_trading[i] = row.first_trading
                    self._buf_delist[i] = row.delist
                    i += 1
                if i > 0:
                    yield self._flush(i)

    cdef object _flush(self, int count): # protobuf extend slice copy avoid append 
        cdef object resp = service_pb2.InstFrame()
        resp.sid.extend(self._buf_sid[:count])
        resp.name.extend(self._buf_name[:count])
        resp.first_trading.extend(self._buf_first_trading[:count])
        resp.delist.extend(self._buf_delist[:count])
        return resp


cdef class Index:

    def __init__(self):
        self._buf_date = np.empty(CHUNK_SIZE, dtype=np.int32)
        self._buf_open = np.empty(CHUNK_SIZE, dtype=np.int32)
        self._buf_high = np.empty(CHUNK_SIZE, dtype=np.int32)
        self._buf_low = np.empty(CHUNK_SIZE, dtype=np.int32)
        self._buf_close = np.empty(CHUNK_SIZE, dtype=np.int32)
        self._buf_volume = np.empty(CHUNK_SIZE, dtype=np.int64)
        self._buf_amount = np.empty(CHUNK_SIZE, dtype=np.int64)

    async def __call__(self, int start_date, int end_date, list sids=None):
        cdef:
            int i = 0
            object row, stream
            bytes r_sid, last_sid = b''

        async with async_ops as ctx:
            stmt = select(
                Benchmark.sid, Benchmark.date,
                cast(func.round(Benchmark.open * MULT), Integer),
                cast(func.round(Benchmark.high * MULT), Integer),
                cast(func.round(Benchmark.low * MULT), Integer),
                cast(func.round(Benchmark.close * MULT), Integer),
                cast(func.round(Benchmark.volume * MULT), BigInteger),
                cast(func.round(Benchmark.amount * MULT), BigInteger),
            ).where(Benchmark.date.between(start_date, end_date)).order_by(Benchmark.sid, Benchmark.date)
            
            if sids: stmt = stmt.where(Benchmark.sid.in_(sids))
            
            stream_wrap = await ctx.on_query(stmt)

            async with stream_wrap as stream_proxy:
                async for row in stream_proxy:
                    r_sid = row[0]

                    if (last_sid and r_sid != last_sid) or i >= CHUNK_SIZE:
                        yield self._flush(i, last_sid)
                        i = 0

                    self._buf_date[i] = row[1]
                    self._buf_open[i] = row[2]
                    self._buf_high[i] = row[3]
                    self._buf_low[i] = row[4]
                    self._buf_close[i] = row[5]
                    self._buf_volume[i] = row[6]
                    self._buf_amount[i] = row[7]
                    last_sid = r_sid
                    i += 1

                if i > 0: yield self._flush(i, last_sid)

    cdef object _flush(self, int count, bytes sid):
        cdef object resp = service_pb2.DailyFrame(sid=sid)
        resp.date.extend(self._buf_date[:count])
        resp.open.extend(self._buf_open[:count])
        resp.high.extend(self._buf_high[:count])
        resp.low.extend(self._buf_low[:count])
        resp.close.extend(self._buf_close[:count])
        resp.volume.extend(self._buf_volume[:count])
        resp.amount.extend(self._buf_amount[:count])
        return resp


cdef class Tick:

    # def __init__(self):
    #     self._buf_sid = np.empty((CHUNK_SIZE, 16), dtype=np.uint8) # cdef cnp.uint8_t[:, :] _buf_sid_raw 

    async def __call__(self, int start_date, int end_date, list sids=[]):
        cdef:
            int num_rows # cdef Py_ssize_t num_rows  # used index
            Request req = Request(start_date=start_date, end_date=end_date, sid=sids)
            object batch, duck_mgr = get_duckdb_manager()

        async with duck_mgr as ctx:
            # async for batch in ctx.query(req_dict, tick_template):
            async for batch in ctx.query(req, tick_template):
                # print("batch :", batch)
                num_rows = batch.num_rows
                if num_rows == 0: continue
                
                # RecordBatchReader ---> primitive array suited for to_numpy C ptr pyarrow  0-bitmap 1-data / avoid table = pa.Table.from_batches([batch]) 
                # _buf_sid = batch.column(0).to_numpy(zero_copy_only=False) # due to pyarrow bytes/str differ from NumPy memory layout 
                _buf_sid = batch.column("sid").to_pylist() # to_pylist is faster than to_numpy for bytes/str
                # grp by sid indices
                c_indices = np.where(_buf_sid[:num_rows-1] != _buf_sid[1:])[0] + 1 
                s_indices = np.insert(c_indices, 0, 0)
                e_indices = np.append(c_indices, num_rows)

                for start, end in zip(s_indices, e_indices):
                    sub_batch = batch.slice(start, end - start)
                    
                    resp = service_pb2.TickFrame()
                    resp.sid = _buf_sid[start] 
                    resp.tick.extend(sub_batch.column(1).to_numpy(zero_copy_only=True))
                    resp.open.extend(sub_batch.column(2).to_numpy(zero_copy_only=True))
                    resp.high.extend(sub_batch.column(3).to_numpy(zero_copy_only=True))
                    resp.low.extend(sub_batch.column(4).to_numpy(zero_copy_only=True))
                    resp.close.extend(sub_batch.column(5).to_numpy(zero_copy_only=True))
                    resp.volume.extend(sub_batch.column(6).to_numpy(zero_copy_only=True))
                    resp.amount.extend(sub_batch.column(7).to_numpy(zero_copy_only=True))
                    yield resp


cdef class Close:

    # def __init__(self):
    #     self._buf_sid = np.empty((CHUNK_SIZE, 16), dtype=np.uint8) # cdef cnp.uint8_t[:, :] _buf_sid_raw 

    async def __call__(self, int start_date, int end_date, list sids=[]):
        """Get dataset data.

        Parameters
        ----------

        Returns
        ----------
        # SELECT * FROM stock
        # WHERE datetime >= TIMESTAMP '2024-06-01 09:30:00'
        # DuckDB 会自动把 '2024-06-01 09:30:00' 解析为内部 TIMESTAMP 类型，与 Parquet 里的 datetime 字段对齐 
        # hive_partitioning  --- automate path to key=value in partition cols 
        """
        cdef:
            int num_rows  # Py_ssize_t
            object batch, duck_mgr = get_duckdb_manager()
            Request req = Request(start_date=start_date, end_date=end_date, sid=sids)

        async with duck_mgr as ctx:
            async for batch in ctx.query(req, close_template):
                num_rows = batch.num_rows
                if num_rows == 0:
                    continue
                
                # RecordBatchReader ---> primitive array suited for to_numpy C ptr pyarrow  0-bitmap 1-data / avoid table = pa.Table.from_batches([batch]) 
                # _buf_sid = batch.column(0).to_numpy(zero_copy_only=False) # due to pyarrow bytes/str differ from NumPy memory layout 
                _buf_sid = batch.column("sid").to_pylist() # to_pylist is faster than to_numpy for bytes/str

                # grp by sid indices
                c_indices = np.where(_buf_sid[:num_rows-1] != _buf_sid[1:])[0] + 1
                s_indices = np.insert(c_indices, 0, 0)
                e_indices = np.append(c_indices, len(_buf_sid))

                for start, end in zip(s_indices, e_indices):
                    # slice zero_copy and fast
                    sub_btach = batch.slice(start, end - start)

                    resp = service_pb2.CloseFrame()
                    resp.sid = _buf_sid[start] 
                    resp.date.extend(sub_btach.column("day").to_numpy()) # Vectorized Extend to_numpy() zero_copy
                    resp.close.extend(sub_btach.column("close").to_numpy())
                    yield resp


cdef class Adjust:
    """
        内存复用  预分配和NumPy 数组
        减少对象创建 load_only
        零拷贝思想 Protobuf 高效 extend 接口
    """

    def __init__(self):
        self._buf_ex_date = np.empty(CHUNK_SIZE, dtype=np.int32)
        self._buf_register_date = np.empty(CHUNK_SIZE, dtype=np.int32)
        self._buf_bonus_share = np.empty(CHUNK_SIZE, dtype=np.int32)
        self._buf_transfer = np.empty(CHUNK_SIZE, dtype=np.int32)
        self._buf_bonus = np.empty(CHUNK_SIZE, dtype=np.int32)

    async def __call__(self, int start_date, int end_date, list sids=None):
        cdef:
            int i = 0
            object row, stream
            bytes r_sid, last_sid = b''

        async with async_ops as ctx:
            stmt = select(
                        Adjustment.sid,
                        Adjustment.ex_date,
                        Adjustment.register_date,
                        cast(func.round(Adjustment.bonus_share * MULT), Integer).label("bonus_share_int"),
                        cast(func.round(Adjustment.transfer * MULT), Integer).label("transfer_int"),
                        cast(func.round(Adjustment.bonus * MULT), Integer).label("bonus_int")
                    ).where(Adjustment.ex_date.between(start_date, end_date)
                ).order_by(Adjustment.sid, Adjustment.ex_date) 
            if sids:
                stmt = stmt.where(Adjustment.sid.in_(sids))

            stream_wrap = await ctx.on_query(stmt)

            async with stream_wrap as stream_proxy:
                async for row in stream_proxy:
                    r_sid = row[0]

                    if (last_sid and r_sid != last_sid) or i >= CHUNK_SIZE:
                        yield self._flush(i, last_sid)
                        i = 0

                    self._buf_ex_date[i] = row[1]
                    self._buf_register_date[i] = row[2]
                    self._buf_bonus_share[i] = row[3]
                    self._buf_transfer[i] = row[4]
                    self._buf_bonus[i] = row[5]

                    last_sid = r_sid
                    i += 1

                if i > 0:
                    yield self._flush(i, last_sid)

    cdef object _flush(self, int count, bytes sid):
        cdef object resp = service_pb2.AdjFrame(sid=sid)
        resp.ex_date.extend(self._buf_ex_date[:count]) # protobuf extend
        resp.register_date.extend(self._buf_register_date[:count])
        resp.bonus_share.extend(self._buf_bonus_share[:count])
        resp.transfer.extend(self._buf_transfer[:count])
        resp.bonus.extend(self._buf_bonus[:count])
        return resp


cdef class Right:

    def __init__(self):
        self._buf_ex_date = np.empty(CHUNK_SIZE, dtype=np.int32)
        self._buf_register_date = np.empty(CHUNK_SIZE, dtype=np.int32)
        self._buf_price = np.empty(CHUNK_SIZE, dtype=np.int32) # np.float32 
        self._buf_ratio = np.empty(CHUNK_SIZE, dtype=np.int32)

    async def __call__(self, int start_date, int end_date, list sids=[]):
        cdef:
            int i = 0
            object row, stream, result
            bytes last_sid = b'', r_sid
        
        async with async_ops as ctx:
            stmt = select(
                        Rightment.sid,
                        Rightment.ex_date,
                        Rightment.register_date,
                        cast(func.round(Rightment.price * MULT), Integer).label("price_int"),
                        cast(func.round(Rightment.ratio * MULT), Integer).label("ratio_int"),
                    ).where(Rightment.ex_date.between(start_date, end_date)
                ).order_by(Rightment.sid, Rightment.ex_date) 
            if sids:
                stmt = stmt.where(Rightment.sid.in_(sids))
                
            strteam_wrap = await async_ops.on_query(stmt)

            async with strteam_wrap as stream_proxy:
                async for row in stream_proxy:
                    r_sid = row[0]

                    if (last_sid and r_sid != last_sid) or i >= CHUNK_SIZE:
                        yield self._flush(i, last_sid)
                        i = 0

                    self._buf_ex_date[i] = row[1]
                    self._buf_register_date[i] = row[2]
                    self._buf_price[i] = row[3]
                    self._buf_ratio[i] = row[4]

                    last_sid = r_sid
                    i += 1

                if i > 0:
                    yield self._flush(i, last_sid)

    cdef object _flush(self, int count, bytes sid):
        cdef object resp = service_pb2.RightmentFrame(sid=sid)
        resp.ex_date.extend(self._buf_ex_date[:count])
        resp.register_date.extend(self._buf_register_date[:count])
        resp.price.extend(self._buf_price[:count])
        resp.ratio.extend(self._buf_ratio[:count])
        return resp
