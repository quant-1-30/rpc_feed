#! /usr/bin/env python3
# -*- coding: utf-8 -*-

# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
# cython: language_level=3, boundscheck=False, wraparound=False
import pyarrow as pa
import numpy as np
from sqlalchemy import select, and_, or_, func, cast, Integer # SQLAlchemy 2.0.39 正确的导入方式

from core.gateway import *
from core.rpc.serialize.pb import service_pb2

from libc.stdint cimport uint8_t, int64_t
from libc.math cimport round 
cimport numpy as cnp
cnp.import_array() # initialize numpy c_api

cdef long CHUNK_SIZE = 1024 
cdef long EVENT_MULT = 1000 


cdef class TradingCalendar:
    """Calendar provider base class

    Provide calendar data.
    """

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
        cdef list dates=[]
        cdef object response 
        cdef object row

        async with async_ops as ctx:
            stmt = select(Benchmark.date).distinct().where(
                    Benchmark.date.between(start_date, end_date)
            ).order_by(Benchmark.date)

            async for row in ctx.on_query(stmt): # async trans struct to dict to keep state
                dates.append(row[0])
            
            response = service_pb2.Calendar()
            response.tz_info = b"Asia/Shanghai"
            response.date.extend(dates) 
            yield response


cdef class Instrument:

    async def __call__(self, int start_date, int end_date, list sids=[]):
        cdef:
            object item
            object row
            object response
            bytes c_sid = b''
        
        def create_batch():
            return {"sid": [], "name": [], "first_trading": [], "delist": []}

        c_batch = create_batch()

        async with async_ops as ctx:
            stmt = select(Asset)
            if sids:
                stmt = stmt.where(Asset.sid.in_(sids))
            else:
                stmt = stmt.where(
                    Asset.first_trading.between(start_date, end_date)
                )

            async for item in ctx.on_query(stmt):
                row = item[0]
                r_sid = row.sid
                
                if (c_sid and r_sid != c_sid) or len(c_batch["sid"]) >= CHUNK_SIZE:
                    response = service_pb2.InstFrame() # initialize 

                    response.sid.extend(c_batch["sid"])
                    response.name.extend(c_batch["name"])
                    response.first_trading.extend(c_batch["first_trading"])
                    response.delist.extend(c_batch["delist"])

                    yield response
                    c_batch = create_batch()

                c_sid = r_sid

                c_batch["sid"].append(row.sid)
                c_batch["name"].append(row.name)
                c_batch["first_trading"].append(row.first_trading)
                c_batch["delist"].append(row.delist)

            if c_batch["sid"]:
                response = service_pb2.InstFrame()
                for key in c_batch:
                    getattr(response, key).extend(c_batch[key])
                yield response


cdef class Index:
    """Index provider base class
    """
    async def __call__(self, int start_date, int end_date, list sids=[]):
        """Get the existiongconfig dictionary for a base market adding several dynamic filters.
        """
        cdef object row
        cdef bytes r_sid, c_sid = b''
        cdef dict c_batch

        def create_batch():
            return {"date": [], "open": [], "high": [], "low": [], "close": [], "volume": [], "amount": []}

        c_batch = create_batch()

        async with async_ops as ctx:
            stmt = select(Benchmark).where(
                    Benchmark.date.between(start_date, end_date)
                ).order_by(Benchmark.sid, Benchmark.date) 
            if sids:
                stmt = stmt.where(Benchmark.sid.in_(sids))
            
            async for item in ctx.on_query(stmt):
                # avoid serialize(), direct via index 
                row = item[0]
                r_sid = row.sid

                if (c_sid and r_sid != c_sid) or len(c_batch["date"]) >= CHUNK_SIZE:

                    response = service_pb2.DailyFrame(sid=r_sid) # initialize   
                    response.date.extend(c_batch["date"])
                    response.open.extend(c_batch["open"])
                    response.high.extend(c_batch["high"])
                    response.low.extend(c_batch["low"])
                    response.close.extend(c_batch["close"])
                    response.volume.extend(c_batch["volume"])
                    response.amount.extend(c_batch["amount"])
                    yield response
                    c_batch = create_batch()

                c_sid = r_sid
                c_batch["date"].append(row.date)
                c_batch["open"].append(row.open)
                c_batch["high"].append(row.high)
                c_batch["low"].append(row.low)
                c_batch["close"].append(row.close)
                c_batch["volume"].append(row.volume)
                c_batch["amount"].append(row.amount)

            if c_batch["date"]:
                response = service_pb2.DailyFrame(sid=r_sid)
                for key in c_batch:
                    getattr(response, key).extend(c_batch[key])
                yield response


cdef class Tick:
    """Dataset provider class via duckdb
    """
    
    async def __call__(self, int start_date, int end_date, list sids=[]):
        cdef:
            object batch
            object duck_mgr = get_duckdb_manager()
            dict req_dict
            list sid_str

            # # primitive buffers (zero-copy) | const means readonly because arrow not mutable
            # # const （int, double, char* ...）
            # # cdef const char[:] sid_mv      # str
            # # cdef const uint8_t[:] sid_mv   # bytes
            # object sid_mv
            # const int64_t[:] tick_mv
            # const long[:] open_mv
            # const long[:] high_mv
            # const long[:] low_mv
            # const long[:] close_mv
            # const long[:] volume_mv
            # const long[:] amount_mv

            int i, num_rows, start, end # Py_ssize_t

        sid_str = [sid.decode("utf-8") for sid in sids]
        req_dict = {
            "start_date": start_date,
            "end_date": end_date,
            "sid": sid_str
        }
        # yield context switch  / avoid yield every time

        # async with duck_mgr as ctx: # only suited for one chunke eg: one sid and intended for calculate */+ ...
        #     async for batch in ctx.query(req_dict, tick_template):

        #         num_rows = batch.num_rows
        #         if num_rows == 0:
        #             continue

        #         # RecordBatchReader ---> primitive array suited for to_numpy C ptr pyarrow  0-bitmap 1-data
        #         sid_mv = batch.column(0).to_numpy(zero_copy_only=False) # bytes 
        #         tick_mv = batch.column(1).to_numpy(zero_copy_only=True)
        #         open_mv = batch.column(2).to_numpy(zero_copy_only=True)
        #         high_mv = batch.column(3).to_numpy(zero_copy_only=True)
        #         low_mv = batch.column(4).to_numpy(zero_copy_only=True)
        #         close_mv = batch.column(5).to_numpy(zero_copy_only=True)
        #         volume_mv = batch.column(6).to_numpy(zero_copy_only=True)
        #         amount_mv = batch.column(7).to_numpy(zero_copy_only=True)

        #         response_frame = service_pb2.TickFrame()
        #         response_frame.sid = current_sid

        #         # Protobuf  repeated  extend numpy / memoryview
        #         # for i in range(n): append(...) fast 10-50 
        #         response_frame.tick.extend(tick_np)
        #         response_frame.open.extend(open_np)
        #         response_frame.high.extend(high_np)
        #         response_frame.low.extend(low_np)
        #         response_frame.close.extend(close_np)
        #         response_frame.volume.extend(volume_np)
        #         response_frame.amount.extend(amount_np)
        #         yield response_frame
        
        async with duck_mgr as ctx: # RecordBatch is the most safe to transfer to numpy
            async for batch in ctx.query(req_dict, tick_template):
                num_rows = batch.num_rows
                if num_rows == 0:
                    continue
                
                # RecordBatch to Table for group_by
                table = pa.Table.from_batches([batch])
                sid_array = table.column("sid").to_numpy()

                # seek sid change indice
                c_indices = np.where(sid_array[:-1] != sid_array[1:])[0] + 1
                s_indices = np.insert(c_indices, 0, 0)
                e_indices = np.append(c_indices, len(sid_array))

                for start, end in zip(s_indices, e_indices):
                    # slice zero_copy and fast
                    sub_table = table.slice(start, end - start)
                    t_sid = sid_array[start] # bytes

                    response_frame = service_pb2.TickFrame()
                    response_frame.sid = t_sid

                    # Vectorized Extend to_numpy() zero_copy
                    response_frame.tick.extend(sub_table.column("tick").to_numpy())
                    response_frame.open.extend(sub_table.column("open").to_numpy())
                    response_frame.high.extend(sub_table.column("high").to_numpy())
                    response_frame.low.extend(sub_table.column("low").to_numpy())
                    response_frame.close.extend(sub_table.column("close").to_numpy())
                    response_frame.volume.extend(sub_table.column("volume").to_numpy())
                    response_frame.amount.extend(sub_table.column("amount").to_numpy())

                    yield response_frame


cdef class Close:

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
            object batch
            object duck_mgr = get_duckdb_manager()
            dict req_dict
            list sid_str

            int num_rows  # Py_ssize_t

        sid_str = [sid.decode("utf-8") for sid in sids]
        req_dict = {
            "start_date": start_date,
            "end_date": end_date,
            "sid": sid_str
        }

        async with duck_mgr as ctx:
            async for batch in ctx.query(req_dict, close_template):

                num_rows = batch.num_rows
                if num_rows == 0:
                    continue
                
                # RecordBatch to Table for group_by
                table = pa.Table.from_batches([batch])
                sid_array = table.column("sid").to_numpy()

                # seek sid change indice
                c_indices = np.where(sid_array[:-1] != sid_array[1:])[0] + 1
                s_indices = np.insert(c_indices, 0, 0)
                e_indices = np.append(c_indices, len(sid_array))

                for start, end in zip(s_indices, e_indices):
                    # slice zero_copy and fast
                    sub_table = table.slice(start, end - start)
                    t_sid = sid_array[start] # bytes

                    response_frame = service_pb2.CloseFrame()
                    response_frame.sid = t_sid

                    # Vectorized Extend to_numpy() zero_copy
                    response_frame.date.extend(sub_table.column("day").to_numpy())
                    response_frame.close.extend(sub_table.column("close").to_numpy())

                    yield response_frame


cdef class Adjust:

    async def __call__(self, int start_date, int end_date, list sids=[]):
        cdef:
            object row
            object response 
            bytes c_sid = b''
        
        def create_batch():
            return {"ex_date": [], "register_date": [], "bonus_share": [], "transfer": [], "bonus": []}

        c_batch = create_batch()

        async with async_ops as ctx:
            stmt = select(
                        Adjustment.sid,
                        Adjustment.ex_date,
                        Adjustment.register_date,
                        cast(func.round(Adjustment.bonus_share * EVENT_MULT), Integer).label("bonus_share_int"),
                        cast(func.round(Adjustment.transfer * EVENT_MULT), Integer).label("transfer_int"),
                        cast(func.round(Adjustment.bonus * EVENT_MULT), Integer).label("bonus_int")
                    ).where(Adjustment.ex_date.between(start_date, end_date)
                ).order_by(Adjustment.sid, Adjustment.ex_date) 
            if sids:
                stmt = stmt.where(Adjustment.sid.in_(sids))
            
            response = service_pb2.AdjFrame() # initialize

            async for row in ctx.on_query(stmt): # tuple 
                r_sid = row[0]
                
                if (c_sid and r_sid != c_sid) or len(c_batch["ex_date"]) >= CHUNK_SIZE:
                    response = service_pb2.AdjFrame(sid=r_sid) # initialize  

                    response.ex_date.extend(c_batch["ex_date"])
                    response.register_date.extend(c_batch["register_date"])
                    response.bonus_share.extend(c_batch["bonus_share"])
                    response.transfer.extend(c_batch["transfer"])
                    response.bonus.extend(c_batch["bonus"])
                    yield response
                    c_batch = create_batch()

                c_sid = r_sid
                c_batch["ex_date"].append(row[1])
                c_batch["register_date"].append(row[2])
                c_batch["bonus_share"].append(row[3])
                c_batch["transfer"].append(row[4])
                c_batch["bonus"].append(row[5])

            if c_batch["ex_date"]:
                response = service_pb2.AdjFrame(sid=r_sid)
                for key in c_batch:
                    getattr(response, key).extend(c_batch[key])
                yield response


cdef class Right:

    async def __call__(self, int start_date, int end_date, list sids=[]):
        cdef:
            object row
            object response = None
            bytes c_sid = b''
        
        def create_batch():
            return {"ex_date": [], "register_date": [], "price": [], "ratio": []}

        c_batch = create_batch()

        async with async_ops as ctx:
            stmt = select(
                        Rightment.sid,
                        Rightment.ex_date,
                        Rightment.register_date,
                        cast(func.round(Rightment.price * EVENT_MULT), Integer).label("price_int"),
                        cast(func.round(Rightment.ratio * EVENT_MULT), Integer).label("ratio_int"),
                    ).where(Rightment.ex_date.between(start_date, end_date)
                ).order_by(Rightment.sid, Rightment.ex_date) 
            if sids:
                stmt = stmt.where(Rightment.sid.in_(sids))
            
            response = service_pb2.RightmentFrame() # initialize

            async for row in ctx.on_query(stmt): # tuple
                r_sid = row[0]
                
                if (c_sid and r_sid != c_sid) or len(c_batch["ex_date"]) >= CHUNK_SIZE:
                    response = service_pb2.RightmentFrame(sid=r_sid) # initialize  
                     
                    response.ex_date.extend(c_batch["ex_date"])
                    response.register_date.extend(c_batch["register_date"])
                    response.price.extend(c_batch["price"])
                    response.ratio.extend(c_batch["ratio"])
                    yield response
                    c_batch = create_batch()

                c_sid = r_sid
                c_batch["ex_date"].append(row[1])
                c_batch["register_date"].append(row[2])
                c_batch["price"].append(row[3])
                c_batch["ratio"].append(row[4])

            if c_batch["ex_date"]:
                response = service_pb2.RightmentFrame(sid=r_sid)
                for key in c_batch:
                    getattr(response, key).extend(c_batch[key])
                yield response

