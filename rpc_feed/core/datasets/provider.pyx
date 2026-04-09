# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False 
# cython: cdivision=True

import asyncio
import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
from sqlalchemy import select, and_, or_, func, cast, Integer, BigInteger
from sqlalchemy.orm import load_only  # load_only 在 orm 模块中

from libc.math cimport round 
from libc.stdint cimport uint8_t, int32_t, int64_t
from libcpp.string cimport string as cpp_string

from rpc_feed.core.rpc.serialize.pb import service_pb2
from rpc_feed.core.gateway.duckdb.utils cimport Request
from rpc_feed.core.gateway import *
from rpc_feed.utils.dateintern cimport intdt2ts


cdef long CHUNK_SIZE = 1024 
cdef long MULT = 1000 
cdef cpp_string tz_info = b"Asia/Shanghai"
cdef TICK_PROCESS_TIMEOUT = 100


cdef object arrow_options = pa.ipc.IpcWriteOptions(
    compression='lz4', # lz4 internal / zstd public 
    use_threads=True
) 

cdef object batch_to_resp(object batch):
    sink = pa.BufferOutputStream() # ipc stream bytes 
    with pa.ipc.new_stream(sink, batch.schema, options=arrow_options) as writer:
        writer.write_batch(batch) # writer.write_table(batch)

    buf = sink.getvalue()
    resp = service_pb2.ArrowFrame(
        payload=buf.to_pybytes()  # $O(N)$ copy ops 
    )
    return resp 


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

    cdef object _flush(self, int count): # protobuf extend slice copy 
        cdef dict metadata
        cdef object batch

        batch = pa.RecordBatch.from_arrays(
            [
                pa.array(self._buf_date[:count], type=pa.int32())
            ],
            names=["date"]
        )
        metadata = {
            # b"tz_info": meta.tz_info,
            b"rpc_type": b"calendar"
        }
        batch = batch.replace_schema_metadata(metadata) # zero_copy
        return batch_to_resp(batch)
  

cdef class Instrument:

    def __init__(self):
        self._buf_first_trading = np.empty(CHUNK_SIZE, dtype=np.int32)
        self._buf_delist = np.empty(CHUNK_SIZE, dtype=np.int32)
        self._buf_sid = [b''] * CHUNK_SIZE
        self._buf_name = [b''] * CHUNK_SIZE

    async def __call__(self, int start_date, int end_date, list sids=None):
        cdef:
            int i = 0
            object row, stream

        async with async_ops as ctx:
            # stmt = select(Asset).options(
            #     load_only(Asset.sid, Asset.name, Asset.first_trading, Asset.delist) # only load necessary columns
            # )
            stmt = select(
                Asset.sid, Asset.name, Asset.first_trading, Asset.delist
            )
            
            if sids:
                stmt = stmt.where(Asset.sid.in_(sids))
            else:
                stmt = stmt.where(Asset.first_trading.between(start_date, end_date))

            stream_wrap = await ctx.on_query(stmt)

            async with stream_wrap as stream_proxy: 
                async for row in stream_proxy:
                    if i >= CHUNK_SIZE:
                        yield self._flush(i)
                        i = 0

                    self._buf_sid[i] = row[0] # avoid row.sid
                    self._buf_name[i] = row[1]
                    self._buf_first_trading[i] = row[2]
                    self._buf_delist[i] = row[3]
                    i += 1
                if i > 0:
                    yield self._flush(i)

    cdef object _flush(self, int count): # protobuf extend slice copy avoid append --- zero_copy 
        cdef dict metadata
        cdef object batch

        batch = pa.RecordBatch.from_arrays(
            [
                pa.array(self._buf_sid[:count], type=pa.binary()),
                pa.array(self._buf_name[:count], type=pa.binary()),
                pa.array(self._buf_first_trading[:count], type=pa.int32()),
                pa.array(self._buf_delist[:count], type=pa.int32()),
            ],
            names=["sid", "name", "first_trading", "delist"]
        )
        metadata = {
            b"rpc_type": b"instrument"
        }
        batch = batch.replace_schema_metadata(metadata) 
        return batch_to_resp(batch)


cdef class Index:

    async def __call__(self, int32_t start_date, int32_t end_date, list sids=None):
        cdef:
            Request req = Request(start_date=start_date, end_date=end_date, sid=sids)
            object batch, duck_mgr = get_duckdb_manager()

        async with duck_mgr as ctx:
            async for batch in ctx.query(req, TICK_TEMPLATE):
                if batch.num_rows == 0: continue
                try:
                    for resp in self._process_batch(batch):
                        yield resp 
                except asyncio.TimeoutError:
                    print("_process_batch Timeout")
                    continue 
                except asyncio.CancelledError:
                    print("_process_batch CancelledError")
                    raise 
    
    def _process_batch(self, object batch):
        cdef object sid_col = batch.column("sid")
        cdef Py_ssize_t num_rows = len(sid_col)
        
        cdef object s_indices = None
        cdef object e_indices = None
        cdef object slice_batch, resp
        cdef Py_ssize_t start, end
        
        if num_rows == 0: 
            return
            
        if num_rows == 1:
            s_indices = pa.array([0], type=pa.int64())
            e_indices = pa.array([1], type=pa.int64())
        else:
            equal_mask = pc.equal(sid_col, sid_col[0])
            if pc.all(equal_mask).as_py():
                s_indices = pa.array([0], type=pa.int64())
                e_indices = pa.array([num_rows], type=pa.int64())
            else:
                lslice = sid_col.slice(offset=0, length=num_rows - 1)
                rslice = sid_col.slice(offset=1, length=num_rows - 1)
                bound_mask = pc.not_equal(lslice, rslice)
                bound_indices = pc.add(pc.indices_nonzero(bound_mask), 1)
                s_indices = pa.concat_arrays([pa.array([0], type=pa.int64()), bound_indices])
                e_indices = pa.concat_arrays([bound_indices, pa.array([num_rows], type=pa.int64())])

        for start_scalar, end_scalar in zip(s_indices, e_indices):
            start = start_scalar.as_py()
            end = end_scalar.as_py()
            sid = sid_col[start].as_py()
            slice_batch = batch.slice(start, end - start)
            yield self._flush(sid, slice_batch)

    cdef _flush(self, bytes sid, object batch):
        metadata = {
        b"sid": sid,
        b"rpc_type": b"index"
        }
        batch = batch.replace_schema_metadata(metadata) # zero_copy
        return batch_to_resp(batch)


cdef class Tick:

    async def __call__(self, int32_t start_date, int32_t end_date, list sids=[]):
        cdef:
            Request req = Request(start_date=start_date, end_date=end_date, sid=sids)
            object batch, duck_mgr = get_duckdb_manager()

        async with duck_mgr as ctx:
            async for batch in ctx.query(req, TICK_TEMPLATE):
                if batch.num_rows == 0: continue
                try:
                    for resp in self._process_batch(batch):
                        yield resp 
                except asyncio.TimeoutError:
                    print("_process_batch Timeout")
                    continue 
                except asyncio.CancelledError:
                    print("_process_batch CancelledError")
                    raise 
                    
    def _process_batch(self, object batch):
        cdef object sid_col = batch.column("sid")
        cdef Py_ssize_t num_rows = len(sid_col)
        
        cdef object s_indices = None
        cdef object e_indices = None
        cdef object slice_batch, resp
        cdef Py_ssize_t start, end
        
        if num_rows == 0: 
            return
            
        if num_rows == 1:
            s_indices = pa.array([0], type=pa.int64())
            e_indices = pa.array([1], type=pa.int64())
        else:
            equal_mask = pc.equal(sid_col, sid_col[0])
            if pc.all(equal_mask).as_py():
                s_indices = pa.array([0], type=pa.int64())
                e_indices = pa.array([num_rows], type=pa.int64())
            else:
                lslice = sid_col.slice(offset=0, length=num_rows - 1)
                rslice = sid_col.slice(offset=1, length=num_rows - 1)
                bound_mask = pc.not_equal(lslice, rslice)
                bound_indices = pc.add(pc.indices_nonzero(bound_mask), 1)
                s_indices = pa.concat_arrays([pa.array([0], type=pa.int64()), bound_indices])
                e_indices = pa.concat_arrays([bound_indices, pa.array([num_rows], type=pa.int64())])

        for start_scalar, end_scalar in zip(s_indices, e_indices):
            start = start_scalar.as_py()
            end = end_scalar.as_py()
            sid = sid_col[start].as_py()
            slice_batch = batch.slice(start, end - start)
            yield self._flush(sid, slice_batch)

    cdef _flush(self, bytes sid, object batch):
        metadata = {
        b"sid": sid,
        b"rpc_type": b"tick"
        }
        batch = batch.replace_schema_metadata(metadata) # zero_copy
        return batch_to_resp(batch)


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
            Request req = Request(start_date=start_date, end_date=end_date, sid=sids)
            object batch, duck_mgr = get_duckdb_manager()
            int32_t num_rows  
            bytes sid

        async with duck_mgr as ctx:
            async for batch in ctx.query(req, CLOSE_TEMPLATE):
                num_rows = batch.num_rows
                if num_rows == 0:
                    continue
                
                sid_array = batch.column("sid").to_numpy(zero_copy_only=False)

                c_indices = np.where(sid_array[:num_rows-1] != sid_array[1:])[0] + 1
                s_indices = np.insert(c_indices, 0, 0)
                e_indices = np.append(c_indices, len(sid_array))

                for start, end in zip(s_indices, e_indices):
                    sid = sid_array[start]
                    frame = self._flush(sid, batch.slice(start, end - start))
                    yield frame

    cdef object _flush(self, bytes sid, object batch):
        cdef dict metadata
        
        metadata = {
            b"sid": sid,
            b"rpc_type": b"close"
        }
        batch = batch.replace_schema_metadata(metadata) # zero_copy
        return batch_to_resp(batch)


cdef class Adjust:

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
        cdef dict metadata
        cdef object batch

        batch = pa.RecordBatch.from_arrays(
            [
                pa.array(self._buf_ex_date[:count], type=pa.int32()),
                pa.array(self._buf_register_date[:count], type=pa.int32()),
                pa.array(self._buf_bonus_share[:count], type=pa.int32()),
                pa.array(self._buf_transfer[:count], type=pa.int32()),
                pa.array(self._buf_bonus[:count], type=pa.int32()),
            ],
            names=["ex_date", "register_date", "bonus_share", "transfer", "bonus"]
        )
        metadata = {
            b"sid": sid,
            b"rpc_type": b"adjustment"
        }
        batch = batch.replace_schema_metadata(metadata) # zero_copy
        return batch_to_resp(batch)


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
        cdef dict rightment
        cdef object batch

        batch = pa.RecordBatch.from_arrays(
            [
                pa.array(self._buf_ex_date[:count], type=pa.int32()),
                pa.array(self._buf_register_date[:count], type=pa.int32()),
                pa.array(self._buf_price[:count], type=pa.int32()),
                pa.array(self._buf_ratio[:count], type=pa.int32()),
            ],
            names=["ex_date", "register_date", "price", "ratio"]
        )
        metadata = {
            b"sid": sid,
            b"rpc_type": b"right"
        }
        batch = batch.replace_schema_metadata(metadata) # zero_copy
        return batch_to_resp(batch)


cdef class Experiment:

    async def __call__(self, int32_t start_date, int32_t end_date, list sids=[]):
        cdef:
            Request req = Request(start_date=start_date, end_date=end_date, sid=sids)
            object batch, duck_mgr = get_duckdb_manager()

        async with duck_mgr as ctx:
            async for batch in ctx.query_experiment(req, EXPERIMENT_TEMPLATE):
                if batch.num_rows == 0: continue
                try:
                    for resp in self._process_batch(batch):
                        yield resp 
                except asyncio.TimeoutError:
                    print("_process_batch Timeout")
                    continue 
                except asyncio.CancelledError:
                    print("_process_batch CancelledError")
                    raise 

    cdef _flush(self, bytes sid, object batch):
        metadata = {
        b"sid": sid,
        b"rpc_type": b"experiment"
        }
        batch = batch.replace_schema_metadata(metadata) # zero_copy
        return batch_to_resp(batch)
