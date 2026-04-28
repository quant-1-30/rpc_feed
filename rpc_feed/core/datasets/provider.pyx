# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False 
# cython: cdivision=True

import asyncio
import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
from sqlalchemy import select, and_, or_, func, cast, Integer, BigInteger

from libc.math cimport round 
from libc.stdint cimport uint8_t, int32_t, int64_t
from libcpp.string cimport string as cpp_string

from rpc_feed.core.rpc.serialize.pb import service_pb2
from rpc_feed.core.gateway.duckdb.utils cimport Request
from rpc_feed.core.gateway import *
from rpc_feed.utils.dateintern cimport intdt2ts

cdef cpp_string tz_info = b"Asia/Shanghai"


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


cdef inline tuple _slice_by_sid(object sid_col, Py_ssize_t num_rows):
    """
    基于 PyArrow Compute 寻找 sid 变化边界，返回 (s_indices, e_indices)
    """
    cdef object equal_mask, lslice, rslice, bound_mask, bound_indices
    cdef object s_indices, e_indices

    if num_rows == 1:
        return (pa.array([0], type=pa.int64()), pa.array([1], type=pa.int64()))
        
    equal_mask = pc.equal(sid_col, sid_col[0])
    if pc.all(equal_mask).as_py():
        return (pa.array([0], type=pa.int64()), pa.array([num_rows], type=pa.int64()))
        
    lslice = sid_col.slice(offset=0, length=num_rows - 1)
    rslice = sid_col.slice(offset=1, length=num_rows - 1)
    bound_mask = pc.not_equal(lslice, rslice)
    bound_indices = pc.add(pc.indices_nonzero(bound_mask), 1)
    
    s_indices = pa.concat_arrays([pa.array([0], type=pa.int64()), bound_indices])
    e_indices = pa.concat_arrays([bound_indices, pa.array([num_rows], type=pa.int64())])
    return s_indices, e_indices


cdef class Instrument:

    async def __call__(self, int start_date, int end_date, list sids=None):
        cdef:
            int i = 0
            object row, stream
        
        # avoid segment and new allocate
        buf_sid = [b''] * CHUNK_SIZE
        buf_name = [b''] * CHUNK_SIZE
        buf_first_trading = np.empty(CHUNK_SIZE, dtype=np.int32)
        buf_delist = np.empty(CHUNK_SIZE, dtype=np.int32)

        async with async_ops as ctx:
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
                    buf_sid[i] = row[0] # avoid row.sid
                    buf_name[i] = row[1]
                    buf_first_trading[i] = row[2]
                    buf_delist[i] = row[3]
                    i += 1

                    if i >= CHUNK_SIZE:
                        yield self._flush(i, buf_sid, buf_name, buf_first_trading, buf_delist)
                        # reallocate 
                        buf_sid = [b''] * CHUNK_SIZE
                        buf_name = [b''] * CHUNK_SIZE
                        buf_first_trading = np.empty(CHUNK_SIZE, dtype=np.int32)
                        buf_delist = np.empty(CHUNK_SIZE, dtype=np.int32)
                        i = 0

                if i > 0:
                    yield self._flush(i, buf_sid, buf_name, buf_first_trading, buf_delist)

    # protobuf extend slice copy avoid append --- zero_copy
    cdef object _flush(self, int count, list buf_sid, list buf_name, object buf_first_trading, object buf_delist):
 
        cdef dict metadata
        cdef object batch

        batch = pa.RecordBatch.from_arrays(
            [
                pa.array(buf_sid[:count], type=pa.binary()),
                pa.array(buf_name[:count], type=pa.binary()),
                pa.array(buf_first_trading[:count], type=pa.int32()),
                pa.array(buf_delist[:count], type=pa.int32()),
            ],
            names=["sid", "name", "first_trading", "delist"]
        )
        metadata = {
            b"rpc_type": b"instrument"
        }
        batch = batch.replace_schema_metadata(metadata) 
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
        
        cdef tuple indices
        cdef object s_indices, e_indices, slice_batch
        cdef Py_ssize_t start, end
        
        if num_rows == 0: 
            return
            
        indices = _slice_by_sid(sid_col, num_rows)
        s_indices = indices[0]
        e_indices = indices[1]

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
        # DuckDB '2024-06-01 09:30:00' TIMESTAMP align with Parquet datetime column
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
        
        cdef tuple indices
        cdef object s_indices, e_indices, slice_batch
        cdef Py_ssize_t start, end
        
        if num_rows == 0: 
            return
            
        indices = _slice_by_sid(sid_col, num_rows)
        s_indices = indices[0]
        e_indices = indices[1]

        for start_scalar, end_scalar in zip(s_indices, e_indices):
            start = start_scalar.as_py()
            end = end_scalar.as_py()
            sid = sid_col[start].as_py()
            slice_batch = batch.slice(start, end - start)
            yield self._flush(sid, slice_batch)

    cdef object _flush(self, bytes sid, object batch):
        cdef dict metadata
        
        metadata = {
            b"sid": sid,
            b"rpc_type": b"close"
        }
        batch = batch.replace_schema_metadata(metadata) # zero_copy
        return batch_to_resp(batch)


cdef class Adjust:

    async def __call__(self, int start_date, int end_date, list sids=None):
        cdef:
            int i = 0
            object row, stream
            bytes r_sid, last_sid = b''

        # avoid segment 
        buf_ex_date = np.empty(CHUNK_SIZE, dtype=np.int32)
        buf_register_date = np.empty(CHUNK_SIZE, dtype=np.int32)
        buf_bonus_share = np.empty(CHUNK_SIZE, dtype=np.int32)
        buf_transfer = np.empty(CHUNK_SIZE, dtype=np.int32)
        buf_bonus = np.empty(CHUNK_SIZE, dtype=np.int32)

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
                        yield self._flush(i, last_sid, buf_ex_date, buf_register_date, buf_bonus_share, buf_transfer, buf_bonus)
                        # reallocate
                        buf_ex_date = np.empty(CHUNK_SIZE, dtype=np.int32)
                        buf_register_date = np.empty(CHUNK_SIZE, dtype=np.int32)
                        buf_bonus_share = np.empty(CHUNK_SIZE, dtype=np.int32)
                        buf_transfer = np.empty(CHUNK_SIZE, dtype=np.int32)
                        buf_bonus = np.empty(CHUNK_SIZE, dtype=np.int32)
                        i = 0

                    buf_ex_date[i] = row[1]
                    buf_register_date[i] = row[2]
                    buf_bonus_share[i] = row[3]
                    buf_transfer[i] = row[4]
                    buf_bonus[i] = row[5]

                    last_sid = r_sid
                    i += 1

                if i > 0:
                    yield self._flush(i, last_sid, buf_ex_date, buf_register_date, buf_bonus_share, buf_transfer, buf_bonus)

    cdef object _flush(self, int count, bytes sid, object buf_ex_date, object buf_register_date, object buf_bonus_share, object buf_transfer, object buf_bonus):
        cdef dict metadata
        cdef object batch

        batch = pa.RecordBatch.from_arrays(
            [
                pa.array(buf_ex_date[:count], type=pa.int32()),
                pa.array(buf_register_date[:count], type=pa.int32()),
                pa.array(buf_bonus_share[:count], type=pa.int32()),
                pa.array(buf_transfer[:count], type=pa.int32()),
                pa.array(buf_bonus[:count], type=pa.int32()),
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

    async def __call__(self, int start_date, int end_date, list sids=[]):
        cdef:
            int i = 0
            object row, stream, result
            bytes last_sid = b'', r_sid
        
        buf_ex_date = np.empty(CHUNK_SIZE, dtype=np.int32)
        buf_register_date = np.empty(CHUNK_SIZE, dtype=np.int32)
        buf_price = np.empty(CHUNK_SIZE, dtype=np.int32) # np.float32 
        buf_ratio = np.empty(CHUNK_SIZE, dtype=np.int32)
        
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
                        yield self._flush(i, last_sid, buf_ex, buf_register_date, buf_price, buf_ratio)
                        # reallocate
                        buf_ex_date = np.empty(CHUNK_SIZE, dtype=np.int32)
                        buf_register_date = np.empty(CHUNK_SIZE, dtype=np.int32)
                        buf_price = np.empty(CHUNK_SIZE, dtype=np.int32) # np.float32 
                        buf_ratio = np.empty(CHUNK_SIZE, dtype=np.int32)
                        i = 0

                    buf_ex_date[i] = row[1]
                    buf_register_date[i] = row[2]
                    buf_price[i] = row[3]
                    buf_ratio[i] = row[4]

                    last_sid = r_sid
                    i += 1

                if i > 0:
                    yield self._flush(i, last_sid, buf_ex_date, buf_register_date, buf_price, buf_ratio)

    cdef object _flush(self, int count, bytes sid, object buf_ex_date, object buf_register_date, object buf_price, object buf_ratio):
        cdef dict rightment
        cdef object batch

        batch = pa.RecordBatch.from_arrays(
            [
                pa.array(buf_ex_date[:count], type=pa.int32()),
                pa.array(buf_register_date[:count], type=pa.int32()),
                pa.array(buf_price[:count], type=pa.int32()),
                pa.array(buf_ratio[:count], type=pa.int32()),
            ],
            names=["ex_date", "register_date", "price", "ratio"]
        )
        metadata = {
            b"sid": sid,
            b"rpc_type": b"right"
        }
        batch = batch.replace_schema_metadata(metadata) # zero_copy
        return batch_to_resp(batch)
