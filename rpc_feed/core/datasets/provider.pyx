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

from rpc_feed.core.gateway.duckdb.utils cimport Request
from rpc_feed.core.gateway import *
from rpc_feed.utils.dateintern cimport intdt2ts

from bt_protocol.serialize.pb import service_pb2
from bt_protocol.template.duckdb_template import *
from bt_protocol.schema.asset import *

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
        PyArrow Compute sid boundary and return (s_indices, e_indices)
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
    
    # indices_nonzero ---> uint64
    s_indices = pa.concat_arrays([pa.array([0], type=pa.int64()), bound_indices])
    e_indices = pa.concat_arrays([bound_indices, pa.array([num_rows], type=pa.int64())])
    return s_indices, e_indices


# =====================================================================
# 1. Public Abstract Buffer
# =====================================================================

cdef class BaseBufferedProvider:

    cdef object _flush_record_batch(self, bytes sid, object batch):
        cdef dict metadata = {
            b"rpc_type": self.rpc_type
        }
        if sid is not None and len(sid) > 0:
            metadata[b"sid"] = sid
        batch = batch.replace_schema_metadata(metadata)
        return batch_to_resp(batch)

    cdef object _create_and_flush_arrays(self, bytes sid, list arrays, list names):
        cdef object batch = pa.RecordBatch.from_arrays(arrays, names=names)
        return self._flush_record_batch(sid, batch)


# =====================================================================
# 2. DuckDB Abstract Tick, Daily, Close
# =====================================================================

cdef class BaseDuckDBProvider(BaseBufferedProvider):

    async def __call__(self, int32_t start_date, int32_t end_date, list sids=None): # avoid list=[] cause leak and None 
        cdef:
            list query_sids = sids if sids is not None else []
            Request req = Request(start_date=start_date, end_date=end_date, sid=query_sids)
            object batch, duck_mgr = get_duckdb_manager()

        async with duck_mgr as ctx:
            async for batch in ctx.query(req, self.template):
                if batch.num_rows == 0: 
                    continue
                try:
                    for resp in self._process_batch(batch):
                        yield resp 
                except asyncio.TimeoutError:
                    print(f"[{self.rpc_type.decode()}] _process_batch Timeout")
                    continue 
                except asyncio.CancelledError:
                    print(f"[{self.rpc_type.decode()}] _process_batch CancelledError")
                    raise 

    def _process_batch(self, object batch):
        cdef object sid_col = batch.column("sid")
        cdef Py_ssize_t num_rows = len(sid_col)
        
        cdef tuple indices
        cdef object s_indices, e_indices, slice_batch
        cdef Py_ssize_t start, end
        cdef bytes sid
        
        if num_rows == 0: 
            return
            
        indices = _slice_by_sid(sid_col, num_rows)
        s_indices, e_indices = indices[0], indices[1]

        for start_scalar, end_scalar in zip(s_indices, e_indices):
            start = start_scalar.as_py()
            end = end_scalar.as_py()
            sid = sid_col[start].as_py()
            slice_batch = batch.slice(start, end - start)
            yield self._flush_record_batch(sid, slice_batch)


# -------------------------------- DuckDB Dispatcher ----------------------------

cdef class Tick(BaseDuckDBProvider):
    def __cinit__(self):
        self.rpc_type = b"tick"
        self.template = TICK_TEMPLATE


cdef class Daily(BaseDuckDBProvider):
    def __cinit__(self):
        self.rpc_type = b"daily"
        self.template = DAILY_TEMPLATE


cdef class Close(BaseDuckDBProvider):
    def __cinit__(self):
        self.rpc_type = b"close"
        self.template = CLOSE_TEMPLATE


# =====================================================================
# 3. SQLAlchemy Instrument, Adjust, Right
# =====================================================================

cdef class BaseSQLAlchemyProvider(BaseBufferedProvider):

    cdef object _build_statement(self, int32_t start_date, int32_t end_date, list sids):
        raise NotImplementedError()

    cdef void _init_buffers(self):
        raise NotImplementedError()

    cdef void _row_to_buffer(self, int i, object row):
        raise NotImplementedError()

    cdef object _flush_buffer(self, int count, bytes sid):
        raise NotImplementedError()

    async def __call__(self, int32_t start_date, int32_t end_date, list sids=None):
        cdef:
            int i = 0
            object row, stream_wrap, stream_proxy
            bytes r_sid, last_sid = None # cdef None is allowed

        self._init_buffers()

        async with async_ops as ctx:
            stmt = self._build_statement(start_date, end_date, sids)
            stream_wrap = await ctx.on_query(stmt)

            async with stream_wrap as stream_proxy:
                async for row in stream_proxy:
                    r_sid = row[0] if self.group_by_sid else b''

                    # i >= CHUNK_SIZE avoid segment fault
                    if i >= CHUNK_SIZE or (self.group_by_sid and last_sid is not None and r_sid != last_sid):
                        yield self._flush_buffer(i, last_sid)
                        self._init_buffers()
                        i = 0

                    self._row_to_buffer(i, row)
                    if self.group_by_sid:
                        last_sid = r_sid
                    i += 1

                if i > 0:
                    yield self._flush_buffer(i, last_sid if self.group_by_sid else b'')


# -------------------------------- SQLAlchemy Dispatcher ----------------------------
cdef class Instrument(BaseSQLAlchemyProvider):

    def __cinit__(self):
        self.rpc_type = b"instrument"
        self.group_by_sid = False

    cdef object _build_statement(self, int32_t start_date, int32_t end_date, list sids):
        stmt = select(
            Asset.sid, Asset.name, Asset.first_trading, 
            Asset.delist, Asset.merger, Asset.ratio
        )
        if sids:
            stmt = stmt.where(Asset.sid.in_(sids))
        else:
            stmt = stmt.where(Asset.first_trading.between(start_date, end_date))
        return stmt

    cdef void _init_buffers(self):
        self.buf_sid = [b''] * CHUNK_SIZE
        self.buf_name = [b''] * CHUNK_SIZE
        self.buf_first_trading = np.empty(CHUNK_SIZE, dtype=np.int32)
        self.buf_delist = np.empty(CHUNK_SIZE, dtype=np.int32)
        self.buf_merger = [b''] * CHUNK_SIZE
        self.buf_ratio = np.empty(CHUNK_SIZE, dtype=np.float32)

    cdef void _row_to_buffer(self, int i, object row):
        self.buf_sid[i] = row[0]
        self.buf_name[i] = row[1]
        self.buf_first_trading[i] = row[2]
        self.buf_delist[i] = row[3]
        self.buf_merger[i] = row[4]
        self.buf_ratio[i] = row[5]

    cdef object _flush_buffer(self, int count, bytes sid):
        cdef list arrays = [
            pa.array(self.buf_sid[:count], type=pa.binary()),
            pa.array(self.buf_name[:count], type=pa.binary()),
            pa.array(self.buf_first_trading[:count], type=pa.int32()),
            pa.array(self.buf_delist[:count], type=pa.int32()),
            pa.array(self.buf_merger[:count], type=pa.binary()),
            pa.array(self.buf_ratio[:count], type=pa.float32()),
        ]
        cdef list names = ["sid", "name", "first_trading", "delist", "merger", "ratio"]
        return self._create_and_flush_arrays(None, arrays, names)


cdef class Adjust(BaseSQLAlchemyProvider):

    def __cinit__(self):
        self.rpc_type = b"adjustment"
        self.group_by_sid = True

    cdef object _build_statement(self, int32_t start_date, int32_t end_date, list sids):
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
        return stmt

    cdef void _init_buffers(self):
        self.buf_ex_date = np.empty(CHUNK_SIZE, dtype=np.int32)
        self.buf_register_date = np.empty(CHUNK_SIZE, dtype=np.int32)
        self.buf_bonus_share = np.empty(CHUNK_SIZE, dtype=np.int32)
        self.buf_transfer = np.empty(CHUNK_SIZE, dtype=np.int32)
        self.buf_bonus = np.empty(CHUNK_SIZE, dtype=np.int32)

    cdef void _row_to_buffer(self, int i, object row):
        self.buf_ex_date[i] = row[1]
        self.buf_register_date[i] = row[2]
        self.buf_bonus_share[i] = row[3]
        self.buf_transfer[i] = row[4]
        self.buf_bonus[i] = row[5]

    cdef object _flush_buffer(self, int count, bytes sid):
        cdef list arrays = [
            pa.array(self.buf_ex_date[:count], type=pa.int32()),
            pa.array(self.buf_register_date[:count], type=pa.int32()),
            pa.array(self.buf_bonus_share[:count], type=pa.int32()),
            pa.array(self.buf_transfer[:count], type=pa.int32()),
            pa.array(self.buf_bonus[:count], type=pa.int32()),
        ]
        cdef list names = ["ex_date", "register_date", "bonus_share", "transfer", "bonus"]
        return self._create_and_flush_arrays(sid, arrays, names)


cdef class Right(BaseSQLAlchemyProvider):

    def __cinit__(self):
        self.rpc_type = b"right"
        self.group_by_sid = True

    cdef object _build_statement(self, int32_t start_date, int32_t end_date, list sids):
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
        return stmt

    cdef void _init_buffers(self):
        self.buf_ex_date = np.empty(CHUNK_SIZE, dtype=np.int32)
        self.buf_register_date = np.empty(CHUNK_SIZE, dtype=np.int32)
        self.buf_price = np.empty(CHUNK_SIZE, dtype=np.int32)
        self.buf_ratio = np.empty(CHUNK_SIZE, dtype=np.int32)

    cdef void _row_to_buffer(self, int i, object row):
        self.buf_ex_date[i] = row[1]
        self.buf_register_date[i] = row[2]
        self.buf_price[i] = row[3]
        self.buf_ratio[i] = row[4]

    cdef object _flush_buffer(self, int count, bytes sid):
        cdef list arrays = [
            pa.array(self.buf_ex_date[:count], type=pa.int32()),
            pa.array(self.buf_register_date[:count], type=pa.int32()),
            pa.array(self.buf_price[:count], type=pa.int32()),
            pa.array(self.buf_ratio[:count], type=pa.int32()),
        ]
        cdef list names = ["ex_date", "register_date", "price", "ratio"]
        return self._create_and_flush_arrays(sid, arrays, names)
