# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False 
# cython: cdivision=True

import os
import logging
import asyncio
import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
from sqlalchemy import select, and_, or_, func, cast, Integer, BigInteger

from libc.stdint cimport uint8_t, int32_t, int64_t
from libcpp.string cimport string as cpp_string

from rpc_feed.core.gateway.duckdb.utils cimport Request
from rpc_feed.core.gateway import *

from bt_protocol.serialize.pb import bt_protocol_service_pb2
from bt_protocol.template.duckdb_template import *
from bt_protocol.schema.asset import *

cdef int PG_BATCH_SIZE = int(os.getenv("PGBATCHSIZE", CHUNK_SIZE))


cdef object arrow_options = pa.ipc.IpcWriteOptions(
    compression='lz4',
    use_threads=False  # avoid threads  
) 

cdef object batch_to_resp(object batch): # protobuf payload bytes
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, batch.schema, options=arrow_options) as writer:
        writer.write_batch(batch)
    return bt_protocol_service_pb2.ArrowFrame(payload=sink.getvalue().to_pybytes())


# =====================================================================
# 1. Public Abstract Buffer
# =====================================================================

cdef class BaseBufferedProvider:

    cdef object _flush_record_batch(self, bytes sid, object batch, bytes asset_type=None):
        cdef dict metadata = {
            b"rpc_type": self.rpc_type
        }
        if sid is not None and len(sid) > 0:
            metadata[b"sid"] = sid
        if asset_type is not None:
            metadata[b"asset_type"] = asset_type
        batch = batch.replace_schema_metadata(metadata)
        return batch_to_resp(batch)

    cdef object _create_and_flush_arrays(self, bytes sid, list arrays, list names):
        cdef object batch = pa.RecordBatch.from_arrays(arrays, names=names)
        return self._flush_record_batch(sid, batch)


# =====================================================================
# 2. DuckDB Abstract Tick, Daily, Close
# =====================================================================

cdef class BaseDuckDBProvider(BaseBufferedProvider):

    async def __call__(self, int32_t start_date, int32_t end_date, list sids=None, object context=None):
        cdef:
            list query_sids = sids if sids is not None else []
            Request req = Request(start_date=start_date, end_date=end_date, sid=query_sids)
            object batch, duck_mgr = get_duckdb_manager()
            object bm_regex = duck_mgr.regex_rules["benchmark"]
            bytes asset_type = (
                b"benchmark"
                if (query_sids and all(bm_regex.match(s) for s in query_sids))
                else None
            )

        async with duck_mgr as ctx:
            async for batch in ctx.query(req, self.template):
                if context is not None and context.done():
                    logging.info("[%s] client cancelled, stopping stream", self.rpc_type.decode())
                    return
                if batch.num_rows == 0: 
                    continue
                yield self._flush_record_batch(None, batch, asset_type)


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
# Inner Buffer Container avoid abrupted when Conncurrency
# =====================================================================

cdef class InstrumentBuffer:
    def __cinit__(self):
        self.buf_sid = [b''] * PG_BATCH_SIZE
        self.buf_name = [b''] * PG_BATCH_SIZE
        self.buf_first_trading = np.empty(PG_BATCH_SIZE, dtype=np.int32)
        self.buf_delist = np.empty(PG_BATCH_SIZE, dtype=np.int32)
        self.buf_merger = [b''] * PG_BATCH_SIZE
        self.buf_ratio = np.empty(PG_BATCH_SIZE, dtype=np.float32)


cdef class AdjustBuffer:
    def __cinit__(self):
        self.buf_ex_date = np.empty(PG_BATCH_SIZE, dtype=np.int32)
        self.buf_register_date = np.empty(PG_BATCH_SIZE, dtype=np.int32)
        self.buf_bonus_share = np.empty(PG_BATCH_SIZE, dtype=np.int32)
        self.buf_transfer = np.empty(PG_BATCH_SIZE, dtype=np.int32)
        self.buf_bonus = np.empty(PG_BATCH_SIZE, dtype=np.int32)


cdef class RightBuffer:
    def __cinit__(self):
        self.buf_ex_date = np.empty(PG_BATCH_SIZE, dtype=np.int32)
        self.buf_register_date = np.empty(PG_BATCH_SIZE, dtype=np.int32)
        self.buf_price = np.empty(PG_BATCH_SIZE, dtype=np.int32)
        self.buf_ratio = np.empty(PG_BATCH_SIZE, dtype=np.int32)


# =====================================================================
# 3. SQLAlchemy Instrument, Adjust, Right
# =====================================================================

cdef class BaseSQLAlchemyProvider(BaseBufferedProvider):

    cdef object _build_statement(self, int32_t start_date, int32_t end_date, list sids):
        raise NotImplementedError()

    cdef object _init_buffers(self):
        raise NotImplementedError()

    cdef void _row_to_buffer(self, object buf, int i, object row):
        raise NotImplementedError()

    cdef object _flush_buffer(self, object buf, int count, bytes sid):
        raise NotImplementedError()

    async def __call__(self, int32_t start_date, int32_t end_date, list sids=None, object context=None):
        cdef:
            int i = 0
            object row, stream_wrap, stream_proxy
            bytes r_sid, last_sid = None
            object buf  

        buf = self._init_buffers()

        async with async_ops as ctx:
            stmt = self._build_statement(start_date, end_date, sids)
            stream_wrap = await ctx.on_query(stmt)

            async with stream_wrap as stream_proxy:
                async for row in stream_proxy:
                    if context is not None and context.done():
                        logging.info("[%s] client cancelled, stopping stream", self.rpc_type.decode())
                        return

                    r_sid = row[0] if self.group_by_sid else b''

                    # i >= PG_BATCH_SIZE avoid segment fault
                    if i >= PG_BATCH_SIZE or (self.group_by_sid and last_sid is not None and r_sid != last_sid):
                        yield self._flush_buffer(buf, i, last_sid)
                        buf = self._init_buffers()
                        i = 0

                    self._row_to_buffer(buf, i, row)
                    if self.group_by_sid:
                        last_sid = r_sid
                    i += 1

                if i > 0:
                    yield self._flush_buffer(buf, i, last_sid if self.group_by_sid else b'')


# -------------------------------- SQLAlchemy Dispatcher ----------------------------
cdef class Instrument(BaseSQLAlchemyProvider):

    def __cinit__(self):
        self.rpc_type = b"instrument"
        self.group_by_sid = False

    cdef object _build_statement(self, int32_t start_date, int32_t end_date, list sids):
        stmt = select(
            Asset.sid,
            Asset.name,
            Asset.first_trading,
            func.coalesce(Asset.delist, 0).label("delist"), # It checks the value of the delist column on the Asset table. If delist is NULL, it falls back to 0. If delist is not NULL, it keeps its original value.
            Asset.merger,
            func.coalesce(Asset.ratio, 0.0).label("ratio"),
        )
        if sids:
            stmt = stmt.where(Asset.sid.in_(sids))
        else:
            stmt = stmt.where(
                and_(
                    Asset.first_trading <= end_date,
                    or_(
                        Asset.delist.is_(None),
                        Asset.delist == 0,
                        Asset.delist >= start_date,
                    ),
                )
            )
        return stmt

    cdef object _init_buffers(self):
        return InstrumentBuffer()

    cdef void _row_to_buffer(self, object buf, int i, object row):
        buf.buf_sid[i] = row[0]
        buf.buf_name[i] = row[1]
        buf.buf_first_trading[i] = row[2]
        buf.buf_delist[i] = row[3]
        buf.buf_merger[i] = row[4]
        buf.buf_ratio[i] = row[5]

    cdef object _flush_buffer(self, object buf, int count, bytes sid):
        cdef list arrays = [
            pa.array(buf.buf_sid[:count], type=pa.binary()),
            pa.array(buf.buf_name[:count], type=pa.binary()),
            pa.array(buf.buf_first_trading[:count], type=pa.int32()),
            pa.array(buf.buf_delist[:count], type=pa.int32()),
            pa.array(buf.buf_merger[:count], type=pa.binary()),
            pa.array(buf.buf_ratio[:count], type=pa.float32()),
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

    cdef object _init_buffers(self):
        return AdjustBuffer()

    cdef void _row_to_buffer(self, object buf, int i, object row):
        buf.buf_ex_date[i] = row[1]
        buf.buf_register_date[i] = row[2]
        buf.buf_bonus_share[i] = row[3]
        buf.buf_transfer[i] = row[4]
        buf.buf_bonus[i] = row[5]

    cdef object _flush_buffer(self, object buf, int count, bytes sid):
        cdef list arrays = [
            pa.array(buf.buf_ex_date[:count], type=pa.int32()),
            pa.array(buf.buf_register_date[:count], type=pa.int32()),
            pa.array(buf.buf_bonus_share[:count], type=pa.int32()),
            pa.array(buf.buf_transfer[:count], type=pa.int32()),
            pa.array(buf.buf_bonus[:count], type=pa.int32()),
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

    cdef object _init_buffers(self):
        return RightBuffer()

    cdef void _row_to_buffer(self, object buf, int i, object row):
        buf.buf_ex_date[i] = row[1]
        buf.buf_register_date[i] = row[2]
        buf.buf_price[i] = row[3]
        buf.buf_ratio[i] = row[4]

    cdef object _flush_buffer(self, object buf, int count, bytes sid):
        cdef list arrays = [
            pa.array(buf.buf_ex_date[:count], type=pa.int32()),
            pa.array(buf.buf_register_date[:count], type=pa.int32()),
            pa.array(buf.buf_price[:count], type=pa.int32()),
            pa.array(buf.buf_ratio[:count], type=pa.int32()),
        ]
        cdef list names = ["ex_date", "register_date", "price", "ratio"]
        return self._create_and_flush_arrays(sid, arrays, names)