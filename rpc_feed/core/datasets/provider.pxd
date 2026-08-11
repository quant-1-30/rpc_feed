# cython: language_level=3
cimport numpy as cnp
cnp.import_array() # initialize numpy c_api
from libc.stdint cimport uint8_t, int32_t, int64_t
from libcpp.string cimport string as cpp_string

cdef enum:
    CHUNK_SIZE = 1024
    MULT = 1000
    TICK_PROCESS_TIMEOUT = 100
    MAX_FRAME_BYTES = 32 * 1024 * 1024
    ESTIMATED_BYTES_PER_ROW = 64


cdef class BaseBufferedProvider:
    cdef bytes rpc_type

    cdef object _flush_record_batch(self, bytes sid, object batch)
    
    cdef object _create_and_flush_arrays(self, bytes sid, list arrays, list names)


cdef class BaseDuckDBProvider(BaseBufferedProvider):
    cdef object template
    

cdef class Tick(BaseDuckDBProvider):

    pass

cdef class Daily(BaseDuckDBProvider):

    pass


cdef class Close(BaseDuckDBProvider):

    pass


# =====================================================================
# Buffer Container
# =====================================================================

cdef class InstrumentBuffer:
    cdef public list buf_sid
    cdef public list buf_name
    cdef public object buf_first_trading
    cdef public object buf_delist
    cdef public list buf_merger
    cdef public object buf_ratio


cdef class AdjustBuffer:
    cdef public object buf_ex_date
    cdef public object buf_register_date
    cdef public object buf_bonus_share
    cdef public object buf_transfer
    cdef public object buf_bonus


cdef class RightBuffer:
    cdef public object buf_ex_date
    cdef public object buf_register_date
    cdef public object buf_price
    cdef public object buf_ratio


# =====================================================================
# 3. SQLAlchemy Instrument, Adjust, Right
# =====================================================================

cdef class BaseSQLAlchemyProvider(BaseBufferedProvider):
    cdef bint group_by_sid

    cdef object _build_statement(self, int32_t start_date, int32_t end_date, list sids)

    cdef object _init_buffers(self)

    cdef void _row_to_buffer(self, object buf, int i, object row)

    cdef object _flush_buffer(self, object buf, int count, bytes sid)


cdef class Instrument(BaseSQLAlchemyProvider):

    pass


cdef class Adjust(BaseSQLAlchemyProvider):

    pass


cdef class Right(BaseSQLAlchemyProvider):

    pass