cimport numpy as cnp
cnp.import_array() # initialize numpy c_api
from libc.stdint cimport uint8_t, int32_t, int64_t
from libcpp.string cimport string as cpp_string

cdef enum:
    CHUNK_SIZE = 1024
    MULT = 1000
    TICK_PROCESS_TIMEOUT = 100


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


cdef class BaseSQLAlchemyProvider(BaseBufferedProvider):
    cdef bint group_by_sid

    cdef object _build_statement(self, int32_t start_date, int32_t end_date, list sids)

    cdef void _init_buffers(self)

    cdef void _row_to_buffer(self, int i, object row)

    cdef object _flush_buffer(self, int count, bytes sid)


cdef class Instrument(BaseSQLAlchemyProvider):
    cdef list buf_sid
    cdef list buf_name
    cdef object buf_first_trading
    cdef object buf_delist
    cdef object buf_ratio
    cdef list buf_merger


cdef class Adjust(BaseSQLAlchemyProvider):
    cdef object buf_ex_date
    cdef object buf_register_date
    cdef object buf_bonus_share
    cdef object buf_transfer
    cdef object buf_bonus


cdef class Right(BaseSQLAlchemyProvider):
    cdef object buf_ex_date
    cdef object buf_register_date
    cdef object buf_price
    cdef object buf_ratio

