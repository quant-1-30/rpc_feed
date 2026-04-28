cimport numpy as cnp
cnp.import_array() # initialize numpy c_api
from libc.stdint cimport uint8_t, int32_t, int64_t
from libcpp.string cimport string as cpp_string

cdef enum:
    CHUNK_SIZE = 1024
    MULT = 1000
    TICK_PROCESS_TIMEOUT = 100


cdef class Instrument:
    
    cdef object _flush(self, int count, list buf_sid, list buf_name, object buf_first_trading, object buf_delist)


cdef class Tick:

    cdef _flush(self, bytes sid, object batch)


cdef class Close:
    
    cdef object _flush(self, bytes sid, object batch)


cdef class Adjust:
    
    cdef object _flush(self, int count, bytes sid, object buf_ex_date, object buf_register_date, object buf_bonus_share, object buf_transfer, object buf_bonus)


cdef class Right:

    cdef object _flush(self, int count, bytes sid, object buf_ex_date, object buf_register_date, object buf_price, object buf_ratio)

