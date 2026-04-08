cimport numpy as cnp
cnp.import_array() # initialize numpy c_api


cdef class TradingCalendar:
    cdef cnp.int32_t[:] _buf_date

    cdef object _flush(self, int count) # protobuf extend slice copy avoid append 


cdef class Instrument:
    cdef cnp.int32_t[:] _buf_first_trading
    cdef cnp.int32_t[:] _buf_delist
    cdef list _buf_sid
    cdef list _buf_name
    
    cdef object _flush(self, int count) # protobuf extend slice copy avoid append 


#cdef class Index:
#    cdef cnp.int32_t[:] _buf_date, _buf_open, _buf_high, _buf_low, _buf_close
#    cdef cnp.int64_t[:] _buf_volume, _buf_amount
#    
#    cdef object _flush(self, int count, bytes sid)
#

cdef class Index:
    cdef str dataset_root
    
    cdef _flush(self, bytes sid, object batch)


cdef class Tick:
    cdef cnp.ndarray _buf_sid # used for pa.BatchArray to_numpy
    cdef Py_ssize_t[:] c_indices # int64_t / cnp.long_t

    cdef _flush(self, bytes sid, object batch)


cdef class Close:
    cdef cnp.ndarray _buf_sid # used for pa.BatchArray to_numpy
    cdef Py_ssize_t[:] c_indices # int64_t / cnp.long_t
    
    cdef object _flush(self, bytes sid, object batch)


cdef class Adjust:
    cdef cnp.int32_t[:] _buf_ex_date, _buf_register_date, _buf_bonus_share, _buf_transfer, _buf_bonus # cdef cnp.float[:] 
    
    cdef object _flush(self, int count, bytes sid)


cdef class Right:
    cdef cnp.int32_t[:] _buf_ex_date, _buf_register_date, _buf_price, _buf_ratio

    cdef object _flush(self, int count, bytes sid)
