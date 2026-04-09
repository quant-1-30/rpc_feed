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


cdef class Index:
    
    cdef _flush(self, bytes sid, object batch)


cdef class Tick:

    cdef _flush(self, bytes sid, object batch)


cdef class Close:
    
    cdef object _flush(self, bytes sid, object batch)


cdef class Adjust:
    cdef cnp.int32_t[:] _buf_ex_date, _buf_register_date, _buf_bonus_share, _buf_transfer, _buf_bonus # cdef cnp.float[:] 
    
    cdef object _flush(self, int count, bytes sid)


cdef class Right:
    cdef cnp.int32_t[:] _buf_ex_date, _buf_register_date, _buf_price, _buf_ratio

    cdef object _flush(self, int count, bytes sid)


cdef class Experiment:

    cdef _flush(self, bytes sid, object batch)
