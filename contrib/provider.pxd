cdef class TradingCalendar:
    cdef cnp.int32_t[:] _buf_date

    cdef object _flush(self, int count) # protobuf extend slice copy avoid append 
    