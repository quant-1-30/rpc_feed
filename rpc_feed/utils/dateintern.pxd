# distutils: language = c++
# cython: language_level=3

from libc.stdint cimport uint8_t, int64_t
from libc.time cimport gmtime, time_t, tm

cdef const int64_t HOURS_PER_DAY = 24 # DEF is desprecated

cdef enum MarketConstants: # DEF is desprecated in Cython 3.x / cdef enum / cdef int64_t 
    MINUTES_PER_HOUR = 60
    SECONDS_PER_MINUTE = 60
    OPEN_OFFSET = 34200
    CLOSE_OFFSET = 54000
    SECONDS_PER_DAY = 86400 
    SHANGHAI_OFFSET = 28800


# cdef struct CTime: # replace datetime
#     int year, month, day, hour, minute, second, microsecond

cdef struct MarketTime:
    int64_t open_ts
    int64_t close_ts


# C typedef / cython ctypedef
cpdef object num2date(double ts, bint native=?)

cpdef double date2num(object dt)

cpdef int64_t ts2intdt(double ts, bint native=?) # only cdef nogil

cpdef int64_t intdt2ts(int64_t date_int, bint native=?) nogil

cpdef object tzparse(str tz)

cdef MarketTime market_utc(int64_t ts, bint native=?) nogil 
           