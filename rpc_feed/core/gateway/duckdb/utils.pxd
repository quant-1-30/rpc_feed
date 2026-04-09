from libcpp.vector cimport vector
from libcpp.string cimport string as cpp_string
from libc.stdint cimport int64_t

cdef struct Request:
    int64_t start_date
    int64_t end_date
    vector[cpp_string] sid # bytes in python


cpdef list schema_range(Request req)
    
cpdef dict preprocess_req(Request req)
