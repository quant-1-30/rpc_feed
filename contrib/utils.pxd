from libcpp.vector cimport vector
from libcpp.string cimport string as cpp_string
from libc.stdint cimport int64_t

cdef struct Request:
    int64_t start_date
    int64_t end_date
    vector[cpp_string] sid # bytes in python


cpdef list schema_range(Request req)
    
cpdef str create_parquet_macro(str path, str view_name)

cpdef dict preprocess_req(Request req)

cpdef str request_to_sql(list view_names, dict req_meta, str template)
