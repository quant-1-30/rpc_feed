# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False
# cython: cdivision=True

from libc.stdint cimport int64_t
from libc.time cimport time_t, strftime, tm

# ==========================================
# C cdef 
# ==========================================
cdef extern from "time.h" nogil: # tm already no need struct
    # tm* localtime_r(const time_t *timep, tm *result) # binding to os timezone
    tm* gmtime_r(const time_t *timep, tm *result) # utc


# ==========================================
# intended for schema_range
# ==========================================
cdef void _parse_to_ymd(int64_t val, int* y, int* m, int* d):
    """
    YYYYMMDD / Timestamp ---> ptr
    """
    cdef time_t t_val
    cdef tm timeinfo
    cdef tm* ret_ptr

    if val <= 99991231:
        y[0] = val // 10000
        m[0] = (val % 10000) // 100
        d[0] = val % 100
    else:
        # Unix Timestamp
        if val > 100000000000000000: # ns
            val = val // 1000000000
        elif val > 100000000000000:  # us
            val = val // 1000000
        elif val > 100000000000:     # ms
            val = val // 1000

        # Hive y/m asia/shanghai UTC + 28800
        val += 28800 
        t_val = <time_t>val
        ret_ptr = gmtime_r(&t_val, &timeinfo)
 
        if ret_ptr != NULL:
            y[0] = timeinfo.tm_year + 1900
            m[0] = timeinfo.tm_mon + 1
            d[0] = timeinfo.tm_mday
        else:
            y[0] = 1970
            m[0] = 1
            d[0] = 1


# ==========================================
# intended for preprocess_req 
# ==========================================
cdef str _parse_date(int64_t val, bint is_end):
    """
    格式化输出 ISO String (兼容 Timestamp / YYYYMMDD)
    """
    cdef int y, m, d
    cdef time_t t_val
    cdef tm timeinfo     
    cdef tm* ret_ptr
    cdef char buffer[32]  

    if val <= 99991231:
        y = val // 10000
        m = (val % 10000) // 100
        d = val % 100

        # if is_end:
        #     return f"{y:04d}-{m:02d}-{d:02d} 23:59:59"
        # else:
        #     return f"{y:04d}-{m:02d}-{d:02d} 00:00:00"

        # YYYYMMDD 北京时间 +08:00
        if is_end:
            return f"{y:04d}-{m:02d}-{d:02d} 23:59:59+08:00"
        else:
            return f"{y:04d}-{m:02d}-{d:02d} 00:00:00+08:00"
    else:
        # UTC 
        if val > 100000000000000000:
            val = val // 1000000000
        elif val > 100000000000000: 
            val = val // 1000000
        elif val > 100000000000:    
            val = val // 1000

        t_val = <time_t>val
        ret_ptr = gmtime_r(&t_val, &timeinfo) # UTC
        
        if ret_ptr == NULL:
            return "1970-01-01 23:59:59+00:00" if is_end else "1970-01-01 00:00:00+00:00"
        
        # UTC (+00:00)
        strftime(buffer, sizeof(buffer), "%Y-%m-%d %H:%M:%S+00:00", &timeinfo)
        return buffer.decode("utf-8")


cpdef list schema_range(Request req):
    """
        20250402 / 1774972800 
    """
    cdef int64_t start_date = req.start_date
    cdef int64_t end_date = req.end_date
    
    cdef int y, m, d
    cdef int ey, em, ed
    
    _parse_to_ymd(start_date, &y, &m, &d)
    _parse_to_ymd(end_date, &ey, &em, &ed)
    
    cdef list result =[]
    cdef int q_num
    cdef str q_str, m_str
    
    while y < ey or (y == ey and m <= em):
        q_num = ((m - 1) // 3) + 1
        q_str = f"Q{q_num}"
        m_str = f"{y}{m:02d}"
        
        result.append((y, q_str, m_str))
        if m == 12:
            y += 1
            m = 1
        else:
            m += 1
    return result


cpdef dict preprocess_req(Request req):
    """
        ISO and sid_bytes
    """
    cdef dict resp
    cdef int64_t start = req.start_date
    cdef int64_t end = req.end_date
    cdef str start_str, end_str
    
    cdef bytes byte_sid
    cdef list str_sids = []

    start_str = _parse_date(start, False)
    end_str = _parse_date(end, True)

    # cython auto transformt to (for auto& / const auto& v in ***)
    # for (std::vector<std::string>::iterator it = funk.begin(); it != funk.end(); ++it)
    for byte_sid in req.sid:
        str_sids.append(byte_sid.decode("utf-8")) # bytes --> str avoid base64 encoding avoid repr auto add b prefix

    # DuckDB support ISO TIMESTAMP 
    resp = {"start_str": start_str, "end_str": end_str, "sids": str_sids}
    return resp
