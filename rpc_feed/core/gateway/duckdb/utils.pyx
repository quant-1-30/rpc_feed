# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False
# cython: cdivision=True

from libc.stdint cimport int64_t
from libc.time cimport time_t, strftime, tm

cdef extern from "time.h" nogil:
    tm* gmtime_r(const time_t *timep, tm *result)

cdef int64_t _NS_THRESHOLD = 100_000_000_000_000_000
cdef int64_t _US_THRESHOLD = 100_000_000_000_000
cdef int64_t _MS_THRESHOLD = 100_000_000_000
cdef int64_t _NS_PER_SEC = 1_000_000_000
cdef int64_t _US_PER_SEC = 1_000_000
cdef int64_t _MS_PER_SEC = 1_000


cdef inline int64_t _normalize_epoch_sec(int64_t val) nogil:
    """ns / us / ms / s ---> Second Unix Epoch"""
    if val > _NS_THRESHOLD:
        return val // _NS_PER_SEC
    elif val > _US_THRESHOLD:
        return val // _US_PER_SEC
    elif val > _MS_THRESHOLD:
        return val // _MS_PER_SEC
    return val


cdef void _parse_to_ymd(int64_t val, int* y, int* m, int* d):
    """YYYYMMDD / Timestamp ---> (y, m, d)"""
    cdef time_t t_val
    cdef tm timeinfo
    cdef tm* ret_ptr

    if val <= MAX_YYYYMMDD:
        y[0] = val // 10000
        m[0] = (val % 10000) // 100
        d[0] = val % 100
    else:
        val = _normalize_epoch_sec(val) + UTC8_OFFSET_SEC
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


cdef str _parse_date(int64_t val, bint is_end):
    """ISO String (Timestamp / YYYYMMDD)"""
    cdef int y, m, d
    cdef time_t t_val
    cdef tm timeinfo
    cdef tm* ret_ptr
    cdef char buffer[32]

    if val <= MAX_YYYYMMDD:
        y = val // 10000
        m = (val % 10000) // 100
        d = val % 100
        if is_end:
            return f"{y:04d}-{m:02d}-{d:02d} 23:59:59+08:00"
        return f"{y:04d}-{m:02d}-{d:02d} 00:00:00+08:00"

    t_val = <time_t>(_normalize_epoch_sec(val) + UTC8_OFFSET_SEC)
    ret_ptr = gmtime_r(&t_val, &timeinfo)
    if ret_ptr == NULL:
        return "1970-01-01 23:59:59+08:00" if is_end else "1970-01-01 00:00:00+08:00"

    strftime(buffer, sizeof(buffer), "%Y-%m-%d %H:%M:%S+08:00", &timeinfo)
    return buffer.decode("utf-8")


cpdef list schema_range(Request req):
    """(year, quarter, YYYYMM)"""
    cdef int64_t start_date = req.start_date
    cdef int64_t end_date = req.end_date
    cdef int y, m, d
    cdef int ey, em, ed

    _parse_to_ymd(start_date, &y, &m, &d)
    _parse_to_ymd(end_date, &ey, &em, &ed)

    if not (1 <= m <= 12 and 1 <= d <= 31):
        raise ValueError(f"invalid start_date {start_date} (y/m/d={y}/{m}/{d})")
    if not (1 <= em <= 12 and 1 <= ed <= 31):
        raise ValueError(f"invalid end_date {end_date} (y/m/d={ey}/{em}/{ed})")

    cdef list result = []
    cdef int q_num

    while y < ey or (y == ey and m <= em):
        q_num = ((m - 1) // 3) + 1
        result.append((y, f"Q{q_num}", f"{y}{m:02d}"))
        if m == 12:
            y += 1
            m = 1
        else:
            m += 1
    return result


cpdef dict preprocess_req(Request req):
    """DuckDB SQL Binding Dict"""
    cdef bytes byte_sid
    cdef list str_sids = [byte_sid.decode("utf-8") for byte_sid in req.sid]
    return {
        "start_str": _parse_date(req.start_date, False),
        "end_str": _parse_date(req.end_date, True),
        "sids": str_sids,
    }
