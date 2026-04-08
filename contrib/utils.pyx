#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False
# cython: cdivision=True

import cython


cpdef list schema_range(Request req):
    """
        输入示例: 20250402, 20260101 
        计算日期范围内的年、季度、月份字符串
    """
    cdef int start_date = req.start_date
    cdef int end_date = req.end_date
    
    cdef int y = start_date // 10000
    cdef int m = (start_date % 10000) // 100
    
    cdef int ey = end_date // 10000
    cdef int em = (end_date % 10000) // 100
    
    cdef list result = []
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


cpdef str create_parquet_macro(str path, str view_name):
    """
    Create a DuckDB macro for scanning parquet files with proper syntax.
    
    Args:
        year: Year (e.g., 2019)
        quarter: Quarter (e.g., 'Q4')
        sid: Stock ID (e.g., 600225)
        date: Date in YYYYMM format (e.g., '201911')
    
    Returns:
        SQL string to create the macro
    """
    # not supported macro
    return (
        f"CREATE OR REPLACE VIEW {view_name} AS "
        f"SELECT * FROM parquet_scan('{path}/**/*.parquet', "
        f"HIVE_PARTITIONING=TRUE, " # extract feature from parquet name
        f"UNION_BY_NAME=TRUE);" # schema evolution
    )


cpdef dict preprocess_req(Request req):
    """
        预处理请求：格式化 SID 列表和 ISO 时间字符串
    """
    cdef dict resp
    cdef int64_t start = req.start_date
    cdef int64_t end = req.end_date
    
    cdef int s_y = start // 10000
    cdef int s_m = (start % 10000) // 100
    cdef int s_d = start % 100
    
    cdef int e_y = end // 10000
    cdef int e_m = (end % 10000) // 100
    cdef int e_d = end % 100
    
    cdef list str_sids = []
    cdef bytes byte_sid
    cdef str py_sid

    # cython auto transformt to (for auto& / const auto& v in ***)
    # for (std::vector<std::string>::iterator it = funk.begin(); it != funk.end(); ++it)
    for byte_sid in req.sid:
        py_sid = byte_sid.decode("utf-8") # bytes --> str avoid base64 encoding
        str_sids.append(f"'{py_sid}'") # avoid repr auto add b prefix
    
    # DuckDB support ISO TIMESTAMP 
    resp = {"start_str": f"{s_y:04d}-{s_m:02d}-{s_d:02d} 00:00:00", 
           "end_str": f"{e_y:04d}-{e_m:02d}-{e_d:02d} 23:59:59", 
           "sid_str": f"({','.join(str_sids)})"}
    return resp


cpdef str request_to_sql(list view_names, dict req_meta, str template):
    """
        构造统一 SQL 查询，用于 sid + datetime 范围过滤
    """
    cdef dict sql_meta = preprocess_req(req_meta)
    
    # join super than + 
    cdef str union_sql = "\nUNION ALL\n".join([f"SELECT * FROM {v}" for v in view_names])
    
    sql_meta["union_sql"] = union_sql
    
    return template.format_map(sql_meta)
