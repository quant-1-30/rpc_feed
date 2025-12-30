# cython: language_level=3, boundscheck=False, wraparound=False

def schema_range(dict req): # 20250402
    cdef int start = req["start_date"]
    cdef int end = req["end_date"]
    
    cdef int y = start // 10000
    cdef int m = (start % 10000) // 100
    
    cdef int ey = end // 10000
    cdef int em = (end % 10000) // 100
    
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


def create_parquet_macro(str path, str view_name):
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
    # # UNION_BY_NAME=TRUE —— 确保数据完整性和正确性避免schema变化
    # not supported macro
    return f"""CREATE OR REPLACE VIEW {view_name} AS 
                SELECT * FROM parquet_scan('{path}/**/*.parquet', 
                HIVE_PARTITIONING=TRUE, 
                UNION_BY_NAME=TRUE);
            """

def preprocess_req(dict req):
    """
        预处理请求整数日期转换为 DuckDB 标准 TIMESTAMP 字符串
    """
    cdef int start = req["start_date"]
    cdef int end = req["end_date"]
    
    cdef int s_y = start // 10000
    cdef int s_m = (start % 10000) // 100
    cdef int s_d = start % 100
    
    cdef int e_y = end // 10000
    cdef int e_m = (end % 10000) // 100
    cdef int e_d = end % 100
    
    cdef list sids = req["sid"]
    cdef list sid_parts = []

    for sid in sids:
        if isinstance(sid, bytes):
            # sid_parts.append(f"X'{sid.hex()}'")
            sid = sid.decode("utf-8")
        sid_parts.append(repr(sid))
            
    cdef str sid_str = f"({','.join(sid_parts)})"
    
    # 构造标准 ISO 时间字符串 (DuckDB 格式)
    cdef str start_str = f"{s_y:04d}-{s_m:02d}-{s_d:02d} 00:00:00"
    cdef str end_str = f"{e_y:04d}-{e_m:02d}-{e_d:02d} 23:59:59"
    
    return {
        "sid_str": sid_str,
        "start_str": start_str,
        "end_str": end_str
    }


def request_to_sql(list view_names, dict req_meta, str template):
    """
        构造统一 SQL 查询，用于 sid + datetime 范围过滤
    """
    cdef dict sql_meta = preprocess_req(req_meta)
    
    cdef str union_sql = "\nUNION ALL\n".join([f"SELECT * FROM {v}" for v in view_names])
    
    sql_meta["union_sql"] = union_sql
    
    # 使用 Python 字典映射填充模板
    return template.format_map(sql_meta)
