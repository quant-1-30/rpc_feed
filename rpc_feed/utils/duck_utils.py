import datetime
from typing import Tuple, List


def schema_range(req: dict) -> List[Tuple[int, str, int]]:
    """Return (year, quarter, month) list between start and end inclusive."""
    start_time = datetime.datetime.fromtimestamp(req["start_date"])
    end_time = datetime.datetime.fromtimestamp(req["end_date"])

    y, m = start_time.year, start_time.month
    ey, em = end_time.year, end_time.month
    result = []
    while y < ey or (y == ey and m <= em):
        q = f"Q{((m - 1) // 3) + 1}"
        result.append((y, q, f"{y}{m:02d}"))
        if m == 12:
            y += 1
            m = 1
        else:
            m += 1
    return result


def create_parquet_macro(path: str, view_name: str) -> str:
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
    # # UNION_BY_NAME=TRUE —— 确保了你的股票数据系统的数据完整性和正确性 避免了schema变化
    # not supported macro
    return f"""CREATE OR REPLACE VIEW {view_name} AS 
    SELECT * FROM parquet_scan('{path}/**/*.parquet', 
    HIVE_PARTITIONING=TRUE, 
    UNION_BY_NAME=TRUE);"""


def request_to_sql(view_names: list[str], req: dict, tz="Asia/Shanghai"):
    """
    构造统一 SQL 查询，用于 sid + datetime 范围过滤
    """
    sid_str = ','.join(repr(sid) for sid in req["sid"])
    sid_str = f"({sid_str})"

    start_time = datetime.datetime.fromtimestamp(req["start_date"])
    start_str = start_time.strftime('%Y-%m-%d %H:%M:%S')

    end_time = datetime.datetime.fromtimestamp(req["end_date"])
    end_str = end_time.strftime('%Y-%m-%d %H:%M:%S')

    # 构建 UNION ALL SQL
    sqls = []
    for v in view_names:
        sqls.append(f"SELECT * FROM {v}")

    union_sql = "\nUNION ALL\n".join(sqls)

    # 构建最终 SQL
    req_sql = f"""
    SELECT sid, tick, open, high, low, close, volume, amount
    FROM (
        {union_sql}
    ) AS merged_view
    WHERE sid IN {sid_str}
      AND datetime BETWEEN TIMESTAMP '{start_str}' AND TIMESTAMP '{end_str}'
    ORDER BY sid, datetime
    """

    return req_sql
