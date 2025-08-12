import datetime
from typing import Tuple, List, TypeVar, Type
from pydantic import BaseModel

T = TypeVar('T', bound=BaseModel)


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

def preprocess_req(req: dict) -> dict:
    """
    Preprocess request to ensure it has the required fields.
    
    Args:
        req: Request dictionary containing 'sid', 'start_date', and 'end_date'.
        
    Returns:
        Processed request dictionary with 'sid' as a list.
    """
    sid_str = ','.join(repr(sid) for sid in req["sid"])
    sid_str = f"({sid_str})"

    start_time = datetime.datetime.fromtimestamp(req["start_date"])
    start_str = start_time.strftime('%Y-%m-%d %H:%M:%S')

    end_time = datetime.datetime.fromtimestamp(req["end_date"])
    end_str = end_time.strftime('%Y-%m-%d %H:%M:%S')
    return {
        "sid_str": sid_str,
        "start_str": start_str,
        "end_str": end_str
    }

def request_to_sql(view_names: list[str], req_meta: dict, template: str = None) -> str:
    """
    构造统一 SQL 查询，用于 sid + datetime 范围过滤
    """
    sql_meta = preprocess_req(req_meta)

    # 构建 UNION ALL SQL
    sqls = []
    for v in view_names:
        sqls.append(f"SELECT * FROM {v}")

    union_sql = "\nUNION ALL\n".join(sqls)
    sql_meta["union_sql"] = union_sql

    # # 构建最终 SQL
    # req_sql = f"""
    # SELECT sid, tick, open, high, low, close, volume, amount
    # FROM (
    #     {union_sql}
    # ) AS merged_view
    # WHERE sid IN {req_str['sid_str']}
    #   AND datetime BETWEEN TIMESTAMP '{req_str["start_str"]}' AND TIMESTAMP '{req_str["end_str"]}'
    # ORDER BY sid, datetime
    # """
    req_sql = template.format_map(sql_meta)
    return req_sql



def tuple_to_model(tuple_data: tuple, model_class: Type[T]) -> T:
    """
    将 SQL 查询返回的 tuple 转换为 Pydantic 对象
    
    Parameters
    ----------
    tuple_data : tuple
        SQL 查询返回的元组数据
    model_class : Type[T]
        Pydantic 模型类
        
    Returns
    -------
    T
        Pydantic 模型实例
        
    Examples
    --------
    >>> row = (1, "AAPL", 100)
    >>> model = tuple_to_model(row, StockModel)
    """
    # 获取模型字段名
    field_names = list(model_class.__annotations__.keys())
    # 创建字段名到值的映射
    data_dict = dict(zip(field_names, tuple_data))
    # 创建并返回模型实例
    return model_class(**data_dict)

