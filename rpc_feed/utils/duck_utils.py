import datetime
from rpc_feed.core.datasets.model import Request


def request_to_sql(req: Request, tz="Asia/Shanghai"):
    """
    request to sql
    """
    sid_str = ','.join(repr(sid) for sid in req.sid)
    sid_str = f"({sid_str})"
    start_time = datetime.datetime.fromtimestamp(req.start_date)
    start_str = start_time.strftime('%Y-%m-%d %H:%M:%S')
    end_time = datetime.datetime.fromtimestamp(req.end_date)
    end_str = end_time.strftime('%Y-%m-%d %H:%M:%S')
    sql = f"""
    SELECT *
    FROM stock
    WHERE sid IN {sid_str}
      AND datetime BETWEEN TIMESTAMP '{start_str}' AND TIMESTAMP '{end_str}'
    ORDER BY sid, datetime
    """
    return sql