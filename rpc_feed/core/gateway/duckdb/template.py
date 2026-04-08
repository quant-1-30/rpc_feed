TICK_TEMPLATE = """
    SELECT 
        CAST(sid AS VARCHAR)::BLOB as sid, 
        tick, open, high, low, close, volume, amount
    FROM read_parquet(?, hive_partitioning=true, hive_types={'sid': 'VARCHAR'})
    WHERE sid IN (SELECT * FROM UNNEST(?))
      AND datetime BETWEEN ?::TIMESTAMP AND ?::TIMESTAMP
    ORDER BY sid, datetime ASC
"""

# arg_max(close, datetime) 
# strftime(datetime, '%Y%m%d')::INTEGER 
CLOSE_TEMPLATE = """
    SELECT 
        CAST(sid AS VARCHAR)::BLOB as sid,
        strftime(datetime, '%Y%m%d')::INTEGER as day,
        arg_max(close, datetime) as close
    FROM read_parquet(?, hive_partitioning=true, hive_types={'sid': 'VARCHAR'})
    WHERE sid IN (SELECT * FROM UNNEST(?))
      AND datetime BETWEEN ?::TIMESTAMP AND ?::TIMESTAMP
      AND close IS NOT NULL 
      AND close > 0
    GROUP BY sid, strftime(datetime, '%Y%m%d')
    ORDER BY sid, day ASC
"""
