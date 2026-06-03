# TICK_TEMPLATE = """
#     SELECT 
#         CAST(sid AS VARCHAR)::BLOB as sid, 
#         tick, open, high, low, close, volume, amount
#     FROM read_parquet(?, hive_partitioning=true, hive_types={'sid': 'VARCHAR'})
#     WHERE sid IN (SELECT * FROM UNNEST(?))
#       AND datetime BETWEEN ?::TIMESTAMP AND ?::TIMESTAMP
#     ORDER BY sid, datetime ASC
# """

# # arg_max(close, datetime) 
# # strftime(datetime, '%Y%m%d')::INTEGER 
# CLOSE_TEMPLATE = """
#     SELECT 
#         CAST(sid AS VARCHAR)::BLOB as sid,
#         strftime(datetime, '%Y%m%d')::INTEGER as day,
#         arg_max(close, datetime) as close
#     FROM read_parquet(?, hive_partitioning=true, hive_types={'sid': 'VARCHAR'})
#     WHERE sid IN (SELECT * FROM UNNEST(?))
#       AND datetime BETWEEN ?::TIMESTAMP AND ?::TIMESTAMP
#       AND close IS NOT NULL 
#       AND close > 0
#     GROUP BY sid, strftime(datetime, '%Y%m%d')
#     ORDER BY sid, day ASC
# """

# DAILY_RET_TEMPLATE = """
#     WITH daily_close AS (
#         SELECT 
#             CAST(sid AS VARCHAR)::BLOB as sid,
#             strftime(datetime, '%Y%m%d')::INTEGER as day,
#             arg_max(close, datetime) as close
#         FROM read_parquet(?, hive_partitioning=true, hive_types={'sid': 'VARCHAR'})
#         WHERE sid IN (SELECT * FROM UNNEST(?))
#           AND datetime BETWEEN ?::TIMESTAMP AND ?::TIMESTAMP
#           AND close IS NOT NULL 
#           AND close > 0
#         GROUP BY sid, strftime(datetime, '%Y%m%d')
#     )
#     SELECT 
#         sid,
#         day,
#         close,
#         (close / LAG(close, 1) OVER (PARTITION BY sid ORDER BY day ASC)) - 1.0 AS daily_ret
#     FROM daily_close
#     ORDER BY sid, day ASC
# """

TICK_TEMPLATE = """
    SELECT 
        CAST(sid AS VARCHAR)::BLOB as sid, 
        tick, open, high, low, close, volume, amount
    FROM read_parquet(?, hive_partitioning=true, hive_types={'sid': 'VARCHAR'})
    WHERE sid IN (SELECT * FROM UNNEST(?))
      -- ? DuckDB TIMESTAMPTZ 
      -- AT TIME ZONE 'UTC'
      AND datetime BETWEEN (?::TIMESTAMPTZ AT TIME ZONE 'UTC') AND (?::TIMESTAMPTZ AT TIME ZONE 'UTC')
    ORDER BY sid, datetime ASC
"""

CLOSE_TEMPLATE = """
    SELECT 
        CAST(sid AS VARCHAR)::BLOB as sid,
        -- strftime(datetime + INTERVAL 8 HOUR, '%Y%m%d')::INTEGER as day,
        -- UTC -> TIMESTAMPTZ -> Shanghai Naive TIMESTAMP -> String
        strftime(
            (datetime AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Shanghai'), 
            '%Y%m%d'
        )::INTEGER as day,
        arg_max(close, datetime) as close
    FROM read_parquet(?, hive_partitioning=true, hive_types={'sid': 'VARCHAR'})
    WHERE sid IN (SELECT * FROM UNNEST(?))
      AND datetime BETWEEN (?::TIMESTAMPTZ AT TIME ZONE 'UTC') AND (?::TIMESTAMPTZ AT TIME ZONE 'UTC')
      AND close IS NOT NULL 
      AND close > 0
    GROUP BY 1, 2
    ORDER BY 1, 2 ASC
"""

DAILY_RET_TEMPLATE = """
    WITH daily_close AS (
        SELECT 
            CAST(sid AS VARCHAR)::BLOB as sid,
            -- strftime(datetime + INTERVAL 8 HOUR, '%Y%m%d')::INTEGER as day,
            -- UTC -> TIMESTAMPTZ -> Shanghai Naive TIMESTAMP -> String
            strftime(
                (datetime AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Shanghai'), 
                '%Y%m%d'
            )::INTEGER as day,
            arg_max(close, datetime) as close
        FROM read_parquet(?, hive_partitioning=true, hive_types={'sid': 'VARCHAR'})
        WHERE sid IN (SELECT * FROM UNNEST(?))
          AND datetime BETWEEN (?::TIMESTAMPTZ AT TIME ZONE 'UTC') AND (?::TIMESTAMPTZ AT TIME ZONE 'UTC')
          AND close IS NOT NULL 
          AND close > 0
        GROUP BY 1, 2
    )
    SELECT 
        sid,
        day,
        close,
        (close / LAG(close, 1) OVER (PARTITION BY sid ORDER BY day ASC)) - 1.0 AS daily_ret
    FROM daily_close
    ORDER BY sid, day ASC
"""
