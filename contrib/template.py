tick_template = """
    SELECT CAST(sid AS VARCHAR)::BLOB as sid, tick, open, high, low, close, volume, amount
    FROM (
        {union_sql}
    ) AS merged_view
    WHERE sid IN {sid_str}
      AND datetime BETWEEN TIMESTAMP '{start_str}' AND TIMESTAMP '{end_str}'
    ORDER BY sid, datetime ASC 
    """

close_template = """
        WITH daily_max_datetime AS (
            SELECT
                sid,
                DATE(datetime) AS day,
                MAX(datetime) AS max_datetime
            FROM  (
            {union_sql}
            ) as merged
            WHERE datetime BETWEEN 
                TIMESTAMP '{start_str}'
                AND
                TIMESTAMP '{end_str}' 
              AND sid in {sid_str}
              AND volume >= 0
            GROUP BY sid, day
        )

        SELECT
            CAST(t.sid AS VARCHAR)::BLOB as sid, -- t.sid::BLOB as sid,
            -- d.day::INTEGER as day not supported  instead of EXTRACT  
            (EXTRACT(YEAR FROM d.day) * 10000 + 
            EXTRACT(MONTH FROM d.day) * 100 + 
            EXTRACT(DAY FROM d.day))::INTEGER as day, 
            t.close
        FROM
          ({union_sql}
           ) t
        JOIN daily_max_datetime d
            ON t.sid = d.sid AND t.datetime = d.max_datetime
        WHERE t.datetime BETWEEN 
            TIMESTAMP '{start_str}'
            AND
            TIMESTAMP '{end_str}' 
          AND t.sid in {sid_str}
          AND t.close IS NOT NULL  
          AND t.close > 0 
        ORDER BY t.sid, d.day ASC;
    """


DAILY_RET_TEMPLATE = """
    WITH daily_close AS (
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
    )
    SELECT 
        sid,
        day,
        close,
        ln(close / LAG(close, 1) OVER (PARTITION BY sid ORDER BY day ASC)) AS log_ret
    FROM daily_close
    ORDER BY sid, day ASC
"""


EXPERIMENT_TEMPLATE = """
    SELECT 
        CAST(sid AS VARCHAR)::BLOB as sid,
        distance, beta, macro_state, pred_score, prior_matrix
    FROM read_parquet(?, hive_partitioning=true)
    WHERE date BETWEEN ? AND ?
    ORDER date ASC
"""
