tick_template = """
    SELECT sid::BLOB as sid, tick, open, high, low, close, volume, amount
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
            t.sid::BLOB as sid,
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
        ORDER BY t.sid, d.day ASC;
    """
