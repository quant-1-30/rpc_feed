#! /usr/bin/env python3
# -*- coding: utf-8 -*-

from rpc_feed.core.com.operator import duck_mgr


if __name__ == "__main__":
    # PyArrow 读取分区目录（如 Hive-style）时，会自动在结果中加入“分区列”，即使这些列并未存储在实际的 parquet 文件中
    req = {
        "sid": ["600225"],
        "start_date": 1572566400,
        "end_date": 1577836800,
    }
    df = duck_mgr._query(req)
    print('fetch df from duckdb', df)