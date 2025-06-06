#! /usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import argparse

from rpc_feed.core.middleware.operator import duck_mgr


if __name__ == "__main__":
    # PyArrow 读取分区目录（如 Hive-style）时，会自动在结果中加入“分区列”，即使这些列并未存储在实际的 parquet 文件中
    df = duck_mgr._query("SELECT * FROM stock WHERE sid in ('600225') and datetime >= TIMESTAMP '2019-11-01 09:30:00' and datetime <= TIMESTAMP '2019-12-01 15:00:00'")
    print('fetch df from duckdb', df.columns, df.head())
    import pdb; pdb.set_trace()