#! /usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import argparse

from rpc_feed.core.middleware.operator import duck_mgr


if __name__ == "__main__":
    duck_mgr.register_view("stock", "~/Downloads/quant/parquet/stock")
    df = duck_mgr._query("SELECT * FROM stock WHERE datetime >= TIMESTAMP '2019-11-01 09:30:00' and datetime <= TIMESTAMP '2019-12-01 15:00:00'")
    print('fetch df from duckdb', df)