
#! /usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import asyncio
import psycopg
from dotenv import load_dotenv
from sqlalchemy import text
from dateutil.relativedelta import relativedelta
from rpc_feed.utils.dt_utilty import ensure_utc
from rpc_feed.core.operator import async_ops
from .pg_init import create_database


async def create_partitions_by_quarter(start: str, end: str):
    utc_start = ensure_utc(start, fmt="%Y-%m-%d")
    utc_end = ensure_utc(end, fmt="%Y-%m-%d")

    async with async_ops as ctx:
        while utc_start < utc_end:
            q_start = utc_start.replace(day=1)
            q_end = q_start + relativedelta(months=3)
            table_suffix = f"{q_start.year}q{(q_start.month - 1)//3 + 1}"
            table_name = f"tick_{table_suffix}"

            sql = f"""
            CREATE TABLE IF NOT EXISTS {table_name}
            PARTITION OF tick
            FOR VALUES FROM ({q_start.timestamp()}) TO ({q_end.timestamp()});
            """
            await ctx.on_execute(text(sql))
            print(f"✅ Created partition: {table_name}")
            utc_start = q_end


async def sequential_execute(intervals):
    await create_database()
    await create_partitions_by_quarter(*intervals)


if __name__ == "__main__":
    load_dotenv()
    # create database / initialize orm tables / create partitions
    intervals = ("2004-01-01", "2035-01-01")
    asyncio.run(sequential_execute(intervals))

