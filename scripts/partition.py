
#! /usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import asyncio
import psycopg
from sqlalchemy import text
from dateutil.relativedelta import relativedelta
from rpc_feed.utils.dt_utilty import ensure_utc
from rpc_feed.core.helper.operator import async_ops


async def initialize_database():
    
    db_host = os.getenv("POSTGRES_HOST", "localhost")
    db_port = os.getenv("POSTGRES_PORT", "5432")
    db_user = os.environ.get("POSTGRES_USER", "postgres")
    db_pass = os.environ.get("POSTGRES_PASSWORD", "20210718")
    target_db = os.environ.get("POSTGRES_DB", "bt_feed")

    conn_url = f"postgresql://{db_user}:{db_pass}@{db_host}:{db_port}/postgres"

    async with await psycopg.AsyncConnection.connect(conn_url) as conn:
        # pg create database under autocommit not a transaction block
        await conn.set_autocommit(True)

        async with conn.cursor() as cur:
            await cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (target_db,))
            exists = await cur.fetchone()
            if not exists:
                print(f"Creating database '{target_db}'...")
                await cur.execute(f'CREATE DATABASE "{target_db}"')
            else:
                print(f"Database '{target_db}' already exists.")

    # initialize the async_ops context with the new database
    async with async_ops as ctx:
        pass 


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
    await initialize_database()
    await create_partitions_by_quarter(*intervals)


if __name__ == "__main__":

    # create database / initialize orm tables / create partitions
    intervals = ("2004-01-01", "2035-01-01")
    asyncio.run(sequential_execute(intervals))

