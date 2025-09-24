#! /usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import asyncio
import psycopg

from dotenv import load_dotenv


async def create_database():
    
    pg_db = os.environ.get("PGDB")

    # 连接默认数据库 postgres
    conn_url = f'postgresql://{os.getenv("PGUSER")}:{os.getenv("PGPWD")}@{os.getenv("PGHOST")}:{os.getenv("PGPORT")}/postgres'

    async with await psycopg.AsyncConnection.connect(conn_url) as conn:
        # pg create database under autocommit not a transaction block
        await conn.set_autocommit(True)

        async with conn.cursor() as cur:
            await cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (pg_db,))
            exists = await cur.fetchone()
            if not exists:
                print(f"Creating database '{pg_db}'...")
                await cur.execute(f'CREATE DATABASE "{pg_db}"')
            else:
                print(f"Database '{pg_db}' already exists.")


if __name__ == "__main__":

    load_dotenv()
    asyncio.run(create_database())
