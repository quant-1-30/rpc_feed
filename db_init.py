#! /usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import asyncio
import psycopg


async def create_database():
    
    db_host = os.getenv("POSTGRES_HOST", "localhost")
    db_port = os.getenv("POSTGRES_PORT", "5432")
    db_user = os.environ.get("POSTGRES_USER")
    db_pass = os.environ.get("POSTGRES_PASSWORD")
    target_db = os.environ.get("POSTGRES_DB")

    # 连接默认数据库 postgres
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


if __name__ == "__main__":

    asyncio.run(create_database())
