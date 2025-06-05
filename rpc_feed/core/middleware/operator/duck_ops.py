# !/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import duckdb
import asyncio
from typing import Optional
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

__all__ = ["duck_mgr"]


def register_parquet_views(conn: duckdb.DuckDBPyConnection, dataset_root: str):
    """
    自动扫描 root_path 下的所有子目录，并为每个子目录注册 DuckDB 视图。
    要求每个子目录都是一个独立的 Parquet Dataset 目录，支持 Hive 分区结构。
    """
    subdirs = [
        name for name in os.listdir(dataset_root)
        if os.path.isdir(os.path.join(dataset_root, name))
    ]

    conn.execute("INSTALL httpfs; LOAD httpfs; SET enable_object_cache=true;")

    for subdir in subdirs:
        dataset_path = os.path.join(dataset_root, subdir)
        # view_name = subdir.replace("-", "_").replace(".", "_")
        view_name = subdir
        query = f"""
            CREATE OR REPLACE VIEW {view_name} AS
            SELECT * FROM parquet_scan('{dataset_path}', HIVE_PARTITIONING=TRUE);
        """
        conn.execute(query)
        print(f"✅ Registered view: {view_name} -> {dataset_path}")


class DuckDBManager:
    """
        # 🔥 DuckDB = 用 SQL 操作 Parquet/Arrow, Polars = 用 DataFrame 操作 Parquet/Arrow, PyArrow = 连接两者的内存格式桥梁。
        # # 内存数据库（类似 SQLite 的 :memory:)
        # con = duckdb.connect(':memory:')
        # # 文件数据库（类似 SQLite 的文件路径）
        # con = duckdb.connect('my_database.db')
        # # 临时数据库（会话结束后自动删除）
        # con = duckdb.connect(':temp:')

        fetch_df --- zero copy execute().fetch_df() | to_df via relation (rel = duckdb.from_df(df) rel.fliter().t0_df())
        duckdb.register('people_view', df) and sql ( table is people_view)
    """

    def __init__(self):
        self.dataset_root = os.path.expanduser(os.getenv("DUCKDATASET"))
        self.db_path = os.getenv("DUCKDBPATH", ":memory:")
        self.conn = duckdb.connect(database=self.db_path)
        self.max_queue_size = int(os.getenv("DUCKQSIZE", 1000))
        self._tasks = set()
        self._init_duckdb()

    async def __aenter__(self):
        return self

    def _init_duckdb(self):
        # 启用远程文件访问模块（S3 / HTTP）
        # self.conn.execute(f"""
        #     INSTALL httpfs;
        #     LOAD httpfs;
        #     SET enable_object_cache=true;
        # """)
        self.conn.execute("INSTALL httpfs;")
        self.conn.execute("LOAD httpfs;")
        self.conn.execute("SET enable_object_cache=true;")  # 读取加速

    def register_view(self, view_name: str, dataset_path: str):
        # once register not need to use FROM parquet_scan('/data/parquet', hive_partitioning=1)
        dataset_path = Path(dataset_path).as_posix()  # 统一路径格式
        self.conn.execute(f"DROP VIEW IF EXISTS {view_name};")  # 重建
        self.conn.execute(f"""
            CREATE VIEW {view_name} AS 
            SELECT * FROM parquet_scan(
                '{dataset_path}/**/*.parquet',
                HIVE_PARTITIONING=TRUE
            );
        """)

    def _query(self, sql: str):
        return self.conn.execute(sql).fetchdf()
    
    def _query_stream(self, sql: str, batch_size: int = 1000):
        cursor = self.conn.execute(sql)
        while True:
            rows = cursor.fetchmany(batch_size)
            if not rows:
                break
            for row in rows:
                yield row  # 每次 yield 一行（或可改为 yield rows）
    
    # async def query(self, sql: str):
    #     # 在线程中执行同步 DuckDB 查询
    #     return await asyncio.to_thread(self._query, sql)
    
    async def query(self, sql: str, batch_size: int = 1000):
        loop = asyncio.get_running_loop()
        # avoid different query mixture
        queue = asyncio.Queue(maxsize=self.max_queue_size)

        def producer():
            try:
                for row in self._query_stream(sql, batch_size):
                    #loop.call_soon_threadsafe(sync_callback, *args) execute callback in loop / put_nowait cause memory pressure
                    # loop.call_soon_threadsafe(queue.put, row)
                    # if callback is async, use asyncio.to_thread(callback) 在非事件循环线程中安全地调度执行协程（async def 函数)
                    asyncio.run_coroutine_threadsafe(queue.put(row), loop).result()
            finally:
                # loop.call_soon_threadsafe(queue.put, None)  # 结束标记
                asyncio.run_coroutine_threadsafe(queue.put(None), loop).result()

        task = asyncio.to_thread(producer)
        self._tasks.add(task)

        # Optional: 清理已完成的任务
        def _done(_):
            self._tasks.discard(task)
        task.add_done_callback(_done)

        # 在后台线程启动同步查询并塞入队列
        asyncio.to_thread(producer)
        while True:
            row = await queue.get()
            if row is None:
                break
            yield row

    async def __aexit__(self, exc_type, exc_value, traceback):
        # 存在问题 duckdb connection is sync func and close will cause InvalidInputException error
        self.close()

    async def close(self):
        if self._tasks:
            # Return a future aggregating results from the given coroutines/futures
            await asyncio.gather(*self._tasks, return_exceptions=True)   # ✅ 等待所有后台线程任务完成
        self.conn.close()


duck_mgr = DuckDBManager()
