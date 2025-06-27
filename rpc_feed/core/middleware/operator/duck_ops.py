# !/usr/bin/env python3
# -*- coding: utf-8 -*-

import re
import os
import json
import duckdb
import asyncio
import threading
from pathlib import Path
from dotenv import load_dotenv
from threading import Lock
from rpc_feed.utils.duck_utils import schema_range, request_to_sql, create_parquet_macro
from rpc_feed.core.filter import _filters


__all__ = ["duck_mgr"]


class DuckDBManager: # 非线程安全

    def __init__(self):
        """
            # 🔥 DuckDB = 用 SQL 操作 Parquet/Arrow, Polars = 用 DataFrame 操作 Parquet/Arrow, PyArrow = 连接两者的内存格式桥梁。
            # # 内存数据库（类似 SQLite 的 :memory:)
            # con = duckdb.connect(':memory:')
            # # 文件数据库（类似 SQLite 的文件路径）
            # con = duckdb.connect('my_database.db')
            # # 临时数据库（会话结束后自动删除）
            # con = duckdb.connect(':temp:')
            增强版注册视图功能：
            - 线程隔离 duckdb connection
            - 并发上限控制
            - 注册进度可视化
            - 注册缓存文件（避免重复注册）

            fetch_df --- zero copy execute().fetch_df() | to_df via relation (rel = duckdb.from_df(df) rel.fliter().t0_df())
            duckdb.register('people_view', df) and sql ( table is people_view)
        """
        load_dotenv()
        self.dataset_root = os.path.expanduser(os.getenv("DUCKDATASET"))
        self.db_path = os.getenv("DUCKDBPATH", "duckdb_macro.db")  # DuckDB 的 宏（macro） 是注册在当前连接（connection）上的，且存在于数据库的 catalog 

        self.max_queue_size = int(os.getenv("DUCKQSIZE", 1000))
        self.cache_file = os.getenv("DUCKCACHE", "registered_views.json")
        self.regex = r"^(6|0|3)\d{5}"  # 匹配 stock/fund 代码
        # self._view_lock = Lock()  # 宏注册锁
        self._thread_local = threading.local()
        self._tasks = set()
        self._registered_views = set()
        self._load_cache()

    async def __aenter__(self):
        return self

    def _init_duckdb(self, conn):
        # 持久化数据库连接，主要做模块安装，宏注册后可跨连接访问
        conn.execute("INSTALL httpfs;")
        conn.execute("LOAD httpfs;")
        conn.execute("SET enable_object_cache=true;")

    def _get_conn(self):
        if not hasattr(self._thread_local, "conn"):
            conn = duckdb.connect(self.db_path)
            self._init_duckdb(conn)
            self._thread_local.conn = conn
        return self._thread_local.conn

    def _load_cache(self):
        if Path(self.cache_file).exists():
            try:
                with open(self.cache_file, "r") as f:
                    cached = json.load(f)
                    self._registered_views.update(cached)
                    print(f"💾 Loaded cached registered views: {len(cached)}")
                    return cached
            except Exception as e:
                print(f"⚠️ Failed to load cache file: {e}")

    def _save_cache(self):
        try:
            with open(self.cache_file, "w") as f:
                json.dump(list(self._registered_views), f, indent=2)
            print(f"💾 Saved cache file: {self.cache_file}")
        except Exception as e:
            print(f"⚠️ Failed to save cache file: {e}")

    def _view_name(self, sid: str, year: int, quarter: str, y_month: str) -> str:
        return f"year{year}_quarter{quarter}_sid{sid}_date{y_month}"

    def _glob_path(self, sid: str, year: int, quarter: str, y_month: str) -> str:
        category = "stock" if re.match(self.regex, sid) else "fund"
        return os.path.join(self.dataset_root, category, f"year={year}", f"quarter={quarter}", f"sid={sid}", f"date={y_month}")

    def _register_macro_if_needed(self, conn, sid: str, year: int, quarter: str, y_month: str):
        view_name = self._view_name(sid, year, quarter, y_month)

        if view_name in self._registered_views:
            return view_name

        path = self._glob_path(sid, year, quarter, y_month)
        if not Path(path).exists():
            print(f"⚠️ Skipping: dataset path not found: {path}")
            return None # 避免注册失败

        macro_sql = create_parquet_macro(path, view_name)
        try:
            conn.execute(macro_sql)
            print(f"⚙️ Registered macro: {view_name} → {path}")
            self._registered_views.add(view_name)
            self._save_cache()
        except Exception as e:
            print(f"⚠️ Failed to register macro: {e}")
            # conn.close() # will cause error next request
        return view_name

    def register_views(self, conn, req: dict):
        req_views = []
        ranges = schema_range(req)
        for sid in req["sid"]:
            for r in ranges:
                view_name = self._register_macro_if_needed(conn, sid, *r)
                if view_name:
                    req_views.append(view_name)
        return req_views
    
    def _query(self, req: dict):
        conn = self._get_conn()
        req_views = self.register_views(conn, req)
        if not req_views:
            return duckdb.from_df([])

        req_sql = request_to_sql(req_views, req)
        print("req_sql", req_sql)
        cursor = conn.execute(req_sql)
        return cursor.fetchdf()

    def _query_stream(self, req: dict, batch_size: int = 1000): # register and query in separate connection or conflict
        conn = self._get_conn()
        req_views = self.register_views(conn, req)
        if not req_views:
            return duckdb.from_df([])
        req_sql = request_to_sql(req_views, req)
        print("Executing SQL:", req_sql)

        query_conn = duckdb.connect(self.db_path) # 每次查询都用新的连接，避免多线程竞争
        try:
            cursor = query_conn.execute(req_sql)
            while True:
                rows = cursor.fetchmany(batch_size)
                if not rows:
                    break
                for row in rows:
                    yield row
        finally:
            query_conn.close()

    async def query(self, req: dict, batch_size: int = 1000):
        loop = asyncio.get_running_loop()
        queue = asyncio.Queue(maxsize=self.max_queue_size)

        def producer():
            try:
                for row in self._query_stream(req, batch_size):
                    fut = asyncio.run_coroutine_threadsafe(queue.put(row), loop)
                    fut.add_done_callback(lambda f: f.exception())
            finally:
                asyncio.run_coroutine_threadsafe(queue.put(None), loop)

        task = asyncio.create_task(asyncio.to_thread(producer))
        self._tasks.add(task)
        task.add_done_callback(lambda _: self._tasks.discard(task))

        try:
            while True:
                row = await queue.get()
                if row is None:
                    break
                yield row
        finally:
            task.cancel()

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        # 等待所有后台任务完成
        self._save_cache()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)

# 全局单例
duck_mgr = DuckDBManager()
