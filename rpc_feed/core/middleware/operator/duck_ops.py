# !/usr/bin/env python3
# -*- coding: utf-8 -*-

import re
import os
import json
import duckdb
import asyncio
from pathlib import Path
from dotenv import load_dotenv
from threading import Lock
from rpc_feed.utils.duck_utils import schema_range, request_to_sql, create_parquet_macro
from rpc_feed.core.filter import _filters

__all__ = ["duck_mgr"]



class DuckDBManager:
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

    def __init__(self):
        load_dotenv()
        self.dataset_root = os.path.expanduser(os.getenv("DUCKDATASET"))
        self.db_path = os.getenv("DUCKDBPATH", ":memory:")
        self.conn = duckdb.connect(database=self.db_path) # 不是线程安全，多线程会存在问题
        self.max_queue_size = int(os.getenv("DUCKQSIZE", 1000))
        self.cache_file = os.getenv("DUCKCACHE", "registered_views.json")
        self.max_workers = int(os.getenv("DUCKWORKERS", os.cpu_count() * 2))
        self._tasks = set()
        self._view_lock = Lock()  # 线程安全
        self._registered_views = set()  # 缓存已注册的视图
        self.regex = "^(6|0|3)\d{5}(?:)" # 判断stock / fund 目录
        self._init_duckdb()

    async def __aenter__(self):
        return self

    def _init_duckdb(self):
        # 启用远程文件访问模块（S3 / HTTP）
        self.conn.execute("INSTALL httpfs;")
        self.conn.execute("LOAD httpfs;")
        self.conn.execute("SET enable_object_cache=true;")  # 读取加速
        # self.conn.execute("INSTALL httpfs; LOAD httpfs; SET enable_object_cache=time;")
        
    def _load_cache(self):
        if Path(self.cache_file).exists():
            try:
                with open(self.cache_file, "r") as f:
                    cached = json.load(f)
                    self._registered_views.update(cached)
                print(f"💾 Loaded cached registered views: {len(self._registered_views)}")
            except Exception as e:
                print(f"⚠️ Failed to load cache file: {e}")

    def _save_cache(self):
        try:
            with open(self.cache_file, "w") as f:
                json.dump(list(self._registered_views), f, indent=2)
            print(f"💾 Saved cache file: {self.cache_file}")
        except Exception as e:
            print(f"⚠️ Failed to save cache file: {e}")
    
    def get_view_names(self) -> list[str]:
        """获取所有已注册的视图名称"""
        return list(self._registered_views)

    def view_exists(self, view_name: str) -> bool:
        """检查视图是否存在"""
        try:
            result = self.conn.execute(f"SELECT 1 FROM information_schema.views WHERE table_name = '{view_name}'").fetchone()
            return result is not None
        except:
            return False

    def _view_name(self, sid: str, year: int, quarter: str, y_month: str) -> str:
        return f"year{year}_quarter{quarter}_sid{sid}_date{y_month}"

    def _glob_path(self, sid: str, year: int, quarter: str, y_month: str) -> Path:
        category = "stock" if re.match(self.regex, sid) else "fund"
        return os.path.join(self.dataset_root, category, f"year={year}", f"quarter={quarter}", f"sid={sid}", f"date={y_month}")

    def _query(self, req: dict):
        req_views = self.register_views(req)
        req_sql = request_to_sql(req_views, req)
        print("req_sql", req_sql)
        cursor = self.conn.execute(req_sql)
        return cursor.fetchdf()

    def _register_macro_if_needed(self, sid: str, year: int, quarter: str, y_month: str):
        view_name = self._view_name(sid, year, quarter, y_month)
        with self._view_lock:
            if view_name in self._registered_views:
                return
            path = self._glob_path(sid, year, quarter, y_month)
            view_sql = create_parquet_macro(path, view_name)
            self._registered_views.add(view_name)
            print(f"⚙️ Registering: {view_name} → {path}")
            self.conn.execute(view_sql)
        return view_name
        
    def register_views(self, req: dict):
        req_views = []
        ranges = schema_range(req)
        for sid in req["sid"]:
            for _r in ranges:
                view_name = self._register_macro_if_needed(sid, *_r)
                req_views.append(view_name)
        return req_views
    
    def _query_stream(self, req: dict, batch_size: int = 1000):
        req_views = self.register_views(req)
        req_sql = request_to_sql(req_views, req)
        print("req_sql", req_sql)
        cursor = self.conn.execute(req_sql)
        while True:
            rows = cursor.fetchmany(batch_size)
            if not rows:
                break
            for row in rows:
                yield row  # 每次 yield 一行（或可改为 yield rows）
    
    async def query(self, req: dict, batch_size: int = 1000):
        loop = asyncio.get_running_loop()
        queue = asyncio.Queue(maxsize=self.max_queue_size) # 每个请求单独的queue

        def producer():
            try:
                for row in self._query_stream(req, batch_size):
                    #loop.call_soon_threadsafe(sync_callback, *args) execute callback in loop / put_nowait cause memory pressure
                    # loop.call_soon_threadsafe(queue.put, row)
                    # if callback is async, use asyncio.to_thread(callback) 
                    # 在非事件循环线程中安全地调度执行协程（async def 函数)
                    fut = asyncio.run_coroutine_threadsafe(queue.put(row), loop)
                    fut.add_done_callback(lambda f: f.exception())  # 异常处理
            finally:
                asyncio.run_coroutine_threadsafe(queue.put(None), loop)

        task = asyncio.create_task(asyncio.to_thread(producer)) # daemon execute
        self._tasks.add(task)
        task.add_done_callback(lambda _: self._tasks.discard(task))

        while True:
            row = await queue.get()
            if row is None:
                break
            yield row

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        # 等待所有后台任务完成
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        # 关闭数据库连接
        self.conn.close()


duck_mgr = DuckDBManager()
