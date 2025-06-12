# !/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import json
import duckdb
import asyncio
import warnings
from typing import Optional, Union, Any
from pathlib import Path
from dotenv import load_dotenv
import concurrent.futures
from threading import Lock
import time
from rpc_feed.utils.io import get_subdirs


__all__ = ["duck_mgr"]


def _normalize_view_name(raw: str) -> str:
    return raw.replace("=", "_").replace("-", "_").replace(".", "_")


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
        self._init_duckdb()

    async def __aenter__(self):
        return self

    def _init_duckdb(self):
        # 启用远程文件访问模块（S3 / HTTP）
        self.conn.execute("INSTALL httpfs;")
        self.conn.execute("LOAD httpfs;")
        self.conn.execute("SET enable_object_cache=true;")  # 读取加速
        # self.conn.execute("INSTALL httpfs; LOAD httpfs; SET enable_object_cache=time;")
        
        # 根据环境变量选择注册方式
        use_lazy_macros = os.getenv("DUCK_VIEW_LAZY", "true").lower() == "true"

        if use_lazy_macros:
            # 🚀 使用最快的懒加载方式 (推荐)
            print("🚀 Using lazy MACRO registration for maximum speed...")
            # query = f"""
            #     CREATE OR REPLACE MACRO {view_name}() AS TABLE parquet_scan('{dataset_path}/**/*.parquet', 
            #                                                                HIVE_PARTITIONING=TRUE, 
            #                                                                UNION_BY_NAME=TRUE);
            # """
            warnings.warn("lazy macros as table is not supported and view instead")
            self.register_parquet_views()
        else:
            # 🔄 使用并行 VIEW 注册 (兼容传统查询语法)
            print("🔄 Using parallel VIEW registration for compatibility...")
            self.register_parquet_views()
     
    def register_parquet_views(self):
        """
        增强版注册视图功能：
        - 线程隔离 duckdb connection
        - 并发上限控制
        - 注册进度可视化
        - 注册缓存文件（避免重复注册）
        """
    
        success_count = 0
        error_count = 0
    
        subdirs = get_subdirs(self.dataset_root)
        if not subdirs:
            print("⚠️  No dataset subdirectories found")
            return
    
        # 尝试加载已注册缓存
        cache_path = Path(self.cache_file)
        if cache_path.exists():
            try:
                with open(cache_path, "r") as f:
                    cached = json.load(f)
                    self._registered_views.update(cached) # set() update inplace
            except Exception as e:
                print(f"⚠️  Failed to load cache file: {e}")
    
        # 过滤已注册
        pending_subdirs = [d for d in subdirs if d not in self._registered_views]
        if not pending_subdirs:
            print("✅ All views are already registered (from cache).")
            return
    
        print(f"🔄 Registering {len(pending_subdirs)} parquet views with {self.max_workers} workers...")
    
        try:
            with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                future_to_subdir = {
                    executor.submit(self._register_single_view, subdir): subdir
                    for subdir in pending_subdirs
                }
    
                for future in concurrent.futures.as_completed(future_to_subdir):
                    view_name, success, msg = future.result()
                    if success:
                        success_count += 1
                        self._registered_views.add(view_name)
                    else:
                        error_count += 1
                        print(f"❌ Failed to register view {view_name}: {msg}")
            # 写入缓存
            try:
                with open(self.cache_file, "w") as f:
                    json.dump(list(self._registered_views), f, indent=2)
                print(f"💾 Cache updated: {self.cache_file}")
            except Exception as e:
                print(f"⚠️  Failed to write cache file: {e}")
    
            print(f"🎉 Registration complete: {success_count} success, {error_count} errors")
    
        except Exception as e:
            print(f"❌ Error during parallel view registration: {e}")

    def _register_single_view(self, subdir: str) -> tuple[str, bool, str]:
        """注册单个视图，返回 (view_name, success, error_msg)"""
        try:
            dataset_path = os.path.join(self.dataset_root, subdir)
            view_name = _normalize_view_name(subdir)
            
            conn = duckdb.connect(self.db_path) # 使用独立连接，避免多线程争用同一个 conn
            # 检查是否已经注册过
            if view_name in self._registered_views:
                return view_name, True, "already registered"
            
            # UNION_BY_NAME=TRUE —— 确保了你的股票数据系统的数据完整性和正确性 避免了schema变化
            query = f"""
                CREATE OR REPLACE VIEW {view_name} AS
                SELECT * FROM parquet_scan('{dataset_path}/**/*.parquet', 
                                         HIVE_PARTITIONING=TRUE, 
                                         UNION_BY_NAME=TRUE);
            """
            conn.execute(query)
            conn.close()
            return view_name, True, f"-> {dataset_path}"
            
        except Exception as e:
            return view_name, False, str(e)
    
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

    # def _query(self, sql: str):
    #     return self.conn.execute(sql).fetchdf()
    
    def _query_stream(self, sql: str, batch_size: int = 1000):
        cursor = self.conn.execute(sql)
        while True:
            rows = cursor.fetchmany(batch_size)
            if not rows:
                break
            for row in rows:
                yield row  # 每次 yield 一行（或可改为 yield rows）
    
    async def query(self, sql: str, batch_size: int = 1000):
        loop = asyncio.get_running_loop()
        queue = asyncio.Queue(maxsize=self.max_queue_size) # 每个请求单独的queue

        def producer():
            try:
                for row in self._query_stream(sql, batch_size):
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
