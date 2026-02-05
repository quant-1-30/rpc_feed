# !/usr/bin/env python3
# -*- coding: utf-8 -*-

import re
import os
import duckdb
import threading
import json
import asyncio
import queue
from pathlib import Path
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor
from .utils import schema_range, request_to_sql, create_parquet_macro
from rpc_feed.utils.wrapper import singleton


class ConnectionPool:
    def __init__(self, db_path, max_connections=10):
        self.db_path = db_path
        self.max_connections = max_connections
        self._pool = queue.Queue(max_connections)
        self._lock = threading.Lock()
        self._initialize_pool()
    
    def _initialize_pool(self):
        """初始化连接池"""
        for _ in range(self.max_connections):
            conn = duckdb.connect(self.db_path)
            self._init_connection(conn)
            self._pool.put(conn)
    
    def _init_connection(self, conn):
        """初始化单个连接"""
        conn.execute("INSTALL httpfs;")
        conn.execute("LOAD httpfs;")
        conn.execute("SET enable_object_cache=true;")
    
    def get_connection(self, timeout=5):
        """从池中获取连接"""
        try:
            return self._pool.get(timeout=timeout)
        except queue.Empty:
            raise Exception("Connection pool exhausted")
    
    def return_connection(self, conn):
        """归还连接到池中"""
        try:
            self._pool.put(conn, timeout=1)
        except:
            # 如果池已满，关闭连接
            conn.close()
    
    def close_all(self):
        """关闭所有连接"""
        while not self._pool.empty():
            try:
                conn = self._pool.get_nowait()
                conn.close()
            except queue.Empty:
                break


# global vars
_duck_inst = None
_duck_lock = threading.Lock()


@singleton
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

        # DuckDB macro register on connection database catalog 
        cache_path = Path(__file__).resolve().parent / "cache"
        self.db_path = cache_path / os.getenv("DUCKDB") # joinpath
        self.view_cache_file = cache_path / os.getenv("DUCKVIEW")
        self.dataset_root = Path(os.getenv("DUCKDATASET")).expanduser()
        self.batch_size = int(os.getenv("DUCKBATCHSIZE"))

        self.regex = r"^(6|0|3)\d{5}"  # stock/fund 
        self._bin_regex = re.compile(self.regex.encode('ascii'))
        self._cache_modified = False
        self._tasks = set()
        self._registered_views = set()

        self._thread_local = threading.local()
        self.db_query_lock = threading.Lock()
        self.connection_pool = ConnectionPool(self.db_path, max_connections=20)

    def _load_cache(self):
        if Path(self.view_cache_file).exists():
            try:
                with open(self.view_cache_file, "r") as f:
                    cached = json.load(f)
                    self._registered_views.update(cached)
                    # print(f"💾 Loaded cached registered views: {len(cached)}")
                    return cached
            except Exception as e:
                print(f"⚠️ Failed to load cache file: {e}")

    async def __aenter__(self):
        self._load_cache()
        return self
    
    def _get_conn(self):
        """获取连接（线程安全）"""
        return self.connection_pool.get_connection()
    
    def _release_conn(self, conn):
        """释放连接"""
        self.connection_pool.return_connection(conn)

    def _view_name(self, sid: str, year: int, quarter: str, y_month: str) -> str:
        return f"year{year}_quarter{quarter}_sid{sid}_date{y_month}"

    def _glob_path(self, sid: str, year: int, quarter: str, y_month: str) -> str:
        sub_dir = "stock" if re.match(self.regex, sid) else "fund" # re.match(self._bin_regex, sid)
        return os.path.join(self.dataset_root, sub_dir, f"year={year}", f"quarter={quarter}", f"sid={sid}", f"date={y_month}")

    def _register_macro_if_needed(self, conn, sid: str, year: int, quarter: str, y_month: str):
        """
         Registers a DuckDB macro for a specific dataset path if it doesn't already exist in the database.
         Handles cases where the database file might have been reset or deleted.
        """
        view_name = self._view_name(sid, year, quarter, y_month)

        if view_name in self._registered_views:
            # print(f"⚙️ Macro already in cache: {view_name}")
            return view_name

        path = self._glob_path(sid, year, quarter, y_month)
        if not Path(path).exists():
            print(f"⚠️ Skipping: dataset path not found: {path}")
            return None # 避免注册失败

        macro_sql = create_parquet_macro(path, view_name)
        try:
            conn.execute(macro_sql) # write into macro.db
            print(f"⚙️ Registered macro: {view_name} → {path}")
            if view_name not in self._registered_views:
                self._registered_views.add(view_name)
                self._cache_modified =True
        except duckdb.CatalogException as e: # 数据库目录问题
            # if "Macro with name" in str(e) and "already exists" in str(e):
            if "already exists" in str(e):
                # Macro already exists, ensure it's in our local set
                if view_name not in self._registered_views:
                    self._registered_views.add(view_name)
                    self._cache_modified =True
            else:
                raise e
        except Exception as e:
            print(f"⚠️ Failed to register macro: {e}")
            # conn.close() # will cause error next request
        return view_name

    def register_views(self, conn, req: dict):
        req_views = []
        ranges = schema_range(req)
        for sid in req["sid"]:
            sid_str = sid.decode("utf-8")
            for r in ranges:
                view_name = self._register_macro_if_needed(conn, sid_str, *r)
                if view_name:
                    req_views.append(view_name)
        return req_views

    async def query(self, req_meta: dict, raw_template: str):
        conn = self._get_conn()
        try:
            req_views = self.register_views(conn, req_meta)
            if not req_views: return

            req_sql = request_to_sql(req_views, req_meta, raw_template)
            # based on Arrow return RecordBatchReader --- Array not ChunkedArray and memory continual
            # df = conn.execute(req_sql).df() / rows = conn.execute(req_sql).fetchmany(batch_size)
            reader = conn.execute(req_sql).fetch_record_batch(self.batch_size)

            # batch --- multi_columns bytes 
            while True:
                try:
                    batch = reader.read_next_batch()
                    yield batch
                    
                except StopIteration:
                    break
            print("Arrow Query completed.")
        finally:
            self._release_conn(conn)

    def _save_cache(self):
        try:
            with _duck_lock:
                with open(self.view_cache_file, "w") as f:
                    json.dump(list(self._registered_views), f, indent=2)
                # print(f"💾 Saved cache file: {self.view_cache_file}")
        except Exception as e:
            print(f"⚠️ Failed to save cache file: {e}")

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        print("__aexit__", exc_type, exc_val, exc_tb)
        # 等待所有后台任务完成
        self._save_cache()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks = set()


def get_duckdb_manager():
    global _duck_inst
    with _duck_lock:
        if _duck_inst is None:
            _duck_inst = DuckDBManager()
    return _duck_inst
