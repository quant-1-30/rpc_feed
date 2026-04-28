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
from .utils import schema_range, preprocess_req
from rpc_feed.utils.wrapper import singleton


class ConnectionPool:
    def __init__(self, db_path, max_connections):
        self.db_path = db_path
        self.max_connections = max_connections
        self._pool = queue.Queue(max_connections)
        self._initialize_pool()
    
    def _initialize_pool(self):
        """初始化连接池"""
        for _ in range(self.max_connections):
            conn = duckdb.connect(self.db_path)
            self._init_connection(conn)
            self._pool.put(conn)
    
    def _init_connection(self, conn):
        """load plugins"""
        conn.execute("INSTALL httpfs;")
        conn.execute("LOAD httpfs;")
        conn.execute("SET enable_object_cache=true;")
        # conn.execute("SET max_expression_depth = 2000;") # not UNION ALL 
    
    def get_connection(self, timeout=5):
        try:
            return self._pool.get(timeout=timeout)
        except queue.Empty:
            raise Exception("Connection pool exhausted")
    
    def return_connection(self, conn):
        try:
            self._pool.put(conn, timeout=1)
        except:
            conn.close()
            
    def close_all(self):
        while not self._pool.empty():
            try:
                conn = self._pool.get_nowait()
                conn.close()
            except queue.Empty:
                break


@singleton
class DuckDBManager:
    """
    - avoid Macro / View and Catalog
    - Glob + Hive Partitioning
    - Parameters Binding solve AST Depth Limit
    """
    def __init__(self):
        self.dataset_root = Path(os.getenv("DUCKDATASET")).expanduser()
        self.batch_size = int(os.getenv("DUCKBATCHSIZE", 100000))

        self.regex_rules = {
            "stock": re.compile(b"^(6|0|3)\d{5}"), # encode('ascii') 
            "fund": re.compile(b"^(51|15|16)\d{4}"),
            "benchmark": re.compile(b"^(000001|000688|399001|399006)")
        }

        max_connections = int(os.getenv("DUCKCONNECTION", 10))
        cache_path = Path(__file__).resolve().parent / "cache" / os.getenv("DUCKDB") 
        self.connection_pool = ConnectionPool(cache_path, max_connections)

    async def __aenter__(self):
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        return False

    def _glob_path(self, req: dict) -> list:
        ranges = schema_range(req) 
        exact_globs =[]
        
        for sid_bytes in req["sid"]:
            cat_name = None
            for name, regex in self.regex_rules.items():
                if regex.match(sid_bytes):
                    cat_name = name
                    break
                    
            if not cat_name:
                continue 
                
            sid_str = sid_bytes.decode("utf-8")
            for y, q, ym in ranges:
                dir_path = os.path.join(
                    self.dataset_root, 
                    cat_name, 
                    f"year={y}", 
                    f"quarter={q}", 
                    f"sid={sid_str}", 
                    f"date={ym}"
                )
                
                if os.path.exists(dir_path): # os cache
                    exact_globs.append(os.path.join(dir_path, "*.parquet"))
        return exact_globs
    
    def _glob_experiment_path(self, req: dict) -> list:
        ranges = schema_range(req) 
        exact_globs =[]
                
        for y, q, ym in ranges:
            dir_path = os.path.join(
                self.dataset_root, 
                "experiment",
                req["sid"][0], # experiment_id --- sid
                f"year={y}", 
                f"quarter={q}", 
                f"date={ym}"
            )
            
        if os.path.exists(dir_path): # os cache
            exact_globs.append(os.path.join(dir_path, "*.parquet"))
        return exact_globs

    async def query(self, req: dict, raw_template: str):
        conn = self.connection_pool.get_connection()
        try:
            file_globs = self._glob_path(req)
            if not file_globs:
                return
                
            sql_meta = preprocess_req(req)
            sids = sql_meta["sids"]
            if not sids:
                return

            # C++ Parameter Binding
            reader = conn.execute(
                raw_template, 
                [file_globs, sids, sql_meta["start_str"], sql_meta["end_str"]]
            ).fetch_record_batch(self.batch_size)

            while True:
                try:
                    yield reader.read_next_batch()
                except StopIteration:
                    break
        finally:
            self.connection_pool.return_connection(conn)


_duck_inst = None
_duck_lock = threading.Lock()


def get_duckdb_manager():
    global _duck_inst
    with _duck_lock:
        if _duck_inst is None:
            _duck_inst = DuckDBManager()
    return _duck_inst
