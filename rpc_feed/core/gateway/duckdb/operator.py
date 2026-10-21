# !/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import atexit
import logging
import duckdb
import asyncio
import queue
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor

from .utils import schema_range, preprocess_req 

from rpc_feed.utils.io import SID_REGEX_BYTES
from rpc_feed.utils.wrapper import env_int, singleton
from bt_protocol.template.duckdb_template import (
    DAILY_TEMPLATE,
    CLOSE_TEMPLATE,
    DAILY_FETCH_TEMPLATE,
    CLOSE_FETCH_TEMPLATE,
)


class ConnectionPoolExhausted(RuntimeError):
    """Raised when no DuckDB connection becomes available within the timeout."""


class ConnectionPool:
    def __init__(self, db_path, max_connections, timeout=None, threads=None):
        self.db_path = db_path
        self.max_connections = max_connections
        self.timeout = env_int("DUCKTIMEOUT", 5) if timeout is None else timeout

        default_threads = max(1, (os.cpu_count() or 4) // max(1, max_connections))
        self.threads = env_int("DUCKTHREADS", default_threads) if threads is None else threads
        
        self._pool = queue.Queue(max_connections)
        self._initialize_pool()
    
    def _initialize_pool(self):
        self._ensure_httpfs_installed()
        for _ in range(self.max_connections):
            conn = duckdb.connect(self.db_path)
            self._init_connection(conn)
            self._pool.put(conn)

    def _ensure_httpfs_installed(self):
        try:
            with duckdb.connect(self.db_path) as conn:
                conn.execute("INSTALL httpfs;")
        except Exception as e:
            logging.warning("DuckDB INSTALL httpfs skipped (offline or already installed): %s", e)
    
    def _init_connection(self, conn):
        """load plugins & tune per-connection concurrency"""
        try:
            conn.execute("LOAD httpfs;")
        except Exception as e:
            logging.warning("DuckDB LOAD httpfs failed: %s", e)
        # DUckDB 1.x based on mmap, so os.replace atomic update will trigger inode/mtime change;    
        # enable_object_cache DuckDB >=0.10 no-op
        conn.execute("SET enable_object_cache=true;")
        # Parquet TIMESTAMP(tz) vs naive TIMESTAMP comparison resolves the naive side
        # in the session timezone -> pin it so the query window never drifts with host TZ
        conn.execute("SET TimeZone='UTC';")
        # 1) DuckDB 1.x default every conn binds cpu_count() thread
        conn.execute(f"SET threads={self.threads};")
        # SQL template ORDER BY and LIMIT are preserved, but the scan phase does not need to preserve order, reducing peak memory by about 30%
        conn.execute("SET preserve_insertion_order=false;")

    def get_connection(self, timeout=None):
        timeout = self.timeout if timeout is None else timeout
        try:
            return self._pool.get(timeout=timeout)
        except queue.Empty:
            raise ConnectionPoolExhausted(
                f"DuckDB connection pool exhausted ({self.max_connections} conns, "
                f"timeout={timeout}s); raise DUCKCONNECTION or lower MAX_CONCURRENT_STREAMS"
            )
    
    def return_connection(self, conn):
        try:
            self._pool.put(conn, timeout=1)
        except queue.Full:
            logging.warning("DuckDB connection pool full, closing connection")
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
    # StopIteration not direct passed to run_in_executor Future replace sentinel 
    # asyncio "StopIteration interacts badly with generators and cannot be raised into a Future"
    _EXHAUSTED = "sentinel"

    def __init__(self):
        self.dataset_root = Path(os.getenv("DUCKDATASET", "~/Downloads/quant/parquet")).expanduser()
        self.batch_size = env_int("DUCKBATCHSIZE", 100000)
        self.regex_rules = SID_REGEX_BYTES

        max_connections = env_int("DUCKCONNECTION", 10)
        # bound concurrent parquet scans to the pool size so the N+1th stream
        # queues instead of raising "pool exhausted" (stream semaphore may be larger)
        self._query_sema = asyncio.Semaphore(max_connections)
        # match max_connections avoid asyncio default thread pool (cpu_count + 4) throttling causing concurrent queries to starve each other
        self._executor = ThreadPoolExecutor(
            max_workers=max_connections, thread_name_prefix="duckdb"
        )

        # exclusive write lock, avoid concurrent processes corrupting the same duckdb_macro.db
        if os.getenv("DUCKDB_FILE", "0") == "1": 
            db_target = str(Path(__file__).resolve().parent / "cache" / os.getenv("DUCKDB", "duckdb_macro.db"))
        else:
        # default sqlite DuckDB :memory:db_name is shared across connections in the same process.
        # mtime/size change of the parquet files will trigger DuckDB to refresh the cache automatically.
            db_target = ":memory:rpc_feed_cache"
        self.connection_pool = ConnectionPool(db_target, max_connections)
        atexit.register(self.close)

    async def __aenter__(self):
        return self
     
    def _group_sids_by_type(self, raw_sids: list) -> dict:
        grouped = {}
        for sid_bytes in raw_sids:
            for name, regex in self.regex_rules.items():
                if regex.match(sid_bytes):
                    grouped.setdefault(name, []).append(sid_bytes.decode("utf-8"))
                    break
        return grouped

    @staticmethod
    def _existing_years(base_prefix: str) -> set:
        if not os.path.isdir(base_prefix):
            return set()
        return {e[5:] for e in os.listdir(base_prefix) if e.startswith("year=")}

    def _glob_path(self, req: dict) -> list:
        root_str = str(self.dataset_root)
        ranges = schema_range(req)
        exact_globs = []
        for type_name, sids in self._group_sids_by_type(req["sid"]).items():
            base_prefix = f"{root_str}/{type_name}"
            valid_years = self._existing_years(base_prefix)
            active_ranges = [(y, q, ym) for y, q, ym in ranges if str(y) in valid_years]
            for sid_str in sids:
                for y, q, ym in active_ranges:
                    dir_path = f"{base_prefix}/year={y}/quarter={q}/sid={sid_str}/date={ym}"
                    if os.path.isdir(dir_path):
                        exact_globs.append(f"{dir_path}/*.parquet")
        return exact_globs

    def _glob_daily_path(self, req: dict) -> list:
        root_str = str(self.dataset_root)
        ranges = schema_range(req)
        daily_globs = []
        for type_name in self._group_sids_by_type(req["sid"]):
            base_prefix = f"{root_str}/daily/{type_name}"
            valid_years = self._existing_years(base_prefix)
            for y, q, ym in ranges:
                if str(y) in valid_years:
                    daily_file = f"{base_prefix}/year={y}/quarter={q}/date={ym}/part-0.parquet"
                    if os.path.exists(daily_file):
                        daily_globs.append(daily_file)
        return daily_globs

    def _resolve_query_plan(self, req: dict, raw_template: str):
        # 99991231 is the max date in the parquet dataset, so if the request is within this range, we can use the daily parquet files for faster access.
        if (
            raw_template in (DAILY_TEMPLATE, CLOSE_TEMPLATE)
            and req["start_date"] <= 99991231
            and req["end_date"] <= 99991231
        ):
            daily_globs = self._glob_daily_path(req)
            if daily_globs:
                fast_tpl = DAILY_FETCH_TEMPLATE if raw_template == DAILY_TEMPLATE else CLOSE_FETCH_TEMPLATE
                return daily_globs, fast_tpl
        return self._glob_path(req), raw_template

    @staticmethod
    def _read_batch_safe(reader):
        try:
            return reader.read_next_batch()
        except StopIteration:
            return DuckDBManager._EXHAUSTED

    async def query(self, req: dict, raw_template: str):
        loop = asyncio.get_running_loop()
        sql_meta = preprocess_req(req)
        sids = sql_meta["sids"]
        if not sids:
            return

        # _resolve_query_plan not occupyDuckDB connection slot  
        file_globs, effective_template = await loop.run_in_executor(
            self._executor, self._resolve_query_plan, req, raw_template
        )
        if not file_globs:
            logging.info("DuckDB query: no file globs")
            return

        async with self._query_sema:
            # avoid await run_in_executor(get_connection) when semaphore acquired, otherwise the connection may be exhausted and raise ConnectionPoolExhausted
            conn = self.connection_pool.get_connection()
            reader = None
            try:
                # conn.execute / to_arrow_reader / read_next_batch sync api block event_loop
                reader = await loop.run_in_executor(
                    self._executor,
                    lambda: conn.execute( # C++ Parameter Binding
                        effective_template,
                        [file_globs, sids, sql_meta["start_str"], sql_meta["end_str"]]
                    ).to_arrow_reader(self.batch_size)
                )

                while True:
                    # read_next_batch sync api block
                    batch = await loop.run_in_executor(self._executor, self._read_batch_safe, reader)
                    if batch is self._EXHAUSTED:
                        break
                    yield batch
            finally:
                if reader is not None:
                    try:
                        reader.close()
                    except Exception:
                        logging.debug("arrow reader close failed", exc_info=True)
                self.connection_pool.return_connection(conn) # avoid connection leak when exception raised in yield
    
    def close(self):
        self.connection_pool.close_all()
        self._executor.shutdown(wait=False)

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        return False


def get_duckdb_manager():
    return DuckDBManager()