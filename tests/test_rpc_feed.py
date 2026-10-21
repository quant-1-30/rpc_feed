#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import gc
import time
import struct
import shutil
import tempfile
import threading
import asyncio
from pathlib import Path
from decimal import Decimal

import pytest
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
import grpc
import grpc.aio
from google.protobuf import empty_pb2

from rpc_feed.core.feed import bt_feed
from rpc_feed.core.datasets import _providers
from rpc_feed.core.rpc.server import _stream_semaphore
from rpc_feed.core.datasets.provider import (
    Tick,
    Daily,
    Close,
    InstrumentBuffer,
    AdjustBuffer,
    RightBuffer,
)
from rpc_feed.core.gateway.duckdb.operator import DuckDBManager, get_duckdb_manager
import rpc_feed.core.gateway.duckdb.operator as duck_op
from rpc_feed.utils.io import upsert_daily_from_arrow_table
from scripts.sync import compact_and_merge
from rpc_feed.core.graph.node.loader import StructUnpacker, TextLoader
from rpc_feed.core.graph.node.format import (
    StructDateParser,
    UniverseDateParser,
    Multiply,
    Dtypes,
)
from rpc_feed.core.graph.node.abnormal import ProcessNa, ProcessInf
from rpc_feed.core.graph.node.writer import ParquetWriter
from rpc_feed.core.rpc.middleware.interceptors.ratelimit import (
    RateLimitInterceptor,
    TokenBucketRateLimiter,
)
from rpc_feed.core.rpc.middleware.interceptors.auth import AuthInterceptor
from rpc_feed.utils.weak_ref import _weak_lru_cache
from bt_protocol.serialize.pb import bt_protocol_service_pb2, bt_protocol_service_pb2_grpc
from bt_protocol.template.duckdb_template import DAILY_TEMPLATE


def test_buffer_isolation_and_singleton_providers():
    b1 = InstrumentBuffer()
    b2 = InstrumentBuffer()
    b1.buf_ratio[0] = 123.45
    assert b1 is not b2
    assert b2.buf_ratio[0] != 123.45
    assert bt_feed is not None
    assert "tick" in _providers and "asset" in _providers
    assert _stream_semaphore._value > 0


def test_schema_range_validation_blocks_dos():
    async def run():
        with pytest.raises(ValueError, match="invalid start_date"):
            async for _ in Tick()(20261301, 20270101, [b"000001"]):
                pass

    asyncio.run(run())


def test_polars_etl_and_daily_sync_and_cache_invalidation():
    tmp = tempfile.mkdtemp(prefix="pytest_rpc_feed_")
    mgr = get_duckdb_manager()
    orig_root = mgr.dataset_root
    mgr.dataset_root = Path(tmp)
    try:
        stock_root = os.path.join(tmp, "stock")
        pw = ParquetWriter(**{"root_path": stock_root, "incr": True})

        # 1. 000001 and 000099
        df1 = pl.DataFrame({
            "datetime": pl.Series(["2026-03-10 09:31:00", "2026-03-10 09:31:00"]).str.to_datetime(),
            "sid": ["000001", "000099"],
            "tick": [1773106260, 1773106260],
            "open": [100, 200], "high": [100, 200], "low": [100, 200], "close": [100, 200],
            "volume": [1000, 2000], "amount": [10000, 20000],
        })
        pw._write_parquet(df1)

        # 2. update 000099 minute and update daily via ParquetWriter
        df2 = pl.DataFrame({
            "datetime": pl.Series(["2026-03-10 09:31:00"]).str.to_datetime(),
            "sid": ["000099"],
            "tick": [1773106260],
            "open": [999], "high": [999], "low": [999], "close": [999],
            "volume": [9000], "amount": [90000],
        })
        pw._write_parquet(df2)

        req_100 = [f"{i:06d}".encode("utf-8") for i in range(1, 101)]

        async def query_daily():
            out = {}
            async for fr in Daily()(20260301, 20260331, req_100):
                b = pa.ipc.open_stream(fr.payload).read_next_batch()
                for s, c in zip(b.column("sid").to_pylist(), b.column("close").to_pylist()):
                    out[s] = c
            return out

        res = asyncio.run(query_daily())
        assert res[b"000001"] == 100
        assert res[b"000099"] == 999

        # 3. Spider float staging -> compact_and_merge -> SyncDataset + daily/
        spider_src = Path(tmp) / "spider_staging"
        sdir = spider_src / "year=2026" / "quarter=Q3" / "sid=1A0001" / "date=202608"
        sdir.mkdir(parents=True, exist_ok=True)
        pq.write_table(
            pa.table({
                "amount": [500000.0],
                "close": [3250.12345],
                "datetime": pa.array([1786066200000], type=pa.timestamp("ms", tz="UTC")),
                "high": [3260.5],
                "low": [3240.25],
                "open": [3255.0],
                "tick": [1786066200],
                "volume": [10000],
            }),
            sdir / "daily_benchmark_20260807_0.parquet",
        )
        bm_root = Path(tmp) / "benchmark"
        assert compact_and_merge(spider_src, bm_root) == 1
        assert not any(spider_src.rglob("*.parquet"))

        async def query_bm_daily():
            out = {}
            async for fr in Daily()(20260801, 20260831, [b"1A0001"]):
                b = pa.ipc.open_stream(fr.payload).read_next_batch()
                for s, c in zip(b.column("sid").to_pylist(), b.column("close").to_pylist()):
                    out[s] = c
            return out

        bm_res = asyncio.run(query_bm_daily())
        assert bm_res[b"1A0001"] == 325012345
    finally:
        mgr.dataset_root = orig_root
        shutil.rmtree(tmp, ignore_errors=True)


def test_grpc_aio_interceptors():
    class DummyServicer(bt_protocol_service_pb2_grpc.btDataFeedServicer):
        async def HeartBeat(self, request, context):
            return empty_pb2.Empty()

    async def run():
        rl = RateLimitInterceptor(TokenBucketRateLimiter(rate=0.0, capacity=1))
        auth = AuthInterceptor(expected_token="secret")
        srv = grpc.aio.server(interceptors=[auth, rl])
        bt_protocol_service_pb2_grpc.add_btDataFeedServicer_to_server(DummyServicer(), srv)
        port = srv.add_insecure_port("127.0.0.1:0")
        await srv.start()
        async with grpc.aio.insecure_channel(f"127.0.0.1:{port}") as ch:
            stub = bt_protocol_service_pb2_grpc.btDataFeedStub(ch)
            with pytest.raises(grpc.aio.AioRpcError) as exc1:
                await stub.HeartBeat(empty_pb2.Empty())
            assert exc1.value.code() == grpc.StatusCode.UNAUTHENTICATED

            await stub.HeartBeat(empty_pb2.Empty(), metadata=(("authorization", "secret"),))

            with pytest.raises(grpc.aio.AioRpcError) as exc2:
                await stub.HeartBeat(empty_pb2.Empty(), metadata=(("authorization", "secret"),))
            assert exc2.value.code() == grpc.StatusCode.RESOURCE_EXHAUSTED
        await srv.stop(0)

    asyncio.run(run())


def test_weak_lru_cache_dead_ref_eviction():
    class Item:
        pass

    @_weak_lru_cache(maxsize=1)
    def compute(x):
        return id(x)

    a = Item()
    compute(a)
    del a
    gc.collect()
    b = Item()
    assert compute(b) == id(b)


def test_cython_so_freshness_and_audit_guards(monkeypatch):
    repo_root = Path(__file__).resolve().parent.parent
    for pyx in (repo_root / "rpc_feed").rglob("*.pyx"):
        so_files = list(pyx.parent.glob(f"{pyx.stem}.*.so"))
        assert so_files, f"Missing compiled .so for {pyx}"
        so_mtime = max(s.stat().st_mtime for s in so_files)
        assert so_mtime >= pyx.stat().st_mtime, f"Stale .so for {pyx}"
        pxd = pyx.with_suffix(".pxd")
        if pxd.exists():
            assert so_mtime >= pxd.stat().st_mtime, f"Stale .so vs {pxd}"

    # @singleton preserves class identity and isolates subclasses
    assert isinstance(get_duckdb_manager(), DuckDBManager)
    from rpc_feed.utils.wrapper import singleton
    @singleton
    class _BaseS:
        pass
    class _SubS(_BaseS):
        pass
    assert _BaseS() is not _SubS()
    assert isinstance(_SubS(), _SubS)

    # AuthInterceptor fails fast when token is missing
    monkeypatch.delenv("GRPC_AUTH_TOKEN", raising=False)
    with pytest.raises(ValueError, match="GRPC_AUTH_TOKEN"):
        AuthInterceptor()

    # _parse_date unifies YYYYMMDD and epoch seconds to +08:00
    from rpc_feed.core.gateway.duckdb.utils import preprocess_req
    meta = preprocess_req({"start_date": 1704038400, "end_date": 20240101, "sid": [b"000001"]})
    assert meta["start_str"].endswith("+08:00")
    assert meta["end_str"].endswith("+08:00")
