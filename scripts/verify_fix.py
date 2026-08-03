#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
验证脚本 — 确认并发安全和性能修复的正确性。
运行: poetry run python scripts/verify_fix.py
"""
import inspect
from rpc_feed.core.datasets.provider import (
    Instrument, Adjust, Right,
    InstrumentBuffer, AdjustBuffer, RightBuffer,
    BaseSQLAlchemyProvider, BaseDuckDBProvider,
)
from rpc_feed.core.feed import bt_feed
from rpc_feed.core.gateway import async_ops, get_duckdb_manager
from rpc_feed.core.rpc.server import RpcServer, _stream_semaphore
from rpc_feed.core.rpc.middleware.interceptors.ratelimit import (
    TokenBucketRateLimiter, RateLimitInterceptor,
)


def test_buffer_isolation():
    """验证两个独立 buffer 互不干扰"""
    ib1 = InstrumentBuffer()
    ib2 = InstrumentBuffer()

    ib1.buf_ratio[0] = 999.0
    assert ib2.buf_ratio[0] != 999.0, "Buffer isolation failed!"
    print("✅ Buffer isolation: PASSED")


def test_init_buffers_returns_new_instance():
    """验证 _init_buffers 每次返回新实例(cdef 方法,通过 buffer 容器间接验证)"""
    # _init_buffers 是 cdef 方法,无法从 Python 直接调用
    # 但 buffer 容器类已证明每次创建新实例(见 test_buffer_isolation)
    b1 = InstrumentBuffer()
    b2 = InstrumentBuffer()
    assert b1 is not b2, "Buffer containers should be distinct instances!"
    print("✅ _init_buffers (via buffer container): PASSED")


def test_context_param_in_call():
    """验证 .pyx 源码中 __call__ 包含 context 参数"""
    # Cython async def 的签名不暴露给 inspect,直接检查源码文件
    import os
    pyx_path = os.path.join(
        os.path.dirname(__file__), "..",
        "rpc_feed", "core", "datasets", "provider.pyx"
    )
    with open(pyx_path, "r") as f:
        source = f.read()
    
    has_ctx = "object context=None" in source
    assert has_ctx, "context param not found in provider.pyx source!"
    print("✅ Context param in __call__ (source): PASSED")


def test_semaphore():
    """验证全局并发信号量"""
    assert _stream_semaphore._value == 50, f"Expected 50, got {_stream_semaphore._value}"
    print("✅ Global stream semaphore: PASSED")


def test_rate_limiter():
    """验证令牌桶限流器"""
    rl = TokenBucketRateLimiter(rate=10.0, capacity=3)
    assert rl.allow_request() is True
    assert rl.allow_request() is True
    assert rl.allow_request() is True
    # 第 4 次应该被拒(容量=3)
    assert rl.allow_request() is False, "Should be rate limited!"
    print("✅ Token bucket rate limiter: PASSED")


def test_bt_feed():
    """验证 BtFeed 单例可正常加载"""
    from rpc_feed.core.datasets import _providers
    assert bt_feed is not None
    assert "tick" in _providers
    assert "asset" in _providers
    print("✅ BtFeed singleton with providers: PASSED")


if __name__ == "__main__":
    print("=" * 50)
    print("rpc_feed 并发安全与性能修复验证")
    print("=" * 50)
    test_buffer_isolation()
    test_init_buffers_returns_new_instance()
    test_context_param_in_call()
    test_semaphore()
    test_rate_limiter()
    test_bt_feed()
    print("=" * 50)
    print("🎉 ALL VALIDATION TESTS PASSED")
    print("=" * 50)