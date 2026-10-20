#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import time
import threading
import grpc
import grpc.aio
from rpc_feed.utils.wrapper import make_grpc_abort_handler


class TokenBucketRateLimiter:
    """
    rate: Token
    capacity: Bulk
    """

    def __init__(self, rate: float, capacity: int):
        self._rate = rate
        self._capacity = capacity
        self._tokens = float(capacity)
        self._last_refill = time.monotonic()
        self._lock = threading.Lock()

    def allow_request(self) -> bool:
        with self._lock:
            now = time.monotonic()
            elapsed = now - self._last_refill
            self._tokens = min(self._capacity, self._tokens + elapsed * self._rate)
            self._last_refill = now

            if self._tokens >= 1.0:
                self._tokens -= 1.0
                return True
            return False


class RateLimitInterceptor(grpc.aio.ServerInterceptor):

    def __init__(self, rate_limiter=None):
        if rate_limiter is None:
            rate = float(os.getenv("GRPC_RATE_LIMIT", "100.0"))
            capacity = int(os.getenv("GRPC_RATE_CAPACITY", "200"))
            rate_limiter = TokenBucketRateLimiter(rate=rate, capacity=capacity)
        self.rate_limiter = rate_limiter

    async def intercept_service(self, continuation, handler_call_details):
        if self.rate_limiter.allow_request():
            return await continuation(handler_call_details)

        return make_grpc_abort_handler(
            grpc.StatusCode.RESOURCE_EXHAUSTED,
            "Rate limit exceeded",
            handler_call_details,
        )
