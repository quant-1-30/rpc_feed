#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import time
import threading
import grpc


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


class RateLimitInterceptor(grpc.ServerInterceptor):

    def __init__(self, rate_limiter=None):
        if rate_limiter is None:
            rate_limiter = TokenBucketRateLimiter(rate=100.0, capacity=200)
        self.rate_limiter = rate_limiter

    def intercept_service(self, continuation, handler_call_details):
        if not self.rate_limiter.allow_request():
            def deny_handler(request_or_iterator, context):
                context.set_code(grpc.StatusCode.RESOURCE_EXHAUSTED)
                context.set_details('Rate limit exceeded')
                return None
            return deny_handler
        return continuation(handler_call_details)
    