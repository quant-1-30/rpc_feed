#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import grpc


# gRPC Interceptor for rate limiting
class RateLimitInterceptor(grpc.ServerInterceptor):

    def __init__(self, rate_limiter):
        self.rate_limiter = rate_limiter

    def intercept_service(self, continuation, handler_call_details):
        # continuation is function that will be called to handle the request
        # handler_call_details is the details of the call
        if not self.rate_limiter.allow_request():
            # access metadatga
            metadata = handler_call_details.invocation_metadata
            # token from metadatqa
            context = grpc.ServicerContext()
            # error handle
            context.set_code(grpc.StatusCode.RESOURCE_EXHAUSTED)
            context.set_details('Rate limit exceeded')
            # context.abort(grpc.StatusCode.UNAUTHENTICATED, "*****")
            return lambda request, context: None  # Return an empty response or handle as needed
        return continuation(handler_call_details)
    
