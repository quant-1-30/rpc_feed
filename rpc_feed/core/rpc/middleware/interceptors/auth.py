#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import hmac
import grpc
import grpc.aio
from rpc_feed.utils.wrapper import make_grpc_abort_handler


class AuthInterceptor(grpc.aio.ServerInterceptor):

    def __init__(self, expected_token: str = None):
        token = expected_token if expected_token is not None else os.getenv("GRPC_AUTH_TOKEN")
        if not token:
            raise ValueError(
                "GRPC_AUTH_TOKEN must be configured when AuthInterceptor is enabled"
            )
        self._expected_token = token

    async def intercept_service(self, continuation, handler_call_details):
        metadata = dict(handler_call_details.invocation_metadata or ())
        token = metadata.get("authorization")

        if token is None or not self.is_valid_token(token):
            return make_grpc_abort_handler(
                grpc.StatusCode.UNAUTHENTICATED,
                "Invalid or missing token",
                handler_call_details,
            )

        return await continuation(handler_call_details)

    def is_valid_token(self, token: str) -> bool:
        return hmac.compare_digest(
            token.encode("utf-8"), self._expected_token.encode("utf-8")
        )
