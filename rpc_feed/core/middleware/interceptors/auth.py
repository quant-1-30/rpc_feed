#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import grpc


class AuthInterceptor(grpc.ServerInterceptor):
    def intercept_service(self, continuation, handler_call_details):
        # Access the metadata
        metadata = dict(handler_call_details.invocation_metadata)

        # Example: Retrieve an authorization token from the metadata
        token = metadata.get('authorization')

        # Perform some logic with the token
        if token is None or not self.is_valid_token(token):
            # Abort the request if the token is invalid
            context = grpc.ServicerContext()
            context.abort(grpc.StatusCode.UNAUTHENTICATED, "Invalid or missing token")
            return None

        # Continue with the RPC if the token is valid
        return continuation(handler_call_details)

    def is_valid_token(self, token):
        # Implement your token validation logic here
        return token == "valid_token"
