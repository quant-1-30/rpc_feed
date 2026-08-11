#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import grpc
import asyncio
import logging
from google.protobuf.json_format import MessageToDict

from rpc_feed.core.feed import bt_feed
from rpc_feed.core.datasets import _providers
from bt_protocol.serialize.pb import bt_protocol_service_pb2, bt_protocol_service_pb2_grpc

# Semaphore
_MAX_CONCURRENT_STREAMS = int(os.getenv("MAX_CONCURRENT_STREAMS", "50"))
_stream_semaphore = asyncio.Semaphore(_MAX_CONCURRENT_STREAMS)


class RpcServer(bt_protocol_service_pb2_grpc.btDataFeedServicer):

    def __init__(self):
        self._id_counter = 0

    async def _set_context(self, context: grpc.ServicerContext) -> None:

        # NoCompression / Gzip
        # context is_active to check if the request is cancelled
        # context.set_compression(grpc.Compression.Deflate)
        context.set_trailing_metadata(
            (
                ("checksum-bin", b"I agree"),
                ("retry", "false"),
            )
        )

    async def _safe_stream(self, name: str, response_iterator, context: grpc.ServicerContext):
        """
        gRPC C-core HTTP/2 raise
        ``ExecuteBatchError`` Python RuntimeError / grpc.RpcError GOAWAY too_many_pings 
        """
        async for response in response_iterator:
            # TOCTOU: done() yield maybe RST_STREAM
            if context.done():
                logging.info("%s: client disconnected", name)
                return
            try:
                yield response
            except asyncio.CancelledError:
                raise
            except (grpc.RpcError, RuntimeError) as e:
                if context.done():
                    logging.info("%s: client disconnected during send", name)
                else:
                    logging.warning("%s: send failed: %r", name, e)
                return
        
    async def CalendarCall(
        self,
        request: bt_protocol_service_pb2.QuoteRequest,
        context: grpc.ServicerContext,
    ) -> bt_protocol_service_pb2.ArrowFrame: # type: ignore
        
        await self._set_context(context)

        for key, value in context.invocation_metadata():
            print("Received initial metadata: key=%s value=%s" % (key, value))

        # calendar provider UNIMPLEMENTED avoid KeyError 
        if "calendar" not in _providers:
            await context.abort(grpc.StatusCode.UNIMPLEMENTED, "calendar provider is not registered")
            return

        async with _stream_semaphore:
            response_iterator = bt_feed.fetch("calendar", request.start_date, request.end_date, list(request.sid), context)
            async for response in self._safe_stream("CalendarCall", response_iterator, context):
                yield response

    async def InstrumentCall(
        self,
        request: bt_protocol_service_pb2.QuoteRequest,
        context: grpc.ServicerContext,
    ) -> bt_protocol_service_pb2.ArrowFrame: # type: ignore
        
        await self._set_context(context)

        async with _stream_semaphore:
            response_iterator = bt_feed.fetch("asset", request.start_date, request.end_date, list(request.sid), context)
            async for response in self._safe_stream("InstrumentCall", response_iterator, context):
                yield response
    
    async def DailyStreamCall(
        self,
        request: bt_protocol_service_pb2.QuoteRequest,
        context: grpc.ServicerContext,
    ) -> bt_protocol_service_pb2.ArrowFrame: # type: ignore
        
        await self._set_context(context)

        async with _stream_semaphore:
            response_iterator = bt_feed.fetch("daily", request.start_date, request.end_date, list(request.sid), context)
            async for response in self._safe_stream("DailyStreamCall", response_iterator, context):
                yield response
    
    async def TickStreamCall(
        self,
        request: bt_protocol_service_pb2.QuoteRequest,
        context: grpc.ServicerContext,
    ) -> bt_protocol_service_pb2.ArrowFrame: # type: ignore
        
        await self._set_context(context)

        async with _stream_semaphore:
            response_iterator = bt_feed.fetch("tick", request.start_date, request.end_date, list(request.sid), context)
            async for response in self._safe_stream("TickStreamCall", response_iterator, context):
                yield response

    async def CloseStreamCall(
        self,
        request: bt_protocol_service_pb2.QuoteRequest,
        context: grpc.ServicerContext,
    ) -> bt_protocol_service_pb2.ArrowFrame: # type: ignore
        
        await self._set_context(context)

        async with _stream_semaphore:
            response_iterator = bt_feed.fetch("close", request.start_date, request.end_date, list(request.sid), context)
            async for response in self._safe_stream("CloseStreamCall", response_iterator, context):
                yield response

    async def AdjustmentStreamCall(
        self,
        request: bt_protocol_service_pb2.QuoteRequest,
        context: grpc.ServicerContext,
    ) -> bt_protocol_service_pb2.ArrowFrame: # type: ignore
        
        await self._set_context(context)

        async with _stream_semaphore:
            response_iterator = bt_feed.fetch("adjust", request.start_date, request.end_date, list(request.sid), context)
            async for response in self._safe_stream("AdjustmentStreamCall", response_iterator, context):
                yield response

    async def RightStreamCall(
        self,
        request: bt_protocol_service_pb2.QuoteRequest,
        context: grpc.ServicerContext,
    ) -> bt_protocol_service_pb2.ArrowFrame: # type: ignore
        
        await self._set_context(context)

        async with _stream_semaphore:
            response_iterator = bt_feed.fetch("right", request.start_date, request.end_date, list(request.sid), context)
            async for response in self._safe_stream("RightStreamCall", response_iterator, context):
                yield response
