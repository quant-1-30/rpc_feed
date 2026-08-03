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
            async for response in response_iterator:
                if context.done():
                    logging.info("CalendarCall: client disconnected")
                    return
                yield response

    async def InstrumentCall(
        self,
        request: bt_protocol_service_pb2.QuoteRequest,
        context: grpc.ServicerContext,
    ) -> bt_protocol_service_pb2.ArrowFrame: # type: ignore
        
        await self._set_context(context)

        async with _stream_semaphore:
            response_iterator = bt_feed.fetch("asset", request.start_date, request.end_date, list(request.sid), context) 

            async for response in response_iterator:
                # print("InstrumentCall repsonse size ", response.ByteSize())
                if context.done():
                    logging.info("InstrumentCall: client disconnected")
                    return
                yield response
    
    async def DailyStreamCall(
        self,
        request: bt_protocol_service_pb2.QuoteRequest,
        context: grpc.ServicerContext,
    ) -> bt_protocol_service_pb2.ArrowFrame: # type: ignore
        
        await self._set_context(context)

        async with _stream_semaphore:
            response_iterator = bt_feed.fetch("daily", request.start_date, request.end_date, list(request.sid), context)
            async for response in response_iterator:
                # print("IndexStreamCall repsonse size ", response.ByteSize())
                if context.done():
                    logging.info("DailyStreamCall: client disconnected")
                    return
                yield response
    
    async def TickStreamCall(
        self,
        request: bt_protocol_service_pb2.QuoteRequest,
        context: grpc.ServicerContext,
    ) -> bt_protocol_service_pb2.ArrowFrame: # type: ignore
        
        await self._set_context(context)

        async with _stream_semaphore:
            response_iterator = bt_feed.fetch("tick", request.start_date, request.end_date, list(request.sid), context)
            async for response in response_iterator:
                # print("LineStreamCall repsonse size ", response.ByteSize())
                if context.done():
                    logging.info("TickStreamCall: client disconnected")
                    return
                yield response

    async def CloseStreamCall(
        self,
        request: bt_protocol_service_pb2.QuoteRequest,
        context: grpc.ServicerContext,
    ) -> bt_protocol_service_pb2.ArrowFrame: # type: ignore
        
        await self._set_context(context)

        async with _stream_semaphore:
            response_iterator = bt_feed.fetch("close", request.start_date, request.end_date, list(request.sid), context)
            async for response in response_iterator:
                # print("CloseStreamCall repsonse size ", response.ByteSize())
                if context.done():
                    logging.info("CloseStreamCall: client disconnected")
                    return
                yield response

    async def AdjustmentStreamCall(
        self,
        request: bt_protocol_service_pb2.QuoteRequest,
        context: grpc.ServicerContext,
    ) -> bt_protocol_service_pb2.ArrowFrame: # type: ignore
        
        await self._set_context(context)

        async with _stream_semaphore:
            response_iterator = bt_feed.fetch("adjust", request.start_date, request.end_date, list(request.sid), context)
            async for response in response_iterator:
                # print("AdjustmentStreamCall repsonse size ", response.ByteSize())
                if context.done():
                    logging.info("AdjustmentStreamCall: client disconnected")
                    return
                yield response

    async def RightStreamCall(
        self,
        request: bt_protocol_service_pb2.QuoteRequest,
        context: grpc.ServicerContext,
    ) -> bt_protocol_service_pb2.ArrowFrame: # type: ignore
        
        await self._set_context(context)

        async with _stream_semaphore:
            response_iterator = bt_feed.fetch("right", request.start_date, request.end_date, list(request.sid), context)
            async for response in response_iterator:
                # print("RightStreamCall repsonse size ", response.ByteSize())
                if context.done():
                    logging.info("RightStreamCall: client disconnected")
                    return
                yield response